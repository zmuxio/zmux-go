package zmux

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
)

// writeRaw sends bytes that need not form a valid frame.
func (p *rawFramePeer) writeRaw(t *testing.T, raw []byte) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := p.conn.Write(raw)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("raw peer write % x: %v", raw, err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatalf("raw peer write % x did not complete", raw)
	}
}

// awaitSessionCloseCode waits for the session's CLOSE(code) on the wire and
// for the session itself to end with that code.
func awaitSessionCloseCode(t *testing.T, c *Conn, peer *rawFramePeer, code ErrorCode) {
	t.Helper()
	peer.await(t, "CLOSE("+code.String()+")", isCloseWithCode(code))
	ctx, cancel := testContext(t)
	defer cancel()
	err := c.Wait(ctx)
	if got, ok := ErrorCodeOf(err); !ok || got != code {
		t.Fatalf("session ended with %v, want %s", err, code)
	}
}

func TestNonCanonicalFrameLengthClosesSessionWithProtocol(t *testing.T) {
	t.Parallel()

	c, peer := newRawPeerConn(t, nil, DefaultSettings())
	// frame_length=2 encoded in two bytes, then DATA on the peer's first stream.
	peer.writeRaw(t, []byte{0x40, 0x02, 0x01, byte(peer.bidiID(0))})
	awaitSessionCloseCode(t, c, peer, CodeProtocol)
}

func TestEXTPayloadShorterThanExtTypeClosesSessionWithProtocol(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"empty_payload", []byte{0x02, 0x0b, 0x00}},
		{"truncated_ext_type", []byte{0x03, 0x0b, 0x00, 0x40}},
		{"non_canonical_ext_type", []byte{0x04, 0x0b, 0x00, 0x40, 0x01}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, peer := newRawPeerConn(t, nil, DefaultSettings())
			peer.writeRaw(t, tc.raw)
			awaitSessionCloseCode(t, c, peer, CodeProtocol)
		})
	}
}

func priorityUpdateFrameBytes(t *testing.T, streamID uint64, body ...byte) []byte {
	t.Helper()
	return rawInvalidFrameBytes(t, byte(FrameTypeEXT), streamID, append([]byte{byte(EXTPriorityUpdate)}, body...))
}

func TestUnnegotiatedPriorityUpdateIsIgnoredWithoutParsing(t *testing.T) {
	t.Parallel()

	c, peer := newRawPeerConn(t, &Config{DisableCapabilities: true}, DefaultSettings())
	streamID := peer.bidiID(0)
	peer.writeData(t, streamID, 0, []byte("hi"))
	// Once priority_update is not negotiated the receiver must ignore these
	// frames (SPEC §7.6), however malformed or misplaced their payload is.
	for _, raw := range [][]byte{
		priorityUpdateFrameBytes(t, streamID, 0x01, 0x01),                               // truncated TLV header
		priorityUpdateFrameBytes(t, streamID, 0x01, 0x02, 0x01),                         // TLV value overrun
		priorityUpdateFrameBytes(t, streamID, 0x01, 0x01, 0x02, 0x01, 0x01, 0x03, 0x01), // duplicate, then truncated
		priorityUpdateFrameBytes(t, 0, 0x01, 0x01, 0x02),                                // stream 0
	} {
		peer.writeRaw(t, raw)
	}
	peer.awaitPong(t, 1)
	peer.assertNoClose(t)
	if got := c.State(); got != SessionStateReady {
		t.Fatalf("session state = %v, want ready", got)
	}
	if got := c.Stats().Diagnostics.DroppedPriorityUpdates; got != 0 {
		t.Fatalf("dropped priority updates = %d, want 0 for unnegotiated frames", got)
	}

	ctx, cancel := testContext(t)
	defer cancel()
	stream, err := c.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	got := make([]byte, 2)
	if _, err := io.ReadFull(stream, got); err != nil || string(got) != "hi" {
		t.Fatalf("read = (%q, %v), want %q", got, err, "hi")
	}
}

func TestNegotiatedPriorityUpdateValidationOrder(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		zero bool
		body []byte
		code ErrorCode
	}{
		{"stream_zero", true, []byte{0x01, 0x01, 0x02}, CodeProtocol},
		{"truncated_tlv_header", false, []byte{0x01}, CodeFrameSize},
		{"tlv_value_overrun", false, []byte{0x01, 0x02, 0x01}, CodeFrameSize},
		// A duplicate singleton must not hide a later structural error.
		{"duplicate_then_truncated", false, []byte{0x01, 0x01, 0x02, 0x01, 0x01, 0x03, 0x01}, CodeFrameSize},
		{"duplicate_then_overrun", false, []byte{0x02, 0x01, 0x05, 0x02, 0x01, 0x06, 0x01, 0x04, 0x00}, CodeFrameSize},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, peer := newRawPeerConn(t, nil, DefaultSettings())
			if !c.config.negotiated.Capabilities.Has(CapabilityPriorityUpdate) {
				t.Fatal("priority_update not negotiated")
			}
			streamID := peer.bidiID(0)
			peer.writeData(t, streamID, 0, []byte("hi"))
			target := streamID
			if tc.zero {
				target = 0
			}
			peer.writeRaw(t, priorityUpdateFrameBytes(t, target, tc.body...))
			awaitSessionCloseCode(t, c, peer, tc.code)
			if got := c.Stats().Diagnostics.DroppedPriorityUpdates; got != 0 {
				t.Fatalf("dropped priority updates = %d, want 0 for a malformed frame", got)
			}
		})
	}

	t.Run("duplicate_with_well_formed_tail_is_dropped", func(t *testing.T) {
		t.Parallel()
		c, peer := newRawPeerConn(t, nil, DefaultSettings())
		streamID := peer.bidiID(0)
		peer.writeData(t, streamID, 0, []byte("hi"))
		peer.writeRaw(t, priorityUpdateFrameBytes(t, streamID, 0x01, 0x01, 0x02, 0x01, 0x01, 0x03, 0x3f, 0x00))
		peer.awaitPong(t, 2)
		peer.assertNoClose(t)
		if got := c.Stats().Diagnostics.DroppedPriorityUpdates; got != 1 {
			t.Fatalf("dropped priority updates = %d, want 1", got)
		}
	})
}

func TestOpenMetadataDuplicateThenTruncatedTLVClosesSessionWithFrameSize(t *testing.T) {
	t.Parallel()

	c, peer := newRawPeerConn(t, nil, DefaultSettings())
	metadata := []byte{0x01, 0x01, 0x02, 0x01, 0x01, 0x03, 0x01}
	payload := append([]byte{byte(len(metadata))}, metadata...)
	payload = append(payload, 'h', 'i')
	peer.writeRaw(t, rawInvalidFrameBytes(t, byte(FrameTypeDATA)|FrameFlagOpenMetadata, peer.bidiID(0), payload))
	awaitSessionCloseCode(t, c, peer, CodeFrameSize)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if stream, err := c.AcceptStream(ctx); err == nil {
		t.Fatalf("accepted stream %d from a malformed opener", stream.StreamID())
	}
}

func TestOpenMetadataOnUsedStreamIsSessionProtocolInEveryState(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		kind      string
		ownership string
		recvHalf  string
	}{
		{"peer_bidi_recv_open", "bidi", "peer_owned", "recv_open"},
		{"peer_bidi_recv_fin", "bidi", "peer_owned", "recv_fin"},
		{"peer_bidi_recv_reset", "bidi", "peer_owned", "recv_reset"},
		{"peer_bidi_recv_aborted", "bidi", "peer_owned", "recv_aborted"},
		{"peer_bidi_recv_stop_sent", "bidi", "peer_owned", "recv_stop_sent"},
		{"local_uni_send_only", "uni_local_send_only", "local_owned", ""},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, frames, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
			defer stop()
			role := c.config.negotiated.LocalRole
			streamID := state.FirstPeerStreamID(role, true)
			if tc.ownership == "local_owned" {
				streamID = state.FirstLocalStreamID(role, false)
			}
			stream := seedStateFixtureStream(t, c, streamID, tc.kind, tc.ownership, stateHalfExpect{RecvHalf: tc.recvHalf})
			if tc.ownership == "local_owned" {
				c.mu.Lock()
				testMarkLocalOpenVisible(stream)
				c.mu.Unlock()
			}

			err := c.handleDataFrame(Frame{
				Type:     FrameTypeDATA,
				Flags:    FrameFlagOpenMetadata,
				StreamID: streamID,
				Payload:  invalidOpenMetadataPayload(t, []TLV{{Type: uint64(MetadataOpenInfo), Value: []byte("ss")}}, []byte("hhi")),
			})
			if !IsErrorCode(err, CodeProtocol) {
				t.Fatalf("DATA|OPEN_METADATA on used stream err = %v, want session %s", err, CodeProtocol)
			}
			assertNoQueuedFrame(t, frames)
		})
	}
}

func TestOpenMetadataAfterPeerFinClosesSessionWithProtocol(t *testing.T) {
	t.Parallel()

	c, peer := newRawPeerConn(t, nil, DefaultSettings())
	streamID := peer.bidiID(0)
	peer.writeData(t, streamID, FrameFlagFIN, []byte("x"))
	peer.writeRaw(t, rawInvalidFrameBytes(t, byte(FrameTypeDATA)|FrameFlagOpenMetadata, streamID,
		invalidOpenMetadataPayload(t, []TLV{{Type: uint64(MetadataOpenInfo), Value: []byte("ss")}}, []byte("hhi"))))
	awaitSessionCloseCode(t, c, peer, CodeProtocol)
	for _, frame := range peer.received() {
		if frame.Type == FrameTypeABORT && frame.StreamID == streamID {
			t.Fatalf("session answered with ABORT on stream %d, want only CLOSE(PROTOCOL)", streamID)
		}
	}
}
