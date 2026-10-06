package zmux

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// The tests in this file replay the vendored invalid fixtures against real
// sessions, so the error code a fixture names is checked on the wire (the
// CLOSE the peer receives) and not only at the codec or handler layer.

// liveInvalidFrameCase is an invalid fixture as a raw peer sends it to a live
// session: optional preceding frames, then the invalid frame.
type liveInvalidFrameCase struct {
	cfg     *Config
	prelude [][]byte
	raw     []byte
}

// liveInvalidFixtureSessionPeerBidi is the first bidirectional stream ID of
// the raw peer, which is the initiator; fixtures name it as stream 4.
const liveInvalidFixtureSessionPeerBidi = 4

func liveInvalidFixtureCase(t *testing.T, fixture invalidFixture) (liveInvalidFrameCase, bool) {
	t.Helper()

	openStream := mustMarshalLiveFrame(t, Frame{Type: FrameTypeDATA, StreamID: liveInvalidFixtureSessionPeerBidi, Payload: []byte("hi")})
	if raw, ok := invalidFixtureFrameBytes(t, fixture); ok {
		// Opening stream 4 first gives frames that target it (such as
		// frame_priority_update_noncanonical_value) the open stream their
		// input_shape describes; the frame-level rejection must not depend on it.
		return liveInvalidFrameCase{prelude: [][]byte{openStream}, raw: raw}, true
	}
	switch fixture.ID {
	case "frame_first_max_data_on_unused_stream",
		"frame_first_blocked_on_unused_stream",
		"frame_first_stop_sending_on_unused_stream",
		"frame_first_reset_on_unused_stream":
		var shape invalidStreamShape
		if err := json.Unmarshal(fixture.InputShape, &shape); err != nil {
			t.Fatalf("decode input_shape for %s: %v", fixture.ID, err)
		}
		if shape.StreamID != liveInvalidFixtureSessionPeerBidi || shape.InitialStreamState != "idle" {
			t.Fatalf("fixture %s input_shape = %+v, want idle stream %d", fixture.ID, shape, liveInvalidFixtureSessionPeerBidi)
		}
		return liveInvalidFrameCase{raw: mustMarshalLiveFrame(t, invalidFirstFrame(t, shape.IncomingFrame, shape.StreamID))}, true
	case "frame_peer_stream_id_gap":
		return liveInvalidFrameCase{
			raw: mustMarshalLiveFrame(t, Frame{Type: FrameTypeDATA, StreamID: liveInvalidFixtureSessionPeerBidi + 4, Payload: []byte("x")}),
		}, true
	case "frame_data_open_metadata_without_capability":
		cfg := DefaultConfig()
		cfg.DisableCapabilities = true
		return liveInvalidFrameCase{
			cfg: cfg,
			raw: rawInvalidFrameBytes(t, byte(FrameTypeDATA)|FrameFlagOpenMetadata, liveInvalidFixtureSessionPeerBidi,
				invalidOpenMetadataPayload(t, []TLV{{Type: uint64(MetadataOpenInfo), Value: []byte("a")}}, []byte("hi"))),
		}, true
	case "frame_data_open_metadata_on_open_stream":
		return liveInvalidFrameCase{
			prelude: [][]byte{openStream},
			raw: rawInvalidFrameBytes(t, byte(FrameTypeDATA)|FrameFlagOpenMetadata, liveInvalidFixtureSessionPeerBidi,
				invalidOpenMetadataPayload(t, []TLV{{Type: uint64(MetadataOpenInfo), Value: []byte("a")}}, []byte("hi"))),
		}, true
	case "frame_data_exceeds_session_max_data":
		var shape struct {
			StreamID             uint64 `json:"stream_id"`
			SessionBytesReceived uint64 `json:"session_bytes_received"`
			PeerSessionMaxData   uint64 `json:"peer_session_max_data"`
			IncomingDataLength   uint64 `json:"incoming_data_length"`
		}
		if err := json.Unmarshal(fixture.InputShape, &shape); err != nil {
			t.Fatalf("decode input_shape for %s: %v", fixture.ID, err)
		}
		cfg := DefaultConfig()
		cfg.Settings.InitialMaxData = shape.PeerSessionMaxData
		return liveInvalidFrameCase{
			cfg:     cfg,
			prelude: [][]byte{mustMarshalLiveFrame(t, Frame{Type: FrameTypeDATA, StreamID: shape.StreamID, Payload: make([]byte, shape.SessionBytesReceived)})},
			raw:     mustMarshalLiveFrame(t, Frame{Type: FrameTypeDATA, StreamID: shape.StreamID, Payload: make([]byte, shape.IncomingDataLength)}),
		}, true
	case "session_goaway_last_accepted_increase":
		var shape struct {
			PriorBidi    uint64 `json:"prior_last_accepted_bidi_stream_id"`
			PriorUni     uint64 `json:"prior_last_accepted_uni_stream_id"`
			IncomingBidi uint64 `json:"incoming_last_accepted_bidi_stream_id"`
			IncomingUni  uint64 `json:"incoming_last_accepted_uni_stream_id"`
		}
		if err := json.Unmarshal(fixture.InputShape, &shape); err != nil {
			t.Fatalf("decode input_shape for %s: %v", fixture.ID, err)
		}
		return liveInvalidFrameCase{
			prelude: [][]byte{mustMarshalLiveFrame(t, Frame{Type: FrameTypeGOAWAY, Payload: mustGoAwayPayload(t, shape.PriorBidi, shape.PriorUni, uint64(CodeNoError), "")})},
			raw:     mustMarshalLiveFrame(t, Frame{Type: FrameTypeGOAWAY, Payload: mustGoAwayPayload(t, shape.IncomingBidi, shape.IncomingUni, uint64(CodeNoError), "")}),
		}, true
	default:
		return liveInvalidFrameCase{}, false
	}
}

func mustMarshalLiveFrame(t *testing.T, frame Frame) []byte {
	t.Helper()
	raw, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal %+v: %v", frame, err)
	}
	return raw
}

// writeInvalid sends raw, which the session may reject and close the
// transport on before it has read every byte.
func (p *rawFramePeer) writeInvalid(t *testing.T, raw []byte) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := p.conn.Write(raw)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("raw peer write % x: %v", raw, err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatalf("raw peer write % x did not complete", raw)
	}
}

func runLiveInvalidFrameCase(t *testing.T, tc liveInvalidFrameCase, code ErrorCode) {
	t.Helper()

	c, peer := newRawPeerConn(t, tc.cfg, DefaultSettings())
	if got := peer.bidiID(0); got != liveInvalidFixtureSessionPeerBidi {
		t.Fatalf("raw peer first bidi stream = %d, want %d", got, liveInvalidFixtureSessionPeerBidi)
	}
	for _, raw := range tc.prelude {
		peer.writeRaw(t, raw)
	}
	peer.writeInvalid(t, tc.raw)
	awaitSessionCloseCode(t, c, peer, code)
	// A session error ends the session; it must not also be answered on a
	// stream, least of all with a first-frame ABORT on an unseen stream ID.
	for _, frame := range peer.received() {
		if frame.Type == FrameTypeABORT {
			t.Fatalf("session answered with ABORT on stream %d before CLOSE(%s) (received %s)", frame.StreamID, code, peer.describe())
		}
	}
}

func TestInvalidFixtureFramesCloseLiveSession(t *testing.T) {
	t.Parallel()
	for _, fixture := range loadInvalidFixtures(t) {
		fixture := fixture
		if fixture.ExpectedResult.Scope != "session" || fixture.ExpectedResult.Error == "" {
			continue
		}
		tc, ok := liveInvalidFixtureCase(t, fixture)
		if !ok {
			t.Fatalf("session-scoped invalid fixture %q has no live-session replay", fixture.ID)
		}
		t.Run(fixture.ID, func(t *testing.T) {
			t.Parallel()
			runLiveInvalidFrameCase(t, tc, fixtureErrorCode(t, fixture.ExpectedResult.Error))
		})
	}
}

func TestWireInvalidFixturesCloseLiveSession(t *testing.T) {
	t.Parallel()
	for _, fixture := range loadWireFixtures(t, "wire_invalid.ndjson") {
		fixture := fixture
		if fixture.Category != "frame_invalid" {
			continue
		}
		t.Run(fixture.ID, func(t *testing.T) {
			t.Parallel()
			var cfg *Config
			if fixture.ReceiverLimits != nil {
				cfg = DefaultConfig()
				limits := fixtureLimits(fixture.ReceiverLimits)
				cfg.Settings.MaxFramePayload = limits.MaxFramePayload
				cfg.Settings.MaxControlPayloadBytes = limits.MaxControlPayloadBytes
				cfg.Settings.MaxExtensionPayloadBytes = limits.MaxExtensionPayloadBytes
			}
			runLiveInvalidFrameCase(t, liveInvalidFrameCase{
				cfg: cfg,
				prelude: [][]byte{mustMarshalLiveFrame(t, Frame{
					Type:     FrameTypeDATA,
					StreamID: liveInvalidFixtureSessionPeerBidi,
					Payload:  []byte("hi"),
				})},
				raw: mustHex(t, fixture.Hex),
			}, fixtureErrorCode(t, fixture.ExpectError))
		})
	}
}

// TestInvalidPrefaceFixturesFailLiveEstablishment sends each raw invalid
// peer preface to a real server. Establishment must fail with the fixture's
// code, and the server must still send its own preface followed by
// CLOSE(code).
func TestInvalidPrefaceFixturesFailLiveEstablishment(t *testing.T) {
	t.Parallel()
	for _, fixture := range loadInvalidFixtures(t) {
		fixture := fixture
		if fixture.Category != "preface" || fixture.Hex == "" {
			continue
		}
		t.Run(fixture.ID, func(t *testing.T) {
			t.Parallel()
			want := fixtureErrorCode(t, fixture.ExpectedResult.Error)
			raw := mustHex(t, fixture.Hex)
			session, rawCh, err := establishOverLoopback(t, true, 0, nil, func(conn net.Conn) rawTCPResult {
				return runRawTCPPeer(conn, 0, raw, 3*time.Second)
			})
			if session != nil {
				_ = session.Close()
				t.Fatal("establishment succeeded, want failure")
			}
			if !IsErrorCode(err, want) {
				t.Fatalf("establish err = %v, want %s", err, want)
			}
			assertEstablishmentPrefaceThenClose(t, awaitRawTCPPeer(t, rawCh), RoleResponder, want)
		})
	}
}
