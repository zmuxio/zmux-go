package zmux

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
)

// transferOneStream writes total bytes on one client stream and drains them
// on the server, failing the test if either session dies on the way.
func transferOneStream(t *testing.T, client, server *Conn, total int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	received := make(chan int64, 1)
	readErr := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			readErr <- err
			return
		}
		n, err := io.Copy(io.Discard, accepted)
		if err != nil {
			readErr <- err
			return
		}
		received <- n
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	chunk := bytes.Repeat([]byte("z"), 64<<10)
	for sent := 0; sent < total; sent += len(chunk) {
		if _, err := stream.Write(chunk); err != nil {
			t.Fatalf("Write after %d bytes: %v (client %v, server %v)", sent, err, client.err(), server.err())
		}
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	select {
	case n := <-received:
		if n != int64(total) {
			t.Fatalf("received %d bytes, want %d", n, total)
		}
	case err := <-readErr:
		t.Fatalf("server read: %v (client %v, server %v)", err, client.err(), server.err())
	case <-ctx.Done():
		t.Fatalf("transfer timed out (client %v, server %v)", client.err(), server.err())
	}
	if err := client.err(); err != nil {
		t.Fatalf("client session failed: %v", err)
	}
	if err := server.err(); err != nil {
		t.Fatalf("server session failed: %v", err)
	}
}

// The repository-default receiver sends a stream and a session MAX_DATA for
// about every 16 KiB consumed. Those frames advance flow control and must not
// count as an inbound control flood on the data sender (SPEC §11, §13).
func TestBulkTransferDoesNotTripInboundControlBudget(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	transferOneStream(t, client, server, 96<<20)
}

func TestBulkTransferWithTinyControlBudgetCountsOnlyNonAdvancingFlowControl(t *testing.T) {
	t.Parallel()

	clientCfg := DefaultConfig()
	clientCfg.InboundControlFrameBudget = 16
	clientCfg.InboundMixedFrameBudget = 16
	serverCfg := DefaultConfig()
	serverCfg.InboundControlFrameBudget = 16
	serverCfg.InboundMixedFrameBudget = 16
	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)
	transferOneStream(t, client, server, 16<<20)
}

func TestAdvancingMaxDataIsNotChargedToInboundControlBudget(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.mu.Lock()
	c.abuse.controlFrameBudget = 16
	c.abuse.mixedFrameBudget = 16
	stream := c.newLocalStreamLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	testMarkLocalOpenCommitted(stream)
	testMarkLocalOpenVisible(stream)
	c.registry.streams[stream.id] = stream
	sessionMax := c.flow.sendSessionMax
	streamMax := stream.sendMax
	c.mu.Unlock()

	for i := 0; i < 4096; i++ {
		sessionMax++
		if err := c.handleFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(sessionMax)}); err != nil {
			t.Fatalf("increasing session MAX_DATA %d err = %v, want nil", i+1, err)
		}
		if i%4 == 0 {
			streamMax++
			if err := c.handleFrame(Frame{Type: FrameTypeMAXDATA, StreamID: stream.id, Payload: mustEncodeVarint(streamMax)}); err != nil {
				t.Fatalf("increasing stream MAX_DATA %d err = %v, want nil", i+1, err)
			}
		}
	}

	// A MAX_DATA that does not raise the limit is still charged.
	var err error
	for i := 0; i < 17 && err == nil; i++ {
		err = c.handleFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(sessionMax)})
	}
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("repeated non-advancing MAX_DATA err = %v, want %s from the control budget", err, CodeProtocol)
	}
}

func TestBlockedThatReleasesCreditIsNotChargedToInboundControlBudget(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.mu.Lock()
	c.abuse.controlFrameBudget = 16
	c.abuse.mixedFrameBudget = 16
	c.mu.Unlock()

	for i := 0; i < 1024; i++ {
		c.mu.Lock()
		c.flow.recvSessionPending = 1
		c.mu.Unlock()
		if err := c.handleFrame(Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}); err != nil {
			t.Fatalf("BLOCKED %d with pending credit err = %v, want nil", i+1, err)
		}
	}

	var err error
	for i := 0; i < 17 && err == nil; i++ {
		err = c.handleFrame(Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)})
	}
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("repeated no-op BLOCKED err = %v, want %s", err, CodeProtocol)
	}
}

// A BLOCKED the peer sent before it saw our latest grant crossed that grant on
// the wire. It is stale, not a no-op; only repeats with no grant in between
// count toward the no-op BLOCKED budget.
func TestBlockedCrossingAGrantIsNotANoOp(t *testing.T) {
	tests := []struct {
		name  string
		frame func(stream *nativeStream) Frame
		grant func(c *Conn, stream *nativeStream)
	}{
		{
			name:  "session",
			frame: func(*nativeStream) Frame { return Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)} },
			grant: func(c *Conn, _ *nativeStream) { c.flow.recvSessionAdvertised += 16 },
		},
		{
			name: "stream",
			frame: func(stream *nativeStream) Frame {
				return Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(0)}
			},
			grant: func(_ *Conn, stream *nativeStream) { stream.recvAdvertised += 16 },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, _, stop := newInvalidPolicyConn(t)
			defer stop()
			stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
				SendHalf: "send_open",
				RecvHalf: "recv_open",
			})
			c.mu.Lock()
			c.abuse.noOpBlockedFloodLimit = 2
			c.mu.Unlock()
			frame := tc.frame(stream)

			for i := 0; i < 2; i++ {
				if err := c.handleBlockedFrame(frame); err != nil {
					t.Fatalf("no-op BLOCKED %d err = %v, want nil", i+1, err)
				}
			}
			c.mu.Lock()
			tc.grant(c, stream)
			c.mu.Unlock()
			for i := 0; i < 3; i++ {
				if err := c.handleBlockedFrame(frame); err != nil {
					t.Fatalf("BLOCKED %d after a grant err = %v, want nil", i+1, err)
				}
			}
			if err := c.handleBlockedFrame(frame); !IsErrorCode(err, CodeProtocol) {
				t.Fatalf("third repeated no-op BLOCKED err = %v, want %s", err, CodeProtocol)
			}
		})
	}
}

// With a zero initial window nothing is ever consumed, so credit must be
// granted from the standing target when the peer reports BLOCKED or the
// application waits to read (IMPLEMENTATION §3.2).
func TestZeroInitialWindowsStillCarryData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		zero func(settings *Settings)
		uni  bool
	}{
		{name: "bidi_stream", zero: func(s *Settings) { s.InitialMaxStreamDataBidiPeerOpened = 0 }},
		{name: "uni_stream", zero: func(s *Settings) { s.InitialMaxStreamDataUni = 0 }, uni: true},
		{name: "session", zero: func(s *Settings) { s.InitialMaxData = 0 }},
		{name: "all", zero: func(s *Settings) {
			s.InitialMaxData = 0
			s.InitialMaxStreamDataBidiPeerOpened = 0
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			serverCfg := DefaultConfig()
			tc.zero(&serverCfg.Settings)
			client, server := newConnPairWithConfig(t, nil, serverCfg)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			payload := []byte("0123456789")
			writeErr := make(chan error, 1)
			go func() {
				var err error
				if tc.uni {
					var stream NativeSendStream
					stream, err = client.OpenUniStream(ctx)
					if err == nil {
						_, err = stream.Write(payload)
					}
				} else {
					var stream NativeStream
					stream, err = client.OpenStream(ctx)
					if err == nil {
						_, err = stream.Write(payload)
					}
				}
				writeErr <- err
			}()

			var reader interface {
				io.Reader
				SetReadDeadline(time.Time) error
			}
			if tc.uni {
				accepted, err := server.AcceptUniStream(ctx)
				if err != nil {
					t.Fatalf("AcceptUniStream: %v", err)
				}
				reader = accepted
			} else {
				accepted, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatalf("AcceptStream: %v", err)
				}
				reader = accepted
			}
			if err := reader.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatalf("SetReadDeadline: %v", err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(reader, got); err != nil {
				t.Fatalf("ReadFull: %v (client %v, server %v)", err, client.err(), server.err())
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("read %q, want %q", got, payload)
			}
			if err := <-writeErr; err != nil {
				t.Fatalf("Write: %v", err)
			}
		})
	}
}

func TestZeroWindowBlockedGetsStandingCreditGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		zero      func(settings *Settings)
		blockedOn func(streamID uint64) uint64
	}{
		{
			name:      "stream",
			zero:      func(s *Settings) { s.InitialMaxStreamDataBidiPeerOpened = 0 },
			blockedOn: func(streamID uint64) uint64 { return streamID },
		},
		{
			name:      "session",
			zero:      func(s *Settings) { s.InitialMaxData = 0 },
			blockedOn: func(uint64) uint64 { return 0 },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			tc.zero(&cfg.Settings)
			_, peer := newRawPeerConn(t, cfg, DefaultSettings())
			id := peer.bidiID(0)
			scope := tc.blockedOn(id)

			// The zero-length opener needs no credit; BLOCKED reports the
			// exhausted scope. Nobody reads on the server side.
			peer.writeData(t, id, 0, nil)
			peer.write(t, Frame{Type: FrameTypeBLOCKED, StreamID: scope, Payload: mustEncodeVarint(0)})
			grant := peer.await(t, "MAX_DATA grant", isFrame(FrameTypeMAXDATA, scope))
			if v := maxDataValue(t, grant); v == 0 {
				t.Fatalf("MAX_DATA grant = 0, want standing credit")
			}
			peer.awaitPong(t, 1)
			peer.assertNoClose(t)
		})
	}
}

func TestZeroWindowBlockingReadGrantsCreditWithoutPeerBlocked(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Settings.InitialMaxData = 0
	cfg.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	server, peer := newRawPeerConn(t, cfg, DefaultSettings())
	id := peer.bidiID(0)
	peer.writeData(t, id, 0, nil)

	ctx, cancel := context.WithTimeout(context.Background(), testSignalTimeout)
	defer cancel()
	accepted, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if err := accepted.SetReadDeadline(time.Now().Add(2 * testSignalTimeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	readDone := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4)
		n, _ := io.ReadFull(accepted, buf)
		readDone <- buf[:n]
	}()

	streamGrant := maxDataValue(t, peer.await(t, "stream MAX_DATA", isFrame(FrameTypeMAXDATA, id)))
	sessionGrant := maxDataValue(t, peer.await(t, "session MAX_DATA", isFrame(FrameTypeMAXDATA, 0)))
	if streamGrant < 4 || sessionGrant < 4 {
		t.Fatalf("grants = (stream %d, session %d), want room for the 4-byte write", streamGrant, sessionGrant)
	}
	peer.writeData(t, id, 0, []byte("data"))
	select {
	case got := <-readDone:
		if string(got) != "data" {
			t.Fatalf("read %q, want %q", got, "data")
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("read did not complete after the grant")
	}
	for _, frame := range peer.received() {
		if frame.Type == FrameTypeBLOCKED {
			t.Fatalf("unexpected BLOCKED %+v", frame)
		}
	}
	peer.assertNoClose(t)
}

func TestZeroWindowGrantRespectsMemoryPressureAndRemainingCredit(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(c *Conn, stream *nativeStream)
	}{
		{
			// Standing growth stops under memory pressure, so the exhausted
			// window stays closed until the application frees memory.
			name: "memory_pressure",
			prepare: func(c *Conn, stream *nativeStream) {
				stream.recvAdvertised = 0
				c.flow.recvSessionAdvertised = 0
				c.flow.sessionMemoryCap = 64
				c.flow.recvSessionUsed = 64
			},
		},
		{
			// Credit still remains, so a BLOCKED forces no growth.
			name: "credit_remaining",
			prepare: func(c *Conn, stream *nativeStream) {
				stream.recvAdvertised = 10
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, _, stop := newInvalidFrameConn(t, 0)
			defer stop()
			stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
				SendHalf: "send_open",
				RecvHalf: "recv_open",
			})
			c.mu.Lock()
			tc.prepare(c, stream)
			streamBefore := stream.recvAdvertised
			sessionBefore := c.flow.recvSessionAdvertised
			c.mu.Unlock()

			if err := c.handleBlockedFrame(Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(0)}); err != nil {
				t.Fatalf("stream BLOCKED: %v", err)
			}
			if err := c.handleBlockedFrame(Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}); err != nil {
				t.Fatalf("session BLOCKED: %v", err)
			}

			c.mu.Lock()
			defer c.mu.Unlock()
			if stream.recvAdvertised != streamBefore {
				t.Fatalf("stream advertised = %d, want %d (no grant)", stream.recvAdvertised, streamBefore)
			}
			if tc.name == "memory_pressure" && c.flow.recvSessionAdvertised != sessionBefore {
				t.Fatalf("session advertised = %d, want %d (no grant)", c.flow.recvSessionAdvertised, sessionBefore)
			}
			if c.pendingStreamControlCountLocked(streamControlMaxData) != 0 {
				t.Fatal("stream MAX_DATA queued, want none")
			}
		})
	}
}

func TestZeroWindowReadStoppedDirectionGetsNoStreamGrant(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_stop_sent",
	})
	c.mu.Lock()
	stream.recvAdvertised = 0
	c.mu.Unlock()

	if err := c.handleBlockedFrame(Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(0)}); err != nil {
		t.Fatalf("stream BLOCKED: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.recvAdvertised != 0 {
		t.Fatalf("read-stopped stream advertised = %d, want 0", stream.recvAdvertised)
	}
}
