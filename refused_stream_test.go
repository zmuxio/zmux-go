package zmux

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func countFrames(frames []Frame, match func(Frame) bool) int {
	n := 0
	for _, frame := range frames {
		if match(frame) {
			n++
		}
	}
	return n
}

// waitSessionMaxData waits up to testSignalTimeout for a session MAX_DATA of
// at least want and returns the highest one received. A PING round trip does
// not order it: the writer sends urgent frames (PONG, ABORT) ahead of a
// pending MAX_DATA.
func (p *rawFramePeer) waitSessionMaxData(t *testing.T, want uint64) uint64 {
	t.Helper()
	deadline := time.NewTimer(testSignalTimeout)
	defer deadline.Stop()
	for {
		got, _ := p.highestMaxData(t, 0)
		if got >= want {
			return got
		}
		p.mu.Lock()
		readErr := p.readErr
		p.mu.Unlock()
		if readErr != nil {
			return got
		}
		select {
		case <-p.notify:
		case <-deadline.C:
			return got
		}
	}
}

// A peer that opened a stream before it saw our GOAWAY may legally keep
// sending on it until our ABORT(REFUSED_STREAM) arrives. Those frames must
// neither fail the session nor get refused again (SPEC §3.1, §6.9), and the
// refused DATA must be credited back to the session window (SPEC §8).
func TestGoAwayRefusedStreamIgnoresRacingControlFrames(t *testing.T) {
	t.Parallel()

	server, peer := newRawPeerConn(t, nil, DefaultSettings())
	draining := acceptRawPeerStream(t, server, peer, peer.bidiID(0))
	if err := server.GoAway(peer.bidiID(0), 0); err != nil {
		t.Fatalf("GoAway: %v", err)
	}
	peer.await(t, "GOAWAY", isFrame(FrameTypeGOAWAY, 0))

	server.mu.Lock()
	beforeReceived := server.flow.recvSessionReceived
	beforeAdvertised := server.flow.recvSessionAdvertised
	server.mu.Unlock()

	refusedBidi := peer.bidiID(1)
	refusedUni := peer.uniID(0)
	abortFirst := peer.bidiID(2)
	chunk := bytes.Repeat([]byte("r"), 1000)
	peer.writeData(t, refusedBidi, 0, chunk)
	peer.write(t, Frame{Type: FrameTypeStopSending, StreamID: refusedBidi, Payload: mustEncodeVarint(uint64(CodeCancelled))})
	peer.write(t, Frame{Type: FrameTypeBLOCKED, StreamID: refusedBidi, Payload: mustEncodeVarint(1000)})
	peer.write(t, Frame{Type: FrameTypeMAXDATA, StreamID: refusedBidi, Payload: mustEncodeVarint(1 << 20)})
	peer.writeData(t, refusedBidi, 0, chunk)
	peer.write(t, Frame{Type: FrameTypeRESET, StreamID: refusedBidi, Payload: mustEncodeVarint(uint64(CodeCancelled))})
	peer.writeData(t, refusedBidi, FrameFlagFIN, chunk)
	peer.writeData(t, refusedUni, 0, chunk)
	peer.write(t, Frame{Type: FrameTypeRESET, StreamID: refusedUni, Payload: mustEncodeVarint(uint64(CodeCancelled))})
	peer.writeData(t, refusedUni, FrameFlagFIN, chunk)
	peer.write(t, Frame{Type: FrameTypeABORT, StreamID: abortFirst, Payload: mustEncodeVarint(uint64(CodeCancelled))})
	peer.write(t, Frame{Type: FrameTypeABORT, StreamID: abortFirst, Payload: mustEncodeVarint(uint64(CodeCancelled))})
	peer.awaitPong(t, 1)

	peer.assertNoClose(t)
	if err := server.err(); err != nil {
		t.Fatalf("session failed by frames racing a GOAWAY refusal: %v", err)
	}
	if got := server.State(); got != SessionStateDraining {
		t.Fatalf("session state = %v, want draining", got)
	}
	frames := peer.received()
	for _, id := range []uint64{refusedBidi, refusedUni, abortFirst} {
		if got := countFrames(frames, isAbortWithCode(id, CodeRefusedStream)); got != 1 {
			t.Fatalf("ABORT(REFUSED_STREAM) on stream %d sent %d times, want once", id, got)
		}
	}

	const refused = 5 * 1000
	server.mu.Lock()
	gotReceived := server.flow.recvSessionReceived - beforeReceived
	gotAdvertised := server.flow.recvSessionAdvertised - beforeAdvertised
	liveStreams := len(server.registry.streams)
	server.mu.Unlock()
	if gotReceived != refused {
		t.Fatalf("refused DATA counted %d session bytes, want %d", gotReceived, refused)
	}
	if gotAdvertised < refused {
		t.Fatalf("session limit grew by %d, want >= %d (refused bytes released)", gotAdvertised, refused)
	}
	if got := peer.waitSessionMaxData(t, beforeAdvertised+refused); got < beforeAdvertised+refused {
		t.Fatalf("session MAX_DATA = %d, want >= %d", got, beforeAdvertised+refused)
	}
	if liveStreams != 1 {
		t.Fatalf("live streams = %d, want only the draining stream", liveStreams)
	}

	// The stream accepted before the GOAWAY drains normally.
	peer.writeData(t, draining.id, FrameFlagFIN, []byte("tail"))
	got, err := io.ReadAll(draining)
	if err != nil || string(got) != "tail" {
		t.Fatalf("draining stream read = %q, %v; want tail", got, err)
	}
	if _, err := draining.Write([]byte("reply")); err != nil {
		t.Fatalf("draining stream write: %v", err)
	}
	peer.await(t, "reply DATA", isFrame(FrameTypeDATA, draining.id))
}

// Refused openers must not leak session credit: a sender that respects the
// session window can push several windows' worth of refused DATA without ever
// stalling (SPEC §8).
func TestRefusedOpenersReleaseSessionCredit(t *testing.T) {
	t.Parallel()

	const window = 4096
	tests := []struct {
		name   string
		config func(*Config)
		refuse func(t *testing.T, server *Conn)
		// streamID returns the stream ID of the i-th refused opener.
		streamID func(peer *rawFramePeer, i uint64) uint64
	}{
		{
			name:   "incoming_limit",
			config: func(cfg *Config) { cfg.Settings.MaxIncomingStreamsBidi = 0 },
			refuse: func(*testing.T, *Conn) {},
			streamID: func(peer *rawFramePeer, i uint64) uint64 {
				return peer.bidiID(i)
			},
		},
		{
			name:   "local_goaway",
			config: func(*Config) {},
			refuse: func(t *testing.T, server *Conn) {
				if err := server.GoAway(0, 0); err != nil {
					t.Fatalf("GoAway: %v", err)
				}
			},
			// Several frames per refused ID: GOAWAY refusals do not consume it.
			streamID: func(peer *rawFramePeer, i uint64) uint64 {
				return peer.bidiID(i / 3)
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			cfg.Settings.InitialMaxData = window
			tc.config(cfg)
			server, peer := newRawPeerConn(t, cfg, DefaultSettings())
			tc.refuse(t, server)
			if tc.name == "local_goaway" {
				peer.await(t, "GOAWAY", isFrame(FrameTypeGOAWAY, 0))
			}

			chunk := bytes.Repeat([]byte("r"), 1000)
			limit := uint64(window)
			sent := uint64(0)
			for i := uint64(0); sent < 3*window; i++ {
				if sent+uint64(len(chunk)) > limit {
					peer.awaitPong(t, byte(i))
					if got := peer.waitSessionMaxData(t, sent+uint64(len(chunk))); got > limit {
						limit = got
					}
					if sent+uint64(len(chunk)) > limit {
						t.Fatalf("session credit stalled after %d refused bytes: limit %d", sent, limit)
					}
				}
				peer.writeData(t, tc.streamID(peer, i), 0, chunk)
				sent += uint64(len(chunk))
			}
			peer.awaitPong(t, 0xff)
			peer.assertNoClose(t)
			if err := server.err(); err != nil {
				t.Fatalf("session failed by refused openers: %v", err)
			}

			server.mu.Lock()
			received := server.flow.recvSessionReceived
			server.mu.Unlock()
			if received != sent {
				t.Fatalf("recvSessionReceived = %d, want %d", received, sent)
			}
		})
	}
}

// Refused opener bytes are still checked against the session window: a peer
// that overruns it fails the session with FLOW_CONTROL.
func TestRefusedOpenerOverSessionWindowFailsFlowControl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config func(*Config)
		goAway bool
	}{
		{name: "incoming_limit", config: func(cfg *Config) { cfg.Settings.MaxIncomingStreamsBidi = 0 }},
		{name: "local_goaway", config: func(*Config) {}, goAway: true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			cfg.Settings.InitialMaxData = 1000
			tc.config(cfg)
			server, peer := newRawPeerConn(t, cfg, DefaultSettings())
			if tc.goAway {
				if err := server.GoAway(0, 0); err != nil {
					t.Fatalf("GoAway: %v", err)
				}
				peer.await(t, "GOAWAY", isFrame(FrameTypeGOAWAY, 0))
			}

			peer.writeData(t, peer.bidiID(0), 0, make([]byte, 1001))
			peer.await(t, "CLOSE(FLOW_CONTROL)", isCloseWithCode(CodeFlowControl))
			ctx, cancel := context.WithTimeout(context.Background(), testSignalTimeout)
			defer cancel()
			if err := server.Wait(ctx); !IsErrorCode(err, CodeFlowControl) {
				t.Fatalf("session err = %v, want %s", err, CodeFlowControl)
			}
		})
	}
}

// Go pair version of the race: the client opens and stops reading a stream
// before the server's GOAWAY reaches it. The server must refuse that stream
// once and keep draining the stream it already accepted.
func TestGoAwayRaceWithPeerCloseReadKeepsDrainingStream(t *testing.T) {
	t.Parallel()

	client, server, delayed := newConnPairWithDelayedServer(t, nil, nil)
	ctx, cancel := testContext(t)
	defer cancel()

	draining, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open draining stream: %v", err)
	}
	if _, err := draining.Write([]byte("a")); err != nil {
		t.Fatalf("write draining stream: %v", err)
	}
	accepted, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("accept draining stream: %v", err)
	}
	if _, err := io.ReadFull(accepted, make([]byte, 1)); err != nil {
		t.Fatalf("read draining stream: %v", err)
	}

	// Hold the server's GOAWAY on the wire while the client keeps opening.
	acceptedID := accepted.StreamID()
	delayed.SetWriteDelay(200 * time.Millisecond)
	goAwayDone := make(chan error, 1)
	go func() { goAwayDone <- server.GoAway(acceptedID, 0) }()
	awaitConnState(t, server, testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.sessionControl.localGoAwayBidi == acceptedID
	}, "server never applied its GOAWAY watermark")

	refused, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream racing the GOAWAY: %v", err)
	}
	if _, err := refused.Write([]byte("x")); err != nil {
		t.Fatalf("write stream racing the GOAWAY: %v", err)
	}
	if err := refused.CloseRead(); err != nil {
		t.Fatalf("CloseRead on stream racing the GOAWAY: %v", err)
	}

	delayed.SetWriteDelay(0)
	if err := <-goAwayDone; err != nil {
		t.Fatalf("GoAway: %v", err)
	}
	if _, err := accepted.Write([]byte("reply")); err != nil {
		t.Fatalf("server write on draining stream: %v", err)
	}
	got := make([]byte, len("reply"))
	if _, err := io.ReadFull(draining, got); err != nil || string(got) != "reply" {
		t.Fatalf("client read on draining stream = %q, %v; want reply", got, err)
	}
	if err := server.err(); err != nil {
		t.Fatalf("server session failed: %v", err)
	}
	if err := client.err(); err != nil {
		t.Fatalf("client session failed: %v", err)
	}
}
