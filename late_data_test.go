package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
)

// acceptRawPeerStream opens a bidi stream from the raw peer with one byte,
// accepts it on the session and reads that byte back.
func acceptRawPeerStream(t *testing.T, server *Conn, peer *rawFramePeer, streamID uint64) *nativeStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testSignalTimeout)
	defer cancel()
	peer.writeData(t, streamID, 0, []byte("x"))
	accepted, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	stream := mustNativeStreamImpl(accepted)
	if stream.id != streamID {
		t.Fatalf("accepted stream %d, want %d", stream.id, streamID)
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("read opener byte: %v", err)
	}
	return stream
}

// writeRawTail sends n bytes of DATA on streamID in max-size frames.
func writeRawTail(t *testing.T, peer *rawFramePeer, streamID, n uint64) {
	t.Helper()
	frame := make([]byte, DefaultSettings().MaxFramePayload)
	for n > 0 {
		chunk := uint64(len(frame))
		if chunk > n {
			chunk = n
		}
		peer.writeData(t, streamID, 0, frame[:chunk])
		n -= chunk
	}
}

// A peer may have its whole advertised stream credit in flight when the local
// side stops reading or aborts. That tail must be discarded without failing
// the session (SPEC §9.3, §9.5); only bytes beyond the credit are a violation.
func TestLateTailUpToStreamCreditAfterLocalStopKeepsSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stop      func(stream *nativeStream) error
		stopFrame FrameType
		// overrun is what one byte beyond the advertised credit must produce.
		overrun func(streamID uint64) func(Frame) bool
		alive   bool
	}{
		{
			name:      "close_read",
			stop:      func(stream *nativeStream) error { return stream.CloseRead() },
			stopFrame: FrameTypeStopSending,
			// A live read-stopped direction keeps enforcing stream credit.
			overrun: func(streamID uint64) func(Frame) bool { return isAbortWithCode(streamID, CodeFlowControl) },
			alive:   true,
		},
		{
			name:      "close_with_error",
			stop:      func(stream *nativeStream) error { return stream.CloseWithError(uint64(CodeCancelled), "") },
			stopFrame: FrameTypeABORT,
			// After a local ABORT only a peer that ignored its credit can exceed
			// the captured allowance; that keeps the existing escalation.
			overrun: func(uint64) func(Frame) bool { return isCloseWithCode(CodeProtocol) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server, peer := newRawPeerConn(t, nil, DefaultSettings())
			id := peer.bidiID(0)
			stream := acceptRawPeerStream(t, server, peer, id)

			server.mu.Lock()
			streamAdvertised := stream.recvAdvertised
			outstanding := csub(stream.recvAdvertised, stream.recvReceived)
			sessionAdvertised := server.flow.recvSessionAdvertised
			server.mu.Unlock()
			if outstanding <= 2*DefaultSettings().MaxFramePayload {
				t.Fatalf("outstanding stream credit = %d, want several max-size frames", outstanding)
			}

			if err := tc.stop(stream); err != nil {
				t.Fatalf("local stop: %v", err)
			}
			peer.await(t, tc.stopFrame.String(), isFrame(tc.stopFrame, id))

			// The tail the peer had already sent before it saw the stop.
			writeRawTail(t, peer, id, outstanding)
			peer.awaitPong(t, 1)
			peer.assertNoClose(t)
			if err := server.err(); err != nil {
				t.Fatalf("session failed by in-credit late tail: %v", err)
			}
			// The PONG only proves the tail was handled; the writer may emit the
			// urgent PONG ahead of the pending session MAX_DATA release.
			peer.await(t, "session MAX_DATA releasing the discarded tail", func(f Frame) bool {
				if f.Type != FrameTypeMAXDATA || f.StreamID != 0 {
					return false
				}
				v, _, err := ParseVarint(f.Payload)
				return err == nil && v >= sessionAdvertised+outstanding
			})
			if got, ok := peer.highestMaxData(t, id); ok && got > streamAdvertised {
				t.Fatalf("stream MAX_DATA = %d after the stop, want no growth beyond %d", got, streamAdvertised)
			}

			peer.writeData(t, id, 0, []byte("!"))
			peer.await(t, "overrun response", tc.overrun(id))
			if !tc.alive {
				return
			}
			peer.awaitPong(t, 2)
			peer.assertNoClose(t)
			next := acceptRawPeerStream(t, server, peer, peer.bidiID(1))
			if next == nil || server.err() != nil {
				t.Fatalf("session unusable after stream FLOW_CONTROL abort: %v", server.err())
			}
		})
	}
}

// Same-implementation pair: cancelling or aborting a stream while the peer is
// mid-transfer must never take the session down.
func TestStopReadingDuringTransferKeepsSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		stop func(stream *nativeStream) error
	}{
		{name: "close_read", stop: func(stream *nativeStream) error { return stream.CloseRead() }},
		{name: "cancel_read", stop: func(stream *nativeStream) error { return stream.CancelRead(uint64(CodeCancelled)) }},
		{name: "close_with_error", stop: func(stream *nativeStream) error {
			return stream.CloseWithError(uint64(CodeCancelled), "")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, server := newConnPair(t)
			ctx, cancel := testContext(t)
			defer cancel()
			payload := make([]byte, 65535)

			for round := 0; round < 12; round++ {
				cs, err := client.OpenStream(ctx)
				if err != nil {
					t.Fatalf("round %d OpenStream: %v", round, err)
				}
				writeDone := make(chan struct{})
				go func() {
					defer close(writeDone)
					_, _ = cs.Write(payload)
				}()
				accepted, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatalf("round %d AcceptStream: %v", round, err)
				}
				ss := mustNativeStreamImpl(accepted)
				if _, err := io.ReadFull(ss, make([]byte, 1)); err != nil {
					t.Fatalf("round %d first read: %v", round, err)
				}
				if err := tc.stop(ss); err != nil {
					t.Fatalf("round %d stop: %v", round, err)
				}
				select {
				case <-writeDone:
				case <-time.After(testSignalTimeout):
					t.Fatalf("round %d writer did not finish after the stop", round)
				}
				_ = ss.Close()
				if err := server.err(); err != nil {
					t.Fatalf("round %d server session failed: %v", round, err)
				}
				if err := client.err(); err != nil {
					t.Fatalf("round %d client session failed: %v", round, err)
				}
			}

			cs, err := client.OpenStream(ctx)
			if err != nil {
				t.Fatalf("final OpenStream: %v", err)
			}
			if _, err := cs.Write([]byte("ok")); err != nil {
				t.Fatalf("final Write: %v", err)
			}
			accepted, err := server.AcceptStream(ctx)
			if err != nil {
				t.Fatalf("final AcceptStream: %v", err)
			}
			got := make([]byte, 2)
			if _, err := io.ReadFull(accepted, got); err != nil || string(got) != "ok" {
				t.Fatalf("final read = %q, %v; want ok", got, err)
			}
		})
	}
}

// The writer aborting its stream while the graceful CloseWrite that answers
// the peer's STOP_SENDING is in progress is a stream-local outcome. It used
// to rewind the abort to stop_seen and close the whole session with the
// stream's CANCELLED error.
func TestPeerStopSendingRacingLocalAbortKeepsSession(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()
	payload := make([]byte, 65536)

	for round := 0; round < 300; round++ {
		cs, err := client.OpenStream(ctx)
		if err != nil {
			t.Fatalf("round %d OpenStream: %v", round, err)
		}
		writeDone := make(chan struct{})
		go func() {
			defer close(writeDone)
			_, _ = cs.Write(payload)
			_ = cs.CloseWithError(uint64(CodeCancelled), "")
		}()
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatalf("round %d AcceptStream: %v", round, err)
		}
		ss := mustNativeStreamImpl(accepted)
		// The writer's ABORT may arrive before either call; read and stop
		// errors are stream-local here, only the sessions must survive.
		_, _ = io.ReadFull(ss, make([]byte, 1))
		_ = ss.CloseRead()
		select {
		case <-writeDone:
		case <-time.After(testSignalTimeout):
			t.Fatalf("round %d writer did not finish after the stop", round)
		}
		_ = ss.Close()
		if err := client.err(); err != nil {
			t.Fatalf("round %d client session failed: %v", round, err)
		}
		if err := server.err(); err != nil {
			t.Fatalf("round %d server session failed: %v", round, err)
		}
	}

	if _, err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("final Ping: %v", err)
	}
	if err := client.err(); err != nil {
		t.Fatalf("client session failed: %v", err)
	}
	if err := server.err(); err != nil {
		t.Fatalf("server session failed: %v", err)
	}
}

// The aggregate late-data counter tracks late bytes still retained by a
// stream or tombstone, not a lifetime total: many sequential stopped streams
// with in-credit tails never fail the session.
func TestSequentialLateTailsDoNotExhaustSessionAggregate(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.TombstoneLimit = 2
	server, peer := newRawPeerConn(t, cfg, DefaultSettings())
	const tail = 6000

	for i := uint64(0); i < 32; i++ {
		id := peer.bidiID(i)
		stream := acceptRawPeerStream(t, server, peer, id)
		if err := stream.CloseRead(); err != nil {
			t.Fatalf("stream %d CloseRead: %v", id, err)
		}
		if err := stream.CloseWrite(); err != nil {
			t.Fatalf("stream %d CloseWrite: %v", id, err)
		}
		peer.await(t, "STOP_SENDING", isFrame(FrameTypeStopSending, id))
		peer.writeData(t, id, 0, make([]byte, tail))
		peer.write(t, Frame{Type: FrameTypeRESET, StreamID: id, Payload: mustEncodeVarint(uint64(CodeCancelled))})
		peer.awaitPong(t, byte(i))
		peer.assertNoClose(t)
		if err := server.err(); err != nil {
			t.Fatalf("stream %d: session failed: %v", id, err)
		}
	}

	awaitConnState(t, server, testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.liveStreamCountLocked() == 0
	}, "stopped streams were not compacted")
	stats := server.Stats()
	if got := stats.Diagnostics.LateDataAfterCloseRead; got != 32*tail {
		t.Fatalf("late data after CloseRead = %d, want %d", got, 32*tail)
	}
	// Only the retained tombstones still account for their tails.
	if got := stats.Pressure.AggregateLateData; got > 2*tail {
		t.Fatalf("aggregate late data = %d, want <= %d with two retained tombstones", got, 2*tail)
	}
}

func TestAggregateLateDataReleasedWhenTombstoneReaped(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_reset",
	})
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("abc")}); err != nil {
		t.Fatalf("late DATA after peer RESET: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ingress.aggregateLateData != 3 {
		t.Fatalf("aggregate before compaction = %d, want 3", c.ingress.aggregateLateData)
	}
	c.maybeCompactTerminalLocked(stream)
	tombstone, ok := c.registry.tombstones[stream.id]
	if !ok || tombstone.LateDataReceived != 3 {
		t.Fatalf("tombstone = %+v (present %v), want the stream's 3 late bytes", tombstone, ok)
	}
	if c.ingress.aggregateLateData != 3 {
		t.Fatalf("aggregate after compaction = %d, want 3 (moved, not dropped)", c.ingress.aggregateLateData)
	}
	if !c.removeTombstoneLocked(stream.id) {
		t.Fatal("removeTombstoneLocked = false")
	}
	if c.ingress.aggregateLateData != 0 {
		t.Fatalf("aggregate after reap = %d, want 0", c.ingress.aggregateLateData)
	}
}

func TestLateDataAllowanceCapturesOutstandingCreditAtLocalStop(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	c.mu.Lock()
	stream.recvReceived = 100
	stream.recvAdvertised = 100 + 40000
	floor := c.effectiveLateDataPerStreamCapLocked(stream)
	c.mu.Unlock()
	if floor.value != lateDataPerStreamCapFor(state.InitialReceiveWindow(c.config.negotiated.LocalRole, c.config.local.Settings, stream.id), c.config.local.Settings.MaxFramePayload) {
		t.Fatalf("allowance before stop = %d, want the repository floor", floor.value)
	}

	if err := stream.CloseRead(); err != nil {
		t.Fatalf("CloseRead: %v", err)
	}
	c.mu.Lock()
	stream.recvReceived += 10000
	stream.setAbortedWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalAbortLocal)
	got := c.effectiveLateDataPerStreamCapLocked(stream)
	c.mu.Unlock()
	// The later ABORT sees less outstanding credit; the larger capture wins.
	if got.value != 40000 {
		t.Fatalf("allowance = %d, want 40000 (credit outstanding at CloseRead)", got.value)
	}
}

// Once the peer FIN was observed on a direction, more DATA on it is a
// stream-state violation answered with ABORT(STREAM_CLOSED), whether or not
// the local side stopped reading and whether or not the stream was compacted
// yet (SPEC §9.2, §9.6; STATE_MACHINE §5.1, §8.1).
func TestDataAfterPeerFinAbortsStreamClosedOnLiveAndCompactedStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, server *Conn, peer *rawFramePeer) uint64
	}{
		{
			name: "read_stopped_live",
			setup: func(t *testing.T, server *Conn, peer *rawFramePeer) uint64 {
				id := peer.bidiID(0)
				stream := acceptRawPeerStream(t, server, peer, id)
				if err := stream.CloseRead(); err != nil {
					t.Fatalf("CloseRead: %v", err)
				}
				peer.await(t, "STOP_SENDING", isFrame(FrameTypeStopSending, id))
				peer.writeData(t, id, FrameFlagFIN, []byte("x"))
				peer.awaitPong(t, 10)
				server.mu.Lock()
				live := server.registry.streams[id] != nil
				server.mu.Unlock()
				if !live {
					t.Fatal("stream compacted although its send half is still open")
				}
				return id
			},
		},
		{
			name: "read_stopped_compacted",
			setup: func(t *testing.T, server *Conn, peer *rawFramePeer) uint64 {
				id := peer.bidiID(0)
				stream := acceptRawPeerStream(t, server, peer, id)
				if err := stream.CloseWrite(); err != nil {
					t.Fatalf("CloseWrite: %v", err)
				}
				if err := stream.CloseRead(); err != nil {
					t.Fatalf("CloseRead: %v", err)
				}
				peer.await(t, "STOP_SENDING", isFrame(FrameTypeStopSending, id))
				peer.writeData(t, id, FrameFlagFIN, []byte("x"))
				awaitConnState(t, server, testSignalTimeout, func(c *Conn) bool {
					c.mu.Lock()
					defer c.mu.Unlock()
					_, ok := c.registry.tombstones[id]
					return ok
				}, "read-stopped stream was not compacted after the peer FIN")
				return id
			},
		},
		{
			name: "fully_terminal_unread",
			setup: func(t *testing.T, server *Conn, peer *rawFramePeer) uint64 {
				id := peer.bidiID(0)
				stream := acceptRawPeerStream(t, server, peer, id)
				if err := stream.CloseWrite(); err != nil {
					t.Fatalf("CloseWrite: %v", err)
				}
				peer.await(t, "DATA|FIN", isFrame(FrameTypeDATA, id))
				peer.writeData(t, id, FrameFlagFIN, []byte("abc"))
				peer.awaitPong(t, 10)
				server.mu.Lock()
				live := server.registry.streams[id] != nil
				server.mu.Unlock()
				if !live {
					t.Fatal("stream with unread data was compacted")
				}
				return id
			},
		},
		{
			name: "uni_not_yet_accepted",
			setup: func(t *testing.T, server *Conn, peer *rawFramePeer) uint64 {
				id := peer.uniID(0)
				peer.writeData(t, id, FrameFlagFIN, []byte("abc"))
				peer.awaitPong(t, 10)
				return id
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server, peer := newRawPeerConn(t, nil, DefaultSettings())
			id := tc.setup(t, server, peer)
			lateTotal := func(c *Conn) uint64 {
				return c.ingress.lateDataAfterClose + c.ingress.lateDataAfterReset + c.ingress.lateDataAfterAbort
			}
			server.mu.Lock()
			sessionReceived := server.flow.recvSessionReceived
			lateBefore := lateTotal(server)
			server.mu.Unlock()

			peer.writeData(t, id, 0, []byte("late"))
			peer.await(t, "ABORT(STREAM_CLOSED)", isAbortWithCode(id, CodeStreamClosed))
			peer.awaitPong(t, 11)
			peer.assertNoClose(t)

			server.mu.Lock()
			defer server.mu.Unlock()
			if server.flow.recvSessionReceived != sessionReceived+4 {
				t.Fatalf("recvSessionReceived = %d, want %d (rejected bytes still count)", server.flow.recvSessionReceived, sessionReceived+4)
			}
			if lateTotal(server) != lateBefore {
				t.Fatalf("late tail diagnostics = %d, want %d: DATA after FIN is not late tail", lateTotal(server), lateBefore)
			}
		})
	}
}

// DATA rejected with a stream-local ABORT still went through the sender's
// session window, so it is checked against, counted in and released from the
// session window here too (SPEC §8).
func TestStreamRejectedDataUsesSessionWindow(t *testing.T) {
	tests := []struct {
		name     string
		seed     func(t *testing.T, c *Conn) *nativeStream
		wantCode ErrorCode
	}{
		{
			name: "data_after_fin",
			seed: func(t *testing.T, c *Conn) *nativeStream {
				return seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
					SendHalf: "send_open",
					RecvHalf: "recv_fin",
				})
			},
			wantCode: CodeStreamClosed,
		},
		{
			name: "wrong_direction",
			seed: func(t *testing.T, c *Conn) *nativeStream {
				stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), "uni", "local_owned", stateHalfExpect{
					SendHalf: "send_open",
				})
				testMarkLocalOpenVisible(stream)
				return stream
			},
			wantCode: CodeStreamState,
		},
		{
			name: "stream_window_overrun",
			seed: func(t *testing.T, c *Conn) *nativeStream {
				stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
					SendHalf: "send_open",
					RecvHalf: "recv_open",
				})
				c.mu.Lock()
				stream.recvAdvertised = stream.recvReceived + 2
				c.mu.Unlock()
				return stream
			},
			wantCode: CodeFlowControl,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, frames, stop := newInvalidFrameConn(t, 0)
			defer stop()
			stream := tc.seed(t, c)
			c.mu.Lock()
			c.flow.recvSessionAdvertised = 100
			c.flow.recvSessionReceived = 10
			c.mu.Unlock()

			if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("xyz")}); err != nil {
				t.Fatalf("rejected DATA err = %v, want stream-local ABORT", err)
			}
			assertInvalidQueuedAbortCode(t, frames, stream.id, tc.wantCode)
			c.mu.Lock()
			if c.flow.recvSessionReceived != 13 || c.flow.recvSessionAdvertised != 103 {
				c.mu.Unlock()
				t.Fatalf("session (received, advertised) = (%d, %d), want (13, 103)", c.flow.recvSessionReceived, c.flow.recvSessionAdvertised)
			}
			if c.ingress.aggregateLateData != 0 || stream.lateDataReceived != 0 {
				c.mu.Unlock()
				t.Fatalf("rejected DATA charged late-data caps: aggregate %d, stream %d", c.ingress.aggregateLateData, stream.lateDataReceived)
			}
			c.mu.Unlock()

			// The same frame against an exhausted session window is a session
			// FLOW_CONTROL error, never a stream ABORT.
			c2, _, stop2 := newInvalidFrameConn(t, 0)
			defer stop2()
			stream2 := tc.seed(t, c2)
			c2.mu.Lock()
			c2.flow.recvSessionAdvertised = 100
			c2.flow.recvSessionReceived = 98
			c2.mu.Unlock()
			err := c2.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: stream2.id, Payload: []byte("xyz")})
			if !IsErrorCode(err, CodeFlowControl) {
				t.Fatalf("rejected DATA over the session window err = %v, want %s", err, CodeFlowControl)
			}
		})
	}
}

func TestLiveReadStoppedDirectionEnforcesStreamCredit(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_stop_sent",
	})
	c.mu.Lock()
	stream.recvReceived = 5
	stream.recvAdvertised = 9
	c.flow.recvSessionAdvertised = 100
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("abcd")}); err != nil {
		t.Fatalf("in-credit late DATA err = %v", err)
	}
	assertNoQueuedFrame(t, frames)
	c.mu.Lock()
	if stream.recvReceived != 9 || stream.lateDataReceived != 4 {
		c.mu.Unlock()
		t.Fatalf("stream (received, late) = (%d, %d), want (9, 4)", stream.recvReceived, stream.lateDataReceived)
	}
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: stream.id, Flags: FrameFlagFIN, Payload: []byte("e")}); err != nil {
		t.Fatalf("over-credit late DATA err = %v, want stream ABORT", err)
	}
	assertInvalidQueuedAbortCode(t, frames, stream.id, CodeFlowControl)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionReceived != 5 || c.flow.recvSessionAdvertised != 105 {
		t.Fatalf("session (received, advertised) = (%d, %d), want (5, 105)", c.flow.recvSessionReceived, c.flow.recvSessionAdvertised)
	}
	if stream.lateDataReceived != 4 {
		t.Fatalf("lateDataReceived = %d, want 4 (overrun is not late tail)", stream.lateDataReceived)
	}
}

func TestLateDataAfterCloseReadKeepsReadClosedError(t *testing.T) {
	server, peer := newRawPeerConn(t, nil, DefaultSettings())
	id := peer.bidiID(0)
	stream := acceptRawPeerStream(t, server, peer, id)
	if err := stream.CloseRead(); err != nil {
		t.Fatalf("CloseRead: %v", err)
	}
	peer.await(t, "STOP_SENDING", isFrame(FrameTypeStopSending, id))
	peer.writeData(t, id, 0, bytes.Repeat([]byte("t"), 100))
	peer.writeData(t, id, FrameFlagFIN, nil)
	peer.awaitPong(t, 1)
	if _, err := stream.Read(make([]byte, 8)); !errors.Is(err, ErrReadClosed) {
		t.Fatalf("Read after CloseRead and peer FIN err = %v, want %v", err, ErrReadClosed)
	}
	peer.assertNoClose(t)
}
