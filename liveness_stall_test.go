package zmux

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// stalledTransportBound is how long a liveness decision may take once the
// transport stops draining: the configured timeout plus the bounded CLOSE
// attempt (closeFrameSendTimeout) and scheduling slack.
const stalledTransportBound = 3 * time.Second

// startStalledStreamWrite blocks the session writer in a transport write: the
// raw peer reads nothing, so over net.Pipe the first DATA never completes.
func startStalledStreamWrite(t *testing.T, c *Conn) <-chan error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testSignalTimeout)
	defer cancel()
	stream, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := stream.Write(make([]byte, 64<<10))
		done <- err
	}()
	return done
}

func requireIdleTimeoutClose(t *testing.T, c *Conn, start time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), stalledTransportBound)
	defer cancel()
	err := c.Wait(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("session still %v %v after the transport stalled; keepalive timeout never fired", c.State(), time.Since(start))
	}
	var appErr *ApplicationError
	if !errors.As(err, &appErr) || appErr.Code != uint64(CodeIdleTimeout) {
		t.Fatalf("Wait err = %v, want ApplicationError(IDLE_TIMEOUT)", err)
	}
}

// The keepalive timeout is the session's defence against a peer whose
// transport stopped draining (IMPLEMENTATION §4), so it must not depend on a
// transport write completing.
func TestKeepaliveTimeoutFiresWhileTransportWriteIsStalled(t *testing.T) {
	t.Parallel()

	t.Run("ping_handed_to_stalled_writer", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultConfig()
		cfg.KeepaliveInterval = 50 * time.Millisecond
		cfg.KeepaliveTimeout = 300 * time.Millisecond
		client, _ := newStalledRawPeerConn(t, cfg, DefaultSettings())

		start := time.Now()
		writeDone := startStalledStreamWrite(t, client)
		requireIdleTimeoutClose(t, client, start)

		select {
		case err := <-writeDone:
			if err == nil {
				t.Fatal("stalled stream Write succeeded, want session error")
			}
		case <-time.After(testSignalTimeout):
			t.Fatal("stalled stream Write was not released by the keepalive close")
		}
	})

	t.Run("ping_cannot_reach_stalled_writer", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultConfig()
		cfg.KeepaliveInterval = 300 * time.Millisecond
		cfg.KeepaliveTimeout = 300 * time.Millisecond
		client, peer := newStalledRawPeerConn(t, cfg, DefaultSettings())

		// The session answers each peer PING with a PONG. With the writer
		// stuck on the first one, the others fill the urgent lane, so the
		// keepalive PING can no longer even be handed to the writer.
		start := time.Now()
		for i := byte(0); i < 4; i++ {
			peer.write(t, Frame{Type: FrameTypePING, Payload: []byte{0, 0, 0, 0, 0, 0, 0x6b, i}})
		}
		requireIdleTimeoutClose(t, client, start)
	})
}

// Close must not block forever on a transport that stopped draining either
// (API_SEMANTICS §8.1): its GOAWAY waits for the writer only within the
// graceful drain budget, then the bounded CLOSE attempt closes the transport.
// Keepalive is off, so nothing else could end the session.
func TestGracefulCloseIsBoundedWhileTransportWriteIsStalled(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.KeepaliveInterval = 0
	cfg.GracefulCloseDrainTimeout = 200 * time.Millisecond
	client, _ := newStalledRawPeerConn(t, cfg, DefaultSettings())
	writeDone := startStalledStreamWrite(t, client)

	start := time.Now()
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close() }()
	select {
	case err := <-closeDone:
		if !errors.Is(err, ErrGracefulCloseTimeout) {
			t.Fatalf("Close err = %v, want ErrGracefulCloseTimeout", err)
		}
	case <-time.After(stalledTransportBound):
		t.Fatalf("Close still blocked %v after the transport stalled (state %v)", time.Since(start), client.State())
	}
	if !client.Closed() {
		t.Fatalf("session state after Close = %v, want closed", client.State())
	}
	select {
	case err := <-writeDone:
		if err == nil {
			t.Fatal("stalled stream Write succeeded, want session error")
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled stream Write was not released by Close")
	}
}

// Ping(ctx) bounds the whole call, including handing its PING to a writer that
// is stuck in a transport write (API_SEMANTICS §5).
func TestPingHonorsContextWhileTransportWriteIsStalled(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.KeepaliveInterval = 0
	client, peer := newStalledRawPeerConn(t, cfg, DefaultSettings())
	writeDone := startStalledStreamWrite(t, client)

	// The first PING reaches the writer's lane; the second cannot even be
	// handed over while the first still waits there.
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		result := make(chan error, 1)
		go func() {
			_, err := client.Ping(ctx, []byte("stalled"))
			result <- err
		}()
		select {
		case err := <-result:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Ping #%d over a stalled writer err = %v, want context.DeadlineExceeded", i+1, err)
			}
		case <-time.After(200*time.Millisecond + testSignalTimeout):
			t.Fatalf("Ping #%d over a stalled writer ignored its 200ms deadline", i+1)
		}
		cancel()
	}
	if state := client.State(); state != SessionStateReady {
		t.Fatalf("session state after Ping deadlines = %v, want ready", state)
	}
	if client.Stats().PingOutstanding {
		t.Fatal("PingOutstanding after the Ping deadlines, want false")
	}

	// Once the transport drains again, the late PONG of a cancelled PING is
	// ignored and a new Ping completes.
	peer.startReading()
	pingResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*testSignalTimeout)
		defer cancel()
		_, err := client.Ping(ctx, []byte("drained"))
		pingResult <- err
	}()
	answerRawPeerPings(t, peer, pingResult)
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("stream Write after the transport drained: %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("stream Write did not complete after the transport drained")
	}
	if state := client.State(); state != SessionStateReady {
		t.Fatalf("session state after late PONGs = %v, want ready", state)
	}
	if client.Stats().PingOutstanding {
		t.Fatal("PingOutstanding after the successful Ping, want false")
	}
}

// answerRawPeerPings echoes every PING the raw peer has read until result
// delivers the outcome of the Ping under test, which must be a success.
func answerRawPeerPings(t *testing.T, peer *rawFramePeer, result <-chan error) {
	t.Helper()
	answered := 0
	deadline := time.After(2 * testSignalTimeout)
	for {
		frames := peer.received()
		pings := 0
		for _, frame := range frames {
			if frame.Type != FrameTypePING {
				continue
			}
			pings++
			if pings > answered {
				peer.write(t, Frame{Type: FrameTypePONG, Payload: bytes.Clone(frame.Payload)})
				answered++
			}
		}
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("Ping after the transport drained: %v (PINGs answered: %d)", err, answered)
			}
			return
		case <-peer.notify:
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatalf("Ping after the transport drained did not complete (PINGs answered: %d)", answered)
		}
	}
}

// Session termination releases an outstanding PING before closedCh closes.
// Ping must report the termination, never a successful round trip.
func TestPingReportsSessionTerminationWhileWaiting(t *testing.T) {
	t.Parallel()

	t.Run("close_with_error", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultConfig()
		cfg.KeepaliveInterval = 0
		client, _ := newStalledRawPeerConn(t, cfg, DefaultSettings())

		result := make(chan error, 1)
		go func() {
			_, err := client.Ping(context.Background(), nil)
			result <- err
		}()
		awaitConnState(t, client, testSignalTimeout, func(c *Conn) bool {
			return c.Stats().PingOutstanding
		}, "user PING never became outstanding")

		client.CloseWithError(&ApplicationError{Code: 77, Reason: "shutdown"})
		select {
		case err := <-result:
			var appErr *ApplicationError
			if !errors.As(err, &appErr) || appErr.Code != 77 {
				t.Fatalf("Ping err = %v, want the session close error (code 77)", err)
			}
		case <-time.After(stalledTransportBound):
			t.Fatal("Ping did not return after the session closed")
		}
	})

	t.Run("keepalive_timeout", func(t *testing.T) {
		t.Parallel()
		cfg := DefaultConfig()
		cfg.KeepaliveInterval = 50 * time.Millisecond
		cfg.KeepaliveTimeout = 300 * time.Millisecond
		client, _ := newStalledRawPeerConn(t, cfg, DefaultSettings())

		// An unbounded user Ping is the outstanding PING the keepalive timeout
		// measures; the stalled transport must not keep either waiting.
		start := time.Now()
		result := make(chan error, 1)
		go func() {
			_, err := client.Ping(context.Background(), nil)
			result <- err
		}()
		requireIdleTimeoutClose(t, client, start)
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("Ping err = nil after keepalive timeout, want session error")
			}
		case <-time.After(testSignalTimeout):
			t.Fatal("Ping did not return after the keepalive timeout closed the session")
		}
	})
}
