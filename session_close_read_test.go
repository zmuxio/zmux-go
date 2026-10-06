package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// Session termination fails every receive half that did not get a peer FIN,
// whatever the CLOSE code (SPEC §6.10). EOF is reserved for a peer FIN whose
// data was all delivered (SPEC §9.2): bytes discarded at session close, even
// behind a FIN, end in the session error too.

func requireSessionClosedRead(t *testing.T, what string, stream *nativeStream) {
	t.Helper()
	n, err := stream.Read(make([]byte, 16))
	requireSessionClosedReadErr(t, what, n, err)
}

func requireSessionClosedReadErr(t *testing.T, what string, n int, err error) {
	t.Helper()
	if n != 0 || err == nil || errors.Is(err, io.EOF) || !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("%s Read = (%d, %v), want (0, ErrSessionClosed)", what, n, err)
	}
	var structured *Error
	if !errors.As(err, &structured) || structured.TerminationKind != TerminationSessionTermination {
		t.Fatalf("%s Read err = %#v, want session-termination metadata", what, err)
	}
}

// startBlockedRead parks a Read on stream and reports its result.
func startBlockedRead(stream *nativeStream) <-chan error {
	result := make(chan error, 1)
	go func() {
		n, err := stream.Read(make([]byte, 16))
		if n != 0 && err == nil {
			err = errors.New("blocked Read returned data")
		}
		result <- err
	}()
	return result
}

func awaitBlockedReadErr(t *testing.T, what string, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		requireSessionClosedReadErr(t, what, 0, err)
	case <-time.After(testSignalTimeout):
		t.Fatalf("%s Read was not released by the session close", what)
	}
}

func waitSessionDone(t *testing.T, c *Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*testSignalTimeout)
	defer cancel()
	if err := c.Wait(ctx); errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("session still %v after close", c.State())
	}
}

// peerClosedStreams are the receive halves a session holds when it ends.
type peerClosedStreams struct {
	partial   *nativeStream // body cut off: no FIN, nothing unread
	buffered  *nativeStream // no FIN, bytes still unread
	unreadFin *nativeStream // complete body (peer FIN) left unread
	drained   *nativeStream // complete body read through its FIN
	blocked   <-chan error  // a Read parked on an unfinished stream
}

func openPeerClosedStreams(t *testing.T, local *Conn, peer *rawFramePeer) peerClosedStreams {
	t.Helper()
	var out peerClosedStreams
	out.partial = acceptRawPeerStream(t, local, peer, peer.bidiID(0))
	// An unfinished response keeps a graceful Close draining.
	if _, err := out.partial.Write([]byte("r")); err != nil {
		t.Fatalf("partial response Write: %v", err)
	}

	out.buffered = acceptRawPeerStream(t, local, peer, peer.bidiID(1))
	peer.writeData(t, peer.bidiID(1), 0, []byte("unread"))

	out.unreadFin = acceptRawPeerStream(t, local, peer, peer.bidiID(2))
	peer.writeData(t, peer.bidiID(2), FrameFlagFIN, []byte("complete"))

	out.drained = acceptRawPeerStream(t, local, peer, peer.bidiID(3))
	peer.writeData(t, peer.bidiID(3), FrameFlagFIN, nil)

	blocked := acceptRawPeerStream(t, local, peer, peer.bidiID(4))
	out.blocked = startBlockedRead(blocked)

	// Every frame above has been handled once the PONG comes back.
	peer.awaitPong(t, 1)
	if n, err := out.drained.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("drained stream Read before close = (%d, %v), want EOF", n, err)
	}
	return out
}

func (s peerClosedStreams) requireFailed(t *testing.T) {
	t.Helper()
	requireSessionClosedRead(t, "partial", s.partial)
	requireSessionClosedRead(t, "buffered", s.buffered)
	requireSessionClosedRead(t, "unread FIN", s.unreadFin)
	awaitBlockedReadErr(t, "blocked", s.blocked)

	// The result does not depend on timing: later Reads, including on the
	// stream whose Read was blocked, fail the same way.
	requireSessionClosedRead(t, "partial (again)", s.partial)
	if data, err := io.ReadAll(s.buffered); err == nil || !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("io.ReadAll(buffered) = (%q, %v), want ErrSessionClosed", data, err)
	}
	// A FIN the application already reached stays a graceful EOF.
	if n, err := s.drained.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("drained stream Read after close = (%d, %v), want EOF", n, err)
	}
	if _, err := s.partial.Write([]byte("x")); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("partial Write after close err = %v, want ErrSessionClosed", err)
	}
}

func TestPeerCloseNoErrorFailsReceiveHalvesWithoutPeerFIN(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.KeepaliveInterval = 0
	local, peer := newRawPeerConn(t, cfg, DefaultSettings())
	streams := openPeerClosedStreams(t, local, peer)

	payload, err := buildCodePayload(uint64(CodeNoError), "", DefaultSettings().MaxControlPayloadBytes)
	if err != nil {
		t.Fatalf("build CLOSE payload: %v", err)
	}
	peer.write(t, Frame{Type: FrameTypeCLOSE, Payload: payload})
	waitSessionDone(t, local)

	streams.requireFailed(t)
}

func TestLocalGracefulSessionCloseFailsReceiveHalvesWithoutPeerFIN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		close func(*testing.T, *Conn)
	}{
		{name: "close_with_error_no_error", close: func(_ *testing.T, c *Conn) { c.CloseWithError(&ApplicationError{Code: uint64(CodeNoError)}) }},
		{name: "close_with_error_nil", close: func(_ *testing.T, c *Conn) { c.CloseWithError(nil) }},
		// Close's graceful drain times out on the unfinished response and ends
		// the session with CLOSE(NO_ERROR).
		{name: "close", close: func(t *testing.T, c *Conn) {
			if err := c.Close(); !errors.Is(err, ErrGracefulCloseTimeout) {
				t.Fatalf("Close err = %v, want ErrGracefulCloseTimeout", err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := DefaultConfig()
			cfg.KeepaliveInterval = 0
			cfg.GracefulCloseDrainTimeout = 50 * time.Millisecond
			local, peer := newRawPeerConn(t, cfg, DefaultSettings())
			streams := openPeerClosedStreams(t, local, peer)

			tc.close(t, local)
			waitSessionDone(t, local)

			streams.requireFailed(t)
		})
	}
}

// Go's own graceful Close ends a session whose streams are still unfinished
// once its drain times out. The peer application must see the response body
// as cut off, never as complete.
func TestPeerGracefulCloseDrainTimeoutFailsTruncatedBody(t *testing.T) {
	t.Parallel()

	serverCfg := DefaultConfig()
	serverCfg.GracefulCloseDrainTimeout = 50 * time.Millisecond
	client, server := newConnPairWithConfig(t, nil, serverCfg)

	ctx, cancel := context.WithTimeout(context.Background(), testSignalTimeout)
	defer cancel()
	cs, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := cs.Write([]byte("request")); err != nil {
		t.Fatalf("request Write: %v", err)
	}
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if _, err := ss.Write([]byte("partial")); err != nil {
		t.Fatalf("response Write: %v", err)
	}

	if err := server.Close(); !errors.Is(err, ErrGracefulCloseTimeout) {
		t.Fatalf("server Close err = %v, want ErrGracefulCloseTimeout", err)
	}
	waitSessionDone(t, client)

	// The response was never finished: reading it to the end must fail,
	// whether or not its first bytes were still buffered.
	data, err := io.ReadAll(cs)
	if err == nil || !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("io.ReadAll = (%q, %v), want ErrSessionClosed", data, err)
	}
	if !bytes.HasPrefix([]byte("partial"), data) {
		t.Fatalf("io.ReadAll data = %q, want a prefix of %q", data, "partial")
	}
}

// closeRacingConn plays a peer that closes the transport as soon as it has
// read the CLOSE: once the session is closing, each write completes and then
// the peer end is closed, and the write is reported done only after the read
// loop has seen that transport close.
type closeRacingConn struct {
	net.Conn
	peer    net.Conn
	session atomic.Pointer[Conn]
}

func (c *closeRacingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if session := c.session.Load(); session != nil && session.State() == SessionStateClosing {
		_ = c.peer.Close()
		select {
		case <-session.lifecycle.closedCh:
		case <-time.After(testSignalTimeout):
		}
	}
	return n, err
}

// A peer closing the transport right after reading the CLOSE ends the session
// in order; graceful Close must report its own outcome, not that EOF.
func TestGracefulCloseIgnoresPeerTransportCloseAfterCLOSE(t *testing.T) {
	t.Parallel()

	left, right := net.Pipe()
	racing := &closeRacingConn{Conn: right, peer: left}
	serverCfg := DefaultConfig()
	serverCfg.GracefulCloseDrainTimeout = 50 * time.Millisecond
	clientCh := make(chan *Conn, 1)
	go func() {
		c, err := Client(left, nil)
		if err != nil {
			t.Errorf("client establish: %v", err)
		}
		clientCh <- c
	}()
	server, err := Server(racing, serverCfg)
	if err != nil {
		t.Fatalf("server establish: %v", err)
	}
	client := <-clientCh
	if client == nil {
		t.FailNow()
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	racing.session.Store(server)

	// An unfinished response keeps Close draining until its timeout.
	ctx, cancel := context.WithTimeout(context.Background(), testSignalTimeout)
	defer cancel()
	ss, err := server.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := ss.Write([]byte("partial")); err != nil {
		t.Fatalf("response Write: %v", err)
	}

	if err := server.Close(); !errors.Is(err, ErrGracefulCloseTimeout) {
		t.Fatalf("server Close err = %v, want ErrGracefulCloseTimeout", err)
	}
	if err := server.Wait(ctx); err != nil {
		t.Fatalf("server Wait err = %v, want nil", err)
	}
}
