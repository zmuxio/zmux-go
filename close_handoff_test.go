package zmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

const closeExitEnv = "ZMUX_TEST_CLOSE_EXIT_ADDR"

func closeExitConfig() *Config {
	cfg := mpConfig()
	cfg.ResumeTimeout = 30 * time.Second // a lost CONNECTION_CLOSE would hold the peer
	return cfg
}

// A process that exits as soon as Close returns has sent CONNECTION_CLOSE:
// the peer, which would otherwise hold the session for its resume
// timeout, ends with the application close.
func TestCloseThenExit(t *testing.T) {
	if addr := os.Getenv(closeExitEnv); addr != "" {
		if err := closeExitChild(addr); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
		return
	}
	leakcheck.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := runCloseExit(t.Context(), ln, ln.Addr().String()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseThenExitChildDialFailure(t *testing.T) {
	leakcheck.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// A syntactically invalid address makes the child fail before any TCP
	// connection reaches the listener.
	err = runCloseExit(t.Context(), ln, "missing-port")
	if err == nil || !strings.Contains(err.Error(), "child dial:") {
		t.Fatalf("expected the child's dial error, got %v", err)
	}
}

func runCloseExit(parent context.Context, ln net.Listener, addr string) error {
	defer func() { _ = ln.Close() }()
	lst := mustNewListener(&ListenerConfig{Config: closeExitConfig(), InstanceID: []byte("instance")})
	defer func() { _ = lst.Close() }()
	deadline, stop := context.WithTimeout(parent, 20*time.Second)
	defer stop()
	ctx, cancel := context.WithCancelCause(deadline)
	closeListener := context.AfterFunc(ctx, func() { _ = ln.Close() })
	defer closeListener()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCloseThenExit$", "-test.count=1")
	cmd.Env = append(os.Environ(), closeExitEnv+"="+addr)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		cancel(err)
		return fmt.Errorf("start child: %w", err)
	}
	childDone := make(chan struct{})
	go func() {
		defer close(childDone)
		if err := cmd.Wait(); err != nil {
			cancel(fmt.Errorf("child exit: %w: %s", err, bytes.TrimSpace(stderr.Bytes())))
		}
	}()
	defer func() {
		cancel(nil)
		<-childDone
	}()

	nc, err := ln.Accept()
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
		return fmt.Errorf("accept child TCP connection: %w", err)
	}
	if err := lst.HandlePipe(ctx, NetPipe(nc), PipeHint{}); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
		return fmt.Errorf("attach child carrier: %w", err)
	}
	server, err := lst.Accept(ctx)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
		return fmt.Errorf("accept child session: %w", err)
	}
	defer func() {
		server.Abort(0, "")
		<-server.Done()
	}()
	<-childDone
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-server.Done():
		err := server.Wait()
		if err == nil {
			return errors.New("the peer ended without application close 7")
		}
		var ae *ApplicationError
		if !errors.As(err, &ae) || ae.ErrorCode != 7 {
			return fmt.Errorf("want application close 7: %w", err)
		}
		return nil
	case <-timer.C:
		return errors.New("the peer still holds the session after the closer exited")
	}
}

// closeExitChild opens a session, closes it (twice: a repeat is safe) and
// exits at once.
func closeExitChild(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nc, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("child dial: %w", err)
	}
	c, err := Client(ctx, nc, closeExitConfig())
	if err != nil {
		return fmt.Errorf("child session: %w", err)
	}
	if _, err := c.Ping(ctx); err != nil {
		return fmt.Errorf("child ping: %w", err)
	}
	_ = c.CloseWithError(7, "bye")
	_ = c.Close()
	return nil
}

// stallConn blocks its writes while stalled, until it is closed.
type stallConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
	stall  atomic.Bool
}

func (s *stallConn) Write(b []byte) (int, error) {
	if s.stall.Load() {
		<-s.closed
		return 0, net.ErrClosed
	}
	return s.Conn.Write(b)
}

func (s *stallConn) Close() error {
	s.once.Do(func() { close(s.closed) })
	return s.Conn.Close()
}

func closeTestConfig() *Config {
	return &Config{ClockDomain: NewClockDomain(timeshim.Real)}
}

// A carrier that does not take CONNECTION_CLOSE ends Close at the limit:
// the carriers are closed and the session ends.
func TestCloseHandOffLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a, b := net.Pipe()
		sc := &stallConn{Conn: a, closed: make(chan struct{})}
		client, _ := bubblePairOn(t, sc, b, closeTestConfig)
		if _, err := client.Ping(ctx); err != nil {
			t.Fatal(err)
		}
		sc.stall.Store(true)
		t0 := time.Now()
		_ = client.Close()
		d := time.Since(t0)
		// The fake clock makes the bound exact: lingering starts only once
		// the carrier took CONNECTION_CLOSE, so nothing ends the close
		// before the limit.
		if d != closeLimit {
			t.Fatalf("Close took %v with a limit of %v", d, closeLimit)
		}
		waited := make(chan struct{})
		go func() { _ = client.Wait(); close(waited) }()
		select {
		case <-waited:
		case <-time.After(2 * time.Second):
			t.Fatal("the session did not end after the limit")
		}
		t1 := time.Now()
		_ = client.Close()
		if d := time.Since(t1); d > 50*time.Millisecond {
			t.Fatalf("a repeated Close took %v", d)
		}
	})
}

// A Close after an Abort returns at once, as the Abort did, though the
// carrier has not taken the Abort's CONNECTION_CLOSE.
func TestCloseAfterAbortReturnsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a, b := net.Pipe()
		sc := &stallConn{Conn: a, closed: make(chan struct{})}
		client, _ := bubblePairOn(t, sc, b, closeTestConfig)
		if _, err := client.Ping(ctx); err != nil {
			t.Fatal(err)
		}
		sc.stall.Store(true)
		t0 := time.Now()
		client.Abort(7, "abort")
		_ = client.Close()
		if d := time.Since(t0); d != 0 {
			t.Fatalf("Abort then Close took %v", d)
		}
		_ = client.Wait()
	})
}

// A session that ends by its idle timeout while the carrier is stuck in an
// earlier write still ends: the close limit bounds the hand-off of
// CONNECTION_CLOSE whoever closed.
func TestIdleCloseStalledCarrierEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		sc := &stallConn{Conn: a, closed: make(chan struct{})}
		cfg := func() *Config {
			return &Config{ClockDomain: NewClockDomain(timeshim.Real), MaxIdleTimeout: 3 * time.Second,
				Liveness: Liveness{KeepAlivePeriod: -1}}
		}
		client, _ := bubblePairOn(t, sc, b, cfg)
		sc.stall.Store(true)
		t0 := time.Now()
		_ = client.Wait()
		if d := time.Since(t0); d > 3*time.Second+closeLimit+time.Second {
			t.Fatalf("the idle session took %v to end", d)
		}
	})
}

// On a synchronous carrier (net.Pipe), which has no half-close, Close
// returns at the hand-off, within the close limit, and the session drains
// in the background until the linger timeout.
func TestCloseHandOffSyncCarrier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		client, server := bubblePair(t, closeTestConfig)
		if _, err := client.Ping(ctx); err != nil {
			t.Fatal(err)
		}
		t0 := time.Now()
		_ = client.Close()
		if d := time.Since(t0); d > closeLimit {
			t.Fatalf("Close took %v", d)
		}
		_ = client.Wait()
		// The peer answered with its CONNECTION_CLOSE: no linger wait
		// without a half-close underneath (SPEC.md Section 6.5 step 3).
		if d := time.Since(t0); d >= 250*time.Millisecond {
			t.Fatalf("the session lingered %v after the peer's CONNECTION_CLOSE", d)
		}
		_ = server.Close()
		_ = server.Wait()
	})
}

// resetSpy records whether a read of the carrier failed with a reset.
type resetSpy struct {
	net.Conn
	reset atomic.Bool
}

func (r *resetSpy) Read(b []byte) (int, error) {
	n, err := r.Conn.Read(b)
	if errors.Is(err, syscall.ECONNRESET) {
		r.reset.Store(true)
	}
	return n, err
}

// A server that aborts with a code while the peer keeps writing, and
// drops its socket once its session has ended (the background half-close
// and linger), ends the TCP connection with a FIN, not a reset: the peer
// reads the close code.
func TestCloseUnderPeerWritesNoReset(t *testing.T) {
	leakcheck.Check(t)
	for i := range 5 {
		closeUnderPeerWrites(t, i)
	}
}

func closeUnderPeerWrites(t *testing.T, round int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	a, b := tcpPair(t)
	spy := &resetSpy{Conn: a}
	client, server := pair(t, spy, b, nil, nil)
	var wg sync.WaitGroup
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
		wg.Wait()
	})
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wg.Go(func() {
		buf := make([]byte, 64<<10)
		for {
			if _, err := st.Write(buf); err != nil {
				return
			}
		}
	})
	in, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := in.ReadChunk(ctx, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	wg.Go(func() { _, _ = io.Copy(io.Discard, in) })
	server.Abort(9, "bye")
	waitTestSignal(t, server.Done(), "server half-close and linger")
	_ = b.Close() // the server drops its socket
	waitTestSignal(t, client.Done(), "client session end")
	err = client.Wait()
	var ae *ApplicationError
	if !errors.As(err, &ae) || ae.ErrorCode != 9 {
		t.Fatalf("round %d: the peer ended with %v, want the close code (reset read: %v)", round, err, spy.reset.Load())
	}
	if spy.reset.Load() {
		t.Fatalf("round %d: the peer's carrier was reset", round)
	}
}

// stuckCloseWrite is a carrier whose CloseWrite blocks until it is closed,
// like a TLS close_notify behind a full send buffer.
type stuckCloseWrite struct {
	net.Conn
	blocked chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (s *stuckCloseWrite) CloseWrite() error {
	close(s.blocked)
	<-s.closed
	return net.ErrClosed
}

func (s *stuckCloseWrite) Close() error {
	s.once.Do(func() { close(s.closed) })
	return s.Conn.Close()
}

// A blocked half-close holds Close until the hand-off limit. At that
// limit the carrier is aborted, releasing the blocked writer.
func TestCloseBlockedHalfCloseLimit(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		sc := &stuckCloseWrite{Conn: a, blocked: make(chan struct{}), closed: make(chan struct{})}
		peer := newProbe(b)
		client, _ := closeWithFrozenClock(t, sc, peer)
		peer.mute.Store(true)
		defer peer.mute.Store(false)
		closed := make(chan struct{})
		start := time.Now()
		go func() { defer close(closed); _ = client.Close() }()
		waitTestSignal(t, sc.blocked, "carrier half-close")
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("Close returned with the carrier's half-close blocked")
		default:
		}
		waitTestSignal(t, closed, "Close return")
		if elapsed := time.Since(start); elapsed != closeLimit {
			t.Fatalf("Close returned after %v, want %v", elapsed, closeLimit)
		}
		synctest.Wait()
		select {
		case <-client.Done():
		default:
			t.Fatal("the hand-off limit did not release the blocked writer")
		}
	})
}
