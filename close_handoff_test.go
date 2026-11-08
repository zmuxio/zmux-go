package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
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
		closeExitChild(addr)
		return
	}
	leakcheck.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	lst := mustNewListener(&ListenerConfig{Config: closeExitConfig(), InstanceID: []byte("instance")})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		c, err := ln.Accept()
		if err == nil {
			_ = lst.HandlePipe(ctx, NetPipe(c), PipeHint{})
		}
	})
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCloseThenExit$", "-test.count=1")
	cmd.Env = append(os.Environ(), closeExitEnv+"="+ln.Addr().String())
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	server, err := lst.Accept(ctx)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child: %v", err)
	}
	ended := make(chan error, 1)
	go func() { ended <- server.Wait() }()
	select {
	case err := <-ended:
		var ae *ApplicationError
		if !errors.As(err, &ae) || ae.ErrorCode != 7 {
			t.Fatalf("the peer ended with %v, not the application close", err)
		}
	case <-time.After(5 * time.Second):
		_ = server.Close()
		if err := <-ended; err == nil {
			t.Log("the session ended without an error")
		}
		t.Fatal("the peer still holds the session after the closer exited")
	}
}

// closeExitChild opens a session, closes it (twice: a repeat is safe) and
// exits at once.
func closeExitChild(addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		os.Exit(2)
	}
	c, err := Client(ctx, nc, closeExitConfig())
	if err != nil {
		os.Exit(3)
	}
	if _, err := c.Ping(ctx); err != nil {
		os.Exit(4)
	}
	_ = c.CloseWithError(7, "bye")
	_ = c.Close()
	os.Exit(0)
}

// stallConn blocks its writes while stalled, until it is closed.
type stallConn struct {
	net.Conn
	stall  atomic.Bool
	once   sync.Once
	closed chan struct{}
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

// A carrier that does not take CONNECTION_CLOSE ends Close at the limit:
// the carriers are closed and the session ends.
func TestCloseHandOffLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a, b := net.Pipe()
		sc := &stallConn{Conn: a, closed: make(chan struct{})}
		ch := make(chan *Conn, 1)
		go func() {
			s, err := Server(ctx, b, &Config{Clock: timeshim.Real})
			if err != nil {
				s = nil
			}
			ch <- s
		}()
		client, err := Client(ctx, sc, &Config{Clock: timeshim.Real})
		if err != nil {
			t.Fatal(err)
		}
		server := <-ch
		if server == nil {
			t.Fatal("no server session")
		}
		defer func() { _ = server.Close(); _ = server.Wait() }()
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

// On a synchronous carrier (net.Pipe), which has no half-close, Close
// drains until the linger timeout, within the close limit.
func TestCloseHandOffSyncCarrier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a, b := net.Pipe()
		ch := make(chan *Conn, 1)
		go func() {
			s, err := Server(ctx, b, &Config{Clock: timeshim.Real})
			if err != nil {
				s = nil
			}
			ch <- s
		}()
		client, err := Client(ctx, a, &Config{Clock: timeshim.Real})
		if err != nil {
			t.Fatal(err)
		}
		server := <-ch
		if server == nil {
			t.Fatal("no server session")
		}
		if _, err := client.Ping(ctx); err != nil {
			t.Fatal(err)
		}
		t0 := time.Now()
		_ = client.Close()
		if d := time.Since(t0); d == 0 || d > closeLimit {
			t.Fatalf("Close took %v", d)
		}
		_ = client.Wait()
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

// A server that closes with a code while the peer keeps writing, and
// drops its socket as soon as CloseWithError returns (a process exit),
// ends the TCP connection with a FIN, not a reset: the peer reads the
// close code.
func TestCloseUnderPeerWritesNoReset(t *testing.T) {
	leakcheck.Check(t)
	for i := range 5 {
		closeUnderPeerWrites(t, i)
	}
}

func closeUnderPeerWrites(t *testing.T, round int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	type res struct {
		c  *Conn
		nc net.Conn
	}
	ch := make(chan res, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			ch <- res{}
			return
		}
		s, err := Server(ctx, nc, nil)
		if err != nil {
			_ = nc.Close()
			s = nil
		}
		ch <- res{s, nc}
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	spy := &resetSpy{Conn: raw}
	client, err := Client(ctx, spy, nil)
	if err != nil {
		t.Fatal(err)
	}
	sr := <-ch
	if sr.c == nil {
		t.Fatal("no server session")
	}
	server := sr.c
	var wg sync.WaitGroup
	wg.Go(func() {
		st, err := server.AcceptStream(ctx)
		if err == nil {
			_, _ = io.Copy(io.Discard, st)
		}
	})
	st, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	wg.Go(func() {
		for {
			if _, err := st.Write(buf); err != nil {
				return
			}
		}
	})
	time.Sleep(100 * time.Millisecond) // the peer writes at full speed
	_ = server.CloseWithError(9, "bye")
	_ = sr.nc.Close() // the process exits: unread data would reset
	err = client.Wait()
	var ae *ApplicationError
	if !errors.As(err, &ae) || ae.ErrorCode != 9 {
		t.Fatalf("round %d: the peer ended with %v, not the close code (reset read: %v)", round, err, spy.reset.Load())
	}
	if spy.reset.Load() {
		t.Fatalf("round %d: the peer's carrier was reset", round)
	}
	_ = server.Wait()
	wg.Wait()
}

// stuckCloseWrite is a carrier whose CloseWrite blocks until it is closed,
// like a TLS close_notify behind a full send buffer.
type stuckCloseWrite struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (s *stuckCloseWrite) CloseWrite() error {
	<-s.closed
	return net.ErrClosed
}

func (s *stuckCloseWrite) Close() error {
	s.once.Do(func() { close(s.closed) })
	return s.Conn.Close()
}

// stallReads blocks its reads while stalled, until it is closed.
type stallReads struct {
	net.Conn
	stall  atomic.Bool
	once   sync.Once
	closed chan struct{}
}

func (s *stallReads) Read(b []byte) (int, error) {
	if s.stall.Load() {
		<-s.closed
		return 0, net.ErrClosed
	}
	return s.Conn.Read(b)
}

func (s *stallReads) Close() error {
	s.once.Do(func() { close(s.closed) })
	return s.Conn.Close()
}

// A half-close that blocks (the peer does not read, as a TLS close_notify
// behind a full send buffer) does not hold Close past its limit: the
// carrier is aborted, which releases the writer.
func TestCloseBlockedHalfCloseLimit(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ch := make(chan *Conn, 1)
	var peer *stallReads
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			ch <- nil
			return
		}
		peer = &stallReads{Conn: newDelayConn(nc, 250*time.Millisecond, 1e9, 1<<20), closed: make(chan struct{})}
		s, err := Server(ctx, peer, nil)
		if err != nil {
			_ = nc.Close()
			s = nil
		}
		ch <- s
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// 500 ms round trips: the linger timeout (3 x SRTT) outlasts Close's
	// limit, so only the limit can end the blocked half-close.
	sc := &stuckCloseWrite{Conn: newDelayConn(raw, 250*time.Millisecond, 1e9, 1<<20), closed: make(chan struct{})}
	client, err := Client(ctx, sc, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := <-ch
	if server == nil {
		t.Fatal("no server session")
	}
	defer func() { _ = server.Close(); _ = server.Wait() }()
	if _, err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	peer.stall.Store(true)
	t0 := time.Now()
	_ = client.Close()
	if d := time.Since(t0); d > closeLimit+300*time.Millisecond {
		t.Fatalf("Close took %v with a limit of %v", d, closeLimit)
	}
	waited := make(chan struct{})
	go func() { _ = client.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(3 * time.Second):
		t.Fatal("the session did not end after the limit")
	}
}
