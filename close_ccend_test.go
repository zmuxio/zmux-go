package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// probeConn reports when its carrier is closed; mute swallows what is
// written and ignores Close, a peer that never answers.
type probeConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
	mute   atomic.Bool
}

func newProbe(c net.Conn) *probeConn {
	return &probeConn{Conn: c, closed: make(chan struct{})}
}

func (p *probeConn) Write(b []byte) (int, error) {
	if p.mute.Load() {
		return len(b), nil
	}
	return p.Conn.Write(b)
}

func (p *probeConn) Close() error {
	if p.mute.Load() {
		return nil
	}
	err := p.Conn.Close()
	p.once.Do(func() { close(p.closed) })
	return err
}

// halfProbe is a probeConn over TCP that offers the half-close.
type halfProbe struct{ *probeConn }

func (h halfProbe) CloseWrite() error { return h.Conn.(*net.TCPConn).CloseWrite() }

func probes(t *testing.T, kind string) (a, b net.Conn, pa, pb *probeConn) {
	t.Helper()
	switch kind {
	case "pipe":
		x, y := net.Pipe()
		pa, pb = newProbe(x), newProbe(y)
		return pa, pb, pa, pb
	case "tcp-nohalf":
		x, y := tcpPair(t)
		pa, pb = newProbe(x), newProbe(y)
		return pa, pb, pa, pb
	default:
		x, y := tcpPair(t)
		pa, pb = newProbe(x), newProbe(y)
		return halfProbe{pa}, halfProbe{pb}, pa, pb
	}
}

// closeWithFrozenClock keeps the session's linger timeout from expiring
// while the test waits for the peer to end its side of a real carrier.
func closeWithFrozenClock(t *testing.T, a, b net.Conn) (*Conn, *Conn) {
	t.Helper()
	clock := timeshim.NewFake(time.Now())
	client, server := pair(t, a, b, &Config{ClockDomain: NewClockDomain(clock)}, &Config{ClockDomain: NewClockDomain(clock)})
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
		clock.Advance(time.Minute)
	})
	return client, server
}

const initialCloseLinger = 3 * 333 * time.Millisecond // no round-trip samples

// The peer's CONNECTION_CLOSE read on a pipe ends that pipe's linger at
// once (SPEC.md Section 6.5 step 3), also on carriers without half-close:
// both carriers close with the linger clock still frozen.
func TestCloseEndsLingerOnPeerClose(t *testing.T) {
	for _, kind := range []string{"pipe", "tcp-nohalf", "tcp"} {
		t.Run(kind+"/both", func(t *testing.T) {
			leakcheck.Check(t)
			a, b, pa, pb := probes(t, kind)
			client, server := closeWithFrozenClock(t, a, b)
			closed := make(chan struct{})
			go func() { defer close(closed); _ = server.CloseWithError(8, "s") }()
			_ = client.CloseWithError(7, "c")
			waitTestSignal(t, closed, "server CloseWithError")
			waitTestSignal(t, pa.closed, "client carrier close")
			waitTestSignal(t, pb.closed, "server carrier close")
			waitTestSignal(t, client.Done(), "client session end")
			waitTestSignal(t, server.Done(), "server session end")
			ce, se := client.Wait(), server.Wait()
			if ce == nil || se == nil {
				t.Fatal("no close error")
			}
		})
		t.Run(kind+"/reply", func(t *testing.T) {
			leakcheck.Check(t)
			a, b, pa, pb := probes(t, kind)
			client, server := closeWithFrozenClock(t, a, b)
			_ = client.CloseWithError(7, "c")
			waitTestSignal(t, pa.closed, "client carrier close")
			waitTestSignal(t, pb.closed, "server carrier close")
			waitTestSignal(t, client.Done(), "client session end")
			waitTestSignal(t, server.Done(), "server session end")
			wantAppClose(t, "the closer", client.Wait(), 7, "c", false)
			wantAppClose(t, "the peer", server.Wait(), 7, "c", true)
		})
	}
}

// A peer that never sends CONNECTION_CLOSE (nor ends its carrier) is waited
// for the linger timeout, no longer.
func TestCloseLingerBoundsSilentPeer(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		a, b, pa, pb := probes(t, "pipe")
		cfg := &Config{ClockDomain: NewClockDomain(timeshim.Real)}
		client, _ := pair(t, a, b, cfg, cfg)
		pb.mute.Store(true)
		defer pb.mute.Store(false)
		start := time.Now()
		_ = client.CloseWithError(7, "c")
		synctest.Wait()
		select {
		case <-pa.closed:
			t.Fatal("the carrier closed before the silent peer's linger expired")
		default:
		}
		waitTestSignal(t, pa.closed, "client carrier close")
		if elapsed := time.Since(start); elapsed != initialCloseLinger {
			t.Fatalf("the silent peer's linger = %v, want %v", elapsed, initialCloseLinger)
		}
	})
}

// abortProbe is a probeConn that records an Abort.
type abortProbe struct {
	*probeConn
	aborted chan struct{}
	once    sync.Once
}

func (p *abortProbe) Abort(error) { p.once.Do(func() { close(p.aborted) }) }

// A carrier closed gracefully on the peer's CONNECTION_CLOSE is aborted a
// linger timeout later (SPEC.md Section 6.5 step 4), so that a carrier
// whose graceful close leaves work running ends in bounded time.
func TestCloseGracefulAbortBound(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		x, y := net.Pipe()
		pa := &abortProbe{probeConn: newProbe(x), aborted: make(chan struct{})}
		cfg := &Config{ClockDomain: NewClockDomain(timeshim.Real)}
		client, _ := pair(t, pa, newProbe(y), cfg, cfg)
		start := time.Now()
		_ = client.CloseWithError(7, "c")
		waitTestSignal(t, pa.closed, "graceful carrier close")
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("the graceful close waited %v", elapsed)
		}
		waitTestSignal(t, pa.aborted, "carrier abort")
		if elapsed := time.Since(start); elapsed != initialCloseLinger {
			t.Fatalf("the carrier aborted after %v, want %v", elapsed, initialCloseLinger)
		}
	})
}

func TestCloseReadDrainReleasesTCPAfterEOF(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	a, b, pa, _ := probes(t, "tcp")
	client, server := closeWithFrozenClock(t, a, b)
	send, err := server.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const payload = "still readable"
	if _, err := send.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	recv, err := client.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// The peer sends CONNECTION_CLOSE and a TCP FIN while the stream is unread.
	_ = server.CloseWithError(0x2a, "bye")
	select {
	case <-pa.closed:
	case <-ctx.Done():
		t.Fatalf("waiting for TCP close before reading the stream: %v", ctx.Err())
	}
	select {
	case <-client.Done():
		t.Fatal("the session ended before the stream was read")
	default:
	}
	buf := make([]byte, len(payload))
	if n, err := recv.TryRead(buf); n != len(payload) || string(buf[:n]) != payload || err != nil {
		t.Fatalf("after TCP close, read %q, %v", buf[:n], err)
	}
	waitTestSignal(t, client.Done(), "client session end after the read drain")
	wantAppClose(t, "the peer", client.Wait(), 0x2a, "bye", true)
}

// The read drain keeps a stream's end, not only its bytes (SPEC.md Section
// 6.5): a FIN or a reset received before the peer's close holds the
// closing session until the application reads it, though the carrier has
// ended, and the read reports the end, not the session's error.
func TestCloseReadDrainKeepsReceivedEnd(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "fin"
		if reset {
			name = "reset"
		}
		t.Run(name, func(t *testing.T) {
			leakcheck.Check(t)
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				a, b, pa, _ := probes(t, "pipe")
				cfg := &Config{ClockDomain: NewClockDomain(timeshim.Real)}
				client, server := pair(t, a, b, cfg, cfg)
				cs, err := client.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := cs.Write([]byte{7}); err != nil {
					t.Fatal(err)
				}
				ss, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				// The end alone: the client's stream never receives a byte.
				if reset {
					ss.CancelWrite(9)
				} else if err := ss.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := client.Ping(ctx); err != nil { // the end reached the client
					t.Fatal(err)
				}
				_ = server.CloseWithError(0x2a, "bye")
				waitTestSignal(t, pa.closed, "client carrier close")
				synctest.Wait()
				select {
				case <-client.Done():
					t.Fatal("the unread end did not hold the closing session")
				default:
				}
				n, err := cs.Read(make([]byte, 16))
				var se *StreamError
				switch {
				case reset && (n != 0 || !errors.As(err, &se) || se.ErrorCode != 9):
					t.Fatalf("reset: read = %d, %v", n, err)
				case !reset && (n != 0 || !errors.Is(err, io.EOF)):
					t.Fatalf("fin: read = %d, %v", n, err)
				}
				synctest.Wait()
				select {
				case <-client.Done():
				default:
					t.Fatal("the session did not end once the end was read")
				}
			})
		})
	}
}
