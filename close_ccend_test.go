package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	p.once.Do(func() { close(p.closed) })
	return p.Conn.Close()
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

func waitClosed(t *testing.T, who string, p *probeConn, within time.Duration) {
	t.Helper()
	select {
	case <-p.closed:
	case <-time.After(within):
		t.Fatalf("%s: the carrier was not closed within %v", who, within)
	}
}

// The peer's CONNECTION_CLOSE read on a pipe ends that pipe's linger at
// once (SPEC.md Section 6.5 step 3), also on carriers without half-close:
// both carriers close well under the linger timeout and the code arrives.
func TestCloseEndsLingerOnPeerClose(t *testing.T) {
	for _, kind := range []string{"pipe", "tcp-nohalf", "tcp"} {
		t.Run(kind+"/both", func(t *testing.T) {
			leakcheck.Check(t)
			a, b, pa, pb := probes(t, kind)
			client, server := pair(t, a, b, nil, nil)
			t0 := time.Now()
			go func() { _ = server.CloseWithError(8, "s") }()
			_ = client.CloseWithError(7, "c")
			waitClosed(t, "client", pa, 100*time.Millisecond)
			waitClosed(t, "server", pb, 100*time.Millisecond)
			<-client.Done()
			<-server.Done()
			if d := time.Since(t0); d >= 100*time.Millisecond {
				t.Fatalf("both ends took %v", d)
			}
			ce, se := client.Wait(), server.Wait()
			if ce == nil || se == nil {
				t.Fatal("no close error")
			}
		})
		t.Run(kind+"/reply", func(t *testing.T) {
			leakcheck.Check(t)
			a, b, pa, pb := probes(t, kind)
			client, server := pair(t, a, b, nil, nil)
			t0 := time.Now()
			_ = client.CloseWithError(7, "c")
			waitClosed(t, "client", pa, 100*time.Millisecond)
			waitClosed(t, "server", pb, 100*time.Millisecond)
			<-client.Done()
			<-server.Done()
			if d := time.Since(t0); d >= 100*time.Millisecond {
				t.Fatalf("the close took %v", d)
			}
			wantAppClose(t, "the closer", client.Wait(), 7, "c", false)
			wantAppClose(t, "the peer", server.Wait(), 7, "c", true)
		})
	}
}

// A peer that never sends CONNECTION_CLOSE (nor ends its carrier) is waited
// for the linger timeout, no longer.
func TestCloseLingerBoundsSilentPeer(t *testing.T) {
	leakcheck.Check(t)
	a, b, pa, pb := probes(t, "pipe")
	client, server := pair(t, a, b, nil, nil)
	pb.mute.Store(true)
	defer func() { pb.mute.Store(false); _ = server.Close() }()
	t0 := time.Now()
	_ = client.CloseWithError(7, "c")
	waitClosed(t, "client", pa, 5*time.Second)
	if d := time.Since(t0); d < 200*time.Millisecond || d > 3*time.Second {
		t.Fatalf("the linger of a silent peer took %v", d)
	}
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
	x, y := net.Pipe()
	pa := &abortProbe{probeConn: newProbe(x), aborted: make(chan struct{})}
	client, server := pair(t, pa, newProbe(y), nil, nil)
	t0 := time.Now()
	_ = client.CloseWithError(7, "c")
	waitClosed(t, "client", pa.probeConn, 100*time.Millisecond)
	<-client.Done()
	<-server.Done()
	select {
	case <-pa.aborted:
	case <-time.After(3 * time.Second):
		t.Fatal("the gracefully closed carrier was never aborted")
	}
	if d := time.Since(t0); d < 200*time.Millisecond {
		t.Fatalf("aborted %v after the close, before the linger bound", d)
	}
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
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			a, b := net.Pipe()
			ch := make(chan *Conn, 1)
			go func() {
				s, err := Server(ctx, b, nil)
				if err != nil {
					s = nil
				}
				ch <- s
			}()
			client, err := Client(ctx, a, nil)
			if err != nil {
				t.Fatal(err)
			}
			server := <-ch
			if server == nil {
				t.Fatal("no server session")
			}
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
			_ = server.Wait()
			select {
			case <-client.Done():
				t.Fatal("the unread end did not hold the closing session")
			case <-time.After(300 * time.Millisecond):
			}
			n, err := cs.Read(make([]byte, 16))
			var se *StreamError
			switch {
			case reset && (n != 0 || !errors.As(err, &se) || se.ErrorCode != 9):
				t.Fatalf("reset: read = %d, %v", n, err)
			case !reset && (n != 0 || !errors.Is(err, io.EOF)):
				t.Fatalf("fin: read = %d, %v", n, err)
			}
			select {
			case <-client.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("the session did not end once the end was read")
			}
		})
	}
}
