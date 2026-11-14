package zmux

import (
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
