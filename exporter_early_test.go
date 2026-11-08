package zmux

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// earlyExporterConn is a server carrier with accepted early data, whose
// exporter errors until its handshake completes.
type earlyExporterConn struct {
	net.Conn
	mu       sync.Mutex
	done     chan struct{}
	failed   chan struct{}
	complete bool
	startErr error
}

func (c *earlyExporterConn) startErrIs(target error) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return errors.Is(c.startErr, target)
}

func newEarlyExporterConn(c net.Conn) *earlyExporterConn {
	return &earlyExporterConn{Conn: c, done: make(chan struct{}), failed: make(chan struct{})}
}

func (c *earlyExporterConn) UsedEarlyData() bool                { return true }
func (c *earlyExporterConn) HandshakeComplete() <-chan struct{} { return c.done }
func (c *earlyExporterConn) HandshakeFailed() <-chan struct{}   { return c.failed }

func (c *earlyExporterConn) completeHandshake() {
	c.mu.Lock()
	c.complete = true
	c.mu.Unlock()
	close(c.done)
}

func (c *earlyExporterConn) ExportKeyingMaterial(_ string, _ []byte, n int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.complete {
		return nil, errHandshakeIncomplete
	}
	return make([]byte, n), nil
}

// fixedExporterConn is a client carrier whose exporter always answers.
type fixedExporterConn struct{ net.Conn }

func (fixedExporterConn) ExportKeyingMaterial(_ string, _ []byte, n int) ([]byte, error) {
	return make([]byte, n), nil
}

func exporterEarlyPair(t *testing.T, ctx context.Context, cfg *Config) (*Conn, *earlyExporterConn, chan *Conn) {
	t.Helper()
	a, b := net.Pipe()
	cc := fixedExporterConn{a}
	sc := newEarlyExporterConn(b)
	ch := make(chan *Conn, 1)
	go func() {
		s, err := startServerNoListener(ctx, PipeIO{R: sc, W: sc}, cfg)
		if err != nil {
			sc.mu.Lock()
			sc.startErr = err
			sc.mu.Unlock()
			s = nil
		}
		ch <- s
	}()
	c, err := Start(context.Background(), PipeIO{R: cc, W: cc}, RoleClient,
		&Config{MaxPipes: 4, PipeAuth: PipeAuth{Exporter: true, EndToEnd: true}, EarlyData: EarlyData{ZmuxPeer: true}})
	if err != nil {
		t.Fatal(err)
	}
	return c, sc, ch
}

func earlyServerConfig() *Config {
	return &Config{MaxPipes: 4, PipeAuth: PipeAuth{Exporter: true, EndToEnd: true}, EarlyData: EarlyData{Replayable: true}}
}

// Parameters that arrive before the handshake gives the exporter wait for
// it; the server's own output does not (the client gets its parameters).
func TestExporterWaitsForEarlyHandshake(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, sc, ch := exporterEarlyPair(t, ctx, earlyServerConfig())
	for c.RememberedParameters() == nil {
		if ctx.Err() != nil {
			t.Fatal("the client did not get the server's parameters")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-ch:
		t.Fatal("established before the handshake gave the exporter")
	case <-time.After(50 * time.Millisecond):
	}
	sc.completeHandshake()
	s := <-ch
	if s == nil {
		t.Fatal("server start failed")
	}
	echoOnce(t, ctx, c, s)
	_ = c.Close()
	_ = s.Close()
	waitDone(t, c)
	waitDone(t, s)
}

// A failed handshake is a carrier failure, not an internal error of the
// exporter.
func TestExporterEarlyHandshakeFails(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, sc, ch := exporterEarlyPair(t, ctx, earlyServerConfig())
	time.Sleep(20 * time.Millisecond)
	close(sc.failed)
	if s := <-ch; s != nil {
		t.Fatal("established on a failed handshake")
	}
	if !sc.startErrIs(errCarrierHandshakeFailed) {
		t.Fatalf("start: %v", sc.startErr)
	}
	_ = c.Close()
	_ = sc.Close()
	waitDone(t, c)
}

// Without either, the handshake timeout bounds the wait.
func TestExporterEarlyHandshakeTimeout(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := earlyServerConfig()
	cfg.HandshakeTimeout = 200 * time.Millisecond
	c, sc, ch := exporterEarlyPair(t, ctx, cfg)
	select {
	case s := <-ch:
		if s != nil {
			t.Fatal("established without the handshake")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no timeout")
	}
	_ = c.Close()
	_ = sc.Close()
	waitDone(t, c)
}
