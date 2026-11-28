package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// gatedCarrier fails its writes once armed and holds its reads until the
// first of them failed, so a write failure always precedes the records
// that are still on their way in.
type gatedCarrier struct {
	c      net.Conn
	failed chan struct{}
	armed  atomic.Bool
	once   atomic.Bool
}

func (g *gatedCarrier) Read(b []byte) (int, error) {
	if g.armed.Load() {
		<-g.failed
	}
	return g.c.Read(b)
}

func (g *gatedCarrier) Write(b []byte) (int, error) {
	if g.armed.Load() {
		if g.once.CompareAndSwap(false, true) {
			close(g.failed)
		}
		return 0, io.ErrClosedPipe
	}
	return g.c.Write(b)
}

func (g *gatedCarrier) Close() error { return g.c.Close() }

// A write failure on the only pipe does not end the session ahead of the
// peer's CONNECTION_CLOSE that its inbound side still delivers.
func TestWriteFailureLetsInboundCloseDecide(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	x, y := net.Pipe()
	g := &gatedCarrier{c: x, failed: make(chan struct{})}
	srv := make(chan *Conn, 1)
	go func() {
		s, err := Server(ctx, y, nil)
		if err != nil {
			t.Error(err)
		}
		srv <- s
	}()
	c, err := Start(ctx, PipeIO{R: g, W: g}, RoleClient, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := <-srv
	if s == nil {
		return
	}
	g.armed.Store(true)
	closed := make(chan error, 1)
	go func() { closed <- s.CloseWithError(9, "bye") }()
	st, err := c.OpenStream()
	if err == nil {
		_, _ = st.Write([]byte("x")) // the write that fails first
	}
	var ae *ApplicationError
	if err := c.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 9 {
		t.Fatalf("client end %v, want the peer's code 9", err)
	}
	if err := <-closed; err != nil {
		t.Logf("server close: %v", err)
	}
}
