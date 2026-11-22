package zmux

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// TestListenerCloseEndsBacklog: a session waiting for Accept when the
// listener closes is ended, not left without an owner, and Accept reports
// the close.
// HS-35.
func TestListenerCloseEndsBacklog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	l := mustNewListener(&ListenerConfig{Config: atkConfig()})
	addr := atkServe(t, func(c net.Conn) { _ = l.HandlePipe(ctx, NetPipe(c), PipeHint{}) })
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	c, err := Start(ctx, NetPipe(nc), RoleClient, atkConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = l.Close()
	select {
	case <-c.Context().Done():
	case <-ctx.Done():
		t.Fatal("the session the listener never handed out still runs")
	}
	if _, err := l.Accept(ctx); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept after Close: %v", err)
	}
}

// closeOnWrite closes a listener at the first write of its carrier: the
// server's flight, which lets the client count the session established.
// It offers CloseWrite as the TCP connection under it does: without it the
// session can only close the socket, and a socket closed with the client's
// data unread is reset (on Windows at once), which drops the
// CONNECTION_CLOSE the client has not read yet.
type closeOnWrite struct {
	net.Conn
	l    *Listener
	once sync.Once
}

func (w *closeOnWrite) CloseWrite() error { return w.Conn.(*net.TCPConn).CloseWrite() }

func (w *closeOnWrite) Write(b []byte) (int, error) {
	w.once.Do(func() { _ = w.l.Close() })
	return w.Conn.Write(b)
}

// HS-35. The first server write crosses the listener cutoff while the
// bootstrap is provisional. Both paths through establishment send code zero.
func TestListenerCloseInHandshakeSendsClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l := mustNewListener(&ListenerConfig{Config: atkConfig()})
	returned := make(chan struct{})
	addr := atkServe(t, func(c net.Conn) {
		_ = l.HandlePipe(ctx, NetPipe(&closeOnWrite{Conn: c, l: l}), PipeHint{})
		close(returned)
	})
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	// The server's close may reach the client before Start returns.
	c, err := Start(ctx, NetPipe(nc), RoleClient, atkConfig())
	if err == nil {
		select {
		case <-c.Context().Done():
			err = context.Cause(c.Context())
		case <-ctx.Done():
			t.Fatal("the session the listener never handed out still runs")
		}
		_ = c.Close()
	}
	var te *TransportError
	var ae *ApplicationError
	zeroCode := errors.As(err, &te) && te.ErrorCode == 0 || errors.As(err, &ae) && ae.ErrorCode == 0
	if !zeroCode {
		t.Fatalf("the client did not get the server's close: %v", err)
	}
	<-returned
}
