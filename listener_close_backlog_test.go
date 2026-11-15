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
type closeOnWrite struct {
	net.Conn
	l    *Listener
	once sync.Once
}

func (w *closeOnWrite) Write(b []byte) (int, error) {
	w.once.Do(func() { _ = w.l.Close() })
	return w.Conn.Write(b)
}

// TestListenerCloseInHandshakeSendsClose: Close lands while the server
// still waits for a handshake whose flight the client already has. When
// the cancelled wait wins (the scheduler's choice; the rounds repeat),
// the listener aborts the session, and HandlePipe must last until that
// CONNECTION_CLOSE went out: it returned at once, the caller (as here)
// closed the carrier first, and the resumable client, seeing a bare
// carrier loss, waited ResumeTimeout to resume a session that was gone.
func TestListenerCloseInHandshakeSendsClose(t *testing.T) {
	aborted := 0
	for range 50 {
		if listenerCloseInHandshake(t) {
			aborted++
		}
	}
	t.Logf("%d of 50 rounds aborted the handshake", aborted)
}

func listenerCloseInHandshake(t *testing.T) (aborted bool) {
	t.Helper()
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
	if !errors.As(err, &te) && !errors.As(err, &ae) {
		t.Fatalf("the client did not get the server's close: %v", err)
	}
	<-returned
	return l.Stats().Rejected > 0
}
