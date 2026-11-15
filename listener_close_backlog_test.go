package zmux

import (
	"context"
	"errors"
	"net"
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
