package zmux

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// A peer can join as soon as it receives our parameters, even while its
// own parameters have not finished reaching us. The locator must already
// route to the pending session; AcceptJoin owns the negotiation hold.
func TestListenerRegistersBeforeBootstrapParameters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener := NewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = listener.Close() }()
	server, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	ended := make(chan error, 1)
	go func() { ended <- listener.startSession(ctx, NetPipe(server)) }()
	if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	if _, err := io.ReadFull(peer, first[:]); err != nil {
		t.Fatal(err)
	}
	listener.mu.Lock()
	locators, live := len(listener.sessions), len(listener.live)
	listener.mu.Unlock()
	if locators != 1 || live != 1 {
		t.Errorf("bootstrap parameters exposed before registration: locators=%d live=%d", locators, live)
	}
	_ = peer.Close()
	select {
	case err := <-ended:
		if err == nil {
			t.Error("missing peer parameters must fail establishment")
		}
	case <-ctx.Done():
		t.Fatal("pending establishment did not terminate")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	listener.mu.Lock()
	defer listener.mu.Unlock()
	if len(listener.sessions) != 0 || len(listener.live) != 0 {
		t.Error("failed establishment retained its registration")
	}
}
