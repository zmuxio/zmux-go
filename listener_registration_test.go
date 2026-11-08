package zmux

import (
	"context"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// A peer can join as soon as it receives our parameters, even while its
// own parameters have not finished reaching us. The locator must already
// route to the pending session; AcceptJoin owns the negotiation hold.
func TestListenerRegistersBeforeBootstrapParameters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener := mustNewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = listener.Close() }()
	server, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	ended := make(chan error, 1)
	go func() { ended <- listener.startSession(ctx, NetPipe(server), nil, netip.AddrPort{}) }()
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

// A carrier waiting for its first record is abandoned when its context
// ends or the listener closes, not only at the first-record timeout.
func TestListenerFirstRecordAbandoned(t *testing.T) {
	leakcheck.Check(t)
	l := mustNewListener(&ListenerConfig{FirstRecordTimeout: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())
	for _, stop := range []func(){cancel, func() { _ = l.Close() }} {
		a, b := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- l.HandlePipe(ctx, NetPipe(a), PipeHint{}) }()
		stop()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("carrier without a first record accepted")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("carrier still waiting for its first record")
		}
		_ = b.Close()
		ctx = context.Background()
	}
}
