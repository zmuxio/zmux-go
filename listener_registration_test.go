package zmux

import (
	"context"
	"errors"
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
	setup, stop := context.WithCancel(ctx)
	defer stop()
	go func() { ended <- listener.startSession(setup, NetPipe(server), nil, netip.AddrPort{}, nil) }()
	if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	if _, err := io.ReadFull(peer, first[:]); err != nil {
		t.Fatal(err)
	}
	cs := listener.registry.snapshot(false)
	live := len(cs)
	var pending *Conn
	locators := 0
	if live == 1 {
		pending = cs[0]
		if listener.registry.lookup(string(pending.s.Config().Local.Locator)) == pending {
			locators++
		}
	}
	if locators != 1 || live != 1 {
		t.Fatalf("bootstrap parameters exposed before registration: locators=%d live=%d", locators, live)
	}
	// The first write is still blocked on the rest of its bytes. Cancelling
	// setup must return the error and leave that write to the session.
	stop()
	select {
	case err := <-ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pending handshake: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("pending establishment did not terminate")
	}
	if _, err := io.ReadFull(peer, first[:]); err != nil {
		t.Fatalf("setup closed a carrier still owned by the session: %v", err)
	}
	_ = peer.Close()
	waitDone(t, pending)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if listener.registry.lookup(string(pending.s.Config().Local.Locator)) != nil || len(listener.registry.snapshot(false)) != 0 {
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

func TestRegistryShutdownSnapshotDuringRebind(t *testing.T) {
	l := mustNewListener(nil)
	defer func() { _ = l.Close() }()
	old, next := &Conn{id: 1}, &Conn{id: 2}
	const key = "epoch"
	if !l.register(old, []byte(key)) {
		t.Fatal("initial registration")
	}
	l.registry.publish(old)
	locs := l.registry.detach(old)
	if !l.register(next, []byte(key)) {
		t.Fatal("new registration")
	}
	l.registry.publish(next)
	for _, loc := range locs {
		l.registry.unbind(old, loc)
	}
	got := l.registry.snapshot(true)
	if len(got) != 1 || got[0] != next || l.registry.lookup(key) != next {
		t.Fatal("stale owner removed new binding")
	}
	l.sessionClosed(next)
	if l.registry.lookup(key) != nil || len(l.registry.snapshot(false)) != 0 {
		t.Fatal("registration retained")
	}
}
