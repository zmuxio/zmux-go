package zmux

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestAdmittedValue checks that an accepting decision's value reaches
// WrapPipe, the session it starts and each pipe that joins it, replacing
// a value given to HandlePipe, and that zmux accepts a join whatever its
// value; the dialing side and a listener without an admission see none.
func TestAdmittedValue(t *testing.T) {
	leakcheck.Check(t)
	var n atomic.Int32
	var mu sync.Mutex
	var wrapped []any
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), InstanceID: []byte("inst"),
		Admission: &Admission{PeekTimeout: 5 * time.Second, Decide: func(context.Context, *CarrierInfo, []byte) Decision {
			return Decision{Verdict: VerdictAccept, Value: fmt.Sprintf("v%d", n.Add(1))}
		}},
		WrapPipe: WrapPipeFunc(func(_ context.Context, p PipeIO, h PipeHint) (PipeIO, error) {
			mu.Lock()
			wrapped = append(wrapped, h.Admitted)
			mu.Unlock()
			return p, nil
		})})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	carrier := func() net.Conn {
		a, b := net.Pipe()
		go func() { _ = lst.HandlePipe(ctx, NetPipe(b), PipeHint{Admitted: "caller"}) }()
		return a
	}
	cli, err := Client(ctx, carrier(), mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close(); _ = srv.Close(); waitDone(t, cli); waitDone(t, srv) }()
	cp, err := cli.AddPipe(ctx, NetPipe(carrier()), PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := srv.admittedValue(); got != "v1" {
		t.Fatalf("session value %v", got)
	}
	if p, ok := srv.Pipe(0); !ok || p.admittedValue() != "v1" {
		t.Fatalf("bootstrap pipe value %v", p.admittedValue())
	}
	waitUntil(t, 5*time.Second, "the joined pipe at the acceptor", func() bool { _, ok := srv.Pipe(cp.ID()); return ok })
	sp, ok := srv.Pipe(cp.ID())
	if !ok || sp.admittedValue() != "v2" {
		t.Fatalf("joined pipe value %v", sp.admittedValue())
	}
	if cli.admittedValue() != nil || cp.admittedValue() != nil {
		t.Fatalf("dialing side values %v, %v", cli.admittedValue(), cp.admittedValue())
	}
	mu.Lock()
	got := fmt.Sprint(wrapped)
	mu.Unlock()
	if got != "[v1 v2]" {
		t.Fatalf("WrapPipe saw %s", got)
	}

	// Without an admission: no value, also where HandlePipe was given one.
	plain := mustNewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = plain.Close() }()
	a, b := net.Pipe()
	go func() { _ = plain.HandlePipe(ctx, NetPipe(b), PipeHint{Admitted: "caller"}) }()
	c2, err := Client(ctx, a, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s2, err := plain.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s2.admittedValue() != nil {
		t.Fatalf("value %v without an admission", s2.admittedValue())
	}
	_ = c2.Close()
	_ = s2.Close()
	waitDone(t, c2)
	waitDone(t, s2)
}
