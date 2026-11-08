package zmux

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
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
	if got := srv.Admitted(); got != "v1" {
		t.Fatalf("session value %v", got)
	}
	if p, ok := srv.Pipe(0); !ok || p.Admitted() != "v1" {
		t.Fatalf("bootstrap pipe value %v", p.Admitted())
	}
	waitUntil(t, 5*time.Second, "the joined pipe at the acceptor", func() bool { _, ok := srv.Pipe(cp.ID()); return ok })
	sp, ok := srv.Pipe(cp.ID())
	if !ok || sp.Admitted() != "v2" {
		t.Fatalf("joined pipe value %v", sp.Admitted())
	}
	if cli.Admitted() != nil || cp.Admitted() != nil {
		t.Fatalf("dialing side values %v, %v", cli.Admitted(), cp.Admitted())
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
	if s2.Admitted() != nil {
		t.Fatalf("value %v without an admission", s2.Admitted())
	}
	_ = c2.Close()
	_ = s2.Close()
	waitDone(t, c2)
	waitDone(t, s2)
}

// TestAdmittedValueSplit checks that a split bootstrap's session takes
// the value of its uplink half.
func TestAdmittedValueSplit(t *testing.T) {
	leakcheck.Check(t)
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(),
		Admission: &Admission{PeekTimeout: 5 * time.Second, Decide: func(_ context.Context, info *CarrierInfo, _ []byte) Decision {
			return Decision{Verdict: VerdictAccept, Value: info.Request.Method}
		}}})
	defer func() { _ = lst.Close() }()
	hh, err := NewHTTPHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(hh)
	defer hs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := HTTPSplitPipe(ctx, hs.Client(), hs.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := Start(ctx, p, RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := srv.Admitted(); got != "POST" {
		t.Fatalf("session value %v, want the uplink's", got)
	}
	_ = cli.Close()
	_ = srv.Close()
	waitDone(t, cli)
	waitDone(t, srv)
	hs.Client().CloseIdleConnections()
}

// TestAdmittedValueExchange checks that a session on an HTTP exchange
// carrier takes the accepting decision's value.
func TestAdmittedValueExchange(t *testing.T) {
	leakcheck.Check(t)
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(),
		Admission: &Admission{PeekTimeout: 5 * time.Second, Decide: func(context.Context, *CarrierInfo, []byte) Decision {
			return Decision{Verdict: VerdictAccept, Value: "ex"}
		}}})
	defer func() { _ = lst.Close() }()
	xc := &HTTPExchangeConfig{AllowInsecure: true}
	eh, err := NewHTTPExchangeHandler(lst, xc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eh.Close() }()
	hs := httptest.NewServer(eh)
	defer hs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := HTTPExchangePipe(ctx, hs.Client(), hs.URL, xc)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := Start(ctx, p, RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := srv.Admitted(); got != "ex" {
		t.Fatalf("session value %v", got)
	}
	_ = cli.Close()
	_ = srv.Close()
	waitDone(t, cli)
	waitDone(t, srv)
	hs.Client().CloseIdleConnections()
}
