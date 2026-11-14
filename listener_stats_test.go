package zmux

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestListenerStats checks each outcome count of a listener.
func TestListenerStats(t *testing.T) {
	leakcheck.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lst := mustNewListener(&ListenerConfig{FirstRecordTimeout: 100 * time.Millisecond, Config: mpConfig()})
	done := make(chan struct{})
	go func() { _ = lst.Serve(ctx, ln); close(done) }()
	defer func() { cancel(); _ = lst.Close(); <-done }()
	dial := func() net.Conn {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		return nc
	}
	waitStats := func(what string, ok func(ListenerStats) bool) {
		t.Helper()
		waitUntil(t, 5*time.Second, what, func() bool { return ok(lst.Stats()) })
	}

	// A session.
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()
	c, err := Client(dctx, dial(), mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	sc, err := lst.Accept(dctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = sc.Close(); waitDone(t, c); waitDone(t, sc) }()
	waitStats("a session", func(s ListenerStats) bool { return s.Sessions == 1 })

	// A join.
	if _, err := c.AddPipe(dctx, NetPipe(dial()), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	waitStats("a join", func(s ListenerStats) bool { return s.Joins == 1 })

	// Not zmux at all.
	nc := dial()
	_, _ = nc.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	waitStats("a rejection", func(s ListenerStats) bool { return s.Rejected == 1 })
	_ = nc.Close()

	// No first record.
	nc = dial()
	waitStats("a timeout", func(s ListenerStats) bool { return s.TimedOut == 1 })
	_ = nc.Close()

	// A TLS handshake that fails.
	srvTLS, _ := selfSignedTLS(t)
	a1, a2 := net.Pipe()
	served := make(chan struct{})
	go func() { _ = lst.HandlePipe(ctx, NetPipe(tls.Server(a2, srvTLS)), PipeHint{}); close(served) }()
	_, _ = a1.Write([]byte("not a client hello"))
	_ = a1.Close()
	<-served
	waitStats("a setup failure", func(s ListenerStats) bool { return s.SetupFailures == 1 })

	// After Close every carrier is refused.
	_ = lst.Close()
	r1, r2 := net.Pipe()
	defer func() { _ = r2.Close() }()
	if err := lst.HandlePipe(ctx, NetPipe(r1), PipeHint{}); err == nil {
		t.Fatal("a closed listener took a carrier")
	}
	got := lst.Stats()
	want := ListenerStats{Sessions: 1, Joins: 1, SetupFailures: 1, Rejected: 1, Refused: 1, TimedOut: 1}
	if got != want {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}

// earlyHalf is a server carrier half that took early data and whose
// handshake never completes; asked closes when the join waits for it.
type earlyHalf struct {
	net.Conn
	asked chan struct{}
	once  sync.Once
}

func (*earlyHalf) UsedEarlyData() bool { return true }
func (h *earlyHalf) HandshakeComplete() <-chan struct{} {
	h.once.Do(func() { close(h.asked) })
	return nil
}

// TestEarlyJoinWaitCancelled checks that a join sent in early data whose
// wait for the handshake is cancelled counts as a setup failure, and
// nowhere else.
func TestEarlyJoinWaitCancelled(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	before := e.lst.Stats()
	cli, srv := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() { _, _ = e.client.AddPipe(ctx, NetPipe(cli), PipeOptions{}) }()
	hctx, hcancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	half := &earlyHalf{Conn: srv, asked: make(chan struct{})}
	go func() { done <- e.lst.HandlePipe(hctx, PipeIO{R: srv, W: half}, PipeHint{}) }()
	// The join waits for the handshake: nothing is counted yet.
	select {
	case <-half.asked:
	case <-ctx.Done():
		t.Fatal("the join never waited for the handshake")
	}
	if got := e.lst.Stats(); got != before {
		t.Fatalf("counted while waiting: %+v", got)
	}
	hcancel()
	if err := <-done; err == nil {
		t.Fatal("a join whose handshake wait was cancelled was taken")
	}
	want := before
	want.SetupFailures++
	if got := e.lst.Stats(); got != want {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}
