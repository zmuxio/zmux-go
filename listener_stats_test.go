package zmux

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestListenerStats checks each outcome count of a listener.
func TestListenerStats(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	clock := timeshim.NewFake(time.Now())
	cfg := mpConfig()
	cfg.ClockDomain = NewClockDomain(clock)
	const firstTimeout = time.Second
	lst := mustNewListener(&ListenerConfig{FirstRecordTimeout: firstTimeout, Config: cfg})
	defer func() { _ = lst.Close() }()

	handle := func(nc net.Conn) <-chan error {
		done := make(chan error, 1)
		go func() { done <- lst.HandlePipe(ctx, NetPipe(nc), PipeHint{}) }()
		return done
	}
	check := func(done <-chan error, wantOK bool) {
		t.Helper()
		select {
		case err := <-done:
			if (err == nil) != wantOK {
				t.Fatalf("HandlePipe returned %v, want success %v", err, wantOK)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	// A session.
	a, b := tcpPair(t)
	done := handle(b)
	c, err := Client(ctx, a, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	sc, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		c.Abort(0, "")
		sc.Abort(0, "")
		_ = a.Close()
		_ = b.Close()
		waitDone(t, c)
		waitDone(t, sc)
	}()
	check(done, true)

	// A join.
	jc, js := tcpPair(t)
	done = handle(js)
	if _, err := c.AddPipe(ctx, NetPipe(jc), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	check(done, true)

	// Not zmux at all.
	bad, rejected := tcpPair(t)
	defer func() { _ = bad.Close() }()
	done = handle(rejected)
	if _, err := bad.Write([]byte("GET / HTTP/1.1\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	check(done, false)

	// No first record. Read entry follows registration of the deadline.
	idle, pending := tcpPair(t)
	defer func() { _ = idle.Close() }()
	watched := &firstReadConn{Conn: pending, reading: make(chan struct{})}
	done = handle(watched)
	waitTestSignal(t, watched.reading, "first-record read")
	clock.Advance(firstTimeout)
	check(done, false)

	// A TLS handshake that fails.
	srvTLS, _ := selfSignedTLS(t)
	a1, a2 := net.Pipe()
	served := make(chan struct{})
	go func() { _ = lst.HandlePipe(ctx, NetPipe(tls.Server(a2, srvTLS)), PipeHint{}); close(served) }()
	_, _ = a1.Write([]byte("not a client hello"))
	_ = a1.Close()
	<-served

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

type firstReadConn struct {
	net.Conn
	reading chan struct{}
	once    sync.Once
}

func (c *firstReadConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.reading) })
	return c.Conn.Read(p)
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
