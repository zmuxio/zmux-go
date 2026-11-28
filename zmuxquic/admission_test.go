package zmuxquic

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/timeshim"
	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/carrier"
	"github.com/zmuxio/zmux-go/internal/prefixtest"
)

func serveAdmissionFixture(t *testing.T, cfg *zmux.Config, adm *zmux.Admission) (*zmux.Listener, string, *tls.Config, context.Context) {
	t.Helper()
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddr("127.0.0.1:0", srv, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	lst := mustNewListener(&zmux.ListenerConfig{Config: cfg, Admission: adm})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	listener := &trackedListener{ConnListener: ln}
	done := make(chan struct{})
	go func() { _ = Serve(ctx, lst, listener); close(done) }()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		_ = lst.Close()
		<-done
		listener.closeAll()
	})
	return lst, ln.Addr().String(), cli, ctx
}

// TestServeAdmission admits connections whose first stream starts with a
// valid prefix token, from Dial and a ConnPool with a CarrierCredential.
func TestServeAdmission(t *testing.T) {
	auth, v, _ := prefixtest.New(t, admit.PrefixCarrierOptions{BindTLS: true}, zmux.VerdictClose)
	adm := v.Admission()
	adm.PeekTimeout = -1
	lst, addr, cli, ctx := serveAdmissionFixture(t, mpConfig(), adm)

	// A valid carrier: Dial presents the credential on its first stream.
	cfg := mpConfig()
	cfg.CarrierCredential = auth
	c, err := Dial(ctx, addr, cli, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s, 1<<16)
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()

	// A pool presents it on the first stream of each connection: a
	// bootstrap, a join on its connection, a join on a second connection.
	pool := NewConnPool(addr, cli, nil, auth, carrier.PoolLimits{MaxPipesPerConnection: 2})
	defer pool.Shutdown()
	first, err := pool.OpenPipe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c, err = zmux.Start(ctx, first, zmux.RoleClient, mpConfig()); err != nil {
		t.Fatal(err)
	}
	if s, err = lst.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		pio, err := pool.OpenPipe(ctx)
		if err != nil {
			t.Fatal(err)
		}
		joined, err := c.AddPipe(ctx, pio, zmux.PipeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := joined.WaitAttached(ctx); err != nil {
			t.Fatal(err)
		}
	}
	echo(t, c, s, 1<<16)
	if n := len(c.PipeStats()); n != 3 {
		t.Fatalf("%d pipes", n)
	}
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

func TestServeAdmissionInvalidTokenStalls(t *testing.T) {
	_, v, _ := prefixtest.New(t, admit.PrefixCarrierOptions{BindTLS: true}, zmux.VerdictStall)
	clk := timeshim.NewFake(time.Now())
	conf := mpConfig()
	conf.ClockDomain = zmux.NewClockDomain(clk)
	const stall = 20 * time.Millisecond
	adm := v.Admission()
	adm.PeekTimeout, adm.StallMin, adm.StallMax = -1, stall, stall
	lst, addr, cli, ctx := serveAdmissionFixture(t, conf, adm)

	qc, err := quic.DialAddr(ctx, addr, cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	p, err := ConnPipe(ctx, qc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.W.Write(make([]byte, admit.PrefixTokenLen)); err != nil {
		t.Fatal(err)
	}
	if err := clk.WaitDeadline(ctx, clk.Now().Add(stall)); err != nil {
		t.Fatalf("rejected carrier did not stall: %v", err)
	}
	clk.Advance(stall)
	deadline, _ := ctx.Deadline()
	r := p.R.(*qReader)
	if err := r.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if n, err := r.Read(make([]byte, 1)); n != 0 || err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("rejected carrier read %d bytes, %v", n, err)
	}
	select {
	case <-qc.Context().Done():
		cause := context.Cause(qc.Context())
		if err, ok := errors.AsType[*quic.ApplicationError](cause); !ok || !err.Remote {
			t.Fatalf("rejected carrier closed for %v", cause)
		}
	case <-ctx.Done():
		t.Fatal("the connection was not closed")
	}
	if got := lst.Stats().Admission.Stalled; got != 1 {
		t.Fatalf("stalled carriers = %d, want 1", got)
	}
}

// TestServeAdmissionRejectsEarlyData pins that a connection whose first
// stream arrived as 0-RTT data is never admitted, even on a listener whose
// quic.Config allows 0-RTT: the server accepts the early data in TLS, and
// Serve closes the connection without calling Decide.
func TestServeAdmissionRejectsEarlyData(t *testing.T) {
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddr("127.0.0.1:0", srv, QUICConfig(&quic.Config{Allow0RTT: true}))
	if err != nil {
		t.Fatal(err)
	}
	var decided atomic.Int32
	adm := &zmux.Admission{Peek: 4, PeekTimeout: -1,
		Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
			decided.Add(1)
			return zmux.Decision{Verdict: zmux.VerdictAccept}
		}}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = Serve(ctx, lst, ln); close(done) }()
	defer func() { cancel(); _ = ln.Close(); _ = lst.Close(); <-done }()

	warm, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	waitTicket(t, ctx, cli)
	_ = warm.CloseWithError(0, "")

	early, err := quic.DialAddrEarly(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = early.CloseWithError(0, "") }()
	s, err := early.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("zmux")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-early.Context().Done():
		err, ok := errors.AsType[*quic.ApplicationError](context.Cause(early.Context()))
		if !ok || !err.Remote {
			t.Fatalf("first stream rejected with %v", context.Cause(early.Context()))
		}
	case <-ctx.Done():
		t.Fatal("the connection was not closed")
	}
	if !early.ConnectionState().Used0RTT {
		t.Fatal("the server did not accept 0-RTT")
	}
	if n := decided.Load(); n != 0 {
		t.Fatalf("Decide called %d times", n)
	}
}

// A QUIC first stream that begins with a host header is read with
// VerdictMore and accepted past it; the session runs on the rest.
func TestServeAdmissionMore(t *testing.T) {
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddr("127.0.0.1:0", srv, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	header := append([]byte{0x01, 20}, make([]byte, 18)...)
	adm := &zmux.Admission{Peek: 2, MaxPeek: 64, PeekTimeout: -1,
		Decide: func(_ context.Context, _ zmux.CarrierInfo, prefix []byte) zmux.Decision {
			if c, _ := admit.ClassifyFirstBytes(prefix, zmux.FramingByteStream); c != admit.FirstBytesOther || prefix[0] != 0x01 {
				return zmux.Decision{Verdict: zmux.VerdictClose}
			}
			if len(prefix) < int(prefix[1]) {
				return zmux.Decision{Verdict: zmux.VerdictMore, Need: int(prefix[1])}
			}
			return zmux.Decision{Verdict: zmux.VerdictAccept, Skip: int(prefix[1]), Variant: "q"}
		}}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = Serve(ctx, lst, ln); close(done) }()
	defer func() { cancel(); _ = ln.Close(); _ = lst.Close(); <-done }()
	qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	p, err := ConnPipe(ctx, qc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.W.Write(header); err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s, 1<<12)
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
	if vs := lst.VariantStats(); !slices.Contains(vs, zmux.VariantStat{Name: "q", Accepted: 1}) {
		t.Fatalf("variants %+v", vs)
	}
}
