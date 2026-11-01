package zmuxquic

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

// TestServeAdmission admits connections whose first stream starts with a
// valid prefix token, from Dial and a Pool with a Credential, and closes
// one whose first stream does not.
func TestServeAdmission(t *testing.T) {
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddr("127.0.0.1:0", srv, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	auth := &zmux.PrefixAuth{Key: []byte("k"), Slot: 30 * time.Second, Skew: 1, BindTLS: true}
	v, err := zmux.NewPrefixAuthVerifier(auth)
	if err != nil {
		t.Fatal(err)
	}
	adm := &zmux.Admission{Peek: zmux.PrefixTokenLen, PeekTimeout: time.Second, Decide: v.Decide,
		StallMin: 10 * time.Millisecond, StallMax: 20 * time.Millisecond}
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = Serve(ctx, ln, lst, nil); close(done) }()
	defer func() { cancel(); _ = ln.Close(); _ = lst.Close(); <-done }()

	// A valid carrier: Dial presents the credential on its first stream.
	cfg := mpConfig()
	cfg.Credential = auth
	c, err := Dial(ctx, ln.Addr().String(), cli, nil, cfg)
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
	pool := NewPool(ln.Addr().String(), cli, nil, auth, zmux.PoolLimits{MaxPipesPerConnection: 2})
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
		if _, err := c.AddPipe(ctx, pio, zmux.PipeOptions{}); err != nil {
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

	// A first stream without a token.
	qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenPipe(ctx, qc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.W.Write(make([]byte, zmux.PrefixTokenLen)); err != nil {
		t.Fatal(err)
	}
	if n, _ := io.ReadAll(p.R); len(n) != 0 {
		t.Fatalf("read %d bytes", len(n))
	}
	select {
	case <-qc.Context().Done():
	case <-ctx.Done():
		t.Fatal("the connection was not closed")
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
	adm := &zmux.Admission{Peek: 4, PeekTimeout: time.Second,
		Decide: func(context.Context, *zmux.CarrierInfo, []byte) zmux.Decision {
			decided.Add(1)
			return zmux.Decision{Verdict: zmux.VerdictAccept}
		}}
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = Serve(ctx, ln, lst, nil); close(done) }()
	defer func() { cancel(); _ = ln.Close(); _ = lst.Close(); <-done }()

	warm, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, ok := cli.ClientSessionCache.Get("localhost"); ok {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("session ticket not received")
		case <-time.After(time.Millisecond):
		}
	}
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
	adm := &zmux.Admission{Peek: 2, MaxPeek: 64, PeekTimeout: 5 * time.Second,
		Decide: func(_ context.Context, _ *zmux.CarrierInfo, prefix []byte) zmux.Decision {
			if c, _ := zmux.ClassifyFirstBytes(prefix, zmux.FramingByteStream); c != zmux.FirstBytesOther || prefix[0] != 0x01 {
				return zmux.Decision{Verdict: zmux.VerdictClose}
			}
			if len(prefix) < int(prefix[1]) {
				return zmux.Decision{Verdict: zmux.VerdictMore, Need: int(prefix[1])}
			}
			return zmux.Decision{Verdict: zmux.VerdictAccept, Skip: int(prefix[1]), Variant: "q"}
		}}
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = Serve(ctx, ln, lst, nil); close(done) }()
	defer func() { cancel(); _ = ln.Close(); _ = lst.Close(); <-done }()
	qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenPipe(ctx, qc)
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
	if st := lst.Stats().Admission; st.Variants["q"].Accepted != 1 {
		t.Fatalf("stats %+v", st)
	}
}
