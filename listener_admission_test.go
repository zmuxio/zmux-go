package zmux

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// decideWith is an Admission whose every decision is v, counted in n.
func decideWith(v Verdict, n *atomic.Int32) *Admission {
	return &Admission{PeekTimeout: 5 * time.Second, StallMin: time.Millisecond, StallMax: time.Millisecond,
		Decide: func(context.Context, *CarrierInfo, []byte) Decision {
			n.Add(1)
			return Decision{Verdict: v}
		}}
}

// TestAdmissionEveryEntry checks that a listener's admission decides on the
// carriers of every entry point: Serve over TCP and TLS, HandlePipe, the
// HTTP stream handler (bidirectional and split), and the exchange
// handler; none of them starts a session.
func TestAdmissionEveryEntry(t *testing.T) {
	leakcheck.Check(t)
	var decided atomic.Int32
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: decideWith(VerdictClose, &decided)})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srvTLS, cliTLS := selfSignedTLS(t)
	serve := func(tlsOn bool) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		sl := ln
		if tlsOn {
			sl = tls.NewListener(ln, srvTLS)
		}
		done := make(chan struct{})
		go func() { _ = lst.Serve(ctx, sl); close(done) }()
		t.Cleanup(func() { _ = ln.Close(); <-done })
		return ln.Addr().String()
	}
	want := int32(0)
	expect := func(what string) {
		t.Helper()
		want++
		waitUntil(t, 5*time.Second, what, func() bool { return decided.Load() == want })
	}
	// TCP and TLS.
	nc, err := net.Dial("tcp", serve(false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(nc); err != nil {
		t.Fatal(err)
	}
	_ = nc.Close()
	expect("tcp")
	tc, err := tls.Dial("tcp", serve(true), cliTLS)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(tc)
	_ = tc.Close()
	expect("tls")
	// HandlePipe.
	a, b := net.Pipe()
	_ = lst.HandlePipe(ctx, NetPipe(a), JoinHint{})
	_ = b.Close()
	expect("HandlePipe")
	// HTTP carriers and the exchange carrier.
	hh, err := NewHTTPHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	ex := newExHandler(t, lst, &HTTPExchangeConfig{PSK: make([]byte, 32)})
	defer func() { _ = ex.Close() }()
	srv := httptest.NewServer(&HTTPEndpoint{Stream: hh, Exchange: ex})
	defer srv.Close()
	dctx, dcancel := context.WithTimeout(ctx, 2*time.Second)
	defer dcancel()
	fresh := func() context.Context {
		dcancel()
		dctx, dcancel = context.WithTimeout(ctx, 2*time.Second)
		return dctx
	}
	if p, err := HTTPPipe(dctx, srv.Client(), srv.URL, nil, DirBidirectional, nil); err == nil {
		_, _ = Start(dctx, p, RoleClient, mpConfig())
	}
	expect("streaming")
	fresh()
	if p, err := HTTPSplitPipe(dctx, srv.Client(), srv.URL, nil, nil); err == nil {
		_, _ = Start(dctx, p, RoleClient, mpConfig())
	}
	waitUntil(t, 5*time.Second, "split", func() bool { return decided.Load() >= want+1 })
	want = decided.Load()
	fresh()
	if p, err := HTTPExchangePipe(dctx, srv.Client(), srv.URL, &HTTPExchangeConfig{PSK: make([]byte, 32)}); err == nil {
		_, _ = Start(dctx, p, RoleClient, mpConfig())
	}
	waitUntil(t, 5*time.Second, "exchange", func() bool { return decided.Load() > want })
	srv.Client().Transport.(*http.Transport).CloseIdleConnections()
	if st := lst.Stats(); st.Sessions != 0 || st.Admission.Closed != uint64(decided.Load()) {
		t.Fatalf("stats %+v after %d decisions", st, decided.Load())
	}
}

// TestAdmissionSource checks the one source of a carrier: JoinHint.Source
// given to HandlePipe, or the peer, is CarrierInfo.Source, and the
// per-source limit counts it, whatever the peer.
func TestAdmissionSource(t *testing.T) {
	leakcheck.Check(t)
	infos := make(chan CarrierInfo, 4)
	release := make(chan struct{})
	adm := &Admission{PeekTimeout: 5 * time.Second, Decide: func(ctx context.Context, info *CarrierInfo, _ []byte) Decision {
		infos <- *info
		select {
		case <-release:
		case <-ctx.Done():
		}
		return Decision{Verdict: VerdictClose}
	}}
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: adm, MaxPendingPerSource: 1})
	defer func() { _ = lst.Close() }()
	ctx := context.Background()
	src := netip.MustParseAddrPort("203.0.113.7:0")
	peer := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1000}
	a, b := net.Pipe()
	done := make(chan struct{})
	go func() { _ = lst.HandlePipe(ctx, NetPipe(addrConn{a, peer}), JoinHint{Source: src}); close(done) }()
	info := <-infos
	if info.Source != src || info.Peer != netip.MustParseAddrPort("192.0.2.1:1000") {
		t.Fatalf("source %v, peer %v", info.Source, info.Peer)
	}
	// Another peer, the same source: refused at once, undecided.
	c, d := net.Pipe()
	other := &net.TCPAddr{IP: net.ParseIP("198.51.100.1"), Port: 1}
	if err := lst.HandlePipe(ctx, NetPipe(addrConn{c, other}), JoinHint{Source: netip.MustParseAddrPort("203.0.113.7:9")}); err == nil {
		t.Fatal("a second carrier of a full source was taken")
	}
	if _, err := d.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("refused carrier: %v", err)
	}
	// Without JoinHint.Source the peer is the source.
	e, f := net.Pipe()
	done2 := make(chan struct{})
	go func() { _ = lst.HandlePipe(ctx, NetPipe(addrConn{e, other}), JoinHint{}); close(done2) }()
	if info := <-infos; info.Source != netip.MustParseAddrPort("198.51.100.1:1") || info.Peer != info.Source {
		t.Fatalf("source %v, peer %v", info.Source, info.Peer)
	}
	close(release)
	<-done
	<-done2
	_, _, _ = b.Close(), d.Close(), f.Close()
	if st := lst.Stats(); st.Refused != 1 || st.Admission.Closed != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// TestAdmissionSlotBeforeHandshake checks that a carrier takes its source
// slot before its TLS handshake: connections that never finish their
// handshake hold the source's slots, and further ones from the source are
// closed without a handshake, while another source is served.
func TestAdmissionSlotBeforeHandshake(t *testing.T) {
	leakcheck.Check(t)
	srvTLS, _ := selfSignedTLS(t)
	var decided atomic.Int32
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: decideWith(VerdictClose, &decided), MaxPendingPerSource: 2})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1}
	var wg sync.WaitGroup
	var clients []net.Conn
	for range 2 { // silent clients: their handshakes never finish
		a, b := net.Pipe()
		clients = append(clients, b)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = lst.HandlePipe(ctx, NetPipe(tls.Server(addrConn{a, src}, srvTLS)), JoinHint{})
		}()
	}
	waitUntil(t, 5*time.Second, "two slots", func() bool {
		lst.mu.Lock()
		defer lst.mu.Unlock()
		return lst.pending.Held(sourceKey(netip.MustParseAddrPort("192.0.2.1:1"))) == 2
	})
	a, b := net.Pipe()
	if err := lst.HandlePipe(ctx, NetPipe(tls.Server(addrConn{a, src}, srvTLS)), JoinHint{}); err == nil {
		t.Fatal("a third carrier of the source was taken")
	}
	if _, err := b.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("refused carrier: %v", err)
	}
	cancel()
	for _, c := range clients {
		_ = c.Close()
	}
	wg.Wait()
	if st := lst.Stats(); st.Refused != 1 || st.Admission.HandshakeFailed != 2 || st.SetupFailures != 0 || decided.Load() != 0 {
		t.Fatalf("stats %+v, %d decisions", st, decided.Load())
	}
}

// TestAdmissionStats checks each AdmissionStats count, and that the
// listener's own counts do not repeat them.
func TestAdmissionStats(t *testing.T) {
	leakcheck.Check(t)
	ctx := context.Background()
	run := func(adm *Admission, fallback func(context.Context, net.Conn), send []byte, tlsServer bool) ListenerStats {
		t.Helper()
		lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: adm, Fallback: fallback})
		defer func() { _ = lst.Close() }()
		a, b := net.Pipe()
		go func() {
			if send != nil {
				_, _ = b.Write(send)
			}
			_, _ = io.Copy(io.Discard, b)
		}()
		p := NetPipe(a)
		if tlsServer {
			srvTLS, _ := selfSignedTLS(t)
			p = NetPipe(tls.Server(a, srvTLS))
			go func() { _ = b.Close() }()
		}
		_ = lst.HandlePipe(ctx, p, JoinHint{})
		_ = b.Close()
		return lst.Stats()
	}
	var n atomic.Int32
	for _, x := range []struct {
		name string
		v    Verdict
		fb   bool
		want AdmissionStats
	}{
		{"fallback", VerdictFallback, true, AdmissionStats{Fallback: 1}},
		{"fallback without target", VerdictFallback, false, AdmissionStats{Fallback: 1}},
		{"stall", VerdictStall, false, AdmissionStats{Stalled: 1}},
		{"close", VerdictClose, false, AdmissionStats{Closed: 1}},
		{"unknown", Verdict(9), false, AdmissionStats{Closed: 1}},
	} {
		var fb func(context.Context, net.Conn)
		if x.fb {
			fb = func(_ context.Context, c net.Conn) { _ = c.Close() }
		}
		st := run(decideWith(x.v, &n), fb, nil, false)
		if st.Admission != x.want || st.Rejected != 0 {
			t.Fatalf("%s: %+v", x.name, st)
		}
	}
	// A panic, and a Skip outside the prefix, are VerdictClose.
	panicking := &Admission{Decide: func(context.Context, *CarrierInfo, []byte) Decision { panic("decide") }}
	if st := run(panicking, nil, nil, false); st.Admission != (AdmissionStats{Closed: 1}) {
		t.Fatalf("panic: %+v", st)
	}
	skip := &Admission{Peek: 2, Decide: func(context.Context, *CarrierInfo, []byte) Decision {
		return Decision{Verdict: VerdictAccept, Skip: 3}
	}}
	if st := run(skip, nil, []byte{1, 2}, false); st.Admission != (AdmissionStats{Closed: 1}) {
		t.Fatalf("skip: %+v", st)
	}
	// Accepted: the carrier goes on to the listener, counted there once.
	if st := run(&Admission{Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{} }}, nil,
		[]byte("GET / HTTP/1.1\r\n\r\n"), false); st.Admission != (AdmissionStats{Accepted: 1}) || st.Rejected != 1 {
		t.Fatalf("accept: %+v", st)
	}
	// A failed TLS handshake is HandshakeFailed, not a setup failure.
	if st := run(decideWith(VerdictAccept, &n), nil, nil, true); st.Admission != (AdmissionStats{HandshakeFailed: 1}) || st.SetupFailures != 0 {
		t.Fatalf("handshake: %+v", st)
	}
}

// fakeEnv is a listener on a fake clock, with a carrier handed to it.
type fakeEnv struct {
	clk  *timeshim.Fake
	lst  *Listener
	peer net.Conn
	done chan struct{}
}

func newFakeEnv(t *testing.T, adm *Admission, firstRecord time.Duration) *fakeEnv {
	t.Helper()
	clk := timeshim.NewFake(time.Unix(1767225600, 0))
	cfg := mpConfig()
	cfg.Clock = clk
	e := &fakeEnv{clk: clk, lst: NewListener(&ListenerConfig{Config: cfg, Admission: adm, FirstRecordTimeout: firstRecord}),
		done: make(chan struct{})}
	a, b := net.Pipe()
	e.peer = b
	go func() { _ = e.lst.HandlePipe(context.Background(), NetPipe(a), JoinHint{}); close(e.done) }()
	t.Cleanup(func() { _ = b.Close(); _ = e.lst.Close() })
	return e
}

// advance waits for n timers of the fake clock and moves it by d.
func (e *fakeEnv) advance(t *testing.T, n int, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.clk.WaitPending(ctx, n); err != nil {
		t.Fatalf("waiting for %d timers: %v", n, err)
	}
	e.clk.Advance(d)
}

// ended checks that the carrier ends at once, and not before.
func (e *fakeEnv) ended(t *testing.T) {
	t.Helper()
	select {
	case <-e.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the carrier did not end")
	}
}

// TestAdmissionFakeClock checks the admission's and the listener's
// timeouts on the listener's clock: the peek timeout, the decision's
// deadline, the stall duration and the first-record timeout each end the
// carrier exactly when the clock reaches them.
func TestAdmissionFakeClock(t *testing.T) {
	leakcheck.Check(t)
	t.Run("peek", func(t *testing.T) {
		e := newFakeEnv(t, &Admission{Peek: 4, PeekTimeout: 10 * time.Second,
			Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{} }}, 0)
		_, _ = e.peer.Write([]byte{1})
		e.advance(t, 1, 10*time.Second-time.Nanosecond)
		select {
		case <-e.done:
			t.Fatal("ended before the peek timeout")
		case <-time.After(20 * time.Millisecond):
		}
		e.clk.Advance(time.Nanosecond)
		e.ended(t)
		if st := e.lst.Stats().Admission; st != (AdmissionStats{PeekTimedOut: 1}) {
			t.Fatalf("stats %+v", st)
		}
	})
	t.Run("decision deadline", func(t *testing.T) {
		dls := make(chan time.Time, 1)
		e := newFakeEnv(t, &Admission{PeekTimeout: 10 * time.Second, Decide: func(ctx context.Context, _ *CarrierInfo, _ []byte) Decision {
			dl, _ := ctx.Deadline()
			<-ctx.Done()
			dls <- dl
			return Decision{}
		}}, 0)
		start := e.clk.Now()
		e.advance(t, 1, 10*time.Second)
		e.ended(t)
		dl := <-dls
		if st := e.lst.Stats().Admission; st != (AdmissionStats{PeekTimedOut: 1}) || !dl.Equal(start.Add(10*time.Second)) {
			t.Fatalf("stats %+v, deadline %v after the start", st, dl.Sub(start))
		}
	})
	t.Run("stall", func(t *testing.T) {
		e := newFakeEnv(t, &Admission{PeekTimeout: time.Second, StallMin: 30 * time.Second, StallMax: 30 * time.Second,
			Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictStall} }}, 0)
		go func() { _, _ = io.Copy(io.Discard, e.peer) }()
		waitUntil(t, 5*time.Second, "stalled", func() bool { return e.lst.Stats().Admission.Stalled == 1 })
		e.advance(t, 1, 30*time.Second-time.Nanosecond)
		select {
		case <-e.done:
			t.Fatal("ended before the stall duration")
		case <-time.After(20 * time.Millisecond):
		}
		e.clk.Advance(time.Nanosecond)
		e.ended(t)
	})
	t.Run("first record", func(t *testing.T) {
		e := newFakeEnv(t, nil, 3*time.Second)
		e.advance(t, 1, 3*time.Second)
		e.ended(t)
		if st := e.lst.Stats(); st.TimedOut != 1 {
			t.Fatalf("stats %+v", st)
		}
	})
}

// TestAdmissionDecideCancel checks that Close cancels a decision in
// progress: its ctx ends with context.Canceled and the carrier is closed.
func TestAdmissionDecideCancel(t *testing.T) {
	leakcheck.Check(t)
	errc := make(chan error, 1)
	started := make(chan struct{})
	adm := &Admission{PeekTimeout: time.Minute, Decide: func(ctx context.Context, _ *CarrierInfo, _ []byte) Decision {
		close(started)
		<-ctx.Done()
		errc <- ctx.Err()
		return Decision{}
	}}
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: adm})
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	done := make(chan error, 1)
	go func() { done <- lst.HandlePipe(context.Background(), NetPipe(a), JoinHint{}) }()
	<-started
	_ = lst.Close()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("HandlePipe succeeded")
	}
	if st := lst.Stats().Admission; st != (AdmissionStats{Closed: 1}) {
		t.Fatalf("stats %+v", st)
	}
}

// TestShutdownKeepsJoins checks Shutdown's graceful close: during it a new
// session is refused while a live session still joins pipes; Close then
// refuses every carrier.
func TestShutdownKeepsJoins(t *testing.T) {
	leakcheck.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lst := NewListener(&ListenerConfig{Config: mpConfig()})
	served := make(chan error, 1)
	go func() { served <- lst.Serve(ctx, ln) }()
	dial := func() net.Conn {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		return nc
	}
	c, err := Client(ctx, dial(), mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// An open stream keeps the session, and so Shutdown, going.
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	ss, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	shut := make(chan error, 1)
	go func() { shut <- lst.Shutdown(ctx, 0, "") }()
	if _, err := lst.Accept(ctx); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept during Shutdown: %v", err)
	}
	// A new session is refused.
	refused := lst.Stats().Refused
	nc := dial()
	ncfg := mpConfig()
	ncfg.HandshakeTimeout = time.Second
	if c2, err := Client(ctx, nc, ncfg); err == nil {
		_ = c2.Wait()
	}
	waitUntil(t, 5*time.Second, "a refused session", func() bool { return lst.Stats().Refused > refused })
	// The live session joins a pipe.
	if _, err := c.AddPipe(ctx, NetPipe(dial()), PipeOptions{}); err != nil {
		t.Fatalf("join during Shutdown: %v", err)
	}
	if lst.Stats().Joins != 1 {
		t.Fatalf("stats %+v", lst.Stats())
	}
	// The stream ends, the session with it, and Shutdown closes the listener.
	_ = st.Close()
	_, _ = io.ReadAll(ss)
	_ = ss.Close()
	_, _ = io.ReadAll(st)
	if err := <-shut; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := <-served; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Serve: %v", err)
	}
	waitDone(t, s)
	_ = c.Close()
	waitDone(t, c)
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	if err := lst.HandlePipe(ctx, NetPipe(a), JoinHint{}); err == nil {
		t.Fatal("a closed listener took a carrier")
	}
}
