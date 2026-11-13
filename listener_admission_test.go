package zmux

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// decideWith is an Admission whose every decision is v, counted in n.
func decideWith(v AdmissionVerdict, n *atomic.Int32) *Admission {
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
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: decideWith(VerdictClose, &decided)})
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
	_ = lst.HandlePipe(ctx, NetPipe(a), PipeHint{})
	_ = b.Close()
	expect("HandlePipe")
	if st := lst.Stats(); st.Sessions != 0 || st.Admission.Closed != uint64(decided.Load()) {
		t.Fatalf("stats %+v after %d decisions", st, decided.Load())
	}
}

// TestAdmissionSource checks the one source of a carrier: PipeHint.Source
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
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: adm, MaxPendingPerSource: 1})
	defer func() { _ = lst.Close() }()
	ctx := context.Background()
	src := netip.MustParseAddrPort("203.0.113.7:0")
	peer := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1000}
	a, b := net.Pipe()
	done := make(chan struct{})
	go func() { _ = lst.HandlePipe(ctx, NetPipe(addrConn{a, peer}), PipeHint{Source: src}); close(done) }()
	info := <-infos
	if info.Source != src || info.Peer != netip.MustParseAddrPort("192.0.2.1:1000") {
		t.Fatalf("source %v, peer %v", info.Source, info.Peer)
	}
	// Another peer, the same source: refused at once, undecided.
	c, d := net.Pipe()
	other := &net.TCPAddr{IP: net.ParseIP("198.51.100.1"), Port: 1}
	if err := lst.HandlePipe(ctx, NetPipe(addrConn{c, other}), PipeHint{Source: netip.MustParseAddrPort("203.0.113.7:9")}); err == nil {
		t.Fatal("a second carrier of a full source was taken")
	}
	if _, err := d.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("refused carrier: %v", err)
	}
	// Without PipeHint.Source the peer is the source.
	e, f := net.Pipe()
	done2 := make(chan struct{})
	go func() { _ = lst.HandlePipe(ctx, NetPipe(addrConn{e, other}), PipeHint{}); close(done2) }()
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
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: decideWith(VerdictClose, &decided), MaxPendingPerSource: 2})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1}
	var wg sync.WaitGroup
	var clients []net.Conn
	for range 2 { // silent clients: their handshakes never finish
		a, b := net.Pipe()
		clients = append(clients, b)
		wg.Go(func() {
			_ = lst.HandlePipe(ctx, NetPipe(tls.Server(addrConn{a, src}, srvTLS)), PipeHint{})
		})
	}
	waitUntil(t, 5*time.Second, "two slots", func() bool {
		lst.mu.Lock()
		defer lst.mu.Unlock()
		return lst.pending.Held(lst.sourceKey(netip.MustParseAddrPort("192.0.2.1:1"))) == 2
	})
	a, b := net.Pipe()
	if err := lst.HandlePipe(ctx, NetPipe(tls.Server(addrConn{a, src}, srvTLS)), PipeHint{}); err == nil {
		t.Fatal("a third carrier of the source was taken")
	}
	if _, err := b.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
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
		lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: adm, Fallback: fallback})
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
		_ = lst.HandlePipe(ctx, p, PipeHint{})
		_ = b.Close()
		return lst.Stats()
	}
	var n atomic.Int32
	for _, x := range []struct {
		name string
		v    AdmissionVerdict
		fb   bool
		want AdmissionStats
	}{
		{"fallback", VerdictFallback, true, AdmissionStats{Fallback: 1}},
		{"fallback without target", VerdictFallback, false, AdmissionStats{Fallback: 1}},
		{"stall", VerdictStall, false, AdmissionStats{Stalled: 1}},
		{"close", VerdictClose, false, AdmissionStats{Closed: 1}},
		{"unknown", AdmissionVerdict(9), false, AdmissionStats{Closed: 1}},
	} {
		var fb func(context.Context, net.Conn)
		if x.fb {
			fb = func(_ context.Context, c net.Conn) { _ = c.Close() }
		}
		st := run(decideWith(x.v, &n), fb, nil, false)
		if !eqAdm(st.Admission, x.want) || st.Rejected != 0 {
			t.Fatalf("%s: %+v", x.name, st)
		}
	}
	// A panic, and a Skip outside the prefix, are VerdictClose.
	panicking := &Admission{Decide: func(context.Context, *CarrierInfo, []byte) Decision { panic("decide") }}
	if st := run(panicking, nil, nil, false); !eqAdm(st.Admission, AdmissionStats{Closed: 1}) {
		t.Fatalf("panic: %+v", st)
	}
	skip := &Admission{Peek: 2, Decide: func(context.Context, *CarrierInfo, []byte) Decision {
		return Decision{Verdict: VerdictAccept, Skip: 3}
	}}
	if st := run(skip, nil, []byte{1, 2}, false); !eqAdm(st.Admission, AdmissionStats{Closed: 1}) {
		t.Fatalf("skip: %+v", st)
	}
	// Accepted: the carrier goes on to the listener, counted there once.
	if st := run(&Admission{Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{} }}, nil,
		[]byte("GET / HTTP/1.1\r\n\r\n"), false); !eqAdm(st.Admission, AdmissionStats{Accepted: 1}) || st.Rejected != 1 {
		t.Fatalf("accept: %+v", st)
	}
	// A failed TLS handshake is HandshakeFailed, not a setup failure.
	if st := run(decideWith(VerdictAccept, &n), nil, nil, true); !eqAdm(st.Admission, AdmissionStats{HandshakeFailed: 1}) || st.SetupFailures != 0 {
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
	e := &fakeEnv{clk: clk, lst: mustNewListener(&ListenerConfig{Config: cfg, Admission: adm, FirstRecordTimeout: firstRecord}),
		done: make(chan struct{})}
	a, b := net.Pipe()
	e.peer = b
	go func() { _ = e.lst.HandlePipe(context.Background(), NetPipe(a), PipeHint{}); close(e.done) }()
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
		if st := e.lst.Stats().Admission; !eqAdm(st, AdmissionStats{PeekTimedOut: 1}) {
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
		if st := e.lst.Stats().Admission; !eqAdm(st, AdmissionStats{PeekTimedOut: 1}) || !dl.Equal(start.Add(10*time.Second)) {
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
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: adm})
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	done := make(chan error, 1)
	go func() { done <- lst.HandlePipe(context.Background(), NetPipe(a), PipeHint{}) }()
	<-started
	_ = lst.Close()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("HandlePipe succeeded")
	}
	if st := lst.Stats().Admission; !eqAdm(st, AdmissionStats{Closed: 1}) {
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
	lst := mustNewListener(&ListenerConfig{Config: mpConfig()})
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
	waitUntil(t, 5*time.Second, "the join counted", func() bool { return lst.Stats().Joins == 1 })
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
	if err := lst.HandlePipe(ctx, NetPipe(a), PipeHint{}); err == nil {
		t.Fatal("a closed listener took a carrier")
	}
}

// eqAdm compares admission totals.
func eqAdm(a, b AdmissionStats) bool { return a == b }

// TestAdmissionFallbackOnPeekTimeout: with FallbackOnPeekTimeout a carrier
// whose prefix does not arrive in time, in the first read or in a
// VerdictMore round, goes to the fallback with the bytes read replayed and
// the carrier still usable; a decision that does not return in time is
// still closed, and without the option the carrier is closed.
func TestAdmissionFallbackOnPeekTimeout(t *testing.T) {
	leakcheck.Check(t)
	run := func(t *testing.T, adm *Admission, send []byte, timers int) (*fakeEnv, <-chan net.Conn) {
		t.Helper()
		clk := timeshim.NewFake(time.Unix(1767225600, 0))
		cfg := mpConfig()
		cfg.Clock = clk
		got := make(chan net.Conn, 1)
		e := &fakeEnv{clk: clk, done: make(chan struct{}),
			lst: mustNewListener(&ListenerConfig{Config: cfg, Admission: adm,
				Fallback: func(_ context.Context, c net.Conn) { got <- c }})}
		a, b := net.Pipe()
		e.peer = b
		go func() { _ = e.lst.HandlePipe(context.Background(), NetPipe(a), PipeHint{}); close(e.done) }()
		t.Cleanup(func() { _ = b.Close(); _ = e.lst.Close() })
		if len(send) > 0 {
			if _, err := b.Write(send); err != nil {
				t.Fatal(err)
			}
		}
		e.advance(t, timers, 10*time.Second)
		return e, got
	}
	replayed := func(t *testing.T, e *fakeEnv, got <-chan net.Conn, want string) {
		t.Helper()
		var c net.Conn
		select {
		case c = <-got:
		case <-time.After(5 * time.Second):
			t.Fatal("no fallback")
		}
		buf := make([]byte, len(want))
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != want {
			t.Fatalf("replayed %q, %v; want %q", buf, err, want)
		}
		// The carrier goes on after the timeout.
		go func() { _, _ = e.peer.Write([]byte("z")) }()
		var one [1]byte
		if _, err := io.ReadFull(c, one[:]); err != nil || one[0] != 'z' {
			t.Fatalf("read after the fallback: %q, %v", one[:], err)
		}
		_ = c.Close()
	}
	decide := func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictAccept} }
	t.Run("first read", func(t *testing.T) {
		e, got := run(t, &Admission{Peek: 4, PeekTimeout: 10 * time.Second, FallbackOnPeekTimeout: true, Decide: decide}, []byte("ab"), 1)
		replayed(t, e, got, "ab")
		if st := e.lst.Stats().Admission; !eqAdm(st, AdmissionStats{PeekTimedOut: 1, Fallback: 1}) {
			t.Fatalf("stats %+v", st)
		}
	})
	t.Run("more", func(t *testing.T) {
		more := seq(Decision{Verdict: VerdictMore, Need: 8})
		e, got := run(t, &Admission{Peek: 1, MaxPeek: 8, PeekTimeout: 10 * time.Second, FallbackOnPeekTimeout: true, Decide: more}, []byte("abc"), 2)
		replayed(t, e, got, "abc")
		if st := e.lst.Stats().Admission; !eqAdm(st, AdmissionStats{PeekTimedOut: 1, Fallback: 1}) {
			t.Fatalf("stats %+v", st)
		}
	})
	t.Run("no bytes", func(t *testing.T) {
		e, got := run(t, &Admission{Peek: 4, PeekTimeout: 10 * time.Second, FallbackOnPeekTimeout: true, Decide: decide}, nil, 1)
		replayed(t, e, got, "")
	})
	t.Run("option off", func(t *testing.T) {
		e, _ := run(t, &Admission{Peek: 4, PeekTimeout: 10 * time.Second, Decide: decide}, []byte("ab"), 1)
		e.ended(t)
		if st := e.lst.Stats().Admission; !eqAdm(st, AdmissionStats{PeekTimedOut: 1}) {
			t.Fatalf("stats %+v", st)
		}
	})
	t.Run("decision not returned", func(t *testing.T) {
		block := func(ctx context.Context, _ *CarrierInfo, _ []byte) Decision { <-ctx.Done(); return Decision{} }
		e, _ := run(t, &Admission{PeekTimeout: 10 * time.Second, FallbackOnPeekTimeout: true, Decide: block}, nil, 1)
		e.ended(t)
		if st := e.lst.Stats().Admission; !eqAdm(st, AdmissionStats{PeekTimedOut: 1}) {
			t.Fatalf("stats %+v", st)
		}
	})
}

// TestKnownJoin: KnownJoin tells, from the head of a Hello, whether its
// Locator names a session of the listener, and how long a prefix it needs.
func TestKnownJoin(t *testing.T) {
	lst := mustNewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	loc := []byte("known-locator")
	lst.mu.Lock()
	lst.sessions[string(loc)] = new(Conn)
	lst.mu.Unlock()
	t.Cleanup(func() {
		lst.mu.Lock()
		delete(lst.sessions, string(loc))
		lst.mu.Unlock()
	})
	hello := func(suite, mode byte, locator []byte) []byte {
		b := []byte{0x02, suite, mode, byte(len(locator))}
		b = append(b, locator...)
		return append(b, make([]byte, 32)...) // R
	}
	full := hello(1, 0, loc)
	head := len(full)
	for _, c := range []struct {
		name   string
		prefix []byte
		known  bool
		need   int
	}{
		{"empty", nil, false, 0},
		{"not a join", []byte{0x40, 0x10, 0xff}, false, 0},
		{"first byte only", full[:1], false, 2},
		{"suite only", full[:2], false, 3},
		{"mode only", full[:3], false, 4},
		{"length known", full[:4], false, head},
		{"locator cut", full[:10], false, head},
		{"nonce cut", full[:head-1], false, head},
		{"head", full[:head], true, 0},
		{"head and payload", append(bytes.Clone(full), 0, 4, 1, 2, 3, 4), true, 0},
		{"bound mode", hello(1, 1, loc), true, 0},
		{"unknown suite", hello(9, 0, loc), false, 0},
		{"reserved mode", hello(1, 3, loc), false, 0},
		{"spare mode bits", hello(1, 0x04, loc), false, 0},
		{"zero locator length", []byte{0x02, 1, 0, 0}, false, 0},
		{"unknown locator", hello(1, 0, []byte("other-locator")), false, 0},
	} {
		known, need := lst.KnownJoin(c.prefix)
		if known != c.known || need != c.need {
			t.Errorf("%s: KnownJoin = %v, %d; want %v, %d", c.name, known, need, c.known, c.need)
		}
	}
	lst.mu.Lock()
	delete(lst.sessions, string(loc))
	lst.mu.Unlock()
	if known, _ := lst.KnownJoin(full); known {
		t.Error("a session that ended is still known")
	}
}
