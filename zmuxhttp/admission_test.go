package zmuxhttp

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// An HTTP request carrier reads more of its body on VerdictMore.
func TestAdmissionMoreHTTP(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var calls sync.Map
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: classifyingAdmission(&calls)})
	defer func() { _ = lst.Close() }()
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(hh)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	p, err := StreamCandidate(srv.Client(), srv.URL, nil).Dial(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.W.Write(testHeader()); err != nil {
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
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	if vs := lst.VariantStats(); variantNamed(vs, "x").Accepted != 1 {
		t.Fatalf("variants %+v", vs)
	}
}

// TestAdmissionHTTP sends the HTTP carrier handler requests without a
// valid token, bodiless ones with a missing, malformed, repeated or
// replayed Zmux-Prefix-Auth field: each is answered by Next exactly as
// Next answers it directly, with the body bytes the admission read
// replayed.
func TestAdmissionHTTP(t *testing.T) {
	leakcheck.Check(t)
	auth := &admit.PrefixAuth{Key: []byte("k"), Slot: 30 * time.Second}
	v, err := admit.NewPrefixAuthVerifier(auth)
	if err != nil {
		t.Fatal(err)
	}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(),
		Admission: &zmux.Admission{Peek: admit.PrefixTokenLen, PeekTimeout: time.Second, Decide: v.Decide, StallMax: time.Second}})
	defer func() { _ = lst.Close() }()
	h, err := NewHandler(lst, &Options{Next: siteHandler()})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	site := httptest.NewServer(siteHandler())
	defer site.Close()
	tok, _ := auth.Prefix(nil)
	if !v.Verify(tok, nil) {
		t.Fatal("token")
	}
	random := make([]byte, 64)
	_, _ = rand.Read(random)
	rec := firstRecord(t)
	_, field, _ := auth.Header(nil)
	field2, _ := auth.Prefix(nil)
	bodyless := func(v ...string) http.Header { return http.Header{admit.HeaderPrefixAuth: v} }
	for _, x := range []struct {
		method string
		body   []byte
		h      http.Header
	}{
		{http.MethodGet, nil, nil},
		{http.MethodGet, nil, bodyless(field + "=")},
		{http.MethodGet, nil, bodyless(field[:42])},
		{http.MethodGet, nil, bodyless(field, base64.RawURLEncoding.EncodeToString(field2))},
		{http.MethodGet, nil, bodyless(base64.RawURLEncoding.EncodeToString(tok))}, // replayed
		{http.MethodPost, random, nil},
		{http.MethodPost, rec, nil},
		{http.MethodPost, append(append([]byte{}, tok...), rec...), nil}, // replayed
		{http.MethodPost, tok[:10], nil},
	} {
		got := doReqHeader(t, srv.Client(), x.method, srv.URL+"/p", x.body, x.h)
		want := doReqHeader(t, site.Client(), x.method, site.URL+"/p", x.body, x.h)
		if got != want {
			t.Fatalf("%s %x %v: %+v, the host gives %+v", x.method, x.body, x.h, got, want)
		}
	}
	srv.Client().Transport.(*http.Transport).CloseIdleConnections()
	site.Client().Transport.(*http.Transport).CloseIdleConnections()
}

// TestAdmissionExchange sends the exchange handler a replayed request, a
// request outside the time window, a truncated one and random bytes: each
// is answered by Next as Next answers it directly, and creates nothing.
func TestAdmissionExchange(t *testing.T) {
	leakcheck.Check(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	h := newExHandler(t, lst, &Options{Next: siteHandler()},
		&ExchangeConfig{PSK: []byte("k"), HoldTime: 50 * time.Millisecond, MaxClockSkew: 30 * time.Second})
	defer func() { _ = h.Close() }()
	srv := httptest.NewServer(h)
	defer srv.Close()
	site := httptest.NewServer(siteHandler())
	defer site.Close()
	var cid [exCIDLen]byte
	_, _ = rand.Read(cid[:])
	key := newExKey([]byte("k"), cid[:])
	req := func(ts uint32) []byte {
		var nonce [exNonceLen]byte
		_, _ = rand.Read(nonce[:])
		return appendExRequest(nil, key, &cid, exKindClose, 0, 0, 0, ts, &nonce, nil)
	}
	now := uint32(time.Now().Unix())
	first := req(now)
	if r := doReq(t, srv.Client(), http.MethodPost, srv.URL, first); r.code != http.StatusOK || r.ct != "application/octet-stream" {
		t.Fatalf("valid request: %+v", r)
	}
	random := make([]byte, 64)
	_, _ = rand.Read(random)
	for _, x := range []struct {
		name string
		body []byte
	}{
		{"replayed", first},
		{"stale", req(now - 120)},
		{"ahead", req(now + 120)},
		{"truncated", first[:20]},
		{"random", random},
	} {
		got := doReq(t, srv.Client(), http.MethodPost, srv.URL, x.body)
		want := doReq(t, site.Client(), http.MethodPost, site.URL, x.body)
		if got != want {
			t.Fatalf("%s: %+v, the host gives %+v", x.name, got, want)
		}
	}
	if got := doReq(t, srv.Client(), http.MethodGet, srv.URL, nil); !strings.HasPrefix(got.body, "site GET") {
		t.Fatalf("GET: %+v", got)
	}
	h.mu.Lock()
	n := len(h.carriers)
	h.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d carriers created", n)
	}
	srv.Client().Transport.(*http.Transport).CloseIdleConnections()
	site.Client().Transport.(*http.Transport).CloseIdleConnections()
}

// TestAdmittedValueSplit checks that a split bootstrap's session takes
// the value of its uplink half.
func TestAdmittedValueSplit(t *testing.T) {
	leakcheck.Check(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(),
		Admission: &zmux.Admission{PeekTimeout: 5 * time.Second, Decide: func(_ context.Context, info *zmux.CarrierInfo, _ []byte) zmux.Decision {
			return zmux.Decision{Verdict: zmux.VerdictAccept, Value: info.Request.Method}
		}}})
	defer func() { _ = lst.Close() }()
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(hh)
	defer hs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := SplitPipe(ctx, hs.Client(), hs.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := zmux.AdmittedAs[string](srv); got != "POST" {
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
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(),
		Admission: &zmux.Admission{PeekTimeout: 5 * time.Second, Decide: func(context.Context, *zmux.CarrierInfo, []byte) zmux.Decision {
			return zmux.Decision{Verdict: zmux.VerdictAccept, Value: "ex"}
		}}})
	defer func() { _ = lst.Close() }()
	xc := &ExchangeConfig{AllowInsecure: true}
	eh, err := NewExchangeHandler(lst, nil, xc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eh.Close() }()
	hs := httptest.NewServer(eh)
	defer hs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := ExchangePipe(ctx, hs.Client(), hs.URL, xc)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := zmux.AdmittedAs[string](srv); got != "ex" {
		t.Fatalf("session value %v", got)
	}
	_ = cli.Close()
	_ = srv.Close()
	waitDone(t, cli)
	waitDone(t, srv)
	hs.Client().CloseIdleConnections()
}

// TestListenerStatsIdentity hands a listener one pipe input of each kind
// whose outcome it decides and checks that every one is counted exactly
// once: Sessions + Joins + Rejected + Refused + TimedOut equals the inputs
// handed over, a split bootstrap's two carriers counting as one session,
// and SetupFailures stays out of the sum.
func TestListenerStatsIdentity(t *testing.T) {
	leakcheck.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scfg := mpConfig()
	scfg.HandshakeTimeout = 300 * time.Millisecond
	lst := mustNewListener(&zmux.ListenerConfig{FirstRecordTimeout: 100 * time.Millisecond, Config: scfg})
	done := make(chan struct{})
	go func() { _ = lst.Serve(ctx, ln); close(done) }()
	defer func() { cancel(); _ = lst.Close(); <-done }()
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hh)
	defer srv.Close()
	dial := func() net.Conn {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		return nc
	}
	sum := func(s zmux.ListenerStats) uint64 { return s.Sessions + s.Joins + s.Rejected + s.Refused + s.TimedOut }
	var inputs uint64
	expect := func(what string, n uint64) {
		t.Helper()
		inputs += n
		waitUntil(t, 5*time.Second, what, func() bool { return sum(lst.Stats()) == inputs })
	}
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()

	// A session and a join over TCP.
	c, err := zmux.Client(dctx, dial(), mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	sc, err := lst.Accept(dctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = sc.Close(); waitDone(t, c); waitDone(t, sc) }()
	expect("a session", 1)
	if _, err := c.AddPipe(dctx, zmux.NetPipe(dial()), zmux.PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	expect("a join", 1)

	// A split bootstrap: two carriers, one input.
	sp, err := SplitPipe(dctx, srv.Client(), srv.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := zmux.Start(dctx, sp, zmux.RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	sc2, err := lst.Accept(dctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close(); _ = sc2.Close(); waitDone(t, c2); waitDone(t, sc2) }()
	expect("a split session", 1)

	// Not zmux, and no first record.
	nc := dial()
	_, _ = nc.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	expect("not zmux", 1)
	_ = nc.Close()
	nc = dial()
	expect("no first record", 1)
	_ = nc.Close()

	// A split half whose other half never arrives.
	h1, h2 := net.Pipe()
	tok := base64.RawURLEncoding.EncodeToString(make([]byte, 16))
	go func() {
		_ = lst.HandlePipe(ctx, zmux.PipeIO{R: h1, Carrier: zmux.CarrierSplit}, zmux.PipeHint{Pair: tok, Uplink: true})
	}()
	expect("an unpaired half", 1)
	_ = h2.Close()

	// Admission verdicts other than accept count in Admission alone.
	for _, v := range []zmux.AdmissionVerdict{zmux.VerdictClose, zmux.VerdictFallback, zmux.VerdictStall} {
		adm := &zmux.Admission{StallMin: time.Millisecond, StallMax: 10 * time.Millisecond,
			Decide: func(context.Context, *zmux.CarrierInfo, []byte) zmux.Decision { return zmux.Decision{Verdict: v} }}
		al := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm,
			Fallback: func(_ context.Context, c net.Conn) { _ = c.Close() }})
		a1, a2 := net.Pipe()
		_ = al.HandlePipe(ctx, zmux.NetPipe(a2), zmux.PipeHint{})
		_ = a1.Close()
		got := al.Stats()
		want := map[zmux.AdmissionVerdict]zmux.AdmissionStats{zmux.VerdictClose: {Closed: 1}, zmux.VerdictFallback: {Fallback: 1}, zmux.VerdictStall: {Stalled: 1}}[v]
		if !eqAdm(got.Admission, want) || sum(got) != 0 {
			t.Fatalf("verdict %d: %+v", v, got)
		}
		_ = al.Close()
	}

	// Setup failures are outside the sum.
	before := lst.Stats()
	lst.CountSetupFailure()
	if got := lst.Stats(); got.SetupFailures != before.SetupFailures+1 || sum(got) != sum(before) {
		t.Fatalf("CountSetupFailure: %+v after %+v", got, before)
	}
	var none *zmux.Listener
	none.CountSetupFailure() // a nil listener does nothing

	// After Close the carrier is refused.
	_ = lst.Close()
	r1, r2 := net.Pipe()
	defer func() { _ = r2.Close() }()
	_ = lst.HandlePipe(ctx, zmux.NetPipe(r1), zmux.PipeHint{})
	expect("a refusal", 1)

	got := lst.Stats()
	want := zmux.ListenerStats{Sessions: 2, Joins: 1, Rejected: 1, Refused: 1, TimedOut: 2, SetupFailures: 1}
	if got != want {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}

// TestAdmissionEveryHTTPEntry runs the listener's admission on every HTTP
// carrier: streaming, split and exchange.
func TestAdmissionEveryHTTPEntry(t *testing.T) {
	leakcheck.Check(t)
	var decided atomic.Int32
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: decideWith(zmux.VerdictClose, &decided)})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	want := int32(0)
	expect := func(what string) {
		t.Helper()
		want++
		waitUntil(t, 5*time.Second, what, func() bool { return decided.Load() == want })
	}
	// HTTP carriers and the exchange carrier.
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	ex := newExHandler(t, lst, nil, &ExchangeConfig{PSK: make([]byte, 32)})
	defer func() { _ = ex.Close() }()
	srv := httptest.NewServer(&Endpoint{Stream: hh, Exchange: ex})
	defer srv.Close()
	dctx, dcancel := context.WithTimeout(ctx, 2*time.Second)
	defer dcancel()
	fresh := func() context.Context {
		dcancel()
		dctx, dcancel = context.WithTimeout(ctx, 2*time.Second)
		return dctx
	}
	if p, err := StreamPipe(dctx, srv.Client(), srv.URL, nil, zmux.DirectionBidirectional, nil); err == nil {
		_, _ = zmux.Start(dctx, p, zmux.RoleClient, mpConfig())
	}
	expect("streaming")
	fresh()
	if p, err := SplitPipe(dctx, srv.Client(), srv.URL, nil, nil); err == nil {
		_, _ = zmux.Start(dctx, p, zmux.RoleClient, mpConfig())
	}
	waitUntil(t, 5*time.Second, "split", func() bool { return decided.Load() >= want+1 })
	want = decided.Load()
	fresh()
	if p, err := ExchangePipe(dctx, srv.Client(), srv.URL, &ExchangeConfig{PSK: make([]byte, 32)}); err == nil {
		_, _ = zmux.Start(dctx, p, zmux.RoleClient, mpConfig())
	}
	waitUntil(t, 5*time.Second, "exchange", func() bool { return decided.Load() > want })
	srv.Client().Transport.(*http.Transport).CloseIdleConnections()
	if st := lst.Stats(); st.Sessions != 0 || st.Admission.Closed != uint64(decided.Load()) {
		t.Fatalf("stats %+v after %d decisions", st, decided.Load())
	}
}
