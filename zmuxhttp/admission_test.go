package zmuxhttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/internal/prefixtest"
)

func TestSplitBoundPrincipals(t *testing.T) {
	for _, tc := range []struct {
		name string
		down any
	}{
		{"matching", carrierio.BoundPrefixPrincipal{1}},
		{"different", carrierio.BoundPrefixPrincipal{2}},
		{"missing", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wrapped atomic.Bool
			l := mustNewListener(&zmux.ListenerConfig{
				Config: mpConfig(),
				Admission: &zmux.Admission{Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
					value := tc.down
					if info.Request.Method == http.MethodPost {
						value = carrierio.BoundPrefixPrincipal{1}
					}
					return zmux.Decision{Verdict: zmux.VerdictAccept, Value: value}
				}},
				WrapPipe: zmux.WrapPipeFunc(func(_ context.Context, p zmux.PipeIO, _ zmux.PipeHint) (zmux.PipeIO, error) {
					wrapped.Store(true)
					return p, nil
				}),
			})
			h, err := NewHandler(l, nil)
			if err != nil {
				_ = l.Close()
				t.Fatal(err)
			}
			srv := httptest.NewServer(h)
			defer srv.Close()
			defer func() { _ = l.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			p, err := (Split{Client: srv.Client(), URL: srv.URL}).Pipe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.R.Close(); _ = p.W.Close() }()
			if tc.name == "matching" {
				c, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = c.Close(); waitDone(t, c) }()
				s, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s.Close(); waitDone(t, s) }()
				if id, ok := admit.BoundPrincipal(s); !ok || id != [16]byte{1} {
					t.Fatalf("bound principal = %x, present %v", id, ok)
				}
				return
			}
			b, err := io.ReadAll(io.LimitReader(p.R, 1))
			st := l.Stats()
			if ctx.Err() != nil || len(b) != 0 || wrapped.Load() || st.Admission.Accepted != 2 {
				t.Fatalf("split rejection: read %x, err %v, wrapped %v, stats %+v", b, err, wrapped.Load(), st)
			}
		})
	}
}

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
	p, err := Streaming{Client: srv.Client(), URL: srv.URL}.Pipe(ctx, zmux.DirectionBidirectional)
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
	auth, v, _ := prefixtest.New(t, admit.PrefixCarrierOptions{AllowCleartext: true}, zmux.VerdictFallback)
	admission := v.Admission()
	admission.PeekTimeout, admission.StallMax = time.Second, time.Second
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: admission})
	defer func() { _ = lst.Close() }()
	h, err := NewHandler(lst, &Options{Next: siteHandler()})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(h)
	defer srv.Close()
	site := httptest.NewServer(siteHandler())
	defer site.Close()
	tok, err := auth.Prefix(&zmux.CarrierInfo{Encrypted: true})
	if err != nil {
		t.Fatal(err)
	}
	if d := v.Decide(t.Context(), zmux.CarrierInfo{Encrypted: true}, tok); d.Verdict != zmux.VerdictAccept {
		t.Fatalf("token: %+v", d)
	}
	random := make([]byte, 64)
	_, _ = rand.Read(random)
	rec := firstRecord(t)
	_, field, _ := auth.Header(&zmux.CarrierInfo{Encrypted: true})
	field2, _ := auth.Prefix(&zmux.CarrierInfo{Encrypted: true})
	bodyless := func(v ...string) http.Header { return http.Header{admit.HeaderPrefixAuth: v} }
	for _, x := range []struct {
		method string
		h      http.Header
		body   []byte
	}{
		{method: http.MethodGet, body: nil, h: nil},
		{method: http.MethodGet, body: nil, h: bodyless(field + "=")},
		{method: http.MethodGet, body: nil, h: bodyless(field[:85])},
		{method: http.MethodGet, body: nil, h: bodyless(field, base64.RawURLEncoding.EncodeToString(field2))},
		{method: http.MethodGet, body: nil, h: bodyless(base64.RawURLEncoding.EncodeToString(tok))}, // replayed
		{method: http.MethodPost, body: random, h: nil},
		{method: http.MethodPost, body: rec, h: nil},
		{method: http.MethodPost, body: slices.Concat(tok, rec), h: nil}, // replayed
		{method: http.MethodPost, body: tok[:10], h: nil},
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

func TestAuthenticatedExchangeUsesEnvelope(t *testing.T) {
	_, verifier, _ := prefixtest.New(t, admit.PrefixCarrierOptions{BindTLS: true}, zmux.VerdictFallback)
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: verifier.Admission()})
	defer func() { _ = l.Close() }()
	var psk [32]byte
	_, _ = rand.Read(psk[:])
	h := newExHandler(t, l, &Options{Next: siteHandler()}, &exCfg{PSK: psk[:]})
	defer func() { _ = h.Close() }()
	var cid [exCIDLen]byte
	_, _ = rand.Read(cid[:])
	var nonce [exNonceLen]byte
	_, _ = rand.Read(nonce[:])
	b := exRequest(newExKey(psk[:], cid[:]), &cid, exKindUp, 0, 0, 0, uint32(time.Now().Unix()), &nonce, firstRecord(t))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b)))
	if w.Code != http.StatusOK || l.Stats().Admission.Accepted != 1 {
		t.Fatalf("exchange status %d, admission %+v", w.Code, l.Stats().Admission)
	}
}

// TestAdmissionExchange sends the exchange handler a replayed request, a
// request outside the time window, a truncated one and random bytes: each
// is answered by Next as Next answers it directly, and creates nothing.
func TestAdmissionExchange(t *testing.T) {
	leakcheck.Check(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	h := newExHandler(t, lst, &Options{Next: siteHandler()},
		&exCfg{PSK: []byte("k"), HoldTime: 50 * time.Millisecond, MaxClockSkew: 30 * time.Second})
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
		return exRequest(key, &cid, exKindClose, 0, 0, 0, ts, &nonce, nil)
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
		Admission: &zmux.Admission{PeekTimeout: 5 * time.Second, Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
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
	p, err := Split{Client: hs.Client(), URL: hs.URL}.Pipe(ctx)
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
		Admission: &zmux.Admission{PeekTimeout: 5 * time.Second, Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
			return zmux.Decision{Verdict: zmux.VerdictAccept, Value: "ex"}
		}}})
	defer func() { _ = lst.Close() }()
	xc := &exCfg{AllowInsecure: true}
	eh, err := xc.handler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eh.Close() }()
	hs := httptest.NewServer(eh)
	defer hs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := xc.exchange(hs.Client(), hs.URL).Pipe(ctx)
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
type listenerTestClock struct {
	*timeshim.Fake
	armed map[time.Duration]chan struct{}
}

const (
	statsFirstRecordTimeout = 100 * time.Millisecond
	statsHandshakeTimeout   = 300 * time.Millisecond
)

func (c *listenerTestClock) AfterFunc(d time.Duration, f func()) timeshim.Timer {
	timer := c.Fake.AfterFunc(d, f)
	select {
	case c.armed[d] <- struct{}{}:
	default:
	}
	return timer
}

func TestListenerStatsIdentity(t *testing.T) {
	leakcheck.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	dctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	clock := &listenerTestClock{Fake: timeshim.NewFake(time.Now()), armed: map[time.Duration]chan struct{}{
		statsFirstRecordTimeout: make(chan struct{}, 1),
		statsHandshakeTimeout:   make(chan struct{}, 1),
	}}
	scfg := mpConfig()
	scfg.HandshakeTimeout = statsHandshakeTimeout
	scfg.ClockDomain = zmux.NewClockDomain(clock)
	lst := mustNewListener(&zmux.ListenerConfig{FirstRecordTimeout: statsFirstRecordTimeout, Config: scfg})
	defer func() { _ = lst.Close() }()
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hh)
	defer srv.Close()
	handle := func(p zmux.PipeIO, hint zmux.PipeHint) <-chan error {
		done := make(chan error, 1)
		go func() { done <- lst.HandlePipe(dctx, p, hint) }()
		return done
	}
	dial := func() (net.Conn, <-chan error) {
		t.Helper()
		var dialer net.Dialer
		nc, err := dialer.DialContext(dctx, "tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = nc.Close() })
		peer, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		return nc, handle(zmux.NetPipe(peer), zmux.PipeHint{})
	}
	check := func(done <-chan error, wantOK bool) {
		t.Helper()
		select {
		case err := <-done:
			if (err == nil) != wantOK {
				t.Fatalf("HandlePipe returned %v, want success %v", err, wantOK)
			}
		case <-dctx.Done():
			t.Fatal(dctx.Err())
		}
	}
	clearSignal := func(signal <-chan struct{}) {
		select {
		case <-signal:
		default:
		}
	}
	expire := func(armed <-chan struct{}, d time.Duration) {
		t.Helper()
		select {
		case <-armed:
			clock.Advance(d)
		case <-dctx.Done():
			t.Fatalf("waiting for %v timer: %v", d, dctx.Err())
		}
	}
	sum := func(s zmux.ListenerStats) uint64 { return s.Sessions + s.Joins + s.Rejected + s.Refused + s.TimedOut }

	// A session and a join over TCP.
	nc, done := dial()
	c, err := zmux.Client(dctx, nc, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	sc, err := lst.Accept(dctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.Abort(0, ""); sc.Abort(0, ""); waitDone(t, c); waitDone(t, sc) }()
	check(done, true)
	nc, done = dial()
	if _, err := c.AddPipe(dctx, zmux.NetPipe(nc), zmux.PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	check(done, true)

	// A split bootstrap: two carriers, one input.
	sp, err := Split{Client: srv.Client(), URL: srv.URL}.Pipe(dctx)
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
	defer func() { c2.Abort(0, ""); sc2.Abort(0, ""); waitDone(t, c2); waitDone(t, sc2) }()

	// Not zmux, and no first record.
	nc, done = dial()
	if _, err := nc.Write([]byte("GET / HTTP/1.1\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	check(done, false)
	clearSignal(clock.armed[statsFirstRecordTimeout])
	_, done = dial()
	expire(clock.armed[statsFirstRecordTimeout], statsFirstRecordTimeout)
	check(done, false)

	// A split half whose other half never arrives.
	clearSignal(clock.armed[statsHandshakeTimeout])
	h1, h2 := net.Pipe()
	defer func() { _ = h2.Close() }()
	tok := base64.RawURLEncoding.EncodeToString(make([]byte, 16))
	done = handle(zmux.PipeIO{R: h1, Carrier: zmux.CarrierSplit}, zmux.PipeHint{Pair: tok, Uplink: true})
	expire(clock.armed[statsHandshakeTimeout], statsHandshakeTimeout)
	check(done, false)

	// Admission verdicts other than accept count in Admission alone.
	for _, v := range []zmux.AdmissionVerdict{zmux.VerdictClose, zmux.VerdictFallback, zmux.VerdictStall} {
		adm := &zmux.Admission{StallMin: time.Millisecond, StallMax: 10 * time.Millisecond,
			Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision { return zmux.Decision{Verdict: v} }}
		al := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm,
			Fallback: func(context.Context, *zmux.Takeover) {}})
		a1, a2 := net.Pipe()
		_ = al.HandlePipe(dctx, zmux.NetPipe(a2), zmux.PipeHint{})
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
	lst.Adapter().CountSetupFailure()
	if got := lst.Stats(); got.SetupFailures != before.SetupFailures+1 || sum(got) != sum(before) {
		t.Fatalf("CountSetupFailure: %+v after %+v", got, before)
	}
	var none *zmux.Listener
	none.Adapter().CountSetupFailure() // a nil listener does nothing

	// After Close the carrier is refused.
	_ = lst.Close()
	r1, r2 := net.Pipe()
	defer func() { _ = r2.Close() }()
	_ = lst.HandlePipe(dctx, zmux.NetPipe(r1), zmux.PipeHint{})

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
	adm := decideWith(zmux.VerdictClose, &decided)
	decisions := make(chan string, 8)
	decide := adm.Decide
	adm.Decide = func(ctx context.Context, info zmux.CarrierInfo, b []byte) zmux.Decision {
		verdict := decide(ctx, info, b)
		if info.Request != nil {
			select {
			case decisions <- info.Request.URL.Path:
			case <-ctx.Done():
			}
		}
		return verdict
	}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	expect := func(path string) {
		t.Helper()
		for {
			select {
			case got := <-decisions:
				if got == path {
					return
				}
			case <-ctx.Done():
				t.Fatalf("admission for %s: %v", path, ctx.Err())
			}
		}
	}
	// HTTP carriers and the exchange carrier.
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	ex := newExHandler(t, lst, nil, &exCfg{PSK: make([]byte, 32)})
	defer func() { _ = ex.Close() }()
	srv := httptest.NewServer(&Mux{Stream: hh, Exchange: ex})
	defer srv.Close()
	dctx, dcancel := context.WithTimeout(ctx, 2*time.Second)
	defer dcancel()
	// fresh replaces dctx with a new deadline derived from ctx.
	fresh := func() {
		dcancel()
		dctx, dcancel = context.WithTimeout(ctx, 2*time.Second) //nolint:fatcontext // replaces dctx from ctx, does not nest
	}
	if p, err := (Streaming{Client: srv.Client(), URL: srv.URL + "/streaming"}).Pipe(dctx, zmux.DirectionBidirectional); err == nil {
		_, _ = zmux.Start(dctx, p, zmux.RoleClient, mpConfig())
	}
	expect("/streaming")
	fresh()
	if p, err := (Split{Client: srv.Client(), URL: srv.URL + "/split"}).Pipe(dctx); err == nil {
		_, _ = zmux.Start(dctx, p, zmux.RoleClient, mpConfig())
	}
	expect("/split")
	fresh()
	if p, err := (&exCfg{PSK: make([]byte, 32)}).exchange(srv.Client(), srv.URL+"/exchange").Pipe(dctx); err == nil {
		_, _ = zmux.Start(dctx, p, zmux.RoleClient, mpConfig())
	}
	expect("/exchange")
	srv.Close() // Handler completion publishes admission counts.
	if st := lst.Stats(); st.Sessions != 0 || st.Admission.Closed != uint64(decided.Load()) {
		t.Fatalf("stats %+v after %d decisions", st, decided.Load())
	}
}
