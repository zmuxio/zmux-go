package zmux

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestPrefixAuthVectors checks the prefix authenticator against fixed vectors.
func TestPrefixAuthVectors(t *testing.T) {
	a := &PrefixAuth{Key: bytes.Repeat([]byte{0x6b}, 32), Slot: 30 * time.Second}
	at := time.Unix(1767225600, 0)
	k, _, _ := a.prefixKey()
	if got := hex.EncodeToString(k); got != "2eaed9a5e8ca0729884488be9c349871b69823f01876ad48062a8b15c6c1839c" {
		t.Fatalf("key %s", got)
	}
	bind := make([]byte, 32)
	for i := range bind {
		bind[i] = byte(i)
	}
	for _, x := range []struct {
		binding []byte
		token   string
	}{
		{nil, "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf3fed5e50dc8fc8fce2e962ddb13dbfae"},
		{bind, "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf36747d163099e42de649d7159d462334"},
	} {
		tok, _ := hex.DecodeString(x.token)
		v, err := NewPrefixAuthVerifier(a)
		if err != nil {
			t.Fatal(err)
		}
		v.now = func() time.Time { return at }
		if !v.Verify(tok, x.binding) {
			t.Fatalf("token %s does not verify", x.token)
		}
		if v.Verify(tok, x.binding) {
			t.Fatal("a token verified twice")
		}
	}
}

// TestPrefixAuthHeaderVectors checks the header form of the vectors of
// the prefix authenticator and that Decide takes an upgrade's token from
// its header field, once.
func TestPrefixAuthHeaderVectors(t *testing.T) {
	a := &PrefixAuth{Key: bytes.Repeat([]byte{0x6b}, 32), Slot: 30 * time.Second}
	for _, x := range []struct{ token, header string }{
		{"a0a1a2a3a4a5a6a7a8a9aaabacadaeaf3fed5e50dc8fc8fce2e962ddb13dbfae", "oKGio6SlpqeoqaqrrK2urz_tXlDcj8j84uli3bE9v64"},
		{"a0a1a2a3a4a5a6a7a8a9aaabacadaeaf36747d163099e42de649d7159d462334", "oKGio6SlpqeoqaqrrK2urzZ0fRYwmeQt5knXFZ1GIzQ"},
	} {
		tok, _ := hex.DecodeString(x.token)
		if got := base64.RawURLEncoding.EncodeToString(tok); got != x.header {
			t.Fatalf("header form %s, want %s", got, x.header)
		}
	}
	v, err := NewPrefixAuthVerifier(a)
	if err != nil {
		t.Fatal(err)
	}
	v.now = func() time.Time { return time.Unix(1767225600, 0) }
	info := &CarrierInfo{Head: true, Request: &http.Request{Header: http.Header{
		HeaderPrefixAuth: {"oKGio6SlpqeoqaqrrK2urz_tXlDcj8j84uli3bE9v64"}}}}
	if d := v.Decide(context.Background(), info, nil); d != (Decision{Verdict: VerdictAccept}) {
		t.Fatalf("unbound vector: %+v", d)
	}
	if d := v.Decide(context.Background(), info, nil); d.Verdict != VerdictFallback {
		t.Fatalf("replayed vector: %+v", d)
	}
}

// TestPrefixAuthHeader admits upgrades over TLS by their header field
// (Listener.AdmitUpgrade): a bound token of the request's connection
// is accepted once; a missing, repeated, malformed or non-canonical
// field, an unbound token, a token of another connection and a field of
// another name are not.
func TestPrefixAuthHeader(t *testing.T) {
	leakcheck.Check(t)
	auth := &PrefixAuth{Key: []byte("k"), Slot: 30 * time.Second, Skew: 1, BindTLS: true, HeaderField: "x-auth"}
	v, err := NewPrefixAuthVerifier(auth)
	if err != nil {
		t.Fatal(err)
	}
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Peek: PrefixTokenLen, Decide: v.Decide}})
	defer func() { _ = lst.Close() }()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a, ok := lst.AdmitUpgrade(w, r, nil); ok {
			a.Release()
			_, _ = io.WriteString(w, "accepted")
		}
	}))
	srvTLS, cliTLS := selfSignedTLS(t)
	srv.TLS = srvTLS
	srv.StartTLS()
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	dial := func() (*tls.Conn, *CarrierInfo) {
		c, err := tls.Dial("tcp", addr, cliTLS)
		if err != nil {
			t.Fatal(err)
		}
		cs := c.ConnectionState()
		return c, &CarrierInfo{TLS: &cs, Head: true}
	}
	header := func(info *CarrierInfo) string {
		name, val, err := auth.Header(info)
		if err != nil || name != "X-Auth" || len(val) != 43 {
			t.Fatalf("Header: %q %q %v", name, val, err)
		}
		return val
	}
	send := func(c *tls.Conn, h http.Header) string {
		req, _ := http.NewRequest(http.MethodGet, "https://localhost/", nil)
		req.Header = h
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if err := req.Write(c); err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(c), req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return string(b)
	}

	c, info := dial()
	val := header(info)
	if got := send(c, http.Header{"X-Auth": {val}}); got != "accepted" {
		t.Fatalf("valid token: %q", got)
	}
	// The same token again on its connection: the Nonce is recorded.
	if got := send(c, http.Header{"X-Auth": {val}}); got == "accepted" {
		t.Fatal("a replayed token was accepted")
	}
	other, otherInfo := dial()
	_ = other.Close()
	unbound := *auth
	unbound.BindTLS = false
	ub, _ := unbound.Prefix(nil)
	fresh := header(info)
	last := strings.IndexByte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_", fresh[42])
	nonCanonical := fresh[:42] + string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"[last|1])
	for name, h := range map[string]http.Header{
		"missing":       {},
		"repeated":      {"X-Auth": {header(info), header(info)}},
		"padded":        {"X-Auth": {header(info) + "="}},
		"short":         {"X-Auth": {header(info)[:42]}},
		"std alphabet":  {"X-Auth": {"+" + fresh[1:]}},
		"non-canonical": {"X-Auth": {nonCanonical}},
		"unbound":       {"X-Auth": {base64.RawURLEncoding.EncodeToString(ub)}},
		"other conn":    {"X-Auth": {header(otherInfo)}},
		"default name":  {HeaderPrefixAuth: {header(info)}},
	} {
		if got := send(c, h); got == "accepted" {
			t.Fatalf("%s: accepted", name)
		}
	}
	_ = c.Close()
}

// TestPrefixAuthWindow checks the slot window and the cache bound.
func TestPrefixAuthWindow(t *testing.T) {
	a := &PrefixAuth{Key: []byte("k"), Slot: 10 * time.Second, Skew: 1, CacheSize: 2}
	v, err := NewPrefixAuthVerifier(a)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1767225600, 0)
	mk := func(at time.Time) []byte {
		tok, err := a.token(nil, at)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	v.now = func() time.Time { return base }
	if v.Verify(mk(base.Add(-20*time.Second)), nil) || v.Verify(mk(base.Add(20*time.Second)), nil) {
		t.Fatal("a token two slots away verified")
	}
	if !v.Verify(mk(base.Add(-10*time.Second)), nil) || !v.Verify(mk(base.Add(10*time.Second)), nil) {
		t.Fatal("a token one slot away did not verify")
	}
	if v.Verify(mk(base), nil) {
		t.Fatal("a token verified with the cache full")
	}
	// Two slots later the old slot's entries are dropped.
	v.now = func() time.Time { return base.Add(20 * time.Second) }
	if !v.Verify(mk(base.Add(20*time.Second)), nil) {
		t.Fatal("expired entries were not dropped")
	}
}

func selfSignedTLS(t testing.TB) (srv, cli *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
		&tls.Config{RootCAs: pool, ServerName: "localhost"}
}

// firstRecord returns the bytes a client session sends first on a carrier.
func firstRecord(t *testing.T) []byte {
	t.Helper()
	c, s := net.Pipe()
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		conn, err := Start(ctx, NetPipe(c), RoleClient, mpConfig())
		if err == nil {
			defer func() { _ = conn.Close(); _ = conn.Wait() }()
		}
		<-ctx.Done()
	}()
	b := make([]byte, 4096)
	_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := s.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = c.Close()
	return b[:n]
}

// TestAdmissionStream sends a TLS listener with prefix tokens a valid
// carrier, its token presented by DialTLS, and carriers without a valid
// token: one of another carrier, a truncated one, random bytes, and a first record without a token. Each of
// the latter reaches the fallback with exactly the bytes sent, and nothing
// else is written; without a fallback they are stalled: no byte is
// written, and the connection ends with an orderly close.
func TestAdmissionStream(t *testing.T) {
	leakcheck.Check(t)
	srvTLS, cliTLS := selfSignedTLS(t)
	auth := &PrefixAuth{Key: []byte("k"), Slot: 30 * time.Second, Skew: 1, BindTLS: true}
	v, err := NewPrefixAuthVerifier(auth)
	if err != nil {
		t.Fatal(err)
	}
	adm := &Admission{Peek: PrefixTokenLen, PeekTimeout: 300 * time.Millisecond, Decide: v.Decide,
		StallMin: 50 * time.Millisecond, StallMax: 100 * time.Millisecond}
	got := make(chan []byte, 8)
	fallback := func(_ context.Context, c net.Conn) {
		b, _ := io.ReadAll(c)
		got <- b
		_, _ = c.Write([]byte("fallback"))
		_ = c.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var lst *Listener
	serve := func(fb func(context.Context, net.Conn)) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lst = NewListener(&ListenerConfig{Config: mpConfig(), Admission: adm, Fallback: fb})
		done := make(chan struct{})
		go func() { _ = lst.Serve(ctx, tls.NewListener(ln, srvTLS)); close(done) }()
		l := lst
		t.Cleanup(func() { _ = l.Close(); <-done })
		return ln.Addr().String()
	}
	dial := func(addr string) *tls.Conn {
		c, err := tls.Dial("tcp", addr, cliTLS)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	token := func(c *tls.Conn) []byte {
		cs := c.ConnectionState()
		tok, err := auth.Prefix(&CarrierInfo{TLS: &cs})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	addr := serve(fallback)

	// A valid carrier, its token presented by DialTLS.
	cfg := mpConfig()
	cfg.Credential = auth
	conn, err := DialTLS(ctx, "tcp", addr, cliTLS, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	_ = s.Close()
	waitDone(t, conn)
	waitDone(t, s)

	rec := firstRecord(t)
	random := make([]byte, 64)
	_, _ = rand.Read(random)
	other := dial(addr)
	valid := token(other)
	_ = other.Close()
	if b := <-got; len(b) != 0 { // the carrier closed without bytes
		t.Fatalf("fallback got %x", b)
	}
	cases := []struct {
		name string
		send []byte
	}{
		{"other carrier", valid},
		{"truncated", valid[:PrefixTokenLen/2]},
		{"random", random},
		{"record", rec},
	}
	for _, x := range cases {
		c := dial(addr)
		if _, err := c.Write(x.send); err != nil {
			t.Fatal(err)
		}
		_ = c.CloseWrite()
		resp, _ := io.ReadAll(c)
		_ = c.Close()
		if string(resp) != "fallback" {
			t.Fatalf("%s: response %q", x.name, resp)
		}
		if b := <-got; !bytes.Equal(b, x.send) {
			t.Fatalf("%s: fallback got %x, want %x", x.name, b, x.send)
		}
	}

	// No fallback: stalled.
	addr = serve(nil)
	for _, x := range cases {
		c := dial(addr)
		_, _ = c.Write(x.send)
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		start := time.Now()
		resp, err := io.ReadAll(c)
		_ = c.Close()
		if len(resp) != 0 || err != nil {
			t.Fatalf("%s: stalled carrier answered %x, %v", x.name, resp, err)
		}
		if d := time.Since(start); d < 40*time.Millisecond {
			t.Fatalf("%s: closed after %v", x.name, d)
		}
	}
}

// siteHandler is a host's own handler: every response it gives.
func siteHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "site "+r.Method+" "+hex.EncodeToString(b))
	})
}

type siteResp struct {
	code int
	ct   string
	body string
}

func doReq(t *testing.T, c *http.Client, method, url string, body []byte) siteResp {
	t.Helper()
	return doReqHeader(t, c, method, url, body, nil)
}

func doReqHeader(t *testing.T, c *http.Client, method, url string, body []byte, h http.Header) siteResp {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	addHeader(req.Header, h)
	if body == nil {
		req.Body = http.NoBody
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return siteResp{resp.StatusCode, resp.Header.Get("Content-Type"), string(b)}
}

// TestAdmissionHTTP sends the HTTP carrier handler requests without a
// valid token, bodiless ones with a missing, malformed, repeated or
// replayed Zmux-Prefix-Auth field: each is answered by Next exactly as
// Next answers it directly, with the body bytes the admission read
// replayed.
func TestAdmissionHTTP(t *testing.T) {
	leakcheck.Check(t)
	auth := &PrefixAuth{Key: []byte("k"), Slot: 30 * time.Second}
	v, err := NewPrefixAuthVerifier(auth)
	if err != nil {
		t.Fatal(err)
	}
	lst := NewListener(&ListenerConfig{Config: mpConfig(),
		Admission: &Admission{Peek: PrefixTokenLen, PeekTimeout: time.Second, Decide: v.Decide, StallMax: time.Second}})
	defer func() { _ = lst.Close() }()
	h, err := NewHTTPHandler(lst, &HTTPOptions{Next: siteHandler()})
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
	bodyless := func(v ...string) http.Header { return http.Header{HeaderPrefixAuth: v} }
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
	lst := NewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	h := newExHandler(t, lst, &HTTPExchangeConfig{PSK: []byte("k"), HoldTime: 50 * time.Millisecond,
		MaxClockSkew: 30 * time.Second, Next: siteHandler()})
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
		b := appendExReqHdr(nil, &cid, exKindClose, 0, 0, 0, ts, &nonce)
		mac := key.requestMAC(b, nil)
		return append(b, mac[:]...)
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

// TestStallDurationBySource checks that a stall's duration is in the
// interval and the same for every carrier from one source.
func TestStallDurationBySource(t *testing.T) {
	stall := func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictStall} }
	a := &Admission{Decide: stall, StallMin: time.Second, StallMax: 1000 * time.Second}
	lst := NewListener(&ListenerConfig{Admission: a})
	defer func() { _ = lst.Close() }()
	ap := netip.MustParseAddrPort
	for _, r := range []string{"192.0.2.1:1", "198.51.100.7:2", "[2001:db8::1]:3"} {
		d := lst.StallDuration(ap(r))
		if d < a.StallMin || d > a.StallMax {
			t.Fatalf("%s: %v outside the interval", r, d)
		}
		if d2 := lst.StallDuration(ap(r)); d2 != d {
			t.Fatalf("%s: %v then %v", r, d, d2)
		}
	}
	same := [][2]string{{"[2001:db8::1]:1", "[2001:db8::ffff:2]:9"}, {"[::ffff:192.0.2.1]:1", "192.0.2.1:2"}, {"192.0.2.1:1", "192.0.2.1:0"}}
	for _, p := range same {
		if x, y := lst.StallDuration(ap(p[0])), lst.StallDuration(ap(p[1])); x != y {
			t.Fatalf("%s %v, %s %v", p[0], x, p[1], y)
		}
	}
	fixed := NewListener(&ListenerConfig{Admission: &Admission{Decide: stall, StallMin: 5 * time.Second, StallMax: 5 * time.Second}})
	defer func() { _ = fixed.Close() }()
	if d := fixed.StallDuration(ap("192.0.2.1:1")); d != 5*time.Second {
		t.Fatalf("fixed interval: %v", d)
	}
	if d := NewListener(nil).StallDuration(ap("192.0.2.1:1")); d != 0 {
		t.Fatalf("without an admission: %v", d)
	}
}

// TestStallCountsPerSource checks that a source holds at most
// MaxPendingPerSource - 1 stalled carriers, that a carrier to be stalled
// beyond it is closed at once (StallOverflow), and that the source keeps
// a slot for a new attempt.
func TestStallCountsPerSource(t *testing.T) {
	leakcheck.Check(t)
	a := &Admission{Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictStall} },
		StallMin: time.Minute, StallMax: time.Minute}
	lst := NewListener(&ListenerConfig{MaxPendingPerSource: 3, Admission: a})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	src := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1}
	var clients []net.Conn
	done := make(chan struct{}, 3)
	for range 2 {
		c, s := net.Pipe()
		clients = append(clients, c)
		go func() { _ = lst.HandlePipe(ctx, NetPipe(addrConn{s, src}), JoinHint{}); done <- struct{}{} }()
	}
	waitUntil(t, 5*time.Second, "two stalled", func() bool { return lst.Stats().Admission.Stalled == 2 })
	// The third: the source has its share of stalled carriers.
	c, s := net.Pipe()
	go func() { _ = lst.HandlePipe(ctx, NetPipe(addrConn{s, src}), JoinHint{}); done <- struct{}{} }()
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read %v, want the end", err)
	}
	<-done
	_ = c.Close()
	if st := lst.Stats().Admission; st.StallOverflow != 1 || st.Stalled != 2 {
		t.Fatalf("stats %+v", st)
	}
	cancel()
	for _, c := range clients {
		_ = c.Close()
	}
	<-done
	<-done
}

// TestAdmitBeforeHandshake answers 425 to a request read before its TLS
// handshake completed (0-RTT), with or without an Admission, before any
// decision.
func TestAdmitBeforeHandshake(t *testing.T) {
	decided := false
	adm := &Admission{Decide: func(context.Context, *CarrierInfo, []byte) Decision {
		decided = true
		return Decision{Verdict: VerdictAccept}
	}}
	for _, a := range []*Admission{nil, adm} {
		lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: a})
		for _, upgrade := range []bool{false, true} {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
			r.TLS = &tls.ConnectionState{HandshakeComplete: false}
			w := httptest.NewRecorder()
			if _, ok := lst.admitRequest(w, r, nil, !upgrade); ok || w.Code != http.StatusTooEarly {
				t.Fatalf("admitted %v, status %d", ok, w.Code)
			}
		}
		// Each 425 is a setup failure and nothing else.
		if st := lst.Stats(); st != (ListenerStats{SetupFailures: 2}) {
			t.Fatalf("stats after two 425 answers: %+v", st)
		}
		_ = lst.Close()
	}
	if decided {
		t.Fatal("decided on an early request")
	}
	lst := NewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
	r.TLS = &tls.ConnectionState{HandshakeComplete: true}
	a, ok := lst.admitRequest(httptest.NewRecorder(), r, nil, true)
	if !ok {
		t.Fatal("refused a request after the handshake")
	}
	a.Release()
	// Any Early-Data field of value 1 marks the request, not the first only.
	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
	r.Header.Add("Early-Data", "0")
	r.Header.Add("Early-Data", "1")
	if w := httptest.NewRecorder(); func() bool { _, ok := lst.admitRequest(w, r, nil, true); return ok }() || w.Code != http.StatusTooEarly {
		t.Fatalf("second Early-Data field: status %d", w.Code)
	}
}

// addrConn is a net.Conn with a given remote address.
type addrConn struct {
	net.Conn
	remote net.Addr
}

func (c addrConn) RemoteAddr() net.Addr { return c.remote }

// TestServeConnSource counts a carrier accepted with prefix bytes left
// over against its own remote host: two silent carriers from two hosts
// each hold a pending slot under MaxPendingPerSource 1.
func TestServeConnSource(t *testing.T) {
	leakcheck.Check(t)
	a := &Admission{Peek: 2, Decide: func(context.Context, *CarrierInfo, []byte) Decision {
		return Decision{Verdict: VerdictAccept, Skip: 1}
	}}
	lst := NewListener(&ListenerConfig{Config: mpConfig(), MaxPendingPerSource: 1, FirstRecordTimeout: 10 * time.Second, Admission: a})
	ctx, cancel := context.WithCancel(context.Background())
	var clients []net.Conn
	done := make(chan struct{}, 2)
	for _, host := range []string{"192.0.2.1", "192.0.2.2"} {
		c, s := net.Pipe()
		clients = append(clients, c)
		go func() {
			_ = lst.HandlePipe(ctx, NetPipe(addrConn{s, &net.TCPAddr{IP: net.ParseIP(host), Port: 1}}), JoinHint{})
			done <- struct{}{}
		}()
		if _, err := c.Write([]byte{0, 0x40}); err != nil {
			t.Fatal(err)
		}
	}
	// The leftover byte starts a record; neither carrier is refused.
	for i, c := range clients {
		_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		_, err := c.Read(make([]byte, 1))
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("carrier %d: %v, want a timeout", i, err)
		}
	}
	cancel()
	_ = lst.Close()
	for _, c := range clients {
		_ = c.Close()
	}
	<-done
	<-done
}

// TestCloseEndsFallbackStall: a carrier stalled for a fallback without
// target, under a caller context that never ends, ends when the listener
// closes.
func TestCloseEndsFallbackStall(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Peek: 4, PeekTimeout: 5 * time.Second,
		StallMin: time.Minute, StallMax: time.Minute,
		Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictFallback} }}})
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	done := make(chan struct{})
	go func() { _ = lst.HandlePipe(context.Background(), NetPipe(b), JoinHint{}); close(done) }()
	if _, err := a.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(5 * time.Second); lst.Stats().Admission.Fallback != 1; time.Sleep(time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("stats %+v", lst.Stats())
		}
	}
	go func() { _, _ = io.Copy(io.Discard, a) }()
	_ = lst.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the stall outlived Listener.Close")
	}
}
