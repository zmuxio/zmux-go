package admit

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
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
	info := &zmux.CarrierInfo{Head: true, Request: &http.Request{Header: http.Header{
		HeaderPrefixAuth: {"oKGio6SlpqeoqaqrrK2urz_tXlDcj8j84uli3bE9v64"}}}}
	if d := v.Decide(context.Background(), info, nil); d != (zmux.Decision{Verdict: zmux.VerdictAccept}) {
		t.Fatalf("unbound vector: %+v", d)
	}
	if d := v.Decide(context.Background(), info, nil); d.Verdict != zmux.VerdictFallback {
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
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: &zmux.Admission{Peek: PrefixTokenLen, Decide: v.Decide}})
	defer func() { _ = lst.Close() }()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a, ok := lst.AdmitUpgrade(w, r, nil, nil); ok {
			a.Release()
			_, _ = io.WriteString(w, "accepted")
		}
	}))
	srvTLS, cliTLS := selfSignedTLS(t)
	srv.TLS = srvTLS
	srv.StartTLS()
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	dial := func() (*tls.Conn, *zmux.CarrierInfo) {
		c, err := tls.Dial("tcp", addr, cliTLS)
		if err != nil {
			t.Fatal(err)
		}
		cs := c.ConnectionState()
		return c, &zmux.CarrierInfo{TLS: &cs, Head: true}
	}
	header := func(info *zmux.CarrierInfo) string {
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
	adm := &zmux.Admission{Peek: PrefixTokenLen, PeekTimeout: 300 * time.Millisecond, Decide: v.Decide,
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
	var lst *zmux.Listener
	serve := func(fb func(context.Context, net.Conn)) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lst = mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm, Fallback: fb})
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
		tok, err := auth.Prefix(&zmux.CarrierInfo{TLS: &cs})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	addr := serve(fallback)

	// A valid carrier, its token presented by DialTLS.
	cfg := mpConfig()
	cfg.CarrierCredential = auth
	conn, err := zmux.DialTLS(ctx, "tcp", addr, cliTLS, cfg)
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
