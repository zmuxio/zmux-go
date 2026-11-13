package zmux

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

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

// TestStallDurationBySource checks that a stall's duration is in the
// interval and the same for every carrier from one source.
func TestStallDurationBySource(t *testing.T) {
	stall := func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictStall} }
	a := &Admission{Decide: stall, StallMin: time.Second, StallMax: 1000 * time.Second}
	lst := mustNewListener(&ListenerConfig{Admission: a})
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
	fixed := mustNewListener(&ListenerConfig{Admission: &Admission{Decide: stall, StallMin: 5 * time.Second, StallMax: 5 * time.Second}})
	defer func() { _ = fixed.Close() }()
	if d := fixed.StallDuration(ap("192.0.2.1:1")); d != 5*time.Second {
		t.Fatalf("fixed interval: %v", d)
	}
	none := mustNewListener(nil)
	defer func() { _ = none.Close() }()
	if d := none.StallDuration(ap("192.0.2.1:1")); d != 0 {
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
	lst := mustNewListener(&ListenerConfig{MaxPendingPerSource: 3, Admission: a})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	src := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1}
	var clients []net.Conn
	done := make(chan struct{}, 3)
	for range 2 {
		c, s := net.Pipe()
		clients = append(clients, c)
		go func() { _ = lst.HandlePipe(ctx, NetPipe(addrConn{s, src}), PipeHint{}); done <- struct{}{} }()
	}
	waitUntil(t, 5*time.Second, "two stalled", func() bool { return lst.Stats().Admission.Stalled == 2 })
	// The third: the source has its share of stalled carriers.
	c, s := net.Pipe()
	go func() { _ = lst.HandlePipe(ctx, NetPipe(addrConn{s, src}), PipeHint{}); done <- struct{}{} }()
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
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
		lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: a})
		for _, upgrade := range []bool{false, true} {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
			r.TLS = &tls.ConnectionState{HandshakeComplete: false}
			w := httptest.NewRecorder()
			if _, ok := lst.admitRequest(w, r, nil, !upgrade, nil); ok || w.Code != http.StatusTooEarly {
				t.Fatalf("admitted %v, status %d", ok, w.Code)
			}
		}
		// Each 425 is a setup failure and nothing else.
		if st := lst.Stats(); !reflect.DeepEqual(st, ListenerStats{SetupFailures: 2}) {
			t.Fatalf("stats after two 425 answers: %+v", st)
		}
		_ = lst.Close()
	}
	if decided {
		t.Fatal("decided on an early request")
	}
	lst := mustNewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
	r.TLS = &tls.ConnectionState{HandshakeComplete: true}
	a, ok := lst.admitRequest(httptest.NewRecorder(), r, nil, true, nil)
	if !ok {
		t.Fatal("refused a request after the handshake")
	}
	a.Release()
	// Any Early-Data field of value 1 marks the request, not the first only.
	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
	r.Header.Add("Early-Data", "0")
	r.Header.Add("Early-Data", "1")
	if w := httptest.NewRecorder(); func() bool { _, ok := lst.admitRequest(w, r, nil, true, nil); return ok }() || w.Code != http.StatusTooEarly {
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
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), MaxPendingPerSource: 1, FirstRecordTimeout: 10 * time.Second, Admission: a})
	ctx, cancel := context.WithCancel(context.Background())
	var clients []net.Conn
	done := make(chan struct{}, 2)
	for _, host := range []string{"192.0.2.1", "192.0.2.2"} {
		c, s := net.Pipe()
		clients = append(clients, c)
		go func() {
			_ = lst.HandlePipe(ctx, NetPipe(addrConn{s, &net.TCPAddr{IP: net.ParseIP(host), Port: 1}}), PipeHint{})
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
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Peek: 4, PeekTimeout: 5 * time.Second,
		StallMin: time.Minute, StallMax: time.Minute,
		Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictFallback} }}})
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	done := make(chan struct{})
	go func() { _ = lst.HandlePipe(context.Background(), NetPipe(b), PipeHint{}); close(done) }()
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

// TestSourcePrefixes checks the source aggregation of
// ListenerConfig.SourcePrefixV4 and SourcePrefixV6: sources in one prefix
// share the per-source slots and the stall duration, sources in different
// prefixes do not, an IPv4-mapped address counts as IPv4, and a prefix
// length beyond its range is the configuration error.
func TestSourcePrefixes(t *testing.T) {
	for _, lc := range []ListenerConfig{{SourcePrefixV4: 33}, {SourcePrefixV4: -1}, {SourcePrefixV6: 129}, {SourcePrefixV6: -1}} {
		if _, err := NewListener(&lc); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: %v", lc, err)
		}
	}
	stall := func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictStall} }
	a := &Admission{Decide: stall, StallMin: time.Second, StallMax: 1 << 40}
	lst := mustNewListener(&ListenerConfig{MaxPendingPerSource: 2, SourcePrefixV4: 24, SourcePrefixV6: 56, Admission: a})
	defer func() { _ = lst.Close() }()
	ap := netip.MustParseAddrPort
	for _, g := range [][3]string{
		// two sources of one prefix, a third of the same, one of another
		{"192.0.2.1:1", "[::ffff:192.0.2.200]:2", "192.0.2.77:3"},
		{"[2001:db8:0:1::1]:1", "[2001:db8:0:ff::2]:2", "[2001:db8:0:42::3]:3"},
	} {
		var held []*Arrival
		for _, s := range g[:2] {
			ar, err := lst.Arrive(ap(s), netip.AddrPort{})
			if err != nil {
				t.Fatalf("%s: %v", s, err)
			}
			held = append(held, ar)
		}
		if _, err := lst.Arrive(ap(g[2]), netip.AddrPort{}); err == nil {
			t.Fatalf("%s: a third source of a full prefix was let in", g[2])
		}
		if x, y := lst.StallDuration(ap(g[0])), lst.StallDuration(ap(g[2])); x != y {
			t.Fatalf("stall durations in one prefix: %v %v", x, y)
		}
		for _, ar := range held {
			ar.Release()
		}
	}
	for _, p := range [][2]string{{"192.0.2.1:1", "192.0.3.1:1"}, {"[2001:db8:0:1::1]:1", "[2001:db8:0:100::1]:1"}} {
		var held []*Arrival
		for range 2 {
			ar, err := lst.Arrive(ap(p[0]), netip.AddrPort{})
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, ar)
		}
		ar, err := lst.Arrive(ap(p[1]), netip.AddrPort{})
		if err != nil {
			t.Fatalf("%s shares the full prefix of %s: %v", p[1], p[0], err)
		}
		held = append(held, ar)
		if x, y := lst.StallDuration(ap(p[0])), lst.StallDuration(ap(p[1])); x == y {
			t.Fatalf("one stall duration %v in different prefixes %s %s", x, p[0], p[1])
		}
		for _, ar := range held {
			ar.Release()
		}
	}
}
