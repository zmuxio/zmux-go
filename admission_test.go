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
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/internal/testcert"
)

func selfSignedTLS(t testing.TB) (srv, cli *tls.Config) {
	t.Helper()
	cert, trust := testcert.Local(t)
	return &tls.Config{Certificates: []tls.Certificate{cert}},
		&tls.Config{RootCAs: trust, ServerName: testcert.LocalName}
}

// TestStallDurationBySource checks that a stall's duration is in the
// interval and the same for every carrier from one source.
func TestStallDurationBySource(t *testing.T) {
	stall := func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictStall} }
	a := &Admission{Decide: stall, StallMin: time.Second, StallMax: 1000 * time.Second}
	lst := mustNewListener(&ListenerConfig{Admission: a})
	defer func() { _ = lst.Close() }()
	ap := netip.MustParseAddrPort
	for _, r := range []string{"192.0.2.1:1", "198.51.100.7:2", "[2001:db8::1]:3"} {
		d := lst.Adapter().StallDuration(ap(r))
		if d < a.StallMin || d > a.StallMax {
			t.Fatalf("%s: %v outside the interval", r, d)
		}
		if d2 := lst.Adapter().StallDuration(ap(r)); d2 != d {
			t.Fatalf("%s: %v then %v", r, d, d2)
		}
	}
	same := [][2]string{{"[2001:db8::1]:1", "[2001:db8::ffff:2]:9"}, {"[::ffff:192.0.2.1]:1", "192.0.2.1:2"}, {"192.0.2.1:1", "192.0.2.1:0"}}
	for _, p := range same {
		if x, y := lst.Adapter().StallDuration(ap(p[0])), lst.Adapter().StallDuration(ap(p[1])); x != y {
			t.Fatalf("%s %v, %s %v", p[0], x, p[1], y)
		}
	}
	fixed := mustNewListener(&ListenerConfig{Admission: &Admission{Decide: stall, StallMin: 5 * time.Second, StallMax: 5 * time.Second}})
	defer func() { _ = fixed.Close() }()
	if d := fixed.Adapter().StallDuration(ap("192.0.2.1:1")); d != 5*time.Second {
		t.Fatalf("fixed interval: %v", d)
	}
	none := mustNewListener(nil)
	defer func() { _ = none.Close() }()
	if d := none.Adapter().StallDuration(ap("192.0.2.1:1")); d != 0 {
		t.Fatalf("without an admission: %v", d)
	}
}

// TestStallCountsPerSource checks that a source holds at most
// MaxPendingPerSource - 1 stalled carriers, that a carrier to be stalled
// beyond it is closed at once (StallOverflow), and that the source keeps
// a slot for a new attempt.
func TestStallCountsPerSource(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		a := &Admission{Decide: func(context.Context, CarrierInfo, []byte) Decision {
			return Decision{Verdict: VerdictFallback, Variant: "targetless"}
		},
			StallMin: time.Minute, StallMax: time.Minute}
		lst := mustNewListener(&ListenerConfig{MaxPendingPerSource: 3, Admission: a})
		defer func() { _ = lst.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		src := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1}
		clients := make([]net.Conn, 0, 2)
		done := make(chan struct{}, 3)
		for range 2 {
			c, s := net.Pipe()
			clients = append(clients, c)
			go func() { _ = lst.HandlePipe(ctx, NetPipe(addrConn{s, src}), PipeHint{}); done <- struct{}{} }()
		}
		synctest.Wait()
		if n := lst.Stats().Admission.Stalled; n != 2 {
			t.Fatalf("%d carriers stalled, want 2", n)
		}
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
		if got := lst.VariantStats(); len(got) != 1 || got[0] != (VariantStat{Name: "targetless", Stalled: 3}) {
			t.Fatalf("variants %+v", got)
		}
		cancel()
		for _, c := range clients {
			_ = c.Close()
		}
		<-done
		<-done
	})
}

// TestAdmitBeforeHandshake answers 425 to a request read before its TLS
// handshake completed (0-RTT), with or without an Admission, before any
// decision.
func TestAdmitBeforeHandshake(t *testing.T) {
	decided := false
	adm := &Admission{Decide: func(context.Context, CarrierInfo, []byte) Decision {
		decided = true
		return Decision{Verdict: VerdictAccept}
	}}
	for _, a := range []*Admission{nil, adm} {
		lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: a})
		for _, upgrade := range []bool{false, true} {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
			r.TLS = &tls.ConnectionState{HandshakeComplete: false}
			w := httptest.NewRecorder()
			if _, ok := lst.admitRequest(w, r, nil, map[bool]reqKind{false: reqBody, true: reqUpgrade}[upgrade], nil); ok || w.Code != http.StatusTooEarly {
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
	a, ok := lst.admitRequest(httptest.NewRecorder(), r, nil, reqBody, nil)
	if !ok {
		t.Fatal("refused a request after the handshake")
	}
	a.Release()
	// Any Early-Data field of value 1 marks the request, not the first only.
	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
	r.Header.Add("Early-Data", "0")
	r.Header.Add("Early-Data", "1")
	if w := httptest.NewRecorder(); func() bool { _, ok := lst.admitRequest(w, r, nil, reqBody, nil); return ok }() || w.Code != http.StatusTooEarly {
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
// over against its own remote host: two silent carriers from each of two hosts
// fill that source's slots without limiting the other one.
func TestServeConnSource(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		a := &Admission{Peek: 2, Decide: func(context.Context, CarrierInfo, []byte) Decision {
			return Decision{Verdict: VerdictAccept, Skip: 1}
		}}
		lst := mustNewListener(&ListenerConfig{Config: mpConfig(), MaxPendingPerSource: 2, FirstRecordTimeout: 10 * time.Second, Admission: a})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		defer func() { _ = lst.Close() }()
		hosts := []string{"192.0.2.1", "192.0.2.1", "192.0.2.2", "192.0.2.2"}
		done := make(chan struct{}, len(hosts))
		for _, host := range hosts {
			c, s := net.Pipe()
			t.Cleanup(func() { _ = c.Close() })
			go func() {
				_ = lst.HandlePipe(ctx, NetPipe(addrConn{s, &net.TCPAddr{IP: net.ParseIP(host), Port: 1}}), PipeHint{})
				done <- struct{}{}
			}()
			if _, err := c.Write([]byte{0, 0x40}); err != nil {
				t.Fatal(err)
			}
		}
		// All carriers are blocked reading the record started by the leftover byte.
		synctest.Wait()
		lst.pmu.Lock()
		pending := lst.pending.Total()
		lst.pmu.Unlock()
		if pending != len(hosts) || len(done) != 0 {
			t.Fatalf("pending carriers = %d, completed = %d", pending, len(done))
		}
		cancel()
		_ = lst.Close()

		for range hosts {
			<-done
		}
	})
}

// TestCloseEndsFallbackStall: a carrier stalled for a fallback without
// target, under a caller context that never ends, ends when the listener
// closes.
func TestCloseEndsFallbackStall(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Peek: 4, PeekTimeout: 5 * time.Second,
			StallMin: time.Minute, StallMax: time.Minute,
			Decide: func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictFallback} }}})
		a, b := net.Pipe()
		defer func() { _ = a.Close() }()
		done := make(chan struct{})
		go func() { _ = lst.HandlePipe(context.Background(), NetPipe(b), PipeHint{}); close(done) }()
		if _, err := a.Write([]byte("abcd")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if lst.Stats().Admission.Stalled != 1 {
			t.Fatalf("stats %+v", lst.Stats())
		}
		peerClosed := make(chan struct{})
		go func() { _, _ = io.Copy(io.Discard, a); close(peerClosed) }()
		_ = lst.Close()
		<-done
		<-peerClosed
	})
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
	stall := func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictStall} }
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
			ar, err := lst.Adapter().Arrive(ap(s), netip.AddrPort{})
			if err != nil {
				t.Fatalf("%s: %v", s, err)
			}
			held = append(held, ar)
		}
		if _, err := lst.Adapter().Arrive(ap(g[2]), netip.AddrPort{}); err == nil {
			t.Fatalf("%s: a third source of a full prefix was let in", g[2])
		}
		if x, y := lst.Adapter().StallDuration(ap(g[0])), lst.Adapter().StallDuration(ap(g[2])); x != y {
			t.Fatalf("stall durations in one prefix: %v %v", x, y)
		}
		for _, ar := range held {
			ar.Release()
		}
	}
	for _, p := range [][2]string{{"192.0.2.1:1", "192.0.3.1:1"}, {"[2001:db8:0:1::1]:1", "[2001:db8:0:100::1]:1"}} {
		held := make([]*Arrival, 0, 3)
		for range 2 {
			ar, err := lst.Adapter().Arrive(ap(p[0]), netip.AddrPort{})
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, ar)
		}
		ar, err := lst.Adapter().Arrive(ap(p[1]), netip.AddrPort{})
		if err != nil {
			t.Fatalf("%s shares the full prefix of %s: %v", p[1], p[0], err)
		}
		held = append(held, ar)
		if x, y := lst.Adapter().StallDuration(ap(p[0])), lst.Adapter().StallDuration(ap(p[1])); x == y {
			t.Fatalf("one stall duration %v in different prefixes %s %s", x, p[0], p[1])
		}
		for _, ar := range held {
			ar.Release()
		}
	}
}
