package zmux

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"net"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestListenerStats checks each outcome count of a listener.
func TestListenerStats(t *testing.T) {
	leakcheck.Check(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lst := NewListener(&ListenerConfig{FirstRecordTimeout: 100 * time.Millisecond, Config: mpConfig()})
	done := make(chan struct{})
	go func() { _ = lst.Serve(ctx, ln); close(done) }()
	defer func() { cancel(); _ = lst.Close(); <-done }()
	dial := func() net.Conn {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		return nc
	}
	waitStats := func(what string, ok func(ListenerStats) bool) {
		t.Helper()
		waitUntil(t, 5*time.Second, what, func() bool { return ok(lst.Stats()) })
	}

	// A session.
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()
	c, err := Client(dctx, dial(), mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	sc, err := lst.Accept(dctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = sc.Close(); waitDone(t, c); waitDone(t, sc) }()
	waitStats("a session", func(s ListenerStats) bool { return s.Sessions == 1 })

	// A join.
	if _, err := c.AddPipe(dctx, NetPipe(dial()), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	waitStats("a join", func(s ListenerStats) bool { return s.Joins == 1 })

	// Not zmux at all.
	nc := dial()
	_, _ = nc.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	waitStats("a rejection", func(s ListenerStats) bool { return s.Rejected == 1 })
	_ = nc.Close()

	// No first record.
	nc = dial()
	waitStats("a timeout", func(s ListenerStats) bool { return s.TimedOut == 1 })
	_ = nc.Close()

	// A TLS handshake that fails.
	srvTLS, _ := selfSignedTLS(t)
	a1, a2 := net.Pipe()
	served := make(chan struct{})
	go func() { _ = lst.HandlePipe(ctx, NetPipe(tls.Server(a2, srvTLS)), JoinHint{}); close(served) }()
	_, _ = a1.Write([]byte("not a client hello"))
	_ = a1.Close()
	<-served
	waitStats("a setup failure", func(s ListenerStats) bool { return s.SetupFailures == 1 })

	// After Close every carrier is refused.
	_ = lst.Close()
	r1, r2 := net.Pipe()
	defer func() { _ = r2.Close() }()
	if err := lst.HandlePipe(ctx, NetPipe(r1), JoinHint{}); err == nil {
		t.Fatal("a closed listener took a carrier")
	}
	got := lst.Stats()
	want := ListenerStats{Sessions: 1, Joins: 1, SetupFailures: 1, Rejected: 1, Refused: 1, TimedOut: 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
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
	lst := NewListener(&ListenerConfig{FirstRecordTimeout: 100 * time.Millisecond, Config: scfg})
	done := make(chan struct{})
	go func() { _ = lst.Serve(ctx, ln); close(done) }()
	defer func() { cancel(); _ = lst.Close(); <-done }()
	hh, err := NewHTTPHandler(lst, nil)
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
	sum := func(s ListenerStats) uint64 { return s.Sessions + s.Joins + s.Rejected + s.Refused + s.TimedOut }
	var inputs uint64
	expect := func(what string, n uint64) {
		t.Helper()
		inputs += n
		waitUntil(t, 5*time.Second, what, func() bool { return sum(lst.Stats()) == inputs })
	}
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()

	// A session and a join over TCP.
	c, err := Client(dctx, dial(), mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	sc, err := lst.Accept(dctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = sc.Close(); waitDone(t, c); waitDone(t, sc) }()
	expect("a session", 1)
	if _, err := c.AddPipe(dctx, NetPipe(dial()), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	expect("a join", 1)

	// A split bootstrap: two carriers, one input.
	sp, err := HTTPSplitPipe(dctx, srv.Client(), srv.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := Start(dctx, sp, RoleClient, mpConfig())
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
		_ = lst.HandlePipe(ctx, PipeIO{R: h1, Carrier: CarrierSplit}, JoinHint{Pair: tok, Uplink: true})
	}()
	expect("an unpaired half", 1)
	_ = h2.Close()

	// Admission verdicts other than accept count in Admission alone.
	for _, v := range []Verdict{VerdictClose, VerdictFallback, VerdictStall} {
		adm := &Admission{StallMin: time.Millisecond, StallMax: 10 * time.Millisecond,
			Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: v} }}
		al := NewListener(&ListenerConfig{Config: mpConfig(), Admission: adm,
			Fallback: func(_ context.Context, c net.Conn) { _ = c.Close() }})
		a1, a2 := net.Pipe()
		_ = al.HandlePipe(ctx, NetPipe(a2), JoinHint{})
		_ = a1.Close()
		got := al.Stats()
		want := map[Verdict]AdmissionStats{VerdictClose: {Closed: 1}, VerdictFallback: {Fallback: 1}, VerdictStall: {Stalled: 1}}[v]
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
	var none *Listener
	none.CountSetupFailure() // a nil listener does nothing

	// After Close the carrier is refused.
	_ = lst.Close()
	r1, r2 := net.Pipe()
	defer func() { _ = r2.Close() }()
	_ = lst.HandlePipe(ctx, NetPipe(r1), JoinHint{})
	expect("a refusal", 1)

	got := lst.Stats()
	want := ListenerStats{Sessions: 2, Joins: 1, Rejected: 1, Refused: 1, TimedOut: 2, SetupFailures: 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}

// earlyHalf is a server carrier half that took early data and whose
// handshake never completes.
type earlyHalf struct{ net.Conn }

func (earlyHalf) UsedEarlyData() bool                { return true }
func (earlyHalf) HandshakeComplete() <-chan struct{} { return nil }

// TestEarlyJoinWaitCancelled checks that a join sent in early data whose
// wait for the handshake is cancelled counts as a setup failure, and
// nowhere else.
func TestEarlyJoinWaitCancelled(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	before := e.lst.Stats()
	cli, srv := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _, _ = e.client.AddPipe(ctx, NetPipe(cli), PipeOptions{}) }()
	hctx, hcancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- e.lst.HandlePipe(hctx, PipeIO{R: srv, W: earlyHalf{srv}}, JoinHint{}) }()
	// The join waits for the handshake: nothing is counted yet.
	time.Sleep(50 * time.Millisecond)
	if got := e.lst.Stats(); !reflect.DeepEqual(got, before) {
		t.Fatalf("counted while waiting: %+v", got)
	}
	hcancel()
	if err := <-done; err == nil {
		t.Fatal("a join whose handshake wait was cancelled was taken")
	}
	want := before
	want.SetupFailures++
	if got := e.lst.Stats(); !reflect.DeepEqual(got, want) {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}
