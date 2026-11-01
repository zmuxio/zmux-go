package zmux

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/internal/testproxy"
)

var modePSK = bytes.Repeat([]byte{5}, 32)

// modeEnv is a unified endpoint (newUnifiedEnv) with an accepting loop and
// a client HTTP transport.
type modeEnv struct {
	*unifiedEnv
	tr       *http.Transport
	hc       *http.Client
	sessions chan *Conn
	stop     context.CancelFunc
	done     chan struct{}
}

func newModeEnv(t *testing.T) *modeEnv {
	t.Helper()
	e := &modeEnv{unifiedEnv: newUnifiedEnv(t, nil, nil), tr: &http.Transport{MaxIdleConnsPerHost: 16},
		sessions: make(chan *Conn, 16), done: make(chan struct{})}
	e.hc = &http.Client{Transport: e.tr}
	ctx, cancel := context.WithCancel(context.Background())
	e.stop = cancel
	go func() {
		defer close(e.done)
		for {
			s, err := e.lst.Accept(ctx)
			if err != nil {
				return
			}
			select {
			case e.sessions <- s:
			case <-ctx.Done():
				_ = s.Close()
				return
			}
		}
	}()
	return e
}

func (e *modeEnv) close() {
	e.stop()
	<-e.done
	e.unifiedEnv.close()
	close(e.sessions)
	for s := range e.sessions {
		_ = s.Close()
	}
	e.tr.CloseIdleConnections()
}

// candidates are the HTTP kinds to url, by preference.
func (e *modeEnv) candidates(url string) []Candidate {
	return []Candidate{
		StreamingCandidate(e.hc, url, nil),
		SplitCandidate(e.hc, url, nil),
		ExchangeCandidate(e.hc, url, &HTTPExchangeConfig{PSK: modePSK}),
	}
}

func modeCfg() *Config {
	c := mpConfig()
	c.HandshakeTimeout = 3 * time.Second
	return c
}

func fastPolicy(noJoin bool) *ModePolicy {
	return &ModePolicy{TimerMin: 400 * time.Millisecond, TimerMax: time.Second, Settle: 200 * time.Millisecond,
		NoJoin: noJoin}
}

// kinds lists the kinds and outcomes of results.
func kinds(rs []CandidateResult) []string {
	var s []string
	for _, r := range rs {
		k := r.Kind.String() + ":" + r.Outcome.String()
		if r.Joined {
			k += "(join)"
		}
		s = append(s, k)
	}
	return s
}

// closeConn closes a client session and waits for its end.
func closeConn(t *testing.T, c *Conn) {
	_ = c.Close()
	waitDone(t, c)
}

// waitCarrier waits until the session's active kind is k.
func waitCarrier(t *testing.T, c *Conn, k CarrierKind, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for c.ConnectionStats().Carrier != k {
		if time.Now().After(deadline) {
			st := c.ConnectionStats()
			t.Fatalf("carrier %v, want %v; tries %v", st.Carrier, k, kinds(st.CarrierCandidates))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestDialModeProfiles dials in auto mode through each proxy profile and
// directly, without joins: the most preferred kind that passes is chosen,
// and the tries are reported as events and statistics.
func TestDialModeProfiles(t *testing.T) {
	leakcheck.Check(t)
	e := newModeEnv(t)
	defer e.close()
	for _, c := range []struct {
		name    string
		profile *testproxy.Profile
		want    CarrierKind
		tries   int
	}{
		{"direct", nil, CarrierStreaming, 1},
		{"pass-through", &testproxy.Profile{}, CarrierStreaming, 1},
		{"nginx defaults", new(testproxy.NginxDefaults()), CarrierExchange, 3},
		{"buffering cdn", new(testproxy.BufferingCDN(3 * time.Second)), CarrierExchange, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			url := e.srv.URL
			if c.profile != nil {
				px := testproxy.New(e.srv.URL, *c.profile)
				defer px.Close()
				url = px.URL
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			conn, err := DialMode(ctx, AutoMode(e.candidates(url)...), modeCfg(), fastPolicy(true))
			if err != nil {
				t.Fatal(err)
			}
			defer closeConn(t, conn)
			st := conn.ConnectionStats()
			if st.Carrier != c.want || len(st.CarrierCandidates) != c.tries {
				t.Fatalf("chose %v after %v, want %v after %d tries", st.Carrier, kinds(st.CarrierCandidates), c.want, c.tries)
			}
			for i, r := range st.CarrierCandidates {
				if (r.Outcome == CandidateUsable) != (i == len(st.CarrierCandidates)-1) {
					t.Fatalf("tries %v", kinds(st.CarrierCandidates))
				}
			}
			// The events: one per try, then the choice.
			for i := 0; i <= c.tries; i++ {
				ev, err := conn.NextEvent(ctx)
				for err == nil && ev.Kind != EventCarrierCandidate && ev.Kind != EventCarrierChosen {
					ev, err = conn.NextEvent(ctx)
				}
				if err != nil {
					t.Fatal(err)
				}
				switch {
				case i < c.tries && (ev.Kind != EventCarrierCandidate || ev.Carrier != st.CarrierCandidates[i].Kind ||
					ev.Outcome != st.CarrierCandidates[i].Outcome):
					t.Fatalf("event %d: %+v", i, ev)
				case i == c.tries && (ev.Kind != EventCarrierChosen || ev.Carrier != c.want):
					t.Fatalf("choice event: %+v", ev)
				}
			}
			s := <-e.sessions
			defer func() { _ = s.Close() }()
			echoOnce(t, ctx, conn, s)
		})
	}
}

// TestDialModeExplicit fails an explicit kind that the path does not pass,
// without trying another.
func TestDialModeExplicit(t *testing.T) {
	leakcheck.Check(t)
	e := newModeEnv(t)
	defer e.close()
	px := testproxy.New(e.srv.URL, testproxy.NginxDefaults())
	defer px.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := modeCfg()
	cfg.HandshakeTimeout = time.Second
	_, err := DialMode(ctx, ExplicitMode(StreamingCandidate(e.hc, px.URL, nil)), cfg, nil)
	var me *ModeError
	if !errors.As(err, &me) || len(me.Results) != 1 || me.Results[0].Kind != CarrierStreaming ||
		me.Results[0].Outcome != CandidateTimedOut || !errors.Is(err, ErrHandshakeTimeout) {
		t.Fatalf("explicit streaming through a buffering proxy: %v", err)
	}
	// A WebSocket-only front refuses the request.
	ws := testproxy.New(e.srv.URL, testproxy.WebSocketFront())
	defer ws.Close()
	_, err = DialMode(ctx, ExplicitMode(ExchangeCandidate(e.hc, ws.URL, &HTTPExchangeConfig{PSK: modePSK})), cfg, nil)
	if !errors.As(err, &me) || len(me.Results) != 1 || me.Results[0].Kind != CarrierExchange {
		t.Fatalf("explicit exchange through a WebSocket front: %v", err)
	}
	// The exchange carrier repeats refused requests until its deadline.
	if o := me.Results[0].Outcome; o != CandidateTimedOut && o != CandidateFailed {
		t.Fatalf("outcome %v", o)
	}
	// Explicit and passing: no other kind is tried.
	c, err := DialMode(ctx, ExplicitMode(ExchangeCandidate(e.hc, px.URL, &HTTPExchangeConfig{PSK: modePSK})), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != CarrierExchange || len(st.CarrierCandidates) != 1 {
		t.Fatalf("explicit exchange: %v %v", st.Carrier, kinds(st.CarrierCandidates))
	}
	closeConn(t, c)
}

// TestDialModeCache skips the kinds a shared cache holds as failed, and
// counts a kind whose carrier drops soon after it was chosen as failed
// (sticky downgrade).
func TestDialModeCache(t *testing.T) {
	leakcheck.Check(t)
	e := newModeEnv(t)
	defer e.close()
	cache := &CarrierCache{}
	pol := fastPolicy(true)
	pol.Cache, pol.Key = cache, "server|lan"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nginx := testproxy.New(e.srv.URL, testproxy.NginxDefaults())
	defer nginx.Close()
	c, err := DialMode(ctx, AutoMode(e.candidates(nginx.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != CarrierExchange || len(st.CarrierCandidates) != 3 {
		t.Fatalf("first dial: %v", kinds(st.CarrierCandidates))
	}
	closeConn(t, c)
	c, err = DialMode(ctx, AutoMode(e.candidates(nginx.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != CarrierExchange || len(st.CarrierCandidates) != 1 {
		t.Fatalf("second dial tried %v; the failed kinds are cached", kinds(st.CarrierCandidates))
	}
	closeConn(t, c)

	// A path that lets streaming through, then drops it.
	pass := testproxy.New(e.srv.URL, testproxy.Profile{})
	defer pass.Close()
	pol.Key = "server|other-lan"
	c, err = DialMode(ctx, AutoMode(e.candidates(pass.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != CarrierStreaming {
		t.Fatalf("pass-through: %v", kinds(st.CarrierCandidates))
	}
	if good, _ := cache.state(pol.Key, CarrierStreaming, time.Now()); !good {
		t.Fatal("streaming not cached as usable")
	}
	pass.CloseClientConnections()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, until := cache.state(pol.Key, CarrierStreaming, time.Now()); !until.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a dropped streaming carrier was not cached as failed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeConn(t, c)
	c, err = DialMode(ctx, AutoMode(e.candidates(pass.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != CarrierSplit || len(st.CarrierCandidates) != 1 {
		t.Fatalf("after the drop: %v, want split at once", kinds(st.CarrierCandidates))
	}
	closeConn(t, c)
}

// TestDialModeJoin establishes on the exchange carrier and moves the
// session to the preferred kind by joining it, without a second session;
// through a buffering proxy it stays on the exchange carrier.
func TestDialModeJoin(t *testing.T) {
	leakcheck.Check(t)
	e := newModeEnv(t)
	defer e.close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := DialMode(ctx, AutoMode(e.candidates(e.srv.URL)...), modeCfg(), fastPolicy(false))
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); len(st.CarrierCandidates) != 1 || st.CarrierCandidates[0].Kind != CarrierExchange {
		t.Fatalf("established on %v", kinds(st.CarrierCandidates))
	}
	s := <-e.sessions
	defer func() { _ = s.Close() }()
	echoOnce(t, ctx, c, s)
	waitCarrier(t, c, CarrierStreaming, 10*time.Second)
	// The exchange pipe is retired: one pipe is left.
	deadline := time.Now().Add(5 * time.Second)
	for len(c.Pipes()) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("%d pipes after the upgrade", len(c.Pipes()))
		}
		time.Sleep(20 * time.Millisecond)
	}
	echoOnce(t, ctx, c, s)
	select {
	case s2 := <-e.sessions:
		t.Fatalf("a second session %p", s2)
	default:
	}
	closeConn(t, c)

	px := testproxy.New(e.srv.URL, testproxy.NginxDefaults())
	defer px.Close()
	c, err = DialMode(ctx, AutoMode(e.candidates(px.URL)...), modeCfg(), fastPolicy(false))
	if err != nil {
		t.Fatal(err)
	}
	s = <-e.sessions
	defer func() { _ = s.Close() }()
	// Both preferred joins fail, and the session stays on the exchange.
	deadline = time.Now().Add(10 * time.Second)
	for {
		st := c.ConnectionStats()
		if len(st.CarrierCandidates) >= 3 {
			if st.Carrier != CarrierExchange || st.CarrierCandidates[1].Outcome == CandidateUsable ||
				st.CarrierCandidates[2].Outcome == CandidateUsable {
				t.Fatalf("through the proxy: %v on %v", kinds(st.CarrierCandidates), st.Carrier)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("joins not tried: %v", kinds(st.CarrierCandidates))
		}
		time.Sleep(20 * time.Millisecond)
	}
	echoOnce(t, ctx, c, s)
	closeConn(t, c)
}

// TestModeBookkeepingBounded leaves no pipe waiter behind once waitPipes
// returns, registers one per wait, and keeps the latest carrier tries.
func TestModeBookkeepingBounded(t *testing.T) {
	a, b := tcpPair(t)
	c, _ := pair(t, a, b, nil, nil)
	retry := make(chan time.Time)
	close(retry)
	for range 100 {
		if ended, _ := c.waitPipes(t.Context(), []uint64{0}, retry); ended {
			t.Fatal("pipe 0 ended")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c.waitPipes(ctx, []uint64{0}, nil)
	c.lock()
	n := len(c.pipeWaiters)
	c.unlock()
	if n != 0 {
		t.Fatalf("%d pipe waiters left", n)
	}
	var tries []CandidateResult
	for i := range 100 {
		tries = append(tries, CandidateResult{Kind: CarrierKind(i)})
	}
	c.reportCarrier(0, tries, false)
	got := c.ConnectionStats().CarrierCandidates
	if len(got) != carrierTriesKept || got[0].Kind != CarrierKind(100-carrierTriesKept) || got[len(got)-1].Kind != 99 {
		t.Fatalf("%d tries kept, from %v", len(got), got[0].Kind)
	}
}
