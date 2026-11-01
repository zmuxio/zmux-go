package zmux

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/zmuxtest"
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
		profile *zmuxtest.Profile
		want    CarrierKind
		tries   int
	}{
		{"direct", nil, CarrierStreaming, 1},
		{"pass-through", &zmuxtest.Profile{}, CarrierStreaming, 1},
		{"nginx defaults", new(zmuxtest.NginxDefaults()), CarrierExchange, 3},
		{"buffering cdn", new(zmuxtest.BufferingCDN(3 * time.Second)), CarrierExchange, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			url := e.srv.URL
			if c.profile != nil {
				px := zmuxtest.New(e.srv.URL, *c.profile)
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
	px := zmuxtest.New(e.srv.URL, zmuxtest.NginxDefaults())
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
	ws := zmuxtest.New(e.srv.URL, zmuxtest.WebSocketFront())
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

	nginx := zmuxtest.New(e.srv.URL, zmuxtest.NginxDefaults())
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
	pass := zmuxtest.New(e.srv.URL, zmuxtest.Profile{})
	defer pass.Close()
	pol.Key = "server|other-lan"
	c, err = DialMode(ctx, AutoMode(e.candidates(pass.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != CarrierStreaming {
		t.Fatalf("pass-through: %v", kinds(st.CarrierCandidates))
	}
	if good, _ := cache.state(pol.Key, Candidate{Kind: CarrierStreaming}, time.Now()); !good {
		t.Fatal("streaming not cached as usable")
	}
	pass.CloseClientConnections()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, until := cache.state(pol.Key, Candidate{Kind: CarrierStreaming}, time.Now()); !until.IsZero() {
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

	px := zmuxtest.New(e.srv.URL, zmuxtest.NginxDefaults())
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

// TestModeBookkeepingBounded keeps the latest carrier tries.
func TestModeBookkeepingBounded(t *testing.T) {
	a, b := tcpPair(t)
	c, _ := pair(t, a, b, nil, nil)
	var tries []CandidateResult
	for i := range 100 {
		tries = append(tries, CandidateResult{Kind: CarrierKind(i)})
	}
	c.reportCarrier(Candidate{}, tries, false)
	got := c.ConnectionStats().CarrierCandidates
	if len(got) != carrierTriesKept || got[0].Kind != CarrierKind(100-carrierTriesKept) || got[len(got)-1].Kind != 99 {
		t.Fatalf("%d tries kept, from %v", len(got), got[0].Kind)
	}
}

// exchangeBase dials a session on the exchange carrier directly, for a
// Mode pool to add members to.
func exchangeBase(t *testing.T, ctx context.Context, e *modeEnv) (*Conn, *Conn) {
	t.Helper()
	c, err := DialMode(ctx, ExplicitMode(ExchangeCandidate(e.hc, e.srv.URL, &HTTPExchangeConfig{PSK: modePSK})), modeCfg(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return c, <-e.sessions
}

// waitCached waits until the cache holds kind under key as usable (good)
// or as failed.
func waitCached(t *testing.T, cc *CarrierCache, key string, kind CarrierKind, good bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		g, until := cc.state(key, Candidate{Kind: kind}, time.Now())
		if good && g || !good && !until.IsZero() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%v not cached as good=%v", kind, good)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPipePoolModeUpgrade dials a Mode pool's member over the most robust
// kind and rotates it to the preferred kind, make before break; a kind
// the cache holds as usable is dialed at once.
func TestPipePoolModeUpgrade(t *testing.T) {
	leakcheck.Check(t)
	e := newModeEnv(t)
	defer e.close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, s := exchangeBase(t, ctx, e)
	defer func() { _ = s.Close() }()
	pol := fastPolicy(false)
	pol.Cache, pol.Key = &CarrierCache{}, "k"
	pctx, pcancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- c.MaintainPipes(pctx, PipePool{Mode: AutoMode(e.candidates(e.srv.URL)...), ModePolicy: pol})
	}()
	waitCarrier(t, c, CarrierStreaming, 10*time.Second)
	waitCached(t, pol.Cache, "k", CarrierStreaming, true)
	tries := kinds(c.ConnectionStats().CarrierCandidates)
	// The base, the pool's exchange member, its streaming replacement.
	if len(tries) < 3 || tries[1] != "exchange:usable(join)" || tries[2] != "streaming:usable(join)" {
		t.Fatalf("tries %v", tries)
	}
	// The exchange member is closed once its replacement settled: pipe 0
	// and the streaming member are left.
	deadline := time.Now().Add(5 * time.Second)
	for len(c.Pipes()) != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("%d pipes after the upgrade", len(c.Pipes()))
		}
		time.Sleep(20 * time.Millisecond)
	}
	echoOnce(t, ctx, c, s)
	pcancel()
	if err := <-done; errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrNotNegotiated) {
		t.Fatalf("MaintainPipes: %v", err)
	}
	closeConn(t, c)

	// The cache now holds streaming as usable: a new pool joins it first.
	c, s = exchangeBase(t, ctx, e)
	defer func() { _ = s.Close() }()
	go func() {
		done <- c.MaintainPipes(ctx, PipePool{Mode: AutoMode(e.candidates(e.srv.URL)...), ModePolicy: pol})
	}()
	waitCarrier(t, c, CarrierStreaming, 10*time.Second)
	if tries := kinds(c.ConnectionStats().CarrierCandidates); len(tries) != 2 || tries[1] != "streaming:usable(join)" {
		t.Fatalf("with a cached kind: %v", tries)
	}
	closeConn(t, c)
	if err := <-done; err == nil {
		t.Fatal("pool outlived its session")
	}
}

// TestPipePoolModeFallback records a member that drops soon after it was
// joined as failed, and replaces it over another kind; a path that
// passes only the exchange carrier keeps the member there.
func TestPipePoolModeFallback(t *testing.T) {
	leakcheck.Check(t)
	e := newModeEnv(t)
	defer e.close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	pass := zmuxtest.New(e.srv.URL, zmuxtest.Profile{})
	defer pass.Close()
	c, s := exchangeBase(t, ctx, e)
	defer func() { _ = s.Close() }()
	pol := fastPolicy(false)
	pol.Cache, pol.Key = &CarrierCache{}, "k"
	done := make(chan error, 1)
	go func() {
		done <- c.MaintainPipes(ctx, PipePool{Mode: AutoMode(e.candidates(pass.URL)...), ModePolicy: pol,
			MinBackoff: 100 * time.Millisecond})
	}()
	waitCarrier(t, c, CarrierStreaming, 10*time.Second)
	waitCached(t, pol.Cache, "k", CarrierStreaming, true)
	pass.CloseClientConnections()
	waitCached(t, pol.Cache, "k", CarrierStreaming, false)
	waitCarrier(t, c, CarrierSplit, 10*time.Second)
	echoOnce(t, ctx, c, s)
	closeConn(t, c)
	if err := <-done; errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrNotNegotiated) {
		t.Fatalf("MaintainPipes: %v", err)
	}

	nginx := zmuxtest.New(e.srv.URL, zmuxtest.NginxDefaults())
	defer nginx.Close()
	c, s = exchangeBase(t, ctx, e)
	defer func() { _ = s.Close() }()
	pol.Cache = &CarrierCache{}
	go func() {
		done <- c.MaintainPipes(ctx, PipePool{Mode: AutoMode(e.candidates(nginx.URL)...), ModePolicy: pol,
			MinBackoff: 100 * time.Millisecond})
	}()
	waitCached(t, pol.Cache, "k", CarrierStreaming, false)
	waitCached(t, pol.Cache, "k", CarrierSplit, false)
	deadline := time.Now().Add(5 * time.Second)
	for len(c.Pipes()) != 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if len(c.Pipes()) != 2 || c.ConnectionStats().Carrier != CarrierExchange {
		t.Fatalf("%d pipes on %v: %v", len(c.Pipes()), c.ConnectionStats().Carrier, kinds(c.ConnectionStats().CarrierCandidates))
	}
	echoOnce(t, ctx, c, s)
	closeConn(t, c)
	if err := <-done; errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrNotNegotiated) {
		t.Fatalf("MaintainPipes: %v", err)
	}
}

// TestPipePoolModeConfig rejects a pool with more than one of Dial, Join
// and Mode, and a Mode with an invalid candidate.
func TestPipePoolModeConfig(t *testing.T) {
	leakcheck.Check(t)
	e := newModeEnv(t)
	defer e.close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, s := exchangeBase(t, ctx, e)
	defer func() { _ = s.Close() }()
	defer closeConn(t, c)
	m := AutoMode(e.candidates(e.srv.URL)...)
	dial := func(context.Context) (PipeIO, error) { return PipeIO{}, errors.New("no") }
	for _, p := range []PipePool{
		{Mode: m, Dial: dial},
		{Mode: AutoMode(Candidate{Kind: CarrierStreaming})},
		{Mode: m, ModePolicy: &ModePolicy{Settle: -1}},
	} {
		if err := c.MaintainPipes(ctx, p); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: %v", p, err)
		}
	}
}

// TestCarrierCacheSnapshot restores a snapshot in a new cache (a restart):
// the entries round-trip, expired ones are dropped, unknown fields and
// kinds are ignored, entries merge newer first, and a bad version, type
// or backoff rejects the whole snapshot.
func TestCarrierCacheSnapshot(t *testing.T) {
	cc := &CarrierCache{}
	now := time.Now()
	st := StreamingCandidate(http.DefaultClient, "https://a.example/z", nil)
	sp := SplitCandidate(http.DefaultClient, "https://a.example/z", nil)
	cc.record("k", st, false, now)
	cc.record("k", st, false, now) // backoff 2 min
	st.Target.Redirected("https://b.example/z")
	cc.record("k2", st, true, now)
	cc.record("old", sp, false, now.Add(-7*time.Hour)) // expired: past the longest backoff
	data, err := cc.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil || raw["version"] != 1.0 || len(raw["entries"].([]any)) != 2 {
		t.Fatalf("snapshot %s", data)
	}
	re := &CarrierCache{}
	if err := re.Restore(data); err != nil {
		t.Fatal(err)
	}
	if _, until := re.state("k", Candidate{Kind: CarrierStreaming}, now); until.Sub(now.Add(2*time.Minute)).Abs() > time.Millisecond {
		t.Fatalf("restored backoff until %v", until)
	}
	if good, _ := re.state("k2", Candidate{Kind: CarrierStreaming}, now); !good {
		t.Fatal("usable entry not restored")
	}
	if _, ok := re.m[cacheKey{"old", CarrierSplit, ""}]; ok {
		t.Fatal("expired entry restored")
	}
	if e := re.m[cacheKey{"k", CarrierStreaming, ""}]; e.backoff != 2*time.Minute {
		t.Fatalf("backoff %v", e.backoff)
	}
	fresh := StreamingCandidate(http.DefaultClient, "https://a.example/z", nil)
	re.prime("k2", fresh)
	if fresh.Target.URL() != "https://b.example/z" {
		t.Fatalf("restored URL %q", fresh.Target.URL())
	}
	// The restored backoff doubles on the next failure.
	re.record("k", st, false, now)
	if e := re.m[cacheKey{"k", CarrierStreaming, ""}]; e.backoff != 4*time.Minute {
		t.Fatalf("backoff after restore %v", e.backoff)
	}

	future := now.Add(time.Hour).UTC().Format(time.RFC3339Nano)
	later := now.Add(2 * time.Hour).UTC().Format(time.RFC3339Nano)
	past := now.Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	longAgo := now.Add(-7 * time.Hour).UTC().Format(time.RFC3339Nano)
	lc := &CarrierCache{}
	lenient := `{"version":1,"extra":true,"entries":[` +
		`{"key":"k","kind":"split","good_until":"` + future + `","fail_until":"","new_field":3},` +
		`{"key":"k","kind":"carrier-of-the-future","good_until":"` + future + `"},` +
		`{"key":"k","kind":"exchange","fail_until":"` + past + `","backoff_ms":60000},` + // backoff history kept
		`{"key":"k","kind":"quic","fail_until":"` + longAgo + `","backoff_ms":60000},` + // expired
		`{"key":"d","kind":"quic","fail_until":"` + later + `","backoff_ms":1},` + // newer, backoff to 1 min
		`{"key":"d","kind":"quic","fail_until":"` + future + `","backoff_ms":999999999999}]}`
	if err := lc.Restore([]byte(lenient)); err != nil {
		t.Fatal(err)
	}
	if len(lc.m) != 3 {
		t.Fatalf("restored %d entries", len(lc.m))
	}
	if good, _ := lc.state("k", Candidate{Kind: CarrierSplit}, now); !good {
		t.Fatal("split not restored")
	}
	if e := lc.m[cacheKey{"k", CarrierExchange, ""}]; e == nil || e.backoff != time.Minute {
		t.Fatal("backoff history not restored")
	}
	if e := lc.m[cacheKey{"d", CarrierQUIC, ""}]; e == nil || e.backoff != time.Minute {
		t.Fatalf("duplicate entry %+v", e)
	}
	// Merge: an older entry does not replace a newer one, a newer does.
	older := `{"version":1,"entries":[{"key":"k","kind":"split","fail_until":"` + future + `","backoff_ms":60000},` +
		`{"key":"d","kind":"quic","good_until":"` + later + `"},{"key":"n","kind":"websocket","good_until":"` + future + `"}]}`
	if err := lc.Restore([]byte(older)); err != nil {
		t.Fatal(err)
	}
	if good, _ := lc.state("k", Candidate{Kind: CarrierSplit}, now); !good {
		t.Fatal("an older entry replaced a newer one")
	}
	if good, _ := lc.state("n", Candidate{Kind: CarrierWebSocket}, now); !good || len(lc.m) != 4 {
		t.Fatal("new entry not merged")
	}
	newer := `{"version":1,"entries":[{"key":"k","kind":"split","fail_until":"` + later + `","backoff_ms":60000}]}`
	if err := lc.Restore([]byte(newer)); err != nil {
		t.Fatal(err)
	}
	if good, _ := lc.state("k", Candidate{Kind: CarrierSplit}, now); good {
		t.Fatal("a newer entry did not replace an older one")
	}
	for _, bad := range []string{`{"version":2,"entries":[]}`, `{"entries":[]}`, `not json`,
		`{"version":1,"entries":[{"key":"z","kind":"split","good_until":"` + future + `"},{"key":"k","kind":"split","backoff_ms":-1}]}`,
		`{"version":1,"entries":[{"key":"z","kind":"split","backoff_ms":1.5}]}`,
		`{"version":1,"entries":[{"key":"z","kind":"split","good_until":3}]}`,
		`{"version":1,"entries":[{"key":"z","kind":"split","good_until":"tomorrow"}]}`,
		`{"version":"1","entries":[]}`} {
		err := lc.Restore([]byte(bad))
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("restored %s: %v", bad, err)
		}
	}
	if len(lc.m) != 4 || lc.m[cacheKey{"z", CarrierSplit, ""}] != nil {
		t.Fatal("a rejected snapshot changed the cache")
	}
}

// TestDialModeRestoredCache skips, after a restart, the kinds a restored
// snapshot holds as failed.
func TestDialModeRestoredCache(t *testing.T) {
	leakcheck.Check(t)
	e := newModeEnv(t)
	defer e.close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before := &CarrierCache{}
	before.record("k", StreamingCandidate(e.hc, e.srv.URL, nil), false, time.Now())
	data, err := before.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	after := &CarrierCache{}
	if err := after.Restore(data); err != nil {
		t.Fatal(err)
	}
	pol := fastPolicy(true)
	pol.Cache, pol.Key = after, "k"
	c, err := DialMode(ctx, AutoMode(e.candidates(e.srv.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != CarrierSplit || len(st.CarrierCandidates) != 1 {
		t.Fatalf("after the restore: %v", kinds(st.CarrierCandidates))
	}
	closeConn(t, c)
}
