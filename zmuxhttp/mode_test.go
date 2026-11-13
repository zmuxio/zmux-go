package zmuxhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
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
	sessions chan *zmux.Conn
	stop     context.CancelFunc
	done     chan struct{}
}

func newModeEnv(t *testing.T) *modeEnv {
	t.Helper()
	e := &modeEnv{unifiedEnv: newUnifiedEnv(t, nil, nil), tr: &http.Transport{MaxIdleConnsPerHost: 16},
		sessions: make(chan *zmux.Conn, 16), done: make(chan struct{})}
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
	// Their carriers keep the server's handlers: end them before it closes.
	close(e.sessions)
	for s := range e.sessions {
		_ = s.Close()
	}
	e.unifiedEnv.close()
	e.tr.CloseIdleConnections()
}

// session is the next session the listener accepted.
func (e *modeEnv) session(t *testing.T, ctx context.Context) *zmux.Conn {
	t.Helper()
	select {
	case s := <-e.sessions:
		return s
	case <-ctx.Done():
		t.Fatal("no session at the listener")
		return nil
	}
}

// candidates are the HTTP kinds to url, by preference.
func (e *modeEnv) candidates(url string) []zmux.Candidate {
	return []zmux.Candidate{
		StreamCandidate(e.hc, url, nil),
		SplitCandidate(e.hc, url, nil),
		ExchangeCandidate(e.hc, url, &ExchangeConfig{PSK: modePSK}),
	}
}

func modeCfg() *zmux.Config {
	c := mpConfig()
	c.HandshakeTimeout = 3 * time.Second
	return c
}

func fastPolicy(noJoin bool) *zmux.CarrierChoicePolicy {
	p := &zmux.CarrierChoicePolicy{TimerMin: 400 * time.Millisecond, TimerMax: time.Second, Settle: 200 * time.Millisecond}
	if noJoin {
		p.Join = zmux.SwitchOff
	}
	return p
}

// kinds lists the kinds and outcomes of results.
func kinds(rs []zmux.CandidateResult) []string {
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
func closeConn(t *testing.T, c *zmux.Conn) {
	_ = c.Close()
	waitDone(t, c)
}

// waitCarrier waits until the session's active kind is k.
func waitCarrier(t *testing.T, c *zmux.Conn, k zmux.CarrierKind, within time.Duration) {
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
		want    zmux.CarrierKind
		tries   int
	}{
		{"direct", nil, zmux.CarrierStream, 1},
		{"pass-through", &zmuxtest.Profile{}, zmux.CarrierStream, 1},
		{"nginx defaults", new(zmuxtest.NginxDefaults()), zmux.CarrierExchange, 3},
		{"buffering cdn", new(zmuxtest.BufferingCDN(3 * time.Second)), zmux.CarrierExchange, 3},
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
			conn, err := zmux.DialChoice(ctx, zmux.AutoChoice(e.candidates(url)...), modeCfg(), fastPolicy(true))
			if err != nil {
				t.Fatal(err)
			}
			defer closeConn(t, conn)
			st := conn.ConnectionStats()
			if st.Carrier != c.want || len(st.CarrierCandidates) != c.tries {
				t.Fatalf("chose %v after %v, want %v after %d tries", st.Carrier, kinds(st.CarrierCandidates), c.want, c.tries)
			}
			for i, r := range st.CarrierCandidates {
				if (r.Outcome == zmux.CandidateUsable) != (i == len(st.CarrierCandidates)-1) {
					t.Fatalf("tries %v", kinds(st.CarrierCandidates))
				}
			}
			// The events: one per try, then the choice.
			for i := 0; i <= c.tries; i++ {
				ev, err := conn.NextEvent(ctx)
				for err == nil && ev.Kind != zmux.EventCarrierCandidate && ev.Kind != zmux.EventCarrierChosen {
					ev, err = conn.NextEvent(ctx)
				}
				if err != nil {
					t.Fatal(err)
				}
				switch {
				case i < c.tries && (ev.Kind != zmux.EventCarrierCandidate || ev.Carrier != st.CarrierCandidates[i].Kind ||
					ev.Outcome != st.CarrierCandidates[i].Outcome):
					t.Fatalf("event %d: %+v", i, ev)
				case i == c.tries && (ev.Kind != zmux.EventCarrierChosen || ev.Carrier != c.want):
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
	_, err := zmux.DialChoice(ctx, zmux.ExplicitChoice(StreamCandidate(e.hc, px.URL, nil)), cfg, nil)
	var me *zmux.CarrierChoiceError
	if !errors.As(err, &me) || len(me.Results) != 1 || me.Results[0].Kind != zmux.CarrierStream ||
		me.Results[0].Outcome != zmux.CandidateTimedOut || !errors.Is(err, zmux.ErrHandshakeTimeout) {
		t.Fatalf("explicit streaming through a buffering proxy: %v", err)
	}
	// A WebSocket-only front refuses the request.
	ws := zmuxtest.New(e.srv.URL, zmuxtest.WebSocketFront())
	defer ws.Close()
	_, err = zmux.DialChoice(ctx, zmux.ExplicitChoice(ExchangeCandidate(e.hc, ws.URL, &ExchangeConfig{PSK: modePSK})), cfg, nil)
	if !errors.As(err, &me) || len(me.Results) != 1 || me.Results[0].Kind != zmux.CarrierExchange {
		t.Fatalf("explicit exchange through a WebSocket front: %v", err)
	}
	// The exchange carrier repeats refused requests until its deadline.
	if o := me.Results[0].Outcome; o != zmux.CandidateTimedOut && o != zmux.CandidateFailed {
		t.Fatalf("outcome %v", o)
	}
	// Explicit and passing: no other kind is tried.
	c, err := zmux.DialChoice(ctx, zmux.ExplicitChoice(ExchangeCandidate(e.hc, px.URL, &ExchangeConfig{PSK: modePSK})), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != zmux.CarrierExchange || len(st.CarrierCandidates) != 1 {
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
	cache := &zmux.CarrierCache{}
	pol := fastPolicy(true)
	pol.Cache, pol.Key = cache, "server|lan"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nginx := zmuxtest.New(e.srv.URL, zmuxtest.NginxDefaults())
	defer nginx.Close()
	c, err := zmux.DialChoice(ctx, zmux.AutoChoice(e.candidates(nginx.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != zmux.CarrierExchange || len(st.CarrierCandidates) != 3 {
		t.Fatalf("first dial: %v", kinds(st.CarrierCandidates))
	}
	closeConn(t, c)
	c, err = zmux.DialChoice(ctx, zmux.AutoChoice(e.candidates(nginx.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != zmux.CarrierExchange || len(st.CarrierCandidates) != 1 {
		t.Fatalf("second dial tried %v; the failed kinds are cached", kinds(st.CarrierCandidates))
	}
	closeConn(t, c)

	// A path that lets streaming through, then drops it.
	pass := zmuxtest.New(e.srv.URL, zmuxtest.Profile{})
	defer pass.Close()
	pol.Key = "server|other-lan"
	c, err = zmux.DialChoice(ctx, zmux.AutoChoice(e.candidates(pass.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != zmux.CarrierStream {
		t.Fatalf("pass-through: %v", kinds(st.CarrierCandidates))
	}
	if good, _ := cacheState(t, cache, pol.Key, zmux.CarrierStream); !good {
		t.Fatal("streaming not cached as usable")
	}
	pass.CloseClientConnections()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, failed := cacheState(t, cache, pol.Key, zmux.CarrierStream); failed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a dropped streaming carrier was not cached as failed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeConn(t, c)
	c, err = zmux.DialChoice(ctx, zmux.AutoChoice(e.candidates(pass.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != zmux.CarrierSplit || len(st.CarrierCandidates) != 1 {
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
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// A join that works has all the time load may take; only the joins
	// through the proxy below are expected to run out of it.
	patient := &zmux.CarrierChoicePolicy{TimerMin: 20 * time.Second, TimerMax: 40 * time.Second, Settle: 200 * time.Millisecond}
	c, err := zmux.DialChoice(ctx, zmux.AutoChoice(e.candidates(e.srv.URL)...), modeCfg(), patient)
	if err != nil {
		t.Fatal(err)
	}
	// Whichever client is open when the test ends, on any path.
	defer func() { _ = c.Close() }()
	// A join may already have followed: the first try is what established.
	if st := c.ConnectionStats(); len(st.CarrierCandidates) == 0 || st.CarrierCandidates[0].Kind != zmux.CarrierExchange {
		t.Fatalf("established on %v", kinds(st.CarrierCandidates))
	}
	s := e.session(t, ctx)
	defer func() { _ = s.Close() }()
	echoOnce(t, ctx, c, s)
	waitCarrier(t, c, zmux.CarrierStream, 60*time.Second)
	// The exchange pipe is retired: one pipe is left.
	deadline := time.Now().Add(30 * time.Second)
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
	c, err = zmux.DialChoice(ctx, zmux.AutoChoice(e.candidates(px.URL)...), modeCfg(), fastPolicy(false))
	if err != nil {
		t.Fatal(err)
	}
	s = e.session(t, ctx)
	defer func() { _ = s.Close() }()
	// Both preferred joins fail, and the session stays on the exchange.
	deadline = time.Now().Add(60 * time.Second)
	for {
		st := c.ConnectionStats()
		if len(st.CarrierCandidates) >= 3 {
			if st.Carrier != zmux.CarrierExchange || st.CarrierCandidates[1].Outcome == zmux.CandidateUsable ||
				st.CarrierCandidates[2].Outcome == zmux.CandidateUsable {
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

// exchangeBase dials a session on the exchange carrier directly, for a
// Mode pool to add members to.
func exchangeBase(t *testing.T, ctx context.Context, e *modeEnv) (*zmux.Conn, *zmux.Conn) {
	t.Helper()
	c, err := zmux.DialChoice(ctx, zmux.ExplicitChoice(ExchangeCandidate(e.hc, e.srv.URL, &ExchangeConfig{PSK: modePSK})), modeCfg(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return c, <-e.sessions
}

// waitCached waits until the cache holds kind under key as usable (good)
// or as failed.
func waitCached(t *testing.T, cc *zmux.CarrierCache, key string, kind zmux.CarrierKind, good bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		g, failed := cacheState(t, cc, key, kind)
		if good && g || !good && failed {
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
	pol.Cache, pol.Key = &zmux.CarrierCache{}, "k"
	pctx, pcancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- c.MaintainPipes(pctx, zmux.PipePool{Choice: zmux.AutoChoice(e.candidates(e.srv.URL)...), ChoicePolicy: pol})
	}()
	waitCarrier(t, c, zmux.CarrierStream, 10*time.Second)
	waitCached(t, pol.Cache, "k", zmux.CarrierStream, true)
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
	if err := <-done; errors.Is(err, zmux.ErrInvalidConfig) || errors.Is(err, zmux.ErrNotNegotiated) {
		t.Fatalf("MaintainPipes: %v", err)
	}
	closeConn(t, c)

	// The cache now holds streaming as usable: a new pool joins it first.
	c, s = exchangeBase(t, ctx, e)
	defer func() { _ = s.Close() }()
	go func() {
		done <- c.MaintainPipes(ctx, zmux.PipePool{Choice: zmux.AutoChoice(e.candidates(e.srv.URL)...), ChoicePolicy: pol})
	}()
	waitCarrier(t, c, zmux.CarrierStream, 10*time.Second)
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
	pol.Cache, pol.Key = &zmux.CarrierCache{}, "k"
	done := make(chan error, 1)
	go func() {
		done <- c.MaintainPipes(ctx, zmux.PipePool{Choice: zmux.AutoChoice(e.candidates(pass.URL)...), ChoicePolicy: pol,
			MinBackoff: 100 * time.Millisecond})
	}()
	waitCarrier(t, c, zmux.CarrierStream, 10*time.Second)
	waitCached(t, pol.Cache, "k", zmux.CarrierStream, true)
	pass.CloseClientConnections()
	waitCached(t, pol.Cache, "k", zmux.CarrierStream, false)
	waitCarrier(t, c, zmux.CarrierSplit, 10*time.Second)
	echoOnce(t, ctx, c, s)
	closeConn(t, c)
	if err := <-done; errors.Is(err, zmux.ErrInvalidConfig) || errors.Is(err, zmux.ErrNotNegotiated) {
		t.Fatalf("MaintainPipes: %v", err)
	}

	nginx := zmuxtest.New(e.srv.URL, zmuxtest.NginxDefaults())
	defer nginx.Close()
	c, s = exchangeBase(t, ctx, e)
	defer func() { _ = s.Close() }()
	pol.Cache = &zmux.CarrierCache{}
	go func() {
		done <- c.MaintainPipes(ctx, zmux.PipePool{Choice: zmux.AutoChoice(e.candidates(nginx.URL)...), ChoicePolicy: pol,
			MinBackoff: 100 * time.Millisecond})
	}()
	waitCached(t, pol.Cache, "k", zmux.CarrierStream, false)
	waitCached(t, pol.Cache, "k", zmux.CarrierSplit, false)
	deadline := time.Now().Add(5 * time.Second)
	for len(c.Pipes()) != 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if len(c.Pipes()) != 2 || c.ConnectionStats().Carrier != zmux.CarrierExchange {
		t.Fatalf("%d pipes on %v: %v", len(c.Pipes()), c.ConnectionStats().Carrier, kinds(c.ConnectionStats().CarrierCandidates))
	}
	echoOnce(t, ctx, c, s)
	closeConn(t, c)
	if err := <-done; errors.Is(err, zmux.ErrInvalidConfig) || errors.Is(err, zmux.ErrNotNegotiated) {
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
	m := zmux.AutoChoice(e.candidates(e.srv.URL)...)
	dial := func(context.Context) (zmux.PipeIO, error) { return zmux.PipeIO{}, errors.New("no") }
	for _, p := range []zmux.PipePool{
		{Choice: m, Dial: dial},
		{Choice: zmux.AutoChoice(zmux.Candidate{Kind: zmux.CarrierStream})},
		{Choice: m, ChoicePolicy: &zmux.CarrierChoicePolicy{Settle: -1}},
	} {
		if err := c.MaintainPipes(ctx, p); !errors.Is(err, zmux.ErrInvalidConfig) {
			t.Fatalf("%+v: %v", p, err)
		}
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
	before := &zmux.CarrierCache{}
	before.Report("k", zmux.CarrierStream, "", zmux.CacheFailed)
	data, err := before.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	after := &zmux.CarrierCache{}
	if err := after.Restore(data); err != nil {
		t.Fatal(err)
	}
	pol := fastPolicy(true)
	pol.Cache, pol.Key = after, "k"
	c, err := zmux.DialChoice(ctx, zmux.AutoChoice(e.candidates(e.srv.URL)...), modeCfg(), pol)
	if err != nil {
		t.Fatal(err)
	}
	if st := c.ConnectionStats(); st.Carrier != zmux.CarrierSplit || len(st.CarrierCandidates) != 1 {
		t.Fatalf("after the restore: %v", kinds(st.CarrierCandidates))
	}
	closeConn(t, c)
}

// cacheState reports what cc's Snapshot holds for kind under key: whether
// it is usable now, and whether it failed and is in its backoff.
func cacheState(t *testing.T, cc *zmux.CarrierCache, key string, kind zmux.CarrierKind) (good, failed bool) {
	t.Helper()
	data, err := cc.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var snap struct {
		Entries []struct {
			Key, Kind, Variant string
			GoodUntil          string `json:"good_until"`
			FailUntil          string `json:"fail_until"`
		}
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	after := func(s string) bool {
		tm, err := time.Parse(time.RFC3339Nano, s)
		return err == nil && tm.After(now)
	}
	for _, e := range snap.Entries {
		if e.Key == key && e.Kind == kind.String() && e.Variant == "" {
			if after(e.FailUntil) {
				return false, true
			}
			return after(e.GoodUntil), false
		}
	}
	return false, false
}
