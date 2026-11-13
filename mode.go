package zmux

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// Carrier selection: DialChoice dials a session over one carrier
// kind (ExplicitChoice) or chooses among candidates (AutoChoice), for paths
// through reverse proxies and CDNs where which kinds pass depends on the
// path. Every kind keeps its own dialer; a Candidate wraps one. A kind
// describes only this end's hop of a carrier, not what lies beyond its
// first peer.

// CarrierKind names a kind of carrier.
type CarrierKind uint8

const (
	// CarrierWebSocket is a WebSocket (zmuxws.Candidate).
	CarrierWebSocket CarrierKind = iota + 1
	// CarrierStream is a bidirectional streaming request: one request
	// whose body is the uplink and whose response body the downlink
	// (zmuxhttp.StreamCandidate).
	CarrierStream
	// CarrierSplit is a split bootstrap: an uplink POST and a downlink GET
	// (zmuxhttp.SplitCandidate).
	CarrierSplit
	// CarrierExchange is the exchange carrier of bounded requests
	// (zmuxhttp.ExchangeCandidate).
	CarrierExchange
	// CarrierQUIC is a QUIC stream (zmuxquic.Candidate), direct only.
	CarrierQUIC
	// CarrierHTTP3 is a streaming request over HTTP/3
	// (zmuxh3.HTTP3Candidate).
	CarrierHTTP3
	// CarrierWebTransport is a WebTransport stream
	// (zmuxh3.Candidate), direct only.
	CarrierWebTransport
)

var carrierKindNames = [...]string{"", "websocket", "streaming", "split", "exchange", "quic", "http3", "webtransport"}

// String returns the kind's name: "websocket", "streaming", "split",
// "exchange", "quic", "http3" or "webtransport".
func (k CarrierKind) String() string {
	if int(k) < len(carrierKindNames) && k != 0 {
		return carrierKindNames[k]
	}
	return "CarrierKind(?)"
}

// CandidateOutcome is the outcome of one try of a candidate.
type CandidateOutcome uint8

const (
	// CandidateUsable means the peer's transport parameters (for a join, the
	// join's confirmation) arrived within the timer.
	CandidateUsable CandidateOutcome = iota + 1
	// CandidateTimedOut means the carrier was opened but the timer expired; it
	// was closed without CONNECTION_CLOSE.
	CandidateTimedOut
	// CandidateRefused means the server or an intermediary refused the carrier,
	// such as with an HTTP status other than 2xx or a refused connection.
	CandidateRefused
	// CandidateFailed means any other failure.
	CandidateFailed
)

var outcomeNames = [...]string{"", "usable", "timed_out", "refused", "failed"}

// String returns "usable", "timed_out", "refused" or "failed".
func (o CandidateOutcome) String() string {
	if int(o) < len(outcomeNames) && o != 0 {
		return outcomeNames[o]
	}
	return "CandidateOutcome(?)"
}

// CandidateResult is the outcome of one try of a candidate.
type CandidateResult struct {
	Variant string // the candidate's Variant
	// Err is the failure (nil when usable).
	Err     error
	Kind    CarrierKind
	Outcome CandidateOutcome
	// Joined is set for a try as a further pipe of the established session.
	Joined bool
}

// Candidate is one carrier kind that DialChoice may use. Dial opens a
// carrier of the kind, presenting cred (the session's Config.Credential)
// for the server's admission; DialChoice sets its PipeIO.Carrier to Kind.
// The carrier must live until the pipe ends, not until ctx does, which
// bounds only the opening. The constructors of each adapter return ready
// candidates.
type Candidate struct {
	Kind CarrierKind
	Dial func(ctx context.Context, cred CarrierCredential) (PipeIO, error)
	// Target, for a kind dialed at a URL, is where its carriers start and
	// what redirects led them to (nil: not known). DialChoice keeps it per
	// server in CarrierChoicePolicy.Cache, and starts it at the base URL again
	// after a failure.
	Target CarrierTarget
	// Variant labels a transform the candidate's carriers run (PipeIO.Variant,
	// which DialChoice sets on what Dial returns when it is not ""). A kind
	// with different variants makes different candidates, each with its
	// own CarrierCache entry; DialChoice orders them as for different kinds,
	// so that the host states its preference by their order.
	Variant string

	// Join joins carriers of the kind to c, for a kind whose Dial does
	// not open a carrier that can join (a split bootstrap); nil: AddPipe
	// on a carrier from Dial. ctx bounds the attempt, life the carriers.
	Join func(ctx, life context.Context, c *Conn, cred CarrierCredential, o PipeOptions) ([]*Pipe, error)
}

// CarrierTarget is where the carriers of a kind dialed at a URL start, and
// what redirects led them to; zmuxhttp.Target is one. CarrierCache
// remembers the URL a usable kind's redirects found and starts the target
// there in later dials.
type CarrierTarget interface {
	// URL is where the next carrier starts.
	URL() string
	// Base is the URL the target was made with.
	Base() string
	// Redirected moves the target to url, a URL redirects led to.
	Redirected(url string)
	// Reset starts the target at its base URL again.
	Reset()
}

// CarrierChoice is how DialChoice, or a PipePool, chooses the carrier: ExplicitChoice
// or AutoChoice.
type CarrierChoice struct {
	auto       bool
	candidates []Candidate
}

// ExplicitChoice dials c only: a failure is reported, never replaced by
// another kind.
func ExplicitChoice(c Candidate) CarrierChoice { return CarrierChoice{candidates: []Candidate{c}} }

// AutoChoice chooses among the candidates, in their order of preference
// (most preferred first).
func AutoChoice(c ...Candidate) CarrierChoice {
	return CarrierChoice{auto: true, candidates: slices.Clone(c)}
}

// CarrierChoicePolicy is the local policy of AutoChoice; the zero value of every
// field selects its default, and a nil *CarrierChoicePolicy is all defaults.
type CarrierChoicePolicy struct {
	// Cache remembers the outcomes per server and local network across
	// dials (nil: a cache private to the session).
	Cache *CarrierCache
	// Key names the server and the local network in Cache, such as the
	// server's URL host with the name of the local network.
	Key string
	// TimerFactor, TimerMin and TimerMax set a candidate's timer: the round
	// trip of opening its carrier times TimerFactor (3), within TimerMin
	// (1 s) and TimerMax (3 s); a zero timer would skip every candidate.
	// Request carriers return before any round
	// trip, so for them the timer is TimerMin.
	TimerFactor        float64
	TimerMin, TimerMax time.Duration
	// Settle is how long a joined pipe of a preferred kind stays joined
	// before the pipes of the kind it replaces are retired (1 s).
	Settle time.Duration
	// DropWindow: a carrier kind whose pipes (or session) fail within this
	// long of being chosen counts as failed, so that the next dials skip it
	// until its backoff expires (30 s).
	DropWindow time.Duration
	// Join establishes a session on the most robust candidate and joins
	// the preferred ones. It is on by default; with SwitchOff the
	// candidates are tried one after another, in order.
	Join Switch
}

type modeParams struct {
	cache              *CarrierCache
	key                string
	factor             float64
	timerMin, timerMax time.Duration
	settle, drop       time.Duration
	noJoin             bool
}

func (p *CarrierChoicePolicy) resolve() (modeParams, error) {
	var x CarrierChoicePolicy
	if p != nil {
		x = *p
	}
	r := modeParams{cache: x.Cache, key: x.Key, factor: x.TimerFactor, timerMin: x.TimerMin,
		timerMax: x.TimerMax, settle: x.Settle, drop: x.DropWindow, noJoin: !x.Join.On()}
	if r.cache == nil {
		r.cache = &CarrierCache{}
	}
	if cc := r.cache; !(r.factor >= 0) || r.timerMin < 0 || r.timerMax < 0 || r.settle < 0 || r.drop < 0 ||
		cc.SuccessTTL < 0 || cc.BackoffMin < 0 || cc.BackoffMax < 0 {
		return r, fmt.Errorf("%w: negative or NaN CarrierChoicePolicy or CarrierCache field", ErrInvalidConfig)
	}
	if r.factor <= 0 {
		r.factor = 3
	}
	if r.timerMin <= 0 {
		r.timerMin = time.Second
	}
	if r.timerMax <= 0 {
		r.timerMax = 3 * time.Second
	}
	r.timerMax = max(r.timerMax, r.timerMin)
	if r.settle <= 0 {
		r.settle = time.Second
	}
	if r.drop <= 0 {
		r.drop = 30 * time.Second
	}
	return r, nil
}

// timer is a candidate's timer for the round trip of opening its carrier.
func (p *modeParams) timer(rtt time.Duration) time.Duration {
	return min(max(time.Duration(float64(rtt)*p.factor), p.timerMin), p.timerMax)
}

// CarrierCache remembers which carrier kinds worked per server and local
// network (CarrierChoicePolicy.Key): a usable kind for SuccessTTL, a failed one is
// skipped for a backoff that doubles with every failure in a row, from
// BackoffMin to BackoffMax. A kind that fails soon after it was chosen
// (CarrierChoicePolicy.DropWindow) counts as failed, so that a path that lets a
// kind through only briefly is not tried again at once. The zero value is
// ready; the fields must not change once it is in use. It is safe for
// concurrent use.
type CarrierCache struct {
	// SuccessTTL is how long a usable kind is remembered (6 h).
	SuccessTTL time.Duration
	// BackoffMin and BackoffMax bound the backoff of a failed kind (1 min,
	// 6 h).
	BackoffMin, BackoffMax time.Duration

	mu sync.Mutex
	m  map[cacheKey]*cacheEntry
}

type cacheKey struct {
	key     string
	kind    CarrierKind
	variant string
}

func candKey(key string, c Candidate) cacheKey { return cacheKey{key, c.Kind, c.Variant} }

type cacheEntry struct {
	goodUntil time.Time
	failUntil time.Time
	backoff   time.Duration
	url       string // where redirects led the usable kind ("": its base)
}

// CacheOutcome is what a host learnt of a candidate (CarrierCache.Report).
type CacheOutcome uint8

const (
	// CacheUsable means that the candidate worked, as a chosen one does.
	CacheUsable CacheOutcome = iota
	// CacheFailed means an ordinary failure; the backoff doubles.
	CacheFailed
	// CacheRejected means a definite verdict, such as a binding check that
	// proves the path unsuitable; the candidate is skipped for
	// BackoffMax.
	CacheRejected
)

// Report records an outcome the host learnt after DialChoice for the
// candidate of kind and variant under key (the CarrierChoicePolicy.Key in use),
// in the entry DialChoice's own outcomes use. Safe for concurrent use.
func (cc *CarrierCache) Report(key string, kind CarrierKind, variant string, o CacheOutcome) {
	cc.update(cacheKey{key, kind, variant}, nil, o, time.Now())
}

// prime starts cand's target at the URL the cache remembers for it.
func (cc *CarrierCache) prime(key string, cand Candidate) {
	if cand.Target == nil {
		return
	}
	cc.mu.Lock()
	var u string
	if e := cc.m[candKey(key, cand)]; e != nil {
		u = e.url
	}
	cc.mu.Unlock()
	if u != "" {
		cand.Target.Redirected(u)
	}
}

// state reports what the cache knows of kind under key at now.
func (cc *CarrierCache) state(key string, cand Candidate, now time.Time) (good bool, failUntil time.Time) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	e := cc.m[candKey(key, cand)]
	if e == nil {
		return false, time.Time{}
	}
	if now.Before(e.failUntil) {
		return false, e.failUntil
	}
	return now.Before(e.goodUntil), time.Time{}
}

// record records the outcome of cand under key; for a kind with a target,
// a usable one's URL, and a failed one starts at its base URL again.
func (cc *CarrierCache) record(key string, cand Candidate, ok bool, now time.Time) {
	o := CacheFailed
	if ok {
		o = CacheUsable
	}
	cc.update(candKey(key, cand), cand.Target, o, now)
}

// update applies o to the entry of k; target, when known, is the
// candidate's.
func (cc *CarrierCache) update(k cacheKey, target CarrierTarget, o CacheOutcome, now time.Time) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.m == nil {
		cc.m = map[cacheKey]*cacheEntry{}
	}
	e := cc.m[k]
	if e == nil {
		e = &cacheEntry{}
		cc.m[k] = e
	}
	if o == CacheUsable {
		url := e.url
		*e = cacheEntry{goodUntil: now.Add(cc.successTTL())}
		switch {
		case target != nil && target.URL() != target.Base():
			e.url = target.URL()
		case target == nil:
			e.url = url // a report keeps what redirects found
		}
		return
	}
	e.url = ""
	if target != nil {
		target.Reset()
	}
	lo, hi := cc.backoffBounds()
	e.backoff = min(max(2*e.backoff, lo), hi)
	if o == CacheRejected {
		e.backoff = hi
	}
	e.goodUntil, e.failUntil = time.Time{}, now.Add(e.backoff)
}

// backoffBounds returns BackoffMin and BackoffMax with their defaults,
// the maximum no less than the minimum.
// successTTL is SuccessTTL or its default.
func (cc *CarrierCache) successTTL() time.Duration {
	if cc.SuccessTTL <= 0 {
		return 6 * time.Hour
	}
	return cc.SuccessTTL
}

func (cc *CarrierCache) backoffBounds() (lo, hi time.Duration) {
	lo, hi = cc.BackoffMin, cc.BackoffMax
	if lo <= 0 {
		lo = time.Minute
	}
	if hi <= 0 {
		hi = 6 * time.Hour
	}
	return lo, max(hi, lo)
}

// expired reports whether an entry no longer tells anything at now: it is
// neither usable nor failed, and its backoff history is older than the
// longest backoff.
func (e *cacheEntry) expired(now time.Time, backoffMax time.Duration) bool {
	return !now.Before(e.goodUntil) && !now.Before(e.failUntil) && !now.Before(e.failUntil.Add(backoffMax))
}

// newer reports whether e was recorded later than o: the later of its
// two times is later.
func (e *cacheEntry) newer(o *cacheEntry) bool {
	return latest(e).After(latest(o))
}

func latest(e *cacheEntry) time.Time {
	if e.failUntil.After(e.goodUntil) {
		return e.failUntil
	}
	return e.goodUntil
}

// carrierCacheVersion is the version of the format of
// CarrierCache.Snapshot.
const carrierCacheVersion = 1

// cacheSnapshot is the format of CarrierCache.Snapshot: JSON with the
// times as RFC 3339 wall-clock times ("" or absent: none) and the backoff
// in milliseconds.
type cacheSnapshot struct {
	Version *int                 `json:"version"`
	Entries []cacheSnapshotEntry `json:"entries"`
}

type cacheSnapshotEntry struct {
	Key       string `json:"key"`
	Kind      string `json:"kind"`
	GoodUntil string `json:"good_until,omitempty"`
	FailUntil string `json:"fail_until,omitempty"`
	BackoffMS int64  `json:"backoff_ms,omitempty"`
	URL       string `json:"url,omitempty"`
	Variant   string `json:"variant,omitempty"`
}

func formatCacheTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Round(0).UTC().Format(time.RFC3339Nano)
}

func parseCacheTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

// Snapshot returns what the cache knows, to be restored by Restore, such as
// in the next run of the process: JSON of the form
//
//	{"version": 1, "entries": [{"key": "...", "kind": "streaming",
//	  "good_until": "2026-11-01T12:00:00Z", "fail_until": "...",
//	  "backoff_ms": 60000, "url": "..."}]}
//
// with the kinds by name (CarrierKind.String) and the times on the wall
// clock. An entry is expired, and left out, when both its times and its
// fail time plus BackoffMax have passed.
func (cc *CarrierCache) Snapshot() ([]byte, error) {
	now := time.Now()
	_, hi := cc.backoffBounds()
	v := carrierCacheVersion
	snap := cacheSnapshot{Version: &v, Entries: []cacheSnapshotEntry{}}
	cc.mu.Lock()
	for k, e := range cc.m {
		if e.expired(now, hi) {
			continue
		}
		snap.Entries = append(snap.Entries, cacheSnapshotEntry{Key: k.key, Kind: k.kind.String(),
			GoodUntil: formatCacheTime(e.goodUntil), FailUntil: formatCacheTime(e.failUntil),
			BackoffMS: e.backoff.Milliseconds(), URL: e.url, Variant: k.variant})
	}
	cc.mu.Unlock()
	slices.SortFunc(snap.Entries, func(a, b cacheSnapshotEntry) int {
		return cmp.Or(strings.Compare(a.Key, b.Key), strings.Compare(a.Kind, b.Kind), strings.Compare(a.Variant, b.Variant))
	})
	return json.Marshal(snap)
}

// Restore merges a Snapshot into the cache. For a key and kind the cache
// already holds, or that the snapshot holds more than once, the newer
// entry (the later of its two times is later) is kept. A nonzero backoff
// outside BackoffMin and BackoffMax is brought within them, a good time
// further ahead than SuccessTTL or a fail time further ahead than
// BackoffMax is brought back to that bound, and expired entries are
// dropped. Unknown fields, and entries of a kind this version does not
// know, are ignored. A version other than 1, a field of the wrong type, a
// negative backoff or malformed JSON is an error wrapping
// ErrInvalidConfig that leaves the cache as it was.
func (cc *CarrierCache) Restore(data []byte) error {
	var snap cacheSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("%w: carrier cache snapshot: %w", ErrInvalidConfig, err)
	}
	if snap.Version == nil || *snap.Version != carrierCacheVersion {
		return fmt.Errorf("%w: carrier cache snapshot not of version %d", ErrInvalidConfig, carrierCacheVersion)
	}
	now := time.Now()
	lo, hi := cc.backoffBounds()
	m := map[cacheKey]*cacheEntry{}
	for _, se := range snap.Entries {
		if se.BackoffMS < 0 {
			return fmt.Errorf("%w: carrier cache snapshot: negative backoff_ms", ErrInvalidConfig)
		}
		good, err := parseCacheTime(se.GoodUntil)
		if err != nil {
			return fmt.Errorf("%w: carrier cache snapshot: %w", ErrInvalidConfig, err)
		}
		fail, err := parseCacheTime(se.FailUntil)
		if err != nil {
			return fmt.Errorf("%w: carrier cache snapshot: %w", ErrInvalidConfig, err)
		}
		i := slices.Index(carrierKindNames[:], se.Kind)
		if i <= 0 {
			continue
		}
		if goodMax := now.Add(cc.successTTL()); good.After(goodMax) {
			good = goodMax
		}
		if failMax := now.Add(hi); fail.After(failMax) {
			fail = failMax
		}
		e := &cacheEntry{goodUntil: good, failUntil: fail, url: se.URL}
		if se.BackoffMS > 0 { // 0: no failure in a row
			e.backoff = min(max(time.Duration(min(se.BackoffMS, int64(hi/time.Millisecond)))*time.Millisecond, lo), hi)
		}
		if e.expired(now, hi) {
			continue
		}
		k := cacheKey{se.Key, CarrierKind(i), se.Variant}
		if o := m[k]; o == nil || e.newer(o) {
			m[k] = e
		}
	}
	cc.mu.Lock()
	if cc.m == nil {
		cc.m = map[cacheKey]*cacheEntry{}
	}
	for k, e := range m {
		if o := cc.m[k]; o == nil || e.newer(o) {
			cc.m[k] = e
		}
	}
	cc.mu.Unlock()
	return nil
}

// CarrierChoiceError reports that DialChoice found no usable candidate, with the
// outcome of every one it tried.
type CarrierChoiceError struct {
	Results []CandidateResult
}

func (e *CarrierChoiceError) Error() string {
	var b strings.Builder
	b.WriteString("zmux: no usable carrier")
	for i, r := range e.Results {
		if i == 0 {
			b.WriteString(": ")
		} else {
			b.WriteString("; ")
		}
		b.WriteString(r.Kind.String())
		b.WriteByte(' ')
		b.WriteString(r.Outcome.String())
		if r.Err != nil {
			b.WriteString(" (" + r.Err.Error() + ")")
		}
	}
	return b.String()
}

// Unwrap returns the candidates' errors.
func (e *CarrierChoiceError) Unwrap() []error {
	var errs []error
	for _, r := range e.Results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errs
}

// DialChoice dials a client session over the carrier the mode selects. ctx
// bounds the dial and the handshakes; each carrier presents
// cfg.Credential. pol is the policy of AutoChoice (nil: the defaults).
//
// ExplicitChoice dials its one candidate as its own dialer does. AutoChoice
// does not use early data (cfg.EarlyData is ignored), so that nothing the
// application sends goes out before a carrier is chosen, and it does not
// race candidates: it skips the kinds the cache holds as failed (unless
// all are) and, when cfg.MaxPipes is above 1, establishes the session on
// the most robust candidate (a kind the cache holds as usable, else the
// exchange carrier, else the last) and joins the preferred ones as further
// pipes in the background, retiring the pipes of the kind they replace
// once one has stayed joined for CarrierChoicePolicy.Settle; it keeps retrying
// preferred kinds as their backoff expires, and joins the best kind
// available again when the active one fails. Without joins (MaxPipes 1,
// Join off, or a peer without multipath) it tries the candidates one after
// another, in order, and keeps the first usable one.
//
// Each try of a candidate is reported as an EventCarrierCandidate, the
// choice as an EventCarrierChosen (again on every switch), and both in
// ConnectionStats. When no candidate is usable, the error is a *CarrierChoiceError.
func DialChoice(ctx context.Context, m CarrierChoice, cfg *Config, pol *CarrierChoicePolicy) (*Conn, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	var cred CarrierCredential
	if cfg != nil {
		cred = cfg.CarrierCredential
	}
	if !m.auto {
		cand := m.candidates[0]
		c, res := startCandidate(ctx, cand, cfg, cred, nil)
		if c == nil {
			if cand.Target != nil {
				cand.Target.Reset()
			}
			return nil, &CarrierChoiceError{Results: []CandidateResult{res}}
		}
		c.reportCarrier(cand, []CandidateResult{res}, true)
		return c, nil
	}
	mp, err := newModePool(m, pol, cred)
	if err != nil {
		return nil, err
	}
	p := &mp.p
	cfg = cfg.Clone()
	cfg.EarlyData = EarlyData{}
	now := time.Now()
	order := mp.usable(now)
	join := !p.noJoin && cfg.MaxPipes > 1
	seq := order
	if join {
		base := baseCandidate(m.candidates, order, p, now)
		seq = append([]int{base}, slices.DeleteFunc(slices.Clone(order), func(i int) bool { return i == base })...)
	}
	var results []CandidateResult
	for _, i := range seq {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cand := m.candidates[i]
		p.cache.prime(p.key, cand)
		c, res := startCandidate(ctx, cand, cfg, cred, p)
		results = append(results, res)
		p.cache.record(p.key, cand, c != nil, time.Now())
		if c == nil {
			continue
		}
		c.reportCarrier(cand, results, true)
		chosen := time.Now()
		if join && c.ConnectionState().Multipath {
			pipe0, _ := c.Pipe(0)
			seed := &modeSeed{mp: mp, pipes: []*Pipe{pipe0}, kind: i, born: chosen}
			labels.Go(c.lctx, "pool", func() { _ = c.maintainPipes(c.Context(), PipePool{Count: 1}, seed) })
			return c, nil
		}
		// One carrier: the session ends with it, and counts against its
		// kind when it fails soon after it was chosen.
		labels.Go(c.lctx, "mode-watch", func() {
			var ae *ApplicationError
			if err := c.Wait(); !errors.As(err, &ae) {
				mp.dropped(i, chosen)
			}
		})
		return c, nil
	}
	return nil, &CarrierChoiceError{Results: results}
}

// validate checks that m has candidates, each with a kind and a Dial, and
// no kind and variant twice.
func (m CarrierChoice) validate() error {
	if len(m.candidates) == 0 {
		return fmt.Errorf("%w: a Mode without a candidate", ErrInvalidConfig)
	}
	seen := map[cacheKey]bool{}
	for _, c := range m.candidates {
		if c.Dial == nil || c.Kind == 0 {
			return fmt.Errorf("%w: candidate without a kind or Dial", ErrInvalidConfig)
		}
		k := candKey("", c)
		if seen[k] {
			return fmt.Errorf("%w: two candidates of kind %v and variant %q", ErrInvalidConfig, c.Kind, c.Variant)
		}
		seen[k] = true
	}
	return nil
}

// baseCandidate is the candidate a session is established on before the
// preferred ones join: a kind the cache holds as usable, else the exchange
// carrier, else the last.
func baseCandidate(cands []Candidate, order []int, p *modeParams, now time.Time) int {
	for _, i := range order {
		if good, _ := p.cache.state(p.key, cands[i], now); good {
			return i
		}
	}
	for _, i := range order {
		if cands[i].Kind == CarrierExchange {
			return i
		}
	}
	return order[len(order)-1]
}

// candidateLife returns a context for the carriers of one try: it ends
// when ctx ends or after bound while the try is under way, and lives on
// once the try succeeded (stop), until the carriers end.
// A try that ends at the bound ends with ErrHandshakeTimeout as the cause
// of life (lifeErr).
func candidateLife(ctx context.Context, bound time.Duration) (life context.Context, cancel context.CancelFunc, stop func() bool) {
	life, cancelCause := context.WithCancelCause(context.WithoutCancel(ctx))
	cancel = func() { cancelCause(nil) }
	stopCtx := context.AfterFunc(ctx, func() { cancelCause(context.Cause(ctx)) })
	t := time.AfterFunc(bound, func() { cancelCause(ErrHandshakeTimeout) })
	return life, cancel, func() bool { return stopCtx() && t.Stop() }
}

// lifeErr is err of a try under life: ErrHandshakeTimeout when life
// ended at its bound and err is only that end.
func lifeErr(life context.Context, err error) error {
	if errors.Is(err, context.Canceled) && errors.Is(context.Cause(life), ErrHandshakeTimeout) {
		return ErrHandshakeTimeout
	}
	return err
}

// startCandidate opens a carrier of cand and starts a session on it. With
// p (auto mode), the session must be established within the candidate's
// timer; a carrier that misses it is closed, which sends nothing.
// dial opens a carrier of the candidate, labelled with its Kind
// (PipeIO.Carrier).
func (c Candidate) dial(ctx context.Context, cred CarrierCredential) (PipeIO, error) {
	p, err := c.Dial(ctx, cred)
	if err == nil {
		p.Carrier = c.Kind
		if c.Variant != "" {
			p.Variant = c.Variant
		}
	}
	return p, err
}

func startCandidate(ctx context.Context, cand Candidate, cfg *Config, cred CarrierCredential, p *modeParams) (*Conn, CandidateResult) {
	res := CandidateResult{Kind: cand.Kind, Variant: cand.Variant}
	life, cancel, stop := candidateLife(ctx, cfg.handshakeTimeout())
	t0 := time.Now()
	pio, err := cand.dial(life, cred)
	if err != nil {
		err = lifeErr(life, err)
		cancel()
		res.Outcome, res.Err = outcomeOf(err), err
		return nil, res
	}
	var expired chan struct{}
	var tm *time.Timer
	if p != nil {
		expired = make(chan struct{})
		tm = time.AfterFunc(p.timer(time.Since(t0)), func() {
			close(expired)
			closePipeIO(pio) // no CONNECTION_CLOSE: the carrier ends
		})
	}
	c, err := Start(life, pio, RoleClient, cfg)
	if tm != nil && !tm.Stop() {
		<-expired
		if c != nil {
			c.Abort(0, "")
		}
		c, err = nil, fmt.Errorf("zmux: %v carrier: no transport parameters within the timer", cand.Kind)
		res.Outcome = CandidateTimedOut
	}
	if c == nil || !stop() {
		if c != nil {
			c.Abort(0, "")
			err = ctx.Err()
			if err == nil {
				err = ErrHandshakeTimeout
			}
		}
		err = lifeErr(life, err)
		cancel()
		closePipeIO(pio)
		if res.Outcome == 0 {
			res.Outcome = outcomeOf(err)
		}
		res.Err = err
		return nil, res
	}
	context.AfterFunc(c.Context(), cancel)
	res.Outcome = CandidateUsable
	return c, res
}

// joinCandidate joins a carrier of cand to c with the options o within the
// candidate's timer; ctx bounds the attempt, and the carriers then live
// until the session ends. A join that the attachment cap still held when
// the timer ended was never tried: its result has no Outcome, and the
// kind is neither failed nor reported.
func joinCandidate(ctx context.Context, c *Conn, cand Candidate, cred CarrierCredential, p *modeParams, o PipeOptions) ([]*Pipe, CandidateResult) {
	res := CandidateResult{Kind: cand.Kind, Variant: cand.Variant, Joined: true}
	life, cancel, stop := candidateLife(ctx, c.cfg.joinTimeout())
	var pipes []*Pipe
	var err error
	if cand.Join != nil {
		jctx, jcancel := context.WithTimeout(ctx, p.timerMin)
		pipes, err = cand.Join(jctx, life, c, cred, o)
		jcancel()
	} else {
		t0 := time.Now()
		var pio PipeIO
		if pio, err = cand.dial(life, cred); err == nil {
			jctx, jcancel := context.WithTimeout(ctx, p.timer(time.Since(t0)))
			var pp *Pipe
			if pp, err = c.AddPipe(jctx, pio, o); err == nil {
				pipes = []*Pipe{pp}
			}
			jcancel()
		}
	}
	if err == nil && !stop() {
		for _, pp := range pipes {
			_ = pp.Abort(ErrHandshakeTimeout)
		}
		err = ErrHandshakeTimeout
	}
	if err != nil {
		err = lifeErr(life, err)
		cancel()
		res.Err = err
		if !errors.Is(err, errJoinHeld) {
			res.Outcome = outcomeOf(err)
		}
		return nil, res
	}
	context.AfterFunc(c.Context(), cancel)
	res.Outcome = CandidateUsable
	return pipes, res
}

// outcomeOf classifies the failure of a try.
func outcomeOf(err error) CandidateOutcome {
	var ht *HandshakeTimeoutError
	switch {
	case errors.Is(err, ErrCarrierRefused), connRefused(err):
		return CandidateRefused
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrHandshakeTimeout), errors.As(err, &ht):
		return CandidateTimedOut
	}
	return CandidateFailed
}

// modePool chooses the carrier kinds of a pipe pool dialed by a mode
// (PipePool.Choice): stable first, then upgrade. A member is dialed over
// the most robust kind available (baseCandidate), and a member of a kind
// a preferred kind could replace is rotated to it, make before break; the
// replacement takes over once it has stayed usable for Settle. A kind
// whose join fails, whose replacement ends before it settled, or whose
// member fails within DropWindow of being joined counts as failed.
type modePool struct {
	cands []Candidate
	auto  bool
	p     modeParams
	cred  CarrierCredential
}

func newModePool(m CarrierChoice, pol *CarrierChoicePolicy, cred CarrierCredential) (*modePool, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	p, err := pol.resolve()
	if err != nil {
		return nil, err
	}
	return &modePool{cands: m.candidates, auto: m.auto, p: p, cred: cred}, nil
}

// usable lists the candidates not backing off, in order (all of them when
// every one is).
func (mp *modePool) usable(now time.Time) []int {
	order := make([]int, 0, len(mp.cands))
	for i, c := range mp.cands {
		if _, until := mp.p.cache.state(mp.p.key, c, now); until.IsZero() {
			order = append(order, i)
		}
	}
	if len(order) == 0 {
		for i := range mp.cands {
			order = append(order, i)
		}
	}
	return order
}

// pick is the kind to dial a new member over: the most robust one
// available.
func (mp *modePool) pick(now time.Time) int {
	if !mp.auto {
		return 0
	}
	return baseCandidate(mp.cands, mp.usable(now), &mp.p, now)
}

// better is the most preferred kind before cur that is not backing off
// (-1: none), which a member of kind cur is rotated to.
func (mp *modePool) better(cur int, now time.Time) int {
	if !mp.auto {
		return -1
	}
	for i := range cur {
		if _, until := mp.p.cache.state(mp.p.key, mp.cands[i], now); until.IsZero() {
			return i
		}
	}
	return -1
}

// join joins a member of kind i to c, reporting the try; a failed try is
// recorded in the cache.
func (mp *modePool) join(ctx context.Context, c *Conn, i int, o PipeOptions) ([]*Pipe, error) {
	cand := mp.cands[i]
	if mp.auto {
		mp.p.cache.prime(mp.p.key, cand)
	}
	pipes, res := joinCandidate(ctx, c, cand, mp.cred, &mp.p, o)
	if res.Outcome == 0 {
		return nil, res.Err // held back by the cap: deferred, not failed
	}
	c.reportCarrier(cand, []CandidateResult{res}, false)
	if pipes == nil {
		mp.record(i, false)
		return nil, res.Err
	}
	return pipes, nil
}

func (mp *modePool) record(i int, ok bool) {
	if mp.auto {
		mp.p.cache.record(mp.p.key, mp.cands[i], ok, time.Now())
	}
}

// established records kind i as usable and reports it as the session's
// carrier when it is not already.
func (mp *modePool) established(c *Conn, i int) {
	mp.record(i, true)
	cand := mp.cands[i]
	c.lock()
	cur, curV := c.carrier, c.carrierVariant
	c.unlock()
	if cur != cand.Kind || curV != cand.Variant {
		c.reportCarrier(cand, nil, true)
	}
}

// dropped records kind i as failed when its member was joined (at born)
// less than DropWindow ago.
func (mp *modePool) dropped(i int, born time.Time) {
	if time.Since(born) < mp.p.drop {
		mp.record(i, false)
	}
}

// modeSeed hands MaintainPipes the pipes a session dialed by DialChoice was
// established on, as the pool's first member, with the pool's mode.
type modeSeed struct {
	mp    *modePool
	pipes []*Pipe
	kind  int
	born  time.Time
}

// carrierTriesKept is the number of the latest tries that ConnectionStats
// reports.
const carrierTriesKept = 64

// watchPipes registers sig, once, to be woken on the next pipe state
// change or suspension of the session (under the lock).
func (c *Conn) watchPipes(sig *core.Signal) {
	if !slices.Contains(c.pipeWaiters, sig) {
		c.pipeWaiters = append(c.pipeWaiters, sig)
	}
}

// unwatchPipes removes sig from the pipe state waiters.
func (c *Conn) unwatchPipes(sig *core.Signal) {
	c.lock()
	defer c.unlock()
	if i := slices.Index(c.pipeWaiters, sig); i >= 0 {
		c.pipeWaiters = slices.Delete(c.pipeWaiters, i, i+1)
	}
}

// reportCarrier records the results of tries and, with chosen, the choice
// of kind, as events and in the statistics.
func (c *Conn) reportCarrier(cand Candidate, results []CandidateResult, chosen bool) {
	c.lock()
	defer c.unlock()
	q := &c.events
	for _, r := range results {
		if len(c.carrierTries) == carrierTriesKept {
			c.carrierTries = append(c.carrierTries[:0], c.carrierTries[1:]...)
		}
		c.carrierTries = append(c.carrierTries, r)
		ev := Event{Kind: EventCarrierCandidate, Carrier: r.Kind, Variant: r.Variant, Outcome: r.Outcome}
		q.push(ev)
		c.hk.trace(traceItem{ev: ev})
	}
	if chosen {
		c.carrier, c.carrierVariant = cand.Kind, cand.Variant
		ev := Event{Kind: EventCarrierChosen, Carrier: cand.Kind, Variant: cand.Variant}
		q.push(ev)
		c.hk.trace(traceItem{ev: ev})
	}
	for _, w := range q.waiters {
		w.Fire()
	}
	q.waiters = q.waiters[:0]
}
