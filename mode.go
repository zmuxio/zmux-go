package zmux

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Carrier selection (API.md 4.8): DialMode dials a session over one carrier
// kind (ExplicitMode) or chooses among candidates (AutoMode), for paths
// through reverse proxies and CDNs where which kinds pass depends on the
// path. Every kind keeps its own dialer; a Candidate wraps one. A kind
// describes only this end's hop of a carrier, not what lies beyond its
// first peer.

// CarrierKind names a kind of carrier.
type CarrierKind uint8

const (
	// CarrierWebSocket is a WebSocket (zmuxws.Candidate).
	CarrierWebSocket CarrierKind = iota + 1
	// CarrierStreaming is a bidirectional streaming request: one request
	// whose body is the uplink and whose response body the downlink
	// (StreamingCandidate).
	CarrierStreaming
	// CarrierSplit is a split bootstrap: an uplink POST and a downlink GET
	// (SplitCandidate).
	CarrierSplit
	// CarrierExchange is the exchange carrier of bounded requests
	// (ExchangeCandidate).
	CarrierExchange
	// CarrierQUIC is a QUIC stream (zmuxquic.Candidate), direct only.
	CarrierQUIC
	// CarrierHTTP3 is a streaming request over HTTP/3
	// (zmuxh3.HTTP3Candidate).
	CarrierHTTP3
	// CarrierWebTransport is a WebTransport stream
	// (zmuxh3.WebTransportCandidate), direct only.
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
	Kind    CarrierKind
	Outcome CandidateOutcome
	// Joined is set for a try as a further pipe of the established session.
	Joined bool
	// Err is the failure (nil when usable).
	Err error
}

// Candidate is one carrier kind that DialMode may use. Dial opens a
// carrier of the kind, presenting cred (the session's Config.Credential)
// for the server's admission; DialMode sets its PipeIO.Carrier to Kind.
// The carrier must live until the pipe ends, not until ctx does, which
// bounds only the opening. The constructors of each adapter return ready
// candidates.
type Candidate struct {
	Kind CarrierKind
	Dial func(ctx context.Context, cred Credential) (PipeIO, error)
	// Target, for a kind dialed at a URL, is where its carriers start and
	// what redirects led them to (nil: not known). DialMode keeps it per
	// server in ModePolicy.Cache, and starts it at the base URL again
	// after a failure.
	Target *HTTPTarget

	// join joins a carrier of the kind to c, for a kind whose Dial does
	// not open a carrier that can join (a split bootstrap); nil: AddPipe
	// on a carrier from Dial. ctx bounds the attempt, life the carriers.
	join func(ctx, life context.Context, c *Conn, cred Credential, o PipeOptions) ([]*Pipe, error)
}

// StreamingCandidate is a bidirectional streaming request (HTTPPipe) to
// url with client and the fields h. It passes intermediaries that forward
// both bodies at once: HTTP/2 or HTTP/3 to the server, or HTTP/1.1 hops
// that do not buffer request bodies.
func StreamingCandidate(client *http.Client, url string, h http.Header) Candidate {
	t := NewHTTPTarget(url)
	return Candidate{Kind: CarrierStreaming, Target: t, Dial: func(ctx context.Context, cred Credential) (PipeIO, error) {
		return httpPipe(ctx, client, url, t, h, DirBidirectional, cred)
	}}
}

// SplitCandidate is a split bootstrap (HTTPSplitPipe) to url with client
// and the fields h. As a further pipe of a session it joins as an uplink
// POST and a downlink GET joined out of band, two pipes. Its carriers must
// be confidential end to end or up to a trusted TLS-terminating
// intermediary.
func SplitCandidate(client *http.Client, url string, h http.Header) Candidate {
	t := NewHTTPTarget(url)
	return Candidate{Kind: CarrierSplit, Target: t,
		Dial: func(ctx context.Context, cred Credential) (PipeIO, error) {
			return httpSplitPipe(ctx, client, url, t, h, cred)
		},
		join: func(ctx, _ context.Context, c *Conn, cred Credential, o PipeOptions) ([]*Pipe, error) {
			return httpSplitJoin(ctx, c, client, url, t, h, cred, o)
		}}
}

// ExchangeCandidate is an exchange carrier (HTTPExchangePipe) to url with
// client and cfg. It passes intermediaries that buffer whole bodies; it
// authenticates with cfg's PSK, and the session's credential is not
// presented on it.
func ExchangeCandidate(client *http.Client, url string, cfg *HTTPExchangeConfig) Candidate {
	t := NewHTTPTarget(url)
	return Candidate{Kind: CarrierExchange, Target: t, Dial: func(ctx context.Context, _ Credential) (PipeIO, error) {
		return httpExchangePipe(ctx, client, url, t, cfg)
	}}
}

// Mode is how DialMode, or a PipePool, chooses the carrier: ExplicitMode
// or AutoMode.
type Mode struct {
	auto       bool
	candidates []Candidate
}

// ExplicitMode dials c only: a failure is reported, never replaced by
// another kind.
func ExplicitMode(c Candidate) Mode { return Mode{candidates: []Candidate{c}} }

// AutoMode chooses among the candidates, in their order of preference
// (most preferred first).
func AutoMode(c ...Candidate) Mode { return Mode{auto: true, candidates: slices.Clone(c)} }

// ModePolicy is the local policy of AutoMode; the zero value of every
// field selects its default, and a nil *ModePolicy is all defaults.
type ModePolicy struct {
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
	// NoJoin turns off establishing on the most robust candidate and
	// joining the preferred ones: the candidates are then tried one after
	// another, in order.
	NoJoin bool
}

type modeParams struct {
	cache              *CarrierCache
	key                string
	factor             float64
	timerMin, timerMax time.Duration
	settle, drop       time.Duration
	noJoin             bool
}

func (p *ModePolicy) resolve() (modeParams, error) {
	var x ModePolicy
	if p != nil {
		x = *p
	}
	r := modeParams{cache: x.Cache, key: x.Key, factor: x.TimerFactor, timerMin: x.TimerMin,
		timerMax: x.TimerMax, settle: x.Settle, drop: x.DropWindow, noJoin: x.NoJoin}
	if r.cache == nil {
		r.cache = &CarrierCache{}
	}
	if cc := r.cache; !(r.factor >= 0) || r.timerMin < 0 || r.timerMax < 0 || r.settle < 0 || r.drop < 0 ||
		cc.SuccessTTL < 0 || cc.BackoffMin < 0 || cc.BackoffMax < 0 {
		return r, fmt.Errorf("%w: negative or NaN ModePolicy or CarrierCache field", ErrInvalidConfig)
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
// network (ModePolicy.Key): a usable kind for SuccessTTL, a failed one is
// skipped for a backoff that doubles with every failure in a row, from
// BackoffMin to BackoffMax. A kind that fails soon after it was chosen
// (ModePolicy.DropWindow) counts as failed, so that a path that lets a
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
	key  string
	kind CarrierKind
}

type cacheEntry struct {
	goodUntil time.Time
	failUntil time.Time
	backoff   time.Duration
	url       string // where redirects led the usable kind ("": its base)
}

// prime starts cand's target at the URL the cache remembers for it.
func (cc *CarrierCache) prime(key string, cand Candidate) {
	if cand.Target == nil {
		return
	}
	cc.mu.Lock()
	var u string
	if e := cc.m[cacheKey{key, cand.Kind}]; e != nil {
		u = e.url
	}
	cc.mu.Unlock()
	if u != "" {
		cand.Target.Redirected(u)
	}
}

// state reports what the cache knows of kind under key at now.
func (cc *CarrierCache) state(key string, kind CarrierKind, now time.Time) (good bool, failUntil time.Time) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	e := cc.m[cacheKey{key, kind}]
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
	kind := cand.Kind
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.m == nil {
		cc.m = map[cacheKey]*cacheEntry{}
	}
	k := cacheKey{key, kind}
	e := cc.m[k]
	if e == nil {
		e = &cacheEntry{}
		cc.m[k] = e
	}
	if ok {
		*e = cacheEntry{goodUntil: now.Add(cc.successTTL())}
		if cand.Target != nil && cand.Target.URL() != cand.Target.Base() {
			e.url = cand.Target.URL()
		}
		return
	}
	e.url = ""
	if cand.Target != nil {
		cand.Target.Reset()
	}
	lo, hi := cc.backoffBounds()
	e.backoff = min(max(2*e.backoff, lo), hi)
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
			BackoffMS: e.backoff.Milliseconds(), URL: e.url})
	}
	cc.mu.Unlock()
	slices.SortFunc(snap.Entries, func(a, b cacheSnapshotEntry) int {
		return cmp.Or(strings.Compare(a.Key, b.Key), strings.Compare(a.Kind, b.Kind))
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
		k := cacheKey{se.Key, CarrierKind(i)}
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

// ModeError reports that DialMode found no usable candidate, with the
// outcome of every one it tried.
type ModeError struct {
	Results []CandidateResult
}

func (e *ModeError) Error() string {
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
func (e *ModeError) Unwrap() []error {
	var errs []error
	for _, r := range e.Results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errs
}

// DialMode dials a client session over the carrier the mode selects. ctx
// bounds the dial and the handshakes; each carrier presents
// cfg.Credential. pol is the policy of AutoMode (nil: the defaults).
//
// ExplicitMode dials its one candidate as its own dialer does. AutoMode
// does not use early data (cfg.EarlyData is ignored), so that nothing the
// application sends goes out before a carrier is chosen, and it does not
// race candidates: it skips the kinds the cache holds as failed (unless
// all are) and, when cfg.MaxPipes is above 1, establishes the session on
// the most robust candidate (a kind the cache holds as usable, else the
// exchange carrier, else the last) and joins the preferred ones as further
// pipes in the background, retiring the pipes of the kind they replace
// once one has stayed joined for ModePolicy.Settle; it keeps retrying
// preferred kinds as their backoff expires, and joins the best kind
// available again when the active one fails. Without joins (MaxPipes 1,
// NoJoin, or a peer without multipath) it tries the candidates one after
// another, in order, and keeps the first usable one.
//
// Each try of a candidate is reported as an EventCarrierCandidate, the
// choice as an EventCarrierChosen (again on every switch), and both in
// ConnectionStats. When no candidate is usable, the error is a *ModeError.
func DialMode(ctx context.Context, m Mode, cfg *Config, pol *ModePolicy) (*Conn, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	var cred Credential
	if cfg != nil {
		cred = cfg.Credential
	}
	if !m.auto {
		cand := m.candidates[0]
		c, res := startCandidate(ctx, cand, cfg, cred, nil)
		if c == nil {
			if cand.Target != nil {
				cand.Target.Reset()
			}
			return nil, &ModeError{Results: []CandidateResult{res}}
		}
		c.reportCarrier(cand.Kind, []CandidateResult{res}, true)
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
		c.reportCarrier(cand.Kind, results, true)
		chosen := time.Now()
		if join && c.ConnectionState().Multipath {
			pipe0, _ := c.Pipe(0)
			seed := &modeSeed{mp: mp, pipes: []*Pipe{pipe0}, kind: i, born: chosen}
			go func() { _ = c.maintainPipes(c.Context(), PipePool{Count: 1}, seed) }()
			return c, nil
		}
		// One carrier: the session ends with it, and counts against its
		// kind when it fails soon after it was chosen.
		go func() {
			var ae *ApplicationError
			if err := c.Wait(); !errors.As(err, &ae) {
				mp.dropped(i, chosen)
			}
		}()
		return c, nil
	}
	return nil, &ModeError{Results: results}
}

// validate checks that m has candidates, each with a kind and a Dial, and
// no kind twice.
func (m Mode) validate() error {
	if len(m.candidates) == 0 {
		return fmt.Errorf("%w: a Mode without a candidate", ErrInvalidConfig)
	}
	var kinds [256]bool
	for _, c := range m.candidates {
		if c.Dial == nil || c.Kind == 0 {
			return fmt.Errorf("%w: candidate without a kind or Dial", ErrInvalidConfig)
		}
		if kinds[c.Kind] {
			return fmt.Errorf("%w: two candidates of kind %v", ErrInvalidConfig, c.Kind)
		}
		kinds[c.Kind] = true
	}
	return nil
}

// baseCandidate is the candidate a session is established on before the
// preferred ones join: a kind the cache holds as usable, else the exchange
// carrier, else the last.
func baseCandidate(cands []Candidate, order []int, p *modeParams, now time.Time) int {
	for _, i := range order {
		if good, _ := p.cache.state(p.key, cands[i].Kind, now); good {
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
func (c Candidate) dial(ctx context.Context, cred Credential) (PipeIO, error) {
	p, err := c.Dial(ctx, cred)
	if err == nil {
		p.Carrier = c.Kind
	}
	return p, err
}

func startCandidate(ctx context.Context, cand Candidate, cfg *Config, cred Credential, p *modeParams) (*Conn, CandidateResult) {
	res := CandidateResult{Kind: cand.Kind}
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
			c.closeWith(0, "")
		}
		c, err = nil, fmt.Errorf("zmux: %v carrier: no transport parameters within the timer", cand.Kind)
		res.Outcome = CandidateTimedOut
	}
	if c == nil || !stop() {
		if c != nil {
			c.closeWith(0, "")
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
// until the session ends.
func joinCandidate(ctx context.Context, c *Conn, cand Candidate, cred Credential, p *modeParams, o PipeOptions) ([]*Pipe, CandidateResult) {
	res := CandidateResult{Kind: cand.Kind, Joined: true}
	life, cancel, stop := candidateLife(ctx, c.cfg.handshakeTimeout())
	var pipes []*Pipe
	var err error
	if cand.join != nil {
		jctx, jcancel := context.WithTimeout(ctx, p.timerMin)
		pipes, err = cand.join(jctx, life, c, cred, o)
		jcancel()
	} else {
		t0 := time.Now()
		var pio PipeIO
		if pio, err = cand.dial(life, cred); err == nil {
			jctx, jcancel := context.WithTimeout(ctx, p.timer(time.Since(t0)))
			var pp *Pipe
			if pp, err = c.AddPipe(jctx, pio, o); err == nil {
				pipes = []*Pipe{pp}
			} else {
				closePipeIO(pio)
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
		res.Outcome, res.Err = outcomeOf(err), err
		return nil, res
	}
	context.AfterFunc(c.Context(), cancel)
	res.Outcome = CandidateUsable
	return pipes, res
}

// outcomeOf classifies the failure of a try.
func outcomeOf(err error) CandidateOutcome {
	var se *HTTPStatusError
	var ht *HandshakeTimeoutError
	var re *HTTPRedirectError
	switch {
	case errors.As(err, &se), errors.As(err, &re), errors.Is(err, syscall.ECONNREFUSED):
		return CandidateRefused
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrHandshakeTimeout), errors.As(err, &ht):
		return CandidateTimedOut
	}
	return CandidateFailed
}

// modePool chooses the carrier kinds of a pipe pool dialed by a mode
// (PipePool.Mode): stable first, then upgrade. A member is dialed over
// the most robust kind available (baseCandidate), and a member of a kind
// a preferred kind could replace is rotated to it, make before break; the
// replacement takes over once it has stayed usable for Settle. A kind
// whose join fails, whose replacement ends before it settled, or whose
// member fails within DropWindow of being joined counts as failed.
type modePool struct {
	cands []Candidate
	auto  bool
	p     modeParams
	cred  Credential
}

func newModePool(m Mode, pol *ModePolicy, cred Credential) (*modePool, error) {
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
		if _, until := mp.p.cache.state(mp.p.key, c.Kind, now); until.IsZero() {
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
		if _, until := mp.p.cache.state(mp.p.key, mp.cands[i].Kind, now); until.IsZero() {
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
	c.reportCarrier(cand.Kind, []CandidateResult{res}, false)
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
	k := mp.cands[i].Kind
	c.lock()
	cur := c.carrier
	c.unlock()
	if cur != k {
		c.reportCarrier(k, nil, true)
	}
}

// dropped records kind i as failed when its member was joined (at born)
// less than DropWindow ago.
func (mp *modePool) dropped(i int, born time.Time) {
	if time.Since(born) < mp.p.drop {
		mp.record(i, false)
	}
}

// modeSeed hands MaintainPipes the pipes a session dialed by DialMode was
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
func (c *Conn) reportCarrier(kind CarrierKind, results []CandidateResult, chosen bool) {
	c.lock()
	defer c.unlock()
	q := &c.events
	for _, r := range results {
		if len(c.carrierTries) == carrierTriesKept {
			c.carrierTries = append(c.carrierTries[:0], c.carrierTries[1:]...)
		}
		c.carrierTries = append(c.carrierTries, r)
		ev := Event{Kind: EventCarrierCandidate, Carrier: r.Kind, Outcome: r.Outcome}
		q.push(ev)
		c.hk.trace(traceItem{ev: ev})
	}
	if chosen {
		c.carrier = kind
		ev := Event{Kind: EventCarrierChosen, Carrier: kind}
		q.push(ev)
		c.hk.trace(traceItem{ev: ev})
	}
	for _, w := range q.waiters {
		w.Fire()
	}
	q.waiters = q.waiters[:0]
}
