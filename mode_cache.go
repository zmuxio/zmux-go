package zmux

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"weak"

	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/internal/core"
)

const carrierCacheVersion = 2

const (
	shortDropWindow = 10 * time.Minute
	shortDropFloor  = 10 * time.Minute
)

var (
	// ErrCarrierCacheVersion rejects unsupported snapshot versions.
	ErrCarrierCacheVersion = errors.New("zmux: unsupported carrier cache version")
	// ErrCarrierCacheFormat rejects malformed cache encodings.
	ErrCarrierCacheFormat = errors.New("zmux: malformed carrier cache")
	// ErrCarrierCacheValue rejects invalid cache fields or observations.
	ErrCarrierCacheValue = errors.New("zmux: invalid carrier cache value")
	// ErrStaleCarrierReport indicates a sequence outside its source replay window.
	ErrStaleCarrierReport = errors.New("zmux: carrier report fell outside the replay window")
	// ErrCarrierCooling means that a candidate cannot be tried yet.
	ErrCarrierCooling = errors.New("zmux: carrier candidate cooling")
	// ErrCarrierInFlight means this key already reserved an attempt.
	ErrCarrierInFlight = errors.New("zmux: carrier candidate already in flight")
	// ErrPeerRetry means an authenticated peer deadline blocks the candidate.
	ErrPeerRetry = errors.New("zmux: peer_retry")
)

// CarrierCacheOptions configures one cache. NewCarrierCache snapshots the
// options, including the kind weights.
type CarrierCacheOptions struct {
	// OrdinarySamples and RecoverySamples optionally supply independent uniform
	// fractions for jitter. Each advancement consumes one available fraction.
	// An empty source uses the standard random generator.
	OrdinarySamples, RecoverySamples                     <-chan float64
	KindPriors                                           map[CarrierKind]float64
	ClockDomain                                          *ClockDomain
	Backoff                                              Backoff
	CooldownProfile                                      CooldownProfile
	HalfLife, DropWindow, PerformanceWindow, HealthReset time.Duration
}

// CarrierCache owns scoring and cooling for each (key, kind, variant).
// Construct a controller cache with NewCarrierCache to bind its clock domain.
type CarrierCache struct {
	clockFloor          time.Time
	clockDomain         *ClockDomain
	m                   map[CarrierCacheKey]*cacheEntry
	faults              map[physicalFaultKey]map[uint64]struct{}
	mu                  sync.Mutex
	params              *cacheParams
	changed             chan struct{}
	waitSample          func(ResolvedBackoff, time.Duration) time.Duration
	nextEpisode         uint64
	cooldownSubscribers map[*CooldownSubscription]struct{}
}

// NewCarrierCache validates and freezes the supplied configuration.
func NewCarrierCache(options CarrierCacheOptions) (*CarrierCache, error) {
	params, err := options.compile()
	if err != nil {
		return nil, err
	}
	return &CarrierCache{clockDomain: options.ClockDomain, params: &params}, nil
}

// CooldownProfile selects the candidate cooldown defaults.
type CooldownProfile uint8

const (
	// CooldownDefault uses ordinary Choice backoff without a short-drop floor.
	CooldownDefault CooldownProfile = iota
	// CooldownShortDropResilient enables a floor after three recent short drops.
	CooldownShortDropResilient
)

func candKey(key string, c Candidate) CarrierCacheKey {
	return CarrierCacheKey{Key: key, Kind: c.Kind, Variant: c.Variant}
}

type cacheEntry struct {
	cooling   cacheCooling
	episode   uint64
	recovery  RecoveryCadence
	demands   map[*Conn]bool
	waiters   []*Conn
	token     uint64
	peerRetry time.Time
	rtt, rate cachePerformance
	history   cacheHistory
	inFlight  bool
	started   bool
	counters  retryCounters
	health    *cooldownHealth
}

// cachePoint keeps an original timestamp for merge ordering and the local
// timestamp used for elapsed-time calculations. Its exported age survives a
// restart whose wall clock is behind the snapshot's.
type cachePoint struct {
	At    time.Time `json:"at"`
	local time.Time
	AgeNS time.Duration `json:"age_ns"`
}

func (p cachePoint) measuredAt() time.Time {
	if !p.local.IsZero() {
		return p.local
	}
	return p.At
}

func (p cachePoint) withAge(now time.Time) cachePoint {
	if !p.At.IsZero() {
		p.AgeNS = max(now.Sub(p.measuredAt()), 0)
	}
	return p
}

func (p cachePoint) reanchor(now time.Time, elapsed time.Duration) cachePoint {
	if !p.At.IsZero() {
		p.local = now.Add(-(min(p.AgeNS, time.Duration(math.MaxInt64)-elapsed) + elapsed))
	}
	return p
}

type cacheHistory struct {
	cachePoint
	Attempts    uint64  `json:"attempts"`
	Success     float64 `json:"success"`
	Failure     float64 `json:"failure"`
	Survived    float64 `json:"survived"`
	ShortDrop   float64 `json:"short_drop"`
	LateDrop    float64 `json:"late_drop"`
	LateSeconds float64 `json:"late_seconds"`
}

type cacheCooling struct {
	reason CooldownReason
	cachePoint
	Level   int64
	nominal time.Duration
	Short   []cachePoint
	until   time.Time
}

type cacheCoolingSnapshot struct {
	cachePoint
	Level       int64        `json:"level,omitempty"`
	NominalMS   int64        `json:"nominal_ms"`
	RemainingMS int64        `json:"remaining_ms"`
	Short       []cachePoint `json:"short,omitempty"`
}

type cachePerformance struct {
	cachePoint
	Value    float64       `json:"value"`
	ValidFor time.Duration `json:"valid_for_ns,omitempty"`
	source   string
}

func (p cachePerformance) evidence(now time.Time, window time.Duration) PerformanceEvidence {
	if !p.fresh(now, window) {
		return PerformanceEvidence{}
	}
	if p.ValidFor > 0 {
		window = p.ValidFor
	}
	source := p.source
	if source == "" {
		source = "snapshot"
	}
	return PerformanceEvidence{At: p.At, Age: now.Sub(p.measuredAt()), ValidUntil: p.measuredAt().Add(window), Value: p.Value, Source: source}
}

func (p cachePerformance) fresh(now time.Time, window time.Duration) bool {
	if p.ValidFor > 0 {
		window = p.ValidFor
	}
	return p.Value > 0 && now.Sub(p.measuredAt()) < window
}

type cacheParams struct {
	ordinarySamples, recoverySamples                     <-chan float64
	halfLife, dropWindow, performanceWindow, healthReset time.Duration
	backoff                                              ResolvedBackoff
	shortDropResilient                                   bool
	kindPriors                                           [len(carrierKindNames)]float64
}

var defaultCacheParams = func() cacheParams {
	params, err := (CarrierCacheOptions{}).compile()
	if err != nil {
		panic(err)
	}
	return params
}()

func (cc *CarrierCache) config() *cacheParams {
	if cc.params == nil {
		return &defaultCacheParams
	}
	return cc.params
}

func (o CarrierCacheOptions) compile() (cacheParams, error) {
	if o.HalfLife < 0 || o.DropWindow < 0 || o.PerformanceWindow < 0 || o.HealthReset < 0 {
		return cacheParams{}, fmt.Errorf("%w: negative carrier cache duration", ErrInvalidConfig)
	}
	var priors [len(carrierKindNames)]float64
	for kind, prior := range o.KindPriors {
		if kind == 0 || int(kind) >= len(carrierKindNames) || !(prior > 0) || math.IsInf(prior, 0) {
			return cacheParams{}, fmt.Errorf("%w: invalid carrier kind prior", ErrInvalidConfig)
		}
		priors[kind] = prior
	}
	defaults := Backoff{Min: time.Minute, Max: 6 * time.Hour, Jitter: Set(DefaultBackoffJitter)}
	switch o.CooldownProfile {
	case CooldownDefault:
	case CooldownShortDropResilient:
		if o.Backoff != (Backoff{}) {
			return cacheParams{}, fmt.Errorf("%w: the short-drop cooldown profile owns backoff values", ErrInvalidConfig)
		}
		defaults = Backoff{Min: 30 * time.Second, Max: 30 * time.Minute, Jitter: Set(0.2), JitterUp: Set(0.2)}
	default:
		return cacheParams{}, fmt.Errorf("%w: unknown cooldown profile", ErrInvalidConfig)
	}
	bo, err := o.Backoff.WithDefaults(defaults)
	if err != nil {
		return cacheParams{}, err
	}
	duration := func(value, fallback time.Duration) time.Duration {
		if value == 0 {
			return fallback
		}
		return value
	}
	return cacheParams{ordinarySamples: o.OrdinarySamples, recoverySamples: o.RecoverySamples,
		halfLife: duration(o.HalfLife, 6*time.Hour), dropWindow: duration(o.DropWindow, time.Minute),
		performanceWindow: duration(o.PerformanceWindow, time.Minute), healthReset: duration(o.HealthReset, 10*time.Minute),
		backoff: bo, kindPriors: priors, shortDropResilient: o.CooldownProfile == CooldownShortDropResilient,
	}, nil
}

// CacheOutcome classifies a single attempt or a later observation of its member.
type CacheOutcome uint8

// Carrier attempt and subsequent observation outcomes.
const (
	CacheUsable CacheOutcome = iota
	CacheFailed
	CacheRejected
	CacheSurvived
	CacheShortDrop
	CacheLateDrop
	CacheLifetime
	CachePerformance
	CacheRecovered
)

// CarrierObservation adds original time, interval and protocol performance to
// a report. Rate is in bytes per second. Zero RTT or Rate means absent.
// ValidFor optionally caps the age of this performance evidence further.
type CarrierObservation struct {
	At       time.Time
	Duration time.Duration
	RTT      time.Duration
	Rate     float64
	ValidFor time.Duration
}

type cacheFeedback struct {
	settledHandshake bool
	source           *carrierReportState
	applied          *bool
	fault            *core.PhysicalFault
	journal          *outageJournal
	retry            time.Duration
	sequence         uint64
}

func (f cacheFeedback) performanceSource() string {
	if f.source != nil {
		return "host_report"
	}
	return "protocol_ack"
}

type physicalFaultKey struct {
	candidate CarrierCacheKey
	carrier   carrierstats.WeakCarrierID
}

type faultCleanup struct {
	cache weak.Pointer[CarrierCache]
	key   physicalFaultKey
}

func forgetPhysicalFaults(f faultCleanup) {
	if cc := f.cache.Value(); cc != nil {
		cc.mu.Lock()
		delete(cc.faults, f.key)
		cc.mu.Unlock()
	}
}

// newPhysicalFaultLocked records distinct generations even if different
// sessions deliver the observations out of order.
func (cc *CarrierCache) newPhysicalFaultLocked(k CarrierCacheKey, f *core.PhysicalFault) bool {
	if f == nil || f.CarrierID == (CarrierID{}) || f.Generation == 0 {
		return true
	}
	key := physicalFaultKey{k, carrierstats.Weaken(f.CarrierID)}
	seen := cc.faults[key]
	if seen == nil {
		if cc.faults == nil {
			cc.faults = make(map[physicalFaultKey]map[uint64]struct{})
		}
		seen = make(map[uint64]struct{})
		cc.faults[key] = seen
		carrierstats.OnCarrierRelease(f.CarrierID, forgetPhysicalFaults, faultCleanup{weak.Make(cc), key})
	}
	_, duplicate := seen[f.Generation]
	seen[f.Generation] = struct{}{}
	return !duplicate
}

// carrierScoreOrder groups neighbouring D160 estimates for deterministic
// ordering across implementations. The original values still drive the
// upgrade threshold and appear in snapshots.
func carrierScoreOrder(score float64) float64 {
	return math.RoundToEven(score * 1e9)
}

// PerformanceEvidence reports the original time, local age and expiry of
// one measured component. Value uses seconds for RTT and bytes/s for rate.
type PerformanceEvidence struct {
	At, ValidUntil time.Time
	Age            time.Duration
	Value          float64
	Source         string
}

// CarrierScore is the current candidate score and its effective evidence.
type CarrierScore struct {
	HandshakeProbability, Stability, SurvivalWeight, ShortDropWeight, LateDropWeight, ExposureSeconds, KindPrior float64
	RTT, Rate                                                                                                    PerformanceEvidence
	CooldownReason                                                                                               CooldownReason
	CoolingUntil                                                                                                 time.Time
	PeerRetryUntil                                                                                               time.Time
	Score, UpgradeScore, Weight, SuccessWeight                                                                   float64
	Recovery                                                                                                     RecoveryCadence
	Attempts                                                                                                     uint64
	Age                                                                                                          time.Duration
	// CooldownLevel is the number of committed cooldown rounds since the
	// last health reset. It stays set after the sampled wait expires.
	CooldownLevel int64
}

func (s CarrierScore) retryAt() time.Time {
	if s.PeerRetryUntil.After(s.CoolingUntil) {
		return s.PeerRetryUntil
	}
	return s.CoolingUntil
}

func (cc *CarrierCache) nowLocked() time.Time {
	now := cc.clockDomain.clockSource().Now()
	if now.Before(cc.clockFloor) {
		return cc.clockFloor
	}
	cc.clockFloor = now
	return now
}

func decayAge(age, halfLife time.Duration) float64 {
	return math.Exp2(-float64(max(age, 0)) / float64(halfLife))
}

func (h cacheHistory) decay(now time.Time, halfLife time.Duration) cacheHistory {
	if h.At.IsZero() {
		return h
	}
	f := decayAge(now.Sub(h.measuredAt()), halfLife)
	h.Success *= f
	h.Failure *= f
	h.Survived *= f
	h.ShortDrop *= f
	h.LateDrop *= f
	h.LateSeconds *= f
	return h
}

// Score returns the same inputs used by carrier selection. Reads do not
// refresh an observation's original time.
func (cc *CarrierCache) Score(key string, kind CarrierKind, variant string) CarrierScore {
	p := cc.config()
	cc.mu.Lock()
	defer cc.mu.Unlock()
	now := cc.nowLocked()
	return cc.scoreLocked(CarrierCacheKey{Key: key, Kind: kind, Variant: variant}, p, now)
}

func (cc *CarrierCache) scoreLocked(key CarrierCacheKey, p *cacheParams, now time.Time) CarrierScore {
	score := cacheScore(cc.m[key], p, now)
	if int(key.Kind) < len(p.kindPriors) {
		if prior := p.kindPriors[key.Kind]; prior != 0 {
			score.KindPrior = prior
			score.Score *= prior
		}
	}
	return score
}

// keyReservation represents a grant through opener return and logical completion.
type keyReservation struct {
	session *Conn
	cache   *CarrierCache
	entry   *cacheEntry
	token   uint64
	intent  AttemptCause
	cutoff  time.Time
}

func (p keyReservation) start() error {
	p.cache.mu.Lock()
	defer p.cache.mu.Unlock()
	e := p.entry
	if !e.inFlight || p.token != e.token || e.started {
		return errControllerStaleStart
	}
	if _, err := e.admission(p.cache.nowLocked(), p.intent, p.token, p.cutoff); err != nil {
		return err
	}
	e.started = true
	if _, live := e.demands[p.session]; live {
		e.demands[p.session] = false
	}
	incrementRetryCount(&e.counters.starts)
	return nil
}

func (p keyReservation) release() {
	p.cache.mu.Lock()
	defer p.cache.mu.Unlock()
	if p.entry.inFlight && p.entry.token == p.token {
		p.entry.inFlight, p.entry.started = false, false
		p.cache.signalLocked()
	}
}

func cacheScore(e *cacheEntry, p *cacheParams, now time.Time) CarrierScore {
	if e == nil {
		return CarrierScore{Score: .125, UpgradeScore: .125, HandshakeProbability: .5, Stability: .5, KindPrior: 1}
	}
	h := e.history.decay(now, p.halfLife)
	pSucc := (1 + h.Success) / (2 + h.Success + h.Failure)
	pLive := (1 + h.Survived) / (2 + h.Survived + h.ShortDrop)
	frequency := h.LateDrop / (p.dropWindow.Seconds() + h.LateSeconds)
	stab := pLive / (1 + frequency*p.dropWindow.Seconds())
	rtt, rate := 1.0, 1.0
	if e.rtt.fresh(now, p.performanceWindow) {
		rtt = max(.5, min(2, .1/e.rtt.Value))
	}
	if e.rate.fresh(now, p.performanceWindow) {
		rate = max(.5, min(2, e.rate.Value/1250000))
	}
	product := pSucc * stab * stab
	s := CarrierScore{HandshakeProbability: pSucc, Stability: stab, SurvivalWeight: h.Survived, ShortDropWeight: h.ShortDrop, LateDropWeight: h.LateDrop, ExposureSeconds: h.LateSeconds, KindPrior: 1, RTT: e.rtt.evidence(now, p.performanceWindow), Rate: e.rate.evidence(now, p.performanceWindow), CooldownReason: e.cooling.reason, UpgradeScore: product, Recovery: e.recovery, Score: product * math.Sqrt(rtt*rate),
		Attempts: h.Attempts, Weight: h.Success + h.Failure, SuccessWeight: h.Success, CooldownLevel: e.cooling.Level}
	if !h.At.IsZero() {
		s.Age = max(now.Sub(h.measuredAt()), 0)
	}
	if now.Before(e.cooling.until) {
		s.CoolingUntil = e.cooling.until
	}
	if now.Before(e.peerRetry) {
		s.PeerRetryUntil = e.peerRetry
	}
	return s
}

// CarrierReportSource submits host observations for one cache candidate.
// Copying a handle shares its sequence space.
type CarrierReportSource struct {
	state *carrierReportState
}

type carrierReportState struct {
	cache      *CarrierCache
	key        CarrierCacheKey
	high, seen uint64 // guarded by cache.mu
}

// RegisterReportSource binds a host evidence source to an exact candidate.
func (cc *CarrierCache) RegisterReportSource(key string, kind CarrierKind, variant string) (CarrierReportSource, error) {
	if cc == nil || kind == 0 || int(kind) >= len(carrierKindNames) {
		return CarrierReportSource{}, ErrCarrierCacheValue
	}
	return CarrierReportSource{&carrierReportState{cache: cc, key: CarrierCacheKey{Key: key, Kind: kind, Variant: variant}}}, nil
}

// Report applies evidence unavailable to the cache itself. Assign increasing
// positive sequence numbers. Concurrent arrivals can reorder within the
// source's 64-sequence replay window. At is the original observation time,
// or the cache's time if omitted.
func (s CarrierReportSource) Report(sequence uint64, o CacheOutcome, ob CarrierObservation) (applied bool, err error) {
	if s.state == nil || sequence == 0 {
		return false, ErrCarrierCacheValue
	}
	f := cacheFeedback{source: s.state, sequence: sequence, applied: &applied}
	err = s.state.cache.applyFeedback(s.state.key, o, ob, f)
	return applied, err
}

func (cc *CarrierCache) update(k CarrierCacheKey, o CacheOutcome, ob CarrierObservation) error {
	return cc.applyFeedback(k, o, ob, cacheFeedback{})
}

func (cc *CarrierCache) applyFeedback(k CarrierCacheKey, o CacheOutcome, ob CarrierObservation, feedback cacheFeedback) error {
	wait := cc.waitSample
	if wait == nil {
		wait = func(b ResolvedBackoff, d time.Duration) time.Duration {
			return sampleWait(cc.config().ordinarySamples, b, d)
		}
	}
	return cc.updateWithWait(k, o, ob, feedback, wait)
}

func (cc *CarrierCache) updateWithWait(k CarrierCacheKey, o CacheOutcome, ob CarrierObservation, feedback cacheFeedback, wait func(ResolvedBackoff, time.Duration) time.Duration) error {
	p := cc.config()
	if o > CacheRecovered || ob.Duration < 0 || ob.RTT < 0 || ob.ValidFor < 0 || ob.Rate < 0 || math.IsNaN(ob.Rate) || math.IsInf(ob.Rate, 0) || k.Kind == 0 || int(k.Kind) >= len(carrierKindNames) {
		return ErrCarrierCacheValue
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	now := cc.nowLocked()
	if ob.At.After(now) {
		return ErrCarrierCacheValue
	}
	if src := feedback.source; src != nil {
		sequence := feedback.sequence
		if sequence > src.high {
			src.seen <<= sequence - src.high
			src.high = sequence
		}
		delta := src.high - sequence
		if delta >= 64 {
			return ErrStaleCarrierReport
		}
		bit := uint64(1) << delta
		if src.seen&bit != 0 {
			return nil
		}
		src.seen |= bit
		*feedback.applied = true
	}
	if ob.At.IsZero() {
		ob.At = now
	}
	e := cc.entry(k)
	if feedback.retry > 0 {
		if deadline := ob.At.Add(feedback.retry); deadline.After(e.peerRetry) {
			e.peerRetry = deadline
		}
	}
	if o <= CacheLifetime {
		e.history = e.history.decay(now, p.halfLife)
		e.history.cachePoint = cachePoint{At: now}
	}
	w := decayAge(now.Sub(ob.At), p.halfLife)
	var undo *outageUndo
	if j := feedback.journal; j != nil && o >= CacheFailed && o <= CacheLateDrop && o != CacheSurvived {
		undo = j.undo(k)
		undo.addHistory(now, p.halfLife, o, w, !feedback.settledHandshake)
	}
	switch o {
	case CacheUsable:
		e.history.Attempts++
		e.history.Success += w
	case CacheFailed, CacheRejected:
		if !feedback.settledHandshake {
			e.history.Attempts++
			e.history.Failure += w
		}
		cc.coolOrDeferLocked(k, e, p, now, o, wait, undo, feedback.journal)
	case CacheSurvived:
		e.history.Survived += w
	case CacheShortDrop, CacheLateDrop:
		if o == CacheShortDrop {
			e.history.ShortDrop += w
		} else {
			e.history.LateDrop += w
		}
		if cc.newPhysicalFaultLocked(k, feedback.fault) {
			cc.coolOrDeferLocked(k, e, p, now, o, wait, undo, feedback.journal)
		}
	case CacheLifetime:
		lambda := math.Ln2 / p.halfLife.Seconds()
		e.history.LateSeconds += w * -math.Expm1(-lambda*ob.Duration.Seconds()) / lambda
	case CachePerformance:
		if ob.RTT > 0 && ob.At.After(e.rtt.At) {
			e.rtt = cachePerformance{cachePoint{At: ob.At}, ob.RTT.Seconds(), ob.ValidFor, feedback.performanceSource()}
		}
		if ob.Rate > 0 && ob.At.After(e.rate.At) {
			e.rate = cachePerformance{cachePoint{At: ob.At}, ob.Rate, ob.ValidFor, feedback.performanceSource()}
		}
	case CacheRecovered:
		if ob.Duration >= p.healthReset && !ob.At.Before(cooldownHealthDeadline(e.cooling, ob.At.Add(-ob.Duration), p.healthReset)) {
			cc.resetCooldownLocked(k, e, now)
		}
	}
	return nil
}

// coolOrDeferLocked defers the round of a fault committed during an outage.
func (cc *CarrierCache) coolOrDeferLocked(key CarrierCacheKey, e *cacheEntry, p *cacheParams, now time.Time, o CacheOutcome, wait func(ResolvedBackoff, time.Duration) time.Duration, undo *outageUndo, journal *outageJournal) {
	if undo != nil && journal.active {
		undo.deferOnce(o)
		return
	}
	cc.coolLocked(key, e, p, now, o, wait, undo)
}

func (cc *CarrierCache) coolLocked(key CarrierCacheKey, e *cacheEntry, p *cacheParams, now time.Time, o CacheOutcome, wait func(ResolvedBackoff, time.Duration) time.Duration, undo *outageUndo) {
	cc.resetCooldownIfDueLocked(key, e, now)
	if undo != nil {
		undo.captureCooling(e)
		undo.rounds = append(undo.rounds, o)
	}
	c := &e.cooling
	if c.Level < math.MaxInt64 {
		c.Level++
	}
	e.episode = 0
	if cc.nextEpisode < math.MaxUint64 {
		cc.nextEpisode++
		e.episode = cc.nextEpisode
	}
	nominal := p.backoff.NextNominal(c.nominal)
	if p.shortDropResilient && o == CacheShortDrop {
		c.Short = slices.DeleteFunc(c.Short, func(t cachePoint) bool { return now.Sub(t.measuredAt()) >= shortDropWindow })
		if len(c.Short) == 3 {
			c.Short = c.Short[1:]
		}
		c.Short = append(c.Short, cachePoint{At: now})
		if len(c.Short) == 3 {
			nominal = max(nominal, shortDropFloor)
		}
	}
	if o == CacheRejected {
		nominal = p.backoff.Max
	}
	c.nominal, c.cachePoint = nominal, cachePoint{At: now}
	sampled := wait(p.backoff, nominal)
	c.until = now.Add(sampled)
	incrementRetryCount(&e.counters.faults)
	var reason CooldownReason
	switch o {
	case CacheRejected:
		reason = CooldownRejected
	case CacheShortDrop:
		reason = CooldownShortDrop
	case CacheLateDrop:
		reason = CooldownLateDrop
	default:
		reason = CooldownAttempt
	}
	c.reason = reason
	if undo != nil {
		undo.episode = e.episode
	}
	cc.publishCooldownLocked(CooldownStarted{Key: key, Episode: e.episode, Level: c.Level, Reason: reason,
		StartedAt: now, RetryAt: c.until, NominalWait: nominal, SampledWait: sampled})
	cc.armCooldownResetLocked(key, e, now)
}

type cacheSnapshot struct {
	Version    int                  `json:"version"`
	ExportedAt time.Time            `json:"exported_at"`
	Entries    []cacheSnapshotEntry `json:"entries"`
}

type cacheSnapshotEntry struct {
	Key     string               `json:"key"`
	Kind    string               `json:"kind"`
	Variant string               `json:"variant"`
	History cacheHistory         `json:"history"`
	Cooling cacheCoolingSnapshot `json:"cooling"`
	RTT     cachePerformance     `json:"rtt_seconds"`
	Rate    cachePerformance     `json:"rate_bytes_per_second"`
}

// Export encodes version 2 with stable key order and no process-local redirects.
func (cc *CarrierCache) Export() ([]byte, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	now := cc.nowLocked()
	doc := cacheSnapshot{Version: carrierCacheVersion, ExportedAt: now.UTC(), Entries: []cacheSnapshotEntry{}}
	for k, e := range cc.m {
		h, rtt, rate := e.history, e.rtt, e.rate
		if h.At.IsZero() && e.cooling.At.IsZero() && rtt.At.IsZero() && rate.At.IsZero() {
			continue
		}
		h.cachePoint = h.withAge(now)
		rtt.cachePoint, rate.cachePoint = rtt.withAge(now), rate.withAge(now)
		local := e.cooling
		c := cacheCoolingSnapshot{cachePoint: local.withAge(now), Level: local.Level,
			NominalMS: local.nominal.Milliseconds(), RemainingMS: max(local.until.Sub(now), 0).Milliseconds(), Short: local.Short}
		if len(c.Short) > 0 {
			c.Short = slices.Clone(c.Short)
			for i, mark := range c.Short {
				c.Short[i] = mark.withAge(now)
			}
		}
		doc.Entries = append(doc.Entries, cacheSnapshotEntry{k.Key, k.Kind.String(), k.Variant, h, c, rtt, rate})
	}
	slices.SortFunc(doc.Entries, func(a, b cacheSnapshotEntry) int {
		return cmp.Or(strings.Compare(a.Key, b.Key), strings.Compare(a.Kind, b.Kind), strings.Compare(a.Variant, b.Variant))
	})
	return json.Marshal(doc)
}

// Import validates the complete snapshot before merging any entries. Equal
// anchors preserve the local value so a replay cannot lengthen cooling.
func (cc *CarrierCache) Import(data []byte) error {
	p := cc.config()
	var doc cacheSnapshot
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("%w: %w", ErrCarrierCacheFormat, err)
	}
	if doc.Version != carrierCacheVersion {
		return ErrCarrierCacheVersion
	}
	if doc.ExportedAt.IsZero() {
		return ErrCarrierCacheValue
	}
	keys := make(map[CarrierCacheKey]bool, len(doc.Entries))
	for i := range doc.Entries {
		s := &doc.Entries[i]
		k := CarrierCacheKey{Key: s.Key, Kind: CarrierKind(slices.Index(carrierKindNames[:], s.Kind)), Variant: s.Variant}
		if k.Kind == 0 || int(k.Kind) >= len(carrierKindNames) || keys[k] || !s.valid(p.backoff) {
			return ErrCarrierCacheValue
		}
		keys[k] = true
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	now := cc.nowLocked()
	elapsed := max(now.Sub(doc.ExportedAt), 0)
	if cc.m == nil {
		cc.m = make(map[CarrierCacheKey]*cacheEntry, len(doc.Entries))
	}
	for _, s := range doc.Entries {
		k := CarrierCacheKey{Key: s.Key, Kind: CarrierKind(slices.Index(carrierKindNames[:], s.Kind)), Variant: s.Variant}
		e := cc.m[k]
		if e == nil {
			e = &cacheEntry{}
			cc.m[k] = e
		}
		if s.History.At.After(e.history.At) {
			e.history = s.History
			e.history.cachePoint = e.history.reanchor(now, elapsed)
		}
		if s.Cooling.At.After(e.cooling.At) {
			e.cooling = cacheCooling{cachePoint: s.Cooling.reanchor(now, elapsed), Level: s.Cooling.Level,
				nominal: time.Duration(s.Cooling.NominalMS) * time.Millisecond, Short: s.Cooling.Short}
			e.episode = 0
			for i, mark := range e.cooling.Short {
				e.cooling.Short[i] = mark.reanchor(now, elapsed)
			}
			e.cooling.until = now.Add(max(time.Duration(s.Cooling.RemainingMS)*time.Millisecond-elapsed, 0))
			cc.armCooldownResetLocked(k, e, now)
		}
		if s.RTT.At.After(e.rtt.At) {
			e.rtt = s.RTT
			e.rtt.cachePoint = e.rtt.reanchor(now, elapsed)
		}
		if s.Rate.At.After(e.rate.At) {
			e.rate = s.Rate
			e.rate.cachePoint = e.rate.reanchor(now, elapsed)
		}
	}
	return nil
}

func (s *cacheSnapshotEntry) valid(bo ResolvedBackoff) bool {
	c, h := s.Cooling, s.History
	if c.Level < 0 || c.Level > 0 && c.At.IsZero() ||
		c.NominalMS < 0 || c.NominalMS > bo.Max.Milliseconds() || c.RemainingMS < 0 ||
		c.RemainingMS > bo.waitUpper(bo.Max).Milliseconds() || len(c.Short) > 3 || s.RTT.ValidFor < 0 || s.Rate.ValidFor < 0 {
		return false
	}
	for _, t := range append([]cachePoint{h.cachePoint, c.cachePoint, s.RTT.cachePoint, s.Rate.cachePoint}, c.Short...) {
		if t.AgeNS < 0 || t.At.IsZero() && t.AgeNS != 0 {
			return false
		}
	}
	for _, v := range []float64{h.Success, h.Failure, h.Survived, h.ShortDrop, h.LateDrop, h.LateSeconds, s.RTT.Value, s.Rate.Value} {
		if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
	}
	return true
}

// admitChecked reserves a key and its domain permits at a single cut.
func (cc *CarrierCache) admitChecked(key CarrierCacheKey, intent AttemptCause, session *Conn, cutoff time.Time, grant func() error) (keyReservation, time.Time, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	now := cc.nowLocked()
	e := cc.entry(key)
	cc.resetCooldownIfDueLocked(key, e, now)
	wake, err := e.admission(now, intent, 0, cutoff)
	if intent == AttemptRecovery && session != nil {
		if e.inFlight || len(e.waiters) > 0 {
			if !slices.Contains(e.waiters, session) {
				e.waiters = append(e.waiters, session)
			}
			if err == nil && e.waiters[0] != session {
				err = ErrCarrierInFlight
			}
		}
	}
	if err == nil && e.token == math.MaxUint64 {
		err = ErrAttemptIDExhausted
	}
	if err == nil && grant != nil {
		err = grant()
	}
	if err != nil {
		return keyReservation{}, wake, err
	}
	if session != nil && intent == AttemptRecovery {
		e.removeWaiter(session)
	}
	e.token++
	e.inFlight = true
	return keyReservation{cache: cc, session: session, entry: e, token: e.token, intent: intent, cutoff: cutoff}, time.Time{}, nil
}

func (e *cacheEntry) admission(now time.Time, intent AttemptCause, token uint64, cutoff time.Time) (time.Time, error) {
	gate, reason := e.cooling.until, ErrCarrierCooling
	if intent == AttemptRecovery {
		gate, reason = e.recovery.gate(cutoff), ErrRecoveryWait
	}
	if !e.peerRetry.Before(gate) {
		gate, reason = e.peerRetry, ErrPeerRetry
	}
	if now.Before(gate) {
		return gate, reason
	}
	if e.inFlight && e.token != token {
		return time.Time{}, ErrCarrierInFlight
	}
	return time.Time{}, nil
}

func (cc *CarrierCache) entry(key CarrierCacheKey) *cacheEntry {
	e := cc.m[key]
	if e == nil {
		if cc.m == nil {
			cc.m = make(map[CarrierCacheKey]*cacheEntry)
		}
		e = new(cacheEntry)
		cc.m[key] = e
	}
	return e
}
