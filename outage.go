package zmux

import (
	"slices"
	"time"
)

// outageJournal records one session's failures that are provisional because
// the session had no usable pipe when they were committed, or because they
// were the member drops that led into such an outage. History and the
// recovery cadence take them at once. Ordinary cooldown never rises during
// an outage: rounds committed before it began are suspended when it begins
// and rounds during it are deferred. If any candidate later succeeds,
// retract reverts this session's own contributions and drops the deferred
// rounds. Otherwise commit applies at most one deferred round per key, as an
// ordinary fault: the outage stood for one failure of each candidate.
type outageJournal struct {
	cache  *CarrierCache
	keys   map[CarrierCacheKey]*outageUndo
	order  []CarrierCacheKey
	active bool // guarded by cache.mu; set once the outage began
}

// outageUndo is the coalesced provisional contribution to one key. History
// weights are kept decayed to anchor. Cooling and recovery are restored only
// while the key still holds the state this journal produced, so a later fault
// observed by another session remains in force.
type outageUndo struct {
	anchor                time.Time
	priorCooling          cacheCooling
	priorRecovery         RecoveryCadence
	recovery              RecoveryCadence
	failure, short, late  float64
	attempts              uint64
	priorEpisode, episode uint64
	cooled, recovered     bool
	rounds, deferred      []CacheOutcome
}

// deferOnce keeps one deferred round per key, the most severe: a
// rejection, which jumps to the maximum backoff, wins over other faults.
func (u *outageUndo) deferOnce(o CacheOutcome) {
	switch {
	case len(u.deferred) == 0:
		u.deferred = append(u.deferred, o)
	case o == CacheRejected:
		u.deferred[0] = o
	}
}

func newOutageJournal(cc *CarrierCache) *outageJournal {
	return &outageJournal{cache: cc, keys: make(map[CarrierCacheKey]*outageUndo)}
}

func (j *outageJournal) undo(key CarrierCacheKey) *outageUndo {
	u := j.keys[key]
	if u == nil {
		u = new(outageUndo)
		j.keys[key] = u
		j.order = append(j.order, key)
	}
	return u
}

func (u *outageUndo) addHistory(now time.Time, halfLife time.Duration, o CacheOutcome, w float64, attempt bool) {
	if !u.anchor.IsZero() {
		f := decayAge(now.Sub(u.anchor), halfLife)
		u.failure *= f
		u.short *= f
		u.late *= f
	}
	u.anchor = now
	switch o {
	case CacheShortDrop:
		u.short += w
	case CacheLateDrop:
		u.late += w
	default:
		if attempt {
			u.failure += w
			u.attempts++
		}
	}
}

// captureCooling runs inside coolLocked after any due reset and before the
// new round is committed.
func (u *outageUndo) captureCooling(e *cacheEntry) {
	if !u.cooled {
		u.cooled = true
		u.priorCooling = e.cooling
		u.priorCooling.Short = slices.Clone(e.cooling.Short)
		u.priorEpisode = e.episode
	}
}

// suspend marks the outage as begun and withdraws the cooldown rounds that
// its onset drops committed, deferring them.
func (j *outageJournal) suspend() {
	cc := j.cache
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if j.active {
		return
	}
	j.active = true
	now := cc.nowLocked()
	for _, key := range j.order {
		u, e := j.keys[key], cc.m[key]
		if e == nil || !u.cooled {
			continue
		}
		if u.episode != 0 && e.episode == u.episode {
			cc.restoreCoolingLocked(key, e, u, now)
			for _, o := range u.rounds {
				u.deferOnce(o)
			}
		}
		u.cooled, u.rounds = false, nil
	}
	cc.signalLocked()
}

func (cc *CarrierCache) restoreCoolingLocked(key CarrierCacheKey, e *cacheEntry, u *outageUndo, now time.Time) {
	e.cooling, e.episode = u.priorCooling, u.priorEpisode
	if !e.cooling.committed() {
		e.health.stop()
	}
	var retry time.Time
	if now.Before(e.cooling.until) {
		retry = e.cooling.until
	}
	cc.publishCooldownLocked(CooldownRetracted{Key: key, Episode: u.episode, RetractedAt: now,
		Level: e.cooling.Level, RetryAt: retry})
	cc.armCooldownResetLocked(key, e, now)
}

// commit makes the provisional failures final: the deferred rounds are
// committed now as ordinary faults.
func (j *outageJournal) commit() {
	if j == nil {
		return
	}
	cc := j.cache
	p := cc.config()
	wait := func(b ResolvedBackoff, d time.Duration) time.Duration { return sampleWait(p.ordinarySamples, b, d) }
	if cc.waitSample != nil {
		wait = cc.waitSample
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	now := cc.nowLocked()
	for _, key := range j.order {
		if e := cc.m[key]; e != nil {
			for _, o := range j.keys[key].deferred {
				cc.coolLocked(key, e, p, now, o, wait, nil)
			}
		}
	}
	j.keys, j.order = nil, nil
	cc.signalLocked()
}

// retract reverts every recorded contribution. The journal must not be used
// afterwards.
func (j *outageJournal) retract() {
	if j == nil || len(j.order) == 0 {
		return
	}
	cc := j.cache
	p := cc.config()
	cc.mu.Lock()
	defer cc.mu.Unlock()
	now := cc.nowLocked()
	for _, key := range j.order {
		u, e := j.keys[key], cc.m[key]
		if e == nil {
			continue
		}
		if !u.anchor.IsZero() {
			f := decayAge(now.Sub(u.anchor), p.halfLife)
			e.history = e.history.decay(now, p.halfLife)
			e.history.cachePoint = cachePoint{At: now}
			e.history.Failure = max(e.history.Failure-u.failure*f, 0)
			e.history.ShortDrop = max(e.history.ShortDrop-u.short*f, 0)
			e.history.LateDrop = max(e.history.LateDrop-u.late*f, 0)
			e.history.Attempts -= min(e.history.Attempts, u.attempts)
		}
		if u.cooled && u.episode != 0 && e.episode == u.episode {
			cc.restoreCoolingLocked(key, e, u, now)
		}
		if u.recovered && e.recovery == u.recovery {
			e.recovery = u.priorRecovery
		}
	}
	j.keys, j.order = nil, nil
	cc.signalLocked()
}

// outageLocked reports whether no attached pipe currently carries any
// direction of the managed session.
func (sc *SessionControl) outageLocked() bool {
	covered, _ := sc.c.s.Coverage(nil)
	return covered == 0
}

// provisionalLocked returns the journal for a failure committed now: the
// outage journal while the session has no usable pipe, otherwise nil.
// A late observation of a superseded binding on the same cache belongs to
// the journal the takeover carried to the current one.
func (sc *SessionControl) provisionalLocked() *outageJournal {
	owner := sc.c.control
	if sc.ctl.cache == nil || owner == nil || owner.ctl.cache != sc.ctl.cache {
		return nil
	}
	if !owner.outageLocked() {
		return nil
	}
	owner.enterOutageLocked()
	return owner.onset
}

func (sc *SessionControl) enterOutageLocked() {
	if sc.onset == nil {
		sc.onset = newOutageJournal(sc.ctl.cache)
	}
	if !sc.inOutage {
		sc.inOutage = true
		sc.onset.suspend()
	}
}

// connectivityLocked ends an outage with success and retracts its journal.
// Outside an outage it makes the earlier drops final.
func (sc *SessionControl) connectivityLocked() {
	if sc.inOutage {
		sc.onset.retract()
	}
	sc.onset, sc.inOutage = nil, false
}

// finalizeOutageLocked ends the binding's journal without a success.
func (sc *SessionControl) finalizeOutageLocked() {
	if sc.inOutage {
		sc.onset.commit()
	}
	sc.onset, sc.inOutage = nil, false
}
