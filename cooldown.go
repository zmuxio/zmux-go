package zmux

import (
	"context"
	"errors"
	"maps"
	"math"
	"net"
	"time"

	"github.com/djylb/timeshim"
)

// CarrierCacheKey identifies a candidate's retry state, shared across sessions.
type CarrierCacheKey struct {
	Key     string
	Kind    CarrierKind
	Variant string
}

// CooldownReason identifies the fault that began a local cooldown.
type CooldownReason string

// Reasons a fault opens a cooldown interval.
const (
	CooldownAttempt   CooldownReason = "attempt"
	CooldownRejected  CooldownReason = "rejected"
	CooldownShortDrop CooldownReason = "short_drop"
	CooldownLateDrop  CooldownReason = "late_drop"
)

// CooldownEvent is a committed change to the local cooldown schedule.
type CooldownEvent interface {
	CandidateKey() CarrierCacheKey
	cooldownEvent()
}

// CooldownStarted is committed once for each qualifying fault episode,
// before the cache grants another retry. Times use the cache clock domain.
type CooldownStarted struct {
	Key                      CarrierCacheKey
	Episode                  uint64
	Level                    int64
	Reason                   CooldownReason
	StartedAt, RetryAt       time.Time
	NominalWait, SampledWait time.Duration
}

// CandidateKey returns the key whose retry state changed.
func (e CooldownStarted) CandidateKey() CarrierCacheKey { return e.Key }
func (CooldownStarted) cooldownEvent()                  {}

// CooldownReset reports that continuous full-direction health cleared the
// cooldown. A zero episode identifies a reset of imported state.
type CooldownReset struct {
	Key     CarrierCacheKey
	Episode uint64
	ResetAt time.Time
}

// CandidateKey returns the key whose retry state changed.
func (e CooldownReset) CandidateKey() CarrierCacheKey { return e.Key }
func (CooldownReset) cooldownEvent()                  {}

// CooldownRetracted reports that a provisional cooldown round was withdrawn:
// when its session's outage began (an onset drop) or when the session
// reconnected. Level is the restored level; RetryAt is the restored retry
// time, zero when no wait remains.
type CooldownRetracted struct {
	Key         CarrierCacheKey
	Episode     uint64
	Level       int64
	RetractedAt time.Time
	RetryAt     time.Time
}

// CandidateKey returns the key whose retry state changed.
func (e CooldownRetracted) CandidateKey() CarrierCacheKey { return e.Key }
func (CooldownRetracted) cooldownEvent()                  {}

// CooldownSubscription observes future local cooldown commits in commit order.
type CooldownSubscription struct {
	cache   *CarrierCache
	events  chan CooldownEvent
	dropped uint64 // guarded by cache.mu
}

// CooldownEvents subscribes to future local cooldown commits.
// Close the subscription when finished.
func (cc *CarrierCache) CooldownEvents() *CooldownSubscription {
	sub := &CooldownSubscription{cache: cc, events: make(chan CooldownEvent, 64)}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.cooldownSubscribers == nil {
		cc.cooldownSubscribers = make(map[*CooldownSubscription]struct{})
	}
	cc.cooldownSubscribers[sub] = struct{}{}
	return sub
}

// Events delivers the subscription's cooldown events until Close.
func (s *CooldownSubscription) Events() <-chan CooldownEvent { return s.events }

// Dropped reports how many commits could not fit in this subscription's queue.
func (s *CooldownSubscription) Dropped() uint64 {
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	return s.dropped
}

// Close removes the subscription and closes its event channel.
func (s *CooldownSubscription) Close() {
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	if _, ok := s.cache.cooldownSubscribers[s]; ok {
		delete(s.cache.cooldownSubscribers, s)
		close(s.events)
	}
}

func (cc *CarrierCache) publishCooldownLocked(event CooldownEvent) {
	for sub := range cc.cooldownSubscribers {
		select {
		case sub.events <- event:
		default:
			incrementRetryCount(&sub.dropped)
		}
	}
}

// RetrySuppression identifies a gate that prevented an opener from starting.
type RetrySuppression string

// Reasons a candidate cannot start an opener.
const (
	RetryAlreadyInFlight RetrySuppression = "already_in_flight"
	RetryCooling         RetrySuppression = "cooling"
	RetryAfter           RetrySuppression = "retry_after"
	RetryDialAfter       RetrySuppression = "dial_after"
	RetryCoverage        RetrySuppression = "coverage"
	RetryUnauthorized    RetrySuppression = "unauthorized"
	RetryCapacity        RetrySuppression = "capacity"
	RetryDeadline        RetrySuppression = "deadline"
	RetryStale           RetrySuppression = "stale"
)

// RetryMetrics contains process-local counters for one cache key. Faults
// counts distinct committed episodes. Elapsed is nil before a cooldown or
// after its health reset. Elapsed and Remaining describe the local wait,
// independently of peer retry and other admission gates.
type RetryMetrics struct {
	AttemptedStarts, Faults uint64
	Suppressions            map[RetrySuppression]uint64
	Elapsed                 *time.Duration
	Remaining               time.Duration
}

type retryCounters struct {
	starts, faults uint64
	suppressions   map[RetrySuppression]uint64
}

func incrementRetryCount(count *uint64) {
	if *count != math.MaxUint64 {
		*count++
	}
}

// RetryMetrics reads the counters and current elapsed and remaining wait for
// the selected candidate.
func (cc *CarrierCache) RetryMetrics(key string, kind CarrierKind, variant string) RetryMetrics {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	e := cc.m[CarrierCacheKey{Key: key, Kind: kind, Variant: variant}]
	if e == nil {
		return RetryMetrics{}
	}
	now := cc.nowLocked()
	c := &e.counters
	out := RetryMetrics{AttemptedStarts: c.starts, Faults: c.faults}
	out.Suppressions = maps.Clone(c.suppressions)
	if e.cooling.committed() {
		elapsed := max(now.Sub(e.cooling.measuredAt()), 0)
		out.Elapsed = &elapsed
		out.Remaining = max(e.cooling.until.Sub(now), 0)
	}
	return out
}

func (c *retryCounters) suppress(reason RetrySuppression) {
	if c.suppressions == nil {
		c.suppressions = make(map[RetrySuppression]uint64)
	}
	n := c.suppressions[reason]
	incrementRetryCount(&n)
	c.suppressions[reason] = n
}

func (cc *CarrierCache) suppress(key CarrierCacheKey, reason RetrySuppression) {
	if reason == "" {
		return
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.entry(key).counters.suppress(reason)
}

func retrySuppression(err error) RetrySuppression {
	switch {
	case errors.Is(err, ErrCarrierInFlight):
		return RetryAlreadyInFlight
	case errors.Is(err, ErrCarrierCooling):
		return RetryCooling
	case errors.Is(err, ErrPeerRetry):
		return RetryAfter
	case errors.Is(err, ErrControllerDialAfter):
		return RetryDialAfter
	case errors.Is(err, ErrNoEligibleCandidate), errors.Is(err, ErrNoDirectionCoverage), errors.Is(err, errRecoveryNoLongerNeeded):
		return RetryCoverage
	case errors.Is(err, ErrTopologyExcluded), errors.Is(err, ErrJoinAuthorizationChanged), errors.Is(err, ErrSessionPolicyConflict):
		return RetryUnauthorized
	case sharedControllerBlock(err), errors.Is(err, ErrControllerQueueFull), errors.Is(err, ErrCapacityBlocked),
		errors.Is(err, ErrCarrierCapacity), errors.Is(err, ErrAttemptIDExhausted), errors.Is(err, ErrChangePending):
		return RetryCapacity
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrJoinTimeout):
		return RetryDeadline
	case errors.Is(err, context.Canceled), errors.Is(err, ErrTargetConflict), errors.Is(err, ErrStaleBinding), errors.Is(err, ErrStaleDecision), errors.Is(err, errControllerStaleStart), errors.Is(err, net.ErrClosed):
		return RetryStale
	default:
		return ""
	}
}

// cooldownHealth tracks only currently healthy members. Its single timer is
// armed only while the key also has a committed cooldown.
type cooldownHealth struct {
	members map[*memberFeedback]time.Time
	timer   *cooldownResetTimer
}

type cooldownResetTimer struct {
	timer timeshim.Timer
	due   time.Time
}

func (cc *CarrierCache) observeHealthyMember(member *memberFeedback, at time.Time, healthy bool) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	now := cc.nowLocked()
	e := cc.entry(member.key)
	if !healthy && e.health == nil {
		return
	}
	if e.health == nil {
		e.health = &cooldownHealth{members: make(map[*memberFeedback]time.Time)}
	}
	h := e.health
	if healthy {
		h.members[member] = at
	} else {
		// Count time through the observed edge. A delayed timer must not
		// miss a health interval that completed before the member left.
		if h.timer != nil && !at.Before(h.timer.due) {
			cc.resetCooldownLocked(member.key, e, now)
		}
		delete(h.members, member)
	}
	cc.armCooldownResetLocked(member.key, e, now)
	if len(h.members) == 0 {
		e.health = nil
	}
}

func (cc *CarrierCache) armCooldownResetLocked(key CarrierCacheKey, e *cacheEntry, now time.Time) {
	h := e.health
	if h == nil {
		return
	}
	var due time.Time
	if e.cooling.committed() {
		for _, since := range h.members {
			due = earliest(due, since)
		}
		if !due.IsZero() {
			due = cooldownHealthDeadline(e.cooling, due, cc.config().healthReset)
		}
	}
	if h.timer != nil && due.Equal(h.timer.due) {
		return
	}
	h.stop()
	if due.IsZero() {
		return
	}
	if !now.Before(due) {
		cc.resetCooldownLocked(key, e, now)
		return
	}
	arm := &cooldownResetTimer{due: due}
	h.timer = arm
	arm.timer = cc.clockDomain.clockSource().AfterFunc(due.Sub(now), func() {
		cc.mu.Lock()
		defer cc.mu.Unlock()
		if h.timer != arm {
			return
		}
		h.timer = nil
		cc.armCooldownResetLocked(key, e, cc.nowLocked())
	})
}

func cooldownHealthDeadline(c cacheCooling, healthySince time.Time, interval time.Duration) time.Time {
	if healthySince.Before(c.measuredAt()) {
		healthySince = c.measuredAt()
	}
	return healthySince.Add(interval)
}

func (h *cooldownHealth) stop() {
	if h != nil && h.timer != nil {
		h.timer.timer.Stop()
		h.timer = nil
	}
}

func (cc *CarrierCache) resetCooldownIfDueLocked(key CarrierCacheKey, e *cacheEntry, now time.Time) {
	if e.health != nil && e.health.timer != nil && !now.Before(e.health.timer.due) {
		cc.resetCooldownLocked(key, e, now)
	}
}

func (cc *CarrierCache) resetCooldownLocked(key CarrierCacheKey, e *cacheEntry, now time.Time) {
	if !e.cooling.committed() {
		return
	}
	episode := e.episode
	e.cooling = cacheCooling{}
	e.cooling.At = now
	e.episode = 0
	e.health.stop()
	cc.publishCooldownLocked(CooldownReset{Key: key, Episode: episode, ResetAt: now})
}

func (c cacheCooling) committed() bool { return c.Level != 0 || c.nominal != 0 }

// ResetCooldown clears the key's ordinary cooldown (level, backoff, retry time
// and short-drop marks) and its recovery cadence, as a health reset does,
// without waiting for the health interval. Scoring history, peer retry
// deadlines and in-flight attempts are kept. Hosts call it when they learn
// that the network changed. It reports whether there was anything to clear.
func (cc *CarrierCache) ResetCooldown(key string, kind CarrierKind, variant string) bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	k := CarrierCacheKey{Key: key, Kind: kind, Variant: variant}
	e := cc.m[k]
	if e == nil {
		return false
	}
	cleared := cc.clearCooldownLocked(k, e, cc.nowLocked())
	if cleared {
		cc.signalLocked()
	}
	return cleared
}

// ResetCooldowns applies ResetCooldown to every key and returns how many
// keys had state to clear.
func (cc *CarrierCache) ResetCooldowns() int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	now := cc.nowLocked()
	n := 0
	for k, e := range cc.m {
		if cc.clearCooldownLocked(k, e, now) {
			n++
		}
	}
	if n != 0 {
		cc.signalLocked()
	}
	return n
}

func (cc *CarrierCache) clearCooldownLocked(k CarrierCacheKey, e *cacheEntry, now time.Time) bool {
	cleared := e.cooling.committed()
	cc.resetCooldownLocked(k, e, now)
	if e.recovery != (RecoveryCadence{}) {
		e.recovery = RecoveryCadence{}
		cleared = true
	}
	return cleared
}
