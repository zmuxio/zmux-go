package zmux

import (
	"errors"
	"slices"
	"time"
)

// ErrRecoveryWait indicates a pending retry for missing directional coverage.
var ErrRecoveryWait = errors.New("zmux: recovery_wait")

// RecoveryCadence is the current process-local retry interval for a candidate.
type RecoveryCadence struct {
	RetryAt          time.Time
	Nominal, Sampled time.Duration
	Advances         uint64
}

var recoveryBackoff = ResolvedBackoff{Min: time.Second, Max: time.Minute, Jitter: 1}

func (cc *CarrierCache) signalLocked() {
	if cc.changed != nil {
		close(cc.changed)
		cc.changed = nil
	}
}

func (cc *CarrierCache) watch() <-chan struct{} {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.changed == nil {
		cc.changed = make(chan struct{})
	}
	return cc.changed
}

// setRecoveryDemand records coverage demand independently of permit queueing.
// A session keeps its identity across an atomic binding replacement.
func (cc *CarrierCache) setRecoveryDemand(c *Conn, key CarrierCacheKey, live bool) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if live {
		e := cc.entry(key)
		if e.demands == nil {
			e.demands = make(map[*Conn]bool)
		}
		if _, ok := e.demands[c]; !ok {
			e.demands[c] = false
		}
	} else if e := cc.m[key]; e != nil {
		delete(e.demands, c)
		if e.removeWaiter(c) {
			cc.signalLocked()
		}
		if len(e.demands) == 0 {
			e.recovery = RecoveryCadence{}
		}
	}
}

func (cc *CarrierCache) advanceRecovery(key CarrierCacheKey) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	e := cc.m[key]
	if e == nil || len(e.demands) == 0 {
		return
	}
	nominal := recoveryBackoff.NextNominal(e.recovery.Nominal)
	sampled := sampleWait(cc.config().recoverySamples, recoveryBackoff, nominal)
	sampled = sampled.Truncate(cc.clockDomain.resolution())
	e.recovery = RecoveryCadence{Advances: e.recovery.Advances, Nominal: nominal, Sampled: sampled, RetryAt: cc.nowLocked().Add(sampled)}
	incrementRetryCount(&e.recovery.Advances)
	cc.signalLocked()
}

func (cc *CarrierCache) confirmRecovery(key CarrierCacheKey) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if e := cc.m[key]; e != nil && !e.recovery.RetryAt.IsZero() {
		e.recovery = RecoveryCadence{}
		cc.signalLocked()
	}
}

func (sc *SessionControl) updateRecoveryLocked() {
	covered, _ := sc.c.s.Coverage(nil)
	need := sc.c.s.RequiredDirections() &^ covered
	if need == sc.recoveryNeed {
		return
	}
	previous := sc.recoveryNeed
	sc.recoveryNeed = need
	for key, dirs := range sc.ctl.recoveryDirections {
		live := dirs&need != 0
		if live != (dirs&previous != 0) {
			sc.ctl.cache.setRecoveryDemand(sc.c, key, live)
		}
	}
}

func (sc *SessionControl) withdrawRecoveryLocked(next *SessionControl) {
	for key, dirs := range sc.ctl.recoveryDirections {
		if dirs&sc.recoveryNeed == 0 {
			continue
		}
		if next != nil && sc.ctl.cache == next.ctl.cache && next.ctl.recoveryDirections[key]&next.recoveryNeed != 0 {
			continue
		}
		sc.ctl.cache.setRecoveryDemand(sc.c, key, false)
	}
}

func sampleWait(source <-chan float64, b ResolvedBackoff, nominal time.Duration) time.Duration {
	select {
	case u, ok := <-source:
		if ok && u >= 0 && u <= 1 {
			return b.waitSample(nominal, u)
		}
	default:
	}
	return b.Wait(nominal)
}

func (e *cacheEntry) removeWaiter(c *Conn) bool {
	i := slices.Index(e.waiters, c)
	if i < 0 {
		return false
	}
	e.waiters = slices.Delete(e.waiters, i, i+1)
	return true
}

func (cc *CarrierCache) withdrawWaiter(key CarrierCacheKey, c *Conn) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if e := cc.m[key]; e != nil && e.removeWaiter(c) {
		cc.signalLocked()
	}
}

func (cc *CarrierCache) suppressRecovery(key CarrierCacheKey, c *Conn, reason RetrySuppression) {
	if reason == "" {
		return
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	e := cc.entry(key)
	counted, live := e.demands[c]
	if live && !counted && !cc.nowLocked().Before(e.recovery.RetryAt) {
		e.demands[c] = true
		e.counters.suppress(reason)
	}
}
