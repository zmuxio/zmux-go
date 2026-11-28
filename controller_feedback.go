package zmux

import (
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

type memberFeedback struct {
	window      time.Time
	risk        time.Time
	key         CarrierCacheKey
	expected    Directions
	stable      bool
	failedRound bool
}

func (sc *SessionControl) observeAttemptLocked(a AttemptSnapshot, peerRetry time.Duration) {
	i, ok := sc.ctl.index[a.Candidate]
	if !ok {
		return
	}
	candidate := sc.ctl.opts.Candidates[i]
	key := candKey(candidate.CacheKey, candidate)
	if a.Result.Attribution == AttributionCarrier {
		_ = sc.ctl.cache.applyFeedback(key, CacheFailed, CarrierObservation{}, cacheFeedback{retry: peerRetry, settledHandshake: a.HandshakeSucceeded})
		if a.Cause == AttemptRecovery {
			if sc.c.control == sc {
				sc.updateRecoveryLocked()
			}
			sc.ctl.cache.advanceRecovery(key)
		}
	}
	if a.Result.Reason == ReasonConfirmed {
		sc.ctl.cache.confirmRecovery(key)
		if sc.c.control != sc {
			return
		}
		for _, m := range sc.c.s.Members(nil) {
			if m.ID == a.Member {
				sc.trackMemberLocked(a.Member, a.Candidate, m.Expected, m.Covered, m.Stable)
				break
			}
		}
	}
}

func (sc *SessionControl) trackMemberLocked(id MemberID, candidate CandidateID, expected, covered, stable Directions) {
	i, ok := sc.ctl.index[candidate]
	if !ok {
		return
	}
	params := sc.ctl.cache.config()
	if sc.feedback == nil {
		sc.feedback = make(map[MemberID]*memberFeedback)
	}
	now := sc.c.clk.source().Now()
	c := sc.ctl.opts.Candidates[i]
	if old := sc.feedback[id]; old != nil && old.stable {
		sc.ctl.cache.observeHealthyMember(old, now, false)
	}
	f := &memberFeedback{key: candKey(c.CacheKey, c), expected: expected,
		window: now.Add(params.dropWindow), stable: stable == expected && covered == expected}
	sc.feedback[id] = f
	if f.stable {
		sc.ctl.cache.observeHealthyMember(f, now, true)
	}
	sc.armFeedbackLocked()
}

func (sc *SessionControl) observeMembersLocked(edges []core.MemberHealthChange) {
	for _, edge := range edges {
		f := sc.feedback[edge.ID]
		if f == nil {
			continue
		}
		at := sc.c.clk.wall(edge.At)
		sc.advanceFeedback(f, at)
		stable := !edge.Failed && !edge.Retired && edge.Stable == f.expected && edge.Covered == f.expected
		if f.stable && !stable {
			sc.ctl.cache.observeHealthyMember(f, at, false)
			sc.closeRisk(f, at)
		}
		if edge.Failed && !f.failedRound {
			f.failedRound = true
			outcome := CacheLateDrop
			if !f.window.IsZero() {
				outcome = CacheShortDrop
				f.window = time.Time{}
			}
			_ = sc.ctl.cache.applyFeedback(f.key, outcome, CarrierObservation{At: at}, cacheFeedback{fault: edge.PhysicalFault})
		}
		if !f.stable && stable {
			if f.window.IsZero() {
				f.risk = at
			}
			sc.ctl.cache.observeHealthyMember(f, at, true)
		}
		if stable {
			f.failedRound = false
		}
		f.stable = stable
		if edge.Retired {
			delete(sc.feedback, edge.ID)
		}
	}
	if len(edges) != 0 {
		sc.armFeedbackLocked()
	}
}

func (sc *SessionControl) advanceFeedback(f *memberFeedback, now time.Time) {
	if !f.window.IsZero() && !now.Before(f.window) {
		if f.stable {
			_ = sc.ctl.cache.update(f.key, CacheSurvived, CarrierObservation{At: f.window})
			f.risk = f.window
		}
		f.window = time.Time{}
	}
}

func (sc *SessionControl) closeRisk(f *memberFeedback, at time.Time) {
	if !f.risk.IsZero() && at.After(f.risk) {
		_ = sc.ctl.cache.update(f.key, CacheLifetime, CarrierObservation{At: at, Duration: at.Sub(f.risk)})
	}
	f.risk = time.Time{}
}

func (sc *SessionControl) armFeedbackLocked() {
	var next time.Time
	for _, f := range sc.feedback {
		next = earliest(next, f.window)
	}
	if next.IsZero() {
		if sc.feedbackTimer != nil {
			sc.feedbackTimer.Stop()
		}
		return
	}
	delay := max(next.Sub(sc.c.clk.source().Now()), 0)
	if sc.feedbackTimer == nil {
		sc.feedbackTimer = sc.c.clk.afterFunc(delay, func() {
			sc.c.locked(func() {
				if sc.c.control != sc || sc.ctx.Err() != nil {
					return
				}
				now := sc.c.clk.source().Now()
				for _, f := range sc.feedback {
					sc.advanceFeedback(f, now)
				}
				sc.armFeedbackLocked()
				sc.c.s.NotifyCapacity()
			})
		})
	} else {
		sc.feedbackTimer.Reset(delay)
	}
}

func (sc *SessionControl) stopFeedbackLocked() {
	if sc.feedbackTimer != nil {
		sc.feedbackTimer.Stop()
	}
	now := sc.c.clk.source().Now()
	for _, f := range sc.feedback {
		sc.advanceFeedback(f, now)
		sc.closeRisk(f, now)
		if f.stable {
			sc.ctl.cache.observeHealthyMember(f, now, false)
		}
	}
	clear(sc.feedback)
}

func (sc *SessionControl) transferFeedbackLocked(next *SessionControl) {
	if sc.feedbackTimer != nil {
		sc.feedbackTimer.Stop()
	}
	now := sc.c.clk.source().Now()
	next.feedback = make(map[MemberID]*memberFeedback)
	for _, m := range sc.c.s.Members(nil) {
		f := sc.feedback[m.ID]
		if f == nil {
			continue
		}
		sc.advanceFeedback(f, now)
		candidate := next.ctl.candidate(CandidateID(m.Candidate))
		key := candKey(candidate.CacheKey, candidate)
		if sc.ctl.cache != next.ctl.cache || candidate.ID == "" || key != f.key {
			sc.closeRisk(f, now)
			if f.stable {
				sc.ctl.cache.observeHealthyMember(f, now, false)
			}
			if candidate.ID == "" {
				continue
			}
			f.key = key
			if f.window.IsZero() && f.stable {
				f.risk = now
			}
			if f.stable {
				next.ctl.cache.observeHealthyMember(f, now, true)
			}
		}
		next.feedback[m.ID] = f
	}
	clear(sc.feedback)
	next.armFeedbackLocked()
}
