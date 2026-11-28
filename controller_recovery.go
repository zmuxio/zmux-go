package zmux

import (
	"context"
	"errors"
	"time"

	"github.com/djylb/timeshim"
)

var errRecoveryNoLongerNeeded = errors.New("zmux: recovery direction already covered")

type recoveryAttempt struct {
	*Attempt
	work *controllerWork
}

// recover keeps one batch and starts alternative layouts at the stagger.
// Every reservation and confirmation rechecks coverage outside that batch.
func (sc *SessionControl) recover(revision uint64, p reconcilePlan) (time.Time, error) {
	var tx *CapacityChange
	var active []recoveryAttempt
	defer func() {
		if tx != nil {
			_ = tx.Cancel()
		}
		for _, attempt := range active {
			attempt.work.releaseKey()
		}
	}()
	round := &recoveryRound{tried: make(map[CandidateID]bool)}
	var stagger time.Time
	for sc.ctx.Err() == nil {
		domain := sc.ctl.watch()
		cache := sc.ctl.cache.watch()
		var capacity CapacitySnapshot
		var current uint64
		var batchDone bool
		sc.c.locked(func() {
			if sc.c.control != sc {
				return
			}
			current = sc.target.Revision
			if current != revision {
				return
			}
			if tx != nil && !tx.done {
				p.need = tx.batch.RecoveryNeed()
				for _, a := range active {
					if !a.done && !a.canSupply(p.need) {
						a.finishLocked(ReasonNoLongerNeeded, nil, 0)
					}
				}
				if p.need == 0 {
					tx.cancelLocked()
				}
			}
			capacity = sc.c.capacityLocked()
			batchDone = tx != nil && tx.done
			if tx == nil || batchDone {
				p.need = capacity.Required &^ capacity.Covered
			}
		})
		if sc.ctx.Err() != nil || current != revision {
			return time.Time{}, ErrTargetConflict
		}

		winner := false
		var failed, running Directions
		keep := active[:0]
		for _, a := range active {
			v := a.Snapshot()
			if v.Finished.IsZero() {
				running |= a.member.Expected
				keep = append(keep, a)
				continue
			}
			a.work.releaseKey()
			round.refusalWatch = nil // this attempt changed capacity
			switch v.Result.Reason {
			case ReasonConfirmed:
				winner = true
			case ReasonNoLongerNeeded:
				delete(round.tried, v.Candidate)
			default:
				failed |= a.member.Expected
				round.lastFailure = v.Result.Err
				if v.Result.Attribution == AttributionCarrier {
					round.carrierFailure = true
					round.noteRetry(sc.c.clk.source().Now(), sc.ctl.recoveryWake(sc.ctl.candidate(v.Candidate)))
				}
			}
		}
		clear(active[len(keep):])
		active = keep
		if failed&p.need&^running != 0 {
			stagger = time.Time{}
		}
		if winner && batchDone || p.need == 0 {
			sc.dropDemand(revision)
			sc.setProgress(revision, p.deficit, time.Time{}, nil)
			return time.Time{}, nil
		}

		next := stagger
		var blocked error
		if !sc.c.clk.source().Now().Before(stagger) {
			candidate, work, wake, err := sc.choose(p, round)
			if err != nil {
				next, blocked = wake, err
			} else {
				a, err := sc.startRecovery(revision, capacity, candidate, work, &tx)
				if err != nil {
					work.releaseKey()
					work.releaseWork()
					work.releaseCleanup()
					retry := sc.ctl.recoveryWake(candidate)
					sc.suppressCurrentStart(revision, candidate, retry, err)
					if errors.Is(err, errRecoveryNoLongerNeeded) {
						continue
					}
					sc.recordRefusal(revision, err)
					// Refunding the reservation wakes the cache and controller.
					// Exhaust the other alternatives before watching for changes
					// so that the refund cannot immediately retry this layout.
					round.tried[candidate.ID] = true
					round.startRefusal = err
					round.noteRetry(sc.c.clk.source().Now(), retry)
					continue
				}
				round.refusalWatch = nil // starting the attempt changed capacity
				active = append(active, recoveryAttempt{Attempt: a, work: work})
				round.tried[candidate.ID] = true
				stagger = a.Snapshot().Started.Add(controllerStagger)
				next = stagger
			}
		}
		if !sharedControllerBlock(blocked) && !errors.Is(blocked, ErrControllerQueueFull) {
			sc.dropDemand(revision)
			domain = sc.ctl.watch()
		}
		if len(active) == 0 && blocked != nil {
			if round.startRefusal != nil {
				if errors.Is(blocked, ErrNoEligibleCandidate) {
					blocked = round.startRefusal
				}
				sc.parkRecoveryRound(tx, p, round, earliest(next, round.retryAt), blocked)
				tx = nil
				return time.Time{}, nil
			}
			if errors.Is(blocked, ErrNoEligibleCandidate) && round.lastFailure != nil {
				if !round.carrierFailure {
					sc.parkRecoveryRound(tx, p, round, time.Time{}, round.lastFailure)
					tx = nil
					return time.Time{}, nil
				}
				blocked = round.lastFailure
			}
			if len(round.tried) != 0 {
				// All capable alternatives have either run or hit a gate. Re-enter
				// arbitration in a new round, preserving the shared key deadlines.
				clear(round.tried)
				round = &recoveryRound{tried: round.tried}
				continue
			}
			sc.setProgress(revision, p.deficit, next, blocked)
			return next, blocked
		}
		sc.setProgress(revision, p.deficit, next, blocked)
		sc.wait(capacity.Changed, domain, cache, next)
	}
	return time.Time{}, sc.ctx.Err()
}

// Refunding an attempt's cleanup permit may race its terminal observation.
// Other owners must see the refund, but it cannot reopen the round that issued it.
type recoveryRound struct {
	tried          map[CandidateID]bool
	lastFailure    error
	startRefusal   error
	refusalWatch   <-chan struct{}
	retryAt        time.Time
	carrierFailure bool
	cleanupSignals uint64 // guarded by the controller lock
}

func (r *recoveryRound) noteRetry(now, retry time.Time) {
	if retry.After(now) {
		r.retryAt = earliest(r.retryAt, retry)
	}
}

func (r *recoveryRound) watch(ctl *SessionController) (<-chan struct{}, uint64) {
	ctl.mu.Lock()
	defer ctl.mu.Unlock()
	return ctl.changed, ctl.changeSequence - r.cleanupSignals
}

func (sc *SessionControl) parkRecoveryRound(tx *CapacityChange, p reconcilePlan, round *recoveryRound, next time.Time, blocked error) {
	var capacity, domain, cache <-chan struct{}
	var seq uint64
	sc.c.locked(func() {
		// Check the watch before rolling back our own batch.
		var changed bool
		select {
		case <-round.refusalWatch:
			changed = true
		default:
		}
		if tx != nil {
			tx.cancelLocked()
		}
		if changed || sc.c.control != sc || sc.target.Revision != p.revision {
			return
		}
		covered, _ := sc.c.s.Coverage(nil)
		if sc.c.s.RequiredDirections()&^covered != p.need {
			return
		}
		_, capacity = sc.c.s.WatchCapacity()
		domain, seq = round.watch(sc.ctl)
		cache = sc.ctl.cache.watch()
		sc.view.Unfilled, sc.view.NextRetry, sc.view.Block = p.deficit, next, blocked
	})
	var tick <-chan time.Time
	if capacity != nil && !next.IsZero() {
		clk := sc.c.clk.source()
		timer := timeshim.NewTimer(clk, max(next.Sub(clk.Now()), 0))
		defer timer.Stop()
		tick = timer.C
	}
	for capacity != nil {
		select {
		case <-capacity:
			return
		case <-cache:
			return
		case <-sc.ctx.Done():
			return
		case <-tick:
			return
		case <-domain:
			var current uint64
			domain, current = round.watch(sc.ctl)
			if current != seq {
				return
			}
		}
	}
}

func (ctl *SessionController) recoveryWake(candidate Candidate) time.Time {
	s := ctl.cache.Score(candidate.CacheKey, candidate.Kind, candidate.Variant)
	if s.PeerRetryUntil.After(s.Recovery.RetryAt) {
		return s.PeerRetryUntil
	}
	return s.Recovery.RetryAt
}

func (sc *SessionControl) startRecovery(revision uint64, capacity CapacitySnapshot, candidate Candidate, work *controllerWork, current **CapacityChange) (*Attempt, error) {
	tx := *current
	if tx == nil {
		req := ChangeRequest{PolicyRevision: capacity.PolicyRevision, Observed: capacity.Observed, Timeout: sc.ctl.opts.Limits.AttemptTimeout}
		var err error
		tx, err = sc.c.beginChange(sc.ctx, req, AttemptRecovery, sc, revision)
		if err != nil {
			return nil, err
		}
		*current = tx
	}
	return tx.startAlternative(applyPreference(candidate, nil), work.reservedAt, func(ctx context.Context, req OpenRequest) ([]PipeIO, error) {
		defer work.releaseWork()
		return openCandidate(ctx, req, candidate)
	}, func() error { return sc.checkOpenerStart(revision, candidate, work) }, work)
}
