package zmux

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

type targetSlot struct {
	group *GroupTarget
	quota *MemberQuota
}

type reconcilePlan struct {
	next        time.Time
	group       *GroupTarget
	quota       *MemberQuota
	upgrade     CandidateID
	drain       []MemberID
	preferences []matchedPreference
	epoch       uint64
	revision    uint64
	deficit     int
	need        Directions
	replacement bool
}

type matchedPreference struct {
	slot   targetSlot
	member MemberSnapshot
}

func hasCandidate(q *MemberQuota, id CandidateID) bool {
	return slices.ContainsFunc(q.Candidates, func(c TargetCandidate) bool { return c.ID == id })
}

func slotsFor(target SessionTarget, limit int) []targetSlot {
	var slots []targetSlot
	for i := range target.Groups {
		g := &target.Groups[i]
		for j := range g.Quotas {
			q := &g.Quotas[j]
			// Additional interchangeable slots of one quota cannot change
			// the matching once all current members and one deficit fit.
			for range min(q.Count, limit+1) {
				slots = append(slots, targetSlot{group: g, quota: q})
			}
		}
	}
	return slots
}

func matchMembers(slots []targetSlot, members []MemberSnapshot, excluded map[MemberID]bool) []int {
	owner := make([]int, len(members))
	for i := range owner {
		owner[i] = -1
	}
	seen := make([]bool, len(members))
	var find func(int) bool
	find = func(i int) bool {
		q := slots[i].quota
		for j, member := range members {
			if seen[j] || excluded[member.ID] || !member.Complete || member.Draining ||
				member.Expected&q.Directions != q.Directions || !hasCandidate(q, member.Candidate) {
				continue
			}
			seen[j] = true
			if owner[j] < 0 || find(owner[j]) {
				owner[j] = i
				return true
			}
		}
		return false
	}
	for i := range slots {
		clear(seen)
		find(i)
	}
	return owner
}

func (sc *SessionControl) plan(target SessionTarget, v CapacitySnapshot) reconcilePlan {
	members := v.Members
	slices.SortStableFunc(members, func(a, b MemberSnapshot) int {
		return cmp.Or(cmp.Compare(sc.ctl.index[a.Candidate], sc.ctl.index[b.Candidate]), cmp.Compare(a.ID, b.ID))
	})
	p := reconcilePlan{need: v.Required &^ v.Covered, revision: target.Revision}
	excluded := make(map[MemberID]bool)
	for _, r := range target.Replace {
		for _, id := range r.Members {
			if slices.ContainsFunc(members, func(m MemberSnapshot) bool { return m.ID == id && !m.Draining }) && !excluded[id] {
				excluded[id] = true
				p.drain = append(p.drain, id)
				p.epoch = max(p.epoch, r.MinEpoch)
			}
		}
	}
	p.replacement = len(p.drain) != 0
	slots := slotsFor(target, len(members))
	owners := matchMembers(slots, members, excluded)
	filled := make([]bool, len(slots))
	for member, i := range owners {
		if i >= 0 {
			filled[i] = true
			p.preferences = append(p.preferences, matchedPreference{member: members[member], slot: slots[i]})
		}
	}
	for i, slot := range slots {
		if !filled[i] {
			p.deficit++
			if p.group == nil || p.group.ID <= sc.lastGroup && slot.group.ID > sc.lastGroup {
				p.quota, p.group = slot.quota, slot.group
			}
		}
	}
	if p.need != 0 {
		// Recovery can use any registered member that supplies a missing
		// direction. Quotas are considered again once coverage returns.
		p.quota, p.group, p.drain, p.epoch = nil, nil, nil, 0
		return p
	}
	if len(p.drain) == 0 && len(target.Groups) != 0 {
		for i, m := range members {
			if m.Draining || m.Candidate == "" {
				continue
			}
			if owners[i] < 0 {
				p.drain = append(p.drain, m.ID)
				continue
			}
			slot := slots[owners[i]]
			if p.quota == nil && m.Stable == m.Expected {
				if candidate := sc.upgradeCandidate(m.Candidate, slot.quota); candidate != "" {
					p.group, p.quota = slot.group, slot.quota
					p.drain, p.upgrade = []MemberID{m.ID}, candidate
					p.replacement = true
					break
				}
			}
			if deadline := sc.rotationDeadline(slot.group.Rotation, m); !deadline.IsZero() {
				if !sc.c.clk.source().Now().Before(deadline) && p.quota == nil {
					p.group, p.quota = slot.group, slot.quota
					p.drain = []MemberID{m.ID}
					p.replacement = true
					break
				}
				p.next = earliest(p.next, deadline)
			}
		}
	}
	if len(p.drain) > 1 {
		// The planned drain is atomic. Bound its layout by the same
		// cleanup capacity that its commit will reserve.
		available := sc.ctl.opts.Limits.CleanupAttachments
		for i, id := range p.drain {
			for _, member := range members {
				if member.ID == id {
					available -= len(member.Pipes)
					break
				}
			}
			if available < 0 {
				p.drain = p.drain[:max(i, 1)]
				break
			}
		}
	}
	return p
}

func (sc *SessionControl) rotationDeadline(policy RotationPolicy, m MemberSnapshot) time.Time {
	var next time.Time
	if policy.MaxAge > 0 {
		next = m.JoinedAt.Add(policy.MaxAge)
	}
	if m.Rotate {
		next = sc.c.clk.source().Now()
	}
	return next
}

func earliest(a, b time.Time) time.Time {
	if a.IsZero() || !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}

func (p reconcilePlan) layouts() []CandidateID {
	if p.upgrade != "" {
		return []CandidateID{p.upgrade}
	}
	if p.quota == nil {
		return nil
	}
	allowed := make([]CandidateID, 0, len(p.quota.Candidates))
	for _, c := range p.quota.Candidates {
		allowed = append(allowed, c.ID)
	}
	return allowed
}

func candidateDirections(c Candidate) Directions {
	var out Directions
	for _, a := range c.Attachments {
		out |= a.Directions
	}
	return out
}

// choose considers ordinary maintenance offers, or ordinary-ready, ordinary-cooling and
// standby offers for recovery. A recovery transaction supplies its tried set.
// choose returns a candidate and its reservation on success. On failure,
// the wake time may identify the next opportunity to retry arbitration.
func (sc *SessionControl) choose(p reconcilePlan, round *recoveryRound) (Candidate, *controllerWork, time.Time, error) {
	sc.demand.starts.prepare(p)
	candidates := sc.ctl.order(p.layouts(), false)
	var wake time.Time
	reason := ErrNoEligibleCandidate
	intent := AttemptGrowth
	if p.need != 0 {
		intent = AttemptRecovery
		candidates = sc.ctl.recoveryOrder(candidates)
	}

	for _, c := range candidates {
		dirs := candidateDirections(c)
		if round != nil && round.tried[c.ID] {
			continue
		}
		key := candKey(c.CacheKey, c)
		if p.need != 0 && dirs&p.need == 0 || p.quota != nil && dirs&p.quota.Directions != p.quota.Directions {
			sc.ctl.suppressStart(&sc.demand, key, time.Time{}, ErrNoDirectionCoverage)
			continue
		}
		var work *controllerWork
		var next time.Time
		var err error
		sc.c.peek(func() {
			switch after := sc.dialAfter[c.ID]; {
			case sc.ctx != nil && !sc.currentLocked(p.revision):
				err = ErrStaleBinding
				return
			case intent == AttemptRecovery && sc.c.clk.source().Now().Before(sc.recoveryWakeAt(c, sc.recoveryCutoffLocked())):
				err, next = ErrRecoveryWait, sc.recoveryWakeAt(c, sc.recoveryCutoffLocked())
				if after.After(next) {
					next = after
				}
			case !sc.c.s.CandidateAllowed(string(c.ID), dirs):
				err = ErrTopologyExcluded
			case sc.c.clk.source().Now().Before(after):
				err, next = ErrControllerDialAfter, after
			default:
				var cutoff time.Time
				if intent == AttemptRecovery {
					cutoff = sc.recoveryCutoffLocked()
				}
				work, next, err = sc.ctl.acquire(c, intent, &sc.demand, len(c.Attachments), cutoff)
				if work != nil {
					work.round = round
				}
				sc.reservation = work
				return
			}
			if intent == AttemptRecovery && !errors.Is(err, ErrRecoveryWait) {
				sc.ctl.cache.withdrawWaiter(key, sc.c)
			}
			sc.ctl.suppressStart(&sc.demand, key, next, err)
		})
		if err == nil {
			return c, work, time.Time{}, nil
		}
		wake = earliest(wake, next)
		reason = err
		if sharedControllerBlock(err) || errors.Is(err, ErrControllerQueueFull) {
			break
		}
	}
	return Candidate{}, nil, wake, reason
}

func (ctl *SessionController) recoveryOrder(order []Candidate) []Candidate {
	scores := make(map[CandidateID]CarrierScore, len(order))
	for _, c := range order {
		scores[c.ID] = ctl.cache.Score(c.CacheKey, c.Kind, c.Variant)
	}
	stage := func(c Candidate) int {
		if c.Standby {
			return 2
		}
		if !scores[c.ID].CoolingUntil.IsZero() {
			return 1
		}
		return 0
	}
	slices.SortStableFunc(order, func(a, b Candidate) int {
		if n := cmp.Or(cmp.Compare(stage(a), stage(b)), cmp.Compare(a.Tier, b.Tier)); n != 0 {
			return n
		}
		sa, sb := scores[a.ID], scores[b.ID]
		if stage(a) == 1 {
			if n := sa.CoolingUntil.Compare(sb.CoolingUntil); n != 0 {
				return n
			}
		}
		return cmp.Or(
			cmp.Compare(carrierScoreOrder(sb.Score), carrierScoreOrder(sa.Score)),
			cmp.Compare(ctl.index[a.ID], ctl.index[b.ID]),
		)
	})
	return order
}

func applyPreference(c Candidate, q *MemberQuota) MemberSpec {
	m := MemberSpec{Candidate: c.ID, Attachments: slices.Clone(c.Attachments)}
	var pref TargetCandidate
	if q != nil {
		for _, x := range q.Candidates {
			if x.ID == c.ID {
				pref = x
				break
			}
		}
	}
	for i := range m.Attachments {
		a := &m.Attachments[i]
		o := &a.Options
		if a.Directions&Send != 0 {
			o.SendPriority, o.SendStandby = Set(c.Tier), Set(c.Standby)
			if p := pref.Send; p != nil {
				o.SendPriority, o.SendStandby, o.Weight = Set(p.Tier), Set(p.Standby), Set(p.Weight)
			}
		}
		if a.Directions&Receive != 0 {
			o.ReceivePriority, o.ReceiveStandby = Set(c.Tier), Set(c.Standby)
			if p := pref.Receive; p != nil {
				o.ReceivePriority, o.ReceiveStandby = Set(p.Tier), Set(p.Standby)
			}
		}
	}
	return m
}

func (sc *SessionControl) run() {
	defer sc.finish()
	for sc.ctx.Err() == nil {
		domain := sc.ctl.watch()
		cache := sc.ctl.cache.watch()
		var capacity CapacitySnapshot
		var target SessionTarget
		sc.c.peek(func() {
			capacity, target = sc.c.capacityLocked(), sc.target
			if sc.c.control == sc {
				sc.updateRecoveryLocked()
			}
		})
		plan := sc.plan(target, capacity)
		sc.applyPreferences(target.Revision, plan.preferences)
		if plan.quota == nil && plan.need == 0 && len(plan.drain) == 0 {
			sc.demand.starts.prepare(plan)
			sc.dropDemand(target.Revision)
			sc.setProgress(target.Revision, plan.deficit, plan.next, nil)
			sc.wait(capacity.Changed, domain, cache, plan.next)
			continue
		}
		var next time.Time
		var err error
		if plan.need != 0 {
			next, err = sc.recover(target.Revision, plan)
		} else {
			next, err = sc.reconcile(target.Revision, capacity, plan)
		}
		if err != nil {
			if p := plan.group; p != nil && p.ID == sc.lastGroup && firedAttempt(err) && len(target.Groups) > 1 {
				continue
			}
			if !sharedControllerBlock(err) && !errors.Is(err, ErrControllerQueueFull) {
				sc.dropDemand(target.Revision)
				domain = sc.ctl.watch()
			}
			sc.setProgress(target.Revision, plan.deficit, next, err)
			var after CapacitySnapshot
			var current uint64
			sc.c.peek(func() { after, current = sc.c.capacityLocked(), sc.target.Revision })
			if current != target.Revision || after.PolicyRevision != capacity.PolicyRevision ||
				after.Covered != capacity.Covered || after.Stable != capacity.Stable ||
				after.OwnAttachments < capacity.OwnAttachments || after.LocalCarriers < capacity.LocalCarriers {
				continue
			}
			sc.wait(after.Changed, domain, cache, next)
		}
	}
}

type controllerAttemptError struct {
	error
	candidate CandidateID
}

func (err controllerAttemptError) Unwrap() error { return err.error }

func firedAttempt(err error) bool {
	var attempt controllerAttemptError
	return errors.As(err, &attempt)
}

func (sc *SessionControl) dropDemand(revision uint64) {
	sc.c.peek(func() {
		if sc.target.Revision == revision {
			sc.ctl.dropDemand(&sc.demand)
		}
	})
}

func (sc *SessionControl) setProgress(revision uint64, unfilled int, next time.Time, err error) {
	sc.c.peek(func() {
		if sc.currentLocked(revision) {
			sc.view.Unfilled, sc.view.NextRetry, sc.view.Block = unfilled, next, err
		}
	})
}

func (sc *SessionControl) finish() {
	sc.stopEnd()
	sc.c.locked(func() {
		if sc.c.control == sc {
			sc.detachLocked()
		}
	})
	sc.ctl.mu.Lock()
	defer sc.ctl.mu.Unlock()
	sc.ctl.dropDemandLocked(&sc.demand)
	sc.ctl.controls--
	sc.ctl.finishLocked()
}

func (sc *SessionControl) reconcile(revision uint64, capacity CapacitySnapshot, p reconcilePlan) (time.Time, error) {
	req := ChangeRequest{PolicyRevision: capacity.PolicyRevision, Observed: capacity.Observed, Timeout: sc.ctl.opts.Limits.AttemptTimeout, Drain: p.drain, MinEpoch: p.epoch}
	if p.epoch != 0 {
		var pending bool
		var err error
		sc.c.locked(func() {
			if !sc.currentLocked(revision) {
				err = ErrStaleBinding
				return
			}
			if p.epoch > sc.c.s.Epoch() {
				pending = true
				sc.c.s.SetNow(sc.c.clk.now())
				err = apiErr(sc.c.s.AdvanceEpoch())
			}
		})
		if err != nil {
			return time.Time{}, err
		}
		if pending {
			return time.Time{}, ErrChangePending
		}
	}
	cause := AttemptGrowth
	if len(p.drain) != 0 {
		cause = AttemptReplace
	}
	var candidate Candidate
	var work *controllerWork
	if p.quota != nil {
		var next time.Time
		var err error
		candidate, work, next, err = sc.choose(p, nil)
		if err != nil {
			return next, err
		}
		req.Add = []MemberSpec{applyPreference(candidate, p.quota)}
		defer work.releaseKey()
		if p.group != nil {
			sc.lastGroup = p.group.ID
		}
	}
	tx, err := sc.c.beginChange(sc.ctx, req, cause, sc, revision)
	if err != nil {
		if work != nil {
			sc.suppressCurrentStart(revision, candidate, time.Time{}, err)
			work.releaseWork()
			work.releaseCleanup()
		}
		return time.Time{}, err
	}
	defer func() { _ = tx.Cancel() }()
	if work != nil {
		a, err := tx.startMember(0, work.reservedAt, func(ctx context.Context, req OpenRequest) ([]PipeIO, error) {
			defer work.releaseWork()
			return openCandidate(ctx, req, candidate)
		}, func() error { return sc.checkOpenerStart(revision, candidate, work) }, work)
		if err != nil {
			sc.suppressCurrentStart(revision, candidate, time.Time{}, err)
			sc.recordRefusal(revision, err)
			work.releaseWork()
			work.releaseCleanup()
			return sc.ctl.cache.Score(candidate.CacheKey, candidate.Kind, candidate.Variant).retryAt(), err
		}
		res, err := a.Wait(sc.ctx)
		work.releaseKey()
		if err != nil {
			return time.Time{}, err
		}
		if res.Reason == ReasonNoLongerNeeded {
			return time.Time{}, nil
		}
		if res.Reason != ReasonConfirmed {
			return sc.ctl.cache.Score(candidate.CacheKey, candidate.Kind, candidate.Variant).retryAt(), controllerAttemptError{error: res.Err, candidate: candidate.ID}
		}
	}
	for {
		domain := sc.ctl.watch()
		result, err := tx.commit(sc.reserveRetirementLocked)
		if !errors.Is(err, ErrChangePending) && !errors.Is(err, ErrControllerCleanup) && !errors.Is(err, ErrStaleDecision) {
			return time.Time{}, err
		}
		// SetTarget can renew the batch under a newer revision; the wait
		// then belongs to that revision and must withdraw its demand.
		var current uint64
		sc.c.peek(func() { current = tx.targetRevision })
		if !sharedControllerBlock(err) {
			sc.dropDemand(current)
			domain = sc.ctl.watch()
		}
		sc.setProgress(current, p.deficit, tx.deadline, err)
		select {
		case <-tx.ctx.Done():
			return time.Time{}, tx.ctx.Err()
		case <-result.Capacity.Changed:
		case <-domain:
		}
	}
}

func (sc *SessionControl) suppressCurrentStart(revision uint64, candidate Candidate, next time.Time, err error) {
	sc.c.peek(func() {
		if sc.currentLocked(revision) {
			sc.ctl.suppressStart(&sc.demand, candKey(candidate.CacheKey, candidate), next, err)
		}
	})
}

func (sc *SessionControl) recordRefusal(revision uint64, err error) {
	sc.c.peek(func() {
		if sc.currentLocked(revision) {
			sc.view.LastRefusal = err
		}
	})
}

func (sc *SessionControl) reserveRetirementLocked(ids []uint64) error {
	if sc.c.control != sc {
		return ErrStaleBinding
	}
	if err := sc.checkQualityLocked(sc.target, false); err != nil {
		return err
	}
	retiring := make([]*pipeDrv, 0, len(ids))
	for _, id := range ids {
		if pd := sc.c.pipes[id]; pd != nil && !pd.retiring {
			retiring = append(retiring, pd)
		}
	}
	if len(retiring) == 0 {
		return nil
	}
	ctl := sc.ctl
	ctl.mu.Lock()
	err := ctl.grantPermitsLocked(&sc.demand, controllerPermits{cleanup: len(retiring)})
	ctl.mu.Unlock()
	if err != nil {
		return err
	}
	for _, pd := range retiring {
		pd.gs.addAfterIO(func() { ctl.releaseCleanup(1, nil, &sc.demand) })
	}
	return nil
}

var errControllerBatchLayout = errors.New("zmux: internal controller batch contains multiple logical member transactions")

func openCandidate(ctx context.Context, req OpenRequest, c Candidate) ([]PipeIO, error) {
	ps, err := c.Open(ctx, req)
	for i := range ps {
		p := &ps[i]
		if err == nil && (p.Carrier != 0 && c.Kind != 0 && p.Carrier != c.Kind ||
			p.Variant != "" && c.Variant != "" && p.Variant != c.Variant) {
			err = ErrInvalidConfig
		}
		if p.Carrier == 0 {
			p.Carrier = c.Kind
		}
		if p.Variant == "" {
			p.Variant = c.Variant
		}
	}
	return ps, err
}

// checkOpenerStart runs with the session owner held after Join and opener
// capacity have been reserved.
func (sc *SessionControl) checkOpenerStart(revision uint64, candidate Candidate, work *controllerWork) error {
	if !sc.currentLocked(revision) {
		return ErrTargetConflict
	}
	if !sc.c.s.CandidateAllowed(string(candidate.ID), candidateDirections(candidate)) {
		return ErrTopologyExcluded
	}
	if sc.c.clk.source().Now().Before(sc.dialAfter[candidate.ID]) {
		return ErrControllerDialAfter
	}
	if err := work.key.start(); err != nil {
		return err
	}
	sc.demand.starts.started(candKey(candidate.CacheKey, candidate))
	sc.reservation = nil
	return nil
}

func (ctl *SessionController) reportAttemptFailure(key CarrierCacheKey, retry time.Duration) {
	_ = ctl.cache.applyFeedback(key, CacheFailed, CarrierObservation{}, cacheFeedback{retry: retry})
}

type installedPreference struct {
	group    GroupID
	quota    QuotaID
	revision uint64
}

func (sc *SessionControl) applyPreferences(revision uint64, preferences []matchedPreference) {
	sc.c.locked(func() {
		if !sc.currentLocked(revision) {
			return
		}
		if sc.preferences == nil {
			sc.preferences = make(map[MemberID]installedPreference, len(preferences))
		}
		active := make(map[MemberID]bool, len(preferences))
		for _, item := range preferences {
			m, slot := item.member, item.slot
			active[m.ID] = true
			applied := installedPreference{revision: revision, group: slot.group.ID, quota: slot.quota.ID}
			if existing, ok := sc.preferences[m.ID]; ok && existing == applied {
				continue
			}
			rotation := slot.group.Rotation
			sc.c.s.SetMemberRotation(m.ID, rotation.SendBytes, rotation.ReceiveBytes)
			candidate := sc.ctl.candidate(m.Candidate)
			for _, id := range m.Pipes {
				directions := sc.c.s.PipeDirections(uint64(id))
				if directions == 0 {
					continue
				}
				candidate.Attachments = []AttachmentSpec{{Directions: directions}}
				settings := applyPreference(candidate, slot.quota).Attachments[0].Options
				out, in := settings.SendPriority.v, settings.ReceivePriority.v
				standbyOut, standbyIn := settings.SendStandby.v, settings.ReceiveStandby.v
				weight := uint64(1)
				if n, set := settings.Weight.get(); set {
					weight = n
				}
				_ = sc.c.s.SetPipe(uint64(id), core.PipeSettings{SendPriority: &out, RecvPriority: &in,
					SendStandby: &standbyOut, RecvStandby: &standbyIn, Weight: &weight})
			}
			sc.preferences[m.ID] = applied
		}
		for id := range sc.preferences {
			if !active[id] {
				delete(sc.preferences, id)
			}
		}
	})
}

func (sc *SessionControl) upgradeCandidate(current CandidateID, quota *MemberQuota) CandidateID {
	if sc.ctl.opts.Choice != ChoiceD160 {
		return ""
	}
	c := sc.ctl.candidate(current)
	floor := sc.ctl.cache.Score(c.CacheKey, c.Kind, c.Variant).UpgradeScore * sc.ctl.opts.UpgradeRatio.v
	ids := make([]CandidateID, 0, len(quota.Candidates))
	for _, x := range quota.Candidates {
		ids = append(ids, x.ID)
	}
	for _, candidate := range sc.ctl.order(ids, false) {
		if candidate.ID == current || candidate.Standby || candidate.Tier > c.Tier ||
			candidateDirections(candidate)&quota.Directions != quota.Directions {
			continue
		}
		score := sc.ctl.cache.Score(candidate.CacheKey, candidate.Kind, candidate.Variant)
		if score.Attempts >= 3 && score.retryAt().IsZero() && score.UpgradeScore >= floor {
			return candidate.ID
		}
	}
	return ""
}
