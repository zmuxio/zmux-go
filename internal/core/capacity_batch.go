package core

import (
	"errors"
	"math"
	"slices"
)

var (
	ErrCapacityBlocked   = errors.New("zmux: blocked_capacity")
	ErrChangePending     = errors.New("zmux: settle_pending")
	ErrUnknownMember     = errors.New("zmux: unknown_member")
	ErrCandidateExcluded = errors.New("zmux: candidate_excluded")
)

type ChangeMember struct {
	Candidate   string
	Directions  Directions
	Attachments int
}

type CapacityBatch struct {
	s        *Session
	reserved []*JoinReservation
	plans    []ChangeMember
	added    []*Member
	drain    []MemberID
	revision uint64
	minEpoch uint64
	deadline Time
	finished bool
	recovery bool
}

func (s *Session) BeginCapacityBatch(revision, minEpoch uint64, deadline Time, add []ChangeMember, drain []MemberID) (*CapacityBatch, error) {
	return s.beginCapacityBatch(revision, minEpoch, deadline, add, drain, false)
}

func (s *Session) beginCapacityBatch(revision, minEpoch uint64, deadline Time, add []ChangeMember, drain []MemberID, recovery bool) (*CapacityBatch, error) {
	if s.isClosing() {
		return nil, s.err
	}
	if s.mp == nil {
		return nil, ErrNotNegotiated
	}
	if revision != s.policy.Revision {
		return nil, ErrPolicyConflict
	}
	if s.now >= deadline || deadline == Never && !recovery {
		return nil, ErrJoinTimeout
	}
	removing := 0
	for i, id := range drain {
		m := s.member(id)
		if m == nil {
			return nil, ErrUnknownMember
		}
		if m.batch != nil || slices.Contains(drain[:i], id) {
			return nil, ErrCapacityBlocked
		}
		for _, p := range m.pipes {
			if p.mp.joinerLocal {
				removing++
			}
		}
	}
	count := 0
	var directions Directions
	for _, plan := range add {
		if plan.Attachments <= 0 || plan.Directions == 0 || plan.Directions&^DirectionBoth != 0 || plan.Attachments > math.MaxInt-count {
			return nil, ErrInvalid
		}
		if !s.CandidateAllowed(plan.Candidate, plan.Directions) {
			return nil, ErrCandidateExcluded
		}
		count += plan.Attachments
		directions |= plan.Directions
	}
	if count != 0 {
		covered, _ := s.Coverage(nil)
		if err := s.allowCapacityBatch(count, directions, removing, s.RequiredDirections()&^covered); err != nil {
			return nil, err
		}
	}
	b := &CapacityBatch{s: s, plans: slices.Clone(add), drain: slices.Clone(drain), added: make([]*Member, len(add)),
		reserved: make([]*JoinReservation, len(add)), revision: revision, minEpoch: minEpoch, deadline: deadline, recovery: recovery}
	for i, plan := range add {
		b.reserved[i] = s.reserveJoins(plan.Attachments)
	}
	for _, id := range drain {
		s.member(id).batch = b
	}
	s.NotifyCapacity()
	return b, nil
}

func (s *Session) allowCapacityBatch(n int, directions Directions, removing int, need Directions) error {
	m := s.mp
	if m.keysPending {
		return s.joinPendingErr()
	}
	if s.now < m.backoffUntil {
		return ErrCapacityBlocked
	}
	recovery := directions&need != 0
	steadyRoom := s.maintenanceJoinRoom() - m.reservedJoins
	if n > steadyRoom && !recovery && (removing == 0 || n-removing > max(steadyRoom, 0)) {
		return ErrCapacityBlocked
	}
	room := s.joinCap(true) - s.ownGoing() - m.reservedJoins
	if n > room {
		// A suspended session may reclaim one failed or redundant path
		// for a Join supplying missing coverage.
		if n != room+1 || s.state != StateSuspended || !recovery ||
			!s.makeRoom(true, directions&DirectionReceive != 0, directions&DirectionSend != 0) {
			return ErrCapacityBlocked
		}
	}
	return nil
}

func (b *CapacityBatch) Valid() error {
	switch {
	case b.finished:
		return ErrTooLate
	case b.s.isClosing():
		return b.s.err
	case b.s.now >= b.deadline:
		return ErrJoinTimeout
	case b.revision != b.s.policy.Revision:
		return ErrPolicyConflict
	}
	return nil
}

func (b *CapacityBatch) StartMember(index int) (*Member, *JoinReservation, error) {
	if err := b.Valid(); err != nil {
		return nil, nil, err
	}
	if index < 0 || index >= len(b.plans) || b.added[index] != nil {
		return nil, nil, ErrInvalid
	}
	if b.reserved[index].remaining == 0 {
		return nil, nil, ErrCapacityBlocked
	}
	if b.s.Epoch() < b.minEpoch {
		return nil, nil, ErrChangePending
	}
	plan := b.plans[index]
	m := b.s.NewMember(plan.Candidate, plan.Directions, plan.Attachments)
	if m == nil {
		return nil, nil, ErrCapacityBlocked
	}
	b.added[index] = m
	return m, b.reserved[index], nil
}

// BeginRecoveryBatch opens one logical member transaction. Alternatives
// reserve their concrete layout only when the owner grants an attempt.
func (s *Session) BeginRecoveryBatch(revision uint64) (*CapacityBatch, error) {
	return s.beginCapacityBatch(revision, 0, Never, nil, nil, true)
}

// RecoveryNeed excludes all provisional members of this transaction. A
// partial confirmation cannot hide work from another alternative.
func (b *CapacityBatch) RecoveryNeed() Directions {
	var covered Directions
	for _, p := range b.s.pipes {
		if p.member == nil || p.member.batch != b {
			c, _ := b.s.directionCoverage(p)
			covered |= c
		}
	}
	return b.s.RequiredDirections() &^ covered
}

func (b *CapacityBatch) StartAlternative(plan ChangeMember) (*Member, *JoinReservation, error) {
	if !b.recovery || plan.Attachments <= 0 || plan.Directions == 0 || plan.Directions&^DirectionBoth != 0 {
		return nil, nil, ErrInvalid
	}
	if err := b.Valid(); err != nil {
		return nil, nil, err
	}
	if !b.s.CandidateAllowed(plan.Candidate, plan.Directions) {
		return nil, nil, ErrCandidateExcluded
	}
	if err := b.s.allowCapacityBatch(plan.Attachments, plan.Directions, 0, b.RecoveryNeed()); err != nil {
		return nil, nil, err
	}
	m := b.s.NewMember(plan.Candidate, plan.Directions, plan.Attachments)
	if m == nil {
		return nil, nil, ErrCapacityBlocked
	}
	r := b.s.reserveJoins(plan.Attachments)
	m.batch = b
	b.added, b.reserved = append(b.added, m), append(b.reserved, r)
	return m, r, nil
}

func (b *CapacityBatch) releaseAlternative(m *Member) {
	m.batch = nil
	i := slices.Index(b.added, m)
	b.s.ReleaseJoins(b.reserved[i])
	b.added = slices.Delete(b.added, i, i+1)
	b.reserved = slices.Delete(b.reserved, i, i+1)
}

func (b *CapacityBatch) DiscardAlternative(m *Member) {
	if m.batch == b {
		b.releaseAlternative(m)
		b.s.RollbackMember(m)
	}
}

// CommitAlternative makes the confirmed member's directions available to
// the rest of the transaction. Other provisional layouts retain their slots.
func (b *CapacityBatch) CommitAlternative(m *Member) error {
	if !b.recovery || m.batch != b {
		return ErrInvalid
	}
	if err := b.Valid(); err != nil {
		return err
	}
	if !b.s.memberReady(m, false) {
		return ErrChangePending
	}
	b.releaseAlternative(m)
	return nil
}

// FinishRecovery closes a transaction after all remaining alternatives
// have been resolved under the same owner.
func (b *CapacityBatch) FinishRecovery() {
	if !b.recovery || len(b.added) != 0 {
		panic("zmux: recovery transaction still holds alternatives")
	}
	b.finish()
}

func (b *CapacityBatch) Commit() ([]MemberID, error) {
	return b.CommitWithRetirement(nil)
}

// CommitWithRetirement admits host cleanup after validating the complete drain.
func (b *CapacityBatch) CommitWithRetirement(admit func([]uint64) error) ([]MemberID, error) {
	if err := b.Valid(); err != nil {
		return nil, err
	}
	if b.recovery {
		return nil, ErrChangePending
	}
	if b.s.Epoch() < b.minEpoch {
		return nil, ErrChangePending
	}
	pending := false
	for _, m := range b.added {
		if !b.s.memberReady(m, len(b.drain) != 0) {
			pending = true
		}
	}
	if pending {
		return nil, ErrChangePending
	}
	var pipes []uint64
	for _, id := range b.drain {
		var ok bool
		pipes, ok = b.s.MemberPipes(id, pipes)
		if !ok {
			return nil, ErrUnknownMember
		}
	}
	if len(pipes) != 0 {
		if err := b.s.drainAttachments(pipes, admit); err != nil {
			return nil, err
		}
	}
	b.finish()
	return slices.Clone(b.drain), nil
}

func (s *Session) memberReady(m *Member, requireStable bool) bool {
	if m == nil {
		return false
	}
	ready := len(m.pipes) == m.Attachments
	for _, p := range m.pipes {
		covered, stable := s.directionCoverage(p)
		expected := directionsOf(p.in != nil, p.out != nil)
		if covered != expected || requireStable && stable != expected {
			s.ObserveStability(p.id)
			ready = false
		}
	}
	return ready
}

func (b *CapacityBatch) Cancel() {
	if b.finished {
		return
	}
	b.finish()
	if b.recovery {
		for len(b.added) > 0 {
			b.DiscardAlternative(b.added[len(b.added)-1])
		}
	} else {
		for _, m := range b.added {
			b.s.RollbackMember(m)
		}
	}
}

func (b *CapacityBatch) finish() {
	b.finished = true
	for _, r := range b.reserved {
		b.s.ReleaseJoins(r)
	}
	for _, id := range b.drain {
		if m := b.s.member(id); m != nil && m.batch == b {
			m.batch = nil
		}
	}
	b.s.NotifyCapacity()
}

func (s *Session) RollbackMember(m *Member) {
	if m == nil {
		return
	}
	for _, p := range slices.Clone(m.pipes) {
		var except = [1]uint64{p.id}
		covered, _ := s.directionCoverage(p)
		remaining, _ := s.Coverage(except[:])
		if covered&^remaining == 0 {
			_ = s.removePipe(p.id, covered != 0 || !s.pipeHealth(p).PeerAttached)
		}
	}
	s.ForgetEmptyMember(m)
}
