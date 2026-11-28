package core

import (
	"math"
	"slices"
)

// CapacityLimits contains the resolved local limits. Protocol parameters
// retain the initial attachment envelope across policy updates.
type CapacityLimits struct {
	SteadyAttachments int
	JoinHeadroom      int
	LocalCarriers     int
	PendingPeerJoins  int
}

const (
	DefaultJoinHeadroom = 8
	// The replay window is twice the envelope. Replay comparisons add
	// twice the window to a protocol PipeID.
	maxAttachmentEnvelope = min(math.MaxInt/2, (1<<62-1)/4)
)

func DefaultCapacityLimits() CapacityLimits {
	l, _ := (CapacityLimits{JoinHeadroom: DefaultJoinHeadroom}).Resolve()
	return l
}

func (l CapacityLimits) Resolve() (CapacityLimits, bool) {
	if l.SteadyAttachments == 0 {
		l.SteadyAttachments = 1
	}
	if l.PendingPeerJoins == 0 {
		l.PendingPeerJoins = DefaultMaxUnconfirmed
	}
	if l.LocalCarriers == 0 && l.validEnvelope() {
		l.LocalCarriers = 2 * l.Envelope()
	}
	return l, l.Valid()
}

func (l CapacityLimits) validEnvelope() bool {
	return l.SteadyAttachments > 0 && l.SteadyAttachments <= maxAttachmentEnvelope &&
		l.JoinHeadroom >= 0 && l.JoinHeadroom <= maxAttachmentEnvelope-l.SteadyAttachments
}

func (l CapacityLimits) Valid() bool {
	return l.validEnvelope() && l.LocalCarriers > 0 && l.PendingPeerJoins > 0
}

func (l CapacityLimits) Envelope() int { return l.SteadyAttachments + l.JoinHeadroom }

// SessionPolicy describes policy state needed by the core. Host callbacks
// associated with it are installed by the driver under the same lock.
type SessionPolicy struct {
	Topology       map[string]Directions
	Limits         CapacityLimits
	Revision       uint64
	AuthGeneration uint64
}

func (s *Session) SessionPolicy() SessionPolicy { return s.policy }

func (s *Session) SetSessionPolicy(next SessionPolicy) error {
	if s.isClosing() {
		return s.err
	}
	if !next.Limits.Valid() || next.Limits.Envelope() > s.cfg.SessionPolicy.Limits.Envelope() {
		return ErrInvalid
	}
	if next.Revision <= s.policy.Revision || next.AuthGeneration < s.policy.AuthGeneration {
		return ErrPolicyConflict
	}
	current := s.policy
	s.policy = next
	if s.mp != nil {
		if next.Limits.SteadyAttachments < current.Limits.SteadyAttachments ||
			next.Limits.Envelope() < current.Limits.Envelope() {
			s.trimJoinReservations()
		}
		s.joinHoldChanged()
		for _, p := range s.pipes {
			s.applyMemberTopology(p)
		}
		if next.AuthGeneration > current.AuthGeneration {
			s.revokePeerMembers(next.AuthGeneration)
		}
	}
	s.NotifyCapacity()
	return nil
}

// BeginPeerAuthorization bounds host authorization work before Claim.
// A slot is held until the callback exits, including after cancellation.
func (s *Session) BeginPeerAuthorization(pj *PeerJoin) bool {
	if pj.owner != s || pj.authorizing || s.isClosing() || s.authorizingPeers >= s.policy.Limits.PendingPeerJoins {
		return false
	}
	pj.authorizing = true
	s.authorizingPeers++
	s.NotifyCapacity()
	return true
}

func (s *Session) EndPeerAuthorization(pj *PeerJoin) {
	if pj.owner == s && pj.authorizing {
		pj.authorizing = false
		s.authorizingPeers--
		s.NotifyCapacity()
	}
}

func (s *Session) AuthorizingPeers() int { return s.authorizingPeers }

func (s *Session) PendingPeerJoins() int { return len(s.pendingPeers) }

func (s *Session) NotifyCapacity() {
	if s.capacitySeq < math.MaxUint64 {
		s.capacitySeq++
	}
	if s.capacityWatch != nil {
		close(s.capacityWatch)
		s.capacityWatch = nil
	}
}

func (s *Session) WatchCapacity() (uint64, <-chan struct{}) {
	if s.capacityWatch == nil {
		s.capacityWatch = make(chan struct{})
	}
	return s.capacitySeq, s.capacityWatch
}

func (s *Session) ProtocolCapacity() (own, peer, reserved, envelope int) {
	if s.mp == nil {
		if s.role == RoleResponder {
			return 0, len(s.pipes), 0, s.cfg.SessionPolicy.Limits.Envelope()
		}
		return len(s.pipes), 0, 0, s.cfg.SessionPolicy.Limits.Envelope()
	}
	return s.ownGoing(), s.peerAttachments(), s.mp.reservedJoins, s.cfg.SessionPolicy.Limits.Envelope()
}

// Directions is the set of local directions required or provided.
type Directions uint8

const (
	DirectionSend Directions = 1 << iota
	DirectionReceive
	DirectionBoth = DirectionSend | DirectionReceive
)

// RequiredDirections reports the directions the session must keep covered.
func (*Session) RequiredDirections() Directions { return DirectionBoth }

func (s *Session) directionCoverage(p *Pipe) (covered, stable Directions) {
	h := s.pipeHealth(p)
	if !h.PeerAttached {
		return 0, 0
	}
	if h.Send.State == HealthUsable || h.Send.State == HealthUnknown {
		covered |= DirectionSend
		if h.Send.Settled {
			stable |= DirectionSend
		}
	}
	if h.Receive.State == HealthUsable || h.Receive.State == HealthUnknown {
		covered |= DirectionReceive
		if h.Receive.Settled {
			stable |= DirectionReceive
		}
	}
	return covered, stable
}

func (s *Session) Coverage(except []uint64) (covered, stable Directions) {
	for _, p := range s.pipes {
		if !slices.Contains(except, p.id) {
			directions, settled := s.directionCoverage(p)
			covered |= directions
			stable |= settled
		}
	}
	return covered, stable
}

// DrainAttachments commits a batch after checking all participating
// attachments and coverage under the same owner.
func (s *Session) DrainAttachments(ids []uint64) error {
	return s.drainAttachments(ids, nil)
}

func (s *Session) drainAttachments(ids []uint64, admit func([]uint64) error) error {
	if s.isClosing() {
		return s.err
	}
	if s.mp == nil {
		return ErrNotNegotiated
	}
	for i, id := range ids {
		if slices.Contains(ids[:i], id) {
			return ErrInvalid
		}
		p := s.pipe(id)
		if p == nil || p.mp == nil || p.mp.retired {
			return ErrPipeGone
		}
	}
	if covered, _ := s.Coverage(ids); s.RequiredDirections()&^covered != 0 {
		return ErrNoDirectionCoverage
	}
	if admit != nil {
		if err := admit(ids); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if err := s.removePipe(id, true); err != nil {
			return err
		}
	}
	return nil
}
