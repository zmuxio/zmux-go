package core

import (
	"math"
	"slices"
)

// MemberID is stable for the life of a group of attachments.
type MemberID uint64

// Member owns an immutable layout and its currently indexed attachments.
type Member struct {
	// Attribution identifies a candidate that the host registered against this
	// member. Preserve the binding through ordinary controller changes.
	AttributionKey     string
	AttributionVariant string
	AttributionLayout  string
	batch              *CapacityBatch
	rotation           *memberRotation
	Candidate          string
	pipes              []*Pipe
	ID                 MemberID
	Attachments        int
	JoinedAt           Time
	generation         uint64
	AttributionKind    uint32
	Expected           Directions
	covered, stable    Directions
	peer               bool
}

type MemberSnapshot struct {
	Candidate          string
	Pipes              []uint64
	ID                 MemberID
	Attachments        int
	JoinedAt           Time
	Rotate             bool
	Expected, Covered  Directions
	Stable             Directions
	Complete, Draining bool
}

func (s *Session) NewMember(candidate string, directions Directions, attachments int) *Member {
	if s.memberID == math.MaxUint64 {
		return nil
	}
	s.memberID++
	m := &Member{ID: s.memberID, Candidate: candidate, Expected: directions, Attachments: attachments, JoinedAt: s.now}
	s.members = append(s.members, m)
	s.NotifyCapacity()
	return m
}

func (s *Session) addMemberPipe(m *Member, p *Pipe, expected Directions) {
	if m == nil {
		m = s.NewMember("", expected, 1)
	}
	if m != nil {
		m.pipes = append(m.pipes, p)
		p.member = m
		s.applyMemberTopology(p)
		s.NotifyCapacity()
	}
}

func (s *Session) forgetMemberPipe(p *Pipe) {
	if m := p.member; m != nil {
		i := slices.Index(m.pipes, p)
		m.pipes = slices.Delete(m.pipes, i, i+1)
		p.member = nil
		s.memberHealthChanged(m, false, len(m.pipes) == 0, nil)
		s.ForgetEmptyMember(m)
	}
}

func (s *Session) ForgetEmptyMember(m *Member) {
	if m != nil && len(m.pipes) == 0 {
		if i := slices.Index(s.members, m); i >= 0 {
			s.members = slices.Delete(s.members, i, i+1)
			s.NotifyCapacity()
		}
	}
}

func (s *Session) SetBootstrapCandidate(candidate string) {
	if p := s.pipe(0); p != nil && p.member != nil {
		p.member.Candidate = candidate
		s.applyMemberTopology(p)
		s.NotifyCapacity()
	}
}

// CandidateIdentity returns the owner-verified candidate identity until
// retirement, independently of a controller's registration set.
func (s *Session) CandidateIdentity(id MemberID) *Member {
	if m := s.member(id); m != nil && len(m.pipes) > 0 {
		return m
	}
	return nil
}

func (s *Session) MemberOf(pipe uint64) MemberID {
	if p := s.pipe(pipe); p != nil && p.member != nil {
		return p.member.ID
	}
	return 0
}

func (s *Session) member(id MemberID) *Member {
	for _, m := range s.members {
		if m.ID == id {
			return m
		}
	}
	return nil
}

func (s *Session) MemberPipes(id MemberID, dst []uint64) ([]uint64, bool) {
	m := s.member(id)
	if m == nil || len(m.pipes) == 0 {
		return dst, false
	}
	for _, p := range m.pipes {
		dst = append(dst, p.id)
	}
	return dst, true
}

func (s *Session) Members(dst []MemberSnapshot) []MemberSnapshot {
	for _, m := range s.members {
		if len(m.pipes) == 0 {
			continue
		}
		v := MemberSnapshot{Rotate: m.rotation != nil && m.rotation.due(), ID: m.ID, Candidate: m.Candidate, Expected: m.Expected, JoinedAt: m.JoinedAt,
			Pipes: make([]uint64, 0, len(m.pipes)), Attachments: m.Attachments, Complete: len(m.pipes) == m.Attachments}
		for _, p := range m.pipes {
			v.Pipes = append(v.Pipes, p.id)
			covered, stable := s.directionCoverage(p)
			v.Covered |= covered
			v.Stable |= stable
			v.Draining = v.Draining || p.mp != nil && p.mp.draining
			v.Complete = v.Complete && covered == directionsOf(p.in != nil, p.out != nil)
		}
		dst = append(dst, v)
	}
	return dst
}

func (s *Session) revokePeerMembers(generation uint64) {
	for _, m := range slices.Clone(s.members) {
		if m.peer && m.generation < generation {
			for _, p := range slices.Clone(m.pipes) {
				s.startDrain(p)
			}
		}
	}
}

func directionsOf(in, out bool) Directions {
	var d Directions
	if in {
		d |= DirectionReceive
	}
	if out {
		d |= DirectionSend
	}
	return d
}

func (s *Session) CandidateAllowed(candidate string, directions Directions) bool {
	allowed, set := s.policy.Topology[candidate]
	return !set || directions&^allowed == 0
}

func (s *Session) applyMemberTopology(p *Pipe) {
	if p.member == nil || p.mp == nil {
		return
	}
	s.SetTopologyExclusion(p.id,
		!s.CandidateAllowed(p.member.Candidate, DirectionSend),
		!s.CandidateAllowed(p.member.Candidate, DirectionReceive))
}

type memberRotation struct {
	sendLimit, receiveLimit uint64
	sent, received          uint64
}

func (r *memberRotation) due() bool {
	return r.sendLimit != 0 && r.sent >= r.sendLimit || r.receiveLimit != 0 && r.received >= r.receiveLimit
}

func (s *Session) SetMemberRotation(id MemberID, send, receive uint64) {
	m := s.member(id)
	if m == nil {
		return
	}
	if send == 0 && receive == 0 {
		m.rotation = nil
		return
	}
	if m.rotation == nil {
		m.rotation = new(memberRotation)
	}
	m.rotation.sendLimit, m.rotation.receiveLimit = send, receive
}

func (s *Session) memberBytes(p *Pipe, send bool, n uint64) {
	if p.member == nil || p.member.rotation == nil {
		return
	}
	r := p.member.rotation
	due := r.due()
	if send {
		r.sent = addMemberBytes(r.sent, n)
	} else {
		r.received = addMemberBytes(r.received, n)
	}
	if !due && r.due() {
		s.NotifyCapacity()
	}
}

func addMemberBytes(a, b uint64) uint64 {
	if a+b < a {
		return math.MaxUint64
	}
	return a + b
}

func (s *Session) PipeDirections(id uint64) Directions {
	if p := s.pipe(id); p != nil {
		return directionsOf(p.in != nil, p.out != nil)
	}
	return 0
}

// MemberHealthChange is a control-path edge emitted only for managed sessions.
type MemberHealthChange struct {
	PhysicalFault   *PhysicalFault
	ID              MemberID
	At              Time
	Covered, Stable Directions
	Failed, Retired bool
}

func (s *Session) EnableMemberFeedback() {
	s.memberFeedback = true
	for _, p := range s.pipes {
		if p.mp != nil && p.out != nil {
			s.enablePathQuality(p)
		}
	}
}

func (s *Session) DisableMemberFeedback() {
	s.memberFeedback = false
	s.memberChanges = nil
	s.qualityUpdates = nil
	if !s.cfg.PathAdvice && len(s.pipes) == 1 && s.pipes[0].mp != nil {
		s.pipes[0].mp.quality = nil
	}
}

func (s *Session) TakeMemberChanges() []MemberHealthChange {
	q := s.memberChanges
	s.memberChanges = nil
	return q
}

func (s *Session) memberHealthChanged(m *Member, failed, retired bool, fault *PhysicalFault) {
	if m == nil || !s.memberFeedback {
		return
	}
	var covered, stable Directions
	for _, p := range m.pipes {
		c, st := s.directionCoverage(p)
		covered |= c
		stable |= st
	}
	if m.covered == covered && m.stable == stable && !failed && !retired {
		return
	}
	m.covered, m.stable = covered, stable
	s.memberChanges = append(s.memberChanges, MemberHealthChange{ID: m.ID, At: s.now,
		Covered: covered, Stable: stable, Failed: failed, Retired: retired, PhysicalFault: fault})
}
