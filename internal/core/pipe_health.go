package core

import (
	"errors"
)

// HealthState describes one local direction of a pipe.
type HealthState uint8

const (
	HealthAbsent HealthState = iota
	HealthJoining
	HealthUnknown
	HealthUsable
	HealthUnavailable
	HealthDraining
	HealthEnded
)

type HealthReason uint8

const (
	HealthNone HealthReason = iota
	HealthAwaitAttachment
	HealthObserveJoin
	HealthIdle
	HealthProbation
	HealthRevalidate
	HealthAckStall
	HealthWriteStall
	HealthInboundSilence
	HealthCarrierError
	HealthJoinTimeout
	HealthProgress
	HealthPlannedDrain
	HealthSessionClosing
	HealthCanceled
	HealthTopologyExcluded
)

// HealthProof is the committed evidence for the current direction generation.
type HealthProof uint8

const (
	HealthProofNone HealthProof = iota
	HealthProofProbe
	HealthProofRecord
)

type DirectionHealth struct {
	Since, StableSince Time
	Generation         uint64
	State              HealthState
	Reason             HealthReason
	CanCarry           bool
	Settled            bool
	Proof              HealthProof
	Stable             bool
}

type PipeHealth struct {
	Failure              error
	Send, Receive        DirectionHealth
	Seq                  uint64
	JoinedAt, AttachedAt Time
	PeerAttached         bool
}

// Settled reports whether all present directions passed the current hold.
func (h PipeHealth) Settled() bool {
	return h.PeerAttached && (h.Send.State == HealthAbsent || h.Send.Settled) &&
		(h.Receive.State == HealthAbsent || h.Receive.Settled)
}

type pipeHealth struct {
	snapshot      PipeHealth
	send, receive directionEvidence
	proofAsked    bool
	reason        HealthReason
}

type directionEvidence struct {
	stableSince         Time
	startPosition       uint64
	probeEnd, probeLane uint64
	generation          uint64
	running             bool
	proof               HealthProof
}

func (d *directionEvidence) update(running bool, now Time, position uint64) {
	if d.running == running {
		return
	}
	d.running, d.proof = running, HealthProofNone
	d.probeEnd = 0
	if running {
		d.stableSince, d.startPosition = now, position
		d.generation++
	}
}

func (s *Session) PipeHealth(id uint64) (PipeHealth, bool) {
	p := s.pipe(id)
	if p == nil || p.mp != nil && p.mp.peer == peerDiscarded {
		return PipeHealth{}, false
	}
	return s.pipeHealth(p), true
}

func (s *Session) pipeHealth(p *Pipe) PipeHealth {
	if p.mp == nil {
		h := PipeHealth{PeerAttached: s.peerSeen}
		if p.out != nil {
			h.Send = DirectionHealth{State: HealthUsable, CanCarry: !p.out.failed && !s.isClosing(), Settled: s.peerSeen}
			if p.out.failed || s.isClosing() {
				h.Send.State = HealthEnded
			}
		}
		if p.in != nil {
			h.Receive = DirectionHealth{State: HealthUsable, CanCarry: !p.in.ended && !s.isClosing(), Settled: s.peerSeen}
			if p.in.ended || s.isClosing() {
				h.Receive.State = HealthEnded
			}
		}
		return h
	}
	return p.mp.health.snapshot
}

func (s *Session) healthHold() Duration {
	if s.cfg.StabilityHold >= 0 {
		if s.cfg.StabilityHold == 0 {
			return 5 * Second
		}
		return s.cfg.StabilityHold
	}
	return 0
}

func (s *Session) syncPipeHealth(p *Pipe) {
	s.commitPipeHealth(p, false)
}

func (s *Session) commitPipeHealth(p *Pipe, receiveRecord bool) {
	a := p.mp
	if a == nil {
		return
	}
	h := &a.health
	old := h.snapshot
	now := s.now
	attached := a.peerAttached(p.id)
	if receiveRecord && p.out == nil {
		a.reval = false // Revalidation of an independent half waits for its next verified record.
	}
	healthy := attached && !s.isClosing() && !a.retired && !a.reval && !a.draining
	previousSend, previousReceive := h.send.generation, h.receive.generation
	var position uint64
	if p.out != nil {
		position = p.out.snd.so
	}
	h.send.update(healthy && p.out != nil && a.out == OutActive && !a.topologyOut, now, position)
	h.receive.update(healthy && p.in != nil && a.in == InOpen && !a.topologyIn, now, position)
	if receiveRecord && p.out == nil && h.receive.running && h.receive.proof == HealthProofNone {
		h.receive.proof = HealthProofRecord
	}
	if previousSend != h.send.generation || previousReceive != h.receive.generation {
		h.proofAsked = false
	}
	joined := a.liveAt
	proj := func(has, isOut bool, evidence directionEvidence, prior DirectionHealth) DirectionHealth {
		if !has {
			return DirectionHealth{}
		}
		out := DirectionHealth{Since: prior.Since, Generation: evidence.generation, Proof: evidence.proof, Stable: evidence.running}
		if evidence.running {
			out.StableSince = evidence.stableSince
		}
		switch {
		case s.isClosing():
			out.State, out.Reason = HealthEnded, HealthSessionClosing
		case a.retired || isOut && (a.out == OutClosed || a.out == OutFailed) || !isOut && a.in == InEnded:
			out.State, out.Reason = HealthEnded, h.reason
			switch {
			case errors.Is(a.failCause, ErrJoinTimeout):
				out.Reason = HealthJoinTimeout
			case errors.Is(a.failCause, ErrPipeAborted):
				out.Reason = HealthCanceled
			case out.Reason == HealthNone && a.draining:
				out.Reason = HealthPlannedDrain
			case out.Reason == HealthNone:
				out.Reason = HealthCarrierError
			}
		case a.draining || a.out == OutDraining:
			out.State, out.Reason = HealthDraining, HealthPlannedDrain
		case !attached || a.hs:
			out.State, out.Reason = HealthJoining, HealthAwaitAttachment
		case isOut && a.out == OutSuspect:
			out.State, out.Reason = HealthUnavailable, h.reason
			if out.Reason == HealthNone {
				out.Reason = HealthAckStall
			}
		case isOut && a.topologyOut || !isOut && a.topologyIn:
			out.State, out.Reason = HealthUnavailable, HealthTopologyExcluded
		case a.reval:
			out.State, out.Reason = HealthUnknown, HealthRevalidate
		default:
			out.CanCarry = !isOut || s.pipeUsable(p, false)
			out.Settled = evidence.running && evidence.proof != HealthProofNone && now.Sub(evidence.stableSince) >= s.healthHold() && (out.CanCarry || isOut && a.penalized && !a.inProbation())
			switch {
			case isOut && a.prob.on:
				out.State, out.Reason = HealthUnknown, HealthProbation
			case evidence.proof != HealthProofNone:
				out.State, out.Reason = HealthUsable, HealthProgress
			case now.Sub(joined) < 10*Second:
				out.State, out.Reason = HealthUnknown, HealthObserveJoin
			default:
				out.State, out.Reason = HealthUnknown, HealthIdle
			}
		}
		if out != prior {
			out.Since = now
		}
		return out
	}
	next := PipeHealth{Seq: old.Seq, JoinedAt: joined, PeerAttached: attached, AttachedAt: old.AttachedAt, Failure: a.failCause}
	if attached && !old.PeerAttached {
		next.AttachedAt = now
	}
	next.Send, next.Receive = proj(p.out != nil, true, h.send, old.Send), proj(p.in != nil, false, h.receive, old.Receive)
	notify := func(dir Dir, before, after DirectionHealth) {
		if before == after {
			return
		}
		s.healthSeq++
		s.NotifyCapacity()
		next.Seq = s.healthSeq
		s.emit(Event{Kind: EvPipeHealth, Pipe: p.id, Dir: dir, Count: next.Seq, At: now, Wait: now.Sub(joined),
			HealthBefore: before, HealthAfter: after, Cause: a.failCause})
	}
	notify(DirOut, old.Send, next.Send)
	notify(DirIn, old.Receive, next.Receive)
	h.snapshot = next
	if old.Send != next.Send {
		s.relativeStallChanged()
		s.wakePrefix()
	}
	if p.pathQuality() != nil && failedHealthEdge(old.Send, next.Send) {
		s.WithdrawMeasurement(p.id, CurrentRTTEvidence)
		s.WithdrawMeasurement(p.id, MinimumRTTEvidence)
		s.WithdrawMeasurement(p.id, DeliveryEvidence)
	}
	s.armPathDeadline(p, pathHealth)
	if s.memberFeedback && (old.Send != next.Send || old.Receive != next.Receive) {
		failed := failedHealthEdge(old.Send, next.Send) || failedHealthEdge(old.Receive, next.Receive)
		// A paired attachment's later failure also ends its receive side.
		// That coverage change belongs to the already reported stall.
		if old.Send.State == HealthUnavailable && faultReason(old.Send.Reason) && next.Send.State == HealthEnded {
			failed = false
		}
		s.memberHealthChanged(p.member, failed, false, a.physicalFault)
	}
}

func failedHealthEdge(before, after DirectionHealth) bool {
	if before.State == HealthEnded || before.State == HealthJoining ||
		before.State == HealthUnavailable && faultReason(before.Reason) {
		return false
	}
	return (after.State == HealthUnavailable || after.State == HealthEnded) && faultReason(after.Reason)
}

func faultReason(reason HealthReason) bool {
	switch reason {
	case HealthAckStall, HealthWriteStall, HealthInboundSilence, HealthCarrierError:
		return true
	default:
		return false
	}
}

func (s *Session) healthDeadline(p *Pipe) Time {
	h := &p.mp.health
	next := Never
	for _, x := range []struct {
		health   DirectionHealth
		evidence directionEvidence
	}{{h.snapshot.Send, h.send}, {h.snapshot.Receive, h.receive}} {
		if x.health.Reason == HealthObserveJoin {
			next = minT(next, h.snapshot.JoinedAt.Add(10*Second))
		}
		if x.evidence.running && x.evidence.proof != HealthProofNone && x.health.CanCarry && !x.health.Settled {
			next = minT(next, x.evidence.stableSince.Add(s.healthHold()))
		}
	}
	return next
}

func (s *Session) healthProof(p *Pipe, dir Dir) {
	if p.mp == nil {
		return
	}
	evidence := &p.mp.health.send
	if dir == DirIn {
		evidence = &p.mp.health.receive
	}
	if evidence.proof != HealthProofNone {
		return
	}
	s.syncPipeHealth(p)
	if evidence.running {
		evidence.proof = HealthProofProbe
		s.syncPipeHealth(p)
	}
}

// ObserveStability requests current-round evidence on a replacement which
// the host plans to commit. The ordinary PIPE_PING owner schedules the probe.
func (s *Session) ObserveStability(id uint64) {
	p := s.pipe(id)
	if p == nil || p.mp == nil || p.out == nil {
		return
	}
	a := p.mp
	h := &a.health
	if h.proofAsked || !h.send.running || h.send.proof != HealthProofNone && (p.in == nil || h.receive.proof != HealthProofNone) {
		return
	}
	h.proofAsked = true
	a.needPing, a.pingSince = true, s.now
	s.armPathDeadline(p, pathProbe)
	s.wakeAllWriters()
}

// ServiceDirections describes confirmed coverage without counting an effective
// fault as service. Deliberate hold-back and unknown idle paths still cover
// their directions, since they can recover without a new attachment.
func (s *Session) ServiceDirections(except uint64) (out, in bool) {
	return s.ServiceDirectionsWithout([]uint64{except})
}

// ServiceDirectionsWithout projects confirmed service after excluding attachments.
func (s *Session) ServiceDirectionsWithout(except []uint64) (out, in bool) {
	covered, _ := s.Coverage(except)
	return covered&DirectionSend != 0, covered&DirectionReceive != 0
}

// SetTopologyExclusion changes local service eligibility without retiring the pipe.
func (s *Session) SetTopologyExclusion(id uint64, out, in bool) {
	p := s.pipe(id)
	if p == nil || p.mp == nil {
		return
	}
	a := p.mp
	if a.topologyOut == out && a.topologyIn == in {
		return
	}
	outChanged := a.topologyOut != out
	a.topologyOut, a.topologyIn = out, in
	s.mp.control.valid = false
	if outChanged && out {
		s.dropDatagramCopies(p)
		s.reinjectPipe(p)
	}
	s.syncPipeHealth(p)
	s.refreshPathDeadlines(p)
	s.refreshTierPlacement()
	if outChanged {
		s.noteDatagramPath()
	}
	s.wakeAllWriters()
}
