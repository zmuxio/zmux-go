package core

import "github.com/zmuxio/zmux-go/internal/wire"

// mpAckEntry processes one PIPE_ACK entry of a multipath session
// (MULTIPATH.md 6.2).
func (s *Session) mpAckEntry(e wire.AckEntry, now Time) *wire.Error {
	m := s.mp
	p := s.pipe(e.PipeID)
	if p == nil {
		if s.ownParity(e.PipeID) && e.PipeID >= m.nextID {
			return wire.Errorf(wire.ProtocolViolation, "PIPE_ACK for an untaken pipe ID", "pipe", e.PipeID)
		}
		if !s.ownParity(e.PipeID) && e.Position > 0 && !m.idRetired(e.PipeID) {
			return s.ackBeyondSent(e)
		}
		return nil
	}
	a := p.mp
	if a.retired || e.Position == 0 {
		return nil
	}
	var ol *outLane
	if p.out != nil {
		ol = p.out.lane(e.Lane)
	}
	if ol == nil {
		return s.ackBeyondSent(e)
	}
	if ol.refused {
		return nil
	}
	if e.Position > ol.pos {
		return s.ackBeyondSent(e)
	}
	if e.Position <= ol.acked {
		return nil
	}
	ol.acked = e.Position
	s.ackLog(ol, e.Position)
	s.ack.advanced(p, ol, e, now)
	return nil
}

// ackBeyondSent: the peer claims data never sent (MULTIPATH.md 6.2,
// RESUME.md 7).
func (s *Session) ackBeyondSent(e wire.AckEntry) *wire.Error {
	code := uint64(wire.ProtocolViolation)
	if s.feat.Resume {
		code = wire.ResumeFailed
	}
	return wire.Errorf(code, "PIPE_ACK position beyond the bytes sent", "pipe", e.PipeID, "pos", e.Position)
}

// mpAdvanced updates the pipe after its acknowledged position advanced.
// mpAdvanced runs when the pipe's cumulative acknowledgement point moved.
func (s *Session) mpAdvanced(p *Pipe, ol *outLane, ps *pipeSend, now Time) {
	a := p.mp
	a.lastAdvance = now
	if len(ps.samples) == 0 {
		a.unacked = false
	} else {
		a.unacked, a.oldestAt = true, ps.samples[0].sent
	}
	switch a.out {
	case outJoining:
		if ol.lane == 0 {
			a.out = outActive
			s.joinConfirmed(p)
		}
	case outSuspect:
		if _, st := s.writeStall(p); !st {
			a.out = outActive
			s.startProbation(p)
			s.emitPipeState(p)
			s.checkTakeover()
		}
	}
	s.revalAcked(p)
	s.penaltyAcked(p)
	s.mpLaneAdvanced(p, ol)
}

// mpLaneAdvanced runs after any lane of the pipe was acknowledged further.
func (s *Session) mpLaneAdvanced(p *Pipe, _ *outLane) {
	if p.mp.out == outDraining {
		s.checkDrain(p)
	}
	s.wakeAllWriters()
}

// joinConfirmed runs when a local join becomes ACTIVE.
func (s *Session) joinConfirmed(p *Pipe) {
	a := p.mp
	a.liveAt = s.now
	s.mp.backoffExp = 0
	s.startProbation(p)
	if len(a.statusQ) > 1 {
		// Only the latest request made while JOINING is sent.
		a.statusQ = append(a.statusQ[:0], a.statusQ[len(a.statusQ)-1])
	}
	s.checkTakeover()
	s.emitPipeState(p)
	s.checkResumed()
}

// onPipeFrame processes PIPE_PING, PIPE_ABANDON and PIPE_STATUS.
func (s *Session) onPipeFrame(p *Pipe, f *wire.Frame) *wire.Error {
	m := s.mp
	switch f.Kind {
	case wire.KindPipePing:
		s.ack.immediate = true
		if p.mp != nil && p.out != nil && p.mp.out != outFailed {
			p.mp.ackPair = true
		}
		s.wakeAllWriters()
	case wire.KindPipeAbandon:
		q := s.pipe(f.PipeID)
		if q == nil {
			if s.ownParity(f.PipeID) {
				if f.PipeID >= m.nextID {
					return wire.Errorf(wire.ProtocolViolation, "PIPE_ABANDON for an untaken pipe ID", "pipe", f.PipeID)
				}
				return nil
			}
			if m.inWindow(f.PipeID) && !m.bits[f.PipeID] {
				m.bits[f.PipeID] = true
				m.pendAbandon[f.PipeID] = true
				m.abandonCodes(f.PipeID, wire.PipeFailed)
				s.wakeAllWriters()
			}
			return nil
		}
		s.peerAbandon(q, f.Code)
	case wire.KindPipeStatus:
		return s.onPipeStatus(f)
	}
	return nil
}
