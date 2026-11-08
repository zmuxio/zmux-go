package core

import "github.com/zmuxio/zmux-go/internal/wire"

// ---- failure and removal ----

// failPipe fails an attachment: reinjection, carrier closed, PIPE_ABANDON
// (unless one was sent), then the last-pipe rule.
func (s *Session) failPipe(p *Pipe, code uint64, cause error) {
	a := p.mp
	if a == nil || a.retired {
		return
	}
	if a.joinerLocal && a.joining() {
		s.mp.joinFailures++ // feeds the join retry backoff
	}
	wasDraining := a.out == OutDraining || a.draining
	if p.out != nil && a.out != OutFailed && a.out != OutClosed {
		a.out = OutFailed
		a.failures++
		var sent uint64
		for _, l := range p.out.lanes {
			sent += l.pos
		}
		a.bytesLost = sent - min64(sent, p.out.snd.cum)
		s.stats.PipeFailures++
		p.out.failed = true
		if s.shp != nil {
			s.shapePipeDown(p)
		}
		s.probationFailed(p)
		s.unsettle(p)
		s.reinjectPipe(p)
	}
	if p.in != nil && a.in != InEnded {
		a.in = InEnded
		p.in.ended = true
	}
	if a.failCause == nil {
		a.failCause = cause
	}
	// A request lost with its pipe stops being outstanding (W4).
	if s.ping.outstanding != 0 && s.ping.pipe == p.id {
		s.pingExpired()
	}
	s.act(Action{Kind: ActAbort, Pipe: p.id, Dir: DirBidi})
	if !a.abandonSent && !a.abandonQueued {
		a.abandonQueued = true
		a.abandonCode = code
	}
	s.emitPipeState(p)
	s.checkTakeover()
	if !wasDraining && code != wire.PipeResourceLimit && a.fate != 0 {
		s.fateGroupHit(p)
	}
	s.maybeRetire(p)
	s.checkLastPipe(cause)
	s.checkLingerDone()
	s.wakeAllWriters()
}

// fateGroupHit marks the other attachments of the same joiner and fate
// group suspect or probes them.
func (s *Session) fateGroupHit(p *Pipe) {
	a := p.mp
	for _, q := range s.pipes {
		qa := q.mp
		if q == p || qa == nil || qa.retired || qa.fate != a.fate || qa.joinerLocal != a.joinerLocal {
			continue
		}
		if qa.draining || qa.abandonRecv && qa.abandonCodeRecv == wire.NoError {
			continue
		}
		if qa.out == OutActive {
			if qa.unacked {
				s.makeSuspect(q)
			} else {
				qa.needPing = true
				qa.pingSince = s.now
			}
		}
	}
}

// makeSuspect moves an outbound pipe to SUSPECT.
func (s *Session) makeSuspect(p *Pipe) {
	a := p.mp
	if a.out != OutActive {
		return
	}
	a.out = OutSuspect
	a.suspects++
	a.suspectAt = s.now
	a.suspHeld = true
	a.suspFloor, a.suspDead = s.deadParams(a)
	a.noSampleAt, a.noSample = s.now, true
	s.probationSuspect(p)
	s.unsettle(p)
	s.checkTakeover()
	s.reinjectPipe(p)
	a.needPing = true
	a.pingSince = s.now
	s.emitPipeState(p)
	if prim, _ := s.primary(); prim != nil && s.mp.ackChanged {
		s.mp.redundantAt = s.now
	}
}

// RemovePipe removes an attachment gracefully (drain) or abruptly.
func (s *Session) RemovePipe(id uint64, graceful bool) error {
	if s.mp == nil {
		return ErrNotNegotiated
	}
	p := s.pipe(id)
	if p == nil || p.mp == nil || p.mp.retired {
		return ErrInvalid
	}
	if !graceful {
		s.failPipe(p, wire.PipeFailed, nil)
		return nil
	}
	s.startDrain(p)
	return nil
}

// startDrain begins a graceful removal.
func (s *Session) startDrain(p *Pipe) {
	a := p.mp
	if a.draining {
		return
	}
	if a.hs {
		// A graceful removal needs the carrier.
		s.failPipe(p, wire.PipeFailed, nil)
		return
	}
	a.draining = true
	a.drainStart = s.now
	if p.out != nil && a.liveOut() {
		a.out = OutDraining
	}
	s.unsettle(p)
	if !a.abandonSent && !a.abandonQueued {
		a.abandonQueued = true
		a.abandonCode = wire.NoError
	}
	s.emitPipeState(p)
	s.checkDrain(p)
	s.checkLastPipe(nil)
	s.wakeAllWriters()
}

// peerAbandon handles a received PIPE_ABANDON.
func (s *Session) peerAbandon(p *Pipe, code uint64) {
	a := p.mp
	if a == nil || a.retired || a.abandonRecv {
		return
	}
	a.abandonRecv = true
	a.abandonRecvAt = s.now
	a.abandonCodeRecv = code
	if a.joinerLocal && code == wire.PipeResourceLimit {
		a.resLimited = true
		s.joinRefused()
	}
	if code == wire.NoError {
		if !a.draining {
			s.startDrain(p)
		}
		s.checkDrain(p)
	} else {
		if code != wire.PipeResourceLimit {
			code = wire.PipeFailed
		}
		s.failPipe(p, code, nil)
	}
	s.maybeRetire(p)
}

// joinRefused counts a refused local join and extends the refusal
// backoff.
func (s *Session) joinRefused() {
	s.stats.JoinRefusals++
	m := s.mp
	m.backoffExp++
	d := Second << min(m.backoffExp-1, 6)
	m.backoffUntil = s.now.Add(minD(d, 60*Second))
}

// unackedStream reports stream data on the pipe not yet acknowledged.
func unackedStream(p *Pipe) bool {
	if p.out == nil {
		return false
	}
	for _, l := range p.out.lanes {
		for _, e := range l.log {
			if e.d.kind == dStream && !e.reinjected {
				return true
			}
		}
	}
	return false
}

// checkDrain completes a drain.
func (s *Session) checkDrain(p *Pipe) {
	a := p.mp
	if a.out != OutDraining || !a.abandonSent || !a.abandonRecv || unackedStream(p) {
		return
	}
	a.out = OutClosed
	s.reinjectPipe(p)
	for _, l := range p.out.lanes {
		if !l.shut {
			l.shut = true
			s.act(Action{Kind: ActShutdown, Pipe: p.id, Lane: l.lane, Dir: DirOut})
		}
	}
	if p.in != nil && a.in == InOpen {
		a.lingerUntil = s.now.Add(s.lingerTimeout())
		a.lingering = true
	}
	s.emitPipeState(p)
	s.maybeRetire(p)
	s.checkLastPipe(nil)
}

// abandonHanded runs when an own PIPE_ABANDON was handed to a carrier.
func (s *Session) abandonHanded(p *Pipe) {
	s.checkDrain(p)
	s.maybeRetire(p)
}

// maybeRetire retires an attachment at the first moment every condition
// holds.
func (s *Session) maybeRetire(p *Pipe) {
	a := p.mp
	if a == nil || a.retired {
		return
	}
	if !a.abandonSent || !a.abandonAcked {
		return
	}
	if !a.abandonRecv && !a.unilateral {
		return
	}
	if p.out != nil && a.out != OutFailed && a.out != OutClosed {
		return
	}
	if p.in != nil && a.in != InEnded {
		return
	}
	s.retire(p)
}

// retire frees the attachment's state; its ID stays used.
func (s *Session) retire(p *Pipe) {
	a := p.mp
	a.retired = true
	// A peer-parity ID stays used, pipe 0 of an initiator peer included:
	// its later PIPE_ACK entries are ignored.
	if !s.ownParity(p.id) {
		s.mp.bits[p.id] = true
	}
	s.act(Action{Kind: ActAbort, Pipe: p.id, Dir: DirBidi})
	for i, q := range s.pipes {
		if q == p {
			copy(s.pipes[i:], s.pipes[i+1:])
			s.pipes[len(s.pipes)-1] = nil
			s.pipes = s.pipes[:len(s.pipes)-1]
			break
		}
	}
	s.retiredPipes = append(s.retiredPipes, p.id)
	if len(s.retiredPipes) > 64 {
		s.retiredPipes = s.retiredPipes[1:]
	}
	s.mp.stateGen++
	s.emit(Event{Kind: EvPipeState, Pipe: p.id, Dir: DirBidi, OutState: OutRetired, InState: InRetired,
		Carrier: p.carrier, Failed: a.failures > 0})
	s.checkLastPipe(nil)
	if a.joinerLocal && !a.resLimited && !a.hs {
		// Another attachment that the acceptor had attached retired: the refusal
		// backoff ends.
		s.mp.backoffUntil = 0
		s.wakeAllWriters()
	}
}

// liveCounts returns the number of live outbound and inbound pipes.
func (s *Session) liveCounts() (out, in int) {
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || a.retired {
			continue
		}
		if p.out != nil && a.liveOut() {
			out++
		}
		if p.in != nil && (a.in == InJoining || a.in == InOpen) {
			in++
		}
	}
	return
}

// checkLastPipe handles the loss of the last live pipe of a direction:
// suspend with resume, else close.
func (s *Session) checkLastPipe(cause error) {
	if s.state >= StateClosing || s.state == StateSuspended {
		return
	}
	out, in := s.liveCounts()
	if out > 0 && in > 0 {
		return
	}
	if s.feat.Resume {
		s.suspend(cause)
		return
	}
	e := &SessionError{Kind: ClosePipesLost, Cause: cause}
	if out > 0 {
		s.closeWith(e, &closeFrame{code: wire.NoViablePath})
		return
	}
	s.closeWith(e, nil)
}

// mpInboundEnded handles the end of an inbound pipe.
func (s *Session) mpInboundEnded(p *Pipe, graceful bool, cause error) {
	a := p.mp
	if a == nil || a.retired {
		return
	}
	expected := graceful && (a.abandonRecv || a.abandonSent && a.abandonCode == wire.NoError || a.draining ||
		s.state >= StateClosing)
	if expected {
		a.in = InEnded
		s.ack.immediate = true
		s.emitPipeState(p)
		s.maybeRetire(p)
		s.checkLastPipe(nil)
		s.wakeAllWriters()
		return
	}
	s.ack.immediate = true
	s.failPipe(p, wire.PipeFailed, cause)
}

// mpOutboundFailed handles a failed write on an outbound pipe.
func (s *Session) mpOutboundFailed(id uint64, err error) {
	if p := s.pipe(id); p != nil && p.mp != nil {
		s.failPipe(p, wire.PipeFailed, err)
	}
}

// RemovesLast reports whether removing pipe id would leave a direction
// without a live pipe, the last pipe of a direction.
func (s *Session) RemovesLast(id uint64) bool {
	p := s.pipe(id)
	if s.mp == nil || p == nil || p.mp == nil || p.mp.retired {
		return false
	}
	out, in := s.liveCounts()
	if p.out != nil && p.mp.liveOut() {
		out--
	}
	if p.in != nil && (p.mp.in == InJoining || p.mp.in == InOpen) {
		in--
	}
	return out <= 0 || in <= 0
}
