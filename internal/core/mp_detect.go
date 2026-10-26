package core

import "github.com/zmuxio/zmux-go/internal/wire"

// Failure detection and the path layer's timers (MULTIPATH.md 7.1, 7.3,
// 8.2, 8.4). Every deadline is computed by mpTimers, which both
// mpNextTimeout and mpHandleTimeout use (IMPLEMENTATION.md 13.4 T2).

// pto is PTO_p (RFC 9002 6.2.1 with the peer's max_ack_delay).
func (s *Session) pto(a *attach) Duration {
	d := a.rtt.smoothed + maxD(4*a.rtt.rttvar, Millisecond)
	if s.ack != nil {
		d += s.ack.peerDelay
	} else {
		d += 25 * Millisecond
	}
	return d
}

func (s *Session) tSuspect(a *attach) Duration {
	f := s.cfg.SuspectFloor
	if a.suspectFloor > 0 {
		f = a.suspectFloor
	}
	return maxD(f, 2*s.pto(a))
}

func (s *Session) tFail(a *attach) Duration { return maxD(2*Second, 8*s.pto(a)) }

func (s *Session) tDead(a *attach) Duration {
	dt := s.cfg.DeadTimeout
	if a.deadTimeout > 0 {
		dt = a.deadTimeout
	}
	if dt == 0 {
		if s.feat.Resume {
			dt = s.tFail(a)
		} else {
			idle := s.idleTimeout()
			if idle == 0 {
				idle = 30 * Second
			}
			dt = maxD(s.tFail(a), idle/2)
		}
	}
	return maxD(s.tSuspect(a), dt)
}

// tref is the start of an ack stall.
func (a *attach) tref() Time {
	t := a.lastAdvance
	if a.liveAt > t {
		t = a.liveAt
	}
	if a.unacked && a.oldestAt > t {
		t = a.oldestAt
	}
	return t
}

// writeStall returns the start of the oldest write stall on p: a lane
// whose carrier takes no bytes while records wait for it.
func (s *Session) writeStall(p *Pipe) (Time, bool) {
	if p.out == nil {
		return 0, false
	}
	var t Time
	ok := false
	for _, ol := range p.out.lanes {
		if !ol.stalled() || ol.shut {
			continue
		}
		if !ol.writing && !s.mpWantsWrite(p, ol) {
			continue
		}
		if !ok || ol.stallAt < t {
			t, ok = ol.stallAt, true
		}
	}
	if ok && p.mp != nil && p.mp.liveAt > t {
		t = p.mp.liveAt
	}
	return t, ok
}

func (s *Session) otherActive(p *Pipe) bool {
	for _, q := range s.pipes {
		if q != p && q.mp != nil && q.out != nil && q.mp.out == outActive {
			return true
		}
	}
	return false
}

func (s *Session) hasLiveInbound() bool {
	_, in := s.liveCounts()
	return in > 0
}

// mpTimers evaluates every path-layer deadline; with apply it acts on the
// due ones. It returns the earliest pending deadline.
func (s *Session) mpTimers(now Time, apply bool) Time {
	m := s.mp
	next := Never
	at := func(t Time) bool {
		if now >= t {
			return true
		}
		next = minT(next, t)
		return false
	}
	liveIn := s.hasLiveInbound()
	for i := 0; i < len(s.pipes); i++ {
		p := s.pipes[i]
		a := p.mp
		if a == nil || a.retired {
			continue
		}
		// Unconfirmed local joins fail at the handshake timeout.
		if a.joinerLocal && (a.out == outJoining || a.out == outNone && a.in == inJoining) && p.id != 0 {
			if at(a.attachedAt.Add(s.cfg.HandshakeTimeout)) && apply {
				s.failPipe(p, 0x296a5bf5, nil)
				continue
			}
		}
		ws, wstall := s.writeStall(p)
		if p.out != nil && a.liveOut() && (a.unacked && liveIn || wstall) {
			stall := a.tref()
			if wstall && (!a.unacked || !liveIn || ws < stall) {
				stall = ws
			}
			switch a.out {
			case outActive, outJoining:
				if at(stall.Add(s.tSuspect(a))) && apply && !(a.out == outJoining && !s.confirmedOnce(p)) {
					s.makeSuspect(p)
				}
			case outSuspect:
				lim := s.tDead(a)
				if s.otherActive(p) {
					lim = s.tFail(a)
				}
				if at(stall.Add(lim)) && apply {
					s.failPipe(p, 0x296a5bf5, nil)
					continue
				}
			case outDraining:
				if !a.drainSuspect && at(stall.Add(s.tSuspect(a))) && apply {
					a.drainSuspect = true
					s.reinjectPipe(p)
				}
			}
		}
		if a.out == outDraining && at(a.drainStart.Add(s.tFail(a))) && apply {
			s.failPipe(p, 0x296a5bf5, nil)
			continue
		}
		// Idle-pipe probe (MULTIPATH.md 7.1).
		if p.out != nil && (a.out == outActive || a.out == outSuspect) && !a.unacked && !a.needPing {
			iv := s.cfg.ProbeInterval
			if p.in == nil {
				iv = 15 * Second
			}
			if sp := s.cfg.StandbyProbe; sp > 0 && now < m.busyUntil && s.backup(p) && (iv == 0 || sp < iv) {
				iv = sp
			}
			if iv > 0 {
				ref := a.lastElicit
				if a.liveAt > ref {
					ref = a.liveAt
				}
				if at(ref.Add(iv)) && apply {
					a.needPing = true
					a.pingSince = now
					s.wakeAllWriters()
				}
			}
		}
		// Unilateral retirement (MULTIPATH.md 8.4).
		if a.abandonAcked && !a.abandonRecv && !a.unilateral {
			if at(a.ackedAbandonAt.Add(maxD(2*Second, 3*s.maxPTO()))) && apply {
				a.unilateral = true
				s.act(Action{Kind: ActAbort, Pipe: p.id, Dir: DirBidi})
				if p.out != nil && a.out != outFailed {
					a.out = outClosed
				}
				if p.in != nil {
					a.in = inEnded
				}
				s.maybeRetire(p)
				continue
			}
		}
		// Linger after a completed PIPE_ABANDON exchange.
		if a.lingering && a.in == inOpen && at(a.lingerUntil) && apply {
			a.in = inEnded
			s.act(Action{Kind: ActClose, Pipe: p.id, Dir: DirIn})
			s.maybeRetire(p)
			continue
		}
		// Inbound silence of an attachment without a local outbound
		// direction (MULTIPATH.md 7.3).
		if p.out == nil && a.in == inOpen {
			_, nIn := s.liveCounts()
			lim := 15*Second + Duration(nIn)*Second/8 + maxD(2*Second, maxD(2*s.srttMaxOut(), a.maxGap-15*Second))
			if at(a.lastRecord.Add(lim)) && apply {
				s.failPipe(p, 0x296a5bf5, nil)
				continue
			}
		}
	}
	if m.backoffUntil > now {
		next = minT(next, m.backoffUntil)
	} else if apply && m.backoffUntil != 0 {
		m.backoffUntil = 0
		s.wakeAllWriters()
	}
	// Redundant snapshot copy (MULTIPATH.md 6.5).
	if m.ackChanged && at(m.redundantAt) && apply {
		s.wakeAllWriters()
	}
	// PIPE_PING rate limit.
	for _, p := range s.pipes {
		if p.mp != nil && p.mp.needPing {
			if t := m.pingNext(); t > now {
				next = minT(next, t)
			} else if apply {
				s.wakeAllWriters()
			}
			break
		}
	}
	if m.pickTimer {
		if at(m.pickDeadline) && apply {
			m.pickTimer = false
			s.wakeAllWriters()
		}
	}
	next = minT(next, s.probationTimers(now, apply))
	if apply {
		s.refreshTiers()
		if len(m.tierSince) > 1 {
			s.wakeAllWriters()
		}
	}
	next = minT(next, s.tierDeadline())
	return next
}

// confirmedOnce reports whether a pipe was ever confirmed.
func (s *Session) confirmedOnce(p *Pipe) bool {
	return p.out != nil && p.out.lanes[0].acked > 0 || !p.mp.joinerLocal || p.id == 0
}

func (s *Session) maxPTO() Duration {
	var m Duration
	for _, p := range s.pipes {
		if p.mp != nil && p.out != nil && p.mp.liveOut() {
			m = maxD(m, s.pto(p.mp))
		}
	}
	if m == 0 {
		m = 333*Millisecond + 4*166*Millisecond + 25*Millisecond
	}
	return m
}

func (s *Session) srttMaxOut() Duration {
	var m Duration
	for _, p := range s.pipes {
		if p.mp != nil && p.out != nil && p.mp.liveOut() {
			m = maxD(m, p.mp.rtt.smoothed)
		}
	}
	if m == 0 {
		m = 333 * Millisecond
	}
	return m
}

func (s *Session) mpNextTimeout() Time { return s.mpTimers(s.now, false) }

func (s *Session) mpHandleTimeout(now Time) { s.mpTimers(now, true) }

// ---- suspension (RESUME.md 4) ----

func (s *Session) suspend(cause error) {
	if s.state == StateSuspended {
		return
	}
	s.mp.suspendCause = cause
	s.preSuspend = s.state
	s.state = StateSuspended
	s.timers.grace = s.now.Add(s.graceDur())
	s.emit(Event{Kind: EvSuspended})
}

// lastPipeErr is the error of the pipe loss that suspended the session.
func (s *Session) lastPipeErr() error {
	if s.mp == nil {
		return nil
	}
	return s.mp.suspendCause
}

func (s *Session) graceDur() Duration {
	g := s.cfg.Resume
	if s.peer.ResumeGrace > 0 {
		if pg := MulD(int64(s.peer.ResumeGrace), Millisecond); pg < g || g == 0 {
			g = pg
		}
	}
	return g
}

// checkResumed leaves SUSPENDED once an ACTIVE outbound and an OPEN
// inbound pipe exist (RESUME.md 4.4).
func (s *Session) checkResumed() {
	if s.state != StateSuspended {
		return
	}
	act, open := false, false
	for _, p := range s.pipes {
		if p.mp == nil || p.mp.retired {
			continue
		}
		if p.out != nil && p.mp.out == outActive {
			act = true
		}
		if p.in != nil && p.mp.in == inOpen {
			open = true
		}
	}
	if !act || !open {
		return
	}
	s.state = s.preSuspend
	if s.ga.sent || s.ga.received {
		s.state = StateDraining
	}
	s.timers.grace = Never
	s.timers.lastActivity = s.now
	s.emit(Event{Kind: EvResumed})
	s.wakeAllWriters()
}

// CloseResumeTimeout closes a suspended session with ZMUX_RESUME_TIMEOUT
// (a listener's limit on suspended sessions, RESUME.md 9).
func (s *Session) CloseResumeTimeout() {
	if s.state != StateSuspended {
		return
	}
	s.closeWith(&SessionError{Kind: CloseResumeTimeout, Cause: s.lastPipeErr()},
		&closeFrame{code: wire.ResumeTimeout})
}

// LiveDirections reports whether the session has a live outbound and a
// live inbound pipe.
func (s *Session) LiveDirections() (out, in bool) {
	if s.mp == nil {
		return s.state < StateClosing, s.state < StateClosing
	}
	o, i := s.liveCounts()
	return o > 0, i > 0
}
