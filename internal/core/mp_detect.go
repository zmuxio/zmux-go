package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// Failure detection and the path layer's timers: suspicion, failure, the
// drain deadline and unilateral retirement. Every deadline is computed by mpTimers, which both
// mpNextTimeout and mpHandleTimeout use.

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

// queueDelay is queue_p = min(1 s, unsent_ahead_p / R_p),
// taken with the snapshot (setAhead).
func (s *Session) queueDelay(a *attach) Duration { return a.queueD }

// aheadSnap is the failure detector's queue snapshot, taken when bytes are
// handed to a carrier: unsent_ahead (Q_p then) and queue_p at R_p then.
type aheadSnap struct {
	bytes uint64
	delay Duration
}

// snapAhead takes the snapshot for a batch handed to p now. queue_p is
// taken only for multipath sessions, which alone use it.
func (s *Session) snapAhead(p *Pipe) aheadSnap {
	n := s.queuedOn(p)
	if n == 0 || p.mp == nil || p.out == nil {
		return aheadSnap{bytes: n}
	}
	return aheadSnap{bytes: n, delay: minD(Second, timeAt(n, s.rateOf(p)))}
}

// setAhead makes snap that of p's oldest unacknowledged ack-eliciting
// byte.
func (s *Session) setAhead(p *Pipe, snap aheadSnap) {
	p.mp.unsentAhead, p.mp.queueD = snap.bytes, snap.delay
}

func (s *Session) tSuspect(a *attach) Duration {
	f, _ := s.deadParams(a)
	return maxD(f, 2*(s.pto(a)+s.queueDelay(a)))
}

func (s *Session) tFail(a *attach) Duration { return maxD(2*Second, 8*(s.pto(a)+s.queueDelay(a))) }

// deadParams are the SuspectFloor and DeadTimeout (0: derived) of a: the
// pipe's own, else the session's.
func (s *Session) deadParams(a *attach) (f, dt Duration) {
	f, dt = s.cfg.SuspectFloor, s.cfg.DeadTimeout
	if a.live.SuspectFloor > 0 {
		f = a.live.SuspectFloor
	}
	if a.live.DeadTimeout > 0 {
		dt = a.live.DeadTimeout
	}
	return f, dt
}

// tDead is T_dead of a suspect pipe, under the SuspectFloor and
// DeadTimeout in force when it turned suspect: a later change of the
// settings applies from its next suspicion.
func (s *Session) tDead(a *attach) Duration {
	f, dt := a.suspFloor, a.suspDead
	if !a.suspHeld {
		f, dt = s.deadParams(a)
	}
	return s.deadLimit(s.pto(a)+s.queueDelay(a), f, dt)
}

// deadLimit is T_dead for PTO + queue d, suspect floor
// f and dead_timeout dt (0: not set).
func (s *Session) deadLimit(d, f, dt Duration) Duration {
	if dt == 0 {
		dt = maxD(2*Second, 8*d) // T_fail
		if !s.feat.Resume {
			idle := s.idleTimeout()
			if idle == 0 {
				idle = DefaultIdleTimeout
			}
			dt = maxD(dt, idle/2)
		}
	}
	return maxD(maxD(f, 2*d), dt)
}

// lastInDead is the T_dead that bounds the silence of the last live
// inbound pipe: that of the last pipe, with the
// largest PTO of the live outbound pipes, or the initial values when there
// are none.
func (s *Session) lastInDead() Duration {
	var d Duration
	for _, p := range s.pipes {
		if p.mp != nil && !p.mp.retired && p.out != nil && p.mp.liveOut() {
			d = maxD(d, s.pto(p.mp))
		}
	}
	if d == 0 {
		var a attach
		a.rtt.smoothed, a.rtt.rttvar = 333*Millisecond, 166*Millisecond
		d = s.pto(&a)
	}
	return s.deadLimit(d, s.cfg.SuspectFloor, s.cfg.DeadTimeout)
}

// probeInterval is the probe interval k of p's outbound direction: the
// pipe's ProbeInterval, else the session's; on an attachment without an
// outbound direction at the peer at most 15 s, and never off.
// 0: no probes.
func (s *Session) probeInterval(p *Pipe) Duration {
	k := s.cfg.ProbeInterval
	switch {
	case p.mp.live.ProbeInterval > 0:
		k = p.mp.live.ProbeInterval
	case p.mp.live.ProbeInterval < 0:
		k = 0 // off for this pipe
	}
	if p.in == nil && (k == 0 || k > 15*Second) {
		k = 15 * Second
	}
	return k
}

// probeHold is H_p, the longest time one record of p that carries no
// urgent frame may wait between being planned and being handed to the
// carrier, as the limits bound it now. 0 when p (nil: not
// yet attached) is not shaped with plans, or nothing bounds one wait.
func (s *Session) probeHold(p *Pipe) Duration {
	if p == nil {
		return 0
	}
	ps := p.sh
	if s.shp == nil || ps == nil || ps.sh == nil || ps.caps&shaping.CapPlan == 0 || s.shp.eff.NoPause {
		return 0
	}
	return max(Duration(s.shp.eff.MaxDelay), 0)
}

// probeWait is the current wait of p's idle-pipe probe: k_eff = max(1 s,
// k - H_p), never above k, with H_p and the draw both taken when the wait
// started (drawProbe), so that the probe reaches the carrier at most k
// after the previous ack-eliciting byte did. 0: no probes.
func (s *Session) probeWait(p *Pipe) Duration {
	k := s.probeInterval(p)
	if k == 0 {
		return 0
	}
	if h := p.mp.probeH; h > 0 {
		k = min(k, max(Second, k-h))
	}
	if c := p.mp.probeCut; c > 0 {
		k = Duration(float64(k) * (1 - c))
	}
	return k
}

// probeJitter is the probe jitter j of a.
func (s *Session) probeJitter(a *attach) float64 {
	switch {
	case a.live.ProbeJitter > 0:
		return a.live.ProbeJitter
	case a.live.ProbeJitter < 0:
		return 0
	}
	return s.cfg.ProbeJitter
}

// drawProbe starts a probe wait of a: w = k_eff x (1 - j x u), u uniform
// in [0, 1), so w lies in (k_eff x (1 - j), k_eff]. The first wait of a
// bootstrap pipe (boot: no PIPE_PING was sent on it by either end) is
// drawn from the whole interval instead when j > 0, so that pipes and
// sessions set up together do not probe in step; a joined pipe carried
// the joiner's PIPE_PING, so both of its ends draw as usual.
// H_p is fixed for the wait here as well: p is a's pipe, nil while
// it is being made (H_p 0).
func (s *Session) drawProbe(a *attach, p *Pipe, boot bool) {
	a.probeH = s.probeHold(p)
	a.probeCut = 0
	if j := s.probeJitter(a); j > 0 {
		u := s.rand().Float64()
		if !boot {
			u *= j
		}
		a.probeCut = u
	}
}

// tref is the start of an ack stall.
func (a *attach) tref() Time {
	t := max(a.liveAt, a.lastAdvance)
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
		if q != p && q.mp != nil && q.out != nil && q.mp.out == OutActive {
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
	// due is at for a deadline whose processing changes a pipe's state:
	// one already passed but not yet processed stays pending, so that a
	// driver that asks for the next deadline between the moment it passed
	// and the timer pass still arms a timer for it.
	due := func(t Time) bool {
		if !at(t) {
			return false
		}
		if !apply {
			next = minT(next, t)
		}
		return true
	}
	s.epochTimers(apply, due)
	liveIn := s.hasLiveInbound()
	for i := 0; i < len(s.pipes); i++ {
		p := s.pipes[i]
		a := p.mp
		if a == nil || a.retired {
			continue
		}
		// Unconfirmed local joins fail at the join timeout.
		if a.joinerLocal && (a.out == OutJoining || a.out == OutNone && a.in == InJoining) && p.id != 0 {
			if due(a.attachedAt.Add(s.cfg.JoinTimeout)) && apply {
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
			case OutActive:
				if due(stall.Add(s.tSuspect(a))) && apply {
					s.makeSuspect(p)
				}
			case OutSuspect:
				lim := s.tDead(a)
				if s.otherActive(p) {
					lim = s.tFail(a)
				}
				if due(stall.Add(lim)) && apply {
					s.failPipe(p, 0x296a5bf5, nil)
					continue
				}
			case OutDraining:
				if !a.drainSuspect && due(stall.Add(s.tSuspect(a))) && apply {
					a.drainSuspect = true
					s.reinjectPipe(p)
				}
			case OutJoining:
				// An unconfirmed join fails at the join timeout above, and
				// the peer's join is not supervised before it is confirmed.
			case OutNone, OutFailed, OutClosed, OutRetired:
				// liveOut excludes these.
			}
		}
		if a.out == OutDraining && due(a.drainStart.Add(s.tFail(a))) && apply {
			s.failPipe(p, 0x296a5bf5, nil)
			continue
		}
		// Idle-pipe probe.
		if p.out != nil && (a.out == OutActive || a.out == OutSuspect) && !a.unacked && !a.needPing {
			iv := s.probeWait(p)
			if sp := s.cfg.StandbyProbe; sp > 0 && now < m.busyUntil && s.backup(p) && (iv == 0 || sp < iv) {
				iv = sp
			}
			if iv > 0 {
				ref := max(a.liveAt, a.lastElicit)
				if due(ref.Add(iv)) && apply {
					a.needPing = true
					a.pingSince = now
					s.wakeAllWriters()
				}
			}
		}
		// Unilateral retirement.
		if a.abandonAcked && !a.abandonRecv && !a.unilateral {
			if due(a.ackedAbandonAt.Add(maxD(2*Second, 3*s.maxPTO()))) && apply {
				a.unilateral = true
				s.act(Action{Kind: ActAbort, Pipe: p.id, Dir: DirBidi})
				if p.out != nil && a.out != OutFailed {
					a.out = OutClosed
				}
				if p.in != nil {
					a.in = InEnded
				}
				s.maybeRetire(p)
				continue
			}
		}
		// Linger after a completed PIPE_ABANDON exchange.
		if a.lingering && a.in == InOpen && due(a.lingerUntil) && apply {
			a.in = InEnded
			s.act(Action{Kind: ActClose, Pipe: p.id, Dir: DirIn})
			s.maybeRetire(p)
			continue
		}
		// Inbound silence of an attachment without a local outbound
		// direction.
		if p.out == nil && a.in == InOpen {
			_, nIn := s.liveCounts()
			lim := 15*Second + Duration(nIn)*Second/8 + maxD(2*Second, maxD(2*s.srttMaxOut(), a.maxGap-15*Second))
			if nIn <= 1 {
				// Losing it leaves no live inbound pipe: the patience of
				// a stalled last outbound pipe.
				lim = maxD(lim, s.lastInDead())
			}
			if due(a.lastRecord.Add(lim)) && apply {
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
	// Redundant snapshot copy.
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

// mpHandleTimeout acts on the due deadlines. A pipe state change in a pass
// can make other deadlines due at once (a failure turns another pipe's
// limit into T_dead, a suspicion whose T_dead equals T_suspect); those are
// no longer pending for mpNextTimeout, so passes repeat until one changes
// no pipe state.
func (s *Session) mpHandleTimeout(now Time) {
	for range 16 {
		g := s.mp.stateGen
		s.mpTimers(now, true)
		if s.mp == nil || s.mp.stateGen == g {
			return
		}
	}
}

// ---- suspension ----

func (s *Session) suspend(cause error) {
	if s.state == StateSuspended {
		return
	}
	s.mp.suspendCause = cause
	s.preSuspend = s.state
	s.state = StateSuspended
	s.suspendedAt = s.now
	s.stats.Suspensions++
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
// inbound pipe exist.
func (s *Session) checkResumed() {
	if s.state != StateSuspended {
		return
	}
	act, open := false, false
	for _, p := range s.pipes {
		if p.mp == nil || p.mp.retired {
			continue
		}
		if p.out != nil && p.mp.out == OutActive {
			act = true
		}
		if p.in != nil && p.mp.in == InOpen {
			open = true
		}
	}
	if !act || !open {
		return
	}
	s.endSuspension()
	s.state = s.preSuspend
	if s.ga.sent || s.ga.received {
		s.state = StateDraining
	}
	s.timers.grace = Never
	s.timers.lastActivity = s.now
	s.emit(Event{Kind: EvResumed})
	s.epochResumed()
	s.wakeAllWriters()
}

// CloseResumeTimeout closes a suspended session with ZMUX_RESUME_TIMEOUT
// (a listener's limit on suspended sessions).
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

// HasStreams reports whether the session has streams not yet released,
// those opened locally whose first frame is not sent yet included.
func (s *Session) HasStreams() bool {
	return len(s.streams) > 0 || s.loc[0].reserved > 0 || s.loc[1].reserved > 0
}
