package core

import (
	"math"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// Failure detection and the path layer's timers: suspicion, failure, the
// drain deadline and unilateral retirement. Each attachment registers its
// own absolute checks in the path clock.

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

func (s *Session) stallCost(a *attach) Duration { return s.pto(a) + s.queueDelay(a) }

func (s *Session) tSuspect(a *attach) Duration {
	f, _ := s.deadParams(a)
	return maxD(f, MulD(s.stallCost(a), 2))
}

func (s *Session) tFail(a *attach) Duration {
	f, _ := s.deadParams(a)
	return failLimit(s.stallCost(a), f)
}

func (s *Session) suspectFail(a *attach) Duration {
	f := a.suspFloor
	if !a.suspHeld {
		f, _ = s.deadParams(a)
	}
	return failLimit(s.stallCost(a), f)
}

func failLimit(d, floor Duration) Duration {
	return max(floor, 2*Second, MulD(d, 8))
}

// deadParams are the SuspectFloor and DeadTimeout (0: derived, negative:
// off) of a: the pipe's own; else off on a carrier that reports its own
// failure (CarrierInfo.OwnFailure); else the session's.
func (s *Session) deadParams(a *attach) (f, dt Duration) {
	f, dt = s.cfg.SuspectFloor, s.cfg.DeadTimeout
	if a.live.SuspectFloor > 0 {
		f = a.live.SuspectFloor
	}
	switch {
	case a.live.DeadTimeout != 0:
		dt = a.live.DeadTimeout
	case a.ownFail:
		dt = -1
	}
	return f, dt
}

// stallDeathOff reports whether a stall never fails a: its DeadTimeout in
// force (the one it turned suspect under, while suspect) is off. The pipe
// still turns suspect, is avoided and probed (Section 7.2), and fails on
// its carrier's error or end, at the drain deadline and on removal.
func (s *Session) stallDeathOff(a *attach) bool {
	dt := a.suspDead
	if !a.suspHeld {
		_, dt = s.deadParams(a)
	}
	return dt < 0
}

// tDead is T_dead of a suspect pipe, under the SuspectFloor and
// DeadTimeout in force when it turned suspect: a later change of the
// settings applies from its next suspicion.
func (s *Session) tDead(a *attach) Duration {
	f, dt := a.suspFloor, a.suspDead
	if !a.suspHeld {
		f, dt = s.deadParams(a)
	}
	return s.deadLimit(s.stallCost(a), f, dt)
}

// deadLimit is T_dead for PTO + queue d, suspect floor
// f and dead_timeout dt (0: not set; negative: off, no limit).
func (s *Session) deadLimit(d, f, dt Duration) Duration {
	if dt < 0 {
		return math.MaxInt64
	}
	if dt == 0 {
		dt = failLimit(d, f)
		if !s.feat.Resume {
			idle := s.idleTimeout()
			if idle == 0 {
				idle = DefaultIdleTimeout
			}
			dt = maxD(dt, idle/2)
		}
	}
	return maxD(maxD(f, MulD(d, 2)), dt)
}

// lastInDead is the T_dead that bounds the silence of the last live
// inbound pipe: that of the last pipe, with the
// largest PTO of the live outbound pipes, or the initial values when there
// are none, under a's SuspectFloor and DeadTimeout (off: no limit).
func (s *Session) lastInDead(a *attach) Duration {
	d := s.maxPTO()
	if s.mp.clock.ptos.Next() == Never {
		initial := attach{rtt: rttEst{smoothed: 333 * Millisecond, rttvar: 166 * Millisecond}}
		d = s.pto(&initial)
	}
	f, dt := s.deadParams(a)
	return s.deadLimit(d, f, dt)
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
	if p := s.shp.eff.Profile; p != nil {
		return Duration(p.MaxAddedDelay)
	}
	return 0
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
// whose carrier takes no bytes while records or stream data wait for it
// (waitsFor). A lane refused only because nothing could be written,
// whether for lack of data or of the peer's credit or under the limiter,
// has no stall.
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
		if !ol.writing && !s.waitsFor(p, ol) {
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

func (s *Session) hasLiveInbound() bool {
	_, in := s.liveCounts()
	return in > 0
}

func (s *Session) setMultipathDeadlines(d *deadlineSlots, withKeys bool) {
	m := s.mp
	s.refreshRelativeStalls(s.now)
	d.path, d.epoch = minT(m.clock.due.Next(), m.clock.relativeCheck()), s.epochDeadline()
	if withKeys {
		d.pathKey = m.clock.due.Peek()
	}
	d.advice, d.prefix = s.pathQualityTimers(s.now, false), s.prefixTimers(s.now, false)
	if m.backoffUntil != 0 {
		d.backoff = m.backoffUntil
	}
	if m.redundantPending {
		d.redundant = m.redundantAt
	}
	if len(m.clock.ping) != 0 {
		if next := m.pingNext(); next > s.now {
			d.ping = next
		}
	}
	if m.pickTimer {
		d.pick = m.pickDeadline
	}
	if m.tierAt != 0 {
		d.tier = m.tierAt
	}
}

// mpHandleTimeout runs the session's due groups and consumes the indexed
// attachment reasons, including deadlines advanced by a path transition.
func (s *Session) mpHandleTimeout(now Time) {
	s.SetNow(now)
	now = s.now
	m := s.mp
	if s.epochDeadline() <= now {
		s.epochProgress()
	}
	s.pathQualityTimers(now, true)
	s.expirePathDeadlines(now)
	s.prefixTimers(now, true)
	if m.backoffUntil != 0 && m.backoffUntil <= now {
		m.backoffUntil = 0
		s.wakeAllWriters()
		s.joinHoldChanged()
	}
	if m.redundantPending && now >= m.redundantAt {
		m.redundantPending = false
		s.wakeAllWriters()
	}
	if len(m.clock.ping) != 0 && m.pingAllowed(now) {
		for _, p := range m.clock.ping {
			s.wakePipe(p)
		}
	}
	if m.pickTimer && m.pickDeadline <= now {
		m.pickTimer = false
		s.wakeAllWriters()
	}
	if m.tierAt != 0 && m.tierAt <= now {
		m.tierAt = 0
		s.refreshTierPlacement()
		if len(m.tierSince) > 1 {
			s.wakeAllWriters()
		}
	}
}

func (s *Session) maxPTO() Duration {
	if at := s.mp.clock.ptos.Next(); at != Never {
		return -Duration(at)
	}
	return 333*Millisecond + 4*166*Millisecond + 25*Millisecond
}

func (s *Session) srttMaxOut() Duration {
	if at := s.mp.clock.rtts.Next(); at != Never && at < 0 {
		return -Duration(at)
	}
	return 333 * Millisecond
}

// ---- suspension ----

func (s *Session) suspend(cause error) {
	if s.state == StateSuspended {
		return
	}
	s.mp.suspendCause = cause
	s.preSuspend = s.state
	s.state = StateSuspended
	s.joinHoldChanged()
	s.suspendedAt = s.now
	s.stats.Suspensions++
	grace := s.graceDur()
	s.timers.grace = s.now.Add(grace)
	s.emit(Event{Kind: EvSuspended, At: s.now, Wait: grace})
}

// RecoveryGraceDeadline returns the active suspension cutoff, or Never.
func (s *Session) RecoveryGraceDeadline() Time {
	if s.state == StateSuspended {
		return s.timers.grace
	}
	return Never
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
	s.noteActivity(s.now)
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
