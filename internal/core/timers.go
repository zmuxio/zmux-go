package core

import "github.com/zmuxio/zmux-go/internal/wire"

// timers are the session's deadlines; every one is computed by nextTimeout.
type timers struct {
	handshake    Time
	linger       Time
	lastActivity Time // last event that restarted the idle timer
	lastKeepRef  Time
	keepCut      float64 // that wait is k x (1 - keepCut), keepCut in [0, j]
	keepHas      bool
	laneWait     Time // CONNECTION_CLOSE waits for lanes until then
	grace        Time // resume grace
	// writeLost bounds the wait for the inbound side after the only
	// pipe's write side failed: records already read (a CONNECTION_CLOSE)
	// still decide the end first.
	writeLost Time
}

// deadlineSlots gathers direct owner deadlines and indexed queue minima.
// Keeping their individual values lets a same-instant dispatch recognize a
// newly due cause even when the minimum stays the same.
type deadlineSlots struct {
	pathKey                                         *Deadline[pathDeadline]
	shapeKey                                        *Deadline[*pipeShape]
	handshake, linger, writeLost, laneWait, session Time
	ack, epoch, advice, prefix, backoff, redundant  Time
	ping, pick, tier, pace, quality, shape          Time
	path, lane, held, datagram, report              Time
}

func (d *deadlineSlots) next() Time {
	return min(d.handshake, d.linger, d.writeLost, d.laneWait, d.session,
		d.ack, d.epoch, d.advice, d.prefix, d.backoff, d.redundant, d.ping,
		d.pick, d.tier, d.pace, d.quality, d.shape, d.path, d.lane, d.held, d.datagram, d.report)
}

func (s *Session) deadlines(withKeys bool) deadlineSlots {
	d := deadlineSlots{
		handshake: Never, linger: Never, writeLost: Never, laneWait: Never, session: Never,
		ack: Never, epoch: Never, advice: Never, prefix: Never, backoff: Never, redundant: Never,
		ping: Never, pick: Never, tier: Never, pace: Never, quality: Never, shape: Never,
		path: Never, lane: Never, held: Never, datagram: Never, report: Never,
	}
	if s.state == StateClosed {
		return d
	}
	d.handshake, d.linger, d.writeLost = s.timers.handshake, s.timers.linger, s.timers.writeLost
	if s.closeFr.waitLanes {
		d.laneWait = s.timers.laneWait
	}
	if !s.transportLive() {
		return d
	}
	if s.state < StateClosing {
		d.session = s.sessionTimeout()
	}
	d.lane, d.held = s.laneTimers.Next(), s.heldDeadline()
	if s.ack != nil {
		d.ack = s.ack.nextTimeout()
		d.pace, d.quality = s.paceDeadline(), s.qualityTimers.Next()
	}
	if s.mp != nil {
		s.setMultipathDeadlines(&d, withKeys)
	}
	if sh := s.shp; sh != nil {
		d.shape = sh.timers.Next()
		if withKeys {
			d.shapeKey = sh.timers.Peek()
		}
	}
	if s.dg != nil {
		d.datagram, d.report = s.dg.expiry.Next(), s.datagramReportWake()
	}
	return d
}

// NextTimeout returns the earliest owner deadline, Never when none.
func (s *Session) NextTimeout() Time {
	d := s.deadlines(false)
	return d.next()
}

// HandleTimeout runs every timer that is due at now.
func (s *Session) HandleTimeout(now Time) {
	s.SetNow(now)
	now = s.now
	s.pickInput()
	s.handleDue(now)
	for pending := s.deadlines(true); pending.next() <= now; {
		s.handleDue(now)
		after := s.deadlines(true)
		if after == pending {
			panic("zmux: deadline did not advance")
		}
		pending = after
	}
}

func (s *Session) handleDue(now Time) {
	if s.state == StateClosed {
		return
	}
	if now >= s.timers.handshake {
		s.timers.handshake = Never
		if !s.peerSeen {
			s.closeWith(&SessionError{Kind: CloseHandshakeTimeout}, nil)
			return
		}
	}
	if now >= s.timers.linger {
		if s.state != StateClosing || !s.closeFr.sentAll || s.closeFr.lingering {
			s.timers.linger = Never
			s.finishClose()
			return
		}
		// Step 1's wait ended without every carrier taking
		// CONNECTION_CLOSE: linger from now, once.
		s.closeFr.lingering = true
		s.timers.linger = now.Add(s.lingerTimeout())
	}
	if now >= s.timers.writeLost {
		s.timers.writeLost = Never
		if s.state < StateClosing {
			s.closeWith(&SessionError{Kind: ClosePipesLost, Cause: s.writeLostErr}, nil)
			return
		}
	}
	if s.closeFr.waitLanes && now >= s.timers.laneWait {
		s.endLaneWait()
	}
	if !s.transportLive() {
		return
	}
	// The session's own deadlines end with CLOSING; the transport's run
	// on while the close waits for PIPE_ACK coverage.
	s.expireLanes(now)
	if s.state < StateClosing && !s.sessionTimers(now) {
		return
	}
	if s.ack != nil {
		s.ack.handleTimeout(now)
	}
	if s.mp != nil {
		s.mpHandleTimeout(now)
	}
	s.expireDatagramSends(now)
	if s.shp != nil && s.shapeTimeout(now) {
		s.wakePrefix()
	}
	s.expireHeld(now)
	s.datagramReportTimeout(now)
	if s.ack != nil {
		s.paceTimeout(now)
		s.expireQuality(now)
	}
}

// sessionTimeout is the earliest deadline of the session itself (resume
// grace, idle timeout, keep-alive, the outstanding PING) before CLOSING.
func (s *Session) sessionTimeout() Time {
	t := Never
	if s.state == StateSuspended {
		t = minT(t, s.timers.grace)
	}
	if s.peerSeen && s.state != StateSuspended {
		if d := s.idleTimeout(); d > 0 {
			t = minT(t, s.timers.lastActivity.Add(d))
		}
		if at, ok := s.keepAliveAt(); ok && s.ping.outstanding == 0 && s.timers.lastKeepRef != s.timers.lastActivity {
			t = minT(t, at)
		}
	}
	if s.ping.outstanding != 0 {
		t = minT(t, s.ping.expires)
	}
	if s.ping.nextTry != 0 {
		t = minT(t, s.ping.nextTry)
	}
	return t
}

// sessionTimers runs the due deadlines of sessionTimeout; it reports false
// when one closed the session.
func (s *Session) sessionTimers(now Time) bool {
	if s.state == StateSuspended && s.timers.grace != Never && now >= s.timers.grace {
		// Grace expiry: CONNECTION_CLOSE on any live
		// outbound pipe, otherwise a local close.
		s.timers.grace = Never
		s.closeWith(&SessionError{Kind: CloseResumeTimeout, Cause: s.lastPipeErr()},
			&closeFrame{code: wire.ResumeTimeout})
		return false
	}
	if s.ping.nextTry != 0 && s.ping.nextTry <= now {
		s.ping.nextTry = 0
		s.wakeAllWriters()
	}
	if s.ping.outstanding != 0 && now >= s.ping.expires {
		s.pingExpired()
	}
	if s.peerSeen && s.state != StateSuspended {
		if d := s.idleTimeout(); d > 0 {
			if at := s.timers.lastActivity.Add(d); at != Never && now >= at {
				s.idleExpired()
				return false
			}
		}
		if at, ok := s.keepAliveAt(); ok && s.ping.outstanding == 0 && now >= at &&
			s.timers.lastKeepRef != s.timers.lastActivity {
			s.timers.lastKeepRef = s.timers.lastActivity
			s.ping.want = true
			s.wakeAllWriters()
		}
	}
	return true
}

// noteActivity owns the keep-alive jitter draw. Reading a deadline never
// mutates the clock or consumes the session's random sequence.
func (s *Session) noteActivity(at Time) {
	tm := &s.timers
	if tm.lastActivity == at && tm.keepHas {
		return
	}
	tm.lastActivity, tm.keepCut = at, 0
	tm.keepHas = s.peerSeen && s.cfg.KeepAlive > 0
	if tm.keepHas && s.cfg.ProbeJitter > 0 {
		tm.keepCut = s.cfg.ProbeJitter * s.rand().Float64()
	}
}

// keepAliveAt returns when the next keep-alive falls due:
// the period k is KeepAlive capped at half the effective idle timeout, and
// each restart of the timer draws its wait from [k x (1 - j), k] with the
// probe jitter j.
func (s *Session) keepAliveAt() (Time, bool) {
	k := s.cfg.KeepAlive
	if k <= 0 {
		return 0, false
	}
	if d := s.idleTimeout(); d > 0 {
		k = min(k, d/2)
	}
	tm := &s.timers
	if c := tm.keepCut; c > 0 {
		k = Duration(float64(k) * (1 - c))
	}
	return tm.lastActivity.Add(k), true
}

// idleExpired closes on the idle timeout.
func (s *Session) idleExpired() {
	e := &SessionError{Kind: CloseIdle}
	if s.feat.Acked() {
		s.closeWith(e, &closeFrame{code: wire.IdleTimeout})
		return
	}
	s.closeSilently(e, true)
}
