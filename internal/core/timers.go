package core

import "github.com/zmuxio/zmux-go/internal/wire"

// timers are the session's deadlines; every one is computed by nextTimeout
// (IMPLEMENTATION.md 13.4 T2).
type timers struct {
	handshake    Time
	linger       Time
	lastActivity Time // last event that restarted the idle timer
	lastKeepRef  Time
	laneWait     Time // CONNECTION_CLOSE waits for lanes until then
	grace        Time // resume grace
}

// NextTimeout returns the earliest deadline, Never when none.
func (s *Session) NextTimeout() Time {
	if s.state == StateClosed {
		return Never
	}
	t := Never
	t = minT(t, s.timers.handshake)
	t = minT(t, s.timers.linger)
	if s.closeFr.waitLanes {
		t = minT(t, s.timers.laneWait)
	}
	if s.state >= StateClosing {
		return t
	}
	t = minT(t, s.laneTimeouts(0, false))
	if s.state == StateSuspended {
		t = minT(t, s.timers.grace)
	}
	if s.peerSeen && s.state != StateSuspended {
		if d := s.idleTimeout(); d > 0 {
			t = minT(t, s.timers.lastActivity.Add(d))
		}
		if k := s.cfg.KeepAlive; k > 0 && s.ping.outstanding == 0 && s.timers.lastKeepRef != s.timers.lastActivity {
			t = minT(t, s.timers.lastActivity.Add(k))
		}
	}
	if s.ping.outstanding != 0 {
		t = minT(t, s.ping.expires)
	}
	if s.ack != nil {
		t = minT(t, s.ack.nextTimeout())
	}
	if s.mp != nil {
		t = minT(t, s.mpNextTimeout())
	}
	if s.ack != nil {
		t = minT(t, s.paceDeadline())
		t = minT(t, s.qualityTimers(s.now, false))
	}
	return t
}

// HandleTimeout runs every timer that is due at now.
func (s *Session) HandleTimeout(now Time) {
	s.SetNow(now)
	s.pickInput()
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
		s.timers.linger = Never
		s.finishClose()
		return
	}
	if s.closeFr.waitLanes && now >= s.timers.laneWait {
		s.closeFr.waitLanes = false
		s.wakeAllWriters()
	}
	if s.state >= StateClosing {
		return
	}
	s.laneTimeouts(now, true)
	if s.state == StateSuspended && now >= s.timers.grace {
		// Grace expiry (RESUME.md 8): CONNECTION_CLOSE on any live
		// outbound pipe, otherwise a local close.
		s.timers.grace = Never
		s.closeWith(&SessionError{Kind: CloseResumeTimeout, Cause: s.lastPipeErr()},
			&closeFrame{code: wire.ResumeTimeout})
		return
	}
	if s.ping.outstanding != 0 && now >= s.ping.expires {
		s.pingExpired()
	}
	if s.peerSeen && s.state != StateSuspended {
		if d := s.idleTimeout(); d > 0 && now >= s.timers.lastActivity.Add(d) {
			s.idleExpired()
			return
		}
		if k := s.cfg.KeepAlive; k > 0 && s.ping.outstanding == 0 && now >= s.timers.lastActivity.Add(k) &&
			s.timers.lastKeepRef != s.timers.lastActivity {
			s.timers.lastKeepRef = s.timers.lastActivity
			s.ping.want = true
			s.wakeAllWriters()
		}
	}
	if s.ack != nil {
		s.ack.handleTimeout(now)
	}
	if s.mp != nil {
		s.mpHandleTimeout(now)
	}
	if s.ack != nil {
		s.paceTimeout(now)
		s.qualityTimers(now, true)
	}
}

// idleExpired closes on the idle timeout (SPEC.md 6.4).
func (s *Session) idleExpired() {
	e := &SessionError{Kind: CloseIdle}
	if s.feat.Acked() {
		s.closeWith(e, &closeFrame{code: wire.IdleTimeout})
		return
	}
	s.closeSilently(e, true)
}
