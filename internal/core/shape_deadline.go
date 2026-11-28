package core

import (
	"math"

	"github.com/zmuxio/zmux-go/shaping"
)

// A shaper's event can replace its other deadlines. Deliver it before
// checking an injection or releasing a record or a data gate.
type shapeDeadlineReason int

const (
	shapeWakeAt shapeDeadlineReason = iota
	shapeInjectAt
	shapeDelayUntil
	shapeGateUntil
	shapeNativeAt
	shapeDeadlineCount
)

func (ps *pipeShape) armDeadline(reason shapeDeadlineReason, at Time) {
	d := &ps.deadlines[reason]
	if at == Never {
		if reason == shapeDelayUntil && d.index != 0 {
			ps.s.shp.delayed--
		}
		ps.s.shp.timers.Cancel(d)
		return
	}
	if reason == shapeDelayUntil && d.index == 0 {
		ps.s.shp.delayed++
	}
	d.Value, d.Order = ps, int(reason)
	ps.s.shp.timers.Set(d, at)
}

func (ps *pipeShape) cancelDeadlines() {
	for i := range ps.deadlines {
		ps.armDeadline(shapeDeadlineReason(i), Never)
	}
}

// armGateDeadline is called when actual data or a retained copy waits.
// The release estimate changes only when its gate or token state changes.
func (ps *pipeShape) gateRelease() Time {
	at := ps.hold
	if b := &ps.bucket; b.rate > 0 && b.held() {
		release := Never
		if us, ok := b.release(); ok && us <= math.MaxInt64/1000 {
			release = Time(us * 1000)
		}
		at = max(at, release)
	}
	if p := ps.s.shp.eff.Profile; p != nil && ps.heldSince != Never {
		at = min(at, ps.heldSince.Add(max(Duration(p.MaxAddedDelay)-ps.priorWait(ps.heldSince), 0)))
	}
	return at
}

func (ps *pipeShape) armGateDeadline() {
	at := ps.gateRelease()
	if ps.heldSince != Never {
		ps.heldUntil = at
	}
	if at <= ps.s.now {
		at = Never
	}
	ps.armDeadline(shapeGateUntil, at)
}

func (ps *pipeShape) refreshGateDeadline() {
	if ps.deadlines[shapeGateUntil].index != 0 {
		ps.armGateDeadline()
	}
}

func (ps *pipeShape) hasGateWork() bool {
	if ps.s.sched.mask != 0 {
		return true
	}
	a := ps.p.mp
	return a != nil && (len(a.copyQ) != 0 || len(a.selfCopyQ) != 0)
}

// ShapingDeadline is the earliest shaping deadline. PIPE_ACK also has its
// own deadline and can release a delayed record before this time.
func (s *Session) ShapingDeadline() Time {
	if s.shp == nil || !s.transportLive() {
		return Never
	}
	at := s.shapeDeadline()
	if s.hasShapingDelay() && s.ack != nil {
		at = min(at, s.ack.nextTimeout())
	}
	return at
}

func (s *Session) hasShapingDelay() bool { return s.shp != nil && s.shp.delayed != 0 }

func (s *Session) shapeDeadline() Time { return s.shp.timers.Next() }

// shapeEventsDue delivers the pipe's due effects before it plans a unit.
func (ps *pipeShape) shapeEventsDue(now Time) {
	for _, reason := range [...]shapeDeadlineReason{shapeWakeAt, shapeInjectAt} {
		d := &ps.deadlines[reason]
		if d.index != 0 && d.at <= now {
			ps.s.shp.timers.Cancel(d)
			ps.fireDeadline(reason, now)
		}
	}
}

func (s *Session) shapeTimeout(now Time) bool {
	q := &s.shp.timers
	fired := false
	for d := q.PopDue(now); d != nil; d = q.PopDue(now) {
		fired = true
		d.Value.fireDeadline(shapeDeadlineReason(d.Order), now)
	}
	return fired
}

func (ps *pipeShape) fireDeadline(reason shapeDeadlineReason, now Time) {
	s := ps.s
	switch reason {
	case shapeWakeAt:
		ps.event(shaping.EventTimer)
	case shapeInjectAt:
		if !s.coverEligible(ps.p) {
			ps.clearInjection()
			return
		}
		ps.injDue = true
		s.wakePipe(ps.p)
	case shapeDelayUntil:
		s.shp.delayed--
		s.wakePipe(ps.p)
	case shapeGateUntil:
		if ps.heldSince != Never && ps.held(now) {
			ps.armGateDeadline()
		} else {
			s.wakePipe(ps.p)
		}
	case shapeNativeAt:
		s.wake(ps.p.chSig)
	case shapeDeadlineCount:
		panic("zmux: invalid shaping deadline")
	}
}
