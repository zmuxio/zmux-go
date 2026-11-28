package core

import (
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// nativePlan reserves the selected queue head in place. The pipe owns one
// reservation shared by originals and copies.
type nativePlan struct {
	flow        *datagramFlow
	unit        shaping.UnitPlan
	reservation shapeDispatch
	ready       Time
	replica     bool
}

func (n *nativePlan) pending() bool { return n.flow != nil || n.replica }

func (ps *pipeShape) settleNativeCopyDelay() {
	if ps.native.replica && len(ps.p.datagramCopies) > 0 {
		c := &ps.p.datagramCopies[0]
		ps.s.settleDatagramDelay(&c.delay, min(ps.native.ready, c.deadline))
	}
}

func (ps *pipeShape) startNativeCopyDelay(ch ChannelUse) {
	if ps.native.replica && len(ps.p.datagramCopies) > 0 {
		c := &ps.p.datagramCopies[0]
		if ps.s.now < ps.native.ready && ch.takes(c.data.len()) {
			c.delay.start, c.delay.waiting = ps.s.now, true
		}
	}
}

func (ps *pipeShape) clearNative() {
	if ps.native.pending() {
		if f := ps.native.flow; f != nil && len(f.queue) != 0 && f.queue[0].reserved == ps {
			if f.delay == (flowDelay{pipe: ps, native: true}) {
				ps.s.settleFlowDelay(f)
			}
			f.queue[0].reserved = nil
		}
		ps.settleNativeCopyDelay()
		ps.s.settleShape(ps.native.reservation, 0, 0)
		ps.s.wake(ps.p.chSig)
	}
	ps.native = nativePlan{}
	ps.armDeadline(shapeNativeAt, Never)
}

func (ps *pipeShape) cancelNative(r shaping.RuntimeReason) {
	if ps.native.pending() {
		ps.s.shapeReason(r)
	}
	ps.clearNative()
}

// validateNative rechecks the original size and eligibility before release.
func (ps *pipeShape) validateNative(ch ChannelUse) {
	s, p := ps.s, ps.p
	if f := ps.native.flow; f != nil {
		o := &f.queue[0]
		opening, constrained, class, usable := s.datagramClass(o.st)
		if ready, _ := o.ready(); !ready || !s.datagramUsable(p, opening, constrained, class, usable) ||
			(!ch.supports(s.datagramDataLen(o)) || s.now >= ps.native.ready && !ch.takes(s.datagramDataLen(o))) || o.deadline <= s.now ||
			uint64(1+s.datagramDataLen(o)) > s.peerMaxDgram {
			r := shaping.ReasonClippedRoom
			if o.deadline <= s.now {
				r = shaping.ReasonApplicationExpired
			}
			ps.cancelNative(r)
		}
	} else if len(p.datagramCopies) == 0 || !ch.supports(p.datagramCopies[0].data.len()) ||
		s.now >= ps.native.ready && !ch.takes(p.datagramCopies[0].data.len()) {
		ps.cancelNative(shaping.ReasonClippedRoom)
	}
}

func (s *Session) nativePadding(ps *pipeShape, ch ChannelUse, n int, f *datagramFlow, c *datagramCopy, expiry Time) (pad int, ready bool) {
	if ps == nil || ps.datagram == nil {
		return 0, true
	}
	base := wire.ChannelDatagramLen(ch.HasID, ch.ID, n)
	room := min(ch.Room, wire.MaxRecordSize+base-n-1)
	var unit shaping.UnitPlan
	if ps.native.pending() {
		if ps.native.flow != f || ps.native.replica != (c != nil) {
			ps.clearNative()
			return 0, true
		}
		if s.now < ps.native.ready {
			return 0, false
		}
		unit = ps.native.unit
		ps.clearNative()
	} else {
		v := shaping.DatagramView{Pipe: ps.p.id, Now: time.Duration(s.now), Len: base, Limit: ch.Limit, Room: ch.Room,
			Padding: s.feat.CarrierDatagramPadding && !s.shp.eff.NoPadding, Replica: c != nil}
		if ps.caps&shaping.CapBudget != 0 {
			v.Meters = s.shapeMeters()
		}
		var plan shaping.DatagramPlan
		if !ps.call(func() { ps.datagram.PlanDatagram(&v, &plan) }) {
			return 0, true
		}
		ps.apply(&plan.Effects, 0)
		if ps.sh == nil {
			return 0, true
		}
		unit = plan.UnitPlan
		// A datagram with a deadline can use all of its remaining wait, but
		// at the deadline it expires before the writer may dispatch it.
		var prior Duration
		if f != nil {
			prior = f.queue[0].delay.spent
		} else if c != nil {
			prior = c.delay.spent
		}
		delay := s.shapePause(min(Duration(unit.Delay), expiry.Sub(s.now)), prior)
		if delay > 0 {
			pad, _ := s.nativePlanRoom(base, room, &unit)
			ps.native = nativePlan{flow: f, unit: unit, reservation: s.reserveShape(uint64(pad)),
				ready: s.now.Add(delay), replica: c != nil}
			ps.armDeadline(shapeNativeAt, ps.native.ready)
			if f != nil {
				f.queue[0].reserved = ps
				s.refreshFlowDelay(f)
			} else {
				ps.startNativeCopyDelay(ch)
			}
			if s.now < ps.native.ready {
				return 0, false
			}
			ps.clearNative()
		}
	}
	pad, reason := s.nativePlanRoom(base, room, &unit)
	if reason != shaping.ReasonNone {
		s.shp.clip(reason)
	}
	return pad, true
}

func (s *Session) nativePlanRoom(base, room int, unit *shaping.UnitPlan) (int, shaping.RuntimeReason) {
	target := shaping.PadFor(base, unit)
	if !s.feat.CarrierDatagramPadding || s.shp.eff.NoPadding {
		room = base
	}
	room = max(room, base)
	limit := base + int(min(uint64(room-base), s.shapeRoom()))
	reason := shaping.ReasonNone
	if target > limit {
		reason = shaping.ReasonClippedRoom
		switch {
		case !s.feat.CarrierDatagramPadding:
			reason = shaping.ReasonUnsupportedCapability
		case s.shp.eff.NoPadding:
			reason = shaping.ReasonDisabled
		case limit < room:
			reason = shaping.ReasonOverheadLimit
		}
	}
	return min(target, limit) - base, reason
}
