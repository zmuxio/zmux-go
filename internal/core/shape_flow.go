package core

import (
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// Every queued original keeps its elapsed budget with its Flow, including
// across head rotation, carrier changes and shaper replacement. A Flow has
// at most one active blocker, even when its members have different routes.
type datagramDelay struct {
	spent   Duration
	start   Time
	waiting bool
}

func (d *datagramDelay) elapsed(end, now Time) Duration {
	if !d.waiting {
		return 0
	}
	return Duration(heldDelay(d.start, end, now))
}

func (s *Session) settleDatagramDelay(d *datagramDelay, end Time) {
	elapsed := d.elapsed(end, s.now)
	d.waiting = false
	d.spent += elapsed
	s.shp.addStall(time.Duration(elapsed))
	s.shp.noteDelay(time.Duration(d.spent))
}

type flowDelay struct {
	pipe   *pipeShape
	native bool
}

func (d flowDelay) end() Time {
	if d.native {
		return d.pipe.native.ready
	}
	return d.pipe.recordDelayEnd()
}

func (s *Session) settleFlowDelay(f *datagramFlow) {
	if f.delay.pipe == nil {
		return
	}
	end := f.delay.end()
	for i := range f.queue {
		o := &f.queue[i]
		s.settleDatagramDelay(&o.delay, min(end, o.deadline))
	}
	f.delay = flowDelay{}
}

// Refresh after an owner turn changes ordinary eligibility or installs a
// blocker. Settle the old eligibility first, then clip the live blocker by
// the earliest remaining budget in the affected Flow.
func (s *Session) refreshFlowDelay(f *datagramFlow) {
	s.settleFlowDelay(f)
	p := f.route.pipe
	if len(f.queue) == 0 || p == nil || p.sh == nil {
		return
	}
	ps := p.sh
	native := f.route.channel
	switch {
	case native:
		if ps.native.flow != f || !s.channelOf(p).takes(s.datagramDataLen(&f.queue[0])) {
			return
		}
	case !ps.delayActive || !s.open(p):
		return
	}
	d := flowDelay{pipe: ps, native: native}
	until := d.end()
	if until <= s.now {
		return
	}
	f.delay = d
	for i := range f.queue {
		o := &f.queue[i]
		if ready, _ := o.ready(); !ready || o.deadline <= s.now ||
			uint64(1+s.datagramDataLen(o)) > s.peerMaxDgram ||
			(i > 0 && s.datagramPathFor(o, false).pipe == nil) {
			continue
		}
		o.delay.start, o.delay.waiting = s.now, true
		end := min(until, o.deadline)
		until = s.now.Add(s.boundedShapeDelay(end.Sub(s.now), o.delay.spent))
	}
	if until < d.end() {
		s.shapeReason(shaping.ReasonPolicyDeadline)
		if native {
			ps.native.ready = until
			ps.armDeadline(shapeNativeAt, until)
			s.wake(p.chSig)
		} else {
			ps.releaseRecordAt(until)
		}
	}
}

func (ps *pipeShape) refreshRecordFlows() {
	if d := ps.s.dg; d != nil {
		for _, f := range d.active {
			if !f.route.channel && f.route.pipe == ps.p || f.delay == (flowDelay{pipe: ps}) {
				ps.s.refreshFlowDelay(f)
			}
		}
	}
	ps.refreshRecordCopyDelay()
}

func (ps *pipeShape) finishRecordFlows() {
	if d := ps.s.dg; d != nil {
		for _, f := range d.active {
			if f.delay == (flowDelay{pipe: ps}) {
				ps.s.settleFlowDelay(f)
			}
		}
	}
	ps.settleRecordCopyDelay()
}

func (ps *pipeShape) releaseRecordAt(at Time) {
	ps.delayUntil = at
	ps.armDeadline(shapeDelayUntil, at)
	ps.s.wakePipe(ps.p)
}

func (ps *pipeShape) settleRecordCopyDelay() {
	if ps.delayCopy {
		c := &ps.p.datagramCopies[0]
		ps.s.settleDatagramDelay(&c.delay, min(ps.recordDelayEnd(), c.deadline))
		ps.delayCopy = false
	}
}

func (ps *pipeShape) refreshRecordCopyDelay() {
	ps.settleRecordCopyDelay()
	s, p := ps.s, ps.p
	if !ps.delayActive || len(p.datagramCopies) == 0 || !maySend(p, false) || !s.open(p) {
		return
	}
	c := &p.datagramCopies[0]
	end := min(ps.recordDelayEnd(), c.deadline)
	if end <= s.now || ps.native.replica {
		return
	}
	path, ready := s.datagramPlacement(p, nil, c.data.len(), c.carrier, c.deadline, false)
	if !ready || path.channel || path.late || c.adaptive && c.targetChannel {
		return
	}
	c.delay.start, c.delay.waiting, ps.delayCopy = s.now, true, true
	if at := s.now.Add(s.boundedShapeDelay(end.Sub(s.now), c.delay.spent)); at < ps.recordDelayEnd() {
		s.shapeReason(shaping.ReasonPolicyDeadline)
		ps.releaseRecordAt(at)
	}
}
