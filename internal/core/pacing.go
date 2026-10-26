package core

// Pacing: which pipes the carrier queue limiter paces, and its inputs
// from the sends and acknowledgements of a pipe (SCHEDULER.md 9.1).

// paced reports whether the limiter paces p: with acknowledgements (a
// multipath session, or the single pipe with pipe acknowledgement), when
// its carrier has neither a low-water mark nor an unsent count.
func (s *Session) paced(p *Pipe) bool {
	return s.ack != nil && p.out != nil && !p.out.lanes[0].info.QueueBound
}

// limInFlight is the bytes in flight as the limiter counts them: the
// ack-eliciting bytes beyond the cumulative point, less the held bytes of
// a pipe with lanes that it leaves out.
func (s *Session) limInFlight(p *Pipe) (uint64, uint64) {
	ps := &p.out.snd
	raw := ps.elicitEnd - min64(ps.elicitEnd, ps.cum)
	held := s.heldBytes(p)
	if ps.lim != nil {
		return raw - min64(raw, ps.lim.leftOut(held)), held
	}
	return raw, held
}

// heldBytes are the bytes acknowledged on their own lanes beyond the
// cumulative point.
func (s *Session) heldBytes(p *Pipe) uint64 {
	d := p.out
	if len(d.lanes) < 2 {
		return 0
	}
	var unacked uint64
	for _, l := range d.lanes {
		for _, g := range l.segs {
			unacked += g.end - max64(g.pos, min64(l.acked, g.end))
		}
	}
	beyond := d.snd.so - min64(d.snd.so, d.snd.cum)
	return beyond - min64(beyond, unacked)
}

// limiterOf returns p's limiter, creating it when p is paced.
func (s *Session) limiterOf(p *Pipe) *limiter {
	if !s.paced(p) {
		p.out.snd.lim = nil
		return nil
	}
	ps := &p.out.snd
	if ps.lim == nil {
		ps.lim = newLimiter(s.now)
	}
	return ps.lim
}

// srttOf is the smoothed RTT the limiter uses for p.
func (s *Session) srttOf(p *Pipe) Duration {
	if p.mp != nil {
		return p.mp.rtt.smoothed
	}
	if p.out.snd.rtt.has {
		return p.out.snd.rtt.smoothed
	}
	return 333 * Millisecond
}

// paceRoom is the stream data, reinjection and copies p may take now;
// -1 when p is not paced.
func (s *Session) paceRoom(p *Pipe) int64 {
	l := s.limiterOf(p)
	if l == nil {
		return -1
	}
	pp := s.limParams()
	in, _ := s.limInFlight(p)
	r := l.room(s.now, in, p.out.snd.re.value(), s.srttOf(p), &pp)
	if r > 1<<40 {
		r = 1 << 40
	}
	return int64(r)
}

// limHeld reports whether the limiter holds p back now.
func (s *Session) limHeld(p *Pipe) bool {
	return p.out != nil && p.out.snd.lim != nil && p.out.snd.lim.held
}

// paceAck feeds an acknowledgement that advanced p's cumulative point.
func (s *Session) paceAck(p *Pipe, ev *limAck) {
	l := s.limiterOf(p)
	if l == nil {
		return
	}
	ps := &p.out.snd
	ev.acked = ps.cum
	ev.sent = max64(ps.elicitEnd, ps.cum)
	held := s.heldBytes(p)
	l.heldMin.update(s.now, float64(held), l.baseOr(s.srttOf(p)))
	ev.held = l.leftOut(held)
	pp := s.limParams()
	wasHeld := l.held
	l.onAck(s.now, ev, &ps.re, s.srttOf(p), &pp)
	if wasHeld && !l.held {
		s.wakeAllWriters()
	}
}

// paceSent: bytes were handed to p's carrier; the bucket pays for all of
// them, though only data waits for it.
func (s *Session) paceSent(p *Pipe, bytes uint64) {
	l := p.out.snd.lim
	if l == nil {
		return
	}
	pp := s.limParams()
	srtt, rate := s.srttOf(p), p.out.snd.re.value()
	l.settle(s.now, bytes, rate, srtt, &pp)
	in, _ := s.limInFlight(p)
	l.refresh(s.now, in, rate, srtt, &pp)
}

// paceSample feeds the single pipe's first QX_PING round trip.
func (s *Session) pacePing(rtt Duration) {
	p := s.pipe(0)
	if p == nil || p.out == nil || p.mp != nil || p.out.snd.rtt.has {
		return
	}
	if l := s.limiterOf(p); l != nil {
		pp := s.limParams()
		l.pingSample(s.now, rtt, &p.out.snd.re, s.srttOf(p), &pp)
	}
}

// paceDeadline is the earliest moment a held pipe may send again.
func (s *Session) paceDeadline() Time {
	if s.sched.mask == 0 {
		return Never // nothing waits for a pipe: no timer (a write wakes the writer)
	}
	t := Never
	for _, p := range s.pipes {
		if p.out != nil && p.out.snd.lim != nil && p.out.snd.lim.held {
			// G: no pacing timer earlier than the granularity (SCHEDULER.md 9.1).
			t = minT(t, max(p.out.snd.lim.release, s.now.Add(limGranularity)))
		}
	}
	return t
}

// paceTimeout lets held pipes go whose release time came.
func (s *Session) paceTimeout(now Time) {
	pp := s.limParams()
	for _, p := range s.pipes {
		if p.out == nil {
			continue
		}
		l := p.out.snd.lim
		if l == nil || !l.held || l.release > now {
			continue
		}
		in, _ := s.limInFlight(p)
		l.refresh(now, in, p.out.snd.re.value(), s.srttOf(p), &pp)
		if !l.held {
			s.wakePipe(p)
			s.wakeAllWriters()
		}
	}
}
