package core

// penalize halves R_p, discards its samples and keeps new data off p until
// everything sent on it is acknowledged (once per episode).
func (s *Session) penalize(p *Pipe) {
	a := p.mp
	if a.penalized {
		return
	}
	a.penalized = true
	s.relativeStallChanged()
	s.wakePrefix()
	s.stats.Penalties++
	a.penaltyEnd = p.out.snd.so
	e := &p.out.snd.re
	r := e.value()
	clear(e.samples)
	e.samples = e.samples[:0]
	e.r, e.measured = r/2, false
	if q := a.quality; q != nil {
		q.rate = min(q.rate, e.r)
	}
	s.syncPipeHealth(p)
	s.capacityChanged()
	s.checkTakeover()
}

// penaltyAcked ends an episode once everything sent on p is acknowledged.
func (s *Session) penaltyAcked(p *Pipe) {
	if a := p.mp; a.penalized && p.out.snd.cum >= a.penaltyEnd {
		a.penalized = false
		s.relativeStallChanged()
		s.wakePrefix()
		s.syncPipeHealth(p)
		s.capacityChanged()
		s.checkTakeover()
		s.wakeAllWriters()
	}
}
