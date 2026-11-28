package core

// DisablePathAdvice stops publishing advisories. Multi-pipe placement keeps
// collecting its measurements, with a fresh advisory episode on reactivation.
func (s *Session) DisablePathAdvice() {
	s.cfg.PathAdvice = false
	if s.mp == nil {
		return
	}
	for _, p := range s.pipes {
		if len(s.pipes) == 1 && !s.memberFeedback {
			p.mp.quality = nil
		} else if q := p.mp.quality; q != nil {
			if q.advice != 0 {
				q.generation++
				s.NotifyCapacity()
			}
			q.pathAdvice = pathAdvice{}
		}
	}
}
