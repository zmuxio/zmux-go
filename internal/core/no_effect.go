package core

// noEffectWindow shares the plain QMux record allowance between probes and
// standalone cover. The next element is the oldest when the window is full.
type noEffectWindow struct {
	times [16]Time
	next  int
	full  bool
}

func (w *noEffectWindow) available(now Time) bool {
	return w == nil || !w.full || w.times[w.next] <= now.Add(-Second)
}

func (s *Session) recordNoEffect() {
	if s.peerZmux {
		return
	}
	if s.noEffect == nil {
		s.noEffect = &noEffectWindow{}
	}
	w := s.noEffect
	w.times[w.next] = s.now
	w.next = (w.next + 1) % len(w.times)
	w.full = w.full || w.next == 0
}
