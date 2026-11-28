package core

// CarrierKeys identifies the physical connections of the two halves.
type CarrierKeys [2]any

func (s *Session) CanAcquireCarriers(keys CarrierKeys) bool {
	needed := 0
	for _, k := range keys {
		if k != nil && s.carriers[k] == 0 {
			needed++
		}
	}
	return needed <= s.policy.Limits.LocalCarriers-len(s.carriers) || needed == 0
}

func (s *Session) AcquireCarriers(keys CarrierKeys) {
	if s.carriers == nil {
		s.carriers = make(map[any]int)
	}
	for _, k := range keys {
		if k != nil {
			s.carriers[k]++
		}
	}
	s.NotifyCapacity()
}

func (s *Session) ReleaseCarriers(keys CarrierKeys) {
	for _, k := range keys {
		if k != nil {
			n := s.carriers[k]
			if n == 1 {
				delete(s.carriers, k)
			} else {
				s.carriers[k] = n - 1
			}
		}
	}
	s.NotifyCapacity()
}

func (s *Session) CarrierCount() int { return len(s.carriers) }

// CanReserveOpener checks actual host execution capacity. A cancelled
// attempt still occupies its slot until the opener returns.
func (s *Session) CanReserveOpener() bool {
	return s.openers < s.cfg.SessionPolicy.Limits.Envelope()
}

// ReserveOpener commits one checked slot under the same session owner.
func (s *Session) ReserveOpener() {
	if !s.CanReserveOpener() {
		panic("zmux: opener capacity changed during reservation")
	}
	s.openers++
	s.NotifyCapacity()
}

func (s *Session) ReleaseOpener() {
	s.openers--
	s.NotifyCapacity()
}

func (s *Session) Openers() int { return s.openers }
