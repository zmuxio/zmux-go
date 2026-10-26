package core

// Quality events (API.md 4.5): a pipe's smoothed RTT or delivery rate
// leaving a band around the last reported values, and the session's
// outbound capacity crossing a floor. Off until set, evaluated in O(1)
// where the estimates change, at most one event per pipe per MinInterval
// (the latest values winning).

// QualityBand is the band of PipeQualityChanged.
type QualityBand struct {
	RTT         Duration // +- this around the last reported smoothed RTT; 0: off
	RateRatio   float64  // [last / r, last x r]; 0: off
	MinInterval Duration // at most one event per pipe per interval (default 1 s; <0: none)
}

// CapacityFloor is the floor of CapacityBelow / CapacityRestored.
type CapacityFloor struct {
	Rate      float64 // bytes/s
	RateRatio float64 // restored at Rate x RateRatio (default 1.25)
}

// pipeQuality is a pipe's band state.
type pipeQuality struct {
	band     *QualityBand // nil: the session's default
	has      bool         // a first report exists
	rtt      Duration
	rate     float64
	lastAt   Time
	pending  bool
	pendTime Time
}

type sessQuality struct {
	band     *QualityBand
	floor    *CapacityFloor
	below    bool
	floorSet bool
}

// SetQualityBand sets the band of one pipe, or (pipe < 0) the default of
// the session's pipes.
func (s *Session) SetQualityBand(pipe int64, b QualityBand) error {
	if b.MinInterval == 0 {
		b.MinInterval = Second
	}
	if pipe < 0 {
		s.q.band = &b
		return nil
	}
	p := s.pipe(uint64(pipe))
	if p == nil || p.out == nil {
		return ErrInvalid
	}
	p.out.q.band = &b
	return nil
}

// SetCapacityFloor sets the capacity floor; it reports at once when the
// capacity is below it.
func (s *Session) SetCapacityFloor(f CapacityFloor) {
	if f.RateRatio <= 0 {
		f.RateRatio = 1.25
	}
	s.q.floor, s.q.below, s.q.floorSet = &f, false, true
	s.checkCapacityFloor()
}

// qualitySample judges p's estimates after they changed.
func (s *Session) qualitySample(p *Pipe) {
	if p.out == nil {
		return
	}
	q := &p.out.q
	b := q.band
	if b == nil {
		b = s.q.band
	}
	if b != nil {
		rtt, rate, ok := s.pipeEstimates(p)
		if ok {
			switch {
			case !q.has:
				q.has, q.rtt, q.rate, q.lastAt = true, rtt, rate, s.now
			case outOfBand(b, q, rtt, rate):
				if b.MinInterval > 0 && s.now < q.lastAt.Add(b.MinInterval) {
					q.pending, q.pendTime = true, q.lastAt.Add(b.MinInterval)
				} else {
					s.reportQuality(p, rtt, rate)
				}
			}
		}
	}
	if s.q.floorSet {
		s.checkCapacityFloor()
	}
}

func outOfBand(b *QualityBand, q *pipeQuality, rtt Duration, rate float64) bool {
	if b.RTT > 0 && (rtt > q.rtt+b.RTT || rtt+b.RTT < q.rtt) {
		return true
	}
	return b.RateRatio > 0 && q.rate > 0 && (rate > q.rate*b.RateRatio || rate*b.RateRatio < q.rate)
}

func (s *Session) pipeEstimates(p *Pipe) (Duration, float64, bool) {
	r := &p.out.snd.rtt
	if p.mp != nil {
		r = &p.mp.rtt
	}
	if !r.has {
		return 0, 0, false
	}
	return r.smoothed, p.out.snd.re.value(), true
}

func (s *Session) reportQuality(p *Pipe, rtt Duration, rate float64) {
	q := &p.out.q
	q.rtt, q.rate, q.lastAt, q.pending = rtt, rate, s.now, false
	s.emit(Event{Kind: EvPipeQuality, Pipe: p.id, RTT: rtt, Rate: rate})
}

// qualityTimers emits the reports held back by MinInterval.
func (s *Session) qualityTimers(now Time, apply bool) Time {
	next := Never
	for _, p := range s.pipes {
		if p.out == nil || !p.out.q.pending {
			continue
		}
		q := &p.out.q
		if now >= q.pendTime {
			if apply {
				if rtt, rate, ok := s.pipeEstimates(p); ok {
					s.reportQuality(p, rtt, rate)
				} else {
					q.pending = false
				}
			}
		} else {
			next = minT(next, q.pendTime)
		}
	}
	return next
}

// checkCapacityFloor compares the measured rates of the usable,
// non-standby outbound pipes with the floor.
func (s *Session) checkCapacityFloor() {
	f := s.q.floor
	if f == nil {
		return
	}
	var sum float64
	for _, p := range s.pipes {
		if p.out == nil || !p.out.snd.re.measured {
			continue
		}
		if p.mp != nil {
			if _, sb := p.mp.effPrio(); sb || !s.pipeUsable(p, true) {
				continue
			}
		}
		sum += p.out.snd.re.value()
	}
	switch {
	case !s.q.below && sum < f.Rate:
		s.q.below = true
		s.emit(Event{Kind: EvCapacityBelow, Rate: sum})
	case s.q.below && sum >= f.Rate*f.RateRatio:
		s.q.below = false
		s.emit(Event{Kind: EvCapacityRestored, Rate: sum})
	}
}
