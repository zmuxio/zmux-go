package core

const (
	measurementWindow = 30 * Second
	unobservedRTT     = 333 * Millisecond
	floorBucketWindow = 45 * Second
)

// pathQuality is attached only when the session has a placement or advice
// consumer. The protocol observations used for liveness and pacing stay in
// rttEst and rateEst.
type pathQuality struct {
	generation        uint64
	measured          MeasuredQuality
	witness           backlogWitness
	unsentAt          map[uint64]Time
	rtt, hold, minRTT Duration
	rttAt, minAt      Time
	olderMinRTT       Duration
	rate              float64
	rateAt            Time
	placedFrom        uint64
	placedAt          Time
	placed            bool
	refreshAt         Time
	pathAdvice
}

type pathAdvice struct {
	since, recoverAt Time
	reference        Duration
	advice, pending  PathQualityReason
}

func (s *Session) enablePathQuality(p *Pipe) {
	if p.mp.quality == nil {
		p.mp.quality = new(pathQuality)
	}
}

// Joining a second attachment enables placement measurement for the existing
// direction as well. A prepared join that never creates a pipe needs no samples.
func (s *Session) enableJoinedPathQuality(p *Pipe) {
	if len(s.pipes) == 0 && !s.cfg.PathAdvice {
		return
	}
	for _, existing := range s.pipes {
		s.enablePathQuality(existing)
	}
	s.enablePathQuality(p)
}

// EnablePathAdvice starts collecting samples when the host registers an event
// callback. It can run before the transport parameters enable multipath.
func (s *Session) EnablePathAdvice() {
	s.cfg.PathAdvice = true
	if s.mp != nil {
		for _, p := range s.pipes {
			s.enablePathQuality(p)
		}
	}
}

func (q *pathQuality) sampleRTT(rtt, hold Duration, now Time, quiet bool) {
	if rtt <= 0 {
		return
	}
	observed := maxD(Microsecond, rtt-hold)
	// Keep the previous minimum while accumulating the current bucket.
	// A floor stays available for at least one bucket, even across the
	// boundary. Observations and the floor both exclude peer ACK hold.
	if q.minRTT == 0 {
		q.minRTT, q.minAt = observed, now
	} else if !quiet {
		age := now.Sub(q.minAt)
		switch {
		case age >= 2*floorBucketWindow:
			q.olderMinRTT = 0
			q.minRTT, q.minAt = observed, now
		case age >= floorBucketWindow:
			q.olderMinRTT = q.minRTT
			q.minRTT, q.minAt = observed, q.minAt.Add(floorBucketWindow)
		default:
			q.minRTT = minD(q.minRTT, observed)
		}
	}
	if !q.freshRTT(now) {
		q.rtt = observed
	} else {
		divisor := Duration(4)
		if observed > q.rtt {
			divisor = 2
		}
		q.rtt += (observed - q.rtt) / divisor
	}
	q.rttAt, q.hold = now, hold
}

func (q *pathQuality) floor(now Time) (Duration, bool) {
	if q.minRTT == 0 || q.rtt != 0 && !q.freshRTT(now) {
		return 0, false
	}
	age := now.Sub(q.minAt)
	if age >= 2*floorBucketWindow {
		return 0, false
	}
	if q.olderMinRTT != 0 && age < floorBucketWindow {
		return minD(q.minRTT, q.olderMinRTT), true
	}
	return q.minRTT, true
}

func (q *pathQuality) freshRTT(now Time) bool {
	return q.rtt > 0 && now.Sub(q.rttAt) < measurementWindow
}

func (q *pathQuality) currentRTT(now Time, fallback Duration) Duration {
	if q.rtt == 0 {
		return fallback
	}
	if !q.freshRTT(now) {
		return unobservedRTT
	}
	return q.rtt
}

func (q *pathQuality) freshRate(now Time) bool {
	return q.rate > 0 && now.Sub(q.rateAt) < rateWindow
}

func (q *pathQuality) currentRate(e *rateEst, now Time) float64 {
	r := q.rate
	if r <= 0 {
		r = initialRate
	} else if !q.freshRate(now) {
		r = min(r, initialRate)
	}
	if e.lo > 0 {
		r = min(r, e.lo)
	}
	return r
}

// An application-limited ACK can demonstrate more service. A sparse
// write does not demonstrate diminished capacity.
func (q *pathQuality) sampleRate(e *rateEst, r float64, bottleneck bool, now Time) {
	if !bottleneck && r <= q.currentRate(e, now) {
		return
	}
	if !q.freshRate(now) {
		q.rate = r
	} else {
		gain := 0.125
		if r < q.rate {
			gain = 0.5
		}
		q.rate += (r - q.rate) * gain
	}
	q.rateAt = now
}

func (p *Pipe) pathQuality() *pathQuality {
	if p.mp != nil {
		return p.mp.quality
	}
	return nil
}

func (a *attach) currentRTT(now Time) Duration {
	if q := a.quality; q != nil {
		return q.currentRTT(now, a.rtt.smoothed)
	}
	return a.rtt.smoothed
}
