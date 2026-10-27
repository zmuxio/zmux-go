package core

// Delivery-rate estimation: BBR's delivery-rate samples,
// a windowed maximum over the last 10 round trips within 10 s, and the
// application-limited rules (the bottleneck observation and the bubble).

const (
	rateRounds   = 10
	rateWindow   = 10 * Second
	initialRate  = 1e6 // bytes/s before the first sample
	rateKeepMany = 64  // samples kept at most (the window is a maximum)
)

// rateEst is R_p of one outbound pipe.
type rateEst struct {
	r       float64 // the windowed maximum; 0 before the first sample
	lo      float64 // R_lo, the limiter's rate bound; 0: none
	samples []rateSmp

	// Round trips: a round ends when a byte sent after it began is
	// acknowledged.
	round    uint64
	roundEnd uint64

	// The bubble: the send-order position recorded at the last
	// application-limited moment; a range sent before the cumulative point
	// passes it is application-limited. 0 at the start, so the first
	// ranges of a new pipe are.
	bubble uint64

	// The bottleneck observation: blockEpoch advances whenever the pipe was
	// not writable while ready data was not committed to any pipe; a range
	// whose epoch moved before its acknowledgement was sent while the pipe
	// was the bottleneck.
	blockEpoch uint64
	blockedNow bool
	measured   bool // a bottleneck sample was taken
}

type rateSmp struct {
	r     float64
	round uint64
	at    Time
}

// value returns R_p, the initial 1 MB/s before any sample.
func (e *rateEst) value() float64 {
	r := e.r
	if r <= 0 {
		r = initialRate
	}
	if e.lo > 0 && e.lo < r {
		return e.lo
	}
	return r
}

// blocked records that the pipe is not writable while data waits.
func (e *rateEst) blocked() {
	e.blockEpoch++
}

// appLimited records an application-limited moment at send order so.
func (e *rateEst) appLimited(so uint64) { e.bubble = so }

// sendFlags returns what a range sent now records: whether a bubble is in
// flight (cum has not passed the recorded position) and the epoch.
func (e *rateEst) sendFlags(cum uint64) (bool, uint64) {
	return cum <= e.bubble, e.blockEpoch
}

// advance moves the round count with the cumulative point; it reports
// whether a round trip ended.
func (e *rateEst) advance(cum, so uint64) bool {
	if cum > e.roundEnd {
		e.round++
		e.roundEnd = so
		return true
	}
	return false
}

// sample feeds one delivery-rate sample.
// It returns the sample's value when it is a bottleneck sample, else 0.
func (e *rateEst) sample(r float64, bubble bool, epoch uint64, now Time) float64 {
	bottleneck := !bubble && (epoch != e.blockEpoch || e.blockedNow)
	if !bottleneck && r <= e.value() {
		return 0
	}
	if bottleneck {
		e.measured = true
	}
	e.samples = append(e.samples, rateSmp{r: r, round: e.round, at: now})
	cut := 0
	for cut < len(e.samples) {
		x := e.samples[cut]
		if x.round+rateRounds >= e.round && now.Sub(x.at) <= rateWindow {
			break
		}
		cut++
	}
	if len(e.samples)-cut > rateKeepMany {
		cut = len(e.samples) - rateKeepMany
	}
	if cut > 0 {
		n := copy(e.samples, e.samples[cut:])
		clear(e.samples[n:])
		e.samples = e.samples[:n]
	}
	m := 0.0
	for _, x := range e.samples {
		if x.r > m {
			m = x.r
		}
	}
	if m > 0 {
		e.r = m
	}
	if bottleneck {
		return r
	}
	return 0
}
