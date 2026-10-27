package core

import "github.com/zmuxio/zmux-go/internal/wire"

// The carrier queue limiter: a pipe whose carrier has
// neither a low-water mark nor an unsent count is paced from what the peer
// acknowledges, in the manner of BBR, in a multipath session and on the
// single pipe with pipe acknowledgement. Every event costs constant
// amortised work: the standing queue is a windowed minimum, r_q a windowed
// maximum, the gain cycle a phase and its start, the pacing a token bucket
// refilled with the time that elapsed.

const (
	limGranularity = Millisecond // G: drivers arm no timer earlier than this
	startupGain    = 2.885
	downPhase      = 1
	downPhases     = 4
	bucketQuanta   = 2.0
	fullGrowth     = 1.25
	fullRounds     = 3
	baseSpan       = 10 * Second
	baseTolerance  = 8
	explainedSpan  = Second
	explainedBases = 8
	probeMin       = 200 * Millisecond
	rqSpan         = 500 * Millisecond
	rqBases        = 4
	rateBoundLife  = 10 * Second
)

var gainCycle = [8]float64{1.25, 0.75, 1, 1, 1, 1, 1, 1}

type limMode uint8

const (
	limStartup limMode = iota
	limDrain
	limCruise
	limProbe
)

// limiter is the carrier queue limiter of one outbound pipe.
type limiter struct {
	mode limMode
	// Startup: the last growth step and the round trips since without
	// 25 % more.
	full float64
	flat int
	// Cruise: the phase of the gain cycle and when it began.
	phase      int
	phaseSince Time
	// Probe (base refresh).
	pr limProbeState

	nearAt, explainedAt Time
	baseD               Duration
	baseHas             bool
	recent              winMin // u over max(srtt/2, the gap since the last sample)
	lastSample          Time
	hasLastSample       bool
	bottleneck          winMax // bottleneck rate samples (r_q)
	roundMax            float64
	boundSet            Time
	boundHas            bool

	tokens    float64
	hasTokens bool
	refilled  Time
	inFlight  uint64
	held      bool
	release   Time // Never: until an acknowledgement makes room
	heldMin   winMin
}

type limProbeState struct {
	since      Time
	length     Duration
	startSent  uint64
	drainedAt  Time
	oldBase    Duration
	afterDrain Duration
	rq         float64
	drained    bool
	hasAfter   bool
	hasRq      bool
}

func newLimiter(now Time) *limiter {
	return &limiter{nearAt: now, explainedAt: now, refilled: now, release: Never}
}

// limParams are the session values the limiter uses.
type limParams struct {
	budget    Duration // B
	threshold uint64   // the peer's zmux_ack_threshold
	ackDelay  Duration // the peer's zmux_max_ack_delay
	chunk     uint64   // spread_chunk
}

// slack is A = min(threshold, R x max_ack_delay) + spread_chunk.
func (p *limParams) slack(rate float64) uint64 {
	d := uint64(rate * float64(p.ackDelay) / 1e9)
	return min64(p.threshold, d) + p.chunk
}

func timeAt(bytes uint64, rate float64) Duration {
	return Duration(float64(bytes) / max(rate, 1) * 1e9)
}

func (s *Session) limParams() limParams {
	p := limParams{budget: s.cfg.LatencyBudget, threshold: 65536, ackDelay: 25 * Millisecond,
		chunk: uint64(s.cfg.SpreadChunk)}
	if s.peer.Has&wire.HasAckThreshold != 0 {
		p.threshold = s.peer.AckThreshold
	}
	if s.ack != nil {
		p.ackDelay = s.ack.peerDelay
	}
	return p
}

func (l *limiter) baseOr(srtt Duration) Duration {
	if l.baseHas {
		return l.baseD
	}
	return srtt
}

func (l *limiter) unit(srtt Duration) Duration { return maxD(l.baseOr(srtt), limGranularity) }

func (l *limiter) downDone(rate float64, srtt Duration, p *limParams) bool {
	bdp := uint64(rate * float64(l.baseOr(srtt)) / 1e9)
	return l.inFlight <= bdp+p.slack(rate)
}

// walk runs the cycle on to t: the phase reached, its start and the gain
// integral from phaseSince to t in seconds.
func (l *limiter) walk(t Time, unit Duration, downDone bool) (int, Time, float64) {
	u := max(unit, 1)
	plen := func(k int) int64 {
		switch {
		case k == downPhase && downDone:
			return 0
		case k == downPhase:
			return u * downPhases
		}
		return u
	}
	left := t.Sub(l.phaseSince)
	if left < 0 {
		left = 0
	}
	k := l.phase
	var start int64
	gain := 0.0
	whole := func() bool {
		n := plen(k)
		if left < n {
			return false
		}
		gain += gainCycle[k] * float64(n)
		start += n
		left -= n
		return true
	}
	if whole() {
		k = (k + 1) % len(gainCycle)
		var period int64
		var pg float64
		for i := range gainCycle {
			period += plen(i)
			pg += gainCycle[i] * float64(plen(i))
		}
		if period > 0 {
			n := left / period
			gain += float64(n) * pg
			start += n * period
			left -= n * period
		}
		for whole() {
			k = (k + 1) % len(gainCycle)
		}
	}
	gain += gainCycle[k] * float64(left)
	return k, l.phaseSince.Add(start), gain / 1e9
}

func (l *limiter) gain(now Time, rate float64, srtt Duration, p *limParams) float64 {
	switch l.mode {
	case limStartup:
		return startupGain
	case limDrain:
		return 1 / startupGain
	case limCruise:
		k, _, _ := l.walk(now, l.unit(srtt), l.downDone(rate, srtt, p))
		return gainCycle[k]
	case limProbe:
		// The probe paces at the measured rate.
	}
	return 1
}

// pace returns gain x R and the quantum max(2 x chunk, gain x R x G).
func (l *limiter) pace(now Time, rate float64, srtt Duration, p *limParams) (float64, float64) {
	r := l.gain(now, rate, srtt, p) * max(rate, 1)
	return r, max(float64(2*p.chunk), r*float64(limGranularity)/1e9)
}

func (l *limiter) gainTime(from, to Time, rate float64, srtt Duration, p *limParams) float64 {
	if l.mode != limCruise {
		return l.gain(to, rate, srtt, p) * float64(max(to.Sub(from), 0)) / 1e9
	}
	unit, done := l.unit(srtt), l.downDone(rate, srtt, p)
	_, _, g1 := l.walk(to, unit, done)
	_, _, g0 := l.walk(from, unit, done)
	return max(g1-g0, 0)
}

func (l *limiter) bucket(now Time, rate float64, srtt Duration, p *limParams) float64 {
	_, q := l.pace(now, rate, srtt, p)
	if !l.hasTokens {
		return q
	}
	return min(l.tokens+max(rate, 1)*l.gainTime(l.refilled, now, rate, srtt, p), bucketQuanta*q)
}

func (l *limiter) settle(now Time, bytes uint64, rate float64, srtt Duration, p *limParams) {
	l.tokens = l.bucket(now, rate, srtt, p) - float64(bytes)
	l.hasTokens = true
	l.refilled = now
}

func (l *limiter) advance(now Time, inFlight uint64, rate float64, srtt Duration, p *limParams) {
	l.settle(now, 0, rate, srtt, p)
	if l.mode == limCruise {
		l.phase, l.phaseSince, _ = l.walk(now, l.unit(srtt), l.downDone(rate, srtt, p))
	}
	l.inFlight = inFlight
	if l.mode == limCruise && l.phase == downPhase && l.downDone(rate, srtt, p) {
		l.phase, l.phaseSince = downPhase+1, now
	}
}

// cap is 2 x R x (RTT_base + B) + A, or A during a probe.
func (l *limiter) cap(rate float64, srtt Duration, p *limParams) uint64 {
	a := p.slack(rate)
	if l.mode == limProbe {
		return a
	}
	return uint64(2*rate*float64(l.baseOr(srtt)+p.budget)/1e9) + a
}

// room is the stream data the pipe may take now, rounded up to whole
// chunks: none while the bucket is empty or the cap reached.
func (l *limiter) room(now Time, inFlight uint64, rate float64, srtt Duration, p *limParams) uint64 {
	t := l.bucket(now, rate, srtt, p)
	c := l.cap(rate, srtt, p)
	if t < 1 || inFlight >= c {
		return 0
	}
	r := max64(min64(uint64(t), c-inFlight), 1)
	ch := max64(p.chunk, 1)
	return (r + ch - 1) / ch * ch
}

// refresh decides whether the pipe is held and until when.
func (l *limiter) refresh(now Time, inFlight uint64, rate float64, srtt Duration, p *limParams) {
	l.advance(now, inFlight, rate, srtt, p)
	l.held = l.room(now, inFlight, rate, srtt, p) == 0
	l.release = Never
	if t := l.bucket(now, rate, srtt, p); l.held && t < 1 {
		// The driver's timer is armed no earlier than G (paceDeadline).
		d := limGranularity
		if pr, _ := l.pace(now, rate, srtt, p); pr > 0 {
			d = Duration(min((1-t)/pr*1e9, float64(Never)/2)) + Microsecond
		}
		l.release = now.Add(d)
	}
}

func (l *limiter) standing(srtt Duration) Duration {
	if u, ok := l.recent.get(); ok {
		return max(u-l.baseOr(srtt), 0)
	}
	return 0
}

// limAck is one acknowledgement that advanced the pipe's position.
type limAck struct {
	rtt       Duration // adjusted RTT sample; 0: none
	rttSentAt Time
	rate      float64 // bottleneck rate sample; 0: none
	roundEnd  bool
	acked     uint64 // the cumulative point
	sent      uint64 // end of the last ack-eliciting byte (at least acked)
	held      uint64 // held bytes left out (lanes)
}

func (l *limiter) onAck(now Time, ev *limAck, e *rateEst, srtt Duration, p *limParams) {
	rate := e.value()
	l.advance(now, l.inFlight, rate, srtt, p)
	if l.mode == limDrain || l.mode == limCruise {
		l.maybeProbe(now, ev, srtt)
	}
	aTime := timeAt(p.slack(rate), rate)
	if ev.rtt > 0 {
		l.rttSample(now, ev.rtt, ev.rttSentAt, srtt, p, aTime)
	}
	base := l.baseOr(srtt)
	if ev.rate > 0 {
		l.roundMax = max(l.roundMax, ev.rate)
		l.bottleneck.update(now, ev.rate, maxD(rqSpan, rqBases*base))
	}
	if ev.roundEnd {
		l.roundEnded(e, srtt, p, aTime)
	}
	if e.lo > 0 && (e.r <= e.lo || !l.boundHas || now.Sub(l.boundSet) >= rateBoundLife) {
		e.lo = 0
		l.boundHas = false
	}
	inFlight := ev.sent - min64(ev.sent, ev.acked)
	inFlight -= min64(inFlight, ev.held)
	switch l.mode {
	case limStartup, limCruise:
		// Phase changes come from refresh below.
	case limDrain:
		bdp := uint64(rate * float64(base) / 1e9)
		if l.standing(srtt) <= p.budget || inFlight <= max64(bdp, p.slack(rate)) {
			l.mode, l.phase, l.phaseSince = limCruise, 0, now
		}
	case limProbe:
		pr := &l.pr
		if !pr.drained && ev.acked >= pr.startSent {
			pr.drained, pr.drainedAt = true, now
		}
		measured := pr.hasAfter || ev.acked >= ev.sent
		if now >= pr.since.Add(pr.length) && measured && pr.drained {
			l.endProbe(now, e, p)
		}
	}
	l.refresh(now, inFlight, e.value(), srtt, p)
}

// pingSample is the QX_PING round trip of a single pipe's first record.
func (l *limiter) pingSample(now Time, rtt Duration, e *rateEst, srtt Duration, p *limParams) {
	rate := e.value()
	l.rttSample(now, rtt, now.Add(-rtt), srtt, p, timeAt(p.slack(rate), rate))
}

func (l *limiter) rttSample(now Time, u Duration, sentAt Time, srtt Duration, p *limParams, aTime Duration) {
	gap := Duration(0)
	if l.hasLastSample {
		gap = now.Sub(l.lastSample)
	}
	l.recent.update(now, float64(u), maxD(srtt/2, gap))
	l.lastSample, l.hasLastSample = now, true
	if l.mode == limProbe && l.pr.drained && sentAt >= l.pr.drainedAt {
		if !l.pr.hasAfter || u < l.pr.afterDrain {
			l.pr.afterDrain, l.pr.hasAfter = u, true
		}
	}
	if !l.baseHas || u < l.baseD {
		l.baseD, l.baseHas = u, true
	}
	base := l.baseOr(srtt)
	if u <= base+p.budget+aTime+base/baseTolerance {
		l.nearAt = now
	}
	if u <= 2*(base+p.budget)+aTime {
		l.explainedAt = now
	}
}

func (l *limiter) roundEnded(e *rateEst, srtt Duration, p *limParams, aTime Duration) {
	sampled := l.roundMax
	l.roundMax = 0
	if l.mode == limStartup && sampled > 0 {
		// Rounds limited by the application say nothing of the path.
		if e.value() >= l.full*fullGrowth {
			l.full, l.flat = e.value(), 0
		} else if l.flat++; l.flat >= fullRounds {
			l.mode = limDrain
		}
	}
	if e.lo > 0 && sampled > e.lo && l.standing(srtt) <= p.budget+aTime {
		e.lo = sampled
	}
}

func (l *limiter) maybeProbe(now Time, ev *limAck, srtt Duration) {
	base := l.baseOr(srtt)
	far := now.Sub(l.nearAt) >= baseSpan
	unexplained := now.Sub(l.explainedAt) >= maxD(explainedSpan, explainedBases*base)
	if !far && !unexplained {
		return
	}
	l.mode = limProbe
	rq, ok := l.bottleneck.get(now, maxD(rqSpan, rqBases*base))
	l.pr = limProbeState{since: now, length: maxD(probeMin, srtt), startSent: ev.sent,
		drained: ev.acked >= ev.sent, drainedAt: now, oldBase: base, rq: rq, hasRq: ok}
}

// endProbe: the base is measured again, unless a byte sent after the old
// queue was gone came back near the old base, which shows the queue was
// the sender's and the path slower: then the old base stays and R_lo = r_q.
func (l *limiter) endProbe(now Time, e *rateEst, p *limParams) {
	pr := l.pr
	unchanged := pr.hasAfter && pr.hasRq && pr.afterDrain <= pr.oldBase+p.budget+timeAt(p.slack(pr.rq), pr.rq)
	switch {
	case unchanged && pr.rq < e.value():
		e.lo = pr.rq
		l.boundSet, l.boundHas = now, true
		l.baseD, l.baseHas = pr.oldBase, true
	case pr.hasAfter:
		l.baseD, l.baseHas = pr.afterDrain, true
	}
	l.nearAt, l.explainedAt = now, now
	l.mode, l.phase, l.phaseSince = limCruise, 0, now
}

// leftOut is the held bytes left out of the bytes in flight: all of them
// during start-up, their smallest count over the last RTT_base afterwards.
func (l *limiter) leftOut(held uint64) uint64 {
	if l.mode == limStartup {
		return held
	}
	if m, ok := l.heldMin.get(); ok {
		return min64(held, uint64(m))
	}
	return held
}

// ---- windowed extremes ----

type winSample struct {
	at Time
	v  float64
}

// winMin is a minimum over a sliding time span (monotonic deque).
type winMin struct{ q []winSample }

func (w *winMin) update(now Time, v float64, span Duration) {
	for len(w.q) > 0 && w.q[len(w.q)-1].v >= v {
		w.q = w.q[:len(w.q)-1]
	}
	w.q = append(w.q, winSample{now, v})
	cut := 0
	for cut < len(w.q)-1 && now.Sub(w.q[cut].at) > span {
		cut++
	}
	if cut > 0 {
		w.q = append(w.q[:0], w.q[cut:]...)
	}
}

func (w *winMin) get() (Duration, bool) {
	if len(w.q) == 0 {
		return 0, false
	}
	return Duration(w.q[0].v), true
}

// winMax is a maximum over a sliding time span (monotonic deque).
type winMax struct{ q []winSample }

func (w *winMax) update(now Time, v float64, span Duration) {
	for len(w.q) > 0 && w.q[len(w.q)-1].v <= v {
		w.q = w.q[:len(w.q)-1]
	}
	w.q = append(w.q, winSample{now, v})
	w.prune(now, span)
}

func (w *winMax) prune(now Time, span Duration) {
	cut := 0
	for cut < len(w.q) && now.Sub(w.q[cut].at) > span {
		cut++
	}
	if cut > 0 {
		w.q = append(w.q[:0], w.q[cut:]...)
	}
}

func (w *winMax) get(now Time, span Duration) (float64, bool) {
	w.prune(now, span)
	if len(w.q) == 0 {
		return 0, false
	}
	return w.q[0].v, true
}
