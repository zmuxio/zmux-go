package core

import "math"

// Multipath scheduling policies: which stream data a pipe takes when it
// can write (late binding), within the sender rules of MULTIPATH.md 11.

const (
	ecfBeta         = 0.25
	smallStream     = 256 << 10
	placementRounds = 4
	saturationEvery = Second
)

// decision is what every stream's choice at one write opportunity of a
// pipe shares; the gate is rebuilt per chunk only where it can change
// (several tiers).
type decision struct {
	p            *Pipe
	g            *gate
	fastest      *Pipe // the eligible pipe of p's tier with the smallest ETA for a chunk
	lv           *levelStats
	tier         int
	multiTier    bool
	open         bool
	usable       bool
	takes        bool
	joiningTakes bool
}

// newDecision prepares the decision for pipe p now.
func (s *Session) newDecision(d *decision, p *Pipe) {
	*d = decision{p: p, lv: d.lv}
	d.open = s.open(p)
	d.g = s.buildGate()
	d.joiningTakes = p.mp.out == outJoining
	if d.g == nil {
		return
	}
	d.multiTier = len(d.g.tiers) > 1
	d.usable = d.g.usable(s, p)
	d.takes = s.takes(d.g, p)
	d.tier = d.g.tierOf(p)
	if d.tier >= 0 {
		d.fastest = s.fastestIn(d.g, d.tier, uint64(s.cfg.SpreadChunk), nil)
	}
}

// renew updates the decision after a chunk: only a multi-tier gate
// depends on the data the chunk took.
func (s *Session) renewDecision(d *decision) {
	if d.lv != nil {
		d.lv.valid = false
	}
	if d.multiTier {
		s.newDecision(d, d.p)
	}
}

// policyOf is the policy scheduling a stream direction.
func (s *Session) policyOf(st *Stream) Policy {
	if st.pol != 0 {
		return Policy(st.pol - 1)
	}
	return s.cfg.Policy
}

// onPipe reports whether the next chunk of st may go on the decision's pipe.
func (s *Session) onPipe(d *decision, st *Stream) bool {
	if !d.open {
		return false
	}
	p := d.p
	sp := &st.snd
	if d.g == nil {
		// No pipe is usable: a JOINING pipe carries stream data
		// (MULTIPATH.md 11 rule S1).
		return d.joiningTakes
	}
	if m := sp.mps; m != nil && (len(m.resend) > 0 || m.finResend) {
		// Reinjected information goes on the eligible pipe of the tier
		// with the earliest arrival (rule S3), within the reinjection hold.
		return d.takes && d.fastest == p && s.reinjectRoom(p)
	}
	if d.takes && s.redundant(st) {
		l := min64(max64(s.readyBytes(st), 1), uint64(s.cfg.SpreadChunk))
		a, b := s.earliestTwo(d.g, d.tier, l, nil)
		if b != nil && (a == p || b == p) {
			return true
		}
	}
	switch s.policyOf(st) {
	case PolicyAffinity:
		return d.usable && s.chosen(p, s.affinityTarget(d, st))
	case PolicyCustom:
		return s.customOK(d, st)
	case PolicySpreadETA:
		if !d.takes {
			return false
		}
		if s.spreadETAOK(d, st) {
			return true
		}
		s.chosen(p, d.fastest)
		return false
	case PolicySpreadRoundRobin, PolicySpreadRateWeighted:
		return d.takes && s.chosen(p, s.rrPick(d.g, d.tier, st))
	case PolicySessionAffinity:
		return d.takes && s.chosen(p, d.g.tiers[d.tier].active)
	}
	return false
}

// chosen reports whether the policy chose p; when it chose another pipe,
// that pipe's writer is woken to take the chunk.
func (s *Session) chosen(p, q *Pipe) bool {
	if q == p {
		return true
	}
	if q != nil && q.out != nil {
		s.wakePipe(q)
	}
	return false
}

// chunkSent runs after new stream data of st, [off, off+n), went on p.
func (s *Session) chunkSent(d *decision, w *recWriter, st *Stream, off, n uint64, fin bool) {
	p := d.p
	switch s.policyOf(st) {
	case PolicyAffinity:
		s.bindOnSend(d, st)
	case PolicySpreadRoundRobin, PolicySpreadRateWeighted:
		if d.g != nil {
			s.rrCommit(d.g, d.tier, p, st)
		}
	case PolicyCustom:
		s.customUsed(st, n)
	case PolicySessionAffinity, PolicySpreadETA:
		// No per-chunk state.
	}
	if d.g != nil {
		s.copyChunk(d, w, st, off, n, fin)
	}
}

// ---- estimates ----

// placementRate is the rate the spreading policies place data with: R_p, or for
// a usable pipe without a bottleneck sample the fastest measured pipe of its
// tier and standby flag, for four of its round trips.
func (s *Session) placementRate(p *Pipe) float64 {
	e := &p.out.snd.re
	own := e.value()
	a := p.mp
	if e.measured || a.placedSet && e.round >= a.placedFrom+placementRounds || !s.pipeUsable(p, false) {
		return own
	}
	pr, sb := a.effPrio()
	fastest := 0.0
	for _, q := range s.pipes {
		if q == p || q.mp == nil || q.out == nil || !q.out.snd.re.measured || !s.pipeUsable(q, false) {
			continue
		}
		if qp, qs := q.mp.effPrio(); qp == pr && qs == sb {
			fastest = max(fastest, q.out.snd.re.value())
		}
	}
	if fastest <= own {
		return own
	}
	if !a.placedSet {
		a.placedSet, a.placedFrom = true, e.round
	}
	return fastest
}

// eta is ETA_p(L) in seconds.
func (s *Session) eta(p *Pipe, l uint64) float64 {
	r := max(s.placementRate(p), 1)
	return (s.queued(p)+float64(l))/r + float64(s.owd(p))/1e9
}

// fastestIn returns the pipe of tier ti with the smallest ETA for l bytes,
// except one pipe.
func (s *Session) fastestIn(g *gate, ti int, l uint64, except *Pipe) *Pipe {
	a, _ := s.earliestTwo(g, ti, l, except)
	return a
}

// earliestTwo returns the two pipes of tier ti with the smallest ETA for l
// bytes, earliest first (ties to the lower ID).
func (s *Session) earliestTwo(g *gate, ti int, l uint64, except *Pipe) (*Pipe, *Pipe) {
	if ti < 0 || ti >= len(g.tiers) {
		return nil, nil
	}
	var a, b *Pipe
	var ea, eb float64
	for _, q := range g.tierPipes(ti) {
		if q == except {
			continue
		}
		e := s.eta(q, l)
		switch {
		case a == nil || e < ea || e == ea && q.id < a.id:
			b, eb = a, ea
			a, ea = q, e
		case b == nil || e < eb || e == eb && q.id < b.id:
			b, eb = q, e
		}
	}
	return a, b
}

// ---- spread-eta ----

// levelStats are the ready amounts of the stream directions by urgency
// level, for ECF's backlog and BLEST's share.
type levelStats struct {
	valid   bool
	total   [8]uint64   // ready data of the level
	nonIncr [8]uint64   // ready data of its non-incremental directions
	incr    [8][]uint64 // ready data of its incremental directions, sorted
	prefix  [8][]uint64 // prefix sums of incr
}

func (s *Session) levels(d *decision) *levelStats {
	if d.lv == nil {
		d.lv = &levelStats{}
	}
	lv := d.lv
	if lv.valid {
		return lv
	}
	for u := range lv.total {
		lv.total[u], lv.nonIncr[u] = 0, 0
		lv.incr[u] = lv.incr[u][:0]
	}
	it := s.sched.iter()
	for st := it.next(); st != nil; st = it.next() {
		r := s.readyBytes(st)
		if r == 0 {
			continue
		}
		u := st.urgency & 7
		lv.total[u] += r
		if st.incr {
			lv.incr[u] = append(lv.incr[u], r)
		} else {
			lv.nonIncr[u] += r
		}
	}
	for u := range lv.incr {
		x := lv.incr[u]
		sortU64(x)
		ps := lv.prefix[u][:0]
		var sum uint64
		for _, v := range x {
			sum += v
			ps = append(ps, sum)
		}
		lv.prefix[u] = ps
	}
	lv.valid = true
	return lv
}

func sortU64(x []uint64) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j] < x[j-1]; j-- {
			x[j], x[j-1] = x[j-1], x[j]
		}
	}
}

// cappedSum is the sum over the level's incremental directions of
// min(ready_j, r).
func (lv *levelStats) cappedSum(u uint8, r uint64) uint64 {
	x, ps := lv.incr[u], lv.prefix[u]
	lo, hi := 0, len(x)
	for lo < hi {
		m := (lo + hi) / 2
		if x[m] <= r {
			lo = m + 1
		} else {
			hi = m
		}
	}
	sum := uint64(0)
	if lo > 0 {
		sum = ps[lo-1]
	}
	return sum + uint64(len(x)-lo)*r
}

func (s *Session) spreadETAOK(d *decision, st *Stream) bool {
	g, p := d.g, d.p
	ready := s.readyBytes(st)
	whole := uint64(s.cfg.SpreadChunk)
	chunk := min64(max64(ready, 1), whole)
	f := d.fastest
	if chunk != whole {
		f = s.fastestIn(g, d.tier, chunk, nil)
	}
	if f == nil {
		return false
	}
	if f == p {
		return true
	}
	etaP := s.eta(p, chunk)
	if etaP <= s.eta(f, chunk) {
		return true
	}
	// ECF with the backlog f carries before this direction's last ready byte.
	u := st.urgency & 7
	lv := s.levels(d)
	var more uint64
	for v := uint8(0); v < u; v++ {
		more += lv.total[v]
	}
	room := s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent)
	var backlog uint64
	beta := ecfBeta
	if u <= 2 {
		backlog = min64(room, more)
		beta = 0
	} else {
		level := lv.total[u]
		if st.incr {
			level = lv.nonIncr[u] + lv.cappedSum(u, ready)
		}
		backlog = min64(room, more+level)
		backlog -= min64(backlog, chunk)
	}
	if etaP > (1+beta)*s.eta(f, backlog+chunk) {
		return false
	}
	// BLEST: what f sends while the chunk travels must fit the credit.
	need := s.rateOf(f) * max(0, etaP-float64(s.owd(f))/1e9)
	w := min64(room, s.reorderBudget())
	if need > float64(w) {
		return false
	}
	n := 1
	if st.incr {
		n = max(1, len(lv.incr[u]))
	}
	sroom := st.snd.maxData - min64(st.snd.maxData, st.snd.sent)
	return need/float64(n) <= float64(sroom)
}

// reorderBudget bounds the stream data spread ahead of the slowest usable
// pipe: twice the summed delivery rate times the one-way-delay spread of
// the usable pipes, plus one chunk, within the connection credit left.
func (s *Session) reorderBudget() uint64 {
	room := s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent)
	var sum float64
	var lo, hi Duration
	first := true
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || p.out == nil || !a.liveOut() {
			continue
		}
		sum += s.rateOf(p)
		if s.pipeUsable(p, true) {
			o := s.owd(p)
			if first || o < lo {
				lo = o
			}
			if first || o > hi {
				hi = o
			}
			first = false
		}
	}
	b := 2*sum*float64(hi-lo)/1e9 + float64(s.cfg.SpreadChunk)
	if b >= float64(math.MaxUint64) {
		return room
	}
	return min64(room, uint64(b))
}

// ---- weighted round-robin ----

// rrWeights calls f with each candidate of tier ti and its weight.
func (s *Session) rrWeights(g *gate, ti int, byRate bool, f func(p *Pipe, w float64)) {
	allZero := true
	for _, q := range g.tierPipes(ti) {
		if s.open(q) && q.mp.weight > 0 {
			allZero = false
		}
	}
	for _, q := range g.tierPipes(ti) {
		if !s.open(q) {
			continue
		}
		w := float64(q.mp.weight)
		if allZero {
			w = 1
		}
		if w == 0 {
			continue
		}
		if byRate {
			w *= max(s.placementRate(q), 1)
		}
		f(q, w)
	}
}

func (s *Session) rrCurrent(st *Stream, own bool, p *Pipe) float64 {
	if !own {
		return p.mp.rrCur
	}
	if m := st.snd.mps; m != nil {
		for _, e := range m.rr {
			if e.pipe == p.id {
				return e.v
			}
		}
	}
	return 0
}

func (s *Session) rrPick(g *gate, ti int, st *Stream) *Pipe {
	own := st.pol != 0
	byRate := s.policyOf(st) == PolicySpreadRateWeighted
	var best *Pipe
	var bv float64
	s.rrWeights(g, ti, byRate, func(q *Pipe, w float64) {
		v := s.rrCurrent(st, own, q) + w
		if best == nil || v > bv || v == bv && q.id < best.id {
			best, bv = q, v
		}
	})
	return best
}

// rrCommit applies a chunk given to p: every candidate gains its weight,
// the chosen one gives up the sum; pipes that are not eligible start from
// 0 when they are again.
func (s *Session) rrCommit(g *gate, ti int, p *Pipe, st *Stream) {
	own := st.pol != 0
	byRate := s.policyOf(st) == PolicySpreadRateWeighted
	var sum float64
	s.rrWeights(g, ti, byRate, func(_ *Pipe, w float64) { sum += w })
	add := func(q *Pipe, dv float64) {
		if !own {
			q.mp.rrCur += dv
			return
		}
		m := st.snd.mpState()
		for i := range m.rr {
			if m.rr[i].pipe == q.id {
				m.rr[i].v += dv
				return
			}
		}
		m.rr = append(m.rr, rrVal{q.id, dv})
	}
	s.rrWeights(g, ti, byRate, func(q *Pipe, w float64) {
		if q == p {
			add(q, w-sum)
		} else {
			add(q, w)
		}
	})
	for _, q := range s.pipes {
		if q.mp == nil || g.admits(s, q) {
			continue
		}
		if !own {
			q.mp.rrCur = 0
		} else if m := st.snd.mps; m != nil {
			for i := range m.rr {
				if m.rr[i].pipe == q.id {
					m.rr[i].v = 0
				}
			}
		}
	}
}

type rrVal struct {
	pipe uint64
	v    float64
}

// ---- affinity ----

// binding is the pipe an affinity stream direction is bound to.
type binding struct {
	pipe        uint64
	epoch       uint64
	moveTo      uint64
	set         bool
	provisional bool
	moving      bool
}

// expectedBytes is E of a stream direction.
func (s *Session) expectedBytes(st *Stream) uint64 {
	e := st.unsent()
	if st.snd.wwait {
		lim := s.sessionSendBuffer()
		if st.snd.bufLimit > 0 {
			lim = st.snd.bufLimit
		}
		e = max64(e, lim)
	}
	return e
}

// affinityLoad is A_p: the expected bytes of the other directions bound to p.
func (s *Session) affinityLoad(p *Pipe, except *Stream) uint64 {
	var a uint64
	it := s.sched.iter()
	for st := it.next(); st != nil; st = it.next() {
		if st == except || st.snd.mps == nil {
			continue
		}
		b := st.snd.mps.bind
		target := b.pipe
		if b.moving {
			target = b.moveTo
		}
		if b.set && target == p.id {
			a += s.expectedBytes(st)
		}
	}
	return a
}

func (s *Session) affinityIdle(st *Stream) bool {
	sp := &st.snd
	m := sp.mps
	if m == nil {
		return true
	}
	return m.acked.prefix() >= sp.sent && len(m.resend) == 0
}

// affinityTarget is the pipe an affinity direction sends on now.
func (s *Session) affinityTarget(d *decision, st *Stream) *Pipe {
	g := d.g
	works := func(id uint64) *Pipe {
		if q := s.pipe(id); q != nil && g.usable(s, q) {
			return q
		}
		return nil
	}
	var b binding
	if m := st.snd.mps; m != nil {
		b = m.bind
	}
	idle := s.affinityIdle(st)
	if b.set && !idle {
		if q := works(b.pipe); q != nil {
			if b.moving {
				return nil
			}
			return q
		}
	}
	if b.set && b.moving {
		if q := works(b.moveTo); q != nil {
			return q
		}
	}
	return s.bindingChoice(g, st)
}

// passingTier is the most-preferred tier whose binding wait is at most D.
func (s *Session) passingTier(g *gate, st *Stream) int {
	d := float64(s.cfg.OverflowDelay) / 1e9
	for i := range g.tiers {
		var wait, rate float64
		for _, q := range g.tierPipes(i) {
			wait += s.queued(q) + float64(s.affinityLoad(q, st))
			rate += max(s.rateOf(q), 1)
		}
		if wait/rate <= d {
			return i
		}
	}
	return -1
}

// bindingChoice is the pipe an affinity direction binds to.
func (s *Session) bindingChoice(g *gate, st *Stream) *Pipe {
	e := s.expectedBytes(st)
	small := e < smallStream || st.urgency <= 2
	ti := s.passingTier(g, st)
	var within []*Pipe
	switch {
	case ti >= 0:
		within = g.tierPipes(ti)
	case small:
		within = g.tierPipes(0)
	default:
		within = g.pipes
	}
	var best *Pipe
	var bs float64
	for _, q := range within {
		score := float64(s.owd(q)) / 1e9
		if !small {
			load := s.queued(q) + float64(s.affinityLoad(q, st)) + float64(e)
			score += load / max(s.rateOf(q), 1)
		}
		if best == nil || score < bs || score == bs && q.id < best.id {
			best, bs = q, score
		}
	}
	return best
}

// allSampled reports whether every usable pipe has an RTT sample.
func (s *Session) allSampled(g *gate) bool {
	for _, q := range g.pipes {
		if !q.mp.rtt.has {
			return false
		}
	}
	return true
}

// bindOnSend records the pipe an affinity direction sent new data on.
func (s *Session) bindOnSend(d *decision, st *Stream) {
	m := st.snd.mpState()
	p := d.p
	if m.bind.set && m.bind.pipe == p.id && !m.bind.moving {
		return
	}
	sampled := d.g != nil && s.allSampled(d.g)
	m.bind = binding{pipe: p.id, set: true, provisional: !sampled, epoch: s.mp.aff.epoch}
	if !sampled {
		s.mp.aff.provisional = true
	}
}

// affinityState is the re-evaluation state of affinity bindings.
type affinityState struct {
	seen        []uint64
	epoch       uint64
	provisional bool
	stale       bool
}

// refreshBindings re-evaluates affinity bindings once per batch:
// provisional ones once every usable pipe has a sample, ones made before
// a pipe that is usable now became usable, and ones in a less-preferred
// tier when a more-preferred tier has room for them. A direction that
// should move stops new data until its data on the old pipe is
// acknowledged (an idle point the sender creates).
func (s *Session) refreshBindings(g *gate) {
	if g == nil {
		return
	}
	af := &s.mp.aff
	sampled := s.allSampled(g)
	prov := af.provisional && sampled
	if prov {
		af.provisional = false
	}
	grew := false
	for _, q := range g.pipes {
		if !containsU64(af.seen, q.id) {
			grew = true
		}
	}
	af.seen = af.seen[:0]
	for _, q := range g.pipes {
		af.seen = append(af.seen, q.id)
	}
	if grew {
		af.epoch++
		af.stale = true
	}
	stale := sampled && af.stale
	topOnly := true
	for _, q := range s.pipes {
		if q.mp != nil && q.out != nil {
			if pr, _ := q.mp.effPrio(); pr > g.tiers[0].prio {
				topOnly = false
			}
		}
	}
	if !prov && !stale && topOnly {
		return
	}
	if sampled {
		af.stale = false
	}
	it := s.sched.iter()
	for st := it.next(); st != nil; st = it.next() {
		if s.policyOf(st) != PolicyAffinity || st.snd.mps == nil {
			continue
		}
		b := &st.snd.mps.bind
		if !b.set || b.moving {
			continue
		}
		var target *Pipe
		old := b.epoch < af.epoch && sampled
		if prov && b.provisional || old {
			target = s.bindingChoice(g, st)
		} else {
			q := s.pipe(b.pipe)
			if q == nil || q.mp == nil {
				continue
			}
			ti := g.tierOf(q)
			if ti == 0 {
				continue
			}
			if pt := s.passingTier(g, st); pt >= 0 && (ti < 0 || pt < ti) {
				target = s.bindingChoice(g, st)
			}
		}
		b.provisional = false
		if old {
			b.epoch = af.epoch
		}
		if target != nil && target.id != b.pipe {
			b.moving, b.moveTo = true, target.id
		}
	}
}

func containsU64(x []uint64, v uint64) bool {
	for _, y := range x {
		if y == v {
			return true
		}
	}
	return false
}

// ---- custom policies ----

// customOK applies a custom answer. Without one, the decision is requested
// from the driver, which asks the policy outside every lock; the direction
// waits for it up to pickWait, then takes the affinity choice for the
// chunk (a miss, counted).
func (s *Session) customOK(d *decision, st *Stream) bool {
	m := st.snd.mpState()
	mp := s.mp
	a := m.pick
	if a.set && a.gen == mp.pickGen {
		if a.wait {
			if a.epoch == mp.inputEpoch {
				return false
			}
		} else {
			return a.pipe == d.p.id && s.pipeTakesAny(d.p)
		}
	}
	if !a.asked || a.gen != mp.pickGen {
		m.pick = pickAnswer{asked: true, gen: mp.pickGen, askedAt: s.now}
		if !mp.pickTimer || s.now.Add(pickWait) < mp.pickDeadline {
			mp.pickTimer, mp.pickDeadline = true, s.now.Add(pickWait)
		}
		if !m.pickQueued {
			m.pickQueued = true
			mp.pickQ = append(mp.pickQ, st)
		}
		mp.pickWanted = true
		return false
	}
	if s.now.Sub(a.askedAt) < pickWait {
		return false
	}
	mp.pickMisses++
	return d.usable && s.affinityTarget(d, st) == d.p
}

// pickWait is how long a direction waits for the policy's answer.
const pickWait = Millisecond

// pipeTakesAny reports whether a pipe may carry new data at all (a custom
// policy may override the tiers, never rule S1).
func (s *Session) pipeTakesAny(p *Pipe) bool {
	return p.mp.out == outActive && p.out != nil && !p.out.failed
}

func (s *Session) customUsed(st *Stream, n uint64) {
	m := st.snd.mps
	if m == nil || !m.pick.set || m.pick.wait {
		return
	}
	if m.pick.left == 0 {
		return // until invalidated
	}
	if n >= m.pick.left {
		m.pick = pickAnswer{}
		return
	}
	m.pick.left -= n
}

// pickAnswer is the custom answer a stream direction holds.
type pickAnswer struct {
	pipe    uint64
	left    uint64 // 0: until invalidated
	gen     uint64
	epoch   uint64
	askedAt Time
	set     bool
	wait    bool
	asked   bool
}

// PickFrame is a stream direction waiting for a custom decision.
type PickFrame struct {
	Stream       uint64
	Len          uint64 // bytes ready to place
	EligibleTier uint64
	LastPipe     uint64
	HasLastPipe  bool
	HasTier      bool
	Urgency      uint8
}

// PickPipe is the value-only view of a pipe for a custom policy.
type PickPipe struct {
	Pipe      uint64
	State     string
	SRTT      Duration
	RTTVar    Duration
	Rate      uint64
	Unsent    uint64
	InFlight  uint64
	Priority  uint64
	Weight    uint64
	Writable  bool
	Standby   bool
	Saturated bool
	Probation bool
	Eligible  bool
}

// PickAnswer is a policy's answer for one direction: a pipe index of the
// view and the bytes it covers (0: until invalidated), or Wait.
type PickAnswer struct {
	Index int
	Bytes uint64
	Wait  bool
}

// PickRequest is one call of the custom policy.
type PickRequest struct {
	Gen    uint64
	Frames []PickFrame
	Pipes  []PickPipe
	keys   []*Stream
}

// WantsPick reports whether directions wait for a custom decision.
func (s *Session) WantsPick() bool { return s.mp != nil && s.mp.pickWanted && len(s.mp.pickQ) > 0 }

// TakePick fills req with the waiting directions and the pipes.
func (s *Session) TakePick(req *PickRequest) bool {
	mp := s.mp
	req.Frames, req.Pipes, req.keys = req.Frames[:0], req.Pipes[:0], req.keys[:0]
	if mp == nil {
		return false
	}
	mp.pickWanted = false
	req.Gen = mp.pickGen
	g := s.buildGate()
	for _, p := range s.pipes {
		if p.mp == nil || p.out == nil || !p.mp.liveOut() {
			continue
		}
		pr, sb := p.mp.effPrio()
		pp := PickPipe{Pipe: p.id, State: outStateNames[p.mp.out], SRTT: p.mp.rtt.smoothed, RTTVar: p.mp.rtt.rttvar,
			Rate: uint64(s.rateOf(p)), Unsent: p.mp.unsentAhead, Writable: s.open(p), Priority: pr, Standby: sb,
			Probation: p.mp.prob.on, Weight: p.mp.weight}
		pp.InFlight = p.out.snd.so - min64(p.out.snd.so, p.out.snd.cum)
		if g != nil {
			pp.Eligible = g.admits(s, p)
			if ti := g.tierOf(p); ti >= 0 && ti <= g.last {
				pp.Saturated = g.tiers[ti].sat
			}
		}
		req.Pipes = append(req.Pipes, pp)
	}
	for _, st := range mp.pickQ {
		st.snd.mps.pickQueued = false
		if st.discarded || !st.sendable() {
			continue
		}
		f := PickFrame{Stream: st.id, Urgency: st.urgency, Len: s.readyBytes(st)}
		if b := st.snd.mps.bind; b.set {
			f.LastPipe, f.HasLastPipe = b.pipe, true
		}
		if g != nil {
			f.EligibleTier, f.HasTier = g.tiers[0].prio, true
		}
		req.Frames = append(req.Frames, f)
		req.keys = append(req.keys, st)
	}
	clear(mp.pickQ)
	mp.pickQ = mp.pickQ[:0]
	return len(req.Frames) > 0
}

// AnswerPick applies the answers of req when its generation is still
// current; an invalid answer is a miss (the affinity choice applies).
func (s *Session) AnswerPick(req *PickRequest, answers []PickAnswer, failed bool) {
	mp := s.mp
	if mp == nil {
		return
	}
	for i, st := range req.keys {
		if st.discarded || st.snd.mps == nil {
			continue
		}
		m := st.snd.mps
		if failed || req.Gen != mp.pickGen || i >= len(answers) {
			mp.pickMisses++
			m.pick = pickAnswer{asked: true, gen: mp.pickGen} // asked long ago: a miss
			continue
		}
		a := answers[i]
		switch {
		case a.Wait:
			m.pick = pickAnswer{set: true, wait: true, gen: req.Gen, epoch: mp.inputEpoch, asked: true}
		case a.Index < 0 || a.Index >= len(req.Pipes):
			mp.pickMisses++
			m.pick = pickAnswer{asked: true, gen: mp.pickGen}
		default:
			mp.pickApplied++
			m.pick = pickAnswer{set: true, pipe: req.Pipes[a.Index].Pipe, left: a.Bytes, gen: req.Gen, asked: true}
		}
	}
	s.wakeAllWriters()
}

// pickInput marks an input to the session: "wait" answers end.
func (s *Session) pickInput() {
	if s.mp != nil {
		s.mp.inputEpoch++
	}
}

// pickChanged invalidates every answer (a pipe's state or eligibility
// changed).
func (s *Session) pickChanged() {
	if s.mp != nil {
		s.mp.pickGen++
	}
}

// ---- policy changes ----

// SetPolicy changes the session's policy; the previous policy's state is
// discarded and the new one starts from its initial state.
func (s *Session) SetPolicy(p Policy) {
	s.cfg.Policy = p
	if s.mp != nil {
		for _, q := range s.pipes {
			if q.mp != nil {
				q.mp.rrCur = 0
			}
		}
		for _, st := range s.streams {
			if st.pol == 0 && st.snd.mps != nil {
				st.snd.mps.bind = binding{}
				st.snd.mps.pick = pickAnswer{}
			}
		}
	}
	s.wakeAllWriters()
}

// SetStreamPolicy sets a stream direction's own policy; inherit returns
// it to the session's.
func (s *Session) SetStreamPolicy(st *Stream, p Policy, inherit bool) {
	if inherit {
		st.pol = 0
	} else {
		st.pol = uint8(p) + 1
	}
	if m := st.snd.mps; m != nil {
		m.bind, m.rr, m.pick = binding{}, nil, pickAnswer{}
	}
	s.wakeAllWriters()
}

// SetRedundant overrides the session's redundancy for a stream direction;
// nil returns it to the default.
func (s *Session) SetRedundant(st *Stream, on *bool) {
	switch {
	case on == nil:
		st.redund = 0
	case *on:
		st.redund = 1
	default:
		st.redund = -1
	}
	s.wakeAllWriters()
}
