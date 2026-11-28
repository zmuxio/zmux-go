package core

import (
	"math"
	"slices"
)

// Multipath scheduling policies: which stream data a pipe takes when it
// can write (late binding), within the multipath sender rules.

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
	p         *Pipe
	g         *gate // &gb, or nil when no pipe is usable
	fastest   *Pipe // the eligible pipe of p's tier with the smallest ETA for a chunk
	blockedBy *Pipe // the preferred open path, when a lower tier is held
	lv        *levelStats
	gb        gate
	tier      int
	multiTier bool
	open      bool
	usable    bool
	takes     bool
	probe     bool // a diagnostic decision never registers or moves work
}

// newDecision prepares the decision for pipe p now.
func (s *Session) newDecision(d *decision, p *Pipe) {
	*d = decision{p: p, lv: d.lv, gb: d.gb}
	d.open = s.open(p)
	d.g = s.buildGate(&d.gb)
	if d.g == nil {
		return
	}
	d.multiTier = len(d.g.tiers) > 1
	d.usable = d.g.usable(s, p)
	d.takes, d.blockedBy = s.tierRequirement(d.g, p)
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

// Policy returns the session's scheduling policy.
func (s *Session) Policy() Policy { return s.cfg.Policy }

// StreamPolicy returns the policy a stream direction set for itself; own is
// false when it follows the session's.
func (s *Session) StreamPolicy(st *Stream) (p Policy, own bool) {
	if st.pol == 0 {
		return 0, false
	}
	return Policy(st.pol - 1), true
}

// onPipe reports whether the next chunk of st may go on the decision's pipe.
func (s *Session) onPipe(d *decision, st *Stream) bool {
	if !d.open {
		return false
	}
	p := d.p
	sp := &st.snd
	if d.g == nil {
		// No pipe is usable: only a Hello's first batch, outside this path, goes.
		return false
	}
	if m := sp.mps; m != nil && (m.resend.len() > 0 || m.finResend) {
		// Reinjected information goes on the eligible pipe of the tier
		// with the earliest arrival, within the reinjection hold.
		if !d.takes {
			return false
		}
		bypass := false
		if m.resend.len() > 0 {
			r := m.resend.first()
			if m.repair.source != nil && m.repair.covers(r.lo) {
				target, _, eligible := s.prefixTarget(d.g, m.repair.source, m.repair.anchor, min64(r.hi, m.repair.hi)-r.lo)
				switch {
				case target != nil:
					return s.chosen(d, target)
				case eligible:
					_, native := s.planNativeResend(st)
					bypass = !native
				default:
					if !d.probe {
						s.retryPrefixTail(st)
					}
				}
			}
		}
		if !bypass && (m.resend.len() > 0 || m.finResend) {
			return d.fastest == p && s.reinjectRoom(p)
		}
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
		return d.usable && s.chosen(d, s.affinityTarget(d, st))
	case PolicyCustom:
		return s.customOK(d, st)
	case PolicySpreadETA:
		if !d.takes {
			return false
		}
		return s.chosen(d, s.spreadETATarget(d, st))
	case PolicySpreadRoundRobin, PolicySpreadRateWeighted:
		return d.takes && s.chosen(d, s.rrPick(d.g, d.tier, st))
	case PolicySessionAffinity:
		return d.takes && s.chosen(d, d.g.tiers[d.tier].active)
	}
	return false
}

// chosen reports whether the policy chose p; when it chose another pipe,
// that pipe's writer is woken to take the chunk.
func (s *Session) chosen(d *decision, q *Pipe) bool {
	if q == d.p {
		return true
	}
	if !d.probe && q != nil && q.out != nil {
		if ol := q.out.lane(0); ol != nil && ol.writing {
			ol.placementWaiting = true
		} else {
			s.wakePipe(q)
		}
	}
	return false
}

// chunkSent runs after new stream data of st, [off, off+n), went on p.
func (s *Session) chunkSent(d *decision, w *recWriter, st *Stream, off, n uint64, fin bool) {
	p := d.p
	s.replicaBytes(streamApplicationBytes(st, off, n), false)
	switch s.policyOf(st) {
	case PolicyAffinity:
		s.bindOnSend(d, st)
	case PolicySpreadRoundRobin, PolicySpreadRateWeighted:
		if d.g != nil {
			s.rrSent(d.g, d.tier, p, st, off, n)
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

func streamApplicationBytes(st *Stream, off, n uint64) uint64 {
	return n - min(n, st.snd.preLen-min(st.snd.preLen, off))
}

// ---- estimates ----

// rateTops are the two largest measured rates of the usable pipes of one
// priority and standby class, with the pipe of the largest.
type rateTops struct {
	p1     *Pipe
	r1, r2 float64
}

// fastestOf works out the rateTops of priority pr, by standby flag, once
// for a caller that needs the placement rate of many pipes.
func (s *Session) fastestOf(pr uint64) (t [2]rateTops) {
	for _, q := range s.pipes {
		if q.mp == nil || q.out == nil || !s.pipeUsable(q, false) {
			continue
		}
		e := &q.out.snd.re
		if !e.measured || q.mp.quality != nil && !q.mp.quality.freshRate(s.now) {
			continue
		}
		qp, qs := q.mp.effPrio()
		if qp != pr {
			continue
		}
		c := &t[b2i(qs)]
		switch r := s.recentRate(q); {
		case c.p1 == nil || r > c.r1:
			if c.p1 != nil {
				c.r2 = c.r1
			}
			c.p1, c.r1 = q, r
		case r > c.r2:
			c.r2 = r
		}
	}
	return t
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// placementRate uses recent service. Until a usable pipe has a bottleneck
// sample it can borrow a fresh rate from its class for four round trips,
// bounded by the rate observation window.
func (s *Session) placementRate(p *Pipe, tops *[2]rateTops) float64 {
	e := &p.out.snd.re
	own := s.recentRate(p)
	a := p.mp
	q := a.quality
	if q == nil || e.measured || a.penaltyEnd > 0 || q.placed && (e.round >= q.placedFrom+placementRounds || s.now.Sub(q.placedAt) >= rateWindow) ||
		!s.pipeUsable(p, false) {
		return own
	}
	_, sb := a.effPrio()
	t := &tops[b2i(sb)]
	fastest := t.r1 // the fastest other pipe of p's class
	if t.p1 == p {
		fastest = t.r2
	}
	if fastest <= own {
		return own
	}
	if !q.placed {
		q.placed, q.placedFrom, q.placedAt = true, e.round, s.now
	}
	return fastest
}

// recentRate is the delivery estimate for placement, not the peak used by
// the carrier limiter. The writer's queue already limits unmeasured paths.
func (s *Session) recentRate(p *Pipe) float64 {
	e := &p.out.snd.re
	if q := p.mp.quality; q != nil {
		return max(q.currentRate(e, s.now), 1)
	}
	return max(e.value(), 1)
}

// pathEstimate is a value snapshot taken under the session lock. All
// placement and repair costs at a decision use these same quality inputs.
type pathEstimate struct {
	rtt, deviation Duration
	rate, arrival  float64 // service in bytes/s and arrival overhead in seconds
}

func (e pathEstimate) eta(n uint64) float64 { return e.arrival + float64(n)/e.rate }

// estimate separates propagation from delay already counted by the queue.
func (s *Session) estimate(p *Pipe, tops *[2]rateTops, queued float64, staged uint64) pathEstimate {
	a := p.mp
	r := s.placementRate(p, tops)
	q := a.quality
	rtt := a.currentRTT(s.now)
	forward := float64(rtt) / 2e9
	var m Duration
	var floorKnown bool
	if q != nil {
		m, floorKnown = q.floor(s.now)
		ps := &p.out.snd
		if floorKnown && ps.re.measured && q.freshRate(s.now) {
			// Bytes exceeding one propagation/ACK flight are evidence of
			// a hidden carrier backlog. Use the larger queue observation.
			outstanding := ps.so - min64(ps.so, ps.cum)
			inFlight := float64(outstanding - min64(outstanding, staged))
			flight := r * float64(minD(rtt, m)+q.hold) / 1e9
			queued = max(queued, inFlight-flight)
		}
	}
	queue := queued / r
	switch {
	case floorKnown:
		// Recent RTT follows the ACK estimator's adjustment. A known
		// propagation floor separates queueing from that RTT.
		floor := float64(minD(rtt, m)) / 2e9
		forward = max(floor+queue, forward)
	case q != nil && q.freshRTT(s.now):
		// With no fresh floor the sample may already include the queue.
		forward = max(forward, queue)
	default:
		forward += queue
	}
	if a.unacked {
		overdue := s.now.Sub(a.tref()) - s.pto(a) - s.queueDelay(a)
		forward = max(forward, float64(rtt)/2e9+float64(overdue)/1e9)
	}
	return pathEstimate{rtt: rtt, deviation: a.rtt.rttvar, rate: r, arrival: forward}
}

// gateQuality gives each usable pipe one estimate for this gate, shared
// by selection, tier backlog, reordering and repair. Each writer and timer
// owns its gate scratch space.
func (s *Session) gateQuality(g *gate) {
	if len(g.quality) == len(g.pipes) {
		return
	}
	for i, tier := range g.tiers {
		tops := s.fastestOf(tier.prio)
		for _, p := range g.tierPipes(i) {
			g.quality = append(g.quality, s.estimate(p, &tops, s.queued(p), 0))
		}
	}
}

func (g *gate) pipeQuality(p *Pipe, ti int) pathEstimate {
	t := g.tiers[ti]
	for i := t.lo; i < t.hi; i++ {
		if g.pipes[i] == p {
			return g.quality[i]
		}
	}
	return pathEstimate{}
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
	t := g.tiers[ti]
	if t.hi-t.lo == 1 {
		if p := g.pipes[t.lo]; p != except {
			return p, nil
		}
		return nil, nil
	}
	var a, b *Pipe
	var ea, eb float64
	s.gateQuality(g)
	for i := t.lo; i < t.hi; i++ {
		q := g.pipes[i]
		if q == except {
			continue
		}
		e := g.quality[i].eta(l)
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
	incr    [8][]uint64 // ready data of its incremental directions, sorted
	prefix  [8][]uint64 // prefix sums of incr
	total   [8]uint64   // ready data of the level
	nonIncr [8]uint64   // ready data of its non-incremental directions
	valid   bool
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
		slices.Sort(x)
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

// spreadETATarget chooses the current pipe if its chunk fits the ordered
// completion and receive-credit budgets.
// Otherwise it returns the fastest pipe for this chunk, which may be smaller
// than the nominal SpreadChunk.
func (s *Session) spreadETATarget(d *decision, st *Stream) *Pipe {
	g, p := d.g, d.p
	ready := s.readyBytes(st)
	whole := uint64(s.cfg.SpreadChunk)
	chunk := min64(max64(ready, 1), whole)
	f := d.fastest
	if chunk != whole {
		f = s.fastestIn(g, d.tier, chunk, nil)
	}
	if f == nil || f == p {
		return f
	}
	qf := g.pipeQuality(f, d.tier)
	etaP := g.pipeQuality(p, d.tier).eta(chunk)
	if etaP <= qf.eta(chunk) {
		return p
	}
	if ready <= chunk {
		return f
	}
	// ECF with the backlog f carries before this direction's last ready byte.
	u := st.urgency & 7
	lv := s.levels(d)
	var more uint64
	for v := range u {
		more += lv.total[v]
	}
	room := s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent)
	var backlog uint64
	if u <= 2 {
		backlog = min64(room, more)
	} else {
		level := lv.total[u]
		if st.incr {
			level = lv.nonIncr[u] + lv.cappedSum(u, ready)
		}
		backlog = min64(room, more+level)
		backlog -= min64(backlog, chunk)
	}
	// An ordered receiver cannot deliver past this chunk until it arrives.
	// Compare with the faster pipe's serial finish time. Its completion
	// margin is limited by the serialization of the stream's own ready
	// tail: an isolated chunk cannot trade delivery time for parallelism.
	finish := qf.eta(backlog + chunk)
	tail := min64(backlog-min64(backlog, more), ready-min64(ready, chunk))
	margin := min(ecfBeta*finish, float64(tail)/qf.rate)
	if etaP > finish+margin {
		return f
	}
	// BLEST: what f sends while the chunk travels must fit the credit.
	need := qf.rate * max(0, etaP-qf.eta(chunk))
	w := min64(room, s.reorderBudget(g, chunk))
	if need > float64(w) {
		return f
	}
	n := 1
	if st.incr {
		n = max(1, len(lv.incr[u]))
	}
	sroom := st.snd.maxData - min64(st.snd.maxData, st.snd.sent)
	if need/float64(n) > float64(sroom) {
		return f
	}
	return p
}

// reorderBudget bounds the stream data spread ahead of the slowest usable
// pipe. The same arrival estimates include variation in serialization,
// so similar RTTs cannot hide the reordering time of unequal rates.
func (s *Session) reorderBudget(g *gate, chunk uint64) uint64 {
	room := s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent)
	var sum float64
	var lo, hi float64
	first := true
	for _, q := range g.quality {
		sum += q.rate
		o := q.eta(chunk)
		if first || o < lo {
			lo = o
		}
		if first || o > hi {
			hi = o
		}
		first = false
	}
	b := 2*sum*(hi-lo) + float64(s.cfg.SpreadChunk)
	if b >= float64(math.MaxUint64) {
		return room
	}
	return min64(room, uint64(b))
}

// ---- weighted round-robin ----

// rrWeights calls f with each candidate of tier ti and its weight.
func (s *Session) rrWeights(g *gate, ti int, byRate bool, f func(p *Pipe, w float64)) {
	var tops [2]rateTops
	if byRate {
		tops = s.fastestOf(g.tiers[ti].prio)
	}
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
			w *= max(s.placementRate(q, &tops), 1)
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

// rrFill is the round-robin chunk being filled: its pipe and the bytes so far.
type rrFill struct {
	pipe uint64
	n    uint64
}

// rrFillOf is the round-robin chunk being filled for st.
func (s *Session) rrFillOf(st *Stream) *rrFill {
	if st.pol != 0 {
		return &st.snd.mpState().rrFill
	}
	return &s.mp.rrFill
}

// chunkRoom is the most new data of st one write on the decision's pipe
// takes: a chunk, or under a round-robin policy what is left of the chunk
// the pipe is filling, so that a turn ends at its chunk's end.
func (s *Session) chunkRoom(d *decision, st *Stream) uint64 {
	c := max64(uint64(s.cfg.SpreadChunk), 1)
	if d == nil || d.g == nil {
		return c
	}
	switch s.policyOf(st) {
	case PolicySpreadRoundRobin, PolicySpreadRateWeighted:
		if f := s.rrFillOf(st); f.pipe == d.p.id && f.n < c {
			return c - f.n
		}
	case PolicyAffinity, PolicySessionAffinity, PolicySpreadETA, PolicyCustom:
		// Every write is a whole decision.
	}
	return c
}

// rrSent accounts n bytes of new data of st, from off, written on p under a
// round-robin policy. A chunk is SpreadChunk bytes, or less when the write
// took all the data ready: a write that a limit cut short (stream or session
// credit, record room) while more data waits does not end the pipe's turn,
// so a pipe whose turns meet a trickle of credit does not give up the turn
// after a few bytes while another fills whole chunks. A write on another
// pipe (the turn's pipe stopped being a candidate) starts a chunk there.
func (s *Session) rrSent(g *gate, ti int, p *Pipe, st *Stream, off, n uint64) {
	if n == 0 {
		return // a FIN alone fills nothing
	}
	chunk := max64(uint64(s.cfg.SpreadChunk), 1)
	f := s.rrFillOf(st)
	b := n
	if f.pipe == p.id {
		b += f.n
	}
	for ; b >= chunk; b -= chunk {
		s.rrCommit(g, ti, p, st)
	}
	if b > 0 && off+n >= st.snd.sendEnd() {
		// The application, not a limit, made the write short.
		s.rrCommit(g, ti, p, st)
		b = 0
	}
	*f = rrFill{p.id, b}
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
	if !own {
		for _, q := range s.pipes {
			if q.mp != nil && !g.admits(s, q) {
				q.mp.rrCur = 0
			}
		}
		return
	}
	// A value of a pipe not eligible (or gone) is dropped: absent is 0, so
	// the list holds only the gate's pipes however many came and went.
	m := st.snd.mpState()
	kept := m.rr[:0]
	for _, e := range m.rr {
		if q := s.pipe(e.pipe); q != nil && q.mp != nil && g.admits(s, q) {
			kept = append(kept, e)
		}
	}
	clear(m.rr[len(kept):])
	m.rr = kept
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
	if af := &s.mp.aff; af.loadValid {
		a := af.load[p.id]
		if t, ok := affinityTargetID(except); ok && t == p.id {
			a -= min64(a, s.expectedBytes(except))
		}
		return a
	}
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

// affinityTargetID is the pipe a bound stream's load counts on (its move
// target while moving).
func affinityTargetID(st *Stream) (uint64, bool) {
	if st == nil || st.snd.mps == nil || !st.snd.mps.bind.set {
		return 0, false
	}
	b := st.snd.mps.bind
	if b.moving {
		return b.moveTo, true
	}
	return b.pipe, true
}

func (s *Session) affinityIdle(st *Stream) bool {
	sp := &st.snd
	m := sp.mps
	if m == nil {
		return true
	}
	return m.acked.prefix() >= sp.sent && m.resend.len() == 0
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
	base := s.owdBase()
	for _, q := range within {
		score := float64(owdFrom(q.mp, base)) / 1e9
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
	if m.bind.set {
		s.invalidateDeliveryBinding(m.bind.pipe)
	}
	sampled := d.g != nil && s.allSampled(d.g)
	m.bind = binding{pipe: p.id, set: true, provisional: !sampled, epoch: s.mp.aff.epoch}
	if !sampled {
		s.mp.aff.provisional = true
	}
}

// affinityState is the re-evaluation state of affinity bindings.
type affinityState struct {
	// load is each pipe's affinity load while refreshBindings runs (valid):
	// computed once per refresh and kept up to date as bindings move, so a
	// refresh costs O(streams x pipes), not O(streams^2 x pipes).
	load        map[uint64]uint64
	seen        []uint64
	epoch       uint64
	loadValid   bool
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
	if af.load == nil {
		af.load = map[uint64]uint64{}
	}
	clear(af.load)
	it := s.sched.iter()
	for st := it.next(); st != nil; st = it.next() {
		if t, ok := affinityTargetID(st); ok {
			af.load[t] += s.expectedBytes(st)
		}
	}
	af.loadValid = true
	defer func() { af.loadValid = false }()
	it = s.sched.iter()
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
			e := s.expectedBytes(st)
			af.load[b.pipe] -= min64(af.load[b.pipe], e)
			s.invalidateDeliveryBinding(b.pipe)
			b.moving, b.moveTo = true, target.id
			af.load[target.id] += e
		}
	}
}

func containsU64(x []uint64, v uint64) bool {
	return slices.Contains(x, v)
}

// ---- custom policies ----

// customOK applies a custom answer. Without one, the decision is requested
// from the driver, which asks the policy outside every lock; the direction
// waits for it up to pickWait, then takes the affinity choice for the
// chunk (a miss, counted).
func (s *Session) customOK(d *decision, st *Stream) bool {
	if d.probe && st.snd.mps == nil {
		return false
	}
	m := st.snd.mpState()
	mp := s.mp
	a := m.pick
	if a.set && a.gen == mp.pickGen {
		if !a.wait {
			q := s.pipe(a.pipe)
			return q != nil && s.pipeTakesAny(q) && s.chosen(d, q)
		}
		if a.epoch == mp.inputEpoch {
			return false
		}
	}
	if !a.asked || a.gen != mp.pickGen {
		if d.probe {
			return false
		}
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
	if !d.probe {
		s.stats.SchedulerFallbacks++
	}
	return d.usable && s.chosen(d, s.affinityTarget(d, st))
}

// pickWait is how long a direction waits for the policy's answer.
const pickWait = Millisecond

// pipeTakesAny reports whether a pipe may carry new data at all (a custom
// policy may override the tiers, never the rule that only an
// active, unfailed outbound direction carries data).
func (s *Session) pipeTakesAny(p *Pipe) bool {
	return p.mp.out == OutActive && p.out != nil && !p.out.failed
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
	SRTT      Duration
	RTTVar    Duration
	Rate      float64 // bytes per second
	Unsent    uint64
	InFlight  uint64
	Priority  uint64
	Weight    uint64
	State     OutState
	Writable  bool
	Standby   bool
	Saturated bool
	Probation bool
	Eligible  bool
	Shaping   bool   // a Shaper is active on the pipe
	ShapingW  uint32 // the Shaper's weight; 0: none
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
	Frames []PickFrame
	Pipes  []PickPipe
	keys   []*Stream
	Gen    uint64
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
	g := s.buildGate(&mp.gateBuf)
	for _, p := range s.pipes {
		if p.mp == nil || p.out == nil || p.mp.hs || !p.mp.liveOut() {
			continue
		}
		pr, sb := p.mp.effPrio()
		pp := PickPipe{Pipe: p.id, State: p.mp.out, SRTT: p.mp.rtt.smoothed, RTTVar: p.mp.rtt.rttvar,
			Rate: s.rateOf(p), Unsent: s.queuedOn(p), Writable: s.open(p), Priority: pr, Standby: sb,
			Probation: p.mp.prob.on, Weight: p.mp.weight}
		pp.InFlight = p.out.snd.so - min64(p.out.snd.so, p.out.snd.cum)
		if s.shp != nil && p.sh != nil && p.sh.sh != nil {
			pp.Shaping, pp.ShapingW = true, p.sh.weight
		}
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
		if st.discarded {
			continue
		}
		// A question nothing waits for any more (the fallback carried
		// the data) is withdrawn, as is one about a direction that cannot
		// be named yet: its next data asks the policy again instead of
		// staying a miss. The policy names a direction by its identifier,
		// so one not opened yet is pinned now (PinID), as its next chunk
		// would be.
		if !st.sendable() {
			st.snd.mps.pick = pickAnswer{}
			continue
		}
		if _, err := s.PinID(st); err != nil {
			st.snd.mps.pick = pickAnswer{}
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
			s.stats.SchedulerFallbacks++
			m.pick = pickAnswer{asked: true, gen: mp.pickGen} // asked long ago: a miss
			continue
		}
		a := answers[i]
		switch {
		case a.Wait: // not counted: only answers that name a pipe are
			m.pick = pickAnswer{set: true, wait: true, gen: req.Gen, epoch: mp.inputEpoch, asked: true}
		case a.Index < 0 || a.Index >= len(req.Pipes):
			s.stats.SchedulerFallbacks++
			m.pick = pickAnswer{asked: true, gen: mp.pickGen}
		default:
			s.stats.SchedulerAnswers++
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
		s.mp.rrFill = rrFill{}
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
		m.bind, m.rr, m.rrFill, m.pick = binding{}, nil, rrFill{}, pickAnswer{}
	}
	s.wakeAllWriters()
}

// Redundant returns the stream's own redundancy setting; nil follows the
// session's.
func (s *Session) Redundant(st *Stream) *bool {
	switch {
	case st.redund > 0:
		on := true
		return &on
	case st.redund < 0:
		on := false
		return &on
	}
	return nil
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
