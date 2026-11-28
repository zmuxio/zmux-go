package core

import "math"

// prefixScheduler is allocated on the first tracked STREAM handoff.
type prefixScheduler struct {
	prefixStreams                     []*Stream
	prefixGate                        gate
	prefixBucket                      tokenBucket
	prefixAt                          Time
	prefixBytes, prefixPending        uint64
	prefixWaiting, prefixStateWaiting bool
}

// prefixRepair accounts for one stream's anticipatory copy. Pending bytes
// are in resend and sent bytes are in the ordinary carrier retention log.
type prefixRepair struct {
	source *Pipe
	anchor Time
	lo, hi uint64
	sent   uint64 // end of the portion already passed to a carrier
}

// prefixRef identifies an original in its carrier's authoritative retention
// log. A lane that has emptied can reuse serials, so also check the record end.
type prefixRef struct {
	source *Pipe
	lane   *outLane
	serial uint64
	end    uint64
}

func (ref prefixRef) locate() *retained {
	log := ref.lane.log
	if ref.lane.prefixCount == 0 || len(log) == 0 {
		return nil
	}
	i := ref.serial - log[0].serial
	if i >= uint64(len(log)) || log[i].end != ref.end || log[i].confirm == 0 {
		return nil
	}
	return &log[i]
}

// prefixTrack follows one sending direction's originals in stream order.
// Only the first outstanding original can contain the next missing offset.
type prefixTrack struct {
	refs   []prefixRef
	head   int
	active int // index in prefixStreams, -1 while empty
}

func (s *Session) trackPrefix(st *Stream, source *Pipe, lane *outLane, r *retained) {
	m := st.snd.mps
	if m.prefix == nil {
		m.prefix = &prefixTrack{active: -1}
	}
	t := m.prefix
	if t.active < 0 {
		t.active = len(s.mp.prefixStreams)
		s.mp.prefixStreams = append(s.mp.prefixStreams, st)
		if r.d.off <= m.acked.prefix() {
			s.checkPrefixAt(r.confirm)
			s.mp.prefixWaiting = true
		}
	}
	if t.head > 0 && t.head >= len(t.refs)/2 && len(t.refs) == cap(t.refs) {
		n := copy(t.refs, t.refs[t.head:])
		clear(t.refs[n:])
		t.refs, t.head = t.refs[:n], 0
	}
	t.refs = append(t.refs, prefixRef{source, lane, r.serial, r.end})
}

func (s *Session) untrackPrefix(st *Stream) {
	t := st.snd.mps.prefix
	if t == nil || t.active < 0 {
		return
	}
	list := s.mp.prefixStreams
	last := len(list) - 1
	list[t.active] = list[last]
	list[t.active].snd.mps.prefix.active = t.active
	list[last] = nil
	s.mp.prefixStreams = list[:last]
	clear(t.refs)
	t.refs, t.head, t.active = t.refs[:0], 0, -1
}

func (s *Session) prefixHead(st *Stream, pre uint64, examined *int) (prefixRef, *retained) {
	t := st.snd.mps.prefix
	if t == nil || t.active < 0 {
		return prefixRef{}, nil
	}
	for t.head < len(t.refs) {
		ref := t.refs[t.head]
		r := ref.locate()
		if examined != nil {
			*examined++
		}
		if r != nil && pre < r.d.off+r.d.n {
			return ref, r
		}
		t.refs[t.head] = prefixRef{}
		t.head++
	}
	s.untrackPrefix(st)
	return prefixRef{}, nil
}

func (s *Session) schedulePrefix(st *Stream, pre uint64) {
	_, r := s.prefixHead(st, pre, nil)
	if r != nil && r.d.off <= pre {
		s.checkPrefixAt(maxT(1, s.now))
	}
}

func (r prefixRepair) covers(off uint64) bool { return r.sent <= off && off < r.hi }

func (r prefixRepair) pending(acked *rangeSet) uint64 {
	return r.hi - r.sent - acked.covered(r.sent, r.hi)
}

func (s *Session) clearPrefixRepair(m *mpSend) {
	if m.repair.hi == 0 {
		return
	}
	pending := m.repair.pending(&m.acked)
	s.mp.prefixBytes -= pending + m.prefixCopies.covered(m.repair.lo, m.repair.hi)
	s.mp.prefixPending -= pending
	m.prefixCopies = rangeSet{}
	m.repair = prefixRepair{}
	s.wakePrefix()
}

func (s *Session) cancelPrefixTail(m *mpSend) {
	r := m.repair
	m.resend.remove(r.sent, r.hi)
	s.releasePrefixTail(m)
}

// releasePrefixTail releases only the reservation. Native loss keeps
// those bytes in the ordinary reinjection queue.
func (s *Session) releasePrefixTail(m *mpSend) {
	r := &m.repair
	if r.sent == r.hi {
		return
	}
	pending := r.pending(&m.acked)
	s.mp.prefixPending -= pending
	s.mp.prefixBytes -= pending
	r.hi = r.sent
	s.finishPrefixRepair(m)
	s.wakePrefix()
}

// retryPrefixTail releases a reservation whose alternatives lost eligibility.
// The original keeps its earliest deadline and wakes on new evidence.
func (s *Session) retryPrefixTail(st *Stream) {
	s.cancelPrefixTail(st.snd.mps)
	s.mp.prefixWaiting = true
}

func (s *Session) finishPrefixRepair(m *mpSend) {
	if m.repair != (prefixRepair{}) && m.prefixCopies.len() == 0 && m.repair.pending(&m.acked) == 0 {
		m.repair = prefixRepair{}
		s.wakePrefix()
	}
}

// ackPrefixRepair runs before the ACK range is inserted in m.acked.
func (s *Session) ackPrefixRepair(m *mpSend, lo, hi uint64) {
	r := &m.repair
	if r.hi == 0 {
		return
	}
	lo, hi = max64(lo, r.sent), min64(hi, r.hi)
	if lo < hi {
		newly := hi - lo - m.acked.covered(lo, hi)
		s.mp.prefixPending -= newly
		s.mp.prefixBytes -= newly
		if newly != 0 {
			s.wakePrefix()
		}
	}
}

func (s *Session) releasePrefixCopy(m *mpSend, lo, hi uint64) {
	if n := m.prefixCopies.covered(lo, hi); n != 0 {
		s.mp.prefixBytes -= n
		m.prefixCopies.remove(lo, hi)
		s.wakePrefix()
	}
}

// commitPrefixRepair charges actual encoded STREAM payload. ACKed holes
// between this fragment and the handoff cursor have already freed credit.
func (s *Session) commitPrefixRepair(m *mpSend, off, n uint64) {
	r := &m.repair
	end := off + n
	b := &s.mp.prefixBucket
	b.fill(usOf(s.now))
	b.tokens -= int64(n)
	s.mp.prefixPending -= n
	r.sent = end
	m.prefixCopies.add(off, end)
	m.oppSent.add(off, end)
	s.wakePrefix()
}

// reservePrefixCredit reserves against one chunk of burst and one chunk per
// second. Queued reservations hold credit until commit or cancellation.
func (s *Session) reservePrefixCredit(n uint64) bool {
	m := s.mp
	b := &m.prefixBucket
	now := usOf(s.now)
	if b.rate == 0 {
		b.burst = int64(s.cfg.SpreadChunk)
		b.rate = uint64(b.burst)
		b.reset(now)
	}
	level := m.prefixPending + n
	if level > uint64(b.burst) {
		return false // a pending reservation must first commit or cancel
	}
	b.fill(now)
	if level <= uint64(b.tokens) {
		m.prefixPending = level
		return true
	}
	if us, ok := b.readyAt(int64(level)); ok && us <= usOf(Never) {
		s.checkPrefixAt(Time(us * uint64(Microsecond)))
	}
	return false
}

func (s *Session) wakePrefix() {
	if s.mp != nil && s.mp.clock.activeOut > 1 && s.mp.prefixScheduler != nil && s.mp.prefixWaiting && s.state < StateClosing {
		s.checkPrefixAt(maxT(1, s.now))
		s.mp.prefixWaiting = false
	}
}

func (s *Session) wakePrefixOnPipeState() {
	if s.mp != nil && s.mp.prefixScheduler != nil && s.mp.prefixStateWaiting && s.state < StateClosing {
		s.checkPrefixAt(maxT(1, s.now))
		s.mp.prefixStateWaiting = false
	}
}

// checkPrefixAt keeps one timer for the earliest outstanding check. ACKs
// remove their retained entries; a stale timer therefore does no repair.
func (s *Session) checkPrefixAt(at Time) {
	if at != Never && (s.mp.prefixAt == 0 || at < s.mp.prefixAt) {
		s.mp.prefixAt = at
	}
}

func (s *Session) prefixDeadline(p *Pipe, ahead, handed, length uint64) Time {
	pr, _ := p.mp.effPrio()
	tops := s.fastestOf(pr)
	e := s.estimate(p, &tops, float64(ahead), handed)
	// One return flight and bounded variation beyond the data ETA. Keep
	// the handover estimate even if this path slows or later loses ACKs.
	seconds := e.eta(length) + float64(p.mp.rtt.smoothed/2+minD(e.deviation, p.mp.rtt.smoothed/2)+s.ack.peerDelay)/1e9
	if seconds >= float64(Never)/1e9 {
		return Never
	}
	return s.now.Add(Duration(math.Round(seconds * 1e9)))
}

// prefixTarget selects a ready destination for precisely n bytes, using the
// scheduler's queue forecasts and the alternative's return-flight margin.
// Evidence can survive a temporary writer or admission gate.
func (s *Session) prefixTarget(g *gate, source *Pipe, anchor Time, n uint64) (*Pipe, Time, bool) {
	if source.mp.out != OutActive || !source.mp.peerAttached(source.id) || !maySend(source, false) {
		return nil, Never, false
	}
	var best *Pipe
	var bestETA float64
	repairAt := Never
	eligible := false
	if g != nil {
		s.gateQuality(g)
		var sourceETA float64
		if g.usable(s, source) {
			sourceETA = g.pipeQuality(source, g.tierOf(source)).eta(n)
		} else {
			pr, _ := source.mp.effPrio()
			tops := s.fastestOf(pr)
			sourceETA = s.estimate(source, &tops, s.queued(source), 0).eta(n)
		}
		original := sourceETA + float64(source.mp.rtt.smoothed/2)/1e9
		for ti := 0; ti <= g.last; ti++ {
			t := g.tiers[ti]
			for i := t.lo; i < t.hi; i++ {
				p := g.pipes[i]
				if p == source || !s.hasDelivery(p, anchor) {
					continue
				}
				eta := g.quality[i].eta(n)
				margin := maxD(50*Millisecond, p.mp.rtt.smoothed/2+s.ack.peerDelay)
				delay := eta + float64(margin)/1e9
				if delay >= original || delay >= float64(Never)/1e9 {
					continue
				}
				eligible = true
				a := p.mp
				if !s.open(p) || !s.takes(g, p) || s.shp != nil && s.shapeHeld(p) ||
					!s.reinjectRoom(p) || a.held1 && n > uint64(s.cfg.SpreadChunk)-a.holdSent {
					continue
				}
				if best == nil || eta < bestETA || eta == bestETA && p.id < best.id {
					best, bestETA, repairAt = p, eta, anchor.Add(Duration(math.Round(delay*1e9)))
				}
			}
		}
	}
	return best, repairAt, eligible
}

// prefixTimers considers only the first outstanding original of each stream.
// ACKs directly advance that stream's index as its contiguous prefix changes.
func (s *Session) prefixTimers(now Time, apply bool) Time {
	return s.prefixTimersExamined(now, apply, nil)
}

func (s *Session) prefixTimersExamined(now Time, apply bool, examined *int) Time {
	m := s.mp
	if m.prefixScheduler == nil || m.prefixAt == 0 || s.state >= StateClosing {
		return Never
	}
	if !apply || now < m.prefixAt {
		return m.prefixAt
	}
	m.prefixAt = 0
	m.prefixWaiting = false
	m.prefixStateWaiting = false
	if s.state == StateSuspended {
		return Never
	}
	var g *gate
	built := false
	var room uint64
	for i := 0; i < len(m.prefixStreams); {
		st := m.prefixStreams[i]
		ms := st.snd.mps
		lo := ms.acked.prefix()
		ref, r := s.prefixHead(st, lo, examined)
		if r == nil {
			continue // untrackPrefix moved another direction to this index
		}
		i++
		if lo < r.d.off || ms.oppSent.covered(lo, lo+1) != 0 {
			continue // only ACK progress can expose a new prefix
		}
		p := ref.source
		if p.mp.out != OutActive || !p.mp.peerAttached(p.id) {
			m.prefixStateWaiting = true
			continue
		}
		if ms.repair.hi != 0 || ms.resend.len() != 0 {
			m.prefixWaiting = true
			continue
		}
		if !built {
			g, built = s.buildGate(&m.prefixGate), true
			limit := s.copyBound()
			room = limit - min64(limit, s.copiesOutstanding())
		}
		capEnd := lo + uint64(s.cfg.SpreadChunk)
		hi := r.d.off + r.d.n
		// Stop the repair at the next already-confirmed range.
		k := ms.acked.from(lo + 1)
		if ms.acked.q.valid(k) {
			hi = min64(hi, ms.acked.q.at(k).lo)
		}
		// Budget can restrict the range sent on this cut. Observe the cap
		// even when reservations currently leave no room.
		n := min64(hi, capEnd) - lo
		if n == 0 {
			continue
		}
		if room != 0 {
			n = min64(n, room)
		}
		target, repairAt, _ := s.prefixTarget(g, p, r.sent, n)
		r.confirm = minT(r.confirm, repairAt)
		if r.confirm > now {
			s.checkPrefixAt(r.confirm)
		} else if target != nil && n <= room && s.queuePrefixRepair(p, r.sent, st, lo, n) {
			room -= n
			continue
		}
		m.prefixWaiting = true
	}
	if m.prefixAt == 0 {
		return Never
	}
	return m.prefixAt
}

func (s *Session) queuePrefixRepair(source *Pipe, anchor Time, st *Stream, lo, n uint64) bool {
	sp := &st.snd
	ms := sp.mps
	if n == 0 || lo == 0 && n < sp.preLen {
		return false
	}
	for _, existing := range s.mp.copies {
		if existing.st == st && existing.off < lo+n && existing.end > lo && s.copyLeft(existing) != 0 {
			return false
		}
	}
	if !s.reservePrefixCredit(n) {
		return false
	}
	ms.repair = prefixRepair{source: source, anchor: anchor, lo: lo, hi: lo + n, sent: lo}
	s.mp.prefixBytes += n
	ms.resend.add(lo, lo+n)
	s.sched.reinstate(st)
	s.wakeAllWriters()
	return true
}
