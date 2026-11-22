package core

// prefixRepair accounts for one stream's anticipatory copy. Pending bytes
// are in resend and sent bytes are in the ordinary carrier retention log.
type prefixRepair struct {
	source *Pipe
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

func (ref prefixRef) locate() (*retained, int) {
	log := ref.lane.log
	if ref.lane.prefixCount == 0 || len(log) == 0 {
		return nil, 0
	}
	i := ref.serial - log[0].serial
	if i >= uint64(len(log)) || log[i].end != ref.end || log[i].confirm == 0 {
		return nil, 0
	}
	return &log[i], int(i)
}

// prefixTrack follows one sending direction's originals in stream order.
// Only the first outstanding original can contain the next missing offset.
type prefixTrack struct {
	extentLane *outLane
	refs       []prefixRef
	head       int
	active     int    // index in prefixStreams, -1 while empty
	extent     uint64 // discovered contiguous expired range at head
	extentNext uint64 // next lane-log serial to consider for that range
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
	t.extent, t.extentNext, t.extentLane = 0, 0, nil
}

func (s *Session) prefixHead(st *Stream, pre uint64, examined *int) (prefixRef, *retained, int) {
	t := st.snd.mps.prefix
	if t == nil || t.active < 0 {
		return prefixRef{}, nil, 0
	}
	for t.head < len(t.refs) {
		ref := t.refs[t.head]
		r, i := ref.locate()
		if examined != nil {
			*examined++
		}
		if r != nil && pre < r.d.off+r.d.n {
			return ref, r, i
		}
		t.refs[t.head] = prefixRef{}
		t.head++
	}
	s.untrackPrefix(st)
	return prefixRef{}, nil, 0
}

func (s *Session) schedulePrefix(st *Stream, pre uint64) {
	_, r, _ := s.prefixHead(st, pre, nil)
	if r != nil && r.d.off <= pre {
		s.checkPrefixAt(maxT(r.confirm, s.now))
	}
}

func (r prefixRepair) covers(off uint64) bool { return r.lo <= off && off < r.hi }

func (s *Session) clearPrefixRepair(m *mpSend) {
	r := m.repair
	if r.hi > r.lo {
		s.mp.prefixBytes -= r.hi - r.lo
		s.mp.prefixPending -= r.hi - r.sent
		m.repair = prefixRepair{}
		s.wakePrefix()
	}
}

func (s *Session) cancelPrefixTail(m *mpSend) {
	r := m.repair
	m.resend.remove(r.sent, r.hi)
	s.releasePrefixTail(m)
}

// releasePrefixTail releases only the unsent reservation. Native loss keeps
// those bytes queued for ordinary reinjection.
func (s *Session) releasePrefixTail(m *mpSend) {
	r := &m.repair
	if r.sent == r.hi {
		return
	}
	m.oppSent.remove(r.sent, r.hi)
	n := r.hi - r.sent
	s.mp.prefixPending -= n
	s.mp.prefixBytes -= n
	r.hi = r.sent
	if r.hi == r.lo {
		m.repair = prefixRepair{}
	}
	s.wakePrefix()
}

// retryPrefixTail releases a stale reservation and gives the original a
// new observation window before attempting the same still-missing bytes.
func (s *Session) retryPrefixTail(st *Stream) {
	m := st.snd.mps
	source := m.repair.source
	s.cancelPrefixTail(m)
	pre := m.acked.prefix()
	if _, r, _ := s.prefixHead(st, pre, nil); r != nil && r.d.off <= pre {
		r.confirm = s.now.Add(maxD(Millisecond, source.mp.currentRTT(s.now)/2))
		s.checkPrefixAt(r.confirm)
	}
}

func (s *Session) ackPrefixRepair(m *mpSend) {
	r := m.repair
	if r.hi > r.lo && m.acked.contains(r.lo, r.hi) {
		s.clearPrefixRepair(m)
	}
}

// commitPrefixRepair charges actual encoded STREAM payload. sent also
// retires any pending reservations for ACKed holes passed by this fragment.
func (s *Session) commitPrefixRepair(m *mpSend, off, n uint64) {
	r := &m.repair
	end := off + n
	b := &s.mp.prefixBucket
	b.fill(usOf(s.now))
	b.tokens -= int64(n)
	s.mp.prefixPending -= end - r.sent
	r.sent = end
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
	if s.mp != nil && s.mp.additionalPipes != nil && s.mp.prefixWaiting && s.state < StateClosing {
		s.checkPrefixAt(maxT(1, s.now))
		s.mp.prefixWaiting = false
	}
}

func (s *Session) wakePrefixOnPipeState() {
	if s.mp != nil && s.mp.additionalPipes != nil && s.mp.prefixStateWaiting && s.state < StateClosing {
		s.checkPrefixAt(maxT(1, s.now))
		s.mp.prefixStateWaiting = false
	}
}

// checkPrefixAt keeps one timer for the earliest outstanding check. ACKs
// remove their retained entries; a stale timer therefore does no repair.
func (s *Session) checkPrefixAt(at Time) {
	if s.mp.prefixAt == 0 || at < s.mp.prefixAt {
		s.mp.prefixAt = at
	}
}

func (s *Session) prefixDeadline(p *Pipe, ahead, length uint64) Time {
	pr, _ := p.mp.effPrio()
	tops := s.fastestOf(pr)
	e := s.estimate(p, &tops, float64(ahead), length)
	// One return flight and bounded variation beyond the data ETA. Keep
	// the handover estimate even if this path slows or later loses ACKs.
	seconds := e.eta(length) + float64(e.rtt/2+minD(e.deviation, e.rtt/2)+s.ack.peerDelay)/1e9
	if seconds >= float64(Never)/1e9 {
		return Never
	}
	return s.now.Add(Duration(seconds * 1e9))
}

// prefixEligible evaluates evidence that a path can carry a repair. Writer
// availability, pacing, shaping and tier admission can change between polls.
func (s *Session) prefixEligible(p, source *Pipe) bool {
	a := p.mp
	return p != source && p.out != nil && !p.out.failed && a != nil &&
		a.out == OutActive && !a.retired && a.peerAttached(p.id) &&
		a.quality != nil && a.quality.freshRTT(s.now) && !s.qualityElevated(p) &&
		(!a.unacked || s.now.Sub(a.tref()) <= s.pto(a)+s.queueDelay(a))
}

// prefixTarget returns the best ready path and whether a qualified path
// remains. An unavailable writer cannot invalidate a pending reservation.
func (s *Session) prefixTarget(g *gate, source *Pipe, n uint64) (*Pipe, float64, bool) {
	var best *Pipe
	var eta float64
	if g != nil {
		s.gateQuality(g)
		for ti := 0; ti <= g.last; ti++ {
			t := g.tiers[ti]
			for i := t.lo; i < t.hi; i++ {
				p := g.pipes[i]
				a := p.mp
				if !s.prefixEligible(p, source) || !s.open(p) || !s.takes(g, p) ||
					!s.pipeUsable(p, false) || s.shp != nil && s.shapeHeld(p) ||
					!s.reinjectRoom(p) || a.held1 && n > uint64(s.cfg.SpreadChunk)-a.holdSent {
					continue
				}
				arrival := g.quality[i].eta(n)
				if best == nil || arrival < eta || arrival == eta && p.id < best.id {
					best, eta = p, arrival
				}
			}
		}
	}
	if best != nil {
		return best, eta, true
	}
	for _, p := range s.pipes {
		if s.prefixEligible(p, source) {
			return nil, 0, true
		}
	}
	return nil, 0, false
}

// prefixTimers considers only the first outstanding original of each stream.
// ACKs directly advance that stream's index as its contiguous prefix changes.
func (s *Session) prefixTimers(now Time, apply bool) Time {
	return s.prefixTimersExamined(now, apply, nil)
}

func (s *Session) prefixTimersExamined(now Time, apply bool, examined *int) Time {
	m := s.mp
	if m.additionalPipes == nil || m.prefixAt == 0 || s.state >= StateClosing {
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
		ref, r, j := s.prefixHead(st, lo, examined)
		if r == nil {
			continue // untrackPrefix moved another direction to this index
		}
		i++
		if lo < r.d.off || ms.oppSent.covered(lo, lo+1) != 0 {
			continue // only ACK progress can expose a new prefix
		}
		if r.confirm > now {
			s.checkPrefixAt(r.confirm)
			continue
		}
		p, ol := ref.source, ref.lane
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
		capEnd := lo + min64(room, uint64(s.cfg.SpreadChunk))
		hi := ms.prefix.extend(ol, r, j, capEnd, now, examined)
		// Stop the repair at the next already-confirmed range.
		k := ms.acked.from(lo + 1)
		if ms.acked.q.valid(k) {
			hi = min64(hi, ms.acked.q.at(k).lo)
		}
		n := min64(hi, capEnd) - lo
		if s.queuePrefixRepair(g, p, ol, r, st, lo, n) {
			room -= n
		} else {
			m.prefixWaiting = true
		}
	}
	if m.prefixAt == 0 {
		return Never
	}
	return m.prefixAt
}

// extend coalesces adjacent expired originals on the same lane. While this
// head remains blocked, later wakeups need only inspect the next boundary.
func (t *prefixTrack) extend(lane *outLane, r *retained, i int, limit uint64, now Time, examined *int) uint64 {
	// ACKs can advance the head within a span we have already discovered.
	// Reuse that span until the new head crosses its lane or range.
	if t.extentLane != lane || r.d.off >= t.extent || r.serial >= t.extentNext {
		t.extent, t.extentNext = r.d.off+r.d.n, r.serial+1
		t.extentLane = lane
	}
	i += int(t.extentNext - r.serial)
	for ; t.extent < limit && i < len(lane.log); i++ {
		more := &lane.log[i]
		if examined != nil {
			*examined++
		}
		if more.confirm == 0 || more.confirm > now || more.d.id != r.d.id || more.d.off != t.extent {
			break
		}
		t.extent += more.d.n
		t.extentNext++
	}
	return t.extent
}

func (s *Session) queuePrefixRepair(g *gate, source *Pipe, ol *outLane, r *retained, st *Stream, lo, n uint64) bool {
	sp := &st.snd
	ms := sp.mps
	if n == 0 || lo == 0 && n < sp.preLen || ms.repair.hi != 0 || ms.resend.len() != 0 {
		return false
	}
	for _, existing := range s.mp.copies {
		if existing.st == st && existing.off <= lo && lo < existing.end && s.copyLeft(existing) != 0 {
			return false
		}
	}
	// Recent advances moving at least a chunk's worth (or the whole
	// original range) keep a merely delayed carrier in service. A trickle
	// below that scale does not restart the frozen deadline.
	progress := ol.acked - min64(ol.acked, r.ackBase)
	if progress >= r.d.n {
		r.ackBase, r.progressAt = ol.acked, source.mp.lastAdvance
	}
	if r.progressAt != 0 {
		if until := r.progressAt.Add(source.mp.currentRTT(s.now) / 2); s.now < until {
			s.checkPrefixAt(until)
			return false
		}
	}
	if target, _ := s.prefixReplacement(g, source, n); target == nil || !s.reservePrefixCredit(n) {
		return false
	}
	ms.repair = prefixRepair{source: source, lo: lo, hi: lo + n, sent: lo}
	ms.oppSent.add(lo, lo+n)
	s.mp.prefixBytes += n
	s.lost(&frameDesc{kind: dStream, id: st.id, off: lo, n: n})
	return true
}

// prefixReplacement compares the best eligible alternative with the
// original using the same observation that the writer uses for admission.
func (s *Session) prefixReplacement(g *gate, source *Pipe, n uint64) (*Pipe, bool) {
	target, eta, eligible := s.prefixTarget(g, source, n)
	if target == nil {
		return nil, eligible
	}
	// A missed confirmation with no meaningful progress costs another
	// feedback opportunity on the original path. Demand a further margin
	// before spending bytes on an alternative.
	var q pathEstimate
	if g.usable(s, source) {
		q = g.pipeQuality(source, g.tierOf(source))
	} else {
		pr, _ := source.mp.effPrio()
		tops := s.fastestOf(pr)
		q = s.estimate(source, &tops, s.queued(source), 0)
	}
	original := q.eta(n) + float64(q.rtt/2)/1e9
	margin := maxD(s.ack.peerDelay, q.rtt/8)
	if eta+float64(margin)/1e9 >= original {
		return nil, true
	}
	return target, true
}
