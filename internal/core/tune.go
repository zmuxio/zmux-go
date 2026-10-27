package core

// Receive-window auto-tuning (SPEC.md 8.3 rule 6):
// each window is the arrival rate times max(K x RTT_min, B) and never
// shrinks. One tuner per scope (a stream's receiving part, the session),
// allocated with the scope's first data.

const (
	tuneK       = 4
	rateFloor   = Millisecond
	tunePendMax = 17
)

// tuner is the auto-tuning state of one scope.
type tuner struct {
	rates   [4]float64  // arrival-rate samples, bytes/s
	rttSeen [4]Duration // multipath: largest pipe_min_rtt per rate interval
	idx     int
	n       int // samples taken (up to 4 counted)
	t0      Time
	off0    uint64
	started bool
	pend    [tunePendMax]tunePending
	ph, pn  int // FIFO head and length
}

type tunePending struct {
	oldLimit uint64
	sentAt   Time
	blocked  bool
}

// minFilter is a 300 s windowed minimum that keeps its value while no
// sample arrives.
type minFilter struct {
	v   Duration
	at  Time
	has bool
}

func (m *minFilter) update(d Duration, now Time) {
	if !m.has || d <= m.v || now.Sub(m.at) > 300*Second {
		m.v, m.at, m.has = d, now, true
	}
}

// tuneState is the session's RTT input.
type tuneState struct {
	carrierMin Duration // the carrier's own minimum RTT, 0 when unknown
	loopMin    minFilter
	paused     bool   // growth paused from the buffer cap until below half
	proposal   uint64 // session window growth awaiting the predicate
	asking     bool
}

func (s *Session) tuneOn() bool { return !s.cfg.NoAutotune }

// SetCarrierMinRTT reports the bootstrap carrier's minimum RTT (TCP_INFO
// tcpi_min_rtt and equivalents); 0 when it reports none.
func (s *Session) SetCarrierMinRTT(d Duration) { s.tn.carrierMin = d }

func (s *Session) tunePingSample(d Duration) { s.tn.loopMin.update(d, s.now) }
func (s *Session) tuneAckSample(d Duration)  { s.tn.loopMin.update(d, s.now) }

// pipeMinRTT is pipe_min_rtt of an attachment.
func (s *Session) pipeMinRTT(p *Pipe) (Duration, bool) {
	if p == nil || p.mp == nil {
		return 0, false
	}
	if p.out != nil && p.mp.rtt.has {
		return p.mp.rtt.min, true
	}
	// Without an outbound direction: the largest of its fate group's, or
	// of the session's.
	var best Duration
	ok := false
	for _, q := range s.pipes {
		if q == p || q.mp == nil || q.out == nil || !q.mp.rtt.has {
			continue
		}
		if p.mp.fate != 0 && (q.mp.fate != p.mp.fate || q.mp.joinerLocal != p.mp.joinerLocal) {
			continue
		}
		if q.mp.rtt.min > best {
			best, ok = q.mp.rtt.min, true
		}
	}
	if !ok && p.mp.fate != 0 {
		for _, q := range s.pipes {
			if q != p && q.mp != nil && q.out != nil && q.mp.rtt.has && q.mp.rtt.min > best {
				best, ok = q.mp.rtt.min, true
			}
		}
	}
	return best, ok
}

// tuneRTT is autotune_rtt(scope).
func (s *Session) tuneRTT(t *tuner) (Duration, bool) {
	if s.mp != nil {
		var m Duration
		ok := false
		for _, v := range t.rttSeen {
			if v > 0 {
				m, ok = max(m, v), true
			}
		}
		return m, ok
	}
	if s.tn.carrierMin > 0 {
		return s.tn.carrierMin, true
	}
	return s.tn.loopMin.v, s.tn.loopMin.has
}

// tuneData runs when the scope's highest received offset becomes off.
func (s *Session) tuneScopeData(t *tuner, off uint64) {
	now := s.now
	if s.mp != nil {
		if d, ok := s.pipeMinRTT(s.rxPipe); ok && d > t.rttSeen[t.idx] {
			t.rttSeen[t.idx] = d
		}
	}
	// Credit-loop sample: the first byte beyond a limit the peer used up.
	for t.pn > 0 {
		p := &t.pend[t.ph]
		if off < p.oldLimit {
			break
		}
		if off == p.oldLimit {
			p.blocked = true
			break
		}
		if p.blocked && s.mp == nil {
			s.tn.loopMin.update(now.Sub(p.sentAt), now)
		}
		t.ph = (t.ph + 1) % tunePendMax
		t.pn--
	}
	if !t.started {
		t.started, t.t0, t.off0 = true, now, off
		return
	}
	iv := rateFloor
	if r, ok := s.tuneRTT(t); ok && r > iv {
		iv = r
	}
	if el := now.Sub(t.t0); el >= iv {
		t.idx = (t.idx + 1) % len(t.rates)
		t.rates[t.idx] = float64(off-t.off0) * 1e9 / float64(el)
		t.rttSeen[t.idx] = 0
		t.n++
		t.t0, t.off0 = now, off
	}
}

// tuneBlocked records a BLOCKED frame at limit.
func (t *tuner) blockedAt(limit uint64) {
	for i := 0; i < t.pn; i++ {
		p := &t.pend[(t.ph+i)%tunePendMax]
		if p.oldLimit == limit {
			p.blocked = true
		}
	}
}

// grow is on_send_update's window rule; it returns the new window.
func (s *Session) tuneGrow(t *tuner, window, maxWindow, allowance uint64) uint64 {
	if t == nil || t.n == 0 || s.tn.paused || s.underPressure() {
		return window
	}
	var r float64
	for _, x := range t.rates {
		r = max(r, x)
	}
	span := s.cfg.LatencyBudget
	if rtt, ok := s.tuneRTT(t); ok && tuneK*rtt > span {
		span = tuneK * rtt
	}
	target := r * float64(span) / 1e9
	if target <= float64(window) {
		return window
	}
	w := uint64(target)
	if maxWindow > 0 {
		w = min64(w, maxWindow)
	}
	w = min64(w, allowance)
	return max64(w, window)
}

// sent records an update from oldLimit (credit-loop sample).
func (t *tuner) sent(oldLimit uint64, now Time, blocked bool) {
	if t.pn == tunePendMax {
		// A full FIFO drops a sample, never credit.
		t.ph = (t.ph + 1) % tunePendMax
		t.pn--
	}
	t.pend[(t.ph+t.pn)%tunePendMax] = tunePending{oldLimit, now, blocked}
	t.pn++
}

// connAllowance is the largest session window the memory budget allows:
// the current window plus what the budget has left.
func (s *Session) connAllowance() uint64 {
	c := &s.conn
	if sb := s.cfg.Budget; sb != nil {
		if sb.limit == 0 {
			return ^uint64(0)
		}
		if u := sb.Used(); u < sb.limit {
			return c.win + (sb.limit - u)
		}
		return c.win
	}
	b := s.cfg.MemoryBudget
	if b == 0 {
		return ^uint64(0)
	}
	used := c.buffered + c.win - min64(c.win, s.initConnWin)
	if s.mp != nil {
		used += s.mp.retained
	}
	if used >= b {
		return c.win
	}
	return c.win + (b - used)
}

// tuneData feeds a stream's new highest offset to its and the session's tuner.
func (s *Session) tuneData(st *Stream, end uint64) {
	r := &st.rcv
	if r.tune == nil && end >= r.window/4 {
		// A stream gets its tuner once it carried a quarter of its window:
		// one that exchanges small messages never needs a larger window.
		r.tune = &tuner{}
	}
	if r.tune != nil {
		s.tuneScopeData(r.tune, end)
	}
	c := &s.conn
	if c.tune == nil && c.recv >= c.win/4 {
		c.tune = &tuner{} // likewise for the session
	}
	if c.tune != nil {
		s.tuneScopeData(c.tune, c.recv)
	}
	cp := s.bufferCap()
	switch {
	case c.buffered >= cp:
		s.tn.paused = true
	case s.tn.paused && c.buffered < cp/2:
		s.tn.paused = false
	}
}

// tuneConsume lifts the pause once the buffer fell below half its cap.
func (s *Session) tuneConsume() {
	if s.tn.paused && s.conn.buffered < s.bufferCap()/2 {
		s.tn.paused = false
	}
}

// tuneStreamUpdate grows a stream window before an update and records it.
func (s *Session) tuneStreamUpdate(st *Stream) {
	r := &st.rcv
	if !s.tuneOn() || r.tune == nil {
		return
	}
	maxW := s.cfg.StreamWindowCap
	if r.maxWin > 0 && (maxW == 0 || r.maxWin < maxW) {
		maxW = r.maxWin
	}
	w := s.tuneGrow(r.tune, r.window, maxW, s.connAllowance())
	if w > r.window {
		r.window = w
		// The session window follows: at least 1.5 x the stream window.
		cw := w + w/2
		if s.cfg.ConnWindowCap > 0 {
			cw = min64(cw, s.cfg.ConnWindowCap)
		}
		cw = min64(cw, s.connAllowance())
		s.growConnWindow(cw)
	}
}

// tuneConnUpdate grows the session window before an update.
func (s *Session) tuneConnUpdate() {
	c := &s.conn
	if !s.tuneOn() || c.tune == nil {
		return
	}
	s.growConnWindow(s.tuneGrow(c.tune, c.win, s.cfg.ConnWindowCap, s.connAllowance()))
}

// growConnWindow raises the session window to w, or proposes it to the
// application's predicate (allow_connection_window_increase), which runs
// outside every lock; its answer applies if the proposal is still current.
func (s *Session) growConnWindow(w uint64) {
	c := &s.conn
	if w <= c.win {
		return
	}
	if s.cfg.WindowPredicate {
		if w > s.tn.proposal {
			s.tn.proposal = w
		}
		return
	}
	c.win = w
	s.connCreditChanged()
}

// WindowProposal returns a pending session window growth for the
// predicate: the proposed window and the increase.
func (s *Session) WindowProposal() (uint64, uint64, bool) {
	p := s.tn.proposal
	if p <= s.conn.win || s.tn.asking {
		return 0, 0, false
	}
	s.tn.asking = true
	return p, p - s.conn.win, true
}

// ApplyWindowProposal applies the predicate's answer for proposal w.
func (s *Session) ApplyWindowProposal(w uint64, ok bool) {
	s.tn.asking = false
	if s.tn.proposal != w {
		return // superseded: asked again with the newer proposal
	}
	s.tn.proposal = 0
	if ok && w > s.conn.win {
		s.conn.win = w
		s.connCreditChanged()
	}
}

// HasWindowProposal reports a session window growth awaiting the predicate.
func (s *Session) HasWindowProposal() bool { return s.tn.proposal > s.conn.win && !s.tn.asking }
