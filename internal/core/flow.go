package core

// connFlow is the session-level flow control (SPEC.md 8.2, 8.3).
type connFlow struct {
	// Sending.
	sendMax  uint64 // peer's MAX_DATA
	sent     uint64 // sum of the highest offsets sent
	blocked  uint64 // limit+1 at which DATA_BLOCKED was sent
	sendHeld uint64 // written, not yet released bytes (send_buffer)
	creditAt Time   // when credit last arrived while data waited for it
	// Send-buffer water-filling (IMPLEMENTATION.md 6.4).
	smallHeld uint64 // bytes held by small directions
	bigCount  int    // directions that are not small

	// Receiving.
	win       uint64 // window W
	adv       uint64 // advertised limit L
	rel       uint64 // released credit (C = rel + win)
	recv      uint64 // sum over streams of max(highest, final)
	buffered  uint64 // unread buffered bytes
	held      uint64 // received at or above the cap, credit not released
	blockSeen uint64 // largest DATA_BLOCKED limit reported
	peerBlock bool   // DATA_BLOCKED at the current limit received
	tune      *tuner
}

func (c *connFlow) initRecv(w uint64) {
	c.win, c.adv = w, w
}

// sessionWindowCapFloor is the 96 MiB floor of the default cap.
const sessionWindowCapFloor = 96 << 20

// bufferCap is max(session window cap, S) (SPEC.md 8.3 rule 4).
func (s *Session) bufferCap() uint64 {
	c := uint64(sessionWindowCapFloor)
	if s.cfg.ConnWindowCap > 0 {
		c = s.cfg.ConnWindowCap
	}
	l := &s.cfg.Local
	S := s.rem[0].initial*l.MaxStreamDataBidiRem + s.rem[1].initial*l.MaxStreamDataUni
	S += s.openLocalBidiRecv * l.MaxStreamDataBidiLoc
	return max64(c, S)
}

// bufAdd accounts n bytes stored in a stream buffer and releases session
// credit for them below the cap. It is the only function that raises the
// buffered count (IMPLEMENTATION.md 13.2 C1).
func (s *Session) bufAdd(n uint64) {
	if n == 0 {
		return
	}
	c := &s.conn
	cp := s.bufferCap()
	if c.buffered < cp && !s.underPressure() {
		r := min64(n, cp-c.buffered)
		c.rel += r
		c.held += n - r
	} else {
		c.held += n
	}
	c.buffered += n
	s.connCreditChanged()
}

// bufSub accounts n buffered bytes consumed (read, discarded, released).
// It is the only function that lowers the buffered count.
func (s *Session) bufSub(n uint64) {
	if n == 0 {
		return
	}
	c := &s.conn
	if n > c.buffered {
		panic("zmux: buffered bytes below zero")
	}
	c.buffered -= n
	if c.held > 0 {
		if c.buffered < s.bufferCap() && !s.underPressure() {
			c.rel += c.held
			c.held = 0
		} else {
			r := min64(n, c.held)
			c.rel += r
			c.held -= r
		}
	}
	s.connCreditChanged()
}

// releaseUnbuffered releases session credit for bytes that are never
// buffered (discarded on arrival, a reset's gap).
func (s *Session) releaseUnbuffered(n uint64) {
	if n == 0 {
		return
	}
	s.conn.rel += n
	s.connCreditChanged()
}

// updateThreshold is min(W/4, max(W/16, 256 KiB)) (IMPLEMENTATION.md 6.2).
func updateThreshold(w uint64) uint64 {
	return min64(w/4, max64(w/16, 256<<10))
}

// creditDue decides whether a scope's released credit must be sent now
// (SPEC.md 8.3 rules 2 and 3). received is what the peer has sent there.
func creditDue(c, l, w, received uint64, blockedFrame bool) bool {
	if c <= l {
		return false
	}
	d := c - l
	if d >= updateThreshold(w) {
		return true
	}
	if received >= l || blockedFrame {
		return true
	}
	if l-received < w/4 && d >= min64(w/4, 32<<10) {
		return true
	}
	return false
}

// connCreditChanged queues MAX_DATA when due.
func (s *Session) connCreditChanged() {
	c := &s.conn
	if creditDue(c.rel+c.win, c.adv, c.win, c.recv, c.peerBlock) {
		s.pend.maxData = true
		s.wakeAllWriters()
	}
}

// streamCreditChanged queues MAX_STREAM_DATA when due.
func (s *Session) streamCreditChanged(st *Stream) {
	r := &st.rcv
	if r.finKnown || r.stopped || r.reset {
		return
	}
	if creditDue(r.readOff+r.window, r.adv, r.window, r.highest, r.blockSeen == r.adv && r.blockSeen > 0) {
		s.queueCtl(st, ctlMaxStreamData)
	}
}

// queueCtl marks a pending control frame of st.
func (s *Session) queueCtl(st *Stream, bit uint8) {
	if !st.inCtl {
		st.inCtl = true
		st.ctlNext = nil
		if s.ctlTail == nil {
			s.ctlHead = st
		} else {
			s.ctlTail.ctlNext = st
		}
		s.ctlTail = st
	}
	st.ctl |= bit
	s.wakeAllWriters()
}
