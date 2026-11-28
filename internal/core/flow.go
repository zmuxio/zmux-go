package core

// connFlow is the session-level flow control.
type connFlow struct {
	// Receiving.
	tune *tuner
	rx   recvMemory
	// Sending.
	sendMax         uint64 // peer's MAX_DATA
	sent            uint64 // sum of the highest offsets sent
	blocked         uint64 // limit+1 at which DATA_BLOCKED was queued
	blockedReported uint64
	sendHeld        uint64 // written, not yet released bytes (send_buffer)
	creditAt        Time   // when credit last arrived while data waited for it
	// Send-buffer water-filling.
	smallHeld uint64 // bytes held by small directions
	bigCount  int    // directions that are not small
	win       uint64 // window W
	adv       uint64 // advertised limit L
	rel       uint64 // released credit (C = rel + win)
	recv      uint64 // sum over streams of max(highest, final)
	settled   uint64 // a consumed byte or one that can no longer occupy storage
	buffered  uint64 // unread buffered bytes
	held      uint64 // received at or above the cap, credit not released
	blockSeen uint64 // largest DATA_BLOCKED limit reported
	peerBlock bool   // DATA_BLOCKED at the current limit received
}

func (c *connFlow) initRecv(w uint64) {
	c.win, c.adv = w, w
}

// sessionWindowCapFloor is the 96 MiB floor of the default cap.
const sessionWindowCapFloor = 96 << 20

// bufferCap is max(session window cap, S).
func (s *Session) bufferCap() uint64 {
	c := uint64(sessionWindowCapFloor)
	if s.cfg.ConnWindowCap > 0 {
		c = s.cfg.ConnWindowCap
	}
	l := &s.cfg.Local
	sum := s.rem[0].initial*l.MaxStreamDataBidiRem + s.rem[1].initial*l.MaxStreamDataUni
	sum += s.openLocalBidiRecv * l.MaxStreamDataBidiLoc
	return max64(c, sum)
}

func (s *Session) compactReceives(current *Buf) {
	s.conn.rx.compact(current)
}

// bufAdd accounts n bytes stored in a stream buffer and releases session
// credit for them below the cap. It is the only function that raises the
// buffered count.
func (s *Session) bufAdd(n uint64) {
	if n == 0 {
		return
	}
	c := &s.conn
	cp := s.bufferCap()
	if held := c.rx.resident(); held < cp && !s.underPressure() {
		r := min64(n, cp-held)
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
	s.compactReceives(nil)
	c.buffered -= n
	c.settled += n
	if c.held > 0 {
		if c.rx.resident() < s.bufferCap() && !s.underPressure() {
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
	s.conn.settled += n
	s.connCreditChanged()
}

// updateThreshold is min(W/4, max(W/16, 256 KiB)).
func updateThreshold(w uint64) uint64 {
	return min64(w/4, max64(w/16, 256<<10))
}

// creditDue decides whether a scope's released credit must be sent now.
// received is what the peer has sent there.
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
	if creditDue(s.nextReceiveCredit(), c.adv, c.win, c.recv, c.peerBlock) {
		s.pend.maxData = true
		s.wakeAllWriters()
	}
}

func (s *Session) nextReceiveCredit() uint64 {
	c := &s.conn
	v := c.rel + c.win
	if s.cfg.receiveHold != 0 {
		// Highest stream offsets include fillable holes. Account only
		// for settled bytes and those whose backing is actually present.
		// Dense queued data can use less space than its in-flight reserve.
		physical := max64(c.rx.queued, c.buffered)
		space := 2*c.win - min64(physical, 2*c.win)
		v = min64(v, c.settled+c.buffered+space/2)
	}
	return max64(v, c.adv)
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
	credit := st.inCtl && st.ctl&ctlMaxStreamData != 0
	st.ctl |= bit
	if st.ctlWaits() {
		return
	}
	if !st.inCtl {
		st.inCtl = true
		st.ctlNext, st.ctlPrev = nil, s.ctlTail
		if s.ctlTail == nil {
			s.ctlHead = st
		} else {
			s.ctlTail.ctlNext = st
		}
		s.ctlTail = st
	}
	if !credit && st.ctl&ctlMaxStreamData != 0 {
		s.creditStreams++
	}
	s.wakeAllWriters()
}

func (s *Session) clearCtl(st *Stream, bits uint8) {
	if st.inCtl && st.ctl&bits&ctlMaxStreamData != 0 {
		s.creditStreams--
	}
	st.ctl &^= bits
}

// ctlWaits reports whether the control frames of st wait for its opening
// STREAM frame, which queues them (openedCtl): before the identifier is
// allocated (a frame then would carry no valid ID). Once allocated, a control frame may itself be the
// stream's first frame.
func (st *Stream) ctlWaits() bool {
	return st.local && !st.idSet
}

// openedCtl queues the control frames that waited for st's opening
// STREAM frame.
func (s *Session) openedCtl(st *Stream) {
	if st.ctl != 0 {
		s.queueCtl(st, 0)
	}
}
