package core

// Retention of unacknowledged information (MULTIPATH.md 7.4, SCHEDULER.md
// 13.1): every ack-eliciting frame with a reinjection rule is logged per
// outbound lane with the position that ends its record; a PIPE_ACK pops
// the entries it covers (acked), a suspect or failed pipe turns the rest
// into "send this scope again" (lost).

type descKind uint8

const (
	dStream descKind = iota + 1
	dReset
	dStop
	dMaxData
	dMaxStreamData
	dMaxStreams
	dDataBlocked
	dStreamDataBlocked
	dStreamsBlocked
	dGoAway
	dPrio
	dStatus
	dAbandon
)

type frameDesc struct {
	kind descKind
	bidi bool
	fin  bool
	id   uint64 // stream ID, or pipe ID for PIPE_STATUS / PIPE_ABANDON
	off  uint64
	n    uint64
	val  uint64
}

type retained struct {
	end        uint64 // lane position after the record carrying the frame
	d          frameDesc
	reinjected bool
	sent       Time     // when the batch was handed to the carrier
	due        Duration // E: the acknowledgement time expected at sending (7.5.1)
}

// mpSend is the multipath side storage of a sending part.
type mpSend struct {
	acked      rangeSet // acknowledged byte ranges
	resend     rangeSet // ranges to send again
	finAcked   bool
	finResend  bool
	bind       binding    // affinity
	rr         []rrVal    // own round-robin values
	pick       pickAnswer // custom policy answer
	pickQueued bool
	oppSent    rangeSet // bytes re-sent opportunistically (once each)
}

func (sp *sendPart) mpState() *mpSend {
	if sp.mps == nil {
		sp.mps = &mpSend{}
	}
	return sp.mps
}

// logFrame records a frame of the record being built (multipath only).
func (w *recWriter) logFrame(d frameDesc) {
	if !w.retain {
		return
	}
	w.descs = append(w.descs, recDesc{rec: w.recs, d: d})
}

type recDesc struct {
	rec int
	d   frameDesc
}

// retainBatch moves the logged frames of a batch into the lane's log.
func (s *Session) retainBatch(w *recWriter, ol *outLane, start uint64) {
	if len(w.descs) == 0 {
		return
	}
	m := s.mp
	ends := m.endsBuf[:0]
	pos := start
	for _, r := range w.b.recs {
		pos += uint64(recordPositionBytes(ol.info.Framing, r.size))
		ends = append(ends, pos)
	}
	m.endsBuf = ends
	// E = (unsent_ahead + L) / R_p + min_rtt_p + peer max_ack_delay,
	// fixed at sending so that a pipe that slows down cannot raise it.
	var due Duration
	if p := w.pipe; p != nil && p.mp != nil {
		due = timeAt(p.mp.unsentAhead+(pos-start), s.rateOf(p)) + p.mp.minRTT() + s.ack.peerDelay
	}
	for _, rd := range w.descs {
		e := pos
		if rd.rec < len(ends) {
			e = ends[rd.rec]
		}
		ol.log = append(ol.log, retained{end: e, d: rd.d, sent: s.now, due: due})
	}
	w.descs = w.descs[:0]
}

// ackLog pops the entries a lane acknowledgement covers.
func (s *Session) ackLog(ol *outLane, pos uint64) {
	i := 0
	for i < len(ol.log) && ol.log[i].end <= pos {
		s.acked(&ol.log[i].d)
		i++
	}
	if i > 0 {
		n := copy(ol.log, ol.log[i:])
		clear(ol.log[n:])
		ol.log = ol.log[:n]
	}
}

// reinjectPipe marks the unacknowledged information of a pipe for sending
// again, at most once per entry (MULTIPATH.md 7.4).
func (s *Session) reinjectPipe(p *Pipe) {
	if p.out == nil {
		return
	}
	for _, ol := range p.out.lanes {
		for i := range ol.log {
			e := &ol.log[i]
			if e.reinjected {
				continue
			}
			e.reinjected = true
			if e.d.kind == dStream {
				p.mp.reinjectedBytes += e.d.n
			}
			s.lost(&e.d)
		}
	}
	s.wakeAllWriters()
}

// acked applies the acknowledgement of one frame.
func (s *Session) acked(d *frameDesc) {
	switch d.kind {
	case dStream:
		st := s.streams[d.id]
		if st == nil || !st.hasS {
			return
		}
		sp := &st.snd
		m := sp.mpState()
		if d.n > 0 {
			m.acked.add(d.off, d.off+d.n)
		}
		if d.fin {
			m.finAcked = true
		}
		s.streamAckProgress(st)
	case dReset:
		if st := s.streams[d.id]; st != nil && st.hasS && st.snd.resetSent {
			s.sendTerminal(st, sendResetRecvd)
		}
	case dPrio:
		if st := s.streams[d.id]; st != nil && st.ext != nil && st.ext.reqSent == d.val {
			st.ext.reqAcked = d.val
		}
	case dAbandon:
		if p := s.pipe(d.id); p != nil && p.mp != nil {
			p.mp.abandonAcked = true
			p.mp.ackedAbandonAt = s.now
			s.maybeRetire(p)
		}
	}
}

// streamAckProgress releases acknowledged bytes and finishes the part.
func (s *Session) streamAckProgress(st *Stream) {
	sp := &st.snd
	m := sp.mps
	pre := m.acked.prefix()
	if pre > sp.q.base {
		rel := sp.q.base
		h := s.heldEnter(st)
		sp.q.release(min64(pre, sp.q.end))
		s.heldLeave(st, h)
		s.mp.retained -= min64(s.mp.retained, min64(sp.q.base, sp.sent)-min64(rel, sp.sent))
		s.wake(st.wsig)
	}
	end := sp.q.end
	if sp.reset {
		end = sp.resetRel
	}
	if !sp.reset && sp.finSent && m.finAcked && pre >= end {
		s.sendTerminal(st, sendDataRecvd)
	}
}

// lost turns one frame's information into pending sending.
func (s *Session) lost(d *frameDesc) {
	switch d.kind {
	case dStream:
		st := s.streams[d.id]
		if st == nil || !st.hasS {
			return
		}
		sp := &st.snd
		if sp.state == sendDataRecvd || sp.state == sendResetRecvd {
			return
		}
		m := sp.mpState()
		lo, hi := d.off, d.off+d.n
		if sp.reset {
			hi = min64(hi, sp.resetRel)
		}
		// Only the parts not yet acknowledged.
		m.acked.gaps(lo, hi, func(a, b uint64) { m.resend.add(a, b) })
		if d.fin && !m.finAcked && !sp.reset {
			m.finResend = true
		}
		s.sched.ensure(st)
	case dReset:
		if st := s.streams[d.id]; st != nil && st.hasS && st.snd.reset && st.snd.state != sendResetRecvd {
			s.queueCtl(st, ctlReset)
		}
	case dStop:
		if st := s.streams[d.id]; st != nil && st.hasR && st.rcv.stopped &&
			st.rcv.state != recvDataRead && st.rcv.state != recvResetRead {
			s.queueCtl(st, ctlStop)
		}
	case dMaxData:
		s.pend.maxData = true
		s.pend.forceMaxData = true
	case dMaxStreamData:
		if st := s.streams[d.id]; st != nil && st.hasR && st.rcv.state == recvRecv {
			st.forceMSD = true
			s.queueCtl(st, ctlMaxStreamData)
		}
	case dMaxStreams:
		t := typeIndex(d.bidi)
		s.pend.maxStreams[t] = true
	case dDataBlocked:
		if s.conn.blocked == s.conn.sendMax+1 {
			s.pend.dataBlocked = true
		}
	case dStreamDataBlocked:
		if st := s.streams[d.id]; st != nil && st.snd.blocked == st.snd.maxData+1 {
			s.queueCtl(st, ctlBlocked)
		}
	case dStreamsBlocked:
		t := typeIndex(d.bidi)
		if s.loc[t].blocked == s.loc[t].limit+1 {
			s.pend.streamsBlocked[t] = true
		}
	case dGoAway:
		if s.ga.sent {
			s.pend.goaway = true
		}
	case dPrio:
		if st := s.streams[d.id]; st != nil && st.ext != nil && st.ext.reqAcked < st.ext.reqSeq {
			s.queueCtl(st, ctlPrio)
		}
	case dStatus:
		if p := s.pipe(d.id); p != nil && p.mp != nil && !p.mp.retired {
			s.statusLost(p, d.val)
		}
	case dAbandon:
		if p := s.pipe(d.id); p != nil && p.mp != nil && !p.mp.abandonAcked {
			p.mp.abandonQueued = true
		}
	}
	s.wakeAllWriters()
}

// dropResend removes [lo, hi) from the resend set.
func (s *Session) dropResend(m *mpSend, lo, hi uint64) {
	out := m.resend[:0]
	for _, r := range m.resend {
		if r.hi <= lo || r.lo >= hi {
			out = append(out, r)
			continue
		}
		if r.lo < lo {
			out = append(out, rng{r.lo, lo})
		}
		if r.hi > hi {
			out = append(out, rng{hi, r.hi})
		}
	}
	m.resend = out
}

// retransmitBudget is SCHEDULER.md 13.1.
func (s *Session) retransmitBudget() uint64 {
	var sum float64
	pd := 25 * Millisecond
	if s.ack != nil {
		pd = s.ack.peerDelay
	}
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || p.out == nil || !a.liveOut() {
			continue
		}
		r := p.out.snd.re.value()
		sum += r * float64(a.rtt.smoothed+pd) / 1e9
	}
	b := uint64(4 * sum)
	max := s.cfg.SendBufferMax
	if max == 0 {
		max = 64 << 20
	}
	return min64(max64(b, 1<<20), max64(max, 1<<20))
}
