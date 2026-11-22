package core

// Retention of unacknowledged information: every
// ack-eliciting frame with a reinjection rule is logged per outbound lane with
// the position that ends its record; a PIPE_ACK pops the entries it covers
// (acked), a suspect or failed pipe turns the rest into "send this scope again"
// (lost).

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
	dAbandonID // PIPE_ABANDON for an ID without attachment state; val: its code
)

type frameDesc struct {
	kind   descKind
	bidi   bool
	fin    bool
	fresh  bool   // original application data under spread ETA
	repair bool   // an anticipatory prefix copy on a different pipe
	id     uint64 // stream ID, or pipe ID for PIPE_STATUS / PIPE_ABANDON
	off    uint64
	n      uint64
	val    uint64
}

type retained struct {
	end        uint64 // lane position after the record carrying the frame
	d          frameDesc
	reinjected bool
	// penalized: re-sent once because a penalty of its pipe moved the
	// binding of its stream direction away.
	penalized  bool
	sent       Time     // when the batch was handed to the carrier
	due        Duration // E: the acknowledgement time expected at sending, for penalization
	confirm    Time     // frozen prefix repair deadline, 0 for other entries
	ackBase    uint64   // lane ACK point when handed to the carrier
	progressAt Time     // last checkpoint with meaningful ACK progress
	serial     uint64   // stable lane-log index while its entries are retained
}

// mpSend is the multipath side storage of a sending part.
type mpSend struct {
	acked      rangeSet // acknowledged byte ranges
	resend     rangeSet // ranges to send again
	rr         []rrVal  // own round-robin values
	rrFill     rrFill   // the own round-robin chunk being filled
	oppSent    rangeSet // bytes re-sent opportunistically (once each)
	repair     prefixRepair
	prefix     *prefixTrack
	pick       pickAnswer // custom policy answer
	bind       binding    // affinity
	openPipe   uint64     // the pipe of the opening frame or of its latest reinjected copy, when openSet
	openSet    bool
	openAcked  bool // the opening frame was acknowledged
	finAcked   bool
	finResend  bool
	pickQueued bool
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
func (s *Session) retainBatch(w *recWriter, ol *outLane, start, ahead uint64) {
	if len(w.descs) == 0 {
		return
	}
	m := s.mp
	ends := m.endsBuf[:0]
	pos := start
	for _, r := range w.b.recs {
		pos += uint64(recordPositionBytes(r.size))
		ends = append(ends, pos)
	}
	m.endsBuf = ends
	// E = (unsent_ahead + L) / R_p + min_rtt_p + peer max_ack_delay,
	// fixed at sending so that a pipe that slows down cannot raise it.
	var due Duration
	if p := w.pipe; p != nil && p.mp != nil {
		due = timeAt(ahead+(pos-start), s.rateOf(p)) + p.mp.minRTT() + s.ack.peerDelay
	}
	var confirm Time
	var serial uint64
	if len(ol.log) > 0 {
		serial = ol.log[len(ol.log)-1].serial + 1
	}
	var st *Stream
	for _, rd := range w.descs {
		e := pos
		if rd.rec < len(ends) {
			e = ends[rd.rec]
		}
		if rd.d.fresh && confirm == 0 {
			confirm = s.prefixDeadline(w.pipe, ahead, pos-start)
		}
		r := retained{end: e, d: rd.d, sent: s.now, due: due, serial: serial}
		serial++
		if rd.d.fresh && confirm != Never && ol.prefixCount < ^uint32(0) {
			if st == nil || st.id != rd.d.id {
				st = s.sendOf(&rd.d)
			}
			if st != nil {
				r.confirm, r.ackBase = confirm, ol.acked
				ol.prefixCount++
				s.trackPrefix(st, w.pipe, ol, &r)
			}
		}
		ol.log = append(ol.log, r)
	}
	if s.cfg.TraceStreams {
		for _, rd := range w.descs {
			if d := rd.d; d.kind == dStream {
				s.emit(Event{Kind: EvStreamPlaced, Pipe: w.b.Pipe, Stream: d.id,
					Offset: d.off, Count: d.n, At: s.now})
			}
		}
	}
	w.descs = w.descs[:0]
}

// ackLog pops the entries a lane acknowledgement covers.
func (s *Session) ackLog(ol *outLane, pos uint64) {
	i := 0
	for i < len(ol.log) && ol.log[i].end <= pos {
		if ol.log[i].confirm != 0 {
			ol.log[i].confirm = 0
			ol.prefixCount--
		}
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
// again, at most once per entry.
func (s *Session) reinjectPipe(p *Pipe) {
	if p.out == nil {
		return
	}
	episode := false
	for _, ol := range p.out.lanes {
		for i := range ol.log {
			e := &ol.log[i]
			if e.reinjected {
				continue
			}
			e.reinjected, episode = true, true
			if e.confirm != 0 {
				e.confirm = 0
				ol.prefixCount--
				if st := s.sendOf(&e.d); st != nil && st.snd.mps.prefix != nil {
					s.checkPrefixAt(maxT(1, s.now))
				}
			}
			if e.d.kind == dStream {
				p.mp.reinjectedBytes += e.d.n
				if st := s.sendOf(&e.d); st != nil && st.snd.mps != nil {
					m := st.snd.mps
					if m.repair.source == p {
						s.releasePrefixTail(m)
						m.repair.source = nil
					}
				}
			}
			s.lost(&e.d)
		}
	}
	if episode {
		s.stats.Reinjections++
	}
	s.wakeAllWriters()
}

// streamOf returns the stream a retained frame is about, nil once the
// stream was discarded: a retained frame outlives its stream, so every
// reader of a logged stream frame takes the stream through it.
func (s *Session) streamOf(d *frameDesc) *Stream { return s.streams[d.id] }

// sendOf is streamOf for a frame of the sending part (STREAM).
func (s *Session) sendOf(d *frameDesc) *Stream {
	if st := s.streamOf(d); st != nil && st.hasS {
		return st
	}
	return nil
}

// acked applies the acknowledgement of one frame.
func (s *Session) acked(d *frameDesc) {
	switch d.kind {
	case dStream:
		st := s.sendOf(d)
		if st == nil {
			return
		}
		sp := &st.snd
		m := sp.mpState()
		if d.n > 0 {
			m.acked.add(d.off, d.off+d.n)
			if m.resend.len() != 0 {
				m.resend.remove(d.off, d.off+d.n)
			}
			s.ackPrefixRepair(m)
		}
		if d.off == 0 && !m.openAcked {
			m.openAcked = true
			if s.dg != nil && len(s.dg.send) > 0 {
				s.wakeAllWriters() // its datagrams go by the usual rule now
			}
		}
		if d.fin {
			m.finAcked, m.finResend = true, false
		}
		s.streamAckProgress(st)
		if !st.sendable() {
			s.sched.remove(st)
		}
	case dReset:
		if st := s.streamOf(d); st != nil && st.hasS && st.snd.resetSent {
			s.sendTerminal(st, sendResetRecvd)
		}
	case dPrio:
		if st := s.streamOf(d); st != nil && st.ext != nil && st.ext.reqSent == d.val {
			st.ext.reqAcked = d.val
		}
	case dAbandon:
		if p := s.pipe(d.id); p != nil && p.mp != nil {
			p.mp.abandonAcked = true
			p.mp.ackedAbandonAt = s.now
			s.maybeRetire(p)
		}
	case dStop, dMaxData, dMaxStreamData, dMaxStreams, dDataBlocked, dStreamDataBlocked,
		dStreamsBlocked, dGoAway, dStatus, dAbandonID:
		// Delivered; nothing waits on the acknowledgement.
	}
}

// streamAckProgress releases acknowledged bytes and finishes the part.
func (s *Session) streamAckProgress(st *Stream) {
	sp := &st.snd
	m := sp.mps
	pre := m.acked.prefix()
	if m.oppSent.len() > 0 {
		m.oppSent.remove(0, pre)
	}
	if pre > sp.q.base {
		s.schedulePrefix(st, pre)
		rel := sp.q.base
		h := s.heldEnter(st)
		sp.q.release(min64(pre, sp.q.end))
		s.heldLeave(st, h)
		s.mp.release(min64(sp.q.base, sp.sent) - min64(rel, sp.sent))
		s.sendChanged(st)
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
		st := s.sendOf(d)
		if st == nil {
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
		if d.repair && m.repair.covers(d.off) {
			// A failed copy is itself retained on its carrier. Its loss
			// hands the range back to ordinary reinjection.
			s.clearPrefixRepair(m)
		}
		// The original's loss must leave a live repair copy alone.
		// Unsent repair bytes and any remaining tail use the same queue.
		repaired := m.repair
		queue := func(a, b uint64) {
			m.acked.gaps(a, b, func(x, y uint64) { m.resend.add(x, y) })
		}
		if repaired.sent > repaired.lo && lo < repaired.sent && hi > repaired.lo {
			queue(lo, min64(hi, repaired.lo))
			queue(max64(lo, repaired.sent), hi)
		} else {
			queue(lo, hi)
		}
		if d.fin && !m.finAcked && !sp.reset {
			m.finResend = true
		}
		s.sched.ensure(st)
	case dReset:
		if st := s.streamOf(d); st != nil && st.hasS && st.snd.reset && st.snd.state != sendResetRecvd {
			s.queueCtl(st, ctlReset)
		}
	case dStop:
		if st := s.streamOf(d); st != nil && st.hasR && st.rcv.stopped &&
			st.rcv.state != recvDataRead && st.rcv.state != recvResetRead {
			s.queueCtl(st, ctlStop)
		}
	case dMaxData:
		s.pend.maxData = true
		s.pend.forceMaxData = true
	case dMaxStreamData:
		if st := s.streamOf(d); st != nil && st.hasR && st.rcv.state == recvRecv {
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
		if st := s.streamOf(d); st != nil && st.snd.blocked == st.snd.maxData+1 {
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
		if st := s.streamOf(d); st != nil && st.ext != nil && st.ext.reqAcked < st.ext.reqSeq {
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
	case dAbandonID:
		// The same frame until acknowledged (MULTIPATH.md Section 7.4),
		// while the ID is still in the replay window.
		if m := s.mp; s.pipe(d.id) == nil && !m.stale(d.id) {
			s.prepareAdditional().pendAbandon[d.id] = d.val
		}
	}
	s.wakeAllWriters()
}

// dropResend removes [lo, hi) from the resend set.
func (s *Session) dropResend(m *mpSend, lo, hi uint64) { m.resend.remove(lo, hi) }

// retransmitBudget bounds the retained unacknowledged stream data: four
// times the summed delivery rate times (smoothed RTT + the peer's ack
// delay) of the live outbound pipes, at least 1 MiB and at most the send
// buffer.
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
	return min64(max64(b, 1<<20), max64(s.sendBufferLimit(), 1<<20))
}

// Retention accounting (multipath only): stream bytes sent and not yet
// acknowledged, bounded by retransmitBudget. The methods are no-ops on a
// session without multipath (nil m).

// retain counts n newly sent stream bytes.
func (m *multiPath) retain(n uint64) {
	if m != nil {
		m.retained += n
	}
}

// release uncounts n acknowledged or dropped stream bytes.
func (m *multiPath) release(n uint64) {
	if m != nil {
		m.retained -= min64(m.retained, n)
	}
}

// retainedBytes is the retained stream data.
func (m *multiPath) retainedBytes() uint64 {
	if m == nil {
		return 0
	}
	return m.retained
}

// capRetained caps n new stream bytes to what the retransmit budget still
// takes.
func (s *Session) capRetained(n uint64) uint64 {
	if s.mp == nil {
		return n
	}
	b := s.retransmitBudget()
	return min64(n, b-min64(b, s.mp.retained))
}
