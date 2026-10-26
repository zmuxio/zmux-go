package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
)

// maxStreamFrame is the largest STREAM frame payload (IMPLEMENTATION.md 9.1).
const maxStreamFrame = 16384

// DefaultBatch is the write batch (IMPLEMENTATION.md 4).
const DefaultBatch = 256 << 10

// recMax returns the largest Size for records on an outbound lane.
func (s *Session) recMax(ol *outLane) int {
	m := int(min64(s.peerMaxRecord, 65535))
	if !s.peerSeen {
		m = wire.DefaultMaxRecordSize
	}
	if ol.info.TLS && m > 16382 {
		m = 16382
	}
	return m
}

// WantsWrite reports whether PollTransmit(pipe, lane) would produce bytes
// now (IMPLEMENTATION.md 13.1 W8).
func (s *Session) WantsWrite(pipe, lane uint64) bool {
	p := s.pipe(pipe)
	if p == nil || p.out == nil || p.out.failed {
		return false
	}
	ol := p.out.lane(lane)
	if ol == nil || ol.shut || ol.closeSent || s.state == StateClosed {
		return false
	}
	if pipe == 0 && lane == 0 && !ol.tpSent {
		return true
	}
	if s.closeFr.pending {
		return lane == 0 && !s.closeFr.waitLanes
	}
	if ol.laneFirst {
		return s.state < StateClosing
	}
	if !ol.confirmed {
		return false
	}
	if s.state == StateClosing {
		return false
	}
	if !s.peerSeen {
		return s.ping.wants() || s.earlyActive() && s.earlyWants()
	}
	if s.mp != nil {
		return s.mpWantsWrite(p, ol)
	}
	if lane == 0 && s.controlPending(p) {
		return true
	}
	return s.dataPendingOn(p, ol) && !s.limHeld(p)
}

// dataPendingOn reports stream data that would go on the lane now.
func (s *Session) dataPendingOn(p *Pipe, ol *outLane) bool {
	if s.sched.mask == 0 {
		return false
	}
	it := s.sched.iter()
	for st := it.next(); st != nil; st = it.next() {
		if !s.canSendNow(st) {
			continue
		}
		if p == nil || !p.out.lo.offered {
			return true
		}
		if l, _, _ := s.laneFor(p, st); l == ol {
			return true
		}
	}
	return false
}

func (s *Session) controlPending(p *Pipe) bool {
	if s.pend.maxData || s.pend.maxStreams[0] || s.pend.maxStreams[1] || s.pend.dataBlocked ||
		s.pend.streamsBlocked[0] || s.pend.streamsBlocked[1] || s.pend.goaway || s.ctlHead != nil {
		return true
	}
	if s.ping.wants() {
		return true
	}
	if s.ack != nil && s.mp == nil && s.ack.due(s.now) {
		return true
	}
	return s.dg != nil && s.dg.sendable(s)
}

func (s *Session) dataPending(_ *outLane) bool { return s.dataPendingOn(nil, nil) }

// canSendNow reports whether the stream could put a frame out now; it uses
// the same plan as sendChunk so that write interest never spins (W8).
func (s *Session) canSendNow(st *Stream) bool {
	if !st.idSet {
		t := typeIndex(st.bidi)
		if !s.peerSeen && !s.earlyActive() || s.loc[t].next >= s.loc[t].limit {
			return false
		}
		// The opening frame needs the whole preamble within credit.
		pre := uint64(0)
		if s.feat.Preamble {
			pre = uint64(st.preSize())
		}
		return pre == 0 || s.conn.sendMax-min64(s.conn.sendMax, s.conn.sent) >= pre
	}
	pl := s.planChunk(st)
	return pl.n > 0 || pl.fin || pl.open
}

// chunkPlan is what a stream could send now, before record room.
type chunkPlan struct {
	off, n     uint64
	fin, open  bool
	resend     bool
	avail      uint64
	credit     uint64
	connCredit uint64
	preBlocked bool
}

// planResend plans a frame of reinjected stream data (MULTIPATH.md 7.4).
func (s *Session) planResend(st *Stream) (chunkPlan, bool) {
	sp := &st.snd
	m := sp.mps
	var p chunkPlan
	p.resend = true
	for len(m.resend) > 0 {
		r := m.resend[0]
		lo, hi := max64(r.lo, sp.q.base), min64(r.hi, sp.q.end)
		if sp.reset {
			hi = min64(hi, sp.resetRel)
		}
		if lo >= hi {
			m.resend = m.resend[1:]
			continue
		}
		p.off, p.n = lo, min64(hi-lo, maxStreamFrame)
		if lo == 0 && sp.preLen > p.n {
			p.n = min64(sp.preLen, sp.q.end)
		}
		p.fin = m.finResend && p.off+p.n == sp.q.end && sp.finSent && !sp.reset
		return p, true
	}
	if m.finResend && !sp.reset && sp.finSent {
		p.off, p.fin = sp.q.end, true
		return p, true
	}
	m.finResend = false
	return p, false
}

// planChunk computes the next frame of a stream that has an identifier.
func (s *Session) planChunk(st *Stream) chunkPlan {
	sp := &st.snd
	var p chunkPlan
	if m := sp.mps; m != nil && (len(m.resend) > 0 || m.finResend) {
		if rp, ok := s.planResend(st); ok {
			return rp
		}
	}
	p.off = max64(sp.sent, sp.q.base)
	if end := sp.sendEnd(); end > p.off {
		p.avail = end - p.off
	}
	p.credit = sp.maxData - min64(sp.maxData, p.off)
	p.connCredit = s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent)
	p.n = min64(p.avail, min64(p.credit, p.connCredit))
	if p.n > maxStreamFrame {
		p.n = maxStreamFrame
	}
	if s.mp != nil && p.off >= sp.sent {
		b := s.retransmitBudget()
		if s.mp.retained >= b {
			p.n = 0
		} else {
			p.n = min64(p.n, b-s.mp.retained)
		}
	}
	// The first frame carries the whole preamble (EXTENSIONS.md 4.2).
	if p.off == 0 && sp.preLen > 0 && p.n < sp.preLen {
		p.n = 0
		p.preBlocked = true
		return p
	}
	if p.off+p.n == sp.q.end && sp.finWr && !sp.finSent && !sp.reset {
		p.fin = true
	}
	p.open = p.n == 0 && !p.fin && !st.opened && st.pinned
	return p
}

// PollTransmit fills b with records for the outbound lane, at most max
// bytes. It returns false when nothing is due.
func (s *Session) PollTransmit(pipe, lane uint64, b *Batch, max int, now Time) bool {
	s.SetNow(now)
	b.reset()
	b.Pipe, b.Lane = pipe, lane
	p := s.pipe(pipe)
	if p == nil || p.out == nil || p.out.failed || s.state == StateClosed {
		return false
	}
	ol := p.out.lane(lane)
	if ol == nil || ol.shut {
		return false
	}
	b.Contiguous = ol.info.TLS
	var w recWriter
	w.init(b, ol.info.Framing, s.recMax(ol), max)
	if s.ack != nil && s.peerSeen {
		w.dataRoom = s.paceRoom(p)
		if w.dataRoom > 0 && !s.dataTurn(p, ol) {
			w.dataRoom = 0
		}
	}
	if pipe == 0 && lane == 0 && !ol.tpSent {
		s.writeParams(&w, ol)
	}
	switch {
	case ol.laneFirst:
		if s.state < StateClosing {
			s.writeLaneFirst(&w, p, ol)
		}
	case lane > 0 && !ol.confirmed:
	case s.closeFr.pending && !ol.closeSent && lane == 0:
		if !s.closeFr.waitLanes {
			s.writeClose(&w, ol)
		}
	case s.state == StateClosing || ol.closeSent:
	case !s.peerSeen:
		s.writePing(&w)
		if s.earlyActive() {
			s.writeEarly(&w)
		}
	case s.mp != nil:
		s.mpWrite(&w, p, ol)
	default:
		if lane == 0 {
			s.writeControl(&w, p)
		}
		s.writeData(&w, p, ol)
		if lane == 0 && (s.pend.dataBlocked || s.pend.streamsBlocked[0] || s.pend.streamsBlocked[1] || s.ctlHead != nil) {
			s.writeControl(&w, p)
		}
	}
	w.end()
	if w.dataSpent > 0 {
		s.tookTurn(p, ol)
	}
	if b.n == 0 {
		if s.ack != nil && !ol.notWritable {
			// A write opportunity that ends with nothing to send: an
			// application-limited moment (SCHEDULER.md 2.3).
			p.out.snd.re.appLimited(p.out.snd.so)
		}
		return false
	}
	s.afterBatch(&w, p, ol)
	if s.ack != nil {
		if (w.budget < 1024 || w.dataRoom == 0) && s.dataPending(ol) {
			p.out.snd.re.blocked() // the write runs, or the limiter holds, while more data waits
		} else {
			p.out.snd.re.appLimited(p.out.snd.so)
		}
	}
	ol.writing = true
	if !ol.notWritable {
		ol.stallAt = now
	}
	return true
}

// afterBatch accounts positions and statistics of a produced batch.
func (s *Session) afterBatch(w *recWriter, p *Pipe, ol *outLane) {
	var pos int
	for _, r := range w.b.recs {
		pos += recordPositionBytes(ol.info.Framing, r.size)
	}
	start := ol.pos
	ol.pos += uint64(pos)
	for _, st := range w.touched {
		st.snd.laneEnd = ol.pos
	}
	s.stats.BytesSent += uint64(pos)
	if s.ack != nil && p != nil && p.out != nil {
		defer s.paceSent(p, uint64(pos))
	}
	s.stats.RecordsSent += uint64(w.recs)
	if p != nil && p.out != nil {
		p.out.records += uint64(w.recs)
	}
	if s.ack != nil {
		s.ack.onSent(p, ol, start, w, s.now)
	}
	if p.mp != nil {
		s.mpAfterBatch(w, p, ol, start)
	}
	if w.closed && !w.laneOnly {
		w.b.Shutdown = true
		s.closeSentOn()
	}
	// Session without acknowledgement: a record passed to the carrier
	// restarts the idle timer unless it carries only QX_PING requests and
	// PADDING (SPEC.md 6.4).
	if !s.feat.Acked() && !w.onlyPing {
		s.timers.lastActivity = s.now
	}
}

// WriteDone reports that a batch was written (or failed); it releases the
// batch's references. n is the bytes the carrier took.
func (s *Session) WriteDone(b *Batch, n int, err error, now Time) {
	s.SetNow(now)
	b.release()
	if p := s.pipe(b.Pipe); p != nil && p.out != nil {
		if ol := p.out.lane(b.Lane); ol != nil {
			ol.writing = false
			if ol.notWritable {
				ol.stallAt = now
			}
		}
	}
	if err != nil {
		s.pipeWriteFailed(b.Pipe, b.Lane, err)
	}
}

// writeParams writes the first record of the bootstrap pipe (SPEC.md 5.1).
func (s *Session) writeParams(w *recWriter, ol *outLane) {
	block := wire.AppendParams(nil, &s.cfg.Local)
	n := wire.VarintLen(wire.TypeTransportParams) + wire.VarintLen(uint64(len(block))) + len(block)
	w.frame(n, true, func(b []byte) []byte { return wire.AppendTransportParams(b, block) })
	ol.tpSent = true
	if !ol.info.MinRTT && !s.cfg.Test.Enabled && s.ping.outstanding == 0 {
		s.ping.requestFirst = true
	}
}

// writeClose writes CONNECTION_CLOSE; nothing follows it on the pipe.
func (s *Session) writeClose(w *recWriter, ol *outLane) {
	c := &s.closeFr
	n := wire.CloseLen(c.app, c.code, c.frameType, len(c.reason))
	w.end()
	if !w.frame(n, false, func(b []byte) []byte {
		return wire.AppendClose(b, c.app, c.code, c.frameType, c.reason)
	}) {
		return
	}
	ol.closeSent = true
	w.closed = true
}

// closeSentOn starts the linger timer once CONNECTION_CLOSE was handed to
// every live outbound pipe (SPEC.md 6.5 step 3).
func (s *Session) closeSentOn() {
	for _, p := range s.pipes {
		if !s.canCarryClose(p) {
			continue
		}
		if l := p.out.lane(0); l != nil && !l.closeSent {
			return
		}
	}
	s.closeFr.pending = false
	s.closeFr.sentAll = true
	for _, p := range s.pipes {
		if p.out == nil {
			continue
		}
		for _, l := range p.out.lanes {
			if !l.shut {
				l.shut = true
				s.act(Action{Kind: ActShutdown, Pipe: p.id, Lane: l.lane, Dir: DirOut})
			}
		}
	}
	s.timers.linger = s.now.Add(s.lingerTimeout())
	s.emitClosedEvent()
	s.checkLingerDone()
}

// checkLingerDone enters CLOSED once every inbound pipe ended.
func (s *Session) checkLingerDone() {
	if s.state != StateClosing || s.closeFr.pending {
		return
	}
	for _, p := range s.pipes {
		if p.in != nil && !p.in.ended {
			return
		}
	}
	s.finishClose()
}

// writeControl writes the coalesced control state (SPEC.md 9.2) ahead of
// data, in the order of IMPLEMENTATION.md 4.
func (s *Session) writeControl(w *recWriter, p *Pipe) {
	if s.ack != nil && s.mp == nil {
		s.ack.write(w, p, s.now)
	}
	s.writePingResponse(w)
	if s.pend.maxData {
		s.tuneConnUpdate()
		v := s.conn.rel + s.conn.win
		if v > s.conn.adv || s.conn.adv == 0 || s.pend.forceMaxData {
			if !w.frame(1+wire.VarintLen(v), true, func(b []byte) []byte { return wire.AppendMaxData(b, v) }) {
				return
			}
			w.logFrame(frameDesc{kind: dMaxData})
			s.pend.forceMaxData = false
			if s.mp != nil && (s.conn.peerBlock || s.conn.recv >= s.conn.adv) {
				s.copyMaxData = true
			}
			if t := s.conn.tune; t != nil && v > s.conn.adv {
				t.sent(s.conn.adv, s.now, s.conn.recv == s.conn.adv)
			}
			s.conn.adv = v
			s.conn.peerBlock = false
		}
		s.pend.maxData = false
	}
	for t := range 2 {
		if s.pend.maxStreams[t] {
			v := s.rem[t].limit
			if !w.frame(1+wire.VarintLen(v), true, func(b []byte) []byte { return wire.AppendMaxStreams(b, t == 0, v) }) {
				return
			}
			w.logFrame(frameDesc{kind: dMaxStreams, bidi: t == 0})
			s.pend.maxStreams[t] = false
		}
	}
	for st := s.ctlHead; st != nil; {
		next := st.ctlNext
		if !s.writeStreamCtl(w, st) {
			return
		}
		st = next
	}
	if s.pend.dataBlocked && s.conn.blocked == s.conn.sendMax+1 {
		v := s.conn.sendMax
		if !w.frame(1+wire.VarintLen(v), true, func(b []byte) []byte { return wire.AppendDataBlocked(b, v) }) {
			return
		}
		w.logFrame(frameDesc{kind: dDataBlocked})
	}
	s.pend.dataBlocked = false
	for t := range 2 {
		if s.pend.streamsBlocked[t] && s.loc[t].blocked == s.loc[t].limit+1 {
			v := s.loc[t].limit
			if !w.frame(1+wire.VarintLen(v), true, func(b []byte) []byte { return wire.AppendStreamsBlocked(b, t == 0, v) }) {
				return
			}
			w.logFrame(frameDesc{kind: dStreamsBlocked, bidi: t == 0})
		}
		s.pend.streamsBlocked[t] = false
	}
	if s.pend.goaway {
		g := &s.ga
		n := 4 + wire.VarintLen(g.sentWater[0]) + wire.VarintLen(g.sentWater[1]) + wire.VarintLen(g.code)
		if !w.frame(n, true, func(b []byte) []byte { return wire.AppendGoAway(b, g.sentWater[0], g.sentWater[1], g.code) }) {
			return
		}
		w.logFrame(frameDesc{kind: dGoAway})
		s.pend.goaway = false
		s.goawaySent()
	}
	s.writePing(w)
	if s.dg != nil {
		s.writeDatagrams(w)
	}
}

// writeStreamCtl writes the pending control frames of one stream; it
// returns false when the batch is full.
func (s *Session) writeStreamCtl(w *recWriter, st *Stream) bool {
	id := st.id
	if st.ctl&ctlMaxStreamData != 0 {
		r := &st.rcv
		s.tuneStreamUpdate(st)
		v := r.readOff + r.window
		if !r.finKnown && !r.stopped && (v > r.adv || st.forceMSD) {
			if !w.frame(1+wire.VarintLen(id)+wire.VarintLen(v), true,
				func(b []byte) []byte { return wire.AppendMaxStreamData(b, id, v) }) {
				return false
			}
			w.logFrame(frameDesc{kind: dMaxStreamData, id: id})
			if s.mp != nil && (r.highest >= r.adv || r.blockSeen == r.adv && r.blockSeen > 0) {
				st.copyMSD = true
			}
			if r.tune != nil && v > r.adv {
				r.tune.sent(r.adv, s.now, r.highest == r.adv)
			}
			r.adv = max64(r.adv, v)
			st.forceMSD = false
		}
		st.ctl &^= ctlMaxStreamData
	}
	if st.ctl&ctlStop != 0 {
		r := &st.rcv
		c := r.stopCode
		if !w.frame(1+wire.VarintLen(id)+wire.VarintLen(c), true,
			func(b []byte) []byte { return wire.AppendStopSending(b, id, c) }) {
			return false
		}
		w.logFrame(frameDesc{kind: dStop, id: id})
		r.stopSent = true
		st.ctl &^= ctlStop
	}
	if st.ctl&ctlReset != 0 {
		sp := &st.snd
		// Outside multipath the data below the Reliable Size precedes the
		// reset on the pipe (SPEC.md 4.3).
		if sp.sent >= sp.resetRel {
			final := max64(sp.sent, sp.resetRel)
			code, rel, at := sp.resetCode, sp.resetRel, sp.resetAt
			n := 1 + wire.VarintLen(id) + wire.VarintLen(code) + wire.VarintLen(final)
			if at {
				n += wire.VarintLen(rel)
			}
			if !w.frame(n, true, func(b []byte) []byte {
				if at {
					return wire.AppendResetStreamAt(b, id, code, final, rel)
				}
				return wire.AppendResetStream(b, id, code, final)
			}) {
				return false
			}
			sp.resetSent = true
			st.ctl &^= ctlReset
			w.logFrame(frameDesc{kind: dReset, id: id})
			if s.conn.sent < s.conn.sent+(final-sp.sent) {
				s.conn.sent += final - sp.sent
			}
			sp.sent = final
			w.activity = true
			w.onlyPing = false
			// Outside multipath a written reset is acknowledged (SPEC.md 7.4).
			if !s.feat.Multipath {
				s.sendTerminal(st, sendResetRecvd)
			}
		}
	}
	if st.ctl&ctlPrio != 0 {
		x := st.x()
		seq, u, inc := x.reqSeq, x.reqUrgency, x.reqIncr
		n := 4 + wire.VarintLen(id) + wire.VarintLen(seq) + 1
		if !w.frame(n, true, func(b []byte) []byte { return wire.AppendPriorityUpdate(b, id, seq, u, inc) }) {
			return false
		}
		w.logFrame(frameDesc{kind: dPrio, id: id, val: seq})
		x.reqSent = seq
		st.ctl &^= ctlPrio
	}
	if st.ctl&ctlBlocked != 0 && st.snd.blocked == st.snd.maxData+1 {
		v := st.snd.maxData
		if !w.frame(1+wire.VarintLen(id)+wire.VarintLen(v), true,
			func(b []byte) []byte { return wire.AppendStreamDataBlocked(b, id, v) }) {
			return false
		}
		w.logFrame(frameDesc{kind: dStreamDataBlocked, id: id})
	}
	st.ctl &^= ctlBlocked
	if st.ctl == 0 {
		s.unlinkCtl(st)
	}
	return true
}

// writeData writes stream data in priority order; with lanes, only the
// directions bound to ol.
func (s *Session) writeData(w *recWriter, p *Pipe, ol *outLane) {
	lanes := p != nil && p.out.lo.offered
	var d *decision
	if p != nil && p.mp != nil {
		d = &s.mp.dec // prepared by mpWrite
	}
	for w.room() > 0 && s.sched.mask != 0 {
		progressed := false
		it := s.sched.iter()
		for st := it.next(); st != nil; st = it.next() {
			if d != nil && (!s.canSendNow(st) || !s.onPipe(d, st)) {
				continue
			}
			if lanes {
				if !s.canSendNow(st) {
					continue
				}
				if l, _, _ := s.laneFor(p, st); l != ol {
					s.bindLane(p, st)
					continue
				}
				s.bindLane(p, st)
			}
			r := s.sendChunk(w, st, d)
			if r == chunkFull {
				return
			}
			if r == chunkSent {
				if lanes {
					w.touched = append(w.touched, st)
				}
				progressed = true
				break
			}
		}
		if !progressed {
			return
		}
	}
}

type chunkResult uint8

const (
	chunkSent chunkResult = iota
	chunkSkip
	chunkFull
)

// sendChunk puts the next frame of st into the record.
func (s *Session) sendChunk(w *recWriter, st *Stream, d *decision) chunkResult {
	sp := &st.snd
	if !st.idSet {
		if !s.canSendNow(st) {
			t := typeIndex(st.bidi)
			if s.peerSeen && s.loc[t].next >= s.loc[t].limit {
				s.markStreamsBlocked(t)
			}
			if (s.peerSeen || s.earlyActive()) && s.feat.Preamble && s.loc[t].next < s.loc[t].limit {
				// The preamble does not fit the session credit: allocate
				// and report the block (SPEC.md 8.4).
				s.allocID(st)
				s.blockedOn(st, false, true)
			}
			return chunkSkip
		}
		s.allocID(st)
	}
	p := s.planChunk(st)
	if p.preBlocked {
		if p.avail >= sp.preLen || sp.finWr {
			s.blockedOn(st, p.credit < sp.preLen, p.connCredit < sp.preLen)
		}
		return chunkSkip
	}
	if p.n == 0 && !p.fin && !p.open {
		if p.avail > 0 {
			s.blockedOn(st, p.credit == 0, p.connCredit == 0)
		}
		if !st.sendable() || p.credit == 0 && p.avail > 0 {
			s.sched.remove(st)
		}
		return chunkSkip
	}
	if w.dataRoom == 0 && (p.n > 0 || p.fin) {
		return chunkFull // the limiter holds the pipe's data back
	}
	off, n, fin := p.off, p.n, p.fin
	if d != nil && !p.resend && n > uint64(s.cfg.SpreadChunk) {
		// One scheduling decision per chunk (SCHEDULER.md 5).
		n, fin = uint64(s.cfg.SpreadChunk), false
		if off == 0 && sp.preLen > n {
			n = sp.preLen
		}
	}
	room := w.room() - wire.StreamHeaderLen(st.id, off, int(n), true)
	if room <= 0 {
		return chunkFull
	}
	if n > uint64(room) {
		if off == 0 && sp.preLen > 0 && uint64(room) < sp.preLen {
			return chunkFull
		}
		n = uint64(room)
		fin = false
	}
	var data []byte
	var blk pin
	want, wantFin := n, fin
	if n > 0 {
		data, blk = sp.q.peek(off, int(n))
		if uint64(len(data)) < n {
			n = uint64(len(data))
			fin = false
		}
	}
	if !w.stream(st.id, off, data, blk, fin) {
		return chunkFull
	}
	// The rest of the chunk from the following buffer segments joins the
	// frame (the writer coalesces contiguous data of one stream).
	for n < want {
		more, mblk := sp.q.peek(off+n, int(want-n))
		if len(more) == 0 {
			break
		}
		f := wantFin && n+uint64(len(more)) == want
		if !w.stream(st.id, off+n, more, mblk, f) {
			break
		}
		n += uint64(len(more))
		fin = f
	}
	w.logFrame(frameDesc{kind: dStream, id: st.id, off: off, n: n, fin: fin})
	w.spendData(n)
	if p.resend {
		if d != nil {
			d.p.mp.holdSent += n
		}
		m := sp.mps
		s.dropResend(m, off, off+n)
		if fin {
			m.finResend = false
		}
		if !st.sendable() {
			s.sched.remove(st)
		}
		return chunkSent
	}
	st.opened = true
	if s.dg != nil && len(s.dg.send) > 0 {
		s.wakeAllWriters() // its held datagrams may follow
	}
	if s.early != nil && !s.peerSeen {
		s.early.sentAny = true
	}
	if sp.state == sendReady {
		sp.state = sendSend
	}
	if d != nil {
		s.chunkSent(d, w, st, off, n, fin)
		defer s.renewDecision(d)
	}
	newEnd := off + n
	if newEnd > sp.sent {
		s.conn.sent += newEnd - sp.sent
		if s.mp != nil {
			s.mp.retained += newEnd - sp.sent
		}
		sp.sent = newEnd
	}
	if fin {
		sp.finSent = true
		sp.state = sendDataSent
	}
	if st.ext != nil && st.ext.capSig != nil {
		s.checkCapacity(st)
	}
	if s.feat.Multipath {
		sp.mpState()
	} else if s.holdSends() {
		// Carrier early data: the bytes stay until the verdict.
	} else {
		h := s.heldEnter(st)
		sp.q.release(sp.sent)
		s.heldLeave(st, h)
		s.wake(st.wsig)
		if fin {
			s.sendTerminal(st, sendDataRecvd)
		}
	}
	if st.incr {
		st.sch.deficit += int32(n) + 1 // n is at most a record
		if st.sch.deficit >= quantum {
			s.sched.rotate(st)
		}
	}
	if !st.sendable() {
		s.sched.remove(st)
	}
	return chunkSent
}

// blockedOn queues STREAM_DATA_BLOCKED / DATA_BLOCKED once per limit
// (SPEC.md 8.4).
func (s *Session) blockedOn(st *Stream, stream, conn bool) {
	if stream && st.snd.blocked != st.snd.maxData+1 {
		st.snd.blocked = st.snd.maxData + 1
		s.queueCtl(st, ctlBlocked)
	}
	if stream {
		s.noteBlocked(st)
	}
	if conn && s.conn.blocked != s.conn.sendMax+1 {
		s.conn.blocked = s.conn.sendMax + 1
		s.pend.dataBlocked = true
	}
}

// markStreamsBlocked queues STREAMS_BLOCKED once per limit.
func (s *Session) markStreamsBlocked(t int) {
	if !s.peerSeen {
		return
	}
	lc := &s.loc[t]
	if lc.blocked != lc.limit+1 {
		lc.blocked = lc.limit + 1
		s.pend.streamsBlocked[t] = true
		s.wakeAllWriters()
	}
}

// canCarryClose reports an outbound pipe that still takes a
// CONNECTION_CLOSE: not failed, not shut, live.
func (s *Session) canCarryClose(p *Pipe) bool {
	if p.out == nil || p.out.failed {
		return false
	}
	if l := p.out.lane(0); l == nil || l.shut {
		return false
	}
	return p.mp == nil || p.mp.liveOut() && !p.mp.retired
}
