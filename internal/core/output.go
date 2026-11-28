package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// maxStreamFrame is the largest STREAM frame payload.
const maxStreamFrame = 16384

// DefaultBatch is the write batch.
const DefaultBatch = 256 << 10

// recMax returns the largest Size for records on an outbound lane.
func (s *Session) recMax(ol *outLane) int {
	m := int(min64(s.peerMaxRecord, wire.MaxRecordSize))
	if !s.peerSeen {
		m = wire.DefaultMaxRecordSize
	}
	if ol.info.TLS && m > wire.DefaultMaxRecordSize {
		m = wire.DefaultMaxRecordSize
	}
	if ol.info.MaxRecord > 0 && m > ol.info.MaxRecord {
		m = ol.info.MaxRecord
	}
	return m
}

// WantsWrite estimates immediate output for diagnostics and simulations.
// A poll can also produce protocol feedback or opportunistic data, or find
// a shaping gate closed. Drivers schedule polls from lane wake requests.
func (s *Session) WantsWrite(pipe, lane uint64) bool {
	p := s.pipe(pipe)
	if p == nil || p.out == nil || p.out.failed || p.mp != nil && (p.mp.hs || p.mp.topologyOut) {
		return false
	}
	ol := p.out.lane(lane)
	if ol == nil || ol.shut || ol.notWritable || ol.closeSent || s.state == StateClosed || s.holdsFirst(pipe) {
		return false
	}
	if pipe == 0 && lane == 0 && !ol.tpSent {
		// The parameters lead the pipe; a shaping delay of that first
		// record holds them like any other record.
		return s.closeFr.pending || s.shp == nil || !s.shapeDelayed(p)
	}
	if s.closeFr.pending && !s.closeFr.waitLanes {
		return lane == 0
	}
	if s.shp != nil && s.shapeDelayed(p) {
		return false // the planned record waits for its delay
	}
	if ol.laneFirst {
		return s.state < StateClosing
	}
	if !ol.confirmed {
		return false
	}
	if !s.transportLive() {
		return false
	}
	if !s.peerSeen {
		return s.ping.wants() || s.earlyActive() && s.earlyWants()
	}
	if s.shp != nil && s.shapeWants(p, lane) {
		return true
	}
	if s.mp != nil {
		return s.mpWantsWrite(p, ol)
	}
	if lane == 0 && (s.controlPending() || s.nextDatagram(p, false, 7) != nil) {
		return true
	}
	return s.dataPendingOn(p, ol) && !s.limHeld(p) && (s.shp == nil || !s.shapeHeld(p))
}

// dataPendingOn reports stream data that would go on the lane now.
func (s *Session) dataPendingOn(p *Pipe, ol *outLane) bool {
	if !p.out.lo.offered {
		return s.dataPending(nil, nil)
	}
	return s.dataPending(p, ol)
}

// dataPending uses an optional lane binding. Early data has no binding.
func (s *Session) dataPending(binding *Pipe, ol *outLane) bool {
	if s.sched.mask == 0 {
		return false
	}
	it := s.sched.iter()
	for st := it.next(); st != nil; st = it.next() {
		if !s.canSendNow(st) {
			continue
		}
		if binding == nil {
			return true
		}
		if l, _, _ := s.laneFor(binding, st); l == ol {
			return true
		}
	}
	return false
}

// dataBlockedDue and streamsBlockedDue are the one test of whether a
// DATA_BLOCKED or STREAMS_BLOCKED goes out: pending and still at the limit
// it names (writeControl sends exactly when controlPending reports it).
func (s *Session) dataBlockedDue() bool {
	return s.pend.dataBlocked && s.conn.blocked == s.conn.sendMax+1
}

func (s *Session) streamsBlockedDue(t int) bool {
	return s.pend.streamsBlocked[t] && s.loc[t].blocked == s.loc[t].limit+1
}

func (s *Session) blockedDue() bool {
	return s.dataBlockedDue() || s.streamsBlockedDue(0) || s.streamsBlockedDue(1)
}

func (s *Session) controlPending() bool {
	if s.pend.maxData || s.pend.maxStreams[0] || s.pend.maxStreams[1] || s.blockedDue() ||
		s.pend.goaway || s.ctlHead != nil {
		return true
	}
	if s.ping.wants() {
		return true
	}
	if s.ack != nil && s.mp == nil && s.ack.due(s.now) {
		return true
	}
	return s.nextDatagramReport() <= s.now
}

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
	avail      uint64
	credit     uint64
	connCredit uint64
	fin, open  bool
	resend     bool
	preBlocked bool
}

// planResend plans a frame of reinjected stream data.
func (s *Session) planResend(st *Stream) (chunkPlan, bool) {
	sp := &st.snd
	m := sp.mps
	var p chunkPlan
	p.resend = true
	for m.resend.len() > 0 {
		if p, ok := s.resendRange(st, m.resend.first()); ok {
			return p, true
		}
		m.resend.popFirst()
	}
	if m.finResend && !sp.reset && sp.finSent {
		p.off, p.fin = sp.q.end, true
		return p, true
	}
	m.finResend = false
	return p, false
}

// resendRange plans the first still-missing bytes of a retained range.
func (s *Session) resendRange(st *Stream, r rng) (chunkPlan, bool) {
	sp := &st.snd
	m := sp.mps
	lo, hi := max64(r.lo, sp.q.base), min64(r.hi, sp.q.end)
	if sp.reset {
		hi = min64(hi, sp.resetRel)
	}
	if lo >= hi {
		return chunkPlan{}, false
	}
	// Acknowledged higher ranges can precede advancement of the stream prefix.
	missing := m.acked.firstGap(lo, hi)
	n := min64(missing.hi-missing.lo, maxStreamFrame)
	if n == 0 || missing.lo == 0 && n < sp.preLen {
		return chunkPlan{}, false
	}
	return chunkPlan{off: missing.lo, n: n, resend: true,
		fin: m.finResend && missing.lo+n == sp.q.end && sp.finSent && !sp.reset}, true
}

// planNativeResend preserves native loss priority while the speculative
// tail is waiting for its writer. Leave that tail in the shared queue.
func (s *Session) planNativeResend(st *Stream) (chunkPlan, bool) {
	sp := &st.snd
	m := sp.mps
	q := &m.resend.q
	for i := q.begin(); q.valid(i); i = q.next(i) {
		r := *q.at(i)
		if r.lo < m.repair.sent {
			if p, ok := s.resendRange(st, rng{r.lo, min64(r.hi, m.repair.sent)}); ok {
				return p, true
			}
		}
		if r.hi > m.repair.hi {
			if p, ok := s.resendRange(st, rng{max64(r.lo, m.repair.hi), r.hi}); ok {
				return p, true
			}
		}
	}
	if m.finResend && !sp.reset && sp.finSent {
		return chunkPlan{off: sp.q.end, fin: true, resend: true}, true
	}
	return chunkPlan{}, false
}

// planChunk computes the next frame of a stream that has an identifier.
func (s *Session) planChunk(st *Stream) chunkPlan {
	if m := st.snd.mps; m != nil && (m.resend.len() > 0 || m.finResend) {
		if rp, ok := s.planResend(st); ok {
			return rp
		}
	}
	return s.planNewChunk(st)
}

func (s *Session) planNewChunk(st *Stream) chunkPlan {
	sp := &st.snd
	var p chunkPlan
	p.off = max64(sp.sent, sp.q.base)
	if end := sp.sendEnd(); end > p.off {
		p.avail = end - p.off
	}
	p.credit = sp.maxData - min64(sp.maxData, p.off)
	p.connCredit = s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent)
	p.n = min(min64(p.avail, min64(p.credit, p.connCredit)), maxStreamFrame)
	if p.off >= sp.sent {
		p.n = s.capRetained(p.n)
	}
	// The first frame carries the whole preamble.
	if p.off == 0 && sp.preLen > 0 && p.n < sp.preLen {
		p.n = 0
		p.preBlocked = true
		return p
	}
	if p.off+p.n == sp.q.end && sp.finWr && !sp.finSent && !sp.reset {
		p.fin = true
	}
	p.open = p.n == 0 && !p.fin && !st.opened && (st.pinned || st.openWant)
	return p
}

// ClaimWrite consumes the permission published for the driver. Stale
// requests are rejected before sampling protocol time or leasing a batch.
func (s *Session) ClaimWrite(pipe, lane uint64, signal *WriteSignal) bool {
	if !signal.Ready.Load() {
		return false
	}
	signal.Ready.Store(false)
	p := s.pipe(pipe)
	if p == nil || p.out == nil {
		return false
	}
	ol := p.out.lane(lane)
	if ol == nil {
		return false
	}
	return signal == p.writeSignal(ol) && !ol.notWritable && s.writeWorkOn(p, ol)
}

// writeWorkOn reads work membership, without predicting which stream a
// scheduler would choose or constructing a routing decision.
func (s *Session) writeWorkOn(p *Pipe, ol *outLane) bool {
	if s.state == StateClosed || p.out.failed || ol.shut || ol.closeSent ||
		s.holdsFirst(p.id) || p.mp != nil && p.mp.hs {
		return false
	}
	if s.closeFr.pending && !s.closeFr.waitLanes {
		return ol.lane == 0
	}
	if s.shp != nil && s.shapeDelayed(p) {
		return false
	}
	if p.id == 0 && ol.lane == 0 && !ol.tpSent {
		return true
	}
	if ol.laneFirst {
		return s.state < StateClosing
	}
	if ol.lane > 0 && !ol.confirmed || !s.transportLive() {
		return false
	}
	if !s.peerSeen {
		return s.ping.wants() || s.earlyActive() && s.earlyWants()
	}
	if s.streamWorkOn(p, ol) {
		return true
	}
	return ol.lane == 0 && (s.sessionControlOn(p) || s.shapeWants(p, 0) ||
		s.recordDatagrams(p) ||
		p.mp != nil && s.pathControlOn(p))
}

func (s *Session) sessionControlOn(p *Pipe) bool {
	if !s.controlPending() {
		return false
	}
	if p.mp == nil {
		return true
	}
	primary, _ := s.primary()
	return primary == nil || primary == p
}

func (s *Session) streamWorkOn(p *Pipe, ol *outLane) bool {
	if s.sched.mask != 0 {
		return true
	}
	return p.mp != nil && (ol.lane == 0 && len(p.mp.copyQ) != 0 ||
		ol == p.out.lo.copyLane && len(p.mp.selfCopyQ) != 0)
}

func (s *Session) pathControlOn(p *Pipe) bool {
	a, m := p.mp, s.mp
	if a.firstRecord || m.pairAcks.contains(p) || a.needPing && m.pingAllowed(s.now) ||
		m.ep.frame != nil && !a.epochSent && a.out == OutActive && epochPipe(p) {
		return true
	}
	primaryAck := s.ack != nil && s.ackOwed() && !s.ackPairOnly()
	backupAck := m.ackChanged && s.now >= m.redundantAt
	if primaryAck || backupAck || len(m.abandonQ) != 0 || len(m.owedAbandons()) != 0 {
		primary, backup := s.primary()
		if (primary == p || primary == nil) && (primaryAck || len(m.owedAbandons()) != 0) || backup == p && backupAck {
			return true
		}
		for _, q := range m.abandonQ {
			if s.abandonGoesOn(q, p, primary) {
				return true
			}
		}
	}
	for _, q := range m.statusQ {
		if statusReady(q, p) {
			return true
		}
	}
	return false
}

// PollTransmit fills b with records for the outbound lane, at most limit
// bytes. It returns false when nothing is due.
func (s *Session) PollTransmit(pipe, lane uint64, b *Batch, limit int, now Time) bool {
	s.SetNow(now)
	if s.mp != nil && s.mp.prefixScheduler != nil &&
		(s.mp.prefixPending != 0 || s.mp.prefixAt != 0 && s.mp.prefixAt <= s.now) {
		// A writer can get the owner before its timer callback. Use the
		// same health-before-repair order at this handoff cut.
		s.expirePathDeadlines(s.now)
		s.prefixTimers(s.now, true)
	}
	b.reset()
	b.Pipe, b.Lane = pipe, lane
	p := s.pipe(pipe)
	if p == nil || p.out == nil || p.out.failed || s.state == StateClosed {
		return false
	}
	ol := p.out.lane(lane)
	if ol == nil || ol.shut || ol.notWritable || s.holdsFirst(pipe) || p.mp != nil && p.mp.hs {
		return false
	}
	var w recWriter
	w.init(b, ol.info.Framing, s.recMax(ol), limit)
	b.owner, b.provisional = ol, s.holdSends() && !s.earlyAccepted
	b.trackProgress = p.member != nil && p.member.rotation != nil
	if s.ack != nil && s.peerSeen {
		w.dataRoom = s.paceRoom(p)
		if w.dataRoom > 0 && !s.dataTurn(p, ol) {
			w.dataRoom = 0
		}
	}
	// Every record of the pipe goes through its Shaper, a lane's PIPE_LANE
	// record included: it is planned, padded and
	// delayed like any other.
	var ps *pipeShape
	if s.shp != nil {
		b.trackProgress = b.trackProgress || s.shp.eff.Wire != nil || s.shp.eff.Profile != nil
		if ps = s.shapeOf(p); ps != nil {
			ps.shapeEventsDue(s.now)
			ps.resumeRecordWait(ol)
			w.sh, w.gate = ps, ps
			if ps.caps&shaping.CapOutbound != 0 {
				b.shape = ps
			}
		}
	}
	if pipe == 0 && lane == 0 && !ol.tpSent {
		if s.closeFr.pending && !s.closeFr.waitLanes {
			// CONNECTION_CLOSE is never held, and the parameters must
			// lead it: both go now, in one first record, unshaped.
			w.sh, w.gate, w.delayed = nil, nil, false
		}
		s.writeParams(&w, ol)
	}
	switch {
	case ol.laneFirst:
		if s.state < StateClosing {
			s.writeLaneFirst(&w, p, ol)
		}
	case lane > 0 && !ol.confirmed:
	case s.closeFr.pending && !s.closeFr.waitLanes && !ol.closeSent && lane == 0:
		s.writeClose(&w, ol)
	case !s.transportLive() || ol.closeSent:
	case !s.peerSeen:
		s.writePing(&w)
		if s.earlyActive() {
			s.writeEarly(&w)
		}
	case s.mp != nil:
		if ps != nil && ps.injFirst && !s.controlPending() && (s.ack == nil || s.ack.deadline() > s.now) {
			s.shapeInject(&w)
		}
		s.mpWrite(&w, p, ol)
		s.shapeInject(&w)
	default:
		if ps != nil && ps.injFirst && !s.controlPending() && (s.ack == nil || s.ack.deadline() > s.now) {
			s.shapeInject(&w)
		}
		if lane == 0 {
			s.writeControl(&w, p)
		}
		s.writeData(&w, p, ol)
		if lane == 0 && (s.blockedDue() || s.ctlHead != nil) {
			s.writeControl(&w, p)
		}
		s.shapeInject(&w)
	}
	if w.oversize {
		// The session fails: what the batch took is not handed to a carrier.
		s.FailInternal("a frame exceeds the record limit")
		w.b.Discard()
		w.b.reset()
		return false
	}
	w.end()
	w.chargeGate()
	if w.dataSpent > 0 {
		s.tookTurn(p, ol)
	}
	s.commitShape(&w)
	if b.n == 0 {
		if s.ack != nil && !ol.notWritable {
			// A write opportunity that ends with nothing to send: an
			// application-limited moment.
			p.out.snd.re.appLimited(p.out.snd.so)
		}
		return false
	}
	s.afterBatch(&w, p, ol)
	if !w.closed && !w.delayed && (w.capped || w.controlFull) {
		s.wakeLaneWriter(p, ol)
	}
	if s.ack != nil {
		if (w.budget < 1024 || w.capped || w.dataHeld()) && s.dataPendingOn(p, ol) {
			p.out.snd.re.blocked() // the write runs, or the limiter holds, while more data waits
		} else {
			p.out.snd.re.appLimited(p.out.snd.so)
		}
	}
	ol.writing = true
	if ps != nil {
		ps.laneBlocked(ol)
		if lane == 0 && ps.delayActive {
			ps.refreshRecordFlows()
		}
	}
	if !ol.notWritable {
		ol.stallAt = now
	}
	if p.mp != nil {
		if lane == 0 && len(p.out.tierWaiters) != 0 {
			s.releaseTierWaiters(p)
		}
		s.armPathDeadline(p, pathStall)
		s.armPathDeadline(p, pathProbe)
	}
	return true
}

// afterBatch accounts positions and statistics of a produced batch; p and
// its outbound direction are live (PollTransmit checked them).
func (s *Session) afterBatch(w *recWriter, p *Pipe, ol *outLane) {
	// A byte-stream batch already includes each record's canonical Size.
	// Message carriers omit Size on the wire but still count it in positions.
	pos := w.b.n
	if w.framing == FramingRecords {
		for _, r := range w.b.recs {
			pos += varint.SizeVarint(uint64(r.size))
		}
	}
	start := ol.pos
	ahead := s.snapAhead(p) // what waits ahead of this batch
	ol.pos += uint64(pos)
	ol.handed += uint64(pos)
	w.b.pos = uint64(pos)
	for _, st := range w.b.touched {
		st.snd.laneEnd = ol.pos
	}
	w.b.clearTouched()
	if w.anyElic {
		// A message carrier's batch counts whole, as for the samples.
		ol.elicitPos = ol.pos
		if w.framing != FramingRecords {
			ol.elicitPos = min64(start+uint64(w.elicEnd), ol.pos)
		}
	}
	s.stats.BytesSent += uint64(pos)
	if s.ack != nil {
		defer s.paceSent(p, uint64(pos))
	}
	s.stats.RecordsSent += uint64(w.recs)
	p.out.records += uint64(w.recs)
	if s.ack != nil {
		w.deliveryRestricted = w.deliveryRestricted || !s.deliveryUnrestricted(p)
		s.ack.onSent(p, ol, start, w, s.now, ahead)
	}
	if w.traffic {
		p.lastTraffic = s.now
	}
	if p.mp != nil {
		s.mpAfterBatch(w, p, ol, start, ahead)
	}
	if w.closed && !w.laneOnly {
		w.b.Shutdown = true
		s.closeSentOn()
	}
	// Session without acknowledgement: a record passed to the carrier
	// restarts the idle timer unless it carries only QX_PING requests and
	// PADDING.
	if !s.feat.Acked() && !w.onlyPing {
		s.noteActivity(s.now)
	}
}

// queuedOn is Q_p: the bytes handed to p's outbound
// carrier that it has not taken yet (the batches being written), plus
// what the carrier holds unsent where the adapter reports it, over all
// lanes.
func (s *Session) queuedOn(p *Pipe) uint64 {
	if p.out == nil {
		return 0
	}
	var q uint64
	for _, l := range p.out.lanes {
		// Retain the last reported backlog for placement until the carrier
		// reports a replacement value. Age alone is no evidence it drained.
		q += l.handed + l.unsent
	}
	return q
}

// CarrierUnsent returns the carrier's own unsent bytes of p over its
// lanes, and whether the adapter reports them for any lane.
func (s *Session) carrierUnsent(p *Pipe) (uint64, bool) {
	if p.out == nil {
		return 0, false
	}
	var q uint64
	var known bool
	quality := p.pathQuality()
	for _, l := range p.out.lanes {
		if l.freshUnsent(quality, s.now) {
			q += l.unsent
			known = true
		}
	}
	return q, known
}

func (ol *outLane) freshUnsent(q *pathQuality, now Time) bool {
	if !ol.unsentKnown || q == nil {
		return ol.unsentKnown
	}
	at, ok := q.unsentAt[ol.lane]
	return ok && now.Sub(at) < rateWindow
}

// SetCarrierUnsent records the bytes the carrier of a lane holds unsent,
// as its adapter reports them (an optional capability).
func (s *Session) SetCarrierUnsent(pipe, lane, n uint64, now Time) {
	if p := s.pipe(pipe); p != nil && p.out != nil {
		if ol := p.out.lane(lane); ol != nil {
			if q := p.pathQuality(); q != nil {
				changed := !ol.freshUnsent(q, now) || ol.unsent != n
				if q.unsentAt == nil {
					q.unsentAt = make(map[uint64]Time)
				}
				q.unsentAt[lane] = now
				if changed {
					s.wakePrefix()
				}
			}
			ol.unsent, ol.unsentKnown = n, true
		}
	}
}

// WriteDone reports that a batch was written (or failed); it releases the
// batch's references. n is the bytes the carrier took.
func (s *Session) WriteDone(b *Batch, n int, err error, now Time, physicalFault ...*PhysicalFault) {
	s.SetNow(now)
	s.wakePrefix()
	p := s.pipe(b.Pipe)
	var ol *outLane
	if p != nil && p.out != nil {
		ol = p.out.lane(b.Lane)
	}
	rotate := ol != nil && b.owner == ol && p.member != nil && p.member.rotation != nil
	if b.owner != nil && (b.Handoffs() || rotate) {
		bytes, records, _ := b.Tally(n, b.owner.info.Framing == FramingRecords, 0)
		b.settleHandoffs(records)
		if rotate {
			s.memberBytes(p, true, bytes)
		}
	}
	b.release()
	s.completeShape(b, n, ol)
	if ol != nil && b.owner == ol {
		ol.writing = false
		if err == nil && p.sh != nil {
			p.sh.resumeRecordWait(ol)
			if b.Lane == 0 && p.sh.delayActive {
				p.sh.refreshRecordCopyDelay()
			}
		}
		ol.handed -= min64(ol.handed, b.pos)
		ol.closeDone = ol.closeSent && err == nil
		if ol.notWritable {
			ol.stallAt = s.now
		}
		if p.mp != nil {
			s.armPathDeadline(p, pathStall)
		}
		if ol.placementWaiting {
			ol.placementWaiting = false
			if err == nil && s.streamWorkOn(p, ol) {
				s.wakePipe(p)
			}
		}
	}
	if err != nil && ol != nil && b.owner == ol {
		var fault *PhysicalFault
		if len(physicalFault) != 0 {
			fault = physicalFault[0]
		}
		s.PipeWriteFailed(b.Pipe, b.Lane, err, fault, now)
	} else if err == nil && b.owner != nil && b.owner == ol && b.Lane == 0 {
		s.routeDatagrams()
	}
	b.owner = nil
	s.closeDonePipes()
	s.startLinger()
}

// peerCloseRead records that the peer's CONNECTION_CLOSE was read on p.
func (s *Session) peerCloseRead(p *Pipe) {
	if p.in == nil {
		return
	}
	p.in.ccRead = true
	s.closePipeIfDone(p)
}

// closeDonePipes closes the carrier of every pipe that is done.
func (s *Session) closeDonePipes() {
	for _, p := range s.pipes {
		if s.state != StateClosing {
			return
		}
		s.closePipeIfDone(p)
	}
}

// closePipeIfDone closes the carrier independently of the application read
// drain (SPEC.md Section 6.5 steps 3 and 4). Once the peer's CONNECTION_CLOSE
// was read and our output is done, wait for the peer's end of input only
// if the carrier supports half-close.
func (s *Session) closePipeIfDone(p *Pipe) {
	if s.state != StateClosing || p.in == nil || !p.in.ccRead || p.in.closed || !s.pipeOutDone(p) {
		return
	}
	if l := p.in.lane(0); l == nil || l.info.HalfClose && !p.in.ended {
		return
	}
	p.in.closed, p.in.ended = true, true
	s.cancelDatagramReports(p)
	for _, l := range p.in.lanes {
		l.ended = true
	}
	s.act(Action{Kind: ActClose, Pipe: p.id, Dir: DirBidi, After: s.lingerTimeout()})
	s.checkLingerDone()
}

// pipeOutDone reports that nothing more is to be written to p: its outbound
// side failed, has none, carried CONNECTION_CLOSE to the carrier, or will
// carry none.
func (s *Session) pipeOutDone(p *Pipe) bool {
	if p.out == nil || p.out.failed {
		return true
	}
	l := p.out.lane(0)
	return l == nil || l.closeDone || !l.closeSent && !s.closeFr.pending
}

// startLinger arms the linger timeout once CONNECTION_CLOSE was handed to
// every carrier that takes it: lingering starts from
// the hand-off, not from the queueing, so a slow write never cuts it short.
func (s *Session) startLinger() {
	if s.state == StateClosing && s.closeFr.sentAll && !s.closeFr.lingering && s.CloseHandedOff() {
		s.closeFr.lingering = true
		s.timers.linger = s.now.Add(s.lingerTimeout())
	}
}

// CloseHandedOff reports whether a close no longer waits for the carriers:
// CONNECTION_CLOSE was written to every pipe that took it and is still
// alive, and no pipe that can carry it still lacks it; also for a session
// CLOSED or closed without CONNECTION_CLOSE. It is false before the close.
func (s *Session) CloseHandedOff() bool {
	switch {
	case s.state < StateClosing:
		return false
	case s.state == StateClosed || !s.closeFr.pending && !s.closeFr.sentAll:
		return true
	case s.closeFr.pending:
		return false
	}
	for _, p := range s.pipes {
		if p.out == nil || p.out.failed {
			continue
		}
		if l := p.out.lane(0); l != nil && l.closeSent && !l.closeDone {
			return false
		}
	}
	return true
}

// EndClose ends a close that the carriers did not take in time: the
// session enters CLOSED and every carrier is closed.
func (s *Session) EndClose() {
	if s.state == StateClosing {
		s.finishClose()
	}
}

// holdsFirst reports whether pipe's output waits for ReleaseFirstFlight
// (a CONNECTION_CLOSE does not wait).
func (s *Session) holdsFirst(pipe uint64) bool {
	return s.firstHeld && pipe == 0 && !s.closeFr.pending
}

// ReleaseFirstFlight lets pipe 0 write what is queued, and starts the
// handshake timeout; it reports whether the first flight was held.
func (s *Session) ReleaseFirstFlight(now Time) bool {
	s.SetNow(now)
	if !s.firstHeld {
		return false
	}
	s.firstHeld = false
	if s.state == StateEstablishing && !s.peerSeen {
		s.timers.handshake = now.Add(s.cfg.HandshakeTimeout)
	}
	if p := s.pipe(0); p != nil {
		s.wakePipe(p)
	}
	return true
}

// writeParams writes the first record of the bootstrap pipe.
func (s *Session) writeParams(w *recWriter, ol *outLane) {
	block, e := wire.AppendParams(nil, &s.cfg.Local)
	if e != nil {
		s.failLocal(e)
		return
	}
	n := varint.SizeVarint(wire.TypeTransportParams) + varint.SizeVarint(uint64(len(block))) + len(block)
	if !w.frame(n, true, func(b []byte) []byte { return wire.AppendTransportParams(b, block) }) {
		// Held by a shaping delay of the first record: the parameters
		// still lead the pipe once it ends (nothing else goes before).
		return
	}
	ol.tpSent = true
	if !ol.info.MinRTT && !s.cfg.Test.Enabled && s.ping.outstanding == 0 {
		s.ping.requestFirst = true
	}
}

// writeClose writes CONNECTION_CLOSE; nothing follows it on the pipe.
func (s *Session) writeClose(w *recWriter, ol *outLane) {
	c := &s.closeFr
	n := wire.CloseLen(c.app, c.code, c.frameType, len(c.reason))
	if w.sh != nil {
		// A shaped open record ends under its plan; CONNECTION_CLOSE then
		// goes unshaped, never held. Unshaped, it joins the open record
		// (the parameters of a first record among them).
		w.end()
	}
	w.sh, w.delayed = nil, false
	if !w.frame(n, false, func(b []byte) []byte {
		return wire.AppendClose(b, c.app, c.code, c.frameType, c.reason)
	}) {
		return
	}
	ol.closeSent = true
	w.closed = true
}

// closeSentOn runs once CONNECTION_CLOSE was queued on every live outbound
// pipe: it shuts their write sides down after it and bounds the wait for
// the carriers to take it.
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
	// Step 1 waits at most the linger timeout for the carriers to take
	// CONNECTION_CLOSE; lingering starts at the hand-off (startLinger).
	s.timers.linger = s.now.Add(s.lingerTimeout())
	s.emitClosedEvent()
	s.closeDonePipes()
	s.startLinger()
	s.checkLingerDone()
}

// checkLingerDone enters CLOSED once every inbound pipe ended. While
// CONNECTION_CLOSE is pending it rechecks whether the pipes that can still
// carry it all have it: a pipe that failed before taking it is no longer
// waited for.
func (s *Session) checkLingerDone() {
	if s.state != StateClosing {
		return
	}
	if s.closeFr.pending {
		if s.closeFr.waitLanes && s.lanesCovered() {
			s.endLaneWait() // a failed pipe is no longer waited for
		}
		if !s.closeFr.waitLanes {
			s.closeSentOn() // calls back once it is no longer pending
		}
		return
	}
	for _, p := range s.pipes {
		if p.in != nil && !p.in.ended {
			return
		}
	}
	if s.drain.on && s.drain.left > 0 {
		return // the reads or the linger timeout end the wait
	}
	s.finishClose()
}

// startReadDrain marks the streams the application opened or accepted
// that hold received data or an end (FIN or reset) it has not read, the
// read drain running up to the end of the stream (SPEC.md Section 6.5);
// nothing arrives after the peer's close, so the count only falls
// (drainRead, stopRecv).
func (s *Session) startReadDrain() {
	for _, st := range s.streams {
		if !st.hasR || !st.local && !st.accepted {
			continue
		}
		if r := &st.rcv; !r.stopped && r.unread() {
			r.drainWait = true
			s.drain.left++
		}
	}
}

// drainRead ends the wait for st once its buffered data and its end have
// been read or dropped; the last one lets CLOSED proceed.
func (s *Session) drainRead(st *Stream) {
	r := &st.rcv
	if r.drainWait && (r.stopped || !r.unread()) {
		r.drainWait = false
		s.drain.left--
		s.checkLingerDone()
	}
}

// discardAcceptQueues drops the peer streams not yet accepted when the
// session closes (SPEC.md Section 6.5): their data is released and accept
// fails.
func (s *Session) discardAcceptQueues() {
	for t := range s.acceptQ {
		for st := s.acceptQ[t].pop(); st != nil; st = s.acceptQ[t].pop() {
			if st.hasR {
				if d := st.rcv.buf.clear(); d > 0 {
					s.bufSub(d)
				}
			}
		}
	}
}

// writeControl writes the coalesced control state ahead of
// data.
func (s *Session) writeControl(w *recWriter, p *Pipe) {
	if s.ack != nil && s.mp == nil {
		s.ack.write(w, p, s.now)
	}
	s.writePingResponse(w)
	s.writeDatagramReports(w)
	if s.pend.maxData {
		s.tuneConnUpdate()
		v := s.nextReceiveCredit()
		if v > s.conn.adv || s.conn.adv == 0 || s.pend.forceMaxData {
			if !w.controlFrame(1+varint.SizeVarint(v), true, v <= s.conn.adv, func(b []byte) []byte { return wire.AppendMaxData(b, v) }) {
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
		if !s.pend.maxStreams[t] {
			continue
		}
		v := s.rem[t].limit
		if !w.streamCountControl(dMaxStreams, t == 0, v, &s.rem[t].reported, wire.AppendMaxStreams) {
			return
		}
		s.pend.maxStreams[t] = false
	}
	for st := s.ctlHead; st != nil; {
		next := st.ctlNext
		if !s.writeStreamCtl(w, st) {
			return
		}
		st = next
	}
	if s.dataBlockedDue() {
		v := s.conn.sendMax
		if !w.controlFrame(1+varint.SizeVarint(v), true, s.conn.blockedReported == v+1, func(b []byte) []byte { return wire.AppendDataBlocked(b, v) }) {
			return
		}
		s.conn.blockedReported = v + 1
		w.logFrame(frameDesc{kind: dDataBlocked})
	}
	s.pend.dataBlocked = false
	for t := range 2 {
		if s.streamsBlockedDue(t) {
			v := s.loc[t].limit
			if !w.streamCountControl(dStreamsBlocked, t == 0, v, &s.loc[t].blockedReported, wire.AppendStreamsBlocked) {
				return
			}
		}
		s.pend.streamsBlocked[t] = false
	}
	if s.pend.goaway {
		g := &s.ga
		n := 4 + varint.SizeVarint(g.sentWater[0]) + varint.SizeVarint(g.sentWater[1]) + varint.SizeVarint(g.code)
		if !w.controlFrame(n, true, g.phaseWritten == g.phase, func(b []byte) []byte { return wire.AppendGoAway(b, g.sentWater[0], g.sentWater[1], g.code) }) {
			return
		}
		g.phaseWritten = g.phase
		w.logFrame(frameDesc{kind: dGoAway})
		s.pend.goaway = false
	}
	s.writePing(w)
}

func (w *recWriter) streamCountControl(kind descKind, bidi bool, count uint64, reported *uint64, appendFrame func([]byte, bool, uint64) []byte) bool {
	if !w.controlFrame(1+varint.SizeVarint(count), true, *reported == count+1,
		func(b []byte) []byte { return appendFrame(b, bidi, count) }) {
		return false
	}
	*reported = count + 1
	w.logFrame(frameDesc{kind: kind, bidi: bidi})
	return true
}

// writeStreamCtl writes the pending control frames of one stream; it
// returns false when the batch is full.
func (s *Session) writeStreamCtl(w *recWriter, st *Stream) bool {
	if st.ctlWaits() {
		// Its frames keep their bits and follow the opening STREAM frame.
		ctl := st.ctl
		s.unlinkCtl(st)
		st.ctl = ctl
		return true
	}
	id := st.id
	if st.ctl&ctlMaxStreamData != 0 {
		r := &st.rcv
		s.tuneStreamUpdate(st)
		v := r.readOff + r.window
		if !r.finKnown && !r.stopped && (v > r.adv || st.forceMSD) {
			if !w.controlFrame(1+varint.SizeVarint(id)+varint.SizeVarint(v), true, v <= r.adv,
				func(b []byte) []byte { return wire.AppendMaxStreamData(b, id, v) }) {
				return false
			}
			w.logFrame(frameDesc{kind: dMaxStreamData, id: id})
			if s.mp != nil && (r.highest >= r.adv || r.blockSeen == r.adv && r.blockSeen > 0) {
				s.markCopyMSD(st)
			}
			if r.tune != nil && v > r.adv {
				r.tune.sent(r.adv, s.now, r.highest == r.adv)
			}
			r.adv = max64(r.adv, v)
			st.forceMSD = false
		}
		s.clearCtl(st, ctlMaxStreamData)
	}
	if st.ctl&ctlStop != 0 {
		r := &st.rcv
		c := r.stopCode
		if !w.controlFrame(1+varint.SizeVarint(id)+varint.SizeVarint(c), true, r.stopSent,
			func(b []byte) []byte { return wire.AppendStopSending(b, id, c) }) {
			return false
		}
		w.logFrame(frameDesc{kind: dStop, id: id})
		w.activity, w.traffic = true, true
		w.onlyPing = false
		r.stopSent = true
		st.ctl &^= ctlStop
	}
	if st.ctl&ctlReset != 0 {
		sp := &st.snd
		// Outside multipath the data below the Reliable Size precedes the
		// reset on the pipe.
		if sp.sent >= sp.resetRel {
			final := max64(sp.sent, sp.resetRel)
			code, rel, at := sp.resetCode, sp.resetRel, sp.resetAt
			n := 1 + varint.SizeVarint(id) + varint.SizeVarint(code) + varint.SizeVarint(final)
			if at {
				n += varint.SizeVarint(rel)
			}
			if !w.controlFrame(n, true, sp.resetSent, func(b []byte) []byte {
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
			s.syncUnsent(st)
			w.activity, w.traffic = true, true
			w.onlyPing = false
			// Outside multipath a written reset is acknowledged.
			if !s.feat.Multipath {
				s.sendTerminal(st, sendResetRecvd)
			}
		}
	}
	if st.ctl&ctlPrio != 0 {
		x := st.x()
		seq, u, inc := x.reqSeq, x.reqUrgency, x.reqIncr
		n := 4 + varint.SizeVarint(id) + varint.SizeVarint(seq) + 1
		if !w.controlFrame(n, true, x.reqSent == seq, func(b []byte) []byte { return wire.AppendPriorityUpdate(b, id, seq, u, inc) }) {
			return false
		}
		w.logFrame(frameDesc{kind: dPrio, id: id, val: seq})
		x.reqSent = seq
		st.ctl &^= ctlPrio
	}
	if st.ctl&ctlBlocked != 0 && st.snd.blocked == st.snd.maxData+1 {
		v := st.snd.maxData
		if !w.controlFrame(1+varint.SizeVarint(id)+varint.SizeVarint(v), true, st.snd.blockedReported == v+1,
			func(b []byte) []byte { return wire.AppendStreamDataBlocked(b, id, v) }) {
			return false
		}
		st.snd.blockedReported = v + 1
		w.logFrame(frameDesc{kind: dStreamDataBlocked, id: id})
	}
	st.ctl &^= ctlBlocked
	if st.ctl == 0 {
		s.unlinkCtl(st)
		if st.rcv.stopSent && st.rcv.stopCode == wire.CodePacketError {
			s.maybeDiscard(st)
		}
	}
	return true
}

// writeData writes stream data in priority order; with lanes, only the
// directions bound to ol.
func (s *Session) writeData(w *recWriter, p *Pipe, ol *outLane) {
	var lp *Pipe // the pipe whose lanes streams bind to; nil without lanes
	if p.out.lo.offered {
		lp = p
	}
	var d *decision
	if p.mp != nil {
		d = &s.mp.dec // prepared by mpWrite
	}
	var dgPipe *Pipe
	if ol.lane == 0 && s.dg != nil {
		dgPipe = p
	}
	s.writeScheduledData(w, lp, ol, d, dgPipe)
}

// writeScheduledData shares stream scheduling with early data. Only an
// established, datagram-capable lane zero supplies dgPipe.
func (s *Session) writeScheduledData(w *recWriter, lp *Pipe, ol *outLane, d *decision, dgPipe *Pipe) {
	preferStream := false
	for s.sched.mask != 0 || dgPipe != nil && s.recordDatagrams(dgPipe) {
		// A record that is not open yet is planned by the chunk that
		// opens it, once one has data to send: a plan is made only for a
		// record that goes.
		if w.open && w.room() <= 0 && !w.nextRecord() {
			return
		}
		progressed := false
		it := s.sched.iter()
		for st := it.next(); st != nil; st = it.next() {
			if (d != nil || lp != nil) && !s.canSendNow(st) {
				s.holdFreshCredit(st)
				continue
			}
			if d != nil && !s.onPipe(d, st) {
				continue
			}
			if lp != nil {
				if l := s.bindLane(lp, st); l != ol {
					// Bound to another lane: its writer may be idle, so it
					// is woken to take the data.
					s.wakeLaneWriter(lp, l)
					continue
				}
			}
			beforeFin := dgPipe != nil && st.snd.finWr && s.dg.waitsFor(st)
			if dgPipe != nil && s.recordDatagrams(dgPipe) && (!preferStream || st.urgency > 0 || beforeFin) {
				urgency := st.urgency
				if preferStream && !beforeFin {
					urgency--
				}
				sent, full := s.writeDatagram(w, dgPipe, urgency)
				if full {
					return
				}
				if sent {
					progressed, preferStream = true, true
					break
				}
			}
			r := s.sendChunk(w, st, d)
			if r == chunkFull {
				if !w.nextRecord() {
					return
				}
				progressed = true // the next record starts from the top priority
				break
			}
			if r == chunkSent {
				if lp != nil && st.snd.laneEnd != pendingLaneEnd {
					// Assembly is serialized by the session. Until afterBatch
					// accounts the final end, this direction stays on this lane
					// and needs only one entry, even across several records.
					st.snd.laneEnd = pendingLaneEnd
					w.b.touched = append(w.b.touched, st)
				}
				progressed, preferStream = true, false
				break
			}
		}
		if !progressed {
			if dgPipe == nil || !s.recordDatagrams(dgPipe) {
				return
			}
			sent, _ := s.writeDatagram(w, dgPipe, 7)
			if !sent {
				return
			}
			preferStream = true
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
//
//goland:noinspection GoDfaConstantCondition // d is nil for a single-pipe writer (writeData)
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
				// and report the block.
				if s.allocID(st) {
					s.blockedOn(st, false, true)
				}
			}
			return chunkSkip
		}
		if !s.allocID(st) {
			return chunkSkip
		}
	}
	p := s.planChunk(st)
	if d != nil && p.resend && p.n != 0 && sp.mps.repair.source != nil && sp.mps.repair.covers(p.off) {
		r := &sp.mps.repair
		target, _, eligible := s.prefixTarget(d.g, r.source, r.anchor, min64(p.n, r.hi-p.off))
		switch {
		case !eligible:
			s.retryPrefixTail(st)
			s.wakeAllWriters()
			return chunkSkip
		case target == nil:
			if native, ok := s.planNativeResend(st); ok {
				p = native
			} else {
				p = s.planNewChunk(st)
			}
		case target != d.p:
			s.chosen(d, target)
			return chunkSkip
		}
	}
	if p.preBlocked {
		if p.avail >= sp.preLen || sp.finWr {
			s.blockedOn(st, p.credit < sp.preLen, p.connCredit < sp.preLen)
		}
		return chunkSkip
	}
	if p.n == 0 && !p.fin && !p.open {
		if !st.sendable() {
			s.sched.remove(st)
		} else {
			s.holdFreshCredit(st)
		}
		return chunkSkip
	}
	off, n, fin := p.off, p.n, p.fin
	repair := p.resend && n != 0 && sp.mps.repair.covers(off)
	if repair {
		r := &sp.mps.repair
		n = min64(n, r.hi-off) // the pending range already reserves bucket credit
		if n == 0 || off == 0 && n < sp.preLen {
			return chunkSkip
		}

		if n != p.n {
			fin = false
		}
	}
	if c := s.chunkRoom(d, st); d != nil && !p.resend && n > c {
		// One scheduling decision per chunk.
		n, fin = c, false
		if off == 0 && sp.preLen > n {
			n = sp.preLen
		}
	}
	// Datagrams queued for a stream not opened yet go right behind its
	// opening frame, ahead of its data and FIN: the opening chunk is the
	// opening frame alone.
	dgWait := off == 0 && !p.resend && !st.opened && s.peerSeen && s.dg != nil && s.dg.waitsFor(st)
	if dgWait {
		n, fin = min64(n, sp.preLen), false
	}
	if (n > 0 || fin) && w.dataHeld() {
		return chunkFull // the limiter or the shaping gate holds the pipe's data back
	}
	hdr := wire.StreamHeaderLen(st.id, off, int(n), true)
	fits := w.room() - hdr // plans a record (shaping)
	if off == 0 && uint64(max(fits, 0)) < sp.preLen && w.cut > 0 && w.recBytes+w.pendLen(false) == 0 &&
		uint64(max(min(w.recMax, w.budget-w.sizeCost())-hdr, 0)) >= sp.preLen {
		// The first frame carries the whole preamble (a record takes its first
		// frame): the plan's largest record gives way, or the stream never opens.
		fits = int(sp.preLen)
		s.shp.stats.Clamped++
	}
	room := w.dataFit(fits)
	if room <= 0 {
		return chunkFull
	}
	if n > uint64(room) {
		if off == 0 && sp.preLen > 0 && uint64(room) < sp.preLen {
			// The preamble goes whole: an open gate lets it through in debt.
			if uint64(max(fits, 0)) < sp.preLen {
				return chunkFull
			}
			room = int(sp.preLen)
		}
		n = uint64(room)
		fin = false
	}
	var data []byte
	var blk pin
	want, wantFin := n, fin
	own := func(off, n uint64) int {
		if p.resend {
			return -1
		}
		return int(n - min(n, max(off, sp.preLen)-off))
	}
	if n > 0 {
		data, blk = sp.q.peek(off, int(n))
		if uint64(len(data)) < n {
			n = uint64(len(data))
			fin = false
		}
	}
	if !w.stream(st.id, off, data, blk, fin, own(off, n)) {
		return chunkFull
	}
	if w.pipe != nil {
		defer func() {
			w.deliveryRestricted = w.deliveryRestricted || !s.unrestrictedDeliveryStream(w.pipe, st)
		}()
	}
	sp.q.markHandoffs(w.b, off, off+n)
	// The rest of the chunk from the following buffer segments joins the
	// frame (the writer coalesces contiguous data of one stream).
	for n < want {
		more, mblk := sp.q.peek(off+n, int(want-n))
		if len(more) == 0 {
			break
		}
		f := wantFin && n+uint64(len(more)) == want
		if !w.stream(st.id, off+n, more, mblk, f, own(off+n, uint64(len(more)))) {
			break
		}
		sp.q.markHandoffs(w.b, off+n, off+n+uint64(len(more)))
		n += uint64(len(more))
		fin = f
	}
	w.logFrame(frameDesc{kind: dStream, id: st.id, off: off, n: n, fin: fin,
		fresh: d != nil && !s.cfg.Test.NoPrefixRepair && !p.resend && n > 0, repair: repair})
	w.spendData(n)
	st.countFrame(off, n)
	if p.resend {
		s.stats.BytesReinjected += n
		if d != nil {
			d.p.mp.holdSent += n
		}
		m := sp.mps
		if repair {
			source := m.repair.source
			s.commitPrefixRepair(m, off, n)
			if source != nil && sp.sendEnd() > max64(sp.sent, sp.q.base) &&
				(sp.sent >= sp.maxData || s.conn.sent >= s.conn.sendMax || m.bind.moving) {
				s.penalize(source)
			}
			s.stats.BytesOpportunistic += n
			d.p.mp.oppBytes += n
		}
		if off == 0 && d != nil {
			m.openPipe, m.openSet = d.p.id, true // a reinjected copy of the opening frame
			s.routeDatagramStream(st)
		}
		s.dropResend(m, off, off+n)
		if fin {
			m.finResend = false
		}
		if !st.sendable() {
			s.sched.remove(st)
		}
		return chunkSent
	}
	if !st.opened && d != nil {
		m := sp.mpState()
		m.openPipe, m.openSet = d.p.id, true
	}
	st.opened = true
	s.openedCtl(st)
	s.routeDatagramStream(st)
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
		s.mp.retain(newEnd - sp.sent)
		sp.sent = newEnd
		s.syncUnsent(st)
	}
	if fin {
		sp.finSent = true
		sp.state = sendDataSent
	}
	switch {
	case s.feat.Multipath:
		sp.mpState()
		s.sendChanged(st)
	case s.holdSends():
		// Carrier early data: the bytes stay until the verdict.
		s.sendChanged(st)
	default:
		h := s.heldEnter(st)
		sp.q.release(sp.sent)
		s.heldLeave(st, h)
		s.sendChanged(st)
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
	} else {
		s.holdFreshCredit(st)
	}
	return chunkSent
}

// blockedOn queues STREAM_DATA_BLOCKED / DATA_BLOCKED once per limit.
func (s *Session) blockedOn(st *Stream, stream, conn bool) {
	if stream && st.snd.blocked != st.snd.maxData+1 {
		st.snd.blocked = st.snd.maxData + 1
		s.queueCtl(st, ctlBlocked)
	}
	if conn && s.conn.blocked != s.conn.sendMax+1 {
		s.conn.blocked = s.conn.sendMax + 1
		s.pend.dataBlocked = true
		s.wakeAllWriters()
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
	return p.liveOut() && !p.retired() && (p.mp == nil || !p.mp.hs)
}
