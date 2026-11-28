package core

import (
	"slices"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// ---- control order ----

// effPrio returns the effective send priority and standby flag of an
// outbound direction: local setting, else the peer's
// request, else the default.
func (a *attach) effPrio() (uint64, bool) {
	pr, sb := a.peerPrio, a.peerStandby
	if a.sendPrio != nil {
		pr = *a.sendPrio
	}
	if a.sendStandby != nil {
		sb = *a.sendStandby
	}
	return pr, sb
}

// Sources of an effective setting, as PipeInfo and EvPipePriority report them.
const (
	SourceDefault = "default"
	SourceLocal   = "local"
	SourcePeer    = "peer"
)

// prioSource is where effPrio's priority comes from: the local setting, the
// peer's request, or the default.
func (a *attach) prioSource() string {
	switch {
	case a.sendPrio != nil:
		return SourceLocal
	case a.peerSeq > 0 || a.peerPrio != 0:
		return SourcePeer
	}
	return SourceDefault
}

// standbySource is where effPrio's standby flag comes from, by the same
// sources as prioSource.
func (a *attach) standbySource() string {
	switch {
	case a.sendStandby != nil:
		return SourceLocal
	case a.peerSeq > 0 || a.peerStandby:
		return SourcePeer
	}
	return SourceDefault
}

// owdBase is the smallest min_rtt of the live outbound pipes with an RTT
// sample (the reference of every OWD_p; noRTT when there is none),
// worked out once by a caller that needs the OWD of many pipes.
func (s *Session) owdBase() Duration {
	if s.mp != nil && s.mp.owdFloorValid {
		return s.mp.owdFloor
	}
	m := noRTT
	for _, q := range s.pipes {
		if q.mp != nil && q.out != nil && q.mp.liveOut() && q.mp.rtt.has && q.mp.rtt.min < m {
			m = q.mp.rtt.min
		}
	}
	if s.mp != nil {
		s.mp.owdFloor, s.mp.owdFloorValid = m, true
	}
	return m
}

// noRTT is owdBase without a sample.
const noRTT = Duration(1<<63 - 1)

// owd is OWD_p.
func (s *Session) owd(p *Pipe) Duration { return owdFrom(p.mp, s.owdBase()) }

// owdFrom is OWD_p given owdBase.
func owdFrom(a *attach, base Duration) Duration {
	if !a.rtt.has {
		return a.rtt.smoothed / 2
	}
	return a.rtt.min - min(a.rtt.min, base)/2
}

func (a *attach) liveOut() bool {
	return a.out == OutJoining || a.out == OutActive || a.out == OutSuspect || a.out == OutDraining
}

// controlKey orders outbound pipes for control frames, lowest first.
type controlKey struct {
	rank int
	sb   int
	prio uint64
	owd  Duration
	id   uint64
}

func (k controlKey) less(o controlKey) bool {
	if k.rank != o.rank {
		return k.rank < o.rank
	}
	if k.sb != o.sb {
		return k.sb < o.sb
	}
	if k.prio != o.prio {
		return k.prio < o.prio
	}
	if k.owd != o.owd {
		return k.owd < o.owd
	}
	return k.id < o.id
}

// controlKeyOf is p's control order key given owdBase.
func controlKeyOf(p *Pipe, base Duration) controlKey {
	a := p.mp
	var r int
	switch {
	case a.out == OutActive:
		r = 0
	case a.out == OutJoining:
		r = 1
	case a.out == OutSuspect:
		r = 2
	case a.out == OutDraining && !a.drainSuspect:
		r = 3
	default:
		r = 4
	}
	pr, sb := a.effPrio()
	k := controlKey{rank: r, prio: pr, owd: owdFrom(a, base), id: p.id}
	if sb {
		k.sb = 1
	}
	return k
}

type controlOrder struct {
	first, second *Pipe
	valid         bool
}

// primary returns the first two live outbound pipes of the control order.
// Path or RTT changes invalidate it. Only a control consumer refreshes it.
func (s *Session) primary() (*Pipe, *Pipe) {
	o := &s.mp.control
	if !o.valid {
		o.first, o.second = s.controlRoutes()
		o.valid = true
	}
	return o.first, o.second
}

func (s *Session) controlRoutes() (*Pipe, *Pipe) {
	if s.workProbe != nil {
		s.workProbe.pathScans++
	}
	base := s.owdBase()
	var a, b *Pipe
	var ka, kb controlKey
	for _, p := range s.pipes {
		if p.mp == nil || !maySend(p, true) ||
			p.mp.out == OutDraining && p.mp.abandonSent && p.mp.abandonRecv {
			continue
		}
		k := controlKeyOf(p, base)
		switch {
		case a == nil || k.less(ka):
			b, kb = a, ka
			a, ka = p, k
		case b == nil || k.less(kb):
			b, kb = p, k
		}
	}
	return a, b
}

// ---- PIPE_PING rate limit ----

func (m *multiPath) pingAllowed(now Time) bool {
	return m.pingCount < 8 || now >= m.pingTimes[m.pingIdx].Add(Second)
}

func (m *multiPath) pingSent(now Time) {
	m.pingTimes[m.pingIdx] = now
	m.pingIdx = (m.pingIdx + 1) % len(m.pingTimes)
	if m.pingCount < 8 {
		m.pingCount++
	}
}

// pingNext returns when the next PIPE_PING may be sent.
func (m *multiPath) pingNext() Time {
	if m.pingCount < 8 {
		return 0
	}
	return m.pingTimes[m.pingIdx].Add(Second)
}

// oldestPingWaiter reports whether p waits longest for a PIPE_PING.
func (s *Session) oldestPingWaiter(p *Pipe) bool {
	for _, q := range s.mp.clock.ping {
		if q != p && q.mp.pingSince < p.mp.pingSince {
			return false
		}
	}
	return true
}

// ---- pipe-level frames ----

// mpWrite builds a batch for one outbound lane of a multipath session.
func (s *Session) mpWrite(w *recWriter, p *Pipe, ol *outLane) {
	w.retain = true
	w.pipe = p
	// A multipath write call holds max(4 KiB, R_p x 50 ms), so that
	// a pipe's queue stays within what its failure detection assumes.
	w.recBound = int(min(max(s.rateOf(p)/20, 4<<10), batchRecBound))
	a := p.mp
	if a.hs {
		return
	}
	// Neither datagrams nor pipe control need a stream placement decision.
	// Preserve the gate for pending stream, copy and prefix-repair work.
	hasStreamWork := s.streamWorkOn(p, ol)
	s.clearTierWait(ol)
	if ol.lane == 0 {
		pathWork := s.pathControlOn(p)
		control := s.controlPending()
		if pathWork || control {
			prim, second := s.primary()
			if pathWork {
				s.mpPipeFrames(w, p, prim, second)
			}
			if control && (prim == p || prim == nil) {
				s.writeControl(w, p)
			}
		}
	}
	if !hasStreamWork {
		s.writeData(w, p, ol)
		return
	}
	d := &s.mp.dec
	s.newDecision(d, p)
	if ol.lane == 0 {
		s.refreshTiers(d.g)
		s.refreshBindings(d.g)
	}
	if ol.lane == 0 {
		s.writeCopies(d, w, &a.copyQ)
	} else if ol == p.out.lo.copyLane {
		s.writeCopies(d, w, &a.selfCopyQ)
	}
	s.writeData(w, p, ol)
	if d.blockedBy != nil && s.sched.mask != 0 && w.dataSpent == 0 {
		s.waitForTier(p, ol, d.blockedBy)
	}
}

func (s *Session) waitForTier(p *Pipe, lane *outLane, blocker *Pipe) {
	lane.tierWaitOwner, lane.tierWaitOn = p, blocker
	lane.tierWaitIndex = len(blocker.out.tierWaiters)
	blocker.out.tierWaiters = append(blocker.out.tierWaiters, lane)
}

func (s *Session) clearTierWait(lane *outLane) {
	on := lane.tierWaitOn
	if on == nil {
		return
	}
	q, i := on.out.tierWaiters, lane.tierWaitIndex
	last := len(q) - 1
	q[i] = q[last]
	q[i].tierWaitIndex = i
	q[last] = nil
	on.out.tierWaiters = q[:last]
	lane.tierWaitOwner, lane.tierWaitOn = nil, nil
}

func (s *Session) releaseTierWaiters(blocker *Pipe) {
	for _, lane := range blocker.out.tierWaiters {
		p := lane.tierWaitOwner
		lane.tierWaitOwner, lane.tierWaitOn = nil, nil
		if p.out != nil && s.pipe(p.id) == p && p.out.lane(lane.lane) == lane &&
			s.writeWorkOn(p, lane) {
			s.wakeLaneWriter(p, lane)
		}
	}
	clear(blocker.out.tierWaiters)
	blocker.out.tierWaiters = blocker.out.tierWaiters[:0]
}

// mpPipeFrames writes PIPE_ACK, PIPE_PING, PIPE_ABANDON and
// PIPE_STATUS for the pipe.
func (s *Session) mpPipeFrames(w *recWriter, p, prim, second *Pipe) {
	a := p.mp
	m := s.mp
	first, pair := a.firstRecord, m.pairAcks.contains(p)
	isPrim := p == prim || prim == nil
	if s.ack != nil {
		pairOnly := s.ackPairOnly()
		switch {
		case first || pair:
			// The first record's snapshot, or the answer to a PIPE_PING on
			// its own pipe pair. Held back whole when
			// the record is delayed.
			if !s.ack.writeSnapshot(w, first || pairOnly || isPrim) {
				return
			}
			if pair {
				s.answerPing(p)
				if isPrim && !pairOnly {
					s.primaryAckSent(p)
				}
			}
		case s.ackOwed() && isPrim && !pairOnly:
			if !s.ack.writeSnapshot(w, true) {
				return
			}
			s.primaryAckSent(p)
		case m.ackChanged && p == second && s.now >= m.redundantAt:
			if !s.ack.writeSnapshot(w, true) {
				return
			}
			m.ackChanged, m.redundantPending = false, false
			s.writeCreditCopies(w)
			s.returnLiveness(p)
		}
		if pair {
			m.pairAcks.remove(p)
		}
	}
	a.firstRecord = false
	if a.needPing && a.out != OutFailed && m.pingAllowed(s.now) && s.oldestPingWaiter(p) {
		if w.frame(varint.SizeVarint(wire.TypePipePing), true, wire.AppendPipePing) {
			a.needPing = false
			a.pingsSent++
			w.pingWritten = true
			w.suspPing, a.suspPing = a.suspPing, false
			s.drawProbe(a, p, false)
			m.pingSent(s.now)
			if a.revalPing {
				w.revalPing = true
			}
		}
	}
	// PIPE_ABANDON for attachments, on this pipe unless it is the one
	// abandoned and another exists.
	for i := 0; i < len(m.abandonQ); {
		q := m.abandonQ[i]
		qa := q.mp
		if !s.abandonGoesOn(q, p, prim) {
			i++
			continue
		}
		code := qa.abandonCode
		id := q.id
		n := varint.SizeVarint(wire.TypePipeAbandon) + varint.SizeVarint(id) + varint.SizeVarint(code)
		if !w.controlFrame(n, true, qa.abandonSent, func(b []byte) []byte { return wire.AppendPipeAbandon(b, id, code) }) {
			return
		}
		w.logFrame(frameDesc{kind: dAbandon, id: id})
		m.abandonQ.removeAt(i)
		if !qa.abandonSent {
			qa.abandonSent = true
			m.control.valid = false
			w.abandonsSent = append(w.abandonsSent, q)
		}
	}
	for id, pending := range m.owedAbandons() {
		code := pending.code
		n := varint.SizeVarint(wire.TypePipeAbandon) + varint.SizeVarint(id) + varint.SizeVarint(code)
		if !w.controlFrame(n, true, pending.repeated, func(b []byte) []byte { return wire.AppendPipeAbandon(b, id, code) }) {
			return
		}
		w.logFrame(frameDesc{kind: dAbandonID, id: id, val: code})
		delete(m.pendAbandon, id)
	}
	s.writeEpoch(w, p)
	// PIPE_STATUS requests: only for ACTIVE attachments.
	for i := 0; i < len(m.statusQ); {
		q := m.statusQ[i]
		if !statusReady(q, p) {
			i++
			continue
		}
		if !s.writeStatus(w, q) {
			return
		}
		m.statusQ.removeAt(i)
	}
}

// ackOwed reports a PIPE_ACK due on the primary pipe: the acknowledgement
// is due, or unreported positions wait for credit frames to ride along.
func (s *Session) ackOwed() bool {
	return s.ack.due(s.now) || s.ack.unreported() && s.creditPending()
}

// quiet reports a quiet session: no traffic frame within the busy window.
func (s *Session) quiet() bool { return s.now >= s.mp.busyUntil }

// ackPairOnly reports whether the pending acknowledgement answers only
// PIPE_PING frames of a quiet session, each on a pipe pair with a usable
// outbound direction: the answers then go on those pairs alone, neither on
// the primary pipe nor as a redundant copy.
func (s *Session) ackPairOnly() bool {
	if s.ack.other || s.ack.pairless || !s.quiet() {
		return false
	}
	return slices.ContainsFunc(s.mp.pairAcks, s.answersOnPair)
}

// answersOnPair reports whether the answer to a PIPE_PING received on p
// can go on p's outbound direction: it exists and is neither suspect nor
// failed.
func (s *Session) answersOnPair(p *Pipe) bool {
	a := p.mp
	return p.out != nil && !p.out.failed && a.liveOut() && a.out != OutSuspect
}

// primaryAckSent follows a PIPE_ACK on the primary pipe: the redundant
// copy is due 50 ms later, and the pipe may owe a liveness PIPE_PING.
func (s *Session) primaryAckSent(p *Pipe) {
	s.mp.ackChanged, s.mp.redundantPending = true, true
	s.mp.redundantAt = s.now.Add(50 * Millisecond)
	s.returnLiveness(p)
}

// idleFor reports whether p is unproven (MULTIPATH.md Section 6.4): for
// more than R_p = max(1 s, T_suspect_p) no ack-eliciting byte was sent on
// it, its acknowledged position did not advance and it was not made live.
// An advance proves the direction as a sent byte does, so an answer that
// acknowledges p's own PIPE_PING carries none, and PIPE_PING frames never
// answer each other however long an exchange takes.
func (s *Session) idleFor(p *Pipe) bool {
	a := p.mp
	return !a.needPing && s.now.Sub(max(a.lastElicit, a.liveAt, a.lastAdvance)) > max(Second, s.tSuspect(a))
}

// answerPing adds a PIPE_PING to the answer that p carries when p has been
// idle for R_p: one exchange then shows both directions of the pair to
// work, and the PIPE_PING starts p's next probe wait.
// Probes turned off do not change this.
func (s *Session) answerPing(p *Pipe) {
	if s.idleFor(p) {
		p.mp.needPing, p.mp.pingSince = true, s.now
		s.armPathDeadline(p, pathProbe)
	}
}

// returnLiveness adds a PIPE_PING to a primary or redundant PIPE_ACK of a
// busy session on a pipe idle for R_p, whose death would otherwise go
// unnoticed; in a quiet session such pipes are probed like any other.
func (s *Session) returnLiveness(p *Pipe) {
	if !s.quiet() && s.idleFor(p) {
		p.mp.needPing, p.mp.pingSince = true, s.now
		s.armPathDeadline(p, pathProbe)
	}
}

// carriesAck reports whether p's next record carries the pending
// PIPE_ACK that discharges the acknowledgement, or the answer to a
// PIPE_PING on p's pipe pair.
func (s *Session) carriesAck(p *Pipe) bool {
	if s.mp == nil || p.mp == nil {
		return true
	}
	a := p.mp
	if a.firstRecord || s.mp.pairAcks.contains(p) {
		return true
	}
	prim, _ := s.primary()
	return (p == prim || prim == nil) && !s.ackPairOnly()
}

// mpWantsWrite is WantsWrite for a pipe of a multipath session: true only
// when mpWrite would produce bytes now (W8).
func (s *Session) mpWantsWrite(p *Pipe, ol *outLane) bool {
	a := p.mp
	if a.hs || a.retired || a.out == OutFailed || a.out == OutClosed {
		return false
	}
	if ol.lane == 0 {
		if s.pathControlOn(p) || s.sessionControlOn(p) || s.recordDatagrams(p) {
			return true
		}
	}
	if ol.lane == 0 && len(a.copyQ) > 0 && s.open(p) {
		return true
	}
	if ol == p.out.lo.copyLane && len(a.selfCopyQ) > 0 && s.laneOpen(p, ol) {
		return true
	}
	return s.dataPendingOn(p, ol) && s.anyStreamOn(p, ol) && (s.shp == nil || !s.shapeHeld(p))
}

// waitsFor reports whether records or stream data wait for lane ol of p:
// mpWantsWrite with p's carrier taken to accept bytes. Data that the
// peer's credit, the limiter or a shaping delay holds back does not wait
// for the carrier, nor does data the scheduler would place on another
// pipe.
func (s *Session) waitsFor(p *Pipe, ol *outLane) bool {
	s.mp.openAs = p
	defer func() { s.mp.openAs = nil }()
	return s.mpWantsWrite(p, ol)
}

// anyStreamOn reports whether some stream direction would put data on
// lane ol of p now.
func (s *Session) anyStreamOn(p *Pipe, ol *outLane) bool {
	d := &s.mp.probe
	s.newDecision(d, p)
	d.probe = true
	it := s.sched.iter()
	for st := it.next(); st != nil; st = it.next() {
		if !s.canSendNow(st) || !s.onPipe(d, st) {
			continue
		}
		if p.out.lo.offered {
			if l, _, _ := s.laneFor(p, st); l != ol {
				continue
			}
		}
		return true
	}
	return false
}

// mpAfterBatch accounts a produced batch on a pipe of a multipath session.
func (s *Session) mpAfterBatch(w *recWriter, p *Pipe, ol *outLane, start uint64, ahead aheadSnap) {
	a := p.mp
	s.retainBatch(w, ol, start, ahead.bytes)
	if w.anyElic {
		if !a.unacked {
			a.unacked, a.oldestAt = true, s.now
			s.setAhead(p, ahead)
		}
		a.lastElicit = s.now
	}
	for _, q := range w.abandonsSent {
		s.abandonHanded(q)
	}
	w.abandonsSent = w.abandonsSent[:0]
	if w.revalPing {
		s.revalPingWritten(p)
	}
	if w.pingWritten {
		// Answered once the batch's end is acknowledged (pingsAcked).
		w.pingWritten, w.suspPing = false, false
		a.pingEnds = append(a.pingEnds, p.out.snd.so)
		if d := &a.health.receive; d.running && d.proof == HealthProofNone && d.probeEnd == 0 {
			d.probeLane, d.probeEnd = ol.lane, ol.pos
		}
	}
	if w.traffic {
		s.markBusy()
	}
}

// abandonGoesOn reports whether q's queued PIPE_ABANDON goes on p.
func (s *Session) abandonGoesOn(q, p, prim *Pipe) bool {
	qa := q.mp
	selfGraceful := qa.abandonCode == wire.NoError && q.out != nil && qa.liveOut() && !qa.drainSuspect
	if selfGraceful {
		return q == p
	}
	return q != p || prim == p || prim == nil
}

// creditPending reports pending credit frames (a PIPE_ACK rides
// along).
func (s *Session) creditPending() bool {
	return s.pend.maxData || s.creditStreams != 0
}

// writeCreditCopies copies the latest limits that were sent while the
// window was the bottleneck into the redundant snapshot's record.
func (s *Session) writeCreditCopies(w *recWriter) {
	if s.copyMaxData {
		v := s.conn.adv
		if w.controlFrame(1+varint.SizeVarint(v), true, true, func(b []byte) []byte { return wire.AppendMaxData(b, v) }) {
			s.copyMaxData = false
		}
	}
	for st := s.copyHead; st != nil; {
		next := st.copyNext
		id, v := st.id, st.rcv.adv
		if w.controlFrame(1+varint.SizeVarint(id)+varint.SizeVarint(v), true, true, func(b []byte) []byte { return wire.AppendMaxStreamData(b, id, v) }) {
			s.unlinkCopyMSD(st)
		}
		st = next
	}
}
