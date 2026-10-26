package core

import (
	"sort"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// ---- control order (MULTIPATH.md 6.5) ----

// effPrio returns the effective send priority and standby flag of an
// outbound direction (MULTIPATH.md 9): local setting, else the peer's
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

// minRTT is min_rtt_p, or the smoothed initial value before a sample.
func (a *attach) minRTT() Duration {
	if a.rtt.has {
		return a.rtt.min
	}
	return a.rtt.smoothed
}

// owd is OWD_p (SCHEDULER.md 2.2).
func (s *Session) owd(p *Pipe) Duration {
	a := p.mp
	if !a.rtt.has {
		return a.rtt.smoothed / 2
	}
	m := a.rtt.min
	for _, q := range s.pipes {
		if q.mp != nil && q.out != nil && q.mp.liveOut() && q.mp.rtt.has && q.mp.rtt.min < m {
			m = q.mp.rtt.min
		}
	}
	return a.rtt.min - m/2
}

func (a *attach) liveOut() bool {
	return a.out == outJoining || a.out == outActive || a.out == outSuspect || a.out == outDraining
}

// controlRank orders outbound pipes for control frames.
func (s *Session) controlRank(p *Pipe) (int, uint64, int, Duration) {
	a := p.mp
	r := 0
	switch {
	case a.out == outActive:
		r = 0
	case a.out == outJoining:
		r = 1
	case a.out == outSuspect:
		r = 2
	case a.out == outDraining && !a.drainSuspect:
		r = 3
	default:
		r = 4
	}
	pr, sb := a.effPrio()
	sbi := 0
	if sb {
		sbi = 1
	}
	return r, pr, sbi, s.owd(p)
}

// controlOrder returns the live outbound pipes in the control order.
func (s *Session) controlOrder(dst []*Pipe) []*Pipe {
	for _, p := range s.pipes {
		if p.mp != nil && p.out != nil && !p.out.failed && p.mp.liveOut() && !p.mp.retired {
			if p.mp.out == outDraining && p.mp.abandonSent && p.mp.abandonRecv {
				continue
			}
			dst = append(dst, p)
		}
	}
	sort.SliceStable(dst, func(i, j int) bool {
		ri, pi, si, oi := s.controlRank(dst[i])
		rj, pj, sj, oj := s.controlRank(dst[j])
		if ri != rj {
			return ri < rj
		}
		if si != sj {
			return si < sj
		}
		if pi != pj {
			return pi < pj
		}
		if oi != oj {
			return oi < oj
		}
		return dst[i].id < dst[j].id
	})
	return dst
}

// primary returns the first pipe of the control order (and the second).
func (s *Session) primary() (*Pipe, *Pipe) {
	var buf [8]*Pipe
	o := s.controlOrder(buf[:0])
	var a, b *Pipe
	if len(o) > 0 {
		a = o[0]
	}
	if len(o) > 1 {
		b = o[1]
	}
	return a, b
}

// ---- PIPE_PING rate limit (MULTIPATH.md 6.4) ----

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
	for _, q := range s.pipes {
		if q.mp != nil && q != p && q.mp.needPing && q.out != nil && !q.out.failed &&
			q.mp.pingSince < p.mp.pingSince {
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
	a := p.mp
	if a.held && !s.joinAllowedFor(p) {
		return
	}
	if ol.lane == 0 {
		s.refreshTiers()
		s.mpPipeFrames(w, p, ol)
		prim, _ := s.primary()
		if prim == p || prim == nil {
			s.writeControl(w, p)
		}
	}
	d := &s.mp.dec
	if ol.lane == 0 {
		s.refreshBindings(s.buildGate())
	}
	s.newDecision(d, p)
	if ol.lane == 0 {
		s.writeCopies(d, w, &a.copyQ)
		s.opportunistic(d, w)
	} else if ol == p.out.lo.copyLane {
		s.writeCopies(d, w, &a.selfCopyQ)
	}
	s.writeData(w, p, ol)
}

// mpPipeFrames writes PIPE_JOIN, PIPE_ACK, PIPE_PING, PIPE_ABANDON and
// PIPE_STATUS for the pipe.
func (s *Session) mpPipeFrames(w *recWriter, p *Pipe, ol *outLane) {
	a := p.mp
	m := s.mp
	if a.held && !s.joinAllowedFor(p) {
		return
	}
	first := a.firstRecord
	if a.join != nil && s.joinAllowedFor(p) {
		j := a.join
		n := len(wire.AppendJoin(nil, j))
		if !w.frame(n, true, func(b []byte) []byte { return wire.AppendJoin(b, j) }) {
			return
		}
		a.join = nil
		if !a.oob {
			a.needPing = true
			a.pingSince = s.now
		}
	}
	prim, second := s.primary()
	full := first || a.ackPair
	if s.ack != nil {
		due := s.ack.due(s.now)
		switch {
		case full:
			s.ack.writeSnapshot(w, true)
		case (due || s.ack.unreported() && s.creditPending()) && (p == prim || prim == nil):
			s.ack.writeSnapshot(w, false)
			m.ackChanged = true
			m.redundantAt = s.now.Add(50 * Millisecond)
		case m.ackChanged && p == second && s.now >= m.redundantAt:
			s.ack.writeSnapshot(w, true)
			m.ackChanged = false
			s.writeCreditCopies(w)
		}
		a.ackPair = false
	}
	a.firstRecord = false
	if a.needPing && a.out != outFailed && m.pingAllowed(s.now) && s.oldestPingWaiter(p) {
		if w.frame(wire.VarintLen(wire.TypePipePing), true, func(b []byte) []byte { return wire.AppendPipePing(b) }) {
			a.needPing = false
			a.pingsSent++
			m.pingSent(s.now)
			if a.revalPing {
				w.revalPing = true
			}
		}
	}
	// PIPE_ABANDON for attachments, on this pipe unless it is the one
	// abandoned and another exists.
	for _, q := range s.pipes {
		qa := q.mp
		if qa == nil || !qa.abandonQueued || qa.retired {
			continue
		}
		// A graceful removal of an own outbound pipe puts its PIPE_ABANDON
		// on that pipe; otherwise another pipe carries it (MULTIPATH 8.1).
		selfGraceful := qa.abandonCode == wire.NoError && q.out != nil && qa.liveOut() && !qa.drainSuspect
		if selfGraceful && q != p || !selfGraceful && q == p && prim != nil && prim != p {
			continue
		}
		code := qa.abandonCode
		id := q.id
		n := wire.VarintLen(wire.TypePipeAbandon) + wire.VarintLen(id) + wire.VarintLen(code)
		if !w.frame(n, true, func(b []byte) []byte { return wire.AppendPipeAbandon(b, id, code) }) {
			return
		}
		w.logFrame(frameDesc{kind: dAbandon, id: id})
		qa.abandonQueued = false
		if !qa.abandonSent {
			qa.abandonSent = true
			w.abandonsSent = append(w.abandonsSent, q)
		}
	}
	for id := range m.pendAbandon {
		code := m.pendCode[id]
		n := wire.VarintLen(wire.TypePipeAbandon) + wire.VarintLen(id) + wire.VarintLen(code)
		if !w.frame(n, true, func(b []byte) []byte { return wire.AppendPipeAbandon(b, id, code) }) {
			return
		}
		delete(m.pendAbandon, id)
		delete(m.pendCode, id)
	}
	// PIPE_STATUS requests (MULTIPATH.md 9): only for ACTIVE attachments.
	for _, q := range s.pipes {
		if statusReady(q) && !s.writeStatus(w, q) {
			return
		}
	}
}

// mpWantsWrite is WantsWrite for a pipe of a multipath session: true only
// when mpWrite would produce bytes now (W8).
func (s *Session) mpWantsWrite(p *Pipe, ol *outLane) bool {
	a := p.mp
	if a.retired || a.out == outFailed || a.out == outClosed || a.held && !s.joinAllowedFor(p) {
		return false
	}
	if ol.lane == 0 {
		if a.join != nil && s.joinAllowedFor(p) || a.firstRecord && a.join == nil && s.ack != nil && s.ack.hasEntries() || a.ackPair {
			return true
		}
		if a.needPing && s.mp.pingAllowed(s.now) && s.oldestPingWaiter(p) {
			return true
		}
		prim, second := s.primary()
		if s.ack != nil && s.ack.due(s.now) && (p == prim || prim == nil) {
			return true
		}
		if s.mp.ackChanged && p == second && s.now >= s.mp.redundantAt {
			return true
		}
		for _, q := range s.pipes {
			if q.mp != nil && !q.mp.retired && (q.mp.abandonQueued && s.abandonGoesOn(q, p, prim) ||
				statusReady(q)) {
				return true
			}
		}
		if len(s.mp.pendAbandon) > 0 {
			return true
		}
		if (p == prim || prim == nil) && s.controlPending(p) {
			return true
		}
	}
	if ol.lane == 0 && len(a.copyQ) > 0 && s.open(p) {
		return true
	}
	if ol == p.out.lo.copyLane && len(a.selfCopyQ) > 0 && !ol.writing && !ol.notWritable && !s.limHeld(p) {
		return true
	}
	return s.dataPendingOn(p, ol) && s.anyStreamOn(p, ol)
}

// anyStreamOn reports whether some stream direction would put data on
// lane ol of p now.
func (s *Session) anyStreamOn(p *Pipe, ol *outLane) bool {
	d := &s.mp.dec
	s.newDecision(d, p)
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
func (s *Session) mpAfterBatch(w *recWriter, p *Pipe, ol *outLane, start uint64) {
	a := p.mp
	s.retainBatch(w, ol, start)
	if w.anyElic {
		if !a.unacked {
			a.unacked, a.oldestAt = true, s.now
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
	if w.activity {
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

// creditPending reports pending credit frames (a PIPE_ACK rides along,
// MULTIPATH.md 6.3).
func (s *Session) creditPending() bool {
	if s.pend.maxData {
		return true
	}
	for st := s.ctlHead; st != nil; st = st.ctlNext {
		if st.ctl&ctlMaxStreamData != 0 {
			return true
		}
	}
	return false
}

// writeCreditCopies copies the latest limits that were sent while the
// window was the bottleneck into the redundant snapshot's record
// (MULTIPATH.md 6.5).
func (s *Session) writeCreditCopies(w *recWriter) {
	if s.copyMaxData {
		v := s.conn.adv
		if w.frame(1+wire.VarintLen(v), true, func(b []byte) []byte { return wire.AppendMaxData(b, v) }) {
			s.copyMaxData = false
		}
	}
	for _, st := range s.streams {
		if !st.copyMSD {
			continue
		}
		id, v := st.id, st.rcv.adv
		if w.frame(1+wire.VarintLen(id)+wire.VarintLen(v), true, func(b []byte) []byte { return wire.AppendMaxStreamData(b, id, v) }) {
			st.copyMSD = false
		}
	}
}
