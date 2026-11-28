package core

import (
	"cmp"
	"slices"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// Tiers, usability and the tier gate.
// The gate is worked out at each scheduling decision from the values in
// force at that moment; it lives in session scratch storage and allocates
// nothing in steady state.

// class is the usable class of one moment: failover to standby pipes,
// and whether pipes in probation or awaiting revalidation count.
type class struct {
	standby       bool
	probOK        bool
	holdReturning bool
}

// tierInfo is one usable tier of a gate.
type tierInfo struct {
	active  *Pipe   // the usable pipe with the smallest OWD (session-affinity)
	lo, hi  int     // its pipes: gate.pipes[lo:hi]
	q, r    float64 // Q_t and R_t
	prio    uint64
	offered float64 // X_t
	sat     bool
}

// gate is the tier gate at one decision.
type gate struct {
	pipes        []*Pipe // usable pipes, tier by tier, in attachment order within a tier
	tiers        []tierInfo
	quality      []pathEstimate // lazy snapshot, indexed like pipes
	oldest       Time
	last         int // index of the least-preferred eligible tier
	fed          int // index of the least-preferred tier offered excess data
	cls          class
	dataWaiting  bool
	allSat       bool
	affinityOnly bool // cascade capacity is limited to the active pipe of each tier
}

func (g *gate) tierOf(p *Pipe) int {
	pr, _ := p.mp.effPrio()
	for i := range g.tiers {
		if g.tiers[i].prio == pr {
			return i
		}
	}
	return -1
}

func (g *gate) tierPipes(i int) []*Pipe { return g.pipes[g.tiers[i].lo:g.tiers[i].hi] }

// usable reports whether p is usable in the gate's class.
func (g *gate) usable(s *Session, p *Pipe) bool { return s.classUsable(g.cls, p) }

// admits reports whether p is eligible: usable, in an eligible tier.
func (g *gate) admits(s *Session, p *Pipe) bool {
	if !g.usable(s, p) {
		return false
	}
	i := g.tierOf(p)
	return i >= 0 && i <= g.last
}

// inProbation reports whether a pipe's probation is running.
func (a *attach) awaitingFailback() bool {
	return a.health.send.generation > 1 && !a.health.snapshot.Send.Settled
}

func (a *attach) inProbation() bool { return a.prob.on }

// maySend is the common outbound protocol gate. Control retains its
// existing allowance for live pre-confirmation, suspect and drain lanes.
func maySend(p *Pipe, control bool) bool {
	if p == nil || p.out == nil || p.out.failed {
		return false
	}
	a := p.mp
	if a == nil {
		return true
	}
	return !a.hs && !a.retired && !a.topologyOut &&
		(a.out == OutActive || control && a.liveOut())
}

func mayReceive(p *Pipe) bool {
	if p == nil || p.in == nil || p.in.ended {
		return false
	}
	a := p.mp
	return a == nil || !a.hs && !a.retired && !a.draining && !a.topologyIn && a.in == InOpen
}

// pipeUsable reports whether an outbound pipe is usable for data; probOK
// lets pipes in probation or awaiting revalidation count.
func (s *Session) pipeUsable(p *Pipe, probOK bool) bool {
	a := p.mp
	if a == nil || !maySend(p, false) {
		return false
	}
	return !a.penalized && (probOK || !a.inProbation() && !a.reval)
}

func (s *Session) classUsable(c class, p *Pipe) bool {
	if p.mp == nil {
		return false
	}
	_, sb := p.mp.effPrio()
	return sb == c.standby && s.pipeUsable(p, c.probOK) && (!c.holdReturning || !p.mp.awaitingFailback())
}

// usableClass returns the class of this moment; ok is false when no pipe
// is usable.
func (s *Session) usableClass() (class, bool) {
	var active, settled [2]bool
	var holdReturning bool
	for _, p := range s.pipes {
		if p.mp != nil && p.mp.health.snapshot.Send.Settled && s.pipeUsable(p, false) {
			holdReturning = true
			break
		}
	}
	for _, p := range s.pipes {
		if !s.pipeUsable(p, true) {
			continue
		}
		_, standby := p.mp.effPrio()
		i := b2i(standby)
		active[i] = true
		if !p.mp.inProbation() && !p.mp.reval && (!holdReturning || !p.mp.awaitingFailback()) {
			if !standby {
				return class{holdReturning: holdReturning}, true
			}
			settled[i] = true
		}
	}
	if settled[1] {
		return class{standby: true, holdReturning: holdReturning}, true
	}
	return class{standby: !active[0], probOK: true}, active[0] || active[1]
}

func (m *multiPath) invalidateCandidates() {
	clear(m.candidateOrder)
	m.candidateOrder = m.candidateOrder[:0]
	m.candidatesValid = false
}

func (s *Session) orderedCandidates() []*Pipe {
	m := s.mp
	if m.candidatesValid && m.candidateGen == m.stateGen && len(m.candidateOrder) == len(s.pipes) {
		return m.candidateOrder
	}
	m.invalidateCandidates()
	m.candidateOrder = append(m.candidateOrder, s.pipes...)
	slices.SortStableFunc(m.candidateOrder, func(a, b *Pipe) int {
		if a.mp == nil || b.mp == nil {
			return b2i(a.mp == nil) - b2i(b.mp == nil)
		}
		pa, _ := a.mp.effPrio()
		pb, _ := b.mp.effPrio()
		return cmp.Compare(pa, pb)
	})
	m.candidateGen, m.candidatesValid = m.stateGen, true
	return m.candidateOrder
}

// rateOf is R_p of an outbound pipe.
func (s *Session) rateOf(p *Pipe) float64 { return p.out.snd.re.value() }

// committed is the Q_p of tier saturation: bytes committed to p and not
// yet transmitted (queuedOn). The core hands bytes over at write
// opportunities only, so a pipe that merely refuses bytes saturates
// nothing.
func (s *Session) committed(p *Pipe) float64 { return float64(s.queuedOn(p)) }

// notsentLowat is the carrier low-water mark of multipath sessions.
const notsentLowat = 32 << 10

// queued is Q_p of the arrival estimate: what is committed, and at least
// the low-water mark while the carrier refuses bytes.
func (s *Session) queued(p *Pipe) float64 {
	q := s.committed(p)
	if !s.open(p) {
		q = max(q, notsentLowat)
	}
	return q
}

// open reports whether a pipe's carrier takes bytes now.
func (s *Session) open(p *Pipe) bool {
	if p.out == nil || p.out.failed {
		return false
	}
	return s.laneOpen(p, p.out.lanes[0])
}

// laneOpen reports whether lane ol of p takes bytes now: its carrier
// accepts them and the limiter does not hold p. While waitsFor evaluates
// p, its carrier counts as accepting them.
func (s *Session) laneOpen(p *Pipe, ol *outLane) bool {
	if s.limHeld(p) {
		return false
	}
	if s.mp != nil && s.mp.openAs == p {
		return true
	}
	return !ol.notWritable && !ol.writing
}

// buildGate works out the gate now into g, whose storage it reuses, and
// returns it (nil when no pipe is usable). Each holder of a gate owns its
// storage (a decision its own, one-shot readers the session's scratch
// gate), so building one never changes a gate another holder still reads.
func (s *Session) buildGate(g *gate) *gate {
	return s.buildGateFor(g, nil, 0, 0)
}

// buildGateFor is the common tier cascade. An original datagram contributes
// its bytes and age to the offered work, and masks pipes it cannot use.
func (s *Session) buildGateFor(g *gate, datagram *outDgram, size int, carrier uint8) *gate {
	if s.workProbe != nil {
		s.workProbe.gates++
	}
	c, ok := s.usableClass()
	if !ok {
		return nil
	}
	g.cls = c
	g.pipes = g.pipes[:0]
	g.tiers = g.tiers[:0]
	g.quality = g.quality[:0]
	for _, p := range s.orderedCandidates() {
		if s.classUsable(c, p) && (datagram == nil || s.datagramFitsCarrier(p, size, carrier)) {
			g.pipes = append(g.pipes, p)
		}
	}
	g.affinityOnly = datagram == nil && s.cfg.Policy == PolicySessionAffinity
	base := s.owdBase()
	for lo := 0; lo < len(g.pipes); {
		pr, _ := g.pipes[lo].mp.effPrio()
		active := g.pipes[lo]
		aOWD := owdFrom(active.mp, base)
		hi := lo + 1
		for hi < len(g.pipes) {
			q, _ := g.pipes[hi].mp.effPrio()
			if q != pr {
				break
			}
			o := owdFrom(g.pipes[hi].mp, base)
			if o < aOWD {
				active, aOWD = g.pipes[hi], o
			}
			hi++
		}
		g.tiers = append(g.tiers, tierInfo{prio: pr, lo: lo, hi: hi, active: active})
		lo = hi
	}
	// The cascade: only multi-tier gates need the
	// offered data and its age.
	g.last, g.fed, g.allSat, g.dataWaiting = 0, 0, false, false
	if len(g.tiers) == 1 {
		return g
	}
	x, oldest, has, spreadETA := s.waitingData()
	g.oldest, g.dataWaiting = oldest, has
	if datagram != nil {
		x += uint64(size)
		if !has || datagram.at < oldest {
			oldest = datagram.at
		}
		has, spreadETA = true, true
	}
	if spreadETA {
		g.affinityOnly = false
		s.gateQuality(g)
	}
	offered := float64(x)
	d := s.cfg.OverflowDelay
	for i := range g.tiers {
		t := &g.tiers[i]
		if g.affinityOnly {
			t.q, t.r = s.committed(t.active), s.rateOf(t.active)
		} else {
			for j := t.lo; j < t.hi; j++ {
				p := g.pipes[j]
				t.q += s.committed(p)
				if spreadETA {
					t.r += g.quality[j].rate
				} else {
					t.r += s.rateOf(p)
				}
			}
		}
		t.offered = offered
		drain := (t.q + offered) > t.r*float64(d)/1e9
		aged := false
		if has {
			since := oldest
			if i > 0 {
				if e, ok := s.mp.eligibleSince(t.prio); ok && e > since {
					since = e
				} else if !ok {
					since = s.now
				}
			}
			aged = s.now.Sub(since) >= d
		}
		t.sat = drain || aged
		g.last = i
		if offered > 0 {
			g.fed = i
		}
		g.allSat = t.sat
		room := t.r*float64(d)/1e9 - t.q
		if room > 0 {
			offered = max(0, offered-room)
		}
		if !t.sat {
			break
		}
	}
	return g
}

// takes reports whether an eligible pipe takes new data now: a pipe of a
// tier that is eligible only because data waited longer than D takes data
// only while no pipe of a more-preferred tier can take bytes.
func (s *Session) takes(g *gate, p *Pipe) bool {
	takes, _ := s.tierRequirement(g, p)
	return takes
}

// tierRequirement returns the open, preferred path preventing a lower
// tier from taking a chunk. A concrete waiter can subscribe to that path.
func (s *Session) tierRequirement(g *gate, p *Pipe) (bool, *Pipe) {
	if !g.admits(s, p) {
		return false, nil
	}
	ti := g.tierOf(p)
	if ti <= g.fed {
		return true, nil
	}
	for i := range ti {
		if g.affinityOnly {
			if q := g.tiers[i].active; s.open(q) {
				return false, q
			}
			continue
		}
		for _, q := range g.tierPipes(i) {
			if s.open(q) {
				return false, q
			}
		}
	}
	return true, nil
}

// tierMark is when a tier last became eligible.
type tierMark struct {
	prio  uint64
	since Time
}

func (m *multiPath) eligibleSince(prio uint64) (Time, bool) {
	for _, t := range m.tierSince {
		if t.prio == prio {
			return t.since, true
		}
	}
	return 0, false
}

// refreshTiers records since when each eligible tier is eligible and
// reports saturation changes (at most one event per tier per second).
func (s *Session) refreshTierPlacement() {
	if s.sched.mask != 0 || len(s.mp.satRep) != 0 {
		s.refreshTiers(s.buildGate(&s.mp.gateBuf))
	}
}

func (s *Session) refreshTiers(g *gate) {
	m := s.mp
	keep := m.tierSince[:0]
	if g != nil {
		for i := 0; i <= g.last; i++ {
			pr := g.tiers[i].prio
			since, ok := Time(0), false
			for _, t := range m.tierSince {
				if t.prio == pr {
					since, ok = t.since, true
				}
			}
			if !ok {
				since = s.now
			}
			keep = append(keep, tierMark{pr, since})
		}
	}
	m.tierSince = keep
	s.reportSaturation(g)
	s.checkTakeover()
	m.tierAt = s.tierDeadline(g)
}

type satReport struct {
	prio uint64
	sat  bool
	at   Time
}

func (s *Session) reportSaturation(g *gate) {
	m := s.mp
	seen := func(pr uint64) (bool, bool) {
		if g == nil {
			return false, false
		}
		for i := 0; i <= g.last && i < len(g.tiers); i++ {
			if g.tiers[i].prio == pr {
				return g.tiers[i].sat, true
			}
		}
		return false, false
	}
	if g != nil {
		for i := 0; i <= g.last; i++ {
			t := g.tiers[i]
			found := false
			for j := range m.satRep {
				r := &m.satRep[j]
				if r.prio != t.prio {
					continue
				}
				found = true
				if r.sat != t.sat && s.now >= r.at.Add(saturationEvery) {
					r.sat, r.at = t.sat, s.now
					s.emit(Event{Kind: EvTierSaturation, Code: t.prio, Saturated: t.sat})
				}
			}
			if !found && t.sat {
				m.satRep = append(m.satRep, satReport{t.prio, true, s.now})
				s.emit(Event{Kind: EvTierSaturation, Code: t.prio, Saturated: true})
			}
		}
	}
	out := m.satRep[:0]
	for _, r := range m.satRep {
		if _, ok := seen(r.prio); ok {
			out = append(out, r)
			continue
		}
		if r.sat {
			s.emit(Event{Kind: EvTierSaturation, Code: r.prio, Saturated: false})
		}
	}
	m.satRep = out
}

// tierDeadline is the next moment the age test can change a decision.
func (s *Session) tierDeadline(g *gate) Time {
	m := s.mp
	if len(m.tierSince) == 0 && len(m.satRep) == 0 {
		return Never
	}
	next := Never
	if g != nil && g.dataWaiting {
		oldest := g.oldest
		d := s.cfg.OverflowDelay
		next = minT(next, oldest.Add(d))
		for _, t := range m.tierSince {
			next = minT(next, maxT(oldest, t.since).Add(d))
		}
	}
	for _, r := range m.satRep {
		next = minT(next, r.at.Add(saturationEvery))
	}
	if next <= s.now {
		return Never
	}
	return next
}

// waitingData is X of the most-preferred tier and the age of its oldest
// byte: stream data ready within stream and session credit and not yet
// committed to a pipe, plus pending reinjection.
func (s *Session) waitingData() (uint64, Time, bool, bool) {
	spreadETA := s.cfg.Policy == PolicySpreadETA
	var fresh, lost uint64
	var oldest Time
	has := false
	it := s.sched.iter()
	for st := it.next(); st != nil; st = it.next() {
		sp := &st.snd
		if m := sp.mps; m != nil {
			lost += m.resend.total()
		}
		r := s.readyBytes(st)
		if r == 0 && (sp.mps == nil || sp.mps.resend.len() == 0) {
			continue
		}
		spreadETA = spreadETA || s.policyOf(st) == PolicySpreadETA
		fresh += r
		w := max(sp.creditAt, st.waitStart(), s.conn.creditAt)
		if !has || w < oldest {
			oldest, has = w, true
		}
	}
	room := s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent)
	return min64(fresh, room) + lost, oldest, has, spreadETA
}

// readyBytes is a stream direction's data ready to send within its own
// credit and not yet committed (the preamble counts with it).
func (s *Session) readyBytes(st *Stream) uint64 {
	sp := &st.snd
	off := max64(sp.sent, sp.q.base)
	end := sp.sendEnd()
	if end <= off {
		return 0
	}
	n := end - off
	if st.idSet {
		n = min64(n, sp.maxData-min64(sp.maxData, off))
	} else if s.feat.Preamble {
		n += uint64(st.preSize()) // prepended when the ID is allocated
	}
	return n
}

// waitStart is when the stream direction's oldest ready byte began to
// wait: its write, or the start of the writer's wait for buffer space.
func (st *Stream) waitStart() Time {
	sp := &st.snd
	w := sp.waitAt
	if sp.wwait && sp.wwaitAt < w {
		w = sp.wwaitAt
	}
	return w
}

// ---- settings ----

// PipeSettings changes the tier settings of an attachment; nil fields
// stay. ClearSend* return a field to the peer's request.
type PipeSettings struct {
	SendPriority      *uint64
	SendStandby       *bool
	RecvPriority      *uint64
	RecvStandby       *bool
	Weight            *uint64
	Probation         *Duration // <0: the session default; 0 disables
	Liveness          *PipeLiveness
	ClearSendPriority bool
	ClearSendStandby  bool
}

// SetPipe applies tier settings to an attachment.
func (s *Session) SetPipe(id uint64, o PipeSettings) error {
	if s.mp == nil {
		return ErrNotNegotiated
	}
	p := s.pipe(id)
	if p == nil || p.mp == nil || p.mp.retired {
		return ErrInvalid
	}
	a := p.mp
	bp, bs := a.effPrio()
	bw := a.weight
	wasProbation := a.prob.on
	if o.SendPriority != nil {
		v := *o.SendPriority
		a.sendPrio = &v
	} else if o.ClearSendPriority {
		a.sendPrio = nil
	}
	if o.SendStandby != nil {
		v := *o.SendStandby
		a.sendStandby = &v
	} else if o.ClearSendStandby {
		a.sendStandby = nil
	}
	if o.Weight != nil {
		a.weight = *o.Weight
	}
	req := false
	if o.RecvPriority != nil {
		a.recvPrio = *o.RecvPriority
		req = true
	}
	if o.RecvStandby != nil {
		a.recvStandby = *o.RecvStandby
		req = true
	}
	if o.Probation != nil {
		a.probOpt = *o.Probation
		s.armPathDeadline(p, pathProbationEnd)
		s.armPathDeadline(p, pathProbationProbe)
		if s.probationBase(p) < 0 && a.prob.on {
			a.prob.on = false
		}
	}
	if o.Liveness != nil {
		a.live = *o.Liveness
		s.refreshPathDeadlines(p)
	}
	if wasProbation != a.prob.on {
		s.syncPipeHealth(p)
		s.capacityChanged()
	}
	if req {
		s.requestStatus(p)
	}
	if ap, as := a.effPrio(); ap != bp || as != bs || a.weight != bw {
		s.mp.control.valid = false
		s.mp.invalidateCandidates()
		s.emitPriority(p)
		s.pickChanged()
	}
	s.checkTakeover()
	s.refreshProbeDeadlines()
	s.wakeAllWriters()
	return nil
}

func (s *Session) emitPriority(p *Pipe) {
	if p.out == nil {
		return
	}
	pr, sb := p.mp.effPrio()
	s.emit(Event{Kind: EvPipePriority, Pipe: p.id, Code: pr, Saturated: sb, Count: p.mp.weight, Source: p.mp.prioSource()})
}

// statusReq is one PIPE_STATUS owed.
type statusReq struct {
	seq     uint64
	standby bool
	prio    uint64
}

const statusOwedMax = 8

// requestStatus queues the current request for the peer's outbound
// direction as a new PIPE_STATUS (each change is one frame with the next
// sequence number).
func (s *Session) requestStatus(p *Pipe) {
	a := p.mp
	if p.in == nil {
		return
	}
	a.statusSeq++
	a.statusHeld = a.statusHeld || a.hs
	if len(a.statusQ) >= statusOwedMax {
		a.statusQ = a.statusQ[1:]
	}
	s.queueStatus(p, a.statusSeq)
}

// statusLost re-queues the latest request after its frame was lost.
func (s *Session) statusLost(p *Pipe, seq uint64) {
	a := p.mp
	if seq != a.statusSeq {
		return
	}
	for _, r := range a.statusQ {
		if r.seq == seq {
			return
		}
	}
	s.queueStatus(p, seq)
}

func (s *Session) queueStatus(p *Pipe, seq uint64) {
	a := p.mp
	if len(a.statusQ) == 0 {
		s.mp.statusQ.add(p)
	}
	a.statusQ = append(a.statusQ, statusReq{seq, a.recvStandby && !a.takeover, a.recvPrio})
	s.wakeAllWriters()
}

// onPipeStatus applies the peer's request for the local outbound
// direction.
func (s *Session) onPipeStatus(f *wire.Frame) *wire.Error {
	m := s.mp
	q := s.pipe(f.PipeID)
	if q == nil {
		if s.ownParity(f.PipeID) && f.PipeID >= m.nextID {
			return wire.NewError(wire.ProtocolViolation, "PIPE_STATUS for an untaken pipe ID", "pipe", f.PipeID)
		}
		return nil
	}
	a := q.mp
	if a == nil || a.retired || q.out == nil || f.Seq <= a.peerSeq {
		return nil
	}
	bp, bs := a.effPrio()
	a.peerSeq = f.Seq
	a.peerPrio, a.peerStandby = f.Priority, f.Status == 1
	if ap, as := a.effPrio(); ap != bp || as != bs {
		m.control.valid = false
		m.invalidateCandidates()
		s.emitPriority(q)
	}
	s.checkTakeover()
	s.refreshProbeDeadlines()
	s.wakeAllWriters()
	return nil
}

// checkTakeover runs the standby takeover: while an
// attachment's own standby outbound pipe carries the traffic because no
// non-standby pipe is usable, its standby request for the peer's
// direction is replaced by "available"; once a non-standby pipe is usable
// again the configured request is restored.
func (s *Session) checkTakeover() {
	if s.mp == nil {
		return
	}
	c, ok := s.usableClass()
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || a.retired {
			continue
		}
		_, sb := a.effPrio()
		want := a.recvStandby && sb && ok && c.standby && s.classUsable(c, p)
		if want != a.takeover {
			a.takeover = want
			s.requestStatus(p)
		}
	}
}

// writeStatus writes the PIPE_STATUS frames owed for q.
func (s *Session) writeStatus(w *recWriter, q *Pipe) bool {
	qa := q.mp
	for len(qa.statusQ) > 0 {
		r := qa.statusQ[0]
		id := q.id
		st := uint64(0)
		if r.standby {
			st = 1
		}
		n := 4 + varint.SizeVarint(id) + varint.SizeVarint(r.seq) + varint.SizeVarint(st) + varint.SizeVarint(r.prio)
		if !w.controlFrame(n, true, r.seq <= qa.statusWritten, func(b []byte) []byte { return wire.AppendPipeStatus(b, id, r.seq, st, r.prio) }) {
			return false
		}
		qa.statusWritten = max(qa.statusWritten, r.seq)
		w.logFrame(frameDesc{kind: dStatus, id: id, val: r.seq})
		qa.statusQ = qa.statusQ[1:]
	}
	qa.statusQ, qa.statusHeld = nil, false
	return true
}

// statusReady reports whether q's PIPE_STATUS may go out on p: the attachment is
// ACTIVE, and a change made while JOINING goes on the attachment itself, or for a
// down attachment once its first record has arrived.
func statusReady(q, p *Pipe) bool {
	qa := q.mp
	if qa == nil || len(qa.statusQ) == 0 || qa.retired || qa.hs || q.in == nil || qa.in != InOpen {
		return false
	}
	return !qa.statusHeld || q == p
}
