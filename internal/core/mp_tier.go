package core

import (
	"slices"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// Tiers, usability and the tier gate (MULTIPATH.md 9).
// The gate is worked out at each scheduling decision from the values in
// force at that moment; it lives in session scratch storage and allocates
// nothing in steady state.

// class is the usable class of one moment: failover to standby pipes,
// and whether pipes in probation or awaiting revalidation count.
type class struct {
	standby bool
	probOK  bool
}

// tierInfo is one usable tier of a gate.
type tierInfo struct {
	prio    uint64
	lo, hi  int     // its pipes: gate.pipes[lo:hi]
	active  *Pipe   // the usable pipe with the smallest OWD (session-affinity)
	q, r    float64 // Q_t and R_t
	offered float64 // X_t
	sat     bool
}

// gate is the tier gate at one decision.
type gate struct {
	cls    class
	pipes  []*Pipe // usable pipes, tier by tier, in attachment order within a tier
	tiers  []tierInfo
	last   int // index of the least-preferred eligible tier
	fed    int // index of the least-preferred tier offered excess data
	allSat bool
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
func (a *attach) inProbation() bool { return a.prob.on }

// pipeUsable reports whether an outbound pipe is usable for data; probOK
// lets pipes in probation or awaiting revalidation count.
func (s *Session) pipeUsable(p *Pipe, probOK bool) bool {
	a := p.mp
	if a == nil || p.out == nil || p.out.failed || a.out != outActive || a.retired {
		return false
	}
	return !a.penalized && (probOK || !a.inProbation() && !a.reval)
}

func (s *Session) classUsable(c class, p *Pipe) bool {
	if p.mp == nil {
		return false
	}
	_, sb := p.mp.effPrio()
	return sb == c.standby && s.pipeUsable(p, c.probOK)
}

// usableClass returns the class of this moment; ok is false when no pipe
// is usable.
func (s *Session) usableClass() (class, bool) {
	hasClass := func(c class) bool {
		for _, p := range s.pipes {
			if s.classUsable(c, p) {
				return true
			}
		}
		return false
	}
	settled := hasClass(class{false, false}) || hasClass(class{true, false})
	c := class{probOK: !settled}
	c.standby = !hasClass(class{false, c.probOK})
	return c, hasClass(c)
}

// rateOf is R_p of an outbound pipe.
func (s *Session) rateOf(p *Pipe) float64 { return p.out.snd.re.value() }

// committed is the Q_p of tier saturation: bytes committed to p and not
// yet transmitted. The core hands bytes over at write opportunities only,
// so what is committed is what the carrier reports unsent; a pipe that
// merely refuses bytes saturates nothing.
func (s *Session) committed(p *Pipe) float64 { return float64(p.mp.unsentAhead) }

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
	ol := p.out.lanes[0]
	return !ol.notWritable && !ol.writing && !s.limHeld(p)
}

// buildGate works out the gate now (nil when no pipe is usable).
func (s *Session) buildGate() *gate {
	c, ok := s.usableClass()
	if !ok {
		return nil
	}
	m := s.mp
	g := &m.gateBuf
	g.cls = c
	g.pipes = g.pipes[:0]
	g.tiers = g.tiers[:0]
	for _, p := range s.pipes {
		if s.classUsable(c, p) {
			g.pipes = append(g.pipes, p)
		}
	}
	slices.SortStableFunc(g.pipes, func(a, b *Pipe) int {
		pa, _ := a.mp.effPrio()
		pb, _ := b.mp.effPrio()
		switch {
		case pa < pb:
			return -1
		case pa > pb:
			return 1
		}
		return 0
	})
	sa := s.cfg.Policy == PolicySessionAffinity
	for lo := 0; lo < len(g.pipes); {
		pr, _ := g.pipes[lo].mp.effPrio()
		active := g.pipes[lo]
		aOWD := s.owd(active)
		hi := lo + 1
		for hi < len(g.pipes) {
			q, _ := g.pipes[hi].mp.effPrio()
			if q != pr {
				break
			}
			o := s.owd(g.pipes[hi])
			if o < aOWD {
				active, aOWD = g.pipes[hi], o
			}
			hi++
		}
		t := tierInfo{prio: pr, lo: lo, hi: hi, active: active}
		if sa {
			t.q, t.r = s.committed(active), s.rateOf(active)
		} else {
			for _, p := range g.pipes[lo:hi] {
				t.q += s.committed(p)
				t.r += s.rateOf(p)
			}
		}
		g.tiers = append(g.tiers, t)
		lo = hi
	}
	// The cascade: only multi-tier gates need the
	// offered data and its age.
	g.last, g.fed, g.allSat = 0, 0, false
	if len(g.tiers) == 1 {
		return g
	}
	x, oldest, has := s.waitingData()
	offered := float64(x)
	d := s.cfg.OverflowDelay
	for i := range g.tiers {
		t := &g.tiers[i]
		t.offered = offered
		drain := (t.q + offered) > t.r*float64(d)/1e9
		aged := false
		if has {
			since := oldest
			if i > 0 {
				if e, ok := m.eligibleSince(t.prio); ok && e > since {
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
	if !g.admits(s, p) {
		return false
	}
	ti := g.tierOf(p)
	if ti <= g.fed {
		return true
	}
	for i := 0; i < ti; i++ {
		if s.cfg.Policy == PolicySessionAffinity {
			if s.open(g.tiers[i].active) {
				return false
			}
			continue
		}
		for _, q := range g.tierPipes(i) {
			if s.open(q) {
				return false
			}
		}
	}
	return true
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
func (s *Session) refreshTiers() {
	m := s.mp
	g := s.buildGate()
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
func (s *Session) tierDeadline() Time {
	m := s.mp
	if len(m.tierSince) == 0 && len(m.satRep) == 0 {
		return Never
	}
	next := Never
	if _, oldest, has := s.waitingData(); has {
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
func (s *Session) waitingData() (uint64, Time, bool) {
	var fresh, lost uint64
	var oldest Time
	has := false
	it := s.sched.iter()
	for st := it.next(); st != nil; st = it.next() {
		sp := &st.snd
		if m := sp.mps; m != nil {
			for _, r := range m.resend {
				lost += r.hi - r.lo
			}
		}
		r := s.readyBytes(st)
		if r == 0 && (sp.mps == nil || len(sp.mps.resend) == 0) {
			continue
		}
		fresh += r
		w := st.waitStart()
		if sp.creditAt > w {
			w = sp.creditAt
		}
		if s.conn.creditAt > w {
			w = s.conn.creditAt
		}
		if !has || w < oldest {
			oldest, has = w, true
		}
	}
	room := s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent)
	return min64(fresh, room) + lost, oldest, has
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
	ClearSendPriority bool
	ClearSendStandby  bool
	RecvPriority      *uint64
	RecvStandby       *bool
	Weight            *uint64
	Probation         *Duration // <0: the session default; 0 disables
}

// SetPipe applies tier settings to an attachment (MULTIPATH.md 9).
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
		if s.probationBase(p) < 0 && a.prob.on {
			a.prob.on = false
		}
	}
	if req {
		s.requestStatus(p)
	}
	if ap, as := a.effPrio(); ap != bp || as != bs || a.weight != bw {
		s.emitPriority(p)
		s.pickChanged()
	}
	s.checkTakeover()
	s.wakeAllWriters()
	return nil
}

func (s *Session) emitPriority(p *Pipe) {
	if p.out == nil {
		return
	}
	pr, sb := p.mp.effPrio()
	s.emit(Event{Kind: EvPipePriority, Pipe: p.id, Code: pr, Saturated: sb, Count: p.mp.weight})
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
	if len(a.statusQ) >= statusOwedMax {
		a.statusQ = a.statusQ[1:]
	}
	a.statusQ = append(a.statusQ, statusReq{a.statusSeq, a.recvStandby && !a.takeover, a.recvPrio})
	s.wakeAllWriters()
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
	a.statusQ = append(a.statusQ, statusReq{seq, a.recvStandby && !a.takeover, a.recvPrio})
	s.wakeAllWriters()
}

// onPipeStatus applies the peer's request for the local outbound
// direction (MULTIPATH.md 9).
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
		s.emitPriority(q)
	}
	s.checkTakeover()
	s.wakeAllWriters()
	return nil
}

// checkTakeover runs the standby takeover (MULTIPATH.md 9): while an
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
		n := 4 + wire.VarintLen(id) + wire.VarintLen(r.seq) + wire.VarintLen(st) + wire.VarintLen(r.prio)
		if !w.frame(n, true, func(b []byte) []byte { return wire.AppendPipeStatus(b, id, r.seq, st, r.prio) }) {
			return false
		}
		w.logFrame(frameDesc{kind: dStatus, id: id, val: r.seq})
		qa.statusQ = qa.statusQ[1:]
	}
	if len(qa.statusQ) == 0 {
		qa.statusQ = nil
	}
	return true
}

// statusReady reports whether q's PIPE_STATUS may go out: the attachment
// is ACTIVE (a change made while JOINING waits for that).
func statusReady(q *Pipe) bool {
	qa := q.mp
	return qa != nil && len(qa.statusQ) > 0 && !qa.retired && q.in != nil && qa.in == inOpen &&
		qa.out != outJoining && (!qa.joinerLocal || qa.out != outNone || qa.in != inJoining)
}
