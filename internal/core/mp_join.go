package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
)

// JoinOptions are the options of a local join.
type JoinOptions struct {
	Dir          uint8 // MULTIPATH 5.1: 0 up, 1 down, 2 bidi (joiner's view)
	FateGroup    uint64
	OOB          bool
	CarrierBound bool
	Binding      []byte // TLS exporter of the new carrier (carrier-bound)
	SendPriority *uint64
	SendStandby  *bool
	RecvPriority uint64
	RecvStandby  bool
	Weight       uint64
	Probation    *Duration // nil: the session default; 0 disables
	SuspectFloor Duration  // 0: the session default
	DeadTimeout  Duration  // 0: the session default
	In, Out      CarrierInfo
	OutSig       *Signal
}

// AddPipeResult says what happened to a local join.
type AddPipeResult uint8

const (
	AddPipeOK AddPipeResult = iota
	// AddPipePending means the cap or the refusal backoff holds the join; the
	// carrier waits and joins once allowed (MULTIPATH.md 5.7 step 1).
	AddPipePending
)

// NextPipeID returns the ID the next local join will take.
func (s *Session) NextPipeID() uint64 {
	if s.mp == nil {
		return 0
	}
	return s.mp.nextID
}

// joinAllowedFor reports whether a held local join may go now.
func (s *Session) joinAllowedFor(p *Pipe) bool {
	a := p.mp
	if !a.held {
		return true
	}
	if s.now < s.mp.backoffUntil {
		return false
	}
	n := 0
	for _, q := range s.pipes {
		if q != p && q.mp != nil && q.mp.joinerLocal && !q.mp.retired && !q.mp.held {
			n++
		}
	}
	if n >= s.mp.ownCap {
		return false
	}
	a.held = false
	a.attachedAt = s.now
	return true
}

// joinAllowed reports whether a local join may start now.
func (s *Session) joinAllowed() bool {
	m := s.mp
	if s.now < m.backoffUntil {
		return false
	}
	return s.ownAttachments() < m.ownCap
}

// ownAttachments counts this endpoint's own unretired joins.
func (s *Session) ownAttachments() int {
	n := 0
	for _, p := range s.pipes {
		if p.mp != nil && p.mp.joinerLocal && !p.mp.retired {
			n++
		}
	}
	return n
}

// AddPipe starts a local join (MULTIPATH.md 5.7). It returns the pipe ID.
func (s *Session) AddPipe(o JoinOptions) (uint64, AddPipeResult, error) {
	if s.mp == nil {
		return 0, 0, ErrNotNegotiated
	}
	if s.isClosing() {
		return 0, 0, s.err
	}
	m := s.mp
	if s.state == StateSuspended && s.ownAttachments() >= m.ownCap {
		s.makeRoom(true, o.Dir != 0, o.Dir != 1)
	}
	id := m.nextID
	m.nextID += 2
	p := &Pipe{id: id}
	a := &attach{out: outNone, in: inNone, joinerLocal: true, dir: o.Dir, fate: o.FateGroup, attachedAt: s.now, liveAt: s.now,
		lastAdvance: s.now, lastRecord: s.now, oob: o.OOB, weight: 1, probOpt: -1}
	if o.Probation != nil {
		a.probOpt = *o.Probation
	}
	if o.Weight > 0 {
		a.weight = o.Weight
	}
	a.rtt = s.initialRTT(id)
	a.suspectFloor, a.deadTimeout = o.SuspectFloor, o.DeadTimeout
	a.sendPrio, a.sendStandby = o.SendPriority, o.SendStandby
	a.recvPrio, a.recvStandby = o.RecvPriority, o.RecvStandby
	p.mp = a
	if o.Dir != 1 { // up or bidi: we write
		p.out = &outDir{lanes: []*outLane{{info: o.Out, confirmed: true}}, sig: o.OutSig}
		p.out.lo.offered = o.Out.Lanes
		a.out = outJoining
		a.firstRecord = true
	}
	if o.Dir != 0 { // down or bidi: we read
		p.in = &inDir{lanes: []*inLane{{info: o.In}}}
		a.in = inJoining
		a.discardTP = true
	}
	j := &wire.Join{Locator: m.peerLocator, PipeID: id, Dir: o.Dir, CarrierBound: o.CarrierBound,
		Standby: o.RecvStandby, FateGroup: o.FateGroup, Priority: o.RecvPriority}
	if o.Dir == 0 {
		j.Priority, j.Standby = 0, false
	}
	mac := wire.AppendJoinMAC(nil, j)
	var binding []byte
	if o.CarrierBound {
		binding = o.Binding
	}
	j.Token = wire.JoinToken(m.keyOwn, mac, binding)
	res := AddPipeOK
	if !s.joinAllowed() {
		res = AddPipePending
		a.held = true
	}
	s.pipes = append(s.pipes, p)
	if p.out != nil {
		s.startReinjectHold(p)
	}
	if o.OOB {
		s.emit(Event{Kind: EvOOBJoin, Pipe: id, Frame: wire.AppendJoin(nil, j)})
	} else {
		a.join = j
	}
	s.emitPipeState(p)
	if p.out != nil {
		s.wakePipe(p)
	}
	return id, res, nil
}

// JoinResult is the outcome of a peer join at the acceptor.
type JoinResult uint8

const (
	// JoinAccepted means the pipe is attached.
	JoinAccepted JoinResult = iota
	// JoinHold means the session is not established yet; offer the join again.
	JoinHold
	// JoinClose asks the caller to close the carrier without writing anything.
	JoinClose
	// JoinRefused asks the caller to close the carrier; a PIPE_ABANDON was
	// queued.
	JoinRefused
)

// Carrier describes the carrier of a peer join to the acceptor.
type Carrier struct {
	HasIn, HasOut bool // the directions the carrier provides to the acceptor
	In, Out       CarrierInfo
	OutSig        *Signal // wakes the outbound writer
	// Refuse: the join would run over its own session (EXTENSIONS.md
	// 10.2); it is refused with ZMUX_PIPE_RESOURCE_LIMIT once verified.
	Refuse bool
}

// AcceptJoin runs steps 4 to 9 of MULTIPATH.md 5.4 for a PIPE_JOIN that
// named this session's locator.
func (s *Session) AcceptJoin(j *wire.Join, macInput, binding []byte, c Carrier) JoinResult {
	hasIn, hasOut, in, out, outSig := c.HasIn, c.HasOut, c.In, c.Out, c.OutSig
	if s.isClosing() {
		return JoinClose
	}
	if !s.peerSeen {
		return JoinHold
	}
	m := s.mp
	if m == nil {
		return JoinClose
	}
	var b []byte
	if j.CarrierBound {
		b = binding
	}
	if !wire.VerifyJoinToken(m.keyPeer, macInput, b, j.Token) {
		m.joinAuthFails++
		reason := "mac"
		if j.CarrierBound {
			reason = "mac_carrier_bound"
		}
		s.emit(Event{Kind: EvJoinAuthFailed, Reason: reason})
		return JoinClose
	}
	id := j.PipeID
	if !s.ownParity(id) {
		if s.pipe(id) != nil || m.idRetired(id) {
			m.joinReplays++
			return JoinClose
		}
	} else {
		s.failLocal(wire.NewError(wire.ProtocolViolation, "join with the wrong parity", "pipe", id))
		return JoinClose
	}
	// Directions the acceptor needs: up = it reads, down = it writes.
	needIn, needOut := j.Dir != 1, j.Dir != 0
	if needIn && !hasIn || needOut && !hasOut {
		s.refuseJoin(id, wire.PipeFailed)
		return JoinRefused
	}
	if c.Refuse {
		s.refuseJoin(id, wire.PipeResourceLimit)
		return JoinRefused
	}
	if s.peerAttachments() >= m.cap && (s.state != StateSuspended || !s.makeRoom(false, needIn, needOut)) {
		s.refuseJoin(id, wire.PipeResourceLimit)
		return JoinRefused
	}
	m.bits[id] = true
	m.raise(id)
	p := &Pipe{id: id}
	a := &attach{out: outNone, in: inNone, dir: j.Dir, fate: j.FateGroup, attachedAt: s.now, liveAt: s.now, lastAdvance: s.now,
		lastRecord: s.now, weight: 1, peerPrio: j.Priority, peerStandby: j.Standby, probOpt: -1}
	a.rtt = s.initialRTT(id)
	p.mp = a
	if needIn {
		p.in = &inDir{lanes: []*inLane{{info: in}}}
		a.in = inOpen
	}
	if needOut {
		p.out = &outDir{lanes: []*outLane{{info: out, confirmed: true}}, sig: outSig}
		p.out.lo.offered = out.Lanes
		a.out = outActive
		a.firstRecord = true
		a.needPing = true
		a.pingSince = s.now
	}
	s.pipes = append(s.pipes, p)
	if p.out != nil {
		s.startProbation(p)
		s.startReinjectHold(p)
	}
	s.emitPipeState(p)
	if p.out != nil {
		s.wake(outSig)
	}
	s.checkTakeover()
	s.checkResumed()
	return JoinAccepted
}

// peerAttachments counts the peer's unretired attachments.
func (s *Session) peerAttachments() int {
	n := 0
	for _, p := range s.pipes {
		if p.mp != nil && !p.mp.joinerLocal && !p.mp.retired {
			n++
		}
	}
	return n
}

// refuseJoin records a refused ID and owes PIPE_ABANDON (5.4 step 7).
func (s *Session) refuseJoin(id, code uint64) {
	m := s.mp
	m.bits[id] = true
	m.raise(id)
	m.pendAbandon[id] = true
	m.abandonCodes(id, code)
	s.wakeAllWriters()
}

// abandonCodes remembers the code of an owed PIPE_ABANDON for an ID
// without attachment state.
func (m *multiPath) abandonCodes(id, code uint64) {
	if m.pendCode == nil {
		m.pendCode = map[uint64]uint64{}
	}
	m.pendCode[id] = code
}

func (s *Session) emitPipeState(p *Pipe) {
	s.pickChanged()
	o, i, _ := s.PipeState(p.id)
	if p.out != nil {
		s.emit(Event{Kind: EvPipeState, Pipe: p.id, Dir: DirOut, State: o})
	}
	if p.in != nil {
		s.emit(Event{Kind: EvPipeState, Pipe: p.id, Dir: DirIn, State: i})
	}
}

// AttachedPipes returns the IDs of the attachments.
func (s *Session) AttachedPipes(dst []uint64) []uint64 {
	for _, p := range s.pipes {
		dst = append(dst, p.id)
	}
	return dst
}

// makeRoom frees a place among the unretired attachments of one joiner
// (local: this endpoint's own) while suspended (RESUME.md 4.3): the oldest
// failed attachment is retired locally; when none has failed and the new
// attachment provides a missing direction (provideIn: the local endpoint
// reads on it, provideOut: it writes), the live attachment with the oldest
// last progress is displaced, failed locally without PIPE_ABANDON.
func (s *Session) roomCandidate(local, provideIn, provideOut bool) *Pipe {
	var oldest *Pipe
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || a.retired || a.joinerLocal != local {
			continue
		}
		if a.liveOut() || a.in == inOpen || a.in == inJoining {
			continue
		}
		if oldest == nil || a.attachedAt < oldest.mp.attachedAt {
			oldest = p
		}
	}
	if oldest == nil {
		out, in := s.liveCounts()
		if (!provideOut || out != 0) && (!provideIn || in != 0) {
			return nil
		}
		for _, p := range s.pipes {
			a := p.mp
			if a == nil || a.retired || a.joinerLocal != local {
				continue
			}
			if oldest == nil || a.progress() < oldest.mp.progress() {
				oldest = p
			}
		}
		if oldest == nil {
			return nil
		}
	}
	return oldest
}

func (s *Session) makeRoom(local, provideIn, provideOut bool) bool {
	oldest := s.roomCandidate(local, provideIn, provideOut)
	if oldest == nil {
		return false
	}
	a := oldest.mp
	if a.liveOut() || a.in == inOpen || a.in == inJoining {
		a.abandonSent = true // displaced without PIPE_ABANDON
		s.failPipe(oldest, wire.PipeFailed, nil)
	}
	s.retireLocally(oldest)
	return true
}

// progress is the attachment's last progress: acknowledgement or record.
func (a *attach) progress() Time { return maxT(a.lastAdvance, a.lastRecord) }

// retireLocally retires a failed attachment without the PIPE_ABANDON
// exchange; a PIPE_ABANDON still owed for it stays owed by ID.
func (s *Session) retireLocally(p *Pipe) {
	a := p.mp
	if a.abandonQueued && !a.abandonSent {
		s.mp.pendAbandon[p.id] = true
		if s.mp.pendCode == nil {
			s.mp.pendCode = map[uint64]uint64{}
		}
		s.mp.pendCode[p.id] = a.abandonCode
	}
	s.retire(p)
}

// JoinReady reports whether maintenance has target room and a local join
// may start now. Failed attachments retain physical headroom until retirement.
func (s *Session) JoinReady() bool {
	if s.isClosing() || s.mp == nil || s.now < s.mp.backoffUntil {
		return false
	}
	if s.ownAttachments() >= s.mp.ownCap && (s.state != StateSuspended || s.roomCandidate(true, true, true) == nil) {
		return false
	}
	n := 0
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || !a.joinerLocal || a.retired || a.out == outFailed || a.out == outNone && a.in == inEnded {
			continue
		}
		n++
	}
	return n < max(1, s.cfg.MaxPipes)
}

// WakePipe wakes the writers of an attachment (a carrier was attached).
func (s *Session) WakePipe(id uint64) {
	if p := s.pipe(id); p != nil && p.out != nil {
		s.wakePipe(p)
	}
}

// SetPipeCause records the error that ends an attachment (reported with
// the session's error when it was the last pipe).
func (s *Session) SetPipeCause(id uint64, err error) {
	if p := s.pipe(id); p != nil && p.mp != nil && p.mp.failCause == nil {
		p.mp.failCause = err
	}
}
