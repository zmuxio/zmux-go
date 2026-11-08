package core

import (
	"errors"
	"slices"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// JoinOptions are the options of a local join.
type JoinOptions struct {
	FateGroup    uint64
	Binding      []byte // Bound: the Binding of the new carrier
	SendPriority *uint64
	SendStandby  *bool
	RecvPriority uint64
	Weight       uint64
	Probation    *Duration    // nil: the session default; 0 disables
	Live         PipeLiveness // the pipe's own liveness settings
	In, Out      CarrierInfo
	OutSig       *Signal
	Mode         join.Mode
	Dir          uint8 // pipe direction: 0 up, 1 down, 2 bidi (joiner's view)
	RecvStandby  bool
	Kind         uint8  // the carrier's kind (SetPipeKind)
	Group        uint64 // 0: the default Group of the session
}

// AddPipeResult says what happened to a local join.
type AddPipeResult uint8

const (
	AddPipeOK AddPipeResult = iota
	// AddPipePending means the cap or the refusal backoff holds the join; the
	// carrier waits and joins once allowed.
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

// AddPipe takes a pipe ID and enters JOINING: nothing reaches
// the carrier but the handshake, which JoinPrepare, JoinHello, VerifyReply and JoinFinish run.
func (s *Session) AddPipe(o JoinOptions) (uint64, AddPipeResult, error) {
	if s.mp == nil {
		return 0, 0, ErrNotNegotiated
	}
	if s.isClosing() {
		return 0, 0, s.err
	}
	m := s.mp
	if m.keysPending {
		return 0, 0, s.joinPendingErr()
	}
	if m.ep.join == nil || len(m.ep.join.peer) == 0 {
		return 0, 0, ErrNotNegotiated
	}
	if s.state == StateSuspended && s.ownAttachments() >= m.ownCap {
		s.makeRoom(true, o.Dir != 0, o.Dir != 1)
	}
	group := join.DefaultJoinGroup(m.usableGroups)
	if o.Group != 0 {
		g, ok := join.LookupGroup(o.Group)
		if !ok || !slices.Contains(m.usableGroups, o.Group) {
			return 0, 0, ErrGroupNotUsable
		}
		group = g
	}
	id := m.nextID
	m.nextID += 2
	p := &Pipe{id: id, carrier: o.Kind}
	s.stats.PipesAdded++
	a := &attach{out: OutNone, in: InNone, joinerLocal: true, dir: o.Dir, fate: o.FateGroup, attachedAt: s.now, liveAt: s.now,
		lastAdvance: s.now, lastRecord: s.now, weight: 1, probOpt: -1, hs: true, mode: o.Mode, binding: o.Binding, group: group}
	if o.Probation != nil {
		a.probOpt = *o.Probation
	}
	if o.Weight > 0 {
		a.weight = o.Weight
	}
	a.rtt = s.initialRTT(id)
	a.live = o.Live
	s.drawProbe(a, nil, false)
	a.sendPrio, a.sendStandby = o.SendPriority, o.SendStandby
	a.recvPrio, a.recvStandby = o.RecvPriority, o.RecvStandby
	p.mp = a
	if o.Dir != 1 { // up or bidi: we write
		p.out = &outDir{lanes: []*outLane{{info: o.Out, confirmed: true}}, sig: o.OutSig}
		p.out.lo.offered = o.Out.Lanes && o.Mode != join.Sealed
		a.out = OutJoining
		a.firstRecord = true
	}
	if o.Dir != 0 { // down or bidi: we read
		p.in = &inDir{lanes: []*inLane{{info: o.In}}}
		a.in = InJoining
	}
	res := AddPipeOK
	if !s.joinAllowed() {
		res = AddPipePending
		a.held = true
	}
	s.pipes = append(s.pipes, p)
	s.emitPipeState(p)
	return id, res, nil
}

// suiteKeys returns the join keys of a Suite in both sets, of the epoch new local
// joins use.
func (m *multiPath) suiteKeys(id uint64) (*joinKeys, bool) {
	if m.ep.join == nil {
		return nil, false
	}
	k, ok := m.ep.join.keys[id]
	return k, ok
}

// Handshake message names of a Join.
const (
	JoinMsgHello      = "hello"
	JoinMsgHelloEarly = "hello_early"
	JoinMsgReply      = "reply"
	JoinMsgConfirm    = "confirm"
)

// JoinPrepare holds the place of a local join and checks its keys, and names
// its Hello: the one of a session without a usable pipe, whose first batch
// recovers it, or the plain one. ready is false while the cap or the refusal
// backoff holds it; nothing is drawn or written yet.
func (s *Session) JoinPrepare(id uint64) (msg string, ready bool, err error) {
	p := s.pipe(id)
	if p == nil || p.mp == nil || !p.mp.joining() || s.isClosing() {
		return "", false, ErrInvalid
	}
	if !s.joinAllowedFor(p) {
		return "", false, nil
	}
	if _, ok := s.mp.suiteKeys(join.Preferred(s.mp.usable).ID); !ok {
		return "", false, s.joinPendingErr()
	}
	p.mp.early = s.cfg.JoinEarlyHello > 0 && s.noUsablePipe(p)
	if p.mp.early {
		return JoinMsgHelloEarly, true, nil
	}
	return JoinMsgHello, true, nil
}

// firstBatchLimit is the limit of a Hello's first batch: limit, and with a
// length to pad to, what the frames item leaves of it above base.
func firstBatchLimit(limit, padTo, base int) int {
	if padTo > 0 {
		v, ok := join.FitValue(padTo - base)
		if !ok {
			return 0
		}
		limit = min(limit, v)
	}
	return limit
}

// JoinHello writes the Hello JoinPrepare held the place of, under plan, on a
// carrier that adds over bytes to a write.
func (s *Session) JoinHello(id uint64, f Framing, over int, plan *shaping.UnitPlan) ([]byte, error) {
	p := s.pipe(id)
	if p == nil || p.mp == nil || !p.mp.joining() || s.isClosing() {
		return nil, ErrInvalid
	}
	a, m := p.mp, s.mp
	suite := join.Preferred(m.usable)
	k, ok := m.suiteKeys(suite.ID)
	if !ok {
		return nil, s.joinPendingErr()
	}
	psk := k.r
	if s.role == RoleInitiator {
		psk = k.i
	}
	jf := join.Stream
	if f == FramingRecords {
		jf = join.Message
	}
	ep := m.ep.join
	a.joinEpoch = ep.n
	j, err := join.NewJoiner(join.JoinerConfig{Suite: suite, Group: a.group, Phase: uint8(ep.n & 1), Mode: a.mode, Framing: jf, Locator: ep.peer, PSK: psk, Binding: a.binding})
	if err != nil {
		return nil, err
	}
	pl := join.HelloPayload{Pipe: join.Pipe{ID: id, Direction: a.dir, FateGroup: a.fate, Priority: a.recvPrio, Standby: a.recvStandby}, KeyShare: j.KeyShare()}
	if a.dir == 0 {
		pl.Pipe.Priority, pl.Pipe.Standby = 0, false
	}
	plain, err := join.HelloPlainLen(pl)
	if err != nil {
		return nil, err
	}
	if a.early && p.out != nil && s.ack != nil {
		lim := firstBatchLimit(s.cfg.JoinEarlyHello, plan.PadTo, j.HelloWire(plain)+over)
		// The frames item stays within what the Hello's ciphertext holds.
		if v, ok := join.FitValue(join.CiphertextRoom(suite, plain)); ok {
			lim = min(lim, v)
		} else {
			lim = 0
		}
		if lim > 0 {
			pl.FirstBatch = s.firstBatch(lim)
			if plain, err = join.HelloPlainLen(pl); err != nil {
				return nil, err
			}
		}
	}
	w := j.HelloWire(plain) + over
	if v, ok := join.ItemFor(shaping.PadFor(w, plan)-w, join.CiphertextRoom(suite, plain)); ok {
		pl.Padding, pl.PadEmpty = v, v == 0
		s.shapeSpend(join.ItemSize(v))
	}
	hello, err := j.WriteHello(nil, pl)
	if err != nil {
		return nil, err
	}
	a.joiner, a.framing = j, jf
	return hello, nil
}

// JoinUnplanned counts a Join message a handshake shaper had no plan for.
func (s *Session) JoinUnplanned() { s.shapeSess().stats.HandshakeUnplanned++ }

// JoinHookFault counts a handshake shaper that panicked on a Join message.
func (s *Session) JoinHookFault() { s.shapeSess().stats.Faults++ }

// noUsablePipe reports that no outbound pipe but p is ACTIVE.
func (s *Session) noUsablePipe(p *Pipe) bool {
	for _, q := range s.pipes {
		if q != p && q.mp != nil && q.out != nil && !q.mp.hs && q.mp.out == OutActive && !q.mp.retired {
			return false
		}
	}
	return true
}

// firstBatch encodes at most limit bytes of unsent or unacknowledged stream data as
// STREAM frames without FIN. New bytes count as sent
// here, against session credit and the retransmit budget, as the acceptor counts them;
// since the batch counts in no position, all it carries goes again as reinjected
// information and the receiver deduplicates. A new stream's ID is allocated here and
// not cancelled afterwards.
func (s *Session) firstBatch(limit int) []byte {
	var out []byte
	for i := range s.sched.head {
		for st := s.sched.head[i]; st != nil; {
			n := st.sch.next
			if !st.idSet && !st.cancelled && st.hasS && st.snd.q.end > 0 && s.canSendNow(st) {
				_, _ = s.PinID(st)
			}
			if n == s.sched.head[i] {
				break
			}
			st = n
		}
	}
	for _, st := range s.streams {
		if !st.hasS || st.snd.reset {
			continue
		}
		sp := &st.snd
		off := sp.q.base
		end := min64(sp.q.end, sp.maxData)
		// The limit is of the encoded frames: their headers count.
		var head [32]byte
		room2 := limit - len(out) - len(wire.AppendStreamHeader(head[:0], st.id, off, limit, false, true))
		if off >= end || room2 <= 0 {
			continue
		}
		want := min64(end-off, uint64(room2))
		old := uint64(0)
		if off < sp.sent {
			old = min64(sp.sent-off, want)
		}
		room := s.capRetained(s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent))
		want = min64(want, old+room)
		// One frame over the queue's chunks, as far as they are contiguous.
		var have uint64
		for have < want {
			c, _ := sp.q.peek(off+have, int(want-have))
			if len(c) == 0 {
				break
			}
			have += uint64(len(c))
		}
		// The frame at offset 0 carries the whole preamble.
		if have == 0 || off == 0 && have < sp.preLen {
			continue
		}
		out = wire.AppendStreamHeader(out, st.id, off, int(have), false, true)
		for n := uint64(0); n < have; {
			c, _ := sp.q.peek(off+n, int(have-n))
			out = append(out, c...)
			n += uint64(len(c))
		}
		if newEnd := off + have; newEnd > sp.sent {
			s.conn.sent += newEnd - sp.sent
			s.mp.retain(newEnd - sp.sent)
			sp.sent = newEnd
			st.opened = true
			s.openedCtl(st)
			if sp.state == sendReady {
				sp.state = sendSend
			}
			if st.ext != nil && st.ext.capSig != nil {
				s.checkCapacity(st)
			}
			s.wake(st.wsig)
		}
		s.lost(&frameDesc{kind: dStream, id: st.id, off: off, n: have})
	}
	return out
}

// applyBatch applies frames of a verified Hello or Reply: STREAM without FIN only. A
// frame in error is dropped and counted and never closes the session, because a batch
// can be a replay; it counts no position and acknowledges nothing.
func (s *Session) applyBatch(b []byte) {
	var f wire.Frame
	s.rxPipe = nil
	for len(b) > 0 {
		n, e := wire.DecodeFrame(b, &f)
		if e != nil {
			s.stats.JoinBatchDropped++
			return
		}
		b = b[n:]
		if f.Kind != wire.KindStream || f.Fin {
			s.stats.JoinBatchDropped++
			continue
		}
		if _, e := s.onStream(&f, nil); e != nil {
			s.stats.JoinBatchDropped++
		}
	}
}

// joining reports a handshake that still runs; a failed one keeps hs, since nothing
// but it was ever allowed on its carrier.
func (a *attach) joining() bool { return a.hs && a.out != OutFailed && a.in != InEnded }

// JoinStatus is what VerifyReply concluded.
type JoinStatus uint8

const (
	// JoinMore means that it wants more bytes; JoinStep.Need is the total it can progress at.
	JoinMore JoinStatus = iota
	// JoinProven means that the Reply verified with decision 0; write Confirm.
	JoinProven
	// JoinDeclined means that the Reply verified with decision 1; the attachment is retired.
	JoinDeclined
	// JoinFailedStep means that the attachment failed.
	JoinFailedStep
)

// JoinStep is one result of VerifyReply.
type JoinStep struct {
	Status  JoinStatus
	N, Need int
}

// joinReadBound bounds what a joiner reads before a verified Reply: one
// transport parameters record and a Reply of the largest unit, never
// max_record_size.
const joinReadBound = 2*wire.MaxRecordSize + 64

// VerifyReply feeds the bytes read from a JOINING carrier; nothing of them reaches
// the session before they verify, and the attachment stays joining until
// JoinFinish. It consumes at most one leading transport parameters record.
// Anything else fails only the attachment.
func (s *Session) VerifyReply(id uint64, b []byte, now Time) JoinStep {
	s.SetNow(now)
	p := s.pipe(id)
	if p == nil || p.mp == nil || !p.mp.joining() || p.mp.joiner == nil {
		return JoinStep{Status: JoinFailedStep}
	}
	a := p.mp
	fail := func(err error) JoinStep {
		s.failPipe(p, wire.PipeFailed, err)
		return JoinStep{Status: JoinFailedStep}
	}
	if len(b) > joinReadBound {
		return fail(join.ErrTooLarge)
	}
	skip := 0
	if len(b) > 0 && b[0] != join.ReplyType {
		if a.tpSkipped {
			return fail(join.ErrMalformed)
		}
		if a.framing == join.Message {
			if len(b) < 8 || [8]byte(b[:8]) != tpTypeBytes {
				return fail(join.ErrMalformed)
			}
			a.tpSkipped = true
			return JoinStep{Status: JoinMore, N: len(b)}
		}
		cls, rl := Classify(b, wire.MaxRecordSize)
		switch {
		case cls == ClassMore, cls == ClassSession && rl == 0:
			return JoinStep{Status: JoinMore, Need: len(b) + 1}
		case cls != ClassSession:
			return fail(join.ErrMalformed)
		}
		a.tpSkipped = true
		skip = rl
		b = b[rl:]
	}
	pl, n, err := a.joiner.ReadReply(b)
	if errors.Is(err, join.ErrIncomplete) {
		return JoinStep{Status: JoinMore, N: skip, Need: n}
	}
	if err != nil {
		return fail(err)
	}
	s.mp.joinFailures = 0
	if pl.Decision == join.DecisionRefuse {
		s.joinRefused()
		a.out, a.in = OutFailed, InEnded
		a.abandonSent, a.abandonAcked = true, true
		s.retire(p)
		a.hs = false
		s.emit(Event{Kind: EvPipeState, Pipe: id, Dir: DirOut, OutState: OutRetired, Carrier: p.carrier})
		return JoinStep{Status: JoinDeclined, N: skip + n}
	}
	a.proven, a.provenFrames = true, slices.Clone(pl.Frames)
	return JoinStep{Status: JoinProven, N: skip + n}
}

// JoinFinish makes the Confirm of a verified Reply under plan, on a carrier
// that adds over bytes to a write, and activates the pipe. The plan may be nil.
func (s *Session) JoinFinish(id uint64, over int, plan *shaping.UnitPlan) (confirm []byte, res *join.Result, err error) {
	p := s.pipe(id)
	if p == nil || p.mp == nil || !p.mp.joining() || !p.mp.proven || s.isClosing() {
		return nil, nil, ErrInvalid
	}
	a := p.mp
	fail := func(err error) ([]byte, *join.Result, error) {
		s.failPipe(p, wire.PipeFailed, err)
		return nil, nil, err
	}
	zeros := 0
	if plan != nil && a.mode != join.Bound {
		w, room := a.joiner.ConfirmWire()
		w += over
		zeros = join.RawFill(shaping.PadFor(w, plan)-w, room)
		s.shapeSpend(zeros)
	}
	if confirm, err = a.joiner.WriteConfirm(nil, zeros); err != nil {
		return fail(err)
	}
	if res, err = a.joiner.Finish(); err != nil {
		return fail(err)
	}
	frames := a.provenFrames
	a.proven, a.provenFrames = false, nil
	a.hs, a.joiner, a.binding = false, nil, nil
	a.lastAdvance, a.lastRecord, a.liveAt = s.now, s.now, s.now
	if p.in != nil {
		a.in = InOpen
	}
	if p.out != nil {
		a.out = OutActive
		a.needPing, a.pingSince = true, s.now
		s.startReinjectHold(p)
	}
	s.applyBatch(frames)
	s.epochJoined()
	s.epochPipeJoined(a.mode)
	s.joinConfirmed(p)
	if p.out != nil {
		s.wakePipe(p)
	}
	return confirm, res, nil
}

// JoinResult is the outcome of a peer join at the acceptor.
type JoinResult uint8

const (
	// JoinAccepted means go on to the next step.
	JoinAccepted JoinResult = iota
	// JoinHold means the session is not established yet; offer the join again.
	JoinHold
	// JoinKeyWait means there are no join keys yet: hold
	// the carrier, without session state, until WaitJoinKeys or the handshake
	// timeout, then offer it again.
	JoinKeyWait
	// JoinClose asks the caller to close the carrier without writing anything.
	JoinClose
	// JoinRefused asks the caller to close the carrier after what it wrote.
	JoinRefused
	// JoinUnknownSuite is JoinClose for a Suite outside the session's set
	// (unknown_suite), refused before any AEAD.
	JoinUnknownSuite
)

// Carrier describes the carrier of a peer join to the acceptor.
type Carrier struct {
	In, Out       CarrierInfo
	OutSig        *Signal // wakes the outbound writer
	HasIn, HasOut bool    // the directions the carrier provides to the acceptor
	Kind          uint8   // the carrier's kind (SetPipeKind)
	// Refuse: the join would run over its own session.
	Refuse bool
}

// PeerJoin is an authenticated Hello: Unconfirmed once Claim has set its bit. It
// holds no session state.
type PeerJoin struct {
	Pipe join.Pipe
	Mode join.Mode
	acc  *join.Acceptor
	pl   join.HelloPayload
	h    *join.Suite
}

// HelloWithoutBinding records a Hello in Bound refused because its carrier has no Binding.
func (s *Session) HelloWithoutBinding() {
	s.stats.JoinAuthFailures++
	s.emit(Event{Kind: EvJoinAuthFailed, Reason: JoinAuthAEAD})
}

// OpenHello opens and authenticates a Hello that named this
// session's locator.
func (s *Session) OpenHello(h join.Hello, f Framing, binding []byte) (*PeerJoin, JoinResult) {
	if s.isClosing() {
		return nil, JoinClose
	}
	if !s.peerSeen {
		return nil, JoinHold
	}
	m := s.mp
	if m == nil {
		return nil, JoinClose
	}
	if m.keysPending {
		s.stats.JoinPending++
		return nil, JoinKeyWait
	}
	if _, ok := m.suiteKeys(h.Suite.ID); !ok {
		return nil, JoinUnknownSuite // before any AEAD
	}
	var psk []byte
	e := m.ep.byPhase(h.Phase)
	if e != nil {
		// The key of the joiner's role, which is the peer's, never inferred from the ID.
		psk = e.keys[h.Suite.ID].i
		if s.role == RoleInitiator {
			psk = e.keys[h.Suite.ID].r
		}
	} else {
		// No accepted epoch of this phase: a key no joiner has, so the Hello
		// fails as auth like any wrong key.
		psk = m.unknownPhaseKey()
	}
	jf := join.Stream
	if f == FramingRecords {
		jf = join.Message
	}
	acc, pl, err := join.Accept(h, jf, psk, binding, func(g uint64) bool { return slices.Contains(m.usableGroups, g) })
	if err != nil {
		s.stats.JoinAuthFailures++
		s.emit(Event{Kind: EvJoinAuthFailed, Reason: joinAuthReason(err)})
		return nil, JoinClose
	}
	if e != nil {
		s.epochPeerJoined(e.n) // a verified join of d+1: A uses d+1 too
	}
	id := pl.Pipe.ID
	if s.ownParity(id) {
		s.failLocal(wire.NewError(wire.ProtocolViolation, "join with the wrong parity", "pipe", id))
		return nil, JoinClose
	}
	if s.pipe(id) != nil || m.idRetired(id) {
		s.stats.JoinReplays++
		return nil, JoinClose
	}
	return &PeerJoin{Pipe: pl.Pipe, Mode: h.Mode, acc: acc, pl: pl, h: h.Suite}, JoinAccepted
}

// replyRoom bounds what a Reply may add to its base wire length before the
// joiner is Confirmed, in Sealed and Bound: 3 times the bytes received, with
// frames before padding. Trusted has no such bound: unbounded is false.
func replyRoom(mode join.Mode, received, base, wantFrames int) (framesLimit, paddingRoom int, bounded bool) {
	if mode == join.Trusted {
		return wantFrames, 0, false
	}
	room := 3*received - base
	v, ok := join.FitValue(room)
	if !ok {
		v = 0
	}
	framesLimit = min(v, wantFrames)
	used := 0
	if framesLimit > 0 {
		used = join.ItemSize(framesLimit)
	}
	return framesLimit, max(room-used, 0), true
}

// ClaimHello is the rest of step 7: it sets the Pipe ID's bit, applies the first
// batch and writes the Reply with its read-only decision, under plan, on a carrier
// that adds over bytes to a write and has received received bytes. It changes no
// pipe, cap or suspension.
func (s *Session) ClaimHello(pj *PeerJoin, c Carrier, over, received int, plan *shaping.UnitPlan) (reply []byte, res JoinResult) {
	m := s.mp
	if s.isClosing() || m == nil || s.pipe(pj.Pipe.ID) != nil || m.idRetired(pj.Pipe.ID) {
		s.stats.JoinReplays++
		return nil, JoinClose
	}
	id := pj.Pipe.ID
	m.bits[id] = true
	m.raise(id)
	// The key exchange after the bit: a refused share leaves the ID used.
	if err := pj.acc.Exchange(nil); err != nil {
		s.stats.JoinAuthFailures++
		s.emit(Event{Kind: EvJoinAuthFailed, Reason: joinAuthReason(err)})
		return nil, JoinClose
	}
	s.applyBatch(pj.pl.FirstBatch)
	needIn, needOut := pj.Pipe.Direction != 1, pj.Pipe.Direction != 0
	dec := join.DecisionAccept
	if needIn && !c.HasIn || needOut && !c.HasOut || c.Refuse || !s.capRoom(needIn, needOut) {
		dec = join.DecisionRefuse
	}
	rp := join.ReplyPayload{Decision: dec, KeyShare: pj.acc.KeyShare()}
	plain, err := join.ReplyPlainLen(rp)
	if err != nil {
		return nil, JoinClose
	}
	w := pj.acc.ReplyWire(plain) + over
	room := join.CiphertextRoom(pj.h, plain)
	if _, padRoom, bounded := replyRoom(pj.Mode, received, w, 0); bounded {
		room = min(room, padRoom)
	}
	if v, ok := join.ItemFor(shaping.PadFor(w, plan)-w, room); ok {
		rp.Padding, rp.PadEmpty = v, v == 0
		s.shapeSpend(join.ItemSize(v))
	}
	reply, err = pj.acc.WriteReply(nil, rp)
	if err != nil {
		return nil, JoinClose
	}
	if dec == join.DecisionRefuse {
		return reply, JoinRefused
	}
	return reply, JoinAccepted
}

// capRoom predicts the attachment cap check of a confirmed join without changing anything.
func (s *Session) capRoom(needIn, needOut bool) bool {
	if s.peerAttachments() < s.mp.cap {
		return true
	}
	return s.state == StateSuspended && s.roomCandidate(false, needIn, needOut) != nil
}

// ReadConfirm reads the joiner's confirmation of an Unconfirmed carrier; in Bound
// there is none. done is false with need when more bytes are wanted.
func (pj *PeerJoin) ReadConfirm(b []byte) (n, need int, done bool, err error) {
	if pj.Mode == join.Bound {
		return 0, 0, true, nil
	}
	n, err = pj.acc.ReadConfirm(b)
	if errors.Is(err, join.ErrIncomplete) {
		return 0, n, false, nil
	}
	if err != nil {
		return 0, 0, false, err
	}
	return n, 0, true, nil
}

// Result is the finished handshake of a Confirmed carrier.
func (pj *PeerJoin) Result() (*join.Result, error) { return pj.acc.Finish() }

// ConfirmJoin confirms a verified peer join: the only place where a peer join moves a
// pipe, the cap or a suspension.
func (s *Session) ConfirmJoin(pj *PeerJoin, c Carrier) JoinResult {
	if s.isClosing() {
		return JoinClose
	}
	m := s.mp
	id, j := pj.Pipe.ID, pj.Pipe
	needIn, needOut := j.Direction != 1, j.Direction != 0
	if c.Refuse || s.peerAttachments() >= m.cap && (s.state != StateSuspended || !s.makeRoom(false, needIn, needOut)) {
		m.pendAbandon[id] = true
		m.abandonCodes(id, wire.PipeResourceLimit)
		s.wakeAllWriters()
		return JoinRefused
	}
	p := &Pipe{id: id, carrier: c.Kind}
	s.stats.PipesAdded++
	s.epochJoined()
	a := &attach{out: OutNone, in: InNone, dir: j.Direction, fate: j.FateGroup, attachedAt: s.now, liveAt: s.now, lastAdvance: s.now,
		lastRecord: s.now, weight: 1, peerPrio: j.Priority, peerStandby: j.Standby, probOpt: -1, mode: pj.Mode}
	a.rtt = s.initialRTT(id)
	s.drawProbe(a, nil, false)
	p.mp = a
	if needIn {
		p.in = &inDir{lanes: []*inLane{{info: c.In}}}
		a.in = InOpen
	}
	if needOut {
		p.out = &outDir{lanes: []*outLane{{info: c.Out, confirmed: true}}, sig: c.OutSig}
		p.out.lo.offered = c.Out.Lanes && pj.Mode != join.Sealed
		a.out = OutActive
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
		s.wake(c.OutSig)
	}
	s.checkTakeover()
	s.checkResumed()
	s.epochPipeJoined(pj.Mode)
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

// abandonCodes remembers the code of an owed PIPE_ABANDON for an ID
// without attachment state.
func (m *multiPath) abandonCodes(id, code uint64) {
	if m.pendCode == nil {
		m.pendCode = map[uint64]uint64{}
	}
	m.pendCode[id] = code
}

func (s *Session) emitPipeState(p *Pipe) {
	s.mp.stateGen++
	s.pickChanged()
	o, i, _ := s.PipeState(p.id)
	if p.out != nil {
		s.emit(Event{Kind: EvPipeState, Pipe: p.id, Dir: DirOut, OutState: o, Carrier: p.carrier})
	}
	if p.in != nil {
		s.emit(Event{Kind: EvPipeState, Pipe: p.id, Dir: DirIn, InState: i, Carrier: p.carrier})
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
// (local: this endpoint's own) while suspended: the oldest
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
		if a.liveOut() || a.in == InOpen || a.in == InJoining {
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
	if a.liveOut() || a.in == InOpen || a.in == InJoining {
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
	if s.isClosing() || s.mp == nil || s.mp.keysPending || s.now < s.mp.backoffUntil {
		return false
	}
	if s.ownAttachments() >= s.mp.ownCap && (s.state != StateSuspended || s.roomCandidate(true, true, true) == nil) {
		return false
	}
	n := 0
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || !a.joinerLocal || a.retired || a.out == OutFailed || a.out == OutNone && a.in == InEnded {
			continue
		}
		n++
	}
	return n < max(1, s.cfg.MaxPipes)
}

// SetPipeCause records the error that ends an attachment (reported with
// the session's error when it was the last pipe).
func (s *Session) SetPipeCause(id uint64, err error) {
	if p := s.pipe(id); p != nil && p.mp != nil && p.mp.failCause == nil {
		p.mp.failCause = err
	}
}

// firstPipeMode is the record mode of pipe 0. An Unmanaged pipe has raw records.
func (s *Session) firstPipeMode() join.Mode {
	if s.cfg.SecureFirstPipe {
		return s.cfg.FirstPipeMode
	}
	return join.Trusted
}

// WeakestMode is the weakest record mode of the session's pipes;
// unmanaged is true when pipe 0 is Unmanaged, the weakest of all.
func (s *Session) WeakestMode() (m join.Mode, unmanaged bool) {
	if !s.cfg.SecureFirstPipe {
		return join.Trusted, true
	}
	m = s.cfg.FirstPipeMode
	for _, p := range s.pipes {
		if p.mp != nil && !p.mp.retired && !p.mp.hs && p.mp.mode > m {
			m = p.mp.mode
		}
	}
	return m, false
}

// PipeMode is the record mode of an attachment.
func (s *Session) PipeMode(id uint64) join.Mode {
	if p := s.pipe(id); p != nil && p.mp != nil {
		return p.mp.mode
	}
	return s.firstPipeMode()
}

// JoinFailures is the count of own consecutive failed joins.
func (s *Session) JoinFailures() uint64 {
	if s.mp == nil {
		return 0
	}
	return s.mp.joinFailures
}

// UsableSuite reports whether a Suite is in both Sets and has its keys.
func (s *Session) UsableSuite(id uint64) bool {
	if s.mp == nil {
		return false
	}
	_, ok := s.mp.suiteKeys(id)
	return ok
}

// JoinMode returns the record mode of a joining attachment.
func (s *Session) JoinMode(id uint64) join.Mode {
	if p := s.pipe(id); p != nil && p.mp != nil {
		return p.mp.mode
	}
	return join.Sealed
}

// FailJoin fails a joining attachment after a local error.
func (s *Session) FailJoin(id uint64, err error) {
	if p := s.pipe(id); p != nil && p.mp != nil && p.mp.joining() {
		s.failPipe(p, wire.PipeFailed, err)
	}
}

// JoinBackoffLeft is what remains of the join refusal backoff at now;
// 0 without one.
func (s *Session) JoinBackoffLeft(now Time) Duration {
	if s.mp == nil || now >= s.mp.backoffUntil {
		return 0
	}
	return s.mp.backoffUntil.Sub(now)
}
