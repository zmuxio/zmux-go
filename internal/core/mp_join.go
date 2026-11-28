package core

import (
	"crypto/rand"
	"errors"
	"slices"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// JoinOptions are the options of a local join.
type JoinOptions struct {
	Member       *Member
	Reservation  *JoinReservation
	SendPriority *uint64
	SendStandby  *bool
	Probation    *Duration // nil: the session default; 0 disables
	OutSig       *WriteSignal
	Binding      []byte // Bound: the Binding of the new carrier
	In, Out      CarrierInfo
	Live         PipeLiveness // the pipe's own liveness settings
	FateGroup    uint64
	RecvPriority uint64
	Weight       uint64
	Group        uint64 // 0: the default Group of the session
	Deadline     Time   // absolute join deadline; 0 uses the core default
	WeightSet    bool
	Mode         join.Mode
	Dir          uint8 // pipe direction: 0 up, 1 down, 2 bidi (joiner's view)
	RecvStandby  bool
	Kind         uint8 // the carrier's kind (SetPipeKind)
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

// joinAllowedFor reports whether a held local join may go now. Held joins
// are judged one by one, not in the order they were held: below the cap
// each goes; at the cap a suspended session displaces an attachment for a
// join that provides a direction it lacks (makeRoom), and the others keep
// waiting.
func (s *Session) joinAllowedFor(p *Pipe) bool {
	a := p.mp
	if !a.held {
		return true
	}
	if s.now < s.mp.backoffUntil {
		return false
	}
	if s.ownGoing()+s.mp.reservedJoins >= s.joinCap(true) &&
		(s.state != StateSuspended || !s.makeRoom(true, p.in != nil, p.out != nil) || s.ownGoing()+s.mp.reservedJoins >= s.joinCap(true)) {
		return false
	}
	a.held = false
	a.attachedAt = s.now
	return true
}

// ownGoing counts this endpoint's own unretired attachments, those its cap
// holds back excluded.
func (s *Session) ownGoing() int {
	n := 0
	for _, q := range s.pipes {
		if q.mp != nil && q.mp.joinerLocal && !q.mp.retired && !q.mp.held {
			n++
		}
	}
	return n
}

// joinAllowed reports whether a local join may start now.
func (s *Session) joinAllowed() bool {
	m := s.mp
	if s.now < m.backoffUntil {
		return false
	}
	return s.ownAttachments()+m.reservedJoins < s.joinCap(true)
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
	s.prepareAdditional()
	group := join.DefaultJoinGroup(m.usableGroups)
	if o.Group != 0 {
		g, ok := join.LookupGroup(o.Group)
		if !ok || !slices.Contains(m.usableGroups, o.Group) {
			return 0, 0, ErrGroupNotUsable
		}
		group = g
	}
	if r := o.Reservation; r != nil {
		if r.owner != m || r.remaining == 0 {
			return 0, 0, ErrInvalid
		}
		r.remaining--
		m.reservedJoins--
		if r.remaining == 0 {
			m.forgetJoinReservation(r)
		}
	}
	id := m.nextID
	m.nextID += 2
	p := &Pipe{id: id, carrier: o.Kind}
	s.stats.PipesAdded++
	deadline := o.Deadline
	if deadline == 0 {
		deadline = s.now.Add(s.cfg.JoinTimeout)
	}
	a := &attach{out: OutNone, in: InNone, joinerLocal: true, dir: o.Dir, fate: o.FateGroup, attachedAt: s.now, joinDeadline: deadline, liveAt: s.now,
		lastAdvance: s.now, lastRecord: s.now, weight: 1, probOpt: -1, hs: true, mode: o.Mode, binding: o.Binding, group: group}
	if o.Probation != nil {
		a.probOpt = *o.Probation
	}
	a.weight = o.Weight
	if o.Weight == 0 && !o.WeightSet {
		a.weight = 1
	}
	a.rtt = s.initialRTT(id)
	a.live = o.Live
	a.ownFail = o.Out.OwnFailure || o.In.OwnFailure
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
	s.enableJoinedPathQuality(p)
	s.pipes = append(s.pipes, p)
	s.addMemberPipe(o.Member, p, directionsOf(p.in != nil, p.out != nil))
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
func (s *Session) JoinPrepare(id uint64, sig *Signal) (msg string, ready bool, err error) {
	p := s.pipe(id)
	if p == nil || p.mp == nil || !p.mp.joining() || s.isClosing() {
		return "", false, ErrInvalid
	}
	if !s.joinAllowedFor(p) {
		if sig != nil && !slices.Contains(s.mp.joinW, sig) { // nil: the caller asks again on its own
			s.mp.joinW = append(s.mp.joinW, sig)
		}
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

// CancelJoinWait drops a held join's signal (its wait ended).
func (s *Session) CancelJoinWait(sig *Signal) {
	if s.mp != nil {
		s.mp.joinW = slices.DeleteFunc(s.mp.joinW, func(w *Signal) bool { return w == sig })
	}
}

// joinHoldChanged wakes the held local joins: what holds them (the
// cap's count, the backoff, a suspension that lets one displace another)
// changed.
func (s *Session) joinHoldChanged() {
	s.NotifyCapacity()
	for _, w := range s.mp.joinW {
		s.wake(w)
	}
	clear(s.mp.joinW)
	s.mp.joinW = s.mp.joinW[:0]
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
func (s *Session) JoinHello(id uint64, f Framing, over int, plan *JoinWrite) ([]byte, uint64, error) {
	plan.recheck()
	p := s.pipe(id)
	if p == nil || p.mp == nil || !p.mp.joining() || s.isClosing() {
		return nil, 0, ErrInvalid
	}
	a, m := p.mp, s.mp
	suite := join.Preferred(m.usable)
	k, ok := m.suiteKeys(suite.ID)
	if !ok {
		return nil, 0, s.joinPendingErr()
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
	j, err := join.NewJoiner(join.JoinerConfig{Suite: suite, Group: a.group, Phase: uint8(ep.n & 1), Mode: a.mode, Framing: jf, Locator: ep.peer, PSK: psk, Binding: a.binding})
	if err != nil {
		return nil, 0, err
	}
	a.joinEpoch, a.joinEpochKnown = ep.n, true
	pl := join.HelloPayload{Pipe: join.Pipe{ID: id, Direction: a.dir, FateGroup: a.fate, Priority: a.recvPrio, Standby: a.recvStandby}, KeyShare: j.KeyShare()}
	if a.dir == 0 {
		pl.Pipe.Priority, pl.Pipe.Standby = 0, false
	}
	plain, err := join.HelloPlainLen(pl)
	if err != nil {
		return nil, 0, err
	}
	var own uint64
	if a.early && p.out != nil && s.ack != nil {
		target := 0
		if plan != nil && s.ShapeLimitsInForce().Profile == nil {
			target = plan.PadTo
		}
		lim := firstBatchLimit(s.cfg.JoinEarlyHello, target, j.HelloWire(plain)+over)
		// The frames item stays within what the Hello's ciphertext holds.
		if v, ok := join.FitValue(j.HelloRoom(plain)); ok {
			lim = min(lim, v)
		} else {
			lim = 0
		}
		if lim > 0 {
			pl.FirstBatch, own = s.firstBatch(lim)
			if plain, err = join.HelloPlainLen(pl); err != nil {
				return nil, 0, err
			}
		}
	}
	w := j.HelloWire(plain) + over
	padding := 0
	if plan != nil {
		room := shaping.HandshakeRoom(w, j.HelloRoom(plain), &plan.UnitPlan)
		if v, ok := join.FitValue(room); ok {
			pl.Padding, pl.PadEmpty = v, v == 0
			padding = join.ItemSize(v)
		}
	}
	plan.prepare(s, padding, j.HelloWire(0)-suite.TagLen())
	hello, err := j.WriteHello(nil, pl)
	if err != nil {
		plan.Complete(0, false)
		return nil, 0, err
	}
	a.reply = &JoinReplyReader{joiner: j, framing: jf}
	return hello, own, nil
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
func (s *Session) firstBatch(limit int) ([]byte, uint64) {
	var out []byte
	var own uint64
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
		want = sp.q.untrackedPrefix(off, min64(want, old+room))
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
			own += newEnd - min(newEnd, max(sp.sent, sp.preLen))
			s.conn.sent += newEnd - sp.sent
			s.mp.retain(newEnd - sp.sent)
			sp.sent = newEnd
			s.syncUnsent(st)
			st.opened = true
			s.openedCtl(st)
			if sp.state == sendReady {
				sp.state = sendSend
			}
			s.sendChanged(st)
		}
		s.lost(&frameDesc{kind: dStream, id: st.id, off: off, n: have})
	}
	return out, own
}

// applyBatch applies frames of a verified Hello or Reply: STREAM without FIN only. A
// frame in error is dropped and counted and never closes the session, because a batch
// can be a replay; it counts no position and acknowledges nothing.
func (s *Session) applyBatch(b []byte) {
	if len(b) == 0 {
		return
	}
	buf := NewBuf(b)
	defer buf.Unref()
	start := 0
	var f wire.Frame
	s.rxPipe = nil
	for len(b) > 0 {
		n, e := wire.DecodeFrame(b, &f)
		if e != nil {
			s.stats.JoinBatchDropped++
			return
		}
		b = b[n:]
		start += n
		if f.Kind != wire.KindStream || f.Fin {
			s.stats.JoinBatchDropped++
			continue
		}
		if _, e := s.onStream(&f, Input{First: buf.Span(start-len(f.Data), start)}); e != nil {
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
	if p == nil || p.mp == nil || !p.mp.joining() || p.mp.reply == nil {
		return JoinStep{Status: JoinFailedStep}
	}
	a := p.mp
	step, frames, err := a.reply.read(b)
	if err != nil {
		s.failPipe(p, wire.PipeFailed, err)
	}
	if step.Status != JoinDeclined && step.Status != JoinProven {
		return step
	}
	s.mp.joinFailures = 0
	if step.Status == JoinDeclined {
		s.joinRefused()
		a.out, a.in = OutFailed, InEnded
		a.abandonSent, a.abandonAcked = true, true
		a.hs = false
		s.retire(p, true) // its EvPipeState reports the retirement
		return step
	}
	a.proven, a.provenFrames = true, slices.Clone(frames)
	return step
}

// JoinReplyReader holds only the original framing and handshake transcript.
// An in-flight read can authenticate a Reply after its pipe is retired.
type JoinReplyReader struct {
	joiner    *join.Joiner
	framing   join.Framing
	tpSkipped bool
}

func (s *Session) JoinReplyReader(id uint64) *JoinReplyReader {
	if p := s.pipe(id); p != nil && p.mp != nil {
		return p.mp.reply
	}
	return nil
}

// Observe authenticates a late Reply without committing session state.
func (a *JoinReplyReader) Observe(b []byte) JoinStep {
	if a == nil {
		return JoinStep{Status: JoinFailedStep}
	}
	step, _, _ := a.read(b)
	return step
}

func (a *JoinReplyReader) read(b []byte) (JoinStep, []byte, error) {
	if len(b) > joinReadBound {
		return JoinStep{Status: JoinFailedStep}, nil, join.ErrTooLarge
	}
	skip := 0
	if len(b) > 0 && b[0] != join.ReplyType {
		if a.tpSkipped {
			return JoinStep{Status: JoinFailedStep}, nil, join.ErrMalformed
		}
		if a.framing == join.Message {
			if len(b) < 8 || [8]byte(b[:8]) != tpTypeBytes {
				return JoinStep{Status: JoinFailedStep}, nil, join.ErrMalformed
			}
			a.tpSkipped = true
			return JoinStep{Status: JoinMore, N: len(b)}, nil, nil
		}
		cls, rl := Classify(b, wire.MaxRecordSize)
		switch {
		case cls == ClassMore, cls == ClassSession && rl == 0:
			return JoinStep{Status: JoinMore, Need: len(b) + 1}, nil, nil
		case cls != ClassSession:
			return JoinStep{Status: JoinFailedStep}, nil, join.ErrMalformed
		}
		a.tpSkipped = true
		skip = rl
		b = b[rl:]
	}
	pl, n, err := a.joiner.ReadReply(b)
	if errors.Is(err, join.ErrIncomplete) {
		return JoinStep{Status: JoinMore, N: skip, Need: n}, nil, nil
	}
	if err != nil {
		return JoinStep{Status: JoinFailedStep}, nil, err
	}
	if pl.Decision == join.DecisionRefuse {
		return JoinStep{Status: JoinDeclined, N: skip + n}, nil, nil
	}
	return JoinStep{Status: JoinProven, N: skip + n}, pl.Frames, nil
}

// JoinFinish makes the Confirm of a verified Reply under plan, on a carrier
// that adds over bytes to a write, and activates the pipe. The plan may be nil.
func (s *Session) JoinFinish(id uint64, over int, plan *JoinWrite) (confirm []byte, res *join.Result, err error) {
	plan.recheck()
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
		w, room := a.reply.joiner.ConfirmWire()
		w += over
		zeros = shaping.HandshakeRoom(w, room, &plan.UnitPlan)
	}
	prefix := join.FramingLen(a.reply.framing)
	if a.mode == join.Trusted {
		prefix, _ = a.reply.joiner.ConfirmWire()
	}
	plan.prepare(s, zeros, prefix)
	if confirm, err = a.reply.joiner.WriteConfirm(nil, zeros); err != nil {
		plan.Complete(0, false)
		return fail(err)
	}
	if res, err = a.reply.joiner.Finish(); err != nil {
		plan.Complete(0, false)
		return fail(err)
	}
	frames := a.provenFrames
	a.proven, a.provenFrames = false, nil
	a.hs, a.reply, a.binding = false, nil, nil
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
	// JoinAuthorizationChanged rejects a Hello authorized by an older policy.
	JoinAuthorizationChanged
)

// Carrier describes the carrier of a peer join to the acceptor.
type Carrier struct {
	Handoff       any          // opaque cleanup token returned if the Join is evicted
	OutSig        *WriteSignal // wakes the outbound writer
	In, Out       CarrierInfo
	HasIn, HasOut bool  // the directions the carrier provides to the acceptor
	Kind          uint8 // the carrier's kind (SetPipeKind)
	// Refuse: the join would run over its own session.
	Refuse bool
}

// PeerJoin is an authenticated Hello: Unconfirmed once Claim has set its bit. It
// holds no session state.
type PeerJoin struct {
	handoff        any
	acc            *join.Acceptor
	owner          *Session
	pl             join.HelloPayload
	Pipe           join.Pipe
	epoch          uint64
	authGeneration uint64
	Mode           join.Mode
	authorizing    bool
	accepted       bool
}

func (pj *PeerJoin) Epoch() uint64 { return pj.epoch }

func (pj *PeerJoin) AuthGeneration() uint64 { return pj.authGeneration }

// FirstBatchInfo describes the authenticated batch's structure and eligible
// STREAM frames. Flow control and stream state are checked at application.
type FirstBatchInfo struct {
	EncodedBytes, StreamFrames, StreamBytes int
	Malformed                               bool
}

func (pj *PeerJoin) FirstBatchInfo() (info FirstBatchInfo) {
	b := pj.pl.FirstBatch
	info.EncodedBytes = len(b)
	var f wire.Frame
	for len(b) > 0 {
		n, err := wire.DecodeFrame(b, &f)
		if err != nil {
			info.Malformed = true
			break
		}
		if f.Kind == wire.KindStream && !f.Fin {
			info.StreamFrames++
			info.StreamBytes += len(f.Data)
		} else {
			info.Malformed = true
		}
		b = b[n:]
	}
	return info
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
	if !s.UsableSuite(h.Suite.ID) {
		return nil, JoinUnknownSuite // before any AEAD
	}
	e := m.ep.byPhase(h.Phase)
	if e != nil {
		e = m.ep.joinEpoch(e.n, s.now)
	}
	psk, err := s.peerJoinKey(e, h.Suite)
	if err != nil {
		return nil, JoinClose
	}
	jf := join.Stream
	if f == FramingRecords {
		jf = join.Message
	}
	acc, pl, err := join.Accept(h, jf, psk, binding, func(g uint64) bool { return slices.Contains(m.usableGroups, g) })
	if err != nil || e == nil {
		s.stats.JoinAuthFailures++
		s.emit(Event{Kind: EvJoinAuthFailed, Reason: joinAuthReason(err)})
		return nil, JoinClose
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
	return &PeerJoin{owner: s, epoch: e.n, authGeneration: s.policy.AuthGeneration, Pipe: pl.Pipe, Mode: h.Mode, acc: acc, pl: pl}, JoinAccepted
}

// peerJoinKey can authenticate the first Hello without preparing attachment
// state. A phase with no epoch takes the same authentication path.
func (s *Session) peerJoinKey(e *epoch, suite *join.Suite) ([]byte, error) {
	if e == nil {
		key := make([]byte, suite.Hash.Size())
		_, _ = rand.Read(key)
		return key, nil
	}
	peer := join.RoleInitiator
	if s.role == RoleInitiator {
		peer = join.RoleResponder
	}
	if keys := e.keys[suite.ID]; keys != nil {
		if peer == join.RoleResponder {
			return keys.r, nil
		}
		return keys.i, nil
	}
	return join.PSK(suite, e.secret, peer, s.mp.ep.crypto)
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
// that adds over bytes to a write and has taken in received bytes. It changes no
// pipe, cap or suspension.
func (s *Session) ClaimHello(pj *PeerJoin, c Carrier, over, received int, plan *JoinWrite) (reply []byte, res JoinResult, evicted []any) {
	plan.recheck()
	if pj.owner != s {
		return nil, JoinClose, nil
	}
	s.EndPeerAuthorization(pj)
	if pj.authGeneration != s.policy.AuthGeneration {
		return nil, JoinAuthorizationChanged, nil
	}
	m := s.mp
	if s.isClosing() || m == nil || m.ep.joinEpoch(pj.epoch, s.now) == nil {
		return nil, JoinClose, nil
	}
	if s.pipe(pj.Pipe.ID) != nil || m.idRetired(pj.Pipe.ID) {
		s.stats.JoinReplays++
		return nil, JoinClose, nil
	}
	id := pj.Pipe.ID
	s.prepareAdditional().bits[id] = true
	m.raise(id)
	// The key exchange after the bit: a refused share leaves the ID used.
	if err := pj.acc.Exchange(nil); err != nil {
		s.stats.JoinAuthFailures++
		s.emit(Event{Kind: EvJoinAuthFailed, Reason: joinAuthReason(err)})
		return nil, JoinClose, nil
	}
	s.epochPeerJoined(pj.epoch)
	s.applyBatch(pj.pl.FirstBatch)
	needIn, needOut := pj.Pipe.Direction != 1, pj.Pipe.Direction != 0
	dec := join.DecisionAccept
	if needIn && !c.HasIn || needOut && !c.HasOut || c.Refuse || !s.capRoom(needIn, needOut) {
		dec = join.DecisionRefuse
	}
	rp := join.ReplyPayload{Decision: dec, KeyShare: pj.acc.KeyShare()}
	plain, err := join.ReplyPlainLen(rp)
	if err != nil {
		return nil, JoinClose, nil
	}
	w := pj.acc.ReplyWire(plain) + over
	room := pj.acc.ReplyRoom(plain)
	if _, padRoom, bounded := replyRoom(pj.Mode, received, w, 0); bounded {
		room = min(room, padRoom)
	}
	padding := 0
	if plan != nil {
		room = shaping.HandshakeRoom(w, room, &plan.UnitPlan)
		if v, ok := join.FitValue(room); ok {
			rp.Padding, rp.PadEmpty = v, v == 0
			padding = join.ItemSize(v)
		}
	}
	plan.prepare(s, padding, pj.acc.ReplyPayloadOffset())
	reply, err = pj.acc.WriteReply(nil, rp)
	if err != nil {
		plan.Complete(0, false)
		return nil, JoinClose, nil
	}
	if dec == join.DecisionRefuse {
		return reply, JoinRefused, nil
	}
	return reply, JoinAccepted, s.holdPeerJoin(pj, c.Handoff)
}

func (s *Session) joinCap(local bool) int {
	maximum := s.mp.cap
	if local {
		maximum = s.mp.ownCap
	}
	return min(maximum, s.policy.Limits.Envelope())
}

// capRoom predicts the attachment cap check of a confirmed join without changing anything.
func (s *Session) capRoom(needIn, needOut bool) bool {
	if s.peerAttachments() < s.joinCap(false) {
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
	if !s.DropPeerJoin(pj) {
		return JoinClose
	}
	if pj.authGeneration != s.policy.AuthGeneration {
		return JoinAuthorizationChanged
	}
	if s.isClosing() || s.mp == nil || s.mp.ep.joinEpoch(pj.epoch, s.now) == nil {
		return JoinClose
	}
	id, j := pj.Pipe.ID, pj.Pipe
	needIn, needOut := j.Direction != 1, j.Direction != 0
	if c.Refuse || needIn && !c.HasIn || needOut && !c.HasOut ||
		s.peerAttachments() >= s.joinCap(false) && (s.state != StateSuspended || !s.makeRoom(false, needIn, needOut)) {
		s.prepareAdditional().pendAbandon[id] = pendingAbandon{code: wire.PipeResourceLimit}
		s.wakeAllWriters()
		return JoinRefused
	}
	p := &Pipe{id: id, carrier: c.Kind}
	s.stats.PipesAdded++
	s.epochJoined()
	a := &attach{out: OutNone, in: InNone, dir: j.Direction, fate: j.FateGroup, attachedAt: s.now, liveAt: s.now, lastAdvance: s.now,
		lastRecord: s.now, weight: 1, peerPrio: j.Priority, peerStandby: j.Standby, probOpt: -1, mode: pj.Mode, joinEpoch: pj.epoch, joinEpochKnown: true}
	a.group, _ = join.LookupGroup(pj.pl.KeyShare.Group)
	a.rtt = s.initialRTT(id)
	a.ownFail = c.HasOut && c.Out.OwnFailure || c.HasIn && c.In.OwnFailure
	if c.HasOut {
		a.live = c.Out.Liveness
	} else {
		a.live = c.In.Liveness
	}
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
	s.enableJoinedPathQuality(p)
	s.pipes = append(s.pipes, p)
	s.addMemberPipe(nil, p, directionsOf(needIn, needOut))
	if p.member != nil {
		p.member.peer, p.member.generation = true, pj.authGeneration
	}
	if p.out != nil {
		s.startProbation(p)
		s.startReinjectHold(p)
	}
	s.emitPipeState(p)
	if p.out != nil {
		s.requestPoll(p.id, 0, c.OutSig)
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

func (s *Session) emitPipeState(p *Pipe) {
	s.NotifyCapacity()
	if p.out != nil && p.mp.out != OutActive {
		s.releaseTierWaiters(p)
		for _, lane := range p.out.lanes {
			s.clearTierWait(lane)
		}
	}
	s.syncPipeHealth(p)
	s.mp.stateGen++
	s.refreshPathTopology()
	s.pickChanged()
	s.noteDatagramPath() // the primary pipe may have changed
	o, i, _ := s.PipeState(p.id)
	var cause error
	switch {
	case p.mp.resLimited && p.mp.peer != peerObserved:
		cause = ErrJoinRefused
	case errors.Is(p.mp.failCause, ErrJoinTimeout):
		cause = ErrJoinTimeout
	}
	if p.out != nil {
		s.emit(Event{Kind: EvPipeState, Pipe: p.id, Dir: DirOut, OutState: o, Carrier: p.carrier, Cause: cause})
	}
	if p.in != nil {
		s.emit(Event{Kind: EvPipeState, Pipe: p.id, Dir: DirIn, InState: i, Carrier: p.carrier, Cause: cause})
	}
}

// AttachedPipes returns the IDs of the attachments.
func (s *Session) AttachedPipes(dst []uint64) []uint64 {
	for _, p := range s.pipes {
		dst = append(dst, p.id)
	}
	return dst
}

// roomCandidate finds a protocol slot for a recovery Join that supplies
// a missing direction. Failed attachments go first. A live attachment can
// be reclaimed only if other confirmed paths cover every direction it serves.
func (s *Session) roomCandidate(local, provideIn, provideOut bool) *Pipe {
	var sends, receives int
	for _, p := range s.pipes {
		d, _ := s.directionCoverage(p)
		if d&DirectionSend != 0 {
			sends++
		}
		if d&DirectionReceive != 0 {
			receives++
		}
	}
	if (receives != 0 || !provideIn) && (sends != 0 || !provideOut) {
		return nil
	}
	var acked, pending *Pipe
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || a.retired || a.held || a.joinerLocal != local || a.alive() {
			continue
		}
		if a.abandonAcked {
			if acked == nil || a.attachedAt < acked.mp.attachedAt {
				acked = p
			}
		} else if pending == nil || a.attachedAt < pending.mp.attachedAt {
			pending = p
		}
	}
	switch {
	case acked != nil:
		return acked
	case pending != nil:
		return pending
	}
	var oldest *Pipe
	for _, p := range s.pipes {
		a := p.mp
		if a == nil || a.retired || a.held || a.joinerLocal != local {
			continue
		}
		d, _ := s.directionCoverage(p)
		if d&DirectionSend != 0 && sends == 1 || d&DirectionReceive != 0 && receives == 1 {
			continue
		}
		if oldest == nil || a.progress() < oldest.mp.progress() {
			oldest = p
		}
	}
	return oldest
}

// alive reports an attachment with a direction that has not failed or ended.
func (a *attach) alive() bool { return a.liveOut() || a.in == InOpen || a.in == InJoining }

// makeRoom displaces the roomCandidate, if any: a failed attachment is
// retired locally, dropping its state and any PIPE_ABANDON still pending
// for it (the peer retires it by unilateral retirement); a live one is
// failed locally without PIPE_ABANDON and retired.
func (s *Session) makeRoom(local, provideIn, provideOut bool) bool {
	oldest := s.roomCandidate(local, provideIn, provideOut)
	if oldest == nil {
		return false
	}
	a := oldest.mp
	if a.alive() {
		a.abandonSent = true // displaced without PIPE_ABANDON
		s.failPipe(oldest, wire.PipeFailed, ErrPipeAborted)
	}
	s.retire(oldest, false)
	return true
}

// progress is the attachment's last progress: acknowledgement or record.
func (a *attach) progress() Time { return maxT(a.lastAdvance, a.lastRecord) }

// JoinReservation owns pending local attachment capacity for one batch.
type JoinReservation struct {
	owner     *multiPath
	remaining int
}

// JoinSlots is the current maintenance room, after commitments and
// outstanding reservations. A suspended session may also use reclamation
// through JoinReady; migrations only reserve free capacity.
func (s *Session) JoinSlots() int {
	m := s.mp
	if s.isClosing() || m == nil || m.keysPending || s.now < m.backoffUntil {
		return 0
	}
	return max(0, min(s.joinCap(true)-s.ownAttachments(), s.maintenanceJoinRoom())-m.reservedJoins)
}

func (s *Session) maintenanceJoinRoom() int {
	n := 0
	for _, p := range s.pipes {
		a := p.mp
		if a != nil && a.joinerLocal && !a.retired && a.out != OutFailed && (a.out != OutNone || a.in != InEnded) {
			n++
		}
	}
	return s.policy.Limits.SteadyAttachments - n
}

func (s *Session) ReserveJoins(n int) *JoinReservation {
	if n <= 0 || n > s.JoinSlots() {
		return nil
	}
	return s.reserveJoins(n)
}

func (s *Session) reserveJoins(n int) *JoinReservation {
	m := s.mp
	r := &JoinReservation{owner: m, remaining: n}
	m.reservedJoins += n
	m.joinReservations = append(m.joinReservations, r)
	s.NotifyCapacity()
	return r
}

func (s *Session) ReleaseJoins(r *JoinReservation) {
	if r == nil || r.owner != s.mp || r.remaining == 0 {
		return
	}
	s.mp.reservedJoins -= r.remaining
	s.NotifyCapacity()
	r.remaining = 0
	s.mp.forgetJoinReservation(r)
	s.joinHoldChanged()
}

func (m *multiPath) forgetJoinReservation(r *JoinReservation) {
	i := slices.Index(m.joinReservations, r)
	m.joinReservations = slices.Delete(m.joinReservations, i, i+1)
}

func (s *Session) trimJoinReservations() {
	m := s.mp
	room := max(0, min(s.joinCap(true)-s.ownAttachments(), s.maintenanceJoinRoom()))
	for m.reservedJoins > room {
		s.ReleaseJoins(m.joinReservations[len(m.joinReservations)-1])
	}
}

// JoinReady reports whether a pool or recovery can begin a local join.
func (s *Session) JoinReady() bool {
	return s.JoinSlots() > 0 || s.state == StateSuspended && s.mp != nil && !s.mp.keysPending &&
		s.now >= s.mp.backoffUntil && s.maintenanceJoinRoom() > s.mp.reservedJoins && s.roomCandidate(true, true, true) != nil
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
	if s.mp.keysPending || s.mp.ep.join == nil {
		return false
	}
	if s.mp.additionalPipes != nil {
		_, ok := s.mp.suiteKeys(id)
		return ok
	}
	_, known := join.LookupSuite(id)
	return known && slices.Contains(s.cfg.Local.SessionCrypto.Suites, id) && slices.Contains(s.peer.SessionCrypto.Suites, id)
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
