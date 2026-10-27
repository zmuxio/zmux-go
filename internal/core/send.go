package core

import "github.com/zmuxio/zmux-go/internal/wire"

// ---- writing (sending part) ----

// SendCapacity returns the bytes the part could transmit now:
// the smaller remaining credit minus what it already holds unsent.
func (s *Session) SendCapacity(st *Stream) uint64 {
	sp := &st.snd
	if !st.hasS || sp.finWr || sp.reset || sp.stopped || s.err != nil {
		return 0
	}
	unsent := st.unsent()
	credit := sp.maxData - min64(sp.maxData, sp.sent)
	switch {
	case !st.idSet && !s.peerSeen:
		credit = 0
	case !st.idSet:
		// Not opened yet: the peer's initial window, less the preamble
		// the opening frame will carry.
		w := s.peer.MaxStreamDataBidiRem
		if !st.bidi {
			w = s.peer.MaxStreamDataUni
		}
		pre := uint64(0)
		if s.feat.Preamble {
			pre = uint64(st.preSize())
		}
		credit = w - min64(w, pre)
	}
	conn := s.conn.sendMax - min64(s.conn.sendMax, s.conn.sent)
	c := min64(credit, conn)
	if c <= unsent {
		return 0
	}
	return c - unsent
}

// HoldsUnsent reports whether the part holds bytes not yet sent.
func (s *Session) HoldsUnsent(st *Stream) bool { return st.unsent() > 0 }

// unsent is what the part holds and has not sent yet.
func (st *Stream) unsent() uint64 {
	sp := &st.snd
	end := sp.sendEnd()
	from := max64(sp.sent, sp.q.base)
	if end <= from {
		return 0
	}
	return end - from
}

// sendEnd is the end of the data that may still be sent: the written end,
// cut at the Reliable Size once reset.
func (sp *sendPart) sendEnd() uint64 {
	if sp.reset {
		return min64(sp.q.end, sp.resetRel)
	}
	return sp.q.end
}

// writeErr returns the error a write gets now, nil when it may proceed.
func (s *Session) writeErr(st *Stream) error {
	sp := &st.snd
	switch {
	case !st.hasS:
		return ErrInvalid
	case sp.errState != nil:
		return sp.errState
	case s.err != nil:
		return s.err
	case sp.finWr:
		return &StreamError{Kind: StreamClosedLocally, StreamID: st.id}
	}
	return nil
}

// Write takes as much of p as the send buffer admits and returns the count;
// 0 with a nil error means wait (WaitWrite). With fin, FIN follows p once
// all of p was taken.
func (s *Session) Write(st *Stream, p []byte, fin bool) (int, error) {
	s.pickInput()
	if err := s.writeErr(st); err != nil {
		return 0, err
	}
	sp := &st.snd
	n := len(p)
	if room := s.sendRoom(st); uint64(n) > room {
		n = int(room)
	}
	h := s.heldEnter(st)
	if n > 0 {
		if st.unsent() == 0 {
			sp.waitAt = s.now
		}
		sp.q.write(p[:n])
		s.stats.AppBytesWritten += uint64(n)
		st.snd.appWritten += uint64(n)
	}
	if fin && n == len(p) {
		sp.finWr = true
	}
	if n == len(p) {
		sp.wwait = false
		sp.writeDone = s.now
	}
	s.heldLeave(st, h)
	if n > 0 || sp.finWr {
		s.becomeSend(st)
	}
	return n, nil
}

func (s *Session) becomeSend(st *Stream) {
	if st.snd.state == sendReady && st.idSet {
		st.snd.state = sendSend
	}
	s.sched.ensure(st)
	s.wakeAllWriters()
}

// smallHeld is the most a "small" stream direction holds.
const smallHeld = 64 << 10

// heldClass is a direction's place in the send-buffer accounting.
type heldClass struct {
	held  uint64
	small bool // at most 64 KiB held and no write waiting
	big   bool // otherwise, with bytes held or a write waiting
}

func classOf(st *Stream) heldClass {
	sp := &st.snd
	h := sp.q.end - sp.q.base
	small := h <= smallHeld && !sp.wwait
	return heldClass{held: h, small: small && h > 0, big: !small}
}

// heldEnter and heldLeave bracket every change of a direction's held bytes
// or waiting state: the single authority of the send-buffer sums (the
// session total, the small directions' bytes and the count of the others).
func (s *Session) heldEnter(st *Stream) heldClass { return classOf(st) }

func (s *Session) heldLeave(st *Stream, before heldClass) {
	after := classOf(st)
	if st.discarded || st.cancelled {
		after = heldClass{}
	}
	c := &s.conn
	c.sendHeld = c.sendHeld - min64(c.sendHeld, before.held) + after.held
	if before.small {
		c.smallHeld -= min64(c.smallHeld, before.held)
	}
	if after.small {
		c.smallHeld += after.held
	}
	if before.big {
		c.bigCount--
	}
	if after.big {
		c.bigCount++
	}
}

// sendRoom applies the session send buffer by water-filling: small directions
// come off first, every other direction may hold max(64 KiB, (send_buffer -
// small bytes) / n); each direction may always hold 64 KiB.
func (s *Session) sendRoom(st *Stream) uint64 {
	cl := classOf(st)
	c := &s.conn
	n := c.bigCount
	pool := s.cfg.SendBuffer - min64(s.cfg.SendBuffer, c.smallHeld)
	if !cl.big {
		n++ // it competes once it holds more
		if cl.small {
			pool += min64(cl.held, s.cfg.SendBuffer-pool)
		}
	}
	share := max64(smallHeld, pool/uint64(max(n, 1)))
	if st.snd.bufLimit > 0 {
		share = min64(share, max64(st.snd.bufLimit, smallHeld))
	}
	if cl.held >= share {
		return 0
	}
	return share - cl.held
}

// SetSendBuffer bounds the part's written-but-unreleased bytes.
func (s *Session) SetSendBuffer(st *Stream, n uint64) {
	st.snd.bufLimit = max64(n, 16<<10)
	s.wake(st.wsig)
}

// WaitWrite registers sig for write progress of st.
// A write that begins waiting within 1 ms of the stream's previous write
// completing continues the same wait (age).
func (s *Session) WaitWrite(st *Stream, sig *Signal) {
	st.wsig = sig
	sp := &st.snd
	if !sp.wwait {
		if !sp.wwaitSet || s.now.Sub(sp.writeDone) > Millisecond {
			sp.wwaitAt = s.now
		}
		h := s.heldEnter(st)
		sp.wwait, sp.wwaitSet = true, true
		s.heldLeave(st, h)
	}
}

// Finish queues FIN after the buffered data (Close of a SendStream).
func (s *Session) Finish(st *Stream) error {
	if st.hasS && st.snd.finWr && st.snd.errState == nil {
		return nil
	}
	if err := s.writeErr(st); err != nil {
		return err
	}
	st.snd.finWr = true
	s.checkCapacity(st)
	s.becomeSend(st)
	return nil
}

// SetReliableBoundary marks the data written so far as delivered even if the
// stream is reset later.
func (s *Session) SetReliableBoundary(st *Stream) {
	st.snd.boundary = st.snd.appWritten
	st.snd.hasBound = true
}

// Reset resets the sending part with code (CancelWrite).
func (s *Session) Reset(st *Stream, code uint64) {
	sp := &st.snd
	if !st.hasS || sp.reset || sp.errState != nil && sp.errState.Kind != StreamStopped ||
		sp.state == sendDataRecvd || sp.state == sendResetRecvd {
		s.maybeDiscard(st)
		return
	}
	if sp.errState == nil {
		sp.errState = &StreamError{Kind: StreamClosedLocally, StreamID: st.id, Code: code}
	}
	if !st.idSet {
		if !st.snd.hasBound || st.snd.boundary == 0 || !s.peerSeen {
			s.cancelUnopened(st)
			return
		}
		// Data below a reliable boundary must be delivered: open now.
		s.allocID(st)
	}
	s.startReset(st, code, false)
}

// startReset queues RESET_STREAM or RESET_STREAM_AT (SPEC.md 4.3,
// EXTENSIONS.md 4.5). stopReply: the reset answers STOP_SENDING.
func (s *Session) startReset(st *Stream, code uint64, stopReply bool) {
	sp := &st.snd
	if sp.reset {
		if stopReply && sp.resetRel > sp.sent {
			sp.resetRel = sp.sent
			sp.resetSent = false
			s.queueCtl(st, ctlReset)
		}
		return
	}
	sp.reset = true
	sp.resetCode = code
	switch {
	case stopReply:
		sp.resetRel = 0
	case sp.hasBound:
		sp.resetRel = sp.preLen + sp.boundary
		sp.resetAt = true
	case s.feat.Preamble && st.local:
		sp.resetRel = sp.preLen
		sp.resetAt = true
	}
	if sp.resetAt && s.peer.Has&wire.HasResetAt == 0 {
		sp.resetAt, sp.resetRel = false, 0
	}
	sp.resetRel = min64(sp.resetRel, sp.q.end)
	if sp.resetRel == 0 {
		sp.resetAt = false
	}
	sp.finWr = false
	s.queueCtl(st, ctlReset)
	s.sched.ensure(st)
	s.checkCapacity(st)
	s.wakeAllWriters()
}

// cancelUnopened forgets a stream that never emitted a frame (SPEC.md 7.2).
func (s *Session) cancelUnopened(st *Stream) {
	if st.cancelled {
		return
	}
	st.cancelled = true
	s.dropProvisional(st)
	h := s.heldEnter(st)
	st.snd.wwait = false
	st.snd.q.drop()
	s.heldLeave(st, h)
	s.sched.remove(st)
	t := typeIndex(st.bidi)
	if s.loc[t].reserved > 0 {
		s.loc[t].reserved--
	}
	s.wakeOpeners()
}

// onStopSending handles a received STOP_SENDING (SPEC.md 4.3).
func (s *Session) onStopSending(st *Stream, code uint64) {
	sp := &st.snd
	if sp.state == sendDataRecvd || sp.state == sendResetRecvd {
		return
	}
	if !sp.stopped {
		sp.stopped, sp.stopCode = true, code
		if sp.errState == nil || sp.errState.Kind == StreamClosedLocally && !sp.reset {
			sp.errState = &StreamError{Kind: StreamStopped, StreamID: st.id, Code: code, Remote: true}
		}
	}
	rc := code
	if sp.reset {
		rc = sp.resetCode
	}
	s.startReset(st, rc, true)
	s.wake(st.wsig)
}

// onSendCredit applies MAX_STREAM_DATA.
func (s *Session) onSendCredit(st *Stream, v uint64) bool {
	if v <= st.snd.maxData {
		return false
	}
	if st.unsent() > 0 && max64(st.snd.sent, st.snd.q.base) >= st.snd.maxData {
		st.snd.creditAt = s.now
	}
	st.snd.maxData = v
	s.checkCapacity(st)
	s.sched.ensure(st)
	s.wake(st.wsig)
	s.wakeAllWriters()
	return true
}

// sendTerminal records a terminal sending state and releases buffers.
func (s *Session) sendTerminal(st *Stream, state sendState) {
	sp := &st.snd
	sp.state = state
	if s.mp != nil && sp.sent > sp.q.base {
		s.mp.retained -= min64(s.mp.retained, sp.sent-sp.q.base)
	}
	h := s.heldEnter(st)
	sp.wwait = false
	sp.q.drop()
	s.heldLeave(st, h)
	s.sched.remove(st)
	s.wake(st.wsig)
	s.maybeDiscard(st)
}

// ---- send capacity ----

// WaitSendCapacity registers sig to be woken once the part's capacity is
// at least min, or above 0 while it holds nothing unsent, or the part can
// no longer send. It reports whether the condition holds now (sig is then
// not registered).
func (s *Session) WaitSendCapacity(st *Stream, min uint64, sig *Signal) bool {
	if s.capReady(st, min) {
		return true
	}
	x := st.x()
	if x.capSig == nil {
		s.capWaiters = append(s.capWaiters, st)
	}
	x.capSig, x.capMin = sig, max64(min, 1)
	return false
}

// CancelCapacityWait drops a waiter.
func (s *Session) CancelCapacityWait(st *Stream) {
	if st.ext != nil {
		st.ext.capSig = nil
	}
	for i, x := range s.capWaiters {
		if x == st {
			s.capWaiters = append(s.capWaiters[:i], s.capWaiters[i+1:]...)
			return
		}
	}
}

func (s *Session) capReady(st *Stream, min uint64) bool {
	sp := &st.snd
	if !st.hasS || sp.finWr || sp.reset || sp.stopped || s.err != nil || st.discarded {
		return true
	}
	c := s.SendCapacity(st)
	return c >= max64(min, 1) || c > 0 && st.unsent() == 0
}

// checkCapacity wakes st's capacity waiter when its condition holds.
func (s *Session) checkCapacity(st *Stream) {
	if x := st.ext; x != nil && x.capSig != nil && s.capReady(st, x.capMin) {
		s.wake(x.capSig)
		s.CancelCapacityWait(st)
	}
}

// wakeCapacity re-evaluates every capacity waiter (session credit or state
// changed).
func (s *Session) wakeCapacity() {
	for i := 0; i < len(s.capWaiters); {
		st := s.capWaiters[i]
		if x := st.ext; x != nil && x.capSig != nil && s.capReady(st, x.capMin) {
			s.wake(x.capSig)
			x.capSig = nil
			s.capWaiters = append(s.capWaiters[:i], s.capWaiters[i+1:]...)
			continue
		}
		i++
	}
}

// WriteChunk queues data that references buf without copying;
// the queue takes its own reference. It returns how much it took (the send
// buffer bounds it, as for Write).
func (s *Session) WriteChunk(st *Stream, data []byte, buf *Buf, fin bool) (int, error) {
	if buf == nil {
		return s.Write(st, data, fin)
	}
	if err := s.writeErr(st); err != nil {
		return 0, err
	}
	sp := &st.snd
	n := len(data)
	if room := s.sendRoom(st); uint64(n) > room {
		n = int(room)
	}
	h := s.heldEnter(st)
	if n > 0 {
		if st.unsent() == 0 {
			sp.waitAt = s.now
		}
		sp.q.writeRef(data[:n], buf)
		s.stats.AppBytesWritten += uint64(n)
		sp.appWritten += uint64(n)
	}
	if fin && n == len(data) {
		sp.finWr = true
	}
	if n == len(data) {
		sp.wwait = false
		sp.writeDone = s.now
	}
	s.heldLeave(st, h)
	if n > 0 || sp.finWr {
		s.becomeSend(st)
	}
	return n, nil
}

// WriteErrOf returns the error a write would get now, nil when it may
// proceed.
func (s *Session) WriteErrOf(st *Stream) error { return s.writeErr(st) }

// SendRoom returns what the send buffer admits for the part now.
func (s *Session) SendRoom(st *Stream) uint64 { return s.sendRoom(st) }

// WriteBatch writes the bytes of b from skip on as stream data of st, as
// far as the send buffer admits (the carrier of a nested
// session): the payload segments b pins are referenced, not copied (their
// blocks and buffers are reference-counted across sessions), the record
// headers are copied. The caller owns b until it reports the write to b's
// session. It returns the bytes taken.
func (s *Session) WriteBatch(st *Stream, b *Batch, skip int) (int, error) {
	if err := s.writeErr(st); err != nil {
		return 0, err
	}
	sp := &st.snd
	room := s.sendRoom(st)
	h := s.heldEnter(st)
	start, taken := skip, 0
	for _, sg := range b.segs {
		if room == 0 {
			break
		}
		d := b.seg(sg)
		if skip >= len(d) {
			skip -= len(d)
			continue
		}
		d = d[skip:]
		skip = 0
		if uint64(len(d)) > room {
			d = d[:room]
		}
		if st.unsent() == 0 {
			sp.waitAt = s.now
		}
		if sg.ext != nil && !sg.pin.none() {
			sp.q.writePin(d, sg.pin)
		} else {
			sp.q.write(d)
		}
		taken += len(d)
		room -= uint64(len(d))
	}
	s.stats.AppBytesWritten += uint64(taken)
	sp.appWritten += uint64(taken)
	if start+taken == b.n {
		sp.wwait = false
		sp.writeDone = s.now
	}
	s.heldLeave(st, h)
	if taken > 0 {
		s.becomeSend(st)
	}
	return taken, nil
}
