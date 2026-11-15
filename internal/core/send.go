package core

import (
	"slices"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// ---- writing (sending part) ----

// SendCapacity returns the bytes the part could transmit now:
// the smaller remaining credit minus what it already holds unsent.
func (s *Session) SendCapacity(st *Stream) uint64 {
	sp := &st.snd
	if !s.writable(st) {
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

// writable reports whether the part takes writes now: the one test of
// every write, capacity and room check (writeErr names why not). A part
// that was reset, stopped, cancelled or discarded has its error state set
// or its FIN written, so these four cover them.
func (s *Session) writable(st *Stream) bool {
	return st.hasS && st.snd.errState == nil && s.err == nil && !st.snd.finWr
}

// writeErr returns the error a write gets now, nil when it may proceed
// (writable).
func (s *Session) writeErr(st *Stream) error {
	if s.writable(st) {
		return nil
	}
	sp := &st.snd
	switch {
	case !st.hasS:
		return ErrInvalid
	case sp.errState != nil:
		return sp.errState
	case s.err != nil:
		return s.err
	}
	return &StreamError{Kind: StreamClosedLocally, StreamID: st.id}
}

// admitWrite is writeErr for a write that takes bytes: a write that fails
// also ends its wait for room, so the stream stops counting as a waiting
// writer whatever ended the part (Close, reset, stop, session end).
func (s *Session) admitWrite(st *Stream) error {
	if st.snd.res != nil {
		panic("zmux: a write during a WriteSpace loan") // one writer at a time (the caller's wmu)
	}
	err := s.writeErr(st)
	if err != nil {
		s.endWaitWrite(st)
	}
	return err
}

// Write takes as much of p as the send buffer admits and returns the count;
// 0 with a nil error means wait (WaitWrite). With fin, FIN follows p once
// all of p was taken.
func (s *Session) Write(st *Stream, p []byte, fin bool) (int, error) {
	s.pickInput()
	if err := s.admitWrite(st); err != nil {
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

// WriteSpace lends the caller the free tail of the part's send block, at
// most limit bytes and no more than the send buffer admits, to fill in
// place; CommitWrite then takes the bytes written into it. An empty slice
// with a nil error means wait (WaitWrite). Until CommitWrite the part takes
// no other write and the caller may fill the slice without the session
// lock: the bytes lie above the block's filled length, which nothing else
// reads or writes.
func (s *Session) WriteSpace(st *Stream, limit int) ([]byte, error) {
	s.pickInput()
	if err := s.admitWrite(st); err != nil {
		return nil, err
	}
	sp := &st.snd
	if sp.res != nil {
		panic("zmux: WriteSpace without CommitWrite")
	}
	room := s.sendRoom(st)
	if room == 0 || limit <= 0 {
		return nil, nil
	}
	blk := sp.q.lendTail()
	n := min(blockSize-blk.n, limit)
	if uint64(n) > room {
		n = int(room)
	}
	sp.res = blk
	s.endWaitWrite(st) // room found: the writer fills it, it no longer waits
	return blk.b[blk.n : blk.n+n : blk.n+n], nil
}

// CommitWrite takes the first n bytes of the slice WriteSpace lent (n may be
// 0) and ends the loan. The bytes are taken even when the send buffer
// shrank meanwhile, so it may run over by less than one block. A part
// reset or closed meanwhile discards them and returns its error.
func (s *Session) CommitWrite(st *Stream, n int) error {
	sp := &st.snd
	blk := sp.res
	if blk == nil {
		panic("zmux: CommitWrite without WriteSpace")
	}
	sp.res = nil
	s.pickInput()
	if err := s.admitWrite(st); err != nil || n == 0 {
		blk.unref()
		return err
	}
	h := s.heldEnter(st)
	if st.unsent() == 0 {
		sp.waitAt = s.now
	}
	sp.q.commitTail(blk, n)
	s.stats.AppBytesWritten += uint64(n)
	sp.appWritten += uint64(n)
	sp.wwait = false
	sp.writeDone = s.now
	s.heldLeave(st, h)
	s.becomeSend(st)
	return nil
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

// sendBufferLimit is the absolute maximum: a fixed value, or the 64 MiB
// floor and what the original memory account can grant. sendHeld is already
// charged, so adding it back avoids charging unsent/retained data twice.
func (s *Session) sendBufferLimit() uint64 {
	if n := s.cfg.SendBufferMax; n > 0 {
		return max64(n, s.cfg.SendBuffer)
	}
	left := ^uint64(0)
	used := s.memUsage()
	if b := s.cfg.Budget; b != nil {
		if b.limit > 0 {
			left = b.limit - min64(b.limit, b.Used())
			growth := used - min64(used, s.charged)
			left -= min64(left, growth)
		}
	} else if b := s.cfg.MemoryBudget; b > 0 {
		left = b - min64(b, used)
	}
	limit := ^uint64(0)
	if left < limit-s.conn.sendHeld {
		limit = s.conn.sendHeld + left
	}
	return max64(max64(64<<20, limit), s.cfg.SendBuffer)
}

// sessionSendBuffer is the one buffer divided by writers and affinity load.
func (s *Session) sessionSendBuffer() uint64 {
	if s.mp == nil {
		return s.cfg.SendBuffer
	}
	return max64(s.cfg.SendBuffer, s.retransmitBudget())
}

// sendRoom applies the session send buffer by water-filling: small directions
// come off first, every other direction may hold max(64 KiB, (send_buffer -
// small bytes) / n); each direction may always hold 64 KiB.
func (s *Session) sendRoom(st *Stream) uint64 {
	cl := classOf(st)
	c := &s.conn
	n := c.bigCount
	total := s.sessionSendBuffer()
	pool := total - min64(total, c.smallHeld)
	if !cl.big {
		n++ // it competes once it holds more
		if cl.small {
			pool += min64(cl.held, total-pool)
		}
	}
	share := max64(smallHeld, pool/uint64(max(n, 1)))
	if st.snd.bufLimit > 0 {
		share = min64(share, st.snd.bufLimit)
	}
	if cl.held >= share {
		return 0
	}
	room := share - cl.held
	totalRoom := total - min64(total, c.sendHeld)
	floor := uint64(smallHeld)
	if st.snd.bufLimit > 0 {
		floor = min64(floor, st.snd.bufLimit)
	}
	floorRoom := floor - min64(floor, cl.held)
	return min64(room, max64(totalRoom, floorRoom))
}

// SetSendBuffer bounds the part's written-but-unreleased bytes.
func (s *Session) SetSendBuffer(st *Stream, n uint64) {
	st.snd.bufLimit = max64(n, 16<<10)
	s.sendChanged(st)
}

// WaitWrite makes st a writer that waits for send-buffer room, until the
// write takes its last byte, fails or ends its wait (EndWaitWrite); the core
// wakes its slot (SetSignals) on every change of room, credit or state.
// A write that begins waiting within 1 ms of the stream's previous write
// completing continues the same wait (age).
func (s *Session) WaitWrite(st *Stream) {
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

// EndWaitWrite ends the wait WaitWrite began: the stream no longer counts as
// a waiting writer (its size class, the scheduler's expected bytes and the
// tier's aging). A write that waits calls it when it stops waiting without
// having taken its last byte: room found, cancelled or past its deadline.
// The core ends the wait itself when the write completes or fails, and
// when the part ends (Finish, reset).
func (s *Session) EndWaitWrite(st *Stream) { s.endWaitWrite(st) }

func (s *Session) endWaitWrite(st *Stream) {
	sp := &st.snd
	if !sp.wwait {
		return
	}
	h := s.heldEnter(st)
	sp.wwait = false
	s.heldLeave(st, h)
}

// Finish queues FIN after the buffered data (Close of a SendStream). It
// wakes a writer that waits for room: its write fails now.
func (s *Session) Finish(st *Stream) error {
	if st.hasS && st.snd.finWr && st.snd.errState == nil {
		return nil
	}
	if err := s.writeErr(st); err != nil {
		return err
	}
	st.snd.finWr = true
	s.endWaitWrite(st)
	s.sendChanged(st)
	s.becomeSend(st)
	return nil
}

// SetReliableBoundary marks the data written so far as delivered even if the
// stream is reset later.
func (s *Session) SetReliableBoundary(st *Stream) {
	st.snd.boundary = st.snd.appWritten
	st.snd.hasBound = true
}

// cancelledLocally reports whether the application reset the sending part
// (CancelWrite) before any STOP_SENDING ended it.
func (sp *sendPart) cancelledLocally() bool {
	return sp.errState != nil && sp.errState.Kind == StreamClosedLocally
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
		if !s.allocID(st) {
			return
		}
	}
	s.startReset(st, code, false)
}

// startReset queues RESET_STREAM or RESET_STREAM_AT.
// stopReply: the reset answers STOP_SENDING.
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
	s.endWaitWrite(st)
	s.queueCtl(st, ctlReset)
	s.sched.ensure(st)
	s.sendChanged(st) // the part takes no more writes: a blocked one fails now
	s.wakeAllWriters()
}

// cancelUnopened forgets a stream that never emitted a frame.
func (s *Session) cancelUnopened(st *Stream) {
	if st.cancelled {
		return
	}
	st.cancelled = true
	// An unannounced identifier cannot receive a reverse half from the peer.
	if st.hasR {
		st.rcv.state = recvDataRead
	}
	s.streamChanged(st)
	s.unlinkCtl(st)
	st.ctl = 0
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

// onStopSending handles a received STOP_SENDING.
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
	s.sched.ensure(st)
	s.sendChanged(st)
	s.wakeAllWriters()
	return true
}

// sendTerminal records a terminal sending state and releases buffers.
func (s *Session) sendTerminal(st *Stream, state sendState) {
	sp := &st.snd
	sp.state = state
	if sp.sent > sp.q.base {
		s.mp.release(sp.sent - sp.q.base)
	}
	h := s.heldEnter(st)
	sp.wwait = false
	sp.q.drop()
	s.heldLeave(st, h)
	s.sched.remove(st)
	s.sendChanged(st)
	s.maybeDiscard(st)
}

// ---- send capacity ----

// WaitSendCapacity reports whether the part's capacity is at least n, or
// above 0 while it holds nothing unsent, or the part can no longer send.
// When not, st waits for capacity until CancelCapacityWait or a later call
// that holds: its waiter (the handle's write slot) is woken through
// sendChanged by every change the condition depends on, the session's
// credit included (wakeCapacityWaiters).
func (s *Session) WaitSendCapacity(st *Stream, n uint64) bool {
	if s.capReady(st, n) {
		s.CancelCapacityWait(st)
		return true
	}
	if x := st.x(); !x.capWait {
		x.capWait = true
		s.capWaiters = append(s.capWaiters, st)
	}
	return false
}

// CancelCapacityWait ends st's capacity wait.
func (s *Session) CancelCapacityWait(st *Stream) {
	if st.ext == nil || !st.ext.capWait {
		return
	}
	st.ext.capWait = false
	if i := slices.Index(s.capWaiters, st); i >= 0 {
		s.capWaiters = slices.Delete(s.capWaiters, i, i+1)
	}
}

func (s *Session) capReady(st *Stream, n uint64) bool {
	if !s.writable(st) {
		return true
	}
	c := s.SendCapacity(st)
	return c >= max64(n, 1) || c > 0 && st.unsent() == 0
}

// wakeCapacityWaiters wakes the streams that wait for capacity when the
// session's credit or parameters changed, the one input of SendCapacity
// that is not the stream's own.
func (s *Session) wakeCapacityWaiters() {
	for _, st := range s.capWaiters {
		s.sendChanged(st)
	}
}

// WriteChunk queues data that references buf without copying;
// the queue takes its own reference. It returns how much it took (the send
// buffer bounds it, as for Write).
func (s *Session) WriteChunk(st *Stream, data []byte, buf *Buf, fin bool) (int, error) {
	if buf == nil {
		return s.Write(st, data, fin)
	}
	if err := s.admitWrite(st); err != nil {
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

// WritableRoom returns the room a write takes now, or the error it gets;
// room found ends a write's wait for it (the caller writes next or not).
func (s *Session) WritableRoom(st *Stream) (uint64, error) {
	if err := s.admitWrite(st); err != nil {
		return 0, err
	}
	room := s.sendRoom(st)
	if room > 0 {
		s.endWaitWrite(st)
	}
	return room, nil
}

// SendRoom returns what the send buffer admits for the part now.
func (s *Session) SendRoom(st *Stream) uint64 { return s.sendRoom(st) }

// WriteBatch writes the bytes of b from skip on as stream data of st, as
// far as the send buffer admits (the carrier of a nested
// session): the payload segments b pins are referenced, not copied (their
// blocks and buffers are reference-counted across sessions), the record
// headers are copied. The caller owns b until it reports the write to b's
// session. It returns the bytes taken.
func (s *Session) WriteBatch(st *Stream, b *Batch, skip int) (int, error) {
	if err := s.admitWrite(st); err != nil {
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

// Flushable reports whether a stream holds written data or a FIN not yet
// sent, on a session that has a live outbound pipe: what a graceful close
// sends before CONNECTION_CLOSE, waiting for credit as for the carriers
// within the close's bound, streams that have no identifier yet included.
// Data that waits for a pipe does not count.
func (s *Session) Flushable() bool {
	if s.isClosing() {
		return false
	}
	if out, _ := s.LiveDirections(); !out {
		return false
	}
	for _, st := range s.streams {
		if st.flushable() {
			return true
		}
	}
	for _, st := range s.provisional { // written before their first frame
		if st.flushable() {
			return true
		}
	}
	return false
}

// flushable reports whether the stream holds written data or a FIN not yet
// sent.
func (st *Stream) flushable() bool {
	sp := &st.snd
	if !st.hasS || st.cancelled || sp.state >= sendDataSent || sp.reset {
		return false
	}
	return st.unsent() > 0 || sp.finWr && !sp.finSent
}
