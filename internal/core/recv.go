package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

// lookupStream resolves the stream a frame references, opening peer streams
// implicitly. It returns nil without error for a
// discarded stream (crossing frame). opens is whether the
// frame may open a peer stream of this direction.
func (s *Session) lookupStream(id uint64, opens bool) (*Stream, *wire.Error) {
	if st := s.streams[id]; st != nil {
		return st, nil
	}
	idx := id >> 2
	t := typeIndex(isBidiID(id))
	if isLocalID(id, s.role) {
		if idx >= s.loc[t].next {
			return nil, wire.NewError(wire.StreamStateError, "frame for an unopened local stream", "stream", id)
		}
		return nil, nil
	}
	rc := &s.rem[t]
	if idx < rc.opened {
		return nil, nil
	}
	if !opens {
		return nil, wire.NewError(wire.StreamStateError, "frame in the wrong direction", "stream", id)
	}
	if idx >= rc.limit {
		return nil, wire.NewError(wire.StreamLimitError, "stream opened beyond the limit",
			"stream", id, "limit", rc.limit)
	}
	var last *Stream
	for i := rc.opened; i <= idx; i++ {
		nid := streamID(i, t == 0, s.role == RoleInitiator)
		st := s.newStream(t == 0, false)
		st.id, st.idSet, st.opened = nid, true, true
		st.urgency, st.incr = 3, true
		s.streams[nid] = st
		if s.shp != nil {
			s.shapeStream(shaping.EventStreamOpen, nid)
		}
		st.applyPeerParams()
		s.stats.StreamsOpenedRemote++
		if s.ga.sent && i >= s.ga.sentWater[t] || s.underPressure() {
			s.stackRejectStream(st)
		} else if !s.feat.Preamble {
			s.offer(st)
		}
		s.releaseHeld(st)
		last = st
	}
	rc.opened = idx + 1
	return last, nil
}

// offer queues a peer stream for accept.
func (s *Session) offer(st *Stream) {
	if st.offered || st.stackReject {
		return
	}
	st.offered = true
	t := typeIndex(st.bidi)
	if idx := st.id>>2 + 1; idx > s.ga.maxOffered[t] {
		s.ga.maxOffered[t] = idx
	}
	s.acceptQ[t].push(st)
	if len(s.acceptW[t]) > 0 {
		s.wake(s.acceptW[t][0])
	}
}

// stackRejectStream rejects a peer stream with ZMUX_REJECTED on both parts
// without offering it.
func (s *Session) stackRejectStream(st *Stream) {
	st.stackReject = true
	st.releasedR, st.releasedS = true, true
	if st.hasS {
		st.snd.errState = &StreamError{Kind: StreamRejected, StreamID: st.id}
		s.startReset(st, wire.CodeRejected, false)
	}
	s.stopRecv(st, wire.CodeRejected)
}

// AcceptStream pops an offered peer stream; ok is false when none waits.
// The first call for unidirectional streams raises staged credit.
func (s *Session) AcceptStream(bidi bool) (*Stream, bool) {
	if s.state >= StateClosing {
		return nil, false // the queue was discarded: accept fails
	}
	t := typeIndex(bidi)
	if !bidi {
		s.unstageUni()
	}
	st := s.acceptQ[t].pop()
	if st == nil {
		return nil, false
	}
	st.accepted = true
	if len(s.acceptW[t]) > 0 && s.acceptQ[t].len() > 0 {
		s.wake(s.acceptW[t][0])
	}
	return st, true
}

// WaitAccept registers sig for an offered stream of the type.
func (s *Session) WaitAccept(bidi bool, sig *Signal) {
	t := typeIndex(bidi)
	if !bidi {
		s.unstageUni()
	}
	s.acceptW[t] = append(s.acceptW[t], sig)
}

// WakeAcceptWaiters signals every blocked accept of this direction to recheck ownership.
func (s *Session) WakeAcceptWaiters(bidi bool) {
	for _, w := range s.acceptW[typeIndex(bidi)] {
		s.wake(w)
	}
}

// CancelAcceptWait removes sig.
func (s *Session) CancelAcceptWait(bidi bool, sig *Signal) {
	t := typeIndex(bidi)
	s.acceptW[t] = removeSig(s.acceptW[t], sig)
	if len(s.acceptW[t]) > 0 && s.acceptQ[t].len() > 0 {
		s.wake(s.acceptW[t][0])
	}
}

// unstageUni raises staged unidirectional credit on the first accept.
func (s *Session) unstageUni() {
	rc := &s.rem[1]
	if !rc.staged {
		return
	}
	rc.staged = false
	rc.initial = s.cfg.UniTarget
	if s.ga.sent {
		return
	}
	if nl := rc.initial + rc.discarded; nl > rc.limit {
		rc.limit = nl
		s.pend.maxStreams[1] = true
		s.wakeAllWriters()
	}
}

// onStream processes a STREAM frame. It returns whether the frame had an
// effect (for the no-effect budget, NE-5).
func (s *Session) onStream(f *wire.Frame, buf *Buf) (bool, *wire.Error) {
	st, err := s.lookupStream(f.StreamID, true)
	if err != nil {
		return false, err
	}
	if st == nil {
		return true, nil // discarded stream: ignored, never chargeable
	}
	if !st.hasR {
		return false, wire.NewError(wire.StreamStateError, "STREAM on a send-only stream", "stream", st.id)
	}
	r := &st.rcv
	off := f.Offset
	end := off + uint64(len(f.Data))
	if r.finKnown && (end > r.final || f.Fin && end != r.final) {
		return false, wire.NewError(wire.FinalSizeError, "data beyond the final size", "stream", st.id,
			"offset", off, "length", len(f.Data), "limit", r.final)
	}
	if f.Fin && end < r.highest {
		return false, wire.NewError(wire.FinalSizeError, "final size below received data", "stream", st.id)
	}
	if end > r.adv {
		return false, wire.NewError(wire.FlowControlError, "stream data beyond the limit", "stream", st.id,
			"offset", off, "length", len(f.Data), "limit", r.adv)
	}
	if !s.feat.Multipath && off != r.highest && (!r.reset || off < r.highest) {
		return false, wire.NewError(wire.ProtocolViolation, "out-of-order stream data", "stream", st.id,
			"offset", off, "length", len(f.Data))
	}
	top := r.highest
	if r.finKnown {
		top = max64(top, r.final)
	}
	if end > top {
		if s.conn.recv+(end-top) > s.conn.adv {
			return false, wire.NewError(wire.FlowControlError, "session data beyond the limit", "stream", st.id,
				"limit", s.conn.adv)
		}
		s.conn.recv += end - top
	}
	if s.feat.Multipath {
		s.countDuplicate(r, off, end)
	}
	effect := end > r.highest || f.Fin && !r.finKnown
	r.highest = max64(r.highest, end)
	if f.Fin && !r.finKnown {
		r.finKnown, r.final = true, end
		if r.state == recvRecv {
			r.state = recvSizeKnown
		}
	}
	if s.tuneOn() {
		s.tuneData(st)
	}
	// Preamble of a peer stream.
	data := f.Data
	if !st.local && s.feat.Preamble && !r.preDone && off == 0 {
		var pre wire.Preamble
		if e := wire.ParsePreamble(data, s.cfg.Local.Preamble, &pre); e != nil {
			return false, e
		}
		effect = true
		r.preDone, r.preSize = true, uint64(pre.Size)
		st.peerPre = copyPreamble(&pre)
		if pre.HasPriority && st.hasS && st.bidi {
			st.urgency, st.incr = pre.Urgency, pre.Incr
		}
		if e := s.checkPreambleReset(st); e != nil {
			return false, e
		}
		// Bytes buffered before the preamble that overlap it leave the
		// buffer through the one accounting path.
		if d := r.buf.discard(r.preSize); d > 0 {
			s.bufSub(d)
		}
		newPre := uint64(pre.Size) - r.arrived.covered(0, uint64(pre.Size))
		r.arrived.add(0, uint64(pre.Size))
		s.releaseUnbuffered(newPre)
		r.readOff = max64(r.readOff, r.preSize)
		data = data[pre.Size:]
		off = uint64(pre.Size)
		if !r.stopped {
			s.offer(st)
		}
	}
	if len(data) > 0 {
		lo := max64(off, r.readOff)
		hi := end
		if r.reset {
			hi = min64(hi, r.resetRel)
		}
		if lo < hi {
			nb := r.arrived.add(lo, hi)
			if r.arrived.len() > maxRecvRanges(r.adv-min64(r.adv, r.readOff)) {
				return false, wire.NewError(wire.ExcessiveLoad, "stream data in too many pieces", "stream", st.id,
					"ranges", r.arrived.len())
			}
			if nb > 0 {
				effect = true
			}
			if r.stopped || st.stackReject {
				s.releaseUnbuffered(nb)
			} else if nb > 0 {
				part := data[lo-off : hi-off]
				stored := r.buf.insert(lo, part, buf)
				s.compactPins(buf)
				s.bufAdd(stored)
				if stored < nb {
					s.releaseUnbuffered(nb - stored)
				}
			}
		}
		if end > hi && r.reset {
			// Bytes above the Reliable Size after a reset: dropped; their
			// credit was released with the reset's gap.
			r.arrived.add(max64(hi, off), end)
		}
	}
	if f.Fin && !r.preDone && s.feat.Preamble && !st.local && end == 0 && !r.stopped {
		return false, wire.NewError(wire.ProtocolViolation, "opener's sending part has final size 0", "stream", st.id)
	}
	if s.cfg.TraceStreams && s.rxPipe != nil && s.mp != nil && !r.stopped && !r.reset {
		s.traceStreamGap(st, f)
	}
	s.recvProgress(st)
	if !effect && (len(f.Data) > 0 || !f.Fin) && !s.feat.Multipath {
		return false, nil
	}
	return effect || st.opened, nil
}

func copyPreamble(p *wire.Preamble) *peerPreamble {
	if !p.HasOpenInfo && !p.HasPriority && !p.PipeCarrier && len(p.App) == 0 {
		return nil // an empty preamble is no preamble
	}
	pp := &peerPreamble{hasOpenInfo: p.HasOpenInfo, hasPrio: p.HasPriority, urgency: p.Urgency,
		incr: p.Incr, pipeCarrier: p.PipeCarrier}
	if p.HasOpenInfo {
		pp.openInfo = append([]byte{}, p.OpenInfo...)
	}
	for _, t := range p.App {
		pp.app = append(pp.app, wire.TLV{Type: t.Type, Value: append([]byte{}, t.Value...)})
	}
	return pp
}

// checkPreambleReset enforces that a reset never cuts into the stream
// preamble, once both the preamble size and the reset are known.
func (s *Session) checkPreambleReset(st *Stream) *wire.Error {
	r := &st.rcv
	if st.local || !s.feat.Preamble || r.stopSent || r.stopped || !r.reset {
		return nil
	}
	if r.preDone && r.resetRel < r.preSize {
		return wire.NewError(wire.ProtocolViolation, "reset below the preamble", "stream", st.id)
	}
	return nil
}

// maxRecvRanges bounds the disjoint ranges of a stream's received data
// (out of order only on multipath): every range costs a piece of its own
// and makes inserts linear in their number, so a peer sending bytes far
// apart would otherwise grow memory and CPU well beyond the bytes its
// window lets it buffer. The bound grows with the window open to the peer
// (one range per 16 KiB), from 1024, ample for the reordering of any
// pipe mix within it.
func maxRecvRanges(window uint64) int {
	return int(max64(1024, window>>14))
}

// unread reports whether the part holds received data the application has
// not read, or an end it has not been told of: a FIN whose data all arrived,
// or a reset (the states before DATA_READ and RESET_READ).
func (r *recvPart) unread() bool {
	return r.buf.contiguous(r.readOff) > r.readOff || r.state == recvDataRecvd || r.state == recvResetRecvd
}

// recvProgress updates the receiving state after input and wakes the reader.
func (s *Session) recvProgress(st *Stream) {
	r := &st.rcv
	if r.finKnown && r.state == recvSizeKnown && r.arrived.contains(r.readOff, r.final) &&
		r.arrived.prefix() >= r.final {
		r.state = recvDataRecvd
	}
	if (r.stopped || st.releasedR) && (r.state == recvDataRecvd || r.state == recvResetRecvd) {
		if r.state == recvDataRecvd {
			r.state = recvDataRead
		} else {
			r.state = recvResetRead
		}
		s.maybeDiscard(st)
		s.drainRead(st)
	}
	s.recvChanged(st)
	s.streamCreditChanged(st)
}

// onReset processes RESET_STREAM (at=false) or RESET_STREAM_AT.
func (s *Session) onReset(f *wire.Frame, at bool) (bool, *wire.Error) {
	st, err := s.lookupStream(f.StreamID, true)
	if err != nil || st == nil {
		return st == nil && err == nil, err
	}
	if !st.hasR {
		return false, wire.NewError(wire.StreamStateError, "reset of a send-only stream", "stream", st.id)
	}
	r := &st.rcv
	rel := uint64(0)
	if at {
		rel = f.Reliable
	}
	if r.finKnown && f.FinalSize != r.final || f.FinalSize < r.highest {
		return false, wire.NewError(wire.FinalSizeError, "final size changed", "stream", st.id)
	}
	if f.FinalSize > r.adv {
		return false, wire.NewError(wire.FlowControlError, "final size beyond the limit", "stream", st.id)
	}
	if r.reset && f.Code != r.resetCode {
		return false, wire.NewError(wire.StreamStateError, "reset changes the error code", "stream", st.id)
	}
	top := r.highest
	if r.finKnown {
		top = max64(top, r.final)
	}
	if f.FinalSize > top {
		s.conn.recv += f.FinalSize - top
		if s.conn.recv > s.conn.adv {
			return false, wire.NewError(wire.FlowControlError, "session data beyond the limit", "stream", st.id)
		}
	}
	if !st.local && s.feat.Preamble && !r.stopSent && !r.stopped {
		if !at || f.FinalSize == 0 {
			return false, wire.NewError(wire.ProtocolViolation, "opener reset without the preamble", "stream", st.id)
		}
	}
	effect := !r.reset || rel < r.resetRel
	if !effect {
		return false, nil
	}
	if !r.reset {
		r.reset, r.resetCode, r.resetRel = true, f.Code, rel
	} else {
		r.resetRel = min64(r.resetRel, rel)
	}
	if !r.finKnown {
		r.finKnown, r.final = true, f.FinalSize
	}
	if e := s.checkPreambleReset(st); e != nil {
		return false, e
	}
	s.forgetStreamGap(st.id)
	// Release the gap the reset makes undeliverable.
	lo, hi := r.resetRel, min64(r.relLo, r.final)
	if lo < hi {
		gap := (hi - lo) - r.arrived.covered(lo, hi)
		r.relLo = lo
		s.releaseUnbuffered(gap)
	}
	if d := r.buf.discardFrom(max64(r.resetRel, r.readOff)); d > 0 {
		s.bufSub(d)
	}
	if r.state != recvDataRead {
		r.state = recvResetRecvd
		r.errState = &StreamError{Kind: StreamReset, StreamID: st.id, Code: r.resetCode, Remote: true,
			Reliable: r.resetRel}
		if r.resetCode == wire.CodeRejected && st.local {
			r.errState.Kind = StreamRejected
		}
	}
	s.recvProgress(st)
	return true, nil
}

// Read copies contiguous data into p. It returns (0, false, nil) when the
// caller waits for its read slot (recvChanged).
func (s *Session) Read(st *Stream, p []byte) (int, bool, error) {
	if !st.hasR {
		return 0, false, ErrInvalid
	}
	r := &st.rcv
	if r.stopped {
		return 0, false, &StreamError{Kind: StreamClosedLocally, StreamID: st.id, Code: r.stopCode}
	}
	if r.state == recvDataRead {
		return 0, true, nil
	}
	n := 0
	if len(p) > 0 {
		n = r.buf.read(r.readOff, p)
	}
	if n > 0 {
		r.readOff += uint64(n)
		s.stats.AppBytesRead += uint64(n)
		st.cnt.appRead += uint64(n)
		s.bufSub(uint64(n))
		s.streamCreditChanged(st)
		s.tuneConsume()
		if r.drainWait {
			s.drainRead(st)
		}
	}
	if r.reset && r.readOff >= r.resetRel && r.buf.contiguous(r.readOff) == r.readOff {
		if n > 0 {
			return n, false, nil
		}
		r.state = recvResetRead
		s.maybeDiscard(st)
		s.drainRead(st)
		return 0, false, r.errState
	}
	if r.finKnown && r.readOff == r.final && !r.reset {
		r.state = recvDataRead
		s.maybeDiscard(st)
		s.drainRead(st)
		return n, true, nil
	}
	if n == 0 && s.err != nil {
		return 0, false, s.err
	}
	return n, false, nil
}

// Readable reports how many bytes a Read would return now.
func (s *Session) Readable(st *Stream) uint64 {
	r := &st.rcv
	return r.buf.contiguous(r.readOff) - r.readOff
}

// Stop abandons the receiving part with code (CancelRead).
func (s *Session) Stop(st *Stream, code uint64) {
	if !st.hasR {
		return
	}
	s.stopRecv(st, code)
	s.maybeDiscard(st)
}

func (s *Session) stopRecv(st *Stream, code uint64) {
	r := &st.rcv
	if r.stopped || r.state == recvDataRead || r.state == recvResetRead {
		return
	}
	r.stopped, r.stopCode = true, code
	s.forgetStreamGap(st.id)
	if d := r.buf.clear(); d > 0 {
		s.bufSub(d)
	}
	if r.drainWait {
		s.drainRead(st)
	}
	if r.finKnown {
		r.readOff = r.final
	} else {
		r.readOff = r.highest
	}
	if !st.cancelled && (r.state == recvRecv || r.state == recvSizeKnown) {
		s.queueCtl(st, ctlStop)
	}
	s.recvProgress(st)
	s.dgramChanged(st) // the stream's datagram flow ended with the part
}

// Release drops every handle: an unfinished sending part is reset with
// ZMUX_CANCELLED and an unread receiving part stopped with code 0.
func (s *Session) Release(st *Stream) bool {
	if st.appReleased() {
		return false
	}
	st.releasedS, st.releasedR = true, true
	s.release(st)
	return true
}

// ReleaseSend and ReleaseRecv record that the application is done with
// one part (closed or cancelled it, read its end or failure); the stream
// is released (Release) once it is done with both. They report whether
// that happened now: the application's one record of which parts it
// still holds.
func (s *Session) ReleaseSend(st *Stream) bool {
	if st.releasedS {
		return false
	}
	st.releasedS = true
	return s.releaseIfDone(st)
}

func (s *Session) ReleaseRecv(st *Stream) bool {
	if st.releasedR {
		return false
	}
	st.releasedR = true
	return s.releaseIfDone(st)
}

func (s *Session) releaseIfDone(st *Stream) bool {
	if !st.appReleased() {
		return false
	}
	s.release(st)
	return true
}

// appReleased reports whether the application is done with every part
// the stream has.
func (st *Stream) appReleased() bool {
	return (st.releasedS || !st.hasS) && (st.releasedR || !st.hasR)
}

// release ends what the application left of a stream it is done with.
func (s *Session) release(st *Stream) {
	if st.hasS && !st.snd.finWr && !st.snd.reset {
		s.Reset(st, wire.CodeCancelled)
	}
	if st.hasR && st.rcv.state != recvDataRead && st.rcv.state != recvResetRead {
		s.Stop(st, 0)
	}
	s.maybeDiscard(st)
}

// Reject declines an accepted stream before reading; it reports whether
// the stream was released now (Release).
func (s *Session) Reject(st *Stream) bool {
	if st.appReleased() {
		return false
	}
	if st.hasS && !st.snd.reset {
		st.snd.errState = &StreamError{Kind: StreamRejected, StreamID: st.id}
		s.startReset(st, wire.CodeRejected, false)
	}
	st.releasedS, st.releasedR = true, true
	if st.hasR {
		s.stopRecv(st, wire.CodeRejected)
	}
	s.maybeDiscard(st)
	return true
}

// maybeDiscard discards the stream's state once both parts are terminal and
// the application released it.
func (s *Session) maybeDiscard(st *Stream) {
	if st.discarded || !st.idSet {
		return
	}
	sendDone := !st.hasS || st.snd.state == sendDataRecvd || st.snd.state == sendResetRecvd
	recvDone := !st.hasR || st.rcv.state == recvDataRead || st.rcv.state == recvResetRead
	released := st.appReleased() || st.stackReject
	if !st.local && !st.offered && !st.stackReject {
		released = false
	}
	if !sendDone || !recvDone || !released {
		return
	}
	st.discarded = true
	s.syncUnsent(st)
	delete(s.streams, st.id)
	if s.shp != nil {
		s.shapeStream(shaping.EventStreamEnd, st.id)
	}
	s.sched.remove(st)
	s.unlinkCtl(st)
	s.unlinkCopyMSD(st)
	s.unbindLane(st)
	if st.hasR {
		if d := st.rcv.buf.clear(); d > 0 {
			s.bufSub(d)
		}
	}
	if st.local && st.bidi && st.hasR {
		s.openLocalBidiRecv--
	}
	if !st.local {
		t := typeIndex(st.bidi)
		rc := &s.rem[t]
		rc.discarded++
		if !s.ga.sent && !rc.staged {
			if nl := rc.initial + rc.discarded; nl > rc.limit {
				rc.limit = nl
				s.pend.maxStreams[t] = true
				s.wakeAllWriters()
			}
		}
	} else {
		t := typeIndex(st.bidi)
		if s.loc[t].reserved > 0 {
			s.loc[t].reserved--
		}
	}
	s.endStreamDgrams(st) // a datagram waiter sees the end; the queue stays
	// A graceful shutdown closes once its last stream is gone: checked by
	// Settle, after the operation that discarded the stream.
	if s.ga.closeWhenDone {
		s.ga.recheck = true
	}
}

func (s *Session) unlinkCtl(st *Stream) {
	if !st.inCtl {
		return
	}
	st.ctl = 0
	st.inCtl = false
	if st.ctlPrev == nil {
		s.ctlHead = st.ctlNext
	} else {
		st.ctlPrev.ctlNext = st.ctlNext
	}
	if st.ctlNext == nil {
		s.ctlTail = st.ctlPrev
	} else {
		st.ctlNext.ctlPrev = st.ctlPrev
	}
	st.ctlNext, st.ctlPrev = nil, nil
}

// markCopyMSD lists st for a copy of its latest MAX_STREAM_DATA in the
// redundant snapshot.
func (s *Session) markCopyMSD(st *Stream) {
	if st.copyMSD {
		return
	}
	st.copyMSD = true
	st.copyPrev, st.copyNext = nil, s.copyHead
	if s.copyHead != nil {
		s.copyHead.copyPrev = st
	}
	s.copyHead = st
}

// unlinkCopyMSD takes st off the copy list.
func (s *Session) unlinkCopyMSD(st *Stream) {
	if !st.copyMSD {
		return
	}
	st.copyMSD = false
	if st.copyPrev == nil {
		s.copyHead = st.copyNext
	} else {
		st.copyPrev.copyNext = st.copyNext
	}
	if st.copyNext != nil {
		st.copyNext.copyPrev = st.copyPrev
	}
	st.copyNext, st.copyPrev = nil, nil
}

// failAllStreams gives every stream the session error (close).
func (s *Session) failAllStreams() {
	s.wakeStreams()
}

// releaseAll drops every buffer of the session (CLOSED).
func (s *Session) releaseAll() {
	// Each stream leaves the scheduler and the control list through their
	// own exits, so its links never claim a membership the lists dropped.
	drop := func(st *Stream) {
		if st.hasR {
			st.rcv.buf.clear()
		}
		if st.hasS {
			st.snd.q.drop()
		}
		s.sched.remove(st)
		s.unlinkCtl(st)
		s.unlinkCopyMSD(st)
		st.snd.unsentC = 0
	}
	for _, st := range s.streams {
		drop(st)
	}
	for _, st := range s.provisional {
		drop(st)
	}
	s.conn.buffered, s.conn.sendHeld, s.conn.smallHeld, s.conn.bigCount = 0, 0, 0, 0
	s.conn.pins = pinSet{}
	s.sched = scheduler{}
	s.ctlHead, s.ctlTail, s.copyHead = nil, nil, nil
	s.unsent = 0
}

// streamFIFO is a FIFO of streams.
type streamFIFO struct {
	q    []*Stream
	head int
}

func (f *streamFIFO) push(st *Stream) { f.q = append(f.q, st) }

func (f *streamFIFO) pop() *Stream {
	for f.head < len(f.q) {
		st := f.q[f.head]
		f.q[f.head] = nil
		f.head++
		if f.head == len(f.q) {
			f.q, f.head = f.q[:0], 0
		}
		if st != nil {
			return st
		}
	}
	return nil
}

func (f *streamFIFO) len() int { return len(f.q) - f.head }

// localWindow is the initial receive window the local parameters give a
// stream's receiving part: what the peer is told when the stream opens.
func (s *Session) localWindow(st *Stream) uint64 {
	l := &s.cfg.Local
	switch {
	case !st.bidi:
		return l.MaxStreamDataUni
	case st.local:
		return l.MaxStreamDataBidiLoc
	default:
		return l.MaxStreamDataBidiRem
	}
}

// ReceiveWindow returns a receiving part's window settings as they were set:
// the initial window (SetReceiveWindow, else the session's), never the window
// auto-tuning has grown it to, and the cap (SetReceiveWindow, else the
// session's StreamWindowCap; 0: automatic).
func (s *Session) ReceiveWindow(st *Stream) (initial, limit uint64) {
	r := &st.rcv
	initial = r.initWin
	if initial == 0 {
		initial = s.localWindow(st)
	}
	limit = r.maxWin
	if !r.maxSet {
		limit = s.cfg.StreamWindowCap
	}
	return initial, limit
}

// MinStreamWindow is the smallest initial stream receive window: the floor of
// the configured one, and of what SetReceiveWindow takes.
const MinStreamWindow = 4096

// SetReceiveWindow sets a receiving part's window: initial is advertised at
// once when it is above the window in force, limit caps auto-tuning (0: no
// cap but the session's memory allowance, whatever the session's configured
// cap is). An initial below 4 KiB (the floor of the configured initial
// window) or a nonzero limit below initial is ErrInvalid and changes nothing.
// Credit already advertised is never withdrawn: an initial below the window in
// force changes only what ReceiveWindow reads back.
func (s *Session) SetReceiveWindow(st *Stream, initial, limit uint64) error {
	if initial < MinStreamWindow || (limit != 0 && limit < initial) {
		return ErrInvalid
	}
	r := &st.rcv
	w := initial
	r.initWin = w
	r.maxSet = true
	r.maxWin = limit
	if w > r.window {
		r.window = w
		s.queueCtl(st, ctlMaxStreamData)
		s.wakeAllWriters()
	}
	return nil
}

// ReadChunk consumes up to max contiguous bytes without copying. buf, when not
// nil, holds a reference the caller releases (Unref) once it no longer needs
// data.
func (s *Session) ReadChunk(st *Stream, limit int) (data []byte, buf *Buf, fin bool, err error) {
	if !st.hasR {
		return nil, nil, false, ErrInvalid
	}
	r := &st.rcv
	if r.stopped {
		return nil, nil, false, &StreamError{Kind: StreamClosedLocally, StreamID: st.id, Code: r.stopCode}
	}
	if r.state == recvDataRead {
		return nil, nil, true, nil
	}
	if r.reset && r.readOff+uint64(limit) > r.resetRel {
		limit = int(r.resetRel - min64(r.resetRel, r.readOff))
	}
	data, buf = r.buf.take(r.readOff, limit)
	if n := uint64(len(data)); n > 0 {
		r.readOff += n
		s.stats.AppBytesRead += n
		st.cnt.appRead += n
		s.bufSub(n)
		s.streamCreditChanged(st)
		s.tuneConsume()
		if r.drainWait {
			s.drainRead(st)
		}
		return data, buf, false, nil
	}
	if r.reset && r.readOff >= r.resetRel {
		r.state = recvResetRead
		s.maybeDiscard(st)
		s.drainRead(st)
		return nil, nil, false, r.errState
	}
	if r.finKnown && r.readOff == r.final && !r.reset {
		r.state = recvDataRead
		s.maybeDiscard(st)
		s.drainRead(st)
		return nil, nil, true, nil
	}
	if s.err != nil {
		return nil, nil, false, s.err
	}
	return nil, nil, false, nil
}

// ReadReady reports the bytes a read would return now and whether, with
// none, the next read returns the part's end (FIN, reset, stop or the
// session's error) at once.
func (s *Session) ReadReady(st *Stream) (uint64, bool) {
	if !st.hasR {
		return 0, true
	}
	r := &st.rcv
	n := r.buf.contiguous(r.readOff) - r.readOff
	if r.reset {
		n = min64(n, r.resetRel-min64(r.resetRel, r.readOff))
	}
	if n > 0 {
		return n, false
	}
	end := r.stopped || r.state == recvDataRead || r.finKnown && r.readOff == r.final && !r.reset ||
		r.reset && r.readOff >= r.resetRel || s.err != nil
	return 0, end
}

// countDuplicate counts the stream bytes of [off, end) that were received
// already, for the session and the pipe of the record (multipath).
func (s *Session) countDuplicate(r *recvPart, off, end uint64) {
	dup := r.arrived.covered(off, end)
	if dup == 0 {
		return
	}
	s.stats.BytesDuplicateReceived += dup
	if p := s.rxPipe; p != nil && p.mp != nil {
		p.mp.dupBytes += dup
	}
}
