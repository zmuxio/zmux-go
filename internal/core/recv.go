package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
)

// lookupStream resolves the stream a frame references, opening peer streams
// implicitly (SPEC.md 7.2 rules 2-4). It returns nil without error for a
// discarded stream (crossing frame, SPEC.md 7.6). opens is whether the
// frame may open a peer stream of this direction.
func (s *Session) lookupStream(id uint64, opens bool) (*Stream, *wire.Error) {
	if st := s.streams[id]; st != nil {
		return st, nil
	}
	idx := id >> 2
	t := typeIndex(isBidiID(id))
	if isLocalID(id, s.role) {
		if idx >= s.loc[t].next {
			return nil, wire.Errorf(wire.StreamStateError, "frame for an unopened local stream", "stream", id)
		}
		return nil, nil
	}
	rc := &s.rem[t]
	if idx < rc.opened {
		return nil, nil
	}
	if !opens {
		return nil, wire.Errorf(wire.StreamStateError, "frame in the wrong direction", "stream", id)
	}
	if idx >= rc.limit {
		return nil, wire.Errorf(wire.StreamLimitError, "stream opened beyond the limit",
			"stream", id, "limit", rc.limit)
	}
	var last *Stream
	for i := rc.opened; i <= idx; i++ {
		nid := streamID(i, t == 0, s.role == RoleInitiator)
		st := s.newStream(t == 0, false)
		st.id, st.idSet, st.opened = nid, true, true
		st.urgency, st.incr = 3, true
		s.streams[nid] = st
		st.applyPeerParams()
		s.stats.StreamsOpenedRemote++
		if s.ga.sent && i >= s.ga.sentWater[t] || s.underPressure() {
			s.stackRejectStream(st)
		} else if !s.feat.Preamble {
			s.offer(st)
		}
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
// without offering it (EXTENSIONS.md 3.4, 7.2).
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

// CancelAcceptWait removes sig.
func (s *Session) CancelAcceptWait(bidi bool, sig *Signal) {
	t := typeIndex(bidi)
	s.acceptW[t] = removeSig(s.acceptW[t], sig)
	if len(s.acceptW[t]) > 0 && s.acceptQ[t].len() > 0 {
		s.wake(s.acceptW[t][0])
	}
}

// unstageUni raises staged unidirectional credit on the first accept
// (SPEC.md 8.5 rule 4).
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
		return false, wire.Errorf(wire.StreamStateError, "STREAM on a send-only stream", "stream", st.id)
	}
	r := &st.rcv
	off := f.Offset
	end := off + uint64(len(f.Data))
	if r.finKnown && (end > r.final || f.Fin && end != r.final) {
		return false, wire.Errorf(wire.FinalSizeError, "data beyond the final size", "stream", st.id,
			"offset", off, "length", len(f.Data), "limit", r.final)
	}
	if f.Fin && end < r.highest {
		return false, wire.Errorf(wire.FinalSizeError, "final size below received data", "stream", st.id)
	}
	if end > r.adv {
		return false, wire.Errorf(wire.FlowControlError, "stream data beyond the limit", "stream", st.id,
			"offset", off, "length", len(f.Data), "limit", r.adv)
	}
	if !s.feat.Multipath && off != r.highest && !(r.reset && off >= r.highest) {
		return false, wire.Errorf(wire.ProtocolViolation, "out-of-order stream data", "stream", st.id,
			"offset", off, "length", len(f.Data))
	}
	top := r.highest
	if r.finKnown {
		top = max64(top, r.final)
	}
	if end > top {
		s.conn.recv += end - top
		if s.conn.recv > s.conn.adv {
			return false, wire.Errorf(wire.FlowControlError, "session data beyond the limit", "stream", st.id,
				"limit", s.conn.adv)
		}
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
		s.tuneData(st, end)
	}
	// Preamble of a peer stream (EXTENSIONS.md 4.4).
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
		// buffer through the one accounting path (C1).
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
		if f.Fin && end == uint64(pre.Size) {
			// nothing else
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
			if nb > 0 {
				effect = true
			}
			if r.stopped || st.stackReject {
				s.releaseUnbuffered(nb)
			} else if nb > 0 {
				part := data[lo-off : hi-off]
				stored := r.buf.insert(lo, part, buf)
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
		return false, wire.Errorf(wire.ProtocolViolation, "opener's sending part has final size 0", "stream", st.id)
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

// checkPreambleReset enforces EXTENSIONS.md 4.4 rule 2 once both the
// preamble size and a reset are known.
func (s *Session) checkPreambleReset(st *Stream) *wire.Error {
	r := &st.rcv
	if st.local || !s.feat.Preamble || r.stopSent || r.stopped || !r.reset {
		return nil
	}
	if r.preDone && r.resetRel < r.preSize {
		return wire.Errorf(wire.ProtocolViolation, "reset below the preamble", "stream", st.id)
	}
	return nil
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
	}
	s.wake(st.rsig)
	s.streamCreditChanged(st)
}

// onReset processes RESET_STREAM (at=false) or RESET_STREAM_AT.
func (s *Session) onReset(f *wire.Frame, at bool) (bool, *wire.Error) {
	st, err := s.lookupStream(f.StreamID, true)
	if err != nil || st == nil {
		return st == nil && err == nil, err
	}
	if !st.hasR {
		return false, wire.Errorf(wire.StreamStateError, "reset of a send-only stream", "stream", st.id)
	}
	r := &st.rcv
	rel := uint64(0)
	if at {
		rel = f.Reliable
	}
	if r.finKnown && f.FinalSize != r.final || f.FinalSize < r.highest {
		return false, wire.Errorf(wire.FinalSizeError, "final size changed", "stream", st.id)
	}
	if f.FinalSize > r.adv {
		return false, wire.Errorf(wire.FlowControlError, "final size beyond the limit", "stream", st.id)
	}
	if r.reset && f.Code != r.resetCode {
		return false, wire.Errorf(wire.StreamStateError, "reset changes the error code", "stream", st.id)
	}
	top := r.highest
	if r.finKnown {
		top = max64(top, r.final)
	}
	if f.FinalSize > top {
		s.conn.recv += f.FinalSize - top
		if s.conn.recv > s.conn.adv {
			return false, wire.Errorf(wire.FlowControlError, "session data beyond the limit", "stream", st.id)
		}
	}
	if !st.local && s.feat.Preamble && !r.stopSent && !r.stopped {
		if !at || f.FinalSize == 0 {
			return false, wire.Errorf(wire.ProtocolViolation, "opener reset without the preamble", "stream", st.id)
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
	// Release the gap the reset makes undeliverable (SPEC.md 8.3).
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
// caller must wait (WaitRead).
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
		s.bufSub(uint64(n))
		s.streamCreditChanged(st)
		s.tuneConsume(st)
	}
	if r.reset && r.readOff >= r.resetRel && r.buf.contiguous(r.readOff) == r.readOff {
		if n > 0 {
			return n, false, nil
		}
		r.state = recvResetRead
		s.maybeDiscard(st)
		return 0, false, r.errState
	}
	if r.finKnown && r.readOff == r.final && !r.reset {
		r.state = recvDataRead
		s.maybeDiscard(st)
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

// WaitRead registers sig for read progress.
func (s *Session) WaitRead(st *Stream, sig *Signal) { st.rsig = sig }

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
	if d := r.buf.clear(); d > 0 {
		s.bufSub(d)
	}
	if r.finKnown {
		r.readOff = r.final
	} else {
		r.readOff = r.highest
	}
	if r.state == recvRecv || r.state == recvSizeKnown {
		s.queueCtl(st, ctlStop)
	}
	s.recvProgress(st)
}

// Release drops every handle: an unfinished sending part is reset and an
// unread receiving part stopped, both with code 0 (EXTENSIONS.md 7.4).
func (s *Session) Release(st *Stream) {
	if st.hasS && !st.snd.finWr && !st.snd.reset {
		s.Reset(st, 0)
	}
	st.releasedS = true
	if st.hasR && st.rcv.state != recvDataRead && st.rcv.state != recvResetRead {
		s.Stop(st, 0)
	}
	st.releasedR = true
	s.maybeDiscard(st)
}

// Reject declines an accepted stream before reading (EXTENSIONS.md 7.2).
func (s *Session) Reject(st *Stream) {
	if st.hasS && !st.snd.reset {
		st.snd.errState = &StreamError{Kind: StreamRejected, StreamID: st.id}
		s.startReset(st, wire.CodeRejected, false)
	}
	st.releasedS = true
	if st.hasR {
		st.releasedR = true
		s.stopRecv(st, wire.CodeRejected)
	}
	s.maybeDiscard(st)
}

// maybeDiscard discards the stream's state once both parts are terminal and
// the application released it (SPEC.md 7.6, 8.5).
func (s *Session) maybeDiscard(st *Stream) {
	if st.discarded || !st.idSet {
		return
	}
	sendDone := !st.hasS || st.snd.state == sendDataRecvd || st.snd.state == sendResetRecvd
	recvDone := !st.hasR || st.rcv.state == recvDataRead || st.rcv.state == recvResetRead
	released := st.releasedS && st.releasedR || st.stackReject
	if !st.local && !st.offered && !st.stackReject {
		released = false
	}
	if !sendDone || !recvDone || !released {
		return
	}
	st.discarded = true
	delete(s.streams, st.id)
	s.sched.remove(st)
	s.unlinkCtl(st)
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
	if st.dg != nil {
		s.dropStreamDgrams(st)
	}
}

func (s *Session) unlinkCtl(st *Stream) {
	if !st.inCtl {
		return
	}
	st.ctl = 0
	st.inCtl = false
	var prev *Stream
	for x := s.ctlHead; x != nil; x = x.ctlNext {
		if x == st {
			if prev == nil {
				s.ctlHead = x.ctlNext
			} else {
				prev.ctlNext = x.ctlNext
			}
			if s.ctlTail == x {
				s.ctlTail = prev
			}
			x.ctlNext = nil
			return
		}
		prev = x
	}
}

// failAllStreams gives every stream the session error (close).
func (s *Session) failAllStreams() {
	for _, st := range s.streams {
		s.wake(st.rsig)
		s.wake(st.wsig)
	}
	s.wakeCapacity()
}

// releaseAll drops every buffer of the session (CLOSED).
func (s *Session) releaseAll() {
	for _, st := range s.streams {
		if st.hasR {
			st.rcv.buf.clear()
		}
		if st.hasS {
			st.snd.q.drop()
		}
	}
	for _, st := range s.provisional {
		st.snd.q.drop()
	}
	s.conn.buffered, s.conn.sendHeld, s.conn.smallHeld, s.conn.bigCount = 0, 0, 0, 0
	s.sched = scheduler{}
	s.ctlHead, s.ctlTail = nil, nil
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

// SetReceiveWindow sets a receiving part's window: initial raises the
// advertised limit at once, max caps auto-tuning and the unread bytes
// (IMPLEMENTATION.md 6.4). Values below 16 KiB are raised to 16 KiB; 0
// leaves a value as it is.
func (s *Session) SetReceiveWindow(st *Stream, initial, max uint64) {
	r := &st.rcv
	if max > 0 {
		r.maxWin = max64(max, 16<<10)
	}
	if initial > 0 {
		w := max64(initial, 16<<10)
		if r.maxWin > 0 {
			w = min64(w, r.maxWin)
		}
		if w > r.window {
			r.window = w
			s.queueCtl(st, ctlMaxStreamData)
			s.wakeAllWriters()
		}
	}
}

// ReadChunk consumes up to max contiguous bytes without copying (API.md
// 5.1). buf, when not nil, holds a reference the caller releases (Unref)
// once it no longer needs data.
func (s *Session) ReadChunk(st *Stream, max int) (data []byte, buf *Buf, fin bool, err error) {
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
	if r.reset && r.readOff+uint64(max) > r.resetRel {
		max = int(r.resetRel - min64(r.resetRel, r.readOff))
	}
	data, buf = r.buf.take(r.readOff, max)
	if n := uint64(len(data)); n > 0 {
		r.readOff += n
		s.stats.AppBytesRead += n
		s.bufSub(n)
		s.streamCreditChanged(st)
		s.tuneConsume(st)
		return data, buf, false, nil
	}
	if r.reset && r.readOff >= r.resetRel {
		r.state = recvResetRead
		s.maybeDiscard(st)
		return nil, nil, false, r.errState
	}
	if r.finKnown && r.readOff == r.final && !r.reset {
		r.state = recvDataRead
		s.maybeDiscard(st)
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
