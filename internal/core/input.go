package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
)

var tpTypeBytes = [8]byte{0xff, 0x51, 0x53, 0x30, 0x0d, 0x0a, 0x0d, 0x0a}

// Receive processes the complete records at the start of data, read from an
// inbound lane, and returns how many bytes it consumed; the caller keeps the
// rest (a partial record) for the next call. buf owns data; pieces of stream
// data may keep a reference to it.
func (s *Session) Receive(pipe, lane uint64, buf *Buf, data []byte, now Time) int {
	s.SetNow(now)
	s.pickInput()
	p := s.pipe(pipe)
	if p == nil || p.in == nil {
		return len(data)
	}
	il := p.in.lane(lane)
	if il == nil || il.ended {
		return len(data)
	}
	if s.state == StateClosed {
		return len(data)
	}
	if s.state == StateClosing && !s.closeFr.waitLanes {
		// Lingering: discard without processing (SPEC.md 6.5 step 3).
		return len(data)
	}
	consumed := 0
	for consumed < len(data) && (s.state < StateClosing || s.closeFr.waitLanes) {
		rest := data[consumed:]
		if il.expectTP {
			if e := s.checkFirstRecord(rest); e != nil {
				s.failLocal(e)
				return len(data)
			}
		}
		size, n := wire.ReadVarint(rest)
		if n == 0 {
			break
		}
		limit := s.cfg.Local.MaxRecordSize
		if size == 0 || size > limit {
			s.failLocal(wire.Errorf(wire.FrameEncodingError, "record size", "value", size, "limit", limit,
				"pipe", pipe, "pos", il.pos))
			return len(data)
		}
		if uint64(len(rest)-n) < size {
			break
		}
		rec := rest[n : n+int(size)]
		consumed += n + int(size)
		s.processRecord(p, il, buf, rec, uint64(n)+size)
	}
	return consumed
}

// ReceiveMessage processes one message of a message-framed carrier
// (SPEC.md 3.4): the message is one record's Frames field.
func (s *Session) ReceiveMessage(pipe, lane uint64, buf *Buf, msg []byte, now Time) {
	s.SetNow(now)
	p := s.pipe(pipe)
	if p == nil || p.in == nil || s.state >= StateClosing {
		return
	}
	il := p.in.lane(lane)
	if il == nil || il.ended {
		return
	}
	if uint64(len(msg)) > s.cfg.Local.MaxRecordSize {
		s.failLocal(wire.ErrFrame("message above max_record_size"))
		return
	}
	if il.expectTP {
		if len(msg) < 9 || [8]byte(msg[:8]) != tpTypeBytes {
			s.failLocal(wire.ErrProto("first message is not QX_TRANSPORT_PARAMETERS"))
			return
		}
	}
	posBytes := uint64(wire.VarintLen(uint64(len(msg))) + len(msg))
	s.processRecord(p, il, buf, msg, posBytes)
}

// checkFirstRecord validates the prefix of the first record of a bootstrap
// pipe byte by byte (SPEC.md 6.2).
func (s *Session) checkFirstRecord(b []byte) *wire.Error {
	if len(b) == 0 {
		return nil
	}
	size, n := wire.ReadVarint(b)
	if n == 0 {
		// Size incomplete: its first byte alone cannot reject except an
		// impossible length class; wait.
		return nil
	}
	if size < 9 || size > s.cfg.Local.MaxRecordSize {
		return wire.Errorf(wire.ProtocolViolation, "not a zmux or QMux peer", "value", size)
	}
	for i := 0; i < 8 && n+i < len(b); i++ {
		if b[n+i] != tpTypeBytes[i] {
			return wire.ErrProto("not a zmux or QMux peer")
		}
	}
	return nil
}

// recordFacts is what one processed record carried.
type recordFacts struct {
	elicit   bool
	activity bool
	onlyPad  bool
	pipePing bool
	traffic  bool // a traffic frame (MULTIPATH.md 7.1): activity without QX_PING
}

// processRecord applies every frame of one complete record, then counts it
// into the lane's position (MULTIPATH.md 6.1).
func (s *Session) processRecord(p *Pipe, il *inLane, buf *Buf, rec []byte, posBytes uint64) {
	s.rxPipe = p
	if a := p.mp; a != nil && a.discardTP && il.lane == 0 {
		a.discardTP = false
		if len(rec) >= 8 && [8]byte(rec[:8]) == tpTypeBytes {
			// A shared listener's parameters leading a join carrier:
			// discarded, not counted (MULTIPATH.md 5.7 step 3).
			return
		}
	}
	first := il.expectTP
	facts := recordFacts{onlyPad: true}
	var f wire.Frame
	idx := 0
	for len(rec) > 0 {
		n, e := wire.DecodeFrame(rec, &f)
		if e == nil && f.Kind == wire.KindTransportParams && !(first && idx == 0) {
			var tp wire.Params
			if pe := wire.DecodeParams(f.Data, &tp); pe != nil {
				e = pe
			} else {
				e = wire.ErrProto("QX_TRANSPORT_PARAMETERS out of place")
			}
		}
		if e != nil {
			e.FrameType = f.Type
			s.failLocal(e)
			return
		}
		if il.skipJoin && idx == 0 && f.Kind == wire.KindPipeJoin {
			il.skipJoin = false
			facts.onlyPad = false
			facts.elicit = true
			rec = rec[n:]
			idx++
			continue
		}
		if il.expectLane && idx == 0 {
			if f.Kind != wire.KindPipeLane {
				s.failLocal(wire.ErrProto("lane does not start with PIPE_LANE"))
				return
			}
			facts.onlyPad = false
			facts.elicit = true
			rec = rec[n:]
			idx++
			continue
		}
		if il.expectLane && f.Kind != wire.KindPadding {
			s.failLocal(wire.ErrProto("a lane's first record holds a frame other than PIPE_LANE"))
			return
		}
		if first && idx == 0 {
			if f.Kind != wire.KindTransportParams {
				s.failLocal(wire.ErrProto("first frame is not QX_TRANSPORT_PARAMETERS"))
				return
			}
			var tp wire.Params
			if pe := wire.DecodeParams(f.Data, &tp); pe != nil {
				s.failLocal(pe)
				return
			}
			il.expectTP = false
			if pe := s.processPeerParams(&tp); pe != nil {
				s.failLocal(pe)
				return
			}
			facts.onlyPad = false
			facts.elicit = true
			rec = rec[n:]
			idx++
			continue
		}
		if !s.peerSeen {
			s.failLocal(wire.ErrProto("frame before transport parameters"))
			return
		}
		if ge := wire.CheckGate(&f, &s.gates); ge != nil {
			s.failLocal(ge)
			return
		}
		if f.Kind != wire.KindPadding {
			facts.onlyPad = false
		}
		if f.Kind.AckEliciting() {
			facts.elicit = true
		}
		switch f.Kind {
		case wire.KindStream, wire.KindResetStream, wire.KindResetStreamAt, wire.KindStopSending,
			wire.KindDatagram, wire.KindPing:
			facts.activity = true
			facts.traffic = facts.traffic || f.Kind != wire.KindPing
		case wire.KindPipePing:
			facts.pipePing = true
		}
		if s.state >= StateClosing {
			// Waiting for lanes before CONNECTION_CLOSE: only
			// acknowledgements and the peer's close still matter.
			if f.Kind == wire.KindPipeAck {
				_ = s.ack.onPipeAck(&f, s.now)
			} else if f.Kind == wire.KindClose {
				s.closeFr.waitLanes = false
			}
			rec = rec[n:]
			idx++
			continue
		}
		if pe := s.dispatch(p, il, &f, buf); pe != nil {
			s.failLocal(pe)
			return
		}
		rec = rec[n:]
		idx++
	}
	il.expectLane = false
	if facts.onlyPad && s.peerSeen {
		s.charge()
	}
	il.pos += posBytes
	il.recs++
	il.posAt = s.now
	s.stats.BytesReceived += posBytes
	s.stats.RecordsReceived++
	if s.ack != nil {
		s.ack.onRecord(p, il, posBytes, facts, s.now)
	}
	if a := p.mp; a != nil {
		if facts.traffic {
			s.markBusy()
		}
		if g := s.now.Sub(a.lastRecord); il.recs > 1 && g > a.maxGap {
			a.maxGap = g
		}
		a.lastRecord = s.now
		if a.in == inJoining {
			a.in = inOpen
			if a.out == outJoining {
				a.out = outActive
			}
			s.joinConfirmed(p)
		}
	}
	if !s.feat.Acked() || facts.activity {
		s.timers.lastActivity = s.now
	}
}

// dispatch applies one decoded frame.
func (s *Session) dispatch(p *Pipe, il *inLane, f *wire.Frame, buf *Buf) *wire.Error {
	switch f.Kind {
	case wire.KindPadding:
	case wire.KindStream:
		eff, e := s.onStream(f, buf)
		if e != nil {
			return e
		}
		if !eff && !s.feat.Multipath {
			s.charge()
		}
	case wire.KindResetStream, wire.KindResetStreamAt:
		eff, e := s.onReset(f, f.Kind == wire.KindResetStreamAt)
		if e != nil {
			return e
		}
		if !eff && !s.feat.Multipath {
			s.charge()
		}
	case wire.KindStopSending:
		st, e := s.lookupStream(f.StreamID, isBidiID(f.StreamID))
		if e != nil {
			return e
		}
		if st == nil {
			return nil
		}
		if !st.hasS {
			return wire.Errorf(wire.StreamStateError, "STOP_SENDING for a receive-only stream", "stream", st.id)
		}
		if st.snd.stopped && !s.feat.Multipath && st.snd.state != sendDataRecvd && st.snd.state != sendResetRecvd {
			s.charge()
		}
		s.onStopSending(st, f.Code)
	case wire.KindMaxData:
		if f.Value > s.conn.sendMax {
			if s.conn.sent >= s.conn.sendMax {
				s.conn.creditAt = s.now
			}
			s.conn.sendMax = f.Value
			s.wakeAllWriters()
			s.wakeCapacity()
		} else if !s.feat.Multipath {
			s.charge()
		}
	case wire.KindMaxStreamData:
		st, e := s.lookupStream(f.StreamID, isBidiID(f.StreamID))
		if e != nil {
			return e
		}
		if st == nil {
			return nil
		}
		if !st.hasS {
			return wire.Errorf(wire.StreamStateError, "MAX_STREAM_DATA for a receive-only stream", "stream", st.id)
		}
		if !s.onSendCredit(st, f.Value) && !s.feat.Multipath && st.snd.state < sendDataRecvd {
			s.charge()
		}
	case wire.KindMaxStreams:
		t := typeIndex(f.Bidi)
		if f.Value > s.loc[t].limit {
			s.loc[t].limit = f.Value
			s.wakeOpeners()
			s.wakeAllWriters()
		} else if !s.feat.Multipath {
			s.charge()
		}
	case wire.KindDataBlocked:
		if f.Value > s.conn.adv {
			return wire.Errorf(wire.ProtocolViolation, "DATA_BLOCKED above the advertised limit", "value", f.Value)
		}
		if f.Value <= s.conn.blockSeen && s.conn.blockSeen > 0 {
			if !s.feat.Multipath {
				s.charge()
			}
		}
		s.conn.blockSeen = max64(s.conn.blockSeen, f.Value)
		if t := s.conn.tune; t != nil {
			t.blockedAt(f.Value)
		}
		if f.Value == s.conn.adv {
			s.conn.peerBlock = true
			s.connCreditChanged()
		}
	case wire.KindStreamDataBlocked:
		st, e := s.lookupStream(f.StreamID, true)
		if e != nil {
			return e
		}
		if st == nil {
			return nil
		}
		if !st.hasR {
			return wire.Errorf(wire.StreamStateError, "STREAM_DATA_BLOCKED for a send-only stream", "stream", st.id)
		}
		if f.Value > st.rcv.adv {
			return wire.Errorf(wire.ProtocolViolation, "STREAM_DATA_BLOCKED above the advertised limit",
				"stream", st.id, "value", f.Value)
		}
		if f.Value <= st.rcv.blockSeen && st.rcv.blockSeen > 0 && !s.feat.Multipath &&
			st.rcv.state == recvRecv {
			s.charge()
		}
		st.rcv.blockSeen = max64(st.rcv.blockSeen, f.Value)
		if t := st.rcv.tune; t != nil {
			t.blockedAt(f.Value)
		}
		s.streamCreditChanged(st)
	case wire.KindStreamsBlocked:
		t := typeIndex(f.Bidi)
		if f.Value > s.rem[t].limit {
			return wire.Errorf(wire.ProtocolViolation, "STREAMS_BLOCKED above the advertised limit", "value", f.Value)
		}
		if f.Value <= s.rem[t].blockedSeen && s.rem[t].blockedSeen > 0 && !s.feat.Multipath {
			s.charge()
		}
		s.rem[t].blockedSeen = max64(s.rem[t].blockedSeen, f.Value)
	case wire.KindClose:
		s.onClose(f)
	case wire.KindDatagram:
		return s.onDatagram(f)
	case wire.KindPing:
		return s.onPing(f, p)
	case wire.KindGoAway:
		s.onGoAway(f)
	case wire.KindPriorityUpdate:
		return s.onPriorityUpdate(f)
	case wire.KindPipeJoin:
		return wire.ErrProto("PIPE_JOIN on a pipe of the session")
	case wire.KindPipeAck:
		return s.ack.onPipeAck(f, s.now)
	case wire.KindPipeLane:
		return wire.ErrProto("PIPE_LANE out of place")
	case wire.KindPipePing, wire.KindPipeAbandon, wire.KindPipeStatus:
		return s.onPipeFrame(p, f)
	}
	return nil
}

// onClose processes a received CONNECTION_CLOSE (SPEC.md 6.5).
func (s *Session) onClose(f *wire.Frame) {
	e := &SessionError{Remote: true, Code: f.Code, FrameType: f.ErrFrame}
	reason := string(f.Data)
	switch {
	case f.App:
		e.Kind = CloseApplication
		e.Reason = reason
	case f.Code == wire.IdleTimeout:
		e.Kind = CloseIdle
	default:
		e.Kind = CloseTransport
		e.Reason, e.Details = splitDetails(reason)
	}
	s.closeWith(e, &closeFrame{code: wire.NoError})
}

// splitDetails separates the diagnostic line (SPEC.md 4.3).
func splitDetails(r string) (string, string) {
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] == '\n' {
			return r[:i], r[i+1:]
		}
	}
	return r, ""
}

// PipeEnded reports the end of an inbound lane: graceful (eof) or failed.
// The driver calls it after feeding every complete record it read.
func (s *Session) PipeEnded(pipe, lane uint64, graceful bool, partial bool, now Time) {
	s.SetNow(now)
	p := s.pipe(pipe)
	if p == nil || p.in == nil {
		return
	}
	il := p.in.lane(lane)
	if il == nil || il.ended {
		return
	}
	il.ended = true
	// A lane that ends before its second record is no event (MULTIPATH.md 5.8).
	early := il.lane > 0 && il.recs <= 1
	allEnded := true
	for _, l := range p.in.lanes {
		if !l.ended {
			allEnded = false
		}
	}
	if allEnded {
		p.in.ended = true
	}
	if s.state >= StateClosing {
		s.checkLingerDone()
		return
	}
	failed := (!graceful || partial) && !early
	if s.feat.Multipath {
		if failed || allEnded {
			s.mpInboundEnded(p, il, !failed)
		}
		return
	}
	if failed {
		s.closeWith(&SessionError{Kind: ClosePipesLost}, nil)
		return
	}
	if !allEnded {
		return
	}
	if s.peerSeen && !s.feat.Acked() {
		// The peer's idle close in a session without acknowledgement.
		s.closeSilently(&SessionError{Kind: CloseIdle, Remote: true}, true)
		return
	}
	s.closeWith(&SessionError{Kind: ClosePipesLost}, nil)
}

// PipeWriteFailed reports a failed write on an outbound lane.
func (s *Session) pipeWriteFailed(pipe, lane uint64, err error) {
	if s.state >= StateClosing {
		if s.state == StateClosing {
			if p := s.pipe(pipe); p != nil && p.out != nil {
				p.out.failed = true
			}
			s.checkLingerDone()
		}
		return
	}
	if lane > 0 {
		if p := s.pipe(pipe); p != nil && p.out != nil {
			if ol := p.out.lane(lane); ol != nil && !ol.confirmed {
				s.laneRefused(p, ol)
				return
			}
		}
	}
	if s.feat.Multipath {
		s.mpOutboundFailed(pipe, err)
		return
	}
	s.closeWith(&SessionError{Kind: ClosePipesLost, Cause: err}, nil)
}
