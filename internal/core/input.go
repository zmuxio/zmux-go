package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

var tpTypeBytes = [8]byte{0xff, 0x51, 0x53, 0x30, 0x0d, 0x0a, 0x0d, 0x0a}

// Receive processes complete byte-framed records and returns the number
// of bytes consumed. The caller retains the unconsumed ranges.
func (s *Session) Receive(pipe, lane uint64, input Input, now Time) int {
	return s.ReceiveTracked(pipe, lane, input, now, nil)
}

// RecordValidation identifies the disposition of a complete framed record.
type RecordValidation uint8

const (
	RecordUnclassified RecordValidation = iota
	RecordValid
	RecordProtocolInvalid
)

// RecordValidations holds the classifications made by the protocol owner.
type RecordValidations struct {
	Valid, ProtocolInvalid uint64
}

func (v *RecordValidations) add(result RecordValidation) {
	switch result {
	case RecordValid:
		v.Valid++
	case RecordProtocolInvalid:
		v.ProtocolInvalid++
	case RecordUnclassified:
	}
}

// ReceiveTracked also observes its complete records when validation is enabled.
func (s *Session) ReceiveTracked(pipe, lane uint64, input Input, now Time, validation *RecordValidations) int {
	s.SetNow(now)
	s.pickInput()
	p, il := s.receiveLane(pipe, lane)
	total := input.Len()
	if il == nil {
		return total
	}
	consumed := 0
	for consumed < total {
		mode := s.inputMode()
		if mode == inputDrop || mode == inputScan && (lane != 0 || p.in.ccRead || il.noScan) {
			return total
		}
		rest := input.Slice(consumed, total)
		data := rest.Bytes()
		if il.expectTP && mode == inputProcess {
			if e := s.checkFirstRecord(data); e != nil {
				if validation != nil {
					size, n := data.Varint()
					if n != 0 && size > 0 && size <= s.cfg.Local.MaxRecordSize && size <= uint64(data.Len()-n) {
						validation.add(RecordProtocolInvalid)
					}
				}
				s.failLocal(e)
				return total
			}
		}
		size, n := data.Varint()
		if n == 0 {
			break
		}
		limit := s.cfg.Local.MaxRecordSize
		if size == 0 || size > limit {
			if mode == inputScan {
				il.noScan = true
			} else {
				s.failLocal(wire.NewError(wire.FrameEncodingError, "record size", "value", size, "limit", limit,
					"pipe", pipe, "pos", il.pos))
			}
			return total
		}
		if size > uint64(rest.Len()-n) {
			break
		}
		end := n + int(size)
		rec := rest.Slice(n, end)
		consumed += end
		if mode == inputScan {
			if recordHasClose(rec.Bytes()) {
				s.peerCloseRead(p)
				return total
			}
		} else {
			result := s.processRecord(p, il, rec, uint64(end))
			if validation != nil {
				validation.add(result)
			}
		}
	}
	return consumed
}

func (s *Session) receiveLane(pipe, lane uint64) (*Pipe, *inLane) {
	p := s.pipe(pipe)
	if p == nil || p.in == nil || p.mp != nil && p.mp.hs {
		return nil, nil
	}
	il := p.in.lane(lane)
	if il == nil || il.ended {
		return nil, nil
	}
	return p, il
}

// inputMode is how input is taken now, the one rule of both entries
// (Receive, ReceiveMessage): processed until CLOSING, and while a graceful
// close waits for its lanes (PIPE_ACK and the peer's CONNECTION_CLOSE
// still count); scanned for the peer's CONNECTION_CLOSE while lingering;
// dropped once CLOSED.
func (s *Session) inputMode() inputMode {
	switch {
	case s.state == StateClosed:
		return inputDrop
	case s.state == StateClosing && !s.closeFr.waitLanes:
		return inputScan
	}
	return inputProcess
}

type inputMode uint8

const (
	inputProcess inputMode = iota
	inputScan
	inputDrop
)

// ReceiveMessage processes one message of a message-framed carrier:
// the message is one record's Frames field.
func (s *Session) ReceiveMessage(pipe, lane uint64, input Span, now Time) RecordValidation {
	s.SetNow(now)
	s.pickInput()
	p, il := s.receiveLane(pipe, lane)
	mode := s.inputMode()
	if il == nil || mode == inputDrop {
		return RecordUnclassified
	}
	msg := input.Bytes()
	if mode == inputScan {
		if lane == 0 && !p.in.ccRead && !il.noScan && recordHasClose(wire.Parts{Head: msg}) {
			s.peerCloseRead(p)
		}
		return RecordUnclassified
	}
	if uint64(len(msg)) > s.cfg.Local.MaxRecordSize {
		s.failLocal(wire.ErrFrame("message above max_record_size"))
		return RecordProtocolInvalid
	}
	if il.expectTP && (len(msg) < 9 || [8]byte(msg[:8]) != tpTypeBytes) {
		s.failLocal(wire.ErrProto("first message is not QX_TRANSPORT_PARAMETERS"))
		return RecordProtocolInvalid
	}
	posBytes := uint64(varint.SizeVarint(uint64(len(msg))) + len(msg))
	return s.processRecord(p, il, Input{First: input}, posBytes)
}

// checkFirstRecord validates the prefix of the first record of a bootstrap
// pipe byte by byte.
func (s *Session) checkFirstRecord(b wire.Parts) *wire.Error {
	// Only the record Size and frame type are needed to classify it.
	const prefixSize = 16
	var scratch [prefixSize]byte
	prefix := b.Head
	if len(prefix) < prefixSize && len(prefix) < b.Len() {
		prefix = b.Slice(0, min(prefixSize, b.Len())).AppendTo(scratch[:0])
	}
	size, n, fits := wire.FirstRecordSize(prefix, s.cfg.Local.MaxRecordSize)
	if n == 0 {
		return nil
	}
	if !fits {
		return wire.NewError(wire.ProtocolViolation, "not a zmux or QMux peer", "value", size)
	}
	for i := 0; i < 8 && n+i < len(prefix); i++ {
		if prefix[n+i] != tpTypeBytes[i] {
			return wire.ErrProto("not a zmux or QMux peer")
		}
	}
	return nil
}

// recordFacts is what one processed record carried.
type recordFacts struct {
	elicit   bool
	other    bool // an ack-eliciting frame other than PIPE_PING
	activity bool
	onlyPad  bool
	pipePing bool
	traffic  bool // a traffic frame: activity without QX_PING
}

// processRecord applies every frame of one complete record, then counts it
// into the lane's position.
func (s *Session) processRecord(p *Pipe, il *inLane, rec Input, posBytes uint64) RecordValidation {
	s.rxPipe = p
	first := il.expectTP
	facts := recordFacts{onlyPad: true}
	var f wire.Frame
	idx := 0
	for rec.Len() > 0 {
		n, e := wire.DecodeFrameParts(rec.Bytes(), &f)
		if e == nil && f.Kind == wire.KindTransportParams && (!first || idx != 0) {
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
			return RecordProtocolInvalid
		}
		if il.expectLane && idx == 0 {
			if f.Kind != wire.KindPipeLane {
				s.failLocal(wire.ErrProto("lane does not start with PIPE_LANE"))
				return RecordProtocolInvalid
			}
			facts.onlyPad = false
			facts.elicit, facts.other = true, true
			rec = rec.Slice(n, rec.Len())
			idx++
			continue
		}
		if il.expectLane && f.Kind != wire.KindPadding {
			s.failLocal(wire.ErrProto("a lane's first record holds a frame other than PIPE_LANE"))
			return RecordProtocolInvalid
		}
		if first && idx == 0 {
			if f.Kind != wire.KindTransportParams {
				s.failLocal(wire.ErrProto("first frame is not QX_TRANSPORT_PARAMETERS"))
				return RecordProtocolInvalid
			}
			var tp wire.Params
			if pe := wire.DecodeParams(f.Data, &tp); pe != nil {
				s.failLocal(pe)
				return RecordProtocolInvalid
			}
			il.expectTP = false
			if pe := s.processPeerParams(&tp); pe != nil {
				s.failLocal(pe)
				return RecordProtocolInvalid
			}
			facts.onlyPad = false
			facts.elicit, facts.other = true, true
			rec = rec.Slice(n, rec.Len())
			idx++
			continue
		}
		if !s.peerSeen {
			s.failLocal(wire.ErrProto("frame before transport parameters"))
			return RecordProtocolInvalid
		}
		if s.carrierEarlyIn {
			switch f.Kind {
			case wire.KindStream:
				if s.peerZmux {
					s.failLocal(wire.ErrProto("application data outside a plain QMux resumption context"))
					return RecordProtocolInvalid
				}
			case wire.KindDatagram, wire.KindDatagramAck:
				s.failLocal(wire.ErrProto("datagram in carrier early data"))
				return RecordProtocolInvalid
			default:
				// Control frames retain their normal checks.
			}
		}
		if ge := wire.CheckGate(&f, &s.gates); ge != nil {
			s.failLocal(ge)
			return RecordProtocolInvalid
		}
		if f.Kind != wire.KindPadding {
			facts.onlyPad = false
		}
		if f.Kind.AckEliciting() {
			facts.elicit = true
			facts.other = facts.other || f.Kind != wire.KindPipePing
		}
		switch f.Kind {
		case wire.KindStream, wire.KindResetStream, wire.KindResetStreamAt, wire.KindStopSending,
			wire.KindDatagram, wire.KindPing:
			facts.activity = true
			facts.traffic = facts.traffic || f.Kind != wire.KindPing
		case wire.KindPipePing:
			facts.pipePing = true
		case wire.KindPadding, wire.KindMaxData, wire.KindMaxStreamData, wire.KindMaxStreams,
			wire.KindDataBlocked, wire.KindStreamDataBlocked, wire.KindStreamsBlocked, wire.KindClose,
			wire.KindTransportParams, wire.KindGoAway, wire.KindPriorityUpdate,
			wire.KindPipeAck, wire.KindPipeLane, wire.KindPipeAbandon, wire.KindPipeStatus, wire.KindSessionEpoch, wire.KindDatagramAck:
			// Neither activity nor a PIPE_PING.
		}
		if s.state >= StateClosing && (!s.closeFr.waitLanes || !closeWaitFrame(f.Kind)) {
			// Closing: acknowledgements and the peer's close still matter
			// (the close waits for no more once the peer's arrives).
			if f.Kind == wire.KindPipeAck {
				_ = s.ack.onPipeAck(&f, s.now, p)
			}
			if f.Kind == wire.KindClose {
				s.endLaneWait()
				if il.lane == 0 {
					s.peerCloseRead(p)
				}
			}
			rec = rec.Slice(n, rec.Len())
			idx++
			continue
		}
		var payload Input
		if (f.Kind == wire.KindStream || f.Kind == wire.KindDatagram) && f.DataLen() > 0 {
			payload = rec.Slice(n-f.DataLen(), n)
		}
		if pe := s.dispatch(p, &f, payload); pe != nil {
			s.failLocal(pe)
			return RecordProtocolInvalid
		}
		rec = rec.Slice(n, rec.Len())
		idx++
	}
	il.expectLane = false
	il.pos += posBytes
	il.recs++
	if p.out == nil {
		s.observePeer(p, true) // A valid record confirms and proves an independent receive half.
	}
	il.posAt = s.now
	s.stats.BytesReceived += posBytes
	s.memberBytes(p, false, posBytes)
	s.stats.RecordsReceived++
	if s.ack != nil {
		s.ack.onRecord(p, posBytes, facts, s.now)
	}
	if facts.traffic {
		p.lastTraffic = s.now
	}
	if a := p.mp; a != nil {
		if facts.traffic {
			s.markBusy()
		}
		if g := s.now.Sub(a.lastRecord); il.recs > 1 && g > a.maxGap {
			a.maxGap = g
		}
		a.lastRecord = s.now
		if p.out == nil {
			a.statusHeld = false // a down attachment: its first record has arrived
		}
	}
	if !s.feat.Acked() || facts.activity {
		s.noteActivity(s.now)
	}
	return RecordValid
}

// closeWaitFrame reports a frame that is still applied while a graceful
// close waits for PIPE_ACK coverage (transportLive): the path and credit
// frames that work depends on. The peer's streams and datagrams are not
// taken any more.
func closeWaitFrame(k wire.Kind) bool {
	switch k {
	case wire.KindPipeAck, wire.KindPipePing, wire.KindPipeAbandon, wire.KindPipeStatus,
		wire.KindMaxData, wire.KindMaxStreamData:
		return true
	case wire.KindPadding, wire.KindStream, wire.KindResetStream, wire.KindResetStreamAt,
		wire.KindStopSending, wire.KindMaxStreams, wire.KindDataBlocked, wire.KindStreamDataBlocked,
		wire.KindStreamsBlocked, wire.KindClose, wire.KindDatagram, wire.KindTransportParams,
		wire.KindPing, wire.KindGoAway, wire.KindPriorityUpdate, wire.KindPipeLane, wire.KindSessionEpoch, wire.KindDatagramAck:
	}
	return false
}

// dispatch applies one decoded frame.
func (s *Session) dispatch(p *Pipe, f *wire.Frame, payload Input) *wire.Error {
	switch f.Kind {
	case wire.KindPadding, wire.KindTransportParams:
		// Padding carries nothing; TRANSPORT_PARAMETERS is only the first
		// frame of the first record, consumed before dispatch.
	case wire.KindStream:
		eff, e := s.onStream(f, payload)
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
			return wire.NewError(wire.StreamStateError, "STOP_SENDING for a receive-only stream", "stream", st.id)
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
			s.sched.releaseConnectionCredit()
			s.wakeAllWriters()
			s.wakeCapacityWaiters()
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
			return wire.NewError(wire.StreamStateError, "MAX_STREAM_DATA for a receive-only stream", "stream", st.id)
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
			return wire.NewError(wire.ProtocolViolation, "DATA_BLOCKED above the advertised limit", "value", f.Value)
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
			return wire.NewError(wire.StreamStateError, "STREAM_DATA_BLOCKED for a send-only stream", "stream", st.id)
		}
		if f.Value > st.rcv.adv {
			return wire.NewError(wire.ProtocolViolation, "STREAM_DATA_BLOCKED above the advertised limit",
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
			return wire.NewError(wire.ProtocolViolation, "STREAMS_BLOCKED above the advertised limit", "value", f.Value)
		}
		if f.Value <= s.rem[t].blockedSeen && s.rem[t].blockedSeen > 0 && !s.feat.Multipath {
			s.charge()
		}
		s.rem[t].blockedSeen = max64(s.rem[t].blockedSeen, f.Value)
	case wire.KindClose:
		s.onClose(f)
	case wire.KindDatagram:
		var buf *Buf
		if payload.First.Len() == len(f.Data) {
			buf = payload.First.Buf
		}
		return s.onDatagram(p, f, buf, false)
	case wire.KindDatagramAck:
		s.onDatagramAck(&f.DatagramAck)
	case wire.KindPing:
		return s.onPing(f)
	case wire.KindGoAway:
		s.onGoAway(f)
	case wire.KindPriorityUpdate:
		return s.onPriorityUpdate(f)
	case wire.KindPipeAck:
		return s.ack.onPipeAck(f, s.now, p)
	case wire.KindPipeLane:
		return wire.ErrProto("PIPE_LANE out of place")
	case wire.KindPipePing, wire.KindPipeAbandon, wire.KindPipeStatus:
		return s.onPipeFrame(p, f)
	case wire.KindSessionEpoch:
		return s.onSessionEpoch(p, f)
	}
	return nil
}

// onClose processes a received CONNECTION_CLOSE.
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
	if s.state < StateClosing {
		s.drain.on = true // only a processed CONNECTION_CLOSE drains reads
	}
	s.closeWith(e, &closeFrame{code: wire.NoError})
	if s.rxPipe != nil && s.state == StateClosing {
		s.peerCloseRead(s.rxPipe)
	}
}

// lingerScan discards the bytes read from an inbound lane while the session
// is closing, and returns how many it consumed: whole records of lane 0
// are looked through for the peer's CONNECTION_CLOSE (SPEC.md Section 6.5
// step 3), the rest of a partial record is kept for the next call.
// recordHasClose reports whether a complete record decodes to a close.
func recordHasClose(rec wire.Parts) bool {
	var f wire.Frame
	for rec.Len() > 0 {
		n, e := wire.DecodeFrameParts(rec, &f)
		if e != nil {
			return false
		}
		if f.Kind == wire.KindClose {
			return true
		}
		rec = rec.Slice(n, rec.Len())
	}
	return false
}

// splitDetails separates the diagnostic line.
func splitDetails(r string) (string, string) {
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] == '\n' {
			return r[:i], r[i+1:]
		}
	}
	return r, ""
}

// PipeEnded reports the end of an inbound lane: graceful (eof) or failed
// with cause (the read error, if any). The driver calls it after feeding
// every complete record it read.
func (s *Session) PipeEnded(pipe, lane uint64, graceful, partial bool, cause error, fault *PhysicalFault, now Time) {
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
	if s.ack != nil {
		s.ack.laneEnded(il)
	}
	// A lane that ends before its second record is no event.
	early := il.lane > 0 && il.recs <= 1
	allEnded := true
	for _, l := range p.in.lanes {
		if !l.ended {
			allEnded = false
		}
	}
	if allEnded {
		p.in.ended = true
		s.cancelDatagramReports(p)
	}
	failed := (!graceful || partial) && !early
	if s.feat.Multipath && s.transportLive() {
		// While a close waits for coverage the pipe's loss reinjects
		// what it carried, as before CLOSING.
		if failed || allEnded {
			s.mpInboundEnded(p, !failed, cause, fault)
		}
		s.checkLingerDone()
		return
	}
	if s.state >= StateClosing {
		if allEnded {
			s.closePipeIfDone(p)
		}
		s.checkLingerDone()
		return
	}
	if cause == nil {
		cause = s.writeLostErr
	}
	if failed {
		s.closeWith(&SessionError{Kind: ClosePipesLost, Cause: cause}, nil)
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
	s.closeWith(&SessionError{Kind: ClosePipesLost, Cause: cause}, nil)
}

// PipeWriteFailed reports that the current outbound lane failed even if
// it has no outstanding batch.
func (s *Session) PipeWriteFailed(pipe, lane uint64, err error, fault *PhysicalFault, now Time) {
	s.SetNow(now)
	if p := s.pipe(pipe); s.shp != nil && lane == 0 && p != nil {
		s.shapePipeDown(p)
	}
	if s.state >= StateClosing && (!s.feat.Multipath || !s.transportLive()) {
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
		s.mpOutboundFailed(pipe, err, fault)
		return
	}
	if p := s.pipe(pipe); lane == 0 && p != nil && p.out != nil && p.in != nil && !p.in.ended {
		// Lane 0's carrier may still hold, on its inbound side, records the peer sent
		// before it went away (its CONNECTION_CLOSE): the end waits for
		// that side, bounded by the linger timeout.
		if !p.out.failed {
			p.out.failed = true
			s.writeLostErr = err
			s.timers.writeLost = s.now.Add(s.lingerTimeout())
		}
		return
	}
	s.closeWith(&SessionError{Kind: ClosePipesLost, Cause: err}, nil)
}
