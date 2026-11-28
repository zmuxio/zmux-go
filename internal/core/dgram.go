package core

import (
	"runtime"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// dgramState holds the datagram queues.
type dgramState struct {
	receiveDeadlines                                       DeadlineQueue[*Stream]
	spliceTransfers, spliceBorrowPeak, spliceBorrowRefused uint64
	flows                                                  map[*Stream]*datagramFlow
	recvBacking                                            map[*Buf]datagramReceiveRoot
	heldIDs                                                map[uint64]int // held datagrams per stream ID (releaseHeld)
	reportTimers                                           DeadlineQueue[datagramReport]
	expiry                                                 DeadlineQueue[datagramExpiry]
	reportTimes                                            []Time
	reportScratch                                          []datagramArrival
	reportFrame                                            []byte
	reportRanges                                           []byte
	active                                                 []*datagramFlow
	recv                                                   []receivedDgram
	recvAt                                                 []Time // when each of recv was queued
	recvW                                                  []*Signal
	sendW                                                  []*Signal
	// Received datagrams that wait for the receive limiter, which the
	// driver runs outside the lock.
	limited []PendingDatagram
	// Received datagrams of peer streams not opened yet, in arrival order
	// and so in the order they expire.
	held                     []heldDgram
	window                   datagramWindow
	pathGate                 gate
	recvCd                   codel // the session queue's sojourn bound
	reportLimit              int
	copyCount, copyBytes     int
	nextSequence             uint64
	nextAdmission            uint64
	sendCount, sendBytes     int
	nativeCount, nativeBytes int // original datagrams retained by channel receipts
	recvCapacity             uint64
	recvSize                 uint64 // bytes of the session queue and the held datagrams
	limitedSize              uint64 // bytes of limited, kept within DgramRecvSize
	reportWoken              bool
}

// heldDgram is a datagram held for a peer stream within the limit that
// has not been opened yet, until its opening frame arrives or until.
type heldDgram struct {
	e     receivedDgram
	id    uint64
	seq   uint64
	until Time
	ch    bool // received on a carrier datagram channel
}

// heldCost is what a held datagram counts against DgramRecvSize besides
// its payload: its entry and allocation, so that the byte limit bounds the
// number of tiny datagrams held as well.
const heldCost = 64

func (h *heldDgram) cost() uint64 { return uint64(len(h.e.data)) + heldCost }

// PendingDatagram is a received datagram waiting for the receive limiter.
type PendingDatagram struct {
	// Expires is an optional local Flow deadline for this received stream item.
	// Zero leaves the item under the ordinary Flow lifetime.
	Expires Time
	st      *Stream // nil: a session datagram
	e       receivedDgram
	seq     uint64
}

// Len is the payload size the limiter is asked for.
func (p *PendingDatagram) Len() int { return len(p.e.data) }

// receivedDgram stores parsed Context separately. While in the source
// session its backing is accounted once, irrespective of queue aliases.
type receivedDgram struct {
	expires Time
	buf     *Buf
	data    []byte
	context uint64
}

type datagramReceiveRoot struct {
	items       int
	transferred bool
}

func (s *Session) retainReceivedDatagram(cx uint64, data []byte, buf *Buf) (receivedDgram, bool) {
	d := s.dgState()
	p := receivedDgram{data: data, context: cx}
	if len(data) == 0 {
		p.data = nil
		return p, true
	}
	if buf == nil || d.recvBacking[buf].items == 0 && uint64(cap(buf.B)) > s.cfg.DgramRecvSize-min64(d.recvCapacity, s.cfg.DgramRecvSize) ||
		!buf.charge(s.datagramMemory) {
		if s.datagramMemory == nil {
			p.data = append([]byte(nil), data...)
			return p, true
		}
		buf = copyBacking(s.datagramMemory, data)
		if buf == nil {
			s.dropRecv(1)
			return receivedDgram{}, false
		}
		p.data = buf.B
	} else {
		buf.Ref()
	}
	p.buf = buf
	if d.recvBacking == nil {
		d.recvBacking = make(map[*Buf]datagramReceiveRoot)
	}
	root := d.recvBacking[buf]
	if root.items == 0 {
		d.recvCapacity += uint64(cap(buf.B))
	}
	root.items++
	d.recvBacking[buf] = root
	return p, true
}

func (s *Session) unaccountReceivedDatagram(p receivedDgram) {
	if p.buf == nil {
		return
	}
	d := s.dg
	root := d.recvBacking[p.buf]
	if root.items--; root.items != 0 {
		d.recvBacking[p.buf] = root
	} else {
		delete(d.recvBacking, p.buf)
		d.recvCapacity -= uint64(cap(p.buf.B))
	}
}

func (s *Session) dropReceivedDatagram(p receivedDgram) {
	s.unaccountReceivedDatagram(p)
	if p.buf != nil {
		p.buf.Unref()
	}
}

func (p receivedDgram) relinquish() []byte {
	if p.buf == nil {
		return p.data
	}
	b := append([]byte(nil), p.data...)
	p.buf.Unref()
	return b
}

// outDgram is a queued datagram: its encoded data (flow prefix + payload)
// and, for a stream datagram, its stream, whose opening frame it must not
// overtake.
type outDgram struct {
	reserved  *pipeShape
	st        *Stream
	handoff   *Handoff
	data      dgramView
	opts      DatagramOptions
	deadline  Time
	delay     datagramDelay
	admission uint64
	n         int  // payload bytes (data without the flow and context prefix)
	at        Time // when it was queued (DgramTarget)
}

// countSent counts a datagram handed to a carrier.
func (s *Session) countSent(st *Stream, n int) {
	s.stats.DatagramsSent++
	s.stats.DatagramBytesSent += uint64(n)
	if st != nil {
		c := &st.cnt
		c.dgSent++
		c.dgBytesSent += uint64(n)
	}
}

// ChannelOut is a channel datagram that PollChannel hands to the driver:
// Frame is sent outside the lock, Failed set when the carrier refused it,
// and ChannelDone then counts it.
type ChannelOut struct {
	*channelReceipt
	Frame                    []byte
	Deadline                 Time
	Failed, Expired, Unknown bool
}

// A value copied by an adapter keeps the original completion identity.
type channelReceipt struct {
	pipe      *Pipe
	shape     *pipeShape
	dispatch  shapeDispatch
	handoff   *Handoff
	stream    *Stream
	flow      *datagramFlow
	payload   int
	seq       uint64
	charged   int64
	queueCost int
	replica   bool
}

// ready reports whether the datagram may go now; dead reports one of a
// stream that will never open (cancelled before its first frame).
func (o *outDgram) ready() (ok, dead bool) {
	if o.st == nil || o.st.opened {
		return true, false
	}
	return false, o.st.cancelled
}

func (d *dgramState) wakeAll(s *Session) {
	for _, w := range d.recvW {
		s.wake(w)
	}
	d.recvW = nil
	for _, w := range d.sendW {
		s.wake(w)
	}
	d.sendW = nil
}

// supports reports whether a datagram may use the channel when it is ready.
func (c ChannelUse) supports(n int) bool {
	return c.Limit < 0 || c.Limit > 0 && wire.ChannelDatagramLen(c.HasID, c.ID, n) <= c.Limit
}

// takes reports whether a DATAGRAM fits in the current room.
func (c ChannelUse) takes(n int) bool {
	return c.Room > 0 && wire.ChannelDatagramLen(c.HasID, c.ID, n) <= c.Room
}

// channelOf reads the attachment's latest capacity snapshot.
func (s *Session) channelOf(p *Pipe) ChannelUse {
	if !s.feat.CarrierDatagrams || p == nil || p.out == nil || p.out.failed {
		return ChannelUse{}
	}
	l := p.out.lane(0)
	if l == nil || !l.info.Channel {
		return ChannelUse{}
	}
	return p.chUse
}

// A capacity snapshot may briefly have no usable channel. Retain its last
// resolved binding so a changed stream ID still expires the old evidence.
type datagramChannelBinding struct {
	id           uint64
	hasID, known bool
}

func (s *Session) updateDatagramChannel(p *Pipe, use ChannelUse) {
	if use.Limit <= 0 {
		return
	}
	binding := datagramChannelBinding{hasID: use.HasID, known: true}
	if use.HasID {
		binding.id = use.ID
	}
	if p.chBinding.known && p.chBinding != binding {
		if p.sh != nil {
			p.sh.cancelNative(shaping.ReasonStaleGeneration)
		}
		s.expireDatagramSource(p, true)
	}
	p.chBinding = binding
}

// SetChannelSignal sets the signal that wakes the sender of a pipe's
// datagram channel.
func (s *Session) SetChannelSignal(pipe uint64, sig *Signal) {
	if p := s.pipe(pipe); p != nil {
		p.chSig = sig
		p.chUse = ChannelUse{Limit: -1}
	}
}

// PollChannel appends to dst the channel datagrams due on the datagram
// channel of pipe, each a new slice the driver sends outside the lock and
// in order; live is false once the pipe can send no
// more. use is what the channel takes now, read by the driver outside the
// lock; the core decides with it until the next poll. ChannelDone counts
// the datagrams once the driver tried them.
func (s *Session) PollChannel(pipe uint64, use ChannelUse, dst []ChannelOut, now Time) (_ []ChannelOut, live bool) {
	s.SetNow(now)
	p := s.pipe(pipe)
	if p == nil || p.out == nil || p.out.failed || s.state >= StateClosing {
		return dst, false
	}
	if p.chUse != use || !p.chBinding.known && use.Limit > 0 {
		if p.sh != nil {
			p.sh.settleNativeCopyDelay()
		}
		s.updateDatagramChannel(p, use)
		p.chUse = use
		if p.sh != nil {
			p.sh.startNativeCopyDelay(s.channelOf(p))
		}
		s.noteDatagramPath()
		s.wakeDatagramCopy(p)
	}
	if !s.peerSeen || !maySend(p, false) {
		return dst, true
	}
	ch := s.channelOf(p)
	ps := p.sh
	if ps != nil && ps.native.pending() {
		ps.validateNative(ch)
	}
	if ps != nil && ps.delayActive {
		ps.refreshRecordCopyDelay()
	}
	s.pruneDatagramHeads()
	if ch.Room <= 0 || s.dg == nil || s.dg.sendCount == 0 && len(p.datagramCopies) == 0 {
		return dst, true
	}
	if s.shp != nil {
		ps = s.shapeOf(p)
		if ps != nil {
			ps.shapeEventsDue(s.now)
		}
	}
	for {
		var f *datagramFlow
		if ps != nil && ps.native.pending() {
			f = ps.native.flow
		} else if !s.datagramExhausted() {
			f = s.nextDatagram(p, true, 7)
		}
		if f != nil {
			if s.datagramCoDel(f) {
				continue
			}
			o := &f.queue[0]
			if !ch.takes(s.datagramDataLen(o)) {
				break
			}
			if o.reserved != nil && o.reserved != ps {
				o.reserved.clearNative()
			}
			pad, ready := s.nativePadding(ps, ch, s.datagramDataLen(o), f, nil, o.deadline)
			if !ready {
				break
			}
			data := s.datagramData(o)
			out, ok := s.channelOut(p, ps, ch, data, pad, o.st, o.n, o.data.cost(), o.handoff, s.dg.nextSequence, o.deadline, f)
			if !ok {
				s.dropDatagram(f)
				continue
			}
			dst = append(dst, out)
			s.commitDatagram(p, o, data, true)
			s.removeDatagram(f, true)
		} else {
			c := s.useDatagramCopy(p, true)
			if c == nil {
				break
			}
			pad, ready := s.nativePadding(ps, ch, c.data.len(), nil, c, c.deadline)
			if !ready {
				break
			}
			out, ok := s.channelOut(p, ps, ch, c.data, pad, c.st, c.payload, c.data.cost(), c.handoff, c.sequence, c.deadline, nil)
			if !ok {
				s.removeDatagramCopy(p, 0)
				s.stats.DatagramCopyBudgetDropped++
				continue
			}
			dst = append(dst, out)
			s.countDatagramCopy(p, true)
		}
		// A planner observes the outcome of this dispatch before selecting the
		// next unit. Keep the batched channel path for sessions without one.
		if ps != nil && (ps.datagram != nil || ps.caps&shaping.CapOutbound != 0) {
			break
		}
	}
	return dst, true
}

func (s *Session) channelOut(p *Pipe, ps *pipeShape, ch ChannelUse, data dgramView, pad int, st *Stream, n, cost int, handoff *Handoff, seq uint64, deadline Time, flow *datagramFlow) (ChannelOut, bool) {
	nBytes := int64(wire.ChannelDatagramLen(ch.HasID, ch.ID, data.len()) + pad)
	if !s.datagramMemory.adjust(nBytes) {
		return ChannelOut{}, false
	}
	dispatch := s.reserveShape(uint64(pad))
	s.datagramDispatchPending(p, seq)
	// Transfer the queue charge to the receipt before removing the entry.
	if flow == nil {
		s.dg.copyCount++
		s.dg.copyBytes += cost
	} else {
		s.dg.nativeCount++
		s.dg.nativeBytes += cost
		flow.nativeCount++
		flow.nativeBytes += cost
	}
	return ChannelOut{Frame: data.channelDatagram(ch, pad), Deadline: deadline, channelReceipt: &channelReceipt{stream: st, payload: n,
		replica: flow == nil, flow: flow, pipe: p, seq: seq, shape: ps, dispatch: dispatch, handoff: handoff.hold(), charged: nBytes, queueCost: cost}}, true
}

// ChannelDone counts the channel datagrams of out that PollChannel
// returned: sent, or dropped when the carrier refused them.
func (s *Session) ChannelDone(out []ChannelOut, now Time) {
	s.SetNow(now)
	for i := range out {
		x := &out[i]
		if x.channelReceipt == nil || x.pipe == nil {
			continue
		}
		x.handoff.datagramDone(!x.Failed)
		x.handoff = nil
		s.datagramDispatchDone(x.pipe, x.seq, x.Failed)
		switch {
		case x.Failed:
			reason := shaping.ReasonCarrierRejected
			if x.Expired {
				reason = shaping.ReasonApplicationExpired
			}
			s.shapeReason(reason)
			if x.replica {
				s.stats.DatagramCopyPathDropped++
			} else {
				s.stats.DatagramsDroppedSend++
			}
		case x.replica:
			s.stats.DatagramCopiesSent++
			s.stats.DatagramCopyBytes += uint64(x.payload)
		default:
			s.countSent(x.stream, x.payload)
			s.shapingEarn(uint64(x.payload))
		}
		if !x.Failed {
			extra := x.dispatch.extra
			s.settleShape(x.dispatch, uint64(x.charged)-extra, extra)
			if !x.replica {
				s.outbound(x.shape, 0, len(x.Frame), shaping.NativeDatagram, true, false)
			}
			s.stats.DatagramsSentChannel++
			if s.pipe(x.pipe.id) == x.pipe {
				s.memberBytes(x.pipe, true, uint64(len(x.Frame)))
			}
		} else {
			if x.Unknown {
				s.retainUnknown(x.dispatch.second, x.dispatch.extra)
			}
			s.settleShape(x.dispatch, 0, 0)
		}
		s.datagramMemory.adjust(-x.charged)
		if x.replica {
			s.dg.copyCount--
			s.dg.copyBytes -= x.queueCost
		} else {
			s.dg.nativeCount--
			s.dg.nativeBytes -= x.queueCost
			f := x.flow
			f.nativeCount--
			f.nativeBytes -= x.queueCost
			s.dg.releaseDatagramFlow(f)
			s.wakeSendRoom(1)
		}
		x.pipe, x.shape, x.flow = nil, nil, nil
	}
}

// ReceiveChannelDatagram processes a channel datagram received for pipe,
// its Pipe Stream ID removed. It is not a record: it
// advances no PIPE_ACK position, is not charged to the no-effect budget
// and restarts no idle timer.
func (s *Session) ReceiveChannelDatagram(pipe uint64, frame []byte, now Time) {
	s.SetNow(now)
	if s.state >= StateClosing {
		return
	}
	p := s.pipe(pipe)
	if !mayReceive(p) || !s.peerSeen || !s.feat.CarrierDatagrams {
		s.dropRecv(1)
		return
	}
	var f wire.Frame
	if e := wire.DecodeChannelFrame(frame, &f, s.feat.CarrierDatagramPadding); e != nil {
		s.failLocal(e)
		return
	}
	if e := wire.CheckGate(&f, &s.gates); e != nil {
		s.failLocal(e)
		return
	}
	if e := s.onDatagram(p, &f, nil, true); e != nil {
		s.failLocal(e)
		return
	}
	s.memberBytes(p, false, uint64(len(frame)))
}

type streamDgrams struct {
	deadlines *DeadlineQueue[*Stream]
	expiry    Deadline[*Stream]
	expired   uint64
	q         []receivedDgram
	at        []Time // when each was queued
	size      uint64 // their payload bytes (DgramStreamSize)
	cd        codel  // the queue's sojourn bound (DgramTarget)
	detached  bool
}

// pop removes the oldest datagram of the queue.
func (q *streamDgrams) pop() receivedDgram {
	e := q.q[0]
	q.q[0] = receivedDgram{}
	q.q = q.q[1:]
	q.at = q.at[1:]
	q.size -= uint64(len(e.data))
	if q.expiry.index != 0 && e.expires == q.expiry.at {
		q.armExpiry()
	}
	return e
}

// popRecv removes the oldest datagram of the session queue.
func (d *dgramState) popRecv() receivedDgram {
	e := d.recv[0]
	d.recv[0] = receivedDgram{}
	d.recv = d.recv[1:]
	d.recvAt = d.recvAt[1:]
	d.recvSize -= uint64(len(e.data))
	return e
}

// wakeSendRoom wakes a sender waiting for room in the send queue for each
// of the n datagrams that left it, sent or dropped.
func (s *Session) wakeSendRoom(n int) {
	d := s.dg
	for ; n > 0 && len(d.sendW) > 0; n-- {
		s.wake(d.sendW[0])
		d.sendW = d.sendW[1:]
	}
}

// sendDrop decides on o, a datagram leaving the send queue with rest
// queued after it: true drops it, counted, for its sojourn (DgramTarget).
func (s *Session) dgState() *dgramState {
	if s.dg == nil {
		s.dg = &dgramState{nextSequence: s.cfg.Test.DatagramSequence}
		s.updateDatagramReportLimit()
	}
	return s.dg
}

// MaxDatagramPayload reports the largest payload the session can admit.
func (s *Session) MaxDatagramPayload(flowLen int) int {
	if !s.peerSeen || s.peerMaxDgram == 0 || s.datagramExhausted() {
		return 0
	}
	record, limit, _ := s.datagramCapacity(nil)
	if record == 0 {
		record = min(int(s.peerMaxRecord), wire.RecordFloor)
	}
	return s.datagramPayload(flowLen, record, limit)
}

func (s *Session) datagramPayload(flowLen, record, channel int) int {
	if !s.peerSeen || s.peerMaxDgram == 0 || s.datagramExhausted() {
		return 0
	}
	m := 0
	if record > 0 {
		m = datagramDataMax(min(s.peerMaxDgram, uint64(record)), false)
	}
	if channel > 0 {
		m = max(m, min(channel, int(s.peerMaxDgram-1)))
	}
	if s.feat.DatagramFamily {
		m -= flowLen + s.datagramSequenceWidth()
	}
	return max(m, 0)
}

// PathDatagramPayload reports the largest payload across eligible record
// and carrier datagram paths.
func (s *Session) PathDatagramPayload(flowLen int) int {
	record, _, channel := s.datagramCapacity(nil)
	return s.datagramPayload(flowLen, record, channel)
}

// StreamPathDatagramPayload is PathDatagramPayload for a datagram of st
// with context identifier context (StreamMaxDatagramPayload's prefix).
func (s *Session) StreamPathDatagramPayload(st *Stream, context uint64) int {
	if s.StreamMaxDatagramPayload(st, context) == 0 {
		return 0
	}
	record, _, channel := s.datagramCapacity(st)
	return s.datagramPayload(streamDgramPrefixLen(st, context), record, channel)
}

// noteDatagramPath reports a change of a session datagram's
// PathDatagramPayload (EvDatagramPath).
func (s *Session) noteDatagramPath() {
	if !s.peerSeen {
		return
	}
	s.routeDatagrams()
	_, path, native, nativeAvailable := s.DatagramPath(nil, 0)
	if path != s.dgPath || native != s.dgNative || nativeAvailable != s.dgNativeAvailable {
		s.dgPath, s.dgNative, s.dgNativeAvailable = path, native, nativeAvailable
		s.emit(Event{Kind: EvDatagramPath, Count: uint64(path), Code: uint64(native), Native: nativeAvailable})
	}
}

// StreamMaxDatagramPayload returns the largest datagram payload the peer
// accepts now on st with context identifier context, 0 when it accepts none
// there. Before st has its identifier the largest identifier encoding is
// assumed, so the value only grows once it is assigned.
func (s *Session) StreamMaxDatagramPayload(st *Stream, context uint64) int {
	if !s.peerSeen || !s.feat.DatagramFamily || !st.bidi || context > varint.MaxVarint {
		return 0
	}
	return s.MaxDatagramPayload(streamDgramPrefixLen(st, context))
}

func streamDgramPrefixLen(st *Stream, context uint64) int {
	idLen := 8
	if st.idSet {
		idLen = varint.SizeVarint(st.id + 1)
	}
	return idLen + varint.SizeVarint(context)
}

// SendDatagram queues a datagram; a full queue drops its oldest entry
// (never blocks). stream is nil for a session datagram.
func (s *Session) SendDatagram(st *Stream, context uint64, p []byte) error {
	return s.SendDatagramWith(st, context, p, DatagramOptions{}, s.now)
}

func (s *Session) SendDatagramWith(st *Stream, context uint64, p []byte, o DatagramOptions, at Time) error {
	_, err := s.AdmitDatagram(DatagramRequest{Stream: st, Context: context, Payload: p, Options: o, At: at})
	return err
}

// DatagramRepresentation describes the representation allowed by the host API.
type DatagramRepresentation uint8

const (
	DatagramOnly DatagramRepresentation = iota
	DatagramOrCapsule
	CapsuleOnly
)

type DatagramAdmission uint8

const (
	DatagramRefused DatagramAdmission = iota
	DatagramQueued
	DatagramCapsule
	DatagramWaiting
)

type DatagramDropReason string

const (
	DatagramDropDeadline          DatagramDropReason = "deadline"
	DatagramDropNoNativePath      DatagramDropReason = "no_native_path"
	DatagramDropLimiter           DatagramDropReason = "limiter"
	DatagramDropQueueOverflow     DatagramDropReason = "queue_overflow"
	DatagramDropSequenceExhausted DatagramDropReason = "sequence_exhausted"
)

type DatagramDroppedError struct{ Reason DatagramDropReason }

func (e DatagramDroppedError) Error() string { return "zmux: datagram dropped: " + string(e.Reason) }

func (s *Session) rejectDatagram(reason DatagramDropReason) error {
	s.stats.DatagramsDroppedSend++
	switch reason {
	case DatagramDropDeadline:
		s.stats.DatagramDeadlineDropped++
	case DatagramDropNoNativePath:
		s.stats.DatagramNativeDropped++
	case DatagramDropLimiter:
		s.stats.DatagramsDroppedLimit++
	case DatagramDropSequenceExhausted, DatagramDropQueueOverflow:
	}
	return DatagramDroppedError{Reason: reason}
}

// DatagramRequest contains a single hop's admission inputs.
type DatagramRequest struct {
	Stream         *Stream
	Context        uint64
	Payload        []byte
	Backing        *Buf
	Options        DatagramOptions
	At             Time
	Representation DatagramRepresentation
	Limited, Owned bool
	Report         func(sent, unsent int)
	FirstDatagram  func(native bool)
	// PayloadCopied accumulates actual send-queue payload copies during admission.
	PayloadCopied *uint64
	Wait          *Signal
	WaitDeadline  *Time
}

// AdmitDatagram decides representation and queues an original in the same owner commit.
func (s *Session) AdmitDatagram(r DatagramRequest) (DatagramAdmission, error) {
	plan, result, err := s.planDatagram(r, len(r.Payload))
	if result != DatagramQueued {
		return result, err
	}
	if err := s.pinDatagramStream(r.Stream); err != nil {
		return DatagramRefused, err
	}
	s.pruneDatagramHeads()
	d := s.dgState()
	data, copied, err := s.allocateDatagram(r, plan.prefix[:plan.prefixLen], d)
	if err != nil {
		return DatagramRefused, err
	}
	defer data.unref()
	result, err = s.commitDatagramAdmission(r, plan, data, d)
	if result == DatagramQueued && r.PayloadCopied != nil {
		*r.PayloadCopied += uint64(copied)
	}
	return result, err
}

type datagramPlan struct {
	options   DatagramOptions
	deadline  Time
	prefix    [24]byte
	prefixLen int
}

func (s *Session) planDatagram(r DatagramRequest, largest int) (datagramPlan, DatagramAdmission, error) {
	var plan datagramPlan
	st, context := r.Stream, r.Context
	if s.err != nil {
		return plan, DatagramRefused, s.err
	}
	if st != nil && st.appReleased() {
		return plan, DatagramRefused, &StreamError{Kind: StreamClosedLocally, StreamID: st.id}
	}
	if context > varint.MaxVarint {
		return plan, DatagramRefused, ErrInvalid
	}
	o := s.DatagramOptions(st, r.Options)
	strict := o.Carrier == DatagramRequireNative
	if r.Representation == CapsuleOnly && strict {
		return plan, DatagramRefused, ErrInvalid
	}
	deadline, err := o.ResolveDeadline(r.At)
	if err != nil {
		return plan, DatagramRefused, err
	}
	if deadline <= s.now {
		return plan, DatagramRefused, s.rejectDatagram(DatagramDropDeadline)
	}
	if r.Representation == CapsuleOnly {
		return plan, DatagramCapsule, nil
	}
	if s.datagramExhausted() {
		return plan, DatagramRefused, s.rejectDatagram(DatagramDropSequenceExhausted)
	}
	if r.Representation == DatagramOrCapsule && s.canUseCapsule(st, context, largest, strict, deadline, s.now) {
		return plan, DatagramCapsule, nil
	}
	if !s.peerSeen || s.peerMaxDgram == 0 || st != nil && (!s.feat.DatagramFamily || !st.bidi) {
		return plan, DatagramRefused, ErrNotNegotiated
	}
	if strict && !s.feat.CarrierDatagrams {
		return plan, DatagramRefused, s.rejectDatagram(DatagramDropNoNativePath)
	}
	if r.Wait != nil && s.DatagramQueueFull() {
		r.Wait.Drain()
		s.CancelDatagramSendWait(r.Wait)
		s.WaitDatagramSend(r.Wait)
		*r.WaitDeadline = deadline
		return plan, DatagramWaiting, nil
	}
	if r.Limited {
		return plan, DatagramRefused, s.rejectDatagram(DatagramDropLimiter)
	}
	prefix := plan.prefix[:0]
	if s.feat.DatagramFamily {
		prefix = plan.prefix[:8] // leading room for the sequence assigned at dispatch
	}
	if st != nil {
		id := st.id
		if !st.idSet {
			id = streamID(s.loc[typeIndex(st.bidi)].next, st.bidi, s.role == RoleResponder)
		}
		if id >= varint.MaxVarint {
			return plan, DatagramRefused, ErrStreamLimit
		}
		prefix = varint.AppendVarint(prefix, id+1)
		prefix = varint.AppendVarint(prefix, context)
	} else if s.feat.DatagramFamily {
		prefix = append(prefix, 0)
	}
	flowLen := max(0, len(prefix)-8)
	if m := s.MaxDatagramPayload(flowLen); largest > m {
		return plan, DatagramRefused, &DatagramTooLargeError{Max: m}
	}
	plan.options, plan.deadline, plan.prefixLen = o, deadline, len(prefix)
	return plan, DatagramQueued, nil
}

func (s *Session) pinDatagramStream(st *Stream) error {
	if st != nil && !st.idSet {
		_, err := s.PinID(st)
		return err
	}
	return nil
}

func (s *Session) allocateDatagram(r DatagramRequest, prefix []byte, d *dgramState) (dgramView, int, error) {
	st, p, buf, owned := r.Stream, r.Payload, r.Backing, r.Owned
	if buf != nil {
		space := s.datagramSendRoom()
		if s.feat.DatagramFamily {
			flowSpace := s.cfg.DatagramFlowBytes
			if f := d.flows[st]; f != nil {
				flowSpace -= f.bytes + f.nativeBytes
			}
			space = min(space, flowSpace)
		}
		// Share a backing when it fits. Otherwise use only the bytes
		// needed by this view, before evicting other flows.
		if cap(buf.B)+len(prefix) > space && len(p)+len(prefix) <= space {
			buf, owned = nil, false
		}
	}
	if buf != nil && !buf.charge(s.datagramMemory) {
		buf, owned = nil, false
	}
	// A copied payload shares its allocation with the prefix.
	n := len(prefix)
	if !owned || buf == nil && s.datagramMemory != nil {
		n += len(p)
		owned = false
	}
	storage := newBacking(s.datagramMemory, n)
	if storage == nil {
		s.stats.DatagramsDroppedSend++
		return dgramView{}, 0, ErrMemoryBudget
	}
	copy(storage.B, prefix)
	copied := 0
	if !owned {
		p, buf = storage.B[len(prefix):], nil
		copied = copy(p, r.Payload)
	}
	if buf != nil {
		buf.Ref()
	}
	return dgramView{head: storage.B[:len(prefix):len(prefix)], payload: p, buf: buf, storage: storage}, copied, nil
}

func (s *Session) commitDatagramAdmission(r DatagramRequest, plan datagramPlan, data dgramView, d *dgramState) (DatagramAdmission, error) {
	entry := s.datagramEntry(r, plan, data, d)
	s.enqueueDatagram(entry)
	f := d.flows[r.Stream]
	accepted := f != nil && len(f.queue) != 0 && f.queue[len(f.queue)-1].admission == entry.admission
	if !accepted {
		return DatagramRefused, DatagramDroppedError{Reason: DatagramDropQueueOverflow}
	}
	if len(f.queue) == 1 && f.route.late {
		s.stats.DatagramDeadlineDropped++
		s.dropDatagram(f)
		return DatagramRefused, DatagramDroppedError{Reason: DatagramDropDeadline}
	}
	s.attachDatagramHandoff(&f.queue[len(f.queue)-1], r)
	return DatagramQueued, nil
}

func (s *Session) datagramEntry(r DatagramRequest, plan datagramPlan, data dgramView, d *dgramState) outDgram {
	d.nextAdmission++
	entry := outDgram{data: data, st: r.Stream, n: len(r.Payload), at: s.now, deadline: plan.deadline, opts: plan.options, admission: d.nextAdmission}
	data.ref()
	return entry
}

func (s *Session) attachDatagramHandoff(entry *outDgram, r DatagramRequest) {
	if r.Report != nil && len(r.Payload) != 0 || r.FirstDatagram != nil {
		ticket := s.newHandoff(len(r.Payload), r.Report)
		ticket.datagram = r.FirstDatagram
		entry.handoff = ticket
	}
}

// closeDatagramSends releases the session's sends that will never dispatch.
func (s *Session) closeDatagramSends() {
	if d := s.dg; d != nil {
		for len(d.active) != 0 {
			s.dropDatagram(d.active[0])
		}
		for _, p := range s.pipes {
			s.dropDatagramCopies(p)
		}
	}
}

// writeDatagram adds one eligible Flow head to the current record batch.
func (s *Session) writeDatagram(w *recWriter, p *Pipe, urgency uint8) (sent, full bool) {
	s.pruneDatagramHeads()
	for f := s.nextDatagram(p, false, urgency); f != nil; f = s.nextDatagram(p, false, urgency) {
		if s.datagramCoDel(f) {
			continue
		}
		o := &f.queue[0]
		if o.reserved != nil {
			o.reserved.clearNative()
		}
		data := s.datagramData(o)
		if s.datagramExhausted() {
			s.dropDatagram(f)
			continue
		}
		sent, never := w.datagram(data, o.handoff, s.peerMaxDgram, o.n)
		if never {
			s.dropDatagram(f)
			continue
		}
		if !sent {
			return false, true
		}
		s.countSent(o.st, o.n)
		s.commitDatagram(p, o, data, false)
		s.removeDatagram(f, true)
		return true, false
	}
	if c := s.useDatagramCopy(p, false); c != nil && (c.st == nil || c.st.urgency <= urgency) {
		sent, never := w.datagram(c.data, c.handoff, s.peerMaxDgram, -1)
		if never {
			s.removeDatagramCopy(p, 0)
			s.stats.DatagramCopyPathDropped++
			return false, false
		}
		if sent {
			s.countDatagramCopy(p, false)
		}
		return sent, !sent
	}
	return false, false
}

// DatagramQueueFull reports whether the send queue is full.
func (s *Session) DatagramQueueFull() bool {
	return s.dg != nil && s.datagramSendSlots() <= 0
}

func (s *Session) datagramSendSlots() int {
	return s.cfg.DgramSendQ - s.dg.sendCount - s.dg.nativeCount
}

func (s *Session) datagramSendRoom() int {
	return s.cfg.DatagramSendBytes - s.dg.sendBytes - s.dg.nativeBytes
}

// WaitDatagramSend registers sig for room in the send queue.
func (s *Session) WaitDatagramSend(sig *Signal) { s.dgState().sendW = append(s.dgState().sendW, sig) }

// CancelDatagramSendWait removes sig.
func (s *Session) CancelDatagramSendWait(sig *Signal) {
	if s.dg != nil {
		s.dg.sendW = removeSig(s.dg.sendW, sig)
	}
}

// TakeLimitedDatagrams moves the received datagrams that wait for the
// receive limiter into dst.
func (s *Session) TakeLimitedDatagrams(dst []PendingDatagram) []PendingDatagram {
	if s.dg == nil || len(s.dg.limited) == 0 {
		return dst
	}
	dst = append(dst, s.dg.limited...)
	clear(s.dg.limited)
	s.dg.limited = s.dg.limited[:0]
	s.dg.limitedSize = 0
	return dst
}

// DeliverDatagram queues a datagram the receive limiter passed and
// reports whether it was queued; drop counts one it did not pass.
func (s *Session) DeliverDatagram(p PendingDatagram, drop bool) bool {
	if drop {
		s.dropReceivedDatagram(p.e)
		s.stats.DatagramsDroppedLimit++
		return false
	}
	if s.feat.DatagramFamily && s.duplicateDatagram(p.seq) {
		s.dropReceivedDatagram(p.e)
		return false
	}
	p.e.expires = p.Expires
	ok := s.queueDatagram(p.st, p.e)
	if ok && s.feat.DatagramFamily {
		s.dg.window.set(p.seq, true)
	}
	return ok
}

// onDatagram processes a received DATAGRAM, from a carrier datagram
// channel when ch is set.
func (s *Session) onDatagram(p *Pipe, f *wire.Frame, buf *Buf, ch bool) *wire.Error {
	data := f.Data
	d := s.dgState()
	var seq, flow, context uint64
	if s.feat.DatagramFamily {
		var n int
		seq, n = varint.ConsumeVarint(data)
		if n == 0 {
			return wire.ErrProto("truncated datagram sequence")
		}
		data = data[n:]
		s.expireHeld(s.now)
		// Check the window before inspecting Flow, so a malformed duplicate
		// cannot affect the session. Valid duplicates still feed reports.
		duplicate := s.duplicateDatagram(seq)
		var err *wire.Error
		flow, context, data, err = s.parseDatagramFlow(data)
		if err != nil {
			if duplicate {
				return nil
			}
			return err
		}
		d.window.advance(seq, s.cfg.DgramWindow)
		s.datagramArrival(p, ch, seq)
		if duplicate {
			return nil
		}
	}
	if flow == 0 {
		if e, ok := s.retainReceivedDatagram(0, data, buf); ok {
			s.countReceived(ch)
			s.receivedDatagram(nil, seq, e)
		}
		return nil
	}
	id := flow - 1
	st := s.streams[id]
	if st == nil && !isLocalID(id, s.role) && id>>2 >= s.rem[0].opened {
		if e, ok := s.retainReceivedDatagram(context, data, buf); ok {
			s.holdDatagram(id, seq, e, ch)
		}
		return nil
	}
	if st == nil || st.discarded || st.rcv.stopped {
		s.dropRecv(1)
		return nil
	}
	if e, ok := s.retainReceivedDatagram(context, data, buf); ok {
		s.countReceived(ch)
		s.receivedDatagram(st, seq, e)
	}
	return nil
}

func (s *Session) parseDatagramFlow(data []byte) (flow, context uint64, payload []byte, err *wire.Error) {
	flow, n := varint.ConsumeVarint(data)
	if n == 0 {
		return 0, 0, nil, wire.ErrProto("truncated datagram flow")
	}
	data = data[n:]
	if flow == 0 {
		return flow, 0, data, nil
	}
	context, n = varint.ConsumeVarint(data)
	if n == 0 {
		return 0, 0, nil, wire.ErrProto("truncated datagram context")
	}
	id := flow - 1
	if !isBidiID(id) {
		return 0, 0, nil, wire.ErrProto("datagram flow names a unidirectional stream")
	}
	if isLocalID(id, s.role) {
		if id>>2 >= s.loc[0].next {
			return 0, 0, nil, wire.ErrProto("datagram flow names an unopened local stream")
		}
	} else if id>>2 >= s.rem[0].limit {
		return 0, 0, nil, wire.ErrProto("datagram flow beyond the stream limit")
	}
	return flow, context, data[n:], nil
}

// countReceived counts a datagram that is delivered to a receive queue
// (or to the receive limiter); one dropped before, on arrival or while
// held, counts in DatagramsDroppedRecv only.
func (s *Session) countReceived(ch bool) {
	s.stats.DatagramsReceived++
	if ch {
		s.stats.DatagramsReceivedChannel++
	}
}

// holdDatagram holds a datagram for the peer stream id, not opened yet,
// within the byte limit. A datagram is held for one smoothed RTT, but not
// past one held after it, so that the held datagrams expire in arrival
// order.
func (s *Session) holdDatagram(id, seq uint64, e receivedDgram, ch bool) {
	d := s.dg
	if limit := s.cfg.Test.DatagramHoldCount; limit != nil && len(d.held) >= *limit {
		s.dropReceivedDatagram(e)
		return
	}
	hold := s.srtt()
	if v := s.cfg.Test.DatagramHoldTime; v > 0 {
		hold = v
	}
	until := s.now.Add(hold)
	if n := len(d.held); n > 0 && until < d.held[n-1].until {
		until = d.held[n-1].until
	}
	d.held = append(d.held, heldDgram{id: id, seq: seq, e: e, until: until, ch: ch})
	d.window.set(seq, true)
	if d.heldIDs == nil {
		d.heldIDs = map[uint64]int{}
	}
	d.heldIDs[id]++
	d.recvSize += d.held[len(d.held)-1].cost()
	s.trimRecvBytes()
}

// dropHeldHead drops the oldest held datagram.
func (s *Session) dropHeldHead() {
	d := s.dg
	d.recvSize -= d.held[0].cost()
	d.window.set(d.held[0].seq, false)
	d.unhold(d.held[0].id)
	s.dropReceivedDatagram(d.held[0].e)
	d.held[0] = heldDgram{}
	d.held = d.held[1:]
	s.dropRecv(1)
}

// releaseHeld delivers the datagrams held for a peer stream that opened,
// in arrival order.
func (s *Session) releaseHeld(st *Stream) {
	d := s.dg
	if d == nil || d.heldIDs[st.id] == 0 { // a stream opening scans nothing when none is held for it
		return
	}
	delete(d.heldIDs, st.id)
	kept := d.held[:0]
	for _, h := range d.held {
		if h.id != st.id {
			kept = append(kept, h)
			continue
		}
		d.recvSize -= h.cost()
		s.countReceived(h.ch)
		d.window.set(h.seq, false)
		s.receivedDatagram(st, h.seq, h.e)
	}
	clear(d.held[len(kept):])
	d.held = kept
}

// unhold counts one held datagram of id gone.
func (d *dgramState) unhold(id uint64) {
	if d.heldIDs[id]--; d.heldIDs[id] <= 0 {
		delete(d.heldIDs, id)
	}
}

// heldDeadline is when the first held datagram expires: the oldest.
func (s *Session) heldDeadline() Time {
	if s.dg == nil {
		return Never
	}
	at := s.dg.receiveDeadlines.Next()
	if len(s.dg.held) > 0 {
		at = min(at, s.dg.held[0].until)
	}
	return at
}

// expireHeld drops the held datagrams whose stream did not open within
// one smoothed RTT.
func (s *Session) expireHeld(now Time) {
	d := s.dg
	if d != nil {
		for {
			deadline := d.receiveDeadlines.PopDue(now)
			if deadline == nil {
				break
			}
			s.expireFlowItems(deadline.Value, now)
		}
	}
	for d != nil && len(d.held) > 0 && d.held[0].until <= now {
		s.dropHeldHead()
	}
}

// receivedDatagram queues a datagram, or holds it for the receive limiter.
func (s *Session) receivedDatagram(st *Stream, seq uint64, e receivedDgram) {
	d := s.dg
	if s.cfg.DgramRecvLimited {
		// Waiting for the limiter stays within the byte limit: the
		// oldest go, each counted.
		d.limited = append(d.limited, PendingDatagram{st: st, seq: seq, e: e})
		d.limitedSize += uint64(len(e.data)) + heldCost
		for d.limitedSize > s.cfg.DgramRecvSize {
			d.limitedSize -= uint64(len(d.limited[0].e.data)) + heldCost
			s.dropReceivedDatagram(d.limited[0].e)
			d.limited[0] = PendingDatagram{}
			d.limited = d.limited[1:]
			s.dropRecv(1)
		}
		return
	}
	if s.queueDatagram(st, e) && s.feat.DatagramFamily {
		d.window.set(seq, true)
	}
}

// queueDatagram puts a datagram in its stream's or the session's queue.
func (s *Session) queueDatagram(st *Stream, e receivedDgram) bool {
	d := s.dgState()
	if s.state >= StateClosing {
		s.dropReceivedDatagram(e)
		return false
	}
	if st != nil {
		if st.discarded || st.rcv.stopped {
			s.dropReceivedDatagram(e)
			s.dropRecv(1)
			return false
		}
		if st.dg == nil {
			st.dg = &streamDgrams{}
		}
		// The count, then the bytes: the oldest go, each counted; a
		// datagram above the byte bound is kept alone.
		n := uint64(len(e.data))
		for len(st.dg.q) > 0 && (len(st.dg.q) >= s.cfg.DgramStreamQ ||
			s.cfg.DgramStreamSize > 0 && st.dg.size+n > s.cfg.DgramStreamSize) {
			s.dropReceivedDatagram(st.dg.pop())
			s.dropRecv(1)
			st.cnt.dgDropped++
		}
		st.cnt.dgReceived++
		st.dg.q = append(st.dg.q, e)
		st.dg.at = append(st.dg.at, s.now)
		st.dg.size += n
		if e.expires != 0 {
			q := st.dg
			q.deadlines = &d.receiveDeadlines
			q.expiry.Value = st
			if q.expiry.index == 0 || e.expires < q.expiry.at {
				q.deadlines.Set(&q.expiry, e.expires)
			}
		}
		s.dgramChanged(st)
		return true
	}
	if len(d.recv) >= s.cfg.DgramRecvQ {
		s.dropReceivedDatagram(d.popRecv())
		s.dropRecv(1)
	}
	d.recv = append(d.recv, e)
	d.recvAt = append(d.recvAt, s.now)
	d.recvSize += uint64(len(e.data))
	s.trimRecvBytes()
	if len(d.recvW) > 0 {
		s.wake(d.recvW[0])
	}
	return len(d.recv) > 0 // trimmed from the front: e is last to go
}

// DropReceivedDatagrams counts n received datagrams that the host dropped
// before handing them to the session.
func (s *Session) DropReceivedDatagrams(n uint64) { s.dropRecv(n) }

func (s *Session) dropRecv(n uint64) {
	s.stats.DatagramsDroppedRecv += n
	// The count is cumulative: drops in a row update the one event not yet
	// taken, so a flood of drops queues one event, not one per datagram.
	if k := len(s.events); k > s.evHead && s.events[k-1].Kind == EvDatagramDropped {
		s.events[k-1].Count = s.stats.DatagramsDroppedRecv
		return
	}
	s.emit(Event{Kind: EvDatagramDropped, Count: s.stats.DatagramsDroppedRecv})
}

// trimRecvBytes keeps received datagrams within the byte limit by dropping
// the oldest held for streams not opened yet, then the oldest of the
// session queue. The stream queues have bounds of their own and do not
// count against it.
func (s *Session) trimRecvBytes() {
	d := s.dg
	for d.recvSize > s.cfg.DgramRecvSize && len(d.held) > 0 {
		s.dropHeldHead()
	}
	for d.recvSize > s.cfg.DgramRecvSize && len(d.recv) > 0 {
		s.dropReceivedDatagram(d.popRecv())
		s.dropRecv(1)
	}
}

// RecvDatagram pops a session datagram; ok is false when none waits.
// Datagrams queued longer than DgramTarget allows are dropped on the way
// (CoDel), counted.
func (s *Session) RecvDatagram() ([]byte, bool) {
	d := s.dg
	if d == nil {
		return nil, false
	}
	for len(d.recv) > 0 && d.recvCd.drop(d.recvAt[0], s.now, s.cfg.DgramTarget, len(d.recv) == 1) {
		s.dropReceivedDatagram(d.popRecv())
		s.dropRecv(1)
	}
	if len(d.recv) == 0 {
		d.recvCd.idle()
		return nil, false
	}
	e := d.popRecv()
	s.unaccountReceivedDatagram(e)
	s.stats.DatagramBytesReceived += uint64(len(e.data))
	return e.relinquish(), true
}

// WaitDatagram registers sig for a session datagram.
func (s *Session) WaitDatagram(sig *Signal) { s.dgState().recvW = append(s.dgState().recvW, sig) }

// CancelDatagramWait removes sig.
func (s *Session) CancelDatagramWait(sig *Signal) {
	if s.dg != nil {
		s.dg.recvW = removeSig(s.dg.recvW, sig)
	}
}

// RecvStreamDatagram pops a stream datagram, dropping on the way those
// queued longer than DgramTarget allows (CoDel), counted.
func (s *Session) RecvStreamDatagram(st *Stream) (uint64, []byte, bool) {
	q := s.spliceDatagramHead(st)
	if q == nil {
		return 0, nil, false
	}
	e := s.consumeStreamDatagram(st, q)
	return e.context, e.relinquish(), true
}

func (s *Session) consumeStreamDatagram(st *Stream, q *streamDgrams) receivedDgram {
	e := s.popStreamDatagram(q)
	s.stats.DatagramBytesReceived += uint64(len(e.data))
	st.cnt.dgBytesReceived += uint64(len(e.data))
	return e
}

func (s *Session) popStreamDatagram(q *streamDgrams) receivedDgram {
	e := q.pop()
	if !q.detached {
		s.unaccountReceivedDatagram(e)
	}
	return e
}

// StreamDatagramsEnded reports whether no stream datagram can arrive any
// more: the session ended (err) or the stream was discarded or its
// receiving part stopped (no error: the flow's end).
func (s *Session) StreamDatagramsEnded(st *Stream) (bool, error) {
	if s.err != nil {
		return true, s.err
	}
	return st.discarded || st.hasR && st.rcv.stopped, nil
}

// endStreamDgrams ends the datagram flow of a discarded stream. The
// datagrams that arrived before the end stay in the queue, bound by the
// stream queue limits and owned by the application's handle, which reads
// them (RecvStreamDatagram) and frees them with itself; the handle is
// what the application reads the stream's data through, so a datagram
// reader running beside it does not lose them to the automatic release.
// Datagrams arriving later are dropped and counted (queueDatagram).
func (s *Session) endStreamDgrams(st *Stream) {
	if q := st.dg; q != nil {
		if len(q.q) == 0 {
			st.dg = nil
		} else if !q.detached {
			// The application can drain the final queue after close.
			// Remove the session's aliases but keep the backing charged
			// while the handle still owns it. pop clears transferred slots.
			for _, e := range q.q {
				s.unaccountReceivedDatagram(e)
			}
			if q.deadlines != nil {
				q.deadlines.Cancel(&q.expiry)
				q.deadlines = nil
				q.expiry.Value = nil
			}
			q.detached = true
			runtime.AddCleanup(q, releaseDetachedDatagrams, q.q)
		}
	}
	s.dgramChanged(st)
}

func releaseDetachedDatagrams(items []receivedDgram) {
	for _, e := range items {
		if e.buf != nil {
			e.buf.Unref()
		}
	}
}

func (s *Session) closeDatagramReceives() {
	if d := s.dg; d != nil {
		for len(d.held) != 0 {
			s.dropHeldHead()
		}
		for len(d.recv) != 0 {
			s.dropReceivedDatagram(d.popRecv())
		}
		for _, p := range d.limited {
			s.dropReceivedDatagram(p.e)
		}
		clear(d.limited)
		d.limited = nil
		d.limitedSize = 0
	}
}

// waitsFor reports a queued datagram of st.
func (d *dgramState) waitsFor(st *Stream) bool {
	if d != nil {
		f := d.flows[st]
		return f != nil && len(f.queue) != 0
	}
	return false
}
