package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// dgramState holds the datagram queues.
type dgramState struct {
	send  []outDgram
	recv  [][]byte
	recvW []*Signal
	sendW []*Signal
	// Received datagrams that wait for the receive limiter, which the
	// driver runs outside the lock.
	limited []PendingDatagram
	// Received datagrams of peer streams not opened yet, in arrival order
	// and so in the order they expire.
	held        []heldDgram
	heldIDs     map[uint64]int // held datagrams per stream ID (releaseHeld)
	recvSize    uint64         // bytes of the session queue and the held datagrams
	limitedSize uint64         // bytes of limited, kept within DgramRecvSize
}

// heldDgram is a datagram held for a peer stream within the limit that
// has not been opened yet, until its opening frame arrives or until.
type heldDgram struct {
	e     []byte // context-prefixed payload
	id    uint64
	until Time
	ch    bool // received on a carrier datagram channel
}

// heldCost is what a held datagram counts against DgramRecvSize besides
// its payload: its entry and allocation, so that the byte limit bounds the
// number of tiny datagrams held as well.
const heldCost = 64

func (h *heldDgram) cost() uint64 { return uint64(len(h.e)) + heldCost }

// PendingDatagram is a received datagram waiting for the receive limiter.
type PendingDatagram struct {
	st *Stream // nil: a session datagram
	e  []byte  // payload (a stream datagram: context-prefixed)
}

// Len is the payload size the limiter is asked for.
func (p *PendingDatagram) Len() int {
	if p.st == nil {
		return len(p.e)
	}
	_, n := varint.ConsumeVarint(p.e)
	return len(p.e) - n
}

// outDgram is a queued datagram: its encoded data (flow prefix + payload)
// and, for a stream datagram, its stream, whose opening frame it must not
// overtake.
type outDgram struct {
	st   *Stream
	data []byte
	n    int // payload bytes (data without the flow and context prefix)
}

// countSent counts a datagram handed to a carrier.
func (s *Session) countSent(o *outDgram) {
	s.stats.DatagramsSent++
	s.stats.DatagramBytesSent += uint64(o.n)
	if o.st != nil {
		c := &o.st.cnt
		c.dgSent++
		c.dgBytesSent += uint64(o.n)
	}
}

// ChannelOut is a channel datagram that PollChannel hands to the driver:
// Frame is sent outside the lock, Failed set when the carrier refused it,
// and ChannelDone then counts it.
type ChannelOut struct {
	Frame  []byte
	d      outDgram
	Failed bool
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

// sendable reports queued datagrams that go in records now (or are to be
// dropped); those for the datagram pipe's channel go by PollChannel.
func (d *dgramState) sendable(s *Session) bool {
	if !s.peerSeen {
		return false
	}
	ch := s.channelOf(s.dgPipe())
	for i := range d.send {
		ok, dead := d.send[i].ready()
		if dead || ok && (!ch.takes(len(d.send[i].data)) || s.followPipe(d.send[i].st) != nil) {
			return true
		}
	}
	return false
}

// dgPipe returns the pipe that DATAGRAM frames go to now: the pipe whose
// lane 0 carries the control frames, the primary pipe of a multipath
// session or the session's outbound pipe;
// nil when every pipe of a multipath session carries them.
func (s *Session) dgPipe() *Pipe {
	if s.mp != nil {
		p, _ := s.primary()
		return p
	}
	for _, p := range s.pipes {
		if p.out != nil && !p.out.failed {
			return p
		}
	}
	return nil
}

// dgInRecords reports whether p's lane 0 records carry the queued
// datagrams that follow no opening frame: every pipe of a session without
// multipath, and the primary pipe of a multipath one (every pipe while it
// has none).
func (s *Session) dgInRecords(p *Pipe) bool {
	if s.mp == nil {
		return true
	}
	prim, _ := s.primary()
	return prim == nil || prim == p
}

// followPipe returns the pipe a stream datagram follows in a multipath
// session: the pipe of its stream's opening frame (or of the latest
// reinjected copy) while that frame is unacknowledged and the pipe is
// live (MULTIPATH.md S7, EXTENSIONS.md 6.4); nil when it goes by the
// usual rule.
func (s *Session) followPipe(st *Stream) *Pipe {
	if s.mp == nil || st == nil || st.snd.mps == nil {
		return nil
	}
	m := st.snd.mps
	if !m.openSet || m.openAcked {
		return nil
	}
	p := s.pipe(m.openPipe)
	if p == nil || p.mp == nil || p.out == nil || p.out.failed || !p.mp.liveOut() || p.mp.retired {
		return nil
	}
	return p
}

// takes reports whether a DATAGRAM frame of n bytes of data goes on the
// channel.
func (c ChannelUse) takes(n int) bool {
	return c.Room > 0 && wire.ChannelDatagramLen(c.HasID, c.ID, n) <= c.Room
}

// channelOf returns how DATAGRAM frames for p may use its carrier's
// datagram channel: when both endpoints sent
// zmux_carrier_datagrams, the carrier has a channel, and p's records are
// not shaped.
func (s *Session) channelOf(p *Pipe) ChannelUse {
	if !s.feat.CarrierDatagrams || p == nil || p.out == nil || p.out.failed {
		return ChannelUse{}
	}
	if !p.out.lanes[0].info.Channel || s.shp != nil && s.shapeOf(p) != nil {
		return ChannelUse{}
	}
	return p.chUse
}

// SetChannelSignal sets the signal that wakes the sender of a pipe's
// datagram channel.
func (s *Session) SetChannelSignal(pipe uint64, sig *Signal) {
	if p := s.pipe(pipe); p != nil {
		p.chSig = sig
	}
}

func (s *Session) wakeChannels() {
	for _, p := range s.pipes {
		if p.chSig != nil {
			s.wake(p.chSig)
		}
	}
}

// PollChannel appends to dst the channel datagrams due on the datagram
// channel of pipe, each a new slice the driver sends outside the lock and
// in order; live is false once the pipe can send no
// more. use is what the channel takes now, read by the driver outside the
// lock; the core decides with it until the next poll. ChannelDone counts
// the datagrams once the driver tried them.
func (s *Session) PollChannel(pipe uint64, use ChannelUse, dst []ChannelOut) (_ []ChannelOut, live bool) {
	p := s.pipe(pipe)
	if p == nil || p.out == nil || p.out.failed || s.state == StateClosed {
		return dst, false
	}
	p.chUse = use
	d := s.dg
	if d == nil || len(d.send) == 0 || !s.peerSeen || s.state >= StateClosing || p != s.dgPipe() {
		return dst, true
	}
	ch := s.channelOf(p)
	if ch.Room == 0 {
		return dst, true
	}
	kept := d.send[:0]
	for _, o := range d.send {
		ok, dead := o.ready()
		if dead || uint64(len(o.data)+1) > s.peerMaxDgram {
			s.stats.DatagramsDroppedSend++
			continue
		}
		if !ok || !ch.takes(len(o.data)) || s.followPipe(o.st) != nil {
			kept = append(kept, o)
			continue
		}
		b := make([]byte, 0, wire.ChannelDatagramLen(ch.HasID, ch.ID, len(o.data)))
		dst = append(dst, ChannelOut{Frame: wire.AppendChannelDatagram(b, ch.HasID, ch.ID, o.data), d: outDgram{st: o.st, n: o.n}})
		if len(d.sendW) > 0 {
			s.wake(d.sendW[0])
			d.sendW = d.sendW[1:]
		}
	}
	clear(d.send[len(kept):])
	d.send = kept
	if len(d.send) == 0 {
		d.send = nil
	}
	return dst, true
}

// ChannelDone counts the channel datagrams of out that PollChannel
// returned: sent, or dropped when the carrier refused them.
func (s *Session) ChannelDone(out []ChannelOut) {
	for i := range out {
		if out[i].Failed {
			s.stats.DatagramsDroppedSend++
			continue
		}
		s.countSent(&out[i].d)
		s.stats.DatagramsSentChannel++
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
	if s.pipe(pipe) == nil || !s.peerSeen || !s.feat.CarrierDatagrams {
		s.dropRecv(1)
		return
	}
	var f wire.Frame
	if e := wire.DecodeChannelFrame(frame, &f); e != nil {
		s.failLocal(e)
		return
	}
	if e := wire.CheckGate(&f, &s.gates); e != nil {
		s.failLocal(e)
		return
	}
	if e := s.onDatagram(&f, true); e != nil {
		s.failLocal(e)
	}
}

type streamDgrams struct {
	q    [][]byte // context-prefixed payloads
	size uint64   // their payload bytes (DgramStreamSize)
}

// dgPayloadLen is the payload length of a context-prefixed stream datagram.
func dgPayloadLen(e []byte) uint64 {
	_, n := varint.ConsumeVarint(e)
	return uint64(len(e) - n)
}

// pop removes the oldest datagram of the queue.
func (q *streamDgrams) pop() []byte {
	e := q.q[0]
	q.q[0] = nil
	q.q = q.q[1:]
	q.size -= dgPayloadLen(e)
	return e
}

func (s *Session) dgState() *dgramState {
	if s.dg == nil {
		s.dg = &dgramState{}
	}
	return s.dg
}

// MaxDatagramPayload returns the largest session datagram payload the peer
// accepts now, 0 when it accepts none.
func (s *Session) MaxDatagramPayload(flowLen int) int {
	if !s.peerSeen || s.peerMaxDgram == 0 {
		return 0
	}
	// What writeDatagrams can send in a record of every pipe that may
	// carry it: the Length field is left out as the last frame of a
	// record, except where a shaper may pad after it.
	m := datagramDataMax(min(s.peerMaxDgram, s.dgRecordCap()), s.shp != nil)
	if s.feat.Flows {
		m -= flowLen
	}
	return max(m, 0)
}

// dgRecordCap returns the smallest record capacity (frames bytes) of the
// pipes that may carry datagrams in records.
func (s *Session) dgRecordCap() uint64 {
	c := min(s.peerMaxRecord, wire.MaxRecordSize)
	add := func(p *Pipe) {
		for _, ol := range p.out.lanes {
			c = min(c, uint64(s.recMax(ol)))
		}
	}
	if p := s.dgPipe(); p != nil {
		add(p)
		return c
	}
	for _, p := range s.pipes {
		if p.out != nil && !p.out.failed {
			add(p)
		}
	}
	return c
}

// StreamMaxDatagramPayload returns the largest datagram payload the peer
// accepts now on st with context identifier context, 0 when it accepts none
// there. Before st has its identifier the largest identifier encoding is
// assumed, so the value only grows once it is assigned.
func (s *Session) StreamMaxDatagramPayload(st *Stream, context uint64) int {
	if !s.cfg.Datagrams || !s.peerSeen || !s.feat.Flows || !st.bidi {
		return 0
	}
	idLen := 8
	if st.idSet {
		idLen = varint.SizeVarint(st.id + 1)
	}
	return s.MaxDatagramPayload(idLen + varint.SizeVarint(context))
}

// SendDatagram queues a datagram; a full queue drops its oldest entry
// (never blocks). stream is nil for a session datagram.
func (s *Session) SendDatagram(st *Stream, context uint64, p []byte) error {
	if s.err != nil {
		return s.err
	}
	if !s.cfg.Datagrams {
		return ErrNotNegotiated
	}
	var prefix []byte
	if st != nil {
		if st.appReleased() {
			return &StreamError{Kind: StreamClosedLocally, StreamID: st.id} // the application is done with the stream
		}
		if s.peerSeen && !s.feat.Flows || !st.bidi {
			return ErrNotNegotiated
		}
		if !st.idSet {
			if _, err := s.PinID(st); err != nil {
				return err
			}
		}
		prefix = varint.AppendVarint(prefix, st.id+1)
		prefix = varint.AppendVarint(prefix, context)
	} else if s.feat.Flows || !s.peerSeen && s.cfg.Local.Has&wire.HasFlows != 0 {
		prefix = []byte{0}
	}
	if s.peerSeen {
		if s.peerMaxDgram == 0 {
			return ErrNotNegotiated
		}
		if m := s.MaxDatagramPayload(len(prefix)); len(p) > m {
			return &DatagramTooLargeError{Max: m}
		}
	}
	d := s.dgState()
	data := make([]byte, 0, len(prefix)+len(p))
	data = append(append(data, prefix...), p...)
	if len(d.send) >= s.cfg.DgramSendQ {
		d.send[0] = outDgram{}
		d.send = d.send[1:]
		s.stats.DatagramsDroppedSend++
	}
	d.send = append(d.send, outDgram{data: data, st: st, n: len(p)})
	s.wakeAllWriters()
	s.wakeChannels()
	return nil
}

// writeDatagrams puts queued datagrams into records on p, leaving those
// for p's datagram channel to PollChannel.
func (s *Session) writeDatagrams(w *recWriter, p *Pipe) {
	d := s.dg
	var ch ChannelUse
	if p == s.dgPipe() {
		ch = s.channelOf(p)
	}
	usual := s.dgInRecords(p)
	held := d.send[:0] // in place: datagrams waiting for their stream's opening frame or the channel
	i := 0
	for ; i < len(d.send); i++ {
		o := d.send[i]
		ok, dead := o.ready()
		if dead || uint64(len(o.data)+1) > s.peerMaxDgram {
			s.stats.DatagramsDroppedSend++
			continue
		}
		if !ok {
			held = append(held, o)
			continue
		}
		if f := s.followPipe(o.st); f != nil {
			// While no pipe of the opening frame may carry it (SUSPECT,
			// DRAINING), it stays queued.
			if f != p || f.mp.out != OutActive {
				held = append(held, o)
				continue
			}
		} else if !usual {
			held = append(held, o)
			continue
		} else if ch.takes(len(o.data)) {
			held = append(held, o)
			s.wake(p.chSig)
			continue
		}
		sent, never := w.datagram(o.data, s.peerMaxDgram)
		if never {
			// It can never fit this pipe's records: drop it rather than
			// hold back the datagrams queued after it.
			s.stats.DatagramsDroppedSend++
			continue
		}
		if !sent {
			break
		}
		s.countSent(&o)
		if len(d.sendW) > 0 {
			s.wake(d.sendW[0])
			d.sendW = d.sendW[1:]
		}
	}
	held = append(held, d.send[i:]...)
	clear(d.send[len(held):])
	d.send = held
	if len(d.send) == 0 {
		d.send = nil
	}
}

// DatagramQueueFull reports whether the send queue is full.
func (s *Session) DatagramQueueFull() bool {
	return s.dg != nil && len(s.dg.send) >= s.cfg.DgramSendQ
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
		s.stats.DatagramsDroppedLimit++
		return false
	}
	return s.queueDatagram(p.st, p.e)
}

// onDatagram processes a received DATAGRAM, from a carrier datagram
// channel when ch is set.
func (s *Session) onDatagram(f *wire.Frame, ch bool) *wire.Error {
	data := f.Data
	s.dgState()
	if s.feat.Flows {
		flow, n := varint.ConsumeVarint(data)
		if n == 0 {
			return wire.ErrProto("truncated datagram flow")
		}
		data = data[n:]
		if flow > 0 {
			ctx, cn := varint.ConsumeVarint(data)
			if cn == 0 {
				return wire.ErrProto("truncated datagram context")
			}
			id := flow - 1
			if !isBidiID(id) {
				return wire.ErrProto("datagram flow names a unidirectional stream")
			}
			if isLocalID(id, s.role) {
				if id>>2 >= s.loc[0].next {
					return wire.ErrProto("datagram flow names an unopened local stream")
				}
			} else if id>>2 >= s.rem[0].limit {
				return wire.ErrProto("datagram flow beyond the stream limit")
			}
			st := s.streams[id]
			if st == nil && !isLocalID(id, s.role) && id>>2 >= s.rem[0].opened {
				// It overtook the stream's opening frame:
				// held for at most one smoothed RTT, and
				// counted as received once delivered.
				s.holdDatagram(id, streamDgram(ctx, data[cn:]), ch)
				return nil
			}
			if st == nil || st.discarded || st.rcv.stopped {
				s.dropRecv(1)
				return nil
			}
			s.countReceived(ch)
			s.receivedDatagram(st, streamDgram(ctx, data[cn:]))
			return nil
		}
	}
	s.countReceived(ch)
	s.receivedDatagram(nil, append([]byte(nil), data...))
	return nil
}

// streamDgram is a stream datagram as its queue holds it: the context ID,
// then the payload.
func streamDgram(ctx uint64, p []byte) []byte {
	e := make([]byte, 0, varint.SizeVarint(ctx)+len(p))
	return append(varint.AppendVarint(e, ctx), p...)
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
func (s *Session) holdDatagram(id uint64, e []byte, ch bool) {
	d := s.dg
	until := s.now.Add(s.srtt())
	if n := len(d.held); n > 0 && until < d.held[n-1].until {
		until = d.held[n-1].until
	}
	d.held = append(d.held, heldDgram{id: id, e: e, until: until, ch: ch})
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
	d.unhold(d.held[0].id)
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
		s.receivedDatagram(st, h.e)
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
	if s.dg == nil || len(s.dg.held) == 0 {
		return Never
	}
	return s.dg.held[0].until
}

// expireHeld drops the held datagrams whose stream did not open within
// one smoothed RTT.
func (s *Session) expireHeld(now Time) {
	d := s.dg
	for d != nil && len(d.held) > 0 && d.held[0].until <= now {
		s.dropHeldHead()
	}
}

// receivedDatagram queues a datagram, or holds it for the receive limiter.
func (s *Session) receivedDatagram(st *Stream, e []byte) {
	d := s.dg
	if s.cfg.DgramRecvLimited {
		// Waiting for the limiter stays within the byte limit: the
		// oldest go, each counted.
		d.limited = append(d.limited, PendingDatagram{st: st, e: e})
		d.limitedSize += uint64(len(e))
		for d.limitedSize > s.cfg.DgramRecvSize && len(d.limited) > 1 {
			d.limitedSize -= uint64(len(d.limited[0].e))
			d.limited[0] = PendingDatagram{}
			d.limited = d.limited[1:]
			s.dropRecv(1)
		}
		return
	}
	s.queueDatagram(st, e)
}

// queueDatagram puts a datagram in its stream's or the session's queue.
func (s *Session) queueDatagram(st *Stream, e []byte) bool {
	d := s.dgState()
	if st != nil {
		if st.discarded || st.rcv.stopped {
			s.dropRecv(1)
			return false
		}
		if st.dg == nil {
			st.dg = &streamDgrams{}
		}
		// The count, then the bytes: the oldest go, each counted; a
		// datagram above the byte bound is kept alone.
		n := dgPayloadLen(e)
		for len(st.dg.q) > 0 && (len(st.dg.q) >= s.cfg.DgramStreamQ ||
			s.cfg.DgramStreamSize > 0 && st.dg.size+n > s.cfg.DgramStreamSize) {
			st.dg.pop()
			s.dropRecv(1)
			st.cnt.dgDropped++
		}
		st.cnt.dgReceived++
		st.dg.q = append(st.dg.q, e)
		st.dg.size += n
		s.dgramChanged(st)
		return true
	}
	if len(d.recv) >= s.cfg.DgramRecvQ {
		d.recvSize -= uint64(len(d.recv[0]))
		d.recv[0] = nil
		d.recv = d.recv[1:]
		s.dropRecv(1)
	}
	d.recv = append(d.recv, e)
	d.recvSize += uint64(len(e))
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
		d.recvSize -= uint64(len(d.recv[0]))
		d.recv[0] = nil
		d.recv = d.recv[1:]
		s.dropRecv(1)
	}
}

// RecvDatagram pops a session datagram; ok is false when none waits.
func (s *Session) RecvDatagram() ([]byte, bool) {
	d := s.dg
	if d == nil || len(d.recv) == 0 {
		return nil, false
	}
	e := d.recv[0]
	d.recv[0] = nil
	d.recv = d.recv[1:]
	d.recvSize -= uint64(len(e))
	s.stats.DatagramBytesReceived += uint64(len(e))
	return e, true
}

// WaitDatagram registers sig for a session datagram.
func (s *Session) WaitDatagram(sig *Signal) { s.dgState().recvW = append(s.dgState().recvW, sig) }

// CancelDatagramWait removes sig.
func (s *Session) CancelDatagramWait(sig *Signal) {
	if s.dg != nil {
		s.dg.recvW = removeSig(s.dg.recvW, sig)
	}
}

// RecvStreamDatagram pops a stream datagram.
func (s *Session) RecvStreamDatagram(st *Stream) (uint64, []byte, bool) {
	if st.dg == nil || len(st.dg.q) == 0 {
		return 0, nil, false
	}
	e := st.dg.pop()
	ctx, n := varint.ConsumeVarint(e)
	p := e[n:]
	s.stats.DatagramBytesReceived += uint64(len(p))
	st.cnt.dgBytesReceived += uint64(len(p))
	return ctx, p, true
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
	if st.dg != nil && len(st.dg.q) == 0 {
		st.dg = nil
	}
	s.dgramChanged(st) // a waiter sees the end, or the queue
}

// waitsFor reports a queued datagram of st.
func (d *dgramState) waitsFor(st *Stream) bool {
	for i := range d.send {
		if d.send[i].st == st {
			return true
		}
	}
	return false
}
