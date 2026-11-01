package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// dgramState holds the datagram queues (EXTENSIONS.md 6.5).
type dgramState struct {
	send     []outDgram
	recv     [][]byte
	recvSize uint64 // bytes held in every receive queue
	recvW    []*Signal
	sendW    []*Signal
	// Received datagrams that wait for the receive limiter, which the
	// driver runs outside the lock.
	limited []PendingDatagram
	// Received datagrams of peer streams not opened yet, in arrival order
	// and so in the order they expire (EXTENSIONS.md 6.4).
	held []heldDgram
}

// heldDgram is a datagram held for a peer stream within the limit that
// has not been opened yet, until its opening frame arrives or until.
type heldDgram struct {
	id    uint64
	e     []byte // context-prefixed payload
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
// overtake (EXTENSIONS.md 6.4).
type outDgram struct {
	data []byte
	st   *Stream
	n    int // payload bytes (data without the flow and context prefix)
}

// countSent counts a datagram handed to a carrier (API.md 7.1).
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
	Failed bool
	d      outDgram
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
		if dead || ok && !ch.takes(len(d.send[i].data)) {
			return true
		}
	}
	return false
}

// dgPipe returns the pipe that DATAGRAM frames go to now: the pipe whose
// lane 0 carries the control frames, the primary pipe of a multipath
// session (MULTIPATH.md 11.1 rule S7) or the session's outbound pipe;
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

// chanUse is the use of a pipe's datagram channel for the frames being
// sent: room 0 when they go in records.
type chanUse struct {
	room  int
	id    uint64
	hasID bool
}

// takes reports whether a DATAGRAM frame of n bytes of data goes on the
// channel.
func (c chanUse) takes(n int) bool {
	return c.room > 0 && wire.ChannelDatagramLen(c.hasID, c.id, n) <= c.room
}

// channelOf returns how DATAGRAM frames for p may use its carrier's
// datagram channel (EXTENSIONS.md 6.8): when both endpoints sent
// zmux_carrier_datagrams, the carrier has a channel, and p's records are
// not shaped.
func (s *Session) channelOf(p *Pipe) chanUse {
	if !s.feat.CarrierDatagrams || p == nil || p.out == nil || p.out.failed {
		return chanUse{}
	}
	ch := p.out.lanes[0].info.Channel
	if ch == nil || s.shp != nil && s.shapeOf(p) != nil {
		return chanUse{}
	}
	c := chanUse{room: ch.MaxChannelDatagram()}
	if c.room > 0 {
		c.id, c.hasID = ch.ChannelStreamID()
	}
	return c
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
// in order (EXTENSIONS.md 6.8); live is false once the pipe can send no
// more. ChannelDone counts them once the driver tried them.
func (s *Session) PollChannel(pipe uint64, dst []ChannelOut) (_ []ChannelOut, live bool) {
	p := s.pipe(pipe)
	if p == nil || p.out == nil || p.out.failed || s.state == StateClosed {
		return dst, false
	}
	d := s.dg
	if d == nil || len(d.send) == 0 || !s.peerSeen || s.state >= StateClosing || p != s.dgPipe() {
		return dst, true
	}
	ch := s.channelOf(p)
	if ch.room == 0 {
		return dst, true
	}
	kept := d.send[:0]
	for _, o := range d.send {
		ok, dead := o.ready()
		if dead || uint64(len(o.data)+1) > s.peerMaxDgram {
			s.stats.DatagramsDroppedSend++
			continue
		}
		if !ok || !ch.takes(len(o.data)) {
			kept = append(kept, o)
			continue
		}
		b := make([]byte, 0, wire.ChannelDatagramLen(ch.hasID, ch.id, len(o.data)))
		dst = append(dst, ChannelOut{Frame: wire.AppendChannelDatagram(b, ch.hasID, ch.id, o.data), d: outDgram{st: o.st, n: o.n}})
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
// its Pipe Stream ID removed (EXTENSIONS.md 6.8). It is not a record: it
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
	q   [][]byte // context-prefixed payloads
	sig *Signal
}

func (s *Session) dgState() *dgramState {
	if s.dg == nil {
		s.dg = &dgramState{}
	}
	return s.dg
}

// MaxDatagramPayload returns the largest session datagram payload the peer
// accepts now, 0 when it accepts none (EXTENSIONS.md 6.6).
func (s *Session) MaxDatagramPayload(flowLen int) int {
	if !s.peerSeen || s.peerMaxDgram == 0 {
		return 0
	}
	m := min64(s.peerMaxDgram, s.peerMaxRecord)
	if s.feat.Flows {
		m -= uint64(flowLen)
	}
	if m < 1 {
		return 0
	}
	return int(m - 1)
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
	var ch chanUse
	if p == s.dgPipe() {
		ch = s.channelOf(p)
	}
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
		if ch.takes(len(o.data)) {
			held = append(held, o)
			s.wake(p.chSig)
			continue
		}
		if !w.datagram(o.data) {
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
	return dst
}

// DeliverDatagram queues a datagram the receive limiter passed; drop
// counts one it did not.
func (s *Session) DeliverDatagram(p PendingDatagram, drop bool) {
	if drop {
		s.stats.DatagramsDroppedLimit++
		return
	}
	s.queueDatagram(p.st, p.e)
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
				// It overtook the stream's opening frame (EXTENSIONS.md
				// 6.4, 6.8): held for at most one smoothed RTT, and
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
	d.recvSize += d.held[len(d.held)-1].cost()
	s.trimRecvBytes()
}

// dropHeldHead drops the oldest held datagram.
func (s *Session) dropHeldHead() {
	d := s.dg
	d.recvSize -= d.held[0].cost()
	d.held[0] = heldDgram{}
	d.held = d.held[1:]
	s.dropRecv(1)
}

// releaseHeld delivers the datagrams held for a peer stream that opened,
// in arrival order.
func (s *Session) releaseHeld(st *Stream) {
	d := s.dg
	if d == nil || len(d.held) == 0 {
		return
	}
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
		d.limited = append(d.limited, PendingDatagram{st: st, e: e})
		return
	}
	s.queueDatagram(st, e)
}

// queueDatagram puts a datagram in its stream's or the session's queue.
func (s *Session) queueDatagram(st *Stream, e []byte) {
	d := s.dgState()
	if st != nil {
		if st.discarded || st.rcv.stopped {
			s.dropRecv(1)
			return
		}
		if st.dg == nil {
			st.dg = &streamDgrams{}
		}
		if len(st.dg.q) >= s.cfg.DgramStreamQ {
			d.recvSize -= uint64(len(st.dg.q[0]))
			st.dg.q = st.dg.q[1:]
			s.dropRecv(1)
			st.cnt.dgDropped++
		}
		st.cnt.dgReceived++
		st.dg.q = append(st.dg.q, e)
		d.recvSize += uint64(len(e))
		s.trimRecvBytes()
		s.wake(st.dg.sig)
		s.wake(st.rsig)
		return
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
}

// DropReceivedDatagrams counts n received datagrams that the host dropped
// before handing them to the session.
func (s *Session) DropReceivedDatagrams(n uint64) { s.dropRecv(n) }

func (s *Session) dropRecv(n uint64) {
	s.stats.DatagramsDroppedRecv += n
	s.emit(Event{Kind: EvDatagramDropped, Count: s.stats.DatagramsDroppedRecv})
}

// trimRecvBytes keeps received datagrams within the byte limit by dropping
// the oldest held for streams not opened yet, then the oldest of the
// session queue.
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
	e := st.dg.q[0]
	st.dg.q[0] = nil
	st.dg.q = st.dg.q[1:]
	if s.dg != nil {
		s.dg.recvSize -= uint64(len(e))
	}
	ctx, n := varint.ConsumeVarint(e)
	p := e[n:]
	s.stats.DatagramBytesReceived += uint64(len(p))
	st.cnt.dgBytesReceived += uint64(len(p))
	return ctx, p, true
}

// WaitStreamDatagram registers sig for a stream datagram.
func (s *Session) WaitStreamDatagram(st *Stream, sig *Signal) {
	if st.dg == nil {
		st.dg = &streamDgrams{}
	}
	st.dg.sig = sig
}

func (s *Session) dropStreamDgrams(st *Stream) {
	if s.dg != nil {
		for _, e := range st.dg.q {
			s.dg.recvSize -= uint64(len(e))
		}
	}
	st.dg = nil
}
