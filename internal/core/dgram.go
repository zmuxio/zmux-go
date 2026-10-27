package core

import "github.com/zmuxio/zmux-go/internal/wire"

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
}

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
	_, n := wire.ReadVarint(p.e)
	return len(p.e) - n
}

// outDgram is a queued datagram: its encoded data (flow prefix + payload)
// and, for a stream datagram, its stream, whose opening frame it must not
// overtake (EXTENSIONS.md 6.4).
type outDgram struct {
	data []byte
	st   *Stream
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

func (d *dgramState) sendable(s *Session) bool {
	if !s.peerSeen {
		return false
	}
	for i := range d.send {
		if ok, dead := d.send[i].ready(); ok || dead {
			return true
		}
	}
	return false
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
		prefix = wire.AppendVarint(prefix, st.id+1)
		prefix = wire.AppendVarint(prefix, context)
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
	d.send = append(d.send, outDgram{data: data, st: st})
	s.wakeAllWriters()
	return nil
}

// writeDatagrams puts queued datagrams into records.
func (s *Session) writeDatagrams(w *recWriter) {
	d := s.dg
	held := d.send[:0] // in place: datagrams waiting for their stream's opening frame
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
		if !w.datagram(o.data) {
			break
		}
		s.stats.DatagramsSent++
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

// onDatagram processes a received DATAGRAM.
func (s *Session) onDatagram(f *wire.Frame) *wire.Error {
	data := f.Data
	s.dgState()
	if s.feat.Flows {
		flow, n := wire.ReadVarint(data)
		if n == 0 {
			return wire.ErrProto("truncated datagram flow")
		}
		data = data[n:]
		if flow > 0 {
			ctx, cn := wire.ReadVarint(data)
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
			if st == nil || st.discarded || st.rcv.stopped {
				s.dropRecv(1)
				return nil
			}
			e := make([]byte, 0, 8+len(data)-cn)
			e = wire.AppendVarint(e, ctx)
			e = append(e, data[cn:]...)
			s.stats.DatagramsReceived++
			s.receivedDatagram(st, e)
			return nil
		}
	}
	s.stats.DatagramsReceived++
	s.receivedDatagram(nil, append([]byte(nil), data...))
	return nil
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
		}
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

func (s *Session) dropRecv(n uint64) {
	s.stats.DatagramsDroppedRecv += n
	s.emit(Event{Kind: EvDatagramDropped, Count: s.stats.DatagramsDroppedRecv})
}

// trimRecvBytes keeps received datagrams within the byte limit by dropping
// the oldest of the session queue.
func (s *Session) trimRecvBytes() {
	d := s.dg
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
	ctx, n := wire.ReadVarint(e)
	return ctx, e[n:], true
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
