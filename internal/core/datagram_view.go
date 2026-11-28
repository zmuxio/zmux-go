package core

import "github.com/zmuxio/zmux-go/internal/wire"

// dgramView separates the target hop's small header from an owned
// application payload. Copies hold the same sealed payload and sequence.
type dgramView struct {
	buf, storage  *Buf
	head, payload []byte
}

func (d dgramView) len() int { return len(d.head) + len(d.payload) }

func (d dgramView) cost() int {
	if d.storage != nil {
		n := cap(d.storage.B)
		if d.buf != nil {
			n += cap(d.buf.B)
		}
		return n
	}
	if d.buf != nil {
		return cap(d.head) + cap(d.buf.B)
	}
	return cap(d.head) + cap(d.payload)
}

func (d dgramView) ref() {
	if d.storage != nil {
		d.storage.Ref()
	}
	if d.buf != nil {
		d.buf.Ref()
	}
}

func (d dgramView) unref() {
	if d.storage != nil {
		d.storage.Unref()
	}
	if d.buf != nil {
		d.buf.Unref()
	}
}

// channelDatagram builds the native transport's final contiguous message.
func (d dgramView) channelDatagram(ch ChannelUse, pad int) []byte {
	b := make([]byte, 0, wire.ChannelDatagramLen(ch.HasID, ch.ID, d.len())+pad)
	b = wire.AppendPaddedChannelDatagram(b, ch.HasID, ch.ID, d.head, pad)
	return append(b, d.payload...)
}

func (h *Handoff) hold() *Handoff {
	if h != nil {
		h.refs++
	}
	return h
}

func (h *Handoff) datagramDone(ok bool) {
	if h == nil {
		return
	}
	if ok {
		h.datagramAccepted(true)
		h.admit(0, h.size)
	}
	h.unref()
}
