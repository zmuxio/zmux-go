package carrier

import (
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// ChannelDemux routes the channel datagrams of one carrier connection
// shared by several pipes, such as a QUIC connection or a WebTransport
// session, to those pipes (zmux.DatagramChannel) by their Pipe Stream ID.
// An adapter runs one receive loop per connection that passes every
// datagram to Deliver, and its pipes' ReceiveChannelDatagrams register
// with Register. The pipes may belong to several sessions. The zero value
// is ready to use.
type ChannelDemux struct{ d carrierio.ChannelDemux }

// Register hands the datagrams for Pipe Stream ID id to deliver until stop
// is called; a later Register of the same ID replaces it.
func (d *ChannelDemux) Register(id uint64, deliver func(frame []byte)) (stop func()) {
	return d.d.Register(id, deliver)
}

// Deliver routes one channel datagram to the pipe its Pipe Stream ID
// names; one that names no registered pipe (unknown, not yet attached, or
// closed) or whose ID is truncated is dropped and counted.
func (d *ChannelDemux) Deliver(b []byte) { d.d.Deliver(b) }

// Dropped returns the datagrams Deliver dropped because they named no
// registered pipe.
func (d *ChannelDemux) Dropped() uint64 { return d.d.Dropped() }

// AppendChannelDatagram appends the channel datagram of one DATAGRAM frame
// with data, after the Pipe Stream ID id when withID.
func AppendChannelDatagram(b []byte, withID bool, id uint64, data []byte) []byte {
	return wire.AppendChannelDatagram(b, withID, id, data)
}
