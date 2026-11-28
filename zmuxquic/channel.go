package zmuxquic

import (
	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go/zmuxquic/streamio"
)

// The datagram channel of QUIC stream pipes: QUIC
// DATAGRAM frames of the connection, each starting with the Pipe Stream
// ID, the QUIC stream ID of the pipe. One receive loop per connection
// routes them to the pipes on it, which may belong to several sessions;
// lanes are not pipes and have no channel.

var connChannels = streamio.NewDatagramRegistry(0, "quic-channel", func(qc *quic.Conn) bool {
	return streamio.SupportsDatagrams(qc.ConnectionState())
})

// ChannelStreamID is the pipe's QUIC stream ID (zmux.DatagramChannel).
func (q *qStream) ChannelStreamID() (uint64, bool) {
	_, s := q.cur()
	return uint64(s.StreamID()), true
}

// ChannelDatagramCapacity returns the connection's limit and current room.
func (q *qStream) ChannelDatagramCapacity() (limit, room int) {
	qc, _ := q.cur()
	return connChannels.Channel(qc).Capacity()
}

// ChannelCapacityChanged wakes pipe writers sharing an updated MTU.
func (q *qStream) ChannelCapacityChanged() <-chan struct{} {
	qc, _ := q.cur()
	return connChannels.Channel(qc).Changed()
}

// SendChannelDatagram sends a QUIC datagram of the connection.
func (q *qStream) SendChannelDatagram(b []byte) error {
	qc, _ := q.cur()
	return connChannels.Channel(qc).Send(b)
}

// ReceiveChannelDatagrams receives the connection's datagrams for the pipe.
func (q *qStream) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	qc, s := q.cur()
	return connChannels.Channel(qc).Register(uint64(s.StreamID()), deliver)
}

// ChannelDropped returns the datagrams received on qc that named no pipe
// attached to a session (an unknown, not yet attached or closed one).
func ChannelDropped(qc *quic.Conn) uint64 {
	return connChannels.Dropped(qc)
}
