package zmuxquic

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

// The datagram channel of QUIC stream pipes (EXTENSIONS.md 6.8): QUIC
// DATAGRAM frames of the connection, each starting with the Pipe Stream
// ID, the QUIC stream ID of the pipe. One receive loop per connection
// routes them to the pipes on it, which may belong to several sessions;
// lanes are not pipes and have no channel.

// sizeProbe is larger than any QUIC datagram: sending it is always refused
// with the connection's current maximum, and nothing is sent.
var sizeProbe = make([]byte, 1<<16)

// sizeRefresh bounds how stale the maximum a channel reports may be: the
// path MTU, and with it the maximum, may grow.
const sizeRefresh = 250 * time.Millisecond

// connChannel is the datagram channel of one QUIC connection.
type connChannel struct {
	qc       *quic.Conn
	demux    zmux.ChannelDemux
	recvOnce sync.Once
	max      atomic.Int64 // largest QUIC datagram payload; 0: none
	probedAt atomic.Int64 // monotonic nanoseconds of the last probe
}

var connChannels sync.Map // *quic.Conn -> *connChannel

var epoch = time.Now()

// channelOf returns the datagram channel of qc, created on first use and
// forgotten when qc ends.
func channelOf(qc *quic.Conn) *connChannel {
	if c, ok := connChannels.Load(qc); ok {
		return c.(*connChannel)
	}
	c, loaded := connChannels.LoadOrStore(qc, &connChannel{qc: qc})
	if !loaded {
		context.AfterFunc(qc.Context(), func() { connChannels.Delete(qc) })
	}
	return c.(*connChannel)
}

// maxDatagram returns the largest QUIC datagram payload the connection
// takes now, 0 while datagrams are not negotiated, refreshed at most once
// per sizeRefresh. A probe does not block and sends nothing.
func (c *connChannel) maxDatagram() int {
	now := int64(time.Since(epoch))
	if last := c.probedAt.Load(); last != 0 && now-last < int64(sizeRefresh) {
		return int(c.max.Load())
	}
	c.probedAt.Store(now)
	st := c.qc.ConnectionState()
	if !st.SupportsDatagrams.Local || !st.SupportsDatagrams.Remote {
		c.max.Store(0)
		return 0
	}
	c.learn(c.qc.SendDatagram(sizeProbe))
	return int(c.max.Load())
}

// learn takes the current maximum from a refused datagram.
func (c *connChannel) learn(err error) {
	if tl, ok := errors.AsType[*quic.DatagramTooLargeError](err); ok {
		c.max.Store(max(tl.MaxDatagramPayloadSize, 0))
	}
}

// send sends one QUIC datagram; it blocks while the connection's datagram
// queue is full.
func (c *connChannel) send(b []byte) error {
	err := c.qc.SendDatagram(b)
	c.learn(err)
	return err
}

// register routes the datagrams for the stream id to deliver, starting the
// connection's receive loop on first use.
func (c *connChannel) register(id uint64, deliver func([]byte)) func() {
	c.recvOnce.Do(func() {
		if !c.qc.ConnectionState().SupportsDatagrams.Local {
			return
		}
		go func() {
			for {
				b, err := c.qc.ReceiveDatagram(c.qc.Context())
				if err != nil {
					return
				}
				c.demux.Deliver(b)
			}
		}()
	})
	return c.demux.Register(id, deliver)
}

// ChannelStreamID is the pipe's QUIC stream ID (zmux.DatagramChannel).
func (q *qStream) ChannelStreamID() (uint64, bool) {
	_, s := q.cur()
	return uint64(s.StreamID()), true
}

// MaxChannelDatagram is the connection's current largest datagram.
func (q *qStream) MaxChannelDatagram() int {
	qc, _ := q.cur()
	return channelOf(qc).maxDatagram()
}

// SendChannelDatagram sends a QUIC datagram of the connection.
func (q *qStream) SendChannelDatagram(b []byte) error {
	qc, _ := q.cur()
	return channelOf(qc).send(b)
}

// ReceiveChannelDatagrams receives the connection's datagrams for the pipe.
func (q *qStream) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	qc, s := q.cur()
	return channelOf(qc).register(uint64(s.StreamID()), deliver)
}

// ChannelDropped returns the datagrams received on qc that named no pipe
// attached to a session (an unknown, not yet attached or closed one).
func ChannelDropped(qc *quic.Conn) uint64 {
	if c, ok := connChannels.Load(qc); ok {
		return c.(*connChannel).demux.Dropped()
	}
	return 0
}
