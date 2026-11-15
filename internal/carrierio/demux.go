package carrierio

import (
	"sync"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// ChannelDemux routes the channel datagrams of one carrier connection
// shared by several pipes, such as a QUIC connection or a WebTransport
// session, to those pipes by their Pipe Stream ID. An
// adapter runs one receive loop per connection that passes every datagram
// to Deliver, and its pipes' ReceiveChannelDatagrams register with
// Register. The pipes may belong to several sessions. The zero value is
// ready to use.
type ChannelDemux struct {
	pipes   map[uint64]*channelSink
	dropped atomic.Uint64
	mu      sync.Mutex
}

// channelSink is one registration. stop takes mu exclusively, so it
// returns only once no Deliver runs deliver, and none does after: deliver
// runs under mu shared (and must not call stop itself).
type channelSink struct {
	deliver func([]byte)
	mu      sync.RWMutex
	closed  bool
}

// Register hands the datagrams for Pipe Stream ID id to deliver until stop
// is called, which returns once no call of deliver runs; a later Register
// of the same ID replaces it. deliver takes the frame over, as
// zmux.DatagramChannel's deliver does.
func (d *ChannelDemux) Register(id uint64, deliver func(frame []byte)) (stop func()) {
	k := &channelSink{deliver: deliver}
	d.mu.Lock()
	if d.pipes == nil {
		d.pipes = map[uint64]*channelSink{}
	}
	d.pipes[id] = k
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		if d.pipes[id] == k {
			delete(d.pipes, id)
		}
		d.mu.Unlock()
		k.mu.Lock()
		k.closed = true
		k.mu.Unlock()
	}
}

// Deliver routes one channel datagram to the pipe its Pipe Stream ID
// names, handing the pipe's deliver the bytes of b after the ID: Deliver
// takes b over, and its caller must not reuse them. One that names no
// registered pipe (unknown, not yet attached, or closed) or whose ID is
// truncated is dropped and counted.
func (d *ChannelDemux) Deliver(b []byte) {
	id, frame, ok := wire.SplitChannelDatagram(b)
	var k *channelSink
	if ok {
		d.mu.Lock()
		k = d.pipes[id]
		d.mu.Unlock()
	}
	if k == nil {
		d.dropped.Add(1)
		return
	}
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		d.dropped.Add(1)
		return
	}
	k.deliver(frame) // deliver takes frame over: it is part of b, which Deliver was given
}

// Dropped returns the datagrams Deliver dropped because they named no
// registered pipe.
func (d *ChannelDemux) Dropped() uint64 { return d.dropped.Load() }
