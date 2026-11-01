package zmux

import (
	"sync"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// DatagramChannel is the datagram channel of a pipe's carrier, an optional
// capability of a PipeIO half (EXTENSIONS.md 6.8): an unreliable datagram
// service of the carrier besides the reliable stream that is the pipe, such
// as QUIC DATAGRAM frames of the connection a QUIC stream belongs to, the
// HTTP Datagrams of an HTTP/3 request stream, or the datagrams of a
// WebTransport session. When both endpoints negotiate it, a session sends
// a DATAGRAM frame that fits the channel on it instead of in a record, so
// that a lost carrier packet loses that datagram instead of delaying every
// later one; nothing changes for the application. A session uses the
// channel only when it enables datagrams.
type DatagramChannel interface {
	// ChannelStreamID returns the Pipe Stream ID that starts the pipe's
	// channel datagrams: the carrier stream ID of the pipe on channels
	// shared by the streams of a connection (QUIC, WebTransport); ok is
	// false on a channel of the pipe's own stream (HTTP/3).
	ChannelStreamID() (id uint64, ok bool)
	// MaxChannelDatagram returns the largest channel datagram, Pipe Stream
	// ID included, that the channel takes now; 0 while it takes none (the
	// carrier did not negotiate datagrams, or has not yet). It must be
	// cheap and must not block: it is called under the session's lock.
	MaxChannelDatagram() int
	// SendChannelDatagram sends one channel datagram. It may block while
	// the carrier's own queue is full: a goroutine of the pipe calls it,
	// and the session's datagram queue drops its oldest datagrams
	// meanwhile. It retains nothing of b once it returns.
	SendChannelDatagram(b []byte) error
	// ReceiveChannelDatagrams starts handing the channel datagrams received
	// for the pipe, Pipe Stream ID removed, to deliver, one call at a time,
	// until stop is called. deliver takes the argument over: the channel
	// does not reuse its bytes. It does not wait for the session's lock.
	// It is called under the session's lock, when the pipe attaches and
	// before its first record, so it must not block.
	ReceiveChannelDatagrams(deliver func(frame []byte)) (stop func())
}

// ChannelDemux routes the channel datagrams of one carrier connection
// shared by several pipes, such as a QUIC connection or a WebTransport
// session, to those pipes by their Pipe Stream ID (EXTENSIONS.md 6.8). An
// adapter runs one receive loop per connection that passes every datagram
// to Deliver, and its pipes' ReceiveChannelDatagrams register with
// Register. The pipes may belong to several sessions. The zero value is
// ready to use.
type ChannelDemux struct {
	mu      sync.Mutex
	pipes   map[uint64]*channelSink
	dropped atomic.Uint64
}

type channelSink struct{ deliver func([]byte) }

// Register hands the datagrams for Pipe Stream ID id to deliver until stop
// is called; a later Register of the same ID replaces it.
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
	}
}

// Deliver routes one channel datagram to the pipe its Pipe Stream ID
// names; one that names no registered pipe (unknown, not yet attached, or
// closed) or whose ID is truncated is dropped and counted.
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
	k.deliver(frame)
}

// Dropped returns the datagrams Deliver dropped because they named no
// registered pipe.
func (d *ChannelDemux) Dropped() uint64 { return d.dropped.Load() }

// AppendChannelDatagram appends the channel datagram of one DATAGRAM frame
// with data, after the Pipe Stream ID id when withID (EXTENSIONS.md 6.8).
func AppendChannelDatagram(b []byte, withID bool, id uint64, data []byte) []byte {
	return wire.AppendChannelDatagram(b, withID, id, data)
}

// channelOf returns the datagram channel of a carrier, if a half offers
// one.
func channelOf(p PipeIO) DatagramChannel {
	if ch, ok := p.W.(DatagramChannel); ok {
		return ch
	}
	if ch, ok := p.R.(DatagramChannel); ok {
		return ch
	}
	return nil
}

// startChannel registers the receiver of the attachment's datagram channel
// and starts its sender (under the lock). It runs before the attachment's
// first record, so that no datagram the peer sends once it learns the
// session's parameters arrives before the receiver.
func (c *Conn) startChannel(pd *pipeDrv) {
	if pd.ch == nil || !c.cfg.EnableDatagrams {
		return
	}
	pd.chSig = core.NewSignal()
	pd.chEnd = make(chan struct{})
	c.s.SetChannelSignal(pd.id, pd.chSig)
	stop := pd.ch.ReceiveChannelDatagrams(func(frame []byte) { c.receiveChannel(pd, frame) })
	c.wg.Add(1)
	pd.gs.add()
	go func() {
		defer pd.gs.done()
		defer stop()
		c.channelLoop(pd)
	}()
}

// endChannel ends the attachment's channel sender and receiver.
func (pd *pipeDrv) endChannel() {
	if pd.chEnd != nil {
		pd.chOnce.Do(func() { close(pd.chEnd) })
	}
}

// channelLoop sends what the core puts on the pipe's datagram channel
// until the pipe or the session ends; the receiver registered by
// startChannel is stopped when it returns. It owns the channel's sending
// side.
func (c *Conn) channelLoop(pd *pipeDrv) {
	defer c.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			c.fail(p)
		}
	}()
	var out []core.ChannelOut
	for {
		c.lock()
		if pd.dead {
			c.unlock()
			return
		}
		var live bool
		out, live = c.s.PollChannel(pd.id, out[:0])
		c.unlock()
		if !live {
			return
		}
		for i := range out {
			out[i].Failed = pd.ch.SendChannelDatagram(out[i].Frame) != nil
			out[i].Frame = nil
		}
		if len(out) > 0 {
			c.lock()
			c.s.ChannelDone(out)
			c.unlock()
			clear(out)
		}
		if len(out) > 0 {
			continue
		}
		select {
		case <-pd.chSig.C:
		case <-pd.chEnd:
			return
		case <-c.done:
			return
		}
	}
}

// chIn is the hand-off of received channel datagrams to a session: a
// connection's receive loop, which may serve the pipes of several
// sessions, queues a datagram without waiting for the session's lock, and
// one drainer at a time processes the queue under it. The queue holds at
// most the session's datagram receive queue length; beyond it a datagram
// is dropped and counted (DatagramsDroppedRecv).
type chIn struct {
	mu       sync.Mutex
	q        []chInDgram
	spare    []chInDgram
	dropped  uint64
	draining bool
}

type chInDgram struct {
	pd    *pipeDrv
	frame []byte
}

// receiveChannel hands a channel datagram received for the pipe to the
// session.
func (c *Conn) receiveChannel(pd *pipeDrv, frame []byte) {
	in := &c.chIn
	in.mu.Lock()
	if len(in.q) >= c.chInMax {
		in.dropped++
		in.mu.Unlock()
		return
	}
	in.q = append(in.q, chInDgram{pd, frame})
	start := !in.draining
	in.draining = true
	in.mu.Unlock()
	if start {
		go c.drainChannel()
	}
}

// drainChannel processes the queued channel datagrams until none is left.
func (c *Conn) drainChannel() {
	in := &c.chIn
	for {
		in.mu.Lock()
		q, dropped := in.q, in.dropped
		if len(q) == 0 && dropped == 0 {
			in.draining = false
			in.mu.Unlock()
			return
		}
		in.q, in.spare, in.dropped = in.spare[:0], nil, 0
		in.mu.Unlock()
		now := c.clk.now()
		c.lock()
		if dropped > 0 {
			c.s.DropReceivedDatagrams(dropped)
		}
		for _, d := range q {
			if !d.pd.dead {
				c.s.ReceiveChannelDatagram(d.pd.id, d.frame, now)
			}
		}
		c.unlock()
		clear(q)
		in.mu.Lock()
		in.spare = q[:0]
		in.mu.Unlock()
		c.passDatagrams()
	}
}
