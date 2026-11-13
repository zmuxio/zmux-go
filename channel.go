package zmux

import (
	"sync"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// DatagramChannel is the datagram channel of a pipe's carrier, an optional
// capability of a PipeIO half: an unreliable datagram
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

// channelOf returns the datagram channel of a carrier, if a half offers
// one.
func channelOf(p PipeIO) DatagramChannel {
	if ch, ok := p.W.(DatagramChannel); ok {
		return ch
	}
	if ch, ok := unprefixed(p.R).(DatagramChannel); ok {
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
	labels.Go(pd.lctx, "channel", func() {
		defer pd.gs.done()
		defer stop()
		c.channelLoop(pd)
	})
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
		// Deferred unlocks turn a panic of the core into a closed session
		// (Conn.unlock) instead of leaving the lock held.
		live := false
		func() {
			c.lock()
			defer c.unlock()
			if !pd.dead {
				out, live = c.s.PollChannel(pd.id, out[:0])
			}
		}()
		if !live {
			return
		}
		for i := range out {
			out[i].Failed = pd.ch.SendChannelDatagram(out[i].Frame) != nil
			out[i].Frame = nil
		}
		if len(out) > 0 {
			func() {
				c.lock()
				defer c.unlock()
				c.s.ChannelDone(out)
			}()
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
		labels.Go(c.lctx, "channel-drain", func() { c.drainChannel() })
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
