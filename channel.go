package zmux

import (
	"sync"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierio"
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
	// ChannelDatagramCapacity returns the structural payload limit and
	// the room available now, both including any Pipe Stream ID. A limit
	// of zero disables the channel. A negative limit means setup has not
	// determined the capacity yet. Only the room changes with path MTU.
	// The driver reads this snapshot outside the session lock.
	ChannelDatagramCapacity() (limit, room int)
	// SendChannelDatagram sends one channel datagram. It may block while
	// the carrier's own queue is full: a goroutine of the pipe calls it,
	// and the session's datagram queue drops its oldest datagrams
	// meanwhile. It retains nothing of b once it returns.
	SendChannelDatagram(b []byte) error
	// ReceiveChannelDatagrams starts handing the channel datagrams received
	// for the pipe, Pipe Stream ID removed, to deliver, one call at a time,
	// until stop is called. deliver takes the argument over: the channel
	// does not reuse its bytes. It does not wait for the session's lock.
	// It is called outside the session's lock, by the pipe's channel
	// goroutine when the pipe attaches; the pipe's first record waits for
	// it, so it must return promptly. A panic in it fails the pipe.
	ReceiveChannelDatagrams(deliver func(frame []byte)) (stop func())
}

// DatagramDeadlineSender lets a channel apply a deadline while waiting for
// space to accept a datagram. The deadline uses the session's clock.
type DatagramDeadlineSender interface {
	carrierio.DatagramDeadlineSender
}

// DatagramCapacityNotifier optionally signals invalidation of a channel's
// capacity snapshot. The channel returned for a generation closes when that
// generation changes. It must return a new channel after invalidation.
type DatagramCapacityNotifier interface {
	carrierio.DatagramCapacityNotifier
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

// startChannel starts the goroutine of the attachment's datagram channel
// (under the lock): it registers the channel's receiver, outside the lock
// (host code), then runs the sender. The lane-0 writer waits for the
// registration (chReady) before the attachment's first record, so that no
// datagram the peer sends once it learns the session's parameters arrives
// before the receiver.
func (c *Conn) startChannel(pd *pipeDrv) {
	if pd.ch == nil || !c.cfg.EnableDatagrams {
		return
	}
	pd.chSig = core.NewSignal()
	pd.chEnd = make(chan struct{})
	pd.chReady = make(chan struct{})
	c.s.SetChannelSignal(pd.id, pd.chSig)
	c.side.add()
	pd.gs.add()
	labels.Go(pd.lctx, "channel", func() {
		defer pd.gs.done()
		if pd.accountRead != nil {
			defer func() {
				c.chIn.mu.Lock()
				pd.channelAccountingClosed = true
				c.chIn.mu.Unlock()
			}()
		}
		var stop func()
		err := hostCall(c.cfg.Logger, "DatagramChannel.ReceiveChannelDatagrams", func() error {
			stop = pd.ch.ReceiveChannelDatagrams(func(frame []byte) { c.receiveChannel(pd, frame) })
			return nil
		})
		close(pd.chReady)
		if err != nil {
			c.side.done()
			pd.closeAll() // a carrier whose channel panicked fails
			return
		}
		if stop != nil {
			defer safeLog(c.cfg.Logger, "DatagramChannel stop", stop)
		}
		c.channelLoop(pd)
	})
}

// channelUse reads what the attachment's datagram channel takes now.
func (pd *pipeDrv) channelUse() core.ChannelUse {
	limit, room := pd.ch.ChannelDatagramCapacity()
	u := core.ChannelUse{Limit: limit}
	if limit > 0 {
		u.Room = min(max(room, 0), limit)
		u.ID, u.HasID = pd.ch.ChannelStreamID()
	}
	return u
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
	defer c.side.done()
	defer func() {
		if p := recover(); p != nil {
			c.fail(p)
		}
	}()
	var out []core.ChannelOut
	for {
		// Deferred unlocks turn a panic of the core into a closed session
		// (Conn.unlock) instead of leaving the lock held.
		// The channel is host code: read outside the lock.
		changed := carrierio.ChannelCapacityChanged(pd.ch)
		use := pd.channelUse()
		live := false
		func() {
			c.lock()
			defer c.unlock()
			if !pd.dead {
				out, live = c.s.PollChannel(pd.id, use, out[:0], c.clk.now())
			}
		}()
		if !live {
			return
		}
		var totals RecordTotals
		for i := range out {
			out[i].Expired = out[i].Deadline != core.Never && c.clk.now() >= out[i].Deadline
			returned := false
			out[i].Failed = out[i].Expired || hostCall(c.cfg.Logger, "DatagramChannel.SendChannelDatagram", func() error {
				var deadline time.Time
				if out[i].Deadline != core.Never {
					deadline = c.clk.wall(out[i].Deadline)
				}
				err := carrierio.SendChannelDatagram(pd.ch, out[i].Frame, deadline)
				returned = true
				return err
			}) != nil
			out[i].Unknown = !out[i].Expired && !returned
			if pd.accountWrite != nil && !out[i].Failed {
				totals.ChannelBytes += uint64(len(out[i].Frame))
				totals.ChannelDatagrams++
			}
		}
		if len(out) > 0 {
			func() {
				c.lock()
				defer c.unlock()
				if totals.ChannelDatagrams != 0 {
					pd.accountWrite.add(totals, false)
				}
				c.s.ChannelDone(out, c.clk.now())
			}()
			clear(out)
		}
		if len(out) > 0 {
			continue
		}
		select {
		case <-pd.chSig.C:
		case <-changed:
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
	q        []chInDgram
	spare    []chInDgram
	dropped  uint64
	mu       sync.Mutex
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
	if pd.accountRead != nil && !pd.channelAccountingClosed {
		pd.accountRead.add(RecordTotals{ChannelBytes: uint64(len(frame)), ChannelDatagrams: 1}, false)
	}
	if len(in.q) >= c.chInMax {
		in.dropped++
		in.mu.Unlock()
		return
	}
	in.q = append(in.q, chInDgram{pd, frame})
	start := !in.draining
	in.draining = true
	in.mu.Unlock()
	if start && !c.goSide(c.lctx, "channel-drain", c.drainChannel) {
		// The session ended: nothing will take the queue.
		in.mu.Lock()
		clear(in.q)
		in.q, in.dropped, in.draining = in.q[:0], 0, false
		in.mu.Unlock()
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
		c.locked(func() {
			if dropped > 0 {
				c.s.DropReceivedDatagrams(dropped)
			}
			for _, d := range q {
				if !d.pd.dead {
					c.s.ReceiveChannelDatagram(d.pd.id, d.frame, now)
				}
			}
		})
		clear(q)
		in.mu.Lock()
		in.spare = q[:0]
		in.mu.Unlock()
		c.passDatagrams()
	}
}
