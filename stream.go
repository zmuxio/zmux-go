package zmux

import (
	"context"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// handle is the application's side of one core stream. rmu and wmu
// serialise reads and writes (lock level L0); close, cancel and deadline
// changes never take them, so they can interrupt a blocked call.
type handle struct {
	c  *Conn
	st *core.Stream
	// orphanNext links the handle into Conn.orphans once it was collected
	// unclosed; written before the push publishes it.
	orphanNext *handle
	dl         atomic.Pointer[deadlines] // nil until a deadline is set
	rsig       core.Signal               // lazy: its channel is made by the first wait
	wsig       core.Signal
	dsig       core.Signal // the stream's datagrams
	dmu        sync.Mutex  // serialises datagram reads (lock level L0, after rmu)
	rmu        sync.Mutex
	wmu        sync.Mutex
	rwaiters   atomic.Int32 // WaitReadable calls and the reads inside their wait, for passing the wake on
	// rnote is the pending NotifyReadable call, nil while there is none.
	rnote atomic.Pointer[readNote]
}

// newHandle makes the handle of st (under the lock).
func (c *Conn) newHandle(st *core.Stream) *handle {
	h := &handle{c: c, st: st}
	h.rsig.Hook = h.readChanged
	c.handles++
	st.SetSignals(&h.rsig, &h.wsig, &h.dsig)
	return h
}

// cleanupHandle runs on the runtime's cleanup goroutine when a handle is
// collected without being closed. That goroutine belongs to no session
// (nor to a synctest bubble the session runs in), so it only queues the
// handle with one atomic push: no channel, timer or lock of the session.
// The session releases it (reset(0) and stop(0)) in its next critical
// section, at the latest at its orphan sweep (Conn.orphanSweepLocked).
func cleanupHandle(h *handle) { h.c.orphans.push(h) }

// deadlines are a handle's read and write deadlines, allocated when the
// first one is set.
type deadlines struct{ r, w deadline }

func (h *handle) deadlines() *deadlines {
	if d := h.dl.Load(); d != nil {
		return d
	}
	d := &deadlines{}
	d.r.expired = h.rsig.Hook // the read deadline makes the stream readable
	h.dl.CompareAndSwap(nil, d)
	return h.dl.Load()
}

// rwait and wwait return the channels a blocked read or write selects on:
// nil while no deadline was ever set (setting one wakes the call, which
// then selects on the new channel).
func (h *handle) rwait() chan struct{} {
	if d := h.dl.Load(); d != nil {
		return d.r.wait()
	}
	return nil
}

func (h *handle) wwait() chan struct{} {
	if d := h.dl.Load(); d != nil {
		return d.w.wait()
	}
	return nil
}

// setDeadline moves a deadline and wakes a call blocked without one.
func (h *handle) setDeadline(read, write bool, t time.Time) {
	d := h.deadlines()
	if read {
		d.r.set(t, h.c.clk.source())
	}
	if write {
		d.w.set(t, h.c.clk.source())
	}
	// The one wake of a slot outside the core: deadlines are the handle's
	// own condition, which the core knows nothing of.
	h.c.locked(func() {
		if read {
			h.rsig.FireLocked()
			h.dsig.FireLocked()
		}
		if write {
			h.wsig.FireLocked()
		}
	})
}

// released ends the handle's share of the session once the core released
// its stream (Session.Release, ReleaseSend, ReleaseRecv, Reject report it),
// under the lock.
func (h *handle) released(now bool) {
	if now {
		h.c.handles--
		h.stopDeadlines()
	}
}

func (h *handle) stopDeadlines() {
	if d := h.dl.Load(); d != nil {
		d.r.stop()
		d.w.stop()
	}
}

// ---- waiting ----

// waitOn runs try under the session lock until it reports done or fails,
// waiting between tries for the core to wake one of the handle's slots
// (a, and b when given: what the try waits for changed), ctx or the
// deadline dl. A try that is not done is followed, under the same lock,
// by drain: it empties the slots of tokens fired for changes the try
// already saw, so a change after the unlock wakes them again, and a lazy
// slot gets its channel only once a wait needs it. When the critical
// section panics the session is closed (unlock) and its error returned.
// It returns the try's error as the API's, or (cut) the end of ctx or dl.
func (h *handle) waitOn(ctx context.Context, a, b *core.Signal, dl func() chan struct{}, try func() (bool, error), wait func()) (cut bool, err error) {
	c := h.c
	done := ctx.Done()
	for {
		var ok, ran bool
		c.locked(func() {
			if ok, err = try(); !ok && err == nil {
				a.Drain()
				if b != nil {
					b.Drain()
				}
				if wait != nil {
					wait()
				}
			}
			ran = true
		})
		switch {
		case !ran:
			return false, c.closedErr()
		case ok || err != nil:
			return false, apiErr(err)
		}
		var bc <-chan struct{}
		if b != nil {
			bc = b.C
		}
		select {
		case <-a.C:
		case <-bc:
		case <-done:
			return true, ctx.Err()
		case <-dl():
			return true, os.ErrDeadlineExceeded
		}
	}
}

// awaitRead waits for the receiving part (and with dgram for the stream's
// datagrams too) as waitOn does, within the read deadline. Every read that
// waits goes through it.
func (h *handle) awaitRead(ctx context.Context, dgram bool, try func() (bool, error)) error {
	var b *core.Signal
	if dgram {
		b = &h.dsig
	}
	h.rwaiters.Add(1)
	_, err := h.waitOn(ctx, &h.rsig, b, h.rwait, try, nil)
	h.passReadWake(1)
	h.rwaiters.Add(-1)
	return err
}

// passReadWake hands the receiving part's one wake token on to the other
// waiters: the token a waiter or read consumed may be the arrival the
// others wait for too, and the slot holds a single token. A woken waiter
// that finds nothing readable drains it again, so passing costs only a
// spurious pass of the loop. others counts the callers' own entries in rwaiters.
func (h *handle) passReadWake(others int32) {
	if h.rwaiters.Load() > others {
		h.c.locked(h.rsig.FireLocked)
	}
}

// awaitWrite waits for the sending part as waitOn does, within the write
// deadline. With room, a try that is not done waits for send-buffer room:
// the stream counts as a waiting writer (core WaitWrite) until the core
// ends that wait (the write took its last byte, failed, found room or the
// part ended), or this does when ctx or the deadline cut it. Every write
// that waits goes through it; the caller holds wmu, so no other write
// shares the slot.
func (h *handle) awaitWrite(ctx context.Context, room bool, try func() (bool, error)) error {
	c := h.c
	var wait func()
	if room {
		wait = func() { c.s.WaitWrite(h.st) }
	}
	cut, err := h.waitOn(ctx, &h.wsig, nil, h.wwait, func() (bool, error) {
		c.s.SetNow(c.clk.now())
		return try()
	}, wait)
	if cut && room {
		c.locked(func() { c.s.EndWaitWrite(h.st) }) // the one end of the wait the core cannot see
	}
	return err
}

// ---- reading ----

func (h *handle) read(p []byte) (int, error) {
	h.rmu.Lock()
	defer h.rmu.Unlock()
	c := h.c
	n, eof := 0, false
	err := h.awaitRead(context.Background(), false, func() (bool, error) {
		var fin bool
		var err error
		n, fin, err = c.s.Read(h.st, p)
		if fin || err != nil {
			h.released(c.s.ReleaseRecv(h.st))
		}
		eof = fin && n == 0
		return n > 0 || fin || len(p) == 0, err
	})
	switch {
	case n > 0:
		return n, nil
	case eof:
		return 0, io.EOF
	}
	return 0, err
}

// tryRead is read without waiting: what is readable now, io.EOF at the
// FIN once everything was read, the part's error, os.ErrDeadlineExceeded
// once the read deadline passed with nothing readable, and ErrWouldBlock
// when nothing is readable yet or another read is in progress.
func (h *handle) tryRead(p []byte) (int, error) {
	if !h.rmu.TryLock() {
		return 0, ErrWouldBlock
	}
	defer h.rmu.Unlock()
	c := h.c
	c.lock()
	defer c.unlock()
	n, fin, err := c.s.Read(h.st, p)
	if fin || err != nil {
		h.released(c.s.ReleaseRecv(h.st))
	}
	switch {
	case n > 0:
		return n, nil
	case fin:
		return 0, io.EOF
	case err != nil:
		return 0, apiErr(err)
	case len(p) == 0:
		return 0, nil
	case h.readDeadlinePassed():
		return 0, os.ErrDeadlineExceeded
	}
	return 0, ErrWouldBlock
}

// readDeadlinePassed reports whether the read deadline has passed.
func (h *handle) readDeadlinePassed() bool {
	d := h.dl.Load()
	return d != nil && isClosed(d.r.wait())
}

// readNote is a pending NotifyReadable call. done is set by the one that
// wins it: the call of f or stop.
type readNote struct {
	f    func()
	h    *handle
	done atomic.Bool
}

// notifyReadable registers f for the next time the receiving part is
// readable (ReadReady, the same test as WaitReadable and Read) or the read
// deadline passes. The note is published before the test, under the
// session lock: a change after the test queues rsig, whose Hook
// (readChanged) runs after the unlock and sees the note, and a deadline
// that passes after it calls the same hook. Every event tests again, so
// none is lost to a token another waiter took.
func (h *handle) notifyReadable(f func()) func() bool {
	n := &readNote{f: f, h: h}
	if !h.rnote.CompareAndSwap(nil, n) {
		panic("zmux: NotifyReadable while a call is pending")
	}
	ready := h.readDeadlinePassed()
	h.c.peek(func() {
		h.rsig.Make() // the core wakes (and FireAll calls the Hook of) a slot with its channel only
		if !ready {
			r, end := h.c.s.ReadReady(h.st)
			ready = r > 0 || end
		}
	})
	if ready {
		n.fire()
	}
	return n.stop
}

// readChanged is rsig's Hook and the read deadline's expiry: it calls the
// pending note's f if the part is readable now. It runs outside the
// session lock.
func (h *handle) readChanged() {
	if n := h.rnote.Load(); n != nil && h.readableNow() {
		n.fire()
	}
}

// readableNow reports whether TryRead would return anything but
// ErrWouldBlock (a concurrent blocked Read aside).
func (h *handle) readableNow() bool {
	if h.readDeadlinePassed() {
		return true
	}
	ready := false
	h.c.peek(func() {
		n, end := h.c.s.ReadReady(h.st)
		ready = n > 0 || end
	})
	return ready
}

// fire calls f unless the note was fired or stopped already. The note is
// unpublished first, so f may register the next one.
func (n *readNote) fire() {
	if n.done.CompareAndSwap(false, true) {
		n.h.rnote.CompareAndSwap(n, nil)
		n.f()
	}
}

// stop is NotifyReadable's stop.
func (n *readNote) stop() bool {
	if n.done.CompareAndSwap(false, true) {
		n.h.rnote.CompareAndSwap(n, nil)
		return true
	}
	return false
}

// awaitReadable waits as a read does, within the read deadline, for data
// or the part's end, without taking any.
func (h *handle) awaitReadable(ctx context.Context) error {
	c := h.c
	h.rwaiters.Add(1)
	defer h.rwaiters.Add(-1)
	defer func() { h.passReadWake(1) }()
	cut, err := h.waitOn(ctx, &h.rsig, nil, h.rwait, func() (bool, error) {
		n, end := c.s.ReadReady(h.st)
		return n > 0 || end, nil
	}, nil)
	if cut {
		return err
	}
	return nil // readable, or the end the next Read returns
}

func (h *handle) cancelRead(code StreamErrorCode) {
	c := h.c
	c.lock()
	defer c.unlock()
	c.s.Stop(h.st, uint64(code))
	h.released(c.s.ReleaseRecv(h.st))
}

// ---- writing ----

// write writes all of p (copied), waiting for room until ctx
// ends or the write deadline passes.
func (h *handle) write(ctx context.Context, p []byte) (int, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	total := 0
	err := h.awaitWrite(ctx, true, func() (bool, error) {
		n, err := c.s.Write(h.st, p[total:], false)
		total += n
		return total == len(p), err
	})
	return total, err
}

// writeBatch writes a nested session's batch into the stream without
// copying its payload, waiting for send-buffer room like write.
func (h *handle) writeBatch(b *core.Batch) (int, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	total := 0
	err := h.awaitWrite(context.Background(), true, func() (bool, error) {
		n, err := c.s.WriteBatch(h.st, b, total)
		total += n
		return total == b.Len(), err
	})
	return total, err
}

func (h *handle) close() error {
	c := h.c
	c.lock()
	defer c.unlock()
	err := c.s.Finish(h.st)
	h.released(c.s.ReleaseSend(h.st))
	return apiErr(err)
}

func (h *handle) cancelWrite(code StreamErrorCode) {
	c := h.c
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	c.s.Reset(h.st, uint64(code))
	h.released(c.s.ReleaseSend(h.st))
}

func (h *handle) streamID() (StreamID, error) {
	c := h.c
	c.lock()
	defer c.unlock()
	id, err := c.s.PinID(h.st)
	return StreamID(id), apiErr(err)
}

// flush sends the opening frame and the buffered data, waiting until they
// left or the write deadline passed.
func (h *handle) flush() error {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	target, flushed := uint64(0), false
	return h.awaitWrite(context.Background(), false, func() (bool, error) {
		if !flushed {
			t, err := c.s.Flush(h.st)
			if err != nil {
				return false, err
			}
			target, flushed = t, true
		}
		return c.s.FlushDone(h.st, target)
	})
}

// ---- public types ----

// ReceiveStream is the receiving part of a stream.
type ReceiveStream struct{ h *handle }

// SendStream is the sending part of a stream.
type SendStream struct{ h *handle }

// Stream is a bidirectional stream. Close finishes the sending part only
// (the quic-go convention); use CancelRead to stop reading.
type Stream struct{ h *handle }

// Urgency is the scheduling urgency of a stream's sending part (as in RFC
// 9218): lower is more urgent.
type Urgency uint8

const (
	// UrgencyHighest is the most urgent level.
	UrgencyHighest Urgency = 0
	// UrgencyDefault is the level of a new stream.
	UrgencyDefault Urgency = 3
	// UrgencyLowest is the least urgent level.
	UrgencyLowest Urgency = 7
)

func newStream(h *handle) *Stream {
	s := &Stream{h: h}
	runtime.AddCleanup(s, cleanupHandle, h)
	return s
}

func newSendStream(h *handle) *SendStream {
	s := &SendStream{h: h}
	runtime.AddCleanup(s, cleanupHandle, h)
	return s
}

func newReceiveStream(h *handle) *ReceiveStream {
	s := &ReceiveStream{h: h}
	runtime.AddCleanup(s, cleanupHandle, h)
	return s
}

// StreamID returns the identifier; before the first frame it pins it and
// the opening frame is sent at once. Before establishment it
// is 0. To open a stream on the wire, Flush is the explicit call.
func (s *Stream) StreamID() StreamID { defer runtime.KeepAlive(s); id, _ := s.h.streamID(); return id }

// Flush sends what was written now: the opening frame at once when the
// stream has none yet, with the preamble and the data written before it
// in that one frame, and the rest of the buffered data, as flow control
// allows. It waits until they were sent or the write deadline passed
// (os.ErrDeadlineExceeded), and fails as Write does on a reset or closed
// stream or session; after Close it flushes the FIN as well. Flush
// succeeds once the data was sent: a STOP_SENDING or reset that the peer
// sent afterwards (a peer that refuses at once) is reported by the next
// Write, Read or Flush, not by this one; a stop or reset that came before
// the data was sent fails it. Stream
// credit is reserved by OpenStream, so the opening frame never waits for
// it. Data written after Flush goes in later frames. Before
// establishment the opening frame goes as soon as the stream may open (in
// early data when that is in use); while the first flight is held
// (EarlyData.HoldFirstFlight) Flush returns at once and the frames leave
// with the flight. Flush counts as the first frame: preamble settings
// fail with ErrTooLate after it. It is the explicit way to open a stream
// on the wire whose peer speaks first; StreamID pins the same way but
// does not wait.
func (s *Stream) Flush() error { defer runtime.KeepAlive(s); return s.h.flush() }

// Read reads stream data; io.EOF only after a FIN that was not reset.
func (s *Stream) Read(p []byte) (int, error) { defer runtime.KeepAlive(s); return s.h.read(p) }

// TryRead reads what is readable now, up to len(p) bytes, and never waits.
// It returns what a Read would return at once: n > 0 bytes, io.EOF once the
// part ended with a FIN and everything was read, the read's error for a
// reset, a stop by this endpoint or the session's end, and
// os.ErrDeadlineExceeded once the read deadline passed with nothing
// readable. When nothing is readable yet it returns (0, ErrWouldBlock), as
// it does while another read of the stream is in progress (a blocked Read
// holds the read turn). An empty p returns (0, nil) unless the part ended.
// It does not allocate. With WaitReadable or NotifyReadable it is a Read
// that consumes nothing while it waits.
func (s *Stream) TryRead(p []byte) (int, error) { defer runtime.KeepAlive(s); return s.h.tryRead(p) }

// NotifyReadable arranges for f to be called once a TryRead would return
// anything but ErrWouldBlock: data arrives, the part ends (FIN, reset,
// CancelRead, the session's end) or the read deadline passes. It returns
// stop, which cancels the call as the stop of context.AfterFunc does: true
// if it kept f from being called, false if f was called or started, or the
// call was stopped already; it does not wait for f, and calling it again is
// harmless.
//
// f is called at most once, and exactly once unless stop cancels it. When
// the stream is readable already, f is called on the calling goroutine
// before NotifyReadable returns. Otherwise it runs on the goroutine of the
// event, such as the one that reads the connection or the deadline's timer,
// without any lock of the session held: f must not block, and may call the
// stream's methods, NotifyReadable included. Moving the read deadline while
// a call is pending calls f when the new deadline passes. The stream's
// datagrams and Close (which finishes only the sending part) do not make it
// readable. One call may be pending at a time: NotifyReadable panics while
// another is. No goroutine waits for a pending call.
func (s *Stream) NotifyReadable(f func()) (stop func() bool) {
	defer runtime.KeepAlive(s)
	return s.h.notifyReadable(f)
}

// WaitReadable waits until a Read would not block: data is readable, or
// the receiving part ended (FIN, reset, CancelRead or the session's end),
// so that the next Read returns the data or that end, once, in its own
// form. It consumes nothing and returns nil in each of these cases; it
// fails only with its own bound: ctx's error when ctx ends first, and
// os.ErrDeadlineExceeded when the read deadline passes. Any number of
// goroutines may wait at once. A nil s returns net.ErrClosed.
func (s *Stream) WaitReadable(ctx context.Context) error {
	if s == nil || s.h == nil {
		return net.ErrClosed
	}
	defer runtime.KeepAlive(s)
	return s.h.awaitReadable(ctx)
}

// Write writes p, waiting for send-buffer space.
func (s *Stream) Write(p []byte) (int, error) {
	defer runtime.KeepAlive(s)
	return s.h.write(context.Background(), p)
}

// Close finishes the sending part (FIN after the buffered data); it never
// blocks and does not stop reading (CancelRead does).
func (s *Stream) Close() error { defer runtime.KeepAlive(s); return s.h.close() }

// CancelWrite resets the sending part with code.
func (s *Stream) CancelWrite(code StreamErrorCode) { defer runtime.KeepAlive(s); s.h.cancelWrite(code) }

// CancelRead stops reading with code (STOP_SENDING).
func (s *Stream) CancelRead(code StreamErrorCode) { defer runtime.KeepAlive(s); s.h.cancelRead(code) }

// SetReadDeadline sets the read deadline.
func (s *Stream) SetReadDeadline(t time.Time) error {
	defer runtime.KeepAlive(s)
	s.h.setDeadline(true, false, t)
	return nil
}

// SetWriteDeadline sets the write deadline.
func (s *Stream) SetWriteDeadline(t time.Time) error {
	defer runtime.KeepAlive(s)
	s.h.setDeadline(false, true, t)
	return nil
}

// SetDeadline sets both deadlines.
func (s *Stream) SetDeadline(t time.Time) error {
	defer runtime.KeepAlive(s)
	s.h.setDeadline(true, true, t)
	return nil
}

// Context is the session's context: canceled when the session ends, not
// when the stream does (unlike a quic-go stream's).
func (s *Stream) Context() context.Context { defer runtime.KeepAlive(s); return s.h.c.ctx }

// StreamID of a sending part.
func (s *SendStream) StreamID() StreamID {
	defer runtime.KeepAlive(s)
	id, _ := s.h.streamID()
	return id
}

// Flush sends what was written now (Stream.Flush).
func (s *SendStream) Flush() error { defer runtime.KeepAlive(s); return s.h.flush() }

// Write writes p.
func (s *SendStream) Write(p []byte) (int, error) {
	defer runtime.KeepAlive(s)
	return s.h.write(context.Background(), p)
}

// Close finishes the stream (FIN after the buffered data); it never
// blocks.
func (s *SendStream) Close() error { defer runtime.KeepAlive(s); return s.h.close() }

// CancelWrite resets the stream.
func (s *SendStream) CancelWrite(code StreamErrorCode) {
	defer runtime.KeepAlive(s)
	s.h.cancelWrite(code)
}

// SetWriteDeadline sets the write deadline.
func (s *SendStream) SetWriteDeadline(t time.Time) error {
	defer runtime.KeepAlive(s)
	s.h.setDeadline(false, true, t)
	return nil
}

// StreamID of a receiving part.
func (s *ReceiveStream) StreamID() StreamID {
	defer runtime.KeepAlive(s)
	id, _ := s.h.streamID()
	return id
}

// Read reads stream data.
func (s *ReceiveStream) Read(p []byte) (int, error) { defer runtime.KeepAlive(s); return s.h.read(p) }

// TryRead reads what is readable now without waiting, as Stream.TryRead.
func (s *ReceiveStream) TryRead(p []byte) (int, error) {
	defer runtime.KeepAlive(s)
	return s.h.tryRead(p)
}

// NotifyReadable calls f once the stream is readable, as
// Stream.NotifyReadable.
func (s *ReceiveStream) NotifyReadable(f func()) (stop func() bool) {
	defer runtime.KeepAlive(s)
	return s.h.notifyReadable(f)
}

// CancelRead stops reading.
func (s *ReceiveStream) CancelRead(code StreamErrorCode) {
	defer runtime.KeepAlive(s)
	s.h.cancelRead(code)
}

// SetReadDeadline sets the read deadline.
func (s *ReceiveStream) SetReadDeadline(t time.Time) error {
	defer runtime.KeepAlive(s)
	s.h.setDeadline(true, false, t)
	return nil
}

// streamConn is the net.Conn view of a stream (StreamConn).
type streamConn struct {
	*Stream
	local, remote net.Addr
}

// StreamConn returns a net.Conn view of s. Unlike Stream.Close, its Close both
// finishes the stream and stops reading (CancelRead(StreamNoError)), as
// net.Conn users expect.
func StreamConn(s *Stream, local, remote net.Addr) net.Conn {
	return &streamConn{Stream: s, local: local, remote: remote}
}

func (c *streamConn) Close() error {
	err := c.Stream.Close()
	c.CancelRead(StreamNoError)
	return err
}

// CloseWrite finishes the stream's sending part and keeps reading, as
// net.TCPConn.CloseWrite does, so that a relay can pass an end on.
func (c *streamConn) CloseWrite() error { return c.Stream.Close() }

func (c *streamConn) LocalAddr() net.Addr  { return c.local }
func (c *streamConn) RemoteAddr() net.Addr { return c.remote }

// StreamStats is a snapshot of a stream's statistics in payload bytes, the
// preamble excluded; each field holds a value it had during
// the call, and PipeID with PipeBound are read at one point. Both
// parts of a bidirectional stream return the same snapshot; the fields of
// a direction the stream lacks stay 0. Once the session released the
// stream the snapshot keeps its final values for as long as the handle is
// held.
type StreamStats struct {
	// BytesWritten counts the payload the sending part accepted from the
	// application; BytesSent the payload put into STREAM frames, every
	// copy (reinjection, redundancy, opportunistic) again.
	BytesWritten, BytesSent uint64
	// BytesAcknowledged counts distinct payload offsets acknowledged:
	// passed to the carrier without multipath, covered by PIPE_ACK with
	// it.
	BytesAcknowledged uint64
	// BytesReceived counts distinct payload bytes received, read or not;
	// none before a peer stream's preamble, and after CancelRead every
	// byte below the highest offset received. BytesRead counts those
	// returned to the application.
	BytesReceived, BytesRead uint64
	// The stream's datagrams handed to a carrier, received and queued,
	// and dropped from its full queue, with their payload bytes.
	DatagramsSent, DatagramsReceived         uint64
	DatagramsDroppedRecv                     uint64
	DatagramBytesSent, DatagramBytesReceived uint64
	// SendBuffered is a gauge of written bytes not yet released (unsent;
	// unacknowledged in multipath), RecvBuffered of received bytes not yet
	// read, RecvWindow of the current receive window.
	SendBuffered, RecvBuffered, RecvWindow uint64
	// PipeID is the pipe the sending direction is bound to under the
	// affinity scheduler while PipeBound is true; pipe 0 is a pipe, so an
	// unbound direction reports PipeBound false (and PipeID 0).
	PipeID    PipeID
	PipeBound bool
}

func (h *handle) stats() StreamStats {
	c := h.c
	c.lock()
	defer c.unlock()
	in := c.s.StreamInfo(h.st)
	return StreamStats{BytesWritten: in.BytesWritten, BytesSent: in.BytesSent,
		BytesAcknowledged: in.BytesAcknowledged, BytesReceived: in.BytesReceived, BytesRead: in.BytesRead,
		DatagramsSent: in.DatagramsSent, DatagramsReceived: in.DatagramsReceived,
		DatagramsDroppedRecv: in.DatagramsDroppedRecv, DatagramBytesSent: in.DatagramBytesSent,
		DatagramBytesReceived: in.DatagramBytesReceived, SendBuffered: in.SendBuffered,
		RecvBuffered: in.RecvBuffered, RecvWindow: in.RecvWindow, PipeID: PipeID(in.Pipe), PipeBound: in.PipeBound}
}

// Stats returns a snapshot of the stream's statistics; a nil s returns
// the zero snapshot.
func (s *Stream) Stats() StreamStats {
	if s == nil || s.h == nil {
		return StreamStats{}
	}
	defer runtime.KeepAlive(s)
	return s.h.stats()
}

// Stats returns a snapshot of the stream's statistics; a nil s returns
// the zero snapshot.
func (s *SendStream) Stats() StreamStats {
	if s == nil || s.h == nil {
		return StreamStats{}
	}
	defer runtime.KeepAlive(s)
	return s.h.stats()
}

// Stats returns a snapshot of the stream's statistics; a nil s returns
// the zero snapshot.
func (s *ReceiveStream) Stats() StreamStats {
	if s == nil || s.h == nil {
		return StreamStats{}
	}
	defer runtime.KeepAlive(s)
	return s.h.stats()
}
