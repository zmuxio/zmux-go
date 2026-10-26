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
	c    *Conn
	st   *core.Stream
	rsig core.Signal // lazy: its channel is made by the first wait
	wsig core.Signal
	rmu  sync.Mutex
	wmu  sync.Mutex
	dl   atomic.Pointer[deadlines] // nil until a deadline is set

	// Guarded by the session lock.
	sendDone, recvDone, released bool
}

func (c *Conn) newHandle(st *core.Stream) *handle {
	h := &handle{c: c, st: st}
	st.SetSignals(&h.rsig, &h.wsig)
	h.sendDone = !st.Bidi() && !st.Local()
	h.recvDone = !st.Bidi() && st.Local()
	return h
}

// cleanupHandle runs on the runtime's cleanup goroutine when a handle is
// collected without being closed: reset(0) and stop(0) (API.md 5.1).
func cleanupHandle(h *handle) {
	h.stopDeadlines()
	h.c.orphan(h.st)
}

// deadlines are a handle's read and write deadlines, allocated when the
// first one is set.
type deadlines struct{ r, w deadline }

func (h *handle) deadlines() *deadlines {
	if d := h.dl.Load(); d != nil {
		return d
	}
	h.dl.CompareAndSwap(nil, &deadlines{})
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
		d.r.set(t)
	}
	if write {
		d.w.set(t)
	}
	h.c.lock()
	if read {
		h.rsig.FireLocked()
	}
	if write {
		h.wsig.FireLocked()
	}
	h.c.unlock()
}

func (h *handle) stopDeadlines() {
	if d := h.dl.Load(); d != nil {
		d.r.stop()
		d.w.stop()
	}
}

// maybeRelease releases the core stream once the application is done with
// both directions (under the lock).
func (h *handle) maybeReleaseLocked() {
	if h.sendDone && h.recvDone && !h.released {
		h.released = true
		h.c.s.Release(h.st)
		h.stopDeadlines()
	}
}

// ---- reading ----

func (h *handle) read(p []byte) (int, error) {
	h.rmu.Lock()
	defer h.rmu.Unlock()
	c := h.c
	for {
		c.lock()
		n, fin, err := c.s.Read(h.st, p)
		if n == 0 && !fin && err == nil {
			h.rsig.Drain()
			c.s.WaitRead(h.st, &h.rsig)
		}
		if fin || err != nil {
			h.recvDone = true
			h.maybeReleaseLocked()
		}
		c.unlock()
		if n > 0 || len(p) == 0 && err == nil {
			if fin && n == 0 {
				return 0, io.EOF
			}
			return n, nil
		}
		if fin {
			return 0, io.EOF
		}
		if err != nil {
			return 0, apiErr(err)
		}
		select {
		case <-h.rsig.C:
		case <-h.rwait():
			return 0, os.ErrDeadlineExceeded
		}
	}
}

func (h *handle) cancelRead(code StreamErrorCode) {
	c := h.c
	c.lock()
	defer c.unlock()
	c.s.Stop(h.st, uint64(code))
	h.recvDone = true
	h.maybeReleaseLocked()
}

// ---- writing ----

func (h *handle) write(p []byte, fin bool) (int, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	total := 0
	for {
		c.lock()
		c.s.SetNow(c.clk.now())
		n, err := c.s.Write(h.st, p[total:], fin)
		total += n
		wait := err == nil && total < len(p)
		if wait {
			h.wsig.Drain()
			c.s.WaitWrite(h.st, &h.wsig)
		}
		if err == nil && fin && total == len(p) {
			h.sendDone = true
			h.maybeReleaseLocked()
		}
		c.unlock()
		if err != nil {
			return total, apiErr(err)
		}
		if !wait {
			return total, nil
		}
		select {
		case <-h.wsig.C:
		case <-h.wwait():
			return total, os.ErrDeadlineExceeded
		}
	}
}

// writeBatch writes a nested session's batch into the stream without
// copying its payload (API.md 4.6), waiting for send-buffer room like
// write.
func (h *handle) writeBatch(b *core.Batch) (int, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	total := 0
	for {
		c.lock()
		c.s.SetNow(c.clk.now())
		n, err := c.s.WriteBatch(h.st, b, total)
		total += n
		wait := err == nil && total < b.Len()
		if wait {
			h.wsig.Drain()
			c.s.WaitWrite(h.st, &h.wsig)
		}
		c.unlock()
		if err != nil {
			return total, apiErr(err)
		}
		if !wait {
			return total, nil
		}
		select {
		case <-h.wsig.C:
		case <-h.wwait():
			return total, os.ErrDeadlineExceeded
		}
	}
}

func (h *handle) close() error {
	c := h.c
	c.lock()
	defer c.unlock()
	err := c.s.Finish(h.st)
	h.sendDone = true
	h.maybeReleaseLocked()
	return apiErr(err)
}

func (h *handle) cancelWrite(code StreamErrorCode) {
	c := h.c
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	c.s.Reset(h.st, uint64(code))
	h.sendDone = true
	h.maybeReleaseLocked()
}

func (h *handle) streamID() (StreamID, error) {
	c := h.c
	c.lock()
	defer c.unlock()
	id, err := c.s.PinID(h.st)
	return StreamID(id), apiErr(err)
}

// ---- public types ----

// ReceiveStream is the receiving part of a stream.
type ReceiveStream struct{ h *handle }

// SendStream is the sending part of a stream.
type SendStream struct{ h *handle }

// Stream is a bidirectional stream. Close finishes the sending part only
// (the quic-go convention); use CancelRead to stop reading.
type Stream struct{ h *handle }

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
// the opening frame is sent at once (SPEC.md 7.2).
func (s *Stream) StreamID() StreamID { defer runtime.KeepAlive(s); id, _ := s.h.streamID(); return id }

// Read reads stream data; io.EOF only after a FIN that was not reset.
func (s *Stream) Read(p []byte) (int, error) { defer runtime.KeepAlive(s); return s.h.read(p) }

// Write writes p, waiting for send-buffer space.
func (s *Stream) Write(p []byte) (int, error) { defer runtime.KeepAlive(s); return s.h.write(p, false) }

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

// Write writes p.
func (s *SendStream) Write(p []byte) (int, error) {
	defer runtime.KeepAlive(s)
	return s.h.write(p, false)
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

// StreamConn returns a net.Conn view of s. Unlike Stream.Close, its Close
// both finishes the stream and stops reading (CancelRead(CodeNoError)), as net.Conn
// users expect.
func StreamConn(s *Stream, local, remote net.Addr) net.Conn {
	return &streamConn{Stream: s, local: local, remote: remote}
}

func (c *streamConn) Close() error {
	err := c.Stream.Close()
	c.CancelRead(CodeNoError)
	return err
}

func (c *streamConn) LocalAddr() net.Addr  { return c.local }
func (c *streamConn) RemoteAddr() net.Addr { return c.remote }
