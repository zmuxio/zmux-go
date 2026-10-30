package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/zmuxio/zmux-go/internal/core"
)

// TLV is an application TLV of a stream's preamble (EXTENSIONS.md 4.3).
type TLV struct {
	Type  uint64
	Value []byte
}

// Chunk is stream data passed on without a copy. A chunk read
// with ReadChunk references the session's receive buffer until it is
// released: by WriteChunk, which takes ownership in every outcome, or by
// Release.
type Chunk struct {
	Data []byte
	buf  *core.Buf
}

// Release returns the chunk's buffer; the chunk must not be used after.
func (c *Chunk) Release() {
	if c.buf != nil {
		c.buf.Unref()
		c.buf = nil
	}
	c.Data = nil
}

// ---- handle operations ----

func (h *handle) do(f func(s *core.Session) error) error {
	c := h.c
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	return apiErr(f(c.s))
}

func (h *handle) setPriority(u Urgency, inc bool) {
	_ = h.do(func(s *core.Session) error { s.SetPriority(h.st, uint8(u), inc); return nil })
}

func (h *handle) setPeerPriority(u Urgency, inc bool) error {
	if u > UrgencyLowest {
		return fmt.Errorf("%w: urgency %d above %d", ErrInvalidConfig, u, UrgencyLowest)
	}
	return h.do(func(s *core.Session) error { return s.SetPeerPriority(h.st, uint8(u), inc) })
}

func (h *handle) preamble() ([]byte, []TLV, bool) {
	c := h.c
	c.lock()
	defer c.unlock()
	p, ok := h.st.PeerPreamble()
	if !ok {
		return nil, nil, false
	}
	tl := make([]TLV, len(p.App))
	for i, t := range p.App {
		tl[i] = TLV{Type: t.Type, Value: t.Value}
	}
	return p.OpenInfo, tl, p.PipeCarrier
}

func (h *handle) sendCapacity() uint64 {
	c := h.c
	c.lock()
	defer c.unlock()
	return c.s.SendCapacity(h.st)
}

func (h *handle) waitSendCapacity(ctx context.Context, n uint64) (uint64, error) {
	c := h.c
	sig := core.NewSignal()
	defer func() {
		c.lock()
		c.s.CancelCapacityWait(h.st)
		c.unlock()
	}()
	for {
		c.lock()
		ready := c.s.WaitSendCapacity(h.st, n, sig)
		cp := c.s.SendCapacity(h.st)
		err := c.s.WriteErrOf(h.st)
		c.unlock()
		if err != nil {
			return 0, apiErr(err)
		}
		if ready {
			return cp, nil
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-h.deadlines().w.wait():
			return 0, os.ErrDeadlineExceeded
		}
	}
}

func (h *handle) readChunk(ctx context.Context, limit int) (Chunk, error) {
	h.rmu.Lock()
	defer h.rmu.Unlock()
	c := h.c
	if limit <= 0 {
		limit = 1 << 20
	}
	for {
		c.lock()
		data, buf, fin, err := c.s.ReadChunk(h.st, limit)
		if len(data) == 0 && !fin && err == nil {
			h.rsig.Drain()
			c.s.WaitRead(h.st, &h.rsig)
		}
		if fin || err != nil {
			h.recvDone = true
			h.maybeReleaseLocked()
		}
		c.unlock()
		if len(data) > 0 {
			return Chunk{Data: data, buf: buf}, nil
		}
		if fin {
			return Chunk{}, io.EOF
		}
		if err != nil {
			return Chunk{}, apiErr(err)
		}
		select {
		case <-h.rsig.C:
		case <-ctx.Done():
			return Chunk{}, ctx.Err()
		case <-h.rwait():
			return Chunk{}, os.ErrDeadlineExceeded
		}
	}
}

func (h *handle) writeChunk(ctx context.Context, ch Chunk) error {
	defer ch.Release()
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	p := ch.Data
	for len(p) > 0 {
		c.lock()
		c.s.SetNow(c.clk.now())
		n, err := c.s.WriteChunk(h.st, p, ch.buf, false)
		p = p[n:]
		wait := err == nil && len(p) > 0
		if wait {
			h.wsig.Drain()
			c.s.WaitWrite(h.st, &h.wsig)
		}
		c.unlock()
		if err != nil {
			return apiErr(err)
		}
		if !wait {
			return nil
		}
		select {
		case <-h.wsig.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-h.wwait():
			return os.ErrDeadlineExceeded
		}
	}
	return nil
}

// tryWriteAll writes all of p now or nothing (no buffer space: false).
func (h *handle) tryWriteAll(p []byte) (bool, error) {
	if !h.wmu.TryLock() {
		return false, nil
	}
	defer h.wmu.Unlock()
	c := h.c
	c.lock()
	defer c.unlock()
	if uint64(len(p)) > c.s.SendRoom(h.st) {
		return false, apiErr(c.s.WriteErrOf(h.st))
	}
	_, err := c.s.Write(h.st, p, false)
	return err == nil, apiErr(err)
}

func (h *handle) reject() {
	c := h.c
	c.lock()
	defer c.unlock()
	c.s.Reject(h.st)
	h.sendDone, h.recvDone = true, true
	h.maybeReleaseLocked()
}

func (h *handle) sendDatagram(ctx uint64, p []byte) error { return h.c.sendDatagram(h.st, ctx, p) }

func (h *handle) receiveDatagram(ctx context.Context) (uint64, []byte, error) {
	c := h.c
	sig := core.NewSignal()
	for {
		c.lock()
		cx, d, ok := c.s.RecvStreamDatagram(h.st)
		var err error
		if !ok {
			if e := c.s.Err(); e != nil {
				err = sessionErr(e)
			} else {
				sig.Drain()
				c.s.WaitStreamDatagram(h.st, sig)
			}
		}
		c.unlock()
		if ok {
			return cx, d, nil
		}
		if err != nil {
			return 0, nil, err
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
}

// writeTo copies the stream to w without an intermediate buffer.
func (h *handle) writeTo(w io.Writer) (int64, error) {
	var n int64
	for {
		ch, err := h.readChunk(context.Background(), 1<<20)
		if len(ch.Data) > 0 {
			m, werr := w.Write(ch.Data)
			n += int64(m)
			ch.Release()
			if werr != nil {
				return n, werr
			}
		}
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}
}

// readFrom copies r into the stream.
func (h *handle) readFrom(r io.Reader) (int64, error) {
	buf := make([]byte, 64<<10)
	var n int64
	for {
		m, err := r.Read(buf)
		if m > 0 {
			if _, werr := h.write(buf[:m], false); werr != nil {
				return n, werr
			}
			n += int64(m)
		}
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}
}

// ---- Stream ----

// SetPriority sets the local scheduling priority of the stream's sending
// part: urgency (UrgencyHighest to UrgencyLowest; values above are taken
// as UrgencyLowest), and whether it is incremental (incremental
// directions of one urgency share it in turns, the others go one after
// the other). New streams have UrgencyDefault, incremental.
func (s *Stream) SetPriority(urgency Urgency, incremental bool) {
	defer runtime.KeepAlive(s)
	s.h.setPriority(urgency, incremental)
}

// SetPeerPriority asks the peer to schedule its sending part of the stream
// so: in the preamble before the first frame, PRIORITY_UPDATE after. An
// urgency above UrgencyLowest is ErrInvalidConfig.
func (s *Stream) SetPeerPriority(urgency Urgency, incremental bool) error {
	defer runtime.KeepAlive(s)
	return s.h.setPeerPriority(urgency, incremental)
}

// SetPreamble sets OPEN_INFO before the first frame.
func (s *Stream) SetPreamble(b []byte) error {
	defer runtime.KeepAlive(s)
	return s.h.do(func(c *core.Session) error { return c.SetOpenInfo(s.h.st, b) })
}

// AddPreambleTLV adds an application TLV (type 0x1000 to 0x3fff) before
// the first frame.
func (s *Stream) AddPreambleTLV(t uint64, v []byte) error {
	defer runtime.KeepAlive(s)
	return s.h.do(func(c *core.Session) error { return c.AddPreambleTLV(s.h.st, t, v) })
}

// Preamble returns the OPEN_INFO the peer opened the stream with.
func (s *Stream) Preamble() []byte { defer runtime.KeepAlive(s); b, _, _ := s.h.preamble(); return b }

// PreambleTLVs returns the application TLVs of the peer's preamble.
func (s *Stream) PreambleTLVs() []TLV {
	defer runtime.KeepAlive(s)
	_, t, _ := s.h.preamble()
	return t
}

// SetPipeCarrier marks the stream as a carrier of another session's pipe before
// the first frame.
func (s *Stream) SetPipeCarrier() error {
	defer runtime.KeepAlive(s)
	return s.h.do(func(c *core.Session) error { return c.SetPipeCarrier(s.h.st) })
}

// PipeCarrier reports whether the peer opened the stream as a pipe carrier.
func (s *Stream) PipeCarrier() bool { defer runtime.KeepAlive(s); _, _, p := s.h.preamble(); return p }

// SendCapacity returns the bytes the sending part could transmit now.
func (s *Stream) SendCapacity() uint64 { defer runtime.KeepAlive(s); return s.h.sendCapacity() }

// WaitSendCapacity waits until SendCapacity is at least n (or above 0
// while nothing is unsent) and returns it.
func (s *Stream) WaitSendCapacity(ctx context.Context, n uint64) (uint64, error) {
	defer runtime.KeepAlive(s)
	return s.h.waitSendCapacity(ctx, n)
}

// Replayable reports whether the stream's opening frame arrived as carrier
// early data before the handshake completed (treat its request as one an
// attacker may have repeated).
func (s *Stream) Replayable() bool {
	defer runtime.KeepAlive(s)
	c := s.h.c
	c.lock()
	defer c.unlock()
	return s.h.st.Replayable()
}

// SetReceiveWindow sets the stream's initial and maximum receive window
// (0 leaves a value; at least 16 KiB).
func (s *Stream) SetReceiveWindow(initial, limit uint64) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetReceiveWindow(s.h.st, initial, limit); return nil })
}

// SetSendBuffer bounds the stream's written-but-unreleased bytes.
func (s *Stream) SetSendBuffer(n uint64) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetSendBuffer(s.h.st, n); return nil })
}

// SetScheduler sets the stream's own scheduling policy (nil: the
// session's).
func (s *Stream) SetScheduler(p Scheduler) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error {
		if p == nil {
			c.SetStreamPolicy(s.h.st, 0, true)
		} else {
			c.SetStreamPolicy(s.h.st, p.policy(), false)
		}
		return nil
	})
}

// SetRedundant turns redundant sending of the stream's chunks on or off
// (multipath; unset, urgency 0 to 2 are redundant).
func (s *Stream) SetRedundant(on bool) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetRedundant(s.h.st, &on); return nil })
}

// SetReliableBoundary marks the data written so far as delivered even if
// the stream is reset later (RESET_STREAM_AT).
func (s *Stream) SetReliableBoundary() {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetReliableBoundary(s.h.st); return nil })
}

// Reject declines an accepted stream before reading it: both directions
// end with ZMUX_REJECTED.
func (s *Stream) Reject() { defer runtime.KeepAlive(s); s.h.reject() }

// TryWriteAll writes all of p now, or nothing when the send buffer has no
// room for it.
func (s *Stream) TryWriteAll(p []byte) (bool, error) {
	defer runtime.KeepAlive(s)
	return s.h.tryWriteAll(p)
}

// ReadChunk returns up to n bytes without copying; release the chunk, or
// hand it to WriteChunk, exactly once. A Chunk is a value: do not keep
// copies of it past that.
func (s *Stream) ReadChunk(ctx context.Context, n int) (Chunk, error) {
	defer runtime.KeepAlive(s)
	return s.h.readChunk(ctx, n)
}

// WriteChunk writes a chunk without copying and takes ownership of it.
func (s *Stream) WriteChunk(ctx context.Context, c Chunk) error {
	defer runtime.KeepAlive(s)
	return s.h.writeChunk(ctx, c)
}

// WriteTo implements io.WriterTo.
func (s *Stream) WriteTo(w io.Writer) (int64, error) {
	defer runtime.KeepAlive(s)
	return s.h.writeTo(w)
}

// ReadFrom implements io.ReaderFrom; it does not finish the stream.
func (s *Stream) ReadFrom(r io.Reader) (int64, error) {
	defer runtime.KeepAlive(s)
	return s.h.readFrom(r)
}

// SendDatagram sends a stream datagram with an application context
// identifier (RFC 9297 Context ID); like Conn.SendDatagram it never blocks
// and drops the oldest queued datagram when the queue is full.
func (s *Stream) SendDatagram(contextID uint64, p []byte) error {
	defer runtime.KeepAlive(s)
	return s.h.sendDatagram(contextID, p)
}

// SendDatagramWait sends a stream datagram like SendDatagram, waiting for
// room in the send queue instead of dropping its oldest entry.
func (s *Stream) SendDatagramWait(ctx context.Context, contextID uint64, p []byte) error {
	defer runtime.KeepAlive(s)
	return s.h.c.sendDatagramWait(ctx, s.h.st, contextID, p)
}

// ReceiveDatagram returns the next stream datagram and its context
// identifier.
func (s *Stream) ReceiveDatagram(ctx context.Context) (uint64, []byte, error) {
	defer runtime.KeepAlive(s)
	return s.h.receiveDatagram(ctx)
}

// ---- SendStream ----

// SetPriority sets the local scheduling priority, as Stream.SetPriority.
func (s *SendStream) SetPriority(urgency Urgency, incremental bool) {
	defer runtime.KeepAlive(s)
	s.h.setPriority(urgency, incremental)
}

// SetPreamble sets OPEN_INFO before the first frame.
func (s *SendStream) SetPreamble(b []byte) error {
	defer runtime.KeepAlive(s)
	return s.h.do(func(c *core.Session) error { return c.SetOpenInfo(s.h.st, b) })
}

// AddPreambleTLV adds an application TLV before the first frame.
func (s *SendStream) AddPreambleTLV(t uint64, v []byte) error {
	defer runtime.KeepAlive(s)
	return s.h.do(func(c *core.Session) error { return c.AddPreambleTLV(s.h.st, t, v) })
}

// SendCapacity returns the bytes the stream could transmit now.
func (s *SendStream) SendCapacity() uint64 { defer runtime.KeepAlive(s); return s.h.sendCapacity() }

// WaitSendCapacity waits for send capacity (see Stream.WaitSendCapacity).
func (s *SendStream) WaitSendCapacity(ctx context.Context, n uint64) (uint64, error) {
	defer runtime.KeepAlive(s)
	return s.h.waitSendCapacity(ctx, n)
}

// SetSendBuffer bounds the stream's written-but-unreleased bytes.
func (s *SendStream) SetSendBuffer(n uint64) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetSendBuffer(s.h.st, n); return nil })
}

// SetScheduler sets the stream's own scheduling policy (nil: the session's).
func (s *SendStream) SetScheduler(p Scheduler) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error {
		if p == nil {
			c.SetStreamPolicy(s.h.st, 0, true)
		} else {
			c.SetStreamPolicy(s.h.st, p.policy(), false)
		}
		return nil
	})
}

// SetRedundant turns redundant sending on or off (multipath).
func (s *SendStream) SetRedundant(on bool) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetRedundant(s.h.st, &on); return nil })
}

// SetReliableBoundary marks the data written so far as delivered.
func (s *SendStream) SetReliableBoundary() {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetReliableBoundary(s.h.st); return nil })
}

// TryWriteAll writes all of p now, or nothing.
func (s *SendStream) TryWriteAll(p []byte) (bool, error) {
	defer runtime.KeepAlive(s)
	return s.h.tryWriteAll(p)
}

// WriteChunk writes a chunk without copying and takes ownership of it.
func (s *SendStream) WriteChunk(ctx context.Context, c Chunk) error {
	defer runtime.KeepAlive(s)
	return s.h.writeChunk(ctx, c)
}

// ReadFrom implements io.ReaderFrom.
func (s *SendStream) ReadFrom(r io.Reader) (int64, error) {
	defer runtime.KeepAlive(s)
	return s.h.readFrom(r)
}

// ---- ReceiveStream ----

// SetPeerPriority asks the peer to schedule the stream so, as
// Stream.SetPeerPriority.
func (s *ReceiveStream) SetPeerPriority(urgency Urgency, incremental bool) error {
	defer runtime.KeepAlive(s)
	return s.h.setPeerPriority(urgency, incremental)
}

// Preamble returns the peer's OPEN_INFO.
func (s *ReceiveStream) Preamble() []byte {
	defer runtime.KeepAlive(s)
	b, _, _ := s.h.preamble()
	return b
}

// PreambleTLVs returns the application TLVs of the peer's preamble.
func (s *ReceiveStream) PreambleTLVs() []TLV {
	defer runtime.KeepAlive(s)
	_, t, _ := s.h.preamble()
	return t
}

// SetReceiveWindow sets the stream's initial and maximum receive window.
func (s *ReceiveStream) SetReceiveWindow(initial, limit uint64) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetReceiveWindow(s.h.st, initial, limit); return nil })
}

// Reject declines the stream before reading it.
func (s *ReceiveStream) Reject() { defer runtime.KeepAlive(s); s.h.reject() }

// Replayable reports whether the stream arrived as carrier early data.
func (s *ReceiveStream) Replayable() bool {
	defer runtime.KeepAlive(s)
	c := s.h.c
	c.lock()
	defer c.unlock()
	return s.h.st.Replayable()
}

// ReadChunk returns up to n bytes without copying.
func (s *ReceiveStream) ReadChunk(ctx context.Context, n int) (Chunk, error) {
	defer runtime.KeepAlive(s)
	return s.h.readChunk(ctx, n)
}

// WriteTo implements io.WriterTo.
func (s *ReceiveStream) WriteTo(w io.Writer) (int64, error) {
	defer runtime.KeepAlive(s)
	return s.h.writeTo(w)
}
