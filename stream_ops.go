package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/core"
)

// TLV is an application TLV of a stream's preamble.
type TLV struct {
	Value []byte
	Type  uint64
}

// Chunk is stream data passed on without a copy. A chunk read
// with ReadChunk references the session's receive buffer until it is
// released: by WriteChunk, which takes ownership in every outcome, or by
// Release. Its Data must not be changed, and must not be used after
// Release.
//
// Copies on the way out: ReadFrom reads into the pooled send blocks and
// WriteChunk queues the chunk's memory, so neither copies in user space;
// what the carrier does when the session sends those blocks is its own:
// a TCP connection takes them with writev, no copy; *tls.Conn flattens
// once while encrypting; a QUIC stream flattens once; an HTTP request body
// is copied once by the Transport and an HTTP response body once by its
// bufio writer; WebSocket flattens once per record; a stream of an outer
// session (nested sessions) takes them without a copy.
type Chunk struct {
	loan *chunkLoan
	Data []byte
}

type chunkLoan struct{ buf atomic.Pointer[core.Buf] }

// Release ends ownership for all copies of the Chunk.
func (c *Chunk) Release() {
	if b := c.take(); b != nil {
		b.ReturnView()
	}
	c.Data = nil
}

func (c *Chunk) take() *core.Buf {
	if c.loan == nil {
		return nil
	}
	return c.loan.buf.Swap(nil)
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

// held reads from the core under the session's lock, without the clock.
func (h *handle) priority() (Urgency, bool) {
	c := h.c
	c.lock()
	defer c.unlock()
	u, inc := c.s.Priority(h.st)
	return Urgency(u), inc
}

func (h *handle) redundant() (on, set bool) {
	c := h.c
	c.lock()
	defer c.unlock()
	if p := c.s.Redundant(h.st); p != nil {
		return *p, true
	}
	return false, false
}

func (h *handle) peerPriority() (Urgency, bool, bool) {
	c := h.c
	c.lock()
	defer c.unlock()
	u, inc, ok := c.s.PeerPriority(h.st)
	return Urgency(u), inc, ok
}

func (h *handle) scheduler() Scheduler {
	c := h.c
	c.lock()
	defer c.unlock()
	p, own := c.s.StreamPolicy(h.st)
	if !own {
		return nil
	}
	return c.schedulerLocked(p)
}

func (h *handle) sendBuffer() int {
	c := h.c
	c.lock()
	defer c.unlock()
	return int(min(c.s.SendBuffer(h.st), math.MaxInt))
}

func (h *handle) receiveWindow() (initial, limit uint64) {
	c := h.c
	c.lock()
	defer c.unlock()
	return c.s.ReceiveWindow(h.st)
}

func (h *handle) setScheduler(p Scheduler) {
	_ = h.do(func(c *core.Session) error {
		if p == nil {
			c.SetStreamPolicy(h.st, 0, true)
		} else {
			c.SetStreamPolicy(h.st, p.policy(), false)
		}
		return nil
	})
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

// preamblePriority returns the PRIORITY TLV of the opening preamble.
func (h *handle) preamblePriority() (Urgency, bool, bool) {
	c := h.c
	c.lock()
	defer c.unlock()
	p, ok := h.st.PeerPreamble()
	if !ok || !p.HasPriority {
		return 0, false, false
	}
	return Urgency(p.Urgency), p.Incremental, true
}

func (h *handle) sendCapacity() uint64 {
	c := h.c
	c.lock()
	defer c.unlock()
	return c.s.SendCapacity(h.st)
}

// waitSendCapacity waits for capacity as a write waits for room: it holds
// the write slot (wmu), so a capacity wait and a write of the stream are
// served one after the other.
func (h *handle) waitSendCapacity(ctx context.Context, n uint64) (uint64, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	var cp uint64
	waiting := false
	err := h.awaitWrite(ctx, false, func() (bool, error) {
		if err := c.s.WriteErrOf(h.st); err != nil {
			return false, err
		}
		ready := c.s.WaitSendCapacity(h.st, n)
		waiting = !ready
		cp = c.s.SendCapacity(h.st)
		return ready, nil
	})
	if waiting {
		c.locked(func() { c.s.CancelCapacityWait(h.st) })
	}
	if err != nil {
		return 0, err
	}
	return cp, nil
}

func (h *handle) readChunk(ctx context.Context, limit int) (Chunk, error) {
	view, err := h.readLoan(ctx, limit)
	if err != nil {
		return Chunk{}, err
	}
	ch := Chunk{Data: view.Bytes(), loan: new(chunkLoan)}
	ch.loan.buf.Store(view.Buf)
	return ch, nil
}

func (h *handle) readLoan(ctx context.Context, limit int) (view core.Span, err error) {
	h.rmu.Lock()
	defer h.rmu.Unlock()
	c := h.c
	if limit <= 0 {
		limit = 1 << 20
	}
	eof := false
	err = h.awaitRead(ctx, false, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var readErr error
		view, eof, readErr = c.s.ReadChunk(h.st, limit)
		if eof || readErr != nil && !errors.Is(readErr, ErrReceiveLoanLimit) {
			h.released(c.s.ReleaseRecv(h.st))
		}
		return view.Len() > 0 || eof, readErr
	})
	if eof {
		err = io.EOF
	}
	return view, err
}

func (h *handle) writeChunk(ctx context.Context, ch Chunk) error {
	_, err := h.writeChunkProgress(ctx, ch)
	return err
}

func (h *handle) writeChunkProgress(ctx context.Context, ch Chunk) (int, error) {
	buf := ch.take()
	if buf != nil {
		defer buf.ReturnView()
	} else if ch.loan != nil {
		return 0, ErrStreamClosed
	}
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	p := ch.Data
	if len(p) == 0 {
		return 0, nil
	}
	err := h.awaitWrite(ctx, true, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		n, err := c.s.WriteChunk(h.st, p, buf, false)
		p = p[n:]
		return len(p) == 0, err
	})
	return len(ch.Data) - len(p), err
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
	c.s.SetNow(c.clk.now())
	ok, err := c.s.TryWriteAll(h.st, p)
	return ok, apiErr(err)
}

// waitWritable waits until a write takes bytes now and returns how many:
// the send-buffer room TryWrite admits. It holds wmu while it waits, as a
// write that waits for room does, so no other write of the stream shares
// wsig with it.
func (h *handle) waitWritable(ctx context.Context) (uint64, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	var room uint64
	err := h.awaitWrite(ctx, true, func() (bool, error) {
		var err error
		room, err = c.s.WritableRoom(h.st)
		return room > 0, err
	})
	if err != nil {
		return 0, err
	}
	return room, nil
}

// tryWrite writes the prefix of p that fits now, never waiting: 0 when
// nothing fits or another write holds the stream.
func (h *handle) tryWrite(p []byte) (int, error) {
	if !h.wmu.TryLock() {
		return 0, nil
	}
	defer h.wmu.Unlock()
	c := h.c
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	n, err := c.s.Write(h.st, p, false)
	return n, apiErr(err)
}

func (h *handle) reject() {
	c := h.c
	c.lock()
	defer c.unlock()
	h.released(c.s.Reject(h.st))
}

func (h *handle) receiveDatagram(ctx context.Context) (uint64, []byte, error) {
	var cx uint64
	var data []byte
	err := h.waitDatagram(ctx, func() bool {
		var ok bool
		cx, data, ok = h.c.s.RecvStreamDatagram(h.st)
		return ok
	})
	return cx, data, err
}

func (h *handle) waitDatagram(ctx context.Context, read func() bool) error {
	h.dmu.Lock()
	defer h.dmu.Unlock()
	c := h.c
	end := false
	_, err := h.waitOn(ctx, &h.dsig, nil, h.rwait, func() (bool, error) {
		c.s.SetNow(c.clk.now())
		if read() {
			return true, nil
		}
		var e error
		end, e = c.s.StreamDatagramsEnded(h.st)
		return end, e
	}, nil)
	if end && err == nil {
		err = io.EOF
	}
	return err
}

// writeTo copies the stream to w without an intermediate buffer.
func (h *handle) writeTo(w io.Writer) (int64, error) {
	var n int64
	for {
		view, err := h.readLoan(context.Background(), 1<<20)
		data := view.Bytes()
		if len(data) > 0 {
			m, werr := func() (int, error) {
				defer view.Buf.ReturnView()
				return w.Write(data)
			}()
			n += int64(m)
			if werr != nil {
				return n, werr
			}
			if m != len(data) {
				return n, io.ErrShortWrite
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

// readFrom copies r into the stream, reading straight into the stream's
// pooled send blocks: no intermediate buffer and no copy.
func (h *handle) readFrom(r io.Reader) (int64, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	lent := false
	defer func() {
		if lent { // r panicked: end the loan
			c.locked(func() { _ = c.s.CommitWrite(h.st, 0) })
		}
	}()
	var n int64
	for {
		var buf []byte
		err := h.awaitWrite(context.Background(), true, func() (bool, error) {
			var err error
			buf, err = c.s.WriteSpace(h.st, 1<<30)
			return len(buf) > 0, err
		})
		if err != nil {
			return n, err
		}
		lent = true
		m, rerr := r.Read(buf)
		if m < 0 || m > len(buf) {
			m, rerr = 0, errInvalidRead
		}
		lent = false
		var werr error
		ran := false
		c.locked(func() {
			c.s.SetNow(c.clk.now())
			werr = c.s.CommitWrite(h.st, m)
			ran = true
		})
		if !ran { // the commit panicked: the session is closed
			return n, c.closedErr()
		}
		if werr != nil {
			return n, apiErr(werr)
		}
		n += int64(m)
		if errors.Is(rerr, io.EOF) {
			return n, nil
		}
		if rerr != nil {
			return n, rerr
		}
	}
}

// errInvalidRead is a reader's count outside its buffer.
var errInvalidRead = errors.New("zmux: reader returned an invalid count")

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

// Priority returns the local scheduling priority of the stream's sending
// part now: the last value SetPriority set or, where the peer's PRIORITY
// signals adjust it, the peer set. It does not fail or wait, and answers on
// a closed stream with the value it held last.
func (s *Stream) Priority() (urgency Urgency, incremental bool) {
	defer runtime.KeepAlive(s)
	return s.h.priority()
}

// Redundant returns what SetRedundant set: on, and whether it is set; unset,
// the stream follows the session. It does not fail or wait.
func (s *Stream) Redundant() (on, set bool) {
	defer runtime.KeepAlive(s)
	return s.h.redundant()
}

// SetPeerPriority asks the peer to schedule its sending part of the stream
// so: in the preamble before the first frame, PRIORITY_UPDATE after. An
// urgency above UrgencyLowest is ErrInvalidConfig.
func (s *Stream) SetPeerPriority(urgency Urgency, incremental bool) error {
	defer runtime.KeepAlive(s)
	return s.h.setPeerPriority(urgency, incremental)
}

// SetPreamble sets OPEN_INFO before the first frame. A preamble that
// would exceed the peer's limit is *PreambleTooLargeError.
func (s *Stream) SetPreamble(b []byte) error {
	defer runtime.KeepAlive(s)
	return s.h.do(func(c *core.Session) error { return c.SetOpenInfo(s.h.st, b) })
}

// AddPreambleTLV adds an application TLV (type 0x1000 to 0x3fff) before
// the first frame. A preamble that would exceed the peer's limit is
// *PreambleTooLargeError.
func (s *Stream) AddPreambleTLV(t uint64, v []byte) error {
	defer runtime.KeepAlive(s)
	return s.h.do(func(c *core.Session) error { return c.AddPreambleTLV(s.h.st, t, v) })
}

// Preamble returns the OPEN_INFO the peer opened the stream with.
func (s *Stream) Preamble() []byte { defer runtime.KeepAlive(s); b, _, _ := s.h.preamble(); return b }

// PreamblePriority returns the urgency and incremental flag of the PRIORITY
// TLV the peer opened the stream with; ok is false when its preamble
// carried none. It reports the opening preamble only, not a later
// PRIORITY_UPDATE.
func (s *Stream) PreamblePriority() (urgency Urgency, incremental, ok bool) {
	defer runtime.KeepAlive(s)
	return s.h.preamblePriority()
}

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
// while nothing is unsent) and returns it. It waits for credit, not for
// what a write takes: see WaitWritable. It waits in turn with the
// stream's writes: a write blocked for room delays it, and it delays the
// next write.
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

// SetReceiveWindow sets the stream's initial and maximum receive window.
// initial is advertised at once when it is above the window in force; credit
// already advertised is never withdrawn, so a lower initial only changes what
// ReceiveWindow reads back. limit caps auto-tuning, 0 making the cap
// automatic (whatever the session's configuration says). An initial below
// 4 KiB or a nonzero limit below initial is ErrInvalidConfig and changes
// nothing.
func (s *Stream) SetReceiveWindow(initial, limit uint64) error {
	defer runtime.KeepAlive(s)
	return s.h.do(func(c *core.Session) error { return c.SetReceiveWindow(s.h.st, initial, limit) })
}

// SetSendBuffer bounds the stream's written-but-unreleased bytes.
func (s *Stream) SetSendBuffer(n uint64) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetSendBuffer(s.h.st, n); return nil })
}

// SetScheduler sets the stream's own scheduling policy; nil, the zero
// Scheduler, makes the stream follow the session's again.
func (s *Stream) SetScheduler(p Scheduler) {
	defer runtime.KeepAlive(s)
	s.h.setScheduler(p)
}

// Scheduler returns the stream's own scheduling policy, nil while it
// follows the session's (never the session's policy it resolves to). It
// does not fail or wait, and answers on a closed stream with the value it
// held last.
func (s *Stream) Scheduler() Scheduler {
	defer runtime.KeepAlive(s)
	return s.h.scheduler()
}

// SendBuffer returns the bound of the stream's written-but-unreleased
// bytes: the value SetSendBuffer set, else the one the session's
// configuration gave it. It does not fail or wait.
func (s *Stream) SendBuffer() int {
	defer runtime.KeepAlive(s)
	return s.h.sendBuffer()
}

// PeerPriority returns the priority this endpoint requested for the peer's
// sending part of the stream (the preamble's PRIORITY TLV or the last
// PRIORITY_UPDATE); ok is false when none was requested. It does not fail or
// wait.
func (s *Stream) PeerPriority() (urgency Urgency, incremental, ok bool) {
	defer runtime.KeepAlive(s)
	return s.h.peerPriority()
}

// ReceiveWindow returns the stream's receive window settings: the initial
// window as set (or copied from the session), never the window auto-tuning
// has grown it to (that is the statistic RecvWindow), and the cap on it as
// set (0: automatic). It does not fail or wait.
func (s *Stream) ReceiveWindow() (initial, limit uint64) {
	defer runtime.KeepAlive(s)
	return s.h.receiveWindow()
}

// SetRedundant turns redundant sending of the stream's chunks on or off
// (multipath; unset, urgency 0 to 2 are redundant).
func (s *Stream) SetRedundant(on bool) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetRedundant(s.h.st, &on); return nil })
}

// ClearRedundant undoes SetRedundant: the stream's redundancy follows the
// session's again (SchedulerParams.NoRedundancy; by default urgency 0 to
// 2 are redundant).
func (s *Stream) ClearRedundant() {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetRedundant(s.h.st, nil); return nil })
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

// TryWrite writes the prefix of p that fits in the send buffer now and
// returns its length, never waiting: 0 when nothing fits or another write
// is in progress (a WaitWritable included: pair the two in one goroutine).
// With WaitWritable it is a write that consumes nothing
// while it waits.
func (s *Stream) TryWrite(p []byte) (int, error) {
	defer runtime.KeepAlive(s)
	return s.h.tryWrite(p)
}

// WaitWritable waits until the send buffer has room for a write and
// returns the bytes TryWrite takes now (above 0), or the error that ended
// the sending part. Room, not credit, admits a write: it may be there
// while SendCapacity is 0, and the other way round. It consumes nothing,
// so ctx may cancel it at any point; the write deadline bounds it too. A
// write in progress on the stream finishes first. Then TryWrite takes at
// least the returned bytes unless other streams took the shared buffer
// meanwhile (0: wait again).
//
// A WaitWritable in progress holds the stream's write turn, as a blocked
// Write does: a TryWrite from another goroutine returns 0 meanwhile. Call
// the two in one goroutine, WaitWritable then TryWrite.
func (s *Stream) WaitWritable(ctx context.Context) (uint64, error) {
	defer runtime.KeepAlive(s)
	return s.h.waitWritable(ctx)
}

// ReadChunk returns up to n bytes without copying; release the chunk, or
// hand it to WriteChunk. ErrReceiveLoanLimit leaves the data available
// for a later call or a normal Read.
func (s *Stream) ReadChunk(ctx context.Context, n int) (Chunk, error) {
	defer runtime.KeepAlive(s)
	if s == nil || s.h == nil {
		return Chunk{}, net.ErrClosed
	}
	return s.h.readChunk(ctx, n)
}

// WriteChunk writes a chunk without copying and takes ownership of it.
func (s *Stream) WriteChunk(ctx context.Context, c Chunk) error {
	defer runtime.KeepAlive(s)
	if s == nil || s.h == nil {
		c.Release()
		return net.ErrClosed
	}
	return s.h.writeChunk(ctx, c)
}

// WriteTo implements io.WriterTo: it passes w the session's receive
// buffers in place, with no intermediate buffer.
func (s *Stream) WriteTo(w io.Writer) (int64, error) {
	defer runtime.KeepAlive(s)
	return s.h.writeTo(w)
}

// ReadFrom implements io.ReaderFrom; it does not finish the stream. It
// reads r straight into the stream's pooled send blocks (at most one block,
// 16 KiB, and what the send buffer admits, per Read), with no intermediate
// buffer, no copy and no allocation once warm; see Chunk for the carrier's
// copies. Writes to the stream wait while it runs, and Close or CancelWrite
// meanwhile ends it with their error, discarding the bytes of the Read in
// progress.
func (s *Stream) ReadFrom(r io.Reader) (int64, error) {
	defer runtime.KeepAlive(s)
	return s.h.readFrom(r)
}

// SendDatagram sends a stream datagram with an application context
// identifier (RFC 9297 Context ID); like Conn.SendDatagram it never blocks
// and drops the oldest queued datagram when the queue is full.
func (s *Stream) SendDatagram(contextID uint64, p []byte) error {
	return s.SendDatagramWith(contextID, p, DatagramSendOptions{})
}

// SendDatagramWith queues a stream datagram with options for this hop.
func (s *Stream) SendDatagramWith(contextID uint64, p []byte, o DatagramSendOptions) error {
	if s == nil {
		return net.ErrClosed
	}
	defer runtime.KeepAlive(s)
	return s.h.c.sendDatagramWith(s.h.st, contextID, p, o)
}

// DatagramPath reports the current payload limits for this Flow and Context.
func (s *Stream) DatagramPath(contextID uint64) DatagramPath {
	if s == nil {
		return DatagramPath{}
	}
	defer runtime.KeepAlive(s)
	c := s.h.c
	c.lock()
	defer c.unlock()
	admission, path, native, available := c.s.DatagramPath(s.h.st, contextID)
	return DatagramPath{MaxPayload: admission, PathPayload: path, NativePayload: native, Native: available}
}

// MaxDatagramPayload returns the largest stream datagram payload, with
// context identifier contextID, the peer accepts now; 0 when it accepts
// none on this stream. Before the stream has its identifier (it is
// assigned when the stream is announced) the value assumes the longest
// identifier encoding and may grow by a few bytes afterwards.
func (s *Stream) MaxDatagramPayload(contextID uint64) int {
	defer runtime.KeepAlive(s)
	c := s.h.c
	c.lock()
	defer c.unlock()
	return c.s.StreamMaxDatagramPayload(s.h.st, contextID)
}

// PathDatagramPayload returns the largest stream datagram payload fitting
// an eligible record or native path, with context identifier contextID.
func (s *Stream) PathDatagramPayload(contextID uint64) int {
	if s == nil || s.h == nil {
		return 0
	}
	defer runtime.KeepAlive(s)
	c := s.h.c
	c.lock()
	defer c.unlock()
	return c.s.StreamPathDatagramPayload(s.h.st, contextID)
}

// SendDatagramWait sends a stream datagram like SendDatagram, waiting for
// room in the send queue instead of dropping its oldest entry.
func (s *Stream) SendDatagramWait(ctx context.Context, contextID uint64, p []byte) error {
	return s.SendDatagramWaitWith(ctx, contextID, p, DatagramSendOptions{})
}

// SendDatagramWaitWith waits for local queue room with the same options as
// SendDatagramWith. A relative deadline starts at this hop's call.
func (s *Stream) SendDatagramWaitWith(ctx context.Context, contextID uint64, p []byte, opts DatagramSendOptions) error {
	if s == nil || s.h == nil {
		return net.ErrClosed
	}
	defer runtime.KeepAlive(s)
	return s.h.c.sendDatagramWait(ctx, s.h.st, contextID, p, opts)
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

// Priority returns the local scheduling priority, as Stream.Priority.
func (s *SendStream) Priority() (urgency Urgency, incremental bool) {
	defer runtime.KeepAlive(s)
	return s.h.priority()
}

// Redundant returns what SetRedundant set, as Stream.Redundant.
func (s *SendStream) Redundant() (on, set bool) {
	defer runtime.KeepAlive(s)
	return s.h.redundant()
}

// SetPreamble sets OPEN_INFO before the first frame. A preamble that
// would exceed the peer's limit is *PreambleTooLargeError.
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

// SetScheduler sets the stream's own scheduling policy; nil makes it
// follow the session's again.
func (s *SendStream) SetScheduler(p Scheduler) {
	defer runtime.KeepAlive(s)
	s.h.setScheduler(p)
}

// Scheduler returns the stream's own scheduling policy, as Stream.Scheduler.
func (s *SendStream) Scheduler() Scheduler {
	defer runtime.KeepAlive(s)
	return s.h.scheduler()
}

// SendBuffer returns the bound of the stream's written-but-unreleased
// bytes, as Stream.SendBuffer.
func (s *SendStream) SendBuffer() int {
	defer runtime.KeepAlive(s)
	return s.h.sendBuffer()
}

// SetRedundant turns redundant sending on or off (multipath).
func (s *SendStream) SetRedundant(on bool) {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetRedundant(s.h.st, &on); return nil })
}

// ClearRedundant undoes SetRedundant: redundancy follows the session's.
func (s *SendStream) ClearRedundant() {
	defer runtime.KeepAlive(s)
	_ = s.h.do(func(c *core.Session) error { c.SetRedundant(s.h.st, nil); return nil })
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

// TryWrite writes the prefix of p that fits now, as Stream.TryWrite.
func (s *SendStream) TryWrite(p []byte) (int, error) {
	defer runtime.KeepAlive(s)
	return s.h.tryWrite(p)
}

// WaitWritable waits for room for a write (see Stream.WaitWritable).
func (s *SendStream) WaitWritable(ctx context.Context) (uint64, error) {
	defer runtime.KeepAlive(s)
	return s.h.waitWritable(ctx)
}

// WriteChunk writes a chunk without copying and takes ownership of it.
func (s *SendStream) WriteChunk(ctx context.Context, c Chunk) error {
	defer runtime.KeepAlive(s)
	if s == nil || s.h == nil {
		c.Release()
		return net.ErrClosed
	}
	return s.h.writeChunk(ctx, c)
}

// ReadFrom implements io.ReaderFrom like Stream.ReadFrom.
func (s *SendStream) ReadFrom(r io.Reader) (int64, error) {
	defer runtime.KeepAlive(s)
	return s.h.readFrom(r)
}

// ---- ReceiveStream ----

// WaitReadable waits until a Read would not block, as Stream.WaitReadable.
func (s *ReceiveStream) WaitReadable(ctx context.Context) error {
	if s == nil || s.h == nil {
		return net.ErrClosed
	}
	defer runtime.KeepAlive(s)
	return s.h.awaitReadable(ctx)
}

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

// PreamblePriority returns the peer's opening PRIORITY, as
// Stream.PreamblePriority.
func (s *ReceiveStream) PreamblePriority() (urgency Urgency, incremental, ok bool) {
	defer runtime.KeepAlive(s)
	return s.h.preamblePriority()
}

// PreambleTLVs returns the application TLVs of the peer's preamble.
func (s *ReceiveStream) PreambleTLVs() []TLV {
	defer runtime.KeepAlive(s)
	_, t, _ := s.h.preamble()
	return t
}

// PeerPriority returns the priority requested for the peer's sending part,
// as Stream.PeerPriority.
func (s *ReceiveStream) PeerPriority() (urgency Urgency, incremental, ok bool) {
	defer runtime.KeepAlive(s)
	return s.h.peerPriority()
}

// ReceiveWindow returns the stream's receive window, as
// Stream.ReceiveWindow.
func (s *ReceiveStream) ReceiveWindow() (initial, limit uint64) {
	defer runtime.KeepAlive(s)
	return s.h.receiveWindow()
}

// SetReceiveWindow sets the stream's initial and maximum receive window, as
// Stream.SetReceiveWindow.
func (s *ReceiveStream) SetReceiveWindow(initial, limit uint64) error {
	defer runtime.KeepAlive(s)
	return s.h.do(func(c *core.Session) error { return c.SetReceiveWindow(s.h.st, initial, limit) })
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
	if s == nil || s.h == nil {
		return Chunk{}, net.ErrClosed
	}
	return s.h.readChunk(ctx, n)
}

// WriteTo implements io.WriterTo like Stream.WriteTo.
func (s *ReceiveStream) WriteTo(w io.Writer) (int64, error) {
	defer runtime.KeepAlive(s)
	return s.h.writeTo(w)
}
