package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Limiter lets an external limiter, budget or meter act on bytes that zmux
// forwards itself (Splice, datagrams). zmux calls it outside
// every session lock.
type Limiter interface {
	// Ready waits until at least minBytes bytes (or the limiter's burst, if
	// less) are allowed and returns the allowance, at most maxBytes.
	Ready(ctx context.Context, minBytes, maxBytes int) (int, error)
	// TryReady returns the allowance now, 0 when it would have to wait.
	TryReady(maxBytes int) int
	// Charge accounts n bytes actually forwarded.
	Charge(n int)
}

// SpliceOptions configure Splice.
type SpliceOptions struct {
	// AtoB and BtoA limit the two directions (nil: none).
	AtoB, BtoA Limiter
	// Chunk is the most one direction holds at once (64 KiB; a splice
	// moves data, so 0 has no other meaning).
	Chunk int
}

// SpliceDirection is the result of one direction of a splice.
type SpliceDirection struct {
	Bytes         uint64 // stream bytes forwarded
	DatagramBytes uint64 // datagram payload bytes forwarded
	// DatagramsDropped counts every datagram not forwarded; of them,
	// DatagramsTooLarge did not fit the next hop's MaxDatagramPayload and
	// DatagramsLimited were refused by the direction's Limiter or the next
	// session's send limiter. SplicePackets forwards datagrams that do not
	// fit as capsules instead (DatagramsEncapsulated), so its
	// DatagramsTooLarge stays 0.
	DatagramsDropped, DatagramsTooLarge, DatagramsLimited uint64
	// DatagramsEncapsulated counts the datagrams SplicePackets forwarded
	// as DATAGRAM capsules on the stream (their payload is in
	// DatagramBytes); 0 for Splice.
	DatagramsEncapsulated uint64
	End                   SpliceEnd // how the direction ended
}

// SpliceEndKind is the event that ended a direction of a splice.
type SpliceEndKind uint8

const (
	SpliceFinished       SpliceEndKind = iota // FIN read upstream and passed on
	SpliceReset                               // the upstream stream was reset with Code
	SpliceStopped                             // the downstream stream was stopped with Code
	SpliceUpstreamLost                        // the upstream stream's session closed or was lost
	SpliceDownstreamLost                      // the downstream stream's session closed or was lost
	SpliceCancelled                           // ctx ended, or the direction's Limiter failed
)

// String returns the kind's name ("finished").
func (k SpliceEndKind) String() string {
	switch k {
	case SpliceFinished:
		return "finished"
	case SpliceReset:
		return "reset"
	case SpliceStopped:
		return "stopped"
	case SpliceUpstreamLost:
		return "upstream lost"
	case SpliceDownstreamLost:
		return "downstream lost"
	case SpliceCancelled:
		return "cancelled"
	}
	return "SpliceEndKind(?)"
}

// SpliceEnd is how a direction of a splice ended; Code is the
// application error code of SpliceReset and SpliceStopped, which travels
// unchanged to the other side, and 0 for the other kinds.
type SpliceEnd struct {
	Kind SpliceEndKind
	Code StreamErrorCode
}

// String returns the end's kind, with its code for a reset or a stop
// ("reset 0x77").
func (e SpliceEnd) String() string {
	if e.Kind == SpliceReset || e.Kind == SpliceStopped {
		return fmt.Sprintf("%v %#x", e.Kind, uint64(e.Code))
	}
	return e.Kind.String()
}

// spliceEnd classifies the error that ended a direction: a remote reset
// (read side) or stop (write side) with its code, a cancellation, or the
// loss of the session on that side.
func spliceEnd(err error, write bool) SpliceEnd {
	var se *StreamError
	switch {
	case errors.As(err, &se) && se.Remote && !write:
		return SpliceEnd{Kind: SpliceReset, Code: se.ErrorCode}
	case errors.As(err, &se) && se.Remote:
		return SpliceEnd{Kind: SpliceStopped, Code: se.ErrorCode}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return SpliceEnd{Kind: SpliceCancelled}
	case write:
		return SpliceEnd{Kind: SpliceDownstreamLost}
	}
	return SpliceEnd{Kind: SpliceUpstreamLost}
}

// SpliceResult is the result of Splice.
type SpliceResult struct {
	AtoB, BtoA SpliceDirection
}

var errSpliceSelf = errors.New("zmux: cannot splice a stream into itself")

// Splice forwards two bidirectional streams into each other until both
// directions have ended: what arrives on a is sent on b and
// what arrives on b is sent on a, a chunk at a time, without copying and
// only within the receiving side's credit, so backpressure crosses the
// relay hop by hop. Ends travel across: FIN finishes the
// other side, a reset resets it with its code after the bytes below its
// Reliable Size, a stop stops the upstream, a lost session fails the other
// side with ZMUX_CARRIER_FAILED, and cancelling ctx ends both with code 0.
func Splice(ctx context.Context, a, b *Stream, opts *SpliceOptions) (SpliceResult, error) {
	if a == b || a.h == b.h {
		return SpliceResult{}, errSpliceSelf
	}
	// The handles are used through their inner state: keep the wrappers
	// (whose collection resets the streams) alive until the splice ends.
	defer runtime.KeepAlive(a)
	defer runtime.KeepAlive(b)
	var o SpliceOptions
	if opts != nil {
		o = *opts
	}
	if o.Chunk < 0 {
		return SpliceResult{}, fmt.Errorf("%w: negative SpliceOptions.Chunk", ErrInvalidConfig)
	}
	if o.Chunk == 0 {
		o.Chunk = 64 << 10
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ab, ba SpliceDirection
	var dgs SpliceResult // datagram counts, merged at the end
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ab = spliceDir(ctx, a, b, o.Chunk, o.AtoB)
	}()
	go func() {
		defer wg.Done()
		ba = spliceDir(ctx, b, a, o.Chunk, o.BtoA)
	}()
	dg := spliceDatagrams(ctx, a, b, &dgs, o)
	wg.Wait()
	cancel()
	dg.Wait()
	ab.mergeDatagrams(&dgs.AtoB)
	ba.mergeDatagrams(&dgs.BtoA)
	return SpliceResult{AtoB: ab, BtoA: ba}, nil
}

// mergeDatagrams copies the datagram counts of d into the direction.
func (s *SpliceDirection) mergeDatagrams(d *SpliceDirection) {
	s.DatagramBytes, s.DatagramsDropped = d.DatagramBytes, d.DatagramsDropped
	s.DatagramsTooLarge, s.DatagramsLimited = d.DatagramsTooLarge, d.DatagramsLimited
	s.DatagramsEncapsulated = d.DatagramsEncapsulated
}

// spliceDir forwards r's receiving part to w's sending part.
func spliceDir(ctx context.Context, r, w *Stream, chunk int, lim Limiter) SpliceDirection {
	var d SpliceDirection
	for {
		_, eof, stop := spliceStep(ctx, r, w, chunk, lim, 1<<62, &d)
		if stop {
			return d
		}
		if eof {
			_ = w.h.close()
			return d
		}
	}
}

// spliceStep forwards one chunk of at most limit bytes from r to w and
// counts it in d. eof reports r's FIN (w is not finished); stop that the
// direction ended otherwise, with both sides ended and d.End set.
func spliceStep(ctx context.Context, r, w *Stream, chunk int, lim Limiter, limit uint64, d *SpliceDirection) (int, bool, bool) {
	n, end, werr, err := r.h.waitReadable(ctx, w.h)
	if werr != nil {
		spliceWriteErr(r, w, werr)
		d.End = spliceEnd(werr, true)
		return 0, false, true
	}
	if err != nil {
		spliceAbort(r, w, err)
		d.End = spliceEnd(err, false)
		return 0, false, true
	}
	var want int
	if !end {
		want = int(min(uint64(chunk), n, 16<<10, limit))
		capy, err := w.h.waitSendCapacity(ctx, uint64(want))
		if err != nil {
			spliceWriteErr(r, w, err)
			d.End = spliceEnd(err, true)
			return 0, false, true
		}
		take := int(min(capy, uint64(chunk), limit))
		if lim != nil {
			take, err = lim.Ready(ctx, min(want, take), take)
			if err != nil {
				spliceAbort(r, w, err)
				d.End = SpliceEnd{Kind: SpliceCancelled}
				return 0, false, true
			}
		}
		want = take
	}
	ch, err := r.h.readChunk(ctx, max(want, 1))
	m := len(ch.Data)
	if m > 0 {
		if werr := w.h.writeChunk(ctx, ch); werr != nil {
			spliceWriteErr(r, w, werr)
			d.End = spliceEnd(werr, true)
			return 0, false, true
		}
		if lim != nil {
			lim.Charge(m)
		}
		d.Bytes += uint64(m)
	}
	switch {
	case errors.Is(err, io.EOF):
		return m, true, false
	case err != nil:
		spliceReadErr(r, w, err)
		d.End = spliceEnd(err, false)
		return m, false, true
	}
	return m, false, false
}

// spliceReadErr ends w after r's end other than FIN.
func spliceReadErr(r, w *Stream, err error) {
	var se *StreamError
	if errors.As(err, &se) && se.Remote {
		// r was reset: the bytes below its Reliable Size were forwarded;
		// they stay reliable on w.
		if se.ReliableSize > 0 && se.ErrorCode != StreamCarrierFailed {
			// A closed session has no boundary left to keep.
			_ = w.h.do(func(s *core.Session) error { s.SetReliableBoundary(w.h.st); return nil })
		}
		w.h.cancelWrite(se.ErrorCode)
		return
	}
	spliceAbort(r, w, err)
}

// spliceWriteErr: w was stopped (stop r with its code), or w's session
// ended (fail r).
func spliceWriteErr(r, w *Stream, err error) {
	var se *StreamError
	if errors.As(err, &se) && se.Remote {
		r.h.cancelRead(se.ErrorCode)
		return
	}
	spliceAbort(r, w, err)
}

// spliceAbort ends both sides: code 0 on cancellation, ZMUX_CARRIER_FAILED
// when a session was closed or lost.
func spliceAbort(r, w *Stream, err error) {
	code := StreamErrorCode(0)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		code = StreamCarrierFailed
	}
	r.h.cancelRead(code)
	w.h.cancelWrite(code)
}

// waitReadable waits until the receiving part has bytes, or its end is
// due at once (end true); the first error is w's, the second ctx's. The
// wait also ends at once when w, the sending part the bytes go to, can no
// longer send (stopped, or its session closed or lost).
func (h *handle) waitReadable(ctx context.Context, w *handle) (uint64, bool, error, error) {
	c := h.c
	for {
		c.lock()
		n, end := c.s.ReadReady(h.st)
		if n == 0 && !end {
			h.rsig.Drain()
			c.s.WaitRead(h.st, &h.rsig)
		}
		c.unlock()
		if n > 0 || end {
			return n, end, nil, nil
		}
		// w's signal is woken by a STOP_SENDING and by the end of its
		// session; drained before the check, so neither is missed.
		w.c.lock()
		w.wsig.Drain()
		e := w.c.s.WriteErrOf(w.st)
		w.c.unlock()
		if e != nil {
			return 0, false, apiErr(e), nil
		}
		select {
		case <-h.rsig.C:
		case <-w.wsig.C:
		case <-ctx.Done():
			return 0, false, nil, ctx.Err()
		}
	}
}

// spliceDatagrams forwards the streams' datagrams to each other when they
// have datagram flows, through each direction's limiter (try_ready, then
// charge), without a queue of its own.
func spliceDatagrams(ctx context.Context, a, b *Stream, res *SpliceResult, o SpliceOptions) *sync.WaitGroup {
	var wg sync.WaitGroup
	if !a.h.c.ConnectionState().SupportsDatagrams.Remote || !b.h.c.ConnectionState().SupportsDatagrams.Remote {
		return &wg
	}
	fwd := func(r, w *Stream, d *SpliceDirection, lim Limiter) {
		defer wg.Done()
		for {
			cx, p, err := r.h.receiveDatagram(ctx)
			if err != nil {
				return
			}
			switch spliceSendDatagram(w, d, lim, cx, p) {
			case datagramSent:
				continue
			case datagramTooLarge:
				d.DatagramsTooLarge++
			case datagramLimited:
				d.DatagramsLimited++
			}
			d.DatagramsDropped++
		}
	}
	wg.Add(2)
	go fwd(a, b, &res.AtoB, o.AtoB)
	go fwd(b, a, &res.BtoA, o.BtoA)
	return &wg
}

// datagramOutcome is what became of a forwarded datagram.
type datagramOutcome uint8

const (
	datagramSent     datagramOutcome = iota
	datagramTooLarge                 // above the next hop's MaxDatagramPayload
	datagramLimited                  // refused by a limiter
	datagramFailed                   // not sent for another reason
)

// spliceSendDatagram sends a forwarded datagram on w through the
// direction's limiter, counting its bytes in d when it was sent.
func spliceSendDatagram(w *Stream, d *SpliceDirection, lim Limiter, cx uint64, p []byte) datagramOutcome {
	if lim != nil && lim.TryReady(len(p)) < len(p) {
		return datagramLimited
	}
	limited, err := w.h.c.sendDatagramLimited(w.h.st, cx, p)
	var tl *DatagramTooLargeError
	switch {
	case limited:
		return datagramLimited
	case errors.As(err, &tl):
		return datagramTooLarge
	case err != nil:
		return datagramFailed
	}
	if lim != nil {
		lim.Charge(len(p))
	}
	d.DatagramBytes += uint64(len(p))
	return datagramSent
}
