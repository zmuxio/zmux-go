package zmux

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Limiter lets an external limiter, budget or meter act on bytes that zmux
// forwards itself (Splice, datagrams; API.md A.1). zmux calls it outside
// every session lock.
type Limiter interface {
	// Ready waits until at least min bytes (or the limiter's burst, if
	// less) are allowed and returns the allowance, at most max.
	Ready(ctx context.Context, min, max int) (int, error)
	// TryReady returns the allowance now, 0 when it would have to wait.
	TryReady(max int) int
	// Charge accounts n bytes actually forwarded.
	Charge(n int)
}

// SpliceOptions configure Splice.
type SpliceOptions struct {
	// Chunk is the most one direction holds at once (64 KiB).
	Chunk int
	// AtoB and BtoA limit the two directions (nil: none).
	AtoB, BtoA Limiter
}

// SpliceDirection is the result of one direction of a splice.
type SpliceDirection struct {
	Bytes            uint64 // stream bytes forwarded
	DatagramBytes    uint64 // datagram bytes forwarded
	DatagramsDropped uint64
	// End is how the direction ended: nil for a FIN, else the reset,
	// stop, session error or cancellation that ended it.
	End error
}

// SpliceResult is the result of Splice.
type SpliceResult struct {
	AtoB, BtoA SpliceDirection
}

var errSpliceSelf = errors.New("zmux: cannot splice a stream into itself")

// Splice forwards two bidirectional streams into each other until both
// directions have ended (API.md A.2): what arrives on a is sent on b and
// what arrives on b is sent on a, a chunk at a time, without copying and
// only within the receiving side's credit, so backpressure crosses the
// relay hop by hop. Ends travel as the table of A.2 says: FIN finishes the
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
	if o.Chunk <= 0 {
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
	ab.DatagramBytes, ab.DatagramsDropped = dgs.AtoB.DatagramBytes, dgs.AtoB.DatagramsDropped
	ba.DatagramBytes, ba.DatagramsDropped = dgs.BtoA.DatagramBytes, dgs.BtoA.DatagramsDropped
	return SpliceResult{AtoB: ab, BtoA: ba}, nil
}

// spliceDir forwards r's receiving part to w's sending part.
func spliceDir(ctx context.Context, r, w *Stream, chunk int, lim Limiter) SpliceDirection {
	var d SpliceDirection
	for {
		n, end, err := r.h.waitReadable(ctx)
		if err != nil {
			spliceAbort(r, w, err)
			d.End = err
			return d
		}
		var want int
		if !end {
			want = min(chunk, int(min(n, 16<<10)))
			capy, err := w.h.waitSendCapacity(ctx, uint64(want))
			if err != nil {
				spliceWriteErr(r, w, err)
				d.End = err
				return d
			}
			take := min(int(capy), chunk)
			if lim != nil {
				take, err = lim.Ready(ctx, min(want, take), take)
				if err != nil {
					spliceAbort(r, w, err)
					d.End = err
					return d
				}
			}
			want = take
		}
		ch, err := r.h.readChunk(ctx, max(want, 1))
		if len(ch.Data) > 0 {
			m := len(ch.Data)
			if werr := w.h.writeChunk(ctx, ch); werr != nil {
				spliceWriteErr(r, w, werr)
				d.End = werr
				return d
			}
			if lim != nil {
				lim.Charge(m)
			}
			d.Bytes += uint64(m)
		}
		switch {
		case errors.Is(err, io.EOF):
			_ = w.h.close()
			return d
		case err != nil:
			spliceReadErr(r, w, err)
			d.End = err
			return d
		}
	}
}

// spliceReadErr ends w after r's end other than FIN.
func spliceReadErr(r, w *Stream, err error) {
	var se *StreamError
	if errors.As(err, &se) && se.Remote {
		// r was reset: the bytes below its Reliable Size were forwarded;
		// they stay reliable on w.
		if se.ReliableSize > 0 && se.ErrorCode != CodeCarrierFailed {
			w.h.do(func(s *core.Session) error { s.SetReliableBoundary(w.h.st); return nil })
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
		code = CodeCarrierFailed
	}
	r.h.cancelRead(code)
	w.h.cancelWrite(code)
}

// waitReadable waits until the receiving part has bytes, or its end is
// due at once (end true).
func (h *handle) waitReadable(ctx context.Context) (uint64, bool, error) {
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
			return n, end, nil
		}
		select {
		case <-h.rsig.C:
		case <-ctx.Done():
			return 0, false, ctx.Err()
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
			if lim != nil && lim.TryReady(len(p)) < len(p) {
				d.DatagramsDropped++
				continue
			}
			if w.h.sendDatagram(cx, p) != nil {
				d.DatagramsDropped++
				continue
			}
			if lim != nil {
				lim.Charge(len(p))
			}
			d.DatagramBytes += uint64(len(p))
		}
	}
	wg.Add(2)
	go fwd(a, b, &res.AtoB, o.AtoB)
	go fwd(b, a, &res.BtoA, o.BtoA)
	return &wg
}
