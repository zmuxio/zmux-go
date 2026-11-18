package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Limiter lets an external limiter, budget or meter act on bytes that zmux
// forwards itself (Splice, SplicePackets, datagrams). The model is reserve,
// then refund: zmux takes bytes before it moves them and gives back what
// it did not use. Units are bytes.
//
// Arguments: maxBytes <= 0 takes nothing and returns 0 at once; minBytes
// < 1 is taken as 1; minBytes > maxBytes as minBytes = maxBytes. (n, n) is
// therefore an all-or-nothing request and (1, max) a request for as much
// as possible. A grant n satisfies min' <= n <= maxBytes, where min' is
// minBytes, or less when the limiter can never allow minBytes at once (its
// burst). For an atomic unit requested as (n, n), a successful grant lets
// the whole unit pass, even when fewer bytes were returned.
//
// Every method is safe for concurrent use. zmux calls them outside every
// session lock, never from inside the session core, and never while it
// holds a buffer of its own in Acquire.
type Limiter interface {
	// Acquire waits until it can grant. When ctx ends it returns the
	// cancellation and nothing is taken in net; an error takes nothing
	// and fails the operation that asked.
	Acquire(ctx context.Context, minBytes, maxBytes int64) (int64, error)
	// TryAcquire grants without blocking, or returns 0 with the reason
	// and nothing taken.
	TryAcquire(minBytes, maxBytes int64) (int64, error)
	// Refund gives back n granted bytes that were not used. It is
	// best-effort, never blocks and is called from any goroutine; n <= 0
	// does nothing.
	Refund(n int64)
}

// guardedLimiter runs a host Limiter on the package's goroutines: a panic
// or an invalid grant (outside [1, maxBytes]) in Acquire fails the
// operation (errLimiterFailed), in TryAcquire counts as nothing taken, and
// a panic in Refund is ignored; none ends the process. A positive invalid
// grant is refunded.
type guardedLimiter struct{ l Limiter }

var errLimiterFailed = errors.New("zmux: the Limiter panicked or returned an invalid grant")

func guardLimiter(l Limiter) Limiter {
	if l == nil {
		return nil
	}
	if _, ok := l.(guardedLimiter); ok {
		return l
	}
	return guardedLimiter{l}
}

// limitArgs normalizes a request as the contract reads it; ok is false
// when it takes nothing.
func limitArgs(minBytes, maxBytes int64) (int64, int64, bool) {
	if maxBytes <= 0 {
		return 0, 0, false
	}
	return min(max(minBytes, 1), maxBytes), maxBytes, true
}

func (g guardedLimiter) Acquire(ctx context.Context, minBytes, maxBytes int64) (n int64, err error) {
	minBytes, maxBytes, ok := limitArgs(minBytes, maxBytes)
	if !ok {
		return 0, nil
	}
	defer func() {
		if recover() != nil {
			n, err = 0, errLimiterFailed
		}
	}()
	n, err = g.l.Acquire(ctx, minBytes, maxBytes)
	if err != nil {
		return 0, err
	}
	if n < 1 || n > maxBytes {
		g.Refund(n)
		return 0, errLimiterFailed
	}
	return n, nil
}

func (g guardedLimiter) TryAcquire(minBytes, maxBytes int64) (n int64, err error) {
	minBytes, maxBytes, ok := limitArgs(minBytes, maxBytes)
	if !ok {
		return 0, nil
	}
	defer func() {
		if recover() != nil {
			n, err = 0, errLimiterFailed
		}
	}()
	n, err = g.l.TryAcquire(minBytes, maxBytes)
	if n < 0 || n > maxBytes || n > 0 && err != nil {
		g.Refund(n)
		return 0, errLimiterFailed
	}
	return n, err
}

func (g guardedLimiter) Refund(n int64) {
	if n <= 0 {
		return
	}
	defer func() { _ = recover() }()
	g.l.Refund(n)
}

// police asks for n bytes all or nothing, without waiting: it reports
// whether the unit may pass and returns the grant to refund if it is then
// not used.
func (g guardedLimiter) police(n int) (int64, bool) {
	if n <= 0 {
		return 0, true // an empty unit takes nothing
	}
	got, _ := g.TryAcquire(int64(n), int64(n))
	return got, got > 0
}

// policeGuarded is police on l, which the splice entry guarded already (it
// is wrapped here only when a caller passed it unguarded).
func policeGuarded(l Limiter, n int) (int64, bool) {
	g, ok := l.(guardedLimiter)
	if !ok {
		g = guardedLimiter{l}
	}
	return g.police(n)
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
	// DatagramsTooLarge were dropped for their size and DatagramsLimited
	// were refused by the direction's Limiter or the next session's send
	// limiter. Splice drops for its size a datagram larger than the next
	// hop's MaxDatagramPayload. SplicePackets forwards such a datagram as
	// a capsule instead (DatagramsAsCapsules) and drops for its size only
	// one larger than DefaultMaxPacket; a capsule it could not write for
	// another reason (the downstream stream ended or was reset, the splice
	// ended) counts in DatagramsDropped alone, as does a datagram whose next
	// session does not use stream datagrams.
	DatagramsDropped, DatagramsTooLarge, DatagramsLimited uint64
	// DatagramsAsCapsules counts the datagrams SplicePackets forwarded
	// as DATAGRAM capsules on the stream, once written whole (their
	// payload is in DatagramBytes); 0 for Splice.
	DatagramsAsCapsules uint64
	End                 SpliceEnd // how the direction ended
}

// SpliceEndKind is the event that ended a direction of a splice.
type SpliceEndKind uint8

// The events that end a splice direction.
const (
	SpliceFinished       SpliceEndKind = iota // FIN read upstream and passed on
	SpliceReset                               // the upstream stream was reset with Code
	SpliceStopped                             // the downstream stream was stopped with Code
	SpliceUpstreamLost                        // the upstream stream's session closed or was lost
	SpliceDownstreamLost                      // the downstream stream's session closed or was lost
	SpliceCancelled                           // ctx ended, with Code
	SpliceLimiterFailed                       // the direction's Limiter failed in Acquire
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
	case SpliceLimiterFailed:
		return "limiter failed"
	}
	return "SpliceEndKind(?)"
}

// SpliceEnd is how a direction of a splice ended; Code is the
// application error code of SpliceReset and SpliceStopped, which travels
// unchanged to the other side, that of the context's cause for
// SpliceCancelled (ZMUX_CANCELLED, the code of the reset, for a plain cancel or
// a deadline), and 0 for the other kinds. SpliceLimiterFailed resets the
// downstream and stops the upstream with ZMUX_CANCELLED; the other
// direction goes on.
type SpliceEnd struct {
	Kind SpliceEndKind
	Code StreamErrorCode
}

// String returns the end's kind, with its code for a reset, a stop or a
// cancellation ("reset 0x77").
func (e SpliceEnd) String() string {
	if e.Kind == SpliceReset || e.Kind == SpliceStopped || e.Kind == SpliceCancelled {
		return fmt.Sprintf("%v %#x", e.Kind, uint64(e.Code))
	}
	return e.Kind.String()
}

// isCtxErr reports whether err is the end of a context.
func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// spliceEnd classifies the error that ended a direction: a remote reset
// (read side) or stop (write side) with its code, a cancellation, or the
// loss of the session on that side.
func spliceEnd(ctx context.Context, err error, write bool) SpliceEnd {
	var se *StreamError
	switch {
	case errors.As(err, &se) && se.Remote && !write:
		return SpliceEnd{Kind: SpliceReset, Code: se.ErrorCode}
	case errors.As(err, &se) && se.Remote:
		return SpliceEnd{Kind: SpliceStopped, Code: se.ErrorCode}
	case isCtxErr(err):
		return SpliceEnd{Kind: SpliceCancelled, Code: spliceCode(context.Cause(ctx))}
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
// side with ZMUX_CARRIER_FAILED, and cancelling ctx ends both with the code
// of its cause (below), else ZMUX_CANCELLED.
//
// A ctx cancelled by context.WithCancelCause with a cause that is, or wraps,
// a *StreamError (CancelCause(code) makes one) ends both streams in both
// directions with its ErrorCode: the downstream sending sides are reset and
// the upstream receiving sides stopped with it, and each direction's End is
// SpliceCancelled with Code set (an explicit 0 included). Any other cause, a plain
// cancel and a deadline reset the downstream sending sides with ZMUX_CANCELLED
// (StreamCancelled) and stop the upstream receiving sides with 0, and Code is
// ZMUX_CANCELLED.
func Splice(ctx context.Context, a, b *Stream, opts *SpliceOptions) (SpliceResult, error) {
	if a == nil || b == nil {
		return SpliceResult{}, net.ErrClosed
	}
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
	o.AtoB, o.BtoA = guardLimiter(o.AtoB), guardLimiter(o.BtoA) // host code on the splice's goroutines
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ab, ba SpliceDirection
	var dgs SpliceResult // datagram counts, merged at the end
	var wg sync.WaitGroup
	wg.Go(func() { ab = spliceDir(ctx, a, b, o.Chunk, o.AtoB) })
	wg.Go(func() { ba = spliceDir(ctx, b, a, o.Chunk, o.BtoA) })
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
	s.DatagramsAsCapsules = d.DatagramsAsCapsules
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
		spliceWriteErr(ctx, r, w, werr)
		d.End = spliceEnd(ctx, werr, true)
		return 0, false, true
	}
	if err != nil {
		spliceAbort(ctx, r, w, err)
		d.End = spliceEnd(ctx, err, false)
		return 0, false, true
	}
	var want int
	var granted int64
	if !end {
		// Step 1: w's capacity for at least min(chunk, readable, 16 KiB).
		need := min(uint64(chunk), n, 16<<10, limit)
		capy, err := w.h.waitSendCapacity(ctx, need)
		if err != nil {
			spliceWriteErr(ctx, r, w, err)
			d.End = spliceEnd(ctx, err, true)
			return 0, false, true
		}
		want = int(min(capy, uint64(chunk), limit))
		if lim != nil {
			// Step 2: r is readable and nothing was consumed; reserve
			// at least one byte, at most what is readable.
			granted, err = lim.Acquire(ctx, 1, int64(min(uint64(want), n)))
			if err != nil {
				d.End = spliceAcquireFailed(ctx, r, w, err)
				return 0, false, true
			}
			want = int(granted)
		}
	}
	// Step 3: r has want bytes readable (or its end), so this does not
	// block; at most the grant is taken.
	ch, err := r.h.readChunk(ctx, max(want, 1))
	// A chunk carries the bytes read before an error, as io.Reader does.
	//goland:noinspection GoDfaErrorMayBeNotNil
	m := len(ch.Data)
	if lim != nil {
		lim.Refund(granted - int64(m)) // step 4: granted and not forwarded
	}
	if m > 0 {
		if werr := w.h.writeChunk(ctx, ch); werr != nil {
			if lim != nil {
				lim.Refund(int64(m)) // read, not forwarded
			}
			spliceWriteErr(ctx, r, w, werr)
			d.End = spliceEnd(ctx, werr, true)
			return 0, false, true
		}
		d.Bytes += uint64(m)
	}
	switch {
	case errors.Is(err, io.EOF):
		return m, true, false
	case err != nil:
		spliceReadErr(ctx, r, w, err)
		d.End = spliceEnd(ctx, err, false)
		return m, false, true
	}
	return m, false, false
}

// spliceReadErr ends w after r's end other than FIN.
func spliceReadErr(ctx context.Context, r, w *Stream, err error) {
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
	spliceAbort(ctx, r, w, err)
}

// spliceWriteErr: w was stopped (stop r with its code), or w's session
// ended (fail r).
func spliceWriteErr(ctx context.Context, r, w *Stream, err error) {
	var se *StreamError
	if errors.As(err, &se) && se.Remote {
		r.h.cancelRead(se.ErrorCode)
		return
	}
	spliceAbort(ctx, r, w, err)
}

// spliceAbortCode is the code spliceAbort ends both sides with: the code of
// ctx's cancellation cause (ZMUX_CANCELLED for a plain cancel), ZMUX_CARRIER_FAILED when a
// session was closed or lost.
func spliceAbortCode(ctx context.Context, err error) StreamErrorCode {
	if isCtxErr(err) {
		return spliceCode(context.Cause(ctx))
	}
	return StreamCarrierFailed
}

// spliceAbort resets w with spliceAbortCode and stops r with it, except that
// a cancellation without a code stops r with 0: an abandoned receiving part
// is stopped with no code (EXTENSIONS.md Section 7.4).
func spliceAbort(ctx context.Context, r, w *Stream, err error) {
	code := spliceAbortCode(ctx, err)
	stop := code
	if isCtxErr(err) {
		if _, ok := errors.AsType[*StreamError](context.Cause(ctx)); !ok {
			stop = 0
		}
	}
	r.h.cancelRead(stop)
	w.h.cancelWrite(code)
}

// spliceAcquireFailed ends a direction whose Limiter.Acquire returned err:
// a cancellation of ctx as such, any other error (the limiter's own, a
// panic or an invalid grant) with SpliceLimiterFailed, w reset and r
// stopped with ZMUX_CANCELLED; the other direction goes on.
func spliceAcquireFailed(ctx context.Context, r, w *Stream, err error) SpliceEnd {
	if ctx.Err() != nil && isCtxErr(err) {
		spliceAbort(ctx, r, w, err)
		return SpliceEnd{Kind: SpliceCancelled, Code: spliceAbortCode(ctx, err)}
	}
	r.h.cancelRead(StreamCancelled)
	w.h.cancelWrite(StreamCancelled)
	return SpliceEnd{Kind: SpliceLimiterFailed}
}

// waitReadable waits until the receiving part has bytes, or its end is
// due at once (end true); the first error is w's, the second ctx's. The
// wait also ends at once when w, the sending part the bytes go to, can no
// longer send (stopped, or its session closed or lost).
func (h *handle) waitReadable(ctx context.Context, w *handle) (uint64, bool, error, error) {
	c := h.c
	// The one wait on two sessions' slots (the plan's exception to
	// awaitRead): each is drained under its own session's lock.
	for {
		var n uint64
		var end bool
		c.locked(func() {
			if n, end = c.s.ReadReady(h.st); n == 0 && !end {
				h.rsig.Drain()
			}
		})
		if n > 0 || end {
			return n, end, nil, nil
		}
		// w's signal is woken by a STOP_SENDING and by the end of its
		// session; drained before the check, so neither is missed.
		var e error
		w.c.locked(func() {
			w.wsig.Drain()
			e = w.c.s.WriteErrOf(w.st)
		})
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
// have datagram flows, through each direction's limiter (TryAcquire(n, n),
// refunded if not sent), without a queue of its own.
func spliceDatagrams(ctx context.Context, a, b *Stream, res *SpliceResult, o SpliceOptions) *sync.WaitGroup {
	var wg sync.WaitGroup
	// A direction reads the datagrams its source session can receive. One
	// whose next session does not use stream datagrams (not in use) counts
	// them in DatagramsDropped alone: not for their size.
	fwd := func(r, w *Stream, d *SpliceDirection, lim Limiter) {
		defer wg.Done()
		if !r.h.c.ConnectionState().SupportsDatagrams.Local {
			return
		}
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
			case datagramFailed:
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
	var granted int64
	if lim != nil {
		var ok bool
		if granted, ok = policeGuarded(lim, len(p)); !ok {
			return datagramLimited
		}
	}
	limited, err := w.h.c.sendDatagramLimited(w.h.st, cx, p)
	if lim != nil && (limited || err != nil) {
		lim.Refund(granted) // not sent
	}
	var tl *DatagramTooLargeError
	switch {
	case limited:
		return datagramLimited
	case errors.As(err, &tl):
		return datagramTooLarge
	case err != nil:
		return datagramFailed
	}
	d.DatagramBytes += uint64(len(p))
	return datagramSent
}
