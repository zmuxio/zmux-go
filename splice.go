package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Limiter lets an external limiter, budget or meter act on bytes that zmux
// forwards itself (Splice, SplicePackets, datagrams). The model is reserve,
// then refund. A splice charges logical bytes at their first successful
// local carrier handoff. Session-level datagram limits are independent,
// so attach a budget at one layer if it should be applied only once.
//
// Arguments: maxBytes <= 0 takes nothing and returns 0 at once; minBytes
// < 1 is taken as 1; minBytes > maxBytes as minBytes = maxBytes. (n, n) is
// therefore an exact atomic reservation and (1, max) can grant a prefix.
// A positive grant is always within the normalized inclusive range. A
// request beyond the limiter's atomic capacity returns an error immediately.
//
// Methods are concurrent and run outside session locks. Splice may hold
// one bounded output while acquiring. Do not wait for the splice to
// complete a command from inside a limiter method.
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
// or an invalid grant (outside [minBytes, maxBytes]) in Acquire fails the
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
		if n > 0 {
			g.Refund(n)
			return 0, errLimiterFailed
		}
		return 0, err
	}
	if n < minBytes || n > maxBytes {
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
	if n < 0 || n > 0 && (n < minBytes || n > maxBytes || err != nil) {
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

// SpliceLimiter declares the largest atomic reservation a direction can make.
// MaxBurstBytes returns -1 for unlimited capacity. A zero or insufficient
// maximum rejects installation before either stream is bound.
type SpliceLimiter interface {
	Limiter
	MaxBurstBytes() int64
}

func validSpliceBurst(lim SpliceLimiter, required int) (valid bool) {
	if lim == nil {
		return true
	}
	defer func() {
		if recover() != nil {
			valid = false
		}
	}()
	maximum := lim.MaxBurstBytes()
	return maximum == -1 || maximum >= int64(required)
}

// SpliceOptions configure Splice.
type SpliceOptions struct {
	// AtoB and BtoA limit the two directions (nil: none).
	AtoB, BtoA SpliceLimiter
	// MirrorAtoB and MirrorBtoA observe local policy and output events.
	MirrorAtoB, MirrorBtoA *SpliceMirror
	// Event observers run on the direction owner outside session locks.
	// They must return promptly and must not wait for this splice.
	// Event observers run on the direction owner. A panic detaches the observer.
	OnEventAtoB, OnEventBtoA func(SpliceEvent)
	// TagA and TagB are application labels for the splice endpoints.
	TagA, TagB string
	// Hooks are frozen when the splice starts.
	HooksAtoB, HooksBtoA SpliceHooks
	// DatagramAtoB and DatagramBtoA apply to their local forwarding hop.
	DatagramAtoB, DatagramBtoA DatagramSendOptions
	// Limits bounds the controlled splice and packet assembly.
	Limits SpliceLimits
	// Chunk caps each reliable stream read and byte-stream output item.
	// Zero selects 64 KiB. Reliable capsules have their own size limit.
	Chunk int
	// MaxPacketPayload and MaxOpaqueValue bound packet relay input and
	// replacements. Zero uses the ordinary packet and opaque defaults.
	MaxPacketPayload, MaxOpaqueValue int
}

// SpliceDirection is the result of one direction of a splice.
type SpliceDirection struct {
	// Cause is the original local or upstream error, when available.
	Cause error
	// Handoffs accounts for output while splice policy or a direction
	// limiter is active. It can still settle after the direction returns.
	Handoffs              *SpliceHandoffs
	End                   SpliceEnd // how the direction ended
	InputBytes, HookCalls uint64
	WaitTime              time.Duration
	Bytes                 uint64 // stream bytes forwarded
	CompleteOutputBytes   uint64 // reliable bytes through the last complete output unit
	DatagramBytes         uint64 // datagram payload bytes forwarded
	// Drops counts one reason per locally discarded datagram group.
	Drops                                                                            map[SpliceDropReason]uint64
	QueuedBytes, DelayedDatagrams, FallbackGroups, GroupPeakBytes, OutputDescriptors int
	PayloadCopyBytes, EncodeBytes, MirrorCopyBytes                                   uint64
	SourceBatches, WaitTimers, ReservedBytes                                         uint64
	LiveGroupRoots, DatagramCleanupInUse                                             int
	// DatagramsAsCapsules counts the datagrams SplicePackets forwarded
	// as DATAGRAM capsules on the stream, once written whole (their
	// payload is in DatagramBytes); 0 for Splice.
	DatagramsAsCapsules                                uint64
	InputUnits, OutputUnits, HookDropped, HookReplaced uint64
	MirrorDropped                                      uint64
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
	SpliceHookReset
	SpliceHookFailed
	SplicePacketError
	SpliceWaitTimeout
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
		return "upstream_lost"
	case SpliceDownstreamLost:
		return "downstream_lost"
	case SpliceCancelled:
		return "cancelled"
	case SpliceLimiterFailed:
		return "limiter_failed"
	case SpliceHookReset:
		return "hook_reset"
	case SpliceHookFailed:
		return "hook_failed"
	case SplicePacketError:
		return "packet_error"
	case SpliceWaitTimeout:
		return "wait_timeout"
	}
	return "SpliceEndKind(?)"
}

// SpliceEnd is how a direction of a splice ended; Code is the
// application error code of SpliceReset and SpliceStopped, which travels
// unchanged to the other side, that of the context's cause for
// SpliceCancelled (ZMUX_CANCELLED for a plain cancel or deadline), or the
// code for SpliceHookReset and SplicePacketError. Other kinds use zero. SpliceLimiterFailed resets the
// downstream and stops the upstream with ZMUX_CANCELLED; the other
// direction goes on.
type SpliceEnd struct {
	Kind SpliceEndKind
	Code StreamErrorCode
}

// String returns the end's kind, with its code for a reset, a stop or a
// cancellation ("reset 0x77").
func (e SpliceEnd) String() string {
	if e.hasCode() {
		return fmt.Sprintf("%v %#x", e.Kind, uint64(e.Code))
	}
	return e.Kind.String()
}

func (e SpliceEnd) hasCode() bool {
	return e.Kind == SpliceReset || e.Kind == SpliceStopped || e.Kind == SpliceCancelled || e.Kind == SpliceHookReset
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

// SpliceResult is the result of a bidirectional splice.
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
	if opts != nil && (!opts.HooksAtoB.empty() || !opts.HooksBtoA.empty() ||
		opts.OnEventAtoB != nil || opts.OnEventBtoA != nil || opts.MirrorAtoB != nil || opts.MirrorBtoA != nil || opts.AtoB != nil || opts.BtoA != nil) {
		relay, err := startSplice(ctx, a, b, opts, false, false)
		if err != nil {
			return SpliceResult{}, err
		}
		return relay.Wait(context.Background())
	}
	return splice(ctx, a, b, opts)
}

// splice keeps the static byte forwarding path free of control work.
func splice(ctx context.Context, a, b *Stream, opts *SpliceOptions) (SpliceResult, error) {
	o, err := normalizeSplice(a, b, opts)
	if err != nil {
		return SpliceResult{}, err
	}
	limits, err := o.Limits.defaults((spliceUnitSizes{o.Chunk, o.MaxPacketPayload, o.MaxOpaqueValue}).largest(false))
	if err != nil {
		return SpliceResult{}, err
	}
	for _, h := range [...]*handle{a.h, b.h} {
		var resetAt bool
		h.c.peek(func() { resetAt = h.c.s.SpliceResetAt() })
		if !resetAt {
			relay, err := startSplice(ctx, a, b, &o, false, false)
			if err != nil {
				return SpliceResult{}, err
			}
			return relay.Wait(context.Background())
		}
	}
	if err := bindSplice(a.h, b.h); err != nil {
		return SpliceResult{}, err
	}
	defer unbindSplice(a.h, b.h)
	defer runtime.KeepAlive(a)
	defer runtime.KeepAlive(b)
	directions := [...]struct {
		r, w    *Stream
		options DatagramSendOptions
	}{{a, b, o.DatagramAtoB}, {b, a, o.DatagramBtoA}}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var reliable, datagrams sync.WaitGroup
	var result, dgResult [2]SpliceDirection
	for i, direction := range directions {
		r, w := direction.r, direction.w
		if !r.h.c.ConnectionState().SupportsDatagrams.Local {
			reliable.Go(func() { result[i] = spliceDir(ctx, r, w, o.Chunk) })
			continue
		}
		dgCtx, stopDatagrams := context.WithCancel(ctx)
		reliable.Go(func() {
			result[i] = spliceDir(ctx, r, w, o.Chunk)
			if result[i].End.Kind != SpliceFinished {
				stopDatagrams()
			}
		})
		datagrams.Go(func() {
			defer stopDatagrams()
			spliceDatagramDir(dgCtx, r, w, &dgResult[i], direction.options, o.MaxPacketPayload, limits.BackingBytes)
		})
	}
	reliable.Wait()
	cancel()
	datagrams.Wait()
	for i := range result {
		result[i].mergeDatagrams(&dgResult[i])
	}
	return SpliceResult{AtoB: result[0], BtoA: result[1]}, nil
}

// mergeDatagrams copies the datagram counts of d into the direction.
func (s *SpliceDirection) mergeDatagrams(d *SpliceDirection) {
	s.DatagramBytes, s.Drops = d.DatagramBytes, d.Drops
	s.DatagramsAsCapsules = d.DatagramsAsCapsules
	s.PayloadCopyBytes += d.PayloadCopyBytes
}

// spliceDir is the static byte forwarding path. It probes credit before
// borrowing a chunk and transfers that reference straight to the target.
func spliceDir(ctx context.Context, r, w *Stream, chunk int) SpliceDirection {
	var d SpliceDirection
	for {
		if err := ctx.Err(); err != nil {
			spliceAbort(ctx, r, w, err)
			d.Cause = err
			d.End = spliceEnd(ctx, err, false)
			return d
		}
		n, end, werr, err := r.h.waitReadable(ctx, w.h)
		if werr == nil && err == nil && !end {
			n, werr = w.h.waitSendCapacity(ctx, min(uint64(chunk), n, 16<<10))
		}
		if werr != nil {
			spliceWriteErr(ctx, r, w, werr)
			d.Cause = werr
			d.End = spliceEnd(ctx, werr, true)
			return d
		}
		if err != nil {
			spliceAbort(ctx, r, w, err)
			d.Cause = err
			d.End = spliceEnd(ctx, err, false)
			return d
		}
		ch, err := r.h.readChunk(ctx, max(1, int(min(uint64(chunk), n))))
		if len(ch.Data) != 0 {
			wrote, werr := w.h.writeChunkProgress(ctx, ch)
			d.Bytes += uint64(wrote)
			if werr != nil {
				spliceWriteErr(ctx, r, w, werr)
				d.Cause = werr
				d.End = spliceEnd(ctx, werr, true)
				return d
			}
		}
		switch {
		case errors.Is(err, io.EOF):
			if werr := w.h.close(); werr != nil {
				spliceWriteErr(ctx, r, w, werr)
				d.Cause = werr
				d.End = spliceEnd(ctx, werr, true)
			}
			return d
		case err != nil:
			spliceReadErr(ctx, r, w, err)
			d.Cause = err
			d.End = spliceEnd(ctx, err, false)
			return d
		}
	}
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

// spliceDatagramDir transfers each owned datagram directly to the target.
func spliceDatagramDir(ctx context.Context, r, w *Stream, d *SpliceDirection, opts DatagramSendOptions, maxPayload, backing int) {
	for ctx.Err() == nil {
		var in core.SpliceDatagramInput
		err := r.h.waitDatagram(ctx, func() bool {
			var ok bool
			in, ok = r.h.c.s.TakeSpliceDatagram(r.h.st, maxPayload, core.SpliceReadBudget{Remaining: backing})
			return ok
		})
		if err != nil {
			break
		}
		if in.TooLarge {
			d.countDrop(SpliceDropTooLarge)
			continue
		}
		h := w.h
		n := len(in.Data)
		at, options := h.c.clk.now(), h.c.clk.datagramOptions(opts)
		if in.Expires != 0 {
			h.c.peek(func() { options = h.c.s.DatagramOptions(h.st, options) })
			hop, _ := options.ResolveDeadline(at)
			options.DeadlineMode = core.DatagramDeadlineAt
			options.Deadline = min(hop, at.Add(in.Expires.Sub(r.h.c.clk.now())))
		}
		d.PayloadCopyBytes += uint64(in.Copied)
		admission, err := h.c.admitDatagram(ctx, core.DatagramRequest{
			Stream: h.st, Context: in.Context, Payload: in.Data, Backing: in.Backing, Owned: true,
			Options: options, At: at, PayloadCopied: &d.PayloadCopyBytes,
		})
		in.Lease.Release()
		if in.Backing != nil {
			in.Backing.Unref()
		}
		if admission == core.DatagramQueued {
			d.DatagramBytes += uint64(n)
			continue
		}
		d.countDrop(spliceDropFor(err))
	}
}
