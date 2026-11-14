package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/zmuxio/zmux-go/varint"
)

// SplicePackets forwards two packet streams (EXTENSIONS.md Section 12)
// into each other like Splice, with the same ends, limiters, bounds and
// result, keeping capsule boundaries: capsules are passed through as they
// are, their values a chunk at a time without copying, so capsules of any
// type, unknown ones included, reach the other side unchanged. A datagram
// is forwarded as a datagram when it fits the other stream's
// MaxDatagramPayload, and otherwise inserted between two capsules as a
// DATAGRAM capsule (counted in DatagramsEncapsulated). Capsules are never
// turned into datagrams: a packet sent reliably stays reliable. The
// helper does not check the capsules it forwards; the receiving end does.
func SplicePackets(ctx context.Context, a, b *Stream, opts *SpliceOptions) (SpliceResult, error) {
	if a == b || a.h == b.h {
		return SpliceResult{}, errSpliceSelf
	}
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
	toB, toA := newCapsuleGate(), newCapsuleGate()
	var ab, ba, dab, dba SpliceDirection
	var wg, dg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ab = splicePacketDir(ctx, a, b, o.Chunk, o.AtoB, toB)
	}()
	go func() {
		defer wg.Done()
		ba = splicePacketDir(ctx, b, a, o.Chunk, o.BtoA, toA)
	}()
	if a.h.c.ConnectionState().SupportsDatagrams.Local {
		dg.Add(1)
		go func() { defer dg.Done(); splicePacketDatagrams(ctx, a, b, o.AtoB, toB, &dab) }()
	}
	if b.h.c.ConnectionState().SupportsDatagrams.Local {
		dg.Add(1)
		go func() { defer dg.Done(); splicePacketDatagrams(ctx, b, a, o.BtoA, toA, &dba) }()
	}
	wg.Wait()
	cancel()
	dg.Wait()
	ab.mergeDatagrams(&dab)
	ba.mergeDatagrams(&dba)
	return SpliceResult{AtoB: ab, BtoA: ba}, nil
}

// capsuleGate serializes the writers of one direction's stream at capsule
// boundaries: the capsule forwarder holds it from a capsule's first byte to
// its last, a datagram written as a capsule holds it for that capsule.
type capsuleGate struct {
	ch     chan struct{}
	closed bool // the stream was finished or ended (under the gate)
}

func newCapsuleGate() *capsuleGate { return &capsuleGate{ch: make(chan struct{}, 1)} }

func (g *capsuleGate) lock(ctx context.Context) error {
	select {
	case g.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *capsuleGate) unlock() { <-g.ch }

// splicePacketDir forwards r's capsules to w.
func splicePacketDir(ctx context.Context, r, w *Stream, chunk int, lim Limiter, g *capsuleGate) SpliceDirection {
	var d SpliceDirection
	var hdr [3 * 8]byte
	for {
		// Wait for the next capsule without holding the gate, so that
		// datagrams can be inserted while the stream is idle.
		_, _, werr, err := r.h.waitReadable(ctx, w.h)
		if werr != nil {
			spliceWriteErr(r, w, werr)
			d.End = spliceEnd(werr, true)
			return d
		}
		if err != nil {
			spliceAbort(r, w, err)
			d.End = spliceEnd(err, false)
			return d
		}
		if err := g.lock(ctx); err != nil {
			spliceAbort(r, w, err)
			d.End = SpliceEnd{Kind: SpliceCancelled}
			return d
		}
		end, done := splicePacketCapsule(ctx, r, w, chunk, lim, &hdr, &d)
		if end && !done {
			_ = w.h.close()
		}
		if end || done {
			g.closed = true
		}
		g.unlock()
		if end || done {
			return d
		}
	}
}

// splicePacketCapsule forwards one capsule: its header as it is, then its
// value through spliceStep. end reports r's FIN (also inside the capsule:
// the bytes so far are forwarded and the receiver sees the truncation);
// done that the direction ended otherwise.
func splicePacketCapsule(ctx context.Context, r, w *Stream, chunk int, lim Limiter, hdr *[3 * 8]byte, d *SpliceDirection) (end, done bool) {
	hn := 0
	var length uint64
	for {
		need := capsuleHeaderNeed(hdr[:hn])
		if need == 0 {
			_, k1 := varint.ConsumeVarint(hdr[:hn])
			length, _ = varint.ConsumeVarint(hdr[k1:hn])
			break
		}
		ch, err := r.h.readChunk(ctx, need)
		hn += copy(hdr[hn:], ch.Data)
		ch.Release()
		if errors.Is(err, io.EOF) {
			return true, !splicePacketWrite(ctx, r, w, lim, hdr[:hn], d)
		}
		if err != nil {
			spliceReadErr(r, w, err)
			d.End = spliceEnd(err, false)
			return false, true
		}
	}
	if !splicePacketWrite(ctx, r, w, lim, hdr[:hn], d) {
		return false, true
	}
	for length > 0 {
		m, eof, stop := spliceStep(ctx, r, w, chunk, lim, length, d)
		if stop {
			return false, true
		}
		if eof {
			return true, false
		}
		length -= uint64(m)
	}
	return false, false
}

// splicePacketWrite writes b (a capsule header) on w, charged to lim;
// false when the direction ended.
func splicePacketWrite(ctx context.Context, r, w *Stream, lim Limiter, b []byte, d *SpliceDirection) bool {
	if len(b) == 0 {
		return true
	}
	if lim != nil {
		// The header waits for its allowance like value bytes do.
		if _, err := lim.Ready(ctx, len(b), len(b)); err != nil {
			spliceAbort(r, w, err)
			d.End = SpliceEnd{Kind: SpliceCancelled}
			return false
		}
	}
	if _, err := w.h.writeCtx(ctx, b); err != nil {
		spliceWriteErr(r, w, err)
		d.End = spliceEnd(err, true)
		return false
	}
	if lim != nil {
		lim.Charge(len(b))
	}
	d.Bytes += uint64(len(b))
	return true
}

// capsuleHeaderNeed returns the bytes still missing from the Type and
// Length at the start of b (0: both complete).
func capsuleHeaderNeed(b []byte) int {
	_, i := varint.ConsumeVarint(b)
	if i == 0 {
		return varintNeed(b)
	}
	if _, k := varint.ConsumeVarint(b[i:]); k == 0 {
		return varintNeed(b[i:])
	}
	return 0
}

// splicePacketDatagrams forwards r's datagrams to w: as datagrams when
// they fit, otherwise as DATAGRAM capsules at a capsule boundary of w.
func splicePacketDatagrams(ctx context.Context, r, w *Stream, lim Limiter, g *capsuleGate, d *SpliceDirection) {
	var hdr []byte
	for {
		cx, p, err := r.h.receiveDatagram(ctx)
		if err != nil {
			return
		}
		if len(p) <= w.MaxDatagramPayload(cx) {
			switch spliceSendDatagram(w, d, lim, cx, p) {
			case datagramSent:
				continue
			case datagramLimited:
				d.DatagramsLimited++
				d.DatagramsDropped++
				continue
			case datagramFailed:
				d.DatagramsDropped++
				continue
			}
			// The bound shrank since it was read: send it as a capsule.
		}
		if lim != nil && lim.TryReady(len(p)) < len(p) {
			d.DatagramsLimited++
			d.DatagramsDropped++
			continue
		}
		if g.lock(ctx) != nil {
			d.DatagramsDropped++ // read, never written
			return
		}
		ok := !g.closed
		if ok {
			hdr = appendCapsuleHeader(hdr[:0], cx, len(p))
			_, err = w.h.writeCtx(ctx, hdr)
			if err == nil {
				_, err = w.h.writeCtx(ctx, p)
			}
			ok = err == nil
		}
		g.unlock()
		if !ok {
			d.DatagramsDropped++
			continue
		}
		if lim != nil {
			lim.Charge(len(p))
		}
		d.DatagramsEncapsulated++
		d.DatagramBytes += uint64(len(p))
	}
}
