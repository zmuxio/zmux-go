package zmux

import (
	"context"
	"errors"
	"io"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/varint"
)

// SplicePackets forwards two streams whose endpoints use packet mode.
// It checks bounds and preserves capsule order and unknown types. Hooks
// for reliable capsules see the complete value before any is forwarded.
// Eligible stream datagrams can be converted to DATAGRAM capsules.
func SplicePackets(ctx context.Context, a, b *Stream, opts *SpliceOptions) (SpliceResult, error) {
	relay, err := startSplice(ctx, a, b, opts, true, false)
	if err != nil {
		return SpliceResult{}, err
	}
	return relay.Wait(context.Background())
}

type spliceCapsule struct {
	pins            map[*core.Buf]struct{}
	parts           []spliceChunk
	compact         []byte
	staging         [1]spliceChunk
	header          capsuleHeader
	n               int
	left            int
	pos             int
	backing         int
	raw             [24]byte
	parsed, collect bool
	eof             bool
}

func (p *spliceCapsule) reset() {
	releaseSpliceChunks(p.parts)
	*p = spliceCapsule{}
}

// readCapsule consumes at most one fragment. The first fragment has
// already reserved a position in the reliable sequence.
func (d *spliceRelay) readCapsule(ctx context.Context, ready uint64, end bool) bool {
	p, g := &d.capsule, d.current
	if !p.parsed {
		h, need, err := parseCapsuleHeader(p.raw[:p.n], d.sizes.packet, uint64(d.sizes.opaque))
		switch {
		case err != nil:
			d.badCapsule(ctx, err)
			return true
		case need != 0:
			if p.eof {
				d.badCapsule(ctx, errCapsuleTruncated)
				return true
			}
			r := d.read(ctx, min(need, d.sizes.chunk), ready, end)
			n := len(r.chunk.Data)
			p.n += n
			r.chunk.Release()
			if errors.Is(r.end, io.EOF) {
				p.eof = true
			} else if r.end != nil {
				d.capsuleReadEnd(ctx, r.end)
			}
			return n > 0 || r.end != nil
		}
		g.meta.Type, g.meta.ContextID = h.typ, h.cx
		p.header, p.left, p.parsed = h, int(h.payload), true
		if p.eof && p.left != 0 {
			d.badCapsule(ctx, errCapsuleTruncated)
			return true
		}
		if p.eof {
			d.fin = true
		}
		g.raw = p.raw[:p.n]
		g.sourceEnd = g.meta.Offset + uint64(h.n) + h.payload
		if !p.collect {
			p.staging[0] = spliceChunk{Data: p.raw[:p.n]}
			d.directMember[0] = spliceMember{n: p.left, wire: p.n + p.left, parts: p.staging[:]}
			g.parts = d.directMember[:]
			g.streaming = true
			d.currentReady = true
			return true
		}
	}
	if p.left > 0 {
		r := d.read(ctx, min(p.left, d.sizes.chunk), ready, end)
		ch := r.chunk
		n := len(ch.Data)
		if n > 0 {
			p.left -= n
			if p.collect {
				if p.compact != nil {
					p.pos += n
				} else {
					p.retain(ch)
				}
			} else {
				p.staging[0] = ch
				g.parts[0].parts = p.staging[:]
			}
		}
		if errors.Is(r.end, io.EOF) && p.left == 0 {
			d.fin = true
		} else if r.end != nil {
			d.capsuleReadEnd(ctx, r.end)
			return true
		}
		if p.left != 0 || !p.collect {
			return n > 0
		}
	}
	if p.collect {
		p.finish(g)
		g.expiry = core.Never
		d.currentReady = d.decide(ctx, g)
		if !d.currentReady && d.current == g {
			d.discard(g, nil)
			d.capsule.reset()
			d.current = nil
		}
	}
	return true
}

func (p *spliceCapsule) retain(ch spliceChunk) {
	if p.pins == nil {
		p.pins = make(map[*core.Buf]struct{})
	}
	if _, ok := p.pins[ch.buf]; !ok {
		p.pins[ch.buf] = struct{}{}
		p.backing += cap(ch.buf.B)
	}
	p.parts = append(p.parts, ch)
}

// compactPayload reserves the final buffer while every previous fragment
// is still live. Subsequent ordinary reads land directly in this buffer.
func (p *spliceCapsule) compactPayload(limit int) (int, error) {
	if p.compact != nil {
		return 0, nil
	}
	if int(p.header.payload) > limit-p.backing {
		return 0, ErrSpliceBudget
	}
	p.compact = make([]byte, int(p.header.payload))
	for _, previous := range p.parts {
		p.pos += copy(p.compact[p.pos:], previous.Data)
	}
	releaseSpliceChunks(p.parts)
	p.parts, p.pins = nil, nil
	p.backing = cap(p.compact)
	return p.pos, nil
}

func (p *spliceCapsule) finish(g *spliceGroup) {
	if p.compact != nil {
		p.parts = []spliceChunk{ownedSpliceChunk(p.compact)}
		p.compact = nil
	}
	g.parts = []spliceMember{{parts: p.parts, n: int(p.header.payload)}}
	p.parts, p.pins = nil, nil
}

func (d *spliceRelay) capsuleReadEnd(ctx context.Context, err error) {
	if errors.Is(err, ErrSpliceBudget) {
		d.result.Cause = err
		d.end(ctx, SpliceEnd{Kind: SpliceHookFailed})
		return
	}
	if errors.Is(err, io.EOF) {
		d.badCapsule(ctx, errCapsuleTruncated)
		return
	}
	d.readEnd(ctx, err)
}

func (d *spliceRelay) badCapsule(ctx context.Context, err error) {
	d.result.Cause = err
	d.resetAfterPrefix(ctx, SpliceEnd{Kind: SplicePacketError, Code: StreamPacketError})
}

func encodeSpliceCapsule(dst []byte, typ, cx uint64, n int) []byte {
	if typ == capsuleDatagram {
		return appendCapsuleHeader(dst, cx, n)
	}
	h := varint.AppendVarint(dst, typ)
	return varint.AppendVarint(h, uint64(n))
}

func spliceCapsuleCost(g *spliceGroup, limits SpliceLimits) (total, backing int, err error) {
	backing, segments := spliceGroupCost(g.parts)
	if len(g.parts) > limits.OutputItems || segments > limits.Segments-len(g.parts) {
		return 0, 0, ErrSpliceBudget
	}
	for _, m := range g.parts {
		header := spliceCapsuleHeaderLen(g, m.n)
		if header > limits.BackingBytes-backing || header > limits.OutputBytes-total || m.n > limits.OutputBytes-total-header {
			return 0, 0, ErrSpliceBudget
		}
		backing += header
		total += header + m.n
	}
	return total, backing, nil
}

func (d *spliceRelay) prepareCapsules(g *spliceGroup) error {
	_, backing, err := spliceCapsuleCost(g, d.limits)
	if err != nil {
		return err
	}
	if g.queued && backing-g.backing > d.limits.QueueBytes-d.queuedBytes {
		return errSpliceQueueFull
	}
	for i := range g.parts {
		m := &g.parts[i]
		h := g.raw
		if h == nil {
			h = encodeSpliceCapsule(make([]byte, 0, spliceCapsuleHeaderLen(g, m.n)), g.meta.Type, g.meta.ContextID, m.n)
		}
		m.wire = len(h) + m.n
		if g.raw != nil {
			saved := make([]byte, len(h))
			copy(saved, h)
			h = saved
		}
		header := ownedSpliceChunk(h)
		m.parts = append([]spliceChunk{header}, m.parts...)
		d.result.EncodeBytes += uint64(len(h))
		if g.queued {
			d.chargeRoot(header.buf)
		}
	}
	g.encoded = true
	g.backing = backing
	return nil
}

func spliceCapsuleHeaderLen(g *spliceGroup, n int) int {
	if g.raw != nil {
		return len(g.raw)
	}
	contextSize := 0
	if g.meta.Type == capsuleDatagram {
		contextSize = varint.SizeVarint(g.meta.ContextID)
	}
	return varint.SizeVarint(g.meta.Type) + varint.SizeVarint(uint64(n+contextSize)) + contextSize
}
