package zmux

import (
	"context"
	"errors"
	"slices"

	"github.com/zmuxio/zmux-go/internal/core"
)

type spliceWriter struct {
	permit  *splicePermit
	sample  *spliceSample
	member  int
	grant   int64
	started bool
}

func (w *spliceWriter) clear(lim Limiter) {
	grant := w.grant
	w.permit.revoke()
	if lim != nil {
		lim.Refund(grant)
	}
	*w = spliceWriter{}
}

func (w *spliceWriter) pause(lim Limiter) {
	if lim == nil {
		return
	}
	lim.Refund(w.grant)
	w.grant = 0
	w.permit.revoke()
	w.permit = nil
}

func (d *spliceRelay) reserve(ctx context.Context, n int64, exact bool) bool {
	w := &d.writer
	if w.grant != 0 || d.limit == nil || n == 0 {
		return true
	}
	if w.permit == nil {
		minimum := int64(1)
		if exact {
			minimum = n
		}
		w.permit = d.reliableSlots.request(ctx, d.limit, minimum, n, false)
	}
	p := w.permit
	if p == nil || !spliceSignalReady(p.done) {
		return false
	}
	if d.observe(ctx) || d.flush {
		return false
	}
	w.permit = nil
	grant, err := p.take()
	w.grant = grant
	switch {
	case err != nil && ctx.Err() != nil:
		d.end(ctx, SpliceEnd{Kind: SpliceCancelled, Code: spliceCode(context.Cause(ctx))})
		return false
	case err != nil:
		d.result.Cause = err
		d.end(ctx, SpliceEnd{Kind: SpliceLimiterFailed})
		return false
	}
	return true
}

func (d *spliceRelay) writeGroup(ctx context.Context) bool {
	g, w := d.current, &d.writer
	capsule := g.reliableCapsule()
	if capsule && !g.streaming && !g.encoded {
		if err := d.prepareCapsules(g); err != nil {
			d.result.Cause = err
			d.end(ctx, SpliceEnd{Kind: SpliceHookFailed})
			return true
		}
	}
	if w.member == len(g.parts) {
		d.completeGroup(g)
		return true
	}
	member := &g.parts[w.member]
	if len(member.parts) == 0 {
		if g.streaming && d.capsule.left > 0 {
			return d.readCapsule(ctx, d.source.Ready, d.source.End)
		}
		d.finishMember(g, member)
		return true
	}
	ch := &member.parts[0]
	if len(ch.Data) == 0 {
		if g.queued {
			d.unchargeRoot(ch.buf)
		}
		ch.Release()
		member.parts = member.parts[1:]
		return true
	}
	n := int(min(uint64(len(ch.Data)), d.capacity(ctx)))
	if n == 0 {
		return false
	}
	if capsule && !w.started {
		if g.downgraded {
			if d.limit != nil && w.grant == 0 {
				w.grant = int64(member.wire)
				g.reserved -= w.grant
			}
		} else if !d.reserve(ctx, int64(member.wire), true) {
			return false
		}
	} else if !capsule && !d.reserve(ctx, int64(n), false) {
		return false
	}
	if d.limit != nil {
		n = int(min(int64(n), w.grant))
	}
	if n == 0 {
		d.end(ctx, SpliceEnd{Kind: SpliceLimiterFailed})
		return true
	}
	took, err := d.write(ctx, *ch, n)
	if took != 0 {
		if !w.started && w.sample == nil && d.mirror.sampled(g.meta, SpliceMirrorOutput) {
			m := *member
			if capsule {
				m.parts = m.parts[1:]
			}
			w.sample = d.sample(g.meta, SpliceMirrorOutput, w.member, m)
		}

		w.started = true
		if g.downgraded {
			g.expiry = core.Never
		}
		w.grant -= min(w.grant, int64(took))
		d.output += uint64(took)
		g.disposition.Bytes += uint64(took)
		if !g.downgraded {
			d.result.Bytes += uint64(took)
		}
		if !capsule {
			d.complete = d.output
		}
		ch.Data = ch.Data[took:]
		if len(ch.Data) == 0 {
			if g.queued {
				d.unchargeRoot(ch.buf)
			}
			ch.Release()
			member.parts = member.parts[1:]
		}
		if len(member.parts) == 0 && (!g.streaming || d.capsule.left == 0) {
			d.finishMember(g, member)
		}
	}
	if errors.Is(err, errSpliceExpired) {
		d.expireFallback()
		return true
	}
	if err != nil {
		d.result.Cause = err
		d.end(ctx, spliceEnd(ctx, err, true))
	}
	return took != 0 || err != nil
}

func (d *spliceRelay) completeGroup(g *spliceGroup) {
	d.preferSource = g.meta.Origin == SpliceInjected || g.downgraded
	d.releaseCurrent(nil)
	d.writer = spliceWriter{}
}

func (d *spliceRelay) finishMember(g *spliceGroup, m *spliceMember) {
	w := &d.writer
	d.complete = d.output
	if w.sample != nil {
		d.mirror.finish(w.sample, true)
		w.sample = nil
	}
	w.member++
	w.started = false
	d.acknowledgePause()
	if g.reliableCapsule() || m.n != 0 {
		d.result.OutputUnits++
		g.disposition.Outputs++
	}
	if g.downgraded {
		d.result.DatagramsAsCapsules++
		d.result.DatagramBytes += uint64(m.n)
	}
	if w.member == len(g.parts) {
		d.completeGroup(g)
	}
}

func (d *spliceRelay) popDatagram() (int, bool) {
	now := d.clock.now()
	best := -1
	for i, g := range d.pendingDG {
		if g.expiry <= now {
			return i, false
		}
		if d.pause != nil || !g.runnable(now) || g.permit != nil && !spliceSignalReady(g.permit.done) {
			continue
		}
		if best == -1 || g.ready < d.pendingDG[best].ready ||
			g.ready == d.pendingDG[best].ready && g.meta.Sequence < d.pendingDG[best].meta.Sequence {
			best = i
		}
	}
	return best, true
}

func (d *spliceRelay) datagramStep(ctx context.Context) bool {
	if d.preferInputDatagram {
		d.preferInputDatagram = false
		if d.readDatagram(ctx) {
			return true
		}
	}
	if i, ready := d.popDatagram(); i >= 0 {
		g := d.pendingDG[i]
		d.pendingDG = slices.Delete(d.pendingDG, i, i+1)
		if !ready {
			d.dropGroup(g, SpliceDropExpired)
		} else {
			d.sendDatagramGroup(ctx, g)
		}
		d.preferInputDatagram = true
		return true
	}
	return d.readDatagram(ctx)
}

func (d *spliceRelay) readDatagram(ctx context.Context) bool {
	if d.pause != nil || d.dgEnded {
		return false
	}
	h := d.input
	var in core.SpliceDatagramInput
	var ok bool
	h.c.locked(func() {
		if ctx.Err() != nil {
			return
		}
		s := h.c.s
		s.SetNow(h.c.clk.now())
		budget := core.SpliceReadBudget{Remaining: d.limits.BackingBytes, ForQueue: d.limit != nil || len(d.pendingDG) != 0,
			QueueRemaining: d.limits.QueueBytes - d.queuedBytes, Queued: d.queuedRoots}
		in, ok = s.TakeSpliceDatagram(h.st, d.sizes.packet, budget)
		if !ok {
			end, err := s.StreamDatagramsEnded(h.st)
			d.dgEnded = end || err != nil
		}
	})
	if !ok {
		return false
	}
	if in.TooLarge {
		d.result.countDrop(SpliceDropTooLarge)
		return true
	}
	p, cx, expires := in.Data, in.Context, in.Expires
	d.result.InputBytes += uint64(len(p))
	d.result.PayloadCopyBytes += uint64(in.Copied)
	ch := spliceChunk{Data: p, buf: in.Backing, lease: in.Lease}
	if in.Backing == nil {
		ch = ownedSpliceChunk(p)
	}
	if d.limit == nil && len(d.hooks.Datagram) == 0 && d.mirror == nil && len(d.pendingDG) == 0 {
		g := &d.directDG
		d.initGroup(g, SpliceDatagram, SpliceReceived, 0, 0, cx)
		d.receiveDeadline(g, expires)
		d.directDGPart[0] = ch
		d.directDGMember[0] = spliceMember{parts: d.directDGPart[:], n: len(p)}
		g.parts = d.directDGMember[:]
		g.backing = splicePayloadCost(g.parts)
		d.sendDatagramGroup(ctx, g)
		return true
	}
	g := d.admit(SpliceDatagram, SpliceReceived, 0, 0, cx)
	d.receiveDeadline(g, expires)
	g.parts = []spliceMember{{parts: []spliceChunk{ch}, n: len(p)}}
	if !d.decide(ctx, g) {
		d.discard(g, nil)
		return true
	}
	// An immediately-ready unit does not consume the reordering queue.
	if now := d.clock.now(); len(d.pendingDG) == 0 && g.runnable(now) && g.expiry > now {
		d.sendDatagramGroup(ctx, g)
	} else if !d.enqueue(g) {
		d.dropGroup(g, SpliceDropQueueFull)
	}
	return true
}

func (d *spliceRelay) sendDatagramGroup(ctx context.Context, g *spliceGroup) {
	if d.observe(ctx) {
		d.discard(g, &d.result.End)
		return
	}
	if d.flush {
		d.discard(g, &d.flushEnd)
		return
	}
	d.finishWait(g)
	h := d.target
	if g.expiry <= d.clock.now() {
		d.dropGroup(g, SpliceDropExpired)
		return
	}
	var inline [1]core.DatagramGroupMember
	members := inline[:]
	if len(g.parts) != 1 {
		members = make([]core.DatagramGroupMember, len(g.parts))
	}
	total := 0
	for i, m := range g.parts {
		total += m.n
		if len(m.parts) != 0 {
			ch := m.parts[0] // a datagram view has one backing
			members[i].Payload, members[i].Backing = ch.Data, ch.buf
		}
	}
	o := h.c.clk.datagramOptions(d.options)
	if g.hopDeadline != core.Never {
		o.DeadlineMode, o.Deadline = core.DatagramDeadlineAt, g.hopDeadline
	} else {
		o.DeadlineMode = core.DatagramDeadlineNever
	}
	req := core.DatagramRequest{Stream: h.st, Context: g.meta.ContextID, Options: o, At: g.targetAt, Owned: true}
	if d.packet && !d.fin {
		req.Representation = core.DatagramOrCapsule
	}
	var grant int64
	if d.limit != nil {
		var class core.DatagramAdmission
		var err error
		h.c.peek(func() {
			h.c.s.SetNow(h.c.clk.now())
			class, err = h.c.s.SpliceDatagramClass(req, members)
		})
		if err != nil {
			d.dropGroup(g, spliceDropFor(apiErr(err)))
			return
		}
		cost := total
		if class == core.DatagramCapsule {
			cost, _, err = spliceCapsuleCost(g, d.limits)
			if err != nil {
				d.failDatagram(ctx, g, err)
				return
			}
		}
		if g.permit != nil && g.pricedClass != class {
			g.permit.revoke()
			g.permit = nil
		}
		if cost > 0 {
			if g.permit == nil {
				g.pricedClass = class
				g.permit = d.datagramSlots.request(ctx, d.limit, int64(cost), int64(cost), true)
				switch {
				case g.permit == nil:
					d.dropGroup(g, SpliceDropBlocked)
				case g.queued:
					d.pendingDG = append(d.pendingDG, g)
				case !d.enqueue(g):
					d.dropGroup(g, SpliceDropQueueFull)
				}
				return
			}
			grant, err = g.permit.take()
			g.permit = nil
			if err != nil || grant != int64(cost) {
				d.dropGroup(g, SpliceDropLimiter)
				return
			}
		}
		if class == core.DatagramCapsule {
			g.reserved = grant
			d.queueDatagramCapsules(ctx, g)
			return
		}
	}
	var tally *SpliceHandoffs
	req.PayloadCopied = &d.result.PayloadCopyBytes
	if d.limit != nil || g.measured {
		tally = d.result.Handoffs
		req.Report, req.FirstDatagram = d.handoffReport, tally.datagram
	}
	result, err := h.c.admitDatagramBatch(ctx, req, members, total, tally, !d.finished)
	if result == core.DatagramCapsule {
		d.refundDatagramGrant(grant)
		if d.limit != nil {
			if g.queued {
				d.pendingDG = append(d.pendingDG, g)
			} else if !d.enqueue(g) {
				d.dropGroup(g, SpliceDropQueueFull)
			}
		} else {
			d.queueDatagramCapsules(ctx, g)
		}
		return
	}
	if result != core.DatagramQueued {
		d.refundDatagramGrant(grant)
		if d.observe(ctx) {
			d.discard(g, &d.result.End)
		} else {
			d.dropGroup(g, spliceDropFor(err))
		}
		return
	}
	for i, m := range g.parts {
		d.result.OutputUnits++
		d.result.DatagramBytes += uint64(m.n)
		g.disposition.Bytes += uint64(m.n)
		g.disposition.Outputs++
		d.publish(g.meta, SpliceMirrorOutput, i, m)
	}
	d.release(g, nil)
}

func (d *spliceRelay) refundDatagramGrant(amount int64) {
	if amount > 0 {
		d.limit.Refund(amount)
	}
}

func (d *spliceRelay) queueDatagramCapsules(ctx context.Context, g *spliceGroup) {
	if d.fin || d.preferSource && d.source.Ready != 0 {
		d.dropGroup(g, SpliceDropBlocked)
		return
	}
	if g == &d.directDG {
		retained := d.directDG
		member := d.directDGMember[0]
		member.parts = slices.Clone(d.directDGPart[:])
		retained.parts = []spliceMember{member}
		d.directDGPart[0] = spliceChunk{}
		d.directDGMember[0] = spliceMember{}
		d.directDG = spliceGroup{}
		g = &retained
	}
	if err := d.prepareCapsules(g); err != nil {
		if errors.Is(err, errSpliceQueueFull) {
			d.dropGroup(g, SpliceDropQueueFull)
		} else {
			d.failDatagram(ctx, g, err)
		}
		return
	}
	g.downgraded = true
	g.ready, g.resumes = d.clock.now(), nil
	if g.queued {
		d.pendingReliable = append(d.pendingReliable, g)
	} else if !d.enqueue(g) {
		d.dropGroup(g, SpliceDropQueueFull)
	}
}

func (d *spliceRelay) failDatagram(ctx context.Context, g *spliceGroup, err error) {
	if !d.finished {
		d.result.Cause = err
		d.end(ctx, SpliceEnd{Kind: SpliceHookFailed})
	}
	d.discard(g, &d.result.End)
}

var errSpliceExpired = errors.New("splice fallback expired before first byte")
var errSpliceQueueFull = errors.New("splice queue is full")

func (d *spliceRelay) expireFallback() {
	d.clearWriter()
	d.dropGroup(d.current, SpliceDropExpired)
	d.current, d.currentReady = nil, false
}

func (d *spliceRelay) clearWriter() {
	d.mirror.finish(d.writer.sample, false)
	d.writer.clear(d.limit)
}

func (d *spliceRelay) receiveDeadline(g *spliceGroup, expires core.Time) {
	if expires != 0 {
		g.expiry = min(g.expiry, expires)
		g.hopDeadline = min(g.hopDeadline, g.targetAt.Add(expires.Sub(g.accepted)))
	}
}
