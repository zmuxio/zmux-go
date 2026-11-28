package zmux

import (
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"reflect"
	"runtime"
	"slices"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/varint"
)

type spliceRelay struct {
	queuedRoots                       map[*core.Buf]int
	onEvent                           func(SpliceEvent)
	batch                             splicePolicy
	batchEnd, sourceBatches           uint64
	reliableSlots, datagramSlots      *spliceSlots
	limit                             Limiter
	handoffReport                     func(sent, unsent int)
	prefix                            *splicePrefixWatch
	input, target                     *handle
	control                           *SpliceControl
	mirror                            *SpliceMirror
	bothFinished                      <-chan struct{}
	reliableDone                      func()
	pause                             *SplicePause
	waitRead, waitWrite, waitDG       <-chan struct{}
	current                           *spliceGroup
	clock                             clock
	sourceTag, targetTag              string
	hooks                             SpliceHooks
	pendingReliable, pendingDG        []*spliceGroup
	writer                            spliceWriter
	directDGPart                      [1]spliceChunk
	directPart                        [1]spliceChunk
	options                           DatagramSendOptions
	directDGMember                    [1]spliceMember
	directMember                      [1]spliceMember
	capsule                           spliceCapsule
	result                            SpliceDirection
	directDG                          spliceGroup
	directGroup                       spliceGroup
	limits                            SpliceLimits
	source                            core.SpliceReadState
	flushEnd                          SpliceEnd
	generation, sequence              uint64
	sizes                             spliceUnitSizes
	output, complete                  uint64
	initialOutput                     uint64
	pauseGeneration                   uint64
	queuedItems, queuedBytes          int
	flushAt                           uint64
	preferSource, preferInputDatagram bool
	reverse, packet                   bool
	fin, finished, stopped, dgEnded   bool
	commandOpen                       bool
	currentReady                      bool
	flush                             bool
	streamCapsules                    bool
}

func (d *spliceRelay) run(ctx context.Context) SpliceDirection {
	d.input.c.peek(func() {
		d.input.rsig.Make()
		d.input.dsig.Make()
		d.waitRead, d.waitDG = d.input.rsig.C, d.input.dsig.C
	})
	d.target.c.peek(func() {
		d.target.wsig.Make()
		d.waitWrite = d.target.wsig.C
		d.output = d.target.c.s.SpliceWriteOffset(d.target.st)
		d.streamCapsules = d.packet && d.target.c.s.SpliceResetAt()
	})
	d.complete = d.output
	d.initialOutput = d.output
	d.commandOpen = d.control != nil
	if d.limit != nil {
		d.reliableSlots = newSpliceSlots(d.limits.ReliableCleanupSlots)
		d.datagramSlots = newSpliceSlots(d.limits.DatagramCleanupSlots)
	}
	tally, lim := new(SpliceHandoffs), guardLimiter(d.limit)
	if d.limit != nil {
		d.limit = guardedLimiter{spliceReservationLimiter{d.limit, tally}}
	}
	d.result.Handoffs = tally
	d.handoffReport = func(sent, unsent int) {
		if lim != nil {
			lim.Refund(int64(unsent))
		}
		tally.complete(sent, unsent, lim != nil)
	}
	steps := 0
	for !d.stopped {
		select {
		case <-d.bothFinished:
			d.stopped = true
			continue
		default:
		}
		progress := d.observe(ctx)
		if d.stopped {
			break
		}

		progress = d.command(ctx) || progress
		if !d.finished {
			progress = d.reliableStep(ctx) || progress
		}
		if !d.stopped && !d.flush {
			progress = d.datagramStep(ctx) || progress
		}
		if !progress {
			d.wait(ctx)
			steps = 0
		} else if steps++; steps == 64 {
			// A permanently readable source (including a chain of
			// drops) should not monopolize its executor.
			runtime.Gosched()
			steps = 0
		}
	}
	if !d.finished {
		d.end(ctx, SpliceEnd{Kind: SpliceCancelled, Code: spliceCode(context.Cause(ctx))})
	}
	d.closeCommands()
	for _, g := range d.pendingDG {
		d.discard(g, &d.result.End)
	}
	d.pendingDG = nil
	return d.snapshot()
}

func (d *spliceRelay) snapshot() SpliceDirection {
	r := d.result
	r.Drops = maps.Clone(r.Drops)
	r.SourceBatches = d.sourceBatches
	if d.datagramSlots != nil {
		r.DatagramCleanupInUse = len(d.datagramSlots.used)
	}
	r.ReservedBytes = uint64(d.writer.grant)
	roots := make(map[*core.Buf]struct{})
	probe := func(g *spliceGroup) {
		if g != nil {
			r.ReservedBytes += uint64(g.reserved)
			for _, m := range g.parts {
				for _, part := range m.parts {
					if part.buf != nil {
						roots[part.buf] = struct{}{}
					}
				}
			}
		}
	}
	probe(d.current)
	for _, g := range d.pendingReliable {
		probe(g)
	}
	for _, g := range d.pendingDG {
		probe(g)
	}
	r.LiveGroupRoots = len(roots)
	if d.current != nil && d.current.downgraded {
		r.FallbackGroups++
	}
	for _, g := range d.pendingReliable {
		if g.downgraded {
			r.FallbackGroups++
		}
	}
	r.QueuedBytes, r.DelayedDatagrams = d.queuedBytes, len(d.pendingDG)
	r.CompleteOutputBytes = d.complete - d.initialOutput
	return r
}

func (d *spliceRelay) observe(ctx context.Context) bool {
	if ctx.Err() != nil {
		d.end(ctx, SpliceEnd{Kind: SpliceCancelled, Code: spliceCode(context.Cause(ctx))})
		return true
	}
	if d.finished {
		return false
	}
	var werr, rerr error
	d.target.c.peek(func() { werr = apiErr(d.target.c.s.WriteErrOf(d.target.st)) })
	d.input.c.peek(func() {
		s := d.input.c.s
		d.source = s.SpliceReadState(d.input.st)
		if s.Err() != nil {
			rerr = apiErr(s.Err())
		}
	})
	switch {
	case werr != nil:
		d.result.Cause = werr
		d.end(ctx, spliceEnd(ctx, werr, true))
	case spliceSignalReady(d.target.wwait()):
		d.end(ctx, SpliceEnd{Kind: SpliceDownstreamLost})
	case rerr != nil:
		d.result.Cause = rerr
		d.end(ctx, spliceEnd(ctx, rerr, false))
	case spliceSignalReady(d.input.rwait()):
		d.end(ctx, SpliceEnd{Kind: SpliceUpstreamLost})
	case d.source.Reset != nil && !d.flush:
		d.sealReliableForReset(ctx)
	case d.source.FIN:
		d.fin = true
	}
	return d.stopped
}

func (d *spliceRelay) sealReliableForReset(ctx context.Context) {
	d.fin = true
	e := SpliceEnd{Kind: SpliceReset, Code: StreamErrorCode(d.source.Reset.Code)}
	if len(d.pendingReliable) != 0 {
		for _, g := range d.pendingReliable {
			d.discard(g, &e)
		}
		d.pendingReliable = nil
	}
	if g := d.current; g != nil && (g.meta.Origin == SpliceInjected || g.downgraded ||
		g.sourceEnd > d.source.ResetAt && (g.meta.Kind == SpliceStream || d.currentReady || d.capsule.parsed)) {
		d.result.Cause = apiErr(d.source.Reset)
		d.resetAfterPrefix(ctx, e)
		return
	}
	if d.source.End && d.current == nil {
		r := d.read(ctx, 1, 0, true)
		d.readEnd(ctx, r.end)
	}
}

func (d *spliceRelay) command(ctx context.Context) bool {
	if !d.commandOpen {
		return false
	}
	select {
	case cmd := <-d.control.cmd:
		d.handleCommand(ctx, cmd)
		return true
	default:
		return false
	}
}

func (d *spliceRelay) handleCommand(ctx context.Context, cmd *spliceCommand) {
	if !cmd.claimed.CompareAndSwap(false, true) {
		d.control.finishCommand(cmd, spliceCommandReply{err: cmd.ctx.Err()})
		return
	}
	// A wakeup can race a terminal event with a queued control command.
	// Observe the terminal state again before applying the command.
	if !d.commandOpen || d.observe(ctx) || d.stopped {
		d.control.finishCommand(cmd, spliceCommandReply{err: ErrSpliceClosed})
		return
	}
	reply := spliceCommandReply{}
	switch cmd.kind {
	case spliceAttach:
		if err := cmd.ctx.Err(); err != nil {
			reply.err = err
		} else {
			d.hooks = cmd.hooks
			d.generation++
			d.control.policy.Store(&splicePolicy{d.hooks, d.generation})
			d.event(SpliceEvent{Name: SpliceHooksReplaced, Generation: d.generation})
			reply.generation = d.generation
		}
	case splicePause:
		if err := cmd.ctx.Err(); err != nil {
			reply.err = err
			break
		}
		if d.pause == nil {
			d.pauseGeneration++
			d.pause = &SplicePause{Generation: d.pauseGeneration, done: make(chan struct{})}
			if !d.writer.started || d.current == nil || !d.current.reliableCapsule() {
				if d.current != nil && d.current.downgraded {
					d.current.reserved += d.writer.grant
					d.writer.grant = 0
				}
				d.writer.pause(d.limit)
				d.acknowledgePause()
			}
		}
		reply.pause = d.pause
	case spliceResume:
		if err := cmd.ctx.Err(); err != nil {
			reply.err = err
		} else if d.pause != nil && d.pause == cmd.pause {
			d.pause.finish(ErrSplicePaused)
			d.pause = nil
		}
	case spliceInspect:
		reply.direction = d.snapshot()
	case spliceInject:
		d.inject(ctx, cmd)
		return
	}
	d.control.finishCommand(cmd, reply)
}

func (d *spliceRelay) closeCommands() {
	if !d.commandOpen {
		return
	}
	d.commandOpen = false
	d.control.submitMu.Lock()
	d.control.closed = true
	d.control.final = d.snapshot()
	d.control.submitMu.Unlock()
	defer close(d.control.done)
	d.pause.finish(ErrSpliceClosed)
	for {
		select {
		case cmd := <-d.control.cmd:
			d.control.finishCommand(cmd, spliceCommandReply{err: ErrSpliceClosed})
		default:
			return
		}
	}
}

func (d *spliceRelay) inject(ctx context.Context, cmd *spliceCommand) {
	in := cmd.injection
	var err error
	switch {
	case cmd.ctx.Err() != nil:
		err = cmd.ctx.Err()
	case d.fin || d.finished:
		err = ErrSpliceClosed
	case d.pause != nil:
		err = ErrSplicePaused
	case d.queuedItems == d.limits.QueueItems || len(in.Data) > d.limits.QueueBytes-d.queuedBytes ||
		in.Kind != SpliceDatagram && d.preferSource && d.source.Ready != 0:
		err = ErrSpliceBudget
	}
	if err != nil {
		d.control.finishCommand(cmd, spliceCommandReply{err: err})
		return
	}
	g := d.admit(in.Kind, SpliceInjected, 0, in.Type, in.ContextID)
	d.result.PayloadCopyBytes += uint64(len(in.Data))
	d.result.InputBytes += uint64(len(in.Data))
	if len(in.Data) != 0 {
		data := make([]byte, len(in.Data))
		copy(data, in.Data)
		g.parts = []spliceMember{{parts: []spliceChunk{ownedSpliceChunk(data)}, n: len(data)}}
	} else {
		g.parts = []spliceMember{{}}
	}
	g.receipt = make(chan SpliceDisposition, 1)
	receipt := SpliceReceipt{Status: SpliceAccepted, Sequence: g.meta.Sequence, Generation: g.meta.Generation, Done: g.receipt}
	d.control.finishCommand(cmd, spliceCommandReply{receipt: receipt})
	if d.decide(ctx, g) && !d.stopped {
		if d.enqueue(g) {
			return
		}
		g.disposition.Reason = SpliceDropQueueFull
	}
	var end *SpliceEnd
	if d.finished {
		end = &d.result.End
	}
	d.discard(g, end)
}

func (d *spliceRelay) enqueue(g *spliceGroup) bool {
	if d.queuedItems == d.limits.QueueItems {
		return false
	}
	// Retain only after every distinct new root fits. All queued payloads
	// have an identified allocation, including encoded capsule headers.
	room := d.limits.QueueBytes - d.queuedBytes
	added := make(map[*core.Buf]struct{})
	for _, m := range g.parts {
		for _, ch := range m.parts {
			if ch.buf == nil || d.queuedRoots[ch.buf] > 0 {
				continue
			}
			if _, ok := added[ch.buf]; ok {
				continue
			}
			added[ch.buf] = struct{}{}
			if cap(ch.buf.B) > room {
				return false
			}
			room -= cap(ch.buf.B)
		}
	}
	if d.queuedRoots == nil {
		d.queuedRoots = make(map[*core.Buf]int)
	}
	for _, m := range g.parts {
		for _, ch := range m.parts {
			d.chargeRoot(ch.buf)
		}
	}
	if g.meta.Kind == SpliceDatagram && !g.downgraded {
		g.expiry = min(g.expiry, d.limits.deadline(g.accepted))
	}
	g.queued = true
	d.queuedItems++
	if g.meta.Kind == SpliceDatagram && !g.downgraded {
		d.pendingDG = append(d.pendingDG, g)
	} else {
		d.pendingReliable = append(d.pendingReliable, g)
	}
	return true
}

func (d *spliceRelay) admit(kind SpliceUnitKind, origin SpliceOrigin, off, typ, cx uint64) *spliceGroup {
	g := new(spliceGroup)
	d.initGroup(g, kind, origin, off, typ, cx)
	return g
}

func (d *spliceRelay) initGroup(g *spliceGroup, kind SpliceUnitKind, origin SpliceOrigin, off, typ, cx uint64) {
	d.sequence++
	now := core.Time(0)
	targetAt := core.Time(0)
	hooks, generation := d.hooks, d.generation
	if d.control != nil && origin == SpliceReceived {
		if kind == SpliceDatagram {
			p := d.control.policy.Load()
			hooks, generation = p.hooks, p.generation
			d.sourceBatches++
		} else {
			hooks, generation = d.batch.hooks, d.batch.generation
		}
	}
	chain := hooks.chain(kind)
	if len(chain) != 0 || kind == SpliceCapsule || kind == SpliceDatagram || d.mirror != nil {
		now = d.clock.now()
	}
	if kind == SpliceDatagram {
		targetAt = d.target.c.clk.now()
	}
	*g = spliceGroup{
		meta: SpliceMeta{Kind: kind, Origin: origin, Reverse: d.reverse, Sequence: d.sequence, Offset: off,
			Generation: generation, Type: typ, ContextID: cx, SourceTag: d.sourceTag, TargetTag: d.targetTag},
		measured: len(chain) != 0 || d.mirror != nil || origin == SpliceInjected,
		chain:    chain, accepted: now, targetAt: targetAt, ready: now, expiry: core.Never,
		disposition: SpliceDisposition{Sequence: d.sequence, Generation: generation},
	}
	if kind == SpliceDatagram {
		h := d.target
		h.c.peek(func() {
			o := h.c.s.DatagramOptions(h.st, h.c.clk.datagramOptions(d.options))
			g.hopDeadline, _ = o.ResolveDeadline(targetAt)
		})
		if g.hopDeadline != core.Never {
			g.expiry = now.Add(g.hopDeadline.Sub(targetAt))
		}
	}
	d.result.InputUnits++
}

func (d *spliceRelay) observeMeta(g *spliceGroup) {
	m := &g.meta
	d.input.c.peek(func() {
		st := d.input.st
		id, ok := st.StreamID()
		m.SourceID, m.HasSourceID, m.Replayable = StreamID(id), ok, st.Replayable()
		if p, ok := st.PeerPreamble(); ok {
			m.OpenInfo = p.OpenInfo
			m.TLVs = make([]TLV, len(p.App))
			for i, t := range p.App {
				m.TLVs[i] = TLV{Type: t.Type, Value: t.Value}
			}
		}
	})
	d.target.c.peek(func() {
		id, ok := d.target.st.StreamID()
		m.TargetID, m.HasTargetID = StreamID(id), ok
	})
}

func (d *spliceRelay) decide(ctx context.Context, g *spliceGroup) bool {
	if len(g.chain) == 0 && d.mirror == nil {
		g.backing = splicePayloadCost(g.parts)
		return true
	}
	d.observeMeta(g)
	for i, m := range g.parts {
		d.publish(g.meta, SpliceMirrorInput, i, m)
	}
	for _, h := range g.chain {
		backing, segments := spliceGroupCost(g.parts)
		d.result.GroupPeakBytes = max(d.result.GroupPeakBytes, backing)
		l := d.limits
		if backing > l.BackingBytes || segments > l.Segments {
			d.result.Cause = ErrSpliceBudget
			d.end(ctx, SpliceEnd{Kind: SpliceHookFailed})
			return false
		}
		sc := &spliceScope{open: true, maxItem: d.sizes.limit(g.meta.Kind, g.meta.Type),
			left: l.BackingBytes - backing, handles: l.OutputItems, segments: l.Segments - segments}
		for _, m := range g.parts {
			sc.views = append(sc.views, splicePayloadView{parts: m.parts, n: m.n})
		}
		d.result.HookCalls++
		decision, err := callSpliceHook(ctx, h, SpliceUnit{meta: g.meta, scope: sc, count: len(g.parts)})
		if err == nil && decision.Action == SpliceForward && decision.Replacement != nil {
			var parts []spliceMember
			parts, err = sc.capture(decision.Replacement, l)
			if err == nil {
				releaseSpliceMembers(g.parts)
				g.parts, g.raw = parts, nil
				d.result.HookReplaced++
			}
		} else {
			sc.close()
		}
		if sc.failed {
			err = ErrSpliceBudget
		}
		d.result.GroupPeakBytes = max(d.result.GroupPeakBytes, l.BackingBytes-sc.left)
		d.result.PayloadCopyBytes += sc.copied
		if d.observe(ctx) || d.stopped || d.finished && g.meta.Kind != SpliceDatagram || d.flush {
			return false
		}
		if err != nil || !d.mergeWait(g, decision) {
			if err == nil {
				err = ErrInvalidConfig
			}
			d.result.Cause = err
			d.end(ctx, SpliceEnd{Kind: SpliceHookFailed})
			return false
		}
		switch decision.Action {
		case SpliceDrop:
			g.disposition.Reason = SpliceDropHook
			d.result.HookDropped++
			return false
		case SpliceResetByHook:
			d.end(ctx, SpliceEnd{Kind: SpliceHookReset, Code: decision.Code})
			return false
		case SpliceForward:
			if len(g.parts) == 0 {
				g.disposition.Reason = SpliceDropHook
				d.result.HookDropped++
				return false
			}
		}
	}
	g.chain = nil
	if g.ready != g.accepted || len(g.resumes) != 0 {
		g.waiting, g.waitStarted = true, d.clock.now()
	}
	g.backing, d.result.OutputDescriptors = spliceGroupCost(g.parts)
	return true
}

func splicePayloadCost(parts []spliceMember) int {
	if len(parts) == 1 && len(parts[0].parts) == 1 {
		ch := parts[0].parts[0]
		if ch.buf != nil {
			return cap(ch.buf.B)
		}
		return cap(ch.Data)
	}
	cost, _ := spliceGroupCost(parts)
	return cost
}

func (d *spliceRelay) mergeWait(g *spliceGroup, decision SpliceDecision) bool {
	w := decision.Wait
	hasWait := w.After != 0 || w.Resume != nil || w.Expiry != 0
	if uint64(decision.Code) > varint.MaxVarint || decision.Action > SpliceResetByHook || decision.Action != SpliceResetByHook && decision.Code != 0 ||
		decision.Action != SpliceForward && (hasWait || decision.Replacement != nil) ||
		w.After < 0 || w.Expiry < 0 {
		return false
	}
	if hasWait {
		g.ready = max(g.ready, g.accepted.Add(core.Duration(w.After)))
		expiry := d.limits.deadline(g.accepted)
		if w.Expiry > 0 {
			expiry = min(expiry, g.accepted.Add(core.Duration(w.Expiry)))
		}
		g.expiry = min(g.expiry, expiry)
		if w.Resume != nil {
			g.resumes = append(g.resumes, w.Resume)
		}
	}
	return true
}

func spliceSignalReady(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (g *spliceGroup) runnable(now core.Time) bool {
	g.awaitTime = now < g.ready
	if g.awaitTime {
		return false
	}
	g.resumes = slices.DeleteFunc(g.resumes, spliceSignalReady)
	return len(g.resumes) == 0
}

func (d *spliceRelay) wait(ctx context.Context) {
	cases := make([]reflect.SelectCase, 0, 12)
	add := func(ch <-chan struct{}) {
		if ch != nil {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
		}
	}
	add(ctx.Done())
	add(d.bothFinished)
	if !d.finished {
		add(d.waitRead)
		add(d.waitWrite)
		add(d.input.rwait())
		add(d.target.wwait())
	}
	if !d.dgEnded {
		add(d.waitDG)
	}
	if d.commandOpen {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(d.control.cmd)})
	}
	next := core.Never
	// Subscribe to the conditions the owner actually found pending. A
	// timer or resume may become ready between that probe and select.
	pushGroup := func(g *spliceGroup) {
		if g == nil {
			return
		}
		next = min(next, g.expiry)
		if d.pause != nil {
			return
		}
		if g.awaitTime {
			next = min(next, g.ready)
			return
		}
		for _, ch := range g.resumes {
			add(ch)
		}
		if g.permit != nil {
			add(g.permit.done)
		}
	}
	if !d.flush {
		pushGroup(d.current)
		for _, g := range d.pendingDG {
			pushGroup(g)
		}
		for _, g := range d.pendingReliable {
			next = min(next, g.expiry)
		}
	}
	if d.reliableSlots != nil {
		add(d.reliableSlots.wake)
	}
	if d.writer.permit != nil {
		add(d.writer.permit.done)
	}
	if next != core.Never {
		ch := make(chan struct{})
		d.result.WaitTimers++
		timer := d.clock.afterFunc(d.clock.until(next), func() { close(ch) })
		defer timer.Stop()
		// A clock advance can reach next before the relative timer is armed.
		if d.clock.now() >= next {
			return
		}
		add(ch)
	}
	_, val, _ := reflect.Select(cases)
	if val.IsValid() && val.Type() == reflect.TypeFor[*spliceCommand]() {
		d.handleCommand(ctx, val.Interface().(*spliceCommand))
	}
}

func (d *spliceRelay) finishWait(g *spliceGroup) {
	if g.waiting {
		g.waiting = false
		d.result.WaitTime += time.Duration(d.clock.now().Sub(g.waitStarted))
	}
}

func (d *spliceRelay) release(g *spliceGroup, end *SpliceEnd) {
	d.finishWait(g)
	g.permit.revoke()
	g.permit = nil
	if g.reserved != 0 {
		d.limit.Refund(g.reserved)
		g.reserved = 0
	}
	if g.queued {
		d.queuedItems--
		for _, m := range g.parts {
			for _, ch := range m.parts {
				d.unchargeRoot(ch.buf)
			}
		}
		g.queued = false
	}
	g.finish(end)
}

func (d *spliceRelay) discard(g *spliceGroup, end *SpliceEnd) {
	g.disposition.Status = SpliceDropped
	if g.meta.Kind == SpliceDatagram || g.downgraded {
		if end == nil && g.disposition.Reason != "" {
			d.result.countDrop(g.disposition.Reason)
		}
	}
	d.publish(g.meta, SpliceMirrorDrop, 0, spliceMember{})
	d.release(g, end)
}

func (d *spliceRelay) releaseCurrent(end *SpliceEnd) {
	g := d.current
	if g != nil {
		if end == nil {
			d.release(g, nil)
		} else {
			d.discard(g, end)
		}
	}
	d.capsule.reset()
	d.current, d.currentReady = nil, false
}

func (d *spliceRelay) end(ctx context.Context, e SpliceEnd) {
	if d.finished {
		d.stopped = true
		return
	}
	if e.Kind == SpliceFinished {
		var err error
		h := d.target
		h.c.locked(func() {
			if err = ctx.Err(); err != nil {
				return
			}
			err = apiErr(h.c.s.Finish(h.st))
			h.released(h.c.s.ReleaseSend(h.st))
		})
		if err != nil {
			d.result.Cause = err
			e = spliceEnd(ctx, err, true)
		} else {
			d.input.c.locked(func() { d.input.released(d.input.c.s.ReleaseRecv(d.input.st)) })
		}
	}
	d.result.End = e
	d.finished, d.fin, d.stopped = true, true, e.Kind != SpliceFinished
	switch e.Kind {
	case SpliceFinished:
	case SpliceReset:
		d.input.c.locked(func() { d.input.released(d.input.c.s.ReleaseRecv(d.input.st)) })
		d.target.cancelWrite(e.Code)
	case SplicePacketError, SpliceHookReset:
		d.input.cancelRead(e.Code)
		d.target.cancelWrite(e.Code)
	case SpliceHookFailed, SpliceLimiterFailed, SpliceWaitTimeout:
		if e.Kind == SpliceWaitTimeout {
			d.result.Cause = context.DeadlineExceeded
		}
		d.input.cancelRead(StreamCancelled)
		d.target.cancelWrite(StreamCancelled)
	case SpliceStopped:
		d.input.cancelRead(e.Code)
	case SpliceCancelled:
		d.result.Cause = context.Cause(ctx)
		stop := e.Code
		if _, ok := errors.AsType[*StreamError](d.result.Cause); !ok {
			stop = 0
		}
		d.input.cancelRead(stop)
		d.target.cancelWrite(e.Code)
	default:
		d.input.cancelRead(StreamCarrierFailed)
		d.target.cancelWrite(StreamCarrierFailed)
	}
	d.clearWriter()
	d.releaseCurrent(&e)
	for _, g := range d.pendingReliable {
		d.discard(g, &e)
	}
	d.pendingReliable = nil
	if d.stopped {
		for _, g := range d.pendingDG {
			d.discard(g, &e)
		}
		d.pendingDG = nil
	}
	d.target.c.locked(func() {
		d.target.c.s.CancelCapacityWait(d.target.st)
		d.target.c.s.EndWaitWrite(d.target.st)
	})
	d.event(SpliceEvent{Name: SpliceDirectionFinished, End: e})
	d.closeCommands()
	d.reliableDone()
}

func (d *spliceRelay) readEnd(ctx context.Context, err error) {
	switch {
	case err == nil:
	case errors.Is(err, io.EOF):
		d.fin = true
		if d.current == nil && len(d.pendingReliable) == 0 {
			d.end(ctx, SpliceEnd{Kind: SpliceFinished})
		}
	default:
		d.result.Cause = err
		e := spliceEnd(ctx, err, false)
		if e.Kind == SpliceReset {
			d.resetAfterPrefix(ctx, e)
		} else {
			d.end(ctx, e)
		}
	}
}

func (d *spliceRelay) resetAfterPrefix(ctx context.Context, e SpliceEnd) {
	d.target.c.locked(func() {
		s := d.target.c.s
		if s.SpliceResetAt() {
			s.SpliceResetBoundary(d.target.st, d.complete)
		} else if d.complete > 0 {
			var err error
			d.flushAt, err = s.SpliceFlushPrefix(d.target.st, d.complete)
			if err == nil {
				d.flush, d.flushEnd = true, e
			}
		}
	})
	if !d.flush {
		d.end(ctx, e)
		return
	}
	d.clearWriter()
	d.releaseCurrent(&e)
	d.fin = true
}

type spliceReadResult struct {
	chunk spliceChunk
	end   error
}

// read returns the bytes consumed and their terminal status. A final
// fragment may carry data together with EOF.
func (d *spliceRelay) read(ctx context.Context, n int, ready uint64, end bool) spliceReadResult {
	if ready == 0 && !end {
		return spliceReadResult{}
	}
	if ctx.Err() != nil {
		return spliceReadResult{end: ctx.Err()}
	}
	if ready > 0 && d.control != nil {
		d.startBatch()
		n = int(min(uint64(n), d.batchEnd-d.source.Offset))
	}
	var out spliceChunk
	var err error
	d.input.c.locked(func() {
		h, c := d.input, d.input.c
		if err = ctx.Err(); err != nil {
			return
		}
		p := &d.capsule
		budget := core.SpliceReadBudget{Remaining: d.limits.BackingBytes}
		collect := d.packet && p.collect && p.parsed
		if collect {
			budget.Remaining -= p.backing
			budget.Existing = p.pins
		}
		var v core.Span
		var lease *core.SpliceLease
		var fin bool
		var e error
		var dst []byte
		switch {
		case d.packet && d.current != nil && !p.parsed:
			dst = p.raw[p.n : p.n+n]
		case collect && (p.compact != nil || len(p.parts) >= d.limits.Segments-1):
			e = core.ErrReceiveLoanLimit
		default:
			v, lease, fin, e = c.s.SpliceReadChunk(h.st, n, budget)
			out = spliceChunk{Data: v.Bytes(), buf: v.Buf, lease: lease}
		}
		if errors.Is(e, core.ErrReceiveLoanLimit) {
			size := int(min(uint64(n), ready))
			if collect {
				var moved int
				moved, e = p.compactPayload(d.limits.BackingBytes)
				if e != nil {
					err = e
					return
				}
				d.result.PayloadCopyBytes += uint64(moved)
				dst = p.compact[p.pos : p.pos+size]
			} else {
				dst = make([]byte, size)
			}
		}
		if dst != nil {
			var took int
			took, fin, e = c.s.Read(h.st, dst)
			out = spliceChunk{Data: dst[:took]}
			if !d.packet || d.capsule.parsed && !collect {
				out = ownedSpliceChunk(dst[:took])
			}
			if !d.packet || p.parsed {
				d.result.PayloadCopyBytes += uint64(took)
			}
		}
		d.result.InputBytes += uint64(len(out.Data))
		err = apiErr(e)
		if (fin || e != nil) && !d.packet {
			h.released(c.s.ReleaseRecv(h.st))
		}
		if fin {
			err = io.EOF
		}
	})
	return spliceReadResult{chunk: out, end: err}
}

func (d *spliceRelay) reliableStep(ctx context.Context) bool {
	if d.flush {
		var done bool
		var err error
		d.target.c.peek(func() { done, err = d.target.c.s.FlushDone(d.target.st, d.flushAt) })
		if err != nil {
			d.end(ctx, spliceEnd(ctx, apiErr(err), true))
		} else {
			done = done && d.prefix.finishedBefore(d.complete)
			if done {
				d.end(ctx, d.flushEnd)
			}
		}
		return done || err != nil
	}
	now := d.clock.now()
	for i, g := range d.pendingReliable {
		if g.expiry <= now {
			if g.downgraded {
				d.pendingReliable = slices.Delete(d.pendingReliable, i, i+1)
				d.dropGroup(g, SpliceDropExpired)
				return true
			}
			d.end(ctx, SpliceEnd{Kind: SpliceWaitTimeout})
			return true
		}
	}
	if d.current != nil {
		if d.current.expiry <= now {
			if d.current.downgraded {
				d.expireFallback()
				return true
			}
			d.end(ctx, SpliceEnd{Kind: SpliceWaitTimeout})
			return true
		}
		if !d.currentReady && (d.pause == nil || d.source.End) {
			return d.readCapsule(ctx, d.source.Ready, d.source.End)
		}
		if d.currentReady && (d.pause == nil || d.writer.started && d.current.reliableCapsule()) {
			if d.current.runnable(now) {
				d.finishWait(d.current)
				if !d.current.downgraded {
					d.current.expiry = core.Never
				}
				return d.writeGroup(ctx)
			}
		}
		return false
	}
	if len(d.pendingReliable) != 0 {
		if d.pause != nil {
			return false
		}
		d.current = d.pendingReliable[0]
		d.pendingReliable = slices.Delete(d.pendingReliable, 0, 1)
		d.currentReady = true
		return true
	}
	if d.source.End {
		r := d.read(ctx, 1, 0, true)
		d.readEnd(ctx, r.end)
		return r.end != nil
	}
	if d.pause != nil || d.source.Ready == 0 {
		return false
	}
	d.preferSource = false
	d.startBatch()
	if d.packet {
		d.initGroup(&d.directGroup, SpliceCapsule, SpliceReceived, d.source.Offset, 0, 0)
		d.current = &d.directGroup
		d.capsule.collect = !d.streamCapsules || len(d.current.chain) != 0 || d.mirror != nil
		if d.capsule.collect {
			d.current.expiry = d.limits.deadline(d.current.accepted)
		}
		return d.readCapsule(ctx, d.source.Ready, d.source.End)
	}
	return d.streamStep(ctx)
}

func (d *spliceRelay) startBatch() {
	if d.control != nil && d.source.Offset >= d.batchEnd && d.source.Ready > 0 {
		d.batch = *d.control.policy.Load()
		d.batchEnd = d.source.Offset + d.source.Ready
		d.sourceBatches++
	}
}

func (d *spliceRelay) streamStep(ctx context.Context) bool {
	n := min(uint64(d.sizes.chunk), d.source.Ready)
	hooks := d.hooks
	if d.control != nil {
		hooks = d.batch.hooks
	}
	if len(hooks.Stream) == 0 && d.mirror == nil {
		n = min(n, d.capacity(ctx))
		if n == 0 {
			return false
		}
	}
	r := d.read(ctx, int(n), d.source.Ready, d.source.End)
	ch := r.chunk
	if len(ch.Data) == 0 {
		ch.Release()
		if r.end != nil {
			d.readEnd(ctx, r.end)
			return true
		}
		return false
	}
	var g *spliceGroup
	direct := len(hooks.Stream) == 0 && d.mirror == nil
	if direct {
		g = &d.directGroup
		d.initGroup(g, SpliceStream, SpliceReceived, d.source.Offset, 0, 0)
	} else {
		g = d.admit(SpliceStream, SpliceReceived, d.source.Offset, 0, 0)
	}
	g.sourceEnd = d.source.Offset + uint64(len(ch.Data))
	if ch.buf != nil && cap(ch.buf.B) > d.limits.BackingBytes {
		p := make([]byte, len(ch.Data))
		d.result.PayloadCopyBytes += uint64(copy(p, ch.Data))
		ch.Release()
		ch = ownedSpliceChunk(p)
	}
	if direct {
		d.directPart[0] = ch
		d.directMember[0] = spliceMember{parts: d.directPart[:], n: len(ch.Data)}
		g.parts = d.directMember[:]
		d.current, d.currentReady = g, true
		d.writeGroup(ctx)
		return true
	}
	g.parts = []spliceMember{{parts: []spliceChunk{ch}, n: len(ch.Data)}}
	d.current = g
	d.currentReady = d.decide(ctx, g)
	if !d.currentReady && d.current == g {
		d.discard(g, nil)
		d.current = nil
	}
	return true
}

func (d *spliceRelay) capacity(ctx context.Context) uint64 {
	var n uint64
	if ctx.Err() != nil {
		return 0
	}
	d.target.c.locked(func() {
		h, c := d.target, d.target.c
		c.s.SetNow(c.clk.now())
		if n = c.s.SendCapacity(h.st); n == 0 {
			c.s.WaitSendCapacity(h.st, 1)
		}
		if room, err := c.s.WritableRoom(h.st); err == nil {
			if room == 0 {
				c.s.WaitWrite(h.st)
			}
			n = min(n, room)
		} else {
			n = 0
		}
	})
	return n
}

func (d *spliceRelay) write(ctx context.Context, ch spliceChunk, n int) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if spliceSignalReady(d.target.wwait()) {
		return 0, os.ErrDeadlineExceeded
	}
	var took int
	var err error
	d.target.c.locked(func() {
		h, c := d.target, d.target.c
		now := c.clk.now()
		if err = ctx.Err(); err != nil {
			return
		}
		c.s.SetNow(now)
		if g := d.current; g.downgraded && g.disposition.Bytes == 0 && (now >= g.hopDeadline || d.clock.now() >= g.expiry) {
			err = errSpliceExpired
			return
		}
		var report func(sent, unsent int)
		if d.limit != nil || d.current.measured {
			report = d.handoffReport
		}
		var part *splicePrefixPart
		if !c.s.SpliceResetAt() {
			if d.prefix == nil {
				d.prefix = &splicePrefixWatch{wake: h.wsig.Fire}
			}
			part = &splicePrefixPart{offset: d.output}
			prefix, limit := d.prefix, report
			report = func(sent, unsent int) {
				prefix.complete(part, sent+unsent)
				if limit != nil {
					limit(sent, unsent)
				}
			}
		}
		if report == nil {
			took, err = c.s.WriteChunk(h.st, ch.Data[:n], ch.buf, false)
		} else {
			took, err = c.s.WriteSpliceChunk(h.st, ch.Data[:n], ch.buf, report)
			if d.limit != nil || d.current.measured {
				d.result.Handoffs.accept(took)
			}
			if part != nil {
				d.prefix.accept(part, took)
			}
		}
	})
	return took, apiErr(err)
}

func (d *spliceRelay) chargeRoot(b *core.Buf) {
	if b == nil {
		return
	}
	if d.queuedRoots[b] == 0 {
		d.queuedBytes += cap(b.B)
	}
	d.queuedRoots[b]++
}

func (d *spliceRelay) unchargeRoot(b *core.Buf) {
	if b == nil {
		return
	}
	d.queuedRoots[b]--
	if d.queuedRoots[b] == 0 {
		delete(d.queuedRoots, b)
		d.queuedBytes -= cap(b.B)
	}
}
