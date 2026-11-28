package core

import (
	"time"

	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

type coverResult struct {
	shape      *pipeShape
	id         uint64
	n, cost    int
	reason     shaping.RuntimeReason
	standalone bool
}

// NeedsProgress tells the driver to preserve message-prefix progress when shaping,
// member rotation or a splice uses the completed prefix.
func (b *Batch) NeedsProgress() bool {
	return b.Handoffs() || b.trackProgress || b.shape != nil || b.cover.n != 0
}

// commitShape publishes the outcome once the batch can no longer be rolled back.
func (s *Session) commitShape(w *recWriter) {
	w.b.dispatch = s.reserveShape(w.added)
	if s.shp == nil {
		return
	}
	sh := s.shp
	w.b.shapeProgress = sh.eff.Profile != nil || w.b.shape != nil || w.b.dispatch.extra != 0
	c := w.b.cover
	if c.shape != nil {
		e := shaping.Event{Pipe: c.shape.p.id, Now: time.Duration(s.now), RequestID: c.id}
		if c.reason == 0 {
			sh.acct.charge(usOf(s.now), uint64(c.cost))
			if c.standalone {
				s.recordNoEffect()
			}
			sh.stats.InjectionAccepted = satAdd(sh.stats.InjectionAccepted, 1)
			sh.stats.InjectionAcceptedBytes = satAdd(sh.stats.InjectionAcceptedBytes, uint64(c.n))
			e.Kind, e.Len, e.WireCost = shaping.EventInjectionAccepted, c.n, c.cost
		} else {
			sh.stats.InjectionRejected = satAdd(sh.stats.InjectionRejected, 1)
			sh.stats.InjectionRejections[c.reason] = satAdd(sh.stats.InjectionRejections[c.reason], 1)
			sh.reason(c.reason)
			e.Kind, e.Reason = shaping.EventInjectionRejected, c.reason
		}
		s.emitShaping(e)
		if c.shape.sh != nil {
			c.shape.eventOn(e)
		}
	}
	if w.stopShape != nil {
		w.stopShape.stop(true)
	}
}

func (s *Session) completeShape(b *Batch, n int, current *outLane) {
	if b.owner == nil {
		return
	}
	// WriteDone reports body bytes. Sealed terminal results identify whole
	// messages. Any partial ciphertext prefix was reported by the writer.
	if b.EnvelopeSize() != 0 {
		body, encoded := 0, 0
		for _, r := range b.recs {
			if body+r.size > n {
				break
			}
			body += r.size
			encoded += r.size + b.EnvelopeSize()
		}
		n = encoded
	}
	reason := shaping.ReasonNone
	if b.cover.n != 0 && max(n, b.shapePrefix) < b.EncodedLen() {
		reason = shaping.ReasonPartialProgress
		if n == 0 && b.shapePrefix == 0 {
			reason = shaping.ReasonCarrierRejected
		}
		s.shapeReason(reason)
	}
	s.shapeProgress(b, n, current, reason)
	s.settleShape(b.dispatch, 0, 0)
}

// EncodedLen includes the immutable sealing headers and tags.
func (b *Batch) EncodedLen() int { return b.n + len(b.recs)*b.EnvelopeSize() }

// EnvelopeSize gives the zmux sealing prefix and tag present per record.
func (b *Batch) EnvelopeSize() int {
	if b.owner == nil {
		return 0
	}
	return b.owner.info.SealPrefix + b.owner.info.SealSuffix
}

// completedRecords reports each complete carrier unit in dispatch order.
func (s *Session) completedRecords(b *Batch, n int, current *outLane) {
	for b.shapeRecords < len(b.recs) {
		r := b.recs[b.shapeRecords]
		size := r.size + b.EnvelopeSize()
		if b.owner.info.Framing == FramingBytes {
			size += varint.SizeVarint(uint64(r.size))
		}
		if size > n-b.shapeRecordEnd {
			return
		}
		b.shapeRecordEnd += size
		b.shapeRecords++
		s.completeOwn(b, current, uint64(r.own))
		if !b.provisional {
			s.outbound(b.shape, b.Lane, size, shaping.Record, r.data, r.control)
		}
	}
}

func (s *Session) emitShaping(e shaping.Event) {
	s.emit(Event{Kind: EvShaping, Pipe: e.Pipe, Shape: &e})
}

func (s *Session) completeOwn(b *Batch, current *outLane, n uint64) {
	if !b.provisional || s.earlyAccepted {
		s.shapingEarn(n)
	} else if b.owner == current {
		s.earlyEarn = satAdd(s.earlyEarn, n)
	}
}

func (s *Session) outbound(ps *pipeShape, lane uint64, size int, unit shaping.UnitKind, data, control bool) {
	if ps == nil || ps.sh == nil || ps.caps&shaping.CapOutbound == 0 ||
		!data && (!control || ps.caps&shaping.CapIncludeControl == 0) ||
		s.pipe(ps.p.id) != ps.p || ps.p.sh != ps || ps.gen != s.shp.gen || !s.coverEligible(ps.p) {
		return
	}
	ps.eventOn(shaping.Event{Kind: shaping.EventOutbound, Pipe: ps.p.id, Lane: lane, Now: time.Duration(s.now),
		Len: size, Unit: unit, Data: data, Control: control})
}

// markShapeSpans translates frame-relative optional fields after the final
// Size is known. The driver retains these offsets along with the batch.
func (w *recWriter) markShapeSpans() {
	start := w.b.n - w.recBytes + len(w.b.recs)*w.b.EnvelopeSize()
	if w.b.owner != nil {
		start += w.b.owner.info.SealPrefix
	}
	add := func(at, n int) {
		if n > 0 {
			w.b.extraSpans = append(w.b.extraSpans, [2]int{at, at + n})
		}
	}
	standalone := w.coverLen != 0 && w.b.cover.standalone
	if standalone && w.b.owner != nil {
		info := w.b.owner.info
		add(start-info.SealPrefix, info.SealPrefix)
		add(start+w.recBytes, info.SealSuffix)
	}
	if w.framing == FramingBytes && (w.baseFrames > 0 || standalone) {
		head := varint.SizeVarint(uint64(w.recBytes))
		ordinary := 0
		if !standalone {
			ordinary = varint.SizeVarint(uint64(w.baseFrames))
		}
		add(start-head+ordinary, head-ordinary)
	}
	add(start+w.lengthStart, w.lengthLen)
	add(start+w.padStart, w.padLen)
	if w.coverLen != 0 {
		w.b.coverOffset = start + w.coverStart
	}
	add(start+w.coverStart, w.coverLen)
	w.baseFrames, w.padLen, w.lengthLen = 0, 0, 0
}

func (b *Batch) acceptedExtra(n int) uint64 {
	var extra uint64
	for _, span := range b.extraSpans {
		extra += uint64(max(0, min(n, span[1])-span[0]))
	}
	return extra
}

// TracksShapeProgress reports whether a live prefix can affect shaping.
func (b *Batch) TracksShapeProgress() bool { return b.shapeProgress }

// ShapeWriteProgress reports an absolute accepted prefix while the immutable
// batch remains in flight. Duplicate observations leave both meters unchanged.
func (s *Session) ShapeWriteProgress(b *Batch, n int, now Time) {
	s.SetNow(now)
	var current *outLane
	if p := s.pipe(b.Pipe); p != nil && p.out != nil {
		current = p.out.lane(b.Lane)
	}
	s.shapeProgress(b, n, current, shaping.ReasonNone)
}

func (s *Session) shapeProgress(b *Batch, n int, current *outLane, reason shaping.RuntimeReason) {
	n = min(n, b.EncodedLen())
	if b.owner == nil {
		return
	}
	n = max(n, b.shapePrefix)
	extra := b.acceptedExtra(n) - b.acceptedExtra(b.shapePrefix)
	if n == b.EncodedLen() {
		extra = b.dispatch.extra
	}
	baseline := uint64(n-b.shapePrefix) - min(extra, uint64(n-b.shapePrefix))
	if b.provisional && !s.earlyAccepted {
		if b.owner == current && b.dispatch.second >= s.pruneShape() {
			bin := s.shapeBin(b.dispatch.second)
			bin.provisional = satAdd(bin.provisional, baseline)
		}
		baseline = 0
	}
	s.settleShape(shapeDispatch{second: b.dispatch.second, extra: extra}, baseline, extra)
	b.dispatch.extra -= extra
	sent := min(n, b.coverOffset+b.cover.n) - max(b.shapePrefix, b.coverOffset)
	b.shapePrefix = n
	if sent > 0 || reason != shaping.ReasonNone {
		sent = max(sent, 0)
		sh := s.shp
		sh.stats.InjectionSentBytes = satAdd(sh.stats.InjectionSentBytes, uint64(sent))
		s.emitShaping(shaping.Event{Kind: shaping.EventInjectionCarrierCompleted, Pipe: b.Pipe, Lane: b.Lane,
			Now: time.Duration(s.now), RequestID: b.cover.id, Len: sent, Reason: reason})
	}
	s.completedRecords(b, n, current)
}

// ShapeWriteUnknown retires a submission whose final carrier progress cannot
// be identified. Unresolved bytes expire with their dispatch bin.
func (s *Session) ShapeWriteUnknown(b *Batch, attemptedEnd int) {
	if b.owner == nil {
		return
	}
	end := max(b.shapePrefix, min(attemptedEnd, b.EncodedLen()))
	unresolved := min(b.dispatch.extra, b.acceptedExtra(end)-b.acceptedExtra(b.shapePrefix))
	if end == b.EncodedLen() {
		unresolved = b.dispatch.extra
	}
	s.retainUnknown(b.dispatch.second, unresolved)
	s.settleShape(b.dispatch, 0, 0)
	b.dispatch.extra = 0
}
