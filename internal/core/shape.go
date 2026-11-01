package core

import (
	"math"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// Record shaping: a Shaper per outbound pipe proposes record sizes and
// timing; this file executes the proposals within the protocol's limits
// and the application's guardrails. Nothing here exists until a Shaper is
// set: the paths that consult it test s.shp (and p.sh) for nil.

// zeroPad backs PADDING frames (0x00 bytes); it is never written.
var zeroPad [wire.MaxRecordSize]byte

// shapeSess is the session's shaping state.
type shapeSess struct {
	fac   Factory
	gen   uint64 // bumped by SetShaper; a pipe's instance of an older one is replaced
	lim   shaping.Limits
	stats shaping.Stats
	// marks: send times of the latest records that SPEC.md 9.4.3 rule 1
	// counts toward a plain QMux peer (PADDING-only records, QX_PING
	// requests, PIPE_PING frames).
	marks [16]Time
	mi    int
	// injWait: an injection may be deferred until the padding ratio
	// allowance covers it (shapeWakeCovered).
	injWait bool
}

// pipeShape is the shaping state of one outbound pipe.
type pipeShape struct {
	s    *Session
	p    *Pipe
	sh   shaping.Shaper // nil: not shaped
	gen  uint64
	seq  uint64
	plan shaping.RecordPlan // applies to the next record while planned
	v    shaping.RecordView
	ev   shaping.Event
	r    shaping.Reply
	hold Time // stream data waits until then (Gate)
	// The planned record's earliest send time (RecordPlan.Delay): until
	// then the pipe sends nothing but CONNECTION_CLOSE.
	delayUntil Time
	gateGen    uint64 // bumped by each Gate
	rate       int64
	burst      int64
	tokens     float64
	filled     Time
	injAt      Time
	injLen     int
	timers     [2]Time
	weight     float64
	caps       shaping.Caps
	planned    bool // plan applies to the next record
	delayWake  bool // the delayed record is due at delayUntil
	wake       bool // a writer waits for hold or tokens
	injDue     bool
	injFirst   bool // the injection waited for batch room: it leads the next batch
}

func (s *Session) shapeSess() *shapeSess {
	if s.shp == nil {
		s.shp = &shapeSess{lim: s.cfg.ShapeLimits}
		for i := range s.shp.marks {
			s.shp.marks[i] = math.MinInt64
		}
		if s.ping.outstanding != 0 {
			s.shp.mark(s.ping.sentAt)
		}
	}
	return s.shp
}

// Factory returns the Shaper of an outbound pipe of carrier kind carrier
// (SetPipeKind) whose carrier adds overhead bytes to each write
// (CarrierInfo.WriteOverhead); nil: the pipe is not shaped.
type Factory func(pipe uint64, carrier uint8, overhead int) shaping.Shaper

// SetShaper installs f on every outbound pipe; nil: no shaping.
func (s *Session) SetShaper(f Factory) {
	sh := s.shapeSess()
	sh.fac = f
	sh.gen++
	for _, p := range s.pipes {
		if p.out != nil {
			s.shapeOf(p)
		}
	}
	s.wakeAllWriters()
}

// SetPipeKind records the kind of a pipe's carrier, which the shaper
// factory gets; the driver sets it before the pipe's first record. A pipe
// whose kind changes (a reservation's placeholder attaching its carrier)
// gets a new instance of the factory's at its next record.
func (s *Session) SetPipeKind(pipe uint64, carrier uint8) bool {
	p := s.pipe(pipe)
	if p == nil {
		return false
	}
	if p.carrier != carrier && p.sh != nil && s.shp != nil && p.sh.gen == s.shp.gen {
		p.sh.gen-- // stale: reinstalled from the factory
	}
	p.carrier = carrier
	return true
}

// PipeKind returns the kind SetPipeKind recorded for a pipe.
func (s *Session) PipeKind(pipe uint64) uint8 {
	if p := s.pipe(pipe); p != nil {
		return p.carrier
	}
	return 0
}

// SetPipeShaper installs x on an outbound pipe until the next SetShaper.
func (s *Session) SetPipeShaper(pipe uint64, x shaping.Shaper) bool {
	p := s.pipe(pipe)
	if p == nil || p.out == nil {
		return false
	}
	s.shapeSess()
	s.shapeInstall(p, x)
	s.wakePipe(p)
	return true
}

// ShapingStats returns the shaping counters.
func (s *Session) ShapingStats() shaping.Stats {
	if s.shp == nil {
		return shaping.Stats{}
	}
	return s.shp.stats
}

// shapeOf returns p's active shaping state, installing the factory's
// instance when p has none of the current generation.
func (s *Session) shapeOf(p *Pipe) *pipeShape {
	ps := p.sh
	if ps == nil || ps.gen != s.shp.gen {
		var x shaping.Shaper
		if f := s.shp.fac; f != nil {
			func() {
				defer func() {
					if recover() != nil {
						x = nil
						s.shp.stats.Faults++
					}
				}()
				x = f(p.id, p.carrier, writeOverhead(p))
			}()
		}
		ps = s.shapeInstall(p, x)
	}
	if ps.sh == nil {
		return nil
	}
	return ps
}

// writeOverhead is the WriteOverhead of p's outbound carrier (lane 0).
func writeOverhead(p *Pipe) int {
	if p == nil || p.out == nil || len(p.out.lanes) == 0 {
		return 0
	}
	return p.out.lanes[0].info.WriteOverhead
}

func (s *Session) shapeInstall(p *Pipe, x shaping.Shaper) *pipeShape {
	if old := p.sh; old != nil && old.sh != nil {
		old.event(shaping.EventReplaced, 0, 0, false, 0)
	}
	ps := &pipeShape{s: s, p: p, sh: x, gen: s.shp.gen, injAt: Never, timers: [2]Time{Never, Never}}
	p.sh = ps
	if x == nil {
		return ps
	}
	if !ps.call(func() { ps.caps = x.Capabilities() }) {
		return ps
	}
	ps.event(shaping.EventStart, 0, 0, false, 0)
	return ps
}

// call runs the instance; a panic ends shaping of the pipe.
func (ps *pipeShape) call(f func()) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
			ps.s.shp.stats.Faults++
			ps.stop(false)
		}
	}()
	f()
	return true
}

func (ps *pipeShape) stop(notify bool) {
	if ps.sh == nil {
		return
	}
	if notify {
		ps.event(shaping.EventStopped, 0, 0, false, 0)
	}
	*ps = pipeShape{s: ps.s, p: ps.p, gen: ps.gen, injAt: Never, timers: [2]Time{Never, Never}}
}

// event delivers a notification and applies the reply.
func (ps *pipeShape) event(k shaping.EventKind, lane uint64, n int, padOnly bool, timer int) {
	ps.eventOn(shaping.Event{Kind: k, Pipe: ps.p.id, Lane: lane, Now: time.Duration(ps.s.now), Len: n,
		PaddingOnly: padOnly, Timer: timer})
}

func (ps *pipeShape) eventOn(e shaping.Event) {
	ps.ev = e
	ps.r = shaping.Reply{}
	x, k := ps.sh, e.Kind
	if !ps.call(func() { x.OnEvent(&ps.ev, &ps.r) }) || k == shaping.EventReplaced || k == shaping.EventStopped ||
		k == shaping.EventPipeFailed {
		return
	}
	ps.apply(&ps.r)
}

// apply executes a reply.
func (ps *pipeShape) apply(r *shaping.Reply) {
	s := ps.s
	if in := r.Inject; in != nil {
		ps.injLen, ps.injAt, ps.injDue, ps.injFirst = 0, Never, false, false
		if !in.Cancel {
			ps.injLen, ps.injAt = max(in.Len, 1), s.now.Add(max(Duration(in.After), 0))
		}
	}
	if g := r.Gate; g != nil {
		ps.hold = s.shapePause(Duration(g.Pause))
		ps.rate, ps.burst, ps.filled = max(g.Rate, 0), g.Burst, s.now
		if ps.burst <= 0 {
			ps.burst = int64(min64(s.peerMaxRecord, wire.MaxRecordSize))
		}
		ps.tokens = float64(ps.burst)
		ps.wake = true
		ps.gateGen++
	}
	for i, t := range r.Timer {
		if t != nil {
			ps.timers[i] = Never
			if !t.Cancel {
				ps.timers[i] = s.now.Add(max(Duration(t.After), 0))
			}
		}
	}
	if w := r.Weight; w != nil && *w > 0 && !math.IsInf(*w, 1) {
		ps.weight = *w
	}
	if r.Stop {
		ps.stop(true)
	}
	s.wakePipe(ps.p)
}

// shapePause bounds a pause by the guardrails and returns its end.
func (s *Session) shapePause(d Duration) Time {
	sh := s.shp
	if d <= 0 {
		return s.now
	}
	if sh.lim.NoPause {
		sh.stats.Clamped++
		return s.now
	}
	if m := Duration(sh.lim.MaxDelay); m > 0 && d > m {
		d = m
		sh.stats.Clamped++
	}
	if m := sh.lim.MaxPauseTotal; m > 0 && sh.stats.PauseTotal+time.Duration(d) > m {
		d = Duration(m - min(sh.stats.PauseTotal, m))
		sh.stats.Clamped++
	}
	sh.stats.PauseTotal += time.Duration(d)
	return s.now.Add(d)
}

// held reports whether the gate holds stream data now.
func (ps *pipeShape) held(now Time) bool {
	if now < ps.hold {
		return true
	}
	if ps.rate == 0 {
		return false
	}
	ps.tokens = min(float64(ps.burst), ps.tokens+float64(ps.rate)*float64(now.Sub(ps.filled))/1e9)
	ps.filled = now
	return ps.tokens < ps.quantum()
}

// quantum is the bucket level that releases a held pipe: the bytes of
// one granularity G, at least one.
func (ps *pipeShape) quantum() float64 {
	return max(1, min(float64(ps.burst), float64(ps.rate)*float64(limGranularity)/1e9))
}

// gateRoom returns the stream data the shaping gate lets through for the
// rest of the batch (-1: no limit). A gate set in the middle of the batch
// takes effect for the data written from then on.
func (w *recWriter) gateRoom() int64 {
	ps := w.gate
	if ps == nil || ps.sh == nil {
		return -1
	}
	if !w.gateOn || w.gateGen != ps.gateGen {
		w.gateOn, w.gateGen, w.gateBase = true, ps.gateGen, w.dataSpent
		w.gateShut = ps.held(ps.s.now)
	}
	if w.gateShut || ps.s.now < ps.hold {
		ps.wake = true
		return 0
	}
	if ps.rate == 0 {
		return -1
	}
	r := int64(ps.tokens) - int64(w.dataSpent-w.gateBase)
	if r <= 0 {
		ps.wake = true
		return 0
	}
	return r
}

// chargeGate takes the batch's stream data written under the current gate
// from its bucket.
func (w *recWriter) chargeGate() {
	ps := w.gate
	if ps == nil || ps.sh == nil {
		return
	}
	w.gateRoom() // follows a gate set by the batch's last plan
	if ps.rate > 0 {
		ps.tokens -= float64(w.dataSpent - w.gateBase)
	}
	w.gateBase = w.dataSpent
}

func (s *Session) shapeHeld(p *Pipe) bool {
	if ps := p.sh; ps != nil && ps.sh != nil && ps.held(s.now) {
		ps.wake = true
		return true
	}
	return false
}

// delayed reports whether the planned record may not go yet.
func (ps *pipeShape) delayed(now Time) bool {
	return ps.sh != nil && ps.planned && now < ps.delayUntil
}

// shapeDelayed reports whether p's planned record waits for its delay.
func (s *Session) shapeDelayed(p *Pipe) bool { return p.sh != nil && p.sh.delayed(s.now) }

// shapeWants reports an injection due on lane 0 of p.
func (s *Session) shapeWants(p *Pipe, lane uint64) bool {
	return lane == 0 && p.sh != nil && p.sh.injDue && s.injCovered(p.sh.injLen)
}

// prep plans the record that the writer opens next.
func (ps *pipeShape) prep(w *recWriter) {
	if ps.sh == nil {
		w.sh = nil
		return
	}
	// Plan is called once per record, and the instance takes the call as
	// the record going out (a state machine moves on record_out): the plan
	// stays until its record closes, whatever events reach the instance
	// meanwhile.
	if !ps.planned {
		s := ps.s
		ps.plan = shaping.RecordPlan{}
		if ps.caps&shaping.CapPlan != 0 {
			ps.v = shaping.RecordView{Pipe: ps.p.id, Lane: w.b.Lane, Seq: ps.seq, Limit: w.recMax,
				WriteStart: w.recs == 0 || w.framing == FramingRecords, Now: time.Duration(s.now)}
			x := ps.sh
			if !ps.call(func() { x.Plan(&ps.v, &ps.plan) }) {
				w.sh = nil
				return
			}
		}
		ps.planned = true
		if m := ps.plan.MaxLen; m > 0 && m < shaping.MinMaxLen {
			ps.plan.MaxLen = shaping.MinMaxLen
			s.shp.stats.Clamped++
		}
		if ps.plan.Reply != (shaping.Reply{}) {
			ps.apply(&ps.plan.Reply)
			if ps.sh == nil {
				w.sh = nil
				return
			}
		}
		ps.delayUntil, ps.delayWake = 0, false
		if d := ps.plan.Delay; d > 0 {
			ps.delayUntil, ps.delayWake = s.shapePause(Duration(d)), true
		}
	}
	if ps.delayed(ps.s.now) {
		// The whole record, control frames included, waits.
		w.delayed = true
		return
	}
	w.cut = 0
	if m := ps.plan.MaxLen; m > 0 && m < w.recMax {
		w.cut = max(m, shaping.MinMaxLen)
	}
}

// padFor returns the PADDING the plan adds to a record of n frames bytes.
func (ps *pipeShape) padFor(n int) int {
	pl := &ps.plan
	t := max(n, pl.PadTo)
	if b := pl.PadMultiple; b > 0 && t%b != 0 {
		t += b - t%b
	}
	return t - n + max(pl.Pad, 0)
}

// padRoom returns the PADDING bytes the guardrails still allow: by
// MaxPaddingBytes (hard) and by MaxPaddingRatio or NoPaddingRatio, at least
// shaping.StartupAllowance in all (ratio); math.MaxUint64: unbounded.
func (s *Session) padRoom() (hard, ratio uint64) {
	sh := s.shp
	hard, ratio = math.MaxUint64, math.MaxUint64
	if sh.lim.NoPadding {
		return 0, 0
	}
	if m := sh.lim.MaxPaddingBytes; m > 0 {
		hard = m - min64(sh.stats.PaddingBytes, m)
	}
	if r := sh.lim.MaxPaddingRatio; r > 0 || sh.lim.NoPaddingRatio {
		if sh.lim.NoPaddingRatio {
			r = 0
		}
		other := s.stats.BytesSent - min64(s.stats.BytesSent, sh.stats.PaddingBytes)
		m := max(uint64(r*float64(other)), shaping.StartupAllowance)
		ratio = m - min64(sh.stats.PaddingBytes, m)
	}
	return hard, ratio
}

// injCovered reports whether an injection of n bytes may go now: the
// ratio allowance covers it, or a hard limit, which cuts it, binds first.
func (s *Session) injCovered(n int) bool {
	hard, ratio := s.padRoom()
	return ratio >= uint64(n) || hard <= ratio
}

// shapeWakeCovered wakes the pipes whose deferred injection the ratio
// allowance now covers.
func (s *Session) shapeWakeCovered() {
	sh := s.shp
	sh.injWait = false
	for _, p := range s.pipes {
		if ps := p.sh; ps != nil && ps.sh != nil && ps.injDue {
			if s.injCovered(ps.injLen) {
				s.wakePipe(p)
			} else {
				sh.injWait = true
			}
		}
	}
}

// allowPad bounds n bytes of PADDING by the guardrails.
func (s *Session) allowPad(n int) int {
	sh := s.shp
	hard, ratio := s.padRoom()
	u := min(uint64(n), hard, ratio)
	if u < uint64(n) {
		sh.stats.Clamped++
	}
	sh.stats.PaddingBytes += u
	return int(u)
}

// endShaped applies the plan to the record being closed; the pending
// frame is still to be flushed. Pad is appended in full after the last
// frame, which then keeps its Length field; PadTo and PadMultiple are met
// exactly: by the last frame's Length field when it ends at the target,
// and when the target falls within that field, by PADDING before the last
// frame, which ends the record without it.
func (w *recWriter) endShaped() {
	ps := w.sh
	last := w.pendLen(false)
	if pad := ps.padFor(w.recBytes + w.pendLen(true)); pad > 0 {
		w.flushPend(true)
		w.pad(w.clampPad(pad, 0))
	} else if pad := ps.padFor(w.recBytes + last); pad == 0 {
		w.flushPend(false)
	} else if pad == w.pendLen(true)-last {
		w.flushPend(true) // the Length field completes the target
	} else {
		w.pad(w.clampPad(pad, last))
		w.flushPend(false)
	}
	ps.planned = false
	ps.seq++
	w.cut = 0
	if ps.plan.Flush {
		w.budget = 0
	}
	if ps.plan.Stop {
		ps.stop(true)
	}
}

// clampPad bounds n bytes of PADDING by the record and batch room left
// besides rest bytes still to follow, then by the guardrails.
func (w *recWriter) clampPad(n, rest int) int {
	avail := min(w.recMax-w.recBytes-rest, w.budget-rest-varint.SizeVarint(uint64(w.recMax)))
	if n > avail {
		n = max(avail, 0)
		w.sh.s.shp.stats.Clamped++
	}
	return w.sh.s.allowPad(n)
}

// pad appends n bytes of PADDING to the open record.
func (w *recWriter) pad(n int) {
	if n <= 0 {
		return
	}
	w.b.segs = append(w.b.segs, bseg{ext: zeroPad[:n:n]})
	w.recBytes += n
	w.budget -= n
	w.b.n += n
}

// shapeInject sends a due injection on lane 0: as PADDING of the open
// record, or as a record of PADDING only within the guardrails and, toward
// a plain QMux peer, SPEC.md 9.4.3 rule 1.
func (s *Session) shapeInject(w *recWriter) {
	ps := w.sh
	if ps == nil || !ps.injDue || w.b.Lane != 0 || ps.delayed(s.now) {
		return
	}
	n := ps.injLen
	ps.injDue, ps.injLen, ps.injFirst = false, 0, false
	sh := s.shp
	if w.open && w.recBytes+w.pendLen(false) > 0 && ps.planned {
		ps.plan.Pad += n
		ps.event(shaping.EventInjected, 0, n, false, 0)
		return
	}
	w.end()
	if w.closed || !s.peerZmux && !sh.allowed(s.now) || sh.lim.NoInjection || sh.lim.NoPadding || sh.lim.MaxInjected > 0 && sh.stats.Injected >= sh.lim.MaxInjected {
		sh.stats.InjectDropped++
		return
	}
	if n > w.recMax {
		n = w.recMax // a record is the most an injection sends
		sh.stats.Clamped++
	}
	// An injection that the ratio allowance or, in a batch that already
	// holds records, the batch room does not cover waits, to go whole
	// later; only a hard limit cuts it.
	if !s.injCovered(n) {
		ps.injDue, ps.injLen, sh.injWait = true, n, true
		return
	}
	if m := w.budget - varint.SizeVarint(uint64(w.recMax)); n > m {
		if w.b.n > 0 {
			ps.injDue, ps.injLen, ps.injFirst = true, n, true
			return
		}
		n = max(m, 0)
		sh.stats.Clamped++
	}
	if n = s.allowPad(n); n == 0 {
		sh.stats.InjectDropped++
		return
	}
	w.sh = nil
	w.begin()
	w.pad(n)
	w.end()
	w.sh = ps
	sh.mark(s.now)
	sh.stats.Injected++
	ps.event(shaping.EventInjected, 0, n, false, 0)
}

// allowed reports whether a record of SPEC.md 9.4.3 rule 1 may go now.
func (sh *shapeSess) allowed(now Time) bool { return sh.marks[sh.mi] <= now.Add(-Second) }

func (sh *shapeSess) mark(t Time) {
	sh.marks[sh.mi] = t
	sh.mi = (sh.mi + 1) % len(sh.marks)
}

// shapeRecordIn reports a received record.
func (s *Session) shapeRecordIn(p *Pipe, lane uint64, n int, padOnly bool) {
	if ps := p.sh; ps != nil && ps.sh != nil && ps.caps&shaping.CapRecordIn != 0 {
		ps.event(shaping.EventRecordIn, lane, n, padOnly, 0)
	}
}

// ShapingDeadline is the earliest shaping deadline (Never: none): a
// delayed record, an injection or a shaping timer, which a driver meets
// without its host timer's lateness.
func (s *Session) ShapingDeadline() Time {
	if s.shp == nil || s.state >= StateClosing {
		return Never
	}
	return s.shapeDeadline()
}

// shapeDeadline is the earliest shaping timer.
func (s *Session) shapeDeadline() Time {
	t := Never
	for _, p := range s.pipes {
		ps := p.sh
		if ps == nil || ps.sh == nil {
			continue
		}
		t = minT(t, minT(ps.timers[0], ps.timers[1]))
		if ps.injLen > 0 && !ps.injDue {
			t = minT(t, ps.injAt)
		}
		if ps.delayWake {
			t = minT(t, ps.delayUntil) // exactly: no granularity floor
		}
		if ps.wake && s.sched.mask != 0 {
			r := ps.hold
			if ps.rate > 0 && ps.tokens < ps.quantum() {
				r = max(r, ps.filled.Add(Duration((ps.quantum()-ps.tokens)*1e9/float64(ps.rate))+1))
				r = max(r, s.now.Add(limGranularity))
			}
			t = minT(t, r)
		}
	}
	return t
}

// shapeTimeout runs the due shaping timers.
func (s *Session) shapeTimeout(now Time) {
	for _, p := range s.pipes {
		ps := p.sh
		for i := range 2 {
			if ps == nil || ps.sh == nil {
				break
			}
			if ps.timers[i] <= now {
				ps.timers[i] = Never
				ps.event(shaping.EventTimer, 0, 0, false, i)
			}
		}
		if ps == nil || ps.sh == nil {
			continue
		}
		if ps.injLen > 0 && !ps.injDue && ps.injAt <= now {
			ps.injDue = true
			s.wakePipe(p)
		}
		if ps.delayWake && now >= ps.delayUntil {
			ps.delayWake = false
			s.wakePipe(p)
		}
		if ps.wake && !ps.held(now) {
			ps.wake = false
			s.wakePipe(p)
		}
	}
}

// shapePipeDown ends shaping of a pipe whose outbound direction failed.
func (s *Session) shapePipeDown(p *Pipe) {
	if ps := p.sh; ps != nil && ps.sh != nil {
		ps.event(shaping.EventPipeFailed, 0, 0, false, 0)
		ps.stop(false)
	}
}

// shapeStream reports a stream event to the pipes that asked for it.
func (s *Session) shapeStream(k shaping.EventKind, id uint64) {
	for _, p := range s.pipes {
		if ps := p.sh; ps != nil && ps.sh != nil && ps.caps&shaping.CapStreams != 0 {
			ps.eventOn(shaping.Event{Kind: k, Pipe: p.id, Now: time.Duration(s.now), Stream: id})
		}
	}
}
