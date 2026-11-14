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
	stats shaping.Stats
	// marks: send times of the latest records that the
	// per-second limit on no-effect records counts toward a plain QMux peer (PADDING-only records, QX_PING
	// requests, PIPE_PING frames).
	marks [16]Time
	mi    int
	acct  wireAccount
	eff   shaping.Limits // the local limits under the budget
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
	r    shaping.Effects
	hold Time // stream data waits until then (Gate)
	// The planned record's earliest send time (RecordPlan.Delay): until
	// then the pipe sends nothing but CONNECTION_CLOSE.
	delayUntil Time
	gateGen    uint64 // bumped by each Gate
	rateAsked  uint64 // as answered; the bucket holds the rate in force
	bucket     tokenBucket
	heldSince  Time // the rate gate holds data since then; Never: not held
	injAt      Time
	injLen     int // frame bytes; 0: none
	wakeAt     Time
	pauseAsked Time // the latest PauseUntil taken
	// How the current record's wire lengths convert to frame bytes (with
	// sizeField below).
	over int
	// The plan's MaxLen and PadTo as asked, on the wire.
	wireMaxLen, wirePadTo int
	injPad                int // PADDING frames bytes an injection added to the record
	injPaid               int // of those, what the account already metered
	caps                  shaping.Caps
	weight                uint32
	sizeField             bool // see over
	planned               bool // plan applies to the next record
	delayWake             bool // the delayed record is due at delayUntil
	wake                  bool // a writer waits for hold or tokens
	injDue                bool
	injFirst              bool // the injection waited for batch room: it leads the next batch
}

// SetShapeLimits replaces the session's local limits; the account carries over.
func (s *Session) SetShapeLimits(l shaping.Limits) {
	sh := s.shapeSess()
	s.cfg.ShapeLimits = l
	s.shapeLimitsChanged(sh)
}

// ShapeLimits returns the session's local limits.
func (s *Session) ShapeLimits() shaping.Limits { return s.cfg.ShapeLimits }

// ShapeLimitsInForce returns the local limits under the budget.
func (s *Session) ShapeLimitsInForce() shaping.Limits {
	if s.shp != nil {
		return s.shp.eff
	}
	return s.cfg.ShapeLimits.With(s.cfg.ShapeBudget)
}

// SetShapeBudget replaces the configuration's budget; the account carries over.
func (s *Session) SetShapeBudget(b shaping.Budget) {
	sh := s.shapeSess()
	s.cfg.ShapeBudget = b
	s.shapeLimitsChanged(sh)
}

func (s *Session) shapeLimitsChanged(sh *shapeSess) {
	sh.eff = s.cfg.ShapeLimits.With(s.cfg.ShapeBudget)
	sh.acct.set(usOf(s.now), s.earnBytes, sh.eff.Wire)
	for _, p := range s.pipes {
		if ps := p.sh; ps != nil && ps.sh != nil && ps.rateAsked != 0 {
			ps.setRate()
		}
	}
}

// usOf is t in microseconds of the session clock.
func usOf(t Time) uint64 { return uint64(max(int64(t), 0)) / 1000 }

// shapeSpend meters wire bytes of an accompanying action.
func (s *Session) shapeSpend(n int) {
	if n > 0 {
		s.shapeSess().acct.spend(uint64(n))
	}
}

// ShapeSpendHandshake meters the padding of a handshake run before the session.
func (s *Session) ShapeSpendHandshake(n uint64) {
	if n > 0 {
		s.shapeSess().acct.spend(n)
	}
}

// ShapeMeters returns the session's readings.
func (s *Session) ShapeMeters() shaping.Meters {
	s.shapeSess()
	return s.shapeMeters()
}

// shapeMeters are the readings a CapBudget instance gets.
func (s *Session) shapeMeters() shaping.Meters {
	sh := s.shp
	sh.acct.accrue(usOf(s.now), s.earnBytes)
	return shaping.Meters{WireLeft: sh.acct.left(), StallLeft: math.MaxUint64, WireSpent: sh.acct.spent,
		StallSpent: uint64(sh.stats.StallSpent / time.Microsecond), SentBytes: s.earnBytes}
}

func (s *Session) shapeSess() *shapeSess {
	if s.shp == nil {
		s.shp = &shapeSess{}
		for i := range s.shp.marks {
			s.shp.marks[i] = math.MinInt64
		}
		if s.ping.outstanding != 0 {
			s.shp.mark(s.ping.sentAt)
		}
		// The account runs from the session's start, whenever it is first needed.
		s.shp.eff = s.cfg.ShapeLimits.With(s.cfg.ShapeBudget)
		s.shp.acct.lastUS = usOf(s.start)
		s.shp.acct.set(usOf(s.start), 0, s.shp.eff.Wire)
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
	st := s.shp.stats
	st.WireSpent = s.shp.acct.spent
	return st
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
		old.settleHeld(s.now)
		old.event(shaping.EventReplaced, 0, 0, false)
	}
	ps := &pipeShape{s: s, p: p, sh: x, gen: s.shp.gen, injAt: Never, wakeAt: Never, heldSince: Never}
	p.sh = ps
	if x == nil {
		return ps
	}
	if !ps.call(func() { ps.caps = x.Capabilities() }) {
		return ps
	}
	ps.event(shaping.EventStart, 0, 0, false)
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
	ps.settleHeld(ps.s.now)
	if notify {
		ps.event(shaping.EventStopped, 0, 0, false)
	}
	*ps = pipeShape{s: ps.s, p: ps.p, gen: ps.gen, injAt: Never, wakeAt: Never, heldSince: Never}
}

// event delivers a notification and applies the reply.
func (ps *pipeShape) event(k shaping.EventKind, lane uint64, n int, padOnly bool) {
	ps.eventOn(shaping.Event{Kind: k, Pipe: ps.p.id, Lane: lane, Now: time.Duration(ps.s.now), Len: n,
		PaddingOnly: padOnly})
}

func (ps *pipeShape) eventOn(e shaping.Event) {
	ps.ev = e
	if ps.caps&shaping.CapBudget != 0 {
		ps.ev.Meters = ps.s.shapeMeters()
	}
	ps.r = shaping.Effects{}
	x, k := ps.sh, e.Kind
	if !ps.call(func() { x.OnEvent(&ps.ev, &ps.r) }) || k == shaping.EventReplaced || k == shaping.EventStopped ||
		k == shaping.EventPipeFailed {
		return
	}
	ps.apply(&ps.r)
}

// apply executes the items an answer changes.
func (ps *pipeShape) apply(r *shaping.Effects) {
	if r.Set == 0 {
		return
	}
	s := ps.s
	if r.Set&shaping.EffectInject != 0 {
		ps.injLen, ps.injAt, ps.injDue, ps.injFirst = 0, Never, false, false
		if r.HasInject {
			// It goes as one write of its own.
			ps.injLen = max(framesOf(r.InjectLen, ps.framedSize(), writeOverhead(ps.p)), 1)
			ps.injAt = Time(r.InjectAt)
		}
	}
	if r.Set&shaping.EffectWake != 0 {
		ps.wakeAt = Never
		if r.HasWake {
			ps.wakeAt = Time(r.Wake)
		}
	}
	if r.Set&shaping.EffectGate != 0 {
		// Only what extends the pauses asked for so far counts: an Effects answer
		// repeats its PauseUntil, and a limit may have shortened that pause.
		if d := Time(r.PauseUntil); d > max(ps.hold, s.now, ps.pauseAsked) {
			from := max(ps.hold, s.now)
			ps.hold = from.Add(s.shapePause(d.Sub(max(from, ps.pauseAsked))))
			ps.pauseAsked = d
		}
		burst, clamped := shapeBurstFloor(r.Burst, int64(min64(s.peerMaxRecord, wire.MaxRecordSize)))
		if rate := min(r.Rate, shaping.MaxAccountRate); rate != ps.rateAsked || burst != ps.bucket.burst {
			if clamped && rate != 0 {
				s.shp.stats.Clamped++
			}
			ps.settleHeld(s.now)
			ps.rateAsked, ps.bucket.burst = rate, burst
			ps.setRate()
			ps.bucket.reset(usOf(s.now))
		}
		ps.wake = true
		ps.gateGen++
	}
	if r.Set&shaping.EffectWeight != 0 && r.Weight != 0 {
		ps.weight = r.Weight
	}
	if r.Set&shaping.EffectStop != 0 {
		ps.stop(true)
	}
	s.wakePipe(ps.p)
}

// setRate puts the rate asked in force, within the lower bound of one wait.
func (ps *pipeShape) setRate() {
	md := int64(-1)
	if m := ps.s.shp.eff.MaxDelay; m >= 0 {
		md = int64(m / time.Microsecond)
	}
	r, clamped := shapeRateFloor(ps.rateAsked, md)
	if clamped {
		ps.s.shp.stats.Clamped++
	}
	if r != ps.bucket.rate {
		// What has passed earns at the old rate and is held under the old rate.
		ps.bucket.fill(usOf(ps.s.now))
		ps.settleHeld(ps.s.now)
	}
	ps.bucket.rate = r
}

// settleHeld adds the time the rate gate has held data to the stall; a later
// hold counts from its own start.
func (ps *pipeShape) settleHeld(now Time) {
	if ps.heldSince != Never {
		ps.s.shp.addStall(time.Duration(now.Sub(ps.heldSince)))
		ps.heldSince = Never
	}
}

// framedSize reports that p's records have a size field: on a byte stream.
func (ps *pipeShape) framedSize() bool {
	o := ps.p.out
	return o != nil && len(o.lanes) > 0 && o.lanes[0].info.Framing != FramingRecords
}

// framesOf is the smallest frame byte count whose record reaches the wire
// length t, with over bytes of the write: a length no record has is taken to
// the next one.
func framesOf(t int, sizeField bool, over int) int {
	x := t - over
	if x <= 0 {
		return 0
	}
	if !sizeField {
		return x
	}
	f := max(x-8, 0)
	for f+varint.SizeVarint(uint64(f)) < x {
		f++
	}
	return f
}

// wireOf is the wire length of a record of f frame bytes.
func wireOf(f int, sizeField bool, over int) int {
	if sizeField {
		f += varint.SizeVarint(uint64(f))
	}
	return f + over
}

// shapePause bounds a pause by the limits and returns what is left of it.
func (s *Session) shapePause(d Duration) Duration {
	sh := s.shp
	if d <= 0 {
		return 0
	}
	if sh.eff.NoPause {
		sh.stats.Clamped++
		return 0
	}
	if m := Duration(sh.eff.MaxDelay); m > 0 && d > m {
		d = m
		sh.stats.Clamped++
	}
	sh.addStall(time.Duration(d))
	return d
}

func (sh *shapeSess) addStall(d time.Duration) {
	if d > math.MaxInt64-sh.stats.StallSpent {
		sh.stats.StallSpent = math.MaxInt64
		return
	}
	sh.stats.StallSpent += d
}

// held reports whether the gate holds stream data now.
func (ps *pipeShape) held(now Time) bool {
	if now < ps.hold {
		return true
	}
	if ps.bucket.rate == 0 {
		ps.settleHeld(now)
		return false
	}
	ps.bucket.fill(usOf(now))
	h := ps.bucket.held()
	if !h {
		ps.settleHeld(now)
	} else if ps.heldSince == Never {
		ps.heldSince = now
	}
	return h
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
	if ps.bucket.rate == 0 {
		return -1
	}
	r := ps.bucket.tokens - int64(w.dataSpent-w.gateBase)
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
	if ps.bucket.rate > 0 {
		ps.bucket.tokens -= int64(w.dataSpent - w.gateBase)
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

// delayed reports whether the planned record may not go yet: its delay
// has not passed, and it carries no PIPE_ACK whose deadline has.
func (ps *pipeShape) delayed(now Time) bool {
	return ps.sh != nil && ps.planned && ps.delayWake && now < ps.delayUntil && now < ps.s.ackRelease(ps.p)
}

// ackRelease is when a shaping delay of p's next record ends because the
// record carries a pending PIPE_ACK: the acknowledgement's deadline. A
// shaper never holds an acknowledgement beyond zmux_max_ack_delay, which
// the peer's PTO and so its T_suspect count on (MULTIPATH.md Section
// 7.1); held longer, the peer's working pipe turns suspect at every
// exchange. Never when p carries none.
func (s *Session) ackRelease(p *Pipe) Time {
	if s.ack == nil || !s.carriesAck(p) {
		return Never
	}
	return s.ack.deadline()
}

// shapeDelayed reports whether p's planned record waits for its delay.
func (s *Session) shapeDelayed(p *Pipe) bool { return p.sh != nil && p.sh.delayed(s.now) }

// shapeWants reports an injection due on lane 0 of p.
func (s *Session) shapeWants(p *Pipe, lane uint64) bool {
	return lane == 0 && p.sh != nil && p.sh.injDue
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
			if ps.caps&shaping.CapBudget != 0 {
				ps.v.Meters = s.shapeMeters()
			}
			x := ps.sh
			if !ps.call(func() { x.Plan(&ps.v, &ps.plan) }) {
				w.sh = nil
				return
			}
		}
		ps.planned = true
		pl := &ps.plan
		ps.wireMaxLen, ps.wirePadTo, ps.injPad, ps.injPaid = pl.MaxLen, pl.PadTo, 0, 0
		ps.fit(w, true)
		for _, f := range [...]*int{&pl.PadMultiple, &pl.Pad} {
			if *f > w.recMax {
				*f = w.recMax
				s.shp.stats.Clamped++
			}
		}
		if pl.Effects.Set != 0 {
			ps.apply(&pl.Effects)
			if ps.sh == nil {
				w.sh = nil
				return
			}
		}
		ps.delayUntil, ps.delayWake = 0, false
		if d := pl.Delay; d > 0 {
			ps.delayUntil, ps.delayWake = s.now.Add(s.shapePause(Duration(d))), true
		}
	}
	if ps.delayed(ps.s.now) {
		// The whole record, control frames included, waits.
		w.delayed = true
		return
	}
	// A record planned in the middle of a write may open at the start of a later
	// one, or the reverse: its lengths follow where it opens.
	if ps.over != ps.overHere(w) {
		ps.fit(w, false)
	}
	w.cut = 0
	if m := ps.plan.MaxLen; m > 0 && m < w.recMax {
		w.cut = max(m, shaping.MinMaxLen)
	}
}

// overHere is the write overhead a record opened now by w carries.
func (ps *pipeShape) overHere(w *recWriter) int {
	if w.recs == 0 || w.framing == FramingRecords {
		return writeOverhead(ps.p)
	}
	return 0
}

// fit converts the plan's wire lengths for a record opened now by w; count says
// that its clamps are counted, once for a record.
func (ps *pipeShape) fit(w *recWriter, count bool) {
	s, pl := ps.s, &ps.plan
	clamped := func() {
		if count {
			s.shp.stats.Clamped++
		}
	}
	ps.sizeField, ps.over = w.framing != FramingRecords, ps.overHere(w)
	pl.MaxLen, pl.PadTo = ps.wireMaxLen, ps.wirePadTo
	// The largest record, in frame bytes from here on.
	if m := pl.MaxLen; m > 0 {
		f := framesOf(m, ps.sizeField, ps.over)
		if f < shaping.MinMaxLen {
			f = shaping.MinMaxLen
			clamped()
			if pl.PadTo == m {
				pl.PadTo = wireOf(f, ps.sizeField, ps.over)
			}
		}
		pl.MaxLen = f
	}
	// No padding target goes beyond a record, so that padFor cannot
	// overflow.
	if m := wireOf(w.recMax, ps.sizeField, ps.over); pl.PadTo > m {
		pl.PadTo = m
		clamped()
	}
}

// padFor returns the PADDING the plan adds to a record of n frames bytes.
func (ps *pipeShape) padFor(n int) int {
	l := wireOf(n, ps.sizeField, ps.over)
	t := shaping.PadFor(l, &ps.plan.UnitPlan)
	if t <= l {
		return ps.injPad
	}
	return max(framesOf(t, ps.sizeField, ps.over), n) - n + ps.injPad
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
	if w.cut > 0 && w.recBytes+w.pendLen(true) >= w.cut {
		// Cut at the plan's largest record: the next record's head.
		ps.s.shapeSpend(wireOf(w.cut, ps.sizeField, ps.over) - w.cut)
	}
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

// clampPad bounds n bytes of PADDING by the record and batch room left besides
// rest bytes still to follow, and meters what is not yet.
func (w *recWriter) clampPad(n, rest int) int {
	ps := w.sh
	sh := ps.s.shp
	avail := min(w.recMax-w.recBytes-rest, w.budget-rest-varint.SizeVarint(uint64(w.recMax)))
	if n > avail {
		n = max(avail, 0)
		sh.stats.Clamped++
	}
	if sh.eff.NoPadding {
		if n > 0 {
			sh.stats.Clamped++
		}
		return 0
	}
	sh.acct.spend(uint64(max(n-ps.injPaid, 0)))
	return n
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
	w.notEarned += uint64(n)
}

// shapeInject sends a due injection on lane 0: as PADDING of the open record, or
// as a record of PADDING only within the per-second no-effect limit toward a plain QMux
// peer. Either way it is spontaneous: what the limits or the account refuse is
// dropped, never deferred.
func (s *Session) shapeInject(w *recWriter) {
	ps := w.sh
	if ps == nil || !ps.injDue || w.b.Lane != 0 || ps.delayed(s.now) {
		return
	}
	n := ps.injLen
	ps.injDue, ps.injFirst = false, false // the slot stays until the answer replaces it
	sh := s.shp
	off := sh.eff.NoInjection || sh.eff.NoPadding
	if w.open && w.recBytes+w.pendLen(false) > 0 && ps.planned {
		if off || !sh.acct.charge(usOf(s.now), s.earnBytes, uint64(n)) {
			s.shapeDropped(ps)
			return
		}
		ps.injPad += n // frames bytes, beside the plan's wire lengths
		ps.injPaid += n
		ps.event(shaping.EventInjected, 0, n, false)
		return
	}
	w.end()
	if !s.peerZmux && !sh.allowed(s.now) || off {
		s.shapeDropped(ps)
		return
	}
	// The record before it closed the batch (its plan flushes): it goes first
	// in the next batch, and nothing is charged until it goes.
	if w.closed {
		ps.injDue, ps.injFirst = true, true
		return
	}
	if n > w.recMax {
		n = w.recMax // a record is the most an injection sends
		sh.stats.Clamped++
	}
	// In a batch that already holds records, what the batch has no room for
	// waits, to go whole in the next; nothing is charged until it goes.
	if m := w.budget - varint.SizeVarint(uint64(w.recMax)); n > m {
		if w.b.n > 0 {
			ps.injDue, ps.injLen, ps.injFirst = true, n, true
			return
		}
		n = max(m, 0)
		sh.stats.Clamped++
	}
	if n == 0 || !sh.acct.charge(usOf(s.now), s.earnBytes, uint64(wireOf(n, w.framing != FramingRecords, ps.overHere(w)))) {
		s.shapeDropped(ps)
		return
	}
	w.sh = nil
	w.begin()
	w.pad(n)
	w.end()
	w.sh = ps
	if w.framing != FramingRecords {
		w.notEarned += uint64(recordPositionBytes(n) - n)
	}
	sh.mark(s.now)
	sh.stats.Injected++
	ps.event(shaping.EventInjected, 0, n, false)
}

// shapeDropped reports an injection that will not go. What the answer leaves in
// the slot, kept or new, is tried no sooner than a granularity later: a chain of
// drops is not a loop, however short the answer's own delays.
func (s *Session) shapeDropped(ps *pipeShape) {
	s.shp.stats.InjectDropped++
	ps.event(shaping.EventInjectDropped, 0, 0, false)
	if ps.injLen > 0 {
		ps.injAt = max(ps.injAt, s.now.Add(limGranularity))
	}
}

// allowed reports whether a no-effect record may go now (at most len(marks) per second).
func (sh *shapeSess) allowed(now Time) bool { return sh.marks[sh.mi] <= now.Add(-Second) }

func (sh *shapeSess) mark(t Time) {
	sh.marks[sh.mi] = t
	sh.mi = (sh.mi + 1) % len(sh.marks)
}

// shapeRecordIn reports a received record.
func (s *Session) shapeRecordIn(p *Pipe, lane uint64, n int, padOnly bool) {
	if ps := p.sh; ps != nil && ps.sh != nil && ps.caps&shaping.CapRecordIn != 0 {
		ps.event(shaping.EventRecordIn, lane, n, padOnly)
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
	ackDl, ackDlSet := Never, false // the acknowledgement deadline, computed once
	for _, p := range s.pipes {
		ps := p.sh
		if ps == nil || ps.sh == nil {
			continue
		}
		t = minT(t, ps.wakeAt)
		if ps.injLen > 0 && !ps.injDue {
			t = minT(t, ps.injAt)
		}
		if ps.delayWake {
			rel := Never
			if s.ack != nil && s.carriesAck(p) {
				if !ackDlSet {
					ackDl, ackDlSet = s.ack.deadline(), true
				}
				rel = ackDl // ackRelease(p)
			}
			t = minT(t, minT(ps.delayUntil, rel)) // exactly: no granularity floor
		}
		if ps.wake && s.sched.mask != 0 {
			r := ps.hold
			if b := &ps.bucket; b.rate > 0 && b.held() {
				at := Never
				if us, ok := b.release(); ok && us <= math.MaxInt64/1000 {
					at = Time(us * 1000)
				}
				r = max(r, at)
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
		if ps == nil || ps.sh == nil {
			continue
		}
		if at := ps.wakeAt; at <= now {
			ps.event(shaping.EventTimer, 0, 0, false)
			if ps.sh == nil {
				continue
			}
			if ps.wakeAt == at { // not replaced: not again at once
				ps.wakeAt = now.Add(limGranularity)
			}
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
		ps.event(shaping.EventPipeFailed, 0, 0, false)
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

// ShapeFaults counts the panics of the session's shapers (ShapingStats.Faults).
func (s *Session) ShapeFaults() uint64 {
	if s.shp == nil {
		return 0
	}
	return s.shp.stats.Faults
}
