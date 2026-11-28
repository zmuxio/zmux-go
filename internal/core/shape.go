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
// and the application's guardrails. The session initializes this state when
// it installs a shaper or a bound.

// zeroPad backs PADDING frames (0x00 bytes); it is never written.
var zeroPad [wire.MaxRecordSize]byte

// shapeSess is the session's shaping state.
type shapeSess struct {
	fac     Factory
	waits   map[*HandshakeWait]struct{}
	eff     shaping.Limits // the local limits under the budget
	timers  DeadlineQueue[*pipeShape]
	stats   shaping.Stats
	acct    wireAccount
	gen     uint64 // bumped by SetShaper; a pipe's instance of an older one is replaced
	delayed int
}

// pipeShape is the shaping state of one outbound pipe.
type pipeShape struct {
	sh        shaping.Shaper // nil: not shaped
	datagram  shaping.DatagramPlanner
	s         *Session
	p         *Pipe
	deadlines [shapeDeadlineCount]Deadline[*pipeShape]
	native    nativePlan
	ev        shaping.Event
	v         shaping.RecordView
	plan      shaping.RecordPlan // applies to the next record while planned
	r         shaping.Effects
	bucket    tokenBucket
	gen       uint64
	seq       uint64
	hold      Time // stream data waits until then (Gate)
	// The planned record's earliest send time (RecordPlan.Delay): until
	// then the pipe sends nothing but CONNECTION_CLOSE.
	delayUntil     Time
	delayStart     Time
	delayLane      *outLane
	gateGen        uint64 // bumped by each Gate
	rateAsked      uint64 // as answered; the bucket holds the rate in force
	waitHistory    recordWait
	heldUntil      Time
	heldSince      Time // the rate gate holds data since then; Never: not held
	heldLane       *outLane
	injLen         int // frame bytes; 0: none
	injID          uint64
	injReservation shapeDispatch
	pauseAsked     Time // the latest PauseUntil taken
	// How the current record's wire lengths convert to frame bytes (with
	// sizeField below).
	over int
	// The plan's MaxLen and PadTo as asked, on the wire.
	wireMaxLen, wirePadTo int
	caps                  shaping.Caps
	weight                uint32
	delayActive           bool
	delayCounting         bool
	delayDatagram         bool
	delayCopy             bool
	sizeField             bool // see over
	planned               bool // plan applies to the next record
	injDue                bool
	injFirst              bool // the injection waited for batch room: it leads the next batch
}

// SetShapeLimits replaces the session's local limits; the account carries over.
func (s *Session) SetShapeLimits(l shaping.Limits) bool {
	return s.setShapeLimits(l, s.cfg.ShapeBudget, false)
}

// ShapeLimits returns the session's local limits.
func (s *Session) ShapeLimits() shaping.Limits { return s.cfg.ShapeLimits.Clone() }

// ShapeLimitsInForce returns the local limits under the budget.
func (s *Session) ShapeLimitsInForce() shaping.Limits {
	if s.shp != nil {
		return s.shp.eff
	}
	return s.cfg.ShapeLimits.With(s.cfg.ShapeBudget)
}

func (s *Session) setShapeLimits(l shaping.Limits, b shaping.Budget, compile bool) bool {
	if !l.Wire.Valid() || !b.Wire.Valid() || !l.Profile.Valid() || !b.Profile.Valid() || b.Boundary > shaping.BoundaryCarrierInterface {
		return false
	}
	if compile && b.Profile == nil {
		profile := shaping.Lightweight()
		b.Profile = &profile
	}
	checked := b
	if s.feat.Preamble {
		checked = b.WithPreamble(s.preambleMax())
	} else if !s.peerSeen && s.cfg.Local.Has&wire.HasPreamble != 0 {
		checked = b.WithPreamble(wire.MaxPreambleSize)
	}
	if !checked.Fits(l) {
		return false
	}
	s.pruneShape()
	s.cfg.ShapeLimits, s.cfg.ShapeBudget = l.Clone(), b.Clone()
	if s.shp == nil && !compile && l.Wire == nil && b.Wire == nil && l.Profile == nil && b.Profile == nil {
		return true
	}
	sh := s.shapeSess()
	sh.eff = s.cfg.ShapeLimits.With(s.cfg.ShapeBudget)
	sh.acct.set(usOf(s.now), sh.eff.Wire)
	for _, p := range s.pipes {
		ps := p.sh
		if ps == nil || ps.sh == nil {
			continue
		}
		if sh.eff.NoPause {
			ps.settleHeld(s.now)
			ps.hold, ps.rateAsked, ps.bucket = 0, 0, tokenBucket{}
			ps.gateGen++
		} else if ps.rateAsked != 0 {
			ps.setRate()
		}
		ps.refreshGateDeadline()
		if ps.delayActive && s.now < ps.delayUntil {
			prior := Duration(0)
			if !ps.delayDatagram {
				prior = ps.priorWait(s.now)
			}
			ps.delayUntil = s.now.Add(s.boundedShapeDelay(ps.delayUntil.Sub(s.now), prior))
			ps.armDeadline(shapeDelayUntil, ps.delayUntil)
			ps.refreshRecordFlows()
		}
		if ps.native.pending() {
			n := &ps.native
			if n.flow != nil {
				s.refreshFlowDelay(n.flow)
			} else {
				ps.settleNativeCopyDelay()
				c := &p.datagramCopies[0]
				n.ready = s.now.Add(s.boundedShapeDelay(n.ready.Sub(s.now), c.delay.spent))
				ps.armDeadline(shapeNativeAt, n.ready)
				ps.startNativeCopyDelay(s.channelOf(p))
			}
			s.wake(p.chSig)
		}
		s.wakePipe(p)
	}
	return true
}

// usOf is t in microseconds of the session clock.
func usOf(t Time) uint64 { return uint64(max(int64(t), 0)) / 1000 }

// AdmitHandshake transfers a proved Establish once, when its carrier attaches.
func (s *Session) AdmitHandshake(writes shaping.HandshakeWrites, wallNow time.Time) bool {
	p, l := s.shapeProfiles()
	if !writes.Fits(p, l) {
		return false
	}
	h := &s.shapeLedger.handshake
	for _, u := range writes {
		if u.At.IsZero() {
			continue
		}
		offset := time.Duration(s.now.Sub(s.start)) - max(wallNow.Sub(u.At), 0)
		second := int64(offset / time.Second)
		if offset%time.Second < 0 {
			second--
		}
		s.settleShape(shapeDispatch{second: second}, u.Baseline, 0)
		h.ConsumedBytes = satAdd(h.ConsumedBytes, u.Extra)
		if u.Delay > 0 {
			sh := s.shapeSess()
			sh.noteDelay(u.Delay)
			sh.addStall(u.Delay)
		}
	}
	return true
}

func (s *Session) shapingEarn(n uint64) {
	s.earnBytes = satAdd(s.earnBytes, n)
	if s.shp != nil {
		s.shp.acct.accrue(usOf(s.now), n)
	}
}

// ConfirmJoinOwnBytes earns the confirmed original bytes in an accepted Hello.
func (s *Session) ConfirmJoinOwnBytes(n uint64, now Time) {
	if n != 0 {
		s.SetNow(now)
		s.shapingEarn(n)
	}
}

// HandshakeWait owns the delay of one Join unit while the driver waits outside
// the session lock.
type HandshakeWait struct {
	start    Time
	duration time.Duration
}

// Deadline returns the end of the registered wait on the session clock.
func (w *HandshakeWait) Deadline() Time { return w.start.Add(Duration(w.duration)) }

// BeginHandshakeWait registers a bounded Join delay.
func (s *Session) BeginHandshakeWait(d time.Duration) *HandshakeWait {
	sh := s.shapeSess()
	w := &HandshakeWait{start: s.now, duration: d}
	if sh.waits == nil {
		sh.waits = make(map[*HandshakeWait]struct{})
	}
	sh.waits[w] = struct{}{}
	return w
}

// FinishHandshakeWait meters the elapsed part of a completed or cancelled wait.
func (s *Session) FinishHandshakeWait(w *HandshakeWait, now Time) {
	s.SetNow(now)
	sh := s.shp
	if _, ok := sh.waits[w]; !ok {
		return
	}
	delete(sh.waits, w)
	elapsed := min(w.duration, time.Duration(s.now.Sub(w.start)))
	sh.noteDelay(elapsed)
	sh.addStall(elapsed)
}

// maxShapeDelay projects active waits for a meter read.
func (s *Session) maxShapeDelay() time.Duration {
	maxDelay := s.shp.stats.MaxAddedDelay
	for _, p := range s.pipes {
		if ps := p.sh; ps != nil && ps.sh != nil {
			maxDelay = max(maxDelay, time.Duration(ps.priorWait(s.now)))
			if ps.native.replica && len(p.datagramCopies) != 0 {
				c := &p.datagramCopies[0]
				maxDelay = max(maxDelay, time.Duration(c.delay.spent+c.delay.elapsed(min(ps.native.ready, c.deadline), s.now)))
			}
			if ps.delayCopy {
				c := &p.datagramCopies[0]
				maxDelay = max(maxDelay, time.Duration(c.delay.spent+c.delay.elapsed(min(ps.recordDelayEnd(), c.deadline), s.now)))
			}
		}
	}
	if s.dg != nil {
		for _, f := range s.dg.active {
			if f.delay.pipe != nil {
				end := f.delay.end()
				for i := range f.queue {
					o := &f.queue[i]
					maxDelay = max(maxDelay, time.Duration(o.delay.spent+o.delay.elapsed(min(end, o.deadline), s.now)))
				}
			}
		}
	}
	for w := range s.shp.waits {
		maxDelay = max(maxDelay, min(w.duration, time.Duration(s.now.Sub(w.start))))
	}
	return maxDelay
}

func heldDelay(start, until, now Time) time.Duration {
	return time.Duration(max(minT(now, until).Sub(start), 0))
}

// ShapeMeters returns the session's readings.
func (s *Session) ShapeMeters() shaping.Meters {
	if s.shp == nil {
		return shaping.Meters{Handshake: s.shapeLedger.handshake, Boundary: shaping.BoundaryCarrierInterface, Precision: s.shapePrecision(), StallLeft: math.MaxUint64, EarnedBytes: s.earnBytes, ConsumedBytes: s.shapeLedger.measured}
	}
	m := s.shapeMeters()
	m.MaxAddedDelay = s.maxShapeDelay()
	return m
}

// shapeMeters are the readings a CapBudget instance gets.
func (s *Session) shapeMeters() shaping.Meters {
	sh := s.shp
	acct := sh.acct
	acct.accrue(usOf(s.now), 0)
	p, l := s.shapeProfiles()
	return shaping.Meters{Handshake: s.shapeLedger.handshake, Boundary: shaping.BoundaryCarrierInterface, Precision: s.shapePrecision(), Bounded: s.cfg.ShapeBudget.Profile != nil,
		WireLeft: acct.left(), WireSpent: acct.spent, EarnedBytes: s.earnBytes, StallLeft: math.MaxUint64,
		StallSpent: uint64(sh.stats.StallSpent / time.Microsecond), MaxAddedDelay: sh.stats.MaxAddedDelay,
		Profile: s.shapeSnapshot(p, 0), Local: s.shapeSnapshot(l, 0), ConsumedBytes: s.shapeLedger.measured}
}

func (s *Session) shapeSess() *shapeSess {
	if s.shp == nil {
		s.shp = &shapeSess{}
		// First enable anchors income at this owner turn.
		s.shp.eff = s.cfg.ShapeLimits.With(s.cfg.ShapeBudget)
		s.shp.acct.lastUS = usOf(s.now)
		s.shp.acct.set(usOf(s.now), s.shp.eff.Wire)
	}
	return s.shp
}

// Factory returns the Shaper of an outbound pipe of carrier kind carrier
// (SetPipeKind) whose carrier adds overhead bytes to each write
// (CarrierInfo.WriteOverhead); nil: the pipe is not shaped.
type Factory func(pipe uint64, carrier uint8, overhead int) shaping.Shaper

// ConfigureShaper atomically installs the host's plan and resolved limits.
func (s *Session) ConfigureShaper(f Factory, b shaping.Budget) bool {
	if !s.setShapeLimits(s.cfg.ShapeLimits, b, f != nil) {
		return false
	}
	s.installFactory(f)
	return true
}

// installFactory publishes the resolved factory to every outbound pipe.
func (s *Session) installFactory(f Factory) {
	if f == nil && s.shp == nil {
		return
	}
	sh := s.shapeSess()
	sh.fac = f
	sh.gen++
	for _, p := range s.pipes {
		if p.out != nil {
			s.shapeOf(p)
		}
	}
	s.wakeAllWriters()
	for _, p := range s.pipes {
		s.wake(p.chSig)
	}
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
		p.sh.cancelNative(shaping.ReasonStaleGeneration)
		p.sh.clearInjection()
		p.sh.gen-- // reinstalled from the factory on the new carrier
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

// ShapingStats returns the shaping counters.
func (s *Session) ShapingStats() shaping.Stats {
	if s.shp == nil {
		return shaping.Stats{}
	}
	st := s.shp.stats
	st.WireSpent = s.shp.acct.spent
	st.MaxAddedDelay = s.maxShapeDelay()
	return st
}

// shapeOf returns p's active shaping state, installing the factory's
// instance when p has none of the current generation.
func (s *Session) shapeOf(p *Pipe) *pipeShape {
	ps := p.sh
	if ps == nil && s.shp.fac == nil {
		return nil
	}
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
	var history recordWait
	if old := p.sh; old != nil && old.sh != nil {
		old.cancelNative(shaping.ReasonStaleGeneration)
		old.finishRecordDelay()
		old.settleHeld(s.now)
		history = old.waitHistory
		old.event(shaping.EventReplaced)
		old.clearInjection()
		old.cancelDeadlines()
	}
	ps := &pipeShape{s: s, p: p, sh: x, gen: s.shp.gen, heldSince: Never, waitHistory: history}
	p.sh = ps
	if x == nil {
		return ps
	}
	if !ps.call(func() { ps.caps = x.Capabilities() }) {
		return ps
	}
	if ps.caps&shaping.CapDatagramPlan != 0 {
		ps.datagram, _ = x.(shaping.DatagramPlanner)
	}
	ps.event(shaping.EventStart)
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
	ps.cancelNative(shaping.ReasonCancelled)
	ps.finishRecordDelay()
	ps.settleHeld(ps.s.now)
	if notify {
		ps.event(shaping.EventStopped)
	}
	ps.clearInjection()
	ps.cancelDeadlines()
	*ps = pipeShape{s: ps.s, p: ps.p, gen: ps.gen, heldSince: Never}
}

// event delivers a notification and applies the reply.
func (ps *pipeShape) event(k shaping.EventKind) {
	ps.eventOn(shaping.Event{Kind: k, Pipe: ps.p.id, Now: time.Duration(ps.s.now)})
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
	ps.apply(&ps.r, k)
}

// apply executes the items an answer changes.
func (ps *pipeShape) apply(r *shaping.Effects, origin shaping.EventKind) {
	if r.Set == 0 {
		return
	}
	s := ps.s
	if r.Set&shaping.EffectInject != 0 {
		req := r.Injection
		switch {
		case req.ID == 0 || req.PadBytes <= 0:
			ps.clearInjection()
		case origin == shaping.EventOutbound || origin == shaping.EventTimer:
			ps.clearInjection()
			if !s.coverEligible(ps.p) && Time(req.At) <= s.now {
				break
			}
			ps.injID, ps.injLen = req.ID, req.PadBytes
			at := Time(req.At)
			ps.injDue, ps.injFirst = at <= s.now, false
			if ps.injDue {
				at = Never
			} else if ol := s.coverLane(ps.p); ol != nil && !s.shp.eff.NoPadding && !s.shp.eff.NoInjection {
				cost := uint64(wireOf(min(req.PadBytes, s.recMax(ol)), ol.info.Framing == FramingBytes, ol.info.SealPrefix+ol.info.SealSuffix))
				if cost <= s.shapeRoom() {
					ps.injReservation = s.reserveShape(cost)
				}
			}
			ps.armDeadline(shapeInjectAt, at)
		}
	}
	if r.Set&shaping.EffectWake != 0 {
		at := Never
		if r.HasWake {
			at = Time(r.Wake)
			if at <= s.now && origin != shaping.EventStart {
				at = s.now.Add(limGranularity)
			}
		}
		ps.armDeadline(shapeWakeAt, at)
	}
	if r.Set&shaping.EffectGate != 0 {
		switch {
		case s.shp.eff.NoPause:
			s.shp.clip(shaping.ReasonDisabled)
		default:
			ps.applyGate(r)
		}
	}
	if r.Set&shaping.EffectWeight != 0 && r.Weight != 0 {
		ps.weight = r.Weight
	}
	if r.Set&shaping.EffectStop != 0 {
		ps.stop(true)
	}
	s.wakePipe(ps.p)
}

func (ps *pipeShape) applyGate(r *shaping.Effects) {
	s := ps.s
	// Only what extends the pauses asked for so far counts: an Effects answer
	// repeats its PauseUntil, and a limit may have shortened that pause.
	if d := Time(r.PauseUntil); d > max(ps.hold, s.now, ps.pauseAsked) {
		from := max(ps.hold, s.now)
		ps.hold = from.Add(s.shapePause(d.Sub(max(from, ps.pauseAsked)), 0))
		ps.pauseAsked = d
	}
	burst, clamped := shapeBurstFloor(r.Burst, int64(min64(s.peerMaxRecord, wire.MaxRecordSize)))
	if rate := min(r.Rate, shaping.MaxAccountRate); rate != ps.rateAsked || burst != ps.bucket.burst {
		if clamped && rate != 0 {
			s.shp.clip(shaping.ReasonClippedRoom)
		}
		ps.settleHeld(s.now)
		if profile := s.shp.eff.Profile; profile != nil && shaping.GateWait(rate, uint64(burst), 0) > profile.MaxAddedDelay {
			s.shp.clip(shaping.ReasonPolicyDeadline)
			rate = 0
		}
		ps.rateAsked, ps.bucket.burst = rate, burst
		ps.setRate()
		ps.bucket.reset(usOf(s.now))
	}
	ps.refreshGateDeadline()
	ps.gateGen++
}

// setRate preserves the requested rate across gate changes.
func (ps *pipeShape) setRate() {
	r := ps.rateAsked
	if r != ps.bucket.rate {
		// What has passed earns at the old rate and is held under the old rate.
		ps.bucket.fill(usOf(ps.s.now))
		ps.settleHeld(ps.s.now)
	}
	ps.bucket.rate = r
}

// recordWait keeps the accumulated union for the pending ordinary record.
// The last interval can overlap a gate which was already running when the
// interval was closed. Earlier disjoint intervals are contained in total.
type recordWait struct {
	start, end Time
	total      Duration
}

func (h *recordWait) add(start, end Time) Duration {
	if end <= start {
		return 0
	}
	d := end.Sub(start)
	if h.total != 0 && end >= h.start && start <= h.end {
		d -= max(min(end, h.end).Sub(max(start, h.start)), 0)
		h.start = min(start, h.start)
		h.end = max(end, h.end)
	} else {
		h.start, h.end = start, end
	}
	h.total += d
	return d
}

func (ps *pipeShape) accountWait(start, end Time) {
	d := ps.waitHistory.add(start, end)
	ps.s.shp.addStall(time.Duration(d))
	ps.s.shp.noteDelay(time.Duration(ps.waitHistory.total))
}

// priorWait is the union already incurred before a proposed interval begins.
func (ps *pipeShape) priorWait(at Time) Duration {
	h := ps.waitHistory
	if ps.heldSince != Never {
		h.add(ps.heldSince, min(at, ps.heldUntil))
	}
	if ps.delayCounting {
		h.add(ps.delayStart, min(at, ps.recordDelayEnd()))
	}
	return h.total - max(h.end.Sub(max(at, h.start)), 0)
}

func (ps *pipeShape) settleHeld(now Time) {
	if ps.heldSince != Never {
		ps.accountWait(ps.heldSince, min(now, ps.heldUntil))
		ps.heldSince = Never
		ps.heldLane = nil
	}
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

func (s *Session) boundedShapeDelay(d, prior Duration) Duration {
	l := s.shp.eff
	if d <= 0 || l.NoPause {
		return 0
	}
	if l.Profile != nil {
		d = min(d, max(Duration(l.Profile.MaxAddedDelay)-prior, 0))
	}
	return d
}

// shapePause bounds one proposed delay.
func (s *Session) shapePause(d, prior Duration) Duration {
	wait := s.boundedShapeDelay(d, prior)
	if wait < d {
		s.shp.clip(shaping.ReasonPolicyDeadline)
	}
	return wait
}

func (s *Session) shapeReason(r shaping.RuntimeReason) {
	if s.shp != nil && r != shaping.ReasonNone {
		s.shp.reason(r)
	}
}

func (sh *shapeSess) reason(r shaping.RuntimeReason) {
	sh.stats.RuntimeReasons[r] = satAdd(sh.stats.RuntimeReasons[r], 1)
}

func (sh *shapeSess) clip(r shaping.RuntimeReason) {
	sh.stats.Clamped = satAdd(sh.stats.Clamped, 1)
	sh.reason(r)
}

func (sh *shapeSess) noteDelay(d time.Duration) {
	sh.stats.MaxAddedDelay = max(sh.stats.MaxAddedDelay, d)
}

func (ps *pipeShape) finishRecordDelay() {
	if !ps.delayActive {
		return
	}
	ps.finishRecordFlows()
	ps.pauseRecordWait(ps.delayLane)
	ps.delayActive, ps.delayDatagram, ps.delayLane = false, false, nil
	ps.delayUntil = 0
	ps.armDeadline(shapeDelayUntil, Never)
}

func (ps *pipeShape) recordDelayEnd() Time {
	return min(ps.delayUntil, ps.s.ackRelease(ps.p))
}

func (ps *pipeShape) pauseRecordWait(lane *outLane) {
	if ps.delayCounting && ps.delayLane == lane {
		ps.accountWait(ps.delayStart, min(ps.recordDelayEnd(), ps.s.now))
		ps.delayCounting = false
	}
}

func (ps *pipeShape) resumeRecordWait(lane *outLane) {
	if ps.delayActive && !ps.delayDatagram && !ps.delayCounting && ps.delayLane == lane &&
		lane != nil && !lane.stalled() && ps.s.now < ps.recordDelayEnd() {
		ps.delayStart, ps.delayCounting = ps.s.now, true
	}
}

func (ps *pipeShape) laneBlocked(lane *outLane) {
	ps.pauseRecordWait(lane)
	if ps.heldLane == lane {
		ps.settleHeld(ps.s.now)
		ps.armDeadline(shapeGateUntil, Never)
	}
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
	ps.bucket.fill(usOf(now))
	closed := now < ps.hold || ps.bucket.rate > 0 && ps.bucket.held()
	if !closed {
		ps.settleHeld(now)
		return false
	}
	if ps.heldSince == Never {
		ps.heldSince = now
	}
	ps.heldUntil = ps.gateRelease()
	return now < ps.heldUntil
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
		if w.gateShut && ps.heldLane == nil {
			ps.heldLane = w.b.owner
		}
	}
	if w.gateShut {
		w.gateWaiting = true
		return 0
	}
	if ps.bucket.rate == 0 || ps.heldSince != Never && !ps.held(ps.s.now) {
		return -1
	}
	r := ps.bucket.tokens - int64(w.dataSpent-w.gateBase)
	if r <= 0 {
		w.gateWaiting = true
		return 0
	}
	return r
}

// chargeGate takes the batch's stream data written under the current gate
// from its bucket.
func (w *recWriter) chargeGate() {
	ps := w.gate
	if ps == nil || ps.sh == nil || !w.gateOn && w.dataSpent == 0 {
		return
	}
	w.gateRoom() // follows a gate set by the batch's last plan
	if ps.bucket.rate > 0 {
		ps.bucket.tokens -= int64(w.dataSpent - w.gateBase)
	}
	if w.gateWaiting || ps.bucket.rate > 0 && ps.bucket.held() && ps.hasGateWork() {
		ps.armGateDeadline()
	}
	if w.dataSpent > w.gateBase {
		ps.settleHeld(ps.s.now)
	}
	w.gateBase = w.dataSpent
}

func (s *Session) shapeHeld(p *Pipe) bool {
	ps := p.sh
	if ps == nil || ps.sh == nil {
		return false
	}
	b := ps.bucket
	b.fill(usOf(s.now))
	if s.now >= ps.hold && (b.rate == 0 || !b.held()) {
		return false
	}
	return s.boundedShapeDelay(1, ps.priorWait(s.now)) != 0
}

// delayed reports whether the planned record may not go yet: its delay
// has not passed, and it carries no PIPE_ACK whose deadline has.
func (ps *pipeShape) delayed(now Time) bool {
	d := &ps.deadlines[shapeDelayUntil]
	return d.index != 0 && now < d.at && now < ps.s.ackRelease(ps.p)
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
	return lane == 0 && p.sh != nil && p.sh.injDue && s.coverEligible(p)
}

// prep plans the record that the writer opens next.
func (ps *pipeShape) prep(w *recWriter) {
	if ps.sh == nil {
		w.sh = nil
		return
	}
	// A planned record retains its layout and release through subsequent
	// polls. Completion is reported independently by the carrier.
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
		if s.shp.eff.Profile != nil && (pl.MaxLen > 0 || pl.Flush) {
			pl.MaxLen, pl.Flush = 0, false
			// This layout was rejected before dispatch. Its known padding
			// can still be priced against the baseline record.
			s.shp.clip(shaping.ReasonUnsupportedCapability)
		}
		ps.wireMaxLen, ps.wirePadTo = pl.MaxLen, pl.PadTo
		ps.fit(w, true)
		for _, f := range [...]*int{&pl.PadMultiple, &pl.Pad} {
			if *f > w.recMax {
				*f = w.recMax
				s.shp.clip(shaping.ReasonClippedRoom)
			}
		}
		if pl.Effects.Set != 0 {
			ps.apply(&pl.Effects, 0)
			if ps.sh == nil {
				w.sh = nil
				return
			}
		}
		var prior Duration
		if !w.datagramPlanning {
			prior = ps.priorWait(s.now)
		}
		if d := s.shapePause(Duration(pl.Delay), prior); d > 0 {
			ps.delayActive, ps.delayDatagram, ps.delayLane = true, w.datagramPlanning, w.b.owner
			ps.delayUntil = s.now.Add(d)
			ps.resumeRecordWait(w.b.owner)
			ps.armDeadline(shapeDelayUntil, ps.delayUntil)
			ps.refreshRecordFlows()
		}
	}
	if ps.delayed(ps.s.now) {
		// The whole record, control frames included, waits.
		w.delayed = true
		return
	}
	ps.finishRecordDelay()
	// A record planned in the middle of a write may open at the start of a later
	// one, or the reverse: its lengths follow where it opens.
	if ps.over != ps.overHere(w) {
		ps.fit(w, false)
	}
	w.cut = 0
	if m := ps.plan.MaxLen; ps.s.shp.eff.Profile == nil && m > 0 && m < w.recMax {
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
			s.shp.clip(shaping.ReasonClippedRoom)
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
		return 0
	}
	return max(framesOf(t, ps.sizeField, ps.over), n) - n
}

// endShaped fixes the business layout before trying to add cover.
func (w *recWriter) endShaped() {
	ps := w.sh
	without, with := w.pendLen(false), w.pendLen(true)
	base := w.recBytes + without
	w.baseFrames = base
	if w.cut > 0 && w.recBytes+with >= w.cut {
		w.added += uint64(wireOf(w.cut, ps.sizeField, ps.over) - w.cut)
	}
	if !ps.s.shp.eff.NoPadding {
		pad, tail := ps.padFor(base), without
		if after := ps.padFor(w.recBytes + with); after > 0 {
			pad, tail = after, with
		}
		if tail == without && pad == with-without && pad > 0 {
			tail, pad = with, 0
		}
		want := pad
		// If retaining Length would exceed the profile room, use the
		// no-length tail and put the optional bytes in front of it.
		if tail > without {
			current := w.recBytes + tail
			if w.recordWire(current)-w.recordWire(base) > int(min(w.shapeRoom(), wire.MaxRecordSize+8)) ||
				current > w.recMax || w.recordWire(current)-w.recBytes > w.budget {
				tail, pad = without, ps.padFor(base)
			}
		}
		pad = w.paddingFit(base, w.recBytes+tail, pad, 0)
		if pad < want {
			r := shaping.ReasonClippedRoom
			if w.shapeRoom() < uint64(want) {
				r = shaping.ReasonOverheadLimit
			}
			ps.s.shp.clip(r)
		}
		w.added += uint64(w.recordWire(w.recBytes+tail+pad) - w.recordWire(base))
		// Cover precedes an unflushed tail so its bytes never become
		// part of the data of a no-Length frame.
		if tail > without {
			w.lengthStart, w.lengthLen = w.recBytes+without-w.pend.n, with-without
			w.flushPend(true)
			tail = 0
		}
		w.padStart, w.padLen = w.recBytes, pad
		w.pad(pad)
		w.mergeInjection(w.recBytes + tail)
		w.flushPend(false)
	} else {
		w.mergeInjection(base)
		w.flushPend(false)
	}
	if w.dataOut || !ps.hasGateWork() {
		ps.settleHeld(ps.s.now)
		ps.waitHistory = recordWait{}
	}
	ps.planned = false
	ps.seq++
	w.cut = 0
	if ps.plan.Flush && ps.s.shp.eff.Profile == nil {
		w.budget = 0
	}
	if ps.plan.Stop {
		w.stopShape = ps
		w.sh, w.gate = nil, nil
	}
}

func (w *recWriter) recordWire(frames int) int {
	return wireOf(frames, w.framing == FramingBytes, 0)
}

func (w *recWriter) shapeRoom() uint64 {
	room := w.sh.s.shapeRoom()
	return room - min(room, w.added)
}

// paddingFit considers final framing and a still-unflushed tail. The
// size prefix can cross at most one of the bounded record's thresholds
// near the chosen end.
func (w *recWriter) paddingFit(base, frames, n, fixed int) int {
	available := w.budget + w.recBytes
	room := int(min(w.shapeRoom(), wire.MaxRecordSize+8))
	limit := min(available, w.recordWire(base)+room-fixed)
	hi := max(0, min(w.recMax-frames, n, limit-frames))
	for hi > 0 && w.recordWire(frames+hi) > limit {
		hi--
	}
	return hi
}

// pad appends shared zeroes to the open record.
func (w *recWriter) pad(n int) {
	if n <= 0 {
		return
	}
	w.b.segs = append(w.b.segs, bseg{ext: zeroPad[:n:n]})
	w.recBytes += n
	w.budget -= n
	w.b.n += n
}

func (ps *pipeShape) clearInjection() {
	ps.releaseInjectionReservation()
	ps.injID, ps.injLen, ps.injDue, ps.injFirst = 0, 0, false, false
	ps.armDeadline(shapeInjectAt, Never)
}

func (ps *pipeShape) releaseInjectionReservation() {
	if ps.injReservation.extra != 0 {
		ps.s.settleShape(ps.injReservation, 0, 0)
		ps.injReservation = shapeDispatch{}
	}
}

func (s *Session) coverEligible(p *Pipe) bool {
	return s.coverLane(p) != nil
}

func (s *Session) coverLane(p *Pipe) *outLane {
	if !s.peerSeen || s.holdSends() || s.state >= StateClosing || !maySend(p, false) {
		return nil
	}
	lane := p.out.lane(0)
	if lane == nil || !lane.confirmed || lane.shut || lane.laneFirst || p.id == 0 && !lane.tpSent {
		return nil
	}
	return lane
}

// shapeInject prepares a due standalone injection after ordinary work. An
// eligible open record merges it when the record is finalized.
func (s *Session) shapeInject(w *recWriter) {
	ps := w.sh
	if ps == nil || !ps.injDue || w.b.Lane != 0 || ps.delayed(s.now) || !s.coverEligible(ps.p) || w.closed {
		return
	}
	if w.open && !w.setup && w.recBytes+w.pendLen(false) > 0 {
		w.mergeCover = true
		return
	}
	w.end()
	n := min(ps.injLen, w.recMax)
	if w.b.Len() != 0 && w.recordWire(n) > w.budget {
		ps.injFirst = true
		return
	}
	n = min(n, max(w.budget, 0))
	for n > 0 && w.recordWire(n) > w.budget {
		n--
	}
	cost := w.recordWire(n) + w.b.EnvelopeSize()
	reason := w.coverRejection(n, cost, !s.peerZmux)
	if reason != 0 {
		w.coverResult(ps, reason, 0, 0)
		return
	}
	w.sh = nil
	w.begin()
	w.coverResult(ps, 0, n, cost)
	w.b.cover.standalone = true
	w.pad(n)
	w.end()
	w.sh = ps
}

func (w *recWriter) mergeInjection(frames int) {
	ps := w.sh
	if !w.mergeCover {
		return
	}
	w.mergeCover = false
	n := min(ps.injLen, max(w.recMax-frames, 0), max(w.budget+w.recBytes-frames, 0))
	for n > 0 && w.recordWire(frames+n) > w.budget+w.recBytes {
		n--
	}
	cost := w.recordWire(frames+n) - w.recordWire(frames)
	if n == 0 && ps.injLen > 0 && !ps.s.shp.eff.NoInjection && !ps.s.shp.eff.NoPadding {
		ps.injFirst = true
		return
	}
	if reason := w.coverRejection(n, cost, false); reason != 0 {
		w.coverResult(ps, reason, 0, 0)
		return
	}
	w.coverResult(ps, 0, n, cost)
	w.pad(n)
}

func (w *recWriter) coverRejection(n, cost int, limited bool) shaping.RuntimeReason {
	s := w.sh.s
	sh := s.shp
	switch {
	case sh.eff.NoPadding || sh.eff.NoInjection:
		return shaping.ReasonDisabled
	case limited && !s.noEffect.available(s.now):
		return shaping.ReasonRateLimit
	case n == 0:
		return shaping.ReasonClippedRoom
	case satAdd(uint64(cost), w.added) > s.shapeRoomExcluding(w.sh.injReservation.extra):
		return shaping.ReasonOverheadLimit
	}
	a := sh.acct
	a.accrue(usOf(s.now), 0)
	if uint64(cost) > a.left() {
		return shaping.ReasonInsufficientCoverCredit
	}
	return 0
}

func (w *recWriter) coverResult(ps *pipeShape, reason shaping.RuntimeReason, n, cost int) {
	w.b.cover = coverResult{shape: ps, id: ps.injID, reason: reason, n: n, cost: cost}
	ps.clearInjection()
	if n > 0 {
		w.coverStart, w.coverLen = w.recBytes, n
		w.added += uint64(cost)
	}
}

func (s *Session) cancelShaping() {
	for _, p := range s.pipes {
		if ps := p.sh; ps != nil && ps.sh != nil {
			ps.stop(false)
		}
	}
}

// shapePipeDown ends shaping of a pipe whose outbound direction failed.
func (s *Session) shapePipeDown(p *Pipe) {
	if ps := p.sh; ps != nil && ps.sh != nil {
		ps.event(shaping.EventPipeFailed)
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
