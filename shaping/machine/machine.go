package machine

import (
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping"
)

func micros(x uint64) time.Duration { return time.Duration(x) * time.Microsecond }

// compiledMachines are machines with their shared values drawn and pipe
// values given slots. They are read only.
type compiledMachines struct {
	o        *Options
	ms       []machineDef
	ncounter int
	ntimer   int
	queue    int // the most triggers pending in one call
	npipe    int32
	caps     shaping.Caps
}

func compileMachines(defs []machineDef, includeControl bool, src rand.Source, shared *[]uint64, o *Options) *compiledMachines {
	c := &compiledMachines{caps: shaping.CapPlan, o: o, ms: make([]machineDef, len(defs))}
	if includeControl {
		c.caps |= shaping.CapIncludeControl
	}
	fix := func(v *value) {
		if !v.set {
			return
		}
		*v = v.resolved(src, shared)
		if v.sc == scopePipe && v.segs != nil {
			v.slot = c.npipe
			c.npipe++
		}
	}
	for i := range defs {
		d := defs[i]
		d.states = slices.Clone(d.states)
		c.queue++
		for j := range d.states {
			st := &d.states[j]
			if st.datagram != nil {
				c.caps |= shaping.CapDatagramPlan
			}
			for _, plan := range []**record{&st.rec, &st.datagram} {
				if *plan != nil {
					r := **plan
					*plan = &r
					for _, v := range []*value{&r.target, &r.maxLen, &r.padTo, &r.padMul, &r.pad, &r.delay} {
						fix(v)
					}
				}
			}
			st.enter = slices.Clone(st.enter)
			c.queue++
			for k := range st.enter {
				a := &st.enter[k]
				fix(&a.a)
				fix(&a.b)
				if a.op == opSet || a.op == opAdd || a.op == opSub {
					c.queue++
				}
			}
			st.next = slices.Clone(st.next)
			for k := range st.next {
				t := &st.next[k]
				fix(&t.val)
				fix(&t.prob)
				switch t.on {
				case onOutbound:
					c.caps |= shaping.CapOutbound
				case onDatagramPlan:
					c.caps |= shaping.CapDatagramPlan
				case onStreamOpen, onStreamEnd:
					c.caps |= shaping.CapStreams
				case onRecordPlan, onInjectionAccepted, onInjectionRejected, onEnter, onTimer, onZero:
				}
				if t.cond && t.what >= whatWireLeft {
					c.caps |= shaping.CapBudget
				}
			}
		}
		c.ms[i] = d
		c.ncounter += d.counters
		c.ntimer += d.timers
	}
	return c
}

// Factory returns the zmux.ShaperFactory of the machines; nil without
// machines. Every pipe runs them as defined, whatever its ShapedPipe says.
func (c *Compiled) factory() zmux.ShaperFactory {
	if c.machines == nil {
		return nil
	}
	m := c.machines
	return func(zmux.ShapedPipe) shaping.Shaper { return m.newShaper() }
}

// Policy returns the plans and their shared direction budget for atomic installation.
func (c *Compiled) Policy() *zmux.ShapingPolicy {
	return &zmux.ShapingPolicy{Factory: c.factory(), Handshake: c.handshake(), Budget: c.budget()}
}

type mstate struct {
	ctr    []uint64
	tm     []int64 // deadlines; noTime: none
	gen    uint64
	rate   uint64
	burst  uint64
	cur    int32
	weight uint32
}

const noTime = int64(math.MinInt64)

type item struct {
	m    int32
	slot int32
	gen  uint64
	t    trigger
}

type due struct {
	at      int64
	m, slot int32
}

// shaper is the Shaper of one pipe.
type shaper struct {
	src                       rand.Source
	c                         *compiledMachines
	ms                        []mstate
	q                         []item
	dues                      []due
	pk                        []uint64
	pset                      []bool
	m                         shaping.Meters
	injection                 shaping.Injection
	now                       int64 // microseconds on the session clock
	n                         uint64
	seq                       uint64
	pauseUntil                int64
	requestSeq, lastInjection uint64
	lastWake                  int64
	lastPause                 int64
	lastRate, lastBurst       uint64
	injectionOwner            int32
	lastWeight                uint32
	stopped                   bool
	canInject                 bool
}

func (c *compiledMachines) newShaper() *shaper {
	s := &shaper{c: c, lastWake: noTime}
	s.src = c.o.source()
	s.ms = make([]mstate, len(c.ms))
	ctr := make([]uint64, c.ncounter)
	tm := make([]int64, c.ntimer)
	for i := range tm {
		tm[i] = noTime
	}
	for i := range c.ms {
		m := &s.ms[i]
		d := &c.ms[i]
		m.cur = int32(d.start)
		m.ctr, ctr = ctr[:d.counters:d.counters], ctr[d.counters:]
		m.tm, tm = tm[:d.timers:d.timers], tm[d.timers:]
	}
	s.q = make([]item, 0, c.queue)
	s.dues = make([]due, 0, c.ntimer)
	s.pk = make([]uint64, c.npipe)
	s.pset = make([]bool, c.npipe)
	return s
}

func (s *shaper) Capabilities() shaping.Caps { return s.c.caps }

func (s *shaper) val(v *value) uint64 {
	if v.segs == nil {
		return v.c
	}
	if v.sc == scopePipe {
		if !s.pset[v.slot] {
			s.pk[v.slot], s.pset[v.slot] = v.drawTable(s.src), true
		}
		return s.pk[v.slot]
	}
	return v.drawTable(s.src)
}

// Plan draws the record's fields and advances its planning entrance.
func (s *shaper) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	s.now, s.m = int64(v.Now/time.Microsecond), v.Meters
	for i := range s.ms {
		r := s.c.ms[i].states[s.ms[i].cur].rec
		if r == nil {
			continue
		}
		m := s.drawUnit(r, &p.UnitPlan)
		if m > 0 && (p.MaxLen == 0 || m < p.MaxLen) {
			p.MaxLen = m
		}
		p.Flush = p.Flush || r.flush
	}
	s.fire(onRecordPlan, 0, 0, -1)
	s.seq++
	p.Stop = s.stopped
	s.effects(&p.Effects)
}

func (s *shaper) PlanDatagram(v *shaping.DatagramView, p *shaping.DatagramPlan) {
	s.now, s.m = int64(v.Now/time.Microsecond), v.Meters
	for i := range s.ms {
		if r := s.c.ms[i].states[s.ms[i].cur].datagram; r != nil {
			s.drawUnit(r, &p.UnitPlan)
		}
	}
	s.fire(onDatagramPlan, 0, uint64(v.Len), -1)
	s.effects(&p.Effects)
	if s.stopped {
		p.Effects.Set |= shaping.EffectStop
	}
}

func (s *shaper) drawUnit(r *record, p *shaping.UnitPlan) (target int) {
	if r.target.set {
		target = int(s.val(&r.target))
		p.PadTo = max(p.PadTo, target)
	} else if r.maxLen.set {
		target = int(s.val(&r.maxLen))
	}
	if r.padTo.set {
		p.PadTo = max(p.PadTo, int(s.val(&r.padTo)))
	}
	if r.padMul.set {
		p.PadMultiple = max(p.PadMultiple, int(s.val(&r.padMul)))
	}
	if r.pad.set {
		p.Pad = min(p.Pad+int(s.val(&r.pad)), math.MaxInt32)
	}
	if r.delay.set {
		p.Delay = max(p.Delay, micros(s.val(&r.delay)))
	}
	return target
}

// OnEvent runs an event entrance.
func (s *shaper) OnEvent(e *shaping.Event, r *shaping.Effects) {
	s.now = int64(e.Now / time.Microsecond)
	s.m = e.Meters
	s.canInject = e.Kind == shaping.EventOutbound || e.Kind == shaping.EventTimer
	switch e.Kind {
	case shaping.EventStart:
		for i := range s.ms {
			s.enter(int32(i), int32(s.c.ms[i].start))
		}
		s.drain()
	case shaping.EventOutbound:
		s.fire(onOutbound, 0, uint64(e.Len), -1)
	case shaping.EventInjectionAccepted, shaping.EventInjectionRejected:
		if e.RequestID != 0 && e.RequestID == s.injection.ID {
			s.injection = shaping.Injection{}
			if e.Kind == shaping.EventInjectionAccepted {
				s.fire(onInjectionAccepted, 0, uint64(e.Len), -1)
			} else {
				s.fire(onInjectionRejected, 0, 0, s.injectionOwner)
			}
		}
	case shaping.EventTimer:
		s.wake()
	case shaping.EventStreamOpen:
		s.fire(onStreamOpen, 0, 0, -1)
	case shaping.EventStreamEnd:
		s.fire(onStreamEnd, 0, 0, -1)
	default:
		return
	}
	s.canInject = false
	s.effects(r)
	if s.stopped {
		r.Set |= shaping.EffectStop
	}
}

func (s *shaper) wake() {
	s.dues = s.dues[:0]
	for i := range s.ms {
		for j, at := range s.ms[i].tm {
			if at != noTime && at <= s.now {
				s.dues = append(s.dues, due{at, int32(i), int32(j)})
			}
		}
	}
	slices.SortFunc(s.dues, func(a, b due) int {
		switch {
		case a.at != b.at:
			if a.at < b.at {
				return -1
			}
			return 1
		case a.m != b.m:
			return int(a.m - b.m)
		}
		return int(a.slot - b.slot)
	})
	for _, d := range s.dues {
		if s.ms[d.m].tm[d.slot] != d.at {
			continue
		}
		s.ms[d.m].tm[d.slot] = noTime
		s.fire(onTimer, d.slot, 0, d.m)
	}
}

// fire offers a trigger to every machine, or to machine only.
func (s *shaper) fire(t trigger, slot int32, n uint64, only int32) {
	s.n = n
	if only >= 0 {
		s.q = append(s.q, item{only, slot, s.ms[only].gen, t})
	} else {
		for i := range s.ms {
			s.q = append(s.q, item{int32(i), slot, s.ms[i].gen, t})
		}
	}
	s.drain()
}

// drain runs the queue first in, first out; a trigger of an earlier entry of
// its machine's state is dropped.
func (s *shaper) drain() {
	for i := 0; i < len(s.q); i++ { //nolint:intrange // step appends to s.q; the bound is re-read
		g := s.q[i]
		if g.gen == s.ms[g.m].gen {
			s.step(g.m, g.t, g.slot)
		}
	}
	s.q = s.q[:0]
}

func (s *shaper) step(m int32, t trigger, slot int32) {
	st := &s.c.ms[m].states[s.ms[m].cur]
	for i := range st.next {
		tr := &st.next[i]
		if tr.on != t || tr.slot != slot {
			continue
		}
		if tr.cond && !s.cond(m, tr) {
			continue
		}
		if tr.prob.set && !probTrue(s.val(&tr.prob), s.src) {
			continue
		}
		s.enter(m, tr.to)
		return
	}
}

func (s *shaper) cond(m int32, tr *transition) bool {
	var x uint64
	switch tr.what {
	case whatCounter:
		x = s.ms[m].ctr[tr.cslot]
	case whatLen:
		x = s.n
	case whatSeq:
		x = s.seq
	case whatWireLeft:
		x = s.m.WireLeft
	case whatStallLeft:
		x = s.m.StallLeft
	case whatWireSpent:
		x = s.m.WireSpent
	case whatEarnedBytes:
		x = s.m.EarnedBytes
	case whatWirePPM:
		x = shaping.WirePPM(s.m.WireSpent, s.m.EarnedBytes)
	}
	y := s.val(&tr.val)
	switch tr.cmp {
	case cmpLT:
		return x < y
	case cmpLE:
		return x <= y
	case cmpEQ:
		return x == y
	case cmpNE:
		return x != y
	case cmpGE:
		return x >= y
	case cmpGT:
	}
	return x > y
}

func (s *shaper) enter(m, to int32) {
	ms := &s.ms[m]
	ms.cur = to
	ms.gen++
	st := &s.c.ms[m].states[to]
	for i := range st.enter {
		a := &st.enter[i]
		if a.op == opRestart {
			s.enter(m, int32(s.c.ms[m].start))
			return
		}
		s.act(m, a)
	}
	s.q = append(s.q, item{m, 0, ms.gen, onEnter})
}

func (s *shaper) act(m int32, a *action) {
	ms := &s.ms[m]
	switch a.op {
	case opInject:
		if a.cancel {
			if s.injectionOwner == m {
				s.injection = shaping.Injection{}
			}
		} else if s.canInject {
			s.requestSeq++
			after := s.val(&a.a)
			s.injection = shaping.Injection{ID: s.requestSeq, At: micros(uint64(s.now) + after), PadBytes: int(s.val(&a.b))}
			s.injectionOwner = m
		}
	case opPause:
		s.pauseUntil = max(s.pauseUntil, s.now+int64(s.val(&a.a)))
	case opRate:
		ms.rate = s.val(&a.a)
		ms.burst = 0
		if a.b.set {
			ms.burst = s.val(&a.b)
		}
	case opTimer:
		if a.cancel {
			ms.tm[a.slot] = noTime
			return
		}
		ms.tm[a.slot] = s.now + int64(s.val(&a.a))
	case opSet, opAdd, opSub:
		c := &ms.ctr[a.slot]
		was, x := *c, s.val(&a.a)
		switch a.op {
		case opSet:
			*c = x
		case opAdd:
			*c = was + x
			if *c < was {
				*c = math.MaxUint64
			}
		default:
			*c = was - min(was, x)
		}
		if *c == 0 && (was != 0 || a.op != opSet) {
			s.q = append(s.q, item{m, a.slot, ms.gen, onZero})
		}
	case opWeight:
		if w := s.val(&a.a); w != 0 {
			ms.weight = uint32(w)
		}
	case opStop:
		s.stopped = true
	case opRestart:
	}
}

// effects sets r to what differs from the last answer.
func (s *shaper) effects(r *shaping.Effects) {
	if s.injection.ID != s.lastInjection {
		s.lastInjection = s.injection.ID
		r.Set |= shaping.EffectInject
		r.Injection = s.injection
	}
	wake := noTime
	for i := range s.ms {
		for _, at := range s.ms[i].tm {
			if at != noTime && (wake == noTime || at < wake) {
				wake = at
			}
		}
	}
	if wake != s.lastWake {
		s.lastWake = wake
		r.Set |= shaping.EffectWake
		if wake != noTime {
			r.HasWake, r.Wake = true, micros(uint64(wake))
		}
	}
	var rate, burst uint64
	var weight uint32
	for i := range s.ms {
		m := &s.ms[i]
		if m.rate != 0 && (rate == 0 || m.rate < rate) {
			rate, burst = m.rate, m.burst
		}
		if m.weight != 0 && (weight == 0 || m.weight < weight) {
			weight = m.weight
		}
	}
	if s.pauseUntil != s.lastPause || rate != s.lastRate || burst != s.lastBurst {
		s.lastPause, s.lastRate, s.lastBurst = s.pauseUntil, rate, burst
		r.Set |= shaping.EffectGate
		r.PauseUntil, r.Rate, r.Burst = micros(uint64(s.pauseUntil)), rate, burst
	}
	if weight != s.lastWeight {
		s.lastWeight = weight
		r.Set |= shaping.EffectWeight
		r.Weight = weight
	}
}
