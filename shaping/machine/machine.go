package machine

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping"
)

// Options configure a Factory. The zero value is valid.
type Options struct {
	// NewSource returns the random source of one pipe; nil: a ChaCha8
	// generator seeded from crypto/rand.
	NewSource func() rand.Source
	// Limits bound the Definition.
	Limits SizeLimits
}

func (o *Options) source() rand.Source {
	if o.NewSource == nil {
		return nil
	}
	return o.NewSource()
}

// Factory returns a zmux.ShaperFactory running d. d is validated and must
// not be modified afterwards. Every pipe runs d as defined, whatever its
// ShapedPipe says, except for its WriteOverhead: the max_len and pad_to
// lengths of d are those of a carrier write below the pipe, so that on
// the first record of each write (shaping.RecordView.WriteStart) the
// pipe's WriteOverhead is taken off them; a max_len that falls below
// shaping.MinMaxLen is raised to it (and counted as clamped). pad_multiple
// (a block size, not a length) and inject lengths (whose write is not
// known when planned) are not changed.
func (d *Definition) Factory(o Options) (zmux.ShaperFactory, error) {
	if err := d.Check(o.Limits); err != nil {
		return nil, err
	}
	f := &factory{d: d, o: o, idx: map[*Value]int{}}
	f.caps = shaping.CapPlan
	for i := range d.Machines {
		f.bound += 4 * (len(d.Machines[i].States) + 1)
		for j := range d.Machines[i].States {
			st := &d.Machines[i].States[j]
			f.index(st)
			for _, t := range st.Next {
				switch t.On {
				case OnRecordIn, OnPaddingIn:
					f.caps |= shaping.CapRecordIn
				case OnStreamOpen, OnStreamEnd:
					f.caps |= shaping.CapStreams
				}
			}
		}
	}
	// Factory-scoped values are drawn once, here.
	sh := f.newShaper()
	f.kept = make([]float64, len(f.idx))
	f.has = make([]bool, len(f.idx))
	for v, i := range f.idx {
		if v.Scope == ScopeShared {
			f.kept[i], f.has[i] = sh.draw(v), true
		}
	}
	return func(p zmux.ShapedPipe) shaping.Shaper {
		s := f.newShaper()
		s.over = max(p.WriteOverhead, 0)
		return s
	}, nil
}

// NewFactory parses a JSON Definition and returns its Factory.
func NewFactory(b []byte, o Options) (zmux.ShaperFactory, error) {
	if m := o.Limits.MaxBytes; m > 0 && len(b) > m {
		return nil, fmt.Errorf("machine: %d bytes, limit %d", len(b), m)
	}
	d, err := Parse(b)
	if err != nil {
		return nil, err
	}
	return d.Factory(o)
}

type factory struct {
	d     *Definition
	o     Options
	idx   map[*Value]int
	kept  []float64 // factory-scoped values
	has   []bool
	bound int // triggers processed per event
	caps  shaping.Caps
}

func (f *factory) value(v *Value) {
	if v == nil {
		return
	}
	if _, ok := f.idx[v]; !ok {
		f.idx[v] = len(f.idx)
	}
	if d := v.Dist; d != nil {
		for i := range d.Of {
			f.value(&d.Of[i])
		}
	}
}

func (f *factory) index(st *State) {
	if r := st.Record; r != nil {
		for _, v := range []*Value{r.MaxLen, r.PadTo, r.PadMultiple, r.Pad, r.Delay} {
			f.value(v)
		}
	}
	for i := range st.Enter {
		a := &st.Enter[i]
		for _, v := range []*Value{a.After, a.Len, a.Rate, a.Burst, a.Value} {
			f.value(v)
		}
	}
	for i := range st.Next {
		t := &st.Next[i]
		f.value(t.Prob)
		if t.If != nil {
			f.value(&t.If.Value)
		}
	}
}

func (f *factory) newShaper() *shaper {
	s := &shaper{f: f, ms: make([]mstate, len(f.d.Machines))}
	s.drawer = drawer{r: newRand(f.o.source()), keeper: s}
	s.pkept = make([]float64, len(f.idx))
	s.phas = make([]bool, len(f.idx))
	return s
}

type mstate struct {
	cur int
	ctr [2]float64
}

// shaper is the Shaper of one pipe.
type shaper struct {
	f *factory
	drawer
	ms    []mstate
	pkept []float64 // pipe-scoped values
	phas  []bool
	seq   uint64
	over  int            // ShapedPipe.WriteOverhead
	rep   *shaping.Reply // the reply being built
	out   struct {       // what rep points to
		inj    shaping.Inject
		gate   shaping.Gate
		timer  [2]shaping.Timer
		weight float64
	}
	n int    // Len of the triggering event
	q []trig // pending triggers
}

type trig struct {
	m int
	t Trigger
}

func (s *shaper) Capabilities() shaping.Caps { return s.f.caps }

func (s *shaper) Plan(v *shaping.RecordView, p *shaping.RecordPlan) {
	for i := range s.ms {
		r := s.f.d.Machines[i].States[s.ms[i].cur].Record
		if r == nil {
			continue
		}
		m := 0
		if r.MaxLen != nil {
			if m = s.length(r.MaxLen); p.MaxLen == 0 || m > 0 && m < p.MaxLen {
				p.MaxLen = m
			}
		}
		if r.PadTo == r.MaxLen && r.PadTo != nil {
			p.PadTo = max(p.PadTo, m) // one draw for both fields
		} else if r.PadTo != nil {
			p.PadTo = max(p.PadTo, s.length(r.PadTo))
		}
		if r.PadMultiple != nil {
			p.PadMultiple = max(p.PadMultiple, s.length(r.PadMultiple))
		}
		if r.Pad != nil {
			p.Pad += s.length(r.Pad)
		}
		if r.Delay != nil {
			p.Delay = max(p.Delay, s.dur(r.Delay))
		}
		p.Flush = p.Flush || r.Flush
	}
	if o := s.over; o > 0 && v.WriteStart {
		if p.MaxLen > 0 {
			p.MaxLen = max(p.MaxLen-o, 1) // below MinMaxLen: raised and counted by the session
		}
		p.PadTo = max(p.PadTo-o, 0)
	}
	s.rep = &p.Reply
	s.fire(OnRecordOut, 0)
	s.seq++
	p.Stop, p.Reply.Stop = p.Reply.Stop, false // the stop follows the record
	s.rep = nil
}

func (s *shaper) OnEvent(e *shaping.Event, r *shaping.Reply) {
	s.rep = r
	defer func() { s.rep = nil }()
	switch e.Kind {
	case shaping.EventStart:
		for i := range s.ms {
			s.enter(i, s.f.d.Machines[i].Start)
		}
		s.drain()
	case shaping.EventRecordIn:
		if e.PaddingOnly {
			s.fire(OnPaddingIn, e.Len)
		} else {
			s.fire(OnRecordIn, e.Len)
		}
	case shaping.EventInjected:
		s.fire(OnInjected, e.Len)
	case shaping.EventTimer:
		s.fire([2]Trigger{OnTimer0, OnTimer1}[e.Timer&1], 0)
	case shaping.EventStreamOpen:
		s.fire(OnStreamOpen, 0)
	case shaping.EventStreamEnd:
		s.fire(OnStreamEnd, 0)
	case shaping.EventReplaced, shaping.EventStopped, shaping.EventPipeFailed:
		// The instance ends; no machine reacts.
	}
}

// fire offers t to every machine.
func (s *shaper) fire(t Trigger, n int) {
	s.n = n
	for i := range s.ms {
		s.q = append(s.q, trig{i, t})
	}
	s.drain()
}

// drain processes pending triggers, at most f.bound per call: a cycle of
// moves on enter or counter triggers ends there.
func (s *shaper) drain() {
	for steps := 0; len(s.q) > 0 && steps < s.f.bound; steps++ {
		g := s.q[0]
		s.q = s.q[1:]
		s.step(g.m, g.t)
	}
	s.q = s.q[:0]
}

func (s *shaper) step(m int, t Trigger) {
	st := &s.f.d.Machines[m].States[s.ms[m].cur]
	for i := range st.Next {
		tr := &st.Next[i]
		if tr.On != t || tr.If != nil && !s.cond(m, tr.If) {
			continue
		}
		if tr.Prob != nil && s.r.Float64() >= s.sample(tr.Prob) {
			continue
		}
		s.enter(m, tr.To)
		return
	}
}

func (s *shaper) enter(m, to int) {
	s.ms[m].cur = to
	st := &s.f.d.Machines[m].States[to]
	for i := range st.Enter {
		s.act(m, &st.Enter[i])
	}
	s.q = append(s.q, trig{m, OnEnter})
}

func (s *shaper) act(m int, a *Action) {
	r := s.rep
	switch a.Op {
	case OpInject:
		s.out.inj = shaping.Inject{Cancel: true}
		if !a.Cancel {
			s.out.inj = shaping.Inject{After: s.dur(a.After), Len: s.length(a.Len)}
		}
		r.Inject = &s.out.inj
	case OpPause:
		s.out.gate = shaping.Gate{Pause: s.dur(a.After)}
		r.Gate = &s.out.gate
	case OpRate:
		// Draws are rounded and bounded as stated values are: a rate by
		// 2^53, a burst by 2^31-1, the same on every platform.
		s.out.gate = shaping.Gate{Rate: int64(min(math.Round(s.sample(a.Rate)), maxCount))}
		if a.Burst != nil {
			s.out.gate.Burst = int64(s.length(a.Burst))
		}
		r.Gate = &s.out.gate
	case OpTimer:
		s.out.timer[a.Slot] = shaping.Timer{Cancel: true}
		if !a.Cancel {
			s.out.timer[a.Slot] = shaping.Timer{After: s.dur(a.After)}
		}
		r.Timer[a.Slot] = &s.out.timer[a.Slot]
	case OpSet, OpAdd, OpSub:
		c := &s.ms[m].ctr[a.Slot]
		was, x := *c, s.sample(a.Value)
		switch a.Op {
		case OpSet:
			*c = x
		case OpAdd:
			*c += x
		case OpSub:
			*c = math.Max(*c-x, 0)
		}
		if *c == 0 && (was != 0 || a.Op != OpSet) {
			s.q = append(s.q, trig{m, [2]Trigger{OnZero0, OnZero1}[a.Slot]})
		}
	case OpWeight:
		s.out.weight = s.sample(a.Value)
		r.Weight = &s.out.weight
	case OpStop:
		r.Stop = true
	case OpRestart:
		s.enter(m, s.f.d.Machines[m].Start)
	}
}

func (s *shaper) cond(m int, c *Cond) bool {
	var x float64
	switch c.What {
	case QuantityCounter0:
		x = s.ms[m].ctr[0]
	case QuantityCounter1:
		x = s.ms[m].ctr[1]
	case QuantityLen:
		x = float64(s.n)
	case QuantitySeq:
		x = float64(s.seq)
	}
	y := s.sample(&c.Value)
	switch c.Cmp {
	case CmpLT:
		return x < y
	case CmpLE:
		return x <= y
	case CmpEQ:
		return x == y
	case CmpNE:
		return x != y
	case CmpGE:
		return x >= y
	}
	return x > y
}

func (s *shaper) length(v *Value) int {
	return int(min(math.Round(s.sample(v)), math.MaxInt32))
}

func (s *shaper) dur(v *Value) time.Duration { return seconds(s.sample(v)) }

func seconds(x float64) time.Duration {
	return time.Duration(math.Round(min(x*1e9, math.MaxInt64/2)))
}

// kept returns the kept value of v for its scope.
func (s *shaper) kept(v *Value) (float64, bool) {
	i := s.f.idx[v]
	switch {
	case v.Scope == ScopeShared && s.f.has != nil && s.f.has[i]:
		return s.f.kept[i], true
	case v.Scope == ScopePipe && s.phas[i]:
		return s.pkept[i], true
	}
	return 0, false
}

// keep keeps x as the value of v if its scope is pipe.
func (s *shaper) keep(v *Value, x float64) {
	if v.Scope == ScopePipe {
		i := s.f.idx[v]
		s.pkept[i], s.phas[i] = x, true
	}
}
