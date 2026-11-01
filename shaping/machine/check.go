package machine

import (
	"fmt"
	"math"
)

// SizeLimits bound the size of a Definition. Zero values: unbounded.
type SizeLimits struct {
	MaxBytes       int // JSON bytes (NewFactory, ParseSequence input bytes)
	MaxMachines    int
	MaxStates      int // per machine
	MaxActions     int // per state
	MaxTransitions int // per state
	MaxPoints      int // points or components per distribution
}

// Check validates d and bounds it by l.
func (d *Definition) Check(l SizeLimits) error {
	if err := d.Validate(); err != nil {
		return err
	}
	over := func(n, m int) bool { return m > 0 && n > m }
	if over(len(d.Machines), l.MaxMachines) {
		return fmt.Errorf("machine: %d machines, limit %d", len(d.Machines), l.MaxMachines)
	}
	for i := range d.Machines {
		m := &d.Machines[i]
		if over(len(m.States), l.MaxStates) {
			return fmt.Errorf("machine %d: %d states, limit %d", i, len(m.States), l.MaxStates)
		}
		for j := range m.States {
			st := &m.States[j]
			if over(len(st.Enter), l.MaxActions) || over(len(st.Next), l.MaxTransitions) {
				return fmt.Errorf("machine %d: state %d: actions or transitions over limit", i, j)
			}
			bad := false
			st.values(func(v *Value) {
				if d := v.Dist; d != nil && (over(len(d.Points), l.MaxPoints) || over(len(d.Of), l.MaxPoints)) {
					bad = true
				}
			})
			if bad {
				return fmt.Errorf("machine %d: state %d: distribution over limit", i, j)
			}
		}
	}
	return nil
}

// values calls f for every Value of st, nested ones included.
func (st *State) values(f func(*Value)) {
	var walk func(v *Value)
	walk = func(v *Value) {
		if v == nil {
			return
		}
		f(v)
		if d := v.Dist; d != nil {
			for i := range d.Of {
				walk(&d.Of[i])
			}
		}
	}
	if r := st.Record; r != nil {
		for _, v := range []*Value{r.MaxLen, r.PadTo, r.PadMultiple, r.Pad, r.Delay} {
			walk(v)
		}
	}
	for i := range st.Enter {
		a := &st.Enter[i]
		for _, v := range []*Value{a.After, a.Len, a.Rate, a.Burst, a.Value} {
			walk(v)
		}
	}
	for i := range st.Next {
		walk(st.Next[i].Prob)
		if c := st.Next[i].If; c != nil {
			walk(&c.Value)
		}
	}
}

// structure checks reachability and that no injection with a zero lower
// bound on its delay lies on a cycle of unconditional enter, injected or
// counter transitions.
func (m *Machine) structure() error {
	n := len(m.States)
	all := make([][]int, n)  // every edge
	fast := make([][]int, n) // edges taken without an outside trigger
	for i := range m.States {
		st := &m.States[i]
		for _, t := range st.Next {
			all[i] = append(all[i], t.To)
			switch t.On {
			case OnEnter, OnInjected, OnZero0, OnZero1:
				if t.If == nil {
					fast[i] = append(fast[i], t.To)
				}
			}
		}
		for _, a := range st.Enter {
			if a.Op == OpRestart {
				all[i] = append(all[i], m.Start)
				fast[i] = append(fast[i], m.Start)
			}
		}
	}
	seen := reach(all, m.Start)
	for i := range seen {
		if !seen[i] {
			return fmt.Errorf("state %d unreachable", i)
		}
	}
	for i := range m.States {
		for _, a := range m.States[i].Enter {
			if a.Op != OpInject || a.Cancel || a.After.lower() > 0 {
				continue
			}
			for _, j := range fast[i] {
				if reach(fast, j)[i] {
					return fmt.Errorf("state %d: injection with zero delay on a cycle of enter, injected or counter transitions", i)
				}
			}
		}
	}
	return nil
}

func reach(g [][]int, from int) []bool {
	seen := make([]bool, len(g))
	stack := []int{from}
	seen[from] = true
	for len(stack) > 0 {
		i := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, j := range g[i] {
			if !seen[j] {
				seen[j] = true
				stack = append(stack, j)
			}
		}
	}
	return seen
}

// family is the range of the numbers a Value states (its const, the ends
// of its range, its min and max, the points of an empirical or bag
// distribution, the parameters of a uniform or loguniform one, and those
// of the components of a mixture), with the rule for its bounds.
type family struct {
	lo, hi float64
	any    bool // any finite number
	rate   bool // no draw in (0, 1)
	prob   bool // upper bound at most 1
}

var (
	famLength      = family{lo: 0, hi: math.MaxInt32}
	famSeconds     = family{lo: 0, hi: maxSeconds}
	famRate        = family{lo: 0, hi: maxCount, rate: true}
	famCounter     = family{lo: 0, hi: maxCount}
	famProb        = family{lo: 0, hi: 1, prob: true}
	famAny         = family{any: true}
	famPoolCount   = family{lo: 1, hi: maxCount}
	famPoolSeconds = family{lo: 0, hi: maxSeconds}
	famPoolProbe   = family{lo: 0, hi: math.Inf(1)}
	famRuleSeconds = family{lo: 0, hi: maxSeconds}
	famRuleCount   = family{lo: 0, hi: maxCount}
)

// maxSeconds is the largest time stated, 1e9 seconds.
const maxSeconds = 1e9

// checkIn validates v, named name in errors, in the family f: the numbers
// it states lie in the family's range and its lower bound is not below
// the family's least value; nil passes.
func (v *Value) checkIn(name string, f family) error {
	if v == nil {
		return nil
	}
	if err := v.validate(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if f.any {
		return nil
	}
	var err error
	v.stated(func(x float64) {
		switch {
		case err != nil:
		case x < f.lo && f.lo == 0:
			err = fmt.Errorf("%s: value %v negative", name, x)
		case x < f.lo:
			err = fmt.Errorf("%s: value %v below %v", name, x, f.lo)
		case x > f.hi:
			err = fmt.Errorf("%s: value %v above the maximum %v", name, x, f.hi)
		}
	})
	if err != nil {
		return err
	}
	if lo := v.lower(); lo < f.lo {
		if f.lo == 0 {
			return fmt.Errorf("%s: may be negative; a distribution that may draw below 0 needs a min of 0 or more", name)
		}
		return fmt.Errorf("%s: may be below %v; a distribution that may draw below it needs a min of %v or more", name, f.lo, f.lo)
	}
	if f.rate && !v.rateOK() {
		return fmt.Errorf("%s: may draw between 0 and 1; a rate is 0 or at least 1", name)
	}
	if f.prob && v.upper() > 1 {
		return fmt.Errorf("%s: may be above 1; a distribution that may draw above 1 needs a max of 1 or less", name)
	}
	return nil
}

// stated calls f with every number v states.
func (v *Value) stated(f func(float64)) {
	for _, p := range []*float64{v.Const, v.Min, v.Max} {
		if p != nil {
			f(*p)
		}
	}
	for _, x := range v.Range {
		f(x)
	}
	if d := v.Dist; d != nil {
		if d.Kind == DistUniform || d.Kind == DistLogUniform {
			for _, x := range d.Params {
				f(x)
			}
		}
		for _, x := range d.Points {
			f(x)
		}
		for i := range d.Of {
			d.Of[i].stated(f)
		}
	}
}

// rateOK reports that v draws no rate in (0, 1).
func (v *Value) rateOK() bool {
	if v.Const != nil {
		x := v.lower()
		return x == 0 || x >= 1
	}
	if d := v.Dist; d != nil && d.Kind == DistMixture && v.Min == nil && v.Max == nil {
		for i := range d.Of {
			if !d.Of[i].rateOK() {
				return false
			}
		}
		return true
	}
	return v.lower() >= 1
}

// lower is a lower bound of the values v takes. Points and components of
// weight 0 count.
func (v *Value) lower() float64 {
	var x float64
	switch {
	case v.Const != nil:
		x = *v.Const
	case v.Range != nil:
		x = v.Range[0]
	default:
		x = v.Dist.lower()
	}
	return v.bound(x)
}

// upper is an upper bound of the values v takes. Points and components of
// weight 0 count.
func (v *Value) upper() float64 {
	var x float64
	switch {
	case v.Const != nil:
		x = *v.Const
	case v.Range != nil:
		x = v.Range[1]
	default:
		x = v.Dist.upper()
	}
	return v.bound(x)
}

func (v *Value) bound(x float64) float64 {
	if v.Min != nil {
		x = math.Max(x, *v.Min)
	}
	if v.Max != nil {
		x = math.Min(x, *v.Max)
	}
	return x
}

func (d *Dist) lower() float64 {
	switch d.Kind {
	case DistUniform, DistLogUniform, DistPareto:
		return d.Params[0]
	case DistNormal:
		return math.Inf(-1)
	case DistGeometric:
		return 1
	case DistEmpirical, DistBag:
		x := math.Inf(1)
		for _, p := range d.Points {
			x = math.Min(x, p)
		}
		return x
	case DistMixture:
		x := math.Inf(1)
		for i := range d.Of {
			x = math.Min(x, d.Of[i].lower())
		}
		return x
	}
	return 0
}

func (d *Dist) upper() float64 {
	switch d.Kind {
	case DistUniform, DistLogUniform:
		return d.Params[1]
	case DistBeta:
		return 1
	case DistBinomial:
		return d.Params[0]
	case DistGeometric:
		if d.Params[0] == 1 {
			return 1
		}
	case DistEmpirical, DistBag:
		x := math.Inf(-1)
		for _, p := range d.Points {
			x = math.Max(x, p)
		}
		return x
	case DistMixture:
		x := math.Inf(-1)
		for i := range d.Of {
			x = math.Max(x, d.Of[i].upper())
		}
		return x
	}
	return math.Inf(1)
}
