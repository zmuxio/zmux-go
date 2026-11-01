// Package machine implements a shaping.Shaper driven by state machines
// read from a Definition.
package machine

import (
	"errors"
	"fmt"
	"io"
	"math"
)

// Version is the Definition version this package reads.
const Version = 1

// Definition is a set of machines that run side by side on each pipe.
type Definition struct {
	Version  int       `json:"version"` // must be Version
	Machines []Machine `json:"machines"`
}

// Machine is a state machine with two counters.
type Machine struct {
	Start  int     `json:"start"` // index into States
	States []State `json:"states"`
}

// State is a machine state.
type State struct {
	// Record applies to each record planned while the machine is in the
	// state; nil: none.
	Record *Record `json:"record,omitempty"`
	// Enter runs in order when the state is entered.
	Enter []Action `json:"enter,omitempty"`
	// Next is tried in order on each trigger; the first match moves.
	Next []Transition `json:"next,omitempty"`
}

// Record holds record decisions; nil values: none. Lengths in bytes,
// times in seconds.
type Record struct {
	MaxLen      *Value `json:"max_len,omitempty"`
	PadTo       *Value `json:"pad_to,omitempty"`
	PadMultiple *Value `json:"pad_multiple,omitempty"`
	Pad         *Value `json:"pad,omitempty"`
	Delay       *Value `json:"delay,omitempty"`
	Flush       bool   `json:"flush,omitempty"`
}

// Op names an Action.
type Op string

const (
	OpInject  Op = "inject"  // Len bytes after After seconds; Cancel: cancels the pending one
	OpPause   Op = "pause"   // stream data waits After seconds
	OpRate    Op = "rate"    // Rate bytes per second, Burst bytes; Rate 0: no limit
	OpTimer   Op = "timer"   // timer Slot fires after After seconds; Cancel: disarms it
	OpSet     Op = "set"     // counter Slot = Value
	OpAdd     Op = "add"     // counter Slot += Value
	OpSub     Op = "sub"     // counter Slot -= Value, not below 0
	OpWeight  Op = "weight"  // pipe weight = Value
	OpStop    Op = "stop"    // shaping of the pipe ends
	OpRestart Op = "restart" // the machine re-enters its start state
)

// Action is an operation. Values Op does not use are ignored at run time
// but validated when set; a JSON Definition may not set them.
type Action struct {
	Op     Op     `json:"op"`
	Cancel bool   `json:"cancel,omitempty"` // OpInject, OpTimer
	Slot   int    `json:"slot,omitempty"`   // 0 or 1
	After  *Value `json:"after,omitempty"`
	Len    *Value `json:"len,omitempty"`
	Rate   *Value `json:"rate,omitempty"`
	Burst  *Value `json:"burst,omitempty"`
	Value  *Value `json:"value,omitempty"`
}

// Trigger names what a Transition reacts to.
type Trigger string

const (
	OnRecordOut  Trigger = "record_out" // a record was planned
	OnRecordIn   Trigger = "record_in"  // a record arrived
	OnPaddingIn  Trigger = "padding_in" // a record of PADDING only arrived
	OnInjected   Trigger = "injected"   // an injection was sent
	OnTimer0     Trigger = "timer0"
	OnTimer1     Trigger = "timer1"
	OnZero0      Trigger = "zero0" // counter 0 reached 0
	OnZero1      Trigger = "zero1" // counter 1 reached 0
	OnEnter      Trigger = "enter" // tried once after the state is entered
	OnStreamOpen Trigger = "stream_open"
	OnStreamEnd  Trigger = "stream_end"
)

// Transition moves the machine to To when On happens, If holds and a draw
// falls below Prob.
type Transition struct {
	On   Trigger `json:"on"`
	To   int     `json:"to"`
	Prob *Value  `json:"prob,omitempty"` // in [0,1]; nil: 1
	If   *Cond   `json:"if,omitempty"`
}

// Cond compares a quantity with a value.
type Cond struct {
	What  Quantity   `json:"what"`
	Cmp   Comparison `json:"cmp"`
	Value Value      `json:"value"`
}

// Quantity names what a Cond compares.
type Quantity string

const (
	QuantityCounter0 Quantity = "counter0" // counter 0 of the machine
	QuantityCounter1 Quantity = "counter1" // counter 1 of the machine
	QuantityLen      Quantity = "len"      // bytes of the triggering record
	QuantitySeq      Quantity = "seq"      // records planned on the pipe
)

// Comparison names how a Cond compares: quantity Cmp value.
type Comparison string

const (
	CmpLT Comparison = "lt"
	CmpLE Comparison = "le"
	CmpEQ Comparison = "eq"
	CmpNE Comparison = "ne"
	CmpGE Comparison = "ge"
	CmpGT Comparison = "gt"
)

// Scope sets how long a sampled value is kept.
type Scope string

const (
	ScopeEach   Scope = "each"   // sampled at each use; also the empty Scope
	ScopePipe   Scope = "pipe"   // once per pipe
	ScopeShared Scope = "shared" // once per Factory value, shared by its pipes
)

// Value is a number: Const, a uniform Range, or a Dist; then bounded by
// Min and Max when set.
type Value struct {
	Const *float64  `json:"const,omitempty"`
	Range []float64 `json:"range,omitempty"` // [a, b]
	Dist  *Dist     `json:"dist,omitempty"`
	Scope Scope     `json:"scope,omitempty"`
	Min   *float64  `json:"min,omitempty"`
	Max   *float64  `json:"max,omitempty"`
}

// Dist is a distribution. Params by Kind:
//
//	uniform a b | loguniform a b (0<a<=b) | exponential rate |
//	normal mean sd | lognormal mu sigma | weibull scale shape |
//	pareto scale shape | gamma shape scale | beta a b | geometric p |
//	binomial n p (n integral, at most 2^53) | poisson mean (at most 2^53) |
//	empirical: Points with Weights | bag: Points, drawn without
//	replacement and refilled when empty | mixture: Of with Weights.
type Dist struct {
	Kind    DistKind  `json:"kind"`
	Params  []float64 `json:"params,omitempty"`
	Points  []float64 `json:"points,omitempty"`
	Weights []float64 `json:"weights,omitempty"`
	Of      []Value   `json:"of,omitempty"`
}

// DistKind names a distribution.
type DistKind string

const (
	DistUniform     DistKind = "uniform"
	DistLogUniform  DistKind = "loguniform"
	DistExponential DistKind = "exponential"
	DistNormal      DistKind = "normal"
	DistLogNormal   DistKind = "lognormal"
	DistWeibull     DistKind = "weibull"
	DistPareto      DistKind = "pareto"
	DistGamma       DistKind = "gamma"
	DistBeta        DistKind = "beta"
	DistGeometric   DistKind = "geometric"
	DistBinomial    DistKind = "binomial"
	DistPoisson     DistKind = "poisson"
	DistEmpirical   DistKind = "empirical"
	DistBag         DistKind = "bag"
	DistMixture     DistKind = "mixture"
)

var nParams = map[DistKind]int{DistUniform: 2, DistLogUniform: 2, DistExponential: 1, DistNormal: 2,
	DistLogNormal: 2, DistWeibull: 2, DistPareto: 2, DistGamma: 2, DistBeta: 2, DistGeometric: 1,
	DistBinomial: 2, DistPoisson: 1, DistEmpirical: 0, DistBag: 0, DistMixture: 0}

// Parse reads a JSON Definition and validates it. Parsing is strict:
// member names match exactly, an object holds a name once, unknown
// members and members the op or kind does not use are errors, null is an
// absent member, version, start, to and slot are JSON integers, and every
// number fits in a float64.
func Parse(b []byte) (*Definition, error) {
	var d Definition
	if err := decodeChecked(b, definitionShape, &d); err != nil {
		return nil, fmt.Errorf("machine: %w", err)
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// Load reads a JSON Definition from r and validates it.
func Load(r io.Reader) (*Definition, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("machine: %w", err)
	}
	return Parse(b)
}

// Validate reports the first error of d.
func (d *Definition) Validate() error {
	if d.Version != Version {
		return fmt.Errorf("machine: version %d, want %d", d.Version, Version)
	}
	if len(d.Machines) == 0 {
		return errors.New("machine: no machines")
	}
	for i := range d.Machines {
		if err := d.Machines[i].validate(); err != nil {
			return fmt.Errorf("machine %d: %w", i, err)
		}
	}
	return nil
}

func (m *Machine) validate() error {
	n := len(m.States)
	if m.Start < 0 || m.Start >= n {
		return fmt.Errorf("start %d out of range", m.Start)
	}
	for i := range m.States {
		st := &m.States[i]
		if err := st.validate(n); err != nil {
			return fmt.Errorf("state %d: %w", i, err)
		}
	}
	return m.structure()
}

func (st *State) validate(n int) error {
	if r := st.Record; r != nil {
		for _, c := range []struct {
			name string
			v    *Value
			f    family
		}{{"max_len", r.MaxLen, famLength}, {"pad_to", r.PadTo, famLength},
			{"pad_multiple", r.PadMultiple, famLength}, {"pad", r.Pad, famLength}, {"delay", r.Delay, famSeconds}} {
			if err := c.v.checkIn(c.name, c.f); err != nil {
				return err
			}
		}
	}
	for j, a := range st.Enter {
		if err := a.validate(); err != nil {
			return fmt.Errorf("action %d: %w", j, err)
		}
	}
	for j, t := range st.Next {
		if err := t.validate(n); err != nil {
			return fmt.Errorf("transition %d: %w", j, err)
		}
	}
	return nil
}

// validate checks the values a uses; a value present but not used by its
// op, or by Cancel, is checked as well.
func (a *Action) validate() error {
	need := func(v *Value, name string) error {
		if v == nil {
			return fmt.Errorf("%s: missing %s", a.Op, name)
		}
		return nil
	}
	if a.Slot != 0 && a.Slot != 1 {
		return fmt.Errorf("slot %d", a.Slot)
	}
	var err error
	switch a.Op {
	case OpInject:
		if !a.Cancel {
			err = errors.Join(need(a.After, "after"), need(a.Len, "len"))
		}
	case OpPause:
		err = need(a.After, "after")
	case OpTimer:
		if !a.Cancel {
			err = need(a.After, "after")
		}
	case OpRate:
		err = need(a.Rate, "rate")
	case OpSet, OpAdd, OpSub, OpWeight:
		err = need(a.Value, "value")
	case OpStop, OpRestart:
	default:
		return fmt.Errorf("unknown op %q", a.Op)
	}
	if err != nil {
		return err
	}
	value := famCounter
	if a.Op == OpWeight {
		value = famAny // a weight may be negative
	}
	return errors.Join(a.After.checkIn("after", famSeconds), a.Len.checkIn("len", famLength),
		a.Rate.checkIn("rate", famRate), a.Burst.checkIn("burst", famLength), a.Value.checkIn("value", value))
}

func (t *Transition) validate(n int) error {
	switch t.On {
	case OnRecordOut, OnRecordIn, OnPaddingIn, OnInjected, OnTimer0, OnTimer1, OnZero0, OnZero1, OnEnter,
		OnStreamOpen, OnStreamEnd:
	default:
		return fmt.Errorf("unknown trigger %q", t.On)
	}
	if t.To < 0 || t.To >= n {
		return fmt.Errorf("target %d out of range", t.To)
	}
	if err := t.Prob.checkIn("prob", famProb); err != nil {
		return err
	}
	if c := t.If; c != nil {
		switch c.What {
		case QuantityCounter0, QuantityCounter1, QuantityLen, QuantitySeq:
		default:
			return fmt.Errorf("unknown quantity %q", c.What)
		}
		switch c.Cmp {
		case CmpLT, CmpLE, CmpEQ, CmpNE, CmpGE, CmpGT:
		default:
			return fmt.Errorf("unknown comparison %q", c.Cmp)
		}
		return c.Value.checkIn("if", famAny)
	}
	return nil
}

func (v *Value) validate() error {
	if v == nil {
		return nil
	}
	k := 0
	if v.Const != nil {
		k++
		if !finite(*v.Const) {
			return errors.New("value: not finite")
		}
	}
	if v.Range != nil {
		k++
		if len(v.Range) != 2 || !finite(v.Range[0]) || !finite(v.Range[1]) || v.Range[0] > v.Range[1] {
			return errors.New("value: range needs two finite numbers a <= b")
		}
	}
	if v.Dist != nil {
		k++
		if err := v.Dist.validate(); err != nil {
			return err
		}
	}
	if k != 1 {
		return errors.New("value: exactly one of const, range, dist")
	}
	switch v.Scope {
	case "", ScopeEach, ScopePipe, ScopeShared:
	default:
		return fmt.Errorf("value: unknown scope %q", v.Scope)
	}
	if v.Min != nil && !finite(*v.Min) || v.Max != nil && !finite(*v.Max) {
		return errors.New("value: min or max not finite")
	}
	if v.Min != nil && v.Max != nil && *v.Min > *v.Max {
		return errors.New("value: min > max")
	}
	return nil
}

func (d *Dist) validate() error {
	want, ok := nParams[d.Kind]
	if !ok {
		return fmt.Errorf("dist: unknown kind %q", d.Kind)
	}
	if len(d.Params) != want {
		return fmt.Errorf("dist %s: %d params, want %d", d.Kind, len(d.Params), want)
	}
	for _, p := range d.Params {
		if !finite(p) {
			return fmt.Errorf("dist %s: param not finite", d.Kind)
		}
	}
	p := d.Params
	bad := false
	switch d.Kind {
	case DistUniform:
		bad = p[0] > p[1]
	case DistLogUniform:
		bad = p[0] <= 0 || p[0] > p[1]
	case DistExponential:
		bad = p[0] <= 0
	case DistPoisson:
		bad = p[0] <= 0 || p[0] > maxCount
	case DistNormal, DistLogNormal:
		bad = p[1] < 0
	case DistWeibull, DistPareto, DistGamma, DistBeta:
		bad = p[0] <= 0 || p[1] <= 0
	case DistGeometric:
		bad = p[0] <= 0 || p[0] > 1
	case DistBinomial:
		bad = p[0] < 0 || p[0] > maxCount || p[0] != math.Trunc(p[0]) || p[1] < 0 || p[1] > 1
	case DistEmpirical:
		bad = len(d.Points) == 0 || len(d.Weights) != len(d.Points) || !weights(d.Weights)
	case DistBag:
		bad = len(d.Points) == 0
	case DistMixture:
		bad = len(d.Of) == 0 || len(d.Weights) != len(d.Of) || !weights(d.Weights)
		for i := range d.Of {
			if err := d.Of[i].validate(); err != nil {
				return err
			}
		}
	}
	if bad {
		return fmt.Errorf("dist %s: invalid parameters", d.Kind)
	}
	for _, x := range d.Points {
		if !finite(x) {
			return fmt.Errorf("dist %s: point not finite", d.Kind)
		}
	}
	return nil
}

func weights(w []float64) bool {
	s := 0.0
	for _, x := range w {
		if !finite(x) || x < 0 {
			return false
		}
		s += x
	}
	return s > 0
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
