package machine

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseSequence reads the compact form of a single machine that runs its
// steps in order, one record each, and then stops shaping the pipe:
//
//	sequence := step ("," step)*
//	step     := dist ("/" modifier)*
//	modifier := "delay=" dist | "scope=" scope | "until=" guard | "inject=" count ":" dist
//	dist     := number | number "-" number | name "(" number ("," number)* ")"
//	scope    := "each" | "pipe" | "shared"
//	guard    := ("seq" | "len" | "counter0" | "counter1") ("<" | "<=" | "==" | "!=" | ">=" | ">") number
//
// The step's dist is the record's frames length in bytes: shorter records
// are padded to it, longer ones are cut at it. delay is in seconds. until
// ends the sequence after the record when the guard holds. inject waits for
// the next received record, then sends count records of PADDING only of
// the dist's length. name is a Dist kind with its Params.
func ParseSequence(s string) (*Definition, error) {
	steps := split(s, ',')
	m := Machine{}
	type pending struct{ state, next int }
	var links []pending // record_out transitions to the following step
	for i, raw := range steps {
		st, extra, err := parseStep(strings.TrimSpace(raw), len(m.States))
		if err != nil {
			return nil, fmt.Errorf("sequence: step %d: %w", i, err)
		}
		at := len(m.States)
		m.States = append(m.States, st)
		m.States = append(m.States, extra...)
		links = append(links, pending{at, len(m.States) - 1})
	}
	end := len(m.States)
	m.States = append(m.States, State{Enter: []Action{{Op: OpStop}}})
	for k, l := range links {
		next := end
		if k+1 < len(links) {
			next = links[k+1].state
		}
		last := &m.States[l.next]
		for j := range last.Next {
			if last.Next[j].To == -1 {
				last.Next[j].To = next
			}
		}
		for j := range m.States[l.state].Next {
			if m.States[l.state].Next[j].To == -2 {
				m.States[l.state].Next[j].To = end
			}
		}
	}
	d := &Definition{Version: Version, Machines: []Machine{m}}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return d, nil
}

// parseStep returns the step's state and the states its injection uses;
// To -1 stands for the following step, -2 for the end.
func parseStep(s string, base int) (State, []State, error) {
	parts := split(s, '/')
	l, err := parseDist(parts[0])
	if err != nil {
		return State{}, nil, err
	}
	st := State{Record: &Record{PadTo: l, MaxLen: l}}
	var until *Transition
	var count int
	var injLen *Value
	for _, p := range parts[1:] {
		k, v, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok {
			return State{}, nil, fmt.Errorf("modifier %q: want name=value", p)
		}
		switch k {
		case "delay":
			if st.Record.Delay, err = parseDist(v); err != nil {
				return State{}, nil, err
			}
		case "scope":
			sc := Scope(v)
			if sc != ScopeEach && sc != ScopePipe && sc != ScopeShared {
				return State{}, nil, fmt.Errorf("unknown scope %q", v)
			}
			l.Scope = sc
			if st.Record.Delay != nil {
				st.Record.Delay.Scope = sc
			}
		case "until":
			c, err := parseGuard(v)
			if err != nil {
				return State{}, nil, err
			}
			until = &Transition{On: OnRecordOut, To: -2, If: c}
		case "inject":
			n, ls, ok := strings.Cut(v, ":")
			if !ok {
				return State{}, nil, fmt.Errorf("inject %q: want count:dist", v)
			}
			if count, err = strconv.Atoi(n); err != nil || count < 0 {
				return State{}, nil, fmt.Errorf("inject count %q", n)
			}
			if injLen, err = parseDist(ls); err != nil {
				return State{}, nil, err
			}
		default:
			return State{}, nil, fmt.Errorf("unknown modifier %q", k)
		}
	}
	if until != nil {
		st.Next = append(st.Next, *until)
	}
	if count == 0 {
		st.Next = append(st.Next, Transition{On: OnRecordOut, To: -1})
		return st, nil, nil
	}
	w, a, b := base+1, base+2, base+3
	st.Next = append(st.Next, Transition{On: OnRecordOut, To: w})
	extra := []State{
		{Next: []Transition{{On: OnRecordIn, To: a}}},
		{Enter: []Action{{Op: OpSet, Slot: 1, Value: cv(float64(count))}}, Next: []Transition{{On: OnEnter, To: b}}},
		{Enter: []Action{{Op: OpInject, After: cv(0), Len: injLen}, {Op: OpSub, Slot: 1, Value: cv(1)}},
			Next: []Transition{{On: OnInjected, To: b, If: &Cond{What: QuantityCounter1, Cmp: CmpGT, Value: *cv(0)}},
				{On: OnInjected, To: -1}}},
	}
	return st, extra, nil
}

func cv(x float64) *Value { return &Value{Const: &x} }

func parseNum(s string) (float64, error) {
	x, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || !finite(x) {
		return 0, fmt.Errorf("number %q", s)
	}
	return x, nil
}

func parseDist(s string) (*Value, error) {
	s = strings.TrimSpace(s)
	if name, args, ok := strings.Cut(s, "("); ok {
		if !strings.HasSuffix(args, ")") {
			return nil, fmt.Errorf("dist %q: missing )", s)
		}
		d := &Dist{Kind: DistKind(strings.TrimSpace(name))}
		for _, a := range strings.Split(strings.TrimSuffix(args, ")"), ",") {
			x, err := parseNum(a)
			if err != nil {
				return nil, fmt.Errorf("dist %q: %w", s, err)
			}
			d.Params = append(d.Params, x)
		}
		v := &Value{Dist: d}
		return v, v.validate()
	}
	if a, b, ok := strings.Cut(s[min(1, len(s)):], "-"); ok {
		x, err := parseNum(s[:min(1, len(s))] + a)
		if err != nil {
			return nil, err
		}
		y, err := parseNum(b)
		if err != nil {
			return nil, err
		}
		v := &Value{Range: []float64{x, y}}
		return v, v.validate()
	}
	x, err := parseNum(s)
	if err != nil {
		return nil, err
	}
	return cv(x), nil
}

func parseGuard(s string) (*Cond, error) {
	for _, op := range []struct {
		sym string
		cmp Comparison
	}{{"<=", CmpLE}, {">=", CmpGE}, {"==", CmpEQ}, {"!=", CmpNE}, {"<", CmpLT}, {">", CmpGT}} {
		if w, v, ok := strings.Cut(s, op.sym); ok {
			x, err := parseNum(v)
			if err != nil {
				return nil, fmt.Errorf("guard %q: %w", s, err)
			}
			c := &Cond{What: Quantity(strings.TrimSpace(w)), Cmp: op.cmp, Value: Value{Const: &x}}
			switch c.What {
			case QuantitySeq, QuantityLen, QuantityCounter0, QuantityCounter1:
				return c, nil
			}
			return nil, fmt.Errorf("guard %q: unknown quantity %q", s, c.What)
		}
	}
	return nil, fmt.Errorf("guard %q: no comparison", s)
}

// split cuts s at sep outside parentheses.
func split(s string, sep byte) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case sep:
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}
