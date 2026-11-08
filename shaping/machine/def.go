// Package machine runs shaping candidates: handshake plans by name, records by
// state machines, and a pool policy, all on one integer value form.
package machine

type trigger uint8

const (
	onRecordOut trigger = iota
	onRecordIn
	onPaddingIn
	onInjected
	onInjectDropped
	onEnter
	onStreamOpen
	onStreamEnd
	onTimer
	onZero
)

var plainTriggers = map[string]trigger{"record_out": onRecordOut, "record_in": onRecordIn, "padding_in": onPaddingIn,
	"injected": onInjected, "inject_dropped": onInjectDropped, "enter": onEnter, "stream_open": onStreamOpen, "stream_end": onStreamEnd}

type opcode uint8

const (
	opInject opcode = iota
	opPause
	opRate
	opTimer
	opSet
	opAdd
	opSub
	opWeight
	opStop
	opRestart
)

type action struct {
	op     opcode
	cancel bool
	slot   int32 // into the machine's counters or timers
	a, b   value // inject after,len; pause after; rate rate,burst; timer after; counter value; weight value
}

type quantity uint8

const (
	whatCounter quantity = iota
	whatLen
	whatSeq
	whatWireLeft
	whatStallLeft
	whatWireSpent
	whatSentBytes
	whatWirePPM
)

type comparison uint8

const (
	cmpLT comparison = iota
	cmpLE
	cmpEQ
	cmpNE
	cmpGE
	cmpGT
)

var comparisons = map[string]comparison{"lt": cmpLT, "le": cmpLE, "eq": cmpEQ, "ne": cmpNE, "ge": cmpGE, "gt": cmpGT}

type transition struct {
	on    trigger
	cond  bool
	what  quantity
	cmp   comparison
	slot  int32
	to    int32
	cslot int32
	val   value
	prob  value
}

type record struct {
	target, maxLen, padTo, padMul, pad, delay value
	flush                                     bool
}

type stateDef struct {
	rec   *record
	enter []action
	next  []transition
}

type machineDef struct {
	start, counters, timers int
	states                  []stateDef
}

func parseMachines(x any, t *tables) ([]machineDef, error) {
	a, ok := x.([]any)
	if !ok || len(a) == 0 {
		return nil, fail("type", "machines")
	}
	if m := t.l.MaxMachines; m > 0 && len(a) > m {
		return nil, fail("limit", "%d machines", len(a))
	}
	out := make([]machineDef, len(a))
	for i := range a {
		if err := parseMachine(&out[i], a[i], t); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func smallInt(x any, what string) (int, error) {
	n, err := integer(x, what)
	if err != nil {
		return 0, err
	}
	if n < 0 || n > 1<<31-1 {
		return 0, fail("type", "%s: %d", what, n)
	}
	return int(n), nil
}

func parseMachine(m *machineDef, x any, t *tables) error {
	o, err := members(x, "machine", "start", "counters", "timers", "states")
	if err != nil {
		return err
	}
	v, err := required(o, "start", "machine")
	if err != nil {
		return err
	}
	if m.start, err = smallInt(v, "start"); err != nil {
		return err
	}
	if v := o.get("counters"); v != nil {
		if m.counters, err = smallInt(v, "counters"); err != nil {
			return err
		}
	}
	if v := o.get("timers"); v != nil {
		if m.timers, err = smallInt(v, "timers"); err != nil {
			return err
		}
	}
	if err := t.useSlots(m.counters); err != nil {
		return err
	}
	if err := t.useSlots(m.timers); err != nil {
		return err
	}
	if v, err = required(o, "states", "machine"); err != nil {
		return err
	}
	a, ok := v.([]any)
	if !ok || len(a) == 0 {
		return fail("type", "states")
	}
	if l := t.l.MaxStates; l > 0 && len(a) > l {
		return fail("limit", "%d states", len(a))
	}
	if m.start >= len(a) {
		return fail("reference", "start %d", m.start)
	}
	m.states = make([]stateDef, len(a))
	for i := range a {
		if err := m.parseState(&m.states[i], a[i], t, len(a)); err != nil {
			return err
		}
	}
	return m.structure()
}

func (m *machineDef) parseState(st *stateDef, x any, t *tables, n int) error {
	o, err := members(x, "state", "record", "enter", "next")
	if err != nil {
		return err
	}
	if v := o.get("record"); v != nil {
		if st.rec, err = parseRecord(v, t); err != nil {
			return err
		}
	}
	if v := o.get("enter"); v != nil {
		a, ok := v.([]any)
		if !ok {
			return fail("type", "enter")
		}
		if l := t.l.MaxActions; l > 0 && len(a) > l {
			return fail("limit", "%d actions", len(a))
		}
		st.enter = make([]action, len(a))
		for i := range a {
			if err := m.parseAction(&st.enter[i], a[i], t); err != nil {
				return err
			}
		}
	}
	if v := o.get("next"); v != nil {
		a, ok := v.([]any)
		if !ok {
			return fail("type", "next")
		}
		if l := t.l.MaxTransitions; l > 0 && len(a) > l {
			return fail("limit", "%d transitions", len(a))
		}
		st.next = make([]transition, len(a))
		for i := range a {
			if err := m.parseTransition(&st.next[i], a[i], t, n); err != nil {
				return err
			}
		}
	}
	return nil
}

func parseRecord(x any, t *tables) (*record, error) {
	o, err := members(x, "record", "target", "max_len", "pad_to", "pad_multiple", "pad", "delay", "flush")
	if err != nil {
		return nil, err
	}
	if o.get("target") != nil && (o.get("max_len") != nil || o.get("pad_to") != nil) {
		return nil, fail("conflict", "target with max_len or pad_to")
	}
	r := &record{}
	for _, f := range []struct {
		k   string
		to  *value
		fam family
	}{{"target", &r.target, famLength}, {"max_len", &r.maxLen, famLength}, {"pad_to", &r.padTo, famLength},
		{"pad_multiple", &r.padMul, famLength}, {"pad", &r.pad, famLength}, {"delay", &r.delay, famTime}} {
		if v := o.get(f.k); v != nil {
			if *f.to, err = parseValue(v, f.fam, t, true, f.k); err != nil {
				return nil, err
			}
		}
	}
	if v := o.get("flush"); v != nil {
		b, ok := v.(bool)
		if !ok {
			return nil, fail("type", "flush")
		}
		r.flush = b
	}
	return r, nil
}

func slotOf(o *jobject, limit int, what string) (int32, error) {
	v := o.get("slot")
	if v == nil {
		return 0, fail("type", "%s.slot", what)
	}
	n, err := integer(v, what+".slot")
	if err != nil {
		return 0, err
	}
	if n < 0 || n >= int64(limit) {
		return 0, fail("reference", "%s.slot %d", what, n)
	}
	return int32(n), nil
}

// cancelOf reads a cancel member, which may only be true.
func cancelOf(o *jobject) (bool, error) {
	v := o.get("cancel")
	if v == nil {
		return false, nil
	}
	if b, ok := v.(bool); !ok || !b {
		return false, fail("type", "cancel")
	}
	return true, nil
}

func (m *machineDef) parseAction(a *action, x any, t *tables) error {
	raw, ok := x.(*jobject)
	if !ok {
		return fail("type", "action")
	}
	name, _ := raw.get("op").(string)
	var o *jobject
	var err error
	req := func(k string, fam family) (value, error) {
		v, err := required(o, k, name)
		if err != nil {
			return value{}, err
		}
		return parseValue(v, fam, t, true, k)
	}
	switch name {
	case "inject":
		a.op = opInject
		if o, err = members(x, name, "op", "cancel", "after", "len"); err != nil {
			return err
		}
		if a.cancel, err = cancelOf(o); err != nil {
			return err
		}
		if a.cancel {
			if o.get("after") != nil || o.get("len") != nil {
				return fail("unknown_member", "inject cancel")
			}
			return nil
		}
		if a.a, err = req("after", famTime); err != nil {
			return err
		}
		a.b, err = req("len", famLength)
		return err
	case "pause":
		a.op = opPause
		if o, err = members(x, name, "op", "after"); err != nil {
			return err
		}
		a.a, err = req("after", famTime)
		return err
	case "rate":
		a.op = opRate
		if o, err = members(x, name, "op", "rate", "burst"); err != nil {
			return err
		}
		if a.a, err = req("rate", famRate); err != nil {
			return err
		}
		if o.get("burst") != nil {
			a.b, err = req("burst", famLength)
		}
		return err
	case "timer":
		a.op = opTimer
		if o, err = members(x, name, "op", "slot", "cancel", "after"); err != nil {
			return err
		}
		if a.slot, err = slotOf(o, m.timers, name); err != nil {
			return err
		}
		if a.cancel, err = cancelOf(o); err != nil {
			return err
		}
		if a.cancel {
			if o.get("after") != nil {
				return fail("unknown_member", "timer cancel")
			}
			return nil
		}
		a.a, err = req("after", famTime)
		return err
	case "set", "add", "sub":
		a.op = map[string]opcode{"set": opSet, "add": opAdd, "sub": opSub}[name]
		if o, err = members(x, name, "op", "slot", "value"); err != nil {
			return err
		}
		if a.slot, err = slotOf(o, m.counters, name); err != nil {
			return err
		}
		a.a, err = req("value", famCounter)
		return err
	case "weight":
		a.op = opWeight
		if o, err = members(x, name, "op", "value"); err != nil {
			return err
		}
		a.a, err = req("value", famWeight)
		return err
	case "stop", "restart":
		a.op = opStop
		if name == "restart" {
			a.op = opRestart
		}
		_, err = members(x, name, "op")
		return err
	}
	return fail("type", "op %q", name)
}

func (m *machineDef) parseTransition(tr *transition, x any, t *tables, n int) error {
	o, err := members(x, "transition", "on", "slot", "to", "if", "prob")
	if err != nil {
		return err
	}
	on, err := required(o, "on", "transition")
	if err != nil {
		return err
	}
	to, err := required(o, "to", "transition")
	if err != nil {
		return err
	}
	k, err := integer(to, "to")
	if err != nil {
		return err
	}
	if k < 0 || k >= int64(n) {
		return fail("reference", "to %d", k)
	}
	tr.to = int32(k)
	name, ok := on.(string)
	if !ok {
		return fail("type", "on")
	}
	switch name {
	case "timer":
		tr.on = onTimer
		tr.slot, err = slotOf(o, m.timers, name)
	case "zero":
		tr.on = onZero
		tr.slot, err = slotOf(o, m.counters, name)
	default:
		var known bool
		if tr.on, known = plainTriggers[name]; !known {
			return fail("type", "on %q", name)
		}
		if o.get("slot") != nil {
			return fail("unknown_member", "slot")
		}
	}
	if err != nil {
		return err
	}
	if v := o.get("if"); v != nil {
		c, err := members(v, "if", "what", "slot", "cmp", "value")
		if err != nil {
			return err
		}
		w, err := required(c, "what", "if")
		if err != nil {
			return err
		}
		tr.cond = true
		switch w {
		case "counter":
			tr.what = whatCounter
			if tr.cslot, err = slotOf(c, m.counters, "if"); err != nil {
				return err
			}
		case "len", "seq", "wire_left", "stall_left", "wire_spent", "sent_bytes", "wire_ppm":
			tr.what = map[any]quantity{"len": whatLen, "seq": whatSeq, "wire_left": whatWireLeft, "stall_left": whatStallLeft,
				"wire_spent": whatWireSpent, "sent_bytes": whatSentBytes, "wire_ppm": whatWirePPM}[w]
			if c.get("slot") != nil {
				return fail("unknown_member", "if.slot")
			}
		default:
			return fail("type", "if.what")
		}
		cm, err := required(c, "cmp", "if")
		if err != nil {
			return err
		}
		s, _ := cm.(string)
		var known bool
		if tr.cmp, known = comparisons[s]; !known {
			return fail("type", "if.cmp")
		}
		v, err := required(c, "value", "if")
		if err != nil {
			return err
		}
		if tr.val, err = parseValue(v, famCounter, t, true, "if.value"); err != nil {
			return err
		}
	}
	if v := o.get("prob"); v != nil {
		if tr.prob, err = parseValue(v, famPPM, t, true, "prob"); err != nil {
			return err
		}
	}
	return nil
}

// structure checks reachability, that enter, zero and restart edges have no
// cycle, and that no state on a cycle that also takes injected and timer edges
// arms an injection or timer that can be due at once.
func (m *machineDef) structure() error {
	n := len(m.states)
	graph := func(on func(trigger) bool) [][]int32 {
		adj := make([][]int32, n)
		for i := range m.states {
			st := &m.states[i]
			for j := range st.next {
				if on(st.next[j].on) {
					adj[i] = append(adj[i], st.next[j].to)
				}
			}
			for j := range st.enter {
				if st.enter[j].op == opRestart {
					adj[i] = append(adj[i], int32(m.start))
					break
				}
			}
		}
		return adj
	}
	seen := make([]bool, n)
	all := graph(func(trigger) bool { return true })
	stack := []int32{int32(m.start)}
	for len(stack) > 0 {
		i := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !seen[i] {
			seen[i] = true
			stack = append(stack, all[i]...)
		}
	}
	for i, ok := range seen {
		if !ok {
			return fail("unreachable", "state %d", i)
		}
	}
	for i, c := range onCycle(graph(func(t trigger) bool { return t == onEnter || t == onZero })) {
		if c {
			return fail("cycle", "state %d", i)
		}
	}
	wide := graph(func(t trigger) bool {
		return t == onEnter || t == onZero || t == onInjected || t == onInjectDropped || t == onTimer
	})
	for i, c := range onCycle(wide) {
		if !c {
			continue
		}
		for j := range m.states[i].enter {
			a := &m.states[i].enter[j]
			if (a.op == opInject || a.op == opTimer) && !a.cancel && a.a.lower() == 0 {
				return fail("zero_delay_cycle", "state %d", i)
			}
		}
	}
	return nil
}

// onCycle reports for each vertex whether it lies on a cycle: Tarjan's strongly
// connected components without recursion, in time linear in the graph.
func onCycle(adj [][]int32) []bool {
	n := len(adj)
	out := make([]bool, n)
	index := make([]int32, n) // 0: not visited
	low := make([]int32, n)
	open := make([]bool, n)
	var comp []int32
	type frame struct {
		v    int32
		next int
	}
	var calls []frame
	var clock int32
	visit := func(v int32) {
		clock++
		index[v], low[v] = clock, clock
		open[v] = true
		comp = append(comp, v)
		calls = append(calls, frame{v: v})
	}
	for s := range adj {
		if index[s] != 0 {
			continue
		}
		visit(int32(s))
		for len(calls) > 0 {
			f := &calls[len(calls)-1]
			v := f.v
			if f.next < len(adj[v]) {
				w := adj[v][f.next]
				f.next++
				switch {
				case w == v:
					out[v] = true
				case index[w] == 0:
					visit(w)
				case open[w]:
					low[v] = min(low[v], index[w])
				}
				continue
			}
			calls = calls[:len(calls)-1]
			if len(calls) > 0 {
				p := calls[len(calls)-1].v
				low[p] = min(low[p], low[v])
			}
			if low[v] != index[v] {
				continue
			}
			first := len(comp) - 1
			for comp[first] != v {
				first--
			}
			for _, w := range comp[first:] {
				open[w] = false
				out[w] = out[w] || len(comp)-first > 1
			}
			comp = comp[:first]
		}
	}
	return out
}
