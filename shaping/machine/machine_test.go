package machine

import (
	"encoding/json"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping"
)

func f64(x float64) *float64 { return &x }

func seeded() Options {
	return Options{NewSource: func() rand.Source { return rand.NewPCG(1, 2) }}
}

func plans(t *testing.T, d *Definition, o Options, n int) []shaping.RecordPlan {
	t.Helper()
	f, err := d.Factory(o)
	if err != nil {
		t.Fatal(err)
	}
	s := f(zmux.ShapedPipe{ID: 0})
	var r shaping.Reply
	s.OnEvent(&shaping.Event{Kind: shaping.EventStart}, &r)
	out := make([]shaping.RecordPlan, n)
	for i := range out {
		s.Plan(&shaping.RecordView{Seq: uint64(i)}, &out[i])
	}
	return out
}

func TestLengthFields(t *testing.T) {
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{{Record: &Record{
		MaxLen: cv(7), PadTo: cv(5), PadMultiple: cv(4), Pad: cv(3), Delay: cv(0.002), Flush: true}}}}}}
	p := plans(t, d, Options{}, 1)[0]
	if p.MaxLen != 7 || p.PadTo != 5 || p.PadMultiple != 4 || p.Pad != 3 || p.Delay != 2*time.Millisecond || !p.Flush {
		t.Fatalf("%+v", p)
	}
}

func TestCounterTransition(t *testing.T) {
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{
		{Record: &Record{Pad: cv(1)}, Enter: []Action{{Op: OpSet, Value: cv(2)}},
			Next: []Transition{{On: OnRecordOut, To: 1}}},
		{Record: &Record{Pad: cv(1)}, Enter: []Action{{Op: OpSub, Value: cv(1)}},
			Next: []Transition{{On: OnZero0, To: 2}, {On: OnRecordOut, To: 1}}},
		{},
	}}}}
	var got []int
	for _, p := range plans(t, d, Options{}, 5) {
		got = append(got, p.Pad)
	}
	if want := []int{1, 1, 0, 0, 0}; !equal(got, want) {
		t.Fatalf("%v, want %v", got, want)
	}
}

func TestGuardAndProbability(t *testing.T) {
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{
		{Record: &Record{Pad: cv(1)}, Next: []Transition{
			{On: OnRecordOut, To: 1, If: &Cond{What: QuantitySeq, Cmp: CmpGE, Value: *cv(2)}},
			{On: OnRecordOut, To: 1, Prob: cv(0)}}},
		{},
	}}}}
	var got []int
	for _, p := range plans(t, d, Options{}, 5) {
		got = append(got, p.Pad)
	}
	if want := []int{1, 1, 1, 0, 0}; !equal(got, want) {
		t.Fatalf("%v, want %v", got, want)
	}
}

func TestScopeAndBounds(t *testing.T) {
	pipe := &Value{Range: []float64{0, 1000}, Scope: ScopePipe}
	use := &Value{Range: []float64{0, 1000}}
	bounded := &Value{Range: []float64{0, 1000}, Min: f64(10), Max: f64(20)}
	d := &Definition{Version: 1, Machines: []Machine{
		{States: []State{{Record: &Record{Pad: pipe}}}},
		{States: []State{{Record: &Record{PadTo: use, PadMultiple: bounded}}}},
	}}
	ps := plans(t, d, seeded(), 20)
	varies := false
	for _, p := range ps {
		if p.Pad != ps[0].Pad {
			t.Fatal("pipe-scoped value changed")
		}
		varies = varies || p.PadTo != ps[0].PadTo
		if p.PadMultiple < 10 || p.PadMultiple > 20 {
			t.Fatalf("block %d", p.PadMultiple)
		}
	}
	if !varies {
		t.Fatal("per-use value constant")
	}
	again := plans(t, d, seeded(), 20)
	for i := range ps {
		if ps[i] != again[i] {
			t.Fatal("same seed, different plans")
		}
	}
}

func TestFactoryScope(t *testing.T) {
	v := &Value{Range: []float64{0, 1e6}, Scope: ScopeShared}
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{{Record: &Record{Pad: v}}}}}}
	f, err := d.Factory(Options{})
	if err != nil {
		t.Fatal(err)
	}
	var a, b shaping.RecordPlan
	f(zmux.ShapedPipe{ID: 0}).Plan(&shaping.RecordView{}, &a)
	f(zmux.ShapedPipe{ID: 1}).Plan(&shaping.RecordView{}, &b)
	if a.Pad != b.Pad {
		t.Fatal("factory-scoped value differs between pipes")
	}
}

func TestEnterActions(t *testing.T) {
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{{Enter: []Action{
		{Op: OpInject, After: cv(0.001), Len: cv(9)},
		{Op: OpRate, Rate: cv(100), Burst: cv(50)},
		{Op: OpTimer, Slot: 1, After: cv(0.003)},
		{Op: OpWeight, Value: cv(2)},
	}}}}}}
	f, err := d.Factory(Options{})
	if err != nil {
		t.Fatal(err)
	}
	var r shaping.Reply
	f(zmux.ShapedPipe{ID: 0}).OnEvent(&shaping.Event{Kind: shaping.EventStart}, &r)
	if r.Inject == nil || *r.Inject != (shaping.Inject{After: time.Millisecond, Len: 9}) ||
		r.Gate == nil || *r.Gate != (shaping.Gate{Rate: 100, Burst: 50}) ||
		r.Timer[0] != nil || r.Timer[1] == nil || *r.Timer[1] != (shaping.Timer{After: 3 * time.Millisecond}) ||
		r.Weight == nil || *r.Weight != 2 || r.Stop {
		t.Fatalf("%+v", r)
	}
	// Cancel: explicit for injections and timers.
	d = &Definition{Version: 1, Machines: []Machine{{States: []State{{Enter: []Action{
		{Op: OpInject, Cancel: true}, {Op: OpTimer, Slot: 0, Cancel: true},
	}}}}}}
	if f, err = d.Factory(Options{}); err != nil {
		t.Fatal(err)
	}
	r = shaping.Reply{}
	f(zmux.ShapedPipe{ID: 0}).OnEvent(&shaping.Event{Kind: shaping.EventStart}, &r)
	if r.Inject == nil || !r.Inject.Cancel || r.Timer[0] == nil || !r.Timer[0].Cancel || r.Gate != nil || r.Weight != nil {
		t.Fatalf("%+v", r)
	}
}

func TestBag(t *testing.T) {
	v := &Value{Dist: &Dist{Kind: "bag", Points: []float64{1, 2, 3}}}
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{{Record: &Record{Pad: v}}}}}}
	ps := plans(t, d, seeded(), 6)
	for k := 0; k < 6; k += 3 {
		seen := map[int]bool{}
		for _, p := range ps[k : k+3] {
			seen[p.Pad] = true
		}
		if len(seen) != 3 {
			t.Fatalf("draws %v", ps)
		}
	}
}

func TestDistributionsInRange(t *testing.T) {
	for _, d := range []Dist{
		{Kind: "uniform", Params: []float64{2, 3}}, {Kind: "loguniform", Params: []float64{2, 3}},
		{Kind: "exponential", Params: []float64{1}}, {Kind: "normal", Params: []float64{5, 1}},
		{Kind: "lognormal", Params: []float64{0, 1}}, {Kind: "weibull", Params: []float64{1, 2}},
		{Kind: "pareto", Params: []float64{1, 2}}, {Kind: "gamma", Params: []float64{0.5, 1}},
		{Kind: "beta", Params: []float64{2, 2}}, {Kind: "geometric", Params: []float64{0.5}},
		{Kind: "binomial", Params: []float64{4, 0.5}}, {Kind: "poisson", Params: []float64{40}},
		{Kind: "empirical", Points: []float64{1, 2}, Weights: []float64{0, 1}},
		{Kind: "mixture", Of: []Value{*cv(4)}, Weights: []float64{1}},
	} {
		v := &Value{Dist: &d}
		if d.Kind == "normal" {
			zero := 0.0
			v.Min = &zero // required: a normal draw may be negative
		}
		def := &Definition{Version: 1, Machines: []Machine{{States: []State{{Record: &Record{Pad: v}}}}}}
		for _, p := range plans(t, def, seeded(), 50) {
			x := p.Pad
			bad := x < 0
			switch d.Kind {
			case "uniform", "loguniform":
				bad = x < 2 || x > 3
			case "beta":
				bad = x > 1
			case "binomial":
				bad = x > 4
			case "geometric":
				bad = x < 1
			case "empirical":
				bad = x != 2
			case "mixture":
				bad = x != 4
			}
			if bad {
				t.Fatalf("%s: %d", d.Kind, x)
			}
		}
	}
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type planVec struct {
	MaxLen      int   `json:"max_len"`
	PadTo       int   `json:"pad_to"`
	PadMultiple int   `json:"pad_multiple"`
	Pad         int   `json:"pad"`
	DelayNs     int64 `json:"delay_ns"`
	Flush       bool  `json:"flush"`
	Stop        bool  `json:"stop"`
}

func TestContractDefinitions(t *testing.T) {
	b, err := os.ReadFile("../../testdata/shaping/contract.json")
	if err != nil {
		t.Fatal(err)
	}
	testDefinitions(t, b)
}

// testDefinitions runs the definitions of a vector file: each loads from
// json, raw (the exact text) or sequence, and fails with an error holding
// error (any case) or yields plans.
func testDefinitions(t *testing.T, b []byte) {
	var k struct {
		Definitions []struct {
			Name     string
			JSON     json.RawMessage `json:"json"`
			Raw      string
			Sequence string
			Error    string
			Plans    []planVec
		}
	}
	if err := json.Unmarshal(b, &k); err != nil {
		t.Fatal(err)
	}
	for _, v := range k.Definitions {
		t.Run(v.Name, func(t *testing.T) {
			var d *Definition
			var err error
			switch {
			case v.Sequence != "":
				d, err = ParseSequence(v.Sequence)
			case v.Raw != "":
				d, err = Parse([]byte(v.Raw))
			default:
				d, err = Load(strings.NewReader(string(v.JSON)))
			}
			if v.Error != "" {
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(v.Error)) {
					t.Fatalf("error %v, want %q", err, v.Error)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for i, p := range plans(t, d, seeded(), len(v.Plans)) {
				got := planVec{p.MaxLen, p.PadTo, p.PadMultiple, p.Pad, int64(p.Delay), p.Flush, p.Stop}
				if got != v.Plans[i] {
					t.Fatalf("plan %d: %+v, want %+v", i, got, v.Plans[i])
				}
			}
		})
	}
}

func TestSequenceInject(t *testing.T) {
	d, err := ParseSequence("10/inject=2:30, 20")
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.Factory(Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := f(zmux.ShapedPipe{ID: 0})
	var r shaping.Reply
	s.OnEvent(&shaping.Event{Kind: shaping.EventStart}, &r)
	var p shaping.RecordPlan
	s.Plan(&shaping.RecordView{}, &p)
	if p.PadTo != 10 || p.Reply.Inject != nil {
		t.Fatalf("%+v", p)
	}
	n := 0
	for ev := shaping.EventRecordIn; ; ev = shaping.EventInjected {
		r = shaping.Reply{}
		s.OnEvent(&shaping.Event{Kind: ev}, &r)
		if r.Inject == nil {
			break
		}
		if r.Inject.Len != 30 || r.Inject.After != 0 {
			t.Fatalf("%+v", r.Inject)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("%d injections", n)
	}
	p = shaping.RecordPlan{}
	s.Plan(&shaping.RecordView{}, &p)
	// The stop follows the record: the reply applied first leaves it.
	if p.PadTo != 20 || !p.Stop || p.Reply.Stop {
		t.Fatalf("%+v", p)
	}
}

func TestSizeLimits(t *testing.T) {
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{
		{Enter: []Action{{Op: OpWeight, Value: cv(1)}, {Op: OpWeight, Value: cv(1)}},
			Next: []Transition{{On: OnRecordOut, To: 1}}}, {}}}}}
	for _, l := range []SizeLimits{{MaxMachines: 0, MaxStates: 1}, {MaxActions: 1}, {MaxTransitions: 0, MaxMachines: 0, MaxPoints: 0, MaxStates: 0, MaxBytes: 0}} {
		_, err := d.Factory(Options{Limits: l})
		if (err == nil) != (l == SizeLimits{}) {
			t.Fatalf("%+v: %v", l, err)
		}
	}
	if _, err := NewFactory([]byte(`{"version":1,"machines":[{"states":[{}]}]}`), Options{Limits: SizeLimits{MaxBytes: 8}}); err == nil {
		t.Fatal("size limit not applied")
	}
}

func TestStreamTriggerCaps(t *testing.T) {
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{
		{Next: []Transition{{On: OnStreamOpen, To: 1}}}, {Record: &Record{Pad: cv(5)}}}}}}
	f, err := d.Factory(Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := f(zmux.ShapedPipe{ID: 0})
	if c := s.Capabilities(); c&shaping.CapStreams == 0 || c&shaping.CapRecordIn != 0 {
		t.Fatalf("caps %b", c)
	}
	var r shaping.Reply
	s.OnEvent(&shaping.Event{Kind: shaping.EventStart}, &r)
	s.OnEvent(&shaping.Event{Kind: shaping.EventStreamOpen, Stream: 4}, &r)
	var p shaping.RecordPlan
	s.Plan(&shaping.RecordView{}, &p)
	if p.Pad != 5 {
		t.Fatalf("%+v", p)
	}
}

// The pipe's write overhead comes off max_len and pad_to on the first
// record of a carrier write only; a max_len it takes below the floor is
// left for the session to raise.
func TestWriteOverhead(t *testing.T) {
	d := &Definition{Version: 1, Machines: []Machine{{States: []State{{Record: &Record{
		MaxLen: cv(1000), PadTo: cv(800), PadMultiple: cv(16), Pad: cv(3)}}}}}}
	f, err := d.Factory(Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := f(zmux.ShapedPipe{WriteOverhead: 22})
	var r shaping.Reply
	s.OnEvent(&shaping.Event{Kind: shaping.EventStart}, &r)
	var first, later shaping.RecordPlan
	s.Plan(&shaping.RecordView{WriteStart: true}, &first)
	s.Plan(&shaping.RecordView{}, &later)
	if first.MaxLen != 978 || first.PadTo != 778 || first.PadMultiple != 16 || first.Pad != 3 {
		t.Fatalf("first record of a write: %+v", first)
	}
	if later.MaxLen != 1000 || later.PadTo != 800 {
		t.Fatalf("later record of a write: %+v", later)
	}
	var zero shaping.RecordPlan
	f(zmux.ShapedPipe{}).Plan(&shaping.RecordView{WriteStart: true}, &zero)
	if zero.MaxLen != 1000 || zero.PadTo != 800 {
		t.Fatalf("without overhead: %+v", zero)
	}
	small := &Definition{Version: 1, Machines: []Machine{{States: []State{{Record: &Record{MaxLen: cv(20)}}}}}}
	fs, err := small.Factory(Options{})
	if err != nil {
		t.Fatal(err)
	}
	var p shaping.RecordPlan
	fs(zmux.ShapedPipe{WriteOverhead: 22}).Plan(&shaping.RecordView{WriteStart: true}, &p)
	if p.MaxLen != 1 {
		t.Fatalf("max_len under the overhead: %d, want 1 (raised by the session)", p.MaxLen)
	}
}
