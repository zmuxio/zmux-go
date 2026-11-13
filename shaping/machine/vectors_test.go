package machine

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping"
)

// testSource is a splitmix64 generator or a fixed list, counting its draws.
type testSource struct {
	seeded bool
	state  uint64
	list   []uint64
	n      uint64
}

func (s *testSource) Uint64() uint64 {
	s.n++
	if !s.seeded {
		if int(s.n) > len(s.list) {
			panic("source exhausted")
		}
		return s.list[s.n-1]
	}
	s.state += 0x9E3779B97F4A7C15
	z := s.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

type srcSpec struct {
	Seed *string  `json:"seed"`
	U    []string `json:"u"`
}

func (c srcSpec) make(t *testing.T) *testSource {
	t.Helper()
	if c.Seed != nil {
		v, err := strconv.ParseUint(*c.Seed, 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		return &testSource{seeded: true, state: v}
	}
	s := &testSource{}
	for _, u := range c.U {
		v, err := strconv.ParseUint(u, 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		s.list = append(s.list, v)
	}
	return s
}

func readVectors(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile("../../testdata/shaping/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

func classOf(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := errors.AsType[*FormatError](err); ok {
		return e.Class
	}
	return "?" + err.Error()
}

func TestDrawVectors(t *testing.T) {
	var d struct {
		Splitmix64 []struct {
			Seed  string   `json:"seed"`
			First []string `json:"first"`
		} `json:"splitmix64"`
		Values []struct {
			Name     string          `json:"name"`
			T        json.RawMessage `json:"t"`
			Source   srcSpec         `json:"source"`
			Draws    []uint64        `json:"draws"`
			Consumed uint64          `json:"consumed"`
		} `json:"values"`
		Prob []struct {
			Name     string  `json:"name"`
			P        uint64  `json:"p"`
			Source   srcSpec `json:"source"`
			Result   bool    `json:"result"`
			Consumed uint64  `json:"consumed"`
		} `json:"prob"`
		Jitter []struct {
			Name     string  `json:"name"`
			Value    uint64  `json:"value"`
			J        uint32  `json:"jitter_ppm"`
			Source   srcSpec `json:"source"`
			Result   uint64  `json:"result"`
			Consumed uint64  `json:"consumed"`
		} `json:"jitter"`
		Compile []struct {
			Name      string                     `json:"name"`
			Candidate json.RawMessage            `json:"candidate"`
			Tables    map[string]json.RawMessage `json:"tables"`
			Source    srcSpec                    `json:"source"`
			Shared    []uint64                   `json:"shared"`
			Consumed  uint64                     `json:"consumed"`
		} `json:"compile_order"`
	}
	readVectors(t, "draw.json", &d)
	for _, c := range d.Splitmix64 {
		s := srcSpec{Seed: &c.Seed}.make(t)
		for _, f := range c.First {
			if got := fmt.Sprintf("%016x", s.Uint64()); got != f {
				t.Errorf("splitmix64 %s: %s, want %s", c.Seed, got, f)
			}
		}
	}
	for _, c := range d.Values {
		x, err := decode(c.T)
		if err != nil {
			t.Fatal(err)
		}
		v, err := parseValue(&jobject{keys: []string{"t"}, vals: map[string]any{"t": x}}, family{0, maxCount}, nil, true, "t")
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		s := c.Source.make(t)
		for i, w := range c.Draws {
			if got := v.drawTable(s); got != w {
				t.Errorf("%s draw %d: %d, want %d", c.Name, i, got, w)
			}
		}
		if s.n != c.Consumed {
			t.Errorf("%s: consumed %d, want %d", c.Name, s.n, c.Consumed)
		}
	}
	for _, c := range d.Prob {
		s := c.Source.make(t)
		if got := probTrue(c.P, s); got != c.Result || s.n != c.Consumed {
			t.Errorf("%s: %v consumed %d", c.Name, got, s.n)
		}
	}
	for _, c := range d.Jitter {
		s := c.Source.make(t)
		got := c.Value
		if c.J != 0 {
			got = jitterWith(c.Value, c.J, s.Uint64())
		}
		if got != c.Result || s.n != c.Consumed {
			t.Errorf("%s: %d consumed %d, want %d", c.Name, got, s.n, c.Result)
		}
	}
	for _, c := range d.Compile {
		cand, err := ParseCandidate(c.Candidate, c.Tables, SizeLimits{})
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		s := c.Source.make(t)
		cc := cand.Compile(Options{NewSource: func() rand.Source { return s }})
		if len(cc.shared)+len(c.Shared) > 0 && !reflect.DeepEqual(cc.shared, c.Shared) || s.n != c.Consumed {
			t.Errorf("%s: shared %v consumed %d, want %v %d", c.Name, cc.shared, s.n, c.Shared, c.Consumed)
		}
	}
}

type effectsJSON = map[string]json.RawMessage

func checkEffects(t *testing.T, where string, r *shaping.Effects, want effectsJSON) {
	t.Helper()
	got := map[string]any{}
	if r.Set&shaping.EffectInject != 0 {
		got["inject"] = nil
		if r.HasInject {
			got["inject"] = map[string]any{"at": float64(r.InjectAt / time.Microsecond), "len": float64(r.InjectLen)}
		}
	}
	if r.Set&shaping.EffectWake != 0 {
		got["wake"] = nil
		if r.HasWake {
			got["wake"] = float64(r.Wake / time.Microsecond)
		}
	}
	if r.Set&shaping.EffectGate != 0 {
		got["gate"] = map[string]any{"pause_until": float64(r.PauseUntil / time.Microsecond), "rate": float64(r.Rate), "burst": float64(r.Burst)}
	}
	if r.Set&shaping.EffectWeight != 0 {
		got["weight"] = float64(r.Weight)
	}
	if r.Set&shaping.EffectStop != 0 {
		got["stop"] = true
	}
	w := map[string]any{}
	for k, v := range want {
		var x any
		if err := json.Unmarshal(v, &x); err != nil {
			t.Fatal(err)
		}
		w[k] = x
	}
	if !reflect.DeepEqual(got, w) {
		t.Errorf("%s: effects %v, want %v", where, got, w)
	}
}

func TestMachineVectors(t *testing.T) {
	var d struct {
		Run []struct {
			Name       string                     `json:"name"`
			Definition json.RawMessage            `json:"definition"`
			Tables     map[string]json.RawMessage `json:"tables"`
			Source     srcSpec                    `json:"source"`
			Pipes      int                        `json:"pipes"`
			Compile    struct {
				Shared   []uint64 `json:"shared"`
				Consumed uint64   `json:"consumed"`
				Caps     []string `json:"caps"`
			} `json:"compile"`
			Steps []struct {
				Op     string `json:"op"`
				Now    int64  `json:"now"`
				Len    int    `json:"len"`
				Pipe   int    `json:"pipe"`
				Meters *struct {
					WireLeft  *uint64 `json:"wire_left"`
					StallLeft *uint64 `json:"stall_left"`
					WireSpent uint64  `json:"wire_spent"`
					SentBytes uint64  `json:"sent_bytes"`
				} `json:"meters"`
				Plan *struct {
					MaxLen      int  `json:"max_len"`
					PadTo       int  `json:"pad_to"`
					PadMultiple int  `json:"pad_multiple"`
					Pad         int  `json:"pad"`
					Delay       int  `json:"delay"`
					Flush       bool `json:"flush"`
					Stop        bool `json:"stop"`
				} `json:"plan"`
				Effects  effectsJSON `json:"effects"`
				Consumed uint64      `json:"consumed"`
				State    []int32     `json:"state"`
				Counters [][]uint64  `json:"counters"`
			} `json:"steps"`
		} `json:"run"`
		Invalid []struct {
			Name       string                     `json:"name"`
			Definition json.RawMessage            `json:"definition"`
			Tables     map[string]json.RawMessage `json:"tables"`
			Error      string                     `json:"error"`
		} `json:"invalid"`
	}
	readVectors(t, "machine.json", &d)
	for _, c := range d.Invalid {
		_, err := ParseCandidate(c.Definition, c.Tables, SizeLimits{})
		if got := classOf(err); got != c.Error {
			t.Errorf("%s: %q, want %q", c.Name, got, c.Error)
		}
	}
	for _, c := range d.Run {
		cand, err := ParseCandidate(c.Definition, c.Tables, SizeLimits{})
		if err != nil {
			t.Errorf("%s: %v", c.Name, err)
			continue
		}
		s := c.Source.make(t)
		cc := cand.Compile(Options{NewSource: func() rand.Source { return s }})
		if len(c.Compile.Shared) > 0 && !reflect.DeepEqual(cc.shared, c.Compile.Shared) || s.n != c.Compile.Consumed {
			t.Errorf("%s: compile shared %v consumed %d", c.Name, cc.shared, s.n)
		}
		pipes := make([]*shaper, c.Pipes)
		f := cc.Factory()
		for i := range pipes {
			pipes[i] = f(zmux.ShapedPipe{}).(*shaper)
		}
		caps := []string{"plan"}
		if cc.machines != nil && cc.machines.caps&shaping.CapBudget != 0 {
			caps = []string{"budget", "plan"}
		}
		if pipes[0].Capabilities()&shaping.CapRecordIn != 0 {
			caps = append(caps, "record_in")
		}
		if pipes[0].Capabilities()&shaping.CapStreams != 0 {
			caps = append(caps, "streams")
		}
		if !reflect.DeepEqual(caps, c.Compile.Caps) {
			t.Errorf("%s: caps %v, want %v", c.Name, caps, c.Compile.Caps)
		}
		for i, st := range c.Steps {
			meters := shaping.Meters{WireLeft: math.MaxUint64, StallLeft: math.MaxUint64}
			if m := st.Meters; m != nil {
				meters.WireSpent, meters.SentBytes = m.WireSpent, m.SentBytes
				if m.WireLeft != nil {
					meters.WireLeft = *m.WireLeft
				}
				if m.StallLeft != nil {
					meters.StallLeft = *m.StallLeft
				}
			}
			where := fmt.Sprintf("%s step %d %s", c.Name, i, st.Op)
			p := pipes[st.Pipe]
			now := time.Duration(st.Now) * time.Microsecond
			var eff shaping.Effects
			if st.Op == "plan" {
				var pl shaping.RecordPlan
				p.Plan(&shaping.RecordView{Now: now, Meters: meters}, &pl)
				eff = pl.Effects
				w := st.Plan
				if pl.MaxLen != w.MaxLen || pl.PadTo != w.PadTo || pl.PadMultiple != w.PadMultiple || pl.Pad != w.Pad ||
					pl.Delay != time.Duration(w.Delay)*time.Microsecond || pl.Flush != w.Flush || pl.Stop != w.Stop {
					t.Errorf("%s: plan %+v, want %+v", where, pl, *w)
				}
			} else {
				e := shaping.Event{Now: now, Len: st.Len, Meters: meters}
				switch st.Op {
				case "start":
					e.Kind = shaping.EventStart
				case "record_in":
					e.Kind = shaping.EventRecordIn
				case "padding_in":
					e.Kind, e.PaddingOnly = shaping.EventRecordIn, true
				case "injected":
					e.Kind = shaping.EventInjected
				case "inject_dropped":
					e.Kind = shaping.EventInjectDropped
				case "wake":
					e.Kind = shaping.EventTimer
				case "stream_open":
					e.Kind = shaping.EventStreamOpen
				case "stream_end":
					e.Kind = shaping.EventStreamEnd
				default:
					t.Fatal(where)
				}
				p.OnEvent(&e, &eff)
			}
			checkEffects(t, where, &eff, st.Effects)
			if s.n != st.Consumed {
				t.Errorf("%s: consumed %d, want %d", where, s.n, st.Consumed)
			}
			for m := range p.ms {
				if p.ms[m].cur != st.State[m] {
					t.Errorf("%s: machine %d state %d, want %d", where, m, p.ms[m].cur, st.State[m])
				}
				if len(st.Counters[m]) > 0 && !reflect.DeepEqual(p.ms[m].ctr, st.Counters[m]) {
					t.Errorf("%s: machine %d counters %v, want %v", where, m, p.ms[m].ctr, st.Counters[m])
				}
			}
		}
	}
}

// A shaped pipe plans and answers events without allocating.
func TestPlanAllocs(t *testing.T) {
	const def = `{"machines":[{"start":0,"counters":1,"timers":1,"states":[
	 {"record":{"target":{"t":[[1,500,900],[3,1000,1400],[1,0,0]]},"pad":{"t":[[1,0,16]],"scope":"pipe"},"delay":{"t":[[9,0,0],[1,100,900]]}},
	  "enter":[{"op":"add","slot":0,"value":1},{"op":"timer","slot":0,"after":{"t":[[1,1000,9000]]}}],
	  "next":[{"on":"record_out","to":1,"prob":500000},{"on":"timer","slot":0,"to":1}]},
	 {"record":{"max_len":1400},"enter":[{"op":"inject","after":100,"len":{"t":[[1,64,900]]}},{"op":"sub","slot":0,"value":1}],
	  "next":[{"on":"record_out","to":0,"if":{"what":"seq","cmp":"ge","value":2}},{"on":"injected","to":0}]}]},
	 {"start":0,"states":[{"record":{"pad_multiple":16}}]}]}`
	f, err := NewFactory([]byte(def), Options{})
	if err != nil {
		t.Fatal(err)
	}
	sh := f(zmux.ShapedPipe{})
	var r shaping.Effects
	sh.OnEvent(&shaping.Event{Kind: shaping.EventStart}, &r)
	now := time.Duration(0)
	// As the session holds them, in the pipe's state.
	var p shaping.RecordPlan
	var e shaping.Effects
	var v shaping.RecordView
	var ev shaping.Event
	if n := testing.AllocsPerRun(2000, func() {
		now += time.Millisecond
		p, v = shaping.RecordPlan{}, shaping.RecordView{Now: now}
		sh.Plan(&v, &p)
		e, ev = shaping.Effects{}, shaping.Event{Kind: shaping.EventTimer, Now: now}
		sh.OnEvent(&ev, &e)
		e, ev = shaping.Effects{}, shaping.Event{Kind: shaping.EventInjected, Now: now, Len: 64}
		sh.OnEvent(&ev, &e)
	}); n != 0 {
		t.Errorf("%v allocations per plan", n)
	}
}

func BenchmarkPlan(b *testing.B) {
	f, err := NewFactory([]byte(`{"machines":[{"start":0,"counters":1,"states":[
	 {"record":{"target":{"t":[[1,500,900],[3,1000,1400]]},"pad":{"t":[[1,0,16]]}},"next":[{"on":"record_out","to":1,"prob":500000}]},
	 {"record":{"max_len":1400},"enter":[{"op":"add","slot":0,"value":1}],"next":[{"on":"record_out","to":0}]}]}]}`), Options{})
	if err != nil {
		b.Fatal(err)
	}
	sh := f(zmux.ShapedPipe{})
	var r shaping.Effects
	sh.OnEvent(&shaping.Event{Kind: shaping.EventStart}, &r)
	b.ReportAllocs()
	var p shaping.RecordPlan
	var v shaping.RecordView
	for b.Loop() {
		p = shaping.RecordPlan{}
		sh.Plan(&v, &p)
	}
}

func TestPoolDecideVectors(t *testing.T) {
	var d struct {
		Decide []struct {
			Name    string          `json:"name"`
			Policy  json.RawMessage `json:"policy"`
			Source  srcSpec         `json:"source"`
			Compile struct {
				Shared   []uint64 `json:"shared"`
				Consumed uint64   `json:"consumed"`
			} `json:"compile"`
			Steps []struct {
				Failures int    `json:"failures"`
				Since    *int64 `json:"since_dial"`
				Members  []struct {
					ID            uint64 `json:"id"`
					Age           int64  `json:"age"`
					Idle          int64  `json:"idle"`
					Pipes         int    `json:"pipes"`
					BytesSent     uint64 `json:"bytes_sent"`
					BytesReceived uint64 `json:"bytes_received"`
				} `json:"members"`
				Want struct {
					Count         *int     `json:"count"`
					DialAfter     *int64   `json:"dial_after"`
					Retire        []uint64 `json:"retire"`
					ProbeInterval *int64   `json:"probe_interval"`
					ProbeJitter   *int64   `json:"probe_jitter"`
					Recheck       *int64   `json:"recheck"`
				} `json:"want"`
				Consumed uint64 `json:"consumed"`
			} `json:"steps"`
		} `json:"decide"`
	}
	readVectors(t, "pool_policy.json", &d)
	for _, c := range d.Decide {
		cand, err := ParseCandidate([]byte(`{"pool":`+string(c.Policy)+`}`), nil, SizeLimits{})
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		s := c.Source.make(t)
		cc := cand.Compile(Options{NewSource: func() rand.Source { return s }})
		if len(cc.shared)+len(c.Compile.Shared) > 0 && !reflect.DeepEqual(cc.shared, c.Compile.Shared) || s.n != c.Compile.Consumed {
			t.Errorf("%s: compile %v %d", c.Name, cc.shared, s.n)
		}
		p := cc.PoolPolicy()
		base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		us := func(x *int64) time.Duration {
			if x == nil {
				return 0
			}
			return time.Duration(*x) * time.Microsecond
		}
		for i, st := range c.Steps {
			v := &zmux.PoolView{Now: base, Failures: st.Failures}
			if st.Since != nil {
				v.LastDial = base.Add(-us(st.Since))
			}
			for _, m := range st.Members {
				v.Members = append(v.Members, zmux.PoolMember{PipeID: zmux.PipeID(m.ID), Age: us(&m.Age), Idle: us(&m.Idle), Pipes: m.Pipes, BytesSent: m.BytesSent, BytesReceived: m.BytesReceived})
			}
			var g zmux.PoolDecision
			p.Decide(v, &g)
			w := st.Want
			wc := 0
			if w.Count != nil {
				wc = *w.Count
			}
			wj := 0.0
			if w.ProbeJitter != nil {
				wj = float64(*w.ProbeJitter) / 1e6
			}
			gi, gj := g.Liveness.ProbeInterval, g.Liveness.ProbeJitter
			if gi < 0 { // off
				gi = 0
			}
			if gj < 0 { // no jitter
				gj = 0
			}
			var wr []zmux.PipeID
			for _, id := range w.Retire {
				wr = append(wr, zmux.PipeID(id))
			}
			if g.Count != wc || g.DialAfter != us(w.DialAfter) || gi != us(w.ProbeInterval) || gj != wj ||
				g.Recheck != us(w.Recheck) || len(g.RetirePipes)+len(wr) > 0 && !reflect.DeepEqual(g.RetirePipes, wr) || s.n != st.Consumed {
				t.Errorf("%s step %d: %+v consumed %d, want %+v %d", c.Name, i, g, s.n, w, st.Consumed)
			}
		}
	}
}

func TestLimitsParseVectors(t *testing.T) {
	var d struct {
		Parse []struct {
			Name   string          `json:"name"`
			Limits json.RawMessage `json:"limits"`
			Error  string          `json:"error"`
			Want   struct {
				Wire struct {
					Initial uint64 `json:"initial"`
					PerMB   uint64 `json:"per_mb"`
					PerSec  uint64 `json:"per_sec"`
				} `json:"wire"`
				MaxDelay    int64 `json:"max_delay"`
				NoPadding   bool  `json:"no_padding"`
				NoPause     bool  `json:"no_pause"`
				NoInjection bool  `json:"no_injection"`
			} `json:"want"`
		} `json:"parse"`
	}
	readVectors(t, "budget.json", &d)
	for _, c := range d.Parse {
		l, err := ParseLimits(c.Limits)
		if got := classOf(err); got != c.Error {
			t.Errorf("%s: %q, want %q", c.Name, got, c.Error)
			continue
		}
		w := c.Want
		if err == nil && l != (shaping.Limits{Wire: shaping.Account{Initial: w.Wire.Initial, PerMB: w.Wire.PerMB, PerSec: w.Wire.PerSec},
			MaxDelay: time.Duration(w.MaxDelay) * time.Microsecond, NoPadding: w.NoPadding, NoPause: w.NoPause, NoInjection: w.NoInjection}) {
			t.Errorf("%s: %+v", c.Name, l)
		}
	}
}

func TestBudgetDrawVectors(t *testing.T) {
	var d struct {
		Budget []struct {
			Name   string          `json:"name"`
			Budget json.RawMessage `json:"budget"`
			Source srcSpec         `json:"source"`
			Draws  []struct {
				Wire struct {
					Initial uint64 `json:"initial"`
					PerMB   uint64 `json:"per_mb"`
					PerSec  uint64 `json:"per_sec"`
				} `json:"wire"`
				NoInjection bool   `json:"no_injection"`
				MaxDelay    int64  `json:"max_delay"`
				Consumed    uint64 `json:"consumed"`
			} `json:"draws"`
		} `json:"budget"`
	}
	readVectors(t, "format.json", &d)
	for _, c := range d.Budget {
		x, err := decode(c.Budget)
		if err != nil {
			t.Fatal(err)
		}
		b, err := parseBudget(x, &tables{})
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		src := c.Source.make(t)
		for i, w := range c.Draws {
			got := b.draw(src)
			if got.Wire != (shaping.Account{Initial: w.Wire.Initial, PerMB: w.Wire.PerMB, PerSec: w.Wire.PerSec}) || got.NoInjection != w.NoInjection ||
				got.MaxDelay != time.Duration(w.MaxDelay)*time.Microsecond || src.n != w.Consumed {
				t.Errorf("%s draw %d: %+v consumed %d", c.Name, i, got, src.n)
			}
		}
	}
}
