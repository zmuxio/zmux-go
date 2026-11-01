package machine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

type poolVectors struct {
	Version int `json:"version"`
	Parse   []struct {
		Name   string          `json:"name"`
		Policy json.RawMessage `json:"policy"`
		Raw    string          `json:"raw"` // the input, when policy cannot express it
		OK     bool            `json:"ok"`
		Error  string          `json:"error"` // a substring of the error, any case
	} `json:"parse"`
	Decide []struct {
		Name   string          `json:"name"`
		Policy json.RawMessage `json:"policy"`
		Steps  []struct {
			Failures  int      `json:"failures"`
			SinceDial *float64 `json:"since_dial"`
			Members   []struct {
				ID            uint64  `json:"id"`
				Age           float64 `json:"age"`
				Idle          float64 `json:"idle"`
				Pipes         float64 `json:"pipes"`
				BytesSent     float64 `json:"bytes_sent"`
				BytesReceived float64 `json:"bytes_received"`
			} `json:"members"`
			Want map[string]json.RawMessage `json:"want"`
		} `json:"steps"`
	} `json:"decide"`
}

// poolVectorsSHA256 pins the copy of the shared vectors (version 3); a
// changed file is synced from the shared source together with this value.
const poolVectorsSHA256 = "29c1492463b17251d37269b9c80f81b180aa448265e4ee9653d5755e23a1a228"

func readPoolVectors(t *testing.T) *poolVectors {
	b, err := os.ReadFile("../../testdata/shaping/pool_policy.json")
	if err != nil {
		t.Fatal(err)
	}
	if h := sha256.Sum256(b); hex.EncodeToString(h[:]) != poolVectorsSHA256 {
		t.Fatalf("pool_policy.json sha256 %x, want %s", h, poolVectorsSHA256)
	}
	var v poolVectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	if v.Version != 3 {
		t.Fatalf("version %d", v.Version)
	}
	return &v
}

func poolSeeded(seed uint64) Options {
	return Options{NewSource: func() rand.Source { return rand.NewPCG(seed, seed) }}
}

func secs(x float64) time.Duration { return time.Duration(x * 1e9) }

func TestPoolPolicyParseVectors(t *testing.T) { testPoolParse(t, readPoolVectors(t)) }

func testPoolParse(t *testing.T, vs *poolVectors) {
	for _, c := range vs.Parse {
		in := []byte(c.Raw)
		if c.Raw == "" {
			in = c.Policy
		}
		_, err := PoolPolicy(in, poolSeeded(1))
		if (err == nil) != c.OK || err != nil && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(c.Error)) {
			t.Errorf("%s: %v, want ok %v error %q", c.Name, err, c.OK, c.Error)
		}
	}
}

func TestPoolPolicyDecideVectors(t *testing.T) { testPoolDecide(t, readPoolVectors(t)) }

func testPoolDecide(t *testing.T, vs *poolVectors) {
	for _, seed := range []uint64{1, 2, 3} {
		for _, c := range vs.Decide {
			p, err := PoolPolicy(c.Policy, poolSeeded(seed))
			if err != nil {
				t.Fatalf("%s: %v", c.Name, err)
			}
			var got []zmux.PoolDecision
			for i, s := range c.Steps {
				v := &zmux.PoolView{Now: time.Now(), Failures: s.Failures}
				if s.SinceDial != nil {
					v.LastDial = v.Now.Add(-secs(*s.SinceDial))
				}
				for _, m := range s.Members {
					v.Members = append(v.Members, zmux.PoolMember{ID: m.ID, Age: secs(m.Age), Idle: secs(m.Idle),
						Pipes: int(m.Pipes), BytesSent: uint64(m.BytesSent), BytesReceived: uint64(m.BytesReceived)})
				}
				var d zmux.PoolDecision
				p.Decide(v, &d)
				fail := func(k string) { t.Errorf("%s step %d seed %d: %s: got %+v", c.Name, i, seed, k, d) }
				num := func(k string) (float64, bool) {
					raw, ok := s.Want[k]
					if !ok {
						return 0, false
					}
					var x *float64
					if err := json.Unmarshal(raw, &x); err != nil {
						t.Fatal(err)
					}
					if x == nil { // null: the pool's own value
						return 0, true
					}
					return *x, true
				}
				span := func(k string) (a, b float64, ok bool) {
					raw, ok := s.Want[k]
					if !ok {
						return 0, 0, false
					}
					var r [2]float64
					if err := json.Unmarshal(raw, &r); err != nil {
						t.Fatal(err)
					}
					return r[0], r[1], true
				}
				if x, ok := num("count"); ok && float64(d.Count) != x {
					fail("count")
				}
				if x, ok := num("dial_after"); ok && d.DialAfter != secs(x) {
					fail("dial_after")
				}
				if raw, ok := s.Want["retire"]; ok {
					var r []uint64
					if err := json.Unmarshal(raw, &r); err != nil {
						t.Fatal(err)
					}
					if !slices.Equal(d.Retire, r) {
						fail("retire")
					}
				}
				if a, b, ok := span("count_in"); ok && (float64(d.Count) < a || float64(d.Count) > b) {
					fail("count_in")
				}
				if a, b, ok := span("dial_after_in"); ok && (d.DialAfter < secs(a) || d.DialAfter > secs(b)) {
					fail("dial_after_in")
				}
				// Recheck 0 is "none", as null in the vectors.
				if x, ok := num("recheck"); ok && math.Abs(d.Recheck.Seconds()-x) >= 1e-6 {
					fail("recheck")
				}
				if a, b, ok := span("recheck_in"); ok && (d.Recheck <= 0 || d.Recheck.Seconds() < a || d.Recheck.Seconds() > b) {
					fail("recheck_in")
				}
				if raw, ok := s.Want["probe_interval"]; ok {
					var x *float64
					if err := json.Unmarshal(raw, &x); err != nil {
						t.Fatal(err)
					}
					if x == nil && (d.ProbeInterval != 0 || d.DisableIdleProbe) || x != nil && (d.ProbeInterval != secs(*x) || d.DisableIdleProbe != (*x == 0)) {
						fail("probe_interval")
					}
				}
				if a, b, ok := span("probe_interval_in"); ok && (d.ProbeInterval < secs(a) || d.ProbeInterval > secs(b)) {
					fail("probe_interval_in")
				}
				if raw, ok := s.Want["probe_jitter"]; ok {
					var x *float64
					if err := json.Unmarshal(raw, &x); err != nil {
						t.Fatal(err)
					}
					switch {
					case x == nil && (d.ProbeJitter != 0 || d.NoProbeJitter), x != nil && *x == 0 && !d.NoProbeJitter,
						x != nil && *x != 0 && (d.ProbeJitter != *x || d.NoProbeJitter):
						fail("probe_jitter")
					}
				}
				if raw, ok := s.Want["dial_after_same_as"]; ok {
					var j int
					if err := json.Unmarshal(raw, &j); err != nil {
						t.Fatal(err)
					}
					if d.DialAfter != got[j].DialAfter {
						fail("dial_after_same_as")
					}
				}
				if raw, ok := s.Want["dial_after_redrawn_from"]; ok {
					var j int
					if err := json.Unmarshal(raw, &j); err != nil {
						t.Fatal(err)
					}
					if d.DialAfter == got[j].DialAfter {
						fail("dial_after_redrawn_from")
					}
				}
				if raw, ok := s.Want["same_as"]; ok {
					var j int
					if err := json.Unmarshal(raw, &j); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(d, got[j]) {
						fail("same_as")
					}
				}
				for k := range s.Want {
					switch k {
					case "count", "dial_after", "retire", "recheck", "count_in", "dial_after_in", "recheck_in", "same_as",
						"probe_interval", "probe_interval_in", "probe_jitter", "dial_after_same_as", "dial_after_redrawn_from":
					default:
						t.Fatalf("%s: unknown want %q", c.Name, k)
					}
				}
				got = append(got, d)
			}
		}
	}
}

func decideOnce(p zmux.PoolPolicy, failures int, ms ...zmux.PoolMember) zmux.PoolDecision {
	var d zmux.PoolDecision
	p.Decide(&zmux.PoolView{Now: time.Now(), Failures: failures, Members: ms}, &d)
	return d
}

func TestPoolPolicyScopes(t *testing.T) {
	// each: drawn at every Decide.
	p, err := PoolPolicy([]byte(`{"dial_after":[{"range":[0,1000]}]}`), poolSeeded(5))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[time.Duration]bool{}
	for range 20 {
		seen[decideOnce(p, 0).DialAfter] = true
	}
	if len(seen) < 2 {
		t.Fatalf("each scope drawn once: %v", seen)
	}
	// shared: one draw per policy, also across tiers holding the same Value.
	p, _ = PoolPolicy([]byte(`{"count":{"range":[1,1e6],"scope":"shared"}}`), poolSeeded(5))
	first := decideOnce(p, 0).Count
	for range 10 {
		if c := decideOnce(p, 3).Count; c != first {
			t.Fatalf("shared count %d then %d", first, c)
		}
	}
	// pipe: one threshold per member ID, dropped when the member leaves.
	p, _ = PoolPolicy([]byte(`{"retire":[{"metric":"age","above":{"range":[0,100],"scope":"pipe"}}]}`), poolSeeded(5))
	pp := p.(*poolPolicy)
	m := func(id uint64) zmux.PoolMember { return zmux.PoolMember{ID: id, Age: secs(50)} }
	decideOnce(p, 0, m(1), m(2))
	t1, t2 := pp.members[1].vals, pp.members[2].vals
	if len(t1) != 1 || len(t2) != 1 || reflect.DeepEqual(t1, t2) {
		t.Fatalf("thresholds %v %v", t1, t2)
	}
	keep := maps(t1)
	for range 10 {
		decideOnce(p, 0, m(1), m(2))
	}
	if !reflect.DeepEqual(maps(pp.members[1].vals), keep) {
		t.Fatal("pipe threshold redrawn")
	}
	decideOnce(p, 0, m(2))
	if _, ok := pp.members[1]; ok || len(pp.members) != 1 {
		t.Fatalf("member 1 kept after leaving: %v", pp.members)
	}
	decideOnce(p, 0, m(1), m(2))
	if reflect.DeepEqual(maps(pp.members[1].vals), keep) {
		t.Fatal("returning member kept its old threshold") // a draw equal by chance is negligible
	}
}

func maps(m map[*Value]float64) []float64 {
	var out []float64
	for _, x := range m {
		out = append(out, x)
	}
	slices.Sort(out)
	return out
}

func TestPoolPolicyErrors(t *testing.T) {
	for _, c := range []struct{ raw, want string }{
		{`{"count":{"const":1,"scope":"pipe"}}`, "scope pipe only in retire"},
		{`{"dial_after":[{"dist":{"kind":"mixture","of":[{"const":1,"scope":"pipe"}],"weights":[1]}}]}`, "scope pipe only in retire"},
		{`{"retire":[{"metric":"lifetime","above":{"const":1}}]}`, "unknown metric"},
		{`{"count":{"const":1,"scope":"session"}}`, "unknown scope"},
		{`{"count":{"const":1}} {}`, "data after"},
	} {
		_, err := PoolPolicy([]byte(c.raw), Options{})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", c.raw, err)
		}
	}
	if _, err := PoolPolicy([]byte(`{}`), Options{Limits: SizeLimits{MaxBytes: 1}}); err == nil {
		t.Error("byte limit not applied")
	}
	if _, err := PoolPolicy([]byte(`{"count":{"dist":{"kind":"bag","points":[1,2,3]}}}`),
		Options{Limits: SizeLimits{MaxPoints: 2}}); err == nil {
		t.Error("point limit not applied")
	}
}

func TestPoolPolicySeedDeterminism(t *testing.T) {
	const raw = `{"count":{"dist":{"kind":"poisson","params":[2]},"min":1,"max":6},
		"dial_after":[{"dist":{"kind":"weibull","params":[3,1.5]}},{"range":[1,9]}],
		"retire":[{"metric":"bytes","above":{"dist":{"kind":"lognormal","params":[5,1]},"scope":"pipe"}}]}`
	run := func(seed uint64) []zmux.PoolDecision {
		p, err := PoolPolicy([]byte(raw), poolSeeded(seed))
		if err != nil {
			t.Fatal(err)
		}
		var out []zmux.PoolDecision
		for i := range 30 {
			out = append(out, decideOnce(p, i%3, zmux.PoolMember{ID: uint64(i % 4), BytesSent: uint64(i * 40)},
				zmux.PoolMember{ID: 9, BytesReceived: 150}))
		}
		return out
	}
	if a, b := run(11), run(11); !reflect.DeepEqual(a, b) {
		t.Fatal("same seed, different decisions")
	}
	if a, b := run(11), run(12); reflect.DeepEqual(a, b) {
		t.Fatal("different seeds, same decisions")
	}
}

// TestPoolPolicyPipePool runs a policy in a PipePool: it keeps count pipes
// and replaces each pipe older than the age threshold.
func TestPoolPolicyPipePool(t *testing.T) {
	pol, err := PoolPolicy([]byte(`{"count":{"const":2},"dial_after":[{"const":0.01}],
		"retire":[{"metric":"age","above":{"const":0.3}}]}`), Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := func() *zmux.Config {
		return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32)},
			SchedulerParams: zmux.SchedulerParams{NoProbation: true}}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: cfg(), InstanceID: []byte("instance")})
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = lst.HandlePipe(ctx, zmux.NetPipe(c), zmux.JoinHint{}) }()
		}
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client, err := zmux.Client(ctx, nc, cfg())
	if err != nil {
		t.Fatal(err)
	}
	server, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pctx, pcancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- client.MaintainPipes(pctx, zmux.PipePool{Count: 1, MinBackoff: 10 * time.Millisecond, Policy: pol,
			Dial: func(context.Context) (zmux.PipeIO, error) {
				c, err := net.Dial("tcp", ln.Addr().String())
				if err != nil {
					return zmux.PipeIO{}, err
				}
				return zmux.NetPipe(c), nil
			}})
	}()
	defer func() {
		pcancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("MaintainPipes returned %v", err)
		}
		_ = client.Close()
		_ = server.Close()
		_ = ln.Close()
		_ = lst.Close()
		<-acceptDone
		cancel()
	}()
	ids := func() []uint64 {
		var out []uint64
		for _, p := range client.PipeStats() {
			if p.Pipe != 0 && p.Outbound == zmux.OutboundActive {
				out = append(out, p.Pipe)
			}
		}
		return out
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(ids()) != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("%d pool pipes, want 2", len(ids()))
		}
		time.Sleep(10 * time.Millisecond)
	}
	first := ids()
	for {
		cur := ids()
		replaced := len(cur) == 2
		for _, id := range cur {
			replaced = replaced && !slices.Contains(first, id)
		}
		if replaced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pipes %v not replaced: %v", first, cur)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPoolPolicyRecheck(t *testing.T) {
	p, err := PoolPolicy([]byte(`{"dial_after":[{"const":3}],"retire":[
		{"metric":"age","above":{"const":100}},{"metric":"idle","above":{"const":50}},
		{"metric":"bytes","above":{"const":10}}]}`), Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	decide := func(last time.Time, ms ...zmux.PoolMember) zmux.PoolDecision {
		var d zmux.PoolDecision
		p.Decide(&zmux.PoolView{Now: now, LastDial: last, Members: ms}, &d)
		return d
	}
	ms := []zmux.PoolMember{{ID: 1, Age: secs(40), Idle: secs(45)}, {ID: 2, Age: secs(99)}, {ID: 3, Age: secs(200)}}
	if d := decide(time.Time{}, ms...); d.Recheck != secs(1) || !slices.Equal(d.Retire, []uint64{3}) {
		t.Fatalf("members: %+v", d)
	}
	if d := decide(now.Add(-secs(2.5)), ms[0]); d.Recheck != secs(0.5) {
		t.Fatalf("dial_after: %+v", d)
	}
	if d := decide(time.Time{}); d.Recheck != 0 {
		t.Fatalf("no member, no dial: %+v", d)
	}
	p, _ = PoolPolicy([]byte(`{"retire":[{"metric":"bytes","above":{"const":10}},{"metric":"pipes","above":{"const":1}}]}`), Options{})
	if d := decide(time.Time{}, ms...); d.Recheck != 0 {
		t.Fatalf("byte rules: %+v", d)
	}
}

// TestPoolPolicyCarrierPoolIdle closes an idle connection of a CarrierPool
// by an idle rule, with no pipe taken or released after it went idle.
func TestPoolPolicyCarrierPoolIdle(t *testing.T) {
	pol, err := PoolPolicy([]byte(`{"retire":[{"metric":"idle","above":{"const":0.1}}]}`), Options{})
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan int, 2)
	n := 0
	p := &zmux.CarrierPool[int]{Dial: func(context.Context) (int, error) { n++; return n, nil },
		Close: func(c int) { closed <- c }, PoolLimits: zmux.PoolLimits{Policy: pol}}
	l, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	select {
	case c := <-closed:
		if c != 1 {
			t.Fatalf("closed %d", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("idle connection not closed")
	}
	p.Shutdown()
}

// A dial wait keeps its jitter draw until a new wait starts: a new dial
// (the time since the last one got shorter) or another failure count.
func TestPoolPolicyDialJitterWaits(t *testing.T) {
	p, err := PoolPolicy([]byte(`{"dial_after":[{"const":10}],"dial_jitter":0.5}`), poolSeeded(3))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	at := func(failures int, since float64) time.Duration {
		var d zmux.PoolDecision
		p.Decide(&zmux.PoolView{Now: now, Failures: failures, LastDial: now.Add(-secs(since))}, &d)
		if d.DialAfter < 5*time.Second || d.DialAfter > 10*time.Second {
			t.Fatalf("DialAfter %v outside [5 s, 10 s]", d.DialAfter)
		}
		return d.DialAfter
	}
	w := at(0, 1)
	if at(0, 2) != w || at(0, 3) != w {
		t.Fatal("redrawn within a wait")
	}
	seen := map[time.Duration]bool{w: true}
	for i := range 20 {
		seen[at(0, 0.5)] = true // a new dial each time: shorter, then longer
		at(0, 1)
		seen[at(i%2+1, 1)] = true // another failure count
	}
	if len(seen) < 10 {
		t.Fatalf("%d distinct waits in 41 new waits", len(seen))
	}
}

// A retire rule's jitter is drawn once per member while it stays in the
// view, and anew when it returns.
func TestPoolPolicyRetireJitterPerMember(t *testing.T) {
	p, err := PoolPolicy([]byte(`{"retire":[{"metric":"age","above":{"const":100},"jitter":0.9}]}`), poolSeeded(4))
	if err != nil {
		t.Fatal(err)
	}
	pp := p.(*poolPolicy)
	m := func(id uint64) zmux.PoolMember { return zmux.PoolMember{ID: id, Age: secs(1)} }
	decideOnce(p, 0, m(1), m(2))
	u1, u2 := pp.members[1].u[0], pp.members[2].u[0]
	if u1 == u2 {
		t.Fatal("members share a draw")
	}
	for range 5 {
		decideOnce(p, 0, m(1), m(2))
	}
	if pp.members[1].u[0] != u1 || pp.members[2].u[0] != u2 {
		t.Fatal("draw changed while the member stayed")
	}
	decideOnce(p, 0, m(2))
	decideOnce(p, 0, m(1), m(2))
	if pp.members[1].u[0] == u1 {
		t.Fatal("a returning member kept its draw")
	}
	// The recheck uses the jittered threshold: 100 x (1 - 0.9 u) - 1.
	d := decideOnce(p, 0, m(2))
	if want := secs(100*(1-0.9*u2) - 1); d.Recheck-want > time.Microsecond || want-d.Recheck > time.Microsecond {
		t.Fatalf("recheck %v, want %v", d.Recheck, want)
	}
}
