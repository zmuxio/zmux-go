package zmux

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

// A decision whose probe settings are out of range is an error, never
// clamped; set ones pass.
func TestPoolDecisionValidate(t *testing.T) {
	for _, d := range []PoolDecision{{Liveness: PipeLiveness{ProbeInterval: 200 * time.Millisecond}},
		{Liveness: PipeLiveness{ProbeInterval: time.Second - 1}}, {Liveness: PipeLiveness{ProbeJitter: 1}},
		{Liveness: PipeLiveness{ProbeJitter: math.NaN()}}, {Count: -1}, {DialAfter: -time.Second}, {Recheck: -1}} {
		if err := d.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%+v: %v", d, err)
		}
		var got PoolDecision
		err := decide(nil, policyFunc(func(_ *PoolView, x *PoolDecision) { *x = d }), &PoolView{}, &got)
		if !errors.Is(err, ErrInvalidConfig) || got.Liveness.ProbeInterval != d.Liveness.ProbeInterval {
			t.Errorf("decide %+v: %v, %+v", d, err, got)
		}
	}
	for _, d := range []PoolDecision{{}, {Liveness: PipeLiveness{ProbeInterval: time.Second}},
		{Liveness: PipeLiveness{ProbeInterval: time.Hour, ProbeJitter: 0.99}}, {Liveness: PipeLiveness{ProbeJitter: -1}},
		{Liveness: PipeLiveness{ProbeInterval: -time.Second}}, {Liveness: PipeLiveness{ProbeJitter: -0.1}}} {
		if err := d.Validate(); err != nil {
			t.Errorf("%+v: %v", d, err)
		}
	}
}

// An invalid decision is rejected as a whole, the pass going on as with
// the zero decision: the pool keeps its count and probe settings.
func TestMaintainPipesInvalidDecision(t *testing.T) {
	t.Parallel()
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithCancel(context.Background())
	var asked atomic.Int64
	pol := policyFunc(func(_ *PoolView, d *PoolDecision) {
		asked.Add(1)
		d.Count, d.Liveness.ProbeInterval, d.Recheck = 3, 500*time.Millisecond, 20*time.Millisecond
	})
	var rejected atomic.Int64
	report := func(err error) {
		if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("reported %v, want ErrInvalidConfig", err)
		}
		_ = e.client.Pipes() // outside every lock of the session
		rejected.Add(1)
	}
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Policy: pol, Dial: tcpDialer(e.ln.Addr().String()), DecisionRejected: report})
	}()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("MaintainPipes ended with %v, want the cancellation", err)
		}
	}()
	var pp *Pipe
	waitUntil(t, 5*time.Second, "pool pipe", func() bool {
		for _, p := range e.client.Pipes() {
			if st, ok := p.Stats(); ok && p.ID() != 0 && st.Outbound == OutboundActive {
				pp = p
				return true
			}
		}
		return false
	})
	n := asked.Load()
	waitUntil(t, 5*time.Second, "later decisions", func() bool { return asked.Load() > n+3 })
	if got := len(e.client.Pipes()); got != 2 {
		t.Fatalf("%d pipes; the rejected Count 3 applied", got)
	}
	if r := rejected.Load(); r < n+3 {
		t.Fatalf("%d rejections reported for more than %d decisions", r, n+3)
	}
	e.client.lock()
	pi, _ := e.client.s.PipeInfo(uint64(pp.ID()))
	e.client.unlock()
	if k := time.Duration(pi.ProbeInterval); k != 15*time.Second {
		t.Fatalf("probe interval %v; the rejected 500 ms applied", k)
	}
}

func TestPoolDecisionProbeOptions(t *testing.T) {
	o := (&PoolDecision{Liveness: PipeLiveness{ProbeInterval: -1, ProbeJitter: -1}}).probeOptions(PipeOptions{Liveness: PipeLiveness{ProbeInterval: 5 * time.Second, ProbeJitter: 0.2}, Label: "x"})
	if o.Liveness.ProbeInterval >= 0 || o.Liveness.ProbeJitter >= 0 || o.Label != "x" {
		t.Fatalf("options %+v", o)
	}
	o = (&PoolDecision{}).probeOptions(PipeOptions{Liveness: PipeLiveness{ProbeInterval: 5 * time.Second, ProbeJitter: 0.2}})
	if o.Liveness.ProbeInterval != 5*time.Second || o.Liveness.ProbeJitter != 0.2 {
		t.Fatalf("unset decision changed the options: %+v", o)
	}
}
