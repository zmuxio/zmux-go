package zmux

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// A pool's idle rule fires while probes run on the member: idle is the
// time since traffic, not since the byte counts last moved.
func TestPoolIdleRuleWithProbes(t *testing.T) {
	t.Parallel()
	e := newMPEnv(t, mpConfig(), mpConfig())
	var retired atomic.Int64
	pol := policyFunc(func(v *PoolView, d *PoolDecision) {
		for _, m := range v.Members {
			if m.Idle > 1500*time.Millisecond {
				d.Retire = append(d.Retire, m.ID)
				retired.Add(1)
			}
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Policy: pol, MinBackoff: 10 * time.Millisecond,
			Options: PipeOptions{ProbeInterval: time.Second}, Dial: tcpDialer(e.ln.Addr().String())})
	}()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("MaintainPipes ended with %v, want the cancellation", err)
		}
	}()
	waitUntil(t, 8*time.Second, "idle member retired", func() bool { return retired.Load() > 0 })
}

// The policy's DialAfter replaces the pool's backoff: a 10 ms DialAfter
// redials long before a 5 s MinBackoff would; a long DialAfter is bounded
// by MaxBackoff.
func TestPoolDialAfterReplacesBackoff(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name       string
		after      time.Duration
		minB, maxB time.Duration
		within     time.Duration
	}{
		{"shorter", 10 * time.Millisecond, 5 * time.Second, time.Minute, 2 * time.Second},
		{"bounded", time.Minute, 50 * time.Millisecond, 100 * time.Millisecond, 2 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			e := newMPEnv(t, mpConfig(), mpConfig())
			var dials atomic.Int64
			dial := tcpDialer(e.ln.Addr().String())
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- e.client.MaintainPipes(ctx, PipePool{MinBackoff: c.minB, MaxBackoff: c.maxB,
					Policy: policyFunc(func(_ *PoolView, d *PoolDecision) { d.DialAfter = c.after }),
					Dial: func(ctx context.Context) (PipeIO, error) {
						if dials.Add(1) <= 3 {
							return PipeIO{}, errors.New("test dial refused")
						}
						return dial(ctx)
					}})
			}()
			defer func() {
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Errorf("MaintainPipes ended with %v, want the cancellation", err)
				}
			}()
			start := time.Now()
			waitUntil(t, c.within, "pool pipe", func() bool {
				for _, st := range e.client.PipeStats() {
					if st.Pipe != 0 && st.Outbound == OutboundActive {
						return true
					}
				}
				return false
			})
			if dials.Load() != 4 {
				t.Fatalf("%d dials after %v", dials.Load(), time.Since(start))
			}
		})
	}
}

// The probe settings of a decision apply to the pipes dialled after it,
// not to the members already there.
func TestPoolProbeSettingsOnNewPipes(t *testing.T) {
	t.Parallel()
	e := newMPEnv(t, mpConfig(), mpConfig())
	var stage atomic.Int64
	pol := policyFunc(func(_ *PoolView, d *PoolDecision) {
		d.Count = 1
		if stage.Load() > 0 {
			d.Count, d.ProbeInterval, d.NoProbeJitter = 2, time.Second, true
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Policy: pol, Dial: tcpDialer(e.ln.Addr().String())})
	}()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("MaintainPipes ended with %v, want the cancellation", err)
		}
	}()
	pool := func() []*Pipe {
		var out []*Pipe
		for _, p := range e.client.Pipes() {
			if st, ok := p.Stats(); ok && p.ID() != 0 && st.Outbound == OutboundActive {
				out = append(out, p)
			}
		}
		return out
	}
	waitUntil(t, 5*time.Second, "first pool pipe", func() bool { return len(pool()) == 1 })
	first := pool()[0]
	stage.Store(1)
	waitUntil(t, 5*time.Second, "second pool pipe", func() bool { return len(pool()) == 2 })
	var second *Pipe
	for _, p := range pool() {
		if p.ID() != first.ID() {
			second = p
		}
	}
	if second == nil {
		t.Fatal("the second pool pipe is the first one")
	}
	probe := func(p *Pipe) (time.Duration, float64) {
		e.client.lock()
		defer e.client.unlock()
		pi, _ := e.client.s.PipeInfo(p.ID())
		return time.Duration(pi.ProbeInterval), pi.ProbeJitter
	}
	if k, j := probe(second); k != time.Second || j != 0 {
		t.Fatalf("new pipe: probe interval %v, jitter %v; want 1 s, 0", k, j)
	}
	if k, j := probe(first); k != 15*time.Second || j != 0.25 {
		t.Fatalf("earlier member: probe interval %v, jitter %v; want the session's 15 s, 0.25", k, j)
	}
	b2 := pipeStat(t, second).PingsSent
	time.Sleep(3500 * time.Millisecond)
	if n := pipeStat(t, second).PingsSent - b2; n < 2 {
		t.Fatalf("new pipe: %d probes in 3.5 s at 1 s", n)
	}
}

// The built-in backoff waits lie in [0.8 b, b] and spread over it.
func TestBackoffWaitJitter(t *testing.T) {
	const b = 10 * time.Second
	var low, high bool
	for range 1000 {
		w := backoffWait(b)
		if w < 8*time.Second || w > b {
			t.Fatalf("wait %v outside [8 s, 10 s]", w)
		}
		low = low || w < 8200*time.Millisecond
		high = high || w > 9800*time.Millisecond
	}
	if !low || !high {
		t.Fatalf("draws near the ends: %v %v", low, high)
	}
}
