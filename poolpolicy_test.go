package zmux

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

type policyFunc func(*PoolView, *PoolDecision)

func (f policyFunc) Decide(v *PoolView, d *PoolDecision) { f(v, d) }

func TestMaintainPipesPolicy(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	count, retire := 2, uint64(0)
	var views []int
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Count: 1, MinBackoff: 10 * time.Millisecond,
			Policy: policyFunc(func(v *PoolView, d *PoolDecision) {
				mu.Lock()
				defer mu.Unlock()
				views = append(views, len(v.Members))
				d.Count = count
				if retire != 0 {
					d.RetirePipes = append(d.RetirePipes, PipeID(retire))
					retire = 0
				}
			}),
			Dial: func(context.Context) (PipeIO, error) {
				nc, err := net.Dial("tcp", e.ln.Addr().String())
				if err != nil {
					return PipeIO{}, err
				}
				return NetPipe(nc), nil
			}})
	}()
	ids := func() []uint64 {
		var out []uint64
		for _, p := range e.client.PipeStats() {
			if p.PipeID != 0 && p.Outbound == OutboundActive {
				out = append(out, uint64(p.PipeID))
			}
		}
		return out
	}
	wait := func(want int) []uint64 {
		deadline := time.Now().Add(5 * time.Second)
		for len(ids()) != want {
			if time.Now().After(deadline) {
				t.Fatalf("%d pool pipes, want %d", len(ids()), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
		return ids()
	}
	first := wait(2)
	mu.Lock()
	count, retire = 2, first[0]
	mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		cur := ids()
		gone := true
		for _, id := range cur {
			gone = gone && id != first[0]
		}
		if gone && len(cur) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pipe %d not replaced: %v", first[0], cur)
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	count = 1
	mu.Unlock()
	wait(1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("MaintainPipes returned %v", err)
	}
}

// countPolicy counts its decisions and asks for a fixed Recheck.
type countPolicy struct {
	mu      sync.Mutex
	n       int
	recheck time.Duration
}

func (c *countPolicy) Decide(_ *PoolView, d *PoolDecision) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	d.Recheck = c.recheck
}

func (c *countPolicy) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// The pool decides again within Recheck when it is shorter than its own
// interval, and no faster than MinRecheck.
func TestMaintainPipesRecheck(t *testing.T) {
	leakcheck.Check(t)
	for _, c := range []struct {
		recheck  time.Duration
		min, max int
	}{
		{30 * time.Millisecond, 10, 40}, // ~20 in 600 ms; the 250 ms interval alone gives 3
		{time.Nanosecond, 10, 80},       // floored at 10 ms: ~60
	} {
		e := newMPEnv(t, mpConfig(), mpConfig())
		ctx, cancel := context.WithCancel(context.Background())
		pol := &countPolicy{recheck: c.recheck}
		e.addPipe(t, PipeOptions{})
		done := make(chan error, 1)
		go func() {
			done <- e.client.MaintainPipes(ctx, PipePool{Count: 0, Policy: pol,
				Dial: func(context.Context) (PipeIO, error) { return PipeIO{}, errors.New("no dial") }})
		}()
		time.Sleep(600 * time.Millisecond)
		got := pol.count()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("MaintainPipes ended with %v, want the cancellation", err)
		}
		if got < c.min || got > c.max {
			t.Errorf("recheck %v: %d decisions in 600 ms, want %d..%d", c.recheck, got, c.min, c.max)
		}
	}
}
