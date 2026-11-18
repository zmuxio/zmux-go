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

func (f policyFunc) Decide(v *PoolView, d *PoolDecision) error { f(v, d); return nil }

func TestMaintainPipesPolicy(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	count, retire := 2, uint64(0)
	var views []int
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Count: 1, Backoff: Backoff{Min: 10 * time.Millisecond},
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

func (c *countPolicy) Decide(_ *PoolView, d *PoolDecision) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	d.Recheck = c.recheck
	return nil
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
		{30 * time.Millisecond, 10, 40}, // ~20 in 600 ms; the 100 ms interval alone gives 6
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

// TestRecheckAfter: a zero Recheck asks for no recheck beyond the pool's
// maintenance interval (100 ms); a positive one is floored at MinRecheck,
// and the interval looks again before a longer one.
func TestRecheckAfter(t *testing.T) {
	for _, c := range []struct{ r, want time.Duration }{
		{0, 100 * time.Millisecond}, {time.Nanosecond, MinRecheck}, {30 * time.Millisecond, 30 * time.Millisecond}, {time.Hour, poolLook},
	} {
		d := PoolDecision{Recheck: c.r}
		if got := d.RecheckAfter(); got != c.want {
			t.Errorf("Recheck %v: RecheckAfter %v, want %v", c.r, got, c.want)
		}
	}
	if err := (&PoolDecision{Recheck: -1}).Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative Recheck: %v", err)
	}
}

// TestMaintainPipesPolicyView: the view holds SinceLastDial (negative before
// the first dial), Usable members and the Replacing member of a rotation;
// when Count drops, the member the policy retires closes first.
func TestMaintainPipesPolicyView(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var first *PoolView
	var last PoolView
	count, retire := 2, PipeID(0)
	replacing := map[PipeID]bool{}
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Count: 2, Backoff: Backoff{Min: 10 * time.Millisecond},
			Policy: policyFunc(func(v *PoolView, d *PoolDecision) {
				mu.Lock()
				defer mu.Unlock()
				if first == nil {
					first = &PoolView{SinceLastDial: v.SinceLastDial, Members: append([]PoolMember(nil), v.Members...)}
				}
				last = PoolView{SinceLastDial: v.SinceLastDial, Failures: v.Failures, Members: append([]PoolMember(nil), v.Members...)}
				for _, m := range v.Members {
					replacing[m.PipeID] = replacing[m.PipeID] || m.Replacing
				}
				d.Count = count
				if retire != 0 {
					d.RetirePipes = append(d.RetirePipes, retire)
				}
			}),
			Dial: tcpDialer(e.ln.Addr().String())})
	}()
	waitView := func(ok func(v *PoolView) bool) PoolView {
		deadline := time.Now().Add(5 * time.Second)
		for {
			mu.Lock()
			v := last
			mu.Unlock()
			if ok(&v) {
				return v
			}
			if time.Now().After(deadline) {
				t.Fatalf("view %+v", v)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	v := waitView(func(v *PoolView) bool {
		return len(v.Members) == 2 && v.Members[0].Usable && v.Members[1].Usable
	})
	mu.Lock()
	if first.SinceLastDial >= 0 || len(first.Members) != 0 || v.SinceLastDial < 0 || v.Failures != 0 {
		t.Errorf("first view %+v, view %+v", first, v)
	}
	keep, drop := v.Members[0].PipeID, v.Members[1].PipeID
	// A rotation: the member is Replacing until its replacement takes over.
	retire = keep
	mu.Unlock()
	waitView(func(v *PoolView) bool {
		for _, m := range v.Members {
			if m.PipeID == keep {
				return false
			}
		}
		return len(v.Members) == 2
	})
	mu.Lock()
	if !replacing[keep] {
		t.Errorf("member %d never Replacing", keep)
	}
	// Count drops to 1 with the other member retired: that one closes, not
	// the longer idle replacement.
	count, retire = 1, drop
	mu.Unlock()
	v = waitView(func(v *PoolView) bool { return len(v.Members) == 1 })
	if v.Members[0].PipeID == drop {
		t.Errorf("retired member %d kept", drop)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("MaintainPipes returned %v", err)
	}
}
