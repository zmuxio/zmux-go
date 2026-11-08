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

func TestCarrierPoolPolicyCountAndRetire(t *testing.T) {
	var mu sync.Mutex
	var retire []uint64
	var closed []int
	n := 0
	p := &CarrierPool[int]{
		Dial:  func(context.Context) (int, error) { n++; return n, nil },
		Close: func(c int) { mu.Lock(); closed = append(closed, c); mu.Unlock() },
		Policy: policyFunc(func(v *PoolView, d *PoolDecision) {
			d.Count = 2
			d.Retire = append(d.Retire, retire...)
		}),
	}
	ctx := context.Background()
	a, _ := p.Get(ctx)
	b, _ := p.Get(ctx)
	c, _ := p.Get(ctx)
	if a.Conn == b.Conn || n != 2 || c.Conn != a.Conn && c.Conn != b.Conn {
		t.Fatalf("connections %d %d %d, %d dials", a.Conn, b.Conn, c.Conn, n)
	}
	retire = []uint64{b.FateGroup}
	b.Release()
	mu.Lock()
	got := append([]int(nil), closed...)
	mu.Unlock()
	if len(got) != 1 || got[0] != b.Conn {
		t.Fatalf("closed %v", got)
	}
	a.Release()
	c.Release()
}

func TestCarrierPoolPolicyDialAfter(t *testing.T) {
	n := 0
	p := &CarrierPool[int]{
		Dial: func(context.Context) (int, error) { n++; return n, nil },
		Policy: policyFunc(func(v *PoolView, d *PoolDecision) {
			d.Count, d.DialAfter = 3, time.Hour
		}),
	}
	ctx := context.Background()
	a, _ := p.Get(ctx)
	b, _ := p.Get(ctx)
	if n != 1 || a.Conn != b.Conn {
		t.Fatalf("%d dials", n)
	}
}

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
					d.Retire = append(d.Retire, retire)
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
			if p.Pipe != 0 && p.Outbound == OutboundActive {
				out = append(out, p.Pipe)
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

// A connection whose pipe stays in use retires by age when the policy's
// Recheck elapses, with no pipe taken or released: the next pipe goes to a
// new connection while the old one keeps its pipe, and the old one closes
// only when that pipe ends.
func TestCarrierPoolRecheckRetiresBusyConnection(t *testing.T) {
	var mu sync.Mutex
	var closed []int
	decisions := 0
	born := map[uint64]time.Duration{}
	n := 0
	p := &CarrierPool[int]{
		Dial:  func(context.Context) (int, error) { n++; return n, nil },
		Close: func(c int) { mu.Lock(); closed = append(closed, c); mu.Unlock() },
		Policy: policyFunc(func(v *PoolView, d *PoolDecision) {
			decisions++
			d.Count = 1
			for _, m := range v.Members {
				born[m.ID] = m.Age
				if m.Age > 100*time.Millisecond {
					d.Retire = append(d.Retire, m.ID)
				} else if r := 100*time.Millisecond - m.Age + time.Millisecond; d.Recheck == 0 || r < d.Recheck {
					d.Recheck = r
				}
			}
		}),
	}
	ctx := context.Background()
	a, _ := p.Get(ctx)
	retired := func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.conns) == 1 && p.conns[0].retired
	}
	deadline := time.Now().Add(5 * time.Second)
	for !retired() {
		if time.Now().After(deadline) {
			t.Fatal("busy connection not retired by Recheck")
		}
		time.Sleep(5 * time.Millisecond)
	}
	b, _ := p.Get(ctx)
	if b.Conn == a.Conn || n != 2 {
		t.Fatalf("new pipe on connection %d after %d dials", b.Conn, n)
	}
	mu.Lock()
	if len(closed) != 0 {
		t.Fatalf("closed %v before its pipe ended", closed)
	}
	mu.Unlock()
	a.Release()
	mu.Lock()
	if len(closed) != 1 || closed[0] != a.Conn {
		t.Fatalf("closed %v", closed)
	}
	mu.Unlock()
	b.Release()
	p.Shutdown()
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
