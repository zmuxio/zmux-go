package carrier

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

func TestPoolPolicyCountAndRetire(t *testing.T) {
	var mu sync.Mutex
	var retire []uint64
	var closed []int
	n := 0
	p := &ConnPool[int]{
		Dial:  func(context.Context) (int, error) { n++; return n, nil },
		Close: func(c int) { mu.Lock(); closed = append(closed, c); mu.Unlock() },
		Policy: policyFunc(func(_ *zmux.PoolView, d *zmux.PoolDecision) {
			d.Count = 2
			d.RetireConnections = append(d.RetireConnections, retire...)
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

func TestPoolPolicyDialAfter(t *testing.T) {
	n := 0
	p := &ConnPool[int]{
		Dial: func(context.Context) (int, error) { n++; return n, nil },
		Policy: policyFunc(func(_ *zmux.PoolView, d *zmux.PoolDecision) {
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

// A connection whose pipe stays in use retires by age when the policy's
// Recheck elapses, with no pipe taken or released: the next pipe goes to a
// new connection while the old one keeps its pipe, and the old one closes
// only when that pipe ends.
func TestPoolRecheckRetiresBusyConnection(t *testing.T) {
	var mu sync.Mutex
	var closed []int
	decisions := 0
	born := map[uint64]time.Duration{}
	n := 0
	p := &ConnPool[int]{
		Dial:  func(context.Context) (int, error) { n++; return n, nil },
		Close: func(c int) { mu.Lock(); closed = append(closed, c); mu.Unlock() },
		Policy: policyFunc(func(v *zmux.PoolView, d *zmux.PoolDecision) {
			decisions++
			d.Count = 1
			for _, m := range v.Members {
				born[m.FateGroup] = m.Age
				if m.Age > 100*time.Millisecond {
					d.RetireConnections = append(d.RetireConnections, m.FateGroup)
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

type policyFunc func(*zmux.PoolView, *zmux.PoolDecision)

func (f policyFunc) Decide(v *zmux.PoolView, d *zmux.PoolDecision) { f(v, d) }

// TestPoolPolicyCallsBack: the policy runs outside the pool's lock, so a
// policy that calls back into the pool (here retiring a connection) does
// not deadlock; the call made meanwhile keeps the last decision.
func TestPoolPolicyCallsBack(t *testing.T) {
	var p *ConnPool[int]
	n := 0
	p = &ConnPool[int]{
		Dial: func(context.Context) (int, error) { n++; return n, nil },
		Policy: policyFunc(func(v *zmux.PoolView, d *zmux.PoolDecision) {
			d.Count = 1
			if len(v.Members) > 0 {
				p.RetireConn(99) // a connection the pool does not have
			}
		}),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		a, err := p.Get(context.Background())
		if err == nil {
			a.Release()
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a policy calling back into the pool deadlocked it")
	}
}

// TestPoolDialFailureFallsBack: when the policy asks for another
// connection and that dial fails, a connection with room still serves.
func TestPoolDialFailureFallsBack(t *testing.T) {
	n := 0
	p := &ConnPool[int]{
		Dial: func(context.Context) (int, error) {
			n++
			if n > 1 {
				return 0, context.DeadlineExceeded
			}
			return n, nil
		},
		Policy: policyFunc(func(_ *zmux.PoolView, d *zmux.PoolDecision) { d.Count = 2 }),
	}
	ctx := context.Background()
	a, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Get(ctx)
	if err != nil || b.Conn != a.Conn {
		t.Fatalf("second Get after a failed dial: %v", err)
	}
	if p.dialing != 0 {
		t.Fatalf("dialing %d after the dials ended", p.dialing)
	}
}

// An event while a decision is under way is decided on before that
// decision's caller goes on, not left to the next Recheck.
func TestPoolPolicyAsksAgainAfterConcurrentEvent(t *testing.T) {
	var asks atomic.Int32
	var gate chan struct{}
	entered := make(chan struct{}, 1)
	p := &ConnPool[int]{
		Dial: func(context.Context) (int, error) { return 1, nil },
		Policy: policyFunc(func(_ *zmux.PoolView, d *zmux.PoolDecision) {
			d.Count, d.Recheck = 1, time.Hour
			if asks.Add(1) == 1 && gate != nil {
				entered <- struct{}{}
				<-gate
			}
		}),
	}
	defer p.Shutdown()
	ctx := context.Background()
	a, _ := p.Get(ctx)
	b, _ := p.Get(ctx)
	asks.Store(0)
	gate = make(chan struct{})
	done := make(chan *Lease[int])
	go func() {
		l, _ := p.Get(ctx)
		done <- l
	}()
	<-entered
	a.Release() // a member's pipe ended while the Get's decision is under way
	close(gate)
	c := <-done
	if n := asks.Load(); n != 2 {
		t.Fatalf("%d asks, want the Get's and one more for the release", n)
	}
	b.Release()
	c.Release()
}

// A decision with a negative Recheck is invalid: the pool rejects it and
// reports the error.
func TestPoolPolicyNegativeRecheckRejected(t *testing.T) {
	var rejected atomic.Int32
	p := &ConnPool[int]{
		Dial: func(context.Context) (int, error) { return 1, nil },
		Policy: policyFunc(func(_ *zmux.PoolView, d *zmux.PoolDecision) {
			d.Count, d.Recheck = 1, -1
		}),
		DecisionRejected: func(err error) {
			if errors.Is(err, zmux.ErrInvalidConfig) {
				rejected.Add(1)
			}
		},
	}
	defer p.Shutdown()
	a, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a.Release()
	if rejected.Load() == 0 {
		t.Fatal("a negative Recheck was not rejected")
	}
}
