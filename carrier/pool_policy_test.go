package carrier

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

func TestPoolPolicyCountAndRetire(t *testing.T) {
	var mu sync.Mutex
	var retire []uint64
	var closed []int
	n := 0
	p := &Pool[int]{
		Dial:  func(context.Context) (int, error) { n++; return n, nil },
		Close: func(c int) { mu.Lock(); closed = append(closed, c); mu.Unlock() },
		Policy: policyFunc(func(v *zmux.PoolView, d *zmux.PoolDecision) {
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
	p := &Pool[int]{
		Dial: func(context.Context) (int, error) { n++; return n, nil },
		Policy: policyFunc(func(v *zmux.PoolView, d *zmux.PoolDecision) {
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
	p := &Pool[int]{
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
