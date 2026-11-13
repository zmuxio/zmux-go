package carrier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/varint"
)

func TestPool(t *testing.T) {
	n := 0
	closed := map[int]bool{}
	p := &Pool[int]{
		Dial:                  func(context.Context) (int, error) { n++; return n, nil },
		Close:                 func(c int) { closed[c] = true },
		MaxPipesPerConnection: 2, MaxPipesPerConnectionLifetime: 3,
	}
	ctx := context.Background()
	get := func() *Lease[int] {
		l, err := p.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	a, b := get(), get()
	if a.Conn != 1 || b.Conn != 1 || a.FateGroup == 0 || a.FateGroup != b.FateGroup {
		t.Fatalf("two pipes on one connection: %+v %+v", a, b)
	}
	c := get() // at most 2 at once: a second connection
	if c.Conn != 2 || c.FateGroup == a.FateGroup {
		t.Fatalf("third pipe %+v", c)
	}
	a.Release()
	a.Release() // idempotent
	d := get()  // connection 1 (lifetime room 1) or 2: both have room 1
	e := get()
	if d.Conn == e.Conn {
		t.Fatalf("both on %d", d.Conn)
	}
	if _, err := p.Get(ctx); err != nil { // both full: a third connection
		t.Fatal(err)
	}
	// Connection 1 carried its 3 pipes: retired, closed when idle.
	for _, l := range []*Lease[int]{b, c, d, e} {
		l.Release()
	}
	if !closed[1] {
		t.Fatal("retired idle connection not closed")
	}
	p.Shutdown()
	if !closed[2] {
		t.Fatal("Shutdown left an idle connection open")
	}
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("Get after Shutdown")
	}
	// Age: an old connection takes no new pipe.
	p2 := &Pool[int]{Dial: func(context.Context) (int, error) { n++; return n, nil }, MaxConnectionAge: time.Millisecond}
	x, _ := p2.Get(ctx)
	time.Sleep(2 * time.Millisecond)
	if y, _ := p2.Get(ctx); y.Conn == x.Conn {
		t.Fatal("aged connection reused")
	}
}

func TestPoolFateFitsTheWireWithoutChangingItsValue(t *testing.T) {
	n := 0
	p := &Pool[int]{Dial: func(context.Context) (int, error) { n++; return n, nil }, MaxPipesPerConnectionLifetime: 1}
	defer p.Shutdown()
	for range 32 {
		lease, err := p.Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		group := lease.FateGroup
		lease.Release()
		decoded, used := varint.ConsumeVarint(varint.AppendVarint(nil, group))
		if group == 0 || group > varint.MaxVarint || used == 0 || decoded != group {
			t.Fatalf("pool group %#x is not preserved by its wire encoding (%#x)", group, decoded)
		}
	}
}

func TestPoolShutdownAndCancelledContextDoNotDial(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		calls := 0
		p := &Pool[int]{Dial: func(context.Context) (int, error) { calls++; return calls, nil }}
		ctx := context.Background()
		want := errPoolClosed
		if shutdown {
			p.Shutdown()
		} else {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
			want = context.Canceled
		}
		lease, err := p.Get(ctx)
		if lease != nil {
			lease.Release()
		}
		p.Shutdown()
		if !errors.Is(err, want) || calls != 0 {
			t.Fatalf("shutdown=%v: err=%v dials=%d", shutdown, err, calls)
		}
	}
}

func TestPoolCancelledLateDialReturnsItsConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closed := 0
	p := &Pool[int]{Dial: func(context.Context) (int, error) { cancel(); return 1, nil }, Close: func(int) { closed++ }}
	lease, err := p.Get(ctx)
	if lease != nil {
		lease.Release()
	}
	p.Shutdown()
	if !errors.Is(err, context.Canceled) || closed != 1 {
		t.Fatalf("late cancelled dial: err=%v closes=%d", err, closed)
	}
}

func TestPoolCollectsExpiredIdleConnectionsAndClosesOutsideItsLock(t *testing.T) {
	n, closed := 0, 0
	p := &Pool[int]{Dial: func(context.Context) (int, error) { n++; return n, nil }, MaxConnectionAge: time.Minute}
	p.Close = func(int) {
		closed++
		if !p.mu.TryLock() {
			t.Error("Close callback ran under pool mutex")
		} else {
			p.mu.Unlock()
		}
	}
	defer p.Shutdown()
	first, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	p.mu.Lock()
	p.conns[0].born = time.Now().Add(-time.Hour)
	p.mu.Unlock()
	second, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if closed != 1 || len(p.conns) != 1 {
		t.Fatalf("retired idle state retained: closes=%d entries=%d", closed, len(p.conns))
	}
}

func TestPoolStaleLeaseRetirementCannotCloseTwice(t *testing.T) {
	closed := 0
	p := &Pool[int]{Dial: func(context.Context) (int, error) { return 1, nil }, Close: func(int) { closed++ }, MaxPipesPerConnectionLifetime: 1}
	lease, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	lease.Retire()
	lease.Retire()
	p.Shutdown()
	if closed != 1 {
		t.Fatalf("connection closed %d times", closed)
	}
}

// A decision that fails Validate is rejected as a whole and reported to
// DecisionRejected outside the pool's lock; the pool goes on.
func TestPoolRejectsAndReportsInvalidDecision(t *testing.T) {
	n := 0
	var p *Pool[int]
	var reported []error
	p = &Pool[int]{
		Dial: func(context.Context) (int, error) { n++; return n, nil },
		Policy: policyFunc(func(_ *zmux.PoolView, d *zmux.PoolDecision) {
			d.Count, d.Liveness.ProbeInterval = 3, 500*time.Millisecond
		}),
		DecisionRejected: func(err error) {
			if !p.mu.TryLock() {
				t.Error("reported under the pool's lock")
			} else {
				p.mu.Unlock()
			}
			reported = append(reported, err)
		},
	}
	a, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a.Conn != b.Conn {
		t.Fatalf("connections %d and %d: the rejected Count 3 applied", a.Conn, b.Conn)
	}
	if len(reported) < 2 {
		t.Fatalf("%d rejections reported", len(reported))
	}
	for _, err := range reported {
		if !errors.Is(err, zmux.ErrInvalidConfig) {
			t.Fatalf("reported %v", err)
		}
	}
	a.Release()
	b.Release()
	p.Shutdown()
}
