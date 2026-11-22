package carrier

import (
	"context"
	"errors"
	"slices"
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
	p.settle()
	b, _ := p.Get(ctx)
	p.settle()
	c, _ := p.Get(ctx)
	p.settle()
	if a.Conn == b.Conn || n != 2 || c.Conn != a.Conn && c.Conn != b.Conn {
		t.Fatalf("connections %d %d %d, %d dials", a.Conn, b.Conn, c.Conn, n)
	}
	retire = []uint64{b.FateGroup}
	b.Release()
	p.settle()
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
	p.settle()
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

func (f policyFunc) Decide(v *zmux.PoolView, d *zmux.PoolDecision) error { f(v, d); return nil }

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
	p.settle()
	b, err := p.Get(ctx)
	if err != nil || b.Conn != a.Conn {
		t.Fatalf("second Get after a failed dial: %v", err)
	}
	if len(p.dials) != 0 {
		t.Fatalf("%d dials under way after the dials ended", len(p.dials))
	}
}

// settle waits until the pool's decide goroutine has ended, that is, until
// every look due has been asked and applied.
func (p *ConnPool[C]) settle() {
	p.mu.Lock()
	p.awaitIdleLocked()
	p.mu.Unlock()
}

// A slow or blocking policy delays neither Get nor Release: both return
// while the look is under way, the events that come meanwhile coalesce into
// one more look, and the latest decision applies after the look returns.
func TestPoolPolicyNeverBlocksCallers(t *testing.T) {
	var asks atomic.Int32
	var count atomic.Int32
	count.Store(1)
	entered := make(chan struct{}, 4)
	gate := make(chan struct{})
	var once sync.Once
	n := 0
	p := &ConnPool[int]{
		Dial: func(context.Context) (int, error) { n++; return n, nil },
		Policy: policyFunc(func(_ *zmux.PoolView, d *zmux.PoolDecision) {
			d.Count, d.Recheck = int(count.Load()), time.Hour
			asks.Add(1)
			once.Do(func() {
				entered <- struct{}{}
				<-gate // the first look does not return until released
			})
		}),
	}
	defer p.Shutdown()
	ctx := context.Background()
	a, err := p.Get(ctx) // starts the first look, which blocks
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	// With the look blocked, Get and Release return at once and see the
	// zero decision (Count 0: no growth dial).
	b, err := p.Get(ctx)
	if err != nil || b.Conn != a.Conn {
		t.Fatalf("Get under a blocked look: %+v, %v", b, err)
	}
	b.Release()
	c, _ := p.Get(ctx)
	c.Release()
	count.Store(2)
	if got := asks.Load(); got != 1 {
		t.Fatalf("%d looks started while the first was under way", got)
	}
	close(gate)
	p.settle()
	// The events during the look coalesced into one more look, and the
	// latest decision (Count 2) applies to the next Get.
	if got := asks.Load(); got != 2 {
		t.Fatalf("%d looks, want the first and one coalesced", got)
	}
	d, err := p.Get(ctx)
	if err != nil || d.Conn == a.Conn {
		t.Fatalf("Get after the decision: %+v, %v", d, err)
	}
	p.settle()
	a.Release()
	d.Release()
}

// A pool with nothing pending holds no goroutine of its own: the decide
// goroutine ends when the looks are done and starts again with the next
// event.
func TestPoolDecideGoroutineEndsWhenIdle(t *testing.T) {
	var asks atomic.Int32
	p := &ConnPool[int]{
		Dial: func(context.Context) (int, error) { return 1, nil },
		Policy: policyFunc(func(_ *zmux.PoolView, d *zmux.PoolDecision) {
			asks.Add(1)
			d.Recheck = time.Hour
		}),
	}
	defer p.Shutdown()
	a, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.settle()
	p.mu.Lock()
	idle := !p.asking && !p.pending
	p.mu.Unlock()
	if !idle || asks.Load() == 0 {
		t.Fatalf("asking %v after the looks, %d looks", !idle, asks.Load())
	}
	before := asks.Load()
	a.Release()
	p.settle()
	if asks.Load() <= before {
		t.Fatal("a release did not start a look")
	}
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
	p.settle()
	if rejected.Load() == 0 {
		t.Fatal("a negative Recheck was not rejected")
	}
}

// A Count below the connections and dials under way cancels the dials
// first, the most recently started first; a Get whose dial was cancelled
// takes a connection with room. PoolView.Dialing counts the dials.
func TestPoolShrinkCancelsNewestDialFirst(t *testing.T) {
	var mu sync.Mutex
	count := 3
	var dialing []int
	n := 0
	p := &ConnPool[int]{
		Dial: func(ctx context.Context) (int, error) {
			mu.Lock()
			n++
			k := n
			mu.Unlock()
			if k == 1 {
				return k, nil
			}
			<-ctx.Done()
			return 0, ctx.Err()
		},
		Policy: policyFunc(func(v *zmux.PoolView, d *zmux.PoolDecision) {
			mu.Lock()
			defer mu.Unlock()
			d.Count = count
			dialing = append(dialing, v.Dialing)
		}),
	}
	ctx := context.Background()
	a, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.settle()
	dials := func(want int) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); ; {
			p.mu.Lock()
			got := len(p.dials)
			p.mu.Unlock()
			if got == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d dials under way, want %d", got, want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	type res struct {
		l   *Lease[int]
		err error
	}
	second, third := make(chan res, 1), make(chan res, 1)
	go func() { l, err := p.Get(ctx); second <- res{l, err} }()
	dials(1)
	go func() { l, err := p.Get(ctx); third <- res{l, err} }()
	dials(2)
	mu.Lock()
	count = 2
	mu.Unlock()
	p.recheckNow()
	p.settle()
	r := <-third // the newest dial is cancelled first
	if r.err != nil || r.l.Conn != a.Conn {
		t.Fatalf("third Get: %v", r.err)
	}
	select {
	case r := <-second:
		t.Fatalf("the older dial was cancelled too: %v", r.err)
	default:
	}
	mu.Lock()
	count = 1
	mu.Unlock()
	p.recheckNow()
	p.settle()
	if r := <-second; r.err != nil || r.l.Conn != a.Conn {
		t.Fatalf("second Get: %v", r.err)
	}
	mu.Lock()
	if !slices.Contains(dialing, 2) || !slices.Contains(dialing, 1) {
		t.Fatalf("PoolView.Dialing seen %v", dialing)
	}
	mu.Unlock()
	p.mu.Lock()
	failures := p.failures
	p.mu.Unlock()
	if failures != 0 {
		t.Fatal("a cancelled dial counted as a failure")
	}
	r.l.Release()
	a.Release()
	p.Shutdown()
}

// With equal idle times, the connection with fewer pipes is retired
// first; a retired connection is reported with Usable false until its
// last pipe ends, and then closes.
func TestPoolShrinkRetiresFewestPipes(t *testing.T) {
	var mu sync.Mutex
	count := 2
	var last zmux.PoolView
	closed := map[int]bool{}
	n := 0
	p := &ConnPool[int]{
		Dial:                  func(context.Context) (int, error) { n++; return n, nil },
		Close:                 func(c int) { closed[c] = true },
		MaxPipesPerConnection: 2,
		Policy: policyFunc(func(v *zmux.PoolView, d *zmux.PoolDecision) {
			mu.Lock()
			defer mu.Unlock()
			d.Count = count
			last = zmux.PoolView{Members: slices.Clone(v.Members)}
		}),
	}
	ctx := context.Background()
	a1, _ := p.Get(ctx)
	p.settle()
	b1, _ := p.Get(ctx)
	p.settle()
	a2, _ := p.Get(ctx)
	p.settle()
	if a1.Conn == b1.Conn || a2.Conn != a1.Conn {
		t.Fatalf("connections %d %d %d", a1.Conn, b1.Conn, a2.Conn)
	}
	mu.Lock()
	count = 1
	mu.Unlock()
	p.recheckNow()
	p.settle()
	p.recheckNow() // a look after the retirement reports it
	p.settle()
	mu.Lock()
	for _, m := range last.Members {
		if m.FateGroup == b1.FateGroup && m.Usable {
			t.Fatalf("members %+v", last.Members)
		}
	}
	if len(last.Members) != 2 {
		t.Fatalf("members %+v: the retired connection is still reported", last.Members)
	}
	mu.Unlock()
	if closed[b1.Conn] {
		t.Fatal("the retired connection closed while it carried a pipe")
	}
	b1.Release()
	if !closed[b1.Conn] || closed[a1.Conn] {
		t.Fatalf("closed %v: want the connection with fewer pipes", closed)
	}
	a1.Release()
	a2.Release()
	p.Shutdown()
}

// A dial Get makes because no connection has room is on demand: it is not
// in PoolView.Dialing, does not count toward Count, and a look that finds
// the pool above Count never cancels it. The connection it makes serves
// its caller and is retired by the next look, after the caller's pipe.
func TestPoolOnDemandDialSurvivesCount(t *testing.T) {
	var mu sync.Mutex
	var dialing []int
	var closed []int
	n := 0
	release := make(chan struct{})
	openDial := sync.OnceFunc(func() { close(release) })
	dialCtx := make(chan context.Context, 2)
	p := &ConnPool[int]{
		Dial: func(ctx context.Context) (int, error) {
			mu.Lock()
			n++
			k := n
			mu.Unlock()
			if k == 2 {
				dialCtx <- ctx
				<-release
			}
			return k, nil
		},
		Close: func(c int) { mu.Lock(); closed = append(closed, c); mu.Unlock() },
		Policy: policyFunc(func(v *zmux.PoolView, d *zmux.PoolDecision) {
			mu.Lock()
			defer mu.Unlock()
			d.Count = 1
			dialing = append(dialing, v.Dialing)
		}),
	}
	defer p.Shutdown()
	defer openDial()
	p.MaxPipesPerConnection = 1
	ctx := context.Background()
	a, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	p.settle()
	type res struct {
		l   *Lease[int]
		err error
	}
	second := make(chan res, 1)
	go func() { l, err := p.Get(ctx); second <- res{l, err} }()
	dc := <-dialCtx // the second connection's dial is under way: a has no room
	p.mu.Lock()
	demand := len(p.dials) == 1 && p.dials[0].demand
	p.mu.Unlock()
	if !demand {
		t.Fatal("the dial of a Get that found no room is not on demand")
	}
	mu.Lock()
	dialing = nil
	mu.Unlock()
	p.recheckNow() // one connection and one dial on demand against Count 1
	p.settle()
	if dc.Err() != nil {
		t.Fatal("a look cancelled the dial on demand")
	}
	mu.Lock()
	seenDialing := slices.Clone(dialing)
	mu.Unlock()
	if len(seenDialing) == 0 || slices.Max(seenDialing) != 0 {
		t.Fatalf("PoolView.Dialing seen %v, want 0 while the dial is on demand", seenDialing)
	}
	// Set the age order while the second dial is still gated. Inserting
	// it can trigger a policy evaluation before the second Get returns.
	p.mu.Lock()
	p.conns[0].born = p.conns[0].born.Add(-time.Hour)
	p.mu.Unlock()
	openDial()
	r := <-second
	if r.err != nil || r.l.Conn != 2 {
		t.Fatalf("second Get: %v", r.err)
	}
	defer r.l.Release()
	p.recheckNow() // now two connections against Count 1: the younger retires
	p.settle()
	mu.Lock()
	closedBeforeRelease := slices.Clone(closed)
	mu.Unlock()
	if len(closedBeforeRelease) != 0 {
		t.Fatalf("closed %v while both carry a pipe", closedBeforeRelease)
	}
	r.l.Release()
	mu.Lock()
	closedAfterRelease := slices.Clone(closed)
	mu.Unlock()
	if !slices.Equal(closedAfterRelease, []int{2}) {
		t.Fatalf("closed %v after the younger connection's pipe ended, want [2]", closedAfterRelease)
	}
}

// Shutdown waits for the look under way: neither Decide nor
// DecisionRejected runs after it returned.
func TestPoolShutdownWaitsForLook(t *testing.T) {
	entered := make(chan struct{})
	gate := make(chan struct{})
	var returned atomic.Bool
	var late atomic.Bool
	var once sync.Once
	p := &ConnPool[int]{
		Dial: func(context.Context) (int, error) { return 1, nil },
		Policy: policyFunc(func(_ *zmux.PoolView, _ *zmux.PoolDecision) {
			if returned.Load() {
				late.Store(true)
			}
			once.Do(func() { close(entered); <-gate })
		}),
	}
	l, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	done := make(chan struct{})
	go func() { p.Shutdown(); returned.Store(true); close(done) }()
	select {
	case <-done:
		t.Fatal("Shutdown returned while a look was under way")
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	<-done
	l.Release()
	p.settle()
	if late.Load() {
		t.Fatal("Decide ran after Shutdown returned")
	}
}

// A failed dial asks the policy again at once, so it sees Failures.
func TestPoolFailedDialLooksAgain(t *testing.T) {
	var failures atomic.Int32
	p := &ConnPool[int]{
		Dial: func(context.Context) (int, error) { return 0, errors.New("down") },
		Policy: policyFunc(func(v *zmux.PoolView, _ *zmux.PoolDecision) {
			failures.Store(int32(v.Failures))
		}),
	}
	defer p.Shutdown()
	if _, err := p.Get(context.Background()); err == nil {
		t.Fatal("Get succeeded")
	}
	p.settle()
	if failures.Load() != 1 {
		t.Fatalf("the last look saw %d failures, want 1", failures.Load())
	}
}
