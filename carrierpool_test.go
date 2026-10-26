package zmux

import (
	"context"
	"testing"
	"time"
)

func TestCarrierPool(t *testing.T) {
	n := 0
	closed := map[int]bool{}
	p := &CarrierPool[int]{
		Dial:                          func(context.Context) (int, error) { n++; return n, nil },
		Close:                         func(c int) { closed[c] = true },
		MaxPipesPerConnection:         2,
		MaxPipesPerConnectionLifetime: 3,
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
	p2 := &CarrierPool[int]{Dial: func(context.Context) (int, error) { n++; return n, nil }, MaxConnectionAge: time.Millisecond}
	x, _ := p2.Get(ctx)
	time.Sleep(2 * time.Millisecond)
	if y, _ := p2.Get(ctx); y.Conn == x.Conn {
		t.Fatal("aged connection reused")
	}
}
