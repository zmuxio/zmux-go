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

func TestListenerMemoryBudget(t *testing.T) {
	leakcheck.Check(t)
	const budget = 1 << 20
	l := mustNewListener(&ListenerConfig{MemoryBudget: budget, Config: &Config{}})
	defer func() { _ = l.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dial := func() (*Conn, error) {
		cl, sv := net.Pipe()
		server := make(chan error, 1)
		go func() { server <- l.HandlePipe(ctx, NetPipe(sv), PipeHint{}) }()
		c, err := Client(ctx, cl, nil)
		if err != nil {
			_ = cl.Close()
		}
		if got := <-server; got != nil && !errors.Is(got, ErrMemoryBudget) {
			t.Errorf("handle pipe: %v", got)
		}
		return c, err
	}
	if c, err := dial(); err == nil {
		c.Abort(0, "")
		t.Fatal("admitted a session without room for its minimum receive window")
	}
	if n := l.Adapter().MemoryUsed(); n != 0 {
		t.Fatalf("rejected bootstrap holds %d bytes", n)
	}
	lim := l.Limits()
	lim.MemoryBudget = 2 << 20
	if err := l.SetLimits(lim); err != nil {
		t.Fatal(err)
	}
	c, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.Abort(0, ""); _ = c.Wait() }()
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Abort(0, ""); _ = s.Wait() }()
	if w := s.s.LocalParams().InitialMaxData; w < 16<<10 || w >= 1<<20 {
		t.Fatalf("initial connection window %d was never reduced", w)
	}
	if used := l.Adapter().MemoryUsed(); used > lim.MemoryBudget {
		t.Fatalf("budget exceeded: %d", used)
	}
}

// The hook runs on one serial context and reads the budget when it runs:
// a pressure start and its end that follow each other while a call is
// still running coalesce into one more call, which reports the state at
// that time, and the last call always reports the final state.
func TestMemoryPressureNotesCoalesce(t *testing.T) {
	leakcheck.Check(t)
	const budget = 1 << 10
	type note struct{ used, budget int64 }
	notes := make(chan note, 16)
	gate := make(chan struct{})
	var first sync.Once
	l := mustNewListener(&ListenerConfig{MemoryBudget: budget, Config: &Config{},
		OnMemoryPressure: func(used, b int64) {
			notes <- note{used, b}
			first.Do(func() { <-gate }) // the first call does not return until released
		}})
	defer func() { _ = l.Close() }()
	a := l.Adapter()
	// The watcher starts with the first carrier the listener takes.
	cl, sv := net.Pipe()
	handled := make(chan struct{})
	go func() { defer close(handled); _ = l.HandlePipe(context.Background(), NetPipe(sv), PipeHint{}) }()
	defer func() { _ = cl.Close(); <-handled }()
	a.ChargeMemory(budget) // pressure starts
	if n := <-notes; n.used != budget || n.budget != budget {
		t.Fatalf("start note %+v", n)
	}
	a.ChargeMemory(-budget) // ends, and starts again, while the hook still runs
	a.ChargeMemory(budget)
	close(gate)
	n := <-notes
	if n.used != budget || !a.MemoryPressure() {
		t.Fatalf("the note after the flap %+v, pressure %v", n, a.MemoryPressure())
	}
	a.ChargeMemory(-budget)
	if n := <-notes; n.used != 0 {
		t.Fatalf("relief note %+v", n)
	}
}
