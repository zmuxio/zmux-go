package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestListenerMemoryBudget(t *testing.T) {
	leakcheck.Check(t)
	type note struct{ used, budget int64 }
	notes := make(chan note, 16)
	const budget = 128 << 10
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	l := mustNewListener(&ListenerConfig{MemoryBudget: budget, Config: &Config{},
		OnMemoryPressure: func(used, b int64) { notes <- note{used, b} }})
	defer func() { _ = l.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = l.HandlePipe(ctx, NetPipe(nc), PipeHint{}) }()
		}
	}()
	dial := func() (*Conn, error) {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return nil, err
		}
		return Client(ctx, nc, nil)
	}
	c, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = c.Wait() }()
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(); _ = s.Wait() }()
	// The server does not read: buffered data reaches the budget.
	st, _ := c.OpenStreamSync(ctx)
	go func() { _, _ = st.Write(make([]byte, 3*budget)) }()
	ss, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-notes:
		if n.used < budget || n.budget != budget {
			t.Fatalf("pressure note %+v", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no pressure (used %d)", l.Adapter().MemoryUsed())
	}
	// Under pressure a new peer stream is rejected and a session refused.
	st2, _ := c.OpenStreamSync(ctx)
	_, _ = st2.Write([]byte("x"))
	_, err = io.ReadAll(st2)
	if !IsRetryable(err) {
		t.Fatalf("new stream under pressure: %v", err)
	}
	if c2, err := dial(); err == nil {
		werr := c2.Wait()
		if _, ok := errors.AsType[*TransportError](werr); !ok {
			t.Fatalf("new session under pressure: %v", werr)
		}
	}
	// Reading drains the buffer: pressure ends. The writer still has data
	// to send, which a fast reader can leave above the budget again before
	// the hook runs, and calls that find the state changed again report the
	// state they find (TestMemoryPressureNotesCoalesce): the relief note is
	// the one that ends the run of notes above the limit.
	go func() { _, _ = io.Copy(io.Discard, ss) }()
	relief := time.After(10 * time.Second)
	for relieved := false; !relieved; {
		select {
		case n := <-notes:
			relieved = n.used < budget/4*3
		case <-relief:
			t.Fatalf("no relief (used %d)", l.Adapter().MemoryUsed())
		}
	}
	runtime.KeepAlive(st) // a collected handle would reset the stream
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
