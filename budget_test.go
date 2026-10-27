package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestListenerMemoryBudget(t *testing.T) {
	leakcheck.Check(t)
	type note struct{ used, budget uint64 }
	notes := make(chan note, 16)
	const budget = 128 << 10
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	l := NewListener(&ListenerConfig{MemoryBudget: budget, Config: &Config{},
		OnMemoryPressure: func(used, b uint64) { notes <- note{used, b} }})
	defer func() { _ = l.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = l.HandlePipe(ctx, NetPipe(nc), JoinHint{}) }()
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
		t.Fatalf("no pressure (used %d)", l.MemoryUsed())
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
		var te *TransportError
		if !errors.As(werr, &te) {
			t.Fatalf("new session under pressure: %v", werr)
		}
	}
	// Reading drains the buffer: pressure ends.
	go func() { _, _ = io.Copy(io.Discard, ss) }()
	select {
	case n := <-notes:
		if n.used >= budget/4*3 {
			t.Fatalf("relief note %+v", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no relief (used %d)", l.MemoryUsed())
	}
	runtime.KeepAlive(st) // a collected handle would reset the stream
}
