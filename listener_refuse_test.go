package zmux

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// A listener that refuses a new session by policy, while draining or with
// its backlog full, sends CONNECTION_REFUSED after its own parameters
// as under memory pressure. TCP, not net.Pipe:
// the refusal must reach a client still writing its first records.
func TestListenerRefusesWithConnectionRefused(t *testing.T) {
	for _, tc := range []struct {
		setup func(*Listener) func()
		name  string
	}{
		{name: "memory", setup: func(l *Listener) func() {
			limit := int64(l.budget.Limit())
			l.Adapter().ChargeMemory(limit)
			return func() { l.Adapter().ChargeMemory(-limit) }
		}},
		{name: "draining", setup: func(l *Listener) func() {
			l.Drain("draining")
			return func() {}
		}},
		{name: "backlog", setup: func(l *Listener) func() {
			// The one backlog slot is taken by a session nobody accepts.
			a, b := tcpPair(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			queued := make(chan error, 1)
			go func() { queued <- l.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
			c, err := Client(ctx, a, nil)
			if err != nil {
				t.Fatalf("first session: %v", err)
			}
			if err := <-queued; err != nil {
				t.Fatal(err)
			}
			return func() { _ = c.Close(); cancel() }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := mustNewListener(&ListenerConfig{Backlog: 1})
			defer func() { _ = l.Close() }()
			defer tc.setup(l)()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			a, b := tcpPair(t)
			// Keep the server's half-close pending until setup has returned.
			g := &gatedWrite{Conn: b, gate: make(chan struct{}), blocked: make(chan struct{}), nth: -1}
			open := sync.OnceFunc(func() { close(g.gate) })
			defer open()
			handled := make(chan error, 1)
			go func() { handled <- l.HandlePipe(ctx, NetPipe(g), PipeHint{}) }()
			peer := make(chan error, 1)
			go func() {
				c, err := Client(ctx, a, nil)
				if err == nil {
					err = c.Wait()
				}
				peer <- err
			}()
			select {
			case err := <-handled:
				if !errors.Is(err, errRefused) {
					t.Fatalf("HandlePipe: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("HandlePipe waited for the refused session's half-close")
			}
			l.pmu.Lock()
			pending := l.pending.Total()
			l.pmu.Unlock()
			if pending != 1 {
				t.Fatalf("pending during refusal flush: %d", pending)
			}
			open()
			err := <-peer
			var te *TransportError
			if !errors.As(err, &te) || !te.Remote || te.ErrorCode != TransportConnectionRefused {
				t.Fatalf("session ended with %v, want a remote CONNECTION_REFUSED", err)
			}
			if n := l.Stats().Refused; n != 1 {
				t.Fatalf("refused: %d", n)
			}
		})
	}
}

// HS-30, HS-32, HS-33.
func TestDrainPolicyAtClassification(t *testing.T) {
	cfg := mpConfig()
	entered, release := make(chan struct{}, 1), make(chan struct{}, 1)
	l := mustNewListener(&ListenerConfig{Config: cfg, SessionConfig: func(context.Context, *CarrierInfo, any) (*Config, error) {
		entered <- struct{}{}
		<-release
		return cfg, nil
	}})
	defer func() { _ = l.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	try := func(split, refuse bool, msg string, change func()) {
		a, b := tcpPair(t)
		p := NetPipe(a)
		handled := make(chan error, 2)
		n := 1
		if split {
			d, w := tcpPair(t)
			p = PipeIO{R: d, W: a}
			n = 2
			const tok = "AAAAAAAAAAAAAAAAAAAAAA"
			go func() { handled <- l.HandlePipe(ctx, PipeIO{W: w}, PipeHint{Pair: tok}) }()
			go func() { handled <- l.HandlePipe(ctx, PipeIO{R: b}, PipeHint{Pair: tok, Uplink: true}) }()
		} else {
			go func() { handled <- l.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
		}
		peer := make(chan error, 1)
		go func() {
			c, err := Start(ctx, p, RoleClient, cfg)
			if err == nil {
				err = c.Wait()
			}
			peer <- err
		}()
		<-entered
		change()
		release <- struct{}{}
		for range n {
			if err := <-handled; errors.Is(err, errRefused) != refuse || !refuse && err != nil {
				t.Fatalf("setup %v", err)
			}
		}
		if !refuse {
			s, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_ = s.Close()
		}
		err := <-peer
		if refuse {
			var se *SessionError
			if !errors.Is(err, ErrConnectionRefused) || !errors.As(err, &se) || se.ErrorMessage != msg {
				t.Fatalf("refusal %v", err)
			}
		}
	}
	l.Drain("one")
	try(false, true, "one", l.Undrain)
	try(false, false, "", func() { l.Drain("two") })
	l.Drain("three")
	try(true, true, "three", l.Undrain)
	l.Drain("")
	try(false, true, "", func() {})
	if l.Stats().Refused != 3 || l.Stats().Sessions != 1 {
		t.Fatal(l.Stats())
	}
}

// HS-31.
func TestDrainAllowsResume(t *testing.T) {
	cfg := mpConfig()
	cfg.ResumeTimeout = time.Minute
	e := newMPEnv(t, cfg, cfg)
	e.lst.Drain("resume elsewhere")
	p := e.addPipe(t, PipeOptions{})
	boot, _ := e.client.Pipe(0)
	_ = boot.Abort()
	_ = p.Abort()
	e.addPipe(t, PipeOptions{})
	echoOnce(t, t.Context(), e.client, e.server)
}
