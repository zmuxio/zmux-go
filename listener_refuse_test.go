package zmux

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A listener that refuses a new session by policy, while draining or with
// its backlog full, sends CONNECTION_REFUSED after its own parameters
// as under memory pressure. TCP, not net.Pipe:
// the refusal must reach a client still writing its first records.
func TestListenerRefusesWithConnectionRefused(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*Listener) func()
	}{
		{"draining", func(l *Listener) func() {
			l.mu.Lock()
			l.draining = true
			l.mu.Unlock()
			return func() {}
		}},
		{"backlog", func(l *Listener) func() {
			// The one backlog slot is taken by a session nobody accepts.
			a, b := tcpPair(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			go func() { _ = l.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
			c, err := Client(ctx, a, nil)
			if err != nil {
				t.Fatalf("first session: %v", err)
			}
			for len(l.accept) == 0 {
				if ctx.Err() != nil {
					t.Fatal("the first session was not queued")
				}
				time.Sleep(time.Millisecond)
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
			handled := make(chan error, 1)
			go func() { handled <- l.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
			var err error
			if c, e := Client(ctx, a, nil); e != nil {
				err = e
			} else {
				err = c.Wait()
			}
			var te *TransportError
			if !errors.As(err, &te) || !te.Remote || te.ErrorCode != TransportConnectionRefused {
				t.Fatalf("session ended with %v, want a remote CONNECTION_REFUSED", err)
			}
			if e := <-handled; !errors.Is(e, errRefused) {
				t.Fatalf("HandlePipe: %v", e)
			}
			if n := l.Stats().Refused; n != 1 {
				t.Fatalf("refused: %d", n)
			}
		})
	}
}
