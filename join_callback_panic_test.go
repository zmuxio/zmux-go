package zmux

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestJoinCallbackPanicRejects: a panic in a join callback the peer drives
// (AuthorizeJoin, AllowRecordMode) rejects that carrier and leaves the
// process and both sessions running.
func TestJoinCallbackPanicRejects(t *testing.T) {
	for _, name := range []string{"AuthorizeJoin", "AllowRecordMode"} {
		t.Run(name, func(t *testing.T) {
			e := newAtkEnv(t, atkOpts{server: func(c *Config) {
				if name == "AuthorizeJoin" {
					c.SessionPolicy.AuthorizeJoin = func(context.Context, JoinAuthorization) (JoinVerdict, error) { panic("host bug") }
				} else {
					c.AllowRecordMode = func(RecordPolicyInput, RecordMode, any) bool { panic("host bug") }
				}
			}})
			ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)
			defer cancel()
			if _, err := e.c.AddPipe(ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err == nil {
				t.Fatal("the join was accepted")
			}
			e.alive(e.c)
			e.alive(e.s)
		})
	}
}

// TestWrapPipePanicFails: a WrapPipe layer that panics on a carrier fails
// that carrier; the listener goes on.
func TestWrapPipePanicFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	l := mustNewListener(&ListenerConfig{Config: atkConfig(), WrapPipe: WrapPipeFunc(func(context.Context, PipeIO, PipeHint) (PipeIO, error) {
		panic("host bug")
	})})
	defer func() { _ = l.Close() }()
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	if err := l.HandlePipe(ctx, NetPipe(a), PipeHint{}); err == nil {
		t.Fatal("a carrier whose layer panicked was taken")
	}
}
