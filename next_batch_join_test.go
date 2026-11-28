package zmux

import (
	"context"
	"errors"
	"net"
	"testing"
	"testing/synctest"
)

func TestJoinAuthorizationGeneration(t *testing.T) {
	for _, stage := range []string{"before_claim", "after_reply"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				testJoinAuthorizationGeneration(t, stage)
			})
		})
	}
}

func testJoinAuthorizationGeneration(t *testing.T, stage string) {
	t.Helper()
	ccfg, scfg := mpConfig(), mpConfig()
	entered := make(chan JoinAuthorization, 2)
	authorization := make(chan struct{})
	confirmation := make(chan struct{})
	authorize := func(ctx context.Context, req JoinAuthorization) (JoinVerdict, error) {
		entered <- req
		select {
		case <-authorization:
			return JoinPermit, nil
		case <-ctx.Done():
			return JoinDeny, ctx.Err()
		}
	}
	scfg.SessionPolicy.Revision = 1
	scfg.SessionPolicy.AuthGeneration = 1
	scfg.SessionPolicy.AuthorizeJoin = authorize
	e := newMemMPEnv(t, ccfg, scfg)
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	clientResult, serverResult := make(chan error, 1), make(chan error, 1)
	go func() { serverResult <- e.lst.HandlePipe(t.Context(), NetPipe(b), PipeHint{}) }()
	go func() {
		_, err := e.client.AddPipe(t.Context(), NetPipe(holdLaterWrites(a, confirmation)), PipeOptions{Direction: DirectionSendOnly})
		clientResult <- err
	}()
	first := <-entered
	if first.AuthGeneration != 1 || first.Session != e.server.id ||
		first.Direction != DirectionReceiveOnly || first.RecordMode != RecordSealed {
		t.Fatalf("authorization input = %+v", first)
	}
	if stage == "after_reply" {
		close(authorization)
		synctest.Wait()
		e.server.peek(func() {
			if got := e.server.s.PendingPeerJoins(); got != 1 {
				t.Fatalf("after Reply there are %d pending Joins, want 1", got)
			}
		})
	}
	policy := scfg.SessionPolicy
	policy.Revision++
	policy.AuthGeneration++
	if err := e.server.SetSessionPolicy(policy); err != nil {
		t.Fatal(err)
	}
	if stage == "before_claim" {
		close(authorization)
	}
	close(confirmation)
	if err := <-serverResult; !errors.Is(err, ErrJoinAuthorizationChanged) {
		t.Fatalf("accepting join under obsolete authorization returned %v", err)
	}
	if err := <-clientResult; err == nil {
		t.Fatal("join succeeded under obsolete authorization")
	}
	if _, ok := e.server.Pipe(first.PipeID); ok {
		t.Fatal("rejected join attached a pipe")
	}
	e.server.peek(func() {
		if got := e.server.s.PendingPeerJoins(); got != 0 {
			t.Fatalf("rejection left %d pending Joins", got)
		}
	})
	select {
	case req := <-entered:
		t.Fatalf("authorization ran again for the same Join: %+v", req)
	default:
	}
	p, err := e.pipeDialer()(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	joined, err := e.client.AddPipe(t.Context(), p, PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if second := <-entered; second.AuthGeneration != 2 || second.PipeID != joined.ID() {
		t.Fatalf("new Join authorized with %+v", second)
	}
}
