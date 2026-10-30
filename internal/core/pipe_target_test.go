package core

import "testing"

func targetSession() *Session {
	cfg := DefaultConfig()
	cfg.MaxPipes, cfg.Headroom = 2, 8
	s := New(cfg, 0)
	s.state = StateActive
	s.mp = &multiPath{ownCap: 10}
	s.pipes = append(s.pipes, &Pipe{mp: &attach{joinerLocal: true, out: outActive, in: inOpen}})
	return s
}

func TestJoinReadyUsesMaintenanceTarget(t *testing.T) {
	s := targetSession()
	if !s.JoinReady() {
		t.Fatal("bootstrap leaves one target slot")
	}
	p := &Pipe{mp: &attach{joinerLocal: true, out: outActive, in: inOpen}}
	s.pipes = append(s.pipes, p)
	if !s.joinAllowed() {
		t.Fatal("explicit join still has physical headroom")
	}
	if s.JoinReady() {
		t.Fatal("maintenance target is full")
	}
	p.mp.out = outDraining
	if s.JoinReady() {
		t.Fatal("draining still counts toward target")
	}
	p.mp.out = outFailed
	if !s.JoinReady() {
		t.Fatal("failed pipe must free the maintenance slot")
	}
	s.mp.backoffUntil = 1
	if s.JoinReady() {
		t.Fatal("refusal backoff must block dialing")
	}
	s.mp.backoffUntil = 0
	for len(s.pipes) < s.mp.ownCap {
		s.pipes = append(s.pipes, &Pipe{mp: &attach{joinerLocal: true, out: outFailed}})
	}
	if s.JoinReady() {
		t.Fatal("failed bookkeeping still consumes physical headroom")
	}
}

func TestJoinReadyIgnoresPeerJoinsAndEndedReceiveOnly(t *testing.T) {
	s := targetSession()
	s.pipes = append(s.pipes, &Pipe{mp: &attach{out: outActive, in: inOpen}})
	if !s.JoinReady() {
		t.Fatal("peer joins cannot consume our target")
	}
	p := &Pipe{mp: &attach{joinerLocal: true, out: outNone, in: inOpen}}
	s.pipes = append(s.pipes, p)
	if s.JoinReady() {
		t.Fatal("live receive-only join counts")
	}
	p.mp.in = inEnded
	if !s.JoinReady() {
		t.Fatal("ended receive-only join must free target")
	}
	s.state = StateClosing
	if s.JoinReady() {
		t.Fatal("closing session cannot invite dialing")
	}
}

func TestJoinReadyAllowsSuspendedCapacityRecoveryWithoutMutating(t *testing.T) {
	s := targetSession()
	s.pipes[0].mp.out, s.pipes[0].mp.in = outFailed, inEnded
	for len(s.pipes) < s.mp.ownCap {
		s.pipes = append(s.pipes, &Pipe{mp: &attach{joinerLocal: true, out: outFailed, in: inEnded}})
	}
	if s.JoinReady() {
		t.Fatal("active session still obeys physical headroom")
	}
	s.state = StateSuspended
	if !s.JoinReady() {
		t.Fatal("suspended join must reach AddPipe's recovery displacement")
	}
	if s.ownAttachments() != s.mp.ownCap {
		t.Fatal("readiness query mutated attachments")
	}
	s.mp.backoffUntil = 1
	if s.JoinReady() {
		t.Fatal("resume must still obey refusal backoff")
	}
}
