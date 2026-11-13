package core

import "testing"

func targetSession() *Session {
	cfg := DefaultConfig()
	cfg.MaxPipes, cfg.Headroom = 2, 8
	s := New(cfg, 0)
	s.state = StateActive
	s.mp = &multiPath{ownCap: 10}
	s.pipes = append(s.pipes, &Pipe{mp: &attach{joinerLocal: true, out: OutActive, in: InOpen}})
	return s
}

func TestJoinReadyUsesMaintenanceTarget(t *testing.T) {
	s := targetSession()
	if !s.JoinReady() {
		t.Fatal("bootstrap leaves one target slot")
	}
	p := &Pipe{mp: &attach{joinerLocal: true, out: OutActive, in: InOpen}}
	s.pipes = append(s.pipes, p)
	if !s.joinAllowed() {
		t.Fatal("explicit join still has physical headroom")
	}
	if s.JoinReady() {
		t.Fatal("maintenance target is full")
	}
	p.mp.out = OutDraining
	if s.JoinReady() {
		t.Fatal("draining still counts toward target")
	}
	p.mp.out = OutFailed
	if !s.JoinReady() {
		t.Fatal("failed pipe must free the maintenance slot")
	}
	s.mp.backoffUntil = 1
	if s.JoinReady() {
		t.Fatal("refusal backoff must block dialing")
	}
	s.mp.backoffUntil = 0
	for len(s.pipes) < s.mp.ownCap {
		s.pipes = append(s.pipes, &Pipe{mp: &attach{joinerLocal: true, out: OutFailed}})
	}
	if s.JoinReady() {
		t.Fatal("failed bookkeeping still consumes physical headroom")
	}
}

func TestJoinReadyIgnoresPeerJoinsAndEndedReceiveOnly(t *testing.T) {
	s := targetSession()
	s.pipes = append(s.pipes, &Pipe{mp: &attach{out: OutActive, in: InOpen}})
	if !s.JoinReady() {
		t.Fatal("peer joins cannot consume our target")
	}
	p := &Pipe{mp: &attach{joinerLocal: true, out: OutNone, in: InOpen}}
	s.pipes = append(s.pipes, p)
	if s.JoinReady() {
		t.Fatal("live receive-only join counts")
	}
	p.mp.in = InEnded
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
	s.pipes[0].mp.out, s.pipes[0].mp.in = OutFailed, InEnded
	for len(s.pipes) < s.mp.ownCap {
		s.pipes = append(s.pipes, &Pipe{mp: &attach{joinerLocal: true, out: OutFailed, in: InEnded}})
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

// TestRoomCandidateOrder checks the order in which a suspended endpoint's
// cap displaces attachments: the oldest failed one whose PIPE_ABANDON was
// acknowledged, then the oldest failed one still pending, and a live one
// only when none has failed and the join provides a missing direction.
// Joins the cap holds back are never displaced.
func TestRoomCandidateOrder(t *testing.T) {
	s := targetSession()
	s.state = StateSuspended
	s.pipes = nil
	add := func(at Time, out OutState, in InState, acked, held bool) *Pipe {
		p := &Pipe{mp: &attach{joinerLocal: true, out: out, in: in, attachedAt: at, lastAdvance: at, abandonAcked: acked, held: held}}
		s.pipes = append(s.pipes, p)
		return p
	}
	live := add(1, OutActive, InNone, false, false)
	live.out = &outDir{}
	add(0, OutJoining, InJoining, false, true) // held: never displaced
	pending := add(2, OutFailed, InEnded, false, false)
	acked := add(3, OutFailed, InEnded, true, false)
	if got := s.roomCandidate(true, false, true); got != nil {
		t.Fatal("a failed attachment is displaced for a join that provides no missing direction")
	}
	if got := s.roomCandidate(true, true, true); got != acked {
		t.Fatal("an acknowledged failed attachment is displaced first")
	}
	acked.mp.retired = true
	if got := s.roomCandidate(true, true, true); got != pending {
		t.Fatal("a failed attachment still pending is displaced before a live one")
	}
	pending.mp.retired = true
	if got := s.roomCandidate(true, false, true); got != nil {
		t.Fatal("a live attachment is displaced for a join that provides no missing direction")
	}
	if got := s.roomCandidate(true, true, false); got != live {
		t.Fatal("the live attachment is displaced for a join that provides the missing inbound direction")
	}
}

// TestHeldJoinDisplacesForMissingDirection lets a held join of a
// suspended endpoint at its cap displace a failed attachment when it
// provides a direction no live attachment has, though the held join's own
// direction is set up already, and holds a join that provides only a
// direction that is live.
func TestHeldJoinDisplacesForMissingDirection(t *testing.T) {
	s := targetSession()
	s.state = StateSuspended
	s.pipes = nil
	// A live send-only attachment: the session lacks only its inbound.
	s.pipes = append(s.pipes, &Pipe{out: &outDir{lanes: []*outLane{{}}}, mp: &attach{joinerLocal: true, out: OutActive, in: InNone}})
	for len(s.pipes) < s.mp.ownCap {
		s.pipes = append(s.pipes, &Pipe{mp: &attach{joinerLocal: true, out: OutFailed, in: InEnded}})
	}
	sendOnly := &Pipe{out: &outDir{lanes: []*outLane{{}}}, mp: &attach{joinerLocal: true, out: OutJoining, in: InNone, held: true}}
	recv := &Pipe{in: &inDir{}, mp: &attach{joinerLocal: true, out: OutNone, in: InJoining, held: true}}
	s.pipes = append(s.pipes, sendOnly, recv)
	if s.joinAllowedFor(sendOnly) {
		t.Fatal("a join that provides only a live direction displaced an attachment")
	}
	if !s.joinAllowedFor(recv) {
		t.Fatal("a join that provides the missing inbound direction stays held")
	}
	if s.ownGoing() != s.mp.ownCap {
		t.Fatalf("%d attachments going, cap %d", s.ownGoing(), s.mp.ownCap)
	}
}
