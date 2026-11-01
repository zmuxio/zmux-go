package core

import "testing"

// A new keep-alive period applies to the wait in progress, still capped at
// half the idle timeout; turning it off removes the deadline.
func TestSetLivenessKeepAlive(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeJitter = 0
	s := New(cfg, 0)
	s.peerSeen, s.state = true, StateActive
	if at, ok := s.keepAliveAt(); !ok || at != Time(15*Second) {
		t.Fatalf("default keep-alive at %v %v", at, ok)
	}
	l := s.Liveness()
	l.KeepAlive = 2 * Second
	s.SetLiveness(l)
	s.timers.lastActivity = Time(Second)
	if got := s.NextTimeout(); got != Time(3*Second) {
		t.Fatalf("keep-alive 2 s: next timeout %v", got)
	}
	l.KeepAlive = 60 * Second // above half the 30 s idle timeout
	s.SetLiveness(l)
	if at, _ := s.keepAliveAt(); at != Time(16*Second) {
		t.Fatalf("keep-alive 60 s: at %v, want the idle cap 15 s after the activity", at)
	}
	l.KeepAlive = 0
	s.SetLiveness(l)
	if _, ok := s.keepAliveAt(); ok {
		t.Fatal("keep-alive off still due")
	}
}

// A pipe already suspect keeps the DeadTimeout it turned suspect under; a
// pipe that turns suspect after the change, and one that joins later, get
// the new one, as does the probe interval of every pipe without its own.
func TestSetLivenessDeadTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeadTimeout = 60 * Second
	s, p := probeSession(cfg)
	a := p.mp
	a.out = OutActive
	s.makeSuspect(p)
	before := s.tDead(a)
	l := s.Liveness()
	l.DeadTimeout, l.ProbeInterval = Second, 3*Second
	s.SetLiveness(l)
	if got := s.tDead(a); got != before || got < 60*Second {
		t.Fatalf("suspect pipe: T_dead %v after the change, was %v", got, before)
	}
	a.out = OutActive
	s.makeSuspect(p)
	if got := s.tDead(a); got >= before || got > 3*Second {
		t.Fatalf("suspect again: T_dead %v, want the new 1 s bound", got)
	}
	if k := s.probeInterval(p); k != 3*Second {
		t.Fatalf("probe interval %v after the change", k)
	}
	a.live = PipeLiveness{ProbeInterval: 5 * Second, DeadTimeout: 20 * Second}
	if k := s.probeInterval(p); k != 5*Second {
		t.Fatalf("pipe's own probe interval: %v", k)
	}
	if _, dt := s.deadParams(a); dt != 20*Second {
		t.Fatalf("pipe's own DeadTimeout: %v", dt)
	}
	// A later pipe without settings of its own follows the session's.
	q := &Pipe{id: 2, out: p.out, in: p.in, mp: &attach{out: OutActive, in: InOpen, probOpt: -1, weight: 1}}
	s.pipes = append(s.pipes, q)
	if k := s.probeInterval(q); k != 3*Second {
		t.Fatalf("later pipe: probe interval %v", k)
	}
	if _, dt := s.deadParams(q.mp); dt != Second {
		t.Fatalf("later pipe: DeadTimeout %v", dt)
	}
}

// The incoming stream concurrency only grows; a raise sends MAX_STREAMS,
// a staged unidirectional credit raises its target instead.
func TestRaiseIncomingStreams(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Local.MaxStreamsBidi = 8
	s := New(cfg, 0)
	if s.RaiseIncomingStreams(true, 8) || s.RaiseIncomingStreams(true, 4) {
		t.Fatal("a raise to the current value or below changed the limit")
	}
	if !s.RaiseIncomingStreams(true, 32) || s.rem[0].limit != 32 || s.rem[0].initial != 32 || !s.pend.maxStreams[0] {
		t.Fatalf("raise to 32: %+v, pending %v", s.rem[0], s.pend.maxStreams[0])
	}
	if !s.RaiseIncomingStreams(false, 512) || s.cfg.UniTarget != 512 || s.rem[1].limit != 16 || s.pend.maxStreams[1] {
		t.Fatalf("staged uni: target %d, %+v", s.cfg.UniTarget, s.rem[1])
	}
	s.unstageUni()
	if s.rem[1].limit != 512 {
		t.Fatalf("unstaged uni limit %d", s.rem[1].limit)
	}
}
