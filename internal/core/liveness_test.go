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
	// N19: a raise to the largest credit sends MAX_STREAMS 2^60.
	s.pend.maxStreams = [2]bool{}
	for t2 := range 2 {
		if !s.RaiseIncomingStreams(t2 == 0, maxStreamCount) || s.rem[t2].limit != 1<<60 || !s.pend.maxStreams[t2] {
			t.Fatalf("raise to 2^60: %+v, pending %v", s.rem[t2], s.pend.maxStreams[t2])
		}
	}
}

// T_dead_p = max(suspect_floor, 2 x (PTO + queue), dead_timeout), where an
// unset dead_timeout is T_fail with resume, and at least half the effective
// idle timeout without.
func TestDeadLimitFormula(t *testing.T) {
	for _, c := range []struct {
		name     string
		resume   bool
		d, f, dt Duration
		idle     uint64
		want     Duration
	}{
		{name: "resume: the 2 s of T_fail", resume: true, d: 100 * Millisecond, f: 250 * Millisecond, want: 2 * Second},
		{name: "resume: 8 x PTO", resume: true, d: Second, f: 250 * Millisecond, want: 8 * Second},
		{name: "resume: floor above T_fail", resume: true, d: 100 * Millisecond, f: 5 * Second, want: 5 * Second},
		{name: "no resume: half the idle timeout", d: 100 * Millisecond, f: 250 * Millisecond, idle: 40000, want: 20 * Second},
		{name: "no resume: T_fail above it", d: 2 * Second, f: 250 * Millisecond, idle: 4000, want: 16 * Second},
		{name: "dead_timeout above", resume: true, d: 100 * Millisecond, f: 250 * Millisecond, dt: 60 * Second, want: 60 * Second},
		{name: "T_suspect above dead_timeout", resume: true, d: Second, f: 250 * Millisecond, dt: Second, want: 2 * Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := New(DefaultConfig(), 0)
			s.feat.Resume = c.resume
			s.peer.MaxIdleTimeout, s.cfg.Local.MaxIdleTimeout = c.idle, c.idle
			if got := s.deadLimit(c.d, c.f, c.dt); got != c.want {
				t.Fatalf("T_dead %v, want %v", got, c.want)
			}
		})
	}
}

// L_in = 15 s + n_in / 8 s + max(2 s, 2 x srtt_max, G_p - 15 s), measured from
// the last complete record, for each term that can lead, on a virtual clock:
// the pipe is open up to the limit and fails at it.
func TestInboundSilenceLimit(t *testing.T) {
	for _, c := range []struct {
		name string
		srtt Duration
		gap  Duration
		want Duration
	}{
		{name: "the 2 s", want: 15*Second + 3*Second/8 + 2*Second},
		{name: "srtt_max", srtt: 3 * Second, want: 15*Second + 3*Second/8 + 6*Second},
		{name: "G_p", srtt: 3 * Second, gap: 25 * Second, want: 15*Second + 3*Second/8 + 10*Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, p := probeSession(DefaultConfig())
			// The pair stays quiet: only pipe 1 is judged by its silence.
			p.mp.rtt.smoothed = c.srtt
			p.mp.lastAdvance, p.mp.lastRecord = Time(100*Second), Time(100*Second)
			q := &Pipe{id: 1, in: p.in, mp: &attach{out: OutNone, in: InOpen, maxGap: c.gap, lastRecord: Time(Second), weight: 1}}
			r := &Pipe{id: 3, in: p.in, mp: &attach{out: OutNone, in: InOpen, lastRecord: Time(100 * Second), weight: 1}}
			s.pipes = append(s.pipes, q, r)
			due := Time(Second).Add(c.want)
			if next := s.mpNextTimeout(); next > due {
				t.Fatalf("next timeout %v after the limit %v", next, due)
			}
			s.mpHandleTimeout(due - 1)
			if q.mp.in != InOpen {
				t.Fatalf("failed before L_in %v", c.want)
			}
			s.mpHandleTimeout(due)
			if q.mp.in == InOpen {
				t.Fatalf("still open at L_in %v", c.want)
			}
		})
	}
}

// The last live inbound pipe waits for the larger of L_in and T_dead, the
// latter with the largest PTO of the live outbound pipes.
func TestInboundSilenceLastPipeAtTDead(t *testing.T) {
	cfg := DefaultConfig()
	s, p := probeSession(cfg)
	s.feat.Resume = true
	p.mp.rtt.smoothed, p.mp.rtt.rttvar = 4*Second, 0
	p.mp.lastAdvance, p.mp.lastRecord = Time(1000*Second), Time(1000*Second)
	q := &Pipe{id: 1, in: p.in, mp: &attach{out: OutNone, in: InOpen, lastRecord: Time(Second), weight: 1}}
	s.pipes = append(s.pipes, q)
	p.mp.in = InEnded
	lim := s.lastInDead()
	if want := s.deadLimit(s.pto(p.mp), cfg.SuspectFloor, 0); lim != want || lim < 32*Second {
		t.Fatalf("last-pipe limit %v, want %v, at least 8 x PTO", lim, want)
	}
	s.mpHandleTimeout(Time(Second).Add(lim) - 1)
	if q.mp.in != InOpen {
		t.Fatalf("failed before T_dead %v", lim)
	}
	s.mpHandleTimeout(Time(Second).Add(lim))
	if q.mp.in == InOpen {
		t.Fatalf("still open at T_dead %v", lim)
	}
}
