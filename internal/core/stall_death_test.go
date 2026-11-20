package core

import (
	"math"
	"testing"
)

// stallSession is a one-pipe multipath session whose pipe has had
// unacknowledged bytes since 1 s and receives (a live inbound pipe), so
// that its acknowledgements stall from then on.
func stallSession(cfg Config, ownFail bool, live PipeLiveness) (*Session, *Pipe) {
	s, p := probeSession(cfg)
	a := p.mp
	a.ownFail, a.live = ownFail, live
	a.unacked, a.oldestAt = true, Time(Second)
	a.liveAt, a.lastAdvance, a.lastRecord = Time(Second), Time(Second), Time(Second)
	return s, p
}

// runTo applies every deadline up to end, one timer pass at each.
func runTo(s *Session, end Time) {
	for {
		t := s.mpNextTimeout()
		if t > end {
			s.mpHandleTimeout(end)
			return
		}
		s.mpHandleTimeout(t)
	}
}

// With stall-based failure off, a pipe whose acknowledgements are delayed
// far beyond any derived T_dead turns suspect but never fails, and turns
// active again once they arrive; by default the same stall fails it.
func TestStallDeathOff(t *testing.T) {
	cases := []struct {
		cfg     func(*Config)
		name    string
		live    PipeLiveness
		ownFail bool
		fails   bool
	}{
		{name: "default", fails: true},
		{name: "pipe off", live: PipeLiveness{DeadTimeout: -1}},
		{name: "session off", cfg: func(c *Config) { c.DeadTimeout = -1 }},
		{name: "carrier reports failure", ownFail: true},
		{name: "carrier, explicit override", ownFail: true, live: PipeLiveness{DeadTimeout: 2 * Second}, fails: true},
		{name: "session off, pipe explicit", cfg: func(c *Config) { c.DeadTimeout = -1 }, live: PipeLiveness{DeadTimeout: 2 * Second}, fails: true},
		{name: "huge positive", live: PipeLiveness{DeadTimeout: math.MaxInt64}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := DefaultConfig()
			if c.cfg != nil {
				c.cfg(&cfg)
			}
			s, p := stallSession(cfg, c.ownFail, c.live)
			s.feat.Resume = true // T_dead not stretched to half the idle timeout
			runTo(s, Time(Second).Add(500*Millisecond))
			if p.mp.out != OutSuspect {
				t.Fatalf("not suspect after 500 ms of stall: %v", p.mp.out)
			}
			runTo(s, Time(1000*3600*Second))
			if failed := p.mp.out == OutFailed; failed != c.fails {
				t.Fatalf("failed %v after an hour of stall, want %v (state %v)", failed, c.fails, p.mp.out)
			}
			if c.fails {
				return
			}
			if next := s.mpNextTimeout(); next != Never && next < Time(1000*3600*Second) {
				t.Fatalf("a deadline %v is pending in the past", next)
			}
			// The acknowledgements arrive: the pipe is active again.
			s.mpAdvanced(p, p.out.lanes[0], &pipeSend{}, Time(3601*Second))
			if p.mp.out != OutActive {
				t.Fatalf("still %v after the acknowledgement", p.mp.out)
			}
		})
	}
}

// With another active pipe a stall fails a pipe at T_fail, unless
// stall-based failure is off for it.
func TestStallDeathOffBesideActive(t *testing.T) {
	for _, off := range []bool{false, true} {
		cfg := DefaultConfig()
		live := PipeLiveness{}
		if off {
			live.DeadTimeout = -1
		}
		s, p := stallSession(cfg, false, live)
		q := &Pipe{id: 2, out: p.out, mp: &attach{out: OutActive, in: InNone, probOpt: -1, weight: 1}}
		s.pipes = append(s.pipes, q)
		runTo(s, Time(3600*Second))
		if failed := p.mp.out == OutFailed; failed == off {
			t.Fatalf("off %v: failed %v", off, failed)
		}
	}
}
