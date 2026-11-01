package core

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// probeSession is a multipath session with pipe 0 idle and live at time 0.
func probeSession(cfg Config) (*Session, *Pipe) {
	s := New(cfg, 0)
	s.AttachBootstrap(CarrierInfo{}, CarrierInfo{}, nil)
	s.peerSeen, s.state = true, StateActive
	s.mp = &multiPath{}
	p := s.pipes[0]
	p.mp = &attach{out: OutActive, in: InOpen, probOpt: -1, weight: 1}
	s.drawProbe(p.mp, p, true)
	return s, p
}

// With j = 0 every wait is exactly k, the first one included, and nothing
// is drawn; with j > 0 a bootstrap pipe's first wait lies in [0, k] and
// every later one in [k(1-j), k], the draws reaching both ends.
func TestProbeWaitJitter(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeInterval = 10 * Second
	cfg.ProbeJitter = 0
	s, p := probeSession(cfg)
	for range 100 {
		s.drawProbe(p.mp, p, false)
		if w := s.probeWait(p); w != 10*Second {
			t.Fatalf("j = 0: wait %v", w)
		}
	}
	if s.rng != nil {
		t.Fatal("j = 0 drew from the random source")
	}
	spread := func(t *testing.T, lo, hi Duration, draw func() Duration) {
		t.Helper()
		var low, high int
		for range 1000 {
			w := draw()
			if w < lo || w > hi {
				t.Fatalf("wait %v outside [%v, %v]", w, lo, hi)
			}
			switch {
			case w < lo+(hi-lo)/10:
				low++
			case w > hi-(hi-lo)/10:
				high++
			}
		}
		if low == 0 || high == 0 {
			t.Fatalf("draws near the ends: %d low, %d high", low, high)
		}
	}
	cfg.ProbeJitter = 0.4
	cfg.Test.Seed = 7
	s, p = probeSession(cfg)
	spread(t, 0, 10*Second, func() Duration { // a bootstrap pipe's first wait
		s.drawProbe(p.mp, p, true)
		return s.probeWait(p)
	})
	spread(t, 6*Second, 10*Second, func() Duration {
		s.drawProbe(p.mp, p, false)
		return s.probeWait(p)
	})
	// The same seed gives the same draws.
	a, _ := probeSession(cfg)
	b, _ := probeSession(cfg)
	for range 10 {
		a.drawProbe(a.pipes[0].mp, a.pipes[0], true)
		b.drawProbe(b.pipes[0].mp, b.pipes[0], true)
		if a.pipes[0].mp.probeCut != b.pipes[0].mp.probeCut {
			t.Fatal("same seed, different draws")
		}
	}
}

// The default jitter is 0.25.
func TestProbeJitterDefault(t *testing.T) {
	if j := DefaultConfig().ProbeJitter; j != 0.25 {
		t.Fatalf("default probe jitter %v", j)
	}
}

// The probe interval is measured to the carrier: with records held for up
// to H_p (the shaping guardrail), probes wait k_eff = max(1 s, k - H_p),
// never more than k; an unshaped pipe or an unbounded delay leaves k.
func TestProbeWaitShapingHold(t *testing.T) {
	for _, c := range []struct {
		k, maxDelay, pauseTotal, want Duration
	}{
		{10 * Second, 3 * Second, 0, 7 * Second},
		{10 * Second, 0, 0, 10 * Second},
		{10 * Second, 3 * Second, 2 * Second, 8 * Second},
		{2 * Second, 3 * Second, 0, Second},
		{Second, 500 * Millisecond, 0, Second},
	} {
		cfg := DefaultConfig()
		cfg.ProbeInterval, cfg.ProbeJitter = c.k, 0
		cfg.ShapeLimits.MaxDelay = time.Duration(c.maxDelay)
		cfg.ShapeLimits.MaxPauseTotal = time.Duration(c.pauseTotal)
		s, p := probeSession(cfg)
		p.out = &outDir{lanes: []*outLane{{}}}
		if w := s.probeWait(p); w != c.k {
			t.Fatalf("unshaped: wait %v, want %v", w, c.k)
		}
		s.SetPipeShaper(0, &delayShaper{delay: time.Second})
		if w := s.probeWait(p); w != c.k {
			t.Fatalf("shaped in the middle of a wait: wait %v, want %v until the next one", w, c.k)
		}
		s.drawProbe(p.mp, p, false)
		if w := s.probeWait(p); w != c.want {
			t.Errorf("k %v, MaxDelay %v, MaxPauseTotal %v: wait %v, want %v", c.k, c.maxDelay, c.pauseTotal, w, c.want)
		}
	}
}

// H_p is computed when a probe wait starts: delays that use up
// MaxPauseTotal during a wait leave it as it is, and the next wait takes
// the smaller remainder; with MaxPauseTotal used up, H_p is 0 even with
// MaxDelay set.
func TestProbeHoldAtWaitStart(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeInterval, cfg.ProbeJitter = 10*Second, 0
	cfg.ShapeLimits.MaxDelay = time.Duration(3 * Second)
	cfg.ShapeLimits.MaxPauseTotal = time.Duration(4 * Second)
	s, p := probeSession(cfg)
	p.out = &outDir{lanes: []*outLane{{}}}
	s.SetPipeShaper(0, &delayShaper{delay: time.Second})
	s.drawProbe(p.mp, p, false)
	if w := s.probeWait(p); w != 7*Second {
		t.Fatalf("H_p = MaxDelay: wait %v, want 7 s", w)
	}
	s.shapePause(2 * Second) // 2 s of the total used during the wait
	if w := s.probeWait(p); w != 7*Second {
		t.Fatalf("H_p changed during the wait: %v", w)
	}
	s.drawProbe(p.mp, p, false)
	if w := s.probeWait(p); w != 8*Second {
		t.Fatalf("H_p = the 2 s left of MaxPauseTotal: wait %v, want 8 s", w)
	}
	s.shapePause(5 * Second) // clamped to the 2 s left
	s.drawProbe(p.mp, p, false)
	if w := s.probeWait(p); w != 10*Second {
		t.Fatalf("MaxPauseTotal used up: wait %v, want k", w)
	}
}

// A sample taken while the session is quiet does not enter min_rtt once
// there is one; smoothed_rtt takes it. A busy sample does.
func TestQuietSampleOutOfMinRTT(t *testing.T) {
	var r rttEst
	r.update(50*Millisecond, 0, 25*Millisecond, 0, true) // the first sample sets it, quiet or not
	if r.min != 50*Millisecond {
		t.Fatalf("first quiet sample: min %v", r.min)
	}
	r.update(20*Millisecond, 0, 25*Millisecond, Time(Second), true)
	if r.min != 50*Millisecond || r.smoothed >= 50*Millisecond {
		t.Fatalf("quiet sample: min %v, smoothed %v", r.min, r.smoothed)
	}
	// Past 300 s a quiet sample keeps the old minimum.
	r.update(90*Millisecond, 0, 25*Millisecond, Time(400*Second), true)
	if r.min != 50*Millisecond {
		t.Fatalf("expired minimum replaced by a quiet sample: %v", r.min)
	}
	r.update(30*Millisecond, 0, 25*Millisecond, Time(401*Second), false)
	if r.min != 30*Millisecond {
		t.Fatalf("busy sample: min %v", r.min)
	}
}

// Keep-alive waits are drawn from [k(1-j), k] with k capped at half the
// idle timeout, once per restart of the timer; j = 0 gives exactly k.
func TestKeepAliveWait(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Test.Seed = 3
	s := New(cfg, 0)
	if k := s.cfg.KeepAlive; k != 15*Second {
		t.Fatalf("default keep-alive %v", k)
	}
	s.cfg.KeepAlive, s.cfg.ProbeJitter = 15*Second, 0
	if at, ok := s.keepAliveAt(); !ok || at != Time(15*Second) {
		t.Fatalf("j = 0: %v %v", at, ok)
	}
	s.cfg.ProbeJitter = 0.5
	var low, high int
	for i := range 1000 {
		s.timers.lastActivity = Time(i) * Time(Second)
		at, _ := s.keepAliveAt()
		w := at.Sub(s.timers.lastActivity)
		if again, _ := s.keepAliveAt(); again != at {
			t.Fatal("redrawn without a restart")
		}
		if w < 7500*Millisecond || w > 15*Second {
			t.Fatalf("wait %v outside [7.5 s, 15 s]", w)
		}
		if w < 8250*Millisecond {
			low++
		} else if w > 14250*Millisecond {
			high++
		}
	}
	if low == 0 || high == 0 {
		t.Fatalf("draws near the ends: %d low, %d high", low, high)
	}
	s.cfg.KeepAlive = 0
	if _, ok := s.keepAliveAt(); ok {
		t.Fatal("keep-alive off still due")
	}
}

// The per-pipe options win over the session's; a negative jitter is an
// explicit 0 over a session jitter.
func TestProbePerPipeOptions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeJitter = 0.5
	s, p := probeSession(cfg)
	a := p.mp
	a.probeIv, a.probeJit = 3*Second, -1
	s.drawProbe(a, p, false)
	if w := s.probeWait(p); w != 3*Second {
		t.Fatalf("explicit 0 jitter: wait %v, want 3 s", w)
	}
	a.probeJit = 0.25
	for range 200 {
		s.drawProbe(a, p, false)
		if w := s.probeWait(p); w < 2250*Millisecond || w > 3*Second {
			t.Fatalf("per-pipe jitter: wait %v", w)
		}
	}
	a.probeJit = 0
	seen := false
	for range 200 {
		s.drawProbe(a, p, false)
		if s.probeWait(p) < 2250*Millisecond {
			seen = true
		}
	}
	if !seen {
		t.Fatal("unset per-pipe jitter did not fall back to the session's 0.5")
	}
}

// The probe falls due w after the later of the pipe becoming live and its
// last ack-eliciting byte; such a byte moves the start but keeps w.
func TestProbeDeadlineKeepsDraw(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeInterval = 10 * Second
	cfg.ProbeJitter = 0.5
	s, p := probeSession(cfg)
	a := p.mp
	s.drawProbe(a, p, false)
	w := s.probeWait(p)
	if got := s.mpTimers(0, false); got != Time(w) {
		t.Fatalf("first probe at %v, want %v", got, w)
	}
	f := a.probeCut
	a.lastElicit = Time(3 * Second)
	if got := s.mpTimers(Time(3*Second), false); got != Time(3*Second).Add(w) || a.probeCut != f {
		t.Fatalf("after a send: probe at %v (draw %v -> %v), want %v", got, f, a.probeCut, Time(3*Second).Add(w))
	}
	due := Time(3 * Second).Add(w)
	s.mpTimers(due, true)
	if !a.needPing {
		t.Fatal("no PIPE_PING owed at the deadline")
	}
}

// An attachment without an outbound direction at the peer probes at most
// every 15 s, also when the setting is longer or probes are off.
func TestProbeUnidirectionalCap(t *testing.T) {
	cfg := DefaultConfig()
	s, p := probeSession(cfg)
	p.in = nil
	p.mp.probeIv = 60 * Second
	if k := s.probeInterval(p); k != 15*Second {
		t.Fatalf("ProbeInterval 60 s on an uplink: %v", k)
	}
	p.mp.probeIv = 0
	s.cfg.ProbeInterval = 0
	if k := s.probeInterval(p); k != 15*Second {
		t.Fatalf("probe_interval 0 on an uplink: %v", k)
	}
	p.mp.probeIv = 5 * Second
	if k := s.probeInterval(p); k != 5*Second {
		t.Fatalf("ProbeInterval 5 s on an uplink: %v", k)
	}
	q := s.pipes[0]
	q.in = &inDir{}
	q.mp.probeIv = 0
	if k := s.probeInterval(q); k != 0 {
		t.Fatalf("probe_interval 0 on a pipe pair: %v", k)
	}
}

// A detection deadline that passed before the timer pass ran stays
// pending: a driver that rearms in between still gets a timer for it, and
// the pass then makes the pipe suspect.
func TestPassedSuspicionStaysPending(t *testing.T) {
	s, p := probeSession(DefaultConfig())
	a := p.mp
	a.unacked, a.oldestAt = true, 0
	if !s.hasLiveInbound() {
		t.Fatal("no live inbound pipe in the test session")
	}
	late := Time(5 * Second)
	if got := s.mpTimers(late, false); got > Time(s.tSuspect(a)) {
		t.Fatalf("passed suspicion deadline reported as %v", got)
	}
	s.SetNow(late)
	s.mpHandleTimeout(late)
	if a.out != OutSuspect {
		t.Fatalf("state %v after the pass", a.out)
	}
}

// A joined pipe carried the joiner's PIPE_PING: the acceptor's first wait
// is drawn as every later one, from [k(1-j), k], never from the whole
// interval that only a bootstrap pipe uses (MULTIPATH.md 7.1).
func TestProbeAcceptedJoinFirstWait(t *testing.T) {
	for i := range 200 {
		s, _ := statsMP()
		s.cfg.ProbeInterval, s.cfg.ProbeJitter = 10*Second, 0.5
		s.rng = nil
		s.cfg.Test.Seed = uint64(i + 1)
		s.role, s.roleKnown = RoleInitiator, true
		key := make([]byte, 32)
		s.mp.keyPeer = key
		mac := []byte("mac input")
		j := &wire.Join{PipeID: 7, Dir: 2, Token: wire.JoinToken(key, mac, nil)}
		s.peerSeen, s.mp.cap = true, 16
		if r := s.AcceptJoin(j, mac, nil, Carrier{HasIn: true, HasOut: true}); r != JoinAccepted {
			t.Fatalf("join: %v", r)
		}
		var p *Pipe
		for _, x := range s.pipes {
			if x.id == 7 {
				p = x
			}
		}
		if p == nil {
			t.Fatal("no accepted pipe")
		}
		if w := s.probeWait(p); w < 5*Second || w > 10*Second {
			t.Fatalf("seed %d: accepted pipe's first wait %v outside [5 s, 10 s]", i+1, w)
		}
	}
}

// The silence limit of an inbound pipe without a local outbound direction
// is L_in, except for the last live inbound pipe, which gets max(L_in,
// T_dead) (MULTIPATH.md 7.3).
func TestInboundSilenceLastPipe(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeadTimeout = 60 * Second
	s, p := probeSession(cfg)
	p.out = nil
	p.mp.out = OutNone
	if next := s.mpNextTimeout(); next != Time(60*Second) {
		t.Fatalf("last inbound pipe: deadline %v, want T_dead 60 s", next)
	}
	s.mpHandleTimeout(Time(30 * Second))
	if p.mp.in != InOpen {
		t.Fatalf("last inbound pipe failed at 30 s, past L_in but within T_dead")
	}
	// With another live inbound pipe the limit is L_in alone.
	q := &Pipe{id: 1, in: p.in, mp: &attach{out: OutNone, in: InOpen}}
	s.pipes = append(s.pipes, q)
	want := Time(15*Second + 2*Second/8 + 2*Second)
	if next := s.mpNextTimeout(); next != want {
		t.Fatalf("two inbound pipes: deadline %v, want L_in %v", next, want)
	}
}
