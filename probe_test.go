package zmux

import (
	"context"
	"errors"
	"math"

	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/shaping"
)

func pipeStat(t *testing.T, p *Pipe) PipeStats {
	t.Helper()
	st, ok := p.Stats()
	if !ok {
		t.Fatal("pipe gone")
	}
	return st
}

func TestProbeOptionsValidation(t *testing.T) {
	for _, j := range []float64{1, 1.5, math.NaN()} {
		c := &Config{Liveness: Liveness{ProbeJitter: j}}
		if _, err := c.coreConfig(core.RoleInitiator, core.RoleInitiator); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("session ProbeJitter %v: %v", j, err)
		}
	}
	// 0 is the default 0.25; a negative jitter is 0.
	for _, c := range []struct {
		set  float64
		want float64
	}{{0, 0.25}, {-1, 0}, {-0.1, 0}, {0.3, 0.3}} {
		cfg := &Config{Liveness: Liveness{ProbeJitter: c.set, ProbeInterval: 20 * time.Second}}
		cc, err := cfg.coreConfig(core.RoleInitiator, core.RoleInitiator)
		if err != nil {
			t.Errorf("session ProbeJitter %v: %v", c.set, err)
			continue
		}
		if cc.ProbeJitter != c.want || cc.ProbeInterval != core.Duration(20*time.Second) {
			t.Errorf("session ProbeJitter %v: %v %v", c.set, cc.ProbeJitter, cc.ProbeInterval)
		}
	}
	for _, o := range []PipeOptions{{Liveness: Set(PipeLiveness{ProbeInterval: 999 * time.Millisecond})}, {Liveness: Set(PipeLiveness{ProbeInterval: time.Nanosecond})},
		{Liveness: Set(PipeLiveness{ProbeJitter: 1})}, {Liveness: Set(PipeLiveness{ProbeJitter: math.NaN()})}} {
		if _, err := o.join(DirectionBidirectional); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%+v: %v", o, err)
		}
	}
	for _, o := range []PipeOptions{{Liveness: Set(PipeLiveness{ProbeInterval: time.Second, ProbeJitter: 0.99})}} {
		j, err := o.join(DirectionBidirectional)
		if l, _ := o.Liveness.get(); err != nil || j.Live.ProbeInterval != core.Duration(l.ProbeInterval) || j.Live.ProbeJitter != l.ProbeJitter {
			t.Errorf("%+v: %+v %v", o, j, err)
		}
	}
}

// A per-pipe ProbeInterval drives that pipe's probes; the others keep the
// session's 15 s.
func TestProbePerPipeInterval(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		p := e.addPipe(t, PipeOptions{Liveness: Set(PipeLiveness{ProbeInterval: time.Second})})
		base := pipeStat(t, p).PingsSent
		synctest.Sleep(3500 * time.Millisecond)
		if n := pipeStat(t, p).PingsSent - base; n < 2 || n > 5 {
			t.Fatalf("%d probes in 3.5 s at a 1 s interval", n)
		}
		// Pipe 0 keeps the session's 15 s (its PIPE_PING frames in this busy
		// session also come from answers and return-pipe liveness).
		e.client.lock()
		pi, _ := e.client.s.PipeInfo(0)
		e.client.unlock()
		if k := time.Duration(pi.ProbeInterval); k != 15*time.Second {
			t.Fatalf("pipe 0 probe interval %v, want the 15 s default", k)
		}
	})
}

// Probes and their answers never keep the session alive: a session that
// carries only them closes at its idle timeout. With keep-alive the session
// lives on and the idle pipes are still probed.
func TestProbesDoNotKeepSessionAlive(t *testing.T) {
	t.Parallel()
	cfg := func(keep time.Duration) *Config {
		c := mpConfig()
		c.MaxIdleTimeout = 3 * time.Second
		c.Liveness.KeepAlivePeriod = keep
		c.Liveness.ProbeInterval = time.Second
		return c
	}
	t.Run("idle", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			e := newMemMPEnv(t, cfg(-1), cfg(-1)) // keep-alive off
			start := time.Now()
			var err error
			done := make(chan struct{})
			go func() { err = e.client.Wait(); close(done) }()
			// Probes run before the idle timeout, on either endpoint.
			synctest.Sleep(2500 * time.Millisecond)
			var pings uint64
			for _, c := range []*Conn{e.client, e.server} {
				for _, st := range c.PipeStats() {
					pings += st.PingsSent
				}
			}
			if pings < 2 {
				t.Fatalf("%d probes in 2.5 s at a 1 s interval", pings)
			}
			select {
			case <-done:
			case <-time.After(8 * time.Second):
				t.Fatal("session alive past its idle timeout")
			}
			if !errors.Is(err, ErrIdleTimeout) {
				t.Fatalf("session ended with %v", err)
			}
			if el := time.Since(start); el < 2500*time.Millisecond {
				t.Fatalf("closed after %v", el)
			}
		})
	})
	t.Run("keep-alive", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			e := newMemMPEnv(t, cfg(time.Second), cfg(-1))
			p := e.addPipe(t, PipeOptions{})
			pings := func() (n uint64) {
				for _, st := range e.client.PipeStats() {
					n += st.PingsSent
				}
				return n
			}
			base := pings()
			synctest.Sleep(4500 * time.Millisecond)
			if e.client.closingNow() {
				t.Fatalf("session closed with keep-alive: %v", e.client.closedErr())
			}
			if n := pings() - base; n < 3 {
				t.Fatalf("%d probes in 4.5 s with keep-alive (pipe %d)", n, p.ID())
			}
		})
	})
}

// The receiver of a quiet probe returns its response on the same pair.
func TestNextBatchFProbeAnsweredOnSamePair(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := func() *Config {
			c := mpConfig()
			c.MaxIdleTimeout = -1
			c.Liveness.KeepAlivePeriod = -1
			return c
		}
		env := newMemMPEnv(t, config(), config())
		pipe := env.addPipe(t, PipeOptions{
			Liveness: Set(PipeLiveness{ProbeInterval: time.Second, ProbeJitter: 0.5}),
		})
		if err := pipe.WaitAttached(t.Context()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		peer, ok := env.server.Pipe(pipe.ID())
		if !ok {
			t.Fatal("peer has no attached pair")
		}
		before, reverse := pipeStat(t, pipe), pipeStat(t, peer)
		for range 30 {
			synctest.Sleep(50 * time.Millisecond)
			synctest.Wait()
			after := pipeStat(t, pipe)
			if after.PingsSent > before.PingsSent {
				if after.PingsAnswered <= before.PingsAnswered || pipeStat(t, peer).RecordsSent <= reverse.RecordsSent {
					t.Fatal("probe was not answered on its pair")
				}
				return
			}
		}
		t.Fatal("probe timer did not fire on the joined pair")
	})
}

// delayCounter delays every record and counts the plans.
type delayCounter struct {
	plans *atomic.Int64
	delay time.Duration
}

func (d delayCounter) Capabilities() shaping.Caps { return shaping.CapPlan }
func (d delayCounter) Plan(_ *shaping.RecordView, p *shaping.RecordPlan) {
	d.plans.Add(1)
	p.Delay = d.delay
}
func (d delayCounter) OnEvent(*shaping.Event, *shaping.Effects) {}

// Probe records go through the shaper; a shaping delay well above
// T_suspect counts from the record's hand-over to the carrier, so it never
// makes the pipe suspect.
func TestProbeThroughShaperNotSuspect(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := mpConfig()
		c.Liveness.ProbeInterval = time.Second
		c.Liveness.ProbeJitter = -1
		c.Liveness.SuspectFloor = 50 * time.Millisecond
		e := newMemMPEnv(t, c, mpConfig())
		var plans atomic.Int64
		mustSetShaper(t, e.client, func(ShapedPipe) shaping.Shaper { return delayCounter{delay: 400 * time.Millisecond, plans: &plans} }, nil)
		p0, _ := e.client.Pipe(0)
		base := pipeStat(t, p0).PingsSent
		synctest.Sleep(4 * time.Second)
		synctest.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		for {
			ev, err := e.client.NextEvent(ctx)
			if errors.Is(err, context.Canceled) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if ev.Kind == EventPipeSuspect || ev.Kind == EventPipeFailed {
				t.Fatalf("event %v on pipe %d under a 400 ms shaping delay", ev.Kind, ev.PipeID)
			}
		}
		n := pipeStat(t, p0).PingsSent - base
		if n < 2 {
			t.Fatalf("%d probes in 4 s of virtual time", n)
		}
		if plans.Load() < int64(n) {
			t.Fatalf("%d plans for %d probes: probes bypass the shaper", plans.Load(), n)
		}
	})
}
