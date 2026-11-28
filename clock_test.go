package zmux

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
)

// fakeConfigs returns two multipath configs sharing a fake clock.
func fakeConfigs(edit func(*Config)) (*timeshim.Fake, *Config, *Config) {
	clk := timeshim.NewFake(time.Now())
	domain := NewClockDomain(clk)
	mk := func() *Config {
		c := mpConfig()
		c.ClockDomain = domain
		edit(c)
		return c
	}
	return clk, mk(), mk()
}

// advanceClock runs the scheduled deadlines in order, allowing each to
// finish its I/O and schedule new work before advancing to the next.
func advanceClock(clk *timeshim.Fake, d time.Duration) {
	end := clk.Now().Add(d)
	for {
		synctest.Wait()
		next, ok := clk.Next()
		if !ok || next.After(end) {
			clk.AdvanceTo(end)
			synctest.Wait()
			return
		}
		clk.AdvanceTo(next)
	}
}

func TestFakeClockIdleTimeout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		clk, cc, sc := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = time.Hour
			c.Liveness.KeepAlivePeriod = -1
		})
		e := newMemMPEnv(t, cc, sc)
		cleanupFakeMPEnv(t, e, clk)
		advanceClock(clk, 59*time.Minute)
		if e.client.closingNow() {
			t.Fatalf("closed before the idle timeout: %v", e.client.closedErr())
		}
		advanceClock(clk, 2*time.Minute)
		waitTestSignal(t, e.client.Done(), "client idle timeout")
		waitTestSignal(t, e.server.Done(), "server idle timeout")
		// The first end to time out can also close the other end's carrier.
		if !errors.Is(e.client.closedErr(), ErrIdleTimeout) && !errors.Is(e.server.closedErr(), ErrIdleTimeout) {
			t.Fatalf("client ended with %v, server with %v", e.client.closedErr(), e.server.closedErr())
		}
	})
}

func TestFakeClockKeepAliveAndProbes(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		clk, cc, sc := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = time.Minute
			c.Liveness.KeepAlivePeriod = 10 * time.Second
			c.Liveness.ProbeInterval = 5 * time.Second
		})
		e := newMemMPEnv(t, cc, sc)
		cleanupFakeMPEnv(t, e, clk)
		pings := func() (n uint64) {
			for _, st := range e.client.PipeStats() {
				n += st.PingsSent
			}
			return n
		}
		base := pings()
		// Ten idle timeouts pass: keep-alive holds the session open.
		advanceClock(clk, 10*time.Minute)
		if e.client.closingNow() || e.server.closingNow() {
			t.Fatalf("session closed with keep-alive: %v", e.client.closedErr())
		}
		if n := pings() - base; n < 60 {
			t.Fatalf("%d probes in 10 min of fake time", n)
		}
	})
}

func TestFakeClockResumeGrace(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		clk, cc, sc := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = -1
			c.Liveness.KeepAlivePeriod = -1
			c.ResumeTimeout = 10 * time.Minute
		})
		e := newMemMPEnv(t, cc, sc)
		cleanupFakeMPEnv(t, e, clk)
		ps := e.client.Pipes()
		if len(ps) != 1 {
			t.Fatalf("%d pipes", len(ps))
		}
		if err := ps[0].Abort(); err != nil {
			t.Fatal(err)
		}
		advanceClock(clk, 9*time.Minute)
		if e.client.closingNow() {
			t.Fatalf("closed within the grace period: %v", e.client.closedErr())
		}
		if !e.client.ConnectionState().Suspended {
			t.Fatal("session did not suspend")
		}
		advanceClock(clk, 2*time.Minute)
		waitTestSignal(t, e.client.Done(), "resume timeout")
		if err := e.client.Wait(); !errors.Is(err, ErrResumeTimeout) {
			t.Fatalf("session ended with %v", err)
		}
	})
}

func cleanupFakeMPEnv(t *testing.T, e *mpEnv, clk *timeshim.Fake) {
	t.Helper()
	t.Cleanup(func() {
		for _, c := range []*Conn{e.client, e.server} {
			for _, p := range c.Pipes() {
				_ = p.Abort()
			}
		}
		advanceClock(clk, time.Hour)
	})
}

// backwardClock reads earlier than the reading a session starts from.
type backwardClock struct {
	*timeshim.Fake
	reads int
}

func (c *backwardClock) Now() time.Time {
	c.reads++
	return c.Fake.Now().Add(-time.Duration(c.reads) * time.Millisecond)
}

// TestClockNeverNegative: session time starts at 0 on the host clock, from
// a session's first reading on, and on a configured clock that steps back.
func TestClockNeverNegative(t *testing.T) {
	for range 10000 {
		if now := newClock(nil).now(); now < 0 {
			t.Fatalf("host clock read %d right after its start", now)
		}
	}
	c := newClock(NewClockDomain(&backwardClock{Fake: timeshim.NewFake(time.Unix(1_000_000, 0))}))
	if now := c.now(); now != 0 {
		t.Fatalf("configured clock stepped back: read %d", now)
	}
}
