package zmux

import (
	"errors"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

// Tests of Config.Clock: a fake clock drives the session timers, so hours
// of session time pass in well under a second of real time.

// fakeConfigs returns two multipath configs sharing a fake clock.
func fakeConfigs(edit func(*Config)) (*timeshim.Fake, *Config, *Config) {
	clk := timeshim.NewFake(time.Unix(1_000_000, 0))
	mk := func() *Config {
		c := mpConfig()
		c.Clock = clk
		edit(c)
		return c
	}
	return clk, mk(), mk()
}

// step advances clk by d in increments of inc, letting the drivers process
// what each increment sent before the next.
func step(clk *timeshim.Fake, d, inc time.Duration) {
	for end := clk.Now().Add(d); clk.Now().Before(end); {
		clk.Advance(min(inc, end.Sub(clk.Now())))
		time.Sleep(time.Millisecond)
	}
}

// waitErr waits up to real for c to end while it keeps clk moving in
// steps of a second. A Fake calls a timer armed for a time already past at
// its next Advance, not on its own: a driver that lags behind step and
// arms the deadline it computed after the last step's Advance would
// otherwise wait for an Advance that never comes.
func waitErr(t *testing.T, clk *timeshim.Fake, c *Conn, real time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	stop := time.After(real)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-tick.C:
			clk.Advance(time.Second)
		case <-stop:
			t.Fatal("session did not end")
			return nil
		}
	}
}

func TestFakeClockIdleTimeout(t *testing.T) {
	t.Parallel()
	clk, cc, sc := fakeConfigs(func(c *Config) {
		c.MaxIdleTimeout = time.Hour
		c.Liveness.KeepAlivePeriod = -1
	})
	e := newMPEnv(t, cc, sc)
	start := time.Now()
	step(clk, 59*time.Minute, time.Minute)
	if e.client.closingNow() {
		t.Fatalf("closed before the idle timeout: %v", e.client.closedErr())
	}
	step(clk, 2*time.Minute, time.Minute)
	if err := waitErr(t, clk, e.client, 5*time.Second); !errors.Is(err, ErrIdleTimeout) {
		t.Fatalf("session ended with %v", err)
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Fatalf("an hour of fake time took %v", el)
	}
}

func TestFakeClockKeepAliveAndProbes(t *testing.T) {
	t.Parallel()
	clk, cc, sc := fakeConfigs(func(c *Config) {
		c.MaxIdleTimeout = time.Minute
		c.Liveness.KeepAlivePeriod = 10 * time.Second
		c.Liveness.ProbeInterval = 5 * time.Second
	})
	e := newMPEnv(t, cc, sc)
	pings := func() (n uint64) {
		for _, st := range e.client.PipeStats() {
			n += st.PingsSent
		}
		return n
	}
	base := pings()
	// Ten idle timeouts pass: keep-alive holds the session open.
	step(clk, 10*time.Minute, 250*time.Millisecond)
	if e.client.closingNow() || e.server.closingNow() {
		t.Fatalf("session closed with keep-alive: %v", e.client.closedErr())
	}
	// Probes (5 s interval) and keep-alive pings ran on the fake clock:
	// at least one per keep-alive period.
	if n := pings() - base; n < 60 {
		t.Fatalf("%d probes in 10 min of fake time", n)
	}
}

func TestFakeClockResumeGrace(t *testing.T) {
	t.Parallel()
	clk, cc, sc := fakeConfigs(func(c *Config) {
		c.MaxIdleTimeout = -1
		c.Liveness.KeepAlivePeriod = -1
		c.ResumeTimeout = 10 * time.Minute
	})
	e := newMPEnv(t, cc, sc)
	ps := e.client.Pipes()
	if len(ps) != 1 {
		t.Fatalf("%d pipes", len(ps))
	}
	_ = ps[0].Close()
	step(clk, 9*time.Minute, 10*time.Second)
	if e.client.closingNow() {
		t.Fatalf("closed within the grace period: %v", e.client.closedErr())
	}
	if !e.client.ConnectionState().Resume {
		t.Fatal("resume not in use")
	}
	step(clk, 2*time.Minute, 10*time.Second)
	if err := waitErr(t, clk, e.client, 5*time.Second); !errors.Is(err, ErrResumeTimeout) {
		t.Fatalf("session ended with %v", err)
	}
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
	c := newClock(&backwardClock{Fake: timeshim.NewFake(time.Unix(1_000_000, 0))})
	if now := c.now(); now != 0 {
		t.Fatalf("configured clock stepped back: read %d", now)
	}
}
