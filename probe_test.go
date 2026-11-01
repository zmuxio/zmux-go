package zmux

import (
	"context"
	"errors"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/testproxy"
	"github.com/zmuxio/zmux-go/shaping"
)

// waitUntil polls cond every 10 ms until it holds or d passes.
func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func pipeStat(t *testing.T, p *Pipe) PipeStats {
	t.Helper()
	st, ok := p.Stats()
	if !ok {
		t.Fatal("pipe gone")
	}
	return st
}

// addPipeVia joins a pipe dialled to addr.
func (e *mpEnv) addPipeVia(t *testing.T, addr string, o PipeOptions) *Pipe {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := e.client.AddPipe(ctx, NetPipe(nc), o)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestProbeOptionsValidation(t *testing.T) {
	for _, j := range []float64{1, 1.5, math.NaN()} {
		c := &Config{SchedulerParams: SchedulerParams{ProbeJitter: j}}
		if _, err := c.coreConfig(core.RoleInitiator, core.RoleInitiator); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("session ProbeJitter %v: %v", j, err)
		}
	}
	// 0 is the default 0.25; NoProbeJitter is 0.
	for _, c := range []struct {
		set  float64
		no   bool
		want float64
	}{{0, false, 0.25}, {0, true, 0}, {0.3, false, 0.3}} {
		cfg := &Config{SchedulerParams: SchedulerParams{ProbeJitter: c.set, NoProbeJitter: c.no, ProbeInterval: 20 * time.Second}}
		cc, err := cfg.coreConfig(core.RoleInitiator, core.RoleInitiator)
		if err != nil {
			t.Errorf("session ProbeJitter %v: %v", c.set, err)
			continue
		}
		if cc.ProbeJitter != c.want || cc.ProbeInterval != core.Duration(20*time.Second) {
			t.Errorf("session ProbeJitter %v: %v %v", c.set, cc.ProbeJitter, cc.ProbeInterval)
		}
	}
	for _, o := range []PipeOptions{{ProbeInterval: 999 * time.Millisecond}, {ProbeInterval: time.Nanosecond},
		{ProbeJitter: 1}, {ProbeJitter: math.NaN()}, {ProbeJitter: -2}} {
		if _, err := o.join(DirBidirectional); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%+v: %v", o, err)
		}
	}
	for _, o := range []PipeOptions{{ProbeInterval: time.Second, ProbeJitter: 0.99}} {
		j, err := o.join(DirBidirectional)
		if err != nil || j.ProbeInterval != core.Duration(o.ProbeInterval) || j.ProbeJitter != o.ProbeJitter {
			t.Errorf("%+v: %+v %v", o, j, err)
		}
	}
}

// A per-pipe ProbeInterval drives that pipe's probes; the others keep the
// session's 15 s.
func TestProbePerPipeInterval(t *testing.T) {
	t.Parallel()
	e := newMPEnv(t, mpConfig(), mpConfig())
	p := e.addPipe(t, PipeOptions{ProbeInterval: time.Second})
	base := pipeStat(t, p).PingsSent
	time.Sleep(3500 * time.Millisecond)
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
}

// Probes and their answers never keep the session alive: a session that
// carries only them closes at its idle timeout. With keep-alive the session
// lives on and the idle pipes are still probed.
func TestProbesDoNotKeepSessionAlive(t *testing.T) {
	t.Parallel()
	cfg := func(keep time.Duration) *Config {
		c := mpConfig()
		c.MaxIdleTimeout = 3 * time.Second
		c.KeepAlivePeriod = keep
		c.SchedulerParams.ProbeInterval = time.Second
		return c
	}
	t.Run("idle", func(t *testing.T) {
		t.Parallel()
		e := newMPEnv(t, cfg(-1), cfg(-1)) // keep-alive off
		start := time.Now()
		var err error
		done := make(chan struct{})
		go func() { err = e.client.Wait(); close(done) }()
		// Probes run before the idle timeout, on either endpoint.
		time.Sleep(2500 * time.Millisecond)
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
	t.Run("keep-alive", func(t *testing.T) {
		t.Parallel()
		e := newMPEnv(t, cfg(time.Second), cfg(-1))
		p := e.addPipe(t, PipeOptions{})
		pings := func() (n uint64) {
			for _, st := range e.client.PipeStats() {
				n += st.PingsSent
			}
			return n
		}
		base := pings()
		time.Sleep(4500 * time.Millisecond)
		if e.client.closingNow() {
			t.Fatalf("session closed with keep-alive: %v", e.client.closedErr())
		}
		if n := pings() - base; n < 3 {
			t.Fatalf("%d probes in 4.5 s with keep-alive (pipe %d)", n, p.ID())
		}
	})
}

// The answer to a probe comes back on the probed pipe pair, with jitter;
// the probe waits lie in [k(1-j), k]. A fake clock measures the waits and
// every probe is answered before the clock moves on, so that the result
// depends neither on the load of the host nor on how the carriers batch.
func TestProbeAnsweredOnSamePairWithJitter(t *testing.T) {
	t.Parallel()
	clk, cc, sc := fakeConfigs(func(c *Config) {
		c.MaxIdleTimeout = -1
		c.KeepAlivePeriod = -1
	})
	e := newMPEnv(t, cc, sc)
	rl, err := testproxy.NewRelay(e.ln.Addr().String(), testproxy.RelayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rl.Close() })
	p := e.addPipeVia(t, rl.Addr(), PipeOptions{ProbeInterval: time.Second, ProbeJitter: 0.5})
	// settled reports whether both endpoints have nothing to write, no
	// write call in progress (one the host is slow to complete would
	// otherwise turn into a write stall as the clock moves on) and every
	// PIPE_PING either sent answered.
	settled := func() bool {
		for _, c := range []*Conn{e.client, e.server} {
			ids := c.PipeStats()
			c.lock()
			ok := true
			for _, st := range ids {
				pi, live := c.s.PipeInfo(st.Pipe)
				ok = ok && (!live || pi.PingsAnswered >= pi.PingsSent && !c.s.WantsWrite(st.Pipe, 0) &&
					(!pi.HasOut || pi.Writable))
			}
			c.unlock()
			if !ok {
				return false
			}
		}
		return true
	}
	// settle advances the clock by inc and waits, in real time, until the
	// exchanges that started settle; it returns the probes sent on p and
	// whether the server sent a record on the pair meanwhile.
	sent := func() uint64 {
		for _, st := range e.server.PipeStats() {
			if st.Pipe == p.ID() {
				return st.RecordsSent
			}
		}
		t.Fatal("no pair at the server")
		return 0
	}
	settle := func(inc time.Duration) (uint64, bool) {
		r0 := sent()
		clk.Advance(inc)
		deadline := time.Now().Add(10 * time.Second)
		for !settled() {
			if time.Now().After(deadline) {
				t.Fatal("exchanges do not settle")
			}
			time.Sleep(time.Millisecond)
		}
		return pipeStat(t, p).PingsSent, sent() > r0
	}
	const inc = 10 * time.Millisecond
	// The join's exchanges settle.
	pings, _ := settle(0)
	for range 150 {
		pings, _ = settle(inc)
	}
	var ups []time.Time
	for end := clk.Now().Add(10 * time.Second); clk.Now().Before(end); {
		n, answeredOnPair := settle(inc)
		if n > pings {
			if !answeredOnPair {
				t.Fatalf("probe %d answered off its pair", n)
			}
			ups = append(ups, clk.Now())
			pings = n
		}
	}
	if len(ups) < 8 {
		t.Fatalf("%d probes in 10 s", len(ups))
	}
	short := false
	for i := 1; i < len(ups); i++ {
		g := ups[i].Sub(ups[i-1])
		// The clock moves in steps of inc. A PIPE_PING of the client inside
		// an exchange the server started restarts the wait, so only the
		// lower end is sharp.
		if g < 500*time.Millisecond-inc || g > 2*time.Second+inc {
			t.Fatalf("probe gap %v outside [k(1-j), 2k] = [0.5 s, 2 s]", g)
		}
		short = short || g < 900*time.Millisecond
	}
	if !short {
		t.Fatal("no gap below 0.9 s: the jitter is not applied")
	}
}

func tcpDialer(addr string) func(context.Context) (PipeIO, error) {
	return func(ctx context.Context) (PipeIO, error) {
		var d net.Dialer
		nc, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return PipeIO{}, err
		}
		return NetPipe(nc), nil
	}
}

// delayCounter delays every record and counts the plans.
type delayCounter struct {
	delay time.Duration
	plans *atomic.Int64
}

func (d delayCounter) Capabilities() shaping.Caps { return shaping.CapPlan }
func (d delayCounter) Plan(_ *shaping.RecordView, p *shaping.RecordPlan) {
	d.plans.Add(1)
	p.Delay = d.delay
}
func (d delayCounter) OnEvent(*shaping.Event, *shaping.Reply) {}

// Probe records go through the shaper; a shaping delay well above
// T_suspect counts from the record's hand-over to the carrier, so it never
// makes the pipe suspect.
func TestProbeThroughShaperNotSuspect(t *testing.T) {
	t.Parallel()
	c := mpConfig()
	c.SchedulerParams.ProbeInterval = time.Second
	c.SchedulerParams.SuspectFloor = 50 * time.Millisecond
	e := newMPEnv(t, c, mpConfig())
	var plans atomic.Int64
	e.client.SetShaper(func(ShapedPipe) shaping.Shaper { return delayCounter{400 * time.Millisecond, &plans} })
	p0, _ := e.client.Pipe(0)
	base := pipeStat(t, p0).PingsSent
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	for {
		ev, err := e.client.NextEvent(ctx)
		if err != nil {
			break
		}
		if ev.Kind == EventPipeSuspect || ev.Kind == EventPipeFailed {
			t.Fatalf("event %v on pipe %d under a 400 ms shaping delay", ev.Kind, ev.Pipe)
		}
	}
	n := pipeStat(t, p0).PingsSent - base
	if n < 2 {
		t.Fatalf("%d probes in 4 s", n)
	}
	if plans.Load() < int64(n) {
		t.Fatalf("%d plans for %d probes: probes bypass the shaper", plans.Load(), n)
	}
}
