package zmux

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/netx"
	"github.com/djylb/timeshim"
)

func TestRedialReplacementRevalidatesAfterFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, scfg := mpConfig(), mpConfig()
		cfg.Clock, scfg.Clock = timeshim.Real, timeshim.Real
		cfg.ResumeTimeout, scfg.ResumeTimeout = time.Minute, time.Minute
		cfg.RedialBackoff = Backoff{Min: time.Second, Max: 4 * time.Second}
		var attempts atomic.Int32
		var outage atomic.Bool
		calls := make(chan time.Time, 8)
		cfg.Redial = func(ctx context.Context, need Direction) (PipeIO, error) {
			id, ok := ReplacingPipe(ctx)
			if outage.Load() && ok || !outage.Load() && (!ok || id != 0 || need != DirectionBidirectional) {
				t.Errorf("replacement callback id=%v present=%v direction=%v", id, ok, need)
			}
			attempts.Add(1)
			calls <- time.Now()
			return PipeIO{}, errors.New("test dial failed")
		}
		ln := netx.NewChanListener(nil, 0)
		shape := link{d: time.Millisecond, rate: 20e6, cap: 64 << 10}
		env := startMPEnv(t, cfg, &ListenerConfig{Config: scfg}, &linkListener{Listener: ln, links: []link{shape}},
			func(ctx context.Context) (net.Conn, error) {
				nc, err := ln.Dial(ctx, nil)
				if err != nil {
					return nil, err
				}
				return newDelayConn(nc, shape.d, shape.rate, shape.cap), nil
			})
		c := env.client
		waitUntil(t, 3*time.Second, "fresh pipe RTT", func() bool {
			return c.ConnectionStats().LatestRTT > 0
		})
		setQueue := func(n uint64) {
			c.locked(func() { c.s.SetCarrierUnsent(0, 0, n, c.clk.now()) })
		}
		setQueue(2 << 20)
		time.Sleep(3 * time.Second)
		if attempts.Load() != 0 {
			t.Fatal("advisory dialed without a host request")
		}
		accepted := c.RequestPipeReplacement(0)
		coalesced := c.RequestPipeReplacement(0)
		if !accepted || !coalesced {
			t.Fatal("loaded connection-specific demand was not coalesced")
		}
		waitCall := func() time.Time {
			t.Helper()
			select {
			case at := <-calls:
				return at
			case <-time.After(4 * time.Second):
				t.Fatal("expected a replacement attempt")
				return time.Time{}
			}
		}
		first, second := waitCall(), waitCall()
		if d := second.Sub(first); d < 800*time.Millisecond || d > time.Second {
			t.Fatalf("retry bypassed replacement backoff: %v", d)
		}
		setQueue(0)
		time.Sleep(4 * time.Second)
		if attempts.Load() != 2 || c.RequestPipeReplacement(0) {
			t.Fatal("recovered demand continued to authorize replacement")
		}
		setQueue(2 << 20)
		time.Sleep(3 * time.Second)
		if !c.RequestPipeReplacement(0) {
			t.Fatal("a later episode could not be requested")
		}
		outage.Store(true)
		old, _ := c.Pipe(0)
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
		waitCall() // the outage takes priority over the queued replacement
		if attempts.Load() != 3 {
			t.Fatal("simultaneous outage and replacement were not coalesced")
		}
	})
}

func newRedialQualityEnv(t *testing.T, cfg, scfg *Config) *mpEnv {
	t.Helper()
	cfg.Clock, scfg.Clock = timeshim.Real, timeshim.Real
	cfg.ResumeTimeout, scfg.ResumeTimeout = time.Minute, time.Minute
	cfg.Liveness.ProbeInterval, scfg.Liveness.ProbeInterval = time.Second, time.Second
	cfg.Liveness.ProbeJitter, scfg.Liveness.ProbeJitter = -1, -1
	ln := netx.NewChanListener(nil, 0)
	shape := link{d: time.Millisecond, rate: 20e6, cap: 64 << 10}
	return startMPEnv(t, cfg, &ListenerConfig{Config: scfg}, &linkListener{Listener: ln, links: []link{shape}},
		func(ctx context.Context) (net.Conn, error) {
			nc, err := ln.Dial(ctx, nil)
			if err != nil {
				return nil, err
			}
			return newDelayConn(nc, shape.d, shape.rate, shape.cap), nil
		})
}

func requestLocalReplacement(t *testing.T, old *Pipe, reason PipePathQualityReason) {
	t.Helper()
	c := old.c
	// A reported queue gives a repeatable local stall without relying on
	// buffering, pacing or a socket's send-buffer size.
	c.locked(func() { c.s.SetCarrierUnsent(old.id, 0, 2<<20, c.clk.now()) })
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	for {
		ev, err := c.NextEvent(ctx)
		if err != nil {
			t.Fatalf("waiting for quality advice %v: %v", reason, err)
		}
		if ev.Kind == EventPipePathQuality && ev.PipeID == old.ID() && ev.PathQuality == reason {
			if ev.RetryAfter <= 0 || !c.RequestPipeReplacement(old.ID()) {
				t.Fatalf("quality advice did not authorize replacement: %+v", ev)
			}
			return
		}
	}
}

type redialQueueDrainConn struct {
	net.Conn
	drain func()
	once  sync.Once
}

func (p *redialQueueDrainConn) Read(b []byte) (int, error) {
	p.once.Do(p.drain)
	return p.Conn.Read(b)
}

func TestRedialSuccessfulReplacementKeepsNewPipe(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fate    uint64
		boot    bool
		sibling bool
		reason  PipePathQualityReason
	}{
		{name: "local_stall_without_fate", boot: true, reason: PipePathQualityLocalStall},
		{name: "local_stall_with_fate", fate: 42, reason: PipePathQualityLocalStall},
		{name: "healthy_sibling", fate: 42, sibling: true, reason: PipePathQualityHealthySibling},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg, scfg := mpConfig(), mpConfig()
				var env *mpEnv
				var old *Pipe
				confirmed := make(chan PipeID, 3)
				cfg.JoinOutcome = func(_ *Conn, id PipeID, err error) {
					if err == nil {
						confirmed <- id
					}
				}
				cfg.Redial = func(ctx context.Context, need Direction) (PipeIO, error) {
					if _, ok := ReplacingPipe(ctx); !ok || need != DirectionBidirectional {
						return PipeIO{}, errors.New("expected a bidirectional replacement")
					}
					nc, err := env.dial(ctx)
					if err != nil {
						return PipeIO{}, err
					}
					// After the host dial is revalidated, progress on
					// the replacement can remove demand from the old path
					// before the accepted episode has actually recovered.
					return NetPipe(&redialQueueDrainConn{Conn: nc, drain: func() {
						c := old.c
						c.locked(func() { c.s.SetCarrierUnsent(old.id, 0, 0, c.clk.now()) })
					}}), nil
				}
				env = newRedialQualityEnv(t, cfg, scfg)
				if tc.boot {
					old, _ = env.client.Pipe(0)
				} else {
					old = env.addPipe(t, PipeOptions{FateGroup: tc.fate})
					if id := <-confirmed; id != old.ID() {
						t.Fatalf("first confirmed pipe is %v, want %v", id, old.ID())
					}
				}
				if tc.sibling {
					sibling := env.addPipe(t, PipeOptions{FateGroup: tc.fate})
					if id := <-confirmed; id != sibling.ID() {
						t.Fatalf("second confirmed pipe is %v, want %v", id, sibling.ID())
					}
				}
				requestLocalReplacement(t, old, tc.reason)
				var replacement PipeID
				select {
				case replacement = <-confirmed:
				case <-time.After(5 * time.Second):
					t.Fatal("replacement did not confirm")
				}
				waitUntil(t, 5*time.Second, "retirement of original pipe", func() bool {
					s, ok := old.Stats()
					return !ok || s.Outbound == OutboundDraining || s.Outbound == OutboundClosed
				})
				// The join result arrives before the worker finishes. Waiting
				// for quiescence also catches a mistaken drain of the new pipe.
				synctest.Wait()
				p, ok := env.client.Pipe(replacement)
				if !ok {
					t.Fatal("confirmed replacement was removed")
				}
				st, ok := p.Stats()
				if !ok || !st.PeerAttached || st.Outbound != OutboundActive || st.FateGroup != tc.fate {
					t.Fatalf("replacement did not stay active with the requested fate: %+v", st)
				}
			})
		})
	}
}

type redialHoldRecordsConn struct {
	net.Conn
	ctx     context.Context
	c       *Conn
	blocked chan struct{}
	once    sync.Once
}

func (p *redialHoldRecordsConn) Write(b []byte) (int, error) {
	var records bool
	p.c.peek(func() {
		for _, pd := range p.c.pipes {
			if out := pd.end.join; out != nil && out.parent == p.ctx && pd.writer.ctx != nil {
				records = true
				return
			}
		}
	})
	if records {
		p.once.Do(func() { close(p.blocked) })
		<-p.ctx.Done()
		return 0, p.ctx.Err()
	}
	return p.Conn.Write(b)
}

func TestRedialOutagePreemptsUnconfirmedReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, scfg := mpConfig(), mpConfig()
		cfg.JoinTimeout = 30 * time.Second
		cfg.RedialBackoff = Backoff{Min: 10 * time.Second, Max: 40 * time.Second}
		var env *mpEnv
		blocked := make(chan struct{})
		outage := make(chan time.Time, 1)
		outcomes := make(chan error, 3)
		cfg.JoinOutcome = func(_ *Conn, _ PipeID, err error) { outcomes <- err }
		cfg.Redial = func(ctx context.Context, need Direction) (PipeIO, error) {
			if _, replacing := ReplacingPipe(ctx); replacing {
				nc, err := env.dial(ctx)
				if err != nil {
					return PipeIO{}, err
				}
				return NetPipe(&redialHoldRecordsConn{Conn: nc, ctx: ctx, c: env.client, blocked: blocked}), nil
			}
			if need != DirectionBidirectional {
				t.Errorf("outage need = %v, want bidirectional", need)
			}
			outage <- time.Now()
			return env.pipeDialer()(ctx)
		}
		env = newRedialQualityEnv(t, cfg, scfg)
		c := env.client
		old, _ := c.Pipe(0)
		requestLocalReplacement(t, old, PipePathQualityLocalStall)
		select {
		case <-blocked:
		case <-time.After(5 * time.Second):
			t.Fatal("replacement never waited for peer confirmation")
		}
		lost := time.Now()
		if err := old.Abort(); err != nil {
			t.Fatal(err)
		}
		select {
		case called := <-outage:
			if elapsed := called.Sub(lost); elapsed >= time.Second {
				t.Fatalf("outage delayed by replacement: %v", elapsed)
			}
		case <-time.After(time.Second):
			t.Fatal("outage waited for replacement confirmation or backoff")
		}
		var canceled, connected bool
		for range 2 {
			select {
			case err := <-outcomes:
				canceled = canceled || errors.Is(err, context.Canceled)
				connected = connected || err == nil
			case <-time.After(5 * time.Second):
				t.Fatal("join outcomes did not resolve after the outage")
			}
		}
		if !canceled || !connected {
			t.Fatalf("want canceled replacement and successful outage join, got canceled=%t connected=%t", canceled, connected)
		}
	})
}
