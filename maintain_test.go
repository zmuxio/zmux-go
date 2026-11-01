package zmux

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestMaintainPipesRedials(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Count: 2, MinBackoff: 10 * time.Millisecond,
			Dial: func(context.Context) (PipeIO, error) {
				nc, err := net.Dial("tcp", e.ln.Addr().String())
				if err != nil {
					return PipeIO{}, err
				}
				return NetPipe(nc), nil
			}})
	}()
	count := func() int {
		n := 0
		for _, p := range e.client.PipeStats() {
			if p.Pipe != 0 && p.Outbound == OutboundActive {
				n++
			}
		}
		return n
	}
	wait := func(want int) {
		deadline := time.Now().Add(5 * time.Second)
		for count() != want {
			if time.Now().After(deadline) {
				t.Fatalf("%d pool pipes, want %d", count(), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	wait(2)
	for _, p := range e.client.PipeStats() {
		if p.Pipe != 0 {
			pp, _ := e.client.Pipe(p.Pipe)
			_ = pp.Abort(nil)
			break
		}
	}
	time.Sleep(50 * time.Millisecond)
	wait(2)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("MaintainPipes returned %v, want context.Canceled", err)
	}
}

func TestMaintainPipesDoesNotDialWhenTargetIsFull(t *testing.T) {
	leakcheck.Check(t)
	cfg := mpConfig()
	cfg.MaxPipes = 2
	e := newMPEnv(t, cfg, cfg)
	occupied := e.addPipe(t, PipeOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan struct{}, 8)
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Dial: func(context.Context) (PipeIO, error) {
			calls <- struct{}{}
			return PipeIO{}, errors.New("test route unavailable")
		}})
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("maintenance stopped with %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("maintenance did not stop")
		}
	}()
	select {
	case <-calls:
		t.Fatal("dialed while existing bootstrap and pipe filled target")
	case <-time.After(550 * time.Millisecond):
	}
	if err := occupied.Abort(nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-calls:
	case <-time.After(3 * time.Second):
		t.Fatal("did not attempt recovery when target slot became free")
	}
}

// A rotation dial waits for the pool's backoff, counted from the start of
// the previous dial, and its failures grow that backoff as a redial's do.
func TestMaintainPipesRotationBacksOff(t *testing.T) {
	t.Parallel()
	e := newMPEnv(t, mpConfig(), mpConfig())
	dial := tcpDialer(e.ln.Addr().String())
	var dials, maxFailures atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{MinBackoff: 400 * time.Millisecond, MaxBackoff: 10 * time.Second,
			Rotate: Rotation{MaxAge: time.Millisecond},
			Policy: policyFunc(func(v *PoolView, d *PoolDecision) {
				if f := int64(v.Failures); f > maxFailures.Load() {
					maxFailures.Store(f)
				}
				d.Recheck = MinRecheck // passes keep coming
			}),
			Dial: func(ctx context.Context) (PipeIO, error) {
				if dials.Add(1) > 1 {
					return PipeIO{}, errors.New("test dial refused")
				}
				return dial(ctx)
			}})
	}()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("MaintainPipes ended with %v, want the cancellation", err)
		}
	}()
	waitUntil(t, 5*time.Second, "first rotation dial", func() bool { return dials.Load() >= 2 })
	time.Sleep(1500 * time.Millisecond)
	// Waits of at least 0.8 x 800 ms and 0.8 x 1.6 s follow the first failure.
	if n := dials.Load() - 2; n > 2 {
		t.Fatalf("%d rotation dials in 1.5 s after a failure", n)
	}
	if maxFailures.Load() < 1 {
		t.Fatal("rotation failures not counted")
	}
}

// A Join member reports its pipes in the pool view.
func TestMaintainPipesJoinMemberPipes(t *testing.T) {
	t.Parallel()
	e := newMPEnv(t, mpConfig(), mpConfig())
	dial := tcpDialer(e.ln.Addr().String())
	var pipes atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{
			Policy: policyFunc(func(v *PoolView, d *PoolDecision) {
				for _, m := range v.Members {
					pipes.Store(int64(m.Pipes))
				}
				d.Recheck = MinRecheck
			}),
			Join: func(ctx context.Context, c *Conn, o PipeOptions) ([]*Pipe, error) {
				var ps []*Pipe
				for range 2 {
					io, err := dial(ctx)
					if err != nil {
						return ps, err
					}
					p, err := c.AddPipe(ctx, io, o)
					if err != nil {
						closePipeIO(io)
						return ps, err
					}
					ps = append(ps, p)
				}
				return ps, nil
			}})
	}()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("MaintainPipes ended with %v, want the cancellation", err)
		}
	}()
	waitUntil(t, 5*time.Second, "member of two pipes", func() bool { return pipes.Load() == 2 })
}
