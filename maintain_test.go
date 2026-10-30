package zmux

import (
	"context"
	"errors"
	"net"
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
			if p.ID != 0 && p.State == "active" {
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
		if p.ID != 0 {
			pp, _ := e.client.Pipe(p.ID)
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
