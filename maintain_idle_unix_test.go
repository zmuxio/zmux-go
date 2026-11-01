//go:build unix

package zmux

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// A NextEvent waiting beside a running pool sleeps: the pool's long-lived
// pipe waiter does not wake it.
func TestMaintainPipesNextEventIdle(t *testing.T) {
	leakcheck.Check(t)
	cfg := mpConfig()
	cfg.MaxPipes = 2
	e := newMPEnv(t, cfg, cfg)
	e.addPipe(t, PipeOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Dial: func(context.Context) (PipeIO, error) {
			return PipeIO{}, errors.New("test route unavailable")
		}})
	}()
	defer func() {
		cancel()
		err := <-done
		_ = err
	}()
	time.Sleep(100 * time.Millisecond)
	for {
		wctx, wcancel := context.WithTimeout(ctx, 10*time.Millisecond)
		_, err := e.client.NextEvent(wctx)
		wcancel()
		if err != nil {
			break // the events of the setup are drained
		}
	}
	before := cpuTime(t)
	wctx, wcancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer wcancel()
	if _, err := e.client.NextEvent(wctx); err == nil {
		t.Fatal("an event while idle")
	}
	if used := cpuTime(t) - before; used > 250*time.Millisecond {
		t.Fatalf("NextEvent used %v of CPU in 500ms idle", used)
	}
}

func cpuTime(t *testing.T) time.Duration {
	t.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatal(err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
