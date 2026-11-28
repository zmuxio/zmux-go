package zmux_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go"
)

// Rows of the liveness bound measured through a relay
// that turns into a black hole: the carrier stays connected and every
// byte is dropped.

// suspectAfter black-holes the relay and returns how long the client took
// to report pipe id suspect, at most limit.
func suspectAfter(t *testing.T, e *relayEnv, id uint64, limit time.Duration) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), limit)
	defer cancel()
	start := time.Now()
	e.relay.Blackhole(true, true)
	for {
		ev, err := e.client.NextEvent(ctx)
		if err != nil {
			t.Fatalf("waiting for pipe %d suspicion: %v; pipes %+v", id, err, e.client.PipeStats())
		}
		if ev.Kind == zmux.EventPipeSuspect && uint64(ev.PipeID) == id {
			return time.Since(start)
		}
	}
}

// busy keeps a stream of the client writing until the test ends.
func busy(t *testing.T, e *relayEnv) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go echoAll(ctx, e.server)
	st, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 512)
		for ctx.Err() == nil {
			if _, err := st.Write(buf); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	go func() {
		b := make([]byte, 4096)
		for {
			if _, err := st.Read(b); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		st.CancelWrite(0)
		st.CancelRead(0)
		<-done
	})
}

// A pipe carrying data is suspect within T_suspect of its death (250 ms
// here, plus the round trips of a loaded test host).
func TestLivenessDataPipe(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newMemRelayEnv(t, relayConfig(0), relayConfig(0), true)
		busy(t, e)
		time.Sleep(300 * time.Millisecond)
		if d := suspectAfter(t, e, 0, 5*time.Second); d > 2*time.Second {
			t.Fatalf("data pipe suspect after %v", d)
		}
	})
}

// A standby pipe of a busy session is probed every standby_probe: its
// silent death is suspected within standby_probe + T_suspect.
func TestLivenessStandbyBusy(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newMemRelayEnv(t, relayConfig(0), relayConfig(0), false)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		nc, err := e.dialRelay(ctx)
		if err != nil {
			t.Fatal(err)
		}
		p, err := e.client.AddPipe(ctx, zmux.NetPipe(nc), zmux.PipeOptions{SendStandby: zmux.Set(true)})
		if err != nil {
			t.Fatal(err)
		}
		busy(t, e)
		time.Sleep(1500 * time.Millisecond) // the join settles and a standby probe runs
		if d := suspectAfter(t, e, uint64(p.ID()), 4*time.Second); d > 2500*time.Millisecond {
			t.Fatalf("standby pipe suspect after %v (standby_probe 1 s + T_suspect)", d)
		}
	})
}

// A session without multipath: keep-alive (on by default, capped at half
// the idle timeout) keeps a live idle session open past its idle timeout,
// and closes it within the idle timeout once its carrier dies silently.
func TestLivenessSingleKeepAlive(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cfg := func() *zmux.Config { return &zmux.Config{MaxIdleTimeout: 2 * time.Second} }
		e := newMemRelayEnv(t, cfg(), cfg(), true)
		done := make(chan error, 1)
		go func() { done <- e.client.Wait() }()
		select {
		case err := <-done:
			t.Fatalf("idle session with keep-alive closed: %v", err)
		case <-time.After(5 * time.Second):
		}
		e.relay.Blackhole(true, true)
		start := time.Now()
		select {
		case err := <-done:
			if !errors.Is(err, zmux.ErrIdleTimeout) {
				t.Fatalf("closed with %v", err)
			}
			if el := time.Since(start); el > 3*time.Second {
				t.Fatalf("closed %v after the carrier died (idle timeout 2 s)", el)
			}
		case <-time.After(6 * time.Second):
			t.Fatal("session alive after its carrier died")
		}
	})
}

// A DeadTimeout set while the session runs bounds the failure of a pipe
// that turns suspect after the change: with the derived limit (half the
// 30 s idle timeout) the black-holed session would stay up for 15 s.
func TestRelaySetLivenessDeadTimeout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := relayConfig(0)
		c.Liveness.ProbeInterval = time.Second
		e := newMemRelayEnv(t, c, relayConfig(0), true)
		if err := e.client.SetLiveness(zmux.Liveness{ProbeInterval: time.Second, DeadTimeout: time.Second}); err != nil {
			t.Fatal(err)
		}
		e.relay.Blackhole(true, true)
		start := time.Now()
		done := make(chan error, 1)
		go func() { done <- e.client.Wait() }()
		select {
		case err := <-done:
			if !errors.Is(err, zmux.ErrPipesLost) {
				t.Fatalf("session ended with %v", err)
			}
			if el := time.Since(start); el > 6*time.Second {
				t.Fatalf("ended after %v", el)
			}
		case <-time.After(12 * time.Second):
			t.Fatal("session still open: the new DeadTimeout was not applied")
		}
	})
}
