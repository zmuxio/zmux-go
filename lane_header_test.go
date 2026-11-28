package zmux

import (
	"context"
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
)

// HandleLane closes a lane whose header does not arrive within
// DefaultFirstRecordTimeout, and not before.
func TestHandleLaneHeaderTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Unix(1_000_000, 0))
		a, b := net.Pipe()
		defer func() { _ = a.Close() }()
		done := make(chan error, 1)
		go func() { done <- handleLaneOn(t.Context(), clk, NetPipe(b)) }()
		wctx, wcancel := context.WithTimeout(t.Context(), time.Minute)
		defer wcancel()
		if err := clk.WaitPending(wctx, 1); err != nil {
			t.Fatalf("the header timer was not armed: %v", err)
		}
		clk.Advance(DefaultFirstRecordTimeout - time.Nanosecond)
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("HandleLane returned %v before the timeout", err)
		default:
		}
		clk.Advance(time.Nanosecond)
		select {
		case err := <-done:
			if !errors.Is(err, errTimedOut) {
				t.Fatalf("HandleLane returned %v, want the first-record timeout", err)
			}
		case <-wctx.Done():
			t.Fatal("HandleLane did not return after the timeout")
		}
	})
}

func TestDefaultFirstRecordTimeout(t *testing.T) {
	if DefaultFirstRecordTimeout != 2*time.Second {
		t.Fatalf("DefaultFirstRecordTimeout = %v", DefaultFirstRecordTimeout)
	}
	cfg, err := (&ListenerConfig{}).resolve()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FirstRecordTimeout != DefaultFirstRecordTimeout {
		t.Fatalf("listener default %v", cfg.FirstRecordTimeout)
	}
}
