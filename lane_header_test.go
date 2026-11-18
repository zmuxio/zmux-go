package zmux

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

// HandleLane closes a lane whose header does not arrive within
// DefaultFirstRecordTimeout, and not before.
func TestHandleLaneHeaderTimeout(t *testing.T) {
	clk := timeshim.NewFake(time.Unix(1_000_000, 0))
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	done := make(chan error, 1)
	go func() { done <- handleLaneOn(context.Background(), clk, NetPipe(b)) }()
	wctx, wcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer wcancel()
	if err := clk.WaitPending(wctx, 1); err != nil {
		t.Fatalf("the header timer was not armed: %v", err)
	}
	clk.Advance(DefaultFirstRecordTimeout - time.Nanosecond)
	select {
	case err := <-done:
		t.Fatalf("HandleLane returned %v before the timeout", err)
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(time.Nanosecond)
	select {
	case err := <-done:
		if !errors.Is(err, errTimedOut) {
			t.Fatalf("HandleLane returned %v, want the first-record timeout", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("HandleLane did not return after the timeout")
	}
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
