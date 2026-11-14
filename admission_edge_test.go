package zmux

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

// lateClock is the host's clock, except that an armed AfterFunc has
// already fired when it is stopped and calls its function only later.
type lateClock struct{ armed atomic.Bool }

func (c *lateClock) Now() time.Time { return time.Now() }

func (c *lateClock) AfterFunc(d time.Duration, f func()) timeshim.Timer {
	if !c.armed.CompareAndSwap(true, false) {
		return time.AfterFunc(d, f)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		f()
	}()
	return lateTimer{}
}

type lateTimer struct{}

func (lateTimer) Stop() bool               { return false }
func (lateTimer) Reset(time.Duration) bool { return false }

// msgHalf is a message-framed half that holds one message and counts
// its closes.
type msgHalf struct{ closed atomic.Int32 }

func (h *msgHalf) Read([]byte) (int, error)     { return 0, nil }
func (h *msgHalf) ReadMessage() ([]byte, error) { return []byte("m"), nil }
func (h *msgHalf) Close() error                 { h.closed.Add(1); return nil }

// TestPeekMessageLateTimer reads a first message while the peek timer
// fires: the peek reports the timeout only after the timer's interruption
// ran, never a message on a carrier the timer then closes.
func TestPeekMessageLateTimer(t *testing.T) {
	clk := &lateClock{}
	cfg := mpConfig()
	cfg.Clock = clk
	lst := mustNewListener(&ListenerConfig{Config: cfg})
	defer func() { _ = lst.Close() }()
	src := netip.MustParseAddrPort("192.0.2.1:1")
	a, err := lst.Adapter().Arrive(src, src)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	h := &msgHalf{}
	clk.armed.Store(true)
	msg, _, fired := a.peekMessage(PipeIO{R: h}, 64, time.Now().Add(time.Hour))
	if !fired || msg != nil || h.closed.Load() != 1 {
		t.Fatalf("message %q, fired %v, closed %d times", msg, fired, h.closed.Load())
	}
}

// TestTakePendingLimitRejects refuses a further carrier of an accepted
// arrival whose source has no free slot as the first carrier is refused:
// through the reject policy, as pending_limit.
func TestTakePendingLimitRejects(t *testing.T) {
	got := make(chan RejectInfo, 1)
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), MaxPendingPerSource: 1,
		OnReject: func(i RejectInfo) RejectAction { got <- i; return RejectClose }})
	defer func() { _ = lst.Close() }()
	src := netip.MustParseAddrPort("192.0.2.1:1")
	a, err := lst.Adapter().Arrive(src, src)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	// The first carrier takes the source's one slot; a further one finds none.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()
	run, err := a.Take(ctx, NetPipe(c2), PipeHint{})
	if err != nil {
		t.Fatal(err)
	}
	ran := make(chan struct{})
	go func() { defer close(ran); _ = run(ctx) }()
	defer func() { cancel(); <-ran }()
	h := &msgHalf{}
	if _, err := a.Take(context.Background(), PipeIO{R: h}, PipeHint{}); !errors.Is(err, errRefused) {
		t.Fatalf("Take: %v", err)
	}
	select {
	case i := <-got:
		if i.Reason != "pending_limit" || i.Src != src {
			t.Fatalf("OnReject %+v", i)
		}
	default:
		t.Fatal("the refusal skipped the reject policy")
	}
	if h.closed.Load() != 1 {
		t.Fatal("the refused carrier was not closed")
	}
}
