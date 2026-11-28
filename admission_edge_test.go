package zmux

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
)

// lateClock is the host's clock, except that an armed AfterFunc has
// already fired when it is stopped and calls its function only later.
type lateClock struct {
	resume   <-chan struct{}
	stopping chan<- struct{}
	armed    atomic.Bool
}

func (c *lateClock) Now() time.Time { return time.Now() }

func (c *lateClock) AfterFunc(d time.Duration, f func()) timeshim.Timer {
	if !c.armed.CompareAndSwap(true, false) {
		return time.AfterFunc(d, f)
	}
	return lateTimer{stopping: c.stopping, run: func() {
		<-c.resume
		f()
	}}
}

type lateTimer struct {
	stopping chan<- struct{}
	run      func()
}

func (t lateTimer) Stop() bool             { close(t.stopping); go t.run(); return false }
func (lateTimer) Reset(time.Duration) bool { return false }

// msgHalf is a message-framed half that holds one message and counts
// its closes.
type msgHalf struct{ closed atomic.Int32 }

func (h *msgHalf) Read([]byte) (int, error)                 { return 0, nil }
func (h *msgHalf) ReadMessage(int) (ReceivedMessage, error) { return OwnMessage([]byte("m")), nil }
func (h *msgHalf) Close() error                             { h.closed.Add(1); return nil }

// TestPeekMessageLateTimer forces the timer's Stop to report that the
// timer already fired while its interruption is still held.
func TestPeekMessageLateTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stopping, resume := make(chan struct{}), make(chan struct{})
		unhold := sync.OnceFunc(func() { close(resume) })
		defer unhold()
		clk := &lateClock{stopping: stopping, resume: resume}
		cfg := mpConfig()
		cfg.ClockDomain = NewClockDomain(clk)
		lst := mustNewListener(&ListenerConfig{Config: cfg})
		defer func() { _ = lst.Close() }()
		a, err := lst.Adapter().Arrive(loop, loop)
		if err != nil {
			t.Fatal(err)
		}
		defer a.Release()
		h := &msgHalf{}
		clk.armed.Store(true)
		type result struct {
			msg   ReceivedMessage
			fired bool
		}
		done := make(chan result, 1)
		go func() {
			msg, _, _, fired := a.peekMessage(t.Context(), PipeIO{R: h}, 64, clk.Now().Add(time.Hour))
			done <- result{msg, fired}
		}()
		<-stopping
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("peek returned before the timer interrupted the carrier")
		default:
		}
		unhold()
		got := <-done
		defer got.msg.Release()
		if !got.fired || got.msg.span.Buf != nil || h.closed.Load() != 1 {
			t.Fatalf("message %q, fired %v, closed %d times", got.msg.Bytes(), got.fired, h.closed.Load())
		}
	})
}

// TestTakePendingLimitRejects refuses a further carrier of an accepted
// arrival whose source has no free slot as the first carrier is refused:
// through the reject policy, as pending_limit.
func TestTakePendingLimitRejects(t *testing.T) {
	got := make(chan RejectInfo, 1)
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), MaxPendingPerSource: 2,
		OnReject: func(i RejectInfo) RejectAction { got <- i; return RejectClose }})
	defer func() { _ = lst.Close() }()
	src := netip.MustParseAddrPort("192.0.2.1:1")
	a, err := lst.Adapter().Arrive(src, src)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	// Keep both source slots occupied until the refusal is observed.
	ctx, cancel := context.WithCancel(context.Background())
	var running []chan struct{}
	var peers []net.Conn
	defer func() {
		cancel()
		for _, peer := range peers {
			_ = peer.Close()
		}
		for _, done := range running {
			<-done
		}
	}()
	for range 2 {
		c1, c2 := net.Pipe()
		peers = append(peers, c1)
		run, err := a.Take(ctx, NetPipe(c2), PipeHint{})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		running = append(running, done)
		go func() { defer close(done); _ = run(ctx) }()
	}
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

// emptyMsgHalf is a message-framed half whose first message is empty.
type emptyMsgHalf struct{ closed atomic.Int32 }

func (h *emptyMsgHalf) Read([]byte) (int, error)                 { return 0, nil }
func (h *emptyMsgHalf) ReadMessage(int) (ReceivedMessage, error) { return OwnMessage(nil), nil }
func (h *emptyMsgHalf) Write(p []byte) (int, error)              { return len(p), nil }
func (h *emptyMsgHalf) Close() error                             { h.closed.Add(1); return nil }

// TestAdmitEmptyFirstMessageCloses closes a message-framed carrier whose
// first message is empty without calling Decide: a takeover could not
// replay an empty message.
func TestAdmitEmptyFirstMessageCloses(t *testing.T) {
	var decided atomic.Int32
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Peek: 1, PeekTimeout: time.Minute,
		Decide: func(context.Context, CarrierInfo, []byte) Decision {
			decided.Add(1)
			return Decision{Verdict: VerdictFallback}
		}}})
	defer func() { _ = lst.Close() }()
	src := netip.MustParseAddrPort("192.0.2.1:1")
	a, err := lst.Adapter().Arrive(src, src)
	if err != nil {
		t.Fatal(err)
	}
	h := &emptyMsgHalf{}
	if _, ok := a.Admit(context.Background(), PipeIO{R: h, W: h, Framing: FramingRecords}); ok {
		t.Fatal("admitted")
	}
	if decided.Load() != 0 || h.closed.Load() == 0 {
		t.Fatalf("Decide called %d times, closed %d times", decided.Load(), h.closed.Load())
	}
}
