package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
)

type deferredReleaseClock struct {
	*timeshim.Fake
	entered chan struct{}
	resume  chan struct{}
	armed   atomic.Bool
}

func (c *deferredReleaseClock) Now() time.Time {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.resume
	}
	return c.Fake.Now()
}

func TestDeferredAdmitFirstCountsCloseBeforeReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &deferredReleaseClock{Fake: timeshim.NewFake(time.Unix(1, 0)),
			entered: make(chan struct{}), resume: make(chan struct{})}
		unhold := sync.OnceFunc(func() { close(clock.resume) })
		defer unhold()
		l := mustNewListener(&ListenerConfig{Config: &Config{ClockDomain: NewClockDomain(clock)},
			Admission: &Admission{PeekTimeout: time.Minute, Decide: func(context.Context, CarrierInfo, []byte) Decision {
				return Decision{Verdict: VerdictDefer}
			}}})
		t.Cleanup(func() { _ = l.Close() })
		a, ok := l.Adapter().AdmitUpgrade(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody), nil, nil)
		if !ok {
			t.Fatal("upgrade was not deferred")
		}
		first, finish := make(chan struct{}), make(chan struct{})
		admitted := make(chan bool, 1)
		go func() {
			_, ok := a.AdmitFirst(t.Context(), func(ctx context.Context) (PipeIO, error) {
				close(first)
				<-ctx.Done()
				<-finish
				return PipeIO{}, ctx.Err()
			})
			admitted <- ok
		}()
		<-first
		clock.armed.Store(true)
		_ = l.Close()
		// Hold the stop callback in its time sample. The first carrier's
		// handler takes its own error path before the callback can finish.
		<-clock.entered
		close(finish)
		if <-admitted {
			t.Fatal("first carrier admitted after listener close")
		}
		want := AdmissionStats{Closed: 1}
		if got := l.Stats().Admission; got != want {
			t.Fatalf("first carrier returned with admission stats %+v, want %+v", got, want)
		}
		unhold()
		synctest.Wait()
		if got := l.Stats().Admission; got != want {
			t.Fatalf("both cleanup paths counted admission stats %+v, want %+v", got, want)
		}
	})
}

// VerdictDefer defers a decision only on an upgrade's head (AdmitUpgrade):
// the arrival is returned unaccepted and uncounted, the head's metadata
// kept for the carrier's decision at StageCarrier. VerdictMore on that
// head, VerdictDefer on an exchange carrier's head (DecideRequest) and on
// a carrier's start are VerdictClose.
func TestAdmissionDeferOnlyUpgrades(t *testing.T) {
	var verdict atomic.Value
	adm := &Admission{Peek: 1, MaxPeek: 32, Decide: func(context.Context, CarrierInfo, []byte) Decision {
		return Decision{Verdict: verdict.Load().(AdmissionVerdict), Need: 8}
	}}
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: adm})
	defer func() { _ = lst.Close() }()
	upgrade := func() (*Arrival, bool, error) {
		var a *Arrival
		var ok bool
		err := catchAbort(func() {
			a, ok = lst.Adapter().AdmitUpgrade(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody), nil, nil)
		})
		return a, ok, err
	}
	verdict.Store(VerdictDefer)
	a, ok, err := upgrade()
	if !ok || err != nil || a.Admitted() {
		t.Fatalf("deferred upgrade: ok %v, %v", ok, err)
	}
	if d := a.deferred; d == nil || d.info.Stage != StageCarrier || !d.info.Upgrade || d.info.Request == nil {
		t.Fatalf("deferral %+v", d)
	}
	if st := lst.Stats().Admission; st != (AdmissionStats{}) {
		t.Fatalf("a deferral counted: %+v", st)
	}
	a.Release() // counted Closed: its decision never came
	verdict.Store(VerdictMore)
	if _, ok, err := upgrade(); ok || err == nil {
		t.Fatal("VerdictMore on an upgrade's head was not closed")
	}
	verdict.Store(VerdictDefer)
	b, err := lst.Adapter().Arrive(loop, loop)
	if err != nil {
		t.Fatal(err)
	}
	err = catchAbort(func() {
		b.DecideRequest(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody), nil)
	})
	if err == nil {
		t.Fatal("an exchange head deferred")
	}
	x, y := net.Pipe()
	defer func() { _ = y.Close() }()
	go func() { _, _ = y.Write([]byte("x")) }()
	if err := lst.HandlePipe(context.Background(), NetPipe(x), PipeHint{}); err == nil {
		t.Fatal("VerdictDefer at a carrier's start was not closed")
	}
	if st := lst.Stats().Admission; st != (AdmissionStats{Closed: 4}) {
		t.Fatalf("stats %+v", st)
	}
}

type observedPeekConn struct {
	net.Conn
	reads chan struct{}
}

func (c *observedPeekConn) Read(p []byte) (int, error) {
	c.reads <- struct{}{}
	return c.Conn.Read(p)
}

func TestAdmissionDeferredByteStreamTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		peek int
		more bool
	}{
		{name: "initial", peek: 4},
		{name: "more", peek: 1, more: true},
		{name: "more from empty", more: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const timeout = time.Second
			clk := timeshim.NewFake(time.Now())
			cfg := mpConfig()
			cfg.ClockDomain = NewClockDomain(clk)
			called := make(chan struct{}, 1)
			lst := mustNewListener(&ListenerConfig{Config: cfg,
				Admission: &Admission{Peek: tc.peek, MaxPeek: 4, PeekTimeout: timeout, FallbackOnPeekTimeout: true,
					Decide: func(_ context.Context, info CarrierInfo, _ []byte) Decision {
						if info.Stage == StageHead {
							return Decision{Verdict: VerdictDefer}
						}
						if tc.more {
							return Decision{Verdict: VerdictMore, Need: 4}
						}
						return Decision{Verdict: VerdictAccept}
					}},
				Fallback: func(context.Context, *Takeover) { called <- struct{}{} },
			})
			t.Cleanup(func() { _ = lst.Close() })
			a, ok := lst.Adapter().AdmitUpgrade(httptest.NewRecorder(), httptest.NewRequest(http.MethodConnect, "/", http.NoBody), nil, nil)
			if !ok {
				t.Fatal("upgrade head did not defer")
			}
			t.Cleanup(a.Release)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			x, y := net.Pipe()
			defer func() { _ = y.Close() }()
			reads := make(chan struct{}, 2)
			done := make(chan bool, 1)
			go func() {
				_, accepted := a.AdmitFirst(ctx, func(context.Context) (PipeIO, error) {
					return NetPipe(&observedPeekConn{Conn: x, reads: reads}), nil
				})
				done <- accepted
			}()
			waitRead := func() {
				t.Helper()
				select {
				case <-reads:
				case <-ctx.Done():
					t.Fatal("prefix read did not start")
				}
			}
			waitRead()
			if tc.more && tc.peek > 0 {
				if _, err := y.Write([]byte("x")); err != nil {
					t.Fatal(err)
				}
				waitRead()
			}
			clk.Advance(timeout)
			select {
			case accepted := <-done:
				if accepted {
					t.Fatal("accepted an incomplete prefix")
				}
			case <-ctx.Done():
				t.Fatal("admission outlived its peek deadline")
			}
			select {
			case <-called:
				t.Fatal("incomplete deferred prefix reached fallback")
			default:
			}
			if got := lst.Stats().Admission; got != (AdmissionStats{PeekTimedOut: 1}) {
				t.Fatalf("stats %+v", got)
			}
		})
	}
}

// loop is a source for arrivals made by hand.
var loop = netip.MustParseAddrPort("127.0.0.1:1")

// catchAbort runs f and returns the http.ErrAbortHandler it panics with.
func catchAbort(f func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	f()
	return nil
}

// A byte-stream carrier that is no net.Conn reaches ListenerConfig.Fallback
// as a Takeover whose Pipe replays the bytes read, its sending half a view
// of its own, with no Conn view; zmux closes it when Fallback returns.
func TestAdmissionFallbackPipe(t *testing.T) {
	type seen struct {
		info   CarrierInfo
		hasErr error
		read   []byte
		conn   bool
		sameW  bool
	}
	got := make(chan seen, 1)
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	p := PipeIO{R: io.NopCloser(a), W: a} // two halves: no net.Conn view
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Peek: 3,
		Decide: func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictFallback} }},
		Fallback: func(_ context.Context, tk *Takeover) {
			_, conn := tk.Conn()
			buf := make([]byte, 6)
			_, err := io.ReadFull(tk.Pipe().R, buf)
			got <- seen{info: tk.Info, read: buf, conn: conn, sameW: tk.Pipe().W.(interface{ Unwrap() io.WriteCloser }).Unwrap() == p.W, hasErr: err}
		}})
	defer func() { _ = lst.Close() }()
	done := make(chan error, 1)
	go func() { done <- lst.HandlePipe(context.Background(), p, PipeHint{}) }()
	go func() { _, _ = b.Write([]byte("abcdef")) }()
	c := <-got
	if c.conn || !c.sameW || c.hasErr != nil || !bytes.Equal(c.read, []byte("abcdef")) ||
		c.info.Stage != StageCarrier || c.info.Framing != FramingByteStream || c.info.Source != c.info.Peer {
		t.Fatalf("takeover %+v", c)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// zmux closed the carrier once Fallback returned.
	_ = b.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := b.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("carrier after the fallback: %v", err)
	}
	if st := lst.Stats().Admission; st != (AdmissionStats{Fallback: 1}) {
		t.Fatalf("stats %+v", st)
	}
}

// Listener.Close ends a prefix read at once rather than at PeekTimeout.
func TestAdmissionPeekEndsOnClose(t *testing.T) {
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Peek: 4, PeekTimeout: time.Minute,
		Decide: func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictAccept} }}})
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	reads := make(chan struct{}, 1)
	carrier := &observedPeekConn{Conn: a, reads: reads}
	done := make(chan struct{})
	go func() { _ = lst.HandlePipe(context.Background(), NetPipe(carrier), PipeHint{}); close(done) }()
	waitTestSignal(t, reads, "prefix read")
	_ = lst.Close()
	waitTestSignal(t, done, "prefix read cancellation")
	if st := lst.Stats().Admission; st != (AdmissionStats{Closed: 1}) {
		t.Fatalf("stats %+v", st)
	}
}

// streamHalf is a byte-stream half whose reads complete at once, counting
// its closes; it takes no read deadline, so an interruption closes it.
type streamHalf struct{ closed atomic.Int32 }

func (h *streamHalf) Read(b []byte) (int, error) { return copy(b, "abcd"), nil }
func (h *streamHalf) Close() error               { h.closed.Add(1); return nil }

// A peek whose context ended (the listener stopped) while the read
// completed is an interrupted read: the carrier is closed and nothing read
// is handed on, however the read and the interruption raced. A context
// ended before the peek makes the interruption's start certain.
func TestPeekInterruptedByStop(t *testing.T) {
	lst := mustNewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	a, err := lst.Adapter().Arrive(loop, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	deadline := time.Now().Add(time.Hour)
	sh := &streamHalf{}
	sp := owned(PipeIO{R: sh})
	if prefix, fired, _, failed := a.peekPipe(ctx, sp, nil, 4, deadline, false); prefix != nil || fired || !failed || sh.closed.Load() != 1 {
		t.Fatalf("stream: prefix %q, fired %v, closed %d", prefix, fired, sh.closed.Load())
	}
	mh := &msgHalf{}
	mp := owned(PipeIO{R: mh, Framing: FramingRecords})
	if msg, _, failed, fired := a.peekMessage(ctx, mp, 64, deadline); msg.span.Buf != nil || !failed || fired {
		t.Fatalf("message: %q, failed %v, fired %v", msg.Bytes(), failed, fired)
	}
}

// failHalf is a message-framed half whose first read fails.
type failHalf struct{ closed atomic.Int32 }

func (h *failHalf) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (h *failHalf) ReadMessage(int) (ReceivedMessage, error) {
	return ReceivedMessage{}, io.ErrUnexpectedEOF
}
func (h *failHalf) Close() error { h.closed.Add(1); return nil }

// A message-framed carrier without a first message to decide on (its read
// fails: it ended, or sent a text message) is closed without a decision,
// counted Closed, never handed to the fallback.
func TestAdmissionFirstMessageFails(t *testing.T) {
	var decided, fell atomic.Int32
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Peek: 1,
		Decide: func(context.Context, CarrierInfo, []byte) Decision {
			decided.Add(1)
			return Decision{Verdict: VerdictFallback}
		}},
		Fallback: func(context.Context, *Takeover) { fell.Add(1) }})
	defer func() { _ = lst.Close() }()
	h := &failHalf{}
	_ = lst.HandlePipe(context.Background(), PipeIO{R: h, W: nopWriteCloser{}, Framing: FramingRecords}, PipeHint{})
	if decided.Load() != 0 || fell.Load() != 0 || h.closed.Load() != 1 {
		t.Fatalf("decided %d, fallback %d, closed %d", decided.Load(), fell.Load(), h.closed.Load())
	}
	if st := lst.Stats().Admission; st != (AdmissionStats{Closed: 1}) {
		t.Fatalf("stats %+v", st)
	}
}

type nopWriteCloser struct{}

func (nopWriteCloser) Write(b []byte) (int, error) { return len(b), nil }
func (nopWriteCloser) Close() error                { return nil }

// AdmitFirst owns the wait for a deferred decision's carrier and counts
// how it ends: PeekTimedOut at the deferral's deadline, Closed when the
// listener stops, HandshakeFailed when the upgrade or the wait fails
// otherwise; a deferral released without AdmitFirst counts Closed. With
// the head accepted, it returns the carrier as it is.
func TestAdmissionAdmitFirst(t *testing.T) {
	blocked := func(ctx context.Context) (PipeIO, error) { <-ctx.Done(); return PipeIO{}, ctx.Err() }
	failed := func(context.Context) (PipeIO, error) { return PipeIO{}, io.ErrUnexpectedEOF }
	for _, x := range []struct {
		run     func(*Listener, *Arrival) bool
		name    string
		verdict AdmissionVerdict
		timeout time.Duration
		want    AdmissionStats
	}{
		{name: "timed out", verdict: VerdictDefer, timeout: 20 * time.Millisecond, run: func(_ *Listener, a *Arrival) bool {
			_, ok := a.AdmitFirst(context.Background(), blocked)
			return ok
		}, want: AdmissionStats{PeekTimedOut: 1}},
		{name: "listener closed", verdict: VerdictDefer, timeout: time.Minute, run: func(l *Listener, a *Arrival) bool {
			entered := make(chan struct{})
			go func() { <-entered; _ = l.Close() }()
			_, ok := a.AdmitFirst(context.Background(), func(ctx context.Context) (PipeIO, error) {
				close(entered)
				return blocked(ctx)
			})
			return ok
		}, want: AdmissionStats{Closed: 1}},
		{name: "upgrade failed", verdict: VerdictDefer, timeout: time.Minute, run: func(_ *Listener, a *Arrival) bool {
			_, ok := a.AdmitFirst(context.Background(), failed)
			return ok
		}, want: AdmissionStats{HandshakeFailed: 1}},
		{name: "released", verdict: VerdictDefer, timeout: time.Minute, run: func(_ *Listener, a *Arrival) bool { a.Release(); return false },
			want: AdmissionStats{Closed: 1}},
		{name: "head accepted", verdict: VerdictAccept, timeout: time.Minute, run: func(_ *Listener, a *Arrival) bool {
			x, y := net.Pipe()
			defer func() { _ = x.Close(); _ = y.Close() }()
			p, ok := a.AdmitFirst(context.Background(), func(context.Context) (PipeIO, error) { return NetPipe(x), nil })
			return ok && p.R == x
		}, want: AdmissionStats{Accepted: 1}},
	} {
		t.Run(x.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := mpConfig()
				cfg.ClockDomain = NewClockDomain(timeshim.Real)
				lst := mustNewListener(&ListenerConfig{Config: cfg, Admission: &Admission{PeekTimeout: x.timeout,
					Decide: func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: x.verdict} }}})
				defer func() { _ = lst.Close() }()
				a, ok := lst.Adapter().AdmitUpgrade(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", http.NoBody), nil, nil)
				if !ok {
					t.Fatalf("%s: not admitted", x.name)
				}
				if got := x.run(lst, a); got != (x.verdict == VerdictAccept) {
					t.Fatalf("%s: AdmitFirst %v", x.name, got)
				}
				a.Release() // a further release counts nothing
				if st := lst.Stats().Admission; st != x.want {
					t.Fatalf("%s: stats %+v", x.name, st)
				}
			})
		})
	}
}
