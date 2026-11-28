package zmux

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"reflect"
	"runtime"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/netx"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/poolhost"
)

func handlerConfig() *Config {
	return &Config{ClockDomain: NewClockDomain(timeshim.Real), MaxIdleTimeout: -1, Liveness: Liveness{KeepAlivePeriod: -1}}
}

func TestHandlerRegistration(t *testing.T) {
	var c *Conn
	for _, err := range []error{c.HandleStreams(nil, 0), c.HandleUniStreams(nil, 0), c.HandleEvents(nil)} {
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	}
	synctest.Test(t, func(t *testing.T) {
		_, c := bubblePair(t, handlerConfig)
		ctx := context.Background()
		bidi, uni, event := func(*Stream) {}, func(*ReceiveStream) {}, func(Event) {}
		for _, err := range []error{c.HandleStreams(nil, 1), c.HandleUniStreams(nil, 1), c.HandleStreams(bidi, 0), c.HandleUniStreams(uni, -1), c.HandleEvents(nil)} {
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		c.locked(func() { drainEvents(&c.events) })
		waits := [3]chan error{}
		for i := range waits {
			waits[i] = make(chan error, 5)
			for range 4 {
				go func() {
					var err error
					switch i {
					case 0:
						_, err = c.AcceptStream(ctx)
					case 1:
						_, err = c.AcceptUniStream(ctx)
					case 2:
						_, err = c.NextEvent(ctx)
					}
					waits[i] <- err
				}()
			}
		}
		synctest.Wait()
		for i, register := range []func() error{func() error { return c.HandleStreams(bidi, 1) }, func() error { return c.HandleUniStreams(uni, 1) }, func() error { return c.HandleEvents(event) }} {
			if err := register(); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			for range 4 {
				if err := <-waits[i]; !errors.Is(err, ErrHandlerRegistered) {
					t.Fatal(err)
				}
			}
			for j := i + 1; j < len(waits); j++ {
				if len(waits[j]) != 0 {
					t.Fatal("registration woke unrelated consumer")
				}
			}
			if err := register(); !errors.Is(err, ErrHandlerRegistered) {
				t.Fatal(err)
			}
		}
	})
}

func offerBidi(t *testing.T, c *Conn) *Stream {
	t.Helper()
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
	return st
}

func TestHandleStreamQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := bubblePair(t, handlerConfig)
		first := offerBidi(t, client)
		reset := offerBidi(t, client)
		reset.CancelWrite(71)
		synctest.Wait()
		got := make(chan *Stream, 4)
		release := make(chan struct{})
		if err := server.HandleStreams(func(st *Stream) { got <- st; <-release }, 1); err != nil {
			t.Fatal(err)
		}
		in := <-got
		if in.StreamID() != first.StreamID() {
			t.Fatal("wrong first stream")
		}
		last := offerBidi(t, client)
		if server.handles != 1 {
			t.Fatalf("%d handles with one slot", server.handles)
		}
		_ = in.Close()
		synctest.Wait()
		if len(got) != 0 {
			t.Fatal("closing a stream released its callback slot")
		}
		release <- struct{}{}
		in = <-got
		if in.StreamID() != reset.StreamID() {
			t.Fatal("lost reset stream")
		}
		var b [1]byte
		_, err := in.Read(b[:])
		var se *StreamError
		if !errors.As(err, &se) || se.ErrorCode != 71 {
			t.Fatalf("reset before accept: %v", err)
		}
		release <- struct{}{}
		in = <-got
		if in.StreamID() != last.StreamID() {
			t.Fatal("wrong final stream")
		}
		close(release)
		synctest.Wait()
		if _, err := in.Write(b[:]); err != nil {
			t.Fatalf("ownership after return: %v", err)
		}
	})
}

func TestHandleStreamClosing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := bubblePair(t, handlerConfig)
		offerBidi(t, client)
		offerBidi(t, client)
		calls := make(chan struct{}, 2)
		if err := server.HandleStreams(func(*Stream) { calls <- struct{}{}; _ = server.Wait() }, 1); err != nil {
			t.Fatal(err)
		}
		<-calls
		server.Abort(0, "")
		synctest.Wait()
		if len(calls) != 0 || server.handlers.bidi.running != 0 || server.handlers.bidi.call != nil {
			t.Fatal("queued stream claimed after close or callback retained")
		}
	})
}

// panicLogHandler records the attempted warning before failing inside slog.
type panicLogHandler struct{ logRecorder }

func (h *panicLogHandler) Handle(ctx context.Context, rec slog.Record) error {
	_ = h.logRecorder.Handle(ctx, rec)
	panic("logger failed")
}

func TestSafeContainsLoggerPanic(t *testing.T) {
	h := new(panicLogHandler)
	if poolhost.Safe(slog.New(h), "test callback", func() { panic("callback failed") }) {
		t.Fatal("Safe reported success after a callback panic")
	}
	if !slices.Equal(h.attrs, []string{"test callback"}) {
		t.Fatalf("logged callbacks %v", h.attrs)
	}
}

func panicLoggerConfig() *Config {
	cfg := handlerConfig()
	cfg.Logger = slog.New(new(panicLogHandler))
	return cfg
}

func TestHandleStreamPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := bubblePair(t, panicLoggerConfig)
		if err := server.HandleStreams(func(*Stream) { panic("bidi") }, 1); err != nil {
			t.Fatal(err)
		}
		st := offerBidi(t, client)
		var b [1]byte
		_, read := st.Read(b[:])
		_, write := st.Write(b[:])
		if err := server.HandleUniStreams(func(*ReceiveStream) { panic("uni") }, 1); err != nil {
			t.Fatal(err)
		}
		uni, err := client.OpenUniStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = uni.Write(b[:]); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		_, uw := uni.Write(b[:])
		if server.handlers.uni.running != 0 || server.handlers.bidi.running != 0 {
			t.Fatal("panic retained a callback slot")
		}
		for _, err := range []error{read, write, uw} {
			var se *StreamError
			if !errors.As(err, &se) || se.ErrorCode != StreamCancelled {
				t.Fatalf("panic cancellation: %v", err)
			}
		}
	})
}

func TestHandleUniCredit(t *testing.T) {
	for _, tc := range []struct {
		name           string
		setting, raise int64
		before, after  int
	}{
		{"default", 0, 0, 16, 256}, {"raised", 0, 300, 16, 300}, {"explicit", 7, 0, 7, 7}, {"disabled", -1, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, server := bubblePair(t, func() *Config { cfg := handlerConfig(); cfg.MaxIncomingUniStreams = tc.setting; return cfg })
				if err := server.RaiseIncomingStreams(0, tc.raise); err != nil {
					t.Fatal(err)
				}
				if err := server.HandleStreams(func(*Stream) {}, 1); err != nil {
					t.Fatal(err)
				}
				var held []*SendStream
				check := func(want int) {
					synctest.Wait()
					for len(held) < want {
						st, err := client.OpenUniStream()
						if err != nil {
							t.Fatalf("open %d of %d: %v", len(held)+1, want, err)
						}
						held = append(held, st)
					}
					_, err := client.OpenUniStream()
					if _, ok := errors.AsType[*StreamLimitReachedError](err); !ok {
						t.Fatalf("expected limit at %d: %v", want, err)
					}
				}
				check(tc.before)
				if err := server.HandleUniStreams(func(*ReceiveStream) {}, 1); err != nil {
					t.Fatal(err)
				}
				check(tc.after)
				runtime.KeepAlive(held)
			})
		})
	}
}

func TestHandleEventQueueModel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, c := bubblePair(t, panicLoggerConfig)
		synctest.Wait()
		first := Event{Kind: EventPipeClosed, PipeID: 12, Direction: DirectionSendOnly}
		c.locked(func() { drainEvents(&c.events); c.events.push(first) })
		got := make(chan Event, 16)
		err := c.HandleEvents(func(ev Event) {
			got <- ev
			if ev.Kind == first.Kind {
				_ = c.Wait()
				panic("first consumed")
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if ev := <-got; !reflect.DeepEqual(ev, first) { //nolint:govet // The fixture has no health error payload.
			t.Fatal(ev)
		}
		update := Event{Kind: EventPipePriorityChanged, PipeID: 8, Priority: 1}
		auth := Event{Kind: EventJoinAuthFailed, Reason: JoinAuthReasonAuth}
		var want []Event
		c.locked(func() {
			for _, ev := range []Event{update, auth, {Kind: EventResumeUnattended}, auth,
				{Kind: update.Kind, PipeID: update.PipeID, Priority: 9}, {Kind: EventSessionResumed}} {
				c.events.push(ev)
				want = modelEventPush(want, ev)
			}
			c.events.push(Event{Kind: EventPipeFailed, PipeID: 14, Direction: DirectionSendOnly})
			c.events.retire(12, false)
			c.events.retire(14, true)
		})
		// Pipe 12's terminal event was read when its callback started.
		// Pipe 14's unread failure is folded into the retirement count.
		want = append(want, Event{Kind: EventPipesRetired, Retired: 2, Failed: 1, Last: []PipeID{12, 14}})
		c.Abort(0, "")
		synctest.Wait()
		for _, w := range want {
			if ev := <-got; !reflect.DeepEqual(ev, w) { //nolint:govet // The fixture has no health error payload.
				t.Fatalf("got %+v want %+v", ev, w)
			}
		}
		if c.handlers.eventRunning || c.handlers.event != nil {
			t.Fatal("frozen queue retained its worker or handler")
		}
	})
}

func TestHandleEventsAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := bubblePair(t, handlerConfig)
		a.locked(func() { a.events.push(Event{Kind: EventCapacityBelow}) })
		a.Abort(0, "")
		_ = a.Wait()
		_ = b.Wait()
		var want []Event
		a.peek(func() { want = a.events.snapshot() })
		var got []Event
		if err := a.HandleEvents(func(e Event) { got = append(got, e) }); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(want) == 0 || !reflect.DeepEqual(got, want) { //nolint:govet // The fixture has no health error payload.
			t.Fatalf("post-close delivery %+v want %+v", got, want)
		}
		for {
			if _, err := b.NextEvent(t.Context()); err != nil {
				break
			}
		}
		if err := b.HandleEvents(func(Event) {}); err == nil || !errors.Is(err, b.Wait()) {
			t.Fatalf("drained frozen queue: %v", err)
		}
	})
}

func TestHandleEventsRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, c := bubblePair(t, handlerConfig)
		synctest.Wait()
		c.locked(func() { drainEvents(&c.events) })
		got := make(chan uint64)
		resume := make(chan struct{})
		err := c.HandleEvents(func(ev Event) {
			if ev.Kind == EventCapacityBelow {
				got <- ev.Weight
				if ev.Weight%2 == 0 {
					<-resume
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range uint64(64) {
			// Even events start an idle worker. Hold their callback through the odd enqueue.
			if i%2 == 0 {
				synctest.Wait()
				if c.handlers.eventRunning {
					t.Fatal("worker stayed active on an empty queue")
				}
			}
			c.locked(func() { c.events.push(Event{Kind: EventCapacityBelow, Weight: i}) })
			if i%2 == 1 {
				resume <- struct{}{}
			}
			if v := <-got; v != i {
				t.Fatalf("received %d want %d", v, i)
			}
		}
		c.Abort(0, "")
		synctest.Wait()
	})
}

func TestHandleEventsGoexit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, c := bubblePair(t, handlerConfig)
		first, second := Event{Kind: EventCapacityBelow}, Event{Kind: EventSessionSuspended}
		c.locked(func() { drainEvents(&c.events); c.events.push(first); c.events.push(second) })
		c.Abort(0, "")
		_ = c.Wait()
		var want []Event
		c.peek(func() { want = c.events.snapshot() })
		var got []Event
		err := c.HandleEvents(func(ev Event) {
			got = append(got, ev)
			if ev.Kind == first.Kind {
				runtime.Goexit()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(want) < 2 || want[0].Kind != first.Kind || want[1].Kind != second.Kind || !reflect.DeepEqual(got, want) { //nolint:govet // The fixture has no health error payload.
			t.Fatalf("delivered %+v, frozen backlog %+v", got, want)
		}
		if c.handlers.eventRunning || c.handlers.event != nil || c.events.head != nil {
			t.Fatal("frozen queue retained work or handler after Goexit")
		}
	})
}

func TestHandleStreamsTwoSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := bubblePair(t, panicLoggerConfig)
		var ids []StreamID
		gates := map[StreamID]chan struct{}{}
		for range 4 {
			st := offerBidi(t, client)
			id := st.StreamID()
			ids = append(ids, id)
			gates[id] = make(chan struct{})
		}
		started := make(chan StreamID, 4)
		err := server.HandleStreams(func(st *Stream) {
			id := st.StreamID()
			started <- id
			<-gates[id]
			if id == ids[0] {
				panic("release first slot")
			}
		}, 2)
		if err != nil {
			t.Fatal(err)
		}
		check := func(want ...StreamID) {
			t.Helper()
			synctest.Wait()
			var got []StreamID
			for len(started) > 0 {
				got = append(got, <-started)
			}
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Fatalf("started %v, want %v", got, want)
			}
		}
		// The two oldest streams can start in either order. Keep the second
		// callback blocked while the first panics and its successor takes the slot.
		check(ids[0], ids[1])
		close(gates[ids[0]])
		check(ids[2])
		close(gates[ids[1]])
		check(ids[3])
		close(gates[ids[2]])
		close(gates[ids[3]])
		check()
		if server.handlers.bidi.running != 0 {
			t.Fatal("callbacks retained slots")
		}
	})
}

func TestHandleStreamsCloseAfterClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := bubblePair(t, handlerConfig)
		first := offerBidi(t, client).StreamID()
		offerBidi(t, client)
		var calls []StreamID
		var claimed []func()
		// Capture the batch an unlock would normally launch immediately.
		server.locked(func() {
			h := &server.handlersLocked().bidi
			h.limit = 1
			h.call = func(h *handle) { st := newStream(h); calls = append(calls, st.StreamID()) }
			claimed = server.collectStreamsLocked()
		})
		if len(claimed) != 1 {
			t.Fatalf("claimed %d streams", len(claimed))
		}
		server.Abort(0, "")
		_ = server.Wait()
		claimed[0]()
		synctest.Wait()
		if !slices.Equal(calls, []StreamID{first}) || server.handlers.bidi.running != 0 {
			t.Fatalf("delivered %v after close, want [%d]", calls, first)
		}
	})
}

func TestPathAdviceEventSubscriptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, scfg := mpConfig(), mpConfig()
		cfg.ClockDomain, scfg.ClockDomain = NewClockDomain(timeshim.Real), NewClockDomain(timeshim.Real)
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
		setQueue := func(n uint64) {
			c.locked(func() { c.s.SetCarrierUnsent(0, 0, n, c.clk.now()) })
		}
		refresh := func() {
			t.Helper()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if _, err := c.Ping(ctx); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
		}
		events := make(chan Event, 1)
		readAdvice := func() {
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
			defer cancel()
			for {
				ev, err := c.NextEvent(ctx)
				if err != nil {
					t.Errorf("NextEvent advice: %v", err)
					return
				}
				if ev.Kind == EventPipePathQuality {
					events <- ev
					return
				}
			}
		}
		wantAdvice := func() {
			t.Helper()
			select {
			case ev := <-events:
				if ev.PathQuality != PipePathQualityLocalStall {
					t.Fatalf("unexpected advice %+v", ev)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("no path advice")
			}
		}
		go readAdvice()
		synctest.Wait()
		refresh()
		setQueue(2 << 20)
		wantAdvice()
		if err := c.StopEvents(); err != nil {
			t.Fatal(err)
		}
		c.locked(func() {
			if _, _, ok := c.s.ReplacementAdvice(0); ok || c.events.pollAdvice {
				t.Error("stopping polling kept its advisory subscription")
			}
		})
		setQueue(0)
		handle := func(ev Event) {
			if ev.Kind == EventPipePathQuality {
				events <- ev
			}
		}
		if err := c.HandleEvents(handle); err != nil {
			t.Fatal(err)
		}
		refresh()
		setQueue(2 << 20)
		wantAdvice()
		if err := c.StopEvents(); err != nil {
			t.Fatal(err)
		}
		c.locked(func() {
			if _, _, ok := c.s.ReplacementAdvice(0); ok || c.handlers.eventsRegistered {
				t.Error("unregistering callback kept its advisory subscription")
			}
		})
	})
}
