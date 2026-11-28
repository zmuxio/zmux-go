package zmuxws

import (
	"bytes"
	"context"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/netx/wsconn"
	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

// takeMark starts a first message that the host takes over.
var takeMark = []byte("TAKE")

// taken is what the host's takeover saw: the carrier's metadata, its
// framing, whether it had a net.Conn view, and the messages it relayed.
type taken struct {
	info zmux.CarrierInfo
	msgs [][]byte
	conn bool
}

// deferEnv is a listener whose admission defers every upgrade to its first
// message and decides there, as a node of a reverse tunnel does: a message
// that starts with takeMark is the host's own (it takes the carrier over
// and relays its records), any other is a zmux carrier, accepted while its
// source has made fewer than limit carriers.
type deferEnv struct {
	l        *zmux.Listener
	taken    chan taken
	perSrc   map[netip.Prefix]int
	firstMsg chan []byte
	finished chan struct{}
	u        string
	mu       sync.Mutex
	heads    atomic.Int32
	firsts   atomic.Int32
	badInfo  atomic.Int32
}

func newDeferEnv(t *testing.T, limit, maxPending int, adm *zmux.Admission, fallback func(context.Context, *zmux.Takeover)) *deferEnv {
	t.Helper()
	return newDeferEnvOn(t, mpConfig(), limit, maxPending, adm, fallback)
}

func newDeferEnvOn(t *testing.T, cfg *zmux.Config, limit, maxPending int, adm *zmux.Admission, fallback func(context.Context, *zmux.Takeover)) *deferEnv {
	t.Helper()
	e := &deferEnv{perSrc: map[netip.Prefix]int{}, taken: make(chan taken, 1), firstMsg: make(chan []byte, 4), finished: make(chan struct{}, 1)}
	if adm == nil {
		adm = &zmux.Admission{}
	}
	if adm.Decide == nil {
		adm.Decide = func(_ context.Context, info zmux.CarrierInfo, prefix []byte) zmux.Decision {
			if !info.Upgrade || info.Request == nil || !info.Source.IsValid() {
				e.badInfo.Add(1)
				return zmux.Decision{Verdict: zmux.VerdictClose}
			}
			if info.Stage == zmux.StageHead {
				e.heads.Add(1)
				return zmux.Decision{Verdict: zmux.VerdictDefer}
			}
			if info.Framing != zmux.FramingRecords {
				e.badInfo.Add(1)
				return zmux.Decision{Verdict: zmux.VerdictClose}
			}
			e.firsts.Add(1)
			e.firstMsg <- bytes.Clone(prefix)
			if bytes.HasPrefix(prefix, takeMark) {
				return zmux.Decision{Verdict: zmux.VerdictFallback, Variant: "take"}
			}
			key, _ := info.Source.Addr().Prefix(24)
			e.mu.Lock()
			defer e.mu.Unlock()
			if e.perSrc[key] >= limit {
				return zmux.Decision{Verdict: zmux.VerdictClose, Variant: "over"}
			}
			e.perSrc[key]++
			return zmux.Decision{Verdict: zmux.VerdictAccept, Variant: "zmux"}
		}
	}
	if fallback == nil {
		// The takeover relays three records (as a node forwards them to
		// its master), answers, and returns: zmux then closes the carrier.
		fallback = func(_ context.Context, tk *zmux.Takeover) {
			p := tk.Pipe()
			_, conn := tk.Conn()
			got := taken{info: tk.Info, conn: conn}
			for range 3 {
				m, err := p.R.(zmux.MessageReader).ReadMessage(zmux.MaxRecordSize)
				if err != nil {
					break
				}
				got.msgs = append(got.msgs, m.TakeBytes())
			}
			_ = p.W.(zmux.MessageWriter).WriteMessage([]byte("reply"))
			e.taken <- got
		}
	}
	e.l = mustNewListener(&zmux.ListenerConfig{Config: cfg, Admission: adm, Fallback: fallback, MaxPending: maxPending})
	h, err := NewHandler(e.l, &zmuxhttp.Options{Next: next})
	if err != nil {
		t.Fatal(err)
	}
	e.u = wsTestServer(t, e.l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			select {
			case e.finished <- struct{}{}:
			default:
			}
		}()
		h.ServeHTTP(w, r)
	}))
	return e
}

// readEnd reads p until it ends; ok: it ended within d.
func readEnd(p zmux.PipeIO, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := p.R.(zmux.MessageReader).ReadMessage(1 << 16); err != nil {
				return
			}
		}
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		zmux.ClosePipeIO(p)
		<-done
		return false
	}
}

// An upgrade deferred to its first message: a zmux first record makes a
// working session, counted per source; a host message goes to the fallback
// with that exact message replayed first and the rest of the stream after
// it, and the fallback's replies reach the peer.
func TestAdmissionDeferredUpgrade(t *testing.T) {
	ctx := testContext(t)
	// One pending slot in total: each carrier must give it back for the next.
	e := newDeferEnv(t, 1, 1, nil, nil)

	// Back to zmux.
	c, err := Dial(ctx, e.u+"/z", nil, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, ctx, c, s, 64<<10)
	closeBoth(c, s)
	if first := <-e.firstMsg; len(first) == 0 || bytes.HasPrefix(first, takeMark) {
		t.Fatalf("first record %x", first)
	}

	// Taken over by the host.
	cli, err := DialPipe(ctx, e.u+"/z", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer zmux.ClosePipeIO(cli)
	msgs := [][]byte{append(bytes.Clone(takeMark), " handle 7"...), []byte("second"), bytes.Repeat([]byte{9}, 8<<10)}
	for _, m := range msgs {
		if err := cli.W.(zmux.MessageWriter).WriteMessage(m); err != nil {
			t.Fatal(err)
		}
	}
	var tk taken
	select {
	case tk = <-e.taken:
	case <-ctx.Done():
		t.Fatal("no takeover")
	}
	if tk.conn || tk.info.Framing != zmux.FramingRecords || !tk.info.Upgrade || tk.info.Stage != zmux.StageCarrier ||
		!tk.info.Source.Addr().IsLoopback() || len(tk.msgs) != len(msgs) {
		t.Fatalf("takeover %+v", tk)
	}
	for i, want := range msgs {
		if !bytes.Equal(tk.msgs[i], want) {
			t.Fatalf("message %d: %d bytes", i, len(tk.msgs[i]))
		}
	}
	got, err := cli.R.(zmux.MessageReader).ReadMessage(64)
	defer got.Release()
	if err != nil || string(got.Bytes()) != "reply" {
		t.Fatalf("reply %q, %v", got.Bytes(), err)
	}
	// zmux closed the carrier when the takeover returned.
	if !readEnd(cli, 5*time.Second) {
		t.Fatal("the taken carrier did not end")
	}

	// The source's count closes a further zmux carrier.
	p, err := DialPipe(ctx, e.u+"/z", nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = zmux.Start(ctx, p, zmux.RoleClient, mpConfig()) }()
	waitStats(t, ctx, e, func(st zmux.AdmissionStats) bool { return st.Closed == 1 })

	st := e.l.Stats().Admission
	if st.Accepted != 1 || st.Fallback != 1 || st.Closed != 1 || st.PeekTimedOut != 0 || e.heads.Load() != 3 ||
		e.firsts.Load() != 3 || e.badInfo.Load() != 0 {
		t.Fatalf("stats %+v, heads %d, firsts %d, bad info %d", st, e.heads.Load(), e.firsts.Load(), e.badInfo.Load())
	}
	if r := e.l.Stats().Refused; r != 0 {
		t.Fatalf("%d carriers refused: a slot was not given back", r)
	}
}

// waitStats observes the stats once a request finishes.
func waitStats(t *testing.T, ctx context.Context, e *deferEnv, ok func(zmux.AdmissionStats) bool) {
	t.Helper()
	for !ok(e.l.Stats().Admission) {
		select {
		case <-e.finished:
		case <-ctx.Done():
			t.Fatalf("stats %+v: %v", e.l.Stats().Admission, ctx.Err())
		}
	}
}

// The edges of a deferred decision: no first message within PeekTimeout,
// a first message above MaxMessage, a Decide or a fallback that panics, a
// stall, and the listener's Close while the first message is awaited.
// Each ends the carrier, counted once, and gives its slot back.
func TestAdmissionDeferredUpgradeEdges(t *testing.T) {
	ctx := testContext(t)
	silent := func(t *testing.T, e *deferEnv) zmux.PipeIO {
		t.Helper()
		p, err := DialPipe(ctx, e.u+"/z", nil)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	send := func(t *testing.T, e *deferEnv, m []byte) zmux.PipeIO {
		t.Helper()
		p := silent(t, e)
		if err := p.W.(zmux.MessageWriter).WriteMessage(m); err != nil {
			t.Fatal(err)
		}
		return p
	}
	check := func(t *testing.T, e *deferEnv, p zmux.PipeIO, within time.Duration, want zmux.AdmissionStats) {
		t.Helper()
		if !readEnd(p, within) {
			t.Fatal("the carrier did not end")
		}
		waitStats(t, ctx, e, func(st zmux.AdmissionStats) bool { return st == want })
	}
	t.Run("timeout", func(t *testing.T) {
		const peek = time.Second
		cfg := mpConfig()
		clk := timeshim.NewFake(time.Now())
		cfg.ClockDomain = zmux.NewClockDomain(clk)
		e := newDeferEnvOn(t, cfg, 9, 0, &zmux.Admission{PeekTimeout: peek}, nil)
		p := silent(t, e)
		if err := clk.WaitDeadline(ctx, clk.Now().Add(peek)); err != nil {
			t.Fatal(err)
		}
		clk.Advance(peek)
		check(t, e, p, 5*time.Second, zmux.AdmissionStats{PeekTimedOut: 1})
	})
	t.Run("too large", func(t *testing.T) {
		e := newDeferEnv(t, 9, 0, &zmux.Admission{MaxMessage: 16}, nil)
		check(t, e, send(t, e, make([]byte, 17)), 5*time.Second, zmux.AdmissionStats{TooLarge: 1})
	})
	t.Run("decide panics", func(t *testing.T) {
		var calls atomic.Int32
		e := newDeferEnv(t, 9, 0, &zmux.Admission{Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
			calls.Add(1)
			if info.Stage == zmux.StageHead {
				return zmux.Decision{Verdict: zmux.VerdictDefer}
			}
			panic("decide")
		}}, nil)
		check(t, e, send(t, e, []byte("x")), 5*time.Second, zmux.AdmissionStats{Closed: 1})
		if calls.Load() != 2 {
			t.Fatalf("%d decisions", calls.Load())
		}
	})
	t.Run("fallback panics", func(t *testing.T) {
		e := newDeferEnv(t, 9, 0, nil, func(context.Context, *zmux.Takeover) { panic("fallback") })
		check(t, e, send(t, e, takeMark), 5*time.Second, zmux.AdmissionStats{Fallback: 1})
	})
	t.Run("stall", func(t *testing.T) {
		const hold = time.Second
		cfg := mpConfig()
		clk := timeshim.NewFake(time.Now())
		cfg.ClockDomain = zmux.NewClockDomain(clk)
		e := newDeferEnvOn(t, cfg, 9, 0, &zmux.Admission{StallMin: hold, StallMax: hold,
			Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
				if info.Stage == zmux.StageHead {
					return zmux.Decision{Verdict: zmux.VerdictDefer}
				}
				return zmux.Decision{Verdict: zmux.VerdictStall}
			}}, nil)
		p := send(t, e, []byte("x"))
		if err := clk.WaitDeadline(ctx, clk.Now().Add(hold)); err != nil {
			t.Fatal(err)
		}
		clk.Advance(hold)
		check(t, e, p, 5*time.Second, zmux.AdmissionStats{Stalled: 1})
	})
	t.Run("text first message", func(t *testing.T) {
		e := newDeferEnv(t, 9, 0, nil, nil)
		p := silent(t, e)
		if err := p.W.(*carrier).c.WriteMessage(wsconn.Text, takeMark); err != nil {
			t.Fatal(err)
		}
		check(t, e, p, 5*time.Second, zmux.AdmissionStats{Closed: 1})
		if e.firsts.Load() != 0 || len(e.taken) != 0 {
			t.Fatalf("decided %d times, %d taken over", e.firsts.Load(), len(e.taken))
		}
	})
	t.Run("peer ends first", func(t *testing.T) {
		e := newDeferEnv(t, 9, 0, nil, nil)
		p := silent(t, e)
		_ = p.W.(*carrier).CloseWrite()
		check(t, e, p, 5*time.Second, zmux.AdmissionStats{Closed: 1})
		if e.firsts.Load() != 0 {
			t.Fatalf("decided %d times", e.firsts.Load())
		}
	})
	t.Run("listener close", func(t *testing.T) {
		e := newDeferEnv(t, 9, 0, &zmux.Admission{PeekTimeout: time.Minute}, nil)
		p := silent(t, e)
		_ = e.l.Close()
		if !readEnd(p, 5*time.Second) {
			t.Fatal("Close left the carrier waiting for its first message")
		}
		waitStats(t, ctx, e, func(st zmux.AdmissionStats) bool { return st == zmux.AdmissionStats{Closed: 1} })
	})
}

// HS-19.
func TestHeadTakeoverUpgradesBeforeHandoff(t *testing.T) {
	ctx := testContext(t)
	e := newDeferEnv(t, 0, 0, &zmux.Admission{Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
		if info.Stage != zmux.StageHead || !info.Upgrade {
			t.Error("upgrade decision repeated")
		}
		return zmux.Decision{Verdict: zmux.VerdictTakeover}
	}}, func(_ context.Context, tk *zmux.Takeover) {
		m, err := tk.Pipe().R.(zmux.MessageReader).ReadMessage(64)
		if err != nil {
			t.Error(err)
			return
		}
		defer m.Release()
		if err = tk.Pipe().W.(zmux.MessageWriter).WriteMessage(m.Bytes()); err != nil {
			t.Error(err)
		}
	})
	p, err := DialPipe(ctx, e.u+"/z", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer zmux.ClosePipeIO(p)
	if err = p.W.(zmux.MessageWriter).WriteMessage(takeMark); err != nil {
		t.Fatal(err)
	}
	reply, err := p.R.(zmux.MessageReader).ReadMessage(64)
	defer reply.Release()
	if err != nil || !bytes.Equal(reply.Bytes(), takeMark) {
		t.Fatalf("reply %q %v", reply.Bytes(), err)
	}
	if e.l.Stats().Admission.Takeover != 1 {
		t.Fatal(e.l.Stats())
	}
}
