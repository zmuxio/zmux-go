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
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

func checkPendingEmpty(t *testing.T, l *Listener) {
	t.Helper()
	l.pmu.Lock()
	n := l.pending.Total()
	l.pmu.Unlock()
	if n != 0 {
		t.Fatalf("setup still holds %d pending slots", n)
	}
}

// A live runner and a normal handler share the paired join's setup. The
// unused direction may block in CloseWrite after the handler gives back its slot.
func TestSplitJoinUnusedHalfHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		const token = "AAAAAAAAAAAAAAAAAAAAAA"
		for _, upFirst := range []bool{true, false} {
			up, read := net.Pipe()
			write, down := net.Pipe()
			gate := &gatedWrite{Conn: write, gate: make(chan struct{}), blocked: make(chan struct{}), nth: -1}
			unblock := sync.OnceFunc(func() { close(gate.gate) })
			t.Cleanup(unblock)
			setup, lifetime := make(chan error, 1), make(chan error, 1)
			arrival, err := e.lst.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
			if err != nil {
				t.Fatal(err)
			}
			run, err := arrival.Take(ctx, PipeIO{W: gate}, PipeHint{Pair: token})
			if err != nil {
				t.Fatal(err)
			}
			uplink := func() { setup <- e.lst.HandlePipe(ctx, PipeIO{R: read}, PipeHint{Pair: token, Uplink: true}) }
			downlink := func() { lifetime <- run(ctx) }
			first, second := uplink, downlink
			if !upFirst {
				first, second = second, first
			}
			go first()
			synctest.Wait()
			go second()
			p, err := e.client.AddPipe(ctx, PipeIO{R: down, W: up}, PipeOptions{Direction: DirectionSendOnly})
			if err != nil {
				t.Fatal(err)
			}
			<-gate.blocked
			synctest.Wait()
			select {
			case err := <-setup:
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("unused CloseWrite blocked the handoff")
			}
			checkPendingEmpty(t, e.lst)
			peer, ok := e.server.Pipe(p.ID())
			if !ok {
				t.Fatal("missing joined pipe")
			}
			if err := peer.Abort(); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			select {
			case err := <-lifetime:
				t.Logf("runner result: %v", err)
				t.Fatal("Take returned before the unused half finished")
			default:
			}
			duplicate := newCountingCarrier()
			if err := e.lst.HandlePipe(ctx, PipeIO{R: duplicate}, PipeHint{Pair: token, Uplink: true}); !errors.Is(err, errRejected) {
				t.Fatalf("live token: %v", err)
			}
			duplicate.checkOnce(t)
			unblock()
			if err := <-lifetime; err != nil {
				t.Fatal(err)
			}
			arrival.Release()
			_ = down.Close()
			// The next iteration reuses the token, with the other arrival order.
		}
	})
}

type countedInput struct {
	io.Reader
	closes int
}

func (r *countedInput) Close() error { r.closes++; return nil }

func TestListenerJoinNeedsReplyHalf(t *testing.T) {
	l := mustNewListener(&ListenerConfig{OnReject: func(RejectInfo) RejectAction {
		t.Error("one-way carrier reached join verification")
		return RejectClose
	}})
	defer func() { _ = l.Close() }()
	// A classified input without a reply direction never enters the handshake parser.
	r := &countedInput{Reader: bytes.NewReader([]byte{join.ProfileJoin, 0xff})}
	a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.HandlePipe(t.Context(), PipeIO{R: r}, PipeHint{}); !errors.Is(err, errRejected) {
		t.Fatalf("one-way join: %v", err)
	}
	if r.closes != 1 || l.Stats().Rejected != 1 {
		t.Fatalf("closes %d, rejected %d", r.closes, l.Stats().Rejected)
	}
	checkPendingEmpty(t, l)
}

type notifiedLane struct {
	net.Conn
	session *Conn
	key     any
	taken   chan struct{}
}

func (r *notifiedLane) CarrierConn() any { _ = r.session.ConnectionStats(); return r.key }
func (r *notifiedLane) CarrierTaken()    { _ = r.session.ConnectionStats(); close(r.taken) }

type blockedCloseConn struct {
	net.Conn
	entered, gate chan struct{}
}

func (w *blockedCloseConn) Close() error { close(w.entered); <-w.gate; return w.Conn.Close() }

// Both identity sampling at join commit and the earlier nesting check may
// reenter the session. A trusted join preserves the carrier's identity.
func TestJoinCarrierIdentityOutsideLock(t *testing.T) {
	cfg := mpConfig()
	cfg.AllowRecordMode = func(RecordPolicyInput, RecordMode, any) bool { return true }
	e := newMPEnv(t, cfg, cfg)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	a, b := net.Pipe()
	r := &notifiedLane{Conn: b, key: new(int), session: e.server, taken: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- e.lst.HandlePipe(ctx, PipeIO{R: r, W: r}, PipeHint{}) }()
	if _, err := e.client.AddPipe(ctx, NetPipe(a), PipeOptions{RecordMode: Set(RecordTrusted)}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// HS-04, HS-31.
func TestListenerLaneUnusedHalfHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cc, sc := mpConfig(), mpConfig()
		cc.MaxLanes, sc.MaxLanes = 4, 4
		e := newMemMPEnv(t, cc, sc)
		e.lst.Drain("new sessions only")
		key := new(int)
		var laneKey [16]byte
		e.server.locked(func() { e.server.s.SetCarrierConn(0, key); laneKey, _ = e.server.s.LaneKey() })
		a, b := net.Pipe()
		d, w := net.Pipe()
		defer func() { _ = d.Close() }()
		writer := &blockedCloseConn{Conn: w, entered: make(chan struct{}), gate: make(chan struct{})}
		unblock := sync.OnceFunc(func() { close(writer.gate) })
		defer unblock()
		r := &notifiedLane{Conn: b, session: e.server, key: key, taken: make(chan struct{})}
		result := make(chan error, 1)
		go func() { result <- e.lst.HandlePipe(t.Context(), PipeIO{R: r, W: writer}, PipeHint{}) }()
		body := wire.AppendPipeLane(nil, laneKey[:], 0, 1)
		if _, err := a.Write(append(varint.AppendVarint(nil, uint64(len(body))), body...)); err != nil {
			t.Fatal(err)
		}
		<-r.taken
		<-writer.entered
		synctest.Wait()
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("lane handoff waited for its unused writer")
		}
		checkPendingEmpty(t, e.lst)
		_ = a.Close()
		e.server.Abort(0, "")
		synctest.Wait()
		select {
		case <-e.server.Done():
			t.Fatal("session finished while lane cleanup was blocked")
		default:
		}
		unblock()
		<-e.server.Done()
	})
}

// HS-43.
func TestSplitSessionConfigCancellation(t *testing.T) {
	for _, reason := range []string{"requests", "listener"} {
		t.Run(reason, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := mpConfig()
				cfg.ClockDomain = NewClockDomain(timeshim.Real)
				entered := make(chan struct{})
				l := mustNewListener(&ListenerConfig{Config: cfg, SessionConfig: func(ctx context.Context, _ *CarrierInfo, _ any) (*Config, error) {
					close(entered)
					<-ctx.Done()
					return nil, ctx.Err()
				}})
				defer func() { _ = l.Close() }()
				first, stopFirst := context.WithCancel(t.Context())
				defer stopFirst()
				second, stopSecond := context.WithCancel(t.Context())
				defer stopSecond()
				a, b := net.Pipe()
				c, d := net.Pipe()
				const token = "AAAAAAAAAAAAAAAAAAAAAA"
				result := make(chan error, 2)
				go func() { result <- l.HandlePipe(first, PipeIO{R: b}, PipeHint{Pair: token, Uplink: true}) }()
				go func() { result <- l.HandlePipe(second, PipeIO{W: c}, PipeHint{Pair: token}) }()
				client := make(chan error, 1)
				go func() { _, err := Start(t.Context(), PipeIO{R: d, W: a}, RoleClient, cfg); client <- err }()
				<-entered
				stopFirst()
				synctest.Wait()
				select {
				case err := <-result:
					t.Logf("setup result: %v", err)
					t.Fatal("one request cancelled combined setup")
				default:
				}
				if reason == "requests" {
					stopSecond()
				} else {
					_ = l.Close()
				}
				for range 2 {
					if err := <-result; !errors.Is(err, context.Canceled) {
						t.Fatalf("setup: %v", err)
					}
				}
				if err := <-client; err == nil {
					t.Fatal("unexpected successful bootstrap")
				}
				checkPendingEmpty(t, l)
			})
		})
	}
}

// controlledOpener lets the test retire an attachment with a second open
// in flight. Even an opener returning a writer after cancellation owns a close.
type controlledOpener struct {
	entered chan struct{}
	opened  io.WriteCloser
	late    io.WriteCloser
}

func (o *controlledOpener) OpenLane(ctx context.Context) (io.WriteCloser, error) {
	if o.opened != nil {
		w := o.opened
		o.opened = nil
		return w, nil
	}
	close(o.entered)
	<-ctx.Done()
	return o.late, nil
}

type cleanupWriter struct {
	session       *Conn
	closing, gate chan struct{}
}

func (w *cleanupWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *cleanupWriter) Close() error {
	_ = w.session.ConnectionStats()
	close(w.closing)
	<-w.gate
	return nil
}

// HS-52.
func TestTakeWaitsForLaneCloseAndPendingOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		a, b := net.Pipe()
		arrival, err := e.lst.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
		if err != nil {
			t.Fatal(err)
		}
		defer arrival.Release()
		run, err := arrival.Take(ctx, NetPipe(b), PipeHint{})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- run(ctx) }()
		p, err := e.client.AddPipe(ctx, NetPipe(a), PipeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		peer, ok := e.server.Pipe(p.ID())
		if !ok {
			t.Fatal("missing pipe")
		}
		active := &cleanupWriter{e.server, make(chan struct{}), make(chan struct{})}
		late := &cleanupWriter{e.server, make(chan struct{}), make(chan struct{})}
		openActive := sync.OnceFunc(func() { close(active.gate) })
		defer openActive()
		openLate := sync.OnceFunc(func() { close(late.gate) })
		defer openLate()
		opener := &controlledOpener{make(chan struct{}), active, late}
		// Exercise the driver's open command directly so the scheduler need not
		// first fill a stream window to request a lane.
		command := func() {
			var action pendingAct
			e.server.locked(func() {
				pd := e.server.pipes[uint64(p.ID())]
				pd.opener = opener
				action = e.server.resolveActionLocked(core.Action{Kind: core.ActOpenLane, Pipe: pd.id})
			})
			e.server.doActionContained(action)
		}
		command()
		synctest.Wait()
		command()
		<-opener.entered
		aborted := make(chan error, 1)
		go func() { aborted <- peer.Abort() }()
		<-active.closing
		<-late.closing
		select {
		case err := <-done:
			t.Logf("runner result: %v", err)
			t.Fatal("runner returned before lane closes")
		default:
		}
		openLate()
		select {
		case err := <-done:
			t.Logf("runner result: %v", err)
			t.Fatal("runner returned while the attached writer was closing")
		default:
		}
		openActive()
		if err := <-aborted; err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if _, err := e.client.Ping(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

type panicLimitWriter struct{ *countingCarrier }

func (*panicLimitWriter) MaxRecord() int { panic("limit") }
func (w *panicLimitWriter) Close() error { _ = w.countingCarrier.Close(); panic("close") }

// HS-07.
func TestAttachmentCapabilityRollback(t *testing.T) {
	l := mustNewListener(nil)
	defer func() { _ = l.Close() }()
	r := newCountingCarrier()
	w := &panicLimitWriter{newCountingCarrier()}
	if err := l.HandlePipe(t.Context(), PipeIO{R: r, W: w}, PipeHint{}); err == nil {
		t.Fatal("accepted panicking carrier")
	}
	r.checkOnce(t)
	w.checkOnce(t)
	checkPendingEmpty(t, l)
}

func TestAttachmentReadOnlyBootstrap(t *testing.T) {
	l := mustNewListener(&ListenerConfig{SessionConfig: func(context.Context, *CarrierInfo, any) (*Config, error) {
		t.Error("read-only bootstrap reached session setup")
		return nil, errRejected
	}})
	defer func() { _ = l.Close() }()
	r := &countedInput{Reader: bytes.NewReader([]byte{9, 0xff, 0x51, 0x53, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0})}
	if err := l.HandlePipe(t.Context(), PipeIO{R: r}, PipeHint{}); !errors.Is(err, errRejected) {
		t.Fatalf("bootstrap: %v", err)
	}
	if r.closes != 1 || len(l.accept) != 0 {
		t.Fatal("read-only carrier escaped setup")
	}
	checkPendingEmpty(t, l)
}

func TestAttachmentCancelledFallback(t *testing.T) {
	l := mustNewListener(&ListenerConfig{Admission: &Admission{Decide: func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictFallback} }},
		Fallback: func(context.Context, *Takeover) { t.Error("called cancelled fallback") }})
	defer func() { _ = l.Close() }()
	a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
	if err != nil {
		t.Fatal(err)
	}
	a.first.handoff = &admissionHandoff{info: &CarrierInfo{}, decision: Decision{Verdict: VerdictTakeover}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	p := newCountingCarrier()
	if err = a.HandlePipe(ctx, p.pipe(), PipeHint{}); err == nil {
		t.Fatal("accepted cancelled takeover")
	}
	p.checkOnce(t)
	checkPendingEmpty(t, l)
}

type gatedAbort struct {
	entered, abortStarted, abortReturn chan struct{}
}

func (w *gatedAbort) Write(p []byte) (int, error) { return len(p), nil }
func (w *gatedAbort) Close() error                { close(w.entered); <-w.abortStarted; return nil }
func (w *gatedAbort) Abort(error)                 { close(w.abortStarted); <-w.abortReturn }

// HS-51.
func TestAttachmentWaitsForGraceAbort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := &gatedAbort{make(chan struct{}), make(chan struct{}), make(chan struct{})}
		c := &Conn{cfg: &Config{}}
		pd := &pipeDrv{c: c, hc: newHalfCloser(PipeIO{W: w})}
		owner := newAttachment(nil, true)
		owner.attach(pd)
		owner.release()
		closed := make(chan struct{})
		go func() { pd.closeGraceful(time.Second); close(closed) }()
		<-w.entered
		<-closed // Close only returned because the deadline fired.
		select {
		case <-owner.done:
			t.Fatal("completion before Abort returned")
		default:
		}
		close(w.abortReturn)
		<-owner.done
	})
}

type panicPeer struct{ *countingCarrier }

func (*panicPeer) CarrierPeer() any { panic("peer") }

// HS-07.
func TestSplitAttachmentPanicReleasesPair(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNewListener(nil)
		defer func() { _ = l.Close() }()
		const token = "AAAAAAAAAAAAAAAAAAAAAA"
		for range 2 {
			r := &countedInput{Reader: bytes.NewReader([]byte{9, 0xff, 0x51, 0x53, 0x30, 0x0d, 0x0a, 0x0d, 0x0a, 0})}
			w := &panicPeer{newCountingCarrier()}
			ended := make(chan error, 1)
			go func() { ended <- l.HandlePipe(t.Context(), PipeIO{W: w}, PipeHint{Pair: token}) }()
			synctest.Wait()
			err := l.HandlePipe(t.Context(), PipeIO{R: r}, PipeHint{Pair: token, Uplink: true})
			if err == nil || !errors.Is(<-ended, err) {
				t.Fatalf("split outcome %v", err)
			}
			w.checkOnce(t)
			if r.closes != 1 {
				t.Fatalf("read closes %d", r.closes)
			}
			checkPendingEmpty(t, l)
		}
	})
}

type gatedIdentity struct {
	net.Conn
	entered, gate chan struct{}
	key           any
}

func (r *gatedIdentity) CarrierConn() any { close(r.entered); <-r.gate; return r.key }

// HS-04.
func TestListenerLaneCancellationBeforeCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		key := new(int)
		var laneKey [16]byte
		e.server.locked(func() { e.server.s.SetCarrierConn(0, key); laneKey, _ = e.server.s.LaneKey() })
		a, b := net.Pipe()
		defer func() { _ = a.Close() }()
		r := &gatedIdentity{b, make(chan struct{}), make(chan struct{}), key}
		result := make(chan error, 1)
		go func() { result <- e.lst.HandlePipe(t.Context(), PipeIO{R: r}, PipeHint{}) }()
		body := wire.AppendPipeLane(nil, laneKey[:], 0, 1)
		if _, err := a.Write(append(varint.AppendVarint(nil, uint64(len(body))), body...)); err != nil {
			t.Fatal(err)
		}
		<-r.entered
		_ = e.lst.Close()
		close(r.gate)
		if err := <-result; err == nil {
			t.Fatal("lane committed after listener close")
		}
		checkPendingEmpty(t, e.lst)
	})
}

type requestHalf struct {
	net.Conn
	tail carrierio.RequestTail
}

func (h *requestHalf) RequestTail() *carrierio.RequestTail { return &h.tail }

// HS-40, HS-44.
func TestSplitTokenJoinsRequestTails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, upFirst := range []bool{false, true} {
			for _, finishUp := range []bool{false, true} {
				for _, success := range []bool{false, true} {
					l := mustNewListener(nil)
					t.Cleanup(func() { _ = l.Close() })
					r, up := net.Pipe()
					w, down := net.Pipe()
					u, d := &requestHalf{Conn: r}, &requestHalf{Conn: w}
					const token = "AAAAAAAAAAAAAAAAAAAAAA"
					done := make(chan error, 2)
					offerUp := func() { done <- l.HandlePipe(t.Context(), PipeIO{R: u}, PipeHint{Pair: token, Uplink: true}) }
					arrival, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
					if err != nil {
						t.Fatal(err)
					}
					run, err := arrival.Take(t.Context(), PipeIO{W: d}, PipeHint{Pair: token})
					if err != nil {
						t.Fatal(err)
					}
					physical := make(chan error, 1)
					offerDown := func() { physical <- run(t.Context()) }
					if upFirst {
						go offerUp()
						synctest.Wait()
						go offerDown()
					} else {
						go offerDown()
						synctest.Wait()
						go offerUp()
					}
					if success {
						client, err := Start(t.Context(), PipeIO{R: down, W: up}, RoleClient, nil)
						if err != nil {
							t.Fatal(err)
						}
						server, err := l.Accept(t.Context())
						if err != nil {
							t.Fatal(err)
						}
						client.Abort(0, "")
						server.Abort(0, "")
						<-client.Done()
						<-server.Done()
					} else {
						_ = up.Close()
						_ = down.Close()
					}
					setupErr := <-done
					if (setupErr == nil) != success {
						t.Fatalf("setup %v", setupErr)
					}
					synctest.Wait()
					canReuse := func() bool {
						l.mu.Lock()
						defer l.mu.Unlock()
						var key [16]byte
						result, _ := l.pairing.Offer(key, true, u)
						if result == core.PairWait {
							l.pairing.Withdraw(key, u)
						}
						return result == core.PairWait
					}
					first, last := &u.tail, &d.tail
					if !finishUp {
						first, last = last, first
					}
					if canReuse() {
						t.Fatal("token released before request cleanup")
					}
					first.Finish()
					if canReuse() {
						t.Fatal("token released with a live request")
					}
					last.Finish()
					if err := <-physical; !errors.Is(err, setupErr) {
						t.Fatal(err)
					}
					if !canReuse() {
						t.Fatal("last tail retained token")
					}
					_ = l.Close()
				}
			}
		}
	})
}

// HS-07, HS-10, HS-11, HS-12.
func TestAttachmentFallbackCompletion(t *testing.T) {
	for _, mode := range []string{"return", "panic", "cancel", "detach"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			cause := errors.New("fallback gate")
			var kept *Takeover
			l := takeoverEnv(t, 0, func(ctx context.Context, tk *Takeover) {
				close(entered)
				<-release
				switch mode {
				case "detach":
					if err := tk.Detach(); err != nil {
						t.Error(err)
					}
					kept = tk
				case "cancel":
					<-ctx.Done()
				case "panic":
					panic(cause)
				}
			})
			r, w := newCountingCarrier(), newCountingCarrier()
			result := make(chan error, 1)
			go func() { result <- l.HandlePipe(ctx, PipeIO{R: r, W: w}, PipeHint{}) }()
			<-entered
			checkPendingEmpty(t, l)
			if mode == "cancel" {
				cancel()
			}
			close(release)
			if err := <-result; (err != nil) != (mode == "panic") || mode == "panic" && !errors.Is(err, cause) {
				t.Fatalf("result %v", err)
			}
			if kept != nil {
				if r.closes.Load() != 0 || w.closes.Load() != 0 {
					t.Fatal("detached halves closed")
				}
				ClosePipeIO(kept.Pipe())
			}
			r.checkOnce(t)
			w.checkOnce(t)
		})
	}
}

// HS-55.
func TestAttachmentHoldsSlowDecision(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate, entered := make(chan struct{}), make(chan struct{})
		l := mustNewListener(&ListenerConfig{MaxPending: 1, Admission: &Admission{PeekTimeout: time.Second, Decide: func(context.Context, CarrierInfo, []byte) Decision {
			close(entered)
			<-gate
			return Decision{Verdict: VerdictAccept}
		}}})
		defer func() { _ = l.Close() }()
		done := make(chan error, 1)
		cc := newCountingCarrier()
		go func() { done <- l.HandlePipe(t.Context(), cc.pipe(), PipeHint{}) }()
		<-entered
		if err := <-done; err == nil {
			t.Fatal("timed out setup succeeded")
		}
		if a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{}); err == nil {
			a.Release()
			t.Fatal("running callback lost its reservation")
		}
		close(gate)
		synctest.Wait()
		checkPendingEmpty(t, l)
		cc.checkOnce(t)
	})
}

// HS-51.
func TestLaneAbortInterruptsClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := &gatedAbort{make(chan struct{}), make(chan struct{}), make(chan struct{})}
		lo := &laneOut{w: w}
		pd := &pipeDrv{c: &Conn{cfg: &Config{}}, hc: newHalfCloser(PipeIO{}), lanes: map[uint64]*laneOut{1: lo}}
		owner := newAttachment(nil, true)
		owner.attach(pd)
		owner.release()
		pd.gs.add()
		go func() { defer pd.gs.done(); lo.close(false) }()
		<-w.entered
		go pd.closeAll()
		<-w.abortStarted
		synctest.Wait()
		select {
		case <-owner.done:
			t.Fatal("published while abort was active")
		default:
		}
		close(w.abortReturn)
		<-owner.done
		lo.close(true) // both operations are idempotent
	})
}

// HS-58.
func TestLaneUsesTargetListenerCutoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := mpConfig()
		cfg.MaxLanes = 4
		e := newMemMPEnv(t, cfg, cfg)
		other := mustNewListener(nil)
		defer func() { _ = other.Close() }()
		key := new(int)
		var laneKey [16]byte
		e.server.locked(func() { e.server.s.SetCarrierConn(0, key); laneKey, _ = e.server.s.LaneKey() })
		_ = e.lst.Close()
		for _, entry := range []*Listener{nil, other} {
			a, b := net.Pipe()
			r := &notifiedLane{Conn: b, session: e.server, key: key, taken: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				if entry != nil {
					done <- entry.HandlePipe(t.Context(), PipeIO{R: r}, PipeHint{})
				} else {
					done <- HandleLane(t.Context(), PipeIO{R: r})
				}
			}()
			body := wire.AppendPipeLane(nil, laneKey[:], 0, 1)
			if _, err := a.Write(append(varint.AppendVarint(nil, uint64(len(body))), body...)); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err == nil {
				t.Fatal("accepted lane after target listener stopped")
			}
			_ = a.Close()
		}
		echoOnce(t, t.Context(), e.client, e.server)
	})
}

// HS-02. The peer receives the session owner’s CONNECTION_CLOSE after the setup context ends.
func TestBootstrapSetupThroughPhysicalClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := &Config{ClockDomain: NewClockDomain(timeshim.Real)}
		l := mustNewListener(&ListenerConfig{Config: cfg})
		defer func() { _ = l.Close() }()
		ctx, stop := context.WithCancel(t.Context())
		defer stop()
		arrival, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
		if err != nil {
			t.Fatal(err)
		}
		owner := arrival.first
		owner.done = make(chan struct{})
		a, b := net.Pipe()
		half := &requestHalf{Conn: b}
		setup := make(chan error, 1)
		go func() { setup <- arrival.HandlePipe(ctx, PipeIO{R: half, W: half}, PipeHint{}) }()
		client, err := Client(ctx, a, cfg)
		if err != nil {
			t.Fatal(err)
		}
		server, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := <-setup; err != nil {
			t.Fatal(err)
		}
		checkPendingEmpty(t, l)
		stop()
		echoOnce(t, t.Context(), client, server)
		if err := server.CloseWithError(17, "finished"); err != nil {
			t.Fatal(err)
		}
		var ae *ApplicationError
		if err := client.Wait(); !errors.As(err, &ae) || !ae.Remote || ae.ErrorCode != 17 || ae.ErrorMessage != "finished" {
			t.Fatalf("peer CONNECTION_CLOSE: got %v, want remote application code 17 and reason %q", err, "finished")
		}
		select {
		case <-owner.done:
			t.Fatal("published before request cleanup")
		default:
		}
		half.tail.Finish()
		<-owner.done
	})
}

// HS-55.
func TestHTTPRequestDecisionAfterSetupTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString("body"))
		req.Header.Set("X-Route", "first")
		gate := make(chan struct{})
		entered := make(chan struct{})
		got := make(chan string, 1)
		l := mustNewListener(&ListenerConfig{MaxPending: 1, Admission: &Admission{Peek: 1, PeekTimeout: time.Second, Decide: func(_ context.Context, info CarrierInfo, _ []byte) Decision {
			close(entered)
			<-gate
			if info.Request.Body != http.NoBody {
				t.Error("decision retained request body")
			}
			got <- info.Request.Header.Get("X-Route")
			return Decision{Verdict: VerdictAccept}
		}}})
		defer func() { _ = l.Close() }()
		done := make(chan any, 1)
		go func() {
			defer func() { done <- recover() }()
			l.Adapter().AdmitRequest(httptest.NewRecorder(), req, nil, nil)
		}()
		<-entered
		v := <-done
		err, _ := v.(error)
		if !errors.Is(err, http.ErrAbortHandler) {
			t.Fatalf("timeout %v", v)
		}
		req.Header.Set("X-Route", "changed")
		if a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{}); err == nil {
			a.Release()
			t.Fatal("pending quota escaped slow decision")
		}
		close(gate)
		if route := <-got; route != "first" {
			t.Fatal(route)
		}
		synctest.Wait()
		checkPendingEmpty(t, l)
	})
}

type takeoverAdmissionCarrier struct {
	*countingCarrier
	beforeCommit func()
}

func (c *takeoverAdmissionCarrier) CarrierTaken() { c.beforeCommit() }

// HS-19. Preparation, transfer to the host and callback return are distinct.
// CarrierTaken is the last host capability checked before the commit.
func TestTakeoverStatsAtHostHandoff(t *testing.T) {
	for _, carrier := range []string{"stream", "request", "stream-fallback", "deferred-takeover", "deferred-fallback"} {
		for _, outcome := range []string{"delivered", "cancelled", "callback panic"} {
			t.Run(carrier+"/"+outcome, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var l *Listener
				check := func(want AdmissionStats, variants ...VariantStat) {
					t.Helper()
					if got := l.Stats().Admission; got != want {
						t.Errorf("admission stats %+v, want %+v", got, want)
					}
					if got := l.VariantStats(); !slices.Equal(got, variants) {
						t.Errorf("variant stats %+v, want %+v", got, variants)
					}
				}
				const name = "relay"
				verdict := VerdictTakeover
				want := AdmissionStats{Takeover: 1}
				variant := VariantStat{Name: name, Takeover: 1}
				if carrier == "stream-fallback" || carrier == "deferred-fallback" {
					verdict = VerdictFallback
					want = AdmissionStats{Fallback: 1}
					variant = VariantStat{Name: name, Fallback: 1}
				}
				deferred := carrier == "deferred-takeover" || carrier == "deferred-fallback"
				calls := 0
				l = mustNewListener(&ListenerConfig{
					Admission: &Admission{Decide: func(_ context.Context, info CarrierInfo, _ []byte) Decision {
						if deferred && info.Stage == StageHead {
							return Decision{Verdict: VerdictDefer, Variant: "head"}
						}
						return Decision{Verdict: verdict, Variant: name}
					}},
					Fallback: func(context.Context, *Takeover) {
						calls++
						check(want, variant)
						if outcome == "callback panic" {
							panic("relay failed")
						}
					},
				})
				defer func() { _ = l.Close() }()
				var arrival *Arrival
				if carrier == "request" || deferred {
					req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
					var ok bool
					admit := l.Adapter().AdmitRequest
					if deferred {
						admit = l.Adapter().AdmitUpgrade
					}
					arrival, ok = admit(httptest.NewRecorder(), req, nil, nil)
					if !ok {
						t.Fatal("takeover request refused")
					}
					defer arrival.Release()
					if err := arrival.PrepareCarrier(ctx, func(context.Context) error { return nil }); err != nil {
						t.Fatal(err)
					}
					check(AdmissionStats{})
				}
				c := &takeoverAdmissionCarrier{newCountingCarrier(), func() {
					check(AdmissionStats{})
					if outcome == "cancelled" {
						cancel()
					}
				}}
				handle := l.HandlePipe
				if arrival != nil {
					handle = arrival.HandlePipe
				}
				err := handle(ctx, PipeIO{R: c, W: c}, PipeHint{})
				if (err == nil) != (outcome == "delivered") {
					t.Fatalf("HandlePipe returned %v", err)
				}
				if outcome == "cancelled" {
					if calls != 0 {
						t.Fatal("cancelled carrier delivered")
					}
					check(AdmissionStats{Closed: 1}, VariantStat{Name: name, Closed: 1})
				} else {
					if calls != 1 {
						t.Fatalf("host called %d times", calls)
					}
					check(want, variant)
				}
				c.checkOnce(t)
				checkPendingEmpty(t, l)
			})
		}
	}
}

// HS-18.
func TestTakeoverWithoutByteTargetStalls(t *testing.T) {
	for _, asRequest := range []bool{true, false} {
		synctest.Test(t, func(t *testing.T) {
			l := mustNewListener(&ListenerConfig{Admission: &Admission{
				StallMin: time.Minute, StallMax: time.Minute,
				Decide: func(context.Context, CarrierInfo, []byte) Decision {
					return Decision{Verdict: VerdictTakeover}
				},
			}})
			defer func() { _ = l.Close() }()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan struct{})
			var peer net.Conn
			if asRequest {
				req := httptest.NewRequestWithContext(ctx, http.MethodConnect, "/", nil)
				go func() {
					defer close(done)
					defer func() {
						v, _ := recover().(error)
						if !errors.Is(v, http.ErrAbortHandler) {
							t.Errorf("request ended with %v", v)
						}
					}()
					l.Adapter().AdmitUpgrade(httptest.NewRecorder(), req, nil, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
						t.Error("Takeover reached HTTP Next")
					}))
				}()
			} else {
				a, b := net.Pipe()
				peer = b
				defer func() { _ = peer.Close() }()
				go func() {
					defer close(done)
					if err := l.HandlePipe(ctx, NetPipe(a), PipeHint{}); err == nil {
						t.Error("empty takeover target accepted")
					}
				}()
			}
			synctest.Wait()
			if got := l.Stats().Admission; got != (AdmissionStats{Stalled: 1}) {
				t.Fatal(got)
			}
			if got := l.VariantStats(); len(got) != 0 {
				t.Fatal(got)
			}
			l.pmu.Lock()
			pending := l.pending.Total()
			l.pmu.Unlock()
			if pending != 1 {
				t.Fatalf("pending during stall %d", pending)
			}
			if !asRequest {
				select {
				case <-done:
				default:
					t.Fatal("stream setup still waiting for stall")
				}
			}
			cancel()
			<-done
			synctest.Wait()
			checkPendingEmpty(t, l)
		})
	}
}

// HS-51.
func TestInboundLaneRetirementAbortsUnusedHalf(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cc, sc := mpConfig(), mpConfig()
		cc.MaxLanes, sc.MaxLanes = 4, 4
		e := newMemMPEnv(t, cc, sc)
		key := new(int)
		var laneKey [16]byte
		e.server.locked(func() { e.server.s.SetCarrierConn(0, key); laneKey, _ = e.server.s.LaneKey() })
		a, b := net.Pipe()
		writer := &gatedAbort{entered: make(chan struct{}), abortStarted: make(chan struct{}), abortReturn: make(chan struct{})}
		r := &notifiedLane{Conn: b, session: e.server, key: key, taken: make(chan struct{})}
		arrival, err := e.lst.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
		if err != nil {
			t.Fatal(err)
		}
		run, err := arrival.Take(t.Context(), PipeIO{R: r, W: writer}, PipeHint{})
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() { result <- run(t.Context()) }()

		body := wire.AppendPipeLane(nil, laneKey[:], 0, 1)
		if _, err := a.Write(append(varint.AppendVarint(nil, uint64(len(body))), body...)); err != nil {
			t.Fatal(err)
		}
		<-r.taken
		<-writer.entered
		_ = a.Close() // the reader exits before the unused half
		synctest.Wait()
		go e.server.Abort(0, "retire")
		<-writer.abortStarted
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("physical completion %v during Abort", err)
		default:
		}
		close(writer.abortReturn)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})
}

// HS-33.
func TestBootstrapPublicationOrdersJoinAndLane(t *testing.T) {
	for _, carrier := range []string{"duplex", "split"} {
		for _, policy := range []string{"publish", "drain", "shutdown"} {
			t.Run(carrier+"/"+policy, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cc, sc := mpConfig(), mpConfig()
					cc.MaxLanes, sc.MaxLanes = 4, 4
					outcomes := make(chan error, 2)
					sc.JoinOutcome = func(_ *Conn, _ PipeID, err error) { outcomes <- err }
					env := newMemMPEnv(t, cc, sc)
					l := env.lst
					if policy == "drain" {
						l.Drain("draining")
					}
					a, b := net.Pipe()
					clientPipe, serverPipe := NetPipe(a), NetPipe(b)
					if carrier == "split" {
						x, y := net.Pipe()
						clientPipe = PairedPipe(NetPipe(a), NetPipe(x))
						serverPipe = PairedPipe(NetPipe(y), NetPipe(b))
					}
					cfg, err := sc.coreConfig(core.RoleResponder, core.RoleResponder)
					if err != nil {
						t.Fatal(err)
					}
					cfg.Local.Locator = []byte("publication-gate")
					started := make(chan *Conn, 1)
					go func() { c, _ := Start(t.Context(), clientPipe, RoleClient, cc); started <- c }()
					owner := newAttachment(l, false)
					defer owner.release()
					server, err := startCore(t.Context(), serverPipe, sc, cfg, l, nil, nil, owner)
					if err != nil {
						t.Fatal(err)
					}
					defer server.Abort(0, "")
					client := <-started
					if client == nil {
						t.Fatal("bootstrap failed")
					}
					defer client.Abort(0, "")
					joined, laneDone := make(chan error, 1), make(chan error, 1)
					x, y := net.Pipe()
					go func() { _ = l.HandlePipe(t.Context(), NetPipe(y), PipeHint{}) }()
					go func() { _, err := client.AddPipe(t.Context(), NetPipe(x), PipeOptions{}); joined <- err }()
					identity := new(int)
					server.locked(func() { server.s.SetCarrierConn(0, identity) })
					tx, rx := net.Pipe()
					defer func() { _ = tx.Close() }()
					half := &notifiedLane{Conn: rx, session: server, key: identity, taken: make(chan struct{})}
					// The classifier has matched the lane key. Do not inject synthetic
					// record bytes into the peer's cumulative acknowledgement space.
					go func() {
						owner := newAttachment(l, false)
						defer owner.release()
						_, err := server.acceptLane(t.Context(), owned(PipeIO{R: half}), identity, 0, 1, owner)
						laneDone <- err
					}()
					<-half.taken
					synctest.Wait()
					for _, ch := range []chan error{joined, outcomes, laneDone} {
						select {
						case err := <-ch:
							t.Fatalf("handoff before publication: %v", err)
						default:
						}
					}
					reason := "draining"
					if policy == "shutdown" {
						reason = "shutting down"
						st, err := env.client.OpenStreamSync(t.Context())
						if err != nil {
							t.Fatal(err)
						}
						if _, err := st.Write([]byte("x")); err != nil {
							t.Fatal(err)
						}
						if _, err := env.server.AcceptStream(t.Context()); err != nil {
							t.Fatal(err)
						}
						go func() { _ = l.Shutdown(t.Context(), 0, reason) }()
						<-l.drain
					}
					refused, err := l.publishBootstrap(t.Context(), server, owner, policy == "drain", "draining")
					if err != nil || refused != (policy != "publish") {
						t.Fatalf("publish %t: %v", refused, err)
					}
					for _, ch := range []chan error{joined, outcomes, laneDone} {
						if err := <-ch; (err != nil) != refused {
							t.Fatalf("handoff after publication: %v", err)
						}
					}
					if refused {
						err := client.Wait()
						var se *TransportError
						if !errors.Is(err, ErrConnectionRefused) || !errors.As(err, &se) || se.ErrorMessage != reason {
							t.Fatalf("refusal: %v", err)
						}
					} else if c, err := l.Accept(t.Context()); c != server || err != nil {
						t.Fatal(err)
					}
				})
			})
		}
	}
}

// HS-18, HS-19. A send-only carrier still goes through stall admission.
// With one pending slot the quota keeps that slot free of stalls.
func TestTakeoverStatsWriteOnlyWithoutTarget(t *testing.T) {
	for _, limit := range []int{1, 4} {
		l := mustNewListener(&ListenerConfig{MaxPending: limit, Admission: &Admission{
			Decide: func(context.Context, CarrierInfo, []byte) Decision {
				return Decision{Verdict: VerdictTakeover, Variant: "send"}
			},
		}})
		t.Cleanup(func() { _ = l.Close() })
		c := newCountingCarrier()
		if err := l.HandlePipe(t.Context(), PipeIO{W: c}, PipeHint{}); err == nil {
			t.Fatal("send-only carrier accepted")
		}
		want := AdmissionStats{Stalled: 1}
		if limit == 1 {
			want = AdmissionStats{StallOverflow: 1}
		}
		if got := l.Stats().Admission; got != want {
			t.Fatalf("limit %d: %+v, want %+v", limit, got, want)
		}
		if got := l.VariantStats(); !slices.Equal(got, []VariantStat{{Name: "send", Stalled: 1}}) {
			t.Fatal(got)
		}
		c.checkOnce(t)
		checkPendingEmpty(t, l)
	}
}

// HS-02a. Hold the pending-slot lock after reserving an arrival. Publication
// can run, but HandlePipe cannot return the slot and deliver its result.
func TestBootstrapCancellationAfterPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := &Config{ClockDomain: NewClockDomain(timeshim.Real)}
		l := mustNewListener(&ListenerConfig{Config: cfg})
		defer func() { _ = l.Close() }()
		a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		x, y := net.Pipe()
		done := make(chan error, 1)
		l.pmu.Lock()
		release := sync.OnceFunc(l.pmu.Unlock)
		defer release()
		go func() { done <- a.HandlePipe(ctx, NetPipe(y), PipeHint{}) }()
		client, err := Client(t.Context(), x, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Abort(0, "")
		server, err := l.Accept(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer server.Abort(0, "")
		cancel()
		select {
		case err := <-done:
			t.Fatalf("setup returned before slot release: %v", err)
		default:
		}
		release()
		if err := <-done; err != nil {
			t.Fatalf("published bootstrap returned %v", err)
		}
		checkPendingEmpty(t, l)
		echoOnce(t, t.Context(), client, server)
	})
}

// HS-02a. Compare the same bootstrap when cancellation wins before the
// provisional driver can take ownership. Policy still returns a valid config.
func TestBootstrapCancellationBeforeCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cfg := &Config{ClockDomain: NewClockDomain(timeshim.Real)}
		l := mustNewListener(&ListenerConfig{Config: cfg, SessionConfig: func(context.Context, *CarrierInfo, any) (*Config, error) {
			cancel()
			return cfg, nil
		}})
		defer func() { _ = l.Close() }()
		x, y := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- l.HandlePipe(ctx, NetPipe(y), PipeHint{}) }()
		if c, err := Client(t.Context(), x, cfg); err == nil {
			c.Abort(0, "")
			t.Fatal("cancelled bootstrap established")
		}
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("setup returned %v, want context.Canceled", err)
		}
		if len(l.accept) != 0 {
			t.Fatal("cancelled bootstrap published")
		}
		checkPendingEmpty(t, l)
	})
}

// HS-02a. The undetached carrier closes after the fallback callback exits.
// Gate that close to cancel between the callback's outcome and HandlePipe's return.
func TestFallbackCancellationAfterReturn(t *testing.T) {
	for _, want := range []error{nil, errors.New("host failure")} {
		name := "success"
		if want != nil {
			name = "panic"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			l := takeoverEnv(t, 0, func(context.Context, *Takeover) {
				calls++
				if want != nil {
					panic(want)
				}
			})
			x, y := net.Pipe()
			defer func() { _ = x.Close() }()
			writer := &blockedCloseConn{Conn: y, entered: make(chan struct{}), gate: make(chan struct{})}
			release := sync.OnceFunc(func() { close(writer.gate) })
			defer release()
			done := make(chan error, 1)
			go func() { done <- l.HandlePipe(ctx, NetPipe(writer), PipeHint{}) }()
			<-writer.entered
			cancel()
			checkPendingEmpty(t, l)
			release()
			if err := <-done; !errors.Is(err, want) || calls != 1 {
				t.Fatalf("HandlePipe returned %v, want %v after one host call, got %d", err, want, calls)
			}
		})
	}
}

// HS-02a. This gate is before fallback commits, while the attachment still
// owns the carrier. The caller's cancellation wins without invoking the host.
func TestFallbackCancellationBeforeCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	l := takeoverEnv(t, 0, func(context.Context, *Takeover) { t.Error("cancelled fallback invoked") })
	c := &takeoverAdmissionCarrier{newCountingCarrier(), cancel}
	if err := l.HandlePipe(ctx, PipeIO{R: c, W: c}, PipeHint{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("setup returned %v, want context.Canceled", err)
	}
	c.checkOnce(t)
	checkPendingEmpty(t, l)
}
