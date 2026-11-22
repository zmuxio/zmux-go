package zmux

import (
	"context"
	"crypto/tls"
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

	"github.com/djylb/netx"
	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/carrierio"
)

// HS-01, HS-05. Keep the adapter's physical receipt while setup is interrupted.
func TestSetupInterruptsOwnedCarrier(t *testing.T) {
	for _, phase := range []string{"before", "transform", "external-close", "handshake"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{})
				cfg := &ListenerConfig{}
				raw := newCountingCarrier()
				p := raw.pipe()
				switch phase {
				case "transform", "external-close":
					cfg.WrapPipe = WrapPipeFunc(func(ctx context.Context, p PipeIO, _ PipeHint) (PipeIO, error) {
						close(entered)
						if phase == "external-close" {
							_, err := p.R.Read(make([]byte, 1))
							return PipeIO{}, err
						}
						<-ctx.Done()
						return PipeIO{}, ctx.Err()
					})
				case "handshake":
					cfg.Admission = &Admission{Decide: func(context.Context, CarrierInfo, []byte) Decision {
						t.Error("decision after cancelled handshake")
						return Decision{}
					}}
					h := &handshakeGate{raw, func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }}
					p = PipeIO{R: h, W: h}
				}
				l := mustNewListener(cfg)
				defer func() { _ = l.Close() }()
				a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
				if err != nil {
					t.Fatal(err)
				}
				owner := a.first
				owner.done = make(chan struct{})
				p = owned(p)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if phase == "before" {
					cancel()
				}
				done := make(chan error, 1)
				go func() { done <- a.HandlePipe(ctx, p, PipeHint{}) }()
				want := context.Canceled
				if phase != "before" {
					<-entered
					if phase == "external-close" {
						closePipe(p)
						want = io.EOF
					} else {
						cancel()
					}
				}
				if err := <-done; !errors.Is(err, want) {
					t.Fatalf("setup %v, want %v", err, want)
				}
				<-owner.done
				raw.checkOnce(t)
				checkPendingEmpty(t, l)
				if len(l.accept) != 0 {
					t.Fatal("published cancelled session")
				}
			})
		})
	}
}

type observedExporter struct {
	*earlyExporterConn
	waiting func()
	closes  atomic.Int32
}

func (e *observedExporter) HandshakeComplete() <-chan struct{} { e.waiting(); return e.done }
func (e *observedExporter) Close() error                       { e.closes.Add(1); return e.Conn.Close() }

// HS-01. Interrupt a provisional session awaiting the carrier exporter.
func TestListenerCancelExporterWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := earlyServerConfig()
		cfg.EarlyData.Replayable = false // only the exporter waits for the transport handshake
		cfg.Clock = timeshim.Real
		l := mustNewListener(&ListenerConfig{Config: cfg})
		defer func() { _ = l.Close() }()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		x, y := net.Pipe()
		gate := make(chan struct{})
		e := &observedExporter{earlyExporterConn: newEarlyExporterConn(y), waiting: sync.OnceFunc(func() { close(gate) })}
		a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
		if err != nil {
			t.Fatal(err)
		}
		physical := make(chan struct{})
		a.first.done = physical
		done := make(chan error, 1)
		go func() { done <- a.HandlePipe(ctx, PipeIO{R: e, W: e}, PipeHint{}) }()
		cc := fixedExporterConn{x}
		client := make(chan struct{})
		go func() {
			defer close(client)
			c, _ := Start(t.Context(), PipeIO{R: cc, W: cc}, RoleClient, &Config{Clock: timeshim.Real, MaxPipes: 4, PipeAuth: PipeAuth{Exporter: true, EndToEnd: true}, EarlyData: EarlyData{ZmuxPeer: true}})
			if c != nil {
				<-c.Done()
			}
		}()
		<-gate
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		<-client
		<-physical
		synctest.Wait()
		if e.closes.Load() != 1 || len(l.accept) != 0 {
			t.Fatalf("closes=%d backlog=%d", e.closes.Load(), len(l.accept))
		}
		checkPendingEmpty(t, l)
	})
}

// HS-03. The callback itself crosses the listener cutoff before returning.
func TestDecisionStopsOwnListener(t *testing.T) {
	for _, v := range []AdmissionVerdict{VerdictFallback, VerdictAccept} {
		var l *Listener
		l = mustNewListener(&ListenerConfig{Admission: &Admission{Decide: func(context.Context, CarrierInfo, []byte) Decision { _ = l.Close(); return Decision{Verdict: v} }}, Fallback: func(context.Context, *Takeover) { t.Error("handoff after stop") }})
		raw := newCountingCarrier()
		if err := l.HandlePipe(t.Context(), raw.pipe(), PipeHint{}); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("decision %v: %v", v, err)
		}
		raw.checkOnce(t)
		if len(l.accept) != 0 {
			t.Fatal("published after stop")
		}
	}
}

// HS-06. A late reservation cannot resurrect completion or strand the resource.
func TestFinishedAttachmentRegistration(t *testing.T) {
	a := newAttachment(nil, true)
	a.release()
	for range 2 {
		if a.reserve() {
			t.Fatal("reopened finished attachment")
		}
		<-a.done
	}
	var tail carrierio.RequestTail
	tail.Finish()
	n := 0
	tail.Bind(func() { n++ })
	tail.Finish()
	if n != 1 {
		t.Fatalf("late tail release count %d", n)
	}
}

// HS-08. A panic creating one TLS handshake leaves Serve accepting connections.
func TestServeRecoversTLSHandshakePanic(t *testing.T) {
	srv, cli := selfSignedTLS(t)
	var calls atomic.Int32
	srv.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		if calls.Add(1) == 1 {
			panic("handshake factory")
		}
		return srv, nil
	}
	ln := netx.NewChanListener(nil, 0)
	l := mustNewListener(&ListenerConfig{Admission: &Admission{Decide: func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictAccept} }}})
	done := make(chan error, 1)
	go func() { done <- l.Serve(t.Context(), tls.NewListener(ln, srv)) }()
	defer func() {
		_ = l.Close()
		if err := <-done; !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	}()
	dial := func() *tls.Conn {
		nc, err := ln.Dial(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Client(nc, cli)
	}
	failed := dial()
	if err := failed.HandshakeContext(t.Context()); err == nil {
		t.Fatal("panicking handshake succeeded")
	}
	_ = failed.Close()
	good := dial()
	c, err := Client(t.Context(), good, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Accept(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	echoOnce(t, t.Context(), c, s)
	s.Abort(0, "")
	c.Abort(0, "")
	<-s.Done()
	<-c.Done()
	if l.Stats().Admission.HandshakeFailed != 1 {
		t.Fatal(l.Stats())
	}
}

// HS-17. A custom request adapter cannot omit the accepted response step.
func TestRequestTakeoverRequiresPreparation(t *testing.T) {
	l := mustNewListener(&ListenerConfig{Admission: &Admission{Decide: func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictTakeover} }}, Fallback: func(context.Context, *Takeover) { t.Error("no accepted header") }})
	defer func() { _ = l.Close() }()
	a, ok := l.Adapter().AdmitRequest(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", http.NoBody), nil, nil)
	if !ok {
		t.Fatal("request declined")
	}
	raw := newCountingCarrier()
	if err := a.HandlePipe(t.Context(), raw.pipe(), PipeHint{}); err == nil {
		t.Fatal("takeover without preparing the carrier")
	}
	raw.checkOnce(t)
	checkPendingEmpty(t, l)
}

// HS-35. Publication is distinct from Accept. Only the latter transfers the
// session to the caller's shutdown policy.
func TestShutdownBacklogCloseCode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNewListener(nil)
		defer func() { _ = l.Close() }()
		connect := func() *Conn {
			x, y := net.Pipe()
			setup := make(chan error, 1)
			go func() { setup <- l.HandlePipe(t.Context(), NetPipe(y), PipeHint{}) }()
			c, err := Client(t.Context(), x, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := <-setup; err != nil {
				t.Fatal(err)
			}
			return c
		}
		first := connect()
		_, err := l.Accept(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		queued := connect()
		if err := l.Shutdown(t.Context(), 37, "exit"); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			c    *Conn
			code ApplicationErrorCode
		}{{first, 37}, {queued, 0}} {
			var ae *ApplicationError
			err := tc.c.Wait()
			if !errors.As(err, &ae) || ae.ErrorCode != tc.code {
				t.Fatalf("peer close %v, want %d", err, tc.code)
			}
		}
	})
}

// HS-41. The new counterpart must enter its own wait after withdrawal.
func TestSplitHalfWithdrawal(t *testing.T) {
	for _, expire := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			l := mustNewListener(&ListenerConfig{Config: &Config{HandshakeTimeout: time.Second, Clock: timeshim.Real}})
			defer func() { _ = l.Close() }()
			const token = "AAAAAAAAAAAAAAAAAAAAAA"
			for _, up := range []bool{true, false} {
				raw := newCountingCarrier()
				p := PipeIO{R: raw}
				if !up {
					p = PipeIO{W: raw}
				}
				ctx, stop := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- l.HandlePipe(ctx, p, PipeHint{Pair: token, Uplink: up}) }()
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("matched stale half: %v", err)
				default:
				}
				want := errTimedOut
				if !expire {
					stop()
					want = context.Canceled
				}
				if err := <-done; !errors.Is(err, want) {
					t.Fatal(err)
				}
				stop()
				raw.checkOnce(t)
				checkPendingEmpty(t, l)
			}
		})
	}
}

// HS-54. Done uses one broadcast channel, including after cancelled selects.
func TestCancelledPipeCompletionObservers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		p := e.client.Pipes()[0]
		done := p.Done()
		for range 128 {
			ctx, stop := context.WithCancel(t.Context())
			stop()
			if p.Done() != done {
				t.Fatal("new subscription")
			}
			select {
			case <-done:
				t.Fatal("still live")
			case <-ctx.Done():
			}
		}
		e.client.Abort(0, "")
		<-done
		select {
		case <-p.Done():
		default:
			t.Fatal("late observer missed completion")
		}
	})
}

// HS-04. Stop while the host supplies the nesting identity that would
// trigger an authenticated refusal. No refusal write or transient event escapes.
func TestListenerStopBeforeRefusalClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := mpConfig()
		out := make(chan error, 2)
		cfg.JoinOutcome = func(_ *Conn, _ PipeID, err error) { out <- err }
		e := newMemMPEnv(t, mpConfig(), cfg)
		x, y := net.Pipe()
		r := &gatedIdentity{y, make(chan struct{}), make(chan struct{}), e.server}
		w := &countedReply{Conn: y}
		server, client := make(chan error, 1), make(chan error, 1)
		go func() { server <- e.lst.HandlePipe(t.Context(), PipeIO{R: r, W: w}, PipeHint{}) }()
		go func() { _, err := e.client.AddPipe(t.Context(), NetPipe(x), PipeOptions{}); client <- err }()
		<-r.entered
		_ = e.lst.Close()
		close(r.gate)
		if <-server == nil || <-client == nil {
			t.Fatal("join succeeded")
		}
		if err := <-out; !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
		if w.writes.Load() != 0 {
			t.Fatal("sent reply past listener cutoff")
		}
		stopped, cancel := context.WithCancel(t.Context())
		cancel()
		for {
			ev, err := e.server.NextEvent(stopped)
			if err != nil {
				break
			}
			if pipeEvent(ev.Kind) && ev.PipeID != 0 {
				t.Fatalf("uncommitted event %+v", ev)
			}
		}
		checkPendingEmpty(t, e.lst)
	})
}

type countedReply struct {
	net.Conn
	writes atomic.Int32
}

func (w *countedReply) Write(p []byte) (int, error) { w.writes.Add(1); return w.Conn.Write(p) }

// HS-42. Either request can leave during configuration of the paired session.
func TestPairedSetupSurvivesOneWithdrawal(t *testing.T) {
	for _, cancelUp := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			cfg := mpConfig()
			cfg.Clock = timeshim.Real
			entered, release := make(chan struct{}), make(chan struct{})
			l := mustNewListener(&ListenerConfig{Config: cfg, SessionConfig: func(context.Context, *CarrierInfo, any) (*Config, error) { close(entered); <-release; return cfg, nil }})
			defer func() { _ = l.Close() }()
			a, b := net.Pipe()
			c, d := net.Pipe()
			ctx, stop := context.WithCancel(t.Context())
			defer stop()
			up, down := t.Context(), ctx
			if cancelUp {
				up, down = ctx, t.Context()
			}
			const tok = "AAAAAAAAAAAAAAAAAAAAAA"
			result := make(chan error, 2)
			go func() { result <- l.HandlePipe(up, PipeIO{R: b}, PipeHint{Pair: tok, Uplink: true}) }()
			go func() { result <- l.HandlePipe(down, PipeIO{W: c}, PipeHint{Pair: tok}) }()
			client := make(chan *Conn, 1)
			go func() { cc, _ := Start(t.Context(), PipeIO{R: d, W: a}, RoleClient, cfg); client <- cc }()
			<-entered
			stop()
			synctest.Wait()
			close(release)
			for range 2 {
				if err := <-result; err != nil {
					t.Fatal(err)
				}
			}
			cc := <-client
			if cc == nil {
				t.Fatal("paired client failed")
			}
			s, err := l.Accept(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			echoOnce(t, t.Context(), cc, s)
			cc.Abort(0, "")
			s.Abort(0, "")
			<-cc.Done()
			<-s.Done()
		})
	}
}

type flushingHalf struct {
	flushed chan struct{}
	aborts  atomic.Int32
}

func (*flushingHalf) Write(p []byte) (int, error) { return len(p), nil }
func (*flushingHalf) Close() error                { return nil }
func (f *flushingHalf) Abort(error)               { f.aborts.Add(1); close(f.flushed) }

// HS-50. A successful Close can leave transport flush work behind.
func TestImmediateCloseLingersUntilAbort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := &flushingHalf{flushed: make(chan struct{})}
		c := &Conn{cfg: &Config{}}
		pd := &pipeDrv{c: c, hc: newHalfCloser(PipeIO{W: w})}
		a := newAttachment(nil, true)
		a.attach(pd)
		a.release()
		pd.closeGraceful(time.Second)
		select {
		case <-a.done:
			t.Fatal("reported teardown before linger")
		default:
		}
		<-w.flushed
		<-a.done
		pd.closeAll()
		if w.aborts.Load() != 1 {
			t.Fatal("multiple aborts")
		}
	})
}

// HS-52. The lane's receipt is complete while pipe zero remains usable.
func TestLaneCompletesWithinLivePipe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cc, sc := mpConfig(), mpConfig()
		cc.MaxLanes, sc.MaxLanes = 4, 4
		e := newMemMPEnv(t, cc, sc)
		key := new(int)
		e.server.locked(func() { e.server.s.SetCarrierConn(0, key) })
		a := newAttachment(e.lst, true)
		x, y := net.Pipe()
		_, err := e.server.acceptLane(t.Context(), owned(PipeIO{R: y}), key, 0, 1, a)
		a.release()
		if err != nil {
			t.Fatal(err)
		}
		_ = x.Close()
		<-a.done
		echoOnce(t, t.Context(), e.client, e.server)
	})
}

// HS-19. Every HTTP decision needing a carrier shares the header-failure outcome.
func TestRequestHeaderFailureAccounting(t *testing.T) {
	for _, v := range []AdmissionVerdict{VerdictAccept, VerdictTakeover} {
		l := mustNewListener(&ListenerConfig{Admission: &Admission{Decide: func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: v, Variant: "header"} }}, Fallback: func(context.Context, *Takeover) { t.Error("failed head delivered") }})
		a, ok := l.Adapter().AdmitRequest(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", http.NoBody), nil, nil)
		if !ok {
			t.Fatal("no carrier")
		}
		err := a.PrepareCarrier(t.Context(), func(context.Context) error { return io.ErrClosedPipe })
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal(err)
		}
		raw := newCountingCarrier()
		if err := a.HandlePipe(t.Context(), raw.pipe(), PipeHint{}); err == nil {
			t.Fatal("failed head reused")
		}
		a.Release()
		raw.checkOnce(t)
		checkPendingEmpty(t, l)
		if got := l.Stats().Admission; got != (AdmissionStats{HandshakeFailed: 1}) {
			t.Fatalf("decision %v: %+v", v, got)
		}
		if len(l.VariantStats()) != 0 {
			t.Fatal("attributed uncommitted variant")
		}
		_ = l.Close()
	}
}

// HS-35. An existing handshake is interrupted during Shutdown even while
// an accepted session is keeping the drain open. A new bootstrap during
// that drain has its separate protocol refusal (TestShutdownDrainsBootstrap).
func TestShutdownInterruptsProvisionalHandshake(t *testing.T) {
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	ss, err := e.server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	x, y := tcpPair(t)
	read, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	// Enter directly after bootstrap classification, so a read gate can hold
	// the peer's parameters while its side receives the server flight.
	owner := newAttachment(e.lst, true)
	c := &gatedHandshakeRead{TCPConn: y.(*net.TCPConn), signal: sync.OnceFunc(func() { close(read) }), release: release}
	setup := make(chan error, 1)
	go func() {
		defer owner.release()
		setup <- e.lst.startSession(ctx, NetPipe(c), nil, netip.AddrPort{}, owner)
	}()
	peer, err := Client(ctx, x, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Abort(0, "")
	<-read
	shut := make(chan error, 1)
	go func() { shut <- e.lst.Shutdown(ctx, 39, "already accepted") }()
	select {
	case err := <-setup:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("provisional handshake outlived shutdown cutoff")
	}
	resume()
	var ae *ApplicationError
	err = peer.Wait()
	if !errors.As(err, &ae) || ae.ErrorCode != 0 {
		t.Fatalf("provisional peer close %v", err)
	}
	select {
	case err := <-shut:
		t.Fatalf("drain lost live stream: %v", err)
	default:
	}
	_ = st.Close()
	_, _ = io.Copy(io.Discard, ss)
	_ = ss.Close()
	_, _ = io.Copy(io.Discard, st)
	if err := <-shut; err != nil {
		t.Fatal(err)
	}
	<-owner.done
}

type gatedHandshakeRead struct {
	*net.TCPConn
	signal  func()
	release <-chan struct{}
}

func (c *gatedHandshakeRead) Read(p []byte) (int, error) {
	c.signal()
	<-c.release
	return c.TCPConn.Read(p)
}

// L06. An upgrade head that falls back keeps HTTP semantics even when
// the listener has a byte-stream target ready for the upgraded carrier.
func TestUpgradeFallbackWithoutNext(t *testing.T) {
	l := mustNewListener(&ListenerConfig{Admission: &Admission{Decide: func(context.Context, CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictFallback} }}, Fallback: func(context.Context, *Takeover) { t.Error("HTTP fallback invoked byte-stream target") }})
	defer func() { _ = l.Close() }()
	w := httptest.NewRecorder()
	a, ok := l.Adapter().AdmitUpgrade(w, httptest.NewRequest(http.MethodConnect, "/", http.NoBody), nil, nil)
	if ok || a != nil || w.Code != http.StatusNotFound {
		t.Fatalf("upgrade admitted=%t, arrival=%v, status=%d", ok, a != nil, w.Code)
	}
	checkPendingEmpty(t, l)
	if got := l.Stats().Admission; got != (AdmissionStats{Stalled: 1}) {
		t.Fatal(got)
	}
}
