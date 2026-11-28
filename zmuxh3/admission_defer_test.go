package zmuxh3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/djylb/timeshim"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// A WebTransport session whose head decision is deferred is decided on the
// first bytes of its first bidirectional stream: a zmux carrier makes a
// working session; a host's stream goes to ListenerConfig.Fallback with
// the bytes read replayed and the QUIC connection reachable, and the
// session is closed once the fallback returned.
func TestWebTransportDeferred(t *testing.T) {
	taken := make(chan []byte, 1)
	lst := mustNewListener(&zmux.ListenerConfig{Config: cfg(),
		Admission: &zmux.Admission{Peek: 4, Decide: func(_ context.Context, info zmux.CarrierInfo, prefix []byte) zmux.Decision {
			switch {
			case !info.Upgrade || !info.Source.IsValid():
				return zmux.Decision{Verdict: zmux.VerdictClose}
			case info.Stage == zmux.StageHead:
				return zmux.Decision{Verdict: zmux.VerdictDefer}
			case info.Framing != zmux.FramingByteStream:
				return zmux.Decision{Verdict: zmux.VerdictClose}
			case string(prefix) == "TAKE":
				return zmux.Decision{Verdict: zmux.VerdictFallback}
			}
			return zmux.Decision{Verdict: zmux.VerdictAccept}
		}},
		Fallback: func(_ context.Context, tk *zmux.Takeover) {
			if _, ok := tk.Pipe().W.(zmux.CarrierConner); !ok {
				taken <- nil
				return
			}
			if _, ok := tk.Conn(); ok {
				taken <- nil
				return
			}
			b := make([]byte, 9)
			_, _ = io.ReadFull(tk.Pipe().R, b)
			taken <- b
		}})
	defer func() { _ = lst.Close() }()
	tr, u := startWTFixture(t, lst, nil)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	c, sess, err := Dial(ctx, tr, u, nil, cfg())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	_ = c.Close()
	_ = s.Close()

	hs, err := DialSession(ctx, tr, u, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	st, err := hs.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = st.Write([]byte("TAKEhello"))
	select {
	case b := <-taken:
		if !bytes.Equal(b, []byte("TAKEhello")) {
			t.Fatalf("fallback read %q", b)
		}
	case <-ctx.Done():
		t.Fatal("no fallback")
	}
	select {
	case <-hs.Context().Done():
	case <-ctx.Done():
		t.Fatal("the session outlived its fallback")
	}
	if a := lst.Stats().Admission; a.Accepted != 1 || a.Fallback != 1 || a.Closed != 0 {
		t.Fatalf("stats %+v", a)
	}
}

func TestWebTransportDetachedTakeoverOwnsSession(t *testing.T) {
	detached := make(chan zmux.PipeIO, 1)
	handled := make(chan struct{})
	lst := mustNewListener(&zmux.ListenerConfig{Config: cfg(),
		Admission: &zmux.Admission{Peek: 4, Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
			if info.Stage == zmux.StageHead {
				return zmux.Decision{Verdict: zmux.VerdictDefer}
			}
			return zmux.Decision{Verdict: zmux.VerdictTakeover}
		}},
		Fallback: func(_ context.Context, tk *zmux.Takeover) {
			if err := tk.Detach(); err != nil {
				t.Error(err)
				return
			}
			detached <- tk.Pipe()
		}})
	defer func() { _ = lst.Close() }()
	tr, endpoint := startWTFixture(t, lst, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(handled)
			next.ServeHTTP(w, r)
		})
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, err := DialSession(ctx, tr, endpoint, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.CloseWithError(0, "") }()
	stream, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	_ = stream.SetDeadline(deadline)
	if _, err := stream.Write([]byte("host")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatal("CONNECT did not return after detaching")
	}
	select {
	case pipe := <-detached:
		owner := pipe.W.(zmux.CarrierConner).CarrierConn().(*Session)
		defer func() { _ = owner.CloseWithError(0, "") }()
		var request [4]byte
		if _, err := io.ReadFull(pipe.R, request[:]); err != nil || string(request[:]) != "host" {
			t.Fatalf("detached request %q, %v", request, err)
		}
		if _, err := pipe.W.Write([]byte("ok")); err != nil {
			t.Fatal(err)
		}
		checkDetachedWTChannel(t, ctx, pipe, client, stream.StreamID())
	default:
		t.Fatal("CONNECT finished without handing over the stream")
	}
	var reply [2]byte
	if _, err := io.ReadFull(stream, reply[:]); err != nil || string(reply[:]) != "ok" {
		t.Fatalf("detached response %q, %v", reply, err)
	}
}

func checkDetachedWTChannel(t *testing.T, ctx context.Context, pipe zmux.PipeIO, client *Session, streamID quic.StreamID) {
	t.Helper()
	if _, ok := pipe.W.(zmux.DatagramChannel); ok {
		t.Fatal("the replay view unexpectedly exposes the native channel")
	}
	unwrap, ok := pipe.W.(interface{ Unwrap() io.WriteCloser })
	if !ok {
		t.Fatal("cannot reach the underlying WebTransport writer")
	}
	channel, ok := unwrap.Unwrap().(zmux.DatagramChannel)
	if !ok {
		t.Fatal("the original WebTransport pipe lacks its native channel")
	}
	id, ok := channel.ChannelStreamID()
	if !ok || id != uint64(streamID) {
		t.Fatalf("native channel id %d, want %d", id, streamID)
	}
	received := make(chan []byte, 1)
	stop := channel.ReceiveChannelDatagrams(func(frame []byte) {
		select {
		case received <- frame:
		default:
		}
	})
	defer stop()
	payload := []byte("native")
	wire := carrier.AppendChannelDatagram(nil, true, id, payload)
	if err := client.SendDatagram(wire); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-received:
		if !bytes.Equal(frame, carrier.AppendChannelDatagram(nil, false, 0, payload)) {
			t.Fatalf("host received %x", frame)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := channel.SendChannelDatagram(wire); err != nil {
		t.Fatal(err)
	}
	if got, err := client.ReceiveDatagram(ctx); err != nil || !bytes.Equal(got, wire) {
		t.Fatalf("client received %x, %v", got, err)
	}
}

func TestWebTransportRejectsForwardedEarlyData(t *testing.T) {
	lst := mustNewListener(&zmux.ListenerConfig{Config: cfg()})
	defer func() { _ = lst.Close() }()
	tr, endpoint := startWTFixture(t, lst, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	session, err := DialSession(ctx, tr, endpoint, http.Header{"Early-Data": {"1"}}, nil)
	if session != nil {
		_ = session.CloseWithError(0, "")
		t.Fatal("early CONNECT opened a session")
	}
	if e, ok := errors.AsType[*ConnectError](err); !ok || e.StatusCode != http.StatusTooEarly {
		t.Fatalf("early CONNECT: %v", err)
	}
	if stats := lst.Stats(); stats.SetupFailures != 1 {
		t.Fatalf("early CONNECT setup failures: %d, want 1", stats.SetupFailures)
	}
}

// wtDeferEnv serves WebTransport sessions whose head defers to their
// first stream, which is then accepted, under admission adm.
func wtDeferEnv(t *testing.T, timeout time.Duration, perSource int) (*zmux.Listener, *Transport, string, *timeshim.Fake, <-chan struct{}) {
	t.Helper()
	clk := timeshim.NewFake(time.Now())
	c := cfg()
	c.ClockDomain = zmux.NewClockDomain(clk)
	finished := make(chan struct{})
	end := sync.OnceFunc(func() { close(finished) })
	l, tr, u := wtSetupConfig(t, wtDeferredConfig(c, timeout, perSource, nil), func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer end()
			h.ServeHTTP(w, r)
		})
	})
	return l, tr, u, clk, finished
}

func wtDeferredConfig(cfg *zmux.Config, timeout time.Duration, perSource int, wrap zmux.PipeWrapper) *zmux.ListenerConfig {
	return &zmux.ListenerConfig{Config: cfg, WrapPipe: wrap, MaxPendingPerSource: perSource,
		Admission: &zmux.Admission{Peek: 4, PeekTimeout: timeout, Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
			if info.Stage == zmux.StageHead {
				return zmux.Decision{Verdict: zmux.VerdictDefer}
			}
			return zmux.Decision{Verdict: zmux.VerdictAccept}
		}}}
}

func wtSetupConfig(t *testing.T, config *zmux.ListenerConfig, middleware func(http.Handler) http.Handler) (*zmux.Listener, *Transport, string) {
	t.Helper()
	lst := mustNewListener(config)
	t.Cleanup(func() { _ = lst.Close() })
	tr, endpoint := startWTFixture(t, lst, middleware)
	return lst, tr, endpoint
}

func startWTFixture(t *testing.T, lst *zmux.Listener, middleware func(http.Handler) http.Handler) (*Transport, string) {
	t.Helper()
	srvTLS, cliTLS := tlsConfigs(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS), QUICConfig: testQUIC(), ConnContext: QUICConnContext}
	server := &Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	tr := &Transport{TLSClientConfig: cliTLS}
	var done chan struct{}
	t.Cleanup(func() {
		_ = tr.Close()
		_ = server.Close()
		_ = pc.Close()
		if done != nil {
			<-done
		}
	})
	handler, err := NewHandler(lst, nil, server)
	if err != nil {
		t.Fatal(err)
	}
	if middleware != nil {
		handler = middleware(handler)
	}
	h3.Handler = handler
	done = make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(pc)
	}()
	return tr, "https://" + pc.LocalAddr().String() + "/zmux"
}

// A deferred session that opens no stream is closed at PeekTimeout,
// counted PeekTimedOut, and gives its source slot back. Both slots are
// held initially, so a replacement requires that release.
func TestWebTransportDeferredNoStream(t *testing.T) {
	const peek = time.Second
	lst, tr, u, clk, finished := wtDeferEnv(t, peek, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hs, err := DialSession(ctx, tr, u, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hs.CloseWithError(0, "") })
	firstDeadline := clk.Now().Add(peek)
	if err := clk.WaitDeadline(ctx, firstDeadline); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Nanosecond)
	other, err := DialSession(ctx, tr, u, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.CloseWithError(0, "") })
	if err := clk.WaitDeadline(ctx, clk.Now().Add(peek)); err != nil {
		t.Fatal(err)
	}
	clk.Advance(firstDeadline.Sub(clk.Now()))
	select {
	case <-hs.Context().Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	checkFinishedAdmission(t, ctx, finished, lst, zmux.AdmissionStats{PeekTimedOut: 1})
	c, sess, err := Dial(ctx, tr, u, nil, cfg())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	_ = s.Close()
	if got, want := lst.Stats().Admission, (zmux.AdmissionStats{Accepted: 1, PeekTimedOut: 1}); got != want {
		t.Fatalf("admission after replacing the expired session %+v, want %+v", got, want)
	}
}

// Listener.Close ends the wait for a deferred session's first stream: the
// session is closed at once, counted Closed.
func TestWebTransportDeferredListenerClose(t *testing.T) {
	lst, tr, u, _, finished := wtDeferEnv(t, time.Minute, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hs, err := DialSession(ctx, tr, u, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = lst.Close()
	select {
	case <-hs.Context().Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	checkFinishedAdmission(t, ctx, finished, lst, zmux.AdmissionStats{Closed: 1})
}

func checkFinishedAdmission(t *testing.T, ctx context.Context, finished <-chan struct{}, l *zmux.Listener, want zmux.AdmissionStats) {
	t.Helper()
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got := l.Stats().Admission; got != want {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}

// HS-57.
func TestWebTransportFirstStreamSetupCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	request := make(chan context.CancelFunc, 1)
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, stop := context.WithCancel(r.Context())
			defer stop()
			request <- stop
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	entered := make(chan struct{})
	ended := make(chan error, 1)
	wrap := zmux.WrapPipeFunc(func(setup context.Context, _ zmux.PipeIO, _ zmux.PipeHint) (zmux.PipeIO, error) {
		close(entered)
		<-setup.Done()
		ended <- setup.Err()
		return zmux.PipeIO{}, setup.Err()
	})
	_, tr, u := wtSetupConfig(t, wtDeferredConfig(cfg(), time.Minute, 0, wrap), middleware)
	sess, err := DialSession(ctx, tr, u, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	st, err := sess.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Write([]byte("test")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop := <-request
	stop()
	select {
	case err := <-ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first setup outlived request")
	}
	if sess.Context().Err() != nil {
		t.Fatal("request cancellation closed the shared session")
	}
}

// A gated CONNECT delays either SETTINGS delivery or the final response
// flush. A write deadline interrupts only this request.
type gatedUpgradeWriter struct {
	entered, interrupt chan struct{}
	phase              string
	observedH3Response
	once sync.Once
}

type observedH3Response struct {
	http.ResponseWriter
	http3.Settingser
	http3.HTTPStreamer
	ownership h3ResponseOwnership
}

type h3ResponseOwnership struct {
	taken, committed, bodyAfterTake bool
	finalHeaders                    int
}

func (w *observedH3Response) HTTPStream() *http3.Stream {
	str := w.HTTPStreamer.HTTPStream()
	w.ownership.taken = str != nil
	return str
}

func (w *observedH3Response) WriteHeader(status int) {
	if status >= 200 {
		w.ownership.finalHeaders++
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *observedH3Response) Write(p []byte) (int, error) {
	w.ownership.bodyAfterTake = w.ownership.bodyAfterTake || w.ownership.taken
	return w.ResponseWriter.Write(p)
}

func (w *observedH3Response) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.ownership.committed = w.ownership.committed || err == nil
	return err
}

func (w *observedH3Response) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *gatedUpgradeWriter) ReceivedSettings() <-chan struct{} {
	if w.phase == "settings" {
		w.once.Do(func() { close(w.entered) })
		return nil
	}
	return w.Settingser.ReceivedSettings()
}
func (w *gatedUpgradeWriter) FlushError() error {
	switch w.phase {
	case "flush-error":
		w.once.Do(func() { close(w.entered) })
		return io.ErrClosedPipe
	case "flush":
		w.once.Do(func() { close(w.entered) })
		<-w.interrupt
		return io.ErrClosedPipe
	default:
		err := w.observedH3Response.FlushError()
		w.once.Do(func() { close(w.entered) })
		if w.phase == "flushed" {
			<-w.interrupt
		}
		return err
	}
}
func (w *gatedUpgradeWriter) SetWriteDeadline(d time.Time) error {
	close(w.interrupt)
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(d)
}

// HS-57.
func TestWebTransportUpgradeStopScope(t *testing.T) {
	for _, end := range []string{"listener", "accept-timeout", "takeover-timeout", "defer-timeout", "write-error"} {
		phases := []string{"settings", "flush", "flushed", "first-stream"}
		if end == "write-error" {
			phases = []string{"flush-error"}
		}
		for _, phase := range phases {
			t.Run(end+"/"+phase, func(t *testing.T) {
				entered := make(chan struct{})
				ended := make(chan h3ResponseOwnership, 1)
				warmDone := make(chan struct{})
				middleware := func(h http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						switch {
						case r.Header.Get("X-Warm") != "":
							defer close(warmDone)
						case r.Header.Get("X-Gate") != "" && r.Header.Get("X-Quota-Probe") == "":
							gate := &gatedUpgradeWriter{
								ResponseWriter: w, Settingser: w.(http3.Settingser), HTTPStreamer: w.(http3.HTTPStreamer),
								phase: phase, entered: entered, interrupt: make(chan struct{}),
							}
							defer func() { ended <- gate.ownership }()
							w = gate
						}
						h.ServeHTTP(w, r)
					})
				}
				clk := timeshim.NewFake(time.Now())
				sc := cfg()
				sc.HandshakeTimeout = time.Second
				sc.MaxLanes = 1
				sc.ClockDomain = zmux.NewClockDomain(clk)
				called := make(chan struct{}, 1)
				// Allow the warm setup and the gated upgrade to overlap on separate connections.
				const slots = 2
				policy := &zmux.ListenerConfig{Config: sc, MaxPending: slots, Admission: &zmux.Admission{PeekTimeout: time.Second / 4, Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
					v := zmux.VerdictAccept
					if info.Stage == zmux.StageHead && info.Request.Header.Get("X-Gate") != "" {
						switch end {
						case "takeover-timeout", "write-error":
							v = zmux.VerdictTakeover
						case "defer-timeout", "listener":
							v = zmux.VerdictDefer
						}
					}
					return zmux.Decision{Verdict: v}
				}}, Fallback: func(context.Context, *zmux.Takeover) { called <- struct{}{} }}
				l, tr, u := wtSetupConfig(t, policy, middleware)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				address, err := url.Parse(u)
				if err != nil {
					t.Fatal(err)
				}
				tls := tr.TLSClientConfig.Clone()
				tls.NextProtos = []string{http3.NextProtoH3}
				qc, err := quic.DialAddr(ctx, address.Host, tls, &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true})
				if err != nil {
					t.Fatal(err)
				}
				var dialed chan struct{}
				defer func() {
					cancel()
					_ = qc.CloseWithError(0, "")
					if dialed != nil {
						<-dialed
					}
				}()
				cc, err := tr.NewClientConn(qc)
				if err != nil {
					t.Fatal(err)
				}
				holdSlots := func() func() {
					t.Helper()
					var held []*zmux.Arrival
					release := func() {
						for _, a := range held {
							a.Release()
						}
					}
					t.Cleanup(release)
					for range slots {
						a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
						if err != nil {
							t.Fatalf("pending slot: %v", err)
						}
						held = append(held, a)
					}
					return release
				}
				if end == "defer-timeout" && phase == "flush" {
					func() {
						defer holdSlots()()
						before := l.Stats()
						rsp, session, dialErr := cc.Dial(ctx, u, http.Header{
							"X-Gate": []string{phase}, "X-Quota-Probe": []string{"1"},
						})
						if rsp != nil {
							defer func() { _ = rsp.Body.Close() }()
						}
						if session != nil {
							_ = session.CloseWithError(0, "")
						}
						if dialErr == nil || rsp == nil || rsp.StatusCode != http.StatusNotFound {
							t.Fatalf("CONNECT with full pending quota: response %v, error %v", rsp, dialErr)
						}
						if after := l.Stats(); after.Rejected != before.Rejected+1 || after.Admission != before.Admission {
							t.Fatalf("full pending quota changed admission: before %+v, after %+v", before, after)
						}
					}()
				}
				first, err := DialSession(ctx, tr, u, http.Header{"X-Warm": []string{"1"}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = first.CloseWithError(0, "") }()
				st, err := first.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				client, err := zmux.Start(ctx, StreamPipe(first, st), zmux.RoleClient, cfg())
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = client.Close() }()
				server, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = server.Close() }()
				dialed = make(chan struct{})
				dialFailed := make(chan error, 1)
				go func() {
					defer close(dialed)
					rsp, s, dialErr := cc.Dial(ctx, u, http.Header{"X-Gate": []string{phase}})
					if dialErr != nil {
						dialFailed <- dialErr
					}
					if rsp != nil {
						defer func() { _ = rsp.Body.Close() }()
					}
					if s != nil {
						<-ctx.Done()
						_ = s.CloseWithError(0, "")
					}
				}()
				var dialErr error
				select {
				case <-entered:
				case dialErr = <-dialFailed:
					select {
					case <-entered: // flush-error can complete the dial after signaling the gate
					default:
						t.Fatalf("CONNECT ended before the upgrade gate: %v", dialErr)
					}
				case <-ctx.Done():
					t.Fatal("upgrade did not reach gate")
				}
				switch end {
				case "listener":
					_ = l.Close()
				case "write-error": // no deadline or stop is needed
				case "defer-timeout":
					clk.Advance(sc.HandshakeTimeout / 4)
				default:
					clk.Advance(sc.HandshakeTimeout)
				}
				select {
				case ownership := <-ended:
					want := h3ResponseOwnership{taken: phase != "settings", committed: phase == "flushed" || phase == "first-stream"}
					if want.taken {
						want.finalHeaders = 1
					}
					if ownership != want {
						t.Fatalf("HTTP/3 response at gate %s: %+v, want %+v", phase, ownership, want)
					}
				case <-ctx.Done():
					t.Fatal("listener stop did not interrupt upgrade")
				}
				if end == "write-error" && dialErr == nil {
					select {
					case err := <-dialFailed:
						dialErr = err
					case <-ctx.Done():
						t.Fatal("failed response flush did not fail the CONNECT")
					}
				}
				if end == "write-error" && (dialErr == nil || ctx.Err() != nil) {
					t.Fatalf("failed response flush: CONNECT error %v, watchdog %v", dialErr, ctx.Err())
				}
				if end == "defer-timeout" {
					want := zmux.AdmissionStats{Accepted: 1, PeekTimedOut: 1}
					if got := l.Stats().Admission; got != want {
						t.Fatalf("admission after fake clock expiry: %+v, want %+v", got, want)
					}
				}
				select {
				case <-called:
					t.Fatal("uncommitted takeover")
				default:
				}
				stream, err := client.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Write([]byte("live")); err != nil {
					t.Fatal(err)
				}
				received, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var data [4]byte
				if _, err := io.ReadFull(received, data[:]); err != nil || string(data[:]) != "live" {
					t.Fatalf("existing pipe: %q, %v", data, err)
				}
				_ = stream.Close()
				_ = received.Close()
				if end != "listener" {
					_ = first.CloseWithError(0, "")
					select {
					case <-warmDone:
					case <-ctx.Done():
						t.Fatal("warm CONNECT did not end")
					}
					holdSlots()()
				}
			})
		}
	}
}
