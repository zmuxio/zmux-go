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
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
)

// A WebTransport session whose head decision is deferred is decided on the
// first bytes of its first bidirectional stream: a zmux carrier makes a
// working session; a host's stream goes to ListenerConfig.Fallback with
// the bytes read replayed and the QUIC connection reachable, and the
// session is closed once the fallback returned.
func TestWebTransportDeferred(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	ws := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
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
	wh, err := NewHandler(lst, nil, ws)
	if err != nil {
		t.Fatal(err)
	}
	h3.Handler = wh
	go func() { _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tr := &webtransport.Transport{TLSClientConfig: cliTLS}
	u := "https://" + pc.LocalAddr().String() + "/zmux"

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

// wtDeferEnv serves WebTransport sessions whose head defers to their
// first stream, which is then accepted, under admission adm.
func wtDeferEnv(t *testing.T, timeout time.Duration, perSource int) (*zmux.Listener, *webtransport.Transport, string) {
	t.Helper()
	return wtSetupEnv(t, timeout, perSource, nil, nil)
}

func wtSetupEnv(t *testing.T, timeout time.Duration, perSource int, wrap zmux.PipeWrapper, middleware func(http.Handler) http.Handler) (*zmux.Listener, *webtransport.Transport, string) {
	t.Helper()
	return wtSetupConfig(t, &zmux.ListenerConfig{Config: cfg(), WrapPipe: wrap, MaxPendingPerSource: perSource,
		Admission: &zmux.Admission{Peek: 4, PeekTimeout: timeout, Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
			if info.Stage == zmux.StageHead {
				return zmux.Decision{Verdict: zmux.VerdictDefer}
			}
			return zmux.Decision{Verdict: zmux.VerdictAccept}
		}}}, middleware)
}

func wtSetupConfig(t *testing.T, config *zmux.ListenerConfig, middleware func(http.Handler) http.Handler) (*zmux.Listener, *webtransport.Transport, string) {
	t.Helper()
	srvTLS, cliTLS := tlsConfigs(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	flow := &webtransport.Config{MaxIncomingStreams: 8, MaxIncomingUniStreams: 8, MaxIncomingData: 4 << 20}
	ws := &webtransport.Server{H3: h3, Config: flow, CheckOrigin: func(*http.Request) bool { return true }}
	lst := mustNewListener(config)
	t.Cleanup(func() { _ = lst.Close() })
	wh, err := NewHandler(lst, nil, ws)
	if err != nil {
		t.Fatal(err)
	}
	if middleware != nil {
		wh = middleware(wh)
	}
	h3.Handler = wh
	go func() { _ = ws.Serve(pc) }()
	t.Cleanup(func() { _ = ws.Close() })
	return lst, &webtransport.Transport{TLSClientConfig: cliTLS, Config: flow}, "https://" + pc.LocalAddr().String() + "/zmux"
}

// A deferred session that opens no stream is closed at PeekTimeout,
// counted PeekTimedOut, and gives its source slot back (one slot per
// source: the next session gets it).
func TestWebTransportDeferredNoStream(t *testing.T) {
	lst, tr, u := wtDeferEnv(t, 300*time.Millisecond, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hs, err := DialSession(ctx, tr, u, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	select {
	case <-hs.Context().Done():
	case <-ctx.Done():
		t.Fatal("the session outlived PeekTimeout")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("closed after %v", d)
	}
	waitAdm(t, lst, zmux.AdmissionStats{PeekTimedOut: 1})
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
}

// Listener.Close ends the wait for a deferred session's first stream: the
// session is closed at once, counted Closed.
func TestWebTransportDeferredListenerClose(t *testing.T) {
	lst, tr, u := wtDeferEnv(t, time.Minute, 0)
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
		t.Fatal("Close left the session waiting")
	}
	waitAdm(t, lst, zmux.AdmissionStats{Closed: 1})
}

// waitAdm waits until l's admission stats are want.
func waitAdm(t *testing.T, l *zmux.Listener, want zmux.AdmissionStats) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); l.Stats().Admission != want; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("stats %+v, want %+v", l.Stats().Admission, want)
		}
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
	_, tr, u := wtSetupEnv(t, time.Minute, 1, wrap, middleware)
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
	http.ResponseWriter
	http3.Settingser
	http3.HTTPStreamer
	entered, interrupt chan struct{}
	phase              string
	once               sync.Once
	acceptedHead       bool
}

func (w *gatedUpgradeWriter) ReceivedSettings() <-chan struct{} {
	if w.phase == "settings" {
		w.once.Do(func() { close(w.entered) })
		return nil
	}
	return w.Settingser.ReceivedSettings()
}
func (w *gatedUpgradeWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.acceptedHead = status == http.StatusOK
}
func (w *gatedUpgradeWriter) Flush() {
	if w.phase == "flush" {
		w.once.Do(func() { close(w.entered) })
		<-w.interrupt
		return
	}
	w.ResponseWriter.(http.Flusher).Flush()
	if w.phase == "first-stream" && w.acceptedHead {
		w.once.Do(func() { close(w.entered) })
	}
}
func (w *gatedUpgradeWriter) FlushError() error {
	if w.phase == "flush-error" {
		w.once.Do(func() { close(w.entered) })
		return io.ErrClosedPipe
	}
	w.Flush()
	return nil
}
func (w *gatedUpgradeWriter) SetWriteDeadline(d time.Time) error {
	close(w.interrupt)
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(d)
}
func (w *gatedUpgradeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// reserveUpgradeSlot waits for the warm carrier's setup to give back the
// listener's sole pending slot. Accept publishes the session before that.
func reserveUpgradeSlot(t *testing.T, ctx context.Context, l *zmux.Listener) *zmux.Arrival {
	t.Helper()
	timer := time.NewTicker(time.Millisecond)
	defer timer.Stop()
	for {
		a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
		if err == nil {
			return a
		}
		select {
		case <-ctx.Done():
			t.Fatalf("warm carrier did not release its pending slot: %v", err)
		case <-timer.C:
		}
	}
}

// HS-57.
func TestWebTransportUpgradeStopScope(t *testing.T) {
	for _, end := range []string{"listener", "accept-timeout", "takeover-timeout", "defer-timeout", "write-error"} {
		phases := []string{"settings", "flush", "first-stream"}
		if end == "write-error" {
			phases = []string{"flush-error"}
		}
		for _, phase := range phases {
			t.Run(end+"/"+phase, func(t *testing.T) {
				entered, ended := make(chan struct{}), make(chan struct{})
				middleware := func(h http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Header.Get("X-Gate") != "" && r.Header.Get("X-Quota-Probe") == "" {
							defer close(ended)
							w = &gatedUpgradeWriter{ResponseWriter: w, Settingser: w.(http3.Settingser), HTTPStreamer: w.(http3.HTTPStreamer), phase: phase, entered: entered, interrupt: make(chan struct{})}
						}
						h.ServeHTTP(w, r)
					})
				}
				clk := timeshim.NewFake(time.Now())
				sc := cfg()
				sc.HandshakeTimeout = time.Second
				sc.Clock = clk
				called := make(chan struct{}, 1)
				policy := &zmux.ListenerConfig{Config: sc, MaxPending: 1, Admission: &zmux.Admission{PeekTimeout: time.Second / 4, Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
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
				rsp, first, err := cc.Dial(ctx, u, nil)
				if rsp != nil {
					defer func() { _ = rsp.Body.Close() }()
				}
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
				slot := reserveUpgradeSlot(t, ctx, l)
				if end == "defer-timeout" && phase == "flush" {
					// Pin the same quota state as a warm carrier that has
					// been published but has not left setup yet.
					func() {
						defer slot.Release()
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
							t.Fatalf("CONNECT with a held pending slot: response %v, error %v", rsp, dialErr)
						}
						if after := l.Stats(); after.Rejected != before.Rejected+1 || after.Admission != before.Admission {
							t.Fatalf("held slot changed admission: before %+v, after %+v", before, after)
						}
					}()
				} else {
					slot.Release()
				}
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
				select {
				case <-entered:
				case err := <-dialFailed:
					select {
					case <-entered: // flush-error can complete the dial after signaling the gate
					default:
						t.Fatalf("CONNECT ended before the upgrade gate: %v", err)
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
				case <-ended:
				case <-ctx.Done():
					t.Fatal("listener stop did not interrupt upgrade")
				}
				if end == "defer-timeout" {
					want := zmux.AdmissionStats{Accepted: 1, PeekTimedOut: 1}
					if got := l.Stats().Admission; got != want {
						t.Fatalf("admission after fake clock expiry: %+v, want %+v", got, want)
					}
				}
				if end != "listener" {
					a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
					if err != nil {
						t.Fatalf("pending slot: %v", err)
					}
					a.Release()
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
			})
		}
	}
}
