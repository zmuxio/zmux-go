package zmuxh3

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

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
	srvTLS, cliTLS := tlsConfigs(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	ws := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	lst := mustNewListener(&zmux.ListenerConfig{Config: cfg(), MaxPendingPerSource: perSource,
		Admission: &zmux.Admission{Peek: 4, PeekTimeout: timeout, Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
			if info.Stage == zmux.StageHead {
				return zmux.Decision{Verdict: zmux.VerdictDefer}
			}
			return zmux.Decision{Verdict: zmux.VerdictAccept}
		}}})
	t.Cleanup(func() { _ = lst.Close() })
	wh, err := NewHandler(lst, nil, ws)
	if err != nil {
		t.Fatal(err)
	}
	h3.Handler = wh
	go func() { _ = ws.Serve(pc) }()
	t.Cleanup(func() { _ = ws.Close() })
	return lst, &webtransport.Transport{TLSClientConfig: cliTLS}, "https://" + pc.LocalAddr().String() + "/zmux"
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
