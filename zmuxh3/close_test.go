package zmuxh3

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// A pooled WebTransport session (WebTransportCandidate's) is closed once its pipe has
// ended, after the peer read the zmux close, with that close's code.
func TestPoolCloseCode(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: cfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	ws := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sessions := make(chan *webtransport.Session, 1)
	h3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := ws.Upgrade(w, r)
		if err != nil {
			return
		}
		sessions <- sess
		_ = Serve(ctx, lst, sess)
	})
	go func() { _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close() }()
	tr := &webtransport.Transport{TLSClientConfig: cliTLS}
	pool := NewConnPool(tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, nil, carrier.PoolLimits{MaxPipesPerConnectionLifetime: 1})
	defer pool.Shutdown()
	p, err := pool.OpenPipe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, cfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	srvSess := <-sessions
	_ = c.CloseWithError(0x2a, "bye")
	var ae *zmux.ApplicationError
	if err := s.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 0x2a || !ae.Remote {
		t.Fatalf("the peer's zmux session ended with %v", err)
	}
	select {
	case <-srvSess.Context().Done():
	case <-ctx.Done():
		t.Fatal("the pooled WebTransport session was not closed")
	}
	// webtransport-go may end the server's side on its CONNECT stream
	// before it reads the close capsule; a session close it did read
	// carries the code.
	var se *webtransport.SessionError
	if err := closeError(srvSess); errors.As(err, &se) && (!se.Remote || se.ErrorCode != 0x2a || se.Message != "bye") {
		t.Fatalf("the WebTransport session ended with %#v", err)
	}
	_ = c.Wait()
}

func TestSessionCloseCode(t *testing.T) {
	for _, c := range []struct {
		err  error
		code webtransport.SessionErrorCode
		msg  string
	}{
		{&zmux.ApplicationError{ErrorCode: 0x2a, ErrorMessage: "bye"}, 0x2a, "bye"},
		{&zmux.ApplicationError{ErrorCode: 1 << 40, ErrorMessage: "big"}, 0, ""},
		{&zmux.IdleTimeoutError{}, 0, ""},
		{nil, 0, ""},
	} {
		if code, msg := SessionCloseCode(c.err); code != c.code || msg != c.msg {
			t.Fatalf("%v: %#x %q", c.err, code, msg)
		}
	}
}

// pipeRelease withdraws its registration on the connection's context once
// the pipe ended: the connection's end calls release no more.
func TestPipeReleaseWithdraws(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var n atomic.Int32
	ended := pipeRelease(ctx, func() { n.Add(1) })
	ended()
	cancel()
	time.Sleep(20 * time.Millisecond)
	if got := n.Load(); got != 1 {
		t.Fatalf("release called %d times", got)
	}
}

// closeError on a session that is still open does not take a stream from
// it without ending it: a unidirectional stream it takes is reset.
func TestCloseErrorResetsTakenStream(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reset := make(chan error, 1)
	h3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := ws.Upgrade(w, r)
		if err != nil {
			reset <- err
			return
		}
		defer func() { _ = sess.CloseWithError(0, "") }()
		s, err := sess.OpenUniStreamSync(ctx)
		if err != nil {
			reset <- err
			return
		}
		for {
			if _, err := s.Write([]byte("x")); err != nil {
				reset <- err
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	go func() { _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close() }()
	tr := &webtransport.Transport{TLSClientConfig: cliTLS}
	sess, err := DialSession(ctx, tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		_ = closeError(sess)
		select {
		case err := <-reset:
			var se *webtransport.StreamError
			if !errors.As(err, &se) {
				t.Fatalf("the sender ended with %v, not a reset", err)
			}
			return
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("the taken stream was not reset")
		}
	}
}
