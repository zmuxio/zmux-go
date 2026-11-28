package zmuxh3

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
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
	ws := &Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sessions := make(chan *Session, 1)
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
	tr := &Transport{TLSClientConfig: cliTLS}
	defer func() { _ = tr.Close() }()
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
	var se *SessionError
	if err := context.Cause(srvSess.Context()); !errors.As(err, &se) || !se.Remote || se.ErrorCode != 0x2a || se.Message != "bye" {
		t.Fatalf("the WebTransport session ended with %#v", err)
	}
	_ = c.Wait()
}

func TestSessionCloseCode(t *testing.T) {
	for _, c := range []struct {
		err  error
		msg  string
		code SessionErrorCode
	}{
		{err: &zmux.ApplicationError{ErrorCode: 0x2a, ErrorMessage: "bye"}, code: 0x2a, msg: "bye"},
		{err: &zmux.ApplicationError{ErrorCode: 1 << 40, ErrorMessage: "big"}, code: 0, msg: ""},
		{err: &zmux.IdleTimeoutError{}, code: 0, msg: ""},
		{err: nil, code: 0, msg: ""},
	} {
		if code, msg := SessionCloseCode(c.err); code != c.code || msg != c.msg {
			t.Fatalf("%v: %#x %q", c.err, code, msg)
		}
	}
}
