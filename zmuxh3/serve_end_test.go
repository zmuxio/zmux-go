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
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
)

// Serve is answered per request and reports only the end of the session: a
// closed listener resets the stream of the request and never ends the call;
// the session's close, with any code, is success; ctx ending is ctx's error.
func TestServeReportsSessionEnd(t *testing.T) {
	for _, c := range []struct {
		name  string
		close func(sess *webtransport.Session, cancel context.CancelFunc)
		want  func(error) bool
	}{
		{"closed by the peer", func(s *webtransport.Session, _ context.CancelFunc) { _ = s.CloseWithError(0, "") },
			func(err error) bool { return err == nil }},
		{"closed by the peer with a code", func(s *webtransport.Session, _ context.CancelFunc) { _ = s.CloseWithError(7, "bye") },
			func(err error) bool { return err == nil }},
		{"stopped by ctx", func(_ *webtransport.Session, cancel context.CancelFunc) { cancel() },
			func(err error) bool { return errors.Is(err, context.Canceled) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			srvTLS, cliTLS := tlsConfigs(t)
			lst := mustNewListener(&zmux.ListenerConfig{Config: cfg()})
			_ = lst.Close() // every request is answered with a refusal
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
			sctx, scancel := context.WithCancel(ctx)
			defer scancel()
			served := make(chan error, 1)
			h3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sess, err := ws.Upgrade(w, r)
				if err != nil {
					served <- err
					return
				}
				served <- Serve(sctx, lst, sess)
			})
			go func() { _ = ws.Serve(pc) }()
			defer func() { _ = ws.Close() }()
			tr := &webtransport.Transport{TLSClientConfig: cliTLS}
			sess, err := DialSession(ctx, tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			// A request on a closed listener: the stream is closed, the call goes on.
			p, err := SessionPipe(ctx, sess)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = p.W.Write([]byte("request"))
			if n, err := p.R.Read(make([]byte, 1)); n != 0 || err == nil {
				t.Fatalf("the request on a closed listener read %d, %v", n, err)
			}
			select {
			case err := <-served:
				t.Fatalf("a refused request ended Serve with %v", err)
			default:
			}
			c.close(sess, scancel)
			select {
			case err := <-served:
				if !c.want(err) {
					t.Fatalf("Serve returned %v (%#v)", err, err)
				}
			case <-ctx.Done():
				t.Fatal("Serve did not return")
			}
		})
	}
}

// sessionEnd tells a close, whoever closed and whatever the code, from a
// failure of the connection or the session, which is the error itself.
func TestSessionEnd(t *testing.T) {
	failed := []error{
		&quic.TransportError{ErrorCode: quic.InternalError},
		&quic.IdleTimeoutError{},
		&quic.HandshakeTimeoutError{},
		&quic.StatelessResetError{},
		&quic.ApplicationError{ErrorCode: 42, Remote: true},
		&http3.Error{ErrorCode: http3.ErrCodeExcessiveLoad},
		&http3.Error{ErrorCode: http3.ErrCodeDatagramError, ErrorMessage: "malformed capsule"},
		&http3.Error{ErrorCode: http3.ErrCodeDatagramError},
		context.Canceled,
		net.ErrClosed,
	}
	for _, err := range failed {
		if got := sessionEnd(err); !errors.Is(got, err) {
			t.Errorf("%#v ended as %v", err, got)
		}
	}
	closed := []error{
		nil,
		&webtransport.SessionError{ErrorCode: 0},
		&webtransport.SessionError{ErrorCode: 7, Remote: true, Message: "bye"},
		&quic.ApplicationError{ErrorCode: quic.ApplicationErrorCode(http3.ErrCodeNoError), Remote: true},
		&http3.Error{ErrorCode: http3.ErrCodeNoError},
		&http3.Error{ErrorCode: http3.ErrCodeDatagramError, ErrorMessage: "H3_NO_ERROR"},
	}
	for _, err := range closed {
		if got := sessionEnd(err); got != nil {
			t.Errorf("%#v ended as %v", err, got)
		}
	}
}
