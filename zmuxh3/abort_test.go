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
)

// A STOP_SENDING for a gone session must wake the stream writer while
// leaving other streams on the session alone.
func TestWebTransportStopSendingInterruptsWrite(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	ws := &Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	openWritten := make(chan struct{})
	h3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := ws.Upgrade(w, r)
		if err != nil {
			return
		}
		st, err := sess.AcceptStream(r.Context())
		if err != nil {
			return
		}
		select {
		case <-openWritten:
		case <-r.Context().Done():
			return
		}
		st.r.CancelRead(wtSessionGone)
		<-r.Context().Done() // the session stays up
	})
	served := make(chan struct{})
	go func() { defer close(served); _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close(); <-served }()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	tr := &Transport{TLSClientConfig: cliTLS}
	defer func() { _ = tr.Close() }()
	sess, err := DialSession(ctx, tr, "https://"+pc.LocalAddr().String()+"/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	pio, err := SessionPipe(ctx, sess)
	if err != nil {
		t.Fatal(err)
	}
	w := pio.W.(*wtStream)
	// Opening the WebTransport stream requires sending its header. Wait
	// for the received STOP_SENDING before exercising the WT error path.
	if _, err := w.Write([]byte("open")); err != nil {
		t.Fatal(err)
	}
	close(openWritten)
	select {
	case <-w.s.Context().Done():
		if err, ok := errors.AsType[*quic.StreamError](context.Cause(w.s.Context())); !ok || !err.Remote || err.ErrorCode != wtSessionGone {
			t.Fatalf("stream cancellation: %v", context.Cause(w.s.Context()))
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := w.Write([]byte("x")); err == nil {
		t.Fatal("write survived STOP_SENDING")
	}
	w.Abort(nil)
	if sess.Context().Err() != nil {
		t.Fatalf("stream abort ended the session: %v", context.Cause(sess.Context()))
	}
}
