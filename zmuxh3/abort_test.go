package zmuxh3

import (
	"context"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

// TestAbortReleasesSessionGoneWrite: once the peer has refused a stream with
// WT_SESSION_GONE, a write on it parks until the session ends; Abort must
// release it so a closing Conn's writer can exit.
func TestAbortReleasesSessionGoneWrite(t *testing.T) {
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
	h3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := ws.Upgrade(w, r)
		if err != nil {
			return
		}
		st, err := sess.AcceptStream(r.Context())
		if err != nil {
			return
		}
		rawCancelRead(st, webtransport.WTSessionGoneErrorCode)
		<-r.Context().Done() // the session stays up
	})
	go func() { _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tr := &webtransport.Transport{TLSClientConfig: cliTLS}
	sess, err := DialSession(ctx, tr, "https://"+pc.LocalAddr().String()+"/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	pio, err := OpenPipe(ctx, sess)
	if err != nil {
		t.Fatal(err)
	}
	w := pio.W.(*wtStream)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := w.Write([]byte("x")); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-done:
		t.Fatal("write returned before Abort")
	case <-time.After(500 * time.Millisecond):
	}
	w.Abort(nil)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Abort did not release the parked write")
	}
}

// rawCancelRead sends STOP_SENDING with the wire code itself (the stream
// API maps its codes into the WebTransport range).
func rawCancelRead(st *webtransport.Stream, code quic.StreamErrorCode) {
	recv := reflect.ValueOf(st).Elem().FieldByName("recvStr").Elem()
	f := recv.FieldByName("str")
	f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
	f.Interface().(interface{ CancelRead(quic.StreamErrorCode) }).CancelRead(code)
}
