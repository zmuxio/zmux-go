package zmuxh3

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go"
)

// The lane loop of Serve keeps the caller's labels under its task; the
// lane loop of a dialled session carries zmux labels only.
func TestGoroutineLabels(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: cfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	ws := &Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	sctx := pprof.WithLabels(ctx, pprof.Labels("app.req", "42"))
	served := make(chan error, 1)
	peer := make(chan *Session, 1)
	h3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := ws.Upgrade(w, r)
		if err != nil {
			served <- err
			return
		}
		peer <- sess
		served <- Serve(sctx, lst, sess)
	})
	serveDone := make(chan struct{})
	go func() { defer close(serveDone); _ = ws.Serve(pc) }()
	defer func() {
		_ = pc.Close()
		<-serveDone
		_ = ws.Close()
	}()
	tr := &Transport{TLSClientConfig: cliTLS}
	defer func() { _ = tr.Close() }()
	c, sess, err := Dial(ctx, tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, cfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var remote *Session
	select {
	case remote = <-peer:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, endpoint := range []*Session{sess, remote} {
		str, err := endpoint.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := str.Write([]byte{1, 0}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-str.Context().Done():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`{"app.req":"42", "zmux":"wt-accept-lanes"}`, `{"zmux":"wt-client-lanes"}`} {
		if !bytes.Contains(buf.Bytes(), []byte("# labels: "+want)) {
			t.Errorf("no goroutine labelled %s:\n%s", want, &buf)
		}
	}
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
	_ = sess.CloseWithError(0, "")
	select {
	case err := <-served:
		_ = err // the session ended
	case <-ctx.Done():
		t.Fatal("Serve did not return when the session ended")
	}
}
