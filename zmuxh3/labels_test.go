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
	"github.com/quic-go/webtransport-go"
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
	defer func() { _ = pc.Close() }()
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	ws := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sctx := pprof.WithLabels(ctx, pprof.Labels("app.req", "42"))
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
	c, sess, err := Dial(ctx, tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, cfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wants := []string{`{"app.req":"42", "zmux":"wt-accept-lanes"}`, `{"zmux":"wt-client-lanes"}`}
	dump, missing := labelled(t, ctx, wants)
	for _, want := range missing {
		t.Errorf("no goroutine labelled %s:\n%s", want, dump)
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

// labelled samples the goroutine profile until every label set in wants
// is present or ctx ends, and returns the last dump and the label sets
// still missing. A goroutine started by labels.Go sets its labels when it
// first runs, which on a loaded machine can come after the call that
// started it has returned.
func labelled(t *testing.T, ctx context.Context, wants []string) ([]byte, []string) {
	t.Helper()
	for {
		var buf bytes.Buffer
		if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
			t.Fatal(err)
		}
		var missing []string
		for _, want := range wants {
			if !bytes.Contains(buf.Bytes(), []byte("# labels: "+want)) {
				missing = append(missing, want)
			}
		}
		if len(missing) == 0 || ctx.Err() != nil {
			return buf.Bytes(), missing
		}
		time.Sleep(time.Millisecond)
	}
}
