package zmuxquic

import (
	"bytes"
	"context"
	"regexp"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// The goroutines that serve a connection for Serve keep the caller's
// labels under their task; the lane loop of a dialled connection and the
// sessions carry zmux labels only.
func TestGoroutineLabels(t *testing.T) {
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", srv, testQUIC(&quic.Config{}))
	if err != nil {
		t.Fatal(err)
	}
	lst := mustNewListener(nil)
	tl := &trackedListener{ConnListener: ln}
	sctx, scancel := context.WithCancel(pprof.WithLabels(context.Background(), pprof.Labels("app.req", "42")))
	served := make(chan struct{})
	go func() { _ = Serve(sctx, lst, tl); close(served) }()
	defer func() {
		scancel()
		_ = ln.Close()
		<-served
		_ = lst.Close()
		tl.closeAll()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, testQUIC(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	c, err := Client(ctx, qc, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = c.Close()
		_ = s.Close()
		_ = c.Wait()
		_ = s.Wait()
	}()
	// Both lane receivers must process a stream before inspecting their
	// goroutine labels. The two senders observe the remote reset.
	for _, endpoint := range []*quic.Conn{qc, tl.last()} {
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
	dump := buf.String()
	for _, want := range []string{
		`{"app.req":"42", "zmux":"quic-accept-pipes"}`,
		`{"app.req":"42", "zmux":"quic-accept-lanes"}`,
		`{"zmux":"quic-client-lanes"}`,
	} {
		if !bytes.Contains(buf.Bytes(), []byte("# labels: "+want)) {
			t.Errorf("no goroutine labelled %s", want)
		}
	}
	// A connection is served by its two accept loops alone: no goroutine
	// waits for its end, its handshake or its ServeConn.
	if regexp.MustCompile(`"zmux":"quic-(conn|release|handshake)"`).MatchString(dump) {
		t.Error("a resident goroutine per connection beyond the accept loops")
	}
	if regexp.MustCompile(`"app.req":"42", "zmux":"(read|write)"`).MatchString(dump) {
		t.Error("a session goroutine took the caller's labels")
	}
	if t.Failed() {
		t.Logf("%s", dump)
	}
}
