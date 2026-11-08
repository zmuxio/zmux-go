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
	go func() { _ = Serve(sctx, tl, lst, nil); close(served) }()
	defer func() {
		scancel()
		_ = ln.Close()
		<-served
		_ = lst.Close()
		tl.closeAll()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, ln.Addr().String(), cli, nil, nil)
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
	dump, missing := labelled(t, ctx, []string{
		`{"app.req":"42", "zmux":"quic-conn"}`,
		`{"app.req":"42", "zmux":"quic-accept-pipes"}`,
		`{"app.req":"42", "zmux":"quic-accept-lanes"}`,
		`{"app.req":"42", "zmux":"quic-release"}`,
		`{"zmux":"quic-client-lanes"}`,
	})
	for _, want := range missing {
		t.Errorf("no goroutine labelled %s", want)
	}
	if regexp.MustCompile(`"app.req":"42", "zmux":"(read|write)"`).Match(dump) {
		t.Error("a session goroutine took the caller's labels")
	}
	if t.Failed() {
		t.Logf("%s", dump)
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
