package zmuxws

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

// A WebSocket carrier taken over on its first message and detached: the
// host serves it on another goroutine once the handler has returned (zmux
// is done with it), through the typed message halves, with the decision's
// Value, and closes it.
func TestTakeoverDetachedWebSocket(t *testing.T) {
	ctx := testContext(t)
	adm := &zmux.Admission{Decide: func(_ context.Context, info zmux.CarrierInfo, prefix []byte) zmux.Decision {
		if info.Stage == zmux.StageHead {
			return zmux.Decision{Verdict: zmux.VerdictDefer}
		}
		if bytes.HasPrefix(prefix, takeMark) {
			return zmux.Decision{Verdict: zmux.VerdictFallback, Value: "relay"}
		}
		return zmux.Decision{Verdict: zmux.VerdictClose}
	}}
	tks := make(chan *zmux.Takeover, 1)
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm,
		Fallback: func(_ context.Context, tk *zmux.Takeover) {
			if err := tk.Detach(); err != nil {
				t.Error(err)
			}
			tks <- tk // served elsewhere; Fallback returns
		}})
	h, err := NewHandler(l, &zmuxhttp.Options{Next: next})
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
		handled <- struct{}{}
	}))
	t.Cleanup(func() { _ = l.Close(); srv.Close() })

	cli, err := DialPipe(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/z", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer zmux.ClosePipeIO(cli)
	if err := cli.W.(zmux.MessageWriter).WriteMessage(append(bytes.Clone(takeMark), " 1"...)); err != nil {
		t.Fatal(err)
	}
	var tk *zmux.Takeover
	select {
	case tk = <-tks:
	case <-ctx.Done():
		t.Fatal("no takeover")
	}
	select {
	case <-handled: // Fallback returned and the handler with it
	case <-ctx.Done():
		t.Fatal("the handler did not return")
	}
	if tk.Value != "relay" {
		t.Fatalf("value %v", tk.Value)
	}
	mr, mw, ok := tk.Messages()
	if !ok {
		t.Fatal("no message halves")
	}
	served := make(chan error, 1)
	go func() {
		defer zmux.ClosePipeIO(tk.Pipe())
		for range 2 {
			m, err := mr.ReadMessage(zmux.MaxRecordSize)
			if err != nil {
				served <- err
				return
			}
			reply := append([]byte("re:"), m.Bytes()...)
			m.Release()
			if err := mw.WriteMessage(reply); err != nil {
				served <- err
				return
			}
		}
		served <- nil
	}()
	if err := cli.W.(zmux.MessageWriter).WriteMessage([]byte("second")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"re:TAKE 1", "re:second"} {
		got, err := cli.R.(zmux.MessageReader).ReadMessage(64)
		if err != nil || string(got.Bytes()) != want {
			t.Fatalf("reply %q, %v; want %q", got.Bytes(), err, want)
		}
		got.Release()
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if !readEnd(cli, 5*time.Second) {
		t.Fatal("the host's Close did not end the carrier")
	}
}
