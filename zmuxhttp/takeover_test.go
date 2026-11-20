package zmuxhttp

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// A stream carrier that the admission hands back on its body's first
// bytes reaches ListenerConfig.Fallback as a full duplex pipe, with the
// decision's Value: the host detaches it, serves it on another goroutine
// after Fallback returned (an echo, while the client still streams its
// body), and its Close ends the response. HTTP/1.1 and HTTP/2.
func TestHTTPRequestCarrierTakeover(t *testing.T) {
	for _, mode := range []string{"h1", "h2", "h1-half", "h2-half"} {
		h2 := strings.HasPrefix(mode, "h2")
		half := strings.HasSuffix(mode, "-half")
		t.Run(mode, func(t *testing.T) {
			leakcheck.Check(t)
			adm := &zmux.Admission{Peek: 4, PeekTimeout: 5 * time.Second,
				Decide: func(_ context.Context, info zmux.CarrierInfo, prefix []byte) zmux.Decision {
					if info.Stage != zmux.StageCarrier || info.Request == nil || string(prefix) != "HOST" {
						return zmux.Decision{Verdict: zmux.VerdictClose}
					}
					return zmux.Decision{Verdict: zmux.VerdictFallback, Value: "echo"}
				}}
			returned := make(chan zmux.PipeIO, 1)
			lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm,
				Fallback: func(_ context.Context, tk *zmux.Takeover) {
					if tk.Value != "echo" || tk.Info.Request == nil {
						t.Errorf("takeover value %v", tk.Value)
						return
					}
					if err := tk.Detach(); err != nil {
						t.Error(err)
						return
					}
					returned <- tk.Pipe()
				}})
			defer func() { _ = lst.Close() }()
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
			h, err := NewHandler(lst, &Options{Next: next})
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewUnstartedServer(h)
			var sp, cp http.Protocols
			sp.SetHTTP1(true)
			sp.SetUnencryptedHTTP2(true)
			if h2 {
				cp.SetUnencryptedHTTP2(true)
			} else {
				cp.SetHTTP1(true)
			}
			srv.Config.Protocols = &sp
			srv.Start()
			defer srv.Close()
			client := &http.Client{Transport: &http.Transport{Protocols: &cp}}
			defer client.CloseIdleConnections()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			pr, pw := io.Pipe()
			defer func() { _ = pw.Close() }()
			go func() { _, _ = pw.Write([]byte("HOSTping")) }()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, pr)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK || (resp.ProtoMajor == 2) != h2 {
				t.Fatalf("status %d, proto %s", resp.StatusCode, resp.Proto)
			}
			var p zmux.PipeIO
			select {
			case p = <-returned:
			case <-ctx.Done():
				t.Fatal("no takeover")
			}
			if half {
				halfClose(t, p, pw, resp.Body)
			} else {
				echoed(t, p, pw, resp.Body)
			}
			if st := lst.Stats().Admission; st.Fallback != 1 {
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

// echoed: the host echoes the body, the bytes read replayed first, while
// the client still streams it; the body's end ends the copy, whose Close
// ends the response.
func echoed(t *testing.T, p zmux.PipeIO, pw *io.PipeWriter, resp io.Reader) {
	t.Helper()
	go func() {
		defer zmux.ClosePipeIO(p)
		_, _ = io.Copy(p.W, p.R)
	}()
	want := []byte("HOSTping")
	got := make([]byte, len(want))
	if _, err := io.ReadFull(resp, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("echo %q, %v", got, err)
	}
	if _, err := pw.Write([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(resp, got[:4]); err != nil || string(got[:4]) != "pong" {
		t.Fatalf("echo %q, %v", got[:4], err)
	}
	_ = pw.Close()
	if rest, err := io.ReadAll(resp); err != nil || len(rest) != 0 {
		t.Fatalf("after the end: %q, %v", rest, err)
	}
}

// halfClose: the host answers, closes the response's half and reads the
// rest of the body to its end: the request stays served until it closes
// the body too.
func halfClose(t *testing.T, p zmux.PipeIO, pw *io.PipeWriter, resp io.Reader) {
	t.Helper()
	rest := make(chan string, 1)
	go func() {
		defer func() { _ = p.R.Close() }()
		head := make([]byte, 8)
		if _, err := io.ReadFull(p.R, head); err != nil {
			rest <- "head: " + err.Error()
			return
		}
		_, _ = p.W.Write([]byte("ack"))
		_ = p.W.Close()
		b, err := io.ReadAll(p.R)
		if err != nil {
			rest <- "rest: " + err.Error()
			return
		}
		rest <- string(b)
	}()
	got := make([]byte, 3)
	if _, err := io.ReadFull(resp, got); err != nil || string(got) != "ack" {
		t.Fatalf("answer %q, %v", got, err)
	}
	if _, err := pw.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	_ = pw.Close()
	if r := <-rest; r != "tail" {
		t.Fatalf("body after the response's half closed: %q", r)
	}
	if b, err := io.ReadAll(resp); err != nil || len(b) != 0 {
		t.Fatalf("after the end: %q, %v", b, err)
	}
}
