package zmuxh3

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djylb/timeshim"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// net/http reads its Extended CONNECT switch at process initialization.
// HS-14.
func TestHTTP2UpgradeFallbackReadInterrupted(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if !strings.Contains(os.Getenv("GODEBUG"), "http2xconnect=1") {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHTTP2UpgradeFallbackReadInterrupted$", "-test.timeout=18s")
		cmd.Env = append(os.Environ(), "GODEBUG="+os.Getenv("GODEBUG")+",http2xconnect=1")
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v", b, err)
		}
		return
	}
	for _, shutdown := range []bool{false, true} {
		l := mustNewListener(&zmux.ListenerConfig{Admission: &zmux.Admission{Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
			return zmux.Decision{Verdict: zmux.VerdictFallback}
		}}})
		entered, done := make(chan struct{}), make(chan struct{})
		next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor != 2 || r.Header.Get(":protocol") != "websocket" {
				t.Error("missing extended CONNECT")
			}
			close(entered)
			_, err := io.Copy(io.Discard, r.Body)
			if err == nil || r.Context().Err() == nil {
				t.Errorf("read %v, context %v", err, r.Context().Err())
			}
		})
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(done)
			l.Adapter().AdmitUpgrade(w, r, nil, next)
		}))
		var protocols http.Protocols
		protocols.SetUnencryptedHTTP2(true)
		srv.Config.Protocols = &protocols
		srv.Start()
		nc, err := net.DialTimeout("tcp", srv.Listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = nc.Close(); srv.Close(); _ = l.Close() })
		_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
		fr := http2.NewFramer(nc, nc)
		if _, err = io.WriteString(nc, http2.ClientPreface); err != nil {
			t.Fatal(err)
		}
		if err = fr.WriteSettings(); err != nil {
			t.Fatal(err)
		}
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		settings, ok := f.(*http2.SettingsFrame)
		if !ok {
			t.Fatalf("first frame %T", f)
		}
		if v, ok := settings.Value(http2.SettingEnableConnectProtocol); !ok || v != 1 {
			t.Fatal("server disabled extended CONNECT")
		}
		if err = fr.WriteSettingsAck(); err != nil {
			t.Fatal(err)
		}
		var header bytes.Buffer
		enc := hpack.NewEncoder(&header)
		for _, field := range []hpack.HeaderField{{Name: ":method", Value: "CONNECT"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: srv.Listener.Addr().String()}, {Name: ":path", Value: "/"}, {Name: ":protocol", Value: "websocket"}} {
			if err := enc.WriteField(field); err != nil {
				t.Fatal(err)
			}
		}
		if err = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: header.Bytes(), EndHeaders: true}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if shutdown {
			err = l.Shutdown(ctx, 0, "stop")
		} else {
			err = l.Close()
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("Next remained in Body.Read")
		}
		_ = nc.Close()
		srv.Close()
	}
}

// HTTP/2 and HTTP/3 multiplex the two streaming requests on one connection.
// HTTP/1 needs a second connection while the first response is streaming.
// HS-16.
func TestStreamingTakeoverWriteInterrupt(t *testing.T) {
	for _, proto := range []string{"h1", "h2", "h3"} {
		for _, end := range []string{"listener", "client", "deadline"} {
			for _, phase := range []string{"header", "flush"} {
				t.Run(proto+"/"+end+"/"+phase, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
					defer cancel()
					clk := timeshim.NewFake(time.Now())
					l := mustNewListener(&zmux.ListenerConfig{Config: &zmux.Config{Clock: clk, HandshakeTimeout: time.Second}, MaxPending: 1,
						Admission: &zmux.Admission{Peek: 5, Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
							return zmux.Decision{Verdict: zmux.VerdictTakeover}
						}},
						Fallback: func(_ context.Context, take *zmux.Takeover) {
							if take.Info.Request.Header.Get("X-Gate") != "" {
								t.Error("handoff after interrupted response")
								return
							}
							p := take.Pipe()
							_, _ = io.Copy(p.W, p.R)
						},
					})
					defer func() { _ = l.Close() }()
					inner, err := zmuxhttp.NewHandler(l, nil)
					if err != nil {
						t.Fatal(err)
					}
					gate := &interruptWriteGate{phase: phase, entered: make(chan struct{}), interrupted: make(chan struct{}), release: make(chan struct{})}
					release := sync.OnceFunc(func() { close(gate.release) })
					defer release()
					live := make(chan string, 1)
					waiting := make(chan string, 1)
					exited := make(chan struct{})
					handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Header.Get("X-Gate") == "" {
							live <- r.RemoteAddr
							inner.ServeHTTP(w, r)
							return
						}
						defer close(exited)
						waiting <- r.RemoteAddr
						gate.ResponseWriter = w
						inner.ServeHTTP(gate, r)
					})
					var client *http.Client
					var target string
					if proto == "h3" {
						srvTLS, cliTLS := tlsConfigs(t)
						pc, err := net.ListenPacket("udp", "127.0.0.1:0")
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = pc.Close() }()
						srv := NewServer("", srvTLS, nil, handler, nil)
						go func() { _ = srv.Serve(pc) }()
						defer func() { _ = srv.Close() }()
						var tr *http3.Transport
						client, tr = HTTPClient(cliTLS, nil)
						defer func() { _ = tr.Close() }()
						target = "https://" + pc.LocalAddr().String() + "/"
					} else {
						srv := httptest.NewUnstartedServer(handler)
						if proto == "h2" {
							srv.EnableHTTP2 = true
							srv.StartTLS()
						} else {
							srv.Start()
						}
						defer srv.Close()
						client, target = srv.Client(), srv.URL
					}
					// Teardown the clients and gates before waiting on server handlers.
					defer cancel()
					defer release()
					pr, pw := io.Pipe()
					defer func() { _ = pw.Close() }()
					req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, pr)
					if err != nil {
						t.Fatal(err)
					}
					go func() { _, _ = pw.Write([]byte("ready")) }()
					resp, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = resp.Body.Close() }()
					var buf [5]byte
					if _, err = io.ReadFull(resp.Body, buf[:]); err != nil {
						t.Fatal(err)
					}
					next, stop := context.WithCancel(ctx)
					defer stop()
					body, send := io.Pipe()
					defer func() { _ = send.Close() }()
					req, err = http.NewRequestWithContext(next, http.MethodPost, target, body)
					if err != nil {
						t.Fatal(err)
					}
					req.Header.Set("X-Gate", phase)
					go func() { _, _ = send.Write([]byte("wait")); _ = send.Close() }()
					done := make(chan struct{})
					go func() {
						defer close(done)
						r, _ := client.Do(req)
						if r != nil {
							_ = r.Body.Close()
						}
					}()
					select {
					case <-gate.entered:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if first, second := <-live, <-waiting; proto != "h1" && first != second {
						t.Fatal("expected multiplexed requests")
					}
					switch end {
					case "listener":
						_ = l.Close()
					case "deadline":
						clk.Advance(time.Second)
					case "client":
						stop()
					}
					select {
					case <-gate.interrupted:
					case <-ctx.Done():
						t.Fatal("no write interrupt")
					}
					release()
					select {
					case <-exited:
					case <-ctx.Done():
						t.Fatal("handler did not exit")
					}
					stop()
					_ = send.Close()
					<-done
					go func() { _, _ = pw.Write([]byte("alive")) }()
					if _, err := io.ReadFull(resp.Body, buf[:]); err != nil || string(buf[:]) != "alive" {
						t.Fatalf("other request %q, %v", buf, err)
					}
				})
			}
		}
	}
}

type interruptWriteGate struct {
	http.ResponseWriter
	entered, interrupted, release chan struct{}
	phase                         string
}

func (w *interruptWriteGate) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *interruptWriteGate) WriteHeader(code int) {
	if w.phase == "header" {
		close(w.entered)
		<-w.release
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *interruptWriteGate) FlushError() error {
	if w.phase == "flush" {
		close(w.entered)
		<-w.release
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}
func (w *interruptWriteGate) SetWriteDeadline(d time.Time) error {
	close(w.interrupted)
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(d)
}
