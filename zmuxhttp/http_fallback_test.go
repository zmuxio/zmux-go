package zmuxhttp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// refusedEnv is a carrier handler whose fallback records the body of every
// request it serves.
type refusedEnv struct {
	lst  *zmux.Listener
	srv  *httptest.Server
	host [][]byte
	mu   sync.Mutex
}

func newRefusedEnv(t *testing.T, adm *zmux.Admission) *refusedEnv {
	t.Helper()
	e := &refusedEnv{lst: mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), FirstRecordTimeout: time.Second, Admission: adm})}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			b = append(b, "<read error>"...)
		}
		e.mu.Lock()
		e.host = append(e.host, b)
		e.mu.Unlock()
		http.Error(w, "host", http.StatusTeapot)
	})
	h, err := NewHandler(e.lst, &Options{Next: next})
	if err != nil {
		t.Fatal(err)
	}
	e.srv = httptest.NewServer(h)
	return e
}

func (e *refusedEnv) close() {
	e.srv.Client().Transport.(*http.Transport).CloseIdleConnections()
	e.srv.Close()
	_ = e.lst.Close()
}

// served returns, and forgets, the bodies the fallback was given.
func (e *refusedEnv) served() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	h := e.host
	e.host = nil
	return h
}

// send makes a request with a streamed body (no Content-Length, so that a
// POST is a stream carrier) and returns the response status (0: the
// request failed).
func (e *refusedEnv) send(t *testing.T, method string, body []byte, h http.Header) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var rd io.Reader
	if body != nil {
		rd = io.MultiReader(bytes.NewReader(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, e.srv.URL, rd)
	if err != nil {
		t.Fatal(err)
	}
	addHeader(req.Header, h)
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// TestHTTPRefusedCarrierFallback sends carrier requests that the listener
// does not take, before anything was written: each reaches the fallback
// with every body byte, those the listener read included.
func TestHTTPRefusedCarrierFallback(t *testing.T) {
	leakcheck.Check(t)
	e := newRefusedEnv(t, nil)
	defer e.close()

	// A join naming a session of another listener.
	other := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	defer func() { _ = other.Close() }()
	osrv := httptest.NewServer(mustHTTPHandler(t, other))
	defer osrv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := Streaming{Client: osrv.Client(), URL: osrv.URL}.Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := other.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = c.Close()
		_ = s.Close()
		waitDone(t, c)
		waitDone(t, s)
		osrv.Client().Transport.(*http.Transport).CloseIdleConnections()
	}()
	garbage := make([]byte, 200<<10)
	_, _ = rand.Read(garbage)
	garbage[0] = 0xff // a record length beyond MaxRecordSize
	small := []byte("hello, host")
	for _, x := range []struct {
		name   string
		method string
		h      http.Header
		body   []byte
	}{
		{name: "garbage first record", method: http.MethodPost, body: garbage, h: nil},
		{name: "incomplete first record", method: http.MethodPost, body: []byte{0x40}, h: nil},
		{name: "small first record", method: http.MethodPost, body: small, h: nil},
	} {
		if code := e.send(t, x.method, x.body, x.h); code != http.StatusTeapot {
			t.Fatalf("%s: status %d, want the fallback's", x.name, code)
		}
		got := e.served()
		if len(got) != 1 || !bytes.Equal(got[0], x.body) && (x.body != nil || len(got[0]) != 0) {
			t.Fatalf("%s: the fallback got %q, want 1 request with the %d-byte body", x.name, got, len(x.body))
		}
	}

	// The listener closed.
	_ = e.lst.Close()
	if code := e.send(t, http.MethodPost, garbage, nil); code != http.StatusTeapot {
		t.Fatalf("closed listener: status %d", code)
	}
	if got := e.served(); len(got) != 1 || !bytes.Equal(got[0], garbage) {
		t.Fatalf("closed listener: the fallback got %d requests", len(got))
	}
}

// TestHTTPAdmittedCarrierNotHandedOn: a carrier whose response head went
// out on an admission's acceptance, and one the admission closes or
// stalls, never reach the fallback.
func TestHTTPAdmittedCarrierNotHandedOn(t *testing.T) {
	leakcheck.Check(t)
	garbage := bytes.Repeat([]byte{0xff}, 4096)
	for _, x := range []struct {
		name string
		v    zmux.AdmissionVerdict
		want int
	}{
		{"accept", zmux.VerdictAccept, http.StatusOK},
		{"close", zmux.VerdictClose, 0},
		{"stall", zmux.VerdictStall, 0},
	} {
		t.Run(x.name, func(t *testing.T) {
			e := newRefusedEnv(t, &zmux.Admission{Peek: 8, PeekTimeout: time.Second,
				StallMin: 10 * time.Millisecond, StallMax: 20 * time.Millisecond,
				Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision { return zmux.Decision{Verdict: x.v} }})
			defer e.close()
			if code := e.send(t, http.MethodPost, garbage, nil); code != x.want {
				t.Fatalf("status %d, want %d", code, x.want)
			}
			if got := e.served(); len(got) != 0 {
				t.Fatalf("the fallback got %d requests", len(got))
			}
		})
	}
}

func mustHTTPHandler(t *testing.T, l *zmux.Listener) http.Handler {
	t.Helper()
	h, err := NewHandler(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestHTTPRejectedStreamFallbackFullDuplex sends an HTTP/1.1 stream
// carrier whose body stays open until the response arrives: the admission
// rejects it to the fallback, whose response must reach the client while
// the body is still being streamed.
func TestHTTPRejectedStreamFallbackFullDuplex(t *testing.T) {
	leakcheck.Check(t)
	adm := &zmux.Admission{Peek: 4, PeekTimeout: time.Second,
		Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
			return zmux.Decision{Verdict: zmux.VerdictFallback}
		}}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm})
	defer func() { _ = lst.Close() }()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	h, err := NewHandler(lst, &Options{Next: next})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	defer srv.Client().Transport.(*http.Transport).CloseIdleConnections()

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	go func() { _, _ = pw.Write([]byte("abcdefgh")) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, pr)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("no fallback response while the body streams: %v", err)
	}
	_ = pw.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status %d, want the fallback's", resp.StatusCode)
	}
}

// A refusal over HTTP/2 reaches the carrier while the transport still
// waits for the request body: the response body's Close waits for the
// body's writer, which the transport's Close of the request body ends.
// A redirect's response body is closed the same way.
func TestHTTP2RefusalWhileUplinkIdle(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/moved", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/zmux")
		w.WriteHeader(http.StatusTemporaryRedirect)
		_, _ = io.WriteString(w, "moved")
	})
	mux.HandleFunc("/zmux", func(w http.ResponseWriter, r *http.Request) {
		// Refuse once the uplink's first bytes arrived, as a carrier
		// handler refuses on its prefix.
		_, _ = io.ReadFull(r.Body, make([]byte, len("preface")))
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "refused") // a body the client leaves unread
	})
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv := httptest.NewUnstartedServer(mux)
	srv.Config.Protocols = &p
	srv.Start()
	defer srv.Close()
	var cp http.Protocols
	cp.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: &cp}}
	defer client.CloseIdleConnections()
	for _, path := range []string{"/zmux", "/moved"} {
		t.Run(path, func(t *testing.T) {
			pipe, err := Streaming{Client: client, URL: srv.URL + path}.Candidate().Dial(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = pipe.W.Close() }()
			// The uplink sends, then waits: the transport's next read of
			// the body waits too.
			go func() { _, _ = pipe.W.Write([]byte("preface")) }()
			got := make(chan error, 1)
			go func() {
				_, err := pipe.R.Read(make([]byte, 1))
				got <- err
			}()
			select {
			case err = <-got:
			case <-time.After(5 * time.Second):
				t.Fatal("refusal not reported while the uplink is idle")
			}
			var se *StatusError
			if !errors.As(err, &se) || se.StatusCode != http.StatusTeapot || !errors.Is(err, zmux.ErrCarrierRefused) {
				t.Fatalf("read error %v", err)
			}
		})
	}
}

// The client sends only junk until Next owns the request. In particular
// admission cannot buffer the rest of a chunked upload to decide its target.
// HS-13.
func TestRequestFallbackPrefersNext(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		name := "chunked"
		if fixed {
			name = "content-length"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const prefix, tail = "junk", "-unread-upload"
			entered := make(chan struct{})
			l := mustNewListener(&zmux.ListenerConfig{
				MaxPending: 1,
				Admission: &zmux.Admission{Peek: len(prefix), Decide: func(_ context.Context, info zmux.CarrierInfo, b []byte) zmux.Decision {
					if string(b) != prefix || info.Stage != zmux.StageCarrier {
						t.Errorf("admission %q stage %v", b, info.Stage)
					}
					return zmux.Decision{Verdict: zmux.VerdictFallback}
				}},
				Fallback: func(context.Context, *zmux.Takeover) { t.Error("unexpected stream takeover") },
			})
			defer func() { _ = l.Close() }()
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				if r.Header.Get("X-Request-Marker") != "original" || r.URL.RawQuery != "q=keep" {
					t.Error("request metadata lost")
				}
				if fixed && r.ContentLength != int64(len(prefix+tail)) || !fixed && (len(r.TransferEncoding) != 1 || r.TransferEncoding[0] != "chunked") {
					t.Errorf("request framing: %d %v", r.ContentLength, r.TransferEncoding)
				}
				a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
				if err != nil {
					t.Errorf("slot held in Next: %v", err)
				} else {
					a.Release()
				}
				body, err := io.ReadAll(r.Body)
				if err != nil || string(body) != prefix+tail {
					t.Errorf("replayed body %q: %v", body, err)
				}
				if r.Context().Err() != nil {
					t.Error("request cancelled during handoff")
				}
				w.Header().Set("X-Handler", "next")
				w.WriteHeader(http.StatusTeapot)
				_, _ = io.WriteString(w, "from Next")
			})
			h, err := NewHandler(l, &Options{Next: next})
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(h)
			defer srv.Close()
			pr, pw := io.Pipe()
			defer func() { _ = pw.Close() }()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/upload?q=keep", pr)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Request-Marker", "original")
			if fixed {
				// A fixed-size request carrier is a split uplink. An unpaired
				// fixed-size POST is dispatched to the exchange adapter.
				req.Header.Set(HeaderPair, "AAAAAAAAAAAAAAAAAAAAAA")
				req.ContentLength = int64(len(prefix + tail))
			}
			sent := make(chan error, 1)
			go func() {
				_, err := io.WriteString(pw, prefix)
				if err == nil {
					select {
					case <-entered:
						_, err = io.WriteString(pw, tail)
					case <-ctx.Done():
						err = ctx.Err()
					}
				}
				_ = pw.CloseWithError(err)
				sent <- err
			}()
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != http.StatusTeapot || resp.Header.Get("X-Handler") != "next" || string(body) != "from Next" {
				t.Fatalf("response %d %q: %v", resp.StatusCode, body, err)
			}
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
			if n := l.Stats().Admission.Fallback; n != 1 {
				t.Fatalf("fallback decisions %d", n)
			}
		})
	}
}

// HS-14.
func TestRequestFallbackCancellation(t *testing.T) {
	for _, source := range []string{"listener", "client"} {
		t.Run(source, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			entered := make(chan struct{})
			ended := make(chan error, 1)
			l := mustNewListener(&zmux.ListenerConfig{MaxPending: 1, Admission: &zmux.Admission{Peek: 1, Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
				return zmux.Decision{Verdict: zmux.VerdictFallback}
			}}})
			defer func() { _ = l.Close() }()
			next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				close(entered)
				_, _ = io.Copy(io.Discard, r.Body)
				select {
				case <-r.Context().Done():
					ended <- r.Context().Err()
				case <-ctx.Done():
					ended <- ctx.Err()
				}
			})
			h, err := NewHandler(l, &Options{Next: next})
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(h)
			defer srv.Close()
			pr, pw := io.Pipe()
			defer func() { _ = pw.Close() }()
			reqCtx, stop := context.WithCancel(ctx)
			defer stop()
			req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, srv.URL, pr)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if resp, err := srv.Client().Do(req); err == nil {
					_ = resp.Body.Close()
				}
			}()
			if _, err = io.WriteString(pw, "junk"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
			if err != nil {
				t.Fatal(err)
			}
			a.Release()
			if source == "listener" {
				_ = l.Close()
			} else {
				stop()
			}
			select {
			case err := <-ended:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("Next retained request")
			}
			stop()
			_ = pw.Close()
			<-done
		})
	}
}

// HS-13.
func TestFallbackChunkedTrailers(t *testing.T) {
	for _, declare := range []bool{false, true} {
		for _, expect := range []bool{false, true} {
			for _, peek := range []int{1, 4, 8} {
				t.Run(fmt.Sprintf("declared=%t/expect=%t/peek=%d", declare, expect, peek), func(t *testing.T) {
					l := mustNewListener(&zmux.ListenerConfig{Admission: &zmux.Admission{Peek: peek, Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
						return zmux.Decision{Verdict: zmux.VerdictFallback}
					}}})
					defer func() { _ = l.Close() }()
					h, err := NewHandler(l, &Options{Next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						b, err := io.ReadAll(r.Body)
						if err != nil || string(b) != "abcd" || r.Trailer.Get("X-Final") != "trail" {
							t.Errorf("body %q trailers %v err %v", b, r.Trailer, err)
						}
						w.WriteHeader(http.StatusTeapot)
					})})
					if err != nil {
						t.Fatal(err)
					}
					srv := httptest.NewServer(h)
					defer srv.Close()
					nc, err := net.DialTimeout("tcp", srv.Listener.Addr().String(), time.Second)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = nc.Close() }()
					_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
					_, _ = fmt.Fprintf(nc, "POST / HTTP/1.1\r\nHost: %s\r\nTransfer-Encoding: chunked\r\n", srv.Listener.Addr())
					if declare {
						_, _ = io.WriteString(nc, "Trailer: X-Final\r\n")
					}
					if expect {
						_, _ = io.WriteString(nc, "Expect: 100-continue\r\n")
					}
					_, _ = io.WriteString(nc, "\r\n")
					reader := bufio.NewReader(nc)
					status := func(want int) {
						res, err := http.ReadResponse(reader, nil)
						if err != nil {
							t.Fatal(err)
						}
						_ = res.Body.Close()
						if res.StatusCode != want {
							t.Fatal(res.Status)
						}
					}
					if expect {
						status(http.StatusContinue)
					}
					_, _ = io.WriteString(nc, "2\r\nab\r\n2\r\ncd\r\n0\r\nX-Final: trail\r\n\r\n")
					status(http.StatusTeapot)
				})
			}
		}
	}
}
