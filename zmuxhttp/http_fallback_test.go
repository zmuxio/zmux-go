package zmuxhttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
		body   []byte
		h      http.Header
	}{
		{"garbage first record", http.MethodPost, garbage, nil},
		{"incomplete first record", http.MethodPost, []byte{0x40}, nil},
		{"small first record", http.MethodPost, small, nil},
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
				Decide: func(context.Context, *zmux.CarrierInfo, []byte) zmux.Decision { return zmux.Decision{Verdict: x.v} }})
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
		Decide: func(context.Context, *zmux.CarrierInfo, []byte) zmux.Decision {
			return zmux.Decision{Verdict: zmux.VerdictFallback}
		}}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: adm})
	defer func() { _ = lst.Close() }()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
