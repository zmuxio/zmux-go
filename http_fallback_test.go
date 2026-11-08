package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// refusedEnv is a carrier handler whose fallback records the body of every
// request it serves.
type refusedEnv struct {
	lst  *Listener
	srv  *httptest.Server
	mu   sync.Mutex
	host [][]byte
}

func newRefusedEnv(t *testing.T, adm *Admission) *refusedEnv {
	t.Helper()
	e := &refusedEnv{lst: mustNewListener(&ListenerConfig{Config: mpConfig(), FirstRecordTimeout: time.Second, Admission: adm})}
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
	h, err := NewHTTPHandler(e.lst, &HTTPOptions{Next: next})
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
	other := mustNewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = other.Close() }()
	osrv := httptest.NewServer(mustHTTPHandler(t, other))
	defer osrv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := HTTPPipe(ctx, osrv.Client(), osrv.URL, nil, DirBidirectional, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Start(ctx, p, RoleClient, mpConfig())
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
		v    Verdict
		want int
	}{
		{"accept", VerdictAccept, http.StatusOK},
		{"close", VerdictClose, 0},
		{"stall", VerdictStall, 0},
	} {
		t.Run(x.name, func(t *testing.T) {
			e := newRefusedEnv(t, &Admission{Peek: 8, PeekTimeout: time.Second,
				StallMin: 10 * time.Millisecond, StallMax: 20 * time.Millisecond,
				Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: x.v} }})
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

func mustHTTPHandler(t *testing.T, l *Listener) http.Handler {
	t.Helper()
	h, err := NewHTTPHandler(l, nil)
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
	adm := &Admission{Peek: 4, PeekTimeout: time.Second,
		Decide: func(context.Context, *CarrierInfo, []byte) Decision { return Decision{Verdict: VerdictFallback} }}
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: adm})
	defer func() { _ = lst.Close() }()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	h, err := NewHTTPHandler(lst, &HTTPOptions{Next: next})
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
