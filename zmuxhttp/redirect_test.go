package zmuxhttp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/zmuxtest"
)

func TestCheckHTTPRedirect(t *testing.T) {
	for _, c := range []struct {
		from, method string
		status       int
		loc, want    string // want "": refused
	}{
		{"http://a.example/z", "POST", 308, "https://a.example/z", "https://a.example/z"},
		{"http://a.example/z", "POST", 301, "https://a.example/z", "https://a.example/z"},
		{"http://a.example/z?n=1", "POST", 302, "https://a.example:8443/z?n=1", "https://a.example:8443/z?n=1"},
		{"http://a.example/z", "POST", 301, "https://a.example/y", ""},
		{"http://a.example/z", "POST", 303, "/y", ""},
		{"http://a.example/z", "POST", 307, "/y", "http://a.example/y"},
		{"https://a.example/z", "POST", 308, "/z/", "https://a.example/z/"},
		{"https://a.example/z", "GET", 301, "/y", "https://a.example/y"},
		{"https://a.example/z", "POST", 307, "http://a.example/z", ""},
		{"http://a.example/z", "POST", 307, "https://b.example/z", ""},
		{"http://a.example/z", "GET", 302, "http://a.example:81/z", ""},
		{"http://a.example/z", "GET", 302, "https://u:p@a.example/z", ""},
		{"http://a.example/z", "GET", 302, "", ""},
		{"ws://a.example/z", "GET", 301, "https://a.example/z", "wss://a.example/z"},
		{"wss://a.example/z", "GET", 301, "http://a.example/z", ""},
		{"http://A.example/z", "GET", 302, "https://a.EXAMPLE/z#f", "https://a.EXAMPLE/z"},
	} {
		from, _ := url.Parse(c.from)
		got, err := CheckRedirect(from, c.method, c.status, c.loc)
		var re *RedirectError
		switch {
		case c.want == "" && !errors.As(err, &re):
			t.Errorf("%s %s %d %q: %v, %v; want refused", c.method, c.from, c.status, c.loc, got, err)
		case c.want != "" && (err != nil || got.String() != c.want):
			t.Errorf("%s %s %d %q: %v, %v; want %s", c.method, c.from, c.status, c.loc, got, err, c.want)
		}
	}
}

// redirectEnv is a unified endpoint behind a TLS front, and a client that
// trusts the front.
type redirectEnv struct {
	*modeEnv
	front *zmuxtest.Proxy // https
}

func newRedirectEnv(t *testing.T) *redirectEnv {
	e := &redirectEnv{modeEnv: newModeEnv(t)}
	e.front = zmuxtest.New(e.srv.URL, zmuxtest.Profile{TLS: true})
	e.tr.TLSClientConfig = e.front.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	return e
}

func (e *redirectEnv) close() {
	e.front.Close()
	e.modeEnv.close()
}

func (e *redirectEnv) candidate(k zmux.CarrierKind, url string) zmux.Candidate {
	for _, c := range e.candidates(url) {
		if c.Kind == k {
			return c
		}
	}
	panic(k)
}

// TestHTTPRedirectUpgrade dials each HTTP kind at a front that redirects
// every request to https: the carriers follow it, with every status for a
// pure scheme upgrade, and the candidate starts at the final URL from then
// on.
func TestHTTPRedirectUpgrade(t *testing.T) {
	leakcheck.Check(t)
	e := newRedirectEnv(t)
	defer e.close()
	for _, st := range []int{301, 302, 303, 307, 308} {
		for _, k := range []zmux.CarrierKind{zmux.CarrierStream, zmux.CarrierSplit, zmux.CarrierExchange} {
			t.Run(http.StatusText(st)+"/"+k.String(), func(t *testing.T) {
				rd := zmuxtest.New("http://unused.invalid", zmuxtest.RedirectToHTTPS(e.front.URL, st))
				defer rd.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cand := e.candidate(k, rd.URL+"/zmux")
				conn, err := zmux.DialChoice(ctx, zmux.ExplicitChoice(cand), modeCfg(), nil)
				if err != nil {
					t.Fatal(err)
				}
				s := <-e.sessions
				echoOnce(t, ctx, conn, s)
				closeConn(t, conn)
				_ = s.Close()
				if got, want := cand.Target.URL(), e.front.URL+"/zmux"; got != want {
					t.Fatalf("target %s, want %s", got, want)
				}
				if rd.Redirects() == 0 {
					t.Fatal("no redirect")
				}
				// The next carrier of the candidate goes to https at once.
				n := rd.Redirects()
				conn, err = zmux.DialChoice(ctx, zmux.ExplicitChoice(cand), modeCfg(), nil)
				if err != nil {
					t.Fatal(err)
				}
				s = <-e.sessions
				echoOnce(t, ctx, conn, s)
				closeConn(t, conn)
				_ = s.Close()
				if rd.Redirects() != n {
					t.Fatalf("redirects %d after %d", rd.Redirects(), n)
				}
			})
		}
	}
}

// TestHTTPRedirectRefused refuses a redirect to another host (which gets
// no request), a downgrade, and a 301 of a POST to another path, as a
// refused candidate; a 307 to another path is followed.
func TestHTTPRedirectRefused(t *testing.T) {
	leakcheck.Check(t)
	e := newRedirectEnv(t)
	defer e.close()
	var other atomic.Int64
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		other.Add(1)
		http.Error(w, "no", http.StatusTeapot)
	}))
	defer elsewhere.Close()
	otherHost := strings.Replace(elsewhere.URL, "127.0.0.1", "localhost", 1)
	downgrade := zmuxtest.New("http://unused.invalid", zmuxtest.Profile{TLS: true, RedirectTo: e.srv.URL})
	defer downgrade.Close()
	moved := zmuxtest.New("http://unused.invalid", zmuxtest.Profile{RedirectTo: e.front.URL + "/moved", RedirectStatus: 301})
	defer moved.Close()
	cross := zmuxtest.New("http://unused.invalid", zmuxtest.Profile{RedirectTo: otherHost})
	defer cross.Close()
	for _, c := range []struct {
		name, url string
		kinds     []zmux.CarrierKind
	}{
		{"cross-host", cross.URL, []zmux.CarrierKind{zmux.CarrierStream, zmux.CarrierSplit, zmux.CarrierExchange}},
		{"downgrade", downgrade.URL, []zmux.CarrierKind{zmux.CarrierStream, zmux.CarrierSplit, zmux.CarrierExchange}},
		// A split bootstrap's GET follows the 301; its POST does not.
		{"301 POST moved", moved.URL, []zmux.CarrierKind{zmux.CarrierStream, zmux.CarrierSplit, zmux.CarrierExchange}},
	} {
		for _, k := range c.kinds {
			t.Run(c.name+"/"+k.String(), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := zmux.DialChoice(ctx, zmux.ExplicitChoice(e.candidate(k, c.url)), modeCfg(), nil)
				var me *zmux.CarrierChoiceError
				var re *RedirectError
				if !errors.As(err, &me) || me.Results[0].Outcome != zmux.CandidateRefused || !errors.As(err, &re) {
					t.Fatalf("got %v", err)
				}
			})
		}
	}
	if n := other.Load(); n != 0 {
		t.Fatalf("the other host got %d requests", n)
	}
	moved307 := zmuxtest.New("http://unused.invalid", zmuxtest.Profile{RedirectTo: e.front.URL + "/moved", RedirectStatus: 307})
	defer moved307.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cand := e.candidate(zmux.CarrierStream, moved307.URL+"/zmux")
	conn, err := zmux.DialChoice(ctx, zmux.ExplicitChoice(cand), modeCfg(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := <-e.sessions
	echoOnce(t, ctx, conn, s)
	closeConn(t, conn)
	_ = s.Close()
	if got, want := cand.Target.URL(), e.front.URL+"/moved/zmux"; got != want {
		t.Fatalf("target %s, want %s", got, want)
	}
}

// TestHTTPRedirectReplay redirects a bidirectional carrier after reading
// part of its body: the bytes read are sent again at the new URL, none
// lost or doubled, and a redirect loop ends at the bound.
func TestHTTPRedirectReplay(t *testing.T) {
	leakcheck.Check(t)
	e := newRedirectEnv(t)
	defer e.close()
	var hits atomic.Int64
	partial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/loop" {
			hits.Add(1)
			// Without a body: a server that closes with the unread
			// request body may reset the connection before the client
			// reads the response.
			w.Header().Set("Location", "/loop")
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		var b [9]byte
		_, _ = io.ReadFull(r.Body, b[:]) // part of the preface
		w.Header().Set("Location", e.front.URL+r.URL.RequestURI())
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusPermanentRedirect)
	}))
	defer partial.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := zmux.DialChoice(ctx, zmux.ExplicitChoice(e.candidate(zmux.CarrierStream, partial.URL+"/zmux")), modeCfg(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := <-e.sessions
	echoOnce(t, ctx, conn, s)
	closeConn(t, conn)
	_ = s.Close()

	_, err = zmux.DialChoice(ctx, zmux.ExplicitChoice(e.candidate(zmux.CarrierStream, partial.URL+"/loop")), modeCfg(), nil)
	var re *RedirectError
	if !errors.As(err, &re) || hits.Load() != DefaultRedirects+1 {
		t.Fatalf("loop: %v after %d requests", err, hits.Load())
	}
}

// TestDialModeRedirectCache remembers the final URL per server: a later
// dial with the same cache starts there, and a failure there starts the
// kind at its base URL again.
func TestDialModeRedirectCache(t *testing.T) {
	leakcheck.Check(t)
	e := newRedirectEnv(t)
	defer e.close()
	rd := zmuxtest.New("http://unused.invalid", zmuxtest.RedirectToHTTPS(e.front.URL, 308))
	defer rd.Close()
	pol := fastPolicy(true)
	pol.Cache, pol.Key = &zmux.CarrierCache{}, "redirect"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dial := func() {
		t.Helper()
		conn, err := zmux.DialChoice(ctx, zmux.AutoChoice(e.candidates(rd.URL)...), modeCfg(), pol)
		if err != nil {
			t.Fatal(err)
		}
		if k := conn.ConnectionStats().Carrier; k != zmux.CarrierStream {
			t.Fatalf("chose %v", k)
		}
		s := <-e.sessions
		echoOnce(t, ctx, conn, s)
		closeConn(t, conn)
		_ = s.Close()
	}
	dial()
	n := rd.Redirects()
	if n == 0 {
		t.Fatal("no redirect")
	}
	dial() // fresh candidates, the cache's URL
	if rd.Redirects() != n {
		t.Fatalf("redirects %d after %d", rd.Redirects(), n)
	}
}

// TestHTTPRedirectPastReplayBound redirects an uplink after the server
// read more of it than a repeat can send again: the redirect is refused,
// and the writer gets the refusal rather than blocking.
func TestHTTPRedirectPastReplayBound(t *testing.T) {
	leakcheck.Check(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.CopyN(io.Discard, r.Body, 2*upReplayMax)
		w.Header().Set("Location", "/elsewhere")
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := StreamPipe(ctx, srv.Client(), srv.URL, nil, zmux.DirectionSendOnly, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.W.Close() }()
	buf := make([]byte, 4<<10)
	for err == nil {
		_, err = p.W.Write(buf)
	}
	if _, ok := errors.AsType[*RedirectError](err); !ok {
		t.Fatalf("got %v", err)
	}
}
