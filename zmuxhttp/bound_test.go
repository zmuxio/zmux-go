package zmuxhttp

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxtest"
)

func boundServerCfg() *zmux.Config {
	c := mpConfig()
	c.AllowRecordMode = func(in zmux.RecordPolicyInput, _ zmux.RecordMode, _ any) bool { return in.Secure && in.HasExporter }
	return c
}

// boundPipe starts a session over a bidirectional carrier to srv and adds a
// second pipe asking for Bound; it returns the second pipe's mode or error.
func boundPipe(t *testing.T, client *http.Client, url string, lst *zmux.Listener) (zmux.RecordMode, error) {
	t.Helper()
	return boundPipeURLs(t, client, url, url, lst)
}

// boundPipeURLs is boundPipe with the first carrier at url and the joining
// one at joinURL.
func boundPipeURLs(t *testing.T, client *http.Client, url, joinURL string, lst *zmux.Listener) (zmux.RecordMode, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := Streaming{Client: client, URL: url}.Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	q, err := Streaming{Client: client, URL: joinURL}.Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	pp, err := c.AddPipe(ctx, q, zmux.PipeOptions{RecordMode: zmux.Set(zmux.RecordBound)})
	if err != nil {
		_ = q.W.Close()
		return 0, err
	}
	st, ok := pp.Stats()
	if !ok {
		t.Fatal("no pipe stats")
	}
	echoOnce(t, ctx, c, s)
	return st.RecordMode, nil
}

// A bidirectional carrier over HTTP/1.1 or HTTP/2 on TLS that both ends
// terminate joins in Bound.
func TestBoundOverTLS(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		name := "http1"
		if h2 {
			name = "http2"
		}
		t.Run(name, func(t *testing.T) {
			lst := mustNewListener(&zmux.ListenerConfig{Config: boundServerCfg()})
			defer func() { _ = lst.Close() }()
			hh, err := NewHandler(lst, nil)
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewUnstartedServer(hh)
			srv.EnableHTTP2 = h2
			srv.StartTLS()
			defer srv.Close()
			client := srv.Client()
			defer client.CloseIdleConnections()
			m, err := boundPipe(t, client, srv.URL, lst)
			if err != nil || m != zmux.RecordBound {
				t.Fatalf("record mode %v: %v", m, err)
			}
		})
	}
}

// Behind a front that terminates TLS (here h2c to the server) the server
// has no TLS of its own: the pipe is not Bound.
func TestBoundNotBehindTLSFront(t *testing.T) {
	lst := mustNewListener(&zmux.ListenerConfig{Config: boundServerCfg()})
	defer func() { _ = lst.Close() }()
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv := httptest.NewUnstartedServer(hh)
	srv.Config.Protocols = &p
	srv.Start()
	defer srv.Close()
	var cp http.Protocols
	cp.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: &cp}}
	defer client.CloseIdleConnections()
	if m, err := boundPipe(t, client, srv.URL, lst); err == nil && m == zmux.RecordBound {
		t.Fatal("Bound without TLS terminated here")
	}
}

type fakeStater struct{ version uint16 }

func (f fakeStater) ConnectionState() tls.ConnectionState {
	return tls.ConnectionState{Version: f.version}
}

// The secure connection belongs to the current hop's request body: a hop
// a redirect starts without one (a plaintext hop) is not secure, and a
// later secure hop's connection replaces the earlier one.
func TestCarrierSecurePerHop(t *testing.T) {
	call := &httpCall{done: make(chan struct{}), up: func() {}}
	s := newUpStream()
	w := &httpReqWriter{p: s.p, call: call}
	hop := func() *httpUpBody {
		b := &httpUpBody{s: s, call: call, closed: make(chan struct{}), wrote: make(chan struct{}), gone: make(chan struct{})}
		s.setLive(b)
		call.cur.Store(b)
		return b
	}
	if w.CarrierSecure() {
		t.Fatal("secure before any hop")
	}
	b1 := hop()
	b1.SetSecureConnection(fakeStater{1})
	if !w.CarrierSecure() || w.ConnectionState().Version != 1 {
		t.Fatal("first hop's connection not used")
	}
	b2 := hop() // redirected to a hop that declares nothing
	if w.CarrierSecure() || w.ConnectionState().Version != 0 {
		t.Fatal("secure after a hop without a connection")
	}
	b1.SetSecureConnection(fakeStater{1}) // late, from the replaced hop
	if w.CarrierSecure() {
		t.Fatal("replaced hop declared the carrier secure")
	}
	b2.SetSecureConnection(fakeStater{2})
	b3 := hop() // a secure hop on another connection
	b3.SetSecureConnection(fakeStater{3})
	if !w.CarrierSecure() || w.ConnectionState().Version != 3 {
		t.Fatal("state does not follow the new connection")
	}
}

// A plain http hop that redirects to TLS ends up Bound on the TLS hop's
// connection; a TLS hop redirecting to plain http is refused (downgrade),
// so no plaintext hop ever carries a secure carrier.
func TestBoundAcrossRedirects(t *testing.T) {
	lst := mustNewListener(&zmux.ListenerConfig{Config: boundServerCfg()})
	defer func() { _ = lst.Close() }()
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	tlsSrv := httptest.NewTLSServer(hh)
	defer tlsSrv.Close()
	client := tlsSrv.Client()
	defer client.CloseIdleConnections()
	plain := httptest.NewServer(hh)
	defer plain.Close()
	up := zmuxtest.New("http://unused.invalid", zmuxtest.RedirectToHTTPS(tlsSrv.URL, http.StatusTemporaryRedirect))
	defer up.Close()
	if m, err := boundPipeURLs(t, client, up.URL, tlsSrv.URL, lst); err != nil || m != zmux.RecordBound {
		t.Fatalf("bound after a plain hop redirected to TLS: %v %v", m, err)
	}
	down := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}))
	defer down.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p, err := Streaming{Client: down.Client(), URL: down.URL}.Pipe(ctx, zmux.DirectionBidirectional)
	if err == nil {
		var c *zmux.Conn
		if c, err = zmux.Start(ctx, p, zmux.RoleClient, mpConfig()); err == nil {
			_ = c.Close()
		}
	}
	if err == nil {
		t.Fatal("a downgrade redirect was followed")
	}
}
