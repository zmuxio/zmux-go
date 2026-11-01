package zmux

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// responseDate is the Date of the halves' response header, both halves
// agreeing when both have the capability.
func responseDate(t *testing.T, p PipeIO) string {
	t.Helper()
	var d string
	for _, x := range []any{p.R, p.W} {
		rh, ok := x.(ResponseHeaderer)
		if !ok {
			continue
		}
		if h := rh.ResponseHeader(); h != nil && d == "" {
			d = h.Get("Date")
		}
	}
	return d
}

// The client halves of streaming, split and exchange carriers expose the
// header of their response: nil before it, with Date after.
func TestResponseHeader(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lst := NewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	hh, err := NewHTTPHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	exh := newExHandler(t, lst, &HTTPExchangeConfig{PSK: []byte("k")})
	defer func() { _ = exh.Close() }()
	mux := http.NewServeMux()
	mux.Handle("/ex", exh)
	mux.Handle("/", hh)
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	dials := map[string]func() (PipeIO, error){
		"streaming": func() (PipeIO, error) { return HTTPPipe(ctx, srv.Client(), srv.URL, nil, DirBidirectional, nil) },
		"split":     func() (PipeIO, error) { return HTTPSplitPipe(ctx, srv.Client(), srv.URL, nil, nil) },
		"exchange": func() (PipeIO, error) {
			return HTTPExchangePipe(ctx, srv.Client(), srv.URL+"/ex", &HTTPExchangeConfig{PSK: []byte("k")})
		},
	}
	for name, dial := range dials {
		p, err := dial()
		if err != nil {
			t.Fatal(name, err)
		}
		if name == "streaming" {
			if rh := p.R.(ResponseHeaderer); rh.ResponseHeader() != nil {
				t.Fatalf("%s: a header before the response", name)
			}
		}
		c, err := Start(ctx, p, RoleClient, mpConfig())
		if err != nil {
			t.Fatal(name, err)
		}
		s, err := lst.Accept(ctx)
		if err != nil {
			t.Fatal(name, err)
		}
		echoOnce(t, ctx, c, s)
		if d := responseDate(t, p); d == "" {
			t.Fatalf("%s: no Date in the response header", name)
		}
		_ = c.Close()
		_ = s.Close()
		waitDone(t, c)
		waitDone(t, s)
	}
	srv.Client().Transport.(*http.Transport).CloseIdleConnections()
}
