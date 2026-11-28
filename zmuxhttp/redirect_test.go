package zmuxhttp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestCheckHTTPRedirect(t *testing.T) {
	for _, c := range []struct {
		from, method string
		loc, want    string // want "": refused
		status       int
	}{
		{from: "http://a.example/z", method: "POST", status: 308, loc: "https://a.example/z", want: "https://a.example/z"},
		{from: "http://a.example/z", method: "POST", status: 301, loc: "https://a.example/z", want: "https://a.example/z"},
		{from: "http://a.example/z?n=1", method: "POST", status: 302, loc: "https://a.example:8443/z?n=1", want: "https://a.example:8443/z?n=1"},
		{from: "http://a.example/z", method: "POST", status: 301, loc: "https://a.example/y", want: ""},
		{from: "http://a.example/z", method: "POST", status: 303, loc: "/y", want: ""},
		{from: "http://a.example/z", method: "POST", status: 307, loc: "/y", want: "http://a.example/y"},
		{from: "https://a.example/z", method: "POST", status: 308, loc: "/z/", want: "https://a.example/z/"},
		{from: "https://a.example/z", method: "GET", status: 301, loc: "/y", want: "https://a.example/y"},
		{from: "https://a.example/z", method: "POST", status: 307, loc: "http://a.example/z", want: ""},
		{from: "http://a.example/z", method: "POST", status: 307, loc: "https://b.example/z", want: ""},
		{from: "http://a.example/z", method: "GET", status: 302, loc: "http://a.example:81/z", want: ""},
		{from: "http://a.example/z", method: "GET", status: 302, loc: "https://u:p@a.example/z", want: ""},
		{from: "http://a.example/z", method: "GET", status: 302, loc: "", want: ""},
		{from: "ws://a.example/z", method: "GET", status: 301, loc: "https://a.example/z", want: "wss://a.example/z"},
		{from: "wss://a.example/z", method: "GET", status: 301, loc: "http://a.example/z", want: ""},
		{from: "http://A.example/z", method: "GET", status: 302, loc: "https://a.EXAMPLE/z#f", want: "https://a.EXAMPLE/z"},
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
	p, err := Streaming{Client: srv.Client(), URL: srv.URL}.Pipe(ctx, zmux.DirectionSendOnly)
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
