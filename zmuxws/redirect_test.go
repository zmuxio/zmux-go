package zmuxws

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxtest"
)

// TestRedirect follows a redirect of the upgrade from ws to wss on the
// same host, with a credential bound to the new TLS connection, and
// starts the candidate's next carrier at the final URL; a redirect to
// another host or a downgrade is refused and the other host gets nothing.
func TestRedirect(t *testing.T) {
	ctx := testContext(t)
	auth := &zmux.PrefixAuth{Key: []byte("psk"), Slot: 30 * time.Second, Skew: 1, BindTLS: true}
	v, err := zmux.NewPrefixAuthVerifier(auth)
	if err != nil {
		t.Fatal(err)
	}
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(),
		Admission: &zmux.Admission{Peek: zmux.PrefixTokenLen, Decide: v.Decide}})
	h, err := Handler(l, &zmux.HTTPOptions{Next: next})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(h)
	var other atomic.Int64
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		other.Add(1)
		http.Error(w, "no", http.StatusTeapot)
	}))
	t.Cleanup(func() {
		_ = l.Close()
		srv.Close()
		elsewhere.Close()
	})
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	o := &DialOptions{TLSConfig: &tls.Config{RootCAs: roots, ServerName: "example.com"}}
	cfg := mpConfig()
	cfg.Credential = auth
	ws := func(u string) string { return "ws" + strings.TrimPrefix(u, "http") }

	for _, st := range []int{301, 302, 307, 308} {
		rd := zmuxtest.New("http://unused.invalid", zmuxtest.RedirectToHTTPS(srv.URL, st))
		cand := Candidate(ws(rd.URL)+"/zmux", o)
		c, err := zmux.DialMode(ctx, zmux.ExplicitMode(cand), cfg, nil)
		if err != nil {
			t.Fatalf("%d: %v", st, err)
		}
		s, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		echo(t, ctx, c, s, 64<<10)
		closeBoth(c, s)
		if got, want := cand.Target.URL(), ws(srv.URL)+"/zmux"; got != want {
			t.Fatalf("%d: target %s, want %s", st, got, want)
		}
		n := rd.Redirects()
		c, err = zmux.DialMode(ctx, zmux.ExplicitMode(cand), cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err = l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		closeBoth(c, s)
		if n == 0 || rd.Redirects() != n {
			t.Fatalf("%d: redirects %d then %d", st, n, rd.Redirects())
		}
		rd.Close()
	}

	cross := zmuxtest.New("http://unused.invalid", zmuxtest.Profile{
		RedirectTo: strings.Replace(elsewhere.URL, "127.0.0.1", "localhost", 1)})
	defer cross.Close()
	down := zmuxtest.New("http://unused.invalid", zmuxtest.Profile{TLS: true, RedirectTo: elsewhere.URL})
	defer down.Close()
	droots := x509.NewCertPool()
	droots.AddCert(down.Certificate())
	for _, c := range []struct {
		url string
		o   *DialOptions
	}{
		{ws(cross.URL), o},
		{ws(down.URL), &DialOptions{TLSConfig: &tls.Config{RootCAs: droots, ServerName: "example.com"}}},
	} {
		_, err := zmux.DialMode(ctx, zmux.ExplicitMode(Candidate(c.url, c.o)), cfg, nil)
		var me *zmux.ModeError
		var re *zmux.HTTPRedirectError
		if !errors.As(err, &me) || me.Results[0].Outcome != zmux.CandidateRefused || !errors.As(err, &re) {
			t.Fatalf("%s: %v", c.url, err)
		}
	}
	if n := other.Load(); n != 0 {
		t.Fatalf("the other host got %d requests", n)
	}
}
