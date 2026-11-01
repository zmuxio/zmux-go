package zmuxws

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zmuxio/zmux-go"
)

// A ListenerConfig.WrapPipe layer runs TLS over WebSocket carriers, one
// message per write: the session's join secret and a carrier-bound join
// use that TLS, and the carriers keep their kind.
func TestWrapPipeTLS(t *testing.T) {
	ctx := testContext(t)
	certSrv := httptest.NewTLSServer(http.NotFoundHandler()) // a certificate to use
	defer certSrv.Close()
	scfg := certSrv.TLS.Clone()
	scfg.MinVersion = tls.VersionTLS13
	roots := x509.NewCertPool()
	roots.AddCert(certSrv.Certificate())
	ccfg := &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS13}

	cfg := func() *zmux.Config {
		return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{Exporter: true},
			SchedulerParams: zmux.SchedulerParams{NoProbation: true}}
	}
	l := zmux.NewListener(&zmux.ListenerConfig{Config: cfg(),
		WrapPipe: func(ctx context.Context, p zmux.PipeIO, _ zmux.JoinHint) (zmux.PipeIO, error) {
			tc := tls.Server(zmux.PipeConn(p), scfg)
			if err := tc.HandshakeContext(ctx); err != nil {
				return zmux.PipeIO{}, err
			}
			return zmux.NetPipe(tc), nil
		}})
	h, err := Handler(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		_ = l.Close()
		srv.Close()
	})
	cand := Candidate("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	dial := cand.Dial
	cand.Dial = func(ctx context.Context, cred zmux.Credential) (zmux.PipeIO, error) {
		p, err := dial(ctx, cred)
		if err != nil {
			return p, err
		}
		tc := tls.Client(zmux.PipeConn(p), ccfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			_ = tc.Close()
			return zmux.PipeIO{}, err
		}
		return zmux.NetPipe(tc), nil
	}
	c, err := zmux.DialMode(ctx, zmux.ExplicitMode(cand), cfg(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBoth(c, s)
	p, err := cand.Dial(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.Carrier = zmux.CarrierWebSocket
	if _, err := c.AddPipe(ctx, p, zmux.PipeOptions{CarrierBound: true}); err != nil {
		t.Fatalf("carrier-bound join: %v", err)
	}
	echo(t, ctx, c, s, 256<<10)
	for _, x := range []*zmux.Conn{c, s} {
		ps := x.PipeStats()
		if len(ps) != 2 {
			t.Fatalf("%d pipes", len(ps))
		}
		for _, st := range ps {
			if st.Carrier != zmux.CarrierWebSocket {
				t.Fatalf("carrier kind %v", st.Carrier)
			}
		}
	}
}
