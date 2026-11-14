package zmuxws

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zmuxio/zmux-go"
)

func tlsAuthConfig() *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{Exporter: true, EndToEnd: true},
		AllowRecordMode: func(zmux.RecordPolicyInput, zmux.RecordMode, any) bool { return true }}
}

// A carrier on TLS terminated here declares the Bound contract.
func TestCarrierSecure(t *testing.T) {
	st := &tls.ConnectionState{Version: tls.VersionTLS13, HandshakeComplete: true}
	resumed12 := &tls.ConnectionState{Version: tls.VersionTLS12, HandshakeComplete: true, DidResume: true}
	for _, c := range []struct {
		p    carrier
		want bool
	}{{carrier{tls: st}, true}, {carrier{tls: resumed12}, false}, {carrier{}, false}} {
		if got := c.p.CarrierSecure(); got != c.want {
			t.Errorf("%+v: %v", c.p, got)
		}
	}
}

// Over wss both ends have the TLS exporter: a session with PipeAuth.Exporter
// starts, and a carrier-bound pipe joins it.
func TestPipeAuthExporter(t *testing.T) {
	ctx := testContext(t)
	l := mustNewListener(&zmux.ListenerConfig{Config: tlsAuthConfig()})
	h, err := NewHandler(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(h)
	t.Cleanup(func() {
		_ = l.Close()
		srv.Close()
	})
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	u := "wss" + strings.TrimPrefix(srv.URL, "https")
	o := &DialOptions{TLSConfig: &tls.Config{RootCAs: roots, ServerName: "example.com"}}
	c, err := Dial(ctx, u, o, tlsAuthConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p, err := DialPipe(ctx, u, o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddPipe(ctx, p, zmux.PipeOptions{RecordMode: zmux.Set(zmux.RecordBound)}); err != nil {
		t.Fatal(err)
	}
	echo(t, ctx, c, s, 64<<10)
	if n := len(c.PipeStats()); n != 2 {
		t.Fatalf("%d pipes", n)
	}
	closeBoth(c, s)
}

// Over plain ws there is no TLS terminated here: PipeAuth.Exporter fails to
// start rather than bind to nothing.
func TestPipeAuthExporterPlain(t *testing.T) {
	ctx := testContext(t)
	l := mustNewListener(&zmux.ListenerConfig{Config: tlsAuthConfig()})
	u := server(t, l, nil)
	if c, err := Dial(ctx, u, nil, tlsAuthConfig()); !errors.Is(err, zmux.ErrInvalidConfig) {
		if c != nil {
			_ = c.Close()
		}
		t.Fatalf("PipeAuth.Exporter over ws: %v", err)
	}
	p, err := DialPipe(ctx, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st := p.W.(interface{ ConnectionState() tls.ConnectionState }).ConnectionState(); st.HandshakeComplete {
		t.Fatal("ws carrier reports a TLS handshake")
	}
	_ = p.W.Close()
}
