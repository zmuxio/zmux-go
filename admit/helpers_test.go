package admit

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

// Helpers of the root package's tests that these tests share.

func mpConfig() *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32), EndToEnd: true},
		SchedulerParams: zmux.SchedulerParams{Probation: -1}}
}

// mustNewListener is NewListener for a configuration the test knows valid.
func mustNewListener(cfg *zmux.ListenerConfig) *zmux.Listener {
	l, err := zmux.NewListener(cfg)
	if err != nil {
		panic(err)
	}
	return l
}

func waitDone(t testing.TB, c *zmux.Conn) {
	done := make(chan struct{})
	go func() { _ = c.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("session did not end")
	}
}

func selfSignedTLS(t testing.TB) (srv, cli *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
		&tls.Config{RootCAs: pool, ServerName: "localhost"}
}

// firstRecord returns the bytes a client session sends first on a carrier.
func firstRecord(t *testing.T) []byte {
	t.Helper()
	c, s := net.Pipe()
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		conn, err := zmux.Start(ctx, zmux.NetPipe(c), zmux.RoleClient, mpConfig())
		if err == nil {
			defer func() { _ = conn.Close(); _ = conn.Wait() }()
		}
		<-ctx.Done()
	}()
	b := make([]byte, 4096)
	_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := s.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = c.Close()
	return b[:n]
}
