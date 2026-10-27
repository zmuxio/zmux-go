package zmuxh3

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
)

func tlsConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
		&tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{http3.NextProtoH3}}
}

func echo(t *testing.T, c, s *zmux.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		st, err := s.AcceptStream(ctx)
		if err == nil {
			_, _ = io.Copy(st, st)
			_ = st.Close()
		}
	}()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 512<<10)
	_, _ = rand.Read(msg)
	go func() { _, _ = st.Write(msg); _ = st.Close() }()
	got, err := io.ReadAll(st)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("echo %d bytes %v", len(got), err)
	}
}

func cfg() *zmux.Config { return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{TLS: true}} }

// pskCfg: HTTP request carriers expose no TLS exporter to the session.
func pskCfg() *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{9}, 32)}}
}

func TestHTTP3RequestStream(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: pskCfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewHTTP3Server("", srvTLS, nil, lst)
	go func() { _ = srv.Serve(pc) }()
	defer func() { _ = srv.Close() }()
	client, tr := HTTPClient(cliTLS, nil)
	defer func() { _ = tr.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := "https://" + pc.LocalAddr().String() + "/"
	p, err := zmux.HTTPPipe(ctx, client, url, nil, zmux.DirBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, pskCfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

func TestWebTransport(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: cfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	ws := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	h3.Handler = Handler(ws, lst)
	go func() { _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tr := &webtransport.Transport{TLSClientConfig: cliTLS}
	c, sess, err := Dial(ctx, tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, cfg())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	// Pooled pipes: two per WebTransport session, then a new session.
	pool := NewPool(tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, zmux.PoolLimits{MaxPipesPerConnection: 2})
	defer pool.Shutdown()
	var fates []uint64
	for range 3 {
		pp, err := pool.OpenPipe(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fates = append(fates, pp.FateGroup)
		if _, err := c.AddPipe(ctx, pp, zmux.PipeOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if fates[0] == 0 || fates[0] != fates[1] || fates[2] == fates[0] {
		t.Fatalf("fate groups %v", fates)
	}
	echo(t, c, s)
	// A second pipe joined over another stream of the session.
	p2, err := OpenPipe(ctx, sess)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddPipe(ctx, p2, zmux.PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}
