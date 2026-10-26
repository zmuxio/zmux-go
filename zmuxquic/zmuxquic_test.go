package zmuxquic

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
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

func tlsConfigs(t *testing.T) (*tls.Config, *tls.Config) {
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
	srv := &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		NextProtos: []string{"zmux"}}
	cli := &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"zmux"},
		ClientSessionCache: tls.NewLRUClientSessionCache(4)}
	return srv, cli
}

func mpConfig() *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{TLS: true}}
}

type env struct {
	ln     *quic.EarlyListener
	lst    *zmux.Listener
	cli    *tls.Config
	cancel context.CancelFunc
}

func newEnv(t *testing.T, scfg *zmux.Config) *env {
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", srv, QUICConfig(&quic.Config{Allow0RTT: true}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &env{ln: ln, lst: zmux.NewListener(&zmux.ListenerConfig{Config: scfg}), cli: cli, cancel: cancel}
	go func() { _ = Serve(ctx, ln, e.lst) }()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		_ = e.lst.Close()
	})
	return e
}

func echo(t *testing.T, c, s *zmux.Conn, n int) {
	t.Helper()
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
	msg := make([]byte, n)
	rand.Read(msg)
	go func() { _, _ = st.Write(msg); _ = st.Close() }()
	got, err := io.ReadAll(st)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("echo: %d of %d bytes, %v", len(got), n, err)
	}
}

func TestQUICPipeAndLanes(t *testing.T) {
	e := newEnv(t, mpConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, e.ln.Addr().String(), e.cli, nil, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = c.Close()
		_ = s.Close()
		_ = c.Wait()
		_ = s.Wait()
	}()
	echo(t, c, s, 1<<20)
	// Several bulk streams at once ask for lanes of pipe 0.
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			echo(t, c, s, 2<<20)
		}()
	}
	wg.Wait()
	var lanes int
	for _, p := range c.Pipes() {
		lanes += p.Lanes
	}
	if lanes < 2 {
		t.Fatalf("no lanes were opened: %+v", c.Pipes())
	}
}

func TestQUICZeroRTT(t *testing.T) {
	e := newEnv(t, &zmux.Config{EarlyData: zmux.EarlyData{Replayable: true}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// A first session gets a session ticket and the parameters.
	c, err := Dial(ctx, e.ln.Addr().String(), e.cli, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s, 1000)
	remembered := c.RememberedParameters()
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
	// The second sends its first stream in 0-RTT.
	cfg := &zmux.Config{EarlyData: zmux.EarlyData{Replayable: true, Remembered: remembered}}
	c2, err := Dial(ctx, e.ln.Addr().String(), e.cli, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st, err := c2.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("early")); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	s2, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ss, err := s2.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(ss)
	if err != nil || string(b) != "early" {
		t.Fatalf("early stream %q %v", b, err)
	}
	_ = c2.Close()
	_ = s2.Close()
	_ = c2.Wait()
	_ = s2.Wait()
}

func TestQUICPool(t *testing.T) {
	e := newEnv(t, mpConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := NewPool(e.ln.Addr().String(), e.cli, nil)
	p.MaxPipesPerConnection = 2
	defer p.Shutdown()
	first, err := p.OpenPipe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, first, zmux.RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = c.Close()
		_ = s.Close()
		_ = c.Wait()
		_ = s.Wait()
	}()
	var fates []uint64
	for range 2 {
		pio, err := p.OpenPipe(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fates = append(fates, pio.FateGroup)
		if _, err := c.AddPipe(ctx, pio, zmux.PipeOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if first.FateGroup == 0 || fates[0] != first.FateGroup || fates[1] == first.FateGroup {
		t.Fatalf("fate groups %d %v", first.FateGroup, fates)
	}
	echo(t, c, s, 1<<20)
	if n := len(c.Pipes()); n != 3 {
		t.Fatalf("%d pipes", n)
	}
}

// TestQUICCopyLane: on a session's only pipe with lanes, urgent data is
// copied to the copy lane (SCHEDULER.md 4.1, 9.2 rule 3).
func TestQUICCopyLane(t *testing.T) {
	e := newEnv(t, mpConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, e.ln.Addr().String(), e.cli, nil, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = c.Close()
		_ = s.Close()
		_ = c.Wait()
		_ = s.Wait()
	}()
	go func() {
		for {
			st, err := s.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(st, st); _ = st.Close() }()
		}
	}()
	for i := range 200 {
		time.Sleep(5 * time.Millisecond) // the copy lane is usable once confirmed
		st, err := c.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		st.SetPriority(0, false)
		msg := bytes.Repeat([]byte{byte(i)}, 4096)
		_, _ = st.Write(msg)
		_ = st.Close()
		got, err := io.ReadAll(st)
		if err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("urgent echo %d: %v", i, err)
		}
		if c.ConnectionStats().BytesRedundant > 0 {
			return
		}
	}
	t.Fatalf("no copy on the copy lane: %+v", c.Pipes())
}
