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
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

func tlsConfigs(t testing.TB) (*tls.Config, *tls.Config) {
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
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{Exporter: true, EndToEnd: true}}
}

type env struct {
	ln     *quic.EarlyListener
	lst    *zmux.Listener
	cli    *tls.Config
	cancel context.CancelFunc
	tl     *trackedListener
}

func newEnv(t testing.TB, scfg *zmux.Config) *env {
	return newEnvAt(t, "127.0.0.1:0", scfg)
}

func newEnvAt(t testing.TB, address string, scfg *zmux.Config) *env {
	return newEnvQUIC(t, address, scfg, &quic.Config{Allow0RTT: true})
}
func newEnvQUIC(t testing.TB, address string, scfg *zmux.Config, qconf *quic.Config) *env {
	t.Helper()
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddrEarly(address, srv, testQUIC(qconf))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &env{ln: ln, lst: mustNewListener(&zmux.ListenerConfig{Config: scfg}), cli: cli, cancel: cancel}
	tl := &trackedListener{ConnListener: ln}
	e.tl = tl
	go func() { _ = Serve(ctx, e.lst, tl, nil) }()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		_ = e.lst.Close()
		tl.closeAll()
	})
	return e
}

// trackedListener remembers what it accepted: closing a QUIC listener
// leaves established connections alone, and a client's close may never
// arrive (a test's relay drops it).
type trackedListener struct {
	ConnListener
	mu    sync.Mutex
	conns []*quic.Conn
}

func (l *trackedListener) Accept(ctx context.Context) (*quic.Conn, error) {
	qc, err := l.ConnListener.Accept(ctx)
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, qc)
		l.mu.Unlock()
	}
	return qc, err
}

// last is the connection accepted last.
func (l *trackedListener) last() *quic.Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conns[len(l.conns)-1]
}

func (l *trackedListener) closeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, qc := range l.conns {
		_ = qc.CloseWithError(0, "")
	}
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
	_, _ = rand.Read(msg)
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
	checkKind(t, zmux.CarrierQUIC, c, s)
	// Several bulk streams at once ask for lanes of pipe 0.
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			echo(t, c, s, 2<<20)
		})
	}
	wg.Wait()
	var lanes int
	for _, p := range c.PipeStats() {
		lanes += p.LanesOut
	}
	if lanes < 2 {
		t.Fatalf("no lanes were opened: %+v", c.PipeStats())
	}
}

func TestQUICReplayableResumedDial(t *testing.T) {
	for _, tc := range []struct {
		name          string
		allow, ticket bool
	}{{"accepted", true, true}, {"rejected", false, true}, {"no_ticket", true, false}} {
		t.Run(tc.name, func(t *testing.T) { exerciseReplayableResumedDial(t, tc.allow, tc.ticket, false) })
		// PipeAuth.Exporter reads the exporter once the handshake completed,
		// of the connection that replaced a rejected 0-RTT one: a join then
		// verifies on both ends.
		t.Run(tc.name+"_exporter", func(t *testing.T) { exerciseReplayableResumedDial(t, tc.allow, tc.ticket, true) })
	}
}
func exerciseReplayableResumedDial(t *testing.T, allowSecond, useTicket, exporter bool) {
	t.Helper()
	withAuth := func(cfg *zmux.Config) *zmux.Config {
		if exporter {
			cfg.MaxPipes, cfg.PipeAuth = 4, zmux.PipeAuth{Exporter: true, EndToEnd: true}
		}
		return cfg
	}
	var allow atomic.Bool
	allow.Store(true)
	base := &quic.Config{Allow0RTT: true}
	base.GetConfigForClient = func(*quic.ClientInfo) (*quic.Config, error) {
		c := base.Clone()
		c.GetConfigForClient = nil
		c.Allow0RTT = allow.Load()
		return c, nil
	}
	e := newEnvQUIC(t, "127.0.0.1:0", withAuth(&zmux.Config{EarlyData: zmux.EarlyData{Replayable: true}}), base)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// A first session gets a session ticket and the parameters; it runs on
	// a connection the test dials itself (Client).
	qc, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, Config(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	c, err := Client(ctx, qc, withAuth(&zmux.Config{}))
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
	for {
		if _, ok := e.cli.ClientSessionCache.Get("localhost"); ok {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("session ticket not received")
		case <-time.After(time.Millisecond):
		}
	}
	allow.Store(allowSecond)
	secondTLS := e.cli.Clone()
	if !useTicket {
		secondTLS.ClientSessionCache = tls.NewLRUClientSessionCache(4)
	}
	// The second uses replayable parameters and a resumed native connection.
	// Native streams currently wait for handshake completion (upstream race).
	cfg := withAuth(&zmux.Config{EarlyData: zmux.EarlyData{Replayable: true, Remembered: remembered}})
	c2, err := Dial(ctx, e.ln.Addr().String(), secondTLS, nil, cfg)
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
	if exporter {
		jc, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, Config(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = jc.CloseWithError(0, "") }()
		pio, err := ConnPipe(ctx, jc)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c2.AddPipe(ctx, pio, zmux.PipeOptions{}); err != nil {
			t.Fatal(err)
		}
		echo(t, c2, s2, 1<<10)
		if n := len(s2.PipeStats()); n != 2 {
			t.Fatalf("server has %d pipes", n)
		}
	}
	_ = c2.Close()
	_ = s2.Close()
	_ = c2.Wait()
	_ = s2.Wait()
}

// Floor early data through Dial without 0-RTT: the stream is not sent
// early, so the session needs no Replayable and waits for no verdict.
func TestQUICDialZmuxPeer(t *testing.T) {
	for _, replayable := range []bool{false, true} {
		e := newEnv(t, mpConfig())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cfg := mpConfig()
		cfg.EarlyData = zmux.EarlyData{ZmuxPeer: true, Replayable: replayable}
		c, err := Dial(ctx, e.ln.Addr().String(), e.cli, nil, cfg)
		if err != nil {
			cancel()
			t.Fatalf("replayable %v: %v", replayable, err)
		}
		s, err := e.lst.Accept(ctx)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		echo(t, c, s, 1000)
		_ = c.Close()
		_ = s.Close()
		_ = c.Wait()
		_ = s.Wait()
		cancel()
	}
}

func TestQUICPool(t *testing.T) {
	e := newEnv(t, mpConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := NewPool(e.ln.Addr().String(), e.cli, nil, nil, carrier.PoolLimits{MaxPipesPerConnection: 2})
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
	if n := len(c.PipeStats()); n != 3 {
		t.Fatalf("%d pipes", n)
	}
}

// TestQUICCopyLane: on a session's only pipe with lanes, urgent data is
// copied to the copy lane.
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
		if r := c.ConnectionStats().BytesRedundant; r > 0 {
			// The session count is the sum of the pipes' (taken later:
			// at least as large).
			var pipes uint64
			for _, ps := range c.PipeStats() {
				pipes += ps.BytesRedundant
			}
			if pipes < r {
				t.Fatalf("pipes count %d redundant bytes, the session %d", pipes, r)
			}
			return
		}
	}
	t.Fatalf("no copy on the copy lane: %+v", c.PipeStats())
}

// testQUIC is Config(base) with a handshake timeout that tolerates a
// loaded test host: the default 5 s fails handshakes under -race with
// many packages running at once.
func testQUIC(base *quic.Config) *quic.Config {
	c := Config(base)
	c.HandshakeIdleTimeout = 30 * time.Second
	return c
}

// checkKind fails the test unless every pipe of each session reports
// carrier kind k.
func checkKind(t *testing.T, k zmux.CarrierKind, cs ...*zmux.Conn) {
	t.Helper()
	for _, c := range cs {
		st := c.PipeStats()
		if len(st) == 0 {
			t.Fatal("no pipes")
		}
		for _, p := range st {
			if p.Carrier != k {
				t.Fatalf("pipe %d: carrier %v, want %v", p.PipeID, p.Carrier, k)
			}
		}
	}
}

// TestQUICHandshakeFailureCounted checks that Serve reports a connection
// whose handshake fails before any stream arrived as a setup failure of
// the listener, and nothing else. The server fails the handshake itself,
// on the client's last flight (no client certificate), so the connection
// has been accepted by then: a handshake that fails before the listener
// hands the connection over is invisible to Serve and counted nowhere.
func TestQUICHandshakeFailureCounted(t *testing.T) {
	srv, cli := tlsConfigs(t)
	srv.ClientAuth = tls.RequireAnyClientCert
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", srv, testQUIC(&quic.Config{Allow0RTT: true}))
	if err != nil {
		t.Fatal(err)
	}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	served := make(chan struct{})
	go func() { _ = Serve(ctx, lst, ln, nil); close(served) }()
	defer func() { cancel(); _ = ln.Close(); _ = lst.Close(); <-served }()
	if qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, testQUIC(nil)); err == nil {
		// The client finishes first; the server's verdict closes the
		// connection.
		select {
		case <-qc.Context().Done():
		case <-ctx.Done():
			t.Fatal("the handshake succeeded")
		}
	}
	for lst.Stats().SetupFailures != 1 {
		if ctx.Err() != nil {
			t.Fatalf("stats %+v", lst.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st := lst.Stats(); !reflect.DeepEqual(st, zmux.ListenerStats{SetupFailures: 1}) {
		t.Fatalf("stats %+v", st)
	}
}
