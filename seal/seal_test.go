package seal

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxws"
)

var testPSK = []byte("0123456789abcdef0123456789abcdef")

type fixture struct {
	t         *testing.T
	srv       *Server
	lst       *zmux.Listener
	addr      string
	srvKey    PrivateKey
	cliKey    PrivateKey
	tags      *TagTable
	authorize func(PublicKey) ([]byte, any, bool)
}

type fixOpt struct {
	mode   Mode
	tls    *tls.Config
	ws     bool
	zcfg   *zmux.Config
	window time.Duration
}

func newFixture(t *testing.T, o fixOpt) *fixture {
	t.Helper()
	f := &fixture{t: t}
	f.srvKey, _ = GenerateKey()
	f.cliKey, _ = GenerateKey()
	f.tags = NewTagTable(nil)
	if err := f.tags.Set("boot-1", testPSK, "boot-value"); err != nil {
		t.Fatal(err)
	}
	cli := f.cliKey.Public()
	f.authorize = func(p PublicKey) ([]byte, any, bool) { return testPSK, "client-1", p == cli }
	old, _ := GenerateKey()
	srv, err := NewServer(&ServerConfig{Keys: []PrivateKey{f.srvKey, old}, Bootstrap: f.tags, Window: o.window,
		PeekTimeout: time.Second,
		Authorize: func(_ context.Context, p PublicKey, _ netip.AddrPort) ([]byte, any, bool) {
			return f.authorize(p)
		}})
	if err != nil {
		t.Fatal(err)
	}
	f.srv = srv
	zcfg := o.zcfg
	if zcfg == nil {
		zcfg = &zmux.Config{EarlyData: zmux.EarlyData{Replayable: true}}
	}
	f.lst = zmux.NewListener(&zmux.ListenerConfig{Config: zcfg, Admission: srv.Admission(o.mode), WrapPipe: srv.WrapPipe})
	if o.ws {
		h, err := zmuxws.Handler(f.lst, nil)
		if err != nil {
			t.Fatal(err)
		}
		hs := httptest.NewServer(h)
		f.addr = "ws" + strings.TrimPrefix(hs.URL, "http")
		t.Cleanup(func() { _ = f.lst.Close(); hs.Close() })
		return f
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if o.tls != nil {
		ln = tls.NewListener(ln, o.tls)
	}
	done := make(chan struct{})
	go func() { _ = f.lst.Serve(context.Background(), ln); close(done) }()
	t.Cleanup(func() { _ = f.lst.Close(); _ = ln.Close(); <-done })
	f.addr = ln.Addr().String()
	return f
}

func (f *fixture) clientConfig() *ClientConfig {
	return &ClientConfig{Peers: []PublicKey{f.srvKey.Public()}, Key: f.cliKey, PSK: testPSK, Skew: &ClockSkew{}}
}

func earlyConfig() *zmux.Config {
	return &zmux.Config{EarlyData: zmux.EarlyData{ZmuxPeer: true, Replayable: true, HoldFirstFlight: true}}
}

// recConn records each write.
type recConn struct {
	net.Conn
	mu     sync.Mutex
	writes [][]byte
	mangle func(i int, b []byte) []byte
}

func (c *recConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	i := len(c.writes)
	c.writes = append(c.writes, bytes.Clone(b))
	m := c.mangle
	c.mu.Unlock()
	if m != nil {
		if _, err := c.Conn.Write(m(i, bytes.Clone(b))); err != nil {
			return 0, err
		}
		return len(b), nil
	}
	return c.Conn.Write(b)
}

func (c *recConn) first() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writes) == 0 {
		return nil
	}
	return c.writes[0]
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// dial opens a carrier to f (TCP, or WebSocket) and wraps it.
func (f *fixture) dial(ctx context.Context, cc *ClientConfig, ws bool, mangle func(int, []byte) []byte) (zmux.PipeIO, *recConn) {
	f.t.Helper()
	if ws {
		p, err := zmuxws.DialPipe(ctx, f.addr, nil)
		if err != nil {
			f.t.Fatal(err)
		}
		q, err := Client(p, cc)
		if err != nil {
			f.t.Fatal(err)
		}
		return q, nil
	}
	nc, err := net.Dial("tcp", f.addr)
	if err != nil {
		f.t.Fatal(err)
	}
	rc := &recConn{Conn: nc, mangle: mangle}
	q, err := Client(zmux.NetPipe(rc), cc)
	if err != nil {
		f.t.Fatal(err)
	}
	return q, rc
}

// hello starts a session on p, writes hello on a stream before releasing
// the first flight, and returns both ends and what the server read.
func (f *fixture) hello(ctx context.Context, p zmux.PipeIO, hello []byte) (c, sc *zmux.Conn, got []byte, rep bool) {
	t := f.t
	t.Helper()
	c, err := zmux.Start(ctx, p, zmux.RoleClient, earlyConfig())
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(hello); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	c.ReleaseFirstFlight()
	sc, err = f.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ss, err := sc.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rep = ss.Replayable()
	got, err = io.ReadAll(ss)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); _ = sc.Close(); _ = c.Wait(); _ = sc.Wait() })
	return c, sc, got, rep
}

func echo(t *testing.T, ctx context.Context, c, s *zmux.Conn, n int) {
	t.Helper()
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
	msg := bytes.Repeat([]byte("0123456789"), n/10)
	go func() { _, _ = st.Write(msg); _ = st.Close() }()
	got, err := io.ReadAll(st)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("echo: %d bytes, %v", len(got), err)
	}
}

// The regular handshake over TCP: HELLO leaves in msg1, the server's
// stream is replayable, the identity reaches Conn.Admitted.
func TestRegularTCP(t *testing.T) {
	ctx := testCtx(t)
	f := newFixture(t, fixOpt{})
	p, rc := f.dial(ctx, f.clientConfig(), false, nil)
	hello := bytes.Repeat([]byte("HELLO"), 40)
	c, sc, got, rep := f.hello(ctx, p, hello)
	if !bytes.Equal(got, hello) || !rep {
		t.Fatalf("server read %q, replayable %v", got, rep)
	}
	if n := len(rc.first()); n < ikMsg1Head+len(hello)+tagLen {
		t.Fatalf("msg1 of %d bytes cannot hold the stream", n)
	}
	if cls, _ := zmux.ClassifyFirstBytes(rc.first(), zmux.FramingByteStream); cls != zmux.FirstBytesOther {
		t.Fatalf("msg1 classified %v", cls)
	}
	peer, _ := sc.Admitted().(*Peer)
	if peer == nil || peer.Value != "client-1" || peer.Static != f.cliKey.Public() || !peer.EarlyAccepted {
		t.Fatalf("admitted %+v", sc.Admitted())
	}
	if acc, known := settled(ctx, p); !acc || !known {
		t.Fatal("first flight not reported accepted")
	}
	echo(t, ctx, c, sc, 1<<20)
	st := f.srv.Stats()
	if st.Handshakes != 1 || st.EarlyAccepted != 1 || st.HandshakeNanos == 0 {
		t.Fatalf("stats %+v", st)
	}
	a, err1 := p.W.(zmux.KeyingMaterialExporter).ExportKeyingMaterial("x", nil, 32)
	b, err2 := sc.ExportKeyingMaterial("x", nil, 32)
	if err1 != nil || err2 != nil || len(a) != 32 || len(b) != 32 {
		t.Fatalf("exporter: %v %v", err1, err2)
	}
}

// The bootstrap handshake over WebSocket: the client learns the server
// key, the identity is the table's.
func TestBootstrapWebSocket(t *testing.T) {
	ctx := testCtx(t)
	f := newFixture(t, fixOpt{ws: true})
	var learnt PublicKey
	cc := &ClientConfig{PSK: testPSK, Skew: &ClockSkew{}, OnPeer: func(k PublicKey) { learnt = k }}
	p, _ := f.dial(ctx, cc, true, nil)
	if p.Variant != VariantBootstrap || p.Framing != zmux.FramingRecords {
		t.Fatalf("pipe %+v", p)
	}
	c, sc, got, _ := f.hello(ctx, p, []byte("boot hello"))
	settled(ctx, p)
	if string(got) != "boot hello" || learnt != f.srvKey.Public() {
		t.Fatalf("read %q, learnt %x", got, learnt)
	}
	peer, _ := sc.Admitted().(*Peer)
	if peer == nil || !peer.Bootstrap || peer.ID != "boot-1" || peer.Value != "boot-value" {
		t.Fatalf("admitted %+v", sc.Admitted())
	}
	echo(t, ctx, c, sc, 256<<10)
	if st := f.srv.Stats(); st.Bootstraps != 1 {
		t.Fatalf("stats %+v", st)
	}
	// The regular handshake on the same entrance, over WebSocket too.
	p2, _ := f.dial(ctx, f.clientConfig(), true, nil)
	c2, sc2, got2, _ := f.hello(ctx, p2, []byte("regular"))
	if string(got2) != "regular" {
		t.Fatalf("read %q", got2)
	}
	echo(t, ctx, c2, sc2, 64<<10)
}

// waitFail waits for the session on p to fail without the server
// accepting it.
func (f *fixture) waitFail(ctx context.Context, p zmux.PipeIO) {
	t := f.t
	t.Helper()
	c, err := zmux.Start(ctx, p, zmux.RoleClient, &zmux.Config{})
	if err != nil {
		return
	}
	done := make(chan struct{})
	go func() { _, _ = c.OpenStreamSync(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("session did not fail")
	}
	_ = c.Close()
	actx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if sc, err := f.lst.Accept(actx); err == nil {
		_ = sc.Close()
		t.Fatal("server accepted the session")
	}
}

func TestFailures(t *testing.T) {
	ctx := testCtx(t)
	f := newFixture(t, fixOpt{})
	other, _ := GenerateKey()

	cc := f.clientConfig()
	cc.PSK = []byte("another PSK of sixteen bytes+")
	p, _ := f.dial(ctx, cc, false, nil)
	f.waitFail(ctx, p)

	cc = f.clientConfig()
	cc.Peers = []PublicKey{other.Public()}
	p, _ = f.dial(ctx, cc, false, nil)
	f.waitFail(ctx, p)

	cc = f.clientConfig()
	cc.Key = other
	p, _ = f.dial(ctx, cc, false, nil)
	f.waitFail(ctx, p)

	// Tampering with msg1's payload.
	p, _ = f.dial(ctx, f.clientConfig(), false, func(i int, b []byte) []byte {
		if i == 0 {
			b[len(b)-1] ^= 1
		}
		return b
	})
	f.waitFail(ctx, p)

	// A bootstrap client with a PSK the table lacks.
	p, _ = f.dial(ctx, &ClientConfig{PSK: []byte("unknown bootstrap PSK"), Skew: &ClockSkew{}}, false, nil)
	f.waitFail(ctx, p)

	st := f.srv.Stats()
	// The unknown bootstrap client looks like a regular msg1 that has not
	// ended: it times out in the admission, counted there.
	if st.FailAuth != 2 || st.FailNoKey != 1 || st.FailUnauthorized != 1 || st.Handshakes != 0 {
		t.Fatalf("stats %+v", st)
	}

	// Tampering with a transport frame (the second write) fails the
	// established session.
	p, _ = f.dial(ctx, f.clientConfig(), false, func(i int, b []byte) []byte {
		if i == 1 {
			b[len(b)-1] ^= 1
		}
		return b
	})
	c, err := zmux.Start(ctx, p, zmux.RoleClient, &zmux.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := f.lst.Accept(ctx)
	if err == nil {
		go func() { _, _ = sc.AcceptStream(ctx) }()
		st, err := c.OpenStreamSync(ctx)
		if err == nil {
			_, _ = st.Write([]byte("data"))
		}
		werr := make(chan error, 1)
		go func() { werr <- sc.Wait() }()
		select {
		case err := <-werr:
			_ = err
		case <-time.After(5 * time.Second):
			t.Fatal("tampered frame did not end the session")
		}
	}
	_ = c.Close()
}

// A replayed msg1 completes no session; the first flight of a stale stamp
// is refused, sent again, and the session works.
func TestReplayAndWindow(t *testing.T) {
	ctx := testCtx(t)
	f := newFixture(t, fixOpt{})
	p, rc := f.dial(ctx, f.clientConfig(), false, nil)
	f.hello(ctx, p, []byte("first"))
	nc, err := net.Dial("tcp", f.addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = nc.Write(rc.first())
	_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, _ := io.ReadFull(nc, make([]byte, ikMsg2Head))
	_ = nc.Close()
	if n != ikMsg2Head {
		t.Fatalf("replay got %d bytes of msg2", n)
	}
	actx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	if sc, err := f.lst.Accept(actx); err == nil {
		_ = sc.Close()
		t.Fatal("replay made a session")
	}
	cancel()
	if st := f.srv.Stats(); st.Replays != 1 || st.EarlyRejected != 1 {
		t.Fatalf("stats %+v", st)
	}

	// A client clock 10 minutes ahead.
	clk := timeshim.NewFake(time.Now().Add(10 * time.Minute))
	cc := f.clientConfig()
	cc.Clock = clk
	p, _ = f.dial(ctx, cc, false, nil)
	c, sc, got, rep := f.hello(ctx, p, []byte("late"))
	if string(got) != "late" || rep {
		t.Fatalf("read %q, replayable %v", got, rep)
	}
	if acc, known := settled(ctx, p); acc || !known {
		t.Fatal("stale first flight reported accepted")
	}
	echo(t, ctx, c, sc, 64<<10)
	if st := f.srv.Stats(); st.WindowMiss != 1 {
		t.Fatalf("stats %+v", st)
	}
	if off, auth := cc.Skew.Offset(clk); !auth || off > -9*time.Minute {
		t.Fatalf("offset %v %v", off, auth)
	}
	// The learnt offset puts the next stamp in the window.
	p, _ = f.dial(ctx, cc, false, nil)
	f.hello(ctx, p, []byte("again"))
	if acc, _ := settled(ctx, p); !acc {
		t.Fatal("calibrated first flight refused")
	}
}

func selfSigned(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	srv := httptest.NewTLSServer(nil)
	cert := srv.TLS.Certificates[0]
	srv.Close()
	return &tls.Config{Certificates: []tls.Certificate{cert}}, &tls.Config{InsecureSkipVerify: true}
}

// Auto takes plain carriers on TLS terminated here and sealed ones;
// SealedOnly takes no plain carrier, and Auto none without TLS.
func TestModes(t *testing.T) {
	ctx := testCtx(t)
	stls, ctls := selfSigned(t)
	f := newFixture(t, fixOpt{mode: Auto, tls: stls})
	c, err := zmux.DialTLS(ctx, "tcp", f.addr, ctls, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := f.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if peer, _ := sc.Admitted().(*Peer); peer == nil || !peer.Plain {
		t.Fatalf("admitted %+v", sc.Admitted())
	}
	echo(t, ctx, c, sc, 16<<10)
	_ = c.Close()
	_ = sc.Close()
	tc, err := tls.Dial("tcp", f.addr, ctls)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Client(zmux.NetPipe(tc), f.clientConfig())
	if err != nil {
		t.Fatal(err)
	}
	f.hello(ctx, p, []byte("sealed in TLS"))
	if st := f.srv.Stats(); st.PlainAccepted != 1 || st.Handshakes != 1 {
		t.Fatalf("stats %+v", st)
	}

	g := newFixture(t, fixOpt{mode: Auto})
	nc, _ := net.Dial("tcp", g.addr)
	g.waitFail(ctx, zmux.NetPipe(nc))
	h := newFixture(t, fixOpt{mode: SealedOnly, tls: stls})
	tc, err = tls.Dial("tcp", h.addr, ctls)
	if err != nil {
		t.Fatal(err)
	}
	h.waitFail(ctx, zmux.NetPipe(tc))
	if g.srv.Stats().PlainNotTerminated != 1 || h.srv.Stats().PlainRejected != 1 {
		t.Fatalf("stats %+v %+v", g.srv.Stats(), h.srv.Stats())
	}
}

// PipeAuth.Exporter binds joins to the carrier's exporter; each joining
// carrier runs its own handshake.
func TestExporterJoin(t *testing.T) {
	ctx := testCtx(t)
	zcfg := &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{Exporter: true}}
	f := newFixture(t, fixOpt{zcfg: zcfg, ws: true})
	p, _ := f.dial(ctx, f.clientConfig(), true, nil)
	c, err := zmux.Start(ctx, p, zmux.RoleClient, &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{Exporter: true}})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := f.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); _ = sc.Close() })
	echo(t, ctx, c, sc, 16<<10)
	for range 2 {
		q, _ := f.dial(ctx, f.clientConfig(), true, nil)
		if _, err := c.AddPipe(ctx, q, zmux.PipeOptions{CarrierBound: true}); err != nil {
			t.Fatal(err)
		}
	}
	echo(t, ctx, c, sc, 1<<20)
	if n := len(c.PipeStats()); n != 3 {
		t.Fatalf("%d pipes", n)
	}
	if st := f.srv.Stats(); st.Handshakes != 3 {
		t.Fatalf("stats %+v", st)
	}
}

// Concurrent streams over both carrier kinds.
func TestConcurrent(t *testing.T) {
	for _, ws := range []bool{false, true} {
		ctx := testCtx(t)
		f := newFixture(t, fixOpt{ws: ws})
		p, _ := f.dial(ctx, f.clientConfig(), ws, nil)
		c, sc, _, _ := f.hello(ctx, p, []byte("x"))
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { echo(t, ctx, c, sc, 256<<10) })
		}
		wg.Wait()
	}
}

// settled waits for msg2 and reports EarlyDataAccepted.
func settled(ctx context.Context, p zmux.PipeIO) (bool, bool) {
	_, _ = p.W.(zmux.EarlyClientCarrier).EarlyAccepted(ctx)
	return EarlyDataAccepted(p)
}
