package seal_test

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

func newKey(t *testing.T) *ecdh.PrivateKey {
	t.Helper()
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

type gate struct {
	cred        *proto.Credential
	handle, psk []byte

	asked   []seal.CredentialRequest
	commits []seal.CommitRequest
	mu      sync.Mutex
}

func (g *gate) Resolve(_ context.Context, h []byte, _ netip.AddrPort) ([]byte, error) {
	if !bytes.Equal(h, g.handle) {
		return nil, errors.New("unknown handle")
	}
	return g.psk, nil
}

func (g *gate) Credential(_ context.Context, r seal.CredentialRequest) (*proto.Credential, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.asked = append(g.asked, r)
	return g.cred, nil
}

func (g *gate) Commit(_ context.Context, a seal.CommitRequest) (any, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.commits = append(g.commits, a)
	return "client 1", nil
}

// secureConn declares the Bound contract over a net.Conn, with a fixed exporter.
type secureConn struct {
	net.Conn
	exp []byte
}

func (c *secureConn) CarrierSecure() bool { return true }
func (c *secureConn) ExportKeyingMaterial(string, []byte, int) ([]byte, error) {
	return c.exp, nil
}

// laneConn has the capabilities that Sealed must hide.
type laneConn struct{ secureConn }

func (*laneConn) OpenLane(context.Context) (io.WriteCloser, error) { return nil, errors.New("no") }
func (*laneConn) ChannelStreamID() (uint64, bool)                  { return 0, false }
func (*laneConn) MaxChannelDatagram() int                          { return 0 }
func (*laneConn) SendChannelDatagram([]byte) error                 { return nil }
func (*laneConn) ReceiveChannelDatagrams(func([]byte)) (stop func()) {
	return func() {}
}

type fixture struct {
	psk, handle []byte
	node        *ecdh.PrivateKey
	client      seal.ClientConfig
	server      seal.ServerConfig
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	handle, psk, err := seal.DeriveHandlePSK(secret, []byte("test"), nil)
	if err != nil {
		t.Fatal(err)
	}
	node := newKey(t)
	return &fixture{psk: psk, handle: handle, node: node,
		client: seal.ClientConfig{Anchor: seal.PinAnchor(seal.NodeKeyID(node.PublicKey().Bytes())), Handle: handle, PSK: psk},
		server: seal.ServerConfig{Gate: &gate{handle: handle, psk: psk}, Key: node}}
}

func anyMode(zmux.RecordPolicyInput, zmux.RecordMode, any) bool { return true }

func listen(t *testing.T, ctx context.Context, lc *zmux.ListenerConfig) (*zmux.Listener, string) {
	t.Helper()
	l, err := zmux.NewListener(lc)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = l.Serve(ctx, ln) }()
	t.Cleanup(func() { _ = l.Close(); _ = ln.Close(); <-done })
	return l, ln.Addr().String()
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func echo(t *testing.T, ctx context.Context, c, s *zmux.Conn) {
	t.Helper()
	msg := bytes.Repeat([]byte("zmux"), 32<<10)
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() {
		_, err := st.Write(msg)
		if err == nil {
			err = st.Close()
		}
		errc <- err
	}()
	in, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(in)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("read %d bytes: %v", len(got), err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

// Each mode establishes a session over TCP, and a further carrier joins it.
func TestModesAndJoin(t *testing.T) {
	for _, mode := range []zmux.RecordMode{zmux.RecordSealed, zmux.RecordBound, zmux.RecordTrusted} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			f := newFixture(t)
			f.server.AllowRecordMode = anyMode
			exp := bytes.Repeat([]byte{7}, 32)
			wrap := func(ctx context.Context, p zmux.PipeIO, h zmux.PipeHint) (zmux.PipeIO, error) {
				if mode == zmux.RecordBound {
					p = zmux.NetPipe(&secureConn{Conn: p.W.(net.Conn), exp: exp})
				}
				return f.server.WrapPipe(ctx, p, h)
			}
			l, addr := listen(t, ctx, &zmux.ListenerConfig{
				Config:   &zmux.Config{MaxPipes: 4, AllowRecordMode: anyMode},
				WrapPipe: zmux.WrapPipeFunc(wrap),
			})
			carrier := func() zmux.PipeIO {
				nc := dial(t, addr)
				if mode == zmux.RecordBound {
					return zmux.NetPipe(&secureConn{Conn: nc, exp: exp})
				}
				return zmux.NetPipe(nc)
			}
			cc := f.client
			cc.RecordMode = mode
			p, err := seal.Establish(ctx, carrier(), &cc)
			if err != nil {
				t.Fatal(err)
			}
			if peer, ok := seal.PeerOf(p); !ok || !bytes.Equal(peer.Static, f.node.PublicKey().Bytes()) || peer.RecordMode != mode {
				t.Fatalf("PeerOf: %+v %v", peer, ok)
			}
			c, err := zmux.Start(ctx, p, zmux.RoleClient, &zmux.Config{MaxPipes: 4, RecordPolicy: func(zmux.RecordPolicyInput) zmux.RecordMode { return zmux.RecordTrusted }})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			s, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			if v, ok := zmux.AdmittedAs[*seal.Peer](s); !ok || v.Value != "client 1" || v.RecordMode != mode {
				t.Fatalf("AdmittedAs: %+v", v)
			}
			echo(t, ctx, c, s)
			if _, err := c.AddPipe(ctx, zmux.NetPipe(dial(t, addr)), zmux.PipeOptions{}); err != nil {
				t.Fatalf("join: %v", err)
			}
			echo(t, ctx, c, s)
			if n := len(c.PipeStats()); n != 2 {
				t.Fatalf("%d pipes", n)
			}
		})
	}
}

func establishPair(t *testing.T, ctx context.Context, cc *seal.ClientConfig, sc *seal.ServerConfig, cl, sv zmux.PipeIO) (zmux.PipeIO, zmux.PipeIO, error, error) {
	t.Helper()
	type res struct {
		p   zmux.PipeIO
		err error
	}
	ch := make(chan res, 1)
	go func() {
		p, err := sc.WrapPipe(ctx, sv, zmux.PipeHint{})
		if err != nil {
			_ = sv.W.Close()
		}
		ch <- res{p, err}
	}()
	p, err := seal.Establish(ctx, cl, cc)
	r := <-ch
	return p, r.p, err, r.err
}

// Over net.Pipe, a Sealed carrier shows only what its own layer offers.
func TestHiddenCapabilities(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	a, b := net.Pipe()
	carrierExp := bytes.Repeat([]byte{9}, 32)
	wrapped := &laneConn{secureConn{Conn: a, exp: carrierExp}}
	cp, sp, cerr, serr := establishPair(t, ctx, &f.client, &f.server, zmux.NetPipe(wrapped), zmux.NetPipe(b))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	defer func() { _ = cp.W.Close(); _ = sp.W.Close() }()
	for _, x := range []any{cp.R, cp.W} {
		if _, ok := x.(zmux.LaneOpener); ok {
			t.Error("LaneOpener visible")
		}
		if _, ok := x.(zmux.DatagramChannel); ok {
			t.Error("DatagramChannel visible")
		}
		if _, ok := x.(zmux.CarrierSecure); ok {
			t.Error("CarrierSecure visible")
		}
		if _, ok := x.(zmux.ConnectionStater); ok {
			t.Error("ConnectionStater visible")
		}
	}
	e, ok := zmux.Exporter(cp)
	if !ok {
		t.Fatal("no exporter")
	}
	got, err := e.ExportKeyingMaterial(proto.BoundExporterLabel, nil, 32)
	if err != nil || bytes.Equal(got, carrierExp) {
		t.Fatalf("the carrier's exporter passed through: %x %v", got, err)
	}
	se, _ := zmux.Exporter(sp)
	if other, err := se.ExportKeyingMaterial(proto.BoundExporterLabel, nil, 32); err != nil || !bytes.Equal(got, other) {
		t.Fatalf("exporters differ: %v", err)
	}
	if m, ok := cp.W.(zmux.CarrierRecordMode); !ok || m.RecordMode() != zmux.RecordSealed {
		t.Fatal("no Sealed mode")
	}
	if cp.Framing != zmux.FramingRecords {
		t.Fatal("not a message carrier")
	}
}

func TestWrongPSK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	cc := f.client
	cc.PSK = bytes.Repeat([]byte{1}, 32)
	a, b := net.Pipe()
	_, _, cerr, serr := establishPair(t, ctx, &cc, &f.server, zmux.NetPipe(a), zmux.NetPipe(b))
	var rej *zmux.RejectError
	if cerr == nil || !errors.As(serr, &rej) || rej.Reason != "auth" {
		t.Fatalf("client %v server %v", cerr, serr)
	}
}

func TestPeerIsSelf(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	cc := f.client
	cc.Key = f.node
	a, b := net.Pipe()
	_, _, cerr, _ := establishPair(t, ctx, &cc, &f.server, zmux.NetPipe(a), zmux.NetPipe(b))
	if !errors.Is(cerr, proto.ErrPeerIsSelf) {
		t.Fatalf("client %v", cerr)
	}
}

func TestBoundNeedsSecureCarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	cc := f.client
	cc.RecordMode = zmux.RecordBound
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	if _, err := seal.Establish(ctx, zmux.NetPipe(a), &cc); !errors.Is(err, proto.ErrInvalidConfig) {
		t.Fatal(err)
	}
}

// A server takes only Sealed unless configured.
func TestServerRefusesModeByDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	cc := f.client
	cc.RecordMode = zmux.RecordTrusted
	a, b := net.Pipe()
	_, _, cerr, serr := establishPair(t, ctx, &cc, &f.server, zmux.NetPipe(a), zmux.NetPipe(b))
	var rej *zmux.RejectError
	if cerr == nil || !errors.As(serr, &rej) || rej.Reason != "record_mode" {
		t.Fatalf("client %v server %v", cerr, serr)
	}
}

// countConn counts the bytes written.

// A refused Establish reaches the listener's hook.
func TestOnReject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	got := make(chan zmux.RejectInfo, 1)
	_, addr := listen(t, ctx, &zmux.ListenerConfig{
		WrapPipe: &f.server,
		OnReject: func(i zmux.RejectInfo) zmux.RejectAction { got <- i; return zmux.RejectClose },
	})
	cc := f.client
	cc.Handle = []byte("nobody-nobody-16")
	if _, err := seal.Establish(ctx, zmux.NetPipe(dial(t, addr)), &cc); err == nil {
		t.Fatal("established")
	}
	select {
	case i := <-got:
		if i.Reason != "unknown_handle" {
			t.Fatalf("%+v", i)
		}
	case <-ctx.Done():
		t.Fatal("no OnReject")
	}
}

// A lane's first record passes the transform; a plain session start does not.
func TestWrapPipeLaneFirstRecord(t *testing.T) {
	lane, need := admit.FirstRecordIsLane([]byte{0x16, 0xba}, zmux.FramingByteStream)
	if !lane || need != 0 {
		t.Fatal(lane, need)
	}
	if lane, _ := admit.FirstRecordIsLane([]byte{0x16, 0xff}, zmux.FramingByteStream); lane {
		t.Fatal("parameters read as a lane")
	}
	if _, need := admit.FirstRecordIsLane([]byte{0x40}, zmux.FramingByteStream); need != 2 {
		t.Fatal(need)
	}
}

// laneHalf is one end of a carrier connection with lanes over net.Pipe. Lanes the client
// opens reach the listener as carriers of their own, as streams of a QUIC connection do.
type laneHalf struct {
	id   any
	open func(context.Context) (io.WriteCloser, error)
	secureConn
}

func (h *laneHalf) CarrierConn() any { return h.id }
func (h *laneHalf) OpenLane(ctx context.Context) (io.WriteCloser, error) {
	return h.open(ctx)
}

// A lane of a Bound session goes through the transform's WrapPipe like any carrier, and
// its first record passes on to the session.
func TestLaneThroughWrapPipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := newFixture(t)
	f.server.AllowRecordMode = anyMode
	l, err := zmux.NewListener(&zmux.ListenerConfig{
		Config:   &zmux.Config{MaxPipes: 4, AllowRecordMode: anyMode},
		WrapPipe: &f.server,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	exp := bytes.Repeat([]byte{7}, 32)
	conn := new(int)
	var conns sync.WaitGroup
	defer conns.Wait()
	defer cancel()
	half := func(nc net.Conn) *laneHalf {
		return &laneHalf{Conn: nc, exp: exp, id: conn, open: func(context.Context) (io.WriteCloser, error) {
			return nil, errors.New("server opens no lane here")
		}}
	}
	serve := func(p zmux.PipeIO) {
		p.Carrier = zmux.CarrierQUIC
		conns.Go(func() { _ = l.HandlePipe(ctx, p, zmux.PipeHint{}) })
	}
	var lanes atomic.Int64
	open := func(context.Context) (io.WriteCloser, error) {
		a, b := net.Pipe()
		lanes.Add(1)
		serve(zmux.PipeIO{R: half(b)}) // a lane reaches the acceptor as a receiving half
		return a, nil
	}
	a, b := net.Pipe()
	sh := half(b)
	serve(zmux.PipeIO{R: sh, W: sh})
	h := &laneHalf{Conn: a, exp: exp, id: conn, open: open}
	cc := f.client
	cc.RecordMode = zmux.RecordBound
	p, err := seal.Establish(ctx, zmux.PipeIO{R: h, W: h, Carrier: zmux.CarrierQUIC}, &cc)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, &zmux.Config{MaxPipes: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	got := func() (out, in int) {
		for _, ps := range c.PipeStats() {
			out = max(out, ps.LanesOut)
		}
		for _, ps := range s.PipeStats() {
			in = max(in, ps.LanesIn)
		}
		return out, in
	}
	for {
		echo(t, ctx, c, s)
		if out, in := got(); out > 1 && in > 1 {
			break
		}
		if ctx.Err() != nil {
			out, in := got()
			t.Fatalf("lanes out %d, in %d, opened %d", out, in, lanes.Load())
		}
	}
	if m := s.WeakestRecordMode(); m != zmux.RecordBound {
		t.Fatalf("mode %v", m)
	}
}

// A pin passes on the key whatever credential message 2 carries; an Anchor
// that needs a credential fails without one.
func TestAnchorWithoutCredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	for _, tc := range []struct {
		name   string
		anchor seal.Anchor
		cred   *proto.Credential
		ok     bool
	}{
		{"pin, none", f.client.Anchor, nil, true},
		{"credential anchor, none", credAnchor{}, nil, false},
		{"pin, unknown type", f.client.Anchor, &proto.Credential{Type: 77}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc, sc := f.client, f.server
			cc.Anchor = tc.anchor
			sc.Gate = &gate{handle: f.handle, psk: f.psk, cred: tc.cred}
			a, b := net.Pipe()
			cp, sp, cerr, _ := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(a), zmux.NetPipe(b))
			if tc.ok != (cerr == nil) {
				t.Fatal(cerr)
			}
			if cerr == nil {
				_ = cp.W.Close()
				_ = sp.W.Close()
			} else if !errors.Is(cerr, seal.ErrVerifyRejected) {
				t.Fatal(cerr)
			}
		})
	}
}

func TestRawCarrierHints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	f.server.AllowRecordMode = anyMode
	cc := f.client
	cc.RecordMode = zmux.RecordTrusted
	_, cert := testCert(t)
	cl, sv := net.Pipe()
	tc := tls.Client(cl, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // a test carrier
	ts := tls.Server(sv, &tls.Config{Certificates: []tls.Certificate{cert}})
	cp, sp, cerr, serr := establishPair(t, ctx, &cc, &f.server, zmux.SecureTLS(tc), zmux.SecureTLS(ts))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	defer func() { _ = cp.W.Close(); _ = sp.W.Close() }()
	if r, ok := cp.W.(zmux.TLSRecordCarrier); !ok || !r.TLSRecords() {
		t.Fatal("the TLS hint is lost")
	}
	if nc, ok := cp.W.(interface{ NetConn() net.Conn }); !ok || nc.NetConn() != cl {
		t.Fatal("NetConn is lost")
	}
	r := cp.R.(interface{ SetReadDeadline(time.Time) error })
	if err := r.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.R.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
	if err := cp.W.(interface{ SetWriteDeadline(time.Time) error }).SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}

// SecureTLS carries Bound: the session needs no other declaration, and both ends mix the
// same exporter.
func TestSecureTLSBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	f.server.AllowRecordMode = anyMode
	cc := f.client
	cc.RecordMode = zmux.RecordBound
	_, cert := testCert(t)
	cl, sv := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, &cc, &f.server,
		zmux.SecureTLS(tls.Client(cl, &tls.Config{InsecureSkipVerify: true})), //nolint:gosec // a test carrier
		zmux.SecureTLS(tls.Server(sv, &tls.Config{Certificates: []tls.Certificate{cert}})))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
}

func testCert(t *testing.T) (ed25519.PrivateKey, tls.Certificate) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	return priv, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}
