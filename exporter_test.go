package zmux

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// kmConn is a carrier with an exporter of its own (a transform that ran a
// handshake): sha256(secret, label, context). It records how many bytes
// had been read when the exporter was first read.
type kmConn struct {
	net.Conn
	secret     string
	fail       error
	read       atomic.Int64
	calls      atomic.Int32
	readAtCall atomic.Int64
}

func (k *kmConn) Read(b []byte) (int, error) {
	n, err := k.Conn.Read(b)
	k.read.Add(int64(n))
	return n, err
}

func (k *kmConn) ExportKeyingMaterial(label string, ctx []byte, n int) ([]byte, error) {
	if k.calls.Add(1) == 1 {
		k.readAtCall.Store(k.read.Load())
	}
	if k.fail != nil {
		return nil, k.fail
	}
	h := sha256.Sum256([]byte(k.secret + "\x00" + label + "\x00" + string(ctx)))
	return h[:n], nil
}

// kmPair is a connected pair of exporting carriers sharing secret.
func kmPair(secret string) (*kmConn, *kmConn) {
	a, b := net.Pipe()
	return &kmConn{Conn: a, secret: secret}, &kmConn{Conn: b, secret: secret}
}

func exporterConfig() *Config {
	return &Config{MaxPipes: 4, PipeAuth: PipeAuth{Exporter: true},
		SchedulerParams: SchedulerParams{NoProbation: true}}
}

// A transform's own exporter keys the session: read once, by the bootstrap
// reader after the carrier delivered the peer's first bytes, and a
// carrier-bound join over a further such carrier succeeds, while one whose
// exporter differs does not.
func TestExporterTransform(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lst := NewListener(&ListenerConfig{Config: exporterConfig()})
	defer func() { _ = lst.Close() }()
	serve := func(k *kmConn) { go func() { _ = lst.HandlePipe(ctx, PipeIO{R: k, W: k}, JoinHint{}) }() }
	ck, sk := kmPair("s0")
	serve(sk)
	c, err := Start(ctx, PipeIO{R: ck, W: ck}, RoleClient, exporterConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	for _, k := range []*kmConn{ck, sk} {
		if n := k.calls.Load(); n != 1 {
			t.Fatalf("exporter read %d times", n)
		}
		if k.readAtCall.Load() == 0 {
			t.Fatal("exporter read before the carrier delivered anything")
		}
	}
	cj, sj := kmPair("j1")
	serve(sj)
	if _, err := c.AddPipe(ctx, PipeIO{R: cj, W: cj}, PipeOptions{CarrierBound: true}); err != nil {
		t.Fatalf("carrier-bound join: %v", err)
	}
	echoOnce(t, ctx, c, s)
	cb, sb := kmPair("j2")
	sb.secret = "other"
	serve(sb)
	jctx, jcancel := context.WithTimeout(ctx, time.Second)
	defer jcancel()
	if _, err := c.AddPipe(jctx, PipeIO{R: cb, W: cb}, PipeOptions{CarrierBound: true}); err == nil {
		t.Fatal("join with a different exporter succeeded")
	}
}

// A failed exporter read closes the session with INTERNAL_ERROR.
func TestExporterReadFails(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lst := NewListener(&ListenerConfig{Config: exporterConfig()})
	defer func() { _ = lst.Close() }()
	ck, sk := kmPair("s0")
	ck.fail = errors.New("handshake incomplete")
	go func() { _ = lst.HandlePipe(ctx, PipeIO{R: sk, W: sk}, JoinHint{}) }()
	c, err := Start(ctx, PipeIO{R: ck, W: ck}, RoleClient, exporterConfig())
	if err == nil {
		_ = c.Close()
		t.Fatal("session started without its exporter")
	}
	if !strings.Contains(err.Error(), "exporter: handshake incomplete") {
		t.Fatalf("error %v", err)
	}
}

// A carrier without an exporter cannot start a session with
// PipeAuth.Exporter.
func TestExporterMissing(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	_, err := Start(context.Background(), NetPipe(a), RoleClient, exporterConfig())
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error %v", err)
	}
}

// exportOnly is a half with an exporter and no TLS state (a transform
// that terminates a handshake of its own and forwards no lower state).
type exportOnly struct {
	net.Conn
	km *kmConn
}

func (e *exportOnly) ExportKeyingMaterial(l string, c []byte, n int) ([]byte, error) {
	return e.km.ExportKeyingMaterial(l, c, n)
}

// stateForward is a transparent transform that forwards the TLS state of
// the connection below.
type stateForward struct {
	net.Conn
	below *tls.Conn
}

func (s *stateForward) ConnectionState() tls.ConnectionState { return s.below.ConnectionState() }

// Exporter finds a transform's own exporter before any TLS state, adapts
// a forwarded terminated TLS state, and finds nothing on a carrier whose
// TLS handshake has not completed.
func TestExporterSources(t *testing.T) {
	leakcheck.Check(t)
	scfg, ccfg := selfSignedTLS(t)
	scfg.MinVersion, ccfg.MinVersion = tls.VersionTLS13, tls.VersionTLS13
	a, b := tcpPair(t)
	sc, cc := tls.Server(a, scfg), tls.Client(b, ccfg)
	defer func() { _ = sc.Close(); _ = cc.Close() }()
	if _, ok := Exporter(NetPipe(cc)); ok {
		t.Fatal("exporter before the TLS handshake")
	}
	done := make(chan error, 1)
	go func() { done <- sc.Handshake() }()
	if err := cc.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	st := cc.ConnectionState()
	want, _ := st.ExportKeyingMaterial("l", nil, 32)
	fw := &stateForward{Conn: cc, below: cc}
	e, ok := Exporter(PipeIO{R: fw, W: fw})
	if !ok {
		t.Fatal("no exporter through a forwarded TLS state")
	}
	if got, _ := e.ExportKeyingMaterial("l", nil, 32); !bytes.Equal(got, want) {
		t.Fatal("forwarded exporter differs from the TLS one")
	}
	own := &exportOnly{Conn: cc, km: &kmConn{secret: "own"}}
	if _, ok := TerminatedTLS(PipeIO{R: own, W: own}); ok {
		t.Fatal("TerminatedTLS on a transform that forwards no TLS state")
	}
	e, ok = Exporter(PipeIO{R: own, W: own})
	if !ok {
		t.Fatal("no exporter on the transform")
	}
	if got, _ := e.ExportKeyingMaterial("l", nil, 32); bytes.Equal(got, want) {
		t.Fatal("transform exporter is the lower TLS one")
	}
}

// A split bootstrap with two different exporters keys the session with the
// uplink's followed by the downlink's; one exporter on both halves counts
// once.
func TestExporterSplit(t *testing.T) {
	up, down := &kmConn{secret: "up"}, &kmConn{secret: "down"}
	u, _ := up.ExportKeyingMaterial(wire.ExporterSession, nil, 32)
	d, _ := down.ExportKeyingMaterial(wire.ExporterSession, nil, 32)
	up.calls.Store(0)
	down.calls.Store(0)
	for _, tc := range []struct {
		name      string
		p         PipeIO
		initiator bool
		want      []byte
	}{
		{"initiator", PipeIO{W: up, R: down}, true, append(append([]byte{}, u...), d...)},
		{"responder", PipeIO{R: up, W: down}, false, append(append([]byte{}, u...), d...)},
		{"one", PipeIO{R: up, W: up}, true, u},
		{"uplink only", PipeIO{W: up, R: &net.TCPConn{}}, true, u},
	} {
		e, err := sessionExporter(tc.p, tc.initiator)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.value(); err == nil {
			t.Fatalf("%s: value before load", tc.name)
		}
		e.load()
		got, err := e.value()
		if err != nil || !bytes.Equal(got, tc.want) {
			t.Fatalf("%s: %x %v", tc.name, got, err)
		}
	}
}
