package zmux

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

var joinSecret = bytes.Repeat([]byte{9}, 32)

func deferredConfig() *Config {
	c := mpConfig()
	c.PipeAuth = PipeAuth{DeferredPSK: true}
	return c
}

// tryPipe joins a carrier to the client within d.
func (e *mpEnv) tryPipe(d time.Duration) (*Pipe, error) {
	nc, err := net.Dial("tcp", e.ln.Addr().String())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	p, err := e.client.AddPipe(ctx, NetPipe(nc), PipeOptions{})
	if err != nil {
		_ = nc.Close()
	}
	return p, err
}

func mustSet(t *testing.T, c *Conn, psk []byte) {
	t.Helper()
	if err := c.SetJoinSecret(psk); err != nil {
		t.Fatal(err)
	}
}

// Both ends defer the PSK: joins wait for it and, once both supplied it,
// a join verifies and a session that lost its pipes resumes.
func TestJoinSecretBothDeferred(t *testing.T) {
	leakcheck.Check(t)
	var ln net.Listener
	ccfg := deferredConfig()
	ccfg.ResumeTimeout = 5 * time.Second
	ccfg.Redial = func(context.Context, Direction) (PipeIO, error) {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return PipeIO{}, err
		}
		return NetPipe(nc), nil
	}
	scfg := deferredConfig()
	scfg.ResumeTimeout = 5 * time.Second
	e := newMPEnv(t, ccfg, scfg)
	ln = e.ln
	for _, c := range []*Conn{e.client, e.server} {
		if st := c.ConnectionState(); !st.Multipath || !st.JoinSecretPending {
			t.Fatalf("state %+v", st)
		}
	}
	if _, err := e.tryPipe(time.Second); !errors.Is(err, ErrJoinSecretPending) {
		t.Fatalf("join before the secret: %v", err)
	}
	mustSet(t, e.client, joinSecret)
	mustSet(t, e.server, joinSecret)
	if e.client.ConnectionState().JoinSecretPending {
		t.Fatal("still pending")
	}
	if err := e.client.SetJoinSecret(joinSecret); !errors.Is(err, ErrJoinSecretSet) {
		t.Fatalf("second SetJoinSecret: %v", err)
	}
	if _, err := e.tryPipe(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	echoOnce(t, ctx, e.client, e.server)
	e.client.lock()
	var pds []*pipeDrv
	for _, pd := range e.client.pipes {
		pds = append(pds, pd)
	}
	e.client.unlock()
	for _, pd := range pds {
		pd.closeAll()
	}
	for {
		ev, err := e.client.NextEvent(ctx)
		if err != nil {
			t.Fatalf("no resumption: %v", err)
		}
		if ev.Kind == EventSessionResumed {
			break
		}
	}
	echoOnce(t, ctx, e.client, e.server)
	if n := e.server.ConnectionStats().JoinAuthFailures; n != 0 {
		t.Fatalf("join auth failures %d", n)
	}
}

// The client supplies the secret first: its join is held at the server
// until the server supplies it, then accepted.
func TestJoinSecretServerLate(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, deferredConfig(), deferredConfig())
	mustSet(t, e.client, joinSecret)
	done := make(chan error, 1)
	go func() { _, err := e.tryPipe(5 * time.Second); done <- err }()
	waitUntil(t, 5*time.Second, "held join", func() bool { return e.server.ConnectionStats().JoinSecretPending == 1 })
	mustSet(t, e.server, joinSecret)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if st := e.server.ConnectionStats(); st.JoinAuthFailures != 0 || st.JoinSecretPending != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// The server supplies it first: the client's joins fail with
// ErrJoinSecretPending until it supplies its own.
func TestJoinSecretClientLate(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, deferredConfig(), deferredConfig())
	mustSet(t, e.server, joinSecret)
	if _, err := e.tryPipe(time.Second); !errors.Is(err, ErrJoinSecretPending) {
		t.Fatalf("join before the client's secret: %v", err)
	}
	if e.client.JoinReady() {
		t.Fatal("JoinReady while the secret is pending")
	}
	mustSet(t, e.client, joinSecret)
	if _, err := e.tryPipe(5 * time.Second); err != nil {
		t.Fatal(err)
	}
}

// Different secrets: the join fails authentication.
func TestJoinSecretMismatch(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, deferredConfig(), deferredConfig())
	mustSet(t, e.client, joinSecret)
	mustSet(t, e.server, bytes.Repeat([]byte{8}, 32))
	if _, err := e.tryPipe(time.Second); err == nil {
		t.Fatal("join with a different secret succeeded")
	}
	waitUntil(t, 5*time.Second, "auth failure", func() bool { return e.server.ConnectionStats().JoinAuthFailures == 1 })
}

// A deferred PSK and a static PSK of the same bytes interoperate.
func TestJoinSecretDeferredAndStatic(t *testing.T) {
	leakcheck.Check(t)
	scfg := mpConfig()
	scfg.PipeAuth.PSK = joinSecret
	e := newMPEnv(t, deferredConfig(), scfg)
	if e.server.ConnectionState().JoinSecretPending {
		t.Fatal("static end pending")
	}
	mustSet(t, e.client, joinSecret)
	if _, err := e.tryPipe(5 * time.Second); err != nil {
		t.Fatal(err)
	}
}

// Without the peer's P the PSK is not selected: SetJoinSecret fails and
// the exporter alone keys the joins.
func TestJoinSecretNotSelected(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scfg := exporterConfig()
	lst := NewListener(&ListenerConfig{Config: scfg})
	defer func() { _ = lst.Close() }()
	ck, sk := kmPair("s0")
	go func() { _ = lst.HandlePipe(ctx, PipeIO{R: sk, W: sk}, JoinHint{}) }()
	ccfg := exporterConfig()
	ccfg.PipeAuth.DeferredPSK = true
	c, err := Start(ctx, PipeIO{R: ck, W: ck}, RoleClient, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	if err := c.SetJoinSecret(joinSecret); !errors.Is(err, ErrNotNegotiated) {
		t.Fatalf("SetJoinSecret without the peer's P: %v", err)
	}
	if c.ConnectionState().JoinSecretPending {
		t.Fatal("pending without P")
	}
	cj, sj := kmPair("j")
	go func() { _ = lst.HandlePipe(ctx, PipeIO{R: sj, W: sj}, JoinHint{}) }()
	if _, err := c.AddPipe(ctx, PipeIO{R: cj, W: cj}, PipeOptions{}); err != nil {
		t.Fatal(err)
	}
}

// Exporter and deferred PSK on both ends: the keys combine both, so that
// a join verifies only once both supplied the same secret.
func TestJoinSecretWithExporter(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := func() *Config { c := exporterConfig(); c.PipeAuth.DeferredPSK = true; return c }
	lst := NewListener(&ListenerConfig{Config: cfg()})
	defer func() { _ = lst.Close() }()
	ck, sk := kmPair("s0")
	go func() { _ = lst.HandlePipe(ctx, PipeIO{R: sk, W: sk}, JoinHint{}) }()
	c, err := Start(ctx, PipeIO{R: ck, W: ck}, RoleClient, cfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	if !c.ConnectionState().JoinSecretPending {
		t.Fatal("not pending")
	}
	mustSet(t, c, joinSecret)
	mustSet(t, s, joinSecret)
	if n := ck.calls.Load(); n != 1 {
		t.Fatalf("exporter read %d times", n)
	}
	cj, sj := kmPair("j")
	go func() { _ = lst.HandlePipe(ctx, PipeIO{R: sj, W: sj}, JoinHint{}) }()
	if _, err := c.AddPipe(ctx, PipeIO{R: cj, W: cj}, PipeOptions{}); err != nil {
		t.Fatal(err)
	}
}

// A held join is closed silently after the handshake timeout: nothing is
// written to it.
func TestJoinSecretHoldTimeout(t *testing.T) {
	leakcheck.Check(t)
	scfg := deferredConfig()
	scfg.HandshakeTimeout = 300 * time.Millisecond
	e := newMPEnv(t, deferredConfig(), scfg)
	mustSet(t, e.client, joinSecret)
	nc, err := net.Dial("tcp", e.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var read atomic.Int64
	counted := countingConn{Conn: nc, n: &read}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := e.client.AddPipe(ctx, NetPipe(counted), PipeOptions{}); err == nil {
		t.Fatal("held join was accepted")
	}
	if n := read.Load(); n != 0 {
		t.Fatalf("the held join was answered with %d bytes", n)
	}
	if st := e.server.ConnectionStats(); st.JoinSecretPending != 1 || st.JoinAuthFailures != 0 {
		t.Fatalf("stats %+v", st)
	}
}

type countingConn struct {
	net.Conn
	n *atomic.Int64
}

func (c countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.n.Add(int64(n))
	return n, err
}

// Configuration errors.
func TestJoinSecretConfig(t *testing.T) {
	leakcheck.Check(t)
	for _, a := range []PipeAuth{
		{DeferredPSK: true, PSK: joinSecret},
		{DeferredPSK: true, AllowInsecure: true},
	} {
		cfg := mpConfig()
		cfg.PipeAuth = a
		c1, c2 := net.Pipe()
		_, err := Start(context.Background(), NetPipe(c1), RoleClient, cfg)
		_ = c1.Close()
		_ = c2.Close()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: %v", a, err)
		}
	}
	e := newMPEnv(t, mpConfig(), mpConfig())
	if err := e.client.SetJoinSecret(joinSecret); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("without DeferredPSK: %v", err)
	}
	e2 := newMPEnv(t, deferredConfig(), deferredConfig())
	if err := e2.client.SetJoinSecret(joinSecret[:15]); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("short: %v", err)
	}
	_ = e2.client.Close()
	waitDone(t, e2.client)
	if err := e2.client.SetJoinSecret(joinSecret); err == nil {
		t.Fatal("SetJoinSecret on a closed session")
	}
}
