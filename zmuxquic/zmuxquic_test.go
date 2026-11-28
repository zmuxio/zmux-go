package zmuxquic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
	"github.com/zmuxio/zmux-go/internal/testcert"
	"github.com/zmuxio/zmux-go/internal/teststream"
)

func tlsConfigs(t testing.TB) (*tls.Config, *tls.Config) {
	t.Helper()
	cert, trust := testcert.Local(t)
	srv := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"zmux"}}
	cli := &tls.Config{RootCAs: trust, ServerName: testcert.LocalName, NextProtos: []string{"zmux"},
		ClientSessionCache: &ticketCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), ready: make(chan struct{})}}
	return srv, cli
}

type ticketCache struct {
	tls.ClientSessionCache
	ready chan struct{}
	once  sync.Once
}

func (c *ticketCache) Put(key string, state *tls.ClientSessionState) {
	c.ClientSessionCache.Put(key, state)
	if state != nil {
		c.once.Do(func() { close(c.ready) })
	}
}

func waitTicket(t *testing.T, ctx context.Context, cfg *tls.Config) {
	t.Helper()
	select {
	case <-cfg.ClientSessionCache.(*ticketCache).ready:
	case <-ctx.Done():
		t.Fatal("session ticket not received")
	}
}

func mpConfig() *zmux.Config {
	return &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}}, PipeAuth: zmux.PipeAuth{Exporter: true, EndToEnd: true}}
}

type env struct {
	ln     *quic.EarlyListener
	lst    *zmux.Listener
	cli    *tls.Config
	cancel context.CancelFunc
	tl     *trackedListener
}

func newEnv(t testing.TB, scfg *zmux.Config) *env {
	t.Helper()
	return newEnvAt(t, "127.0.0.1:0", scfg)
}

func newEnvAt(t testing.TB, address string, scfg *zmux.Config) *env {
	t.Helper()
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
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = Serve(ctx, e.lst, tl)
	}()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		_ = e.lst.Close()
		<-served
		tl.closeAll()
	})
	return e
}

// trackedListener remembers what it accepted: closing a QUIC listener
// leaves established connections alone.
type trackedListener struct {
	ConnListener
	conns []*quic.Conn
	mu    sync.Mutex
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
	msg := make([]byte, n)
	_, _ = rand.Read(msg)
	teststream.Echo(t, t.Context(), c.OpenStreamSync, s.AcceptStream, msg)
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
	var lanes uint64
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
			cfg.SessionPolicy.Limits.SteadyAttachments, cfg.PipeAuth = 4, zmux.PipeAuth{Exporter: true, EndToEnd: true}
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
	qc, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, testQUIC(nil))
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
	waitTicket(t, ctx, e.cli)
	allow.Store(allowSecond)
	secondTLS := e.cli.Clone()
	if !useTicket {
		secondTLS.ClientSessionCache = tls.NewLRUClientSessionCache(4)
	}
	// The second uses replayable parameters and a resumed native connection.
	// Native streams currently wait for handshake completion (upstream race).
	cfg := withAuth(&zmux.Config{EarlyData: zmux.EarlyData{Replayable: true, Remembered: remembered}})
	c2, err := Dial(ctx, e.ln.Addr().String(), secondTLS, testQUIC(nil), cfg)
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
		jc, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, testQUIC(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = jc.CloseWithError(0, "") }()
		pio, err := ConnPipe(ctx, jc)
		if err != nil {
			t.Fatal(err)
		}
		joined, err := c2.AddPipe(ctx, pio, zmux.PipeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := joined.WaitAttached(ctx); err != nil {
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
		cfg.EarlyData = zmux.EarlyData{PlainQMux: true, Replayable: replayable}
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
	p := NewConnPool(e.ln.Addr().String(), e.cli, nil, nil, carrier.PoolLimits{MaxPipesPerConnection: 2})
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
	fates := make([]uint64, 0, 2)
	for range 2 {
		pio, err := p.OpenPipe(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fates = append(fates, pio.FateGroup)
		joined, err := c.AddPipe(ctx, pio, zmux.PipeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := joined.WaitAttached(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if first.FateGroup == 0 || fates[0] != first.FateGroup || fates[1] == first.FateGroup {
		t.Fatalf("fate groups %d %v", first.FateGroup, fates)
	}
	echo(t, c, s, 1<<20)
	for _, endpoint := range []*zmux.Conn{c, s} {
		stats := endpoint.PipeStats()
		if len(stats) != 3 {
			t.Fatalf("%d pipes", len(stats))
		}
		for _, st := range stats {
			sample := st.CarrierSample
			if sample == nil || sample.Source != zmux.CarrierSourceQUICGo || sample.Counts.QUIC == nil ||
				sample.Counts.QUIC.PacketsSent == 0 || sample.Generation != 0 {
				t.Fatalf("pipe %d (out=%v, in=%v, failure=%v): unexpected carrier sample %+v", st.PipeID, st.Outbound, st.Inbound, st.Failure, sample)
			}
		}
		a, b, replacement := stats[0].CarrierSample, stats[1].CarrierSample, stats[2].CarrierSample
		if a.CarrierID != b.CarrierID || a.CarrierID == replacement.CarrierID {
			t.Fatalf("pooled connection identities %v, %v, %v", a.CarrierID, b.CarrierID, replacement.CarrierID)
		}
	}
}

// copyLaneReader signals when the pipe reader asks for more data, after
// processing the preceding records. Once armed, it withholds ACKs so the
// primary copy cannot retire an urgent write before the lane runs.
type copyLaneReader struct {
	*qReader
	progress chan struct{}
	release  chan struct{}
	holdACK  atomic.Bool
}

func (r *copyLaneReader) Read(p []byte) (int, error) {
	select {
	case r.progress <- struct{}{}:
	default:
	}
	n, err := r.qReader.Read(p)
	if r.holdACK.Load() {
		<-r.release
	}
	return n, err
}

type copyLaneOpener struct {
	*qStream
	written chan struct{}
}

func (o *copyLaneOpener) OpenLane(ctx context.Context) (io.WriteCloser, error) {
	w, err := o.qStream.OpenLane(ctx)
	if err != nil {
		return nil, err
	}
	return &observedCopyLane{laneWriter: w.(*laneWriter), written: o.written}, nil
}

type observedCopyLane struct {
	*laneWriter
	written chan struct{}
	header  bool
}

func (w *observedCopyLane) Write(p []byte) (int, error) {
	n, err := w.laneWriter.Write(p)
	if err == nil && w.header {
		select {
		case w.written <- struct{}{}:
		default:
		}
	}
	w.header = true
	return n, err
}

func (w *observedCopyLane) WriteBuffers(p [][]byte) (int, error) { return carrier.WriteFlat(w, p) }

func waitLaneAck(t *testing.T, ctx context.Context, pipe *zmux.Pipe, progress <-chan struct{}, lane uint64) {
	t.Helper()
	for {
		stats, _ := pipe.Stats()
		for _, got := range stats.Lanes {
			if got.Lane == lane && got.PositionAcked > 0 {
				return
			}
		}
		select {
		case <-progress:
		case <-ctx.Done():
			t.Fatalf("no acknowledgement on lane %d: %v", lane, ctx.Err())
		}
	}
}

// TestQUICCopyLane checks redundant urgent data on the single pipe's copy
// lane, after the peer acknowledges both the primary and the lane header.
func TestQUICCopyLane(t *testing.T) {
	conf := mpConfig()
	conf.MaxLanes = 2
	conf.CopyBudgetPercent = zmux.Set(100)
	e := newEnv(t, conf)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	qc, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, testQUIC(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	p, err := ConnPipe(ctx, qc)
	if err != nil {
		t.Fatal(err)
	}
	reader := &copyLaneReader{qReader: p.R.(*qReader), progress: make(chan struct{}, 1), release: make(chan struct{})}
	writer := &copyLaneOpener{qStream: p.W.(*qStream), written: make(chan struct{}, 1)}
	p.R, p.W = reader, writer
	c, err := zmux.Start(ctx, p, zmux.RoleClient, conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); _ = c.Wait() })
	defer close(reader.release)
	s, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = s.Wait() })
	pipe, _ := c.Pipe(0)

	sendUrgent := func(tag byte) {
		t.Helper()
		teststream.Echo(t, ctx, func(ctx context.Context) (*zmux.Stream, error) {
			st, err := c.OpenStreamSync(ctx)
			if err == nil {
				st.SetRedundant(true)
			}
			return st, err
		}, s.AcceptStream, bytes.Repeat([]byte{tag}, 4096))
	}
	sendUrgent(1)
	waitLaneAck(t, ctx, pipe, reader.progress, 0)
	sendUrgent(2)
	waitLaneAck(t, ctx, pipe, reader.progress, 1)

	reader.holdACK.Store(true)
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st.SetRedundant(true)
	if _, err := st.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	select {
	case <-writer.written:
	case <-ctx.Done():
		t.Fatalf("copy lane did not carry redundant data: %v", ctx.Err())
	}
	session := c.ConnectionStats().BytesRedundant
	stats, _ := pipe.Stats()
	if session == 0 || stats.BytesRedundant < session {
		t.Fatalf("redundant bytes: session %d, pipe %d", session, stats.BytesRedundant)
	}
}

// testQUIC is QUICConfig(base) with a handshake timeout that tolerates a
// loaded test host: the default 5 s fails handshakes under -race with
// many packages running at once.
func testQUIC(base *quic.Config) *quic.Config {
	c := QUICConfig(base)
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
	defer func() { _ = ln.Close() }()
	defer func() { _ = lst.Close() }()
	allowCert := make(chan struct{})
	release := sync.OnceFunc(func() { close(allowCert) })
	defer release()
	cli.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		select {
		case <-allowCert:
			return &tls.Certificate{}, nil // the server requires a credential
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	type adopted struct {
		svc *ConnService
		err error
	}
	service := make(chan adopted, 1)
	go func() {
		peer, err := ln.Accept(ctx)
		if err != nil {
			service <- adopted{err: err}
			return
		}
		svc, err := AdoptConn(ctx, lst, peer)
		service <- adopted{svc: svc, err: err}
	}()
	dialDone := make(chan struct{})
	go func() {
		defer close(dialDone)
		if qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, testQUIC(nil)); err == nil {
			select {
			case <-qc.Context().Done():
			case <-ctx.Done():
				_ = qc.CloseWithError(0, "")
			}
		}
	}()
	defer func() { release(); cancel(); <-dialDone }()
	select {
	case r := <-service:
		if r.err != nil {
			t.Fatal(r.err)
		}
		release()
		if err := r.svc.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if st := lst.Stats(); !reflect.DeepEqual(st, zmux.ListenerStats{SetupFailures: 1}) {
		t.Fatalf("stats %+v", st)
	}
}

// PipeIO.MaxRecord applies to every lane of the pipe: lanes are streams of
// the same QUIC connection, and their records are as short as lane 0's.
func TestQUICLanesHonourMaxRecord(t *testing.T) {
	const capRec = zmux.MinMaxRecord
	e := newEnv(t, mpConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	qc, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, testQUIC(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	p, err := ConnPipe(ctx, qc)
	if err != nil {
		t.Fatal(err)
	}
	p.MaxRecord = capRec
	c, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
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
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() { echo(t, c, s, 2<<20) })
	}
	wg.Wait()
	var lanes uint64
	for _, ps := range c.PipeStats() {
		lanes += ps.LanesOut
		// A record is its Size field and at most capRec bytes.
		if ps.RecordsSent == 0 || ps.BytesSent > ps.RecordsSent*(capRec+2) {
			t.Fatalf("pipe: %d bytes in %d records", ps.BytesSent, ps.RecordsSent)
		}
	}
	if lanes < 2 {
		t.Fatalf("no lanes were opened: %+v", c.PipeStats())
	}
}
