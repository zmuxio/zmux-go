package zmuxquic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/testpeer"
)

// Interoperability over QUIC with another implementation's scripted peer
// (ZMUX_INTEROP_PEER, modes quic-server and quic-client): pipes, lanes and
// 0-RTT.

func startPeer(t *testing.T, args ...string) *testpeer.Process {
	t.Helper()
	return testpeer.Start(t, testpeer.Command(t, "ZMUX_INTEROP_PEER", args...), false)
}

var interopPSK = []byte("zmux interop pre-shared key 0123")

func interopConfig(remembered []byte) *zmux.Config {
	return &zmux.Config{EnableDatagrams: true, SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}}, PipeAuth: zmux.PipeAuth{PSK: interopPSK, EndToEnd: true},
		EarlyData: zmux.EarlyData{Replayable: true, Remembered: remembered}}
}

func command(t *testing.T, ctx context.Context, c *zmux.Conn, cmd string, body []byte) string {
	t.Helper()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetPreamble([]byte(cmd)); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = st.Write(body); _ = st.Close() }()
	b, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("%s: %v", cmd, err)
	}
	return string(b)
}

// bulkAndClose runs parallel bulk echoes (which open lanes), checks the
// lanes, then has the peer close the session.
func bulkAndClose(t *testing.T, ctx context.Context, c *zmux.Conn, p *testpeer.Process) { //nolint:thelper // scenario body, not a helper: failures point inside it
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for range 6 {
		wg.Go(func() {
			msg := make([]byte, 8<<20)
			_, _ = rand.Read(msg)
			st, err := c.OpenStreamSync(ctx)
			if err != nil {
				errs <- err
				return
			}
			_ = st.SetPreamble([]byte("echo"))
			deadline, _ := ctx.Deadline()
			_ = st.SetDeadline(deadline)
			go func() { _, _ = st.Write(msg); _ = st.Close() }()
			got, err := io.ReadAll(st)
			if err == nil && !bytes.Equal(got, msg) {
				err = errors.New("echo mismatch")
			}
			if err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("%v; pipes %+v; session %v", err, c.PipeStats(), context.Cause(c.Context()))
	}
	var lanes uint64
	for _, ps := range c.PipeStats() {
		lanes += ps.LanesOut
	}
	if lanes < 1 {
		t.Errorf("no lanes: %+v", c.PipeStats())
	}
	if r := command(t, ctx, c, "close:7:done", nil); r != "closing" {
		t.Fatalf("close: %q", r)
	}
	var ae *zmux.ApplicationError
	if err := c.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 7 {
		t.Fatalf("close: %v", err)
	}
	p.ExpectNext(t, "CLOSED")
}

func serveInteropListener(t *testing.T, ctx context.Context, l *zmux.Listener, ln *quic.EarlyListener) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	listener := &trackedListener{ConnListener: ln}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Serve(ctx, l, listener)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		listener.closeAll()
	})
}

func TestInteropQUICClient(t *testing.T) {
	p := startPeer(t, "quic-server", "2")
	addr := p.ExpectNext(t, "LISTEN ")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{"qmux-02"},
		ClientSessionCache: &ticketCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), ready: make(chan struct{})}}
	c, err := Dial(ctx, addr, tc, nil, interopConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	p.ExpectNext(t, "ESTABLISHED")
	remembered := c.RememberedParameters()
	waitTicket(t, ctx, tc)
	bulkAndClose(t, ctx, c, p)
	// Reconnect with a TLS ticket and remembered zmux parameters.
	c2, err := Dial(ctx, addr, tc, nil, interopConfig(remembered))
	if err != nil {
		t.Fatal(err)
	}
	if r := command(t, ctx, c2, "early", []byte("zero-rtt")); !strings.HasPrefix(r, "early=zero-rtt;replayable=") {
		t.Fatalf("early: %q", r)
	}
	p.ExpectNext(t, "ESTABLISHED")
	bulkAndClose(t, ctx, c2, p)
}

func TestInteropQUICServer(t *testing.T) {
	if os.Getenv("ZMUX_INTEROP_PEER") == "" {
		t.Skip("ZMUX_INTEROP_PEER not set")
	}
	srv, _ := tlsConfigs(t)
	srv.NextProtos = []string{"qmux-02"}
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", srv, QUICConfig(&quic.Config{Allow0RTT: true}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	l := mustNewListener(&zmux.ListenerConfig{Config: interopConfig(nil)})
	defer func() { _ = l.Close() }()
	serveInteropListener(t, ctx, l, ln)
	p := startPeer(t, "quic-client", ln.Addr().String())
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.ExpectNext(t, "ESTABLISHED")
	bulkAndClose(t, ctx, c, p)
	// The peer's resumed connection sends its first stream in 0-RTT.
	c2, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st, err := c2.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replayable := st.Replayable()
	b, _ := io.ReadAll(st)
	_, _ = st.Write([]byte("ok"))
	_ = st.Close()
	if r := p.ExpectNext(t, "EARLY "); !peerEarlyOK(t, r) {
		t.Fatalf("peer early: %q", r)
	}
	// Replayable holds only until the handshake completes, which on a
	// loopback may be before the stream is accepted.
	t.Logf("early stream replayable when accepted: %v", replayable)
	if string(st.Preamble()) != "early" || string(b) != "early-data" {
		t.Fatalf("early stream: %q %q", st.Preamble(), b)
	}
	p.ExpectNext(t, "ESTABLISHED")
	bulkAndClose(t, ctx, c2, p)
}

// The join secret is derived from the exporter alone. Compare both peer
// exports on a fresh connection and on a reconnect. The pre-Finished
// exporter ordering is exercised separately by TestExporterWaitsForEarlyHandshake.
const interopAppLabel = "EXPORTER-interop-app"

func exporterConfig(remembered []byte) *zmux.Config {
	return &zmux.Config{EnableDatagrams: true, SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}}, PipeAuth: zmux.PipeAuth{Exporter: true, EndToEnd: true},
		EarlyData: zmux.EarlyData{Replayable: true, Remembered: remembered}}
}

// reports reads one line per prefix, in any order, and returns them by
// prefix without it.
func interopReports(t *testing.T, p *testpeer.Process, prefixes ...string) map[string]string {
	t.Helper()
	got := map[string]string{}
	watchdog := time.NewTimer(10 * time.Second)
	defer watchdog.Stop()
next:
	for len(got) < len(prefixes) {
		select {
		case l, ok := <-p.Lines:
			if !ok {
				t.Fatalf("peer ended; have %q", got)
			}
			for _, pre := range prefixes {
				if v, ok := strings.CutPrefix(l, pre); ok {
					got[pre] = v
					continue next
				}
			}
			t.Fatalf("peer: unexpected %q", l)
		case <-watchdog.C:
			t.Fatalf("peer: have %q of %q", got, prefixes)
		}
	}
	return got
}

// exporterKeys compares the session's exporter with the peer's value.
func exporterKeys(t *testing.T, c *zmux.Conn, peer string) {
	t.Helper()
	got, err := c.ExportKeyingMaterial(interopAppLabel, nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	if peer != hex.EncodeToString(got) {
		t.Fatalf("exporter values differ: peer %s, here %x", peer, got)
	}
}

func closeByPeer(t *testing.T, ctx context.Context, c *zmux.Conn, p *testpeer.Process) {
	t.Helper()
	if r := command(t, ctx, c, "close:7:done", nil); r != "closing" {
		t.Fatalf("close: %q", r)
	}
	var ae *zmux.ApplicationError
	if err := c.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 7 {
		t.Fatalf("close: %v", err)
	}
	p.ExpectNext(t, "CLOSED")
}

// Reconnect to the peer server using the native early dialer. The peer
// can accept or decline 0-RTT; compare exporters after establishment.
func TestInteropQUICExporterEarlyToPeerServer(t *testing.T) {
	p := startPeer(t, "quic-server", "2", "exporter")
	addr := p.ExpectNext(t, "LISTEN ")
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	tc := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{"qmux-02"},
		ClientSessionCache: &ticketCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), ready: make(chan struct{})}}
	c, err := Dial(ctx, addr, tc, nil, exporterConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	if r := command(t, ctx, c, "early", []byte("first")); !strings.HasPrefix(r, "early=first;") {
		t.Fatalf("first session: %q", r)
	}
	remembered := c.RememberedParameters()
	exporterKeys(t, c, interopReports(t, p, "ESTABLISHED", "KEYS ")["KEYS "])
	waitTicket(t, ctx, tc)
	closeByPeer(t, ctx, c, p)

	qc, _, err := dialNative(ctx, addr, tc, QUICConfig(nil), true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	s, err := qc.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientLanes(qc)
	c2, err := zmux.Start(ctx, (&qStream{qc: qc, s: s, early: true}).pipe(), zmux.RoleClient, exporterConfig(remembered))
	if err != nil {
		t.Fatal(err)
	}
	r := command(t, ctx, c2, "early", []byte("zero-rtt"))
	if !strings.HasPrefix(r, "early=zero-rtt;replayable=") {
		t.Fatalf("early: %q; session %v", r, context.Cause(c2.Context()))
	}
	if !qc.ConnectionState().Used0RTT {
		// 0-RTT is optional (SPEC Section 6.1.2): a peer server may refuse it,
		// and the stream then goes after the handshake.
		t.Log("the peer's server refused 0-RTT")
	}
	exporterKeys(t, c2, interopReports(t, p, "ESTABLISHED", "KEYS ")["KEYS "])
	closeByPeer(t, ctx, c2, p)
}

// Compare the exporter when the peer reconnects to a Go server.
func TestInteropQUICExporterEarlyFromPeerClient(t *testing.T) {
	if os.Getenv("ZMUX_INTEROP_PEER") == "" {
		t.Skip("ZMUX_INTEROP_PEER not set")
	}
	srv, _ := tlsConfigs(t)
	srv.NextProtos = []string{"qmux-02"}
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", srv, QUICConfig(&quic.Config{Allow0RTT: true}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	l := mustNewListener(&zmux.ListenerConfig{Config: exporterConfig(nil)})
	defer func() { _ = l.Close() }()
	serveInteropListener(t, ctx, l, ln)
	p := startPeer(t, "quic-client", ln.Addr().String(), "exporter")
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exporterKeys(t, c, interopReports(t, p, "ESTABLISHED", "KEYS ")["KEYS "])
	closeByPeer(t, ctx, c, p)

	c2, err := l.Accept(ctx)
	if err != nil {
		for len(p.Lines) > 0 {
			t.Log(<-p.Lines)
		}
		t.Fatal(err)
	}
	st, err := c2.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("%v; session %v", err, context.Cause(c2.Context()))
	}
	b, _ := io.ReadAll(st)
	_, _ = st.Write([]byte("ok"))
	_ = st.Close()
	if string(st.Preamble()) != "early" || string(b) != "early-data" {
		t.Fatalf("early stream: %q %q", st.Preamble(), b)
	}
	rep := interopReports(t, p, "EARLY ", "KEYS ", "ESTABLISHED")
	if r := rep["EARLY "]; !peerEarlyOK(t, r) {
		t.Fatalf("peer early: %q", r)
	}
	exporterKeys(t, c2, rep["KEYS "])
	closeByPeer(t, ctx, c2, p)
}

// peerEarlyOK reports whether a peer's EARLY report shows its early stream
// answered: sent in 0-RTT and accepted, or, from a peer whose client offers
// no 0-RTT (optional, SPEC Section 6), sent after the handshake.
func peerEarlyOK(t *testing.T, r string) bool {
	t.Helper()
	if !strings.HasSuffix(r, " reply=ok") {
		return false
	}
	if strings.HasPrefix(r, "sent=false ") {
		t.Log("the peer's client offers no 0-RTT: its early stream went after the handshake")
		return true
	}
	return strings.HasPrefix(r, "sent=true accepted=Some(true) ") || strings.HasPrefix(r, "sent=true accepted=true ")
}
