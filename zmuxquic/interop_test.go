package zmuxquic

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

// Interoperability over QUIC with another implementation's scripted peer
// (ZMUX_INTEROP_PEER, modes quic-server and quic-client): pipes, lanes and
// 0-RTT.

type peerProc struct{ lines chan string }

func startPeer(t *testing.T, args ...string) *peerProc {
	t.Helper()
	bin := os.Getenv("ZMUX_INTEROP_PEER")
	if bin == "" {
		t.Skip("ZMUX_INTEROP_PEER not set")
	}
	cmd := exec.Command(bin, args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &peerProc{lines: make(chan string, 16)}
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			p.lines <- sc.Text()
		}
		close(p.lines)
	}()
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	return p
}

func (p *peerProc) expect(t *testing.T, prefix string) string {
	t.Helper()
	select {
	case l, ok := <-p.lines:
		if !ok || !strings.HasPrefix(l, prefix) {
			t.Fatalf("peer: want %q, got %q", prefix, l)
		}
		return strings.TrimSpace(strings.TrimPrefix(l, prefix))
	case <-time.After(10 * time.Second):
		t.Fatalf("peer: no %q", prefix)
	}
	return ""
}

var interopPSK = []byte("zmux interop pre-shared key 0123")

func interopConfig(remembered []byte) *zmux.Config {
	return &zmux.Config{EnableDatagrams: true, MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: interopPSK, EndToEnd: true},
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
func bulkAndClose(t *testing.T, ctx context.Context, c *zmux.Conn, p *peerProc) {
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
			_ = st.SetDeadline(time.Now().Add(20 * time.Second))
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
	var lanes int
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
	p.expect(t, "CLOSED")
}

func TestInteropQUICClient(t *testing.T) {
	p := startPeer(t, "quic-server", "2")
	addr := p.expect(t, "LISTEN ")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{"qmux-02"},
		ClientSessionCache: tls.NewLRUClientSessionCache(4)}
	c, err := Dial(ctx, addr, tc, nil, interopConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	p.expect(t, "ESTABLISHED")
	remembered := c.RememberedParameters()
	bulkAndClose(t, ctx, c, p)
	// A resumed connection: the first stream goes in 0-RTT.
	c2, err := Dial(ctx, addr, tc, nil, interopConfig(remembered))
	if err != nil {
		t.Fatal(err)
	}
	if r := command(t, ctx, c2, "early", []byte("zero-rtt")); !strings.HasPrefix(r, "early=zero-rtt;replayable=") {
		t.Fatalf("early: %q", r)
	}
	p.expect(t, "ESTABLISHED")
	bulkAndClose(t, ctx, c2, p)
}

func TestInteropQUICServer(t *testing.T) {
	if os.Getenv("ZMUX_INTEROP_PEER") == "" {
		t.Skip("ZMUX_INTEROP_PEER not set")
	}
	srv, _ := tlsConfigs(t)
	srv.NextProtos = []string{"qmux-02"}
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", srv, Config(&quic.Config{Allow0RTT: true}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	l := mustNewListener(&zmux.ListenerConfig{Config: interopConfig(nil)})
	defer func() { _ = l.Close() }()
	go func() { _ = Serve(ctx, l, ln, nil) }()
	p := startPeer(t, "quic-client", ln.Addr().String())
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.expect(t, "ESTABLISHED")
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
	if r := p.expect(t, "EARLY "); !strings.HasPrefix(r, "sent=true ") || !strings.HasSuffix(r, " reply=ok") {
		t.Fatalf("peer early: %q", r)
	}
	// Replayable holds only until the handshake completes, which on a
	// loopback may be before the stream is accepted.
	t.Logf("early stream replayable when accepted: %v", replayable)
	if string(st.Preamble()) != "early" || string(b) != "early-data" {
		t.Fatalf("early stream: %q %q", st.Preamble(), b)
	}
	p.expect(t, "ESTABLISHED")
	bulkAndClose(t, ctx, c2, p)
}

// Early data with the join secret from the exporter alone:
// the client's parameters arrive in 0-RTT, and a relay holds back
// the server's packets so that its Finished comes well after them. The
// exporter is read at the parameters and must give the value both ends
// have once the handshake completed.

const interopAppLabel = "EXPORTER-interop-app"

// exporterHold is how long the relay holds back the server's packets.
const exporterHold = 150 * time.Millisecond

func exporterConfig(remembered []byte) *zmux.Config {
	return &zmux.Config{EnableDatagrams: true, MaxPipes: 4, PipeAuth: zmux.PipeAuth{Exporter: true, EndToEnd: true},
		EarlyData: zmux.EarlyData{Replayable: true, Remembered: remembered}}
}

// holdRelay forwards UDP to backend, holding back what comes from it; it
// returns the relay's address.
func holdRelay(t *testing.T, backend string, hold time.Duration) string {
	t.Helper()
	back, err := net.ResolveUDPAddr("udp4", backend)
	if err != nil {
		t.Fatal(err)
	}
	front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	conns := map[string]*net.UDPConn{}
	wg.Go(func() {
		buf := make([]byte, 65535)
		for {
			n, from, err := front.ReadFromUDP(buf)
			if err != nil {
				return
			}
			up := conns[from.String()]
			if up == nil {
				if up, err = net.DialUDP("udp4", nil, back); err != nil {
					return
				}
				conns[from.String()] = up
				wg.Go(func() {
					b := make([]byte, 65535)
					for {
						n, err := up.Read(b)
						if err != nil {
							return
						}
						p := bytes.Clone(b[:n])
						time.AfterFunc(hold, func() { _, _ = front.WriteToUDP(p, from) })
					}
				})
			}
			_, _ = up.Write(buf[:n])
		}
	})
	t.Cleanup(func() {
		_ = front.Close()
		time.Sleep(hold) // the writes held back
		for _, up := range conns {
			_ = up.Close()
		}
		wg.Wait()
	})
	return front.LocalAddr().String()
}

// reports reads one line per prefix, in any order, and returns them by
// prefix without it.
func (p *peerProc) reports(t *testing.T, prefixes ...string) map[string]string {
	t.Helper()
	got := map[string]string{}
	timeout := time.After(10 * time.Second)
next:
	for len(got) < len(prefixes) {
		select {
		case l, ok := <-p.lines:
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
		case <-timeout:
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

func closeByPeer(t *testing.T, ctx context.Context, c *zmux.Conn, p *peerProc) {
	t.Helper()
	if r := command(t, ctx, c, "close:7:done", nil); r != "closing" {
		t.Fatalf("close: %q", r)
	}
	var ae *zmux.ApplicationError
	if err := c.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 7 {
		t.Fatalf("close: %v", err)
	}
	p.expect(t, "CLOSED")
}

// A Go client's 0-RTT (bypassing the handshake barrier of the dialers,
// which holds outgoing data back) at the peer's server.
func TestInteropQUICExporterEarlyToPeerServer(t *testing.T) {
	p := startPeer(t, "quic-server", "2", "exporter")
	addr := holdRelay(t, p.expect(t, "LISTEN "), exporterHold)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	tc := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{"qmux-02"},
		ClientSessionCache: tls.NewLRUClientSessionCache(4)}
	c, err := Dial(ctx, addr, tc, nil, exporterConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	if r := command(t, ctx, c, "early", []byte("first")); !strings.HasPrefix(r, "early=first;") {
		t.Fatalf("first session: %q", r)
	}
	remembered := c.RememberedParameters()
	exporterKeys(t, c, p.reports(t, "ESTABLISHED", "KEYS ")["KEYS "])
	closeByPeer(t, ctx, c, p)

	qc, err := dialNative(ctx, addr, tc, Config(nil), true)
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
		t.Fatal("the connection did not use 0-RTT")
	}
	exporterKeys(t, c2, p.reports(t, "ESTABLISHED", "KEYS ")["KEYS "])
	closeByPeer(t, ctx, c2, p)
}

// The peer's 0-RTT at a Go server.
func TestInteropQUICExporterEarlyFromPeerClient(t *testing.T) {
	if os.Getenv("ZMUX_INTEROP_PEER") == "" {
		t.Skip("ZMUX_INTEROP_PEER not set")
	}
	srv, _ := tlsConfigs(t)
	srv.NextProtos = []string{"qmux-02"}
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", srv, Config(&quic.Config{Allow0RTT: true}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	l := mustNewListener(&zmux.ListenerConfig{Config: exporterConfig(nil)})
	defer func() { _ = l.Close() }()
	go func() { _ = Serve(ctx, l, ln, nil) }()
	p := startPeer(t, "quic-client", holdRelay(t, ln.Addr().String(), exporterHold), "exporter")
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exporterKeys(t, c, p.reports(t, "ESTABLISHED", "KEYS ")["KEYS "])
	closeByPeer(t, ctx, c, p)

	c2, err := l.Accept(ctx)
	if err != nil {
		for len(p.lines) > 0 {
			t.Log(<-p.lines)
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
	rep := p.reports(t, "EARLY ", "KEYS ", "ESTABLISHED")
	if r := rep["EARLY "]; !strings.HasPrefix(r, "sent=true accepted=Some(true) ") || !strings.HasSuffix(r, " reply=ok") {
		t.Fatalf("peer early: %q", r)
	}
	exporterKeys(t, c2, rep["KEYS "])
	closeByPeer(t, ctx, c2, p)
}
