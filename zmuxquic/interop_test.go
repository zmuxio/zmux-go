package zmuxquic

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	zmux "github.com/zmuxio/zmux-go"
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
		go func() { cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
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
	return &zmux.Config{EnableDatagrams: true, MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: interopPSK},
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
	go func() { st.Write(body); st.Close() }()
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
			rand.Read(msg)
			st, err := c.OpenStreamSync(ctx)
			if err != nil {
				errs <- err
				return
			}
			st.SetPreamble([]byte("echo"))
			st.SetDeadline(time.Now().Add(20 * time.Second))
			go func() { st.Write(msg); st.Close() }()
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
		t.Fatalf("%v; pipes %+v; session %v", err, c.Pipes(), context.Cause(c.Context()))
	}
	var lanes int
	for _, ps := range c.Pipes() {
		lanes += ps.Lanes
	}
	if lanes < 1 {
		t.Errorf("no lanes: %+v", c.Pipes())
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
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", srv, QUICConfig(&quic.Config{Allow0RTT: true}))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	l := zmux.NewListener(&zmux.ListenerConfig{Config: interopConfig(nil)})
	defer l.Close()
	go Serve(ctx, ln, l)
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
	st.Write([]byte("ok"))
	st.Close()
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
