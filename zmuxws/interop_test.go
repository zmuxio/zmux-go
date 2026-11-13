package zmuxws_test

// Interoperability over wss:// with another implementation's scripted
// peer (ZMUX_INTEROP_PEER, modes wss-server and wss-client <addr>): the
// session secret from the bootstrap WebSocket's TLS exporter alone, a
// Bound join on a further WebSocket, both ends printing the exporter
// label EXPORTER-interop-app (16 bytes).

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"io"
	"math/big"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxws"
)

const wssAppLabel = "EXPORTER-interop-app"

type wssPeer struct{ lines chan string }

func startWSSPeer(t *testing.T, args ...string) *wssPeer {
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
	p := &wssPeer{lines: make(chan string, 16)}
	done := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			p.lines <- sc.Text()
		}
		close(p.lines)
		done <- cmd.Wait()
	}()
	t.Cleanup(func() {
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("peer: %v", err)
			}
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			t.Error("peer did not exit")
		}
	})
	return p
}

func (p *wssPeer) expect(t *testing.T, prefix string) string {
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

func wssConfig() *zmux.Config {
	return &zmux.Config{EnableDatagrams: true, MaxPipes: 4,
		PipeAuth:        zmux.PipeAuth{Exporter: true, EndToEnd: true},
		AllowRecordMode: func(zmux.RecordPolicyInput, zmux.RecordMode, any) bool { return true },
		// A join on a carrier declared secure (a WebSocket on a TLS
		// connection of its own) is Bound.
		RecordPolicy: func(c zmux.RecordPolicyInput) zmux.RecordMode {
			if c.Secure {
				return zmux.RecordBound
			}
			return zmux.RecordSealed
		}}
}

func wssCert(t *testing.T) *tls.Config {
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
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion: tls.VersionTLS13, NextProtos: []string{"http/1.1"}}
}

// wssCheck compares the exporter values, waits for the join, echoes and
// closes the session.
func wssCheck(t *testing.T, ctx context.Context, c *zmux.Conn, p *wssPeer) {
	t.Helper()
	got, err := c.ExportKeyingMaterial(wssAppLabel, nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	if peer := p.expect(t, "KEYS "); peer != hex.EncodeToString(got) {
		t.Fatalf("exporter values differ: peer %s, here %x", peer, got)
	}
	p.expect(t, "ESTABLISHED")
	deadline := time.Now().Add(10 * time.Second)
	for len(c.PipeStats()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	ps := c.PipeStats()
	if len(ps) < 2 {
		t.Fatalf("%d pipes, want the joined one too", len(ps))
	}
	for _, s := range ps[1:] {
		if s.RecordMode != zmux.RecordBound {
			t.Fatalf("join in record mode %v, want Bound: %+v", s.RecordMode, ps)
		}
	}
	msg := make([]byte, 1<<20)
	_, _ = rand.Read(msg)
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = st.SetPreamble([]byte("echo"))
	go func() { _, _ = st.Write(msg); _ = st.Close() }()
	back, err := io.ReadAll(st)
	if err != nil || !bytes.Equal(back, msg) {
		t.Fatalf("echo: %v", err)
	}
	_ = c.Close()
	p.expect(t, "CLOSED")
}

func TestInteropWSSClient(t *testing.T) {
	p := startWSSPeer(t, "wss-server")
	addr := p.expect(t, "LISTEN ")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	o := &zmuxws.DialOptions{TLSConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}}
	url := "wss://" + addr + "/zmux"
	c, err := zmuxws.Dial(ctx, url, o, wssConfig())
	if err != nil {
		t.Fatal(err)
	}
	join, err := zmuxws.DialPipe(ctx, url, o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddPipe(ctx, join, zmux.PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	wssCheck(t, ctx, c, p)
}

func TestInteropWSSServer(t *testing.T) {
	if os.Getenv("ZMUX_INTEROP_PEER") == "" {
		t.Skip("ZMUX_INTEROP_PEER not set")
	}
	l, err := zmux.NewListener(&zmux.ListenerConfig{Config: wssConfig()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	h, err := zmuxws.NewHandler(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	tl, err := tls.Listen("tcp", "127.0.0.1:0", wssCert(t))
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(tl) }()
	defer func() { _ = srv.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p := startWSSPeer(t, "wss-client", tl.Addr().String())
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wssCheck(t, ctx, c, p)
}
