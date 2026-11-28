package zmuxws_test

// Interoperability over wss:// with another implementation's scripted
// peer (ZMUX_INTEROP_PEER, modes wss-server and wss-client <addr>): the
// session secret from the bootstrap WebSocket's TLS exporter alone, a
// Bound join on a further WebSocket, both ends printing the exporter
// label EXPORTER-interop-app (16 bytes).

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/testcert"
	"github.com/zmuxio/zmux-go/internal/testpeer"
	"github.com/zmuxio/zmux-go/zmuxws"
)

const wssAppLabel = "EXPORTER-interop-app"

func startWSSPeer(t *testing.T, args ...string) *testpeer.Process {
	t.Helper()
	return testpeer.Start(t, testpeer.Command(t, "ZMUX_INTEROP_PEER", args...), true)
}

func wssConfig() *zmux.Config {
	return &zmux.Config{EnableDatagrams: true, SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}},
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
	cert, _ := testcert.Local(t)
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13, NextProtos: []string{"http/1.1"}}
}

// wssCheck compares the exporter values, waits for the join, echoes and
// closes the session.
func wssCheck(t *testing.T, ctx context.Context, c *zmux.Conn, p *testpeer.Process) {
	t.Helper()
	got, err := c.ExportKeyingMaterial(wssAppLabel, nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	if peer := p.Expect(t, "KEYS "); peer != hex.EncodeToString(got) {
		t.Fatalf("exporter values differ: peer %s, here %x", peer, got)
	}
	p.Expect(t, "ESTABLISHED")
	for len(c.PipeStats()) < 2 {
		if _, err := c.NextEvent(ctx); err != nil {
			t.Fatalf("the peer's joined pipe did not arrive: %v", err)
		}
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
	p.Expect(t, "CLOSED")
}

func TestInteropWSSClient(t *testing.T) {
	p := startWSSPeer(t, "wss-server")
	addr := p.Expect(t, "LISTEN ")
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

// The peer prints some reports from concurrent tasks; expect must hold the
// line it passes over for the next expect.
