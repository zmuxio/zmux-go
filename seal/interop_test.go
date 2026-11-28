package seal_test

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/testpeer"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

// Interoperability of Establish with another implementation's scripted peer,
// the binary named by ZMUX_INTEROP_PEER (the one the root module's interop
// tests run). It takes "seal-server <profiles>" (comma-separated; prints
// "LISTEN addr") or "seal-client <addr> <profile>" (prints "PROFILE n"),
// prints "ESTABLISHED" and "KEYS <hex>" (EXPORTER-interop-app, 16 bytes),
// answers the stream commands "echo" and "close:<code>:<reason>", and prints
// "CLOSED". The node's static key is 32 bytes of 0x5a, pinned by the client;
// the Handle and pre-shared key derive from interopSecret with the domain
// "interop".
//
//	ZMUX_INTEROP_PEER=/path/to/peer go test -run Interop .

const interopSecret = "zmux interop pre-shared key 0123"

var interopNode = bytes.Repeat([]byte{0x5a}, 32)

var interopProfiles = []proto.Profile{proto.ProfileEstablishPSK, proto.ProfileEstablishPSKAES, proto.ProfileEstablish, proto.ProfileEstablishAES}

func interopKeys(t *testing.T) (handle, psk []byte) {
	t.Helper()
	h, p, err := seal.DeriveHandlePSK([]byte(interopSecret), []byte("interop"), nil)
	if err != nil {
		t.Fatal(err)
	}
	return h, p
}

func startInteropPeer(t *testing.T, args ...string) *testpeer.Process {
	t.Helper()
	return testpeer.Start(t, testpeer.Command(t, "ZMUX_INTEROP_PEER", args...), true)
}

// interopScript compares the exporters, echoes 1 MiB, and has the peer close
// the session with an application error.
func interopScript(t *testing.T, ctx context.Context, c *zmux.Conn, p *testpeer.Process) {
	t.Helper()
	got, err := c.ExportKeyingMaterial("EXPORTER-interop-app", nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	if theirs := p.Expect(t, "KEYS "); theirs != hex.EncodeToString(got) {
		t.Fatalf("exporters differ: peer %s, here %x", theirs, got)
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
		t.Fatalf("echo: %v, %d bytes", err, len(back))
	}
	if st, err = c.OpenStreamSync(ctx); err != nil {
		t.Fatal(err)
	}
	_ = st.SetPreamble([]byte("close:7:bye"))
	_ = st.Close()
	err = c.Wait()
	var ae *zmux.ApplicationError
	if !errors.As(err, &ae) || ae.ErrorCode != 7 || ae.ErrorMessage != "bye" || !ae.Remote {
		t.Fatalf("close: %v", err)
	}
	p.Expect(t, "CLOSED")
}

func interopConfig() *zmux.Config { return &zmux.Config{EnableDatagrams: true} }

func TestInteropEstablishClient(t *testing.T) {
	for _, pr := range interopProfiles {
		t.Run(strconv.FormatUint(uint64(pr), 10), func(t *testing.T) {
			p := startInteropPeer(t, "seal-server", strconv.FormatUint(uint64(pr), 10))
			addr := p.Expect(t, "LISTEN ")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			nc, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			nk, err := ecdh.X25519().NewPrivateKey(interopNode)
			if err != nil {
				t.Fatal(err)
			}
			cfg := &seal.ClientConfig{
				Anchor:          seal.PinAnchor(seal.StaticKeyID(nk.PublicKey().Bytes())),
				AllowedProfiles: []proto.Profile{pr},
			}
			if pr.HasPSK() {
				cfg.Handle, cfg.PSK = interopKeys(t)
			}
			q, err := seal.Establish(ctx, zmux.NetPipe(nc), cfg)
			if err != nil {
				t.Fatal(err)
			}
			c, err := zmux.Start(ctx, q, zmux.RoleClient, interopConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			p.Expect(t, "ESTABLISHED")
			interopScript(t, ctx, c, p)
		})
	}
}

func TestInteropEstablishServer(t *testing.T) {
	for _, pr := range interopProfiles {
		t.Run(strconv.FormatUint(uint64(pr), 10), func(t *testing.T) {
			if os.Getenv("ZMUX_INTEROP_PEER") == "" {
				t.Skip("ZMUX_INTEROP_PEER not set")
			}
			h, k := interopKeys(t)
			nk, err := ecdh.X25519().NewPrivateKey(interopNode)
			if err != nil {
				t.Fatal(err)
			}
			srv := &seal.ServerConfig{Gate: &gate{handle: h, psk: k}, StaticKey: nk, AcceptedProfiles: []proto.Profile{pr}}
			l, err := zmux.NewListener(&zmux.ListenerConfig{Config: interopConfig(), WrapPipe: srv})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = l.Close() }()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			served := make(chan struct{})
			go func() { defer close(served); _ = l.Serve(ctx, ln) }()
			defer func() { cancel(); _ = ln.Close(); <-served }()
			p := startInteropPeer(t, "seal-client", ln.Addr().String(), strconv.FormatUint(uint64(pr), 10))
			if got := p.Expect(t, "PROFILE "); got != strconv.FormatUint(uint64(pr), 10) {
				t.Fatalf("profile %s", got)
			}
			c, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			p.Expect(t, "ESTABLISHED")
			interopScript(t, ctx, c, p)
		})
	}
}

// The peer prints some reports from concurrent tasks; expect must hold the
// line it passes over for the next expect.
