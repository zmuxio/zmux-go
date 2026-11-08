package seal_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
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

type interopPeer struct {
	lines chan string
}

// startInteropPeer runs the peer; it must exit within five seconds of the
// test's end, or it is killed.
func startInteropPeer(t *testing.T, args ...string) *interopPeer {
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
	p := &interopPeer{lines: make(chan string, 16)}
	done := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(out)
		var outErr error
		for sc.Scan() {
			select {
			case p.lines <- sc.Text():
			default:
				outErr = errors.New("peer output queue exceeded its bound")
				_ = cmd.Process.Kill()
			}
			if outErr != nil {
				break
			}
		}
		close(p.lines)
		_, _ = io.Copy(io.Discard, out)
		if err := cmd.Wait(); outErr == nil {
			outErr = err
		}
		done <- outErr
	}()
	t.Cleanup(func() {
		select {
		case err := <-done:
			if err != nil && !t.Failed() {
				t.Errorf("peer failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			err := <-done
			if !t.Failed() {
				t.Errorf("peer did not exit: %v", err)
			}
		}
	})
	return p
}

// expect skips lines up to the first with prefix.
func (p *interopPeer) expect(t *testing.T, prefix string) string {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case l, ok := <-p.lines:
			if !ok {
				t.Fatalf("peer exited before %q", prefix)
			}
			if strings.HasPrefix(l, prefix) {
				return strings.TrimSpace(strings.TrimPrefix(l, prefix))
			}
		case <-deadline:
			t.Fatalf("peer: no %q", prefix)
		}
	}
}

// interopScript compares the exporters, echoes 1 MiB, and has the peer close
// the session with an application error.
func interopScript(t *testing.T, ctx context.Context, c *zmux.Conn, p *interopPeer) {
	t.Helper()
	got, err := c.ExportKeyingMaterial("EXPORTER-interop-app", nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	if theirs := p.expect(t, "KEYS "); theirs != hex.EncodeToString(got) {
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
	p.expect(t, "CLOSED")
}

func interopConfig() *zmux.Config { return &zmux.Config{EnableDatagrams: true} }

func TestInteropEstablishClient(t *testing.T) {
	for _, pr := range interopProfiles {
		t.Run(fmt.Sprint(uint64(pr)), func(t *testing.T) {
			p := startInteropPeer(t, "seal-server", fmt.Sprint(uint64(pr)))
			addr := p.expect(t, "LISTEN ")
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
				Anchor:          seal.PinAnchor(seal.NodeKeyID(nk.PublicKey().Bytes())),
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
			p.expect(t, "ESTABLISHED")
			interopScript(t, ctx, c, p)
		})
	}
}

func TestInteropEstablishServer(t *testing.T) {
	for _, pr := range interopProfiles {
		t.Run(fmt.Sprint(uint64(pr)), func(t *testing.T) {
			if os.Getenv("ZMUX_INTEROP_PEER") == "" {
				t.Skip("ZMUX_INTEROP_PEER not set")
			}
			h, k := interopKeys(t)
			nk, err := ecdh.X25519().NewPrivateKey(interopNode)
			if err != nil {
				t.Fatal(err)
			}
			srv := &seal.ServerConfig{Gate: &gate{handle: h, psk: k}, Key: nk, AcceptedProfiles: []proto.Profile{pr}}
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
			p := startInteropPeer(t, "seal-client", ln.Addr().String(), fmt.Sprint(uint64(pr)))
			if got := p.expect(t, "PROFILE "); got != fmt.Sprint(uint64(pr)) {
				t.Fatalf("profile %s", got)
			}
			c, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			p.expect(t, "ESTABLISHED")
			interopScript(t, ctx, c, p)
		})
	}
}
