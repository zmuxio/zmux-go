package zmuxquic

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

func TestPoolDialUsesDestinationUDPFamily(t *testing.T) {
	for _, tc := range []struct {
		name, address string
		v4            bool
	}{{"v4", "127.0.0.1:0", true}, {"v6", "[::1]:0", false}} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnvAt(t, tc.address, mpConfig())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pool := NewPool(e.ln.Addr().String(), e.cli, nil, zmux.PoolLimits{})
			defer pool.Shutdown()
			qc, err := pool.pool.Dial(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = qc.CloseWithError(0, "") }()
			local := qc.LocalAddr().(*net.UDPAddr)
			if (local.IP.To4() != nil) != tc.v4 {
				t.Fatalf("wrong UDP family: local=%s remote=%s", local, qc.RemoteAddr())
			}
			_ = qc.CloseWithError(0, "")
			network := "udp6"
			if tc.v4 {
				network = "udp4"
			}
			for {
				socket, err := net.ListenUDP(network, local)
				if err == nil {
					_ = socket.Close()
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("owned UDP socket was not released: %v", err)
				case <-time.After(time.Millisecond):
				}
			}
		})
	}
}
func TestPoolDialPreservesHostnameWithoutChangingCallerTLS(t *testing.T) {
	e := newEnv(t, mpConfig())
	cfg := e.cli.Clone()
	cfg.ServerName = ""
	address := net.JoinHostPort("localhost", strconv.Itoa(e.ln.Addr().(*net.UDPAddr).Port))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool := NewPool(address, cfg, nil, zmux.PoolLimits{})
	defer pool.Shutdown()
	qc, err := pool.pool.Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	if cfg.ServerName != "" {
		t.Fatalf("caller TLS config changed: %q", cfg.ServerName)
	}
}

func TestOwnedFamilySocketSurvivesZeroRTTRejection(t *testing.T) {
	exerciseRejectedEarlyConnection(t, dialAddr)
}
func exerciseRejectedEarlyConnection(t *testing.T, dial func(context.Context, string, *tls.Config, *quic.Config, bool) (*quic.Conn, error)) {
	t.Helper()
	serverTLS, clientTLS := tlsConfigs(t)
	var allow atomic.Bool
	allow.Store(true)
	base := QUICConfig(&quic.Config{Allow0RTT: true})
	base.GetConfigForClient = func(*quic.ClientInfo) (*quic.Config, error) {
		c := base.Clone()
		c.GetConfigForClient = nil
		c.Allow0RTT = allow.Load()
		return c, nil
	}
	listener, err := quic.ListenAddrEarly("127.0.0.1:0", serverTLS, base)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	warm, err := dial(ctx, listener.Addr().String(), clientTLS, QUICConfig(nil), false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = warm.CloseWithError(0, "") }()
	accepted, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = accepted.CloseWithError(0, "") }()
	for {
		if _, ok := clientTLS.ClientSessionCache.Get("localhost"); ok {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("session ticket not received")
		case <-time.After(time.Millisecond):
		}
	}
	_ = warm.CloseWithError(0, "")
	_ = accepted.CloseWithError(0, "")
	allow.Store(false)
	early, err := dial(ctx, listener.Addr().String(), clientTLS, QUICConfig(nil), true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = early.CloseWithError(0, "") }()
	peer, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.CloseWithError(0, "") }()
	stream, err := early.OpenStreamSync(ctx)
	if err == nil {
		if err = stream.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		_, err = stream.Read(make([]byte, 1))
	}
	if !errors.Is(err, quic.Err0RTTRejected) {
		t.Fatalf("expected native 0-RTT rejection, got %v", err)
	}
	next, err := early.NextConnection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st, err := next.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	received, err := peer.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = received.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(received)
	if err != nil || string(data) != "ok" {
		t.Fatalf("post-rejection data=%q err=%v", data, err)
	}
}
