package zmuxquic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/testcert"
	"github.com/zmuxio/zmux-go/internal/testpeer"
)

// The QUIC datagram channel against another implementation's datagram peer
// (ZMUX_RS_QUIC_CHANNEL_PEER names its binary), both directions:
//
//	server CERT KEY   listens, prints "LISTEN addr", echoes every session
//	                  datagram and stream, prints "STATS sent=.. sentch=..
//	                  recv=.. recvch=.." when the session ends
//	client ADDR N     sends N datagrams of 100 bytes (the index as a
//	                  big-endian u32 first), expects each echoed, prints STATS

const channelALPN = "qmux-02"

func startChannelPeer(t *testing.T, args ...string) *testpeer.Process {
	t.Helper()
	return testpeer.Start(t, testpeer.Command(t, "ZMUX_RS_QUIC_CHANNEL_PEER", args...), false)
}

// channelCert makes a self-signed certificate, with its PEM files.
func channelCert(t *testing.T) (tls.Certificate, string, string) {
	t.Helper()
	cert, _ := testcert.Local(t)
	kd, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	crt, k := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(crt, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(k, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kd}), 0o600); err != nil {
		t.Fatal(err)
	}
	return cert, crt, k
}

func channelStamp(i int) []byte {
	d := make([]byte, 100)
	binary.BigEndian.PutUint32(d, uint32(i))
	return d
}

func channelWant(n int) []string {
	return []string{fmt.Sprintf("sentch=%d ", n), fmt.Sprintf("recvch=%d", n)}
}

func checkChannelStats(t *testing.T, stats string, n int) {
	t.Helper()
	for _, w := range channelWant(n) {
		if !strings.Contains(stats, w) {
			t.Fatalf("peer STATS %q lacks %q", stats, w)
		}
	}
}

// Go dials the peer's server and sends n datagrams, each echoed.
func TestInteropQUICChannelGoClient(t *testing.T) {
	if os.Getenv("ZMUX_RS_QUIC_CHANNEL_PEER") == "" {
		t.Skip("ZMUX_RS_QUIC_CHANNEL_PEER not set")
	}
	_, crt, key := channelCert(t)
	p := startChannelPeer(t, "server", crt, key)
	addr := p.Expect(t, "LISTEN ")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{channelALPN}}
	c, err := Dial(ctx, addr, tc, nil, &zmux.Config{EnableDatagrams: true, DatagramQueueDelay: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	const n = 20
	for i := range n {
		if err := c.SendDatagramWait(ctx, channelStamp(i)); err != nil {
			t.Fatal(err)
		}
		d, err := c.ReceiveDatagram(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(d) != 100 || int(binary.BigEndian.Uint32(d)) != i {
			t.Fatalf("unexpected echo for %d: %x", i, d)
		}
	}
	if s := c.ConnectionStats(); s.DatagramsReceivedChannel != n {
		t.Fatalf("channel counts: %+v", s)
	}
	_ = c.Close()
	checkChannelStats(t, p.Expect(t, "STATS "), n)
}

// The peer dials Go's server, which echoes its datagrams.
func TestInteropQUICChannelGoServer(t *testing.T) {
	if os.Getenv("ZMUX_RS_QUIC_CHANNEL_PEER") == "" {
		t.Skip("ZMUX_RS_QUIC_CHANNEL_PEER not set")
	}
	cert, _, _ := channelCert(t)
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{channelALPN}}, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	l := mustNewListener(&zmux.ListenerConfig{Config: &zmux.Config{EnableDatagrams: true, DatagramQueueDelay: -1}})
	defer func() { _ = l.Close() }()
	serveInteropListener(t, ctx, l, ln)
	const n = 20
	p := startChannelPeer(t, "client", ln.Addr().String(), strconv.Itoa(n))
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for range n {
		d, err := c.ReceiveDatagram(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.SendDatagramWait(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	checkChannelStats(t, p.Expect(t, "STATS "), n)
	if s := c.ConnectionStats(); s.DatagramsReceivedChannel != n {
		t.Fatalf("channel counts: %+v", s)
	}
	if err := p.Wait(t); err != nil {
		t.Fatalf("peer: %v", err)
	}
}
