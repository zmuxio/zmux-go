package zmuxquic

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
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

type channelPeer struct {
	cmd   *exec.Cmd
	lines chan string
}

func startChannelPeer(t *testing.T, args ...string) *channelPeer {
	t.Helper()
	bin := os.Getenv("ZMUX_RS_QUIC_CHANNEL_PEER")
	if bin == "" {
		t.Skip("ZMUX_RS_QUIC_CHANNEL_PEER not set")
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
	p := &channelPeer{cmd: cmd, lines: make(chan string, 16)}
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			p.lines <- sc.Text()
		}
		close(p.lines)
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	return p
}

func (p *channelPeer) expect(t *testing.T, prefix string) string {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case l, ok := <-p.lines:
			if !ok {
				t.Fatalf("peer output ended before %q", prefix)
			}
			if rest, ok := strings.CutPrefix(l, prefix); ok {
				return strings.TrimSpace(rest)
			}
		case <-deadline:
			t.Fatalf("peer: no %q", prefix)
		}
	}
}

// channelCert makes a self-signed certificate, with its PEM files.
func channelCert(t *testing.T) (tls.Certificate, string, string) {
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
	kd, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	crt, k := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(crt, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(k, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kd}), 0o600); err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, crt, k
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
	addr := p.expect(t, "LISTEN ")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{channelALPN}}
	c, err := Dial(ctx, addr, tc, nil, &zmux.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	const n = 20
	for i := range n {
		if err := c.SendDatagram(channelStamp(i)); err != nil {
			t.Fatal(err)
		}
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		d, err := c.ReceiveDatagram(rctx)
		rcancel()
		if err != nil {
			t.Fatal(err)
		}
		if got := binary.BigEndian.Uint32(d); int(got) != i {
			t.Fatalf("echo %d for %d", got, i)
		}
	}
	if s := c.ConnectionStats(); s.DatagramsSentChannel != n || s.DatagramsReceivedChannel != n {
		t.Fatalf("channel counts: %+v", s)
	}
	_ = c.Close()
	checkChannelStats(t, p.expect(t, "STATS "), n)
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
	l := mustNewListener(&zmux.ListenerConfig{Config: &zmux.Config{EnableDatagrams: true}})
	defer func() { _ = l.Close() }()
	go func() { _ = Serve(ctx, l, ln) }()
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
		if err := c.SendDatagram(d); err != nil {
			t.Fatal(err)
		}
	}
	checkChannelStats(t, p.expect(t, "STATS "), n)
	if s := c.ConnectionStats(); s.DatagramsSentChannel != n || s.DatagramsReceivedChannel != n {
		t.Fatalf("channel counts: %+v", s)
	}
	if err := p.cmd.Wait(); err != nil {
		t.Fatalf("peer: %v", err)
	}
}
