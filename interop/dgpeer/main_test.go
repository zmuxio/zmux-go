package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
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
)

// The test binary is its own peer: with DGPEER_RUN set it runs main instead
// of the tests, so the peer is driven through its command line and output
// exactly as the other implementations drive it.
func TestMain(m *testing.M) {
	if os.Getenv("DGPEER_RUN") != "" {
		main()
		return
	}
	m.Run()
}

func peerCmd(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.Env = append(os.Environ(), "DGPEER_RUN=1")
	cmd.Stderr = os.Stderr
	return cmd
}

func writeCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kd, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kd}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// counts parses "STATS sent=.. sentch=.. recv=.. recvch=..".
func counts(t *testing.T, line string) [4]int {
	t.Helper()
	var s, sc, r, rc int
	if _, err := fmt.Sscanf(line, "STATS sent=%d sentch=%d recv=%d recvch=%d", &s, &sc, &r, &rc); err != nil {
		t.Fatalf("not a STATS line: %q: %v", line, err)
	}
	return [4]int{s, sc, r, rc}
}

// A client gets every datagram echoed, all of them over the QUIC datagram
// channel, and both peers report the same counts.
func TestEchoOverTheChannel(t *testing.T) {
	const n = 20
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	certPath, keyPath := writeCert(t)

	server := peerCmd(ctx, "server", certPath, keyPath)
	pipe, err := server.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	exited := false
	defer func() {
		if !exited {
			_ = server.Process.Kill()
			_ = server.Wait()
		}
	}()
	lines := bufio.NewScanner(pipe)
	if !lines.Scan() {
		t.Fatal("server printed no LISTEN line")
	}
	addr, ok := strings.CutPrefix(lines.Text(), "LISTEN ")
	if !ok {
		t.Fatalf("no LISTEN line: %q", lines.Text())
	}

	client, err := peerCmd(ctx, "client", addr, strconv.Itoa(n)).Output()
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	want := [4]int{n, n, n, n}
	if got := counts(t, strings.TrimSpace(string(client))); got != want {
		t.Fatalf("client counts %v, want %v", got, want)
	}

	// The server's session ends with the client's close.
	if !lines.Scan() {
		t.Fatal("server printed no STATS line")
	}
	if got := counts(t, lines.Text()); got != want {
		t.Fatalf("server counts %v, want %v", got, want)
	}
	err = server.Wait()
	exited = true
	if err != nil {
		t.Fatalf("server exit: %v", err)
	}
}

func TestUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"server"}, {"client", "127.0.0.1:1", "x"}, {"other", "a", "b"}} {
		if err := run(args, os.Stderr); err == nil {
			t.Errorf("run(%q) succeeded", args)
		}
	}
}
