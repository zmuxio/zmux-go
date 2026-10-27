package zmux

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Interoperability with another implementation's scripted peer, a binary
// named by ZMUX_INTEROP_PEER that takes "server" (printing "LISTEN addr")
// or "client addr", prints "ESTABLISHED", and answers the commands carried
// in the open info of the streams this side opens.

type peerProc struct {
	cmd   *exec.Cmd
	lines chan string
}

func startPeer(t *testing.T, args ...string) *peerProc {
	t.Helper()
	return startPeerEnv(t, "ZMUX_INTEROP_PEER", args...)
}

func startPeerEnv(t *testing.T, env string, args ...string) *peerProc {
	t.Helper()
	bin := os.Getenv(env)
	if bin == "" {
		t.Skip(env + " not set")
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
	p := &peerProc{cmd: cmd, lines: make(chan string, 16)}
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

func interopConfig() *Config { return &Config{EnableDatagrams: true} }

func TestInteropClient(t *testing.T) {
	p := startPeer(t, "server")
	addr := p.expect(t, "LISTEN ")
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Client(ctx, nc, interopConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.expect(t, "ESTABLISHED")
	interopScript(t, ctx, c, p)
}

func TestInteropServer(t *testing.T) {
	if os.Getenv("ZMUX_INTEROP_PEER") == "" {
		t.Skip("ZMUX_INTEROP_PEER not set")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	p := startPeer(t, "client", ln.Addr().String())
	nc, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Server(ctx, nc, interopConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.expect(t, "ESTABLISHED")
	interopScript(t, ctx, c, p)
}

// command opens a stream carrying cmd, writes body, finishes and returns
// the reply.
func command(t *testing.T, ctx context.Context, c *Conn, cmd string, body []byte) string {
	t.Helper()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cmd != "" {
		if err := st.SetPreamble([]byte(cmd)); err != nil {
			t.Fatal(err)
		}
	}
	go func() { _, _ = st.Write(body); _ = st.Close() }()
	b, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("%s: %v", cmd, err)
	}
	return string(b)
}

func interopScript(t *testing.T, ctx context.Context, c *Conn, p *peerProc) {
	// Bulk echo, several at once.
	errs := make(chan error, 4)
	for range 4 {
		go func() {
			msg := make([]byte, 4<<20)
			_, _ = rand.Read(msg)
			st, err := c.OpenStreamSync(ctx)
			if err != nil {
				errs <- err
				return
			}
			_ = st.SetPreamble([]byte("echo"))
			go func() { _, _ = st.Write(msg); _ = st.Close() }()
			got, err := io.ReadAll(st)
			if err == nil && !bytes.Equal(got, msg) {
				err = errors.New("echo mismatch")
			}
			errs <- err
		}()
	}
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	// Priority and open info.
	{
		st, _ := c.OpenStreamSync(ctx)
		_ = st.SetPreamble([]byte("info"))
		_ = st.SetPeerPriority(5, true)
		_ = st.Close()
		b, _ := io.ReadAll(st)
		if string(b) != "info=info;prio=5,true" {
			t.Fatalf("info: %q", b)
		}
	}
	// Our reset reaches the peer.
	{
		st, _ := c.OpenStreamSync(ctx)
		_ = st.SetPreamble([]byte("reset"))
		_, _ = st.Write([]byte("x"))
		st.SetReliableBoundary() // "x" opens the stream: delivered, then the reset
		st.CancelWrite(77)
		b, _ := io.ReadAll(st)
		if string(b) != "reset=77" {
			t.Fatalf("reset: %q", b)
		}
	}
	// The peer's STOP_SENDING.
	if r := command(t, ctx, c, "stop:9", nil); r != "stopped" {
		t.Fatalf("stop: %q", r)
	}
	// The peer's reset.
	{
		st, _ := c.OpenStreamSync(ctx)
		_ = st.SetPreamble([]byte("sendreset:33"))
		_ = st.Close()
		_, err := io.ReadAll(st)
		var se *StreamError
		if !errors.As(err, &se) || se.ErrorCode != 33 || !se.Remote {
			t.Fatalf("sendreset: %v", err)
		}
	}
	// A bidirectional stream the peer opens.
	{
		go func() {
			st, err := c.AcceptStream(ctx)
			if err != nil {
				return
			}
			b, _ := io.ReadAll(st)
			_, _ = st.Write(append([]byte(string(st.Preamble())+":"), b...))
			_ = st.Close()
		}()
		if r := command(t, ctx, c, "open-bidi:hello", nil); r != "opened=back:hello" {
			t.Fatalf("open-bidi: %q", r)
		}
	}
	// A unidirectional stream the peer opens.
	{
		got := make(chan string, 1)
		go func() {
			rs, err := c.AcceptUniStream(ctx)
			if err != nil {
				got <- err.Error()
				return
			}
			b, _ := io.ReadAll(rs)
			got <- string(b)
		}()
		if r := command(t, ctx, c, "open-uni:uni", nil); r != "sent" {
			t.Fatalf("open-uni: %q", r)
		}
		if g := <-got; g != "uni" {
			t.Fatalf("uni: %q", g)
		}
	}
	// A unidirectional stream echoed on a new one.
	{
		us, err := c.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = us.Write([]byte("u-echo"))
		_ = us.Close()
		rs, err := c.AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b, _ := io.ReadAll(rs); string(b) != "u-echo" {
			t.Fatalf("uni echo: %q", b)
		}
	}
	// Session and stream datagrams.
	{
		if err := c.SendDatagram([]byte("dg")); err != nil {
			t.Fatal(err)
		}
		d, err := c.ReceiveDatagram(ctx)
		if err != nil || string(d) != "dg" {
			t.Fatalf("datagram: %q %v", d, err)
		}
		st, _ := c.OpenStreamSync(ctx)
		_ = st.SetPreamble([]byte("dgram:1"))
		_, _ = st.Write(nil)
		if err := st.SendDatagram(4, []byte("sdg")); err != nil {
			t.Fatal(err)
		}
		ctxv, d2, err := st.ReceiveDatagram(ctx)
		if err != nil || ctxv != 5 || string(d2) != "sdg" {
			t.Fatalf("stream datagram: %d %q %v", ctxv, d2, err)
		}
		_ = st.Close()
		if b, _ := io.ReadAll(st); string(b) != "dgram-done" {
			t.Fatalf("dgram: %q", b)
		}
	}
	// The peer closes with a code and reason.
	if r := command(t, ctx, c, "close:42:bye", nil); r != "closing" {
		t.Fatalf("close: %q", r)
	}
	err := c.Wait()
	var ae *ApplicationError
	if !errors.As(err, &ae) || ae.ErrorCode != 42 || ae.ErrorMessage != "bye" || !ae.Remote {
		t.Fatalf("close: %v", err)
	}
	p.expect(t, "CLOSED")
}

var interopPSK = []byte("zmux interop pre-shared key 0123")

func interopMPConfig() *Config {
	return &Config{EnableDatagrams: true, MaxPipes: 4, PipeAuth: PipeAuth{PSK: interopPSK}}
}

// spreadCheck sends bulk with round-robin spreading and checks that every
// pipe carried part of it.
func spreadCheck(t *testing.T, ctx context.Context, c *Conn) {
	t.Helper()
	// Every pipe usable first (a join may still be in probation).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ps := c.PipeStats()
		ok := len(ps) >= 2
		for _, p := range ps {
			ok = ok && p.Eligible
		}
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.SetScheduler(SpreadRoundRobin())
	before := map[uint64]uint64{}
	for _, p := range c.PipeStats() {
		before[p.ID] = p.BytesSent
	}
	msg := make([]byte, 16<<20)
	_, _ = rand.Read(msg)
	if r := command(t, ctx, c, "echo", msg); r != string(msg) {
		t.Fatal("multipath echo mismatch")
	}
	ps := c.PipeStats()
	if len(ps) < 2 {
		t.Fatalf("pipes: %d", len(ps))
	}
	for _, p := range ps {
		if p.BytesSent-before[p.ID] < 1<<20 {
			t.Fatalf("pipe %d carried %d bytes", p.ID, p.BytesSent-before[p.ID])
		}
	}
	c.SetScheduler(nil)
}

func TestInteropMultipathClient(t *testing.T) {
	p := startPeer(t, "mp-server")
	addr := p.expect(t, "LISTEN ")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Client(ctx, nc, interopMPConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.expect(t, "ESTABLISHED")
	nc2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddPipe(ctx, NetPipe(nc2), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	spreadCheck(t, ctx, c)
	interopScript(t, ctx, c, p)
}

func TestInteropMultipathServer(t *testing.T) {
	if os.Getenv("ZMUX_INTEROP_PEER") == "" {
		t.Skip("ZMUX_INTEROP_PEER not set")
	}
	tl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tl.Close() }()
	l := NewListener(&ListenerConfig{Config: interopMPConfig()})
	defer func() { _ = l.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() {
		for {
			nc, err := tl.Accept()
			if err != nil {
				return
			}
			go func() { _ = l.HandlePipe(ctx, NetPipe(nc), JoinHint{}) }()
		}
	}()
	p := startPeer(t, "mp-client", tl.Addr().String())
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.expect(t, "ESTABLISHED")
	deadline := time.Now().Add(10 * time.Second)
	for len(c.PipeStats()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	spreadCheck(t, ctx, c)
	interopScript(t, ctx, c, p)
}
