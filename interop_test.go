package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/testpeer"
)

// Interoperability with another implementation's scripted peer, a binary
// named by ZMUX_INTEROP_PEER that takes "server" (printing "LISTEN addr")
// or "client addr", prints "ESTABLISHED", and answers the commands carried
// in the open info of the streams this side opens.

func startPeer(t *testing.T, args ...string) *testpeer.Process {
	t.Helper()
	return startPeerEnv(t, "ZMUX_INTEROP_PEER", args...)
}

func startPeerEnv(t *testing.T, env string, args ...string) *testpeer.Process {
	t.Helper()
	return testpeer.Start(t, testpeer.Command(t, env, args...), env == "ZMUX_INTEROP_PEER")
}

func interopConfig() *Config { return &Config{EnableDatagrams: true} }

func TestInteropClient(t *testing.T) {
	p := startPeer(t, "server")
	addr := p.Expect(t, "LISTEN ")
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
	p.Expect(t, "ESTABLISHED")
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
	p.Expect(t, "ESTABLISHED")
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

func interopScript(t *testing.T, ctx context.Context, c *Conn, p *testpeer.Process) { //nolint:thelper // scenario body, not a helper: failures point inside it
	interopSteps(t, ctx, c)
	interopPeerClose(t, ctx, c, p)
}

func interopSteps(t *testing.T, ctx context.Context, c *Conn) { //nolint:thelper // scenario body, not a helper: failures point inside it
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
}

// interopPeerClose has the peer close with a code and reason.
func interopPeerClose(t *testing.T, ctx context.Context, c *Conn, p *testpeer.Process) { //nolint:thelper // scenario body, not a helper: failures point inside it
	closeStream, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := closeStream.SetPreamble([]byte("close:42:bye")); err != nil {
		t.Fatal(err)
	}
	if err := closeStream.Close(); err != nil {
		t.Fatal(err)
	}
	err = c.Wait()
	var ae *ApplicationError
	if !errors.As(err, &ae) || ae.ErrorCode != 42 || ae.ErrorMessage != "bye" || !ae.Remote {
		t.Fatalf("close: %v", err)
	}
	p.Expect(t, "CLOSED")
}

var interopPSK = []byte("zmux interop pre-shared key 0123")

func interopAnyMode(RecordPolicyInput, RecordMode, any) bool { return true }

func interopMPConfig() *Config {
	return &Config{EnableDatagrams: true, SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 4}}, PipeAuth: PipeAuth{PSK: interopPSK, EndToEnd: true}, AllowRecordMode: interopAnyMode}
}

// spreadCheck sends bulk with round-robin spreading and checks that every
// pipe carried part of it.
func spreadCheck(t *testing.T, ctx context.Context, c *Conn) {
	t.Helper()
	// Attachment and probation change the pipe's health and wake NextEvent.
	for {
		ps := c.PipeStats()
		ready := len(ps) >= 2
		for _, p := range ps {
			ready = ready && p.Eligible
		}
		if ready {
			break
		}
		waitTestEvent(t, ctx, c, "eligible interop pipes", func(ev Event) bool {
			return ev.Kind == EventPipeHealthChanged
		})
	}
	c.SetScheduler(SchedulerSpreadRoundRobin())
	before := map[uint64]uint64{}
	for _, p := range c.PipeStats() {
		before[uint64(p.PipeID)] = p.BytesSent
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
		if p.BytesSent-before[uint64(p.PipeID)] < 1<<20 {
			t.Fatalf("pipe %d carried %d bytes; stats: %+v", p.PipeID, p.BytesSent-before[uint64(p.PipeID)], ps)
		}
	}
	c.SetScheduler(nil)
}

func TestInteropMultipathClient(t *testing.T) {
	p := startPeer(t, "mp-server")
	addr := p.Expect(t, "LISTEN ")
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
	p.Expect(t, "ESTABLISHED")
	nc2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddPipe(ctx, NetPipe(nc2), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	spreadCheck(t, ctx, c)
	interopSteps(t, ctx, c)
	interopPeerCloseTail(t, ctx, c, p)
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
	l := mustNewListener(&ListenerConfig{Config: interopMPConfig()})
	defer func() { _ = l.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() {
		for {
			nc, err := tl.Accept()
			if err != nil {
				return
			}
			go func() { _ = l.HandlePipe(ctx, NetPipe(nc), PipeHint{}) }()
		}
	}()
	p := startPeer(t, "mp-client", tl.Addr().String())
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.Expect(t, "ESTABLISHED")
	spreadCheck(t, ctx, c)
	interopSteps(t, ctx, c)
	interopCloseTail(t, ctx, c, p)
}

// interopTail is the stream written just before a close in a multipath
// session: the close must not overtake it on another pipe.
const interopTail = 256 << 10

// interopPeerCloseTail has the peer write interopTail bytes on a stream it
// opens and close with a code at once; every byte must arrive first.
func interopPeerCloseTail(t *testing.T, ctx context.Context, c *Conn, p *testpeer.Process) {
	t.Helper()
	cmd, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.SetPreamble([]byte(fmt.Sprintf("close-tail:42:%d", interopTail))); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := c.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(st)
	if len(b) != interopTail {
		t.Fatalf("tail: read %d of %d (%v)", len(b), interopTail, err)
	}
	var ae *ApplicationError
	if err := c.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 42 || !ae.Remote {
		t.Fatalf("close: %v", err)
	}
	p.Expect(t, "CLOSED")
}

// interopCloseTail writes interopTail bytes on a stream and closes with a
// code at once; the peer reports how many bytes it read.
func interopCloseTail(t *testing.T, ctx context.Context, c *Conn, p *testpeer.Process) {
	t.Helper()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetPreamble([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(make([]byte, interopTail)); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWithError(42, "bye"); err != nil {
		t.Fatal(err)
	}
	if got := p.Expect(t, "TAIL "); got != strconv.Itoa(interopTail) {
		t.Fatalf("tail: peer read %s of %d", got, interopTail)
	}
	runtime.KeepAlive(st)
}

// TLS with the join secret from the exporter alone: the
// join succeeds only with the same session secret at both ends. Each connection's first byte in TLS says what it is: B a
// bootstrap carrier, J a join, U and D the halves of a split bootstrap on two
// connections, whose exporter value is the uplink's followed by the downlink's.

const (
	interopAppLabel = "EXPORTER-interop-app"
	interopPair     = "zmux split pair!"
)

func interopTLSConfig() *Config {
	// Joins on a Secure carrier are Bound, so the interop covers that mode.
	bound := func(c RecordPolicyInput) RecordMode {
		if c.Secure {
			return RecordBound
		}
		return RecordSealed
	}
	return &Config{EnableDatagrams: true, SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 4}}, PipeAuth: PipeAuth{Exporter: true, EndToEnd: true}, AllowRecordMode: interopAnyMode, RecordPolicy: bound}
}

func interopKeys(t *testing.T, c *Conn, p *testpeer.Process, wantLen int) {
	t.Helper()
	got, err := c.ExportKeyingMaterial(interopAppLabel, nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != wantLen {
		t.Fatalf("exporter value of %d bytes, want %d", len(got), wantLen)
	}
	if peer := p.Expect(t, "KEYS "); peer != hex.EncodeToString(got) {
		t.Fatalf("exporter values differ: peer %s, here %x", peer, got)
	}
}

func interopTLSClient(t *testing.T, split bool) {
	t.Helper()
	p := startPeer(t, "tls-server")
	addr := p.Expect(t, "LISTEN ")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dial := func(kind byte) *tls.Conn {
		tc, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tc.Write([]byte{kind}); err != nil {
			t.Fatal(err)
		}
		return tc
	}
	var boot PipeIO
	wantLen := 16
	if split {
		boot = PipeIO{W: dial('U'), R: dial('D')}
		wantLen = 32
	} else {
		boot = NetPipe(dial('B'))
	}
	c, err := Start(ctx, boot, RoleClient, interopTLSConfig())
	if err != nil {
		t.Fatal(err)
	}
	interopKeys(t, c, p, wantLen)
	p.Expect(t, "ESTABLISHED")
	if _, err := c.AddPipe(ctx, NetPipe(dial('J')), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	spreadCheck(t, ctx, c)
	interopScript(t, ctx, c, p)
}

func interopTLSServer(t *testing.T, mode string, wantLen int) {
	t.Helper()
	if os.Getenv("ZMUX_INTEROP_PEER") == "" {
		t.Skip("ZMUX_INTEROP_PEER not set")
	}
	srv, _ := selfSignedTLS(t)
	srv.MinVersion = tls.VersionTLS13
	tl, err := tls.Listen("tcp", "127.0.0.1:0", srv)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tl.Close() }()
	l := mustNewListener(&ListenerConfig{Config: interopTLSConfig()})
	defer func() { _ = l.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() {
		for {
			nc, err := tl.Accept()
			if err != nil {
				return
			}
			go func() {
				kind := make([]byte, 1)
				if _, err := io.ReadFull(nc, kind); err != nil {
					_ = nc.Close()
					return
				}
				pair := base64.RawURLEncoding.EncodeToString([]byte(interopPair))
				switch kind[0] {
				case 'U':
					_ = l.HandlePipe(ctx, PipeIO{R: nc}, PipeHint{Pair: pair, Uplink: true})
				case 'D':
					_ = l.HandlePipe(ctx, PipeIO{W: nc}, PipeHint{Pair: pair})
				default:
					_ = l.HandlePipe(ctx, NetPipe(secureTLS{nc.(*tls.Conn)}), PipeHint{})
				}
			}()
		}
	}()
	p := startPeer(t, mode, tl.Addr().String())
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	interopKeys(t, c, p, wantLen)
	p.Expect(t, "ESTABLISHED")
	spreadCheck(t, ctx, c)
	interopScript(t, ctx, c, p)
}

func TestInteropTLSExporterJoinClient(t *testing.T)  { interopTLSClient(t, false) }
func TestInteropTLSExporterSplitClient(t *testing.T) { interopTLSClient(t, true) }
func TestInteropTLSExporterJoinServer(t *testing.T)  { interopTLSServer(t, "tls-client", 16) }
func TestInteropTLSExporterSplitServer(t *testing.T) { interopTLSServer(t, "tls-split-client", 32) }

// The peer prints some reports from concurrent tasks; expect must hold the
// line it passes over for the next expect.
