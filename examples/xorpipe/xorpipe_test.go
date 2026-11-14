package xorpipe_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/examples/xorpipe"
)

const key = 0x5a

// recConn records the bytes of each write, for the test's view of the
// carrier's first write.
type recConn struct {
	net.Conn
	writes [][]byte
	mu     sync.Mutex
}

func (c *recConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, bytes.Clone(b))
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *recConn) first() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writes) == 0 {
		return nil
	}
	return c.writes[0]
}

type server struct {
	lst  *zmux.Listener
	addr string
}

func newServer(t *testing.T) *server {
	t.Helper()
	filter, err := admit.NewReplayFilter(admit.ReplayConfig{Window: time.Minute, Max: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	lst, err := zmux.NewListener(&zmux.ListenerConfig{Config: &zmux.Config{},
		Admission: xorpipe.Admission(), WrapPipe: xorpipe.WrapPipe(filter, key)})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = lst.Serve(context.Background(), ln); close(done) }()
	t.Cleanup(func() { _ = lst.Close(); _ = ln.Close(); <-done })
	return &server{lst: lst, addr: ln.Addr().String()}
}

func clientConfig() *zmux.Config {
	return &zmux.Config{EarlyData: zmux.EarlyData{ZmuxPeer: true, Replayable: true, HoldFirstFlight: true}}
}

// session dials the server through the transform with nonce, sends hello
// on a stream opened before the first flight is released, and returns
// the sessions, the carrier and what the server read from that stream.
func session(t *testing.T, ctx context.Context, s *server, nonce [8]byte, hello []byte) (c, sc *zmux.Conn, rc *recConn, got []byte) {
	t.Helper()
	nc, err := net.Dial("tcp", s.addr)
	if err != nil {
		t.Fatal(err)
	}
	rc = &recConn{Conn: nc}
	c, err = zmux.Start(ctx, xorpipe.Client(rc, key, nonce), zmux.RoleClient, clientConfig())
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(hello); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	c.ReleaseFirstFlight()
	sc, err = s.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ss, err := sc.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(ss)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); _ = sc.Close(); _ = c.Wait(); _ = sc.Wait() })
	return c, sc, rc, got
}

func xorBytes(b []byte) []byte {
	out := make([]byte, len(b))
	for i, x := range b {
		out[i] = x ^ key
	}
	return out
}

// The first carrier write holds the header and the first flight with the
// stream's data; the session works; the admission counted the variant.
func TestFirstFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := newServer(t)
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	hello := []byte("HELLO in the first flight")
	c, sc, rc, got := session(t, ctx, s, nonce, hello)
	if !bytes.Equal(got, hello) {
		t.Fatalf("server read %q", got)
	}
	first := rc.first()
	if len(first) <= xorpipe.HeaderLen || !bytes.Contains(xorBytes(first[xorpipe.HeaderLen:]), hello) {
		t.Fatalf("first write of %d bytes lacks the stream data", len(first))
	}
	if cls, _ := admit.ClassifyFirstBytes(first, zmux.FramingByteStream); cls != admit.FirstBytesOther {
		t.Fatalf("the header is classified %v", cls)
	}
	echo(t, ctx, c, sc)
	if vs := s.lst.VariantStats(); !slices.Contains(vs, zmux.VariantStat{Name: xorpipe.Variant, Accepted: 1}) {
		t.Fatalf("variant stats %+v", vs)
	}
}

// A replayed header has its first flight dropped: the session sends its
// parameters and the stream again, which arrives whole.
func TestFirstFlightRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := newServer(t)
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	session(t, ctx, s, nonce, []byte("first"))
	hello := []byte("HELLO sent again after a refused flight")
	c, sc, _, got := session(t, ctx, s, nonce, hello)
	if !bytes.Equal(got, hello) {
		t.Fatalf("server read %q", got)
	}
	echo(t, ctx, c, sc)
}

// A plain zmux client on the same listener is told apart by its first
// record and counted without a variant.
func TestPlainNextToTransform(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := newServer(t)
	nc, err := net.Dial("tcp", s.addr)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Client(ctx, nc, &zmux.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := s.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = sc.Close(); _ = c.Wait(); _ = sc.Wait() }()
	echo(t, ctx, c, sc)
	if vs := s.lst.VariantStats(); !slices.Contains(vs, zmux.VariantStat{Accepted: 1}) {
		t.Fatalf("variant stats %+v", vs)
	}
}

func echo(t *testing.T, ctx context.Context, c, s *zmux.Conn) {
	t.Helper()
	go func() {
		st, err := s.AcceptStream(ctx)
		if err == nil {
			_, _ = io.Copy(st, st)
			_ = st.Close()
		}
	}()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msg := bytes.Repeat([]byte("x"), 64<<10)
	go func() { _, _ = st.Write(msg); _ = st.Close() }()
	got, err := io.ReadAll(st)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("echo: %d bytes, %v", len(got), err)
	}
}

// The example imports the public API only (zmux's own packages import its
// internal ones).
func TestPublicAPIOnly(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{join .Imports \"\\n\"}}{{\"\\n\"}}{{join .TestImports \"\\n\"}}{{\"\\n\"}}{{join .XTestImports \"\\n\"}}", ".").Output()
	if err != nil {
		t.Skip("go list:", err)
	}
	for p := range strings.FieldsSeq(string(out)) {
		if strings.Contains(p, "/internal") {
			t.Fatalf("the example imports %s", p)
		}
	}
}
