package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// writeLog is a sending half (a layer, no net.Conn) that records each
// write call, a WriteBuffers call as one.
type writeLog struct {
	c      net.Conn
	first  chan struct{}
	writes [][]byte
	mu     sync.Mutex
}

func (w *writeLog) Write(b []byte) (int, error) {
	w.mu.Lock()
	w.writes = append(w.writes, bytes.Clone(b))
	if len(w.writes) == 1 && w.first != nil {
		close(w.first)
	}
	w.mu.Unlock()
	return w.c.Write(b)
}

func (w *writeLog) WriteBuffers(bufs [][]byte) (int, error) {
	return carrierio.WriteFlat(w, bufs)
}

func (w *writeLog) Close() error { return w.c.Close() }

func (w *writeLog) log() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([][]byte(nil), w.writes...)
}

type firstFlightReader struct {
	io.ReadCloser
	wrote <-chan struct{}
}

func (r firstFlightReader) Read(p []byte) (int, error) {
	<-r.wrote
	return r.ReadCloser.Read(p)
}

type firstFlightMessageReader struct {
	firstFlightReader
}

func (r firstFlightMessageReader) ReadMessage(limit int) (ReceivedMessage, error) {
	<-r.wrote
	return r.ReadCloser.(MessageReader).ReadMessage(limit)
}

func holdConfig() *Config {
	cfg := mpConfig()
	cfg.EarlyData = EarlyData{HoldFirstFlight: true}
	return cfg
}

// startHeld starts a held client on p and its server on the other end.
func startHeld(t *testing.T, ctx context.Context, p, peer PipeIO, cfg *Config) (c *Conn, server func() *Conn) {
	t.Helper()
	c, err := Start(ctx, p, RoleClient, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan *Conn, 1)
	go func() {
		s, err := startServerNoListener(ctx, peer, mpConfig())
		if err != nil {
			t.Error(err)
		}
		ch <- s
	}()
	return c, func() *Conn { return <-ch }
}

// The held first flight: nothing is written before ReleaseFirstFlight,
// then the parameters leave while the peer parameters are gated. Early
// stream bytes stay queued until the peer parameters arrive.
func TestHoldFirstFlight(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := net.Pipe()
	wl := &writeLog{c: a, first: make(chan struct{})}
	c, server := startHeld(t, ctx, PipeIO{R: firstFlightReader{ReadCloser: a, wrote: wl.first}, W: wl}, NetPipe(b), holdConfig())
	hello := []byte("HELLO, first flight")
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(hello); err != nil {
		t.Fatal(err)
	}
	if n := len(wl.log()); n != 0 {
		t.Fatalf("%d writes before the release", n)
	}
	c.ReleaseFirstFlight()
	c.ReleaseFirstFlight()
	s := server()
	if s == nil {
		_ = c.Close()
		return
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	first := wl.log()[0]
	if cls, _ := wire.ClassifyFirstBytes(first, false); cls != wire.FirstRecord || bytes.Contains(first, hello) {
		t.Fatalf("first write %x", first)
	}
	ss, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(hello))
	if _, err := io.ReadFull(ss, got); err != nil || !bytes.Equal(got, hello) {
		t.Fatalf("read %q, %v", got, err)
	}
	if n := c.ConnectionStats().FirstFlightTimeouts; n != 0 {
		t.Fatalf("FirstFlightTimeouts %d", n)
	}
	// A collected handle resets its stream (doc.go, Ownership): st stays
	// referenced until the server read what it wrote.
	runtime.KeepAlive(st)
}

// On a message-framed carrier the first flight is one WriteMessages call.
func TestHoldFirstFlightMessages(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	x, y := msgCarriers()
	bx := &batchCarrier{msgCarrier: x, wrote: make(chan struct{})}
	r := firstFlightMessageReader{firstFlightReader{ReadCloser: bx, wrote: bx.wrote}}
	c, server := startHeld(t, ctx, PipeIO{R: r, W: bx, Framing: FramingRecords},
		PipeIO{R: y, W: y, Framing: FramingRecords}, holdConfig())
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if bx.batches.Load() != 0 {
		t.Fatal("written before the release")
	}
	c.ReleaseFirstFlight()
	s := server()
	if s == nil {
		_ = c.Close()
		return
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	if _, err := s.AcceptStream(ctx); err != nil {
		t.Fatal(err)
	}
	echoOnce(t, ctx, c, s)
	if f := bx.first.Load(); f == nil || bytes.Contains(*f, []byte("hello")) || bx.single.Load() != 0 {
		t.Fatalf("first batch of %d messages unexpectedly contained early application bytes, %d single writes", bx.firstBatch.Load(), bx.single.Load())
	}
	runtime.KeepAlive(st)
}

// Without a release, the handshake timeout releases the first flight,
// counted, and the session still establishes.
func TestHoldFirstFlightTimeout(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := net.Pipe()
	cfg := holdConfig()
	cfg.HandshakeTimeout = 200 * time.Millisecond
	c, server := startHeld(t, ctx, NetPipe(a), NetPipe(b), cfg)
	s := server()
	if s == nil {
		_ = c.Close()
		return
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	if n := c.ConnectionStats().FirstFlightTimeouts; n != 1 {
		t.Fatalf("FirstFlightTimeouts %d", n)
	}
}

// Only the bootstrap initiator can hold its first flight.
func TestHoldFirstFlightConfig(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	if _, err := startServerNoListener(context.Background(), NetPipe(a), holdConfig()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("server: %v", err)
	}
}

// A session that closes while its first flight is held stops the release
// timer: nothing fires after the session ended.
func TestHoldFirstFlightClosed(t *testing.T) {
	leakcheck.Check(t)
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	cfg := holdConfig()
	cfg.HandshakeTimeout = time.Hour
	c, err := Start(context.Background(), NetPipe(a), RoleClient, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	waitDone(t, c)
	c.lock()
	held := c.firstTimer != nil
	c.unlock()
	if held {
		t.Fatal("the first flight timer outlived the session")
	}
	if n := c.ConnectionStats().FirstFlightTimeouts; n != 0 {
		t.Fatalf("FirstFlightTimeouts %d", n)
	}
}
