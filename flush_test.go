package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// A protocol whose server speaks first: the opener sets the preamble and
// flushes without writing; the peer accepts the stream, reads the
// preamble and writes first.
func TestFlushServerFirst(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := net.Pipe()
	c, s := pair(t, a, b, nil, nil)
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetPreamble([]byte("svc")); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPreamble([]byte("late")); !errors.Is(err, ErrTooLate) {
		t.Fatalf("SetPreamble after Flush: %v", err)
	}
	ss, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(ss.Preamble()) != "svc" || ss.StreamID() != st.StreamID() {
		t.Fatalf("preamble %q, id %d/%d", ss.Preamble(), ss.StreamID(), st.StreamID())
	}
	if _, err := ss.Write([]byte("banner")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6)
	if _, err := io.ReadFull(st, buf); err != nil || string(buf) != "banner" {
		t.Fatalf("read %q, %v", buf, err)
	}
	// Flush, then CancelRead: the STOP_SENDING carries the stream's ID.
	st.CancelRead(7)
	waitUntil(t, 5*time.Second, "STOP_SENDING", func() bool {
		_, err := ss.Write([]byte("x"))
		return err != nil
	})
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush after CancelRead: %v", err)
	}
	_ = c.Close()
	if err := st.Flush(); err == nil {
		t.Fatal("Flush on a closed session succeeded")
	}
	_ = s.Close()
	waitDone(t, c)
	waitDone(t, s)
}

// Under a held first flight Flush returns at once; the opening frame
// leaves with the flight, and the peer accepts the stream.
func TestFlushHeldFirstFlight(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := net.Pipe()
	wl := &writeLog{c: a}
	c, server := startHeld(t, ctx, PipeIO{R: a, W: wl}, NetPipe(b), holdConfig())
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := len(wl.log()); n != 0 {
		t.Fatalf("%d writes before the release", n)
	}
	c.ReleaseFirstFlight()
	s := server()
	if s == nil {
		_ = c.Close()
		return
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	ss, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(st, buf); err != nil || !bytes.Equal(buf, []byte("hi")) {
		t.Fatalf("read %q, %v", buf, err)
	}
	runtime.KeepAlive(st)
}

// Flush before establishment, in early data: the opening frame leaves
// without waiting for the peer's parameters, and Flush returns then.
func TestFlushEarly(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := net.Pipe()
	wl := &writeLog{c: a}
	cfg := mpConfig()
	cfg.EarlyData = EarlyData{ZmuxPeer: true}
	c, server := startHeld(t, ctx, PipeIO{R: a, W: wl}, NetPipe(b), cfg)
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetPreamble([]byte("svc")); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	s := server()
	if s == nil {
		_ = c.Close()
		return
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	ss, err := s.AcceptStream(ctx)
	if err != nil || string(ss.Preamble()) != "svc" {
		t.Fatalf("accept %v", err)
	}
	runtime.KeepAlive(st)
}
