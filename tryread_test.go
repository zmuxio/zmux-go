package zmux

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/israce"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TryRead takes what is readable now and never waits: (0, nil) while nothing is,
// the data, io.EOF at the FIN, the stream's error after a reset, and with
// WaitReadable it reads without a Read that blocks.
func TestTryRead(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := streamPairT(t)
	buf := make([]byte, 16)
	// The first byte of the pair helper is already there.
	if n, err := ss.TryRead(buf); n != 1 || err != nil || buf[0] != 'a' {
		t.Fatalf("buffered byte: %d %v", n, err)
	}
	if n, err := ss.TryRead(buf); n != 0 || err != nil {
		t.Fatalf("nothing readable: %d %v", n, err)
	}
	if n, err := ss.TryRead(nil); n != 0 || err != nil {
		t.Fatalf("empty buffer: %d %v", n, err)
	}
	if _, err := cs.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	var got []byte
	for len(got) < 5 {
		if err := ss.WaitReadable(ctx); err != nil {
			t.Fatal(err)
		}
		n, err := ss.TryRead(buf[:3])
		if err != nil || n == 0 {
			t.Fatalf("readable, TryRead returned %d %v", n, err)
		}
		got = append(got, buf[:n]...)
	}
	if string(got) != "hello" {
		t.Fatalf("read %q", got)
	}
	if n, err := ss.TryRead(buf); n != 0 || err != nil {
		t.Fatalf("drained: %d %v", n, err)
	}
	// The end: io.EOF, after the data, and again on a repeated call.
	if _, err := cs.Write([]byte("z")); err != nil {
		t.Fatal(err)
	}
	_ = cs.Close()
	if err := ss.WaitReadable(ctx); err != nil {
		t.Fatal(err)
	}
	var eof bool
	for range 3 {
		if err := ss.WaitReadable(ctx); err != nil {
			t.Fatal(err)
		}
		n, err := ss.TryRead(buf)
		if n > 0 {
			if string(buf[:n]) != "z" || err != nil {
				t.Fatalf("last data %q %v", buf[:n], err)
			}
			continue
		}
		if !errors.Is(err, io.EOF) {
			t.Fatalf("at the end: %d %v", n, err)
		}
		eof = true
		break
	}
	if !eof {
		t.Fatal("no io.EOF")
	}
	if n, err := ss.TryRead(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("after the end: %d %v", n, err)
	}
}

// TryRead reports a reset and the session's end as errors, like Read, and
// leaves a Read that waits alone.
func TestTryReadErrors(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := streamPairT(t)
	buf := make([]byte, 8)
	_, _ = ss.Read(buf)
	// A Read that waits holds the read turn: TryRead answers "nothing now".
	read := make(chan error, 1)
	go func() { _, err := ss.Read(buf); read <- err }()
	if n, err := ss.TryRead(buf); n != 0 || err != nil {
		t.Fatalf("beside a waiting Read: %d %v", n, err)
	}
	cs.CancelWrite(9)
	var se *StreamError
	if err := <-read; !errors.As(err, &se) {
		t.Fatalf("waiting Read: %v", err)
	}
	if _, err := ss.TryRead(buf); !errors.As(err, &se) || se.ErrorCode != 9 {
		t.Fatalf("after the reset: %v", err)
	}

	// A stop by this endpoint and the session's end.
	cs2, ss2 := streamPairT(t)
	ss2.CancelRead(3)
	if _, err := ss2.TryRead(buf); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("after CancelRead: %v", err)
	}
	cs3, ss3 := streamPairT(t)
	_, _ = ss3.Read(buf)
	_ = cs3.h.c.Close()
	<-cs3.h.c.Done()
	if err := ss3.WaitReadable(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := cs3.TryRead(buf); n != 0 || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("after the session's end: %d %v", n, err)
	}
	_, _ = cs2, ss3
}

// TryRead does not allocate, whether it finds data or nothing.
func TestTryReadNoAlloc(t *testing.T) {
	if israce.On {
		t.Skip("the race detector allocates")
	}
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := streamPairT(t)
	buf := make([]byte, 1)
	_, _ = ss.Read(buf)
	if n := testing.AllocsPerRun(100, func() { _, _ = ss.TryRead(buf) }); n != 0 {
		t.Fatalf("nothing readable: %v allocations", n)
	}
	if _, err := cs.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := cs.Flush(); err != nil {
		t.Fatal(err)
	}
	ready := func() (n uint64) {
		ss.h.c.locked(func() { n, _ = ss.h.c.s.ReadReady(ss.h.st) })
		return n
	}
	for ready() < 4096 {
		if err := ss.WaitReadable(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond) // the rest of the data is on its way
	}
	if n := testing.AllocsPerRun(100, func() { _, _ = ss.TryRead(buf) }); n != 0 {
		t.Fatalf("data readable: %v allocations", n)
	}
}
