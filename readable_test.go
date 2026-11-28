package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// streamPairT opens a stream from a client to its server and returns both
// ends, the server's accepted once the first byte arrived.
func streamPairT(t *testing.T) (cs, ss *Stream) {
	t.Helper()
	a, b := tcpPair(t)
	c, s := pair(t, a, b, nil, nil)
	return openTestStream(t, c, s)
}

func memStreamPairT(t *testing.T) (*Stream, *Stream) {
	t.Helper()
	a, b := net.Pipe()
	cfg := &Config{ClockDomain: NewClockDomain(timeshim.Real)}
	c, s := pair(t, a, b, cfg, cfg)
	return openTestStream(t, c, s)
}

func openTestStream(t *testing.T, c, s *Conn) (cs, ss *Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if ss, err = s.AcceptStream(ctx); err != nil {
		t.Fatal(err)
	}
	return cs, ss
}

func TestWaitReadable(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := streamPairT(t)
	// Ready before the call: data already buffered; nothing is consumed.
	if err := ss.WaitReadable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ss.WaitReadable(ctx); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 8)
	if n, err := ss.Read(b); n != 1 || err != nil || b[0] != 'a' {
		t.Fatalf("read %d %v", n, err)
	}
	// Ready after the call: the wait ends when data arrives.
	done := make(chan error, 1)
	go func() { done <- ss.WaitReadable(ctx) }()
	if _, err := cs.Write([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n, _ := ss.Read(b); n != 1 || b[0] != 'b' {
		t.Fatalf("read %q", b[:n])
	}
	// ctx ends the wait; the read deadline too.
	short, scancel := context.WithCancel(ctx)
	scancel()
	if err := ss.WaitReadable(short); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	_ = ss.SetReadDeadline(time.Now())
	if err := ss.WaitReadable(ctx); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	_ = ss.SetReadDeadline(time.Time{})
	// EOF: ready, and Read returns io.EOF.
	_ = cs.Close()
	if err := ss.WaitReadable(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Read(b); !errors.Is(err, io.EOF) {
		t.Fatalf("after FIN: %v", err)
	}
	var nilStream *Stream
	if err := nilStream.WaitReadable(ctx); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("nil stream: %v", err)
	}
}

func TestWaitReadableReset(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs, ss := streamPairT(t)
	b := make([]byte, 8)
	_, _ = ss.Read(b)
	cs.CancelWrite(7)
	if err := ss.WaitReadable(ctx); err != nil {
		t.Fatal(err)
	}
	var se *StreamError
	if _, err := ss.Read(b); !errors.As(err, &se) {
		t.Fatalf("after reset: %v", err)
	}
}

func BenchmarkWaitReadableReady(b *testing.B) {
	a, c := tcpPair(b)
	cl, sv := pair(b, a, c, nil, nil)
	ctx := context.Background()
	cs, _ := cl.OpenStreamSync(ctx)
	_, _ = cs.Write([]byte("a"))
	ss, _ := sv.AcceptStream(ctx)
	b.ReportAllocs()
	for b.Loop() {
		_ = ss.WaitReadable(ctx)
	}
}

// Every end of the receiving part is ready, not an error of the wait: a local
// stop and the session's end too; the read that follows reports it.
func TestWaitReadableEnds(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := make([]byte, 8)
	cs, ss := streamPairT(t)
	_, _ = ss.Read(b)
	ss.CancelRead(9)
	if err := ss.WaitReadable(ctx); err != nil {
		t.Fatalf("after CancelRead: %v", err)
	}
	if _, err := ss.Read(b); err == nil {
		t.Fatal("Read after CancelRead succeeded")
	}
	cs2, ss2 := streamPairT(t)
	_, _ = ss2.Read(b)
	waited := make(chan error, 2)
	go func() { waited <- ss2.WaitReadable(ctx) }()
	go func() { waited <- ss2.WaitReadable(ctx) }() // several waiters
	ss2.h.c.Abort(3, "bye")
	for range 2 {
		if err := <-waited; err != nil {
			t.Fatalf("at the session's end: %v", err)
		}
	}
	if _, err := ss2.Read(b); err == nil {
		t.Fatal("Read at the session's end succeeded")
	}
	_, _ = cs, cs2
	// The receiving part alone.
	a, bb := tcpPair(t)
	c, s := pair(t, a, bb, nil, nil)
	us, err := c.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = us.Write([]byte("x"))
	rs, err := s.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.WaitReadable(ctx); err != nil {
		t.Fatal(err)
	}
	var nilRS *ReceiveStream
	if err := nilRS.WaitReadable(ctx); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("nil: %v", err)
	}
}

// One arrival wakes every WaitReadable and a blocked Read alike: the
// receiving part's wake slot holds one token, which each hands on.
func TestWaitReadableManyWaiters(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		b := make([]byte, 8)
		for _, withRead := range []bool{false, true} {
			cs, ss := memStreamPairT(t)
			_, _ = ss.Read(b)
			const n = 4
			waited := make(chan error, n)
			for range n {
				go func() { waited <- ss.WaitReadable(ctx) }()
			}
			var read chan error
			if withRead {
				read = make(chan error, 1)
				go func() {
					one := make([]byte, 1)
					_, err := ss.Read(one)
					read <- err
				}()
			}
			synctest.Wait()
			if _, err := cs.Write([]byte("xy")); err != nil {
				t.Fatal(err)
			}
			for range n {
				select {
				case err := <-waited:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatalf("withRead=%v: a waiter missed the arrival", withRead)
				}
			}
			if withRead {
				if err := <-read; err != nil {
					t.Fatal(err)
				}
			}
		}
	})
}

// A stream datagram of a stream the application released is refused, not
// queued.
func TestSendDatagramAfterRelease(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := tcpPair(t)
	cfg := &Config{EnableDatagrams: true}
	client, server := pair(t, a, b, cfg, cfg)
	cs, _ := openPackets(t, ctx, client, server)
	if err := cs.SendDatagram(0, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}
	cs.CancelRead(0)
	if err := cs.SendDatagram(0, []byte("x")); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendDatagram after release: %v", err)
	}
}
