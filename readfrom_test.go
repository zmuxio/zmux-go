package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"

	"github.com/zmuxio/zmux-go/internal/israce"
)

// readFromPair opens a stream from c to s and returns both ends.
func readFromPair(t *testing.T, scfg *Config) (*Stream, *Stream) {
	t.Helper()
	a, b := tcpPair(t)
	c, s := pair(t, a, b, nil, scfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	src, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Write([]byte{0}); err != nil { // makes the stream visible
		t.Fatal(err)
	}
	dst, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := io.ReadFull(dst, one[:]); err != nil {
		t.Fatal(err)
	}
	return src, dst
}

func TestReadFromData(t *testing.T) {
	leakcheck.Check(t)
	msg := make([]byte, 1<<20+333)
	_, _ = rand.Read(msg)
	readers := map[string]func() io.Reader{
		"plain":   func() io.Reader { return bytes.NewReader(msg) },
		"onebyte": func() io.Reader { return io.LimitReader(iotest.OneByteReader(bytes.NewReader(msg)), 5000) },
		"half":    func() io.Reader { return iotest.HalfReader(bytes.NewReader(msg)) },
		"dataeof": func() io.Reader { return iotest.DataErrReader(bytes.NewReader(msg)) },
	}
	for name, mk := range readers {
		t.Run(name, func(t *testing.T) {
			// A small window makes ReadFrom wait for stream credit.
			src, dst := readFromPair(t, &Config{InitialStreamReceiveWindow: 16 << 10})
			want, _ := io.ReadAll(mk())
			done := make(chan []byte)
			go func() {
				got, _ := io.ReadAll(dst)
				done <- got
			}()
			n, err := src.ReadFrom(mk())
			if err != nil || n != int64(len(want)) {
				t.Fatalf("ReadFrom = %d, %v; want %d", n, err, len(want))
			}
			_ = src.Close()
			if got := <-done; !bytes.Equal(got, want) {
				t.Fatalf("got %d bytes, mismatch", len(got))
			}
		})
	}
}

func TestReadFromReaderError(t *testing.T) {
	leakcheck.Check(t)
	src, dst := readFromPair(t, nil)
	boom := errors.New("boom")
	r := io.MultiReader(bytes.NewReader([]byte("abc")), iotest.ErrReader(boom))
	n, err := src.ReadFrom(r)
	if n != 3 || !errors.Is(err, boom) {
		t.Fatalf("ReadFrom = %d, %v", n, err)
	}
	// The stream stays usable for writes after a reader error.
	if _, err := src.Write([]byte("d")); err != nil {
		t.Fatal(err)
	}
	_ = src.Close()
	got, err := io.ReadAll(dst)
	if err != nil || string(got) != "abcd" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestReadFromInvalidCount(t *testing.T) {
	leakcheck.Check(t)
	src, _ := readFromPair(t, nil)
	if _, err := src.ReadFrom(badReader{}); !errors.Is(err, errInvalidRead) {
		t.Fatalf("err = %v", err)
	}
	if _, err := src.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
}

type badReader struct{}

func (badReader) Read(p []byte) (int, error) { return len(p) + 1, nil }

func TestReadFromPanicEndsLoan(t *testing.T) {
	leakcheck.Check(t)
	src, _ := readFromPair(t, nil)
	func() {
		defer func() { _ = recover() }()
		_, _ = src.ReadFrom(panicReader{})
	}()
	if _, err := src.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("reader") }

type gatedReadFrom struct {
	entered chan<- struct{}
	resume  <-chan struct{}
}

func (r gatedReadFrom) Read(b []byte) (int, error) {
	close(r.entered)
	<-r.resume
	return copy(b, "lost"), io.EOF
}

// A reset, close or stop while Read blocks ends ReadFrom with that error
// and discards the bytes of the Read in progress.
func TestReadFromInterrupted(t *testing.T) {
	leakcheck.Check(t)
	for _, tc := range []string{"cancelwrite", "close", "stopsending"} {
		t.Run(tc, func(t *testing.T) {
			src, dst := readFromPair(t, nil)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			entered, resume := make(chan struct{}), make(chan struct{})
			unhold := sync.OnceFunc(func() { close(resume) })
			defer unhold()
			type result struct {
				err error
				n   int64
			}
			res := make(chan result, 1)
			go func() {
				n, err := src.ReadFrom(gatedReadFrom{entered, resume})
				res <- result{err, n}
			}()
			waitTestSignal(t, entered, "ReadFrom reader")
			switch tc {
			case "cancelwrite":
				src.CancelWrite(7)
			case "close":
				_ = src.Close()
			case "stopsending":
				dst.CancelRead(9)
				var stop *StreamError
				if err := waitStreamStop(ctx, src); !errors.As(err, &stop) || !stop.Remote || stop.ErrorCode != 9 {
					t.Fatalf("STOP_SENDING: %v", err)
				}
			}
			unhold()
			select {
			case got := <-res:
				if got.err == nil || got.n != 0 {
					t.Fatalf("ReadFrom returned %d, %v after %s", got.n, got.err, tc)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

// ReadFrom allocates no buffer once the pools are warm: the reader is
// read straight into pooled send blocks. The exact count is checked in the
// core (TestWriteSpaceNoAlloc); here the process-wide figure includes the
// session's sending and the peer's receiving, so it bounds the bytes: the
// former 64 KiB buffer per call would exceed it.
func TestReadFromNoBuffer(t *testing.T) {
	if israce.On {
		t.Skip("the race detector allocates")
	}
	src, dst := readFromPair(t, nil)
	go func() { _, _ = io.Copy(io.Discard, dst) }()
	chunk := make([]byte, 256<<10)
	r := bytes.NewReader(chunk)
	run := func() {
		r.Reset(chunk)
		if _, err := src.ReadFrom(r); err != nil {
			t.Fatal(err)
		}
	}
	for range 20 {
		run()
	}
	// The figure is process-wide and counts pool misses: a collection
	// empties the pools, and the blocks and read buffers are then made
	// again. No collection runs while it is measured, and the least of
	// several rounds leaves out what other goroutines allocated meanwhile.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	per := uint64(math.MaxUint64)
	for range 5 {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		before := ms.TotalAlloc
		const runs = 20
		for range runs {
			run()
		}
		runtime.ReadMemStats(&ms)
		per = min(per, (ms.TotalAlloc-before)/runs)
	}
	if per >= 48<<10 {
		t.Fatalf("ReadFrom of 256 KiB: %d bytes allocated per call", per)
	}
}

func BenchmarkReadFromTCP(b *testing.B) {
	a, s := tcpPair(b)
	c, srv := pair(b, a, s, nil, nil)
	go func() {
		st, err := srv.AcceptStream(context.Background())
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, st)
		_ = st.Close()
	}()
	st, _ := c.OpenStreamSync(context.Background())
	chunk := make([]byte, 1<<20)
	r := bytes.NewReader(chunk)
	b.SetBytes(int64(len(chunk)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.Reset(chunk)
		if _, err := st.ReadFrom(r); err != nil {
			b.Fatal(err)
		}
	}
	_ = st.Close()
	_, _ = io.ReadAll(st)
}
