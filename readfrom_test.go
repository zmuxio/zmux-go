package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"runtime"
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

// A reset, close or stop while Read blocks ends ReadFrom with that error
// and discards the bytes of the Read in progress.
func TestReadFromInterrupted(t *testing.T) {
	leakcheck.Check(t)
	for _, tc := range []struct {
		name string
		stop func(src, dst *Stream)
	}{
		{"cancelwrite", func(src, _ *Stream) { src.CancelWrite(7) }},
		{"close", func(src, _ *Stream) { _ = src.Close() }},
		{"stopsending", func(_, dst *Stream) { dst.CancelRead(9) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, dst := readFromPair(t, nil)
			pr, pw := io.Pipe()
			res := make(chan error, 1)
			go func() {
				_, err := src.ReadFrom(pr)
				res <- err
			}()
			if _, err := pw.Write([]byte("kept")); err != nil {
				t.Fatal(err)
			}
			tc.stop(src, dst)
			deadline := time.Now().Add(5 * time.Second)
			for {
				// Keep feeding until a commit sees the stopped stream.
				go func() { _, _ = pw.Write([]byte("lost")) }()
				select {
				case err := <-res:
					if err == nil {
						t.Fatal("ReadFrom returned nil")
					}
					_ = pr.Close()
					return
				case <-time.After(20 * time.Millisecond):
				}
				if time.Now().After(deadline) {
					t.Fatal("ReadFrom did not return")
				}
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
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	before := ms.TotalAlloc
	const runs = 50
	for range runs {
		run()
	}
	runtime.ReadMemStats(&ms)
	if per := (ms.TotalAlloc - before) / runs; per >= 48<<10 {
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
