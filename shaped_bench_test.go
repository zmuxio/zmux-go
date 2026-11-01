package zmux

import (
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/zmuxio/zmux-go/shaping"
)

// writeCalls returns the process's write-family system calls (Linux
// /proc/self/io syscw); false where it is not available.
func writeCalls() (uint64, bool) {
	b, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, false
	}
	for _, l := range bytes.Split(b, []byte("\n")) {
		if v, ok := bytes.CutPrefix(l, []byte("syscw: ")); ok {
			n, err := strconv.ParseUint(string(v), 10, 64)
			return n, err == nil
		}
	}
	return 0, false
}

// BenchmarkBulkShaped measures bulk throughput with every record planned
// at 100 to 700 bytes (TCP loopback), and write calls per MB on Linux.
func BenchmarkBulkShaped(b *testing.B) {
	a, s := tcpPair(b)
	c, srv := pair(b, a, s, nil, nil)
	var seq int
	c.SetShaper(func(ShapedPipe) shaping.Shaper {
		return &fnShaper{caps: shaping.CapPlan, plan: func(_ *shaping.RecordView, p *shaping.RecordPlan) {
			seq = (seq*1103515245 + 12345) & 0x7fffffff
			p.MaxLen = 100 + seq%601
		}}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		st, err := srv.AcceptStream(context.Background())
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, st)
		_ = st.Close()
	}()
	st, _ := c.OpenStreamSync(context.Background())
	buf := make([]byte, 64<<10)
	b.SetBytes(int64(len(buf)))
	w0, ok := writeCalls()
	b.ResetTimer()
	for range b.N {
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
	_ = st.Close()
	<-done
	b.StopTimer()
	if w1, ok2 := writeCalls(); ok && ok2 {
		b.ReportMetric(float64(w1-w0)/(float64(b.N)*float64(len(buf))/1e6), "writes/MB")
	}
}

// BenchmarkBulkTCP1M is BenchmarkBulkTCP with 1 MiB writes.
func BenchmarkBulkTCP1M(b *testing.B) {
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
	buf := make([]byte, 1<<20)
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for range b.N {
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
	_ = st.Close()
	_, _ = io.ReadAll(st)
}
