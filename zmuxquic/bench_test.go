package zmuxquic

import (
	"context"
	"io"
	"testing"
	"time"
)

// BenchmarkBulkQUIC measures one bulk stream over a QUIC stream pipe.
func BenchmarkBulkQUIC(b *testing.B) {
	e := newEnv(b, mpConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, e.ln.Addr().String(), e.cli, nil, mpConfig())
	if err != nil {
		b.Fatal(err)
	}
	s, err := e.lst.Accept(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = c.Close()
		_ = s.Close()
		_ = c.Wait()
		_ = s.Wait()
	}()
	go func() {
		st, err := s.AcceptStream(context.Background())
		if err == nil {
			_, _ = io.Copy(io.Discard, st)
			_ = st.Close()
		}
	}()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
	_ = st.Close()
	_, _ = io.ReadAll(st)
}
