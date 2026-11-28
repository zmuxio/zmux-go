package zmuxquic

import (
	"context"
	"io"
	"sync"
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

// BenchmarkBulkQUICLanes measures eight concurrent bulk streams on one pipe.
func BenchmarkBulkQUICLanes(b *testing.B) {
	const streams, chunk = 8, 64 << 10
	cfg := mpConfig()
	cfg.MaxLanes = streams + 1
	e := newEnv(b, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	c, err := Dial(ctx, e.ln.Addr().String(), e.cli, nil, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = c.Close()
		_ = c.Wait()
	}()
	s, err := e.lst.Accept(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = s.Close()
		_ = s.Wait()
	}()
	deadline, _ := ctx.Deadline()
	var senders [streams]func()
	for i := range senders {
		st, err := c.OpenUniStreamSync(ctx)
		if err != nil {
			b.Fatal(err)
		}
		st.SetPriority(4, true)
		if err := st.SetWriteDeadline(deadline); err != nil {
			b.Fatal(err)
		}
		senders[i] = func() {
			defer func() {
				if err := st.Close(); err != nil {
					b.Error(err)
				}
			}()
			buf := make([]byte, chunk)
			for range b.N {
				if _, err := st.Write(buf); err != nil {
					b.Error(err)
					return
				}
			}
		}
	}
	var readers, writers sync.WaitGroup
	readers.Go(func() {
		var drains sync.WaitGroup
		defer drains.Wait()
		for range streams {
			st, err := s.AcceptUniStream(ctx)
			if err != nil {
				b.Error(err)
				return
			}
			drains.Go(func() {
				if err := st.SetReadDeadline(deadline); err != nil {
					b.Error(err)
					return
				}
				if n, err := io.Copy(io.Discard, st); err != nil || n != int64(b.N)*chunk {
					b.Errorf("drained %d bytes: %v", n, err)
				}
			})
		}
	})
	b.SetBytes(streams * chunk)
	b.ReportAllocs()
	b.ResetTimer()
	for _, send := range senders {
		writers.Go(send)
	}
	writers.Wait()
	readers.Wait()
	b.StopTimer()
	if b.N > 32 {
		for _, p := range c.PipeStats() {
			if p.LanesOut > 1 {
				return
			}
		}
		b.Fatal("no data lane opened")
	}
}
