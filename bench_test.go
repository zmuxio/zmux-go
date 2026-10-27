package zmux

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkBulkTCP(b *testing.B) {
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
	buf := make([]byte, 64<<10)
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

func BenchmarkRRTCP(b *testing.B) {
	a, s := tcpPair(b)
	c, srv := pair(b, a, s, nil, nil)
	go func() {
		st, err := srv.AcceptStream(context.Background())
		if err != nil {
			return
		}
		buf := make([]byte, 64)
		for {
			if _, err := io.ReadFull(st, buf); err != nil {
				return
			}
			_, _ = st.Write(buf)
		}
	}()
	st, _ := c.OpenStreamSync(context.Background())
	buf := make([]byte, 64)
	b.ResetTimer()
	for range b.N {
		_, _ = st.Write(buf)
		if _, err := io.ReadFull(st, buf); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRRUnderBulk measures 64-byte request-response latency while a
// bulk stream runs on the same session (TCP loopback): p50 and p99 of the
// requests, and the bulk stream's throughput meanwhile.
func BenchmarkRRUnderBulk(b *testing.B) {
	a, s := tcpPair(b)
	c, srv := pair(b, a, s, nil, nil)
	ctx := context.Background()
	go func() {
		for {
			st, err := srv.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func() {
				hdr := make([]byte, 1)
				if _, err := io.ReadFull(st, hdr); err != nil {
					return
				}
				if hdr[0] == 'b' {
					_, _ = io.Copy(io.Discard, st)
					_ = st.Close()
					return
				}
				buf := make([]byte, 64)
				for {
					if _, err := io.ReadFull(st, buf); err != nil {
						return
					}
					_, _ = st.Write(buf)
				}
			}()
		}
	}()
	bulk, _ := c.OpenStreamSync(ctx)
	_, _ = bulk.Write([]byte{'b'})
	var bulkBytes atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64<<10)
		for {
			select {
			case <-stop:
				_ = bulk.Close()
				return
			default:
			}
			n, err := bulk.Write(buf)
			bulkBytes.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
	rr, _ := c.OpenStreamSync(ctx)
	_, _ = rr.Write([]byte{'r'})
	rr.SetPriority(0, false) // latency-sensitive: ahead of the bulk stream
	bulk.SetPriority(UrgencyDefault, false)
	buf := make([]byte, 64)
	lat := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	t0 := time.Now()
	b0 := bulkBytes.Load()
	for range b.N {
		t := time.Now()
		_, _ = rr.Write(buf)
		if _, err := io.ReadFull(rr, buf); err != nil {
			b.Fatal(err)
		}
		lat = append(lat, time.Since(t))
	}
	el := time.Since(t0)
	b.StopTimer()
	close(stop)
	<-done
	slices.Sort(lat)
	b.ReportMetric(float64(lat[len(lat)/2].Nanoseconds())/1e3, "p50-us")
	b.ReportMetric(float64(lat[len(lat)*99/100].Nanoseconds())/1e3, "p99-us")
	b.ReportMetric(float64(bulkBytes.Load()-b0)/el.Seconds()/1e6, "bulk-MB/s")
}

// BenchmarkMultipathSpread measures one bulk stream spread over two TCP
// pipes (round robin), and reports each pipe's share.
func BenchmarkMultipathSpread(b *testing.B) {
	for _, sched := range []struct {
		name string
		s    Scheduler
	}{{"round-robin", SpreadRoundRobin()}, {"eta", SpreadETA()}} {
		b.Run(sched.name, func(b *testing.B) {
			cfg := &Config{MaxPipes: 4, PipeAuth: PipeAuth{PSK: bytes.Repeat([]byte{7}, 32)}}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			l := NewListener(&ListenerConfig{Config: cfg})
			defer func() { _ = l.Close() }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				for {
					nc, err := ln.Accept()
					if err != nil {
						return
					}
					go func() { _ = l.HandlePipe(ctx, NetPipe(nc), JoinHint{}) }()
				}
			}()
			nc, _ := net.Dial("tcp", ln.Addr().String())
			c, err := Client(ctx, nc, cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			srv, err := l.Accept(ctx)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = srv.Close() }()
			nc2, _ := net.Dial("tcp", ln.Addr().String())
			if _, err := c.AddPipe(ctx, NetPipe(nc2), PipeOptions{}); err != nil {
				b.Fatal(err)
			}
			c.SetScheduler(sched.s)
			got := make(chan struct{})
			go func() {
				defer close(got)
				st, err := srv.AcceptStream(ctx)
				if err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, st)
				_ = st.Close()
			}()
			st, _ := c.OpenStreamSync(ctx)
			buf := make([]byte, 64<<10)
			b.SetBytes(int64(len(buf)))
			b.ResetTimer()
			for range b.N {
				if _, err := st.Write(buf); err != nil {
					b.Fatal(err)
				}
			}
			_ = st.Close()
			_, _ = io.ReadAll(st)
			<-got
			b.StopTimer()
			var total float64
			ps := c.PipeStats()
			for _, p := range ps {
				total += float64(p.BytesSent)
			}
			for _, p := range ps {
				b.ReportMetric(float64(p.BytesSent)/total*100, fmt.Sprintf("pipe%d-%%", p.ID))
			}
		})
	}
}

// BenchmarkRRRawTCP is the baseline of BenchmarkRRTCP: 64-byte
// request-response on a bare loopback TCP connection.
func BenchmarkRRRawTCP(b *testing.B) {
	a, s := tcpPair(b)
	defer func() { _ = a.Close() }()
	defer func() { _ = s.Close() }()
	go func() {
		buf := make([]byte, 64)
		for {
			if _, err := io.ReadFull(s, buf); err != nil {
				return
			}
			_, _ = s.Write(buf)
		}
	}()
	buf := make([]byte, 64)
	b.ResetTimer()
	for range b.N {
		_, _ = a.Write(buf)
		if _, err := io.ReadFull(a, buf); err != nil {
			b.Fatal(err)
		}
	}
}
