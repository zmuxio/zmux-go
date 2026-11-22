package zmux

import (
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/netx"
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
	lat := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		t := time.Now()
		_, _ = st.Write(buf)
		if _, err := io.ReadFull(st, buf); err != nil {
			b.Fatal(err)
		}
		lat = append(lat, time.Since(t))
	}
	b.StopTimer()
	slices.Sort(lat)
	b.ReportMetric(float64(lat[(len(lat)*99+99)/100-1])/1e3, "p99-us")
	b.ReportMetric(float64(c.ConnectionStats().BytesReinjected), "reinjected-B")
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
			_, err := bulk.Write(buf)
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
	b.ReportAllocs()
	b0 := srv.ConnectionStats().BytesRead
	b.ResetTimer()
	t0 := time.Now()
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
	received := srv.ConnectionStats().BytesRead - b0
	close(stop)
	<-done
	slices.Sort(lat)
	b.ReportMetric(float64(lat[len(lat)/2].Nanoseconds())/1e3, "p50-us")
	b.ReportMetric(float64(lat[len(lat)*99/100].Nanoseconds())/1e3, "p99-us")
	b.ReportMetric(float64(received-min(received, uint64(b.N)*64))/el.Seconds()/1e6, "bulk-MB/s")
	b.ReportMetric(float64(c.ConnectionStats().BytesReinjected), "reinjected-B")
}

// BenchmarkMultipathSpread measures one bulk stream spread over two TCP
// pipes and reports each pipe's share.
func BenchmarkMultipathSpread(b *testing.B) {
	for _, sched := range []struct {
		s    Scheduler
		name string
	}{{name: "round-robin", s: SchedulerSpreadRoundRobin()}, {name: "eta", s: SchedulerSpreadETA()}} {
		b.Run(sched.name, func(b *testing.B) {
			c, srv := multipathBenchPair(b, sched.s)
			benchmarkBulkPair(b, c, srv)
		})
	}
}

// BenchmarkMultipathHeterogeneousBulk sends a saturated stream across two
// carrier pairs with independently paced delivery and distinct RTTs.
func BenchmarkMultipathHeterogeneousBulk(b *testing.B) {
	ln := netx.NewChanListener(nil, 0)
	links := []link{
		{d: time.Millisecond, rate: 80e6, cap: 512 << 10},
		{d: 4 * time.Millisecond, rate: 35e6, cap: 512 << 10},
	}
	var dials atomic.Int32
	env := startMPEnv(b, mpConfig(), &ListenerConfig{Config: mpConfig()},
		&linkListener{Listener: ln, links: links},
		func(ctx context.Context) (net.Conn, error) {
			c, err := ln.Dial(ctx, nil)
			if err != nil {
				return nil, err
			}
			k := links[min(int(dials.Add(1))-1, len(links)-1)]
			return newDelayConn(c, k.d, k.rate, k.cap), nil
		})
	env.addPipe(b, PipeOptions{})
	env.client.SetScheduler(SchedulerSpreadETA())
	env.server.SetScheduler(SchedulerSpreadETA())
	benchmarkBulkPair(b, env.client, env.server)
}

func benchmarkBulkPair(b *testing.B, c, srv *Conn) {
	b.Helper()
	ctx := b.Context()
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
	b.ReportAllocs()
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
	b.ReportMetric(float64(c.ConnectionStats().BytesReinjected), "reinjected-B")
	var total float64
	ps := c.PipeStats()
	for _, p := range ps {
		total += float64(p.BytesSent)
	}
	for _, p := range ps {
		b.ReportMetric(float64(p.BytesSent)/total*100, fmt.Sprintf("pipe%d-%%", p.PipeID))
	}
}

// BenchmarkMultipathRRTCP measures a 64-byte request and its echo over
// two TCP carriers with spread ETA selected in both directions.
func BenchmarkMultipathRRTCP(b *testing.B) {
	c, srv := multipathBenchPair(b, SchedulerSpreadETA())
	ctx := b.Context()
	go func() {
		st, err := srv.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 64)
		for {
			if _, err := io.ReadFull(st, buf); err != nil {
				return
			}
			if _, err := st.Write(buf); err != nil {
				return
			}
		}
	}()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64)
	lat := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		at := time.Now()
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(st, buf); err != nil {
			b.Fatal(err)
		}
		lat = append(lat, time.Since(at))
	}
	b.StopTimer()
	slices.Sort(lat)
	b.ReportMetric(float64(lat[(len(lat)-1)/2])/1e3, "p50-us")
	b.ReportMetric(float64(lat[(len(lat)*99+99)/100-1])/1e3, "p99-us")
	b.ReportMetric(float64(c.ConnectionStats().BytesReinjected), "reinjected-B")
}

func multipathBenchPair(b *testing.B, sched Scheduler) (*Conn, *Conn) {
	b.Helper()
	env := newMPEnv(b, mpConfig(), mpConfig())
	env.addPipe(b, PipeOptions{})
	env.client.SetScheduler(sched)
	env.server.SetScheduler(sched)
	return env.client, env.server
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
