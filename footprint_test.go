package zmux

import (
	"context"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

// Idle footprint: the live heap of idle sessions,
// streams and multipath sessions, both ends in this process over
// in-memory pipes, after a warm-up, without idle timeout or keep-alive,
// once the idle pipes gave their buffers back. Bounds: 17 408 B per
// session pair, 1536 B per stream (both ends), 45 056 B per multipath
// session pair (two pipe pairs, one standby). Run alone (the race
// detector and parallel tests distort it):
//
//	ZMUX_FOOTPRINT=1 go test -run Footprint -count=1 -v .

const fpN = 200

// warmGoroutines leaves n goroutine descriptors in the runtime's free
// list: the runtime never frees them, so a phase that runs more goroutines
// at once than any before it (timer callbacks, which a quiet session does
// not have once settled) would otherwise count their descriptors.
func warmGoroutines(n int) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range n {
		wg.Go(func() { <-start })
	}
	close(start)
	wg.Wait()
}

func heapNow() uint64 {
	var m runtime.MemStats
	for range 3 {
		runtime.GC()
	}
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func fpConfig() *Config { return &Config{MaxIdleTimeout: -1} }

func fpMPConfig() *Config {
	return &Config{MaxIdleTimeout: -1, MaxPipes: 4, PipeAuth: PipeAuth{PSK: []byte("a pre-shared key of 32 bytes....")}}
}

func fpPairs(t *testing.T, n int) [][2]*Conn { return fpPairsOver(t, n, nil) }

func fpPairsOver(t *testing.T, n int, ln net.Listener) [][2]*Conn {
	t.Helper()
	ctx := context.Background()
	out := make([][2]*Conn, n)
	for i := range out {
		a, b := net.Pipe()
		if ln != nil {
			var err error
			if a, err = net.Dial("tcp", ln.Addr().String()); err != nil {
				t.Fatal(err)
			}
			if b, err = ln.Accept(); err != nil {
				t.Fatal(err)
			}
		}
		ch := make(chan *Conn, 1)
		go func() {
			s, err := Server(ctx, b, fpConfig())
			if err != nil {
				t.Error(err)
			}
			ch <- s
		}()
		c, err := Client(ctx, a, fpConfig())
		if err != nil {
			t.Fatal(err)
		}
		out[i] = [2]*Conn{c, <-ch}
	}
	return out
}

func fpMPPairs(t *testing.T, l *Listener, n int) [][2]*Conn {
	t.Helper()
	ctx := context.Background()
	out := make([][2]*Conn, n)
	standby := true
	for i := range out {
		a, b := net.Pipe()
		go func() { _ = l.HandlePipe(ctx, NetPipe(b), JoinHint{}) }()
		c, err := Client(ctx, a, fpMPConfig())
		if err != nil {
			t.Fatal(err)
		}
		s, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		a2, b2 := net.Pipe()
		go func() { _ = l.HandlePipe(ctx, NetPipe(b2), JoinHint{}) }()
		if _, err := c.AddPipe(ctx, NetPipe(a2), PipeOptions{SendStandby: &standby, ReceiveStandby: true}); err != nil {
			t.Fatal(err)
		}
		out[i] = [2]*Conn{c, s}
	}
	return out
}

func closePairs(v [][2]*Conn) {
	for _, p := range v {
		_ = p[0].Close()
		_ = p[1].Close()
	}
	for _, p := range v {
		_ = p[0].Wait()
		_ = p[1].Wait()
	}
}

func TestFootprint(t *testing.T) {
	if os.Getenv("ZMUX_FOOTPRINT") == "" {
		t.Skip("ZMUX_FOOTPRINT not set")
	}
	idle := 1500 * time.Millisecond
	if d, err := time.ParseDuration(os.Getenv("ZMUX_FOOTPRINT_IDLE")); err == nil {
		idle = d
	}
	check := func(name string, per, bound float64) {
		t.Logf("%s: %.0f B (bound %.0f B)", name, per, bound)
		if per > bound {
			t.Errorf("%s: %.0f B above the bound %.0f B", name, per, bound)
		}
	}
	if os.Getenv("ZMUX_FOOTPRINT_PRECISE") != "" {
		runtime.MemProfileRate = 4096
	}
	// Sessions: a warm-up round (with a stream each), then the measured
	// one.
	warmup := fpPairs(t, fpN)
	for _, p := range warmup {
		st, _ := p[0].OpenStreamSync(context.Background())
		_, _ = st.Write([]byte{1})
		ss, _ := p[1].AcceptStream(context.Background())
		_, _ = io.ReadFull(ss, make([]byte, 1))
	}
	closePairs(warmup)
	warmGoroutines(20 * fpN)
	time.Sleep(idle) // the closed sessions' memory released
	g0 := runtime.NumGoroutine()
	before := heapNow()
	if f := os.Getenv("ZMUX_FOOTPRINT_PROFILE"); f != "" && os.Getenv("ZMUX_FOOTPRINT_PRECISE") != "" {
		if w, err := os.Create(f + ".before"); err == nil {
			_ = pprof.WriteHeapProfile(w)
			_ = w.Close()
		}
	}
	v := fpPairs(t, fpN)
	time.Sleep(idle)
	after := heapNow()
	if f := os.Getenv("ZMUX_FOOTPRINT_PROFILE"); f != "" {
		if w, err := os.Create(f); err == nil {
			_ = pprof.WriteHeapProfile(w)
			_ = w.Close()
		}
	}
	check("idle session pair, in-memory pipe", float64(after-before)/fpN, 17408)
	if g := float64(runtime.NumGoroutine()-g0) / (2 * fpN); g > 2 {
		t.Errorf("goroutines per idle session (one end): %.2f, more than 2", g)
	}

	closePairs(v)

	// The same over loopback TCP (the sockets' user-space state counted).
	tl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tl.Close() }()
	closePairs(fpPairsOver(t, fpN, tl))
	time.Sleep(idle)
	before = heapNow()
	v = fpPairsOver(t, fpN, tl)
	time.Sleep(idle)
	after = heapNow()
	check("idle session pair, loopback TCP", float64(after-before)/fpN, 17408)
	closePairs(v)

	// Streams: opened by one end, accepted by the other, nothing buffered;
	// many on one session pair, as the per-stream cost.
	const nst = 10000
	scfg := &Config{MaxIdleTimeout: -1, MaxIncomingStreams: 2 * nst}
	pc, pd := net.Pipe()
	sc, ss := pair(t, pc, pd, scfg, scfg)
	ctx := context.Background()
	streams := make([][2]*Stream, 0, nst)
	accepted := make(chan *Stream, nst)
	go func() {
		for range nst {
			st, err := ss.AcceptStream(ctx)
			if err != nil {
				close(accepted)
				return
			}
			accepted <- st
		}
	}()
	before = heapNow()
	for range nst {
		st, err := sc.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = st.Write([]byte{1})
		streams = append(streams, [2]*Stream{st, nil})
	}
	for i := range streams {
		st, ok := <-accepted
		if !ok {
			t.Fatal("accept failed")
		}
		_, _ = io.ReadFull(st, make([]byte, 1))
		streams[i][1] = st
	}
	time.Sleep(idle)
	after = heapNow()
	if f := os.Getenv("ZMUX_FOOTPRINT_PROFILE"); f != "" {
		if w, err := os.Create(f + ".streams"); err == nil {
			_ = pprof.WriteHeapProfile(w)
			_ = w.Close()
		}
	}
	check("idle stream (both ends)", float64(after-before)/nst, 1536)
	runtime.KeepAlive(streams)
	_ = sc.Close()
	_ = ss.Close()
	_ = sc.Wait()
	_ = ss.Wait()

	// Multipath: two pipe pairs, the second standby, after the join.
	warm := NewListener(&ListenerConfig{Config: fpMPConfig()})
	closePairs(fpMPPairs(t, warm, fpN))
	_ = warm.Close()
	l := NewListener(&ListenerConfig{Config: fpMPConfig()})
	defer func() { _ = l.Close() }()
	time.Sleep(idle)
	g0 = runtime.NumGoroutine()
	before = heapNow()
	mv := fpMPPairs(t, l, fpN)
	// Quiet-session probing ends 15 s after the join.
	time.Sleep(16 * time.Second)
	after = heapNow()
	check("idle multipath session pair, two pipes", float64(after-before)/fpN, 45056)
	// The listener side's HandlePipe calls (one per pipe) are the
	// application's goroutines, not the session's.
	if g := float64(runtime.NumGoroutine()-g0-2*fpN) / (2 * fpN); g > 4 {
		t.Errorf("goroutines per idle multipath session (one end): %.2f, more than 4", g)
	}
	closePairs(mv)
}

// TestQuietSessionsFireNoTimer checks that settled quiet
// sessions (no idle timeout, no keep-alive) arm no timer, and settling
// takes a few timer callbacks, not a busy loop.
func TestQuietSessionsFireNoTimer(t *testing.T) {
	const n = 100
	f0 := timerFires.Load()
	v := fpPairs(t, n)
	defer closePairs(v)
	time.Sleep(time.Second)
	settle := timerFires.Load() - f0
	if settle > 10*2*n {
		t.Fatalf("%d timer callbacks while %d sessions settled", settle, 2*n)
	}
	f1 := timerFires.Load()
	time.Sleep(2 * time.Second)
	if d := timerFires.Load() - f1; d != 0 {
		t.Fatalf("%d timer callbacks in quiet sessions", d)
	}
}
