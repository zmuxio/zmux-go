package zmux

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Performance probe (opt-in): TestSessionMemory measures idle multipath
// sessions with both endpoints in this process. Run it on its own without
// the race detector for measurements.
func TestSessionMemory(t *testing.T) {
	if os.Getenv("ZMUX_MEM_PROBE") != "1" {
		t.Skip("set ZMUX_MEM_PROBE=1 to run the session memory probe")
	}
	n := 400
	if value := os.Getenv("ZMUX_MEM_N"); value != "" {
		var err error
		n, err = strconv.Atoi(value)
		if err != nil || n < 1 {
			t.Fatalf("ZMUX_MEM_N must be a positive integer, got %q", value)
		}
	}

	cfg := mpConfig()
	// Keep early sessions idle even while later sessions are still being set up.
	cfg.MaxIdleTimeout = -1
	cfg.Liveness = Liveness{KeepAlivePeriod: -1, ProbeInterval: -1, StandbyProbe: -1}
	l := mustNewListener(&ListenerConfig{Config: cfg})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	served := make(chan error, 1)
	go func() { served <- l.Serve(t.Context(), ln) }()

	sessions := make([][2]*Conn, n)
	streams := make([][2]*Stream, n)
	t.Cleanup(func() {
		_ = l.Close()
		for _, pair := range sessions {
			for _, c := range pair {
				if c != nil {
					c.Abort(0, "")
				}
			}
		}
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		for i, pair := range sessions {
			for j, c := range pair {
				if c != nil {
					select {
					case <-c.Done():
					case <-deadline.C:
						t.Fatalf("timed out closing session %d endpoint %d", i+1, j+1)
					}
				}
			}
		}
		select {
		case err := <-served:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Serve returned %v", err)
			}
		case <-deadline.C:
			t.Error("timed out stopping Serve")
		}
	})

	snapshot := func() (runtime.MemStats, int) {
		// Several cycles: each GC halves the stacks of goroutines that use
		// less than a quarter of theirs, and frees the stacks of exited ones.
		for range 6 {
			runtime.GC()
		}
		debug.FreeOSMemory()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m, runtime.NumGoroutine()
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	mode := os.Getenv("ZMUX_MEM_MODE")
	if mode != "" && mode != "handlers" {
		t.Fatalf("unknown memory probe mode %q", mode)
	}
	var loops sync.WaitGroup
	defer func() { cancel(); loops.Wait() }()
	if os.Getenv("ZMUX_MEM_PROFILE") != "" {
		runtime.MemProfileRate = 1 // every allocation, for an exact per-site breakdown
	}
	before, goroutinesBefore := snapshot()
	for i := range sessions {
		client, err := Dial(ctx, "tcp", addr, cfg)
		if err != nil {
			t.Fatalf("session %d dial: %v", i+1, err)
		}
		sessions[i][0] = client
		server, err := l.Accept(ctx)
		if err != nil {
			t.Fatalf("session %d accept: %v", i+1, err)
		}
		sessions[i][1] = server
		if !client.ConnectionState().Multipath {
			t.Fatalf("session %d did not negotiate multipath", i+1)
		}

		out, err := client.OpenStreamSync(ctx)
		if err != nil {
			t.Fatalf("session %d open stream: %v", i+1, err)
		}
		// Use bounded operations without adding per-stream deadline state to the sample.
		if wrote, err := out.TryWrite([]byte{1}); err != nil || wrote != 1 {
			t.Fatalf("session %d write: %d bytes, err %v", i+1, wrote, err)
		}
		in, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatalf("session %d accept stream: %v", i+1, err)
		}
		if err := in.WaitReadable(ctx); err != nil {
			t.Fatalf("session %d wait for stream data: %v", i+1, err)
		}
		var b [1]byte
		if read, err := in.TryRead(b[:]); err != nil || read != 1 || b[0] != 1 {
			t.Fatalf("session %d read: %d bytes, first byte %d, err %v", i+1, read, b[0], err)
		}
		streams[i] = [2]*Stream{out, in}
		if mode == "handlers" {
			if err := server.HandleStreams(func(st *Stream) { _ = st.Close() }, 1); err != nil {
				t.Fatal(err)
			}
			if err := server.HandleEvents(func(Event) {}); err != nil {
				t.Fatal(err)
			}
		} else {
			loops.Go(func() {
				for {
					st, err := server.AcceptStream(ctx)
					if err != nil {
						return
					}
					_ = st.Close()
				}
			})
			loops.Go(func() {
				for {
					if _, err := server.NextEvent(ctx); err != nil {
						return
					}
				}
			})
		}
	}

	// Let the last session process its acknowledgements and its idle writers exit.
	time.Sleep(writerIdle + 500*time.Millisecond)
	after, goroutinesAfter := snapshot()
	if dir := os.Getenv("ZMUX_MEM_PROFILE"); dir != "" {
		// Heap and goroutine profiles of the idle sessions, for go tool pprof.
		for _, name := range []string{"heap", "goroutine"} {
			f, err := os.Create(filepath.Join(dir, name+".pprof"))
			if err != nil {
				t.Fatal(err)
			}
			if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()
		}
	}
	runtime.KeepAlive(streams)
	kibPerSession := func(before, after uint64) float64 {
		return (float64(after) - float64(before)) / (1024 * float64(n))
	}
	t.Logf("%d idle multipath sessions, one TCP pipe and one bidirectional stream each", n)
	t.Logf("Per session (client + server): HeapInuse %.2f KiB, HeapAlloc %.2f KiB, StackInuse %.2f KiB, goroutines %.2f",
		kibPerSession(before.HeapInuse, after.HeapInuse), kibPerSession(before.HeapAlloc, after.HeapAlloc),
		kibPerSession(before.StackInuse, after.StackInuse), float64(goroutinesAfter-goroutinesBefore)/float64(n))
}
