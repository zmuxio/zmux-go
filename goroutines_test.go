package zmux

import (
	"bytes"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// finished closes once the owner finished and the last goroutine ended,
// whichever comes last, with no goroutine of its own.
func TestPipeGoroutinesFinished(t *testing.T) {
	closed := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	var g pipeGoroutines
	g.add()
	ch := g.finished()
	g.finish()
	if closed(ch) {
		t.Fatal("finished with a goroutine running")
	}
	g.done()
	if !closed(ch) || !closed(g.finished()) {
		t.Fatal("not finished after the last goroutine")
	}
	var h pipeGoroutines
	h.finish()
	if !closed(h.finished()) {
		t.Fatal("an idle owner that finished is not finished")
	}
}

func BenchmarkDoneChannel(b *testing.B) {
	var g pipeGoroutines
	b.ReportAllocs()
	for b.Loop() {
		_ = g.finished()
	}
}

// A pipe's Done closes once it was retired and its driver's goroutines
// ended, with no goroutine of its own.
func TestPipeEndDone(t *testing.T) {
	e := &pipeEnd{retired: make(chan struct{})}
	var g pipeGoroutines
	g.add()
	ch := e.done()
	g.finishThen(e.end)
	select {
	case <-ch:
		t.Fatal("Done closed while a goroutine of the pipe runs")
	default:
	}
	g.done()
	select {
	case <-ch:
	default:
		t.Fatal("Done open after the pipe's last goroutine")
	}
	if e.done() != closedChan {
		t.Fatal("a later Done is not closed")
	}
}

// arm is called when synctest.Wait has established that the carrier is idle.
type writerGate struct {
	net.Conn
	entered, release chan struct{}
	active           atomic.Int32
	overlap, closed  atomic.Bool
}

func (g *writerGate) arm() {
	g.entered = make(chan struct{}, 1)
	g.release = make(chan struct{})
}
func (g *writerGate) Write(b []byte) (int, error) {
	if g.active.Add(1) != 1 {
		g.overlap.Store(true)
	}
	defer g.active.Add(-1)
	if g.release != nil {
		select {
		case g.entered <- struct{}{}:
		default:
		}
		<-g.release
	}
	return g.Conn.Write(b)
}
func (g *writerGate) Close() error { g.closed.Store(true); return g.Conn.Close() }

// Race several writes and a FIN from an idle lane. Every accepted byte
// must precede FIN, including writes that won the race to the stream lock.
func TestIdleWriterConcurrentClose(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		gate := &writerGate{Conn: a}
		client, server := pair(t, gate, b, nil, nil)
		out, err := client.OpenStreamSync(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = out.Write([]byte{0}); err != nil {
			t.Fatal(err)
		}
		in, err := server.AcceptStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var first [1]byte
		if _, err = io.ReadFull(in, first[:]); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		gate.arm()
		if n, err := out.Write([]byte{99}); n != 1 || err != nil {
			t.Fatal(n, err)
		}
		<-gate.entered
		const writers = 8
		start := make(chan struct{})
		accepted := make(chan int, writers)
		for id := range writers {
			go func() {
				<-start
				n, _ := out.Write([]byte{byte(id + 1)})
				if n == 1 {
					accepted <- id + 1
				} else {
					accepted <- 0
				}
			}()
		}
		done := make(chan error, 1)
		go func() { <-start; done <- out.Close() }()
		close(start)
		synctest.Wait()
		close(gate.release)
		got, err := io.ReadAll(in)
		if err != nil {
			t.Fatal(err)
		}
		if err = <-done; err != nil {
			t.Fatal(err)
		}
		if gate.overlap.Load() || len(got) == 0 || got[0] != 99 {
			t.Fatalf("overlapping carrier writes or missing first byte: %v", got)
		}
		got = got[1:]
		for range writers {
			if id := <-accepted; id != 0 {
				if bytes.Count(got, []byte{byte(id)}) != 1 {
					t.Fatalf("missing or duplicate write %d in %v", id, got)
				}
				got = bytes.Replace(got, []byte{byte(id)}, nil, 1)
			}
		}
		if len(got) > 0 {
			t.Fatalf("unreported bytes %v", got)
		}
	})
}

// Take must wait for the restarted writer even after the transport closed.
func TestTakeWaitsForRestartedWriter(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		l := mustNewListener(&ListenerConfig{Config: &Config{ClockDomain: NewClockDomain(timeshim.Real)}})
		defer func() { _ = l.Close() }()
		a, b := net.Pipe()
		g := &writerGate{Conn: b}
		arrival, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
		if err != nil {
			t.Fatal(err)
		}
		defer arrival.Release()
		run, err := arrival.Take(t.Context(), NetPipe(g), PipeHint{})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- run(t.Context()) }()
		c, err := Client(t.Context(), a, &Config{ClockDomain: NewClockDomain(timeshim.Real)})
		if err != nil {
			t.Fatal(err)
		}
		server, err := l.Accept(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		out, err := server.OpenStreamSync(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * writerIdle) // synctest virtual time
		synctest.Wait()
		if server.pipes[0].writer.running {
			t.Fatal("server writer still active before restart")
		}
		g.arm()
		if n, err := out.Write([]byte{42}); n != 1 || err != nil {
			t.Fatal(n, err)
		}
		<-g.entered
		_ = a.Close()
		synctest.Wait()
		if !g.closed.Load() {
			t.Fatal("carrier still open")
		}
		select {
		case err := <-done:
			t.Fatalf("Take finished with carrier write blocked: %v", err)
		default:
		}
		close(g.release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		<-server.Done()
		<-c.Done()
	})
}

func TestWriterIdleDeadlineAndReuse(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Now())
		cfg := &Config{ClockDomain: NewClockDomain(clk), MaxIdleTimeout: -1}
		a, b := net.Pipe()
		client, server := pair(t, a, b, cfg, cfg)
		out, err := client.OpenStreamSync(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		send := func(s *Stream, value byte) {
			t.Helper()
			if n, err := s.Write([]byte{value}); n != 1 || err != nil {
				t.Fatalf("write: %d, %v", n, err)
			}
		}
		receive := func(s *Stream, value byte) {
			t.Helper()
			var data [1]byte
			if _, err := io.ReadFull(s, data[:]); err != nil || data[0] != value {
				t.Fatalf("read %v, %v; want %d", data, err, value)
			}
		}
		send(out, 0)
		in, err := server.AcceptStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		receive(in, 0)
		send(in, 0)
		receive(out, 0)
		synctest.Wait()
		writer := &client.pipes[0].writer
		peer := &server.pipes[0].writer
		starts, peerStarts := writer.starts, peer.starts
		for i := byte(1); i <= 5; i++ {
			clk.Advance(writerIdle - time.Nanosecond)
			synctest.Wait()
			if !writer.running || !peer.running {
				t.Fatal("writer exited before idle deadline")
			}
			send(out, i)
			receive(in, i)
			send(in, i)
			receive(out, i)
			synctest.Wait()
			if writer.starts != starts || peer.starts != peerStarts {
				t.Fatal("writer restarted during request/response")
			}
		}
		// Order the timeout and the new request in the same critical section.
		// The woken writer can only check its ownership after both are visible.
		client.locked(func() {
			if writer.idleAt <= 0 {
				t.Fatal("writer never parked")
			}
			client.expireWritersLocked(writer.idleAt)
			if n, err := client.s.Write(out.h.st, []byte{42}, false); n != 1 || err != nil {
				t.Errorf("write at expiry: %d, %v", n, err)
			}
		})
		receive(in, 42)
		synctest.Wait()
		if writer.starts != starts {
			t.Fatal("lost ownership with a pending request")
		}
		// Allow any acknowledgement requested by the last exchange to settle.
		clk.Advance(writerIdle)
		synctest.Wait()
		clk.Advance(writerIdle + time.Nanosecond)
		synctest.Wait()
		if writer.running || peer.running {
			t.Fatal("writer remained after idle deadline")
		}
		starts = writer.starts
		send(out, 43)
		receive(in, 43)
		synctest.Wait()
		if writer.starts != starts+1 {
			t.Fatalf("expected one restart, got %d", writer.starts-starts)
		}
	})
}

// Expiring a parked writer alone must not run the multipath timeout logic:
// that can request another poll and keep a quiet writer alive indefinitely.
func TestMultipathWriterIdleExpiry(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		cc, sc := mpConfig(), mpConfig()
		cc.Liveness.ProbeInterval, sc.Liveness.ProbeInterval = -1, -1
		e := newMemMPEnv(t, cc, sc)
		synctest.Wait()
		<-time.After(writerIdle + 500*time.Millisecond) // virtual time, including the bootstrap acknowledgement
		synctest.Wait()
		for _, c := range []*Conn{e.client, e.server} {
			c.peek(func() {
				if c.pipes[0].writer.running {
					t.Error("bootstrap writer remained after its idle deadline")
				}
			})
		}
	})
}

// Model parked writers that already received a wake and are waiting to
// reacquire the session lock. A reservation scan would send another token
// to every one of them while servicing a fresh request for a single lane.
func TestWriterReservationsVisitRequestedLane(t *testing.T) {
	c := &Conn{s: &core.Session{}, pipes: make(map[uint64]*pipeDrv)}
	for id := range uint64(32) {
		pd := &pipeDrv{end: &pipeEnd{}, lanes: make(map[uint64]*laneOut)}
		c.pipes[id] = pd
		for lane := uint64(1); lane <= 16; lane++ {
			lo := &laneOut{}
			pd.lanes[lane] = lo
			w := &lo.writer
			w.ctx, w.running, w.idleAt = t.Context(), true, 1
			w.poll.Ready.Store(true)
			w.wake.Make()
		}
	}
	const pipe, lane = 29, 13
	want := &c.pipes[pipe].lanes[lane].writer
	c.s.QueueWriter(pipe, lane, &want.poll)
	if starts := c.reserveWritersLocked(nil); len(starts) != 0 {
		t.Fatal("reserved a second running writer")
	}
	for id, pd := range c.pipes {
		for n, lo := range pd.lanes {
			if got := len(lo.writer.wake.C) != 0; got != (&lo.writer == want) {
				t.Fatalf("writer (%d, %d) received token = %t", id, n, got)
			}
		}
	}
}

// Idling or waking existing writers reuses the core request list and
// never allocates, including with multiple ready lanes.
func TestWriterRequestAllocations(t *testing.T) {
	for _, n := range []int{0, 1, 16} {
		pd := &pipeDrv{end: &pipeEnd{}, lanes: make(map[uint64]*laneOut)}
		c := &Conn{s: &core.Session{}, pipes: map[uint64]*pipeDrv{0: pd}}
		for i := 1; i <= n; i++ {
			pd.lanes[uint64(i)] = &laneOut{writer: laneWriter{ctx: t.Context(), running: true}}
		}
		c.lock() // the request walk executes within this single lock entry
		allocs := testing.AllocsPerRun(100, func() {
			for i := 1; i <= n; i++ {
				w := &pd.lanes[uint64(i)].writer
				w.poll.Ready.Store(true)
				c.s.QueueWriter(0, uint64(i), &w.poll)
			}
			if starts := c.reserveWritersLocked(nil); len(starts) != 0 {
				t.Fatal("duplicated writer")
			}
		})
		c.unlockPeek()
		if allocs != 0 {
			t.Fatalf("%d lanes: %g allocations", n, allocs)
		}
	}
}

func TestSessionTimerDeferredDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Now())
		domain := NewClockDomain(clk)
		cfg := core.DefaultConfig()
		cfg.HoldFirstFlight = true
		c := newConn(&Config{ClockDomain: domain}, cfg, newClock(domain))
		c.handles, c.sweepAt = 1, core.Time(core.Second)
		c.rearmLocked(core.Never)
		defer c.timer.Stop()
		c.sweepAt = core.Time(3 * core.Second)
		c.rearmLocked(core.Never)
		if c.timerArm != core.Time(core.Second) || c.timerAt != c.sweepAt {
			t.Fatal("later deadline reprogrammed the host timer")
		}
		clk.Advance(time.Second)
		synctest.Wait()
		if c.timerArm != c.sweepAt || c.s.Now() != 0 {
			t.Fatal("early wake ran protocol work or lost the later deadline")
		}
		c.sweepAt = core.Time(2 * core.Second)
		c.rearmLocked(core.Never)
		if c.timerArm != c.sweepAt {
			t.Fatal("earlier deadline was deferred")
		}
	})
}

// gateClock stops the already-awake callback on its first unlocked clock
// sample. The scheduler can install an earlier logical deadline meanwhile.
type preciseGateClock struct {
	*timeshim.Fake
	entered, release chan struct{}
	calls            atomic.Int32
}

func (g *preciseGateClock) Now() time.Time {
	if g.calls.Add(1) == 2 {
		close(g.entered)
		<-g.release
	}
	return g.Fake.Now()
}
func TestPreciseCallbackDoesNotDelayEarlierDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := timeshim.NewFake(time.Now())
		domain := NewClockDomain(f)
		cfg := core.DefaultConfig()
		cfg.HoldFirstFlight = true
		c := newConn(&Config{ClockDomain: domain}, cfg, newClock(domain))
		clk := &preciseGateClock{Fake: f, entered: make(chan struct{}), release: make(chan struct{})}
		c.clk.ext = clk
		c.handles, c.timerPrecise, c.timerArm, c.timerAt = 1, true, 0, core.Time(core.Millisecond)
		c.timer = f.AfterFunc(time.Hour, func() {})
		done := make(chan struct{})
		go func() { c.waitPreciseTimer(); close(done) }()
		<-clk.entered
		c.peek(func() { c.sweepAt = c.timerAt / 2; c.rearmLocked(core.Never) })
		if c.timerArm != c.sweepAt {
			t.Fatalf("armed %v, earlier deadline %v", c.timerArm, c.sweepAt)
		}
		close(clk.release)
		<-done // the obsolete callback exits without clock advancement
		if len(f.Deadlines()) != 1 || !f.Deadlines()[0].Equal(c.clk.wall(c.sweepAt)) {
			t.Fatal(f.Deadlines())
		}
		c.timer.Stop()
	})
}
