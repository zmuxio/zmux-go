package carrierstats

import (
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

type testConn struct {
	reads   int
	readout Readout
}

func readTestConn(c *testConn) Readout {
	c.reads++
	return c.readout
}

func TestSharedObservation(t *testing.T) {
	now := time.Unix(10, 0)
	clock := func() time.Time { return now }
	a := &testConn{readout: Readout{Sent: 42, Lost: 6, Known: true}}
	s := For(a, SourceQUICGo, readTestConn)
	s.prime(clock)
	first := s.sample(clock, runQuery)
	now = now.Add(time.Second - 1)
	a.readout.Lost = 2
	if got := For(a, SourceQUICGo, readTestConn).sample(clock, runQuery); got != first || a.reads != 1 {
		t.Fatalf("cached observation = %+v, reads = %d", got, a.reads)
	}
	now = now.Add(1)
	s.sample(clock, runQuery) // the refresh itself still returns first
	next := s.sample(clock, runQuery)
	if next.ID != first.ID || next.Generation != 0 || next.Lost != 2 || !next.At.After(first.At) || a.reads != 2 {
		t.Fatalf("QUIC correction = %+v, reads = %d", next, a.reads)
	}
	now = now.Add(time.Second)
	a.readout.Sent = 3
	s.sample(clock, runQuery)
	if got := s.sample(clock, runQuery); got.ID != first.ID || got.Generation != 1 {
		t.Fatalf("reset = %+v", got)
	}
	runtime.KeepAlive(a)
	b := &testConn{}
	replacement := For(b, SourceQUICGo, readTestConn)
	replacement.prime(clock)
	if got := replacement.sample(clock, runQuery); got.ID == first.ID || got.Generation != 0 || got.Known {
		t.Fatalf("replacement = %+v", got)
	}
	runtime.KeepAlive(b)
}

func TestLinuxWrapAndReset(t *testing.T) {
	now := time.Unix(10, 0)
	clock := func() time.Time { return now }
	c := &testConn{readout: Readout{Sent: 1<<32 - 5, Lost: 1<<32 - 10, Known: true}}
	s := For(c, SourceLinuxTCPInfo, readTestConn)
	s.prime(clock)
	first := s.sample(clock, runQuery)
	now = now.Add(time.Second)
	c.readout.Sent, c.readout.Lost = 10, 3
	s.sample(clock, runQuery)
	if got := s.sample(clock, runQuery); got.Sent != 1<<32+10 || got.Lost != 1<<32+3 || got.Generation != 0 {
		t.Fatalf("wrap = %+v", got)
	}
	now = now.Add(time.Second)
	c.readout.Sent, c.readout.Lost = 2, 0
	s.sample(clock, runQuery)
	if got := s.sample(clock, runQuery); got.ID != first.ID || got.Sent != 2 || got.Lost != 0 || got.Generation != 1 {
		t.Fatalf("reset = %+v", got)
	}
	runtime.KeepAlive(c)
}

func TestKnownProviderAdoptsConnection(t *testing.T) {
	now := time.Unix(10, 0)
	clock := func() time.Time { return now }
	conn := &testConn{readout: Readout{Known: true}}
	unknown := Unknown(conn)
	unknown.prime(clock)
	first := unknown.sample(clock, runQuery)
	s := For(conn, SourceQUICGo, readTestConn)
	var query func()
	if got := s.sample(clock, func(run func()) { query = run }); !got.At.IsZero() || conn.reads != 0 || query == nil {
		t.Fatalf("observation during upgrade = %+v, reads = %d", got, conn.reads)
	}
	if got := Cached(s); !got.At.IsZero() {
		t.Fatalf("cached observation during upgrade = %+v", got)
	}
	query()
	got := Unknown(conn).sample(clock, runQuery)
	if got.ID != first.ID || got.Source != SourceQUICGo || !got.Known || conn.reads != 1 {
		t.Fatalf("observed provider = %+v, reads = %d", got, conn.reads)
	}
}

func runQuery(query func()) { query() }

func TestConcurrentReadsReturnStaleSample(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		reads := 0
		s := &Sampler{source: SourceQUICGo, read: func() Readout {
			reads++
			if reads == 2 {
				close(started)
				<-release
			}
			return Readout{Sent: uint64(reads), Known: true}
		}}
		Prime(s)
		first := Sample(s)
		time.Sleep(time.Second)
		if got := Sample(s); got != first {
			t.Errorf("refreshing read returned %+v, want %+v", got, first)
		}
		<-started
		const n = 128
		results := make(chan Observation, n)
		for range n {
			go func() { results <- Sample(s) }()
		}
		synctest.Wait()
		if len(results) != n || reads != 2 {
			t.Errorf("completed %d reads, queried %d times", len(results), reads)
		}
		for len(results) > 0 {
			if got := <-results; got != first {
				t.Errorf("concurrent read returned %+v, want %+v", got, first)
			}
		}
		close(release)
		synctest.Wait()
		if got := Sample(s); got.Sent != 2 || !got.At.After(first.At) {
			t.Errorf("completed refresh = %+v", got)
		}
	})
}

func TestIdleSamplers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 1000
		samplers := make([]Sampler, n)
		reads := 0
		for i := range samplers {
			samplers[i].read = func() Readout {
				reads++
				return Readout{}
			}
			Prime(&samplers[i])
		}
		if allocs := testing.AllocsPerRun(1, func() { time.Sleep(10 * time.Second) }); allocs != 0 {
			t.Errorf("allocations during idle = %v", allocs)
		}
		synctest.Wait()
		if reads != n {
			t.Fatalf("native queries during idle = %d", reads-n)
		}
	})
}

func TestFirstKnownProviderStays(t *testing.T) {
	conn := &testConn{readout: Readout{Known: true, Sent: 3}}
	s := For(conn, SourceQUICGo, readTestConn)
	other := For(conn, SourceLinuxTCPInfo, func(*testConn) Readout {
		t.Fatal("second provider queried")
		return Readout{}
	})
	Prime(other)
	got := Cached(s)
	if other != s || got.Source != SourceQUICGo || got.Sent != 3 || conn.reads != 1 {
		t.Fatalf("first provider observation = %+v, reads = %d", got, conn.reads)
	}
}

func TestUnavailableProviderPublishesOnce(t *testing.T) {
	for _, source := range []Source{SourceWindowsSIOTCPInfo, SourceUnknown} {
		t.Run(string(source), func(t *testing.T) {
			now := time.Unix(10, 0)
			clock := func() time.Time { return now }
			reads := 0
			s := &Sampler{source: source, read: func() Readout {
				reads++
				return Readout{}
			}}
			s.prime(clock)
			first := Cached(s)
			now = now.Add(time.Hour)
			if got := s.sample(clock, runQuery); got != first || got.Source != source || got.Known || reads != 1 {
				t.Fatalf("unavailable observation = %+v, reads = %d", got, reads)
			}
		})
	}
}

func TestPrimePanicReleasesWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		panicked := make(chan any, 1)
		calls := 0
		s := &Sampler{source: SourceQUICGo, read: func() Readout {
			calls++
			if calls == 1 {
				close(started)
				<-release
				panic("native read failed")
			}
			return Readout{Known: true}
		}}
		go func() {
			defer func() { panicked <- recover() }()
			Prime(s)
		}()
		<-started
		if got := Sample(s); !got.At.IsZero() {
			t.Errorf("sample during prime = %+v", got)
		}
		done := make(chan struct{})
		go func() {
			Prime(s)
			close(done)
		}()
		synctest.Wait()
		close(release)
		synctest.Wait()
		if got := <-panicked; got != "native read failed" {
			t.Errorf("initial panic = %v", got)
		}
		select {
		case <-done:
			if got := Cached(s); !got.Known || calls != 2 || s.firstDone != nil || s.inFlight {
				t.Errorf("recovered sample = %+v, calls = %d", got, calls)
			}
		default:
			t.Fatal("waiting attachment was not released")
		}
	})
}
