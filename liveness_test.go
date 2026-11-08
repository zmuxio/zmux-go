package zmux

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
)

// countConn counts the writes on a carrier.
type countConn struct {
	net.Conn
	n atomic.Int64
}

func (c *countConn) Write(b []byte) (int, error) {
	c.n.Add(1)
	return c.Conn.Write(b)
}

// Zero fields are the defaults, Liveness round-trips, and an invalid value
// is ErrInvalidConfig with nothing changed.
func TestSetLivenessValidation(t *testing.T) {
	a, b := tcpPair(t)
	c, _ := pair(t, a, b, &Config{KeepAlivePeriod: 5 * time.Second}, nil)
	l := c.Liveness()
	if l.KeepAlivePeriod != 5*time.Second || l.SuspectFloor != 250*time.Millisecond || l.DeadTimeout != 0 ||
		l.ProbeInterval != 15*time.Second || l.StandbyProbe != time.Second || l.ProbeJitter != DefaultProbeJitter {
		t.Fatalf("initial %+v", l)
	}
	for _, bad := range []Liveness{
		{KeepAlivePeriod: time.Millisecond}, {SuspectFloor: -1}, {SuspectFloor: time.Millisecond},
		{DeadTimeout: -1}, {DeadTimeout: 500 * time.Millisecond}, {ProbeInterval: time.Millisecond},
		{StandbyProbe: time.Millisecond}, {ProbeJitter: 1}, {ProbeJitter: -0.1},
	} {
		if err := c.SetLiveness(bad); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: %v", bad, err)
		}
		if got := c.Liveness(); got != l {
			t.Fatalf("%+v changed the settings to %+v", bad, got)
		}
	}
	if err := c.SetLiveness(l); err != nil || c.Liveness() != l {
		t.Fatalf("round trip: %v, %+v", err, c.Liveness())
	}
	if err := c.SetLiveness(Liveness{}); err != nil {
		t.Fatal(err)
	}
	want := Liveness{KeepAlivePeriod: DefaultKeepAlivePeriod, SuspectFloor: 250 * time.Millisecond,
		StandbyProbe: time.Second, ProbeInterval: 15 * time.Second, ProbeJitter: DefaultProbeJitter}
	if got := c.Liveness(); got != want {
		t.Fatalf("zero value: %+v", got)
	}
	off := Liveness{KeepAlivePeriod: -1, StandbyProbe: -1, ProbeInterval: -1, NoProbeJitter: true, DeadTimeout: 3 * time.Second}
	if err := c.SetLiveness(off); err != nil {
		t.Fatal(err)
	}
	if got := c.Liveness(); got.KeepAlivePeriod != -1 || got.StandbyProbe != -1 || got.ProbeInterval != -1 ||
		!got.NoProbeJitter || got.ProbeJitter != 0 || got.DeadTimeout != 3*time.Second {
		t.Fatalf("off: %+v", got)
	}
}

// A keep-alive period turned on while the session runs takes effect at
// once on an idle session.
func TestSetLivenessKeepAliveRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		cc := &countConn{Conn: a}
		c, _ := pair(t, cc, b, &Config{KeepAlivePeriod: -1, Clock: timeshim.Real}, &Config{KeepAlivePeriod: -1, Clock: timeshim.Real})
		synctest.Sleep(200 * time.Millisecond)
		cc.n.Store(0)
		synctest.Sleep(1200 * time.Millisecond)
		if n := cc.n.Load(); n != 0 {
			t.Fatalf("%d writes with keep-alive off", n)
		}
		if err := c.SetLiveness(Liveness{KeepAlivePeriod: time.Second, NoProbeJitter: true}); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(2500 * time.Millisecond)
		if n := cc.n.Load(); n < 2 {
			t.Fatalf("%d writes in 2.5 s with a 1 s keep-alive", n)
		}
	})
}

// Pipe.SetLiveness validates as PipeOptions does and fails on a retired
// pipe; the settings race with traffic and session changes cleanly.
func TestPipeSetLiveness(t *testing.T) {
	e := newMPEnv(t, mpConfig(), mpConfig())
	p := e.addPipe(t, PipeOptions{ProbeInterval: 5 * time.Second})
	for _, bad := range []PipeLiveness{{SuspectFloor: time.Millisecond}, {DeadTimeout: time.Millisecond},
		{ProbeInterval: time.Millisecond}, {ProbeJitter: 1}} {
		if err := p.SetLiveness(bad); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: %v", bad, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		st, err := e.server.AcceptStream(ctx)
		if err == nil {
			buf := make([]byte, 32<<10)
			for {
				if _, err := st.Read(buf); err != nil {
					return
				}
			}
		}
	}()
	st, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(3)
	go func() {
		defer wg.Done()
		buf := make([]byte, 16<<10)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := st.Write(buf); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 200 {
			l := Liveness{ProbeInterval: time.Duration(1+i%5) * time.Second, DeadTimeout: time.Duration(1+i%3) * time.Second}
			if err := e.client.SetLiveness(l); err != nil {
				t.Error(err)
				return
			}
			_ = e.client.Liveness()
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 200 {
			var err error
			if i%2 == 0 {
				err = p.SetLiveness(PipeLiveness{ProbeInterval: 2 * time.Second, NoProbeJitter: true})
			} else {
				err = p.ClearLiveness()
			}
			if err != nil {
				t.Error(err)
				return
			}
			_ = e.server.RaiseIncomingStreams(int64(300+i), 0)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	_ = st.Close()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for p.SetLiveness(PipeLiveness{}) == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := p.SetLiveness(PipeLiveness{}); !errors.Is(err, ErrPipeGone) {
		t.Fatalf("retired pipe: %v", err)
	}
}

// A server that grants 8 bidirectional streams at first (before the
// application has authenticated its peer, say) raises the limit later:
// the ninth stream waits until then.
func TestRaiseIncomingStreams(t *testing.T) {
	a, b := tcpPair(t)
	c, s := pair(t, a, b, nil, &Config{MaxIncomingStreams: 8})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 8 {
		st, err := c.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	short, scancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err := c.OpenStreamSync(short)
	scancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ninth stream: %v", err)
	}
	for _, bad := range [][2]int64{{-1, 0}, {0, -1}, {1<<60 + 1, 0}} {
		if err := s.RaiseIncomingStreams(bad[0], bad[1]); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%v: %v", bad, err)
		}
	}
	if err := s.RaiseIncomingStreams(4, 0); err != nil { // below: no-op
		t.Fatal(err)
	}
	if err := s.RaiseIncomingStreams(16, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.OpenStreamSync(ctx); err != nil {
		t.Fatalf("after the raise: %v", err)
	}
}

// A MinBackoff above MaxBackoff is invalid, the defaults filled in.
func TestMaintainPipesBackoffOrder(t *testing.T) {
	e := newMPEnv(t, mpConfig(), mpConfig())
	dial := func(context.Context) (PipeIO, error) { return PipeIO{}, errors.New("unused") }
	for _, pool := range []PipePool{
		{MinBackoff: 2 * time.Second, MaxBackoff: time.Second, Dial: dial},
		{MinBackoff: 2 * DefaultMaxBackoff, Dial: dial},
	} {
		if err := e.client.MaintainPipes(context.Background(), pool); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%v > %v: %v", pool.MinBackoff, pool.MaxBackoff, err)
		}
	}
}
