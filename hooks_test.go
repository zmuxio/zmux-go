package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestTracerSeesEstablishedAndClosed(t *testing.T) {
	leakcheck.Check(t)
	var mu sync.Mutex
	var got []string
	closed := make(chan error, 1)
	tr := &Tracer{
		Established: func(SessionID) { mu.Lock(); got = append(got, "est"); mu.Unlock() },
		Closed:      func(_ SessionID, err error) { closed <- err },
	}
	a, b := tcpPair(t)
	c, s := pair(t, a, b, &Config{Tracer: tr}, nil)
	_ = s
	c.Abort(3, "bye")
	select {
	case err := <-closed:
		var ae *ApplicationError
		if err == nil || !errors.As(err, &ae) || ae.ErrorCode != 3 {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Closed not called")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("events %v", got)
	}
}

// pickSecond sends every chunk on the last pipe of the view.
type pickLast struct{ calls atomic.Int32 }

func (p *pickLast) Pick(_ []PickRequest, pipes []PickPipe, out []PickAnswer) {
	p.calls.Add(1)
	for i := range out {
		out[i] = PickAnswer{Index: len(pipes) - 1}
	}
}

func TestCustomPolicy(t *testing.T) {
	leakcheck.Check(t)
	pol := &pickLast{}
	ccfg := mpConfig()
	ccfg.Scheduler = SchedulerCustom(pol)
	e := newMPEnv(t, ccfg, mpConfig())
	p2 := e.addPipe(t, PipeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		s, err := e.server.AcceptStream(ctx)
		if err == nil {
			_, _ = io.Copy(io.Discard, s)
		}
	}()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(make([]byte, 256<<10)); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, ok := p2.Stats()
		if !ok {
			t.Fatal("pipe gone")
		}
		if st.BytesSent > 200<<10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pipe 2 carried %d bytes; policy called %d times", st.BytesSent, pol.calls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRedialAfterSuspension(t *testing.T) {
	leakcheck.Check(t)
	var ln net.Listener
	ccfg := mpConfig()
	ccfg.ResumeTimeout = 5 * time.Second
	var dials atomic.Int32
	ccfg.Redial = func(context.Context, Direction) (PipeIO, error) {
		dials.Add(1)
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return PipeIO{}, err
		}
		return NetPipe(nc), nil
	}
	scfg := mpConfig()
	scfg.ResumeTimeout = 5 * time.Second
	e := newMPEnv(t, ccfg, scfg)
	ln = e.ln
	// Break pipe 0 under the session: both directions are lost, the
	// session suspends and the hook rejoins.
	pd := e.client.pipes[0]
	pd.closeAll()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		ev, err := e.client.NextEvent(ctx)
		if err != nil {
			t.Fatalf("no resumption: %v (dials %d)", err, dials.Load())
		}
		if ev.Kind == EventSessionResumed {
			break
		}
	}
	go func() {
		s, err := e.server.AcceptStream(ctx)
		if err == nil {
			_, _ = io.Copy(s, s)
			_ = s.Close()
		}
	}()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = s.Write([]byte("after")); _ = s.Close() }()
	b, err := io.ReadAll(s)
	if err != nil || string(b) != "after" {
		t.Fatalf("echo after resume %q %v", b, err)
	}
}

// An invalid RedialPolicy decision is rejected as the zero decision and
// reported to Config.DecisionRejected outside every lock; redialling goes
// on and resumes the session.
func TestRedialPolicyInvalidDecision(t *testing.T) {
	leakcheck.Check(t)
	var ln net.Listener
	ccfg := mpConfig()
	ccfg.ResumeTimeout = 5 * time.Second
	ccfg.Redial = func(context.Context, Direction) (PipeIO, error) {
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			return PipeIO{}, err
		}
		return NetPipe(nc), nil
	}
	ccfg.RedialPolicy = policyFunc(func(_ *PoolView, d *PoolDecision) {
		d.DialAfter, d.Liveness.ProbeInterval = time.Hour, 500*time.Millisecond
	})
	var e *mpEnv
	var rejected atomic.Int32
	ccfg.DecisionRejected = func(err error) {
		if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("reported %v, want ErrInvalidConfig", err)
		}
		_ = e.client.Pipes() // outside every lock of the session
		rejected.Add(1)
		panic("contained")
	}
	scfg := mpConfig()
	scfg.ResumeTimeout = 5 * time.Second
	e = newMPEnv(t, ccfg, scfg)
	ln = e.ln
	e.client.pipes[0].closeAll()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		ev, err := e.client.NextEvent(ctx)
		if err != nil {
			t.Fatalf("no resumption: %v (rejected %d)", err, rejected.Load())
		}
		if ev.Kind == EventSessionResumed {
			break
		}
	}
	if rejected.Load() == 0 {
		t.Fatal("rejected decision not reported")
	}
}

// TestTracerMayWait: Tracer callbacks may call Wait and Done; neither they
// nor another Wait deadlock.
func TestTracerMayWait(t *testing.T) {
	var c *Conn
	ready := make(chan struct{})
	waited := make(chan struct{}, 2)
	tr := &Tracer{
		Event: func(SessionID, Event) { <-ready; <-c.Done() },
		Closed: func(SessionID, error) {
			<-ready
			_ = c.Wait()
			waited <- struct{}{}
			<-c.Done()
			waited <- struct{}{}
		},
	}
	a, b := tcpPair(t)
	c, _ = pair(t, a, b, &Config{Tracer: tr}, nil)
	close(ready)
	c.Abort(3, "bye")
	waitDone(t, c)
	for range 2 {
		select {
		case <-waited:
		case <-time.After(10 * time.Second):
			t.Fatal("Wait inside Tracer.Closed blocked")
		}
	}
}

// gatedPick answers every chunk with the last pipe; it reports each call's
// streams on asked and holds the first call until gate is closed.
type gatedPick struct {
	asked chan []StreamID
	gate  chan struct{}
	first atomic.Bool
}

func newGatedPick(held bool) *gatedPick {
	p := &gatedPick{asked: make(chan []StreamID, 64), gate: make(chan struct{})}
	if !held {
		close(p.gate)
	}
	return p
}

func (p *gatedPick) Pick(reqs []PickRequest, pipes []PickPipe, out []PickAnswer) {
	ids := make([]StreamID, len(reqs))
	for i := range reqs {
		ids[i] = reqs[i].Stream
		out[i] = PickAnswer{Index: len(pipes) - 1}
	}
	select {
	case p.asked <- ids:
	default:
	}
	if !p.first.Swap(true) {
		<-p.gate
	}
}

// writeUntilAsked writes a byte to s every few milliseconds until a Pick
// call names s, and returns the bytes written.
// A direction asks when it has data to place, and the fallback after
// pickWait may carry it before the policy runs, which withdraws the
// question: writing again asks again.
func (p *gatedPick) writeUntilAsked(t *testing.T, s *Stream, what string) int {
	t.Helper()
	timeout := time.After(10 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	n := 0
	for {
		select {
		case ids := <-p.asked:
			for _, x := range ids {
				if x == s.StreamID() {
					return n
				}
			}
		case <-tick.C:
			if _, err := s.Write([]byte{0}); err != nil {
				t.Fatal(err)
			}
			n++
		case <-timeout:
			t.Fatalf("Pick not asked %s", what)
		}
	}
}

// TestSlowTracerDoesNotDelayPick: a Tracer blocked in a callback delays
// none of the scheduler's decisions: the policy is asked while the Tracer
// stays blocked.
func TestSlowTracerDoesNotDelayPick(t *testing.T) {
	pol := newGatedPick(false)
	release := make(chan struct{})
	defer close(release)
	ccfg := mpConfig()
	ccfg.Scheduler = SchedulerCustom(pol)
	ccfg.Tracer = &Tracer{Established: func(SessionID) { <-release }, Event: func(SessionID, Event) { <-release }}
	e := newMPEnv(t, ccfg, mpConfig())
	_ = e.addPipe(t, PipeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := make(chan int64, 1)
	go func() {
		s, err := e.server.AcceptStream(ctx)
		var n int64
		if err == nil {
			n, _ = io.Copy(io.Discard, s)
		}
		got <- n
	}()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(make([]byte, 64<<10)); err != nil {
		t.Fatal(err)
	}
	want := int64(64<<10 + pol.writeUntilAsked(t, s, "while the Tracer was blocked"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if n := <-got; n != want {
		t.Fatalf("the peer read %d bytes of %d while the Tracer was blocked", n, want)
	}
}

// TestPickAskedAgainAfterWithdrawnQuestion: a direction whose question the
// fallback made moot (all its data went before the policy ran) asks the
// policy again for its next data instead of staying a miss, and a stream
// asked about before its opening frame is named by its own identifier.
func TestPickAskedAgainAfterWithdrawnQuestion(t *testing.T) {
	pol := newGatedPick(true)
	ccfg := mpConfig()
	ccfg.Scheduler = SchedulerCustom(pol)
	e := newMPEnv(t, ccfg, mpConfig())
	_ = e.addPipe(t, PipeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var readB atomic.Int64
	go func() {
		for range 3 {
			s, err := e.server.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 512)
				for {
					n, err := s.Read(buf)
					if s.StreamID() == 4 {
						readB.Add(int64(n))
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	open := func() *Stream {
		s, err := e.client.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	// The first call (stream a) holds the policy.
	a := open()
	pol.writeUntilAsked(t, a, "for the first stream")
	// Stream b asks meanwhile; the fallback carries all its data.
	b := open()
	if b.StreamID() != 4 {
		t.Fatalf("stream b is %d", b.StreamID())
	}
	if _, err := b.Write([]byte("b")); err != nil {
		t.Fatal(err)
	}
	for readB.Load() == 0 {
		if ctx.Err() != nil {
			t.Fatal("the fallback did not carry stream b's data")
		}
		time.Sleep(time.Millisecond)
	}
	// Release the policy; new stream c is asked about only after b's
	// question was taken with nothing of b left to place.
	close(pol.gate)
	c := open()
	pol.writeUntilAsked(t, c, "for a new stream")
	pol.writeUntilAsked(t, b, "for data after a withdrawn question")
	for _, s := range []*Stream{a, b, c} {
		_ = s.Close()
	}
}

// TestTracerNamesTheSession: one Tracer shared by both ends of a pair is
// told which session each record belongs to.
func TestTracerNamesTheSession(t *testing.T) {
	var mu sync.Mutex
	est := map[SessionID]int{}
	closed := make(chan SessionID, 2)
	tr := &Tracer{
		Established: func(id SessionID) { mu.Lock(); est[id]++; mu.Unlock() },
		Closed:      func(id SessionID, _ error) { closed <- id },
	}
	a, b := tcpPair(t)
	c, s := pair(t, a, b, &Config{Tracer: tr}, &Config{Tracer: tr})
	if c.ID() == s.ID() || c.ID() == 0 || s.ID() == 0 {
		t.Fatalf("ids %d %d", c.ID(), s.ID())
	}
	c.Abort(0, "")
	waitDone(t, c)
	waitDone(t, s)
	got := map[SessionID]bool{}
	for range 2 {
		select {
		case id := <-closed:
			got[id] = true
		case <-time.After(5 * time.Second):
			t.Fatal("no Closed")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !got[c.ID()] || !got[s.ID()] || est[c.ID()] != 1 || est[s.ID()] != 1 {
		t.Fatalf("closed %v established %v for %d and %d", got, est, c.ID(), s.ID())
	}
}
