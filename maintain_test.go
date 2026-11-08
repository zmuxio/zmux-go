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

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestMaintainPipesRedials(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Count: 2, MinBackoff: 10 * time.Millisecond,
			Dial: func(context.Context) (PipeIO, error) {
				nc, err := net.Dial("tcp", e.ln.Addr().String())
				if err != nil {
					return PipeIO{}, err
				}
				return NetPipe(nc), nil
			}})
	}()
	count := func() int {
		n := 0
		for _, p := range e.client.PipeStats() {
			if p.Pipe != 0 && p.Outbound == OutboundActive {
				n++
			}
		}
		return n
	}
	wait := func(want int) {
		deadline := time.Now().Add(5 * time.Second)
		for count() != want {
			if time.Now().After(deadline) {
				t.Fatalf("%d pool pipes, want %d", count(), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	wait(2)
	for _, p := range e.client.PipeStats() {
		if p.Pipe != 0 {
			pp, _ := e.client.Pipe(p.Pipe)
			_ = pp.Abort(nil)
			break
		}
	}
	time.Sleep(50 * time.Millisecond)
	wait(2)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("MaintainPipes returned %v, want context.Canceled", err)
	}
}

func TestMaintainPipesDoesNotDialWhenTargetIsFull(t *testing.T) {
	leakcheck.Check(t)
	cfg := mpConfig()
	cfg.MaxPipes = 2
	e := newMPEnv(t, cfg, cfg)
	occupied := e.addPipe(t, PipeOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan struct{}, 8)
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Dial: func(context.Context) (PipeIO, error) {
			calls <- struct{}{}
			return PipeIO{}, errors.New("test route unavailable")
		}})
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("maintenance stopped with %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("maintenance did not stop")
		}
	}()
	select {
	case <-calls:
		t.Fatal("dialed while existing bootstrap and pipe filled target")
	case <-time.After(550 * time.Millisecond):
	}
	if err := occupied.Abort(nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-calls:
	case <-time.After(3 * time.Second):
		t.Fatal("did not attempt recovery when target slot became free")
	}
}

// A rotation dial waits for the pool's backoff, counted from the start of
// the previous dial, and its failures grow that backoff as a redial's do.
func TestMaintainPipesRotationBacksOff(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		dial := e.pipeDialer()
		var dials, maxFailures atomic.Int64
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- e.client.MaintainPipes(ctx, PipePool{MinBackoff: 400 * time.Millisecond, MaxBackoff: 10 * time.Second,
				Rotate: Rotation{MaxAge: time.Millisecond},
				Policy: policyFunc(func(v *PoolView, d *PoolDecision) {
					if f := int64(v.Failures); f > maxFailures.Load() {
						maxFailures.Store(f)
					}
					d.Recheck = MinRecheck // passes keep coming
				}),
				Dial: func(ctx context.Context) (PipeIO, error) {
					if dials.Add(1) > 1 {
						return PipeIO{}, errors.New("test dial refused")
					}
					return dial(ctx)
				}})
		}()
		defer func() {
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("MaintainPipes ended with %v, want the cancellation", err)
			}
		}()
		waitUntil(t, 5*time.Second, "first rotation dial", func() bool { return dials.Load() >= 2 })
		time.Sleep(1500 * time.Millisecond)
		// Waits of at least 0.8 x 800 ms and 0.8 x 1.6 s follow the first failure.
		if n := dials.Load() - 2; n > 2 {
			t.Fatalf("%d rotation dials in 1.5 s after a failure", n)
		}
		if maxFailures.Load() < 1 {
			t.Fatal("rotation failures not counted")
		}
	})
}

// A Join member reports its pipes in the pool view.
func TestMaintainPipesJoinMemberPipes(t *testing.T) {
	t.Parallel()
	e := newMPEnv(t, mpConfig(), mpConfig())
	dial := tcpDialer(e.ln.Addr().String())
	var pipes atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{
			Policy: policyFunc(func(v *PoolView, d *PoolDecision) {
				for _, m := range v.Members {
					pipes.Store(int64(m.Pipes))
				}
				d.Recheck = MinRecheck
			}),
			Join: func(ctx context.Context, c *Conn, o PipeOptions) ([]*Pipe, error) {
				var ps []*Pipe
				for range 2 {
					io, err := dial(ctx)
					if err != nil {
						return ps, err
					}
					p, err := c.AddPipe(ctx, io, o)
					if err != nil {
						closePipeIO(io)
						return ps, err
					}
					ps = append(ps, p)
				}
				return ps, nil
			}})
	}()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("MaintainPipes ended with %v, want the cancellation", err)
		}
	}()
	waitUntil(t, 5*time.Second, "member of two pipes", func() bool { return pipes.Load() == 2 })
}

// A replacement still being rotated in when ctx ends is closed; the
// member it was to replace stays.
func TestMaintainPipesStopClosesReplacement(t *testing.T) {
	t.Parallel()
	e := newMPEnv(t, mpConfig(), mpConfig())
	dial := tcpDialer(e.ln.Addr().String())
	var mu sync.Mutex
	var joined []*Pipe
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Rotate: Rotation{MaxAge: time.Millisecond},
			Join: func(ctx context.Context, c *Conn, o PipeOptions) ([]*Pipe, error) {
				mu.Lock()
				n := len(joined)
				mu.Unlock()
				if n > 0 {
					o.Probation = time.Hour // never usable while the test runs
				}
				pio, err := dial(ctx)
				if err != nil {
					return nil, err
				}
				p, err := c.AddPipe(ctx, pio, o)
				if err != nil {
					closePipeIO(pio)
					return nil, err
				}
				mu.Lock()
				joined = append(joined, p)
				mu.Unlock()
				return []*Pipe{p}, nil
			}})
	}()
	waitUntil(t, 5*time.Second, "a replacement", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(joined) >= 2
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("MaintainPipes ended with %v", err)
	}
	mu.Lock()
	first, repl := joined[0], joined[1]
	mu.Unlock()
	waitUntil(t, 5*time.Second, "the replacement closed", func() bool { _, ok := repl.Stats(); return !ok })
	if _, ok := first.Stats(); !ok {
		t.Fatal("the member's pipe went with the pool")
	}
}

// heldCarrier holds a joiner's carrier after the join for a while: its
// writes after the Confirm and its reads after the Reply, so that no
// evidence of the acceptor's attachment arrives meanwhile.
type heldCarrier struct {
	net.Conn
	writes, reads atomic.Int32
	until         time.Time
	once          sync.Once
	closed        chan struct{}
}

func newHeldCarrier(c net.Conn, d time.Duration) *heldCarrier {
	return &heldCarrier{Conn: c, until: time.Now().Add(d), closed: make(chan struct{})}
}

func (c *heldCarrier) hold() error {
	t := time.NewTimer(time.Until(c.until))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-c.closed:
		return net.ErrClosed
	}
}

func (c *heldCarrier) Write(b []byte) (int, error) {
	if c.writes.Add(1) > 2 {
		if err := c.hold(); err != nil {
			return 0, err
		}
	}
	return c.Conn.Write(b)
}

func (c *heldCarrier) Read(b []byte) (int, error) {
	if c.reads.Add(1) > 1 {
		if err := c.hold(); err != nil {
			return 0, err
		}
	}
	return c.Conn.Read(b)
}

// CloseWrite keeps the half close of a receive-only pipe's carrier.
func (c *heldCarrier) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

func (c *heldCarrier) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// The peer's attachment: the bootstrap pipe and a pipe
// the peer joined are attached from the start; a pipe this endpoint joined
// is attached by an acknowledged byte, or a record for one that only
// receives, and AddPipe with NoWaitAttached returns before that.
func TestWaitAttached(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, c := range []*Conn{e.client, e.server} {
			p0, _ := c.Pipe(0)
			if st, _ := p0.Stats(); !st.PeerAttached {
				t.Fatalf("pipe 0 not attached: %+v", st)
			}
			if err := p0.WaitAttached(ctx); err != nil {
				t.Fatal(err)
			}
		}
		for _, dir := range []Direction{DirBidirectional, DirSendOnly, DirReceiveOnly} {
			nc, err := e.dial(ctx)
			if err != nil {
				t.Fatal(err)
			}
			p, err := e.client.AddPipe(ctx, NetPipe(newHeldCarrier(nc, 300*time.Millisecond)), PipeOptions{Direction: dir, NoWaitAttached: true})
			if err != nil {
				t.Fatal(err)
			}
			if st, _ := p.Stats(); st.PeerAttached {
				t.Fatalf("%v: attached before the Confirm went: %+v", dir, st)
			}
			short, scancel := context.WithTimeout(ctx, 50*time.Millisecond)
			err = p.WaitAttached(short)
			scancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%v: early wait returned %v", dir, err)
			}
			if err := p.WaitAttached(ctx); err != nil {
				t.Fatalf("%v: %v", dir, err)
			}
			if st, _ := p.Stats(); !st.PeerAttached {
				t.Fatalf("%v: not attached after the wait: %+v", dir, st)
			}
			sp, ok := e.server.Pipe(p.ID())
			if !ok {
				t.Fatalf("%v: the server has no pipe %d", dir, p.ID())
			}
			if st, _ := sp.Stats(); !st.PeerAttached {
				t.Fatalf("%v: an accepted pipe not attached: %+v", dir, st)
			}
			if err := p.Abort(nil); err != nil {
				t.Fatal(err)
			}
		}
		// A pipe that fails before its attachment ends the wait.
		nc, err := e.dial(ctx)
		if err != nil {
			t.Fatal(err)
		}
		p, err := e.client.AddPipe(ctx, NetPipe(newHeldCarrier(nc, 5*time.Second)), PipeOptions{NoWaitAttached: true})
		if err != nil {
			t.Fatal(err)
		}
		werr := make(chan error, 1)
		go func() { werr <- p.WaitAttached(ctx) }()
		time.Sleep(20 * time.Millisecond)
		_ = p.Abort(nil)
		if err := <-werr; !errors.Is(err, ErrPipeGone) {
			t.Fatalf("wait on an aborted pipe returned %v", err)
		}
	})
}

// The pool judges a member usable by the peer's attachment alone: the
// bootstrap pipe and an accepted pipe need no acknowledgement (a DialMode
// pool's first member is pipe 0), a joined one does.
func TestMemberStateAttachment(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	for _, c := range []*Conn{e.client, e.server} {
		p0, _ := c.Pipe(0)
		if _, alive, usable := memberState([]*Pipe{p0}); !alive || !usable {
			t.Fatalf("pipe 0: alive %v usable %v", alive, usable)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nc, err := net.Dial("tcp", e.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p, err := e.client.AddPipe(ctx, NetPipe(newHeldCarrier(nc, 300*time.Millisecond)), PipeOptions{NoWaitAttached: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, alive, usable := memberState([]*Pipe{p}); !alive || usable {
		t.Fatalf("joined pipe before its attachment: alive %v usable %v", alive, usable)
	}
	if err := p.WaitAttached(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, usable := memberState([]*Pipe{p}); !usable {
		t.Fatal("joined pipe not usable once attached")
	}
	sp, _ := e.server.Pipe(p.ID())
	if _, alive, usable := memberState([]*Pipe{sp}); !alive || !usable {
		t.Fatalf("accepted pipe: alive %v usable %v", alive, usable)
	}
}

// AddPipe waits by default for the peer's attachment, which a held
// Confirm and acknowledgement delay; at the deadline it aborts the pipe and
// returns the deadline's error, and the session keeps its other pipes.
func TestAddPipeWaitsAttached(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		nc, err := e.dial(ctx)
		if err != nil {
			t.Fatal(err)
		}
		const held = 300 * time.Millisecond
		t0 := time.Now()
		p, err := e.client.AddPipe(ctx, NetPipe(newHeldCarrier(nc, held)), PipeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(t0); d < held-50*time.Millisecond {
			t.Fatalf("AddPipe returned after %v, before the held attachment", d)
		}
		if st, _ := p.Stats(); !st.PeerAttached {
			t.Fatalf("AddPipe returned before the attachment: %+v", st)
		}
		if err := p.Abort(nil); err != nil {
			t.Fatal(err)
		}
		waitUntil(t, 5*time.Second, "the aborted pipe to go", func() bool { return len(e.client.PipeStats()) == 1 })

		if nc, err = e.dial(ctx); err != nil {
			t.Fatal(err)
		}
		short, scancel := context.WithTimeout(ctx, 200*time.Millisecond)
		t0 = time.Now()
		_, err = e.client.AddPipe(short, NetPipe(newHeldCarrier(nc, 5*time.Second)), PipeOptions{})
		scancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a wait past the deadline returned %v", err)
		}
		if d := time.Since(t0); d > 2*time.Second {
			t.Fatalf("AddPipe returned %v after its deadline", d)
		}
		waitUntil(t, 5*time.Second, "the unattached pipe to go", func() bool { return len(e.client.PipeStats()) == 1 })
		if _, err := e.client.Ping(ctx); err != nil {
			t.Fatalf("the session after a failed wait: %v", err)
		}
	})
}
