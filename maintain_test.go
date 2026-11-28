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
)

// heldCarrier holds a joiner's carrier after the join for a while: its
// writes after the Confirm and its reads after the Reply, so that no
// evidence of the acceptor's attachment arrives meanwhile.
type heldCarrier struct {
	release chan struct{}
	net.Conn
	closed        chan struct{}
	once          sync.Once
	writes, reads atomic.Int32
}

func newHeldCarrier(c net.Conn) *heldCarrier {
	return &heldCarrier{Conn: c, release: make(chan struct{}), closed: make(chan struct{})}
}

func (c *heldCarrier) hold() error {
	select {
	case <-c.release:
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
// receives, and AddPipe with ReturnOnReply returns before that.
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
		for _, dir := range []Direction{DirectionBidirectional, DirectionSendOnly, DirectionReceiveOnly} {
			nc, err := e.dial(ctx)
			if err != nil {
				t.Fatal(err)
			}
			carrier := newHeldCarrier(nc)
			p, err := e.client.AddPipe(ctx, NetPipe(carrier), PipeOptions{Direction: dir, ReturnOnReply: true})
			if err != nil {
				t.Fatal(err)
			}
			if st, _ := p.Stats(); st.PeerAttached {
				t.Fatalf("%v: attached before the Confirm went: %+v", dir, st)
			}
			short, scancel := context.WithCancel(ctx)
			scancel()
			err = p.WaitAttached(short)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%v: early wait returned %v", dir, err)
			}
			close(carrier.release)
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
			if err := p.Abort(); err != nil {
				t.Fatal(err)
			}
		}
		// A pipe that fails before its attachment ends the wait.
		nc, err := e.dial(ctx)
		if err != nil {
			t.Fatal(err)
		}
		p, err := e.client.AddPipe(ctx, NetPipe(newHeldCarrier(nc)), PipeOptions{ReturnOnReply: true})
		if err != nil {
			t.Fatal(err)
		}
		werr := make(chan error, 1)
		go func() { werr <- p.WaitAttached(ctx) }()
		synctest.Wait()
		_ = p.Abort()
		if err := <-werr; !errors.Is(err, ErrPipeGone) {
			t.Fatalf("wait on an aborted pipe returned %v", err)
		}
	})
}

// AddPipe waits for the peer's attachment by default and abandons an
// unconfirmed attachment when its caller cancels.
func TestAddPipeWaitsAttached(t *testing.T) {
	for _, cancelJoin := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			e := newMemMPEnv(t, mpConfig(), mpConfig())
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			nc, err := e.dial(ctx)
			if err != nil {
				t.Fatal(err)
			}
			carrier := newHeldCarrier(nc)
			type result struct {
				pipe *Pipe
				err  error
			}
			joined := make(chan result, 1)
			jctx, stop := context.WithCancel(ctx)
			defer stop()
			go func() {
				p, err := e.client.AddPipe(jctx, NetPipe(carrier), PipeOptions{})
				joined <- result{p, err}
			}()
			synctest.Wait()
			select {
			case early := <-joined:
				t.Fatalf("AddPipe returned before attachment: %+v", early)
			default:
			}
			if cancelJoin {
				stop()
				if got := <-joined; !errors.Is(got.err, context.Canceled) {
					t.Fatalf("canceled join returned %v", got.err)
				}
				if _, err := e.client.Ping(ctx); err != nil {
					t.Fatalf("session after canceled join: %v", err)
				}
				return
			}
			close(carrier.release)
			got := <-joined
			if got.err != nil {
				t.Fatal(got.err)
			}
			if h, ok := got.pipe.Health(); !ok || !h.PeerAttached {
				t.Fatalf("AddPipe returned before attachment: %+v", h)
			}
		})
	}
}
