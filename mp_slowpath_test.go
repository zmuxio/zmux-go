package zmux

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/netx"
	"github.com/djylb/timeshim"
)

// delayConn delivers what is written to it after a one-way delay.
type delayConn struct {
	net.Conn
	d    time.Duration
	rate float64 // bytes per second
	cap  int     // bytes buffered before Write blocks
	last time.Time
	held int
	room chan struct{}
	mu   sync.Mutex
	q    [][]byte
	at   []time.Time
	sig  chan struct{}
	done chan struct{}
	once sync.Once
}

func newDelayConn(c net.Conn, d time.Duration, rate float64, cap int) *delayConn {
	dc := &delayConn{Conn: c, d: d, rate: rate, cap: cap, sig: make(chan struct{}, 1), room: make(chan struct{}, 1), done: make(chan struct{})}
	go dc.run()
	return dc
}

func (c *delayConn) Write(b []byte) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	c.mu.Lock()
	for c.held >= c.cap {
		c.mu.Unlock()
		select {
		case <-c.room:
		case <-c.done:
			return 0, net.ErrClosed
		}
		c.mu.Lock()
	}
	now := time.Now()
	start := c.last
	if now.After(start) {
		start = now
	}
	c.last = start.Add(time.Duration(float64(len(b)) / c.rate * 1e9))
	c.held += len(b)
	c.q = append(c.q, append([]byte(nil), b...))
	c.at = append(c.at, c.last.Add(c.d))
	c.mu.Unlock()
	select {
	case c.sig <- struct{}{}:
	default:
	}
	return len(b), nil
}

func (c *delayConn) run() {
	for {
		c.mu.Lock()
		if len(c.q) == 0 {
			c.mu.Unlock()
			select {
			case <-c.sig:
				continue
			case <-c.done:
				return
			}
		}
		b, at := c.q[0], c.at[0]
		c.mu.Unlock()
		if w := time.Until(at); w > 0 {
			select {
			case <-time.After(w):
			case <-c.done:
				return
			}
		}
		if _, err := c.Conn.Write(b); err != nil {
			return
		}
		c.mu.Lock()
		c.q, c.at = c.q[1:], c.at[1:]
		c.held -= len(b)
		c.mu.Unlock()
		select {
		case c.room <- struct{}{}:
		default:
		}
	}
}

func (c *delayConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.Conn.Close()
}

type link struct {
	d    time.Duration
	rate float64
	cap  int
}

// linkListener shapes its i-th connection by links[i].
type linkListener struct {
	net.Listener
	links []link
	n     atomic.Int32
}

func (l *linkListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return c, err
	}
	k := l.links[min(int(l.n.Add(1))-1, len(l.links)-1)]
	return newDelayConn(c, k.d, k.rate, k.cap), nil
}

// echoed runs an echo stream paced at about 3 MB/s over pipes shaped by
// links (the first is the bootstrap pair, the others join) in a synctest
// bubble and returns the bytes echoed after at.
func echoed(t *testing.T, links []link, win uint64, at time.Duration) (int64, ConnectionStats, uint64) {
	var got int64
	var cs ConnectionStats
	var firstSent uint64 // bytes sent on the bootstrap pipe
	synctest.Test(t, func(t *testing.T) {
		ccfg, scfg := mpConfig(), mpConfig()
		// Default probation: the joined pipe takes no new data at first.
		ccfg.SchedulerParams, scfg.SchedulerParams = SchedulerParams{}, SchedulerParams{}
		ccfg.Clock, scfg.Clock = timeshim.Real, timeshim.Real
		if win != 0 {
			for _, c := range []*Config{ccfg, scfg} {
				c.InitialStreamReceiveWindow, c.MaxStreamReceiveWindow = int64(win), int64(win)
				c.InitialConnectionReceiveWindow, c.MaxConnectionReceiveWindow = 2*int64(win), 2*int64(win)
			}
		}
		ln := netx.NewChanListener(nil, 0)
		var dials atomic.Int32
		sl := &linkListener{Listener: ln, links: links}
		dial := func(ctx context.Context) (net.Conn, error) {
			c, err := ln.Dial(ctx, nil)
			if err != nil {
				return c, err
			}
			k := links[min(int(dials.Add(1))-1, len(links)-1)]
			return newDelayConn(c, k.d, k.rate, k.cap), nil
		}
		e := startMPEnv(t, ccfg, &ListenerConfig{Config: scfg, InstanceID: []byte("instance")}, sl, dial)
		for range links[1:] {
			e.addPipe(t, PipeOptions{})
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			st, err := e.server.AcceptStream(ctx)
			if err == nil {
				_, _ = io.Copy(st, st)
			}
		}()
		st, err := e.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		var n atomic.Int64
		wrote := make(chan struct{})
		go func() {
			defer close(wrote)
			buf := make([]byte, 32<<10)
			for {
				if _, err := st.Write(buf); err != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
		go func() {
			buf := make([]byte, 64<<10)
			for {
				k, err := st.Read(buf)
				n.Add(int64(k))
				if err != nil {
					return
				}
			}
		}()
		time.Sleep(at)
		got = n.Load()
		cs = e.client.ConnectionStats()
		if ps, ok := e.client.Pipes()[0].Stats(); ok {
			firstSent = ps.BytesSent
		}
		_ = st.Close()
		<-wrote // its next Write fails: the bubble ends without it
	})
	return got, cs, firstSent
}

// A slow bootstrap path (300 ms one way, 30 kB/s, deep buffer) next to a
// fast joined one: the first window goes on the slow path while the joined
// pipe is in probation, and the stream's binding then waits for that data.
// The stream is blocked by the peer's stream credit all along, so
// opportunistic reinjection re-sends the blocking data on the fast pipe and
// the echo runs at the offered rate instead of the slow path's.
func TestSlowPathDoesNotHoldSession(t *testing.T) {
	slow := link{300 * time.Millisecond, 3e4, 16 << 20}
	fast := link{20 * time.Millisecond, 5e6, 256 << 10}
	if got, _, _ := echoed(t, []link{slow, fast}, 0, 6*time.Second); got < 8<<20 {
		t.Fatalf("echoed %d bytes in 6 s next to a slow path, want at least 8 MiB of the ~19 MB offered", got)
	}
}

// With windows wide enough that the peer's credit never blocks the stream,
// its binding still waits for its data on the slow pipe once it moves to
// the fast one: that wait triggers opportunistic reinjection, the penalty
// moves the binding, and the direction's data left on the slow pipe is
// re-sent once on the fast one, so the echo is not held by it. The copies
// stay within what the slow pipe had in flight.
func TestSlowPathMovingBindingReinjects(t *testing.T) {
	slow := link{300 * time.Millisecond, 3e4, 16 << 20}
	fast := link{20 * time.Millisecond, 5e6, 256 << 10}
	got, cs, slowSent := echoed(t, []link{slow, fast}, 16<<20, 6*time.Second)
	if cs.BytesOpportunistic == 0 || cs.Penalties == 0 {
		t.Fatalf("no opportunistic reinjection for a moving binding: %d bytes, %d penalties", cs.BytesOpportunistic, cs.Penalties)
	}
	if got < 8<<20 {
		t.Fatalf("echoed %d bytes in 6 s, want at least 8 MiB", got)
	}
	// Each byte on the slow pipe is re-sent at most once for the penalty
	// and once opportunistically.
	if cs.BytesReinjected > slowSent+cs.BytesOpportunistic {
		t.Fatalf("%d bytes re-sent, more than the %d sent on the slow pipe plus %d opportunistic", cs.BytesReinjected, slowSent, cs.BytesOpportunistic)
	}
	t.Logf("echoed %d, re-sent %d (opportunistic %d), penalties %d", got, cs.BytesReinjected, cs.BytesOpportunistic, cs.Penalties)
}
