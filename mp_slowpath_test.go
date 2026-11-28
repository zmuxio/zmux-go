package zmux

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// delayConn delivers what is written to it after a one-way delay.
type delayConn struct {
	last time.Time
	net.Conn
	room chan struct{}
	sig  chan struct{}
	done chan struct{}
	q    [][]byte
	at   []time.Time
	d    time.Duration
	rate float64 // bytes per second
	cap  int     // bytes buffered before Write blocks
	held int
	once sync.Once
	mu   sync.Mutex
}

func newDelayConn(c net.Conn, d time.Duration, rate float64, capacity int) *delayConn {
	dc := &delayConn{Conn: c, d: d, rate: rate, cap: capacity, sig: make(chan struct{}, 1), room: make(chan struct{}, 1), done: make(chan struct{})}
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
