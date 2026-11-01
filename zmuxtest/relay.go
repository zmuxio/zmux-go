package zmuxtest

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// RelayOptions are the behaviour of a Relay toward the connections it
// relays, as intermediaries on a path behave.
type RelayOptions struct {
	// IdleCut ends a connection that carried no byte in either direction
	// for this long, as a NAT mapping or a load balancer's idle limit
	// (zero: never).
	IdleCut time.Duration
	// MaxAge ends a connection this long after it was accepted, idle or
	// not, as a load balancer's limit on a connection's duration (zero:
	// never).
	MaxAge time.Duration
	// OnData, when set, is called for every chunk relayed: the
	// connection's number (from 1, in accept order), its direction (up:
	// from the client) and its size.
	OnData func(conn int, up bool, n int)
}

// Relay is a TCP relay from a local listener to a target address for
// tests of carrier loss: it ends connections idle or old beyond a limit,
// drops the bytes of chosen directions without ending the connection (a
// black hole), refuses new connections, or cuts the current ones.
type Relay struct {
	o      RelayOptions
	target string
	ln     net.Listener
	wg     sync.WaitGroup

	mu      sync.Mutex
	conns   map[*relayConn]struct{}
	refuse  bool
	closed  bool
	n       int
	cuts    atomic.Int64
	refused atomic.Int64
	quit    chan struct{}
}

type relayConn struct {
	id     int
	c, u   net.Conn
	born   time.Time
	last   atomic.Int64 // UnixNano of the last relayed byte
	dropUp atomic.Bool
	dropDn atomic.Bool
	once   sync.Once
	done   chan struct{}
}

// NewRelay listens on 127.0.0.1 and relays each accepted connection to
// target.
func NewRelay(target string, o RelayOptions) (*Relay, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &Relay{o: o, target: target, ln: ln, conns: map[*relayConn]struct{}{}, quit: make(chan struct{})}
	r.wg.Add(1)
	go r.accept()
	return r, nil
}

// Addr is the relay's listening address.
func (r *Relay) Addr() string { return r.ln.Addr().String() }

// Refuse makes the relay close new connections at once (on) or relay them
// again (off).
func (r *Relay) Refuse(on bool) {
	r.mu.Lock()
	r.refuse = on
	r.mu.Unlock()
}

// Blackhole drops, on the current connections, the bytes from the client
// (up) and toward it (down), and the end of a direction once it is
// dropped, without ending the connections; later connections are relayed
// normally.
func (r *Relay) Blackhole(up, down bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for rc := range r.conns {
		rc.dropUp.Store(up)
		rc.dropDn.Store(down)
	}
}

// CutAll ends the current connections.
func (r *Relay) CutAll() {
	r.mu.Lock()
	cs := make([]*relayConn, 0, len(r.conns))
	for rc := range r.conns {
		cs = append(cs, rc)
	}
	r.mu.Unlock()
	for _, rc := range cs {
		r.end(rc, true)
	}
}

// Cuts is the number of connections the relay ended itself: by IdleCut,
// MaxAge or CutAll.
func (r *Relay) Cuts() int { return int(r.cuts.Load()) }

// Refused is the number of connections closed at once by Refuse.
func (r *Relay) Refused() int { return int(r.refused.Load()) }

// Accepted is the number of connections accepted, refused ones included.
func (r *Relay) Accepted() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// Live is the number of connections being relayed.
func (r *Relay) Live() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.conns)
}

// Close stops the relay, ends every connection and waits for its
// goroutines.
func (r *Relay) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.quit)
	r.mu.Unlock()
	err := r.ln.Close()
	r.mu.Lock()
	cs := make([]*relayConn, 0, len(r.conns))
	for rc := range r.conns {
		cs = append(cs, rc)
	}
	r.mu.Unlock()
	for _, rc := range cs {
		r.end(rc, false)
	}
	r.wg.Wait()
	return err
}

func (r *Relay) accept() {
	defer r.wg.Done()
	for {
		c, err := r.ln.Accept()
		if err != nil {
			return
		}
		r.mu.Lock()
		r.n++
		id, refuse, closed := r.n, r.refuse, r.closed
		r.mu.Unlock()
		if refuse || closed {
			r.refused.Add(1)
			_ = c.Close()
			continue
		}
		u, err := net.DialTimeout("tcp", r.target, 5*time.Second)
		if err != nil {
			_ = c.Close()
			continue
		}
		rc := &relayConn{id: id, c: c, u: u, born: time.Now(), done: make(chan struct{})}
		rc.last.Store(time.Now().UnixNano())
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			_ = c.Close()
			_ = u.Close()
			continue
		}
		r.conns[rc] = struct{}{}
		r.mu.Unlock()
		r.wg.Add(3)
		go r.copy(rc, rc.u, rc.c, true)
		go r.copy(rc, rc.c, rc.u, false)
		go r.watch(rc)
	}
}

// copy relays src to dst, dropping while the direction is black-holed.
func (r *Relay) copy(rc *relayConn, dst, src net.Conn, up bool) {
	defer r.wg.Done()
	defer r.end(rc, false)
	drop := &rc.dropDn
	if up {
		drop = &rc.dropUp
	}
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !drop.Load() {
			rc.last.Store(time.Now().UnixNano())
			if f := r.o.OnData; f != nil {
				f(rc.id, up, n)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if drop.Load() {
				// A black hole delivers no end either: the far side's FIN
				// or reset is lost like its bytes, and the connection
				// stays open until the relay ends it.
				<-rc.done
				return
			}
			if err == io.EOF {
				if cw, ok := dst.(interface{ CloseWrite() error }); ok {
					_ = cw.CloseWrite()
					// The other direction ends the connection.
					<-rc.done
				}
			}
			return
		}
	}
}

// watch ends rc at its idle or age limit.
func (r *Relay) watch(rc *relayConn) {
	defer r.wg.Done()
	if r.o.IdleCut <= 0 && r.o.MaxAge <= 0 {
		<-rc.done
		return
	}
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-rc.done:
			return
		case <-r.quit:
			return
		case now := <-t.C:
			idle := now.Sub(time.Unix(0, rc.last.Load()))
			if r.o.IdleCut > 0 && idle >= r.o.IdleCut || r.o.MaxAge > 0 && now.Sub(rc.born) >= r.o.MaxAge {
				r.end(rc, true)
				return
			}
		}
	}
}

// end closes both sides of rc once; cut counts it as ended by the relay.
func (r *Relay) end(rc *relayConn, cut bool) {
	rc.once.Do(func() {
		if cut {
			r.cuts.Add(1)
		}
		r.mu.Lock()
		delete(r.conns, rc)
		r.mu.Unlock()
		close(rc.done)
		_ = rc.c.Close()
		_ = rc.u.Close()
	})
}
