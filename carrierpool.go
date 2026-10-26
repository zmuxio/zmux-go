package zmux

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// CarrierPool shares underlying connections between pipes, for adapters
// that carry several pipes over one connection (HTTP/2 and HTTP/3 request
// streams, QUIC streams; API.md Appendix A). A new pipe goes to the open
// connection with the most room within the limits, else to a new one; a
// connection that reached its lifetime limits takes no new pipe and is
// closed once its last pipe ends. The pipes of one connection get one
// fate group: they fail together and are never counted as independent
// paths. The zero value with Dial set is ready; it is safe for concurrent
// use.
type CarrierPool[C comparable] struct {
	// Dial opens an underlying connection.
	Dial func(ctx context.Context) (C, error)
	// Close closes a connection the pool retired once no pipe uses it.
	Close func(C)
	// MaxPipesPerConnection bounds the pipes on a connection at once
	// (default 8).
	MaxPipesPerConnection int
	// MaxPipesPerConnectionLifetime bounds the pipes a connection carries
	// over its life (0: unlimited).
	MaxPipesPerConnectionLifetime int
	// MaxConnectionAge stops giving pipes to a connection this old (0:
	// unlimited).
	MaxConnectionAge time.Duration

	mu     sync.Mutex
	conns  []*pooledConn[C]
	closed bool
}

type pooledConn[C comparable] struct {
	c       C
	fate    uint64
	born    time.Time
	active  int
	total   int
	retired bool
}

// Lease is one pipe's use of a pooled connection; the adapter releases it
// when the pipe ends.
type Lease[C comparable] struct {
	// Conn is the connection to open the pipe on.
	Conn C
	// FateGroup is the connection's fate group (set it as the PipeIO's).
	FateGroup uint64

	p    *CarrierPool[C]
	pc   *pooledConn[C]
	once sync.Once
}

// Release ends the lease when its pipe ended; the last lease of a retired
// connection closes it. Release is idempotent.
func (l *Lease[C]) Release() { l.once.Do(func() { l.p.release(l.pc) }) }

// Retire stops giving new pipes to the lease's connection (for example
// after it failed); it closes once its pipes are released.
func (l *Lease[C]) Retire() { l.p.retire(l.pc) }

var errPoolClosed = errors.New("zmux: carrier pool closed")

// Get returns a lease on a connection with room, dialling one when none
// has; the dial runs outside the pool's lock.
func (p *CarrierPool[C]) Get(ctx context.Context) (*Lease[C], error) {
	if l := p.take(); l != nil {
		return l, nil
	}
	c, err := p.Dial(ctx)
	if err != nil {
		return nil, err
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	pc := &pooledConn[C]{c: c, fate: binary.BigEndian.Uint64(b[:]) | 1, born: time.Now(), active: 1, total: 1}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		if p.Close != nil {
			p.Close(c)
		}
		return nil, errPoolClosed
	}
	p.conns = append(p.conns, pc)
	p.retireLocked(pc)
	p.mu.Unlock()
	return &Lease[C]{Conn: c, FateGroup: pc.fate, p: p, pc: pc}, nil
}

func (p *CarrierPool[C]) maxActive() int {
	if p.MaxPipesPerConnection > 0 {
		return p.MaxPipesPerConnection
	}
	return 8
}

// take leases the open connection with the most room, if any.
func (p *CarrierPool[C]) take() *Lease[C] {
	p.mu.Lock()
	defer p.mu.Unlock()
	var best *pooledConn[C]
	for _, pc := range p.conns {
		p.retireLocked(pc)
		if pc.retired || pc.active >= p.maxActive() {
			continue
		}
		if best == nil || p.room(pc) > p.room(best) {
			best = pc
		}
	}
	if best == nil {
		return nil
	}
	best.active++
	best.total++
	p.retireLocked(best)
	return &Lease[C]{Conn: best.c, FateGroup: best.fate, p: p, pc: best}
}

func (p *CarrierPool[C]) room(pc *pooledConn[C]) int {
	r := p.maxActive() - pc.active
	if m := p.MaxPipesPerConnectionLifetime; m > 0 {
		r = min(r, m-pc.total)
	}
	return r
}

// retireLocked retires a connection that reached a lifetime limit.
func (p *CarrierPool[C]) retireLocked(pc *pooledConn[C]) {
	if m := p.MaxPipesPerConnectionLifetime; m > 0 && pc.total >= m {
		pc.retired = true
	}
	if a := p.MaxConnectionAge; a > 0 && time.Since(pc.born) >= a {
		pc.retired = true
	}
}

func (p *CarrierPool[C]) release(pc *pooledConn[C]) {
	p.mu.Lock()
	pc.active--
	p.retireLocked(pc)
	done := (pc.retired || p.closed) && pc.active == 0
	if done {
		p.drop(pc)
	}
	p.mu.Unlock()
	if done && p.Close != nil {
		p.Close(pc.c)
	}
}

// RetireConn retires connection c (for example when it failed): it takes
// no new pipe and closes once its pipes are released.
func (p *CarrierPool[C]) RetireConn(c C) {
	p.mu.Lock()
	var pc *pooledConn[C]
	for _, x := range p.conns {
		if x.c == c {
			pc = x
		}
	}
	p.mu.Unlock()
	if pc != nil {
		p.retire(pc)
	}
}

func (p *CarrierPool[C]) retire(pc *pooledConn[C]) {
	p.mu.Lock()
	pc.retired = true
	done := pc.active == 0
	if done {
		p.drop(pc)
	}
	p.mu.Unlock()
	if done && p.Close != nil {
		p.Close(pc.c)
	}
}

func (p *CarrierPool[C]) drop(pc *pooledConn[C]) {
	for i, x := range p.conns {
		if x == pc {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			return
		}
	}
}

// Shutdown retires every connection: idle ones close now, the others when
// their last pipe ends; Get fails afterwards.
func (p *CarrierPool[C]) Shutdown() {
	p.mu.Lock()
	p.closed = true
	var idle []C
	for _, pc := range p.conns {
		pc.retired = true
		if pc.active == 0 {
			idle = append(idle, pc.c)
		}
	}
	p.conns = keepActive(p.conns)
	p.mu.Unlock()
	if p.Close != nil {
		for _, c := range idle {
			p.Close(c)
		}
	}
}

func keepActive[C comparable](cs []*pooledConn[C]) []*pooledConn[C] {
	out := cs[:0]
	for _, pc := range cs {
		if pc.active > 0 {
			out = append(out, pc)
		}
	}
	clear(cs[len(out):])
	return out
}
