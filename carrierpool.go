package zmux

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// PoolLimits bound how a carrier pool uses each underlying connection. The
// zero value selects the defaults.
type PoolLimits struct {
	// MaxPipesPerConnection bounds the pipes on a connection at once
	// (0: 8).
	MaxPipesPerConnection int
	// MaxPipesPerConnectionLifetime bounds the pipes a connection carries
	// over its life (0: unlimited).
	MaxPipesPerConnectionLifetime int
	// MaxConnectionAge stops giving pipes to a connection this old (0:
	// unlimited).
	MaxConnectionAge time.Duration
}

// CarrierPool shares underlying connections between pipes, for adapters
// that carry several pipes over one connection (HTTP/2 and HTTP/3 request
// streams, QUIC streams). A new pipe goes to the open
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
	// PoolLimits bound the use of each connection; set them before the
	// first Get.
	PoolLimits

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
// has; the dial runs outside the pool's lock. The lease is non-nil exactly
// when the error is nil.
func (p *CarrierPool[C]) Get(ctx context.Context) (*Lease[C], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l, err := p.take()
	if err != nil {
		return nil, err
	}
	if l != nil {
		return l, nil
	}
	// An expired-connection Close callback may have stopped this pool or context.
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, errPoolClosed
	}
	c, err := p.Dial(ctx)
	if err != nil {
		return nil, err
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	pc := &pooledConn[C]{c: c, fate: binary.BigEndian.Uint64(b[:])&wire.MaxVarint | 1, born: time.Now(), active: 1, total: 1}
	cancelled := ctx.Err()
	p.mu.Lock()
	if p.closed || cancelled != nil {
		if p.closed {
			cancelled = errPoolClosed
		}
		p.mu.Unlock()
		if p.Close != nil {
			p.Close(c)
		}
		return nil, cancelled
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
func (p *CarrierPool[C]) take() (*Lease[C], error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPoolClosed
	}
	var best *pooledConn[C]
	var idle []C
	live := p.conns[:0]
	for _, pc := range p.conns {
		p.retireLocked(pc)
		if pc.retired && pc.active == 0 {
			idle = append(idle, pc.c)
			continue
		}
		live = append(live, pc)
		if pc.retired || pc.active >= p.maxActive() {
			continue
		}
		if best == nil || p.room(pc) > p.room(best) {
			best = pc
		}
	}
	clear(p.conns[len(live):])
	p.conns = live
	var lease *Lease[C]
	if best != nil {
		best.active++
		best.total++
		p.retireLocked(best)
		lease = &Lease[C]{Conn: best.c, FateGroup: best.fate, p: p, pc: best}
	}
	p.mu.Unlock()
	if p.Close != nil {
		for _, c := range idle {
			p.Close(c)
		}
	}
	return lease, nil
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
		done = p.drop(pc)
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
		done = p.drop(pc)
	}
	p.mu.Unlock()
	if done && p.Close != nil {
		p.Close(pc.c)
	}
}

func (p *CarrierPool[C]) drop(pc *pooledConn[C]) bool {
	for i, x := range p.conns {
		if x == pc {
			copy(p.conns[i:], p.conns[i+1:])
			p.conns[len(p.conns)-1] = nil
			p.conns = p.conns[:len(p.conns)-1]
			return true
		}
	}
	return false
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
