package zmux

import (
	"context"
	"sync"
)

// A request keeps its slot until adoption or refund. It can outlive its
// direction and never retains either stream.
type spliceSlots struct {
	used chan struct{}
	wake chan struct{}
}

func newSpliceSlots(n int) *spliceSlots {
	return &spliceSlots{used: make(chan struct{}, n), wake: make(chan struct{}, 1)}
}

func (s *spliceSlots) release() {
	<-s.used
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

type splicePermit struct {
	mu     sync.Mutex
	done   chan struct{}
	cancel context.CancelFunc
	lim    Limiter

	slots          *spliceSlots
	amount         int64
	err            error
	ready, revoked bool
}

func (s *spliceSlots) request(ctx context.Context, lim Limiter, minimum, maximum int64, datagram bool) *splicePermit {
	select {
	case s.used <- struct{}{}:
	default:
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &splicePermit{done: make(chan struct{}), cancel: cancel, lim: lim, slots: s}
	go func() {
		var amount int64
		var err error
		if datagram {
			amount, err = lim.TryAcquire(minimum, maximum)
		} else {
			amount, err = lim.Acquire(ctx, minimum, maximum)
		}
		p.mu.Lock()
		p.ready, p.amount, p.err = true, amount, err
		revoked := p.revoked
		p.mu.Unlock()
		if revoked {
			p.returnGrant(amount)
		}
		close(p.done)
	}()
	return p
}

func (p *splicePermit) returnGrant(n int64) {
	p.lim.Refund(n)
	p.slots.release()
}

// revoke and take have one caller, the direction owner. Only completion
// races with revocation.
func (p *splicePermit) revoke() {
	if p == nil {
		return
	}
	p.cancel()
	p.mu.Lock()
	p.revoked = true
	ready, n := p.ready, p.amount
	p.mu.Unlock()
	if ready {
		p.returnGrant(n)
	}
}

func (p *splicePermit) take() (int64, error) {
	p.cancel()
	p.slots.release()
	return p.amount, p.err
}
