package zmux

import (
	"context"
	"sync"
)

// The script controls available credit independently of maximum burst.
type spliceScriptLimiter struct {
	spec     spliceVectorLimiter
	cancel   <-chan struct{}
	mu       sync.Mutex
	requests []*spliceScriptGrant
	refunds  []int64
	calls    int
}

type spliceScriptGrant struct {
	amount int64
	reply  chan int64
}

func newSpliceScriptLimiter(l *spliceVectorLimiter, cancel <-chan struct{}) *spliceScriptLimiter {
	return &spliceScriptLimiter{spec: *l, cancel: cancel}
}
func (l *spliceScriptLimiter) MaxBurstBytes() int64 {
	if l.spec.Burst == nil {
		return -1
	}
	return *l.spec.Burst
}
func (l *spliceScriptLimiter) Acquire(_ context.Context, minimum, maximum int64) (int64, error) {
	return l.TryAcquire(minimum, maximum)
}
func (l *spliceScriptLimiter) TryAcquire(minimum, maximum int64) (int64, error) {
	l.mu.Lock()
	l.calls++
	if l.spec.Hold {
		g := &spliceScriptGrant{maximum, make(chan int64, 1)}
		l.requests = append(l.requests, g)
		l.mu.Unlock()
		select {
		case n := <-g.reply:
			return n, nil
		case <-l.cancel:
			return 0, context.Canceled
		}
	}
	defer l.mu.Unlock()
	n := min(maximum, l.spec.Available)
	if n < minimum && !l.spec.Short {
		return 0, nil
	}
	l.spec.Available -= n
	return n, nil
}
func (l *spliceScriptLimiter) Refund(n int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refunds = append(l.refunds, n)
	l.spec.Available += n
}
func (l *spliceScriptLimiter) grant(n int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	g := l.requests[0]
	l.requests = l.requests[1:]
	g.reply <- n
}

type spliceVectorLimiter struct {
	Burst     *int64 `json:"max_burst_bytes"`
	Available int64  `json:"available_bytes"`
	Short     bool   `json:"return_available_on_shortfall"`
	Hold      bool   `json:"hold_acquire_result"`
}
