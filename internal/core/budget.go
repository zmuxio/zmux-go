package core

import "sync/atomic"

// Budget is a memory budget shared by the sessions of a listener: the sum over
// them of buffered receive data, retained data and the growth of each session
// window above its initial size. Sessions charge it from their own critical
// sections with atomics only (lock level L3); pressure starts when the sum
// reaches the limit and ends when it falls below three quarters of it, and each
// transition is signalled on Changed for the listener's serial context.
type Budget struct {
	limit   uint64 // 0: unlimited
	used    atomic.Int64
	over    atomic.Bool
	Changed chan struct{} // capacity 1
}

// NewBudget returns a budget of limit bytes (0: unlimited).
func NewBudget(limit uint64) *Budget {
	return &Budget{limit: limit, Changed: make(chan struct{}, 1)}
}

// Limit is the budget's size (0: unlimited).
func (b *Budget) Limit() uint64 { return b.limit }

// Used is the sum currently charged.
func (b *Budget) Used() uint64 { return uint64(max(b.used.Load(), 0)) }

// Pressure reports whether the sum is above the budget.
func (b *Budget) Pressure() bool { return b.over.Load() }

// Add charges d bytes (negative: releases them); carrier adapters charge
// the bytes they hold for the listener's sessions this way.
func (b *Budget) Add(d int64) {
	u := b.used.Add(d)
	if b.limit == 0 {
		return
	}
	switch {
	case u >= int64(b.limit) && !b.over.Load():
		if b.over.CompareAndSwap(false, true) {
			b.signal()
		}
	case u < int64(b.limit/4*3) && b.over.Load():
		if b.over.CompareAndSwap(true, false) {
			b.signal()
		}
	}
}

func (b *Budget) signal() {
	select {
	case b.Changed <- struct{}{}:
	default:
	}
}

// memUsage is what the session holds against a budget.
func (s *Session) memUsage() uint64 {
	if s.state == StateClosed {
		return 0
	}
	c := &s.conn
	return c.buffered + c.sendHeld + c.win - min64(c.win, s.initConnWin)
}

// ChargeBudget brings the session's charge on a shared budget up to date;
// the driver calls it at the end of every critical section.
func (s *Session) ChargeBudget() {
	b := s.cfg.Budget
	if b == nil {
		return
	}
	if u := s.memUsage(); u != s.charged {
		b.Add(int64(u) - int64(s.charged))
		s.charged = u
	}
}

// Charged is the session's current charge on its budget.
func (s *Session) Charged() uint64 { return s.charged }

// underPressure reports listener memory pressure:
// the session behaves as at its buffer cap and refuses new peer streams.
func (s *Session) underPressure() bool {
	return s.cfg.Budget != nil && s.cfg.Budget.over.Load()
}

// Refuse closes a session the listener does not take (CONNECTION_REFUSED
// after its own parameters).
func (s *Session) Refuse() {
	s.closeTransport(&SessionError{Kind: CloseTransport, Code: 0x02, Reason: "memory budget"})
}
