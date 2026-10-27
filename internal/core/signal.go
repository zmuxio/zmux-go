package core

// Signal is a single-waiter wake-up slot.
// The core queues it when the waiter's condition may have become true; the
// driver delivers it after releasing the lock. Its channel has capacity one
// so a signal sent before the waiter selects is never lost.
type Signal struct {
	C      chan struct{}
	queued bool
}

// NewSignal returns a ready slot.
func NewSignal() *Signal { return &Signal{C: make(chan struct{}, 1)} }

// Drain removes a stale token; called under the session lock when the
// waiter registers. A zero Signal is lazy: Drain makes its channel, so a
// slot that never waited holds none (the core never queues a slot
// without one, and the channel never changes once made).
func (s *Signal) Drain() {
	if s.C == nil {
		s.C = make(chan struct{}, 1)
		return
	}
	select {
	case <-s.C:
	default:
	}
}

// Fire delivers the token without blocking (outside the session lock).
func (s *Signal) Fire() { s.fire() }

// FireLocked delivers the token under the session lock; a lazy slot that
// never waited has no waiter to wake.
func (s *Signal) FireLocked() {
	if s.C != nil {
		s.fire()
	}
}

// fire delivers the token without blocking.
func (s *Signal) fire() {
	select {
	case s.C <- struct{}{}:
	default:
	}
}

// wake queues sig for delivery after the unlock.
func (s *Session) wake(sig *Signal) {
	if sig == nil || sig.queued || sig.C == nil {
		return
	}
	sig.queued = true
	s.wakes = append(s.wakes, sig)
}

// TakeWakes returns the queued signals and clears their queued flags. The
// caller fires them after unlocking with FireAll.
func (s *Session) TakeWakes(dst []*Signal) []*Signal {
	for _, w := range s.wakes {
		w.queued = false
	}
	dst = append(dst, s.wakes...)
	clear(s.wakes)
	s.wakes = s.wakes[:0]
	return dst
}

// FireAll delivers signals; call without the session lock.
func FireAll(sigs []*Signal) {
	for _, s := range sigs {
		s.fire()
	}
}
