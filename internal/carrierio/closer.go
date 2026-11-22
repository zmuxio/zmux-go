package carrierio

import (
	"io"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/poolhost"
)

// Closer states.
const (
	closerOpen uint32 = iota
	closerClosed
	closerReleased
)

// Closer is the one closer of a carrier's halves while it is being set up:
// every party that may end the carrier before its next owner takes it (a
// setup bound, each failure path, an eviction) closes it through the same
// Closer, so that each half is closed once, whatever the order, also when
// the halves are one value. Release hands the halves to their next owner,
// to which a later Close is forwarded.
type Closer struct {
	r, w  io.Closer     // r nil when it is w
	next  func()        // the next owner's close; set before the release
	done  chan struct{} // closed once the halves are closed
	state atomic.Uint32
}

// NewCloser returns the closer of the halves r and w (either may be nil).
func NewCloser(r, w io.Closer) *Closer {
	if r != nil && SameHalf(r, w) {
		r = nil
	}
	return &Closer{r: r, w: w, done: make(chan struct{})}
}

// Close closes the halves, the write half first, unless they were closed
// already, and returns once they are closed: a Close that finds another
// one in progress waits for it, as sync.Once.Do does, so that a carrier is
// closed when the call that ended it returns, whichever party closed it.
// The halves' own Close must therefore not call back into the Closer. Once
// they were released it asks their next owner to close them.
func (c *Closer) Close() {
	if c.CloseUnreleased() && c.next != nil {
		c.next()
	}
}

// CloseUnreleased closes the halves unless they were closed or released
// already; released reports that they were released. It is Close for the
// next owner itself, which closes released halves on its own.
func (c *Closer) CloseUnreleased() (released bool) {
	if !c.state.CompareAndSwap(closerOpen, closerClosed) {
		if c.state.Load() == closerReleased {
			return true
		}
		<-c.done
		return false
	}
	defer close(c.done)
	if c.w != nil {
		CloseHalf(c.w)
	}
	if c.r != nil {
		CloseHalf(c.r)
	}
	return false
}

// Release hands the halves to their next owner, which closes them from
// then on; next (nil: none) is that owner's close, which a later Close
// calls. It reports false when a Close came first: the halves are closed.
// Halves are released once.
func (c *Closer) Release(next func()) bool {
	c.next = next
	return c.state.CompareAndSwap(closerOpen, closerReleased)
}

// CloseHalf contains a host panic so teardown continues with the other half.
func CloseHalf(h io.Closer) {
	poolhost.Safe(nil, "carrier Close", func() { _ = h.Close() })
}
