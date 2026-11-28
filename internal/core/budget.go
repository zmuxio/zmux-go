package core

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// ErrMemoryBudget reports work that cannot be admitted by a memory budget.
var ErrMemoryBudget = errors.New("zmux: memory budget exhausted")

const (
	// Compact when queued backing exceeds twice the logical payload. The
	// reservation covers both that backing and credit that is still in flight.
	receiveCreditCost = 2
	// A backing being published and one packed destination can coexist.
	receiveWorkingSpace = 2 * RecvBlockSize
	// A read can hand off a full backing while the advertised receive
	// window remains available to the peer.
	receiveLoanFloor = RecvBlockSize
	// A second carrier can authenticate and start while receive credit and
	// writes are using the rest of the budget.
	joinFlightSpace = 3 * RecvBlockSize
	// A full small-direction write can proceed independently of MAX_DATA.
	sendWorkingSpace = (smallHeld / blockSize) * sendBlockCharge
)

// ReserveReceive computes the initial advertised window before any transport
// parameters can leave. A listener and all of its sessions use the same
// ledger. Standalone sessions have a private one.
func (c *Config) ReserveReceive() error {
	c.InitBudget()
	if c.Budget == nil {
		return nil
	}
	overhead := uint64(receiveWorkingSpace + receiveLoanFloor + sendWorkingSpace)
	if c.Local.Has&wire.HasMultipath != 0 {
		c.joinHold = joinFlightSpace
		overhead += c.joinHold
	}
	granted, ok := c.Budget.Reserve(overhead, receiveCreditCost, MinConnWindow, c.Local.InitialMaxData)
	if !ok {
		return ErrMemoryBudget
	}
	c.Local.InitialMaxData = granted
	c.receiveHold = overhead + receiveCreditCost*granted
	return nil
}

func (c *Config) InitBudget() {
	if c.Budget == nil && c.MemoryBudget != 0 {
		c.Budget = NewBudget(c.MemoryBudget)
	}
}

// Budget tracks committed backing capacity, including reservations for work
// that cannot be refused after admission. Updates never take a session lock.
// Changed reports pressure transitions to the listener.
type Budget struct {
	Changed chan struct{} // capacity 1
	mu      sync.Mutex    // serializes commitments with limit changes
	limit   atomic.Uint64 // 0: unlimited
	used    atomic.Int64
	over    atomic.Bool
	ingress uint64 // space kept available for first-record classification
}

// NewBudget returns a budget of limit bytes (0: unlimited).
func NewBudget(limit uint64) *Budget {
	b := &Budget{Changed: make(chan struct{}, 1)}
	b.limit.Store(limit)
	return b
}

// NewListenerBudget keeps room to classify one more carrier when session
// storage is full. Until it has a locator, the carrier cannot use a
// session's Join reservation.
func NewListenerBudget(limit uint64) *Budget {
	b := NewBudget(limit)
	b.ingress = joinFlightSpace
	return b
}

// Limit is the budget's size (0: unlimited).
func (b *Budget) Limit() uint64 { return b.limit.Load() }

// SetLimit refuses a limit below the bytes already charged.
func (b *Budget) SetLimit(limit uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit != 0 && limit < b.Used() {
		return false
	}
	b.limit.Store(limit)
	b.updatePressure()
	return true
}

// Used is the sum currently charged.
func (b *Budget) Used() uint64 { return uint64(max(b.used.Load(), 0)) }

// Pressure reports whether the sum is above the budget.
func (b *Budget) Pressure() bool { return b.over.Load() }

// Release returns bytes previously charged to the budget.
func (b *Budget) Release(n uint64) bool {
	if n == 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.Used() {
		return false
	}
	b.used.Add(-int64(n))
	b.updatePressure()
	return true
}

// Reserve grants up to want units at cost bytes per unit after reserving
// the fixed overhead. If the minimum cannot fit, it leaves the ledger intact.
func (b *Budget) Reserve(fixed, cost, minimum, want uint64) (uint64, bool) {
	return b.reserve(fixed, cost, minimum, want, b.ingress)
}

func (b *Budget) reserve(fixed, cost, minimum, want, spare uint64) (uint64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	limit := b.Limit()
	if limit == 0 {
		limit = ^uint64(0) >> 1
	} else {
		limit -= min(spare, limit)
	}
	used := b.Used()
	if used > limit || fixed > limit-used {
		return 0, false
	}
	granted := min(want, (limit-used-fixed)/cost)
	if granted < minimum {
		return 0, false
	}
	b.used.Add(int64(fixed + granted*cost))
	b.updatePressure()
	return granted, true
}

func (b *Budget) TryCharge(n uint64) bool {
	_, ok := b.reserve(n, 1, 0, 0, 0)
	return ok
}

func (b *Budget) updatePressure() {
	u, limit, over := b.Used(), b.Limit(), b.over.Load()
	switch {
	case !over && limit != 0 && u >= limit:
		b.over.Store(true)
		b.signal()
	case over && (limit == 0 || u < limit-limit/4):
		b.over.Store(false)
		b.signal()
	}
}

func (b *Budget) signal() {
	select {
	case b.Changed <- struct{}{}:
	default:
	}
}

// budgetAllowance applies separately reserved space to holdings that can
// outlive their session. Positive available is unused reservation, while
// negative available represents space already charged beyond the reservation.
type budgetAllowance struct {
	b         *Budget
	available atomic.Int64
	floor     int64
	cache     int64
	mu        sync.Mutex
}

func newBudgetAllowance(b *Budget, floor, cache int64) *budgetAllowance {
	a := &budgetAllowance{b: b, floor: floor, cache: cache}
	a.available.Store(floor)
	return a
}

const allowanceLocked = math.MinInt64

func (a *budgetAllowance) lock() int64 {
	a.mu.Lock()
	return a.available.Swap(allowanceLocked)
}

func (a *budgetAllowance) unlock(available int64) {
	a.available.Store(available)
	a.mu.Unlock()
}

func (a *budgetAllowance) adjust(d int64) bool {
	return a.adjustReserved(d) || a.adjustSlow(d)
}

// adjustReserved takes or returns part of an existing reservation without
// touching the shared ledger. Slow operations freeze this counter first.
func (a *budgetAllowance) adjustReserved(d int64) bool {
	if a == nil || d == 0 {
		return true
	}
	for n := a.available.Load(); n >= 0 && d <= n; n = a.available.Load() {
		if a.available.CompareAndSwap(n, n-d) {
			return true
		}
	}
	return false
}

func (a *budgetAllowance) adjustSlow(d int64) bool {
	n := a.lock()
	defer func() { a.unlock(n) }()
	if d > 0 && n < d-math.MaxInt64 {
		return false
	}
	next := n - d
	delta := max(-next, 0) - max(-n, 0)
	if delta > 0 {
		if n >= 0 && a.floor < a.cache {
			// Grow the local allowance in one transaction. This avoids
			// reacquiring the shared budget on every returned send block.
			growth := min(a.cache-a.floor, max(DefaultBatch, a.floor))
			extra, ok := a.b.Reserve(0, 1, uint64(delta), uint64(max(delta, growth)))
			if !ok {
				return false
			}
			a.floor += int64(extra)
			next += int64(extra)
		} else if _, ok := a.b.Reserve(uint64(delta), 1, 0, 0); !ok {
			return false
		}
	} else if delta < 0 {
		a.b.Release(uint64(-delta))
	}
	n = next
	return true
}

// changeLoanFloor adjusts replacement space for loans above the initial
// reserve. The backing itself stays charged through its last holder.
func (a *budgetAllowance) changeLoanFloor(change int64) bool {
	n := a.lock()
	defer func() { a.unlock(n) }()
	if a.floor == 0 {
		return true // a closed session has no more receive credit to replace
	}
	delta := max(n+change, 0) - max(n, 0)
	if delta > 0 {
		if _, ok := a.b.Reserve(uint64(delta), 1, 0, 0); !ok {
			return false
		}
	} else if delta < 0 {
		a.b.Release(uint64(-delta))
	}
	a.floor += change
	n += change
	return true
}

// grow grants as much of a window increase as the ledger can cover.
func (a *budgetAllowance) grow(want uint64) uint64 {
	n := a.lock()
	defer func() { a.unlock(n) }()
	covered := uint64(max(-n, 0))
	wantBytes := want * receiveCreditCost
	bytes := wantBytes
	if wantBytes > covered {
		paid, _ := a.b.Reserve(0, receiveCreditCost, 0, (wantBytes-covered)/receiveCreditCost)
		bytes = covered + paid*receiveCreditCost
	}
	a.floor += int64(bytes)
	n += int64(bytes)
	return bytes / receiveCreditCost
}

func (a *budgetAllowance) charged() uint64 {
	if a == nil {
		return 0
	}
	n := a.lock()
	used := a.floor + max(-n, 0)
	a.unlock(n)
	return uint64(used)
}

func (a *budgetAllowance) close() {
	if a == nil {
		return
	}
	n := a.lock()
	floor := a.floor
	a.floor, a.cache = 0, 0
	a.b.Release(uint64(max(n, 0)))
	a.unlock(n - floor)
}

// TrimSendReserve gives idle send work space back to the listener. Blocks
// in a queue, a write loan or an in-flight batch still own their capacity.
func (s *Session) TrimSendReserve() {
	a := s.sendMemory
	if a == nil || a.available.Load() <= sendWorkingSpace {
		return
	}
	n := a.lock()
	if n == a.floor && n > sendWorkingSpace {
		free := n - sendWorkingSpace
		a.b.Release(uint64(free))
		a.floor, n = sendWorkingSpace, sendWorkingSpace
	}
	a.unlock(n)
}

// unsharedUsage estimates room for a standalone session with no shared ledger.
func (s *Session) unsharedUsage() uint64 {
	if s.state == StateClosed {
		return 0
	}
	c := &s.conn
	return c.rx.metadata() + c.sendHeld + c.win - min64(c.win, s.initConnWin)
}

// Charged is the session's current charge on its budget.
func (s *Session) Charged() uint64 {
	return s.conn.rx.account.memory.charged() + s.sendMemory.charged() + s.flightMemory.charged() + s.datagramMemory.charged()
}

// ReserveFlight claims space that stays live until the carrier reader exits.
func (s *Session) ReserveFlight(n int64) bool { return s.flightMemory.adjust(n) }

// ReleaseFlight returns a carrier reader's claim.
func (s *Session) ReleaseFlight(n int64) { s.flightMemory.adjust(-n) }

// underPressure reports listener memory pressure for opportunistic growth.
func (s *Session) underPressure() bool {
	return s.cfg.Budget != nil && s.cfg.Budget.over.Load()
}

// Refuse closes a session the listener does not take by its policy
// (CONNECTION_REFUSED after its own parameters).
func (s *Session) Refuse(reason string) {
	s.closeTransport(&SessionError{Kind: CloseTransport, Code: wire.ConnectionRefused, Reason: reason})
}
