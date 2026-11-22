// Package poolhost holds what the pools of the root package and of carrier
// share: the order in which a shrinking pool closes members and how a
// rejected policy decision is reported.
package poolhost

import (
	"fmt"
	"log/slog"
	"slices"
	"time"
)

// Member is a pool member as the shrink order sees it: PoolMember's Idle,
// Pipes and Age, its key (PipeID, fate group) and whether the policy
// retired it.
type Member struct {
	Idle   time.Duration
	Age    time.Duration
	Key    uint64
	Pipes  int
	Retire bool
}

// Before reports whether member a closes before b when a pool shrinks:
// those the policy retires first, then the longest idle, the fewest pipes,
// the least age, then the highest key.
func Before(a, b Member) bool {
	switch {
	case a.Retire != b.Retire:
		return a.Retire
	case a.Idle != b.Idle:
		return a.Idle > b.Idle
	case a.Pipes != b.Pipes:
		return a.Pipes < b.Pipes
	case a.Age != b.Age:
		return a.Age < b.Age
	}
	return a.Key > b.Key
}

// Plan is the shrink order of the pool policy, over the growth dials under
// way and the members: when they exceed count it cancels the growth dials first (how
// many, the most recently started first), then closes members in Before
// order (their indexes in ms, in that order).
func Plan(growth int, ms []Member, count int) (cancel int, closing []int) {
	excess := growth + len(ms) - count
	if excess <= 0 {
		return 0, nil
	}
	cancel = min(growth, excess)
	rest := make([]int, len(ms))
	for i := range rest {
		rest[i] = i
	}
	for n := excess - cancel; n > 0 && len(rest) > 0; n-- {
		k := 0
		for j := 1; j < len(rest); j++ {
			if Before(ms[rest[j]], ms[rest[k]]) {
				k = j
			}
		}
		closing = append(closing, rest[k])
		rest = slices.Delete(rest, k, k+1)
	}
	return cancel, closing
}

// Safe runs the host callback f, containing a panic (ok false), which it
// logs as the panic of what to l (nil: not logged), outside every lock.
func Safe(l *slog.Logger, what string, f func()) (ok bool) {
	return Call(l, what, func() error { f(); return nil }) == nil
}

// Call isolates host code and preserves an error raised by a panic.
func Call(l *slog.Logger, what string, f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if cause, ok := r.(error); ok {
				err = fmt.Errorf("zmux: %s panicked: %w", what, cause)
			} else {
				err = fmt.Errorf("zmux: %s panicked: %v", what, r)
			}
			if l != nil {
				// The warning itself calls host code through the logger.
				defer func() { _ = recover() }()
				l.Warn("zmux: a host callback panicked; the panic was contained", "callback", what, "panic", fmt.Sprint(r))
			}
		}
	}()
	return f()
}

// ReportRejected hands the error of a rejected decision to a pool's
// DecisionRejected callback f, containing a panic (logged to l), or, when
// f is unset, logs it to l. Callers hold no lock.
func ReportRejected(l *slog.Logger, f func(error), err error) {
	if f != nil {
		Safe(l, "DecisionRejected", func() { f(err) })
		return
	}
	if l != nil {
		l.Warn("zmux: a pool policy decision was rejected; the pool's own behaviour applies", "error", err)
	}
}
