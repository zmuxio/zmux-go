// Package core is the sans-IO zmux session (IMPLEMENTATION.md Section 2.1).
// Every method must be called with the session's lock held by the caller;
// nothing here blocks, performs I/O or calls application code.
package core

import "math"

// Time is a point in nanoseconds on the session's monotonic clock.
type Time int64

// Duration is a span in nanoseconds.
type Duration = int64

// Never is a deadline that never fires (IMPLEMENTATION.md 13.4 T1).
const Never Time = math.MaxInt64

const (
	Microsecond Duration = 1000
	Millisecond          = 1000 * Microsecond
	Second               = 1000 * Millisecond
)

// Add adds d with saturation: an overflow is Never.
func (t Time) Add(d Duration) Time {
	if t == Never {
		return Never
	}
	if d > 0 && Duration(t) > math.MaxInt64-d {
		return Never
	}
	if d < 0 && Duration(t) < math.MinInt64-d {
		return math.MinInt64
	}
	return t + Time(d)
}

// Sub returns t-u, saturating.
func (t Time) Sub(u Time) Duration {
	if t == Never {
		return math.MaxInt64
	}
	return Duration(t - u)
}

// MulD multiplies a duration with saturation.
func MulD(d Duration, k int64) Duration {
	if d == 0 || k == 0 {
		return 0
	}
	if d > math.MaxInt64/k {
		return math.MaxInt64
	}
	return d * k
}

func minT(a, b Time) Time {
	if a < b {
		return a
	}
	return b
}

func maxD(a, b Duration) Duration {
	if a > b {
		return a
	}
	return b
}

func minD(a, b Duration) Duration {
	if a < b {
		return a
	}
	return b
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func maxT(a, b Time) Time {
	if a > b {
		return a
	}
	return b
}
