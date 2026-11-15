// Package testscale scales the time budgets of tests that wait for
// something to happen, so that they hold on a loaded host and under the race
// detector: a budget is an upper bound, which a passing test never waits
// for whole, so scaling it costs a passing run nothing.
package testscale

import (
	"os"
	"strconv"
	"time"

	"github.com/zmuxio/zmux-go/internal/israce"
)

// factor is ZMUX_TEST_SCALE (a positive number, 1 when unset or invalid),
// doubled under the race detector.
var factor = func() float64 {
	f := 1.0
	if v, err := strconv.ParseFloat(os.Getenv("ZMUX_TEST_SCALE"), 64); err == nil && v > 0 && v < 1000 {
		f = v
	}
	if israce.On {
		f *= 2
	}
	return f
}()

// D returns the budget d scaled for this run.
func D(d time.Duration) time.Duration { return time.Duration(float64(d) * factor) }
