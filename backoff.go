package zmux

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Backoff configures exponential retry from Min up to Max. An omitted
// jitter uses the caller's default. Set(0.0) explicitly disables jitter.
type Backoff struct {
	Min, Max         time.Duration
	Jitter, JitterUp Change[float64]
}

// ResolvedBackoff is a validated retry configuration with numeric jitter
// fractions in [0, 1].
type ResolvedBackoff struct {
	Min, Max         time.Duration
	Jitter, JitterUp float64
}

// WithDefaults resolves omitted values and validates the maximum sampled wait.
func (b Backoff) WithDefaults(def Backoff) (ResolvedBackoff, error) {
	down, ok := b.Jitter.get()
	if !ok {
		down, _ = def.Jitter.get()
	}
	up, ok := b.JitterUp.get()
	if !ok {
		up, _ = def.JitterUp.get()
	}
	first, last := b.Min, b.Max
	if first == 0 {
		first = def.Min
	}
	if last == 0 {
		last = def.Max
	}
	if first <= 0 || last < first || !(down >= 0 && down <= 1) || !(up >= 0 && up <= 1) {
		return ResolvedBackoff{}, fmt.Errorf("%w: Backoff Min, Max or jitter out of range", ErrInvalidConfig)
	}
	if up > 0 && float64(last)*(1+up) >= 0x1p63 {
		return ResolvedBackoff{}, fmt.Errorf("%w: maximum jittered Backoff overflows", ErrInvalidConfig)
	}
	return ResolvedBackoff{Min: first, Max: last, Jitter: down, JitterUp: up}, nil
}

// retryDefaults is the default Backoff for ordinary retries.
var retryDefaults = Backoff{Min: DefaultMinBackoff, Max: DefaultMaxBackoff, Jitter: Set(DefaultBackoffJitter)}

// filled returns b with the ordinary retry defaults filled in; b is valid.
func (b Backoff) filled() ResolvedBackoff {
	f, _ := b.WithDefaults(retryDefaults)
	return f
}

// validate reports an invalid b, as the field of what names it.
func (b Backoff) validate(what string) error {
	if _, err := b.WithDefaults(retryDefaults); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

// Wait draws the sampled delay for the given nominal wait.
func (b ResolvedBackoff) Wait(d time.Duration) time.Duration {
	if b.Jitter == 0 && b.JitterUp == 0 {
		return d
	}
	return b.waitSample(d, rand.Float64()) //nolint:gosec // jitter needs no unpredictability
}

func (b ResolvedBackoff) waitSample(d time.Duration, u float64) time.Duration {
	return jitteredDuration(d, 1-b.Jitter+(b.Jitter+b.JitterUp)*u)
}

// NextNominal doubles the previous nominal delay within the configured bounds.
func (b ResolvedBackoff) NextNominal(previous time.Duration) time.Duration {
	return min(max(satDouble(previous), b.Min), b.Max)
}

func (b ResolvedBackoff) waitUpper(d time.Duration) time.Duration {
	return jitteredDuration(d, 1+b.JitterUp)
}

func jitteredDuration(d time.Duration, factor float64) time.Duration {
	if factor == 1 {
		return d
	}
	n := float64(d) * factor
	if n >= math.MaxInt64 {
		return math.MaxInt64
	}
	return time.Duration(n)
}
