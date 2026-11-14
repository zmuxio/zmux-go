package zmux

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Backoff is a retry backoff: after a failed attempt the wait starts at
// Min and doubles up to Max, each wait reduced by a random fraction of up
// to Jitter. A zero field takes the default of the user of the Backoff
// (WithDefaults): for a redial source (Config.RedialBackoff,
// PipePool.Backoff, where the wait counts from the start of the previous
// attempt) DefaultMinBackoff, DefaultMaxBackoff and DefaultBackoffJitter;
// another user, such as a carrier, documents its own. A negative Min or
// Max, a Min above Max (the defaults filled in) or a Jitter of 1 or more
// (or NaN) is invalid (ErrInvalidConfig); a negative Jitter is a jitter
// of 0.
type Backoff struct {
	// Min is the first wait. Zero: the user's default.
	Min time.Duration
	// Max is the longest wait. Zero: the user's default.
	Max time.Duration
	// Jitter is the largest fraction a wait is reduced by, in [0, 1).
	// Zero: the user's default; a negative value: none.
	Jitter float64
}

// WithDefaults returns b with the fields of def in place of its zero
// fields and a negative Jitter as 0, or an ErrInvalidConfig error when b
// is invalid (see Backoff). def is taken as valid.
func (b Backoff) WithDefaults(def Backoff) (Backoff, error) {
	if b.Min < 0 || b.Max < 0 || math.IsNaN(b.Jitter) || b.Jitter >= 1 {
		return Backoff{}, fmt.Errorf("%w: Backoff with a negative Min or Max, or a Jitter of 1 or more", ErrInvalidConfig)
	}
	if b.Min == 0 {
		b.Min = def.Min
	}
	if b.Max == 0 {
		b.Max = def.Max
	}
	switch {
	case b.Jitter == 0:
		b.Jitter = def.Jitter
	case b.Jitter < 0:
		b.Jitter = 0
	}
	if b.Min > b.Max {
		return Backoff{}, fmt.Errorf("%w: Backoff Min %v above Max %v", ErrInvalidConfig, b.Min, b.Max)
	}
	return b, nil
}

// redialDefaults is the default Backoff of a redial source.
var redialDefaults = Backoff{Min: DefaultMinBackoff, Max: DefaultMaxBackoff, Jitter: DefaultBackoffJitter}

// filled returns b with the redial defaults filled in; b is valid.
func (b Backoff) filled() Backoff {
	f, _ := b.WithDefaults(redialDefaults)
	return f
}

// validate reports an invalid b, as the field of what names it.
func (b Backoff) validate(what string) error {
	if _, err := b.WithDefaults(redialDefaults); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

// wait returns d reduced by a random fraction of up to b.Jitter, drawn
// once per wait; b is filled.
func (b Backoff) wait(d time.Duration) time.Duration {
	if b.Jitter == 0 {
		return d
	}
	return time.Duration(float64(d) * (1 - b.Jitter*rand.Float64()))
}
