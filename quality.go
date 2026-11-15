package zmux

import (
	"fmt"
	"math"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// QualityBand asks for EventPipeQualityChanged events: emitted for an
// outbound pipe when its smoothed RTT leaves +-RTT around the last
// reported value, or its delivery rate leaves [last/RateRatio,
// last*RateRatio]; at most one per pipe per MinInterval, the latest values
// winning. The zero QualityBand reports nothing. A negative RTT, or a
// RateRatio other than 0 or a finite number of at least 1, is invalid
// (ErrInvalidConfig).
type QualityBand struct {
	// RTT is the band around the reported smoothed RTT. Zero turns this
	// half off.
	RTT time.Duration
	// RateRatio is the factor around the reported delivery rate. Zero
	// turns this half off.
	RateRatio float64
	// MinInterval is the least time between two events of a pipe. Zero
	// is 1 s; a negative value sets no minimum.
	MinInterval time.Duration
}

// CapacityFloor asks for EventCapacityBelow once the summed delivery rate
// of the usable, non-standby outbound pipes falls below Rate (bytes per
// second, as every delivery rate of the API), and
// EventCapacityRestored once it is back at Rate*RecoveryRatio. A zero Rate
// turns it off, so the zero CapacityFloor reports nothing. A zero
// RecoveryRatio means 1.25 (1 is no hysteresis); any other must be a
// finite number of at least 1 (a smaller one would restore below the
// floor, an infinite one never), else SetCapacityFloor returns
// ErrInvalidConfig.
type CapacityFloor struct {
	Rate          uint64
	RecoveryRatio float64
}

// rateBytes is a rate estimate of the core in whole bytes per second.
func rateBytes(f float64) uint64 {
	if f > 0 {
		return uint64(f)
	}
	return 0
}

// core resolves b for the core, whose MinInterval is 0 for the default
// and negative for none.
func (b QualityBand) core() (core.QualityBand, error) {
	if b.RTT < 0 || !validRatio(b.RateRatio) {
		return core.QualityBand{}, fmt.Errorf("%w: QualityBand RTT %v, RateRatio %v", ErrInvalidConfig, b.RTT, b.RateRatio)
	}
	mi := core.Duration(max(b.MinInterval, -1))
	return core.QualityBand{RTT: core.Duration(b.RTT), RateRatio: b.RateRatio, MinInterval: mi}, nil
}

// SetQualityBand sets the default band of the session's pipes.
func (c *Conn) SetQualityBand(b QualityBand) error {
	cb, err := b.core()
	if err != nil {
		return err
	}
	c.lock()
	defer c.unlock()
	_ = c.s.SetQualityBand(-1, &cb)
	return nil
}

// validRatio reports whether r is a valid ratio of a QualityBand or a
// CapacityFloor: 0 (the default) or a
// finite factor of at least 1. NaN, +Inf and factors below 1 are not.
func validRatio(r float64) bool {
	return r == 0 || r >= 1 && r <= math.MaxFloat64
}

// SetCapacityFloor sets the capacity floor of the outbound direction; a
// RecoveryRatio that is not valid (see CapacityFloor) is ErrInvalidConfig, as
// in a QualityBand.
func (c *Conn) SetCapacityFloor(f CapacityFloor) error {
	if !validRatio(f.RecoveryRatio) {
		return fmt.Errorf("%w: CapacityFloor.RecoveryRatio %v", ErrInvalidConfig, f.RecoveryRatio)
	}
	c.lock()
	defer c.unlock()
	c.s.SetCapacityFloor(core.CapacityFloor{Rate: float64(f.Rate), RateRatio: f.RecoveryRatio})
	return nil
}
