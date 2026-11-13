package zmux

import (
	"fmt"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// QualityBand asks for EventPipeQualityChanged events: emitted for an
// outbound pipe when its smoothed RTT leaves +-RTT around the last
// reported value, or its delivery rate leaves [last/RateRatio,
// last*RateRatio]; at most one per pipe per MinInterval, the latest values
// winning. The zero QualityBand reports nothing. A negative or NaN field
// is invalid (ErrInvalidConfig).
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
// EventCapacityRestored once it is back at Rate*RateRatio. A zero Rate
// turns it off, so the zero CapacityFloor reports nothing; a zero
// RateRatio means 1.25.
type CapacityFloor struct {
	Rate      float64
	RateRatio float64
}

// core resolves b for the core, whose MinInterval is 0 for the default
// and negative for none.
func (b QualityBand) core() (core.QualityBand, error) {
	if b.RTT < 0 || !(b.RateRatio >= 0) {
		return core.QualityBand{}, fmt.Errorf("%w: negative or NaN QualityBand field", ErrInvalidConfig)
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
	_ = c.s.SetQualityBand(-1, cb)
	return nil
}

// SetQualityBand sets the band of this pipe.
func (p *Pipe) SetQualityBand(b QualityBand) error {
	cb, err := b.core()
	if err != nil {
		return err
	}
	c := p.c
	c.lock()
	defer c.unlock()
	return pipeErr(c.s.SetQualityBand(int64(p.id), cb))
}

// SetCapacityFloor sets the capacity floor of the outbound direction.
func (c *Conn) SetCapacityFloor(f CapacityFloor) {
	c.lock()
	defer c.unlock()
	c.s.SetCapacityFloor(core.CapacityFloor{Rate: f.Rate, RateRatio: f.RateRatio})
}
