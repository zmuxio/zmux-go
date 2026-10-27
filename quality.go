package zmux

import (
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// QualityBand asks for EventPipeQualityChanged events: emitted for an
// outbound pipe when its smoothed RTT leaves +-RTT around the last
// reported value, or its delivery rate leaves [last/RateRatio,
// last*RateRatio]; at most one per pipe per MinInterval, the latest values
// winning. A zero RTT or RateRatio turns that half off, so the zero
// QualityBand reports nothing; a zero MinInterval means 1 s, a negative
// one no minimum.
type QualityBand struct {
	RTT         time.Duration
	RateRatio   float64
	MinInterval time.Duration
}

// CapacityFloor asks for EventCapacityBelow once the summed delivery rate
// of the usable, non-standby outbound pipes falls below Rate (bytes/s), and
// EventCapacityRestored once it is back at Rate*RateRatio. A zero Rate
// turns it off, so the zero CapacityFloor reports nothing; a zero
// RateRatio means 1.25.
type CapacityFloor struct {
	Rate      float64
	RateRatio float64
}

func (b QualityBand) core() core.QualityBand {
	mi := core.Duration(b.MinInterval)
	if b.MinInterval < 0 {
		mi = -1
	}
	return core.QualityBand{RTT: core.Duration(b.RTT), RateRatio: b.RateRatio, MinInterval: mi}
}

// SetQualityBand sets the default band of the session's pipes.
func (c *Conn) SetQualityBand(b QualityBand) {
	c.lock()
	defer c.unlock()
	_ = c.s.SetQualityBand(-1, b.core())
}

// SetQualityBand sets the band of this pipe.
func (p *Pipe) SetQualityBand(b QualityBand) error {
	c := p.c
	c.lock()
	defer c.unlock()
	return pipeErr(c.s.SetQualityBand(int64(p.id), b.core()))
}

// SetCapacityFloor sets the capacity floor of the outbound direction.
func (c *Conn) SetCapacityFloor(f CapacityFloor) {
	c.lock()
	defer c.unlock()
	c.s.SetCapacityFloor(core.CapacityFloor{Rate: f.Rate, RateRatio: f.RateRatio})
}
