package zmux

import "github.com/zmuxio/zmux-go/shaping"

// ShapedPipe is the outbound pipe a ShaperFactory makes a Shaper for.
type ShapedPipe struct {
	// ID is the pipe ID.
	ID uint64
	// Carrier is the pipe's carrier kind (PipeIO.Carrier); 0 for a carrier
	// of no listed kind, such as raw TCP or TLS.
	Carrier CarrierKind
	// WriteOverhead is the pipe's PipeIO.WriteOverhead: the fixed number
	// of bytes each carrier write adds below the pipe.
	WriteOverhead int
	// Variant is the pipe's PipeIO.Variant, so that a factory can choose a
	// policy per transform.
	Variant string
}

// ShaperFactory returns the Shaper of an outbound pipe; nil: the pipe is
// not shaped. It is called when the pipe's first record is made after the
// factory was installed, and may be called concurrently for different
// sessions.
type ShaperFactory func(ShapedPipe) shaping.Shaper

// coreFactory adapts f to the core, which keeps a pipe's kind opaque; the
// core calls it under the session lock.
func (c *Conn) coreFactory(f ShaperFactory) func(uint64, uint8, int) shaping.Shaper {
	if f == nil {
		return nil
	}
	return func(id uint64, kind uint8, overhead int) shaping.Shaper {
		return f(ShapedPipe{ID: id, Carrier: CarrierKind(kind), WriteOverhead: overhead, Variant: c.variantLocked(id)})
	}
}

// variantLocked is the PipeIO.Variant of pipe id's carrier ("" for none
// or a reservation without its carrier).
func (c *Conn) variantLocked(id uint64) string {
	if pd := c.pipes[id]; pd != nil {
		return pd.hc.io.Variant
	}
	return ""
}

// SetShaper installs f on every outbound pipe of the session, replacing
// earlier instances; nil: no shaping.
//
// A deadline that a Shaper sets (a delay, a gate) is met precisely: the
// session's timer is armed ahead of it by the host timer's observed
// lateness, at most 2 ms, and its callback then yields in a loop until the
// deadline. Each shaping deadline so costs up to that lead in CPU time on
// one goroutine; a Shaper that sets deadlines at a high rate trades CPU
// for timing precision.
func (c *Conn) SetShaper(f ShaperFactory) {
	c.lock()
	defer c.unlock()
	c.s.SetShaper(c.coreFactory(f))
}

// SetPipeShaper installs s on one outbound pipe until the next SetShaper;
// nil: the pipe is not shaped. It reports whether the pipe exists.
func (c *Conn) SetPipeShaper(pipe uint64, s shaping.Shaper) bool {
	c.lock()
	defer c.unlock()
	return c.s.SetPipeShaper(pipe, s)
}

// SetShapingLimits replaces the session's Config.ShapingLimits: records
// planned from then on follow l, whether shaping is installed yet or not.
// Limits may be tightened or loosened (they bound this endpoint's own
// overhead and are never sent). Padding and pause already spent stay
// counted: past a tightened bound nothing more is padded or paused, and
// nothing already sent is undone. An invalid l fails with
// ErrInvalidConfig and changes nothing.
func (c *Conn) SetShapingLimits(l shaping.Limits) error {
	if err := checkLimits(&l); err != nil {
		return err
	}
	c.lock()
	defer c.unlock()
	c.s.SetShapeLimits(l)
	return nil
}

// ShapingLimits returns the session's shaping limits.
func (c *Conn) ShapingLimits() shaping.Limits {
	c.lock()
	defer c.unlock()
	return c.s.ShapeLimits()
}

// ShapingStats returns the shaping counters.
func (c *Conn) ShapingStats() shaping.Stats {
	c.lock()
	defer c.unlock()
	return c.s.ShapingStats()
}
