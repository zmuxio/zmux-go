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
}

// ShaperFactory returns the Shaper of an outbound pipe; nil: the pipe is
// not shaped. It is called when the pipe's first record is made after the
// factory was installed, and may be called concurrently for different
// sessions.
type ShaperFactory func(ShapedPipe) shaping.Shaper

// coreFactory adapts f to the core, which keeps a pipe's kind opaque.
func coreFactory(f ShaperFactory) func(uint64, uint8, int) shaping.Shaper {
	if f == nil {
		return nil
	}
	return func(id uint64, kind uint8, overhead int) shaping.Shaper {
		return f(ShapedPipe{ID: id, Carrier: CarrierKind(kind), WriteOverhead: overhead})
	}
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
	c.s.SetShaper(coreFactory(f))
}

// SetPipeShaper installs s on one outbound pipe until the next SetShaper;
// nil: the pipe is not shaped. It reports whether the pipe exists.
func (c *Conn) SetPipeShaper(pipe uint64, s shaping.Shaper) bool {
	c.lock()
	defer c.unlock()
	return c.s.SetPipeShaper(pipe, s)
}

// ShapingStats returns the shaping counters.
func (c *Conn) ShapingStats() shaping.Stats {
	c.lock()
	defer c.unlock()
	return c.s.ShapingStats()
}
