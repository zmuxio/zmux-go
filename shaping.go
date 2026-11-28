package zmux

import (
	"fmt"
	"net"

	"github.com/zmuxio/zmux-go/shaping"
)

// ShapedPipe is the outbound pipe a ShaperFactory makes a Shaper for.
type ShapedPipe struct {
	// Variant is the pipe's PipeIO.Variant, so that a factory can choose a
	// policy per transform.
	Variant string
	// PipeID is the pipe ID.
	PipeID PipeID
	// WriteOverhead is the pipe's PipeIO.WriteOverhead: the fixed number
	// of bytes each carrier write adds below the pipe.
	WriteOverhead int
	// Carrier is the pipe's carrier kind (PipeIO.Carrier); 0 for a carrier
	// of no listed kind, such as raw TCP or TLS.
	Carrier CarrierKind
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
		return f(ShapedPipe{PipeID: PipeID(id), Carrier: CarrierKind(kind), WriteOverhead: overhead, Variant: c.variantLocked(id)})
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

// ShapingPolicy selects all shaping behavior for one outbound direction.
// Factory can select machines by carrier or pipe. Budget is drawn once per
// installation outside the session lock.
type ShapingPolicy struct {
	Factory   ShaperFactory
	Handshake shaping.HandshakeShaper
	Budget    func() shaping.Budget
}

// SetShaper replaces the entire direction policy. Nil disables it.
func (c *Conn) SetShaper(policy *ShapingPolicy) error {
	if c == nil {
		return net.ErrClosed
	}
	b, err := policy.resolve(c.cfg.Logger)
	if err != nil {
		return err
	}
	var next ShapingPolicy
	if policy != nil {
		next = *policy
	}
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	if !c.s.ConfigureShaper(c.coreFactory(next.Factory), b) {
		return fmt.Errorf("%w: invalid shaping policy", ErrInvalidConfig)
	}
	c.handshakeShaper = next.Handshake
	c.invalidateHandshakePlans()
	return nil
}

func (c *Conn) invalidateHandshakePlans() {
	if c.handshakeChange != nil {
		close(c.handshakeChange)
		c.handshakeChange = nil
	}
}

// SetShapingLimits replaces the session's Config.ShapingLimits, tighter or
// looser; they are never sent. The account's spending carries over. An invalid
// l fails with ErrInvalidConfig and changes nothing.
func (c *Conn) SetShapingLimits(l shaping.Limits) error {
	if c == nil {
		return net.ErrClosed
	}
	if err := checkLimits(&l); err != nil {
		return err
	}
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	if !c.s.SetShapeLimits(l) {
		return fmt.Errorf("%w: invalid shaping limits", ErrInvalidConfig)
	}
	c.invalidateHandshakePlans()
	return nil
}

// ShapingLimits returns the session's shaping limits.
func (c *Conn) ShapingLimits() shaping.Limits {
	if c == nil {
		return shaping.Limits{}
	}
	c.lock()
	defer c.unlock()
	return c.s.ShapeLimits()
}

// ShapingMeters returns the session's shaping readings.
func (c *Conn) ShapingMeters() shaping.Meters {
	if c == nil {
		return shaping.Meters{}
	}
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	return c.s.ShapeMeters()
}

// ShapingStats returns the shaping counters.
func (c *Conn) ShapingStats() shaping.Stats {
	if c == nil {
		return shaping.Stats{}
	}
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	return c.s.ShapingStats()
}
