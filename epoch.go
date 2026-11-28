package zmux

import "net"

// AdvanceEpoch starts a session epoch exchange at
// once when the session is Stable, or right after the one in progress. The
// exchange is reported by EventEpochCommitted and EventEpochDeleted. It fails
// with ErrNotNegotiated when multipath is not in use and with ErrJoinPending
// before the session has its join keys.
func (c *Conn) AdvanceEpoch() error {
	if c == nil {
		return net.ErrClosed
	}
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	return apiErr(c.s.AdvanceEpoch())
}

// Epoch is the epoch new local joins use.
func (c *Conn) Epoch() uint64 {
	if c == nil {
		return 0
	}
	c.lock()
	defer c.unlock()
	return c.s.Epoch()
}
