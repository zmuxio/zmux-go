package zmux

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

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

// ForceEpochAdvance advances the epoch as AdvanceEpoch does and then replaces
// every carrier the session had, through Config.Redial, within deadline: a new
// pipe joins with the new epoch and the old one is removed gracefully. It
// serves only a suspected leak of a session-level secret; ordinary rotation is
// AdvanceEpoch or the epoch policy. Without Config.Redial it is
// ErrInvalidConfig.
func (c *Conn) ForceEpochAdvance(deadline time.Duration) error {
	if c == nil {
		return net.ErrClosed
	}
	if c.cfg.Redial == nil {
		return fmt.Errorf("%w: ForceEpochAdvance without Config.Redial", ErrInvalidConfig)
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	old := c.Pipes()
	start := c.Epoch()
	if err := c.AdvanceEpoch(); err != nil {
		return err
	}
	if err := c.waitEpoch(ctx, start+1); err != nil {
		return err
	}
	for _, p := range old {
		dir := DirBidirectional
		if st, ok := p.Stats(); ok {
			switch {
			case st.Inbound == InboundNone:
				dir = DirSendOnly
			case st.Outbound == OutboundNone:
				dir = DirReceiveOnly
			}
		}
		io, err := c.cfg.Redial(ctx, dir)
		if err != nil {
			return fmt.Errorf("zmux: ForceEpochAdvance: redial: %w", err)
		}
		if _, err := c.AddPipe(ctx, io, PipeOptions{Direction: dir}); err != nil {
			return fmt.Errorf("zmux: ForceEpochAdvance: %w", err)
		}
		_ = p.Close() // a pipe already gone needs no replacement
	}
	return nil
}

// waitEpoch waits until new joins use epoch n.
func (c *Conn) waitEpoch(ctx context.Context, n uint64) error {
	sig := core.NewSignal()
	defer c.unwatchPipes(sig)
	for {
		c.lock()
		if c.s.Epoch() >= n {
			c.unlock()
			return nil
		}
		if c.closing {
			c.unlock()
			return net.ErrClosed
		}
		c.watchPipes(sig)
		c.unlock()
		select {
		case <-sig.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
