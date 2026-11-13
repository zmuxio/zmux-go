package zmux

import (
	"context"
	"fmt"
	"net"

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

// ReplacePipes advances the epoch as AdvanceEpoch does, waits until new
// joins use the new epoch, and then replaces every pipe the session had
// when it was called: for each, it dials a carrier of the same direction
// through Config.Redial, joins it, and closes the old pipe gracefully once
// the new one is attached (make before break). It serves only a suspected
// leak of a session-level secret; ordinary rotation is AdvanceEpoch or the
// epoch policy. Without Config.Redial it is ErrInvalidConfig.
//
// ctx bounds the whole call: when it ends, the call fails with ctx's error,
// the pipes already replaced stay replaced and the old pipes not yet
// replaced are kept. A dial or join that fails ends the call with its error
// on the same terms. While the previous epoch is still kept, the next offer
// waits until it is deleted, so a ctx shorter than that wait fails the
// call before any pipe is replaced.
func (c *Conn) ReplacePipes(ctx context.Context) error {
	if c == nil {
		return net.ErrClosed
	}
	if c.cfg.Redial == nil {
		return fmt.Errorf("%w: ReplacePipes without Config.Redial", ErrInvalidConfig)
	}
	old := c.Pipes()
	start := c.Epoch()
	if err := c.AdvanceEpoch(); err != nil {
		return err
	}
	if err := c.waitEpoch(ctx, start+1); err != nil {
		return err
	}
	for _, p := range old {
		dir := DirectionBidirectional
		if st, ok := p.Stats(); ok {
			switch {
			case st.Inbound == InboundNone:
				dir = DirectionSendOnly
			case st.Outbound == OutboundNone:
				dir = DirectionReceiveOnly
			}
		}
		io, err := c.cfg.Redial(ctx, dir)
		if err != nil {
			return fmt.Errorf("zmux: ReplacePipes: redial: %w", err)
		}
		if _, err := c.AddPipe(ctx, io, PipeOptions{Direction: dir}); err != nil {
			return fmt.Errorf("zmux: ReplacePipes: %w", err)
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
