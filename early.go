package zmux

import (
	"context"
	"errors"
	"io"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// EarlyClientCarrier is a carrier that reports the verdict on its 0-RTT
// attempt, and can restart the bootstrap if the carrier rejected it.
type EarlyClientCarrier interface {
	// SendsEarlyData reports whether the initial bytes go out as carrier 0-RTT.
	SendsEarlyData() bool
	// EarlyAccepted waits for the carrier handshake's verdict for this pipe's
	// initial bytes. Return true if they remain usable, including bytes sent
	// only after the handshake. Return false only when those bytes were
	// invalidated and RestartEarly must supply their replacement stream.
	// What the pipe reads reaches the session only after the verdict has
	// been applied, so the verdict must not depend on the session; a read
	// half may itself deliver it, as long as it returns no bytes before.
	EarlyAccepted(ctx context.Context) (bool, error)
	// RestartEarly returns the pipe as it stands after the handshake: new
	// halves, with their Framing, WriteOverhead and capabilities (on QUIC
	// the bootstrap stream opened again; the bootstrap halves are closed),
	// or the bootstrap halves themselves, with the same Framing (a layered
	// carrier past its handshake, which discarded what it held; the
	// session goes on with them). Halves of both kinds fail the session.
	// The session sets Carrier, Label, FateGroup and Variant from the
	// bootstrap pipe.
	RestartEarly(ctx context.Context) (PipeIO, error)
}

// EarlyServerCarrier is the optional capability of a server carrier that
// accepted early data, on the W half: with EarlyData.Replayable, streams
// opened before HandshakeComplete is closed report Stream.Replayable.
//
// A carrier may add HandshakeFailed() <-chan struct{}, closed when the
// handshake can no longer complete: a reader waiting for it then ends
// at once rather than at Config.HandshakeTimeout.
type EarlyServerCarrier interface {
	UsedEarlyData() bool
	HandshakeComplete() <-chan struct{}
}

// RememberedParameters returns the plain QMux peer's parameters for a
// later carrier resumption. Store them with that resumption context.
func (c *Conn) RememberedParameters() []byte {
	if c == nil {
		return nil
	}
	c.lock()
	defer c.unlock()
	p, ok := c.s.PeerParams()
	if !ok || p.IsZmux() {
		return nil
	}
	b, err := wire.AppendParams(nil, p)
	if err != nil {
		return nil
	}
	return b
}

func decodeRemembered(b []byte) *wire.Params {
	if len(b) == 0 {
		return nil
	}
	var p wire.Params
	if wire.DecodeParams(b, &p) != nil || p.IsZmux() {
		return nil
	}
	return &p
}

// watchEarly follows the bootstrap carrier's early data (after Start).
func (c *Conn) watchEarly(p PipeIO) {
	if ec, ok := earlyCarrier(p, &c.cfg.EarlyData); ok {
		c.side.add()
		labels.Go(c.lctx, "early-verdict", func() { c.earlyVerdict(ec, p) })
	}
	if es, ok := earlyIn(p); ok {
		c.side.add()
		labels.Go(c.lctx, "early-watch", func() {
			defer c.side.done()
			select {
			case <-es.HandshakeComplete():
				c.locked(func() {
					c.s.CarrierHandshakeDone()
				})
			case <-c.done:
			}
		})
	}
}

// earlyIn is the bootstrap carrier of a server whose streams arriving in
// carrier early data are replayable. The session is marked (CarrierEarlyIn)
// before the bootstrap reader starts, so no early stream is missed.
func earlyIn(p PipeIO) (EarlyServerCarrier, bool) {
	es, ok := p.W.(EarlyServerCarrier)
	return es, ok && es.UsedEarlyData()
}

// earlyCarrier returns the verdict source for a plain QMux 0-RTT attempt.
func earlyCarrier(p PipeIO, e *EarlyData) (EarlyClientCarrier, bool) {
	ec, ok := p.W.(EarlyClientCarrier)
	return ec, ok && e.PlainQMux && e.Replayable && ec.SendsEarlyData() && decodeRemembered(e.Remembered) != nil
}

// failRestartLocked closes the session after a failed early-data restart
// and starts the stopped bootstrap writer again, so that the
// CONNECTION_CLOSE reaches the peer on the carrier it has.
func (c *Conn) failRestartLocked(old *pipeDrv, reason string) {
	c.s.EarlyDiscarded()
	c.s.FailInternal(reason)
	if old != nil && !old.dead && c.pipes[0] == old {
		old.wstop = false
		c.queuePipeWriterLocked(old)
	}
}

// awaitVerdict holds the bootstrap carrier's first bytes until the verdict
// has been applied. A server speaks as soon as its carrier decided, so the
// reader can have the peer's parameters while earlyVerdict has yet to take
// the lock; delivered first, they would end the session's chance to restart
// and hide that the peer never saw this side's parameters.
func (c *Conn) awaitVerdict(pd *pipeDrv, lane uint64) {
	if pd.id != 0 || lane != 0 || c.verdictDone == nil {
		return
	}
	select {
	case <-c.verdictDone:
	case <-c.done:
	}
}

// earlyVerdict applies the carrier's verdict; on rejection the restart
// hook is called first, and a failing hook fails the session. boot is
// the bootstrap pipe.
func (c *Conn) earlyVerdict(ec EarlyClientCarrier, boot PipeIO) {
	defer c.side.done()
	defer close(c.verdictDone)
	var ok bool
	var err error
	if !safeLog(c.cfg.Logger, "EarlyClientCarrier.EarlyAccepted", func() { ok, err = ec.EarlyAccepted(c.ctx) }) {
		c.locked(func() { c.s.FailInternal("the early carrier's verdict panicked") })
		return
	}
	if err != nil {
		return // the carrier failed: the pipe's own end reports it
	}
	if ok {
		c.locked(func() {
			c.s.CarrierEarlyAccepted()
		})
		return
	}
	// What the carrier is given from here on follows the restart: the
	// bootstrap writer stops before RestartEarly is called.
	c.lock()
	old := c.pipes[0]
	var wdone chan struct{}
	if old != nil && !old.dead {
		old.wstop = true
		old.writer.wake.FireLocked()
		if old.writer.running {
			old.wdone = make(chan struct{})
			wdone = old.wdone
		}
	}
	c.unlock()
	if wdone != nil {
		select {
		case <-wdone:
		case <-c.done:
			return
		}
	}
	var p PipeIO
	err = hostCall(c.cfg.Logger, "EarlyClientCarrier.RestartEarly", func() (err error) {
		p, err = ec.RestartEarly(c.ctx)
		return err
	}) // a failure, a panic included, fails the session below
	if err == nil && p.R == nil && p.W == nil {
		err = errors.New("no pipe halves")
	}
	if err == nil {
		// The restart's closer owns the halves of p that are not the boot
		// carrier's (a restart that returned one new half and one old
		// included); the boot carrier stays its pipe's.
		var r, w io.Closer
		if p.R != nil && !carrierio.SameHalf(p.R, boot.R) {
			r = p.R
		}
		if p.W != nil && !carrierio.SameHalf(p.W, boot.W) {
			w = p.W
		}
		p.own = carrierio.NewCloser(r, w)
		err = p.validate()
		if err != nil {
			closePipe(p)
		}
	}
	if err != nil {
		c.locked(func() {
			c.failRestartLocked(old, "early data restart failed")
		})
		return
	}
	p.Carrier, p.Label, p.FateGroup, p.Variant = boot.Carrier, boot.Label, boot.FateGroup, boot.Variant
	sameR, sameW := carrierio.SameHalf(p.R, boot.R), carrierio.SameHalf(p.W, boot.W)
	a, err := c.preparePipe(0, p, p, 0)
	if err != nil {
		closePipe(p)
		c.locked(func() { c.failRestartLocked(old, "early carrier properties") })
		return
	}
	pd, info := a.pd, a.info
	flight, err := c.reserveReceiveFlight(p.R)
	if err != nil {
		closePipe(p)
		c.locked(func() { c.failRestartLocked(old, "early carrier memory") })
		return
	}
	defer func() { flight.release() }()
	var discard bool
	defer func() {
		if discard {
			closePipe(p)
		}
	}()
	c.lock()
	defer c.unlock()
	if c.closing || old == nil || old.retiring || c.pipes[0] != old {
		discard = true
		return
	}
	if sameR || sameW {
		// The same carrier past its handshake: its reader goes on, its
		// writer starts again on the restarted session.
		if !sameR || !sameW || p.Framing != boot.Framing || !c.s.RestartEarly(info, &old.writer.poll, false, c.clk.now()) {
			discard = true
			c.failRestartLocked(old, "early data restart failed")
			return
		}
		old.wstop = false
		c.queuePipeWriterLocked(old)
		return
	}
	if acct := old.end.accounting; acct != nil && acct.checkSources(p.ReceiveSource, p.SendSource, true, true) != nil {
		discard = true
		c.failRestartLocked(old, "early carrier accounting sources")
		return
	}
	if !c.s.CanAcquireCarriers(pd.physical) {
		discard = true
		c.failRestartLocked(old, "early carrier capacity exhausted")
		return
	}
	if !c.s.RestartEarly(info, &pd.writer.poll, true, c.clk.now()) {
		// The verdict contradicts what the early carrier delivered (the
		// peer's parameters arrived on it): fail rather than mix carriers.
		discard = true
		c.failRestartLocked(old, "early data rejected after the peer's parameters")
		return
	}
	old.dead = true // its goroutines no longer report to the core
	old.accountSegment.retire()
	pd.end = old.end // a handle of pipe 0 follows the restart
	pd.end.drv.Store(pd)
	c.startPipeLocked(pd, true, true, a.carrier, &flight)
	old.worker("early-close", old.closeAll) // its reader ends with its carrier
}
