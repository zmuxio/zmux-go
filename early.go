package zmux

import (
	"context"
	"errors"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// Carrier early data (TLS 1.3 early data, QUIC 0-RTT, a transform's first
// flight; SPEC.md 6.1.2). A client with early data (EarlyData.ZmuxPeer, or
// Remembered with Replayable) whose bootstrap carrier can tell whether the
// server accepted its early data sends its first streams in it; the
// carrier reports the verdict, and on rejection the session restarts the
// bootstrap pipe on the pipe the carrier's restart hook returns.

// EarlyClientCarrier is the optional capability of a client carrier that
// may send early data, on the W half of the bootstrap PipeIO. When
// SendsEarlyData reports true, the session's early sends stay provisional
// until EarlyAccepted returns.
type EarlyClientCarrier interface {
	// SendsEarlyData reports whether this pipe's initial bytes go out as
	// carrier early data, which the server may refuse or see replayed; it
	// must not change for the pipe. A carrier that sends them only after
	// its handshake (QUIC without 0-RTT) returns false: floor early data
	// (EarlyData.ZmuxPeer) then neither waits for a verdict nor needs
	// Replayable. Remembered parameters with Replayable wait for
	// EarlyAccepted either way.
	SendsEarlyData() bool
	// EarlyAccepted waits for the carrier handshake's verdict for this pipe's
	// initial bytes. Return true if they remain usable, including bytes sent
	// only after the handshake. Return false only when those bytes were
	// invalidated and RestartEarly must supply their replacement stream.
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
type EarlyServerCarrier interface {
	UsedEarlyData() bool
	HandshakeComplete() <-chan struct{}
}

// RememberedParameters returns the peer's transport parameters for a later
// session's carrier early data (EarlyData.Remembered); store them only with
// the application's consent, per peer.
func (c *Conn) RememberedParameters() []byte {
	c.lock()
	defer c.unlock()
	p, ok := c.s.PeerParams()
	if !ok {
		return nil
	}
	return wire.AppendParams(nil, p)
}

func decodeRemembered(b []byte) *wire.Params {
	if len(b) == 0 {
		return nil
	}
	var p wire.Params
	if wire.DecodeParams(b, &p) != nil {
		return nil
	}
	return &p
}

// watchEarly follows the bootstrap carrier's early data (after Start).
func (c *Conn) watchEarly(p PipeIO) {
	if ec, ok := earlyCarrier(p, &c.cfg.EarlyData); ok {
		c.wg.Add(1)
		go c.earlyVerdict(ec, p)
	}
	if es, ok := p.W.(EarlyServerCarrier); ok && c.cfg.EarlyData.Replayable && es.UsedEarlyData() {
		c.lock()
		c.s.CarrierEarlyIn()
		c.unlock()
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			select {
			case <-es.HandshakeComplete():
				c.lock()
				c.s.CarrierHandshakeDone()
				c.unlock()
			case <-c.done:
			}
		}()
	}
}

// earlyCarrier is the bootstrap carrier's verdict hook when the session's
// early data waits for it, on a carrier with EarlyClientCarrier: a client
// with Remembered parameters and Replayable (carrier early data by
// definition), or with floor early data on a carrier that sends early data
// this time. The order is the core's (Remembered first).
func earlyCarrier(p PipeIO, e *EarlyData) (EarlyClientCarrier, bool) {
	ec, ok := p.W.(EarlyClientCarrier)
	switch {
	case !ok:
		return nil, false
	case e.Replayable && len(e.Remembered) > 0:
		return ec, true
	case e.ZmuxPeer && ec.SendsEarlyData():
		return ec, true
	}
	return nil, false
}

// failRestartLocked closes the session after a failed early-data restart
// and starts the stopped bootstrap writer again, so that the
// CONNECTION_CLOSE reaches the peer on the carrier it has.
func (c *Conn) failRestartLocked(old *pipeDrv, reason string) {
	c.s.EarlyDiscarded()
	c.s.Close(uint64(TransportInternalError), []byte(reason))
	if old != nil && !old.dead && c.pipes[0] == old && old.wstop {
		c.startWriterLocked(old)
	}
}

// earlyVerdict applies the carrier's verdict; on rejection the restart
// hook is called first, and a hook that fails fails the session. boot is
// the bootstrap pipe.
func (c *Conn) earlyVerdict(ec EarlyClientCarrier, boot PipeIO) {
	defer c.wg.Done()
	ok, err := ec.EarlyAccepted(c.ctx)
	if err != nil {
		return // the carrier failed: the pipe's own end reports it
	}
	if ok {
		c.lock()
		c.s.CarrierEarlyAccepted()
		c.unlock()
		return
	}
	// What the carrier is given from here on follows the restart: the
	// bootstrap writer stops before RestartEarly is called.
	c.lock()
	old := c.pipes[0]
	var wdone chan struct{}
	if old != nil && !old.dead {
		old.wstop = true
		wdone = old.wdone
		old.wsig.Fire()
	}
	c.unlock()
	if wdone != nil {
		select {
		case <-wdone:
		case <-c.done:
			return
		}
	}
	p, err := ec.RestartEarly(c.ctx)
	if err == nil && p.R == nil && p.W == nil {
		err = errors.New("no pipe halves")
	}
	if err == nil {
		err = p.checkOverhead()
		if err != nil {
			closePipeIO(p)
		}
	}
	if err != nil {
		c.lock()
		c.failRestartLocked(old, "early data restart failed")
		c.unlock()
		return
	}
	p.Carrier, p.Label, p.FateGroup, p.Variant = boot.Carrier, boot.Label, boot.FateGroup, boot.Variant
	sameR, sameW := any(p.R) == any(boot.R), any(p.W) == any(boot.W)
	c.lock()
	defer c.unlock()
	if c.closing || old == nil || c.pipes[0] != old {
		if !sameR && !sameW {
			closePipeIO(p)
		}
		return
	}
	if sameR || sameW {
		// The same carrier past its handshake: its reader goes on, its
		// writer starts again on the restarted session.
		if !sameR || !sameW || p.Framing != boot.Framing || !c.s.RestartEarly(carrierInfo(p, old.tcp, false), c.clk.now()) {
			c.failRestartLocked(old, "early data restart failed")
			return
		}
		c.startWriterLocked(old)
		return
	}
	pd := c.newPipeDrv(0, p)
	info := carrierInfo(p, pd.tcp, false)
	if !c.s.RestartEarly(info, c.clk.now()) {
		// The verdict contradicts what the early carrier delivered (the
		// peer's parameters arrived on it): fail rather than mix carriers.
		closePipeIO(p)
		c.failRestartLocked(old, "early data rejected after the peer's parameters")
		return
	}
	old.dead = true // its goroutines no longer report to the core
	c.s.SetWriterSignal(0, pd.wsig)
	c.orphanSig.Store(pd.wsig)
	c.startPipeLocked(pd, true, true)
	old.wsig.Fire()   // its writer sees it is dead and ends
	go old.closeAll() // its reader ends with its carrier
}
