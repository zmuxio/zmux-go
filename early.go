package zmux

import (
	"context"
	"io"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// Carrier early data (TLS 1.3 early data, QUIC 0-RTT; SPEC.md 6.1.2). A client that remembers its peer's parameters from an
// earlier session sends its first streams in the carrier's early data; the
// carrier reports whether the server accepted them, and on rejection the
// session restarts the bootstrap pipe on the byte stream the adapter's
// restart hook returns.

// EarlyClientCarrier is the optional capability of a client carrier that
// sends early data. The halves of the bootstrap PipeIO implement it.
type EarlyClientCarrier interface {
	// EarlyAccepted blocks until the handshake decided: true when the
	// server accepted the early data.
	EarlyAccepted(ctx context.Context) (bool, error)
	// RestartEarly returns the pipe's byte stream as it stands after the
	// handshake (on QUIC the bootstrap stream opened again).
	RestartEarly(ctx context.Context) (io.ReadWriteCloser, error)
}

// EarlyServerCarrier is the optional capability of a server carrier that
// accepted early data: streams opened in it are replayable until the
// handshake completes.
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
	if ec, ok := p.W.(EarlyClientCarrier); ok && c.cfg.EarlyData.Replayable && len(c.cfg.EarlyData.Remembered) > 0 {
		c.wg.Add(1)
		go c.earlyVerdict(ec)
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

// earlyVerdict applies the carrier's verdict; on rejection the restart
// hook is called first, and a hook that fails fails the session.
func (c *Conn) earlyVerdict(ec EarlyClientCarrier) {
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
	rw, err := ec.RestartEarly(c.ctx)
	if err != nil {
		c.lock()
		c.s.Close(uint64(TransportInternalError), []byte("early data restart failed"))
		c.unlock()
		return
	}
	p := PipeIO{R: rw, W: rw}
	c.lock()
	defer c.unlock()
	if c.closing {
		_ = rw.Close()
		return
	}
	pd := c.newPipeDrv(0, p)
	info := carrierInfo(p, pd.tcp, false)
	if !c.s.RestartEarly(info, c.clk.now()) {
		// The verdict contradicts what the early carrier delivered (the
		// peer's parameters arrived on it): fail rather than mix carriers.
		_ = rw.Close()
		c.s.Close(uint64(TransportInternalError), []byte("early data rejected after the peer's parameters"))
		return
	}
	old := c.pipes[0]
	old.dead = true // its goroutines no longer report to the core
	c.s.SetWriterSignal(0, pd.wsig)
	c.orphanSig.Store(pd.wsig)
	c.startPipeLocked(pd, true, true)
	old.wsig.Fire()       // its writer sees it is dead and ends
	go old.closeAll(true) // its reader ends with its carrier
}
