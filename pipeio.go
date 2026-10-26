package zmux

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"
)

// Framing is a carrier's record framing (SPEC.md 3.1, 3.4).
type Framing uint8

const (
	// ByteStream: records are concatenated with their Size fields.
	ByteStream Framing = iota
	// Records: one message per record, without Size (WebSocket); the
	// halves implement MessageReader and MessageWriter.
	Records
)

// MessageReader is the receiving half of a message-framed carrier.
type MessageReader interface {
	// ReadMessage returns the next whole message.
	ReadMessage() ([]byte, error)
}

// MessageWriter is the sending half of a message-framed carrier.
type MessageWriter interface {
	// WriteMessage writes one whole message.
	WriteMessage(p []byte) error
}

// PipeIO is a pipe input: at most one receiving half and at most one
// sending half, at least one of them, each closed by zmux exactly once
// (API.md 4.1). Optional capabilities are discovered on the halves:
//
//   - interface{ Abort(error) } on W: used instead of Close when a pipe fails;
//   - interface{ CloseWrite() error } on W: graceful end of the write side;
//   - interface{ ConnectionState() tls.ConnectionState }: a TLS carrier
//     (records built at 16382 bytes, exporter for PipeAuth.TLS);
//   - interface{ CarrierConn() any } and interface{ OpenLane(ctx) (io.WriteCloser, error) }: lanes;
//   - interface{ RestartEarly(ctx) (io.ReadWriteCloser, error) }: carrier early data.
//
// A half's Close must interrupt a blocked Read or Write.
type PipeIO struct {
	R       io.ReadCloser
	W       io.WriteCloser
	Framing Framing
	// Label names the pipe in statistics.
	Label string
	// FateGroup is the carrier's own fate group (a carrier pool's pipes on
	// one connection share one), used when PipeOptions.FateGroup is 0.
	FateGroup uint64
}

// NetPipe returns the PipeIO of a bidirectional net.Conn (TCP, TLS, Unix,
// or any net.Conn view such as a WebSocket library's).
func NetPipe(c net.Conn) PipeIO { return PipeIO{R: c, W: c} }

type tlsStater interface {
	ConnectionState() tls.ConnectionState
}

type closeWriter interface{ CloseWrite() error }

type aborter interface{ Abort(error) }

type laneOpener interface {
	OpenLane(ctx context.Context) (io.WriteCloser, error)
}

type carrierConner interface{ CarrierConn() any }

// halfCloser closes the halves of a carrier exactly once each, also when R
// and W are the same object.
type halfCloser struct {
	io      PipeIO
	same    bool
	rOnce   sync.Once
	wOnce   sync.Once
	allOnce sync.Once
}

func newHalfCloser(p PipeIO) *halfCloser {
	same := p.R != nil && p.W != nil && any(p.R) == any(p.W)
	return &halfCloser{io: p, same: same}
}

// closeRead closes the receiving half (after its end, or at CLOSED).
func (h *halfCloser) closeRead() {
	if h.io.R == nil {
		return
	}
	if h.same {
		return // closed with the write half or by closeAll
	}
	h.rOnce.Do(func() { _ = h.io.R.Close() })
}

// shutdownWrite ends the write side gracefully.
func (h *halfCloser) shutdownWrite() {
	if h.io.W == nil {
		return
	}
	h.wOnce.Do(func() {
		if cw, ok := h.io.W.(closeWriter); ok {
			_ = cw.CloseWrite()
			return
		}
		if !h.same {
			_ = h.io.W.Close()
		}
	})
}

// closeAll closes every half; abort uses Abort where offered and closes the
// lowest connection of a TLS carrier so that a blocked write is interrupted.
func (h *halfCloser) closeAll(abort bool, cause error) {
	h.allOnce.Do(func() {
		if abort {
			if a, ok := h.io.W.(aborter); ok {
				a.Abort(cause)
			} else if tc, ok := h.io.W.(*tls.Conn); ok {
				_ = tc.NetConn().Close()
			}
		}
		if h.io.W != nil {
			_ = h.io.W.Close()
		}
		if h.io.R != nil && !h.same {
			_ = h.io.R.Close()
		}
	})
}

func carrierIsTLS(p PipeIO) (tls.ConnectionState, bool) {
	for _, x := range []any{p.W, p.R} {
		if s, ok := x.(tlsStater); ok {
			return s.ConnectionState(), true
		}
	}
	return tls.ConnectionState{}, false
}
