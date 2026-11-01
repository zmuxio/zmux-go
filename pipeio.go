package zmux

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// Framing is a carrier's record framing (SPEC.md 3.1, 3.4).
type Framing uint8

const (
	// FramingByteStream concatenates records with their Size fields.
	FramingByteStream Framing = iota
	// FramingRecords carries one message per record, without Size (as on
	// WebSocket); the halves implement MessageReader and MessageWriter.
	FramingRecords
)

// MaxRecordSize is the largest record zmux accepts (its max_record_size):
// with FramingRecords, the largest message a carrier needs to deliver.
const MaxRecordSize = wire.MaxRecordSize

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

// BuffersWriter is an optional capability of a byte-stream sending half
// that takes a batch of buffers in one call, as net.Buffers does for a
// net.Conn: one flush or one locked copy per batch instead of one per
// buffer.
type BuffersWriter interface {
	// WriteBuffers writes bufs in order, as one Write of their
	// concatenation would. It may consume bufs (advance or empty its
	// elements) but retains neither bufs nor their contents once it
	// returns.
	WriteBuffers(bufs [][]byte) (int, error)
}

var flatPool = sync.Pool{New: func() any { return new([]byte) }}

// WriteFlat writes bufs to w with one Write of their concatenation, copied
// into a pooled buffer: one call per batch for a writer whose every call
// costs a record or a locked hand-over (TLS, a QUIC stream). A
// BuffersWriter may implement WriteBuffers with it.
func WriteFlat(w io.Writer, bufs [][]byte) (int, error) {
	if len(bufs) == 1 {
		return w.Write(bufs[0])
	}
	bp := flatPool.Get().(*[]byte)
	b := (*bp)[:0]
	for _, p := range bufs {
		b = append(b, p...)
	}
	n, err := w.Write(b) // w retains nothing of b once it returns
	*bp = b[:0]
	flatPool.Put(bp)
	return n, err
}

// PipeIO is a pipe input: at most one receiving half and at most one
// sending half, at least one of them, each closed by zmux exactly once.
// Optional capabilities are discovered on the halves:
//
//   - interface{ Abort(error) } on W: used instead of Close when a pipe fails;
//   - BuffersWriter on W: a batch of records in one call;
//   - interface{ CloseWrite() error } on W: graceful end of the write side;
//   - interface{ ConnectionState() tls.ConnectionState }: the TLS that the
//     carrier runs over and that this process terminates, for the exporters
//     of PipeAuth.TLS and PipeOptions.CarrierBound and CarrierInfo.TLS; a
//     state without HandshakeComplete is no TLS, so that these fail
//     (records are built at 16382 bytes on a *tls.Conn);
//   - interface{ CarrierConn() any } and interface{ OpenLane(ctx) (io.WriteCloser, error) }: lanes;
//   - interface{ RestartEarly(ctx) (io.ReadWriteCloser, error) }: carrier early data.
//   - DatagramChannel: the carrier's datagram channel (EXTENSIONS.md 6.8).
//   - interface{ Unsent() (uint64, bool) } on W, and on a lane's writer:
//     the bytes write calls took that the carrier has not yet transmitted,
//     false when it cannot tell; queried by the driver outside the session
//     lock before each write (PipeStats.QueuedBytes, the scheduler's Q_p).
//     A TCP connection, or a TLS one over it, reports the socket's
//     unsent bytes where the platform has a query (Linux SIOCOUTQNSD)
//     without it.
//   - interface{ Retransmitted() (uint64, bool) } on W: the bytes the
//     carrier has retransmitted, false when it cannot tell; queried as
//     Unsent is, at most once a second (PipeStats.BytesRetransmitted,
//     LossRate). A TCP connection, or a TLS one over it, reports the
//     socket's count without it (Linux tcpi_bytes_retrans, Darwin
//     tcpi_txretransmitbytes, Windows SIO_TCP_INFO BytesRetrans, a
//     32-bit count widened to 64 bits).
//
// A half's Close must interrupt a blocked Read or Write.
type PipeIO struct {
	R       io.ReadCloser
	W       io.WriteCloser
	Framing Framing
	// Carrier is the carrier's kind, which the adapters set on both ends
	// and a Candidate's dial sets to its Kind; 0 for a carrier of no listed
	// kind, such as raw TCP or TLS. The session reports it to its
	// ShaperFactory, in pipe events and in PipeStats.
	Carrier CarrierKind
	// Label names the pipe in statistics.
	Label string
	// FateGroup is the carrier's own fate group (a carrier pool's pipes on
	// one connection share one), used when PipeOptions.FateGroup is 0.
	FateGroup uint64
	// WriteOverhead is the fixed number of bytes that each write call on
	// the carrier adds below it, a hint passed to shaping
	// (ShapedPipe.WriteOverhead) and not checked by zmux; 0: none. A value
	// outside [0, MaxWriteOverhead] is ErrInvalidConfig where the PipeIO
	// is used (Start, AddPipe, the listener's HandlePipe and WrapPipe's
	// result). WrapPipe's result keeps the WriteOverhead it sets.
	WriteOverhead int
}

// MaxWriteOverhead is the largest PipeIO.WriteOverhead.
const MaxWriteOverhead = 65535

// checkOverhead reports a WriteOverhead outside [0, MaxWriteOverhead].
func (p PipeIO) checkOverhead() error {
	if p.WriteOverhead < 0 || p.WriteOverhead > MaxWriteOverhead {
		return fmt.Errorf("%w: PipeIO.WriteOverhead %d outside [0, %d]", ErrInvalidConfig, p.WriteOverhead, MaxWriteOverhead)
	}
	return nil
}

// NetPipe returns the PipeIO of a bidirectional net.Conn (TCP, TLS, Unix,
// or any net.Conn view such as a WebSocket library's).
func NetPipe(c net.Conn) PipeIO { return PipeIO{R: c, W: c} }

type tlsStater interface {
	ConnectionState() tls.ConnectionState
}

type closeWriter interface{ CloseWrite() error }

// unsentReporter is the optional unsent count of a sending half.
type unsentReporter interface{ Unsent() (uint64, bool) }

// retransReporter is the optional retransmission count of a sending half.
type retransReporter interface{ Retransmitted() (uint64, bool) }

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
			if st := s.ConnectionState(); st.HandshakeComplete {
				return st, true
			}
		}
	}
	return tls.ConnectionState{}, false
}
