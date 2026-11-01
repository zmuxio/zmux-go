package zmux

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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

// MessageLimitReader is an optional capability of a MessageReader:
// ReadMessageLimit returns the next message when it holds at most limit
// bytes, and fails with ErrMessageTooLarge for a longer one after reading
// no more than limit+1 of its bytes into memory (the carrier is then
// unusable). The admission of a listener reads a first message with it
// (Admission.Peek, MaxPeek); without it, the message is read whole, up
// to the carrier's own limit, and then checked.
type MessageLimitReader interface {
	ReadMessageLimit(limit int) ([]byte, error)
}

// MessageWriter is the sending half of a message-framed carrier.
type MessageWriter interface {
	// WriteMessage writes one whole message.
	WriteMessage(p []byte) error
}

// MessageBatchWriter is an optional capability of a message-framed
// sending half: WriteMessages writes msgs in order, as len(msgs)
// WriteMessage calls would, in one call (one flush, one locked hand-over).
// zmux calls it once per output batch, the batch a byte stream gets in one
// Write or WriteBuffers. It retains neither msgs nor their contents once
// it returns. An error means none or some were written; the pipe fails.
type MessageBatchWriter interface {
	WriteMessages(msgs [][]byte) error
}

// ResponseHeaderer is an optional capability of a client half of an HTTP
// carrier: the header of the HTTP response that opened it (a WebSocket
// 101, a streaming or split carrier's response), or of the most recent
// response (an exchange carrier), as a copy; nil before any response. A
// host or transform reads outer metadata from it, such as Date as a clock
// hint; it changes nothing. A transparent transform may forward it.
//
// The header is not authenticated: on plain HTTP or WebSocket anyone on
// the path, and over TLS any proxy that terminates it, can write or alter
// it. Never base authentication or authorization on it; a host that takes
// Date as a clock hint bounds what it accepts (a maximum offset from its
// own clock, never earlier than a time it already trusts).
type ResponseHeaderer interface {
	ResponseHeader() http.Header
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
// Optional capabilities are discovered on the halves (see the interfaces
// below for each, and for the rules a wrapping transform follows):
//
//   - Aborter on W: used before Close when a pipe fails;
//   - BuffersWriter on W: a batch of records in one call;
//   - MessageBatchWriter on a message-framed W: a batch of messages in one call;
//   - CloseWriter on W: graceful end of the write side;
//   - ConnectionStater: the TLS that this process terminates for the
//     carrier (TerminatedTLS, CarrierInfo.TLS); records are built at
//     16382 bytes on a *tls.Conn;
//   - KeyingMaterialExporter: the exporter of the innermost end-to-end
//     handshake, for PipeAuth.Exporter and PipeOptions.CarrierBound
//     (Exporter, which falls back to a terminated TLS state);
//   - Handshaker: a handshake the listener runs before admission;
//   - CarrierConner and LaneOpener: lanes;
//   - EarlyClientCarrier and EarlyServerCarrier on W: carrier early data;
//   - DatagramChannel: the carrier's datagram channel (EXTENSIONS.md 6.8);
//   - UnsentReporter on W and on a lane's writer: bytes not yet
//     transmitted (a TCP connection, or a TLS one over it, reports the
//     socket's where the platform has a query, Linux SIOCOUTQNSD);
//   - RetransReporter on W: bytes retransmitted (a TCP connection, or a
//     TLS one over it, reports the socket's: Linux tcpi_bytes_retrans,
//     Darwin tcpi_txretransmitbytes, Windows SIO_TCP_INFO BytesRetrans,
//     a 32-bit count widened to 64 bits).
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
	// Variant is a short host label (at most 32 bytes advised) for the
	// transform the carrier runs, "" for none: set by a Candidate
	// (Candidate.Variant), an admission (Decision.Variant) or WrapPipe.
	// zmux only reports it, in pipe events, PipeStats, ShapedPipe and
	// CarrierCache keys; the host bounds the number of variants.
	Variant string
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

// The optional capabilities of a PipeIO half. zmux looks for them on the
// halves it is given and never unwraps a half. Their methods may be called
// concurrently with Read and Write; an implementation is safe for that.
//
// A transform that wraps a lower carrier (WrapPipe, a Candidate's Dial)
// offers a lower capability only where its meaning survives the transform:
// CloseWriter and Aborter may be forwarded; UnsentReporter and
// RetransReporter count lower bytes, so a transform that changes lengths
// does not forward them (or converts them); ConnectionStater is forwarded
// only by a transparent transform (such as compression), never by one
// that terminates its own handshake, which reports its own state or none,
// so that TerminatedTLS and Exporter describe the innermost handshake;
// LaneOpener is offered only when every lane can be wrapped alike.

// ConnectionStater is a half whose TLS (or QUIC) handshake this process
// runs: the state behind TerminatedTLS and CarrierInfo.TLS. A state
// without HandshakeComplete counts as no TLS.
type ConnectionStater interface {
	ConnectionState() tls.ConnectionState
}

// Handshaker is a half whose own handshake may not have run yet: the
// listener calls HandshakeContext within Config.HandshakeTimeout before
// admission. It returns nil at once when the handshake has completed. A
// *tls.Conn is one (called only while its handshake is incomplete).
type Handshaker interface {
	HandshakeContext(ctx context.Context) error
}

// CloseWriter is a sending half with a graceful end of its write side,
// used instead of Close when the session ends its sending.
type CloseWriter interface{ CloseWrite() error }

// Aborter is a sending half that fails abruptly: Abort is used before
// Close when a pipe fails.
type Aborter interface{ Abort(cause error) }

// UnsentReporter is a sending half (or a lane's writer) that counts the
// bytes its write calls took that the carrier has not yet transmitted; ok
// false when it cannot tell. The driver queries it outside the session
// lock before each write (PipeStats.QueuedBytes, the scheduler's Q_p).
type UnsentReporter interface{ Unsent() (n uint64, ok bool) }

// RetransReporter is a sending half that counts the bytes the carrier has
// retransmitted; ok false when it cannot tell. Queried as Unsent is, at
// most once a second (PipeStats.BytesRetransmitted, LossRate).
type RetransReporter interface{ Retransmitted() (n uint64, ok bool) }

// LaneOpener is a half whose carrier opens further write lanes
// (EXTENSIONS.md lanes): each a writer of whole records.
type LaneOpener interface {
	OpenLane(ctx context.Context) (io.WriteCloser, error)
}

// CarrierConner is a half that names the carrier connection it runs on
// (comparable), so that the pipes of one connection share lane keys and
// fate.
type CarrierConner interface{ CarrierConn() any }

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
		if cw, ok := h.io.W.(CloseWriter); ok {
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
			if a, ok := h.io.W.(Aborter); ok {
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

// KeyingMaterialExporter is a half that terminates an end-to-end
// handshake and exports keying material from it (RFC 5705, RFC 8446
// Section 7.5 semantics). It returns an error until that handshake has
// completed. A transform that runs its own handshake implements it on its
// halves so that PipeAuth.Exporter and PipeOptions.CarrierBound bind to
// the innermost handshake.
type KeyingMaterialExporter interface {
	ExportKeyingMaterial(label string, context []byte, length int) ([]byte, error)
}

// Exporter returns the exporter of p: the first half (W, then R) that
// implements KeyingMaterialExporter, else the TLS handshake this process
// runs for a ConnectionStater half, read from its ConnectionState when
// ExportKeyingMaterial is called: one that completed (TerminatedTLS), or
// one that may still complete (a *tls.Conn, a carrier with early data such
// as QUIC 0-RTT); ok false: none. It is the only source of the exporters
// of PipeAuth.Exporter and PipeOptions.CarrierBound. Exporter only finds
// the exporter; reading it fails while the handshake is incomplete.
func Exporter(p PipeIO) (e KeyingMaterialExporter, ok bool) {
	for _, x := range []any{p.W, p.R} {
		if e, ok := exporterOf(x); ok {
			return e, true
		}
	}
	return nil, false
}

// exporterOf is Exporter for one half of a pipe.
func exporterOf(x any) (KeyingMaterialExporter, bool) {
	if e, ok := x.(KeyingMaterialExporter); ok {
		return e, true
	}
	s, ok := x.(ConnectionStater)
	if !ok {
		return nil, false
	}
	if handshaking(x) || s.ConnectionState().HandshakeComplete {
		return stateExporter{s}, true
	}
	return nil, false
}

// handshaking reports a half whose TLS (or QUIC) handshake this process
// runs and may not have completed yet: a *tls.Conn, or a carrier with early
// data (QUIC 0-RTT). Without TLS, or with TLS ended at a proxy (a plain
// WebSocket), a ConnectionStater never completes a handshake.
func handshaking(x any) bool {
	switch x.(type) {
	case *tls.Conn, EarlyClientCarrier, EarlyServerCarrier:
		return true
	}
	return false
}

// stateExporter exports from the handshake state of a ConnectionStater at
// the time it is read, not when it was found: the handshake may complete
// later, and a carrier may replace its connection (a QUIC 0-RTT rejection).
type stateExporter struct{ s ConnectionStater }

var errHandshakeIncomplete = errors.New("zmux: TLS handshake not complete")

func (e stateExporter) ExportKeyingMaterial(label string, context []byte, length int) ([]byte, error) {
	st := e.s.ConnectionState()
	if !st.HandshakeComplete {
		return nil, errHandshakeIncomplete
	}
	return st.ExportKeyingMaterial(label, context, length)
}

// TerminatedTLS reports the state of the TLS (or QUIC) handshake that this
// process ran for p's carrier (a ConnectionStater half whose handshake
// completed: a TLS connection, a QUIC stream, a zmuxws WebSocket over TLS
// terminated here): CarrierInfo.TLS, and a host's test of whether an
// entrance terminates TLS. It is false for a carrier without TLS, or whose
// TLS ends at a proxy. It is no proof of an end-to-end handshake: it holds
// as well when an intermediary reaches this process over TLS of its own;
// exporters come from Exporter.
func TerminatedTLS(p PipeIO) (tls.ConnectionState, bool) {
	for _, x := range []any{p.W, p.R} {
		if st, ok := terminatedTLS(x); ok {
			return st, true
		}
	}
	return tls.ConnectionState{}, false
}

// terminatedTLS is TerminatedTLS for one half of a pipe.
func terminatedTLS(x any) (tls.ConnectionState, bool) {
	if s, ok := x.(ConnectionStater); ok {
		if st := s.ConnectionState(); st.HandshakeComplete {
			return st, true
		}
	}
	return tls.ConnectionState{}, false
}
