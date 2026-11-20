package zmux

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// Framing is a carrier's record framing.
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
	// ReadMessage returns the next whole message when it holds at most
	// limit bytes, and fails with ErrMessageTooLarge for a longer one
	// after reading no more than limit+1 of its bytes into memory (the
	// carrier is then unusable). limit is always positive: zmux never asks
	// for more than MaxRecordSize, and the admission of a listener asks for
	// its peek (Admission.Peek, MaxPeek). The caller owns the returned
	// slice.
	ReadMessage(limit int) ([]byte, error)
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

// BuffersWriter is an optional capability of a byte-stream sending half
// that takes a batch of buffers in one call, as net.Buffers does for a
// net.Conn: one flush or one locked copy per batch instead of one per
// buffer. carrier.WriteFlat implements WriteBuffers over a single Write.
type BuffersWriter interface {
	// WriteBuffers writes bufs in order, as one Write of their
	// concatenation would. It may consume bufs (advance or empty its
	// elements) but retains neither bufs nor their contents once it
	// returns.
	WriteBuffers(bufs [][]byte) (int, error)
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
//     handshake, for PipeAuth.Exporter and the Bound record mode
//     (Exporter, which falls back to a terminated TLS state);
//   - Handshaker: a handshake the listener runs before admission;
//   - CarrierConner and LaneOpener: lanes;
//   - EarlyClientCarrier and EarlyServerCarrier on W: carrier early data;
//   - DatagramChannel: the carrier's datagram channel;
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
	R io.ReadCloser
	W io.WriteCloser
	// own is the one closer of the carrier while zmux sets it up (owned),
	// shared by every copy and prefixed view of it; nil for a PipeIO zmux
	// has not taken.
	own *carrierio.Closer
	// Protocol is the application protocol the carrier negotiated other
	// than by the ALPN of a TLS handshake this process runs, such as a
	// WebSocket subprotocol (ConnectionState.ApplicationProtocol); empty
	// otherwise.
	Protocol string
	// Label names the pipe in statistics.
	Label string
	// Variant is a short host label (at most 32 bytes advised) for the
	// transform the carrier runs, "" for none: set by a Candidate
	// (Candidate.Variant), an admission (Decision.Variant) or WrapPipe.
	// zmux only reports it, in pipe events, PipeStats, ShapedPipe and
	// CarrierCache keys; the host bounds the number of variants.
	Variant string
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
	// MaxRecord is the largest record plaintext zmux writes on the carrier,
	// a local bound applied beside the peer's max_record_size and never
	// negotiated; 0: the carrier's default. A half implementing
	// CarrierRecordLimit lowers it further (the smaller bound holds). It
	// applies to the first pipe and to added pipes alike. It has a floor
	// but no upper bound: a value below MinMaxRecord other than 0 is
	// ErrInvalidConfig where the PipeIO is used, as for WriteOverhead; any
	// larger value is a cap that the peer's max_record_size and the
	// implementation's own record limit (MaxRecordSize) reduce further.
	MaxRecord int
	// HandshakePadding is the padding bytes a handshake run on the carrier
	// before this PipeIO added; the session the carrier becomes part of, by
	// starting it or by joining it, meters it once. 0: none.
	HandshakePadding uint64
	Framing          Framing
	// Carrier is the carrier's kind, which the adapters set on both ends
	// and a Candidate's dial sets to its Kind; 0 for a carrier of no listed
	// kind, such as raw TCP or TLS. The session reports it to its
	// ShaperFactory, in pipe events and in PipeStats.
	Carrier CarrierKind
}

// MaxWriteOverhead is the largest PipeIO.WriteOverhead.
const MaxWriteOverhead = 65535

// MinMaxRecord is the carrier record floor: every carrier of a pipe, each
// lane included, takes records of this many bytes, so it is the smallest
// nonzero PipeIO.MaxRecord and the smallest limit a carrier may report
// (CarrierRecordLimit, less the overhead of a record transform). Every
// frame that cannot be split across records fits one record of it.
const MinMaxRecord = wire.RecordFloor

// validate reports a WriteOverhead outside [0, MaxWriteOverhead], a
// MaxRecord that is neither 0 nor at least MinMaxRecord, or a limit a
// carrier half reports (CarrierRecordLimit) below MinMaxRecord.
func (p PipeIO) validate() error {
	if p.WriteOverhead < 0 || p.WriteOverhead > MaxWriteOverhead {
		return fmt.Errorf("%w: PipeIO.WriteOverhead %d outside [0, %d]", ErrInvalidConfig, p.WriteOverhead, MaxWriteOverhead)
	}
	if p.MaxRecord != 0 && p.MaxRecord < MinMaxRecord {
		return fmt.Errorf("%w: PipeIO.MaxRecord %d is below %d", ErrInvalidConfig, p.MaxRecord, MinMaxRecord)
	}
	if lim := carrierRecordLimit(p); lim != 0 && lim < MinMaxRecord {
		return fmt.Errorf("%w: the carrier's record limit %d is below %d", ErrInvalidConfig, lim, MinMaxRecord)
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
// LaneOpener is offered only when every lane can be wrapped alike;
// FailureReporter may be forwarded by any transform that fails when the
// carrier below it fails.

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

// NetConner is a half over a connection that it names: the connection
// below it, as a TLS connection's NetConn. The listener measures the TCP
// socket under a half through it (Unsent, Retransmitted).
type NetConner interface{ NetConn() net.Conn }

// CloseWriter is a sending half with a graceful end of its write side,
// used instead of Close when the session ends its sending.
type CloseWriter interface{ CloseWrite() error }

// Aborter is a sending half that fails abruptly: Abort is used before
// Close when a pipe fails, and after a graceful Close whose work (a
// final flush, a TLS close_notify) has not ended within the close bound,
// to end what that Close left running.
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

// FailureReporter is a half whose carrier reports its own failure:
// ReportsFailure true means that when the path under the carrier fails,
// its reads or writes fail (an error, a reset, an unexpected end), as a
// stream of an outer zmux session does when that session's liveness ends
// it (StreamPipe, Conn.OpenPipeStream). The carrier reports it only when
// every half does: a split carrier with one plain half (a raw
// connection, which can stall silently) keeps stall detection. Stall
// detection then never fails the pipe: a carrier queued behind a rate
// limit stalls without being dead, and its own end is the failure signal.
// The pipe still turns suspect and is avoided and probed while it
// stalls. An explicit PipeLiveness.DeadTimeout of the pipe overrides it.
type FailureReporter interface{ ReportsFailure() bool }

// LaneOpener is a half whose carrier opens further write lanes
// (lanes): each a writer of whole records.
type LaneOpener interface {
	OpenLane(ctx context.Context) (io.WriteCloser, error)
}

// CarrierConner is a half that names the carrier connection it runs on
// (comparable), so that the pipes of one connection share lane keys and
// fate.
type CarrierConner interface{ CarrierConn() any }

// CloseCodeRecorder is a half whose carrier connection outlives its pipes
// and ends with an application error code of its own, such as a QUIC
// connection: when the session ends, zmux passes its close code and reason
// to each pipe's halves before it ends them: those of its application
// close (CONNECTION_CLOSE 0x1d, sent or received), or 0 and an empty
// reason after any other end (a transport error, an idle timeout), so that
// the adapter closes the connection, once no pipe uses it, with the code
// of the last session it carried. It may be called more than once with
// the same code; a pipe removed while its session runs passes nothing. A
// transform may forward it.
type CloseCodeRecorder interface {
	RecordCloseCode(code ApplicationErrorCode, reason string)
}

// owned returns p with the closer that ends it while zmux sets it up: the
// one it has, or a new one when zmux takes it here.
func owned(p PipeIO) PipeIO {
	if p.own == nil {
		p.own = carrierio.NewCloser(p.R, p.W)
	}
	return p
}

// closePipe closes a carrier zmux sets up through its closer: each half
// once, whichever of its holders asks first; once its attachment owns it
// (halfCloser.take), the attachment closes it.
func closePipe(p PipeIO) {
	if p.own != nil {
		p.own.Close()
		return
	}
	ClosePipeIO(p)
}

// releasePipe hands p from its setup closer to the next owner, whose close
// is next (nil: none); false: it was closed already.
func releasePipe(p PipeIO, next func()) bool { return p.own == nil || p.own.Release(next) }

// halfCloser closes the halves of a carrier exactly once each, also when R
// and W are the same object. It owns them from take on; until then the
// setup's closer (PipeIO.own) does.
type halfCloser struct {
	io      PipeIO
	same    bool
	rOnce   sync.Once
	wOnce   sync.Once   // the write side's graceful end
	wClose  sync.Once   // W.Close: once, whichever end calls it (each half is closed exactly once)
	wDone   atomic.Bool // the write side's graceful end has returned
	allOnce sync.Once
}

func newHalfCloser(p PipeIO) *halfCloser {
	same := p.R != nil && p.W != nil && carrierio.SameHalf(p.R, p.W)
	return &halfCloser{io: p, same: same}
}

// setupClose closes the halves through the setup's closer while it owns
// them (before take), and reports whether it did.
func (h *halfCloser) setupClose() bool {
	return h.io.own != nil && !h.io.own.CloseUnreleased()
}

// take makes h the owner of the halves as the attachment starts; a later
// close through the setup's closer is next. Halves the setup closed
// already are not closed again.
func (h *halfCloser) take(next func()) {
	if !releasePipe(h.io, next) {
		h.rOnce.Do(func() {})
		h.wClose.Do(func() {})
	}
}

// recordCloseCode passes the session's close code to the halves that take
// it (CloseCodeRecorder).
func (h *halfCloser) recordCloseCode(code ApplicationErrorCode, reason string) {
	if r, ok := h.io.W.(CloseCodeRecorder); ok {
		r.RecordCloseCode(code, reason)
	}
	if r, ok := h.io.R.(CloseCodeRecorder); ok && !h.same {
		r.RecordCloseCode(code, reason)
	}
}

// closeRead closes the receiving half (after its end, or at CLOSED).
func (h *halfCloser) closeRead() {
	if h.io.R == nil {
		return
	}
	if h.same || h.setupClose() {
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
		defer h.wDone.Store(true)
		if cw, ok := h.io.W.(CloseWriter); ok {
			_ = cw.CloseWrite()
			return
		}
		if !h.same {
			h.closeW()
		}
	})
}

func (h *halfCloser) closeW() {
	if h.setupClose() {
		return
	}
	h.wClose.Do(func() { _ = h.io.W.Close() })
}

// closeAll closes every half; abort uses Abort where offered and closes the
// lowest connection of a TLS carrier so that a blocked write is interrupted.
func (h *halfCloser) closeAll(abort bool, cause error) {
	h.allOnce.Do(func() {
		if abort {
			h.abort(cause)
		}
		if h.setupClose() {
			return
		}
		if h.io.W != nil {
			h.closeW()
		}
		if h.io.R != nil && !h.same {
			h.rOnce.Do(func() { _ = h.io.R.Close() })
		}
	})
}

// abort aborts the write side, also after a graceful closeAll: Abort where
// offered, else the lowest connection of a TLS carrier is closed. It ends
// what a graceful close left running.
func (h *halfCloser) abort(cause error) {
	switch w := h.io.W.(type) {
	case Aborter:
		w.Abort(cause)
	case *tls.Conn:
		_ = w.NetConn().Close()
	}
}

// KeyingMaterialExporter is a half that terminates an end-to-end
// handshake and exports keying material from it (RFC 5705, RFC 8446
// Section 7.5 semantics). It returns an error until that handshake has
// completed. A transform that runs its own handshake implements it on its
// halves so that PipeAuth.Exporter and the Bound record mode bind to
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
// of PipeAuth.Exporter and the Bound record mode. Exporter only finds
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
	x = unprefixed(x)
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
	case *tls.Conn, *secureTLSConn, EarlyClientCarrier, EarlyServerCarrier:
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

// ClosePipeIO closes a carrier's halves: W, then R unless it is W. Each
// call closes each distinct half once; it is not idempotent, so the
// holders of one carrier must agree on which of them closes it. R and W
// are one half when they are the same comparable value (a pointer, as a
// net.Conn usually is); one value of a type that cannot be compared (a
// struct holding a slice, say) counts as two halves and is closed twice.
func ClosePipeIO(p PipeIO) {
	if p.W != nil {
		_ = p.W.Close()
	}
	if p.R != nil && !carrierio.SameHalf(p.R, p.W) {
		_ = p.R.Close()
	}
}
