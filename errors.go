package zmux

import (
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// ApplicationErrorCode is an application error code (62 bits).
type ApplicationErrorCode uint64

// StreamErrorCode is an application error code of a stream.
type StreamErrorCode uint64

// TransportErrorCode is a transport error code (SPEC.md Section 10.2).
type TransportErrorCode uint64

// Application error-code conventions (EXTENSIONS.md Section 7).
const (
	StreamNoError       StreamErrorCode = 0
	StreamCancelled     StreamErrorCode = 0x5a4d0001
	StreamRejected      StreamErrorCode = 0x5a4d0002
	StreamCarrierFailed StreamErrorCode = 0x5a4d0003
)

// Transport error codes.
const (
	TransportNoError            TransportErrorCode = 0x00
	TransportInternalError      TransportErrorCode = 0x01
	TransportConnectionRefused  TransportErrorCode = 0x02
	TransportFlowControlError   TransportErrorCode = 0x03
	TransportStreamLimitError   TransportErrorCode = 0x04
	TransportStreamStateError   TransportErrorCode = 0x05
	TransportFinalSizeError     TransportErrorCode = 0x06
	TransportFrameEncodingError TransportErrorCode = 0x07
	TransportParameterError     TransportErrorCode = 0x08
	TransportProtocolViolation  TransportErrorCode = 0x0a
	TransportNoViablePath       TransportErrorCode = 0x10
	TransportExcessiveLoad      TransportErrorCode = 0x25ff75ef
	TransportRoleConflict       TransportErrorCode = 0x277db2c3
	TransportResumeTimeout      TransportErrorCode = 0x0eb87167
	TransportResumeFailed       TransportErrorCode = 0x0f05f74e
	TransportIdleTimeout        TransportErrorCode = 0x2736d55c
)

func (c TransportErrorCode) String() string { return wire.TransportErrorName(uint64(c)) }

// ApplicationError is a session closed by an application (CONNECTION_CLOSE
// 0x1d).
type ApplicationError struct {
	Remote       bool
	ErrorCode    ApplicationErrorCode
	ErrorMessage string
}

// Error describes the close.
func (e *ApplicationError) Error() string {
	return fmt.Sprintf("zmux: application error %#x (%s): %s", uint64(e.ErrorCode), remoteStr(e.Remote), e.ErrorMessage)
}

// Is makes every session termination match net.ErrClosed.
func (e *ApplicationError) Is(t error) bool { return t == net.ErrClosed }

// TransportError is a session closed for a protocol error (0x1c).
type TransportError struct {
	Remote       bool
	ErrorCode    TransportErrorCode
	FrameType    uint64
	ErrorMessage string
	// Details are the key=value pairs of the diagnostic line (SPEC.md 4.3).
	Details string
}

// Error describes the close.
func (e *TransportError) Error() string {
	s := fmt.Sprintf("zmux: transport error %v (%s)", e.ErrorCode, remoteStr(e.Remote))
	if e.ErrorMessage != "" {
		s += ": " + e.ErrorMessage
	}
	return s
}

// Is makes every session termination match net.ErrClosed.
func (e *TransportError) Is(t error) bool { return t == net.ErrClosed }

// IdleTimeoutError reports the idle timeout; Remote when the peer's timer
// expired. It matches ErrIdleTimeout, net.ErrClosed and
// os.ErrDeadlineExceeded.
type IdleTimeoutError struct{ Remote bool }

// Error describes the timeout.
func (e *IdleTimeoutError) Error() string { return "zmux: idle timeout (" + remoteStr(e.Remote) + ")" }

// Is matches ErrIdleTimeout, net.ErrClosed and os.ErrDeadlineExceeded.
func (e *IdleTimeoutError) Is(t error) bool {
	return t == ErrIdleTimeout || t == net.ErrClosed || t == os.ErrDeadlineExceeded
}

// Timeout reports true.
func (e *IdleTimeoutError) Timeout() bool { return true }

// HandshakeTimeoutError reports that the peer's transport parameters did not
// arrive within Config.HandshakeTimeout. It matches ErrHandshakeTimeout,
// net.ErrClosed and os.ErrDeadlineExceeded.
type HandshakeTimeoutError struct{}

// Error describes the timeout.
func (e *HandshakeTimeoutError) Error() string { return "zmux: handshake timeout" }

// Is matches ErrHandshakeTimeout, net.ErrClosed and os.ErrDeadlineExceeded.
func (e *HandshakeTimeoutError) Is(t error) bool {
	return t == ErrHandshakeTimeout || t == net.ErrClosed || t == os.ErrDeadlineExceeded
}

// Timeout reports true.
func (e *HandshakeTimeoutError) Timeout() bool { return true }

// EarlyDataRejectedError reports that the peer could not take early data
// and processed none of it: retry on a new session (see EarlyData). It
// matches ErrEarlyDataRejected and net.ErrClosed, and IsRetryable reports
// true for it.
type EarlyDataRejectedError struct{}

// Error describes the rejection.
func (e *EarlyDataRejectedError) Error() string { return "zmux: early data rejected" }

// Is matches ErrEarlyDataRejected and net.ErrClosed.
func (e *EarlyDataRejectedError) Is(t error) bool {
	return t == ErrEarlyDataRejected || t == net.ErrClosed
}

// ResumeTimeoutError reports that a suspended session found no pipe within
// the resume grace period. It matches ErrResumeTimeout and net.ErrClosed
// and unwraps to the error of the last pipe lost, if any.
type ResumeTimeoutError struct{ LastPipeErr error }

// Error describes the timeout.
func (e *ResumeTimeoutError) Error() string { return "zmux: resume timeout" }

// Unwrap returns LastPipeErr.
func (e *ResumeTimeoutError) Unwrap() error { return e.LastPipeErr }

// Is matches ErrResumeTimeout and net.ErrClosed.
func (e *ResumeTimeoutError) Is(t error) bool { return t == ErrResumeTimeout || t == net.ErrClosed }

// PipesLostError reports that the carrier failed or every pipe of a
// direction was lost without resume. It matches ErrPipesLost and
// net.ErrClosed and unwraps to the error of the last pipe lost, if any.
type PipesLostError struct{ LastPipeErr error }

// Error describes the loss.
func (e *PipesLostError) Error() string {
	if e.LastPipeErr != nil {
		return "zmux: pipes lost: " + e.LastPipeErr.Error()
	}
	return "zmux: pipes lost"
}

// Unwrap returns LastPipeErr.
func (e *PipesLostError) Unwrap() error { return e.LastPipeErr }

// Is matches ErrPipesLost and net.ErrClosed.
func (e *PipesLostError) Is(t error) bool { return t == ErrPipesLost || t == net.ErrClosed }

// StreamError is a stream ended by the peer: a reset on reads, a
// STOP_SENDING on writes.
type StreamError struct {
	StreamID  StreamID
	ErrorCode StreamErrorCode
	Remote    bool
	// ReliableSize is the RESET_STREAM_AT Reliable Size of a reset.
	ReliableSize uint64
}

// Error describes the reset or STOP_SENDING.
func (e *StreamError) Error() string {
	return fmt.Sprintf("zmux: stream %d canceled with error code %#x (%s)", e.StreamID, uint64(e.ErrorCode), remoteStr(e.Remote))
}

// Is matches ErrRejected for a peer's ZMUX_REJECTED: the peer did not
// process the stream, which can be retried (IsRetryable).
func (e *StreamError) Is(t error) bool {
	return t == ErrRejected && e.Remote && e.ErrorCode == StreamRejected
}

// DatagramTooLargeError reports the largest payload the peer accepts.
type DatagramTooLargeError struct{ MaxDatagramPayloadSize int64 }

// Error describes the limit.
func (e *DatagramTooLargeError) Error() string {
	return fmt.Sprintf("zmux: datagram too large (max %d)", e.MaxDatagramPayloadSize)
}

// StreamLimitReachedError reports that no stream credit is available now
// (OpenStream); OpenStreamSync waits for credit instead.
type StreamLimitReachedError struct{}

// Error describes the condition.
func (e *StreamLimitReachedError) Error() string { return "zmux: too many open streams" }

// Temporary reports that credit may arrive later.
func (e *StreamLimitReachedError) Temporary() bool { return true }

// Sentinel errors of session ends. Each session-end type matches its
// sentinel with errors.Is, so errors.Is(err, ErrIdleTimeout) works on any
// error of the session or of a call that failed because the session ended.
var (
	// ErrHandshakeTimeout reports that the peer's transport parameters did not
	// arrive in time (*HandshakeTimeoutError).
	ErrHandshakeTimeout = errors.New("zmux: handshake timeout")
	// ErrEarlyDataRejected reports that the peer rejected early data and
	// processed none of it (*EarlyDataRejectedError).
	ErrEarlyDataRejected = errors.New("zmux: early data rejected")
	// ErrIdleTimeout reports that the idle timeout closed the session
	// (*IdleTimeoutError).
	ErrIdleTimeout = errors.New("zmux: idle timeout")
	// ErrResumeTimeout reports that a suspended session was not resumed in time
	// (*ResumeTimeoutError).
	ErrResumeTimeout = errors.New("zmux: resume timeout")
	// ErrPipesLost reports that the session lost its pipes and could not resume
	// (*PipesLostError).
	ErrPipesLost = errors.New("zmux: pipes lost")
)

// Errors of individual calls.
var (
	// ErrStreamClosed is returned for operations on a direction the
	// application closed itself.
	ErrStreamClosed = errors.New("zmux: stream closed")
	// ErrNotNegotiated is returned for features the session does not use.
	ErrNotNegotiated = errors.New("zmux: feature not negotiated")
	// ErrNotEstablished is returned for calls that need the peer's
	// parameters before they arrived.
	ErrNotEstablished = errors.New("zmux: session not established")
	// ErrRejected reports a stream the peer did not process (ZMUX_REJECTED
	// or at or above a GOAWAY watermark); it is retryable.
	ErrRejected = errors.New("zmux: stream rejected")
	// ErrGoingAway reports an open after the peer's GOAWAY; it is retryable.
	ErrGoingAway = errors.New("zmux: going away")
	// ErrTooLate is returned for settings that must precede the first frame.
	ErrTooLate = errors.New("zmux: too late: the stream's first frame was sent")
	// ErrInvalidConfig wraps configuration errors.
	ErrInvalidConfig = errors.New("zmux: invalid configuration")
	// ErrPipeGone is returned by Pipe methods once the session no longer
	// knows the attachment.
	ErrPipeGone = errors.New("zmux: pipe gone")
	// ErrNoDecision is returned by ResolveDuplicate when no input of the
	// duplicate-session rule decides between the two sessions.
	ErrNoDecision = errors.New("zmux: the duplicate-session rule has no input that decides")
)

// IsRetryable reports whether err says that the peer did not process the
// stream, so it can be sent again: ErrRejected and ErrGoingAway (retry on
// this or another session) and ErrEarlyDataRejected (retry on a new
// session).
func IsRetryable(err error) bool {
	return errors.Is(err, ErrRejected) || errors.Is(err, ErrGoingAway) || errors.Is(err, ErrEarlyDataRejected)
}

func remoteStr(r bool) string {
	if r {
		return "remote"
	}
	return "local"
}

// sessionErr converts the core's terminal error.
func sessionErr(e *core.SessionError) error {
	if e == nil {
		return nil
	}
	switch e.Kind {
	case core.CloseApplication:
		return &ApplicationError{Remote: e.Remote, ErrorCode: ApplicationErrorCode(e.Code), ErrorMessage: e.Reason}
	case core.CloseTransport:
		return &TransportError{Remote: e.Remote, ErrorCode: TransportErrorCode(e.Code), FrameType: e.FrameType,
			ErrorMessage: e.Reason, Details: e.Details}
	case core.CloseIdle:
		return &IdleTimeoutError{Remote: e.Remote}
	case core.CloseHandshakeTimeout:
		return &HandshakeTimeoutError{}
	case core.CloseEarlyRejected:
		return &EarlyDataRejectedError{}
	case core.CloseResumeTimeout:
		return &ResumeTimeoutError{LastPipeErr: e.Cause}
	case core.ClosePipesLost:
		return &PipesLostError{LastPipeErr: e.Cause}
	}
	return &TransportError{ErrorCode: TransportInternalError, ErrorMessage: e.Error()}
}

// apiErr converts an error returned by the core to the public form.
func apiErr(err error) error {
	if err == nil {
		return nil
	}
	var se *core.SessionError
	var st *core.StreamError
	var tl *core.DatagramTooLargeError
	switch {
	case errors.As(err, &se):
		return sessionErr(se)
	case errors.As(err, &st):
		switch st.Kind {
		case core.StreamReset, core.StreamStopped:
			return &StreamError{StreamID: StreamID(st.StreamID), ErrorCode: StreamErrorCode(st.Code), Remote: st.Remote,
				ReliableSize: st.Reliable}
		case core.StreamRejected:
			return fmt.Errorf("stream %d: %w", st.StreamID, ErrRejected)
		case core.StreamGoingAway:
			return ErrGoingAway
		case core.StreamClosedLocally:
		}
		return ErrStreamClosed
	case errors.As(err, &tl):
		return &DatagramTooLargeError{MaxDatagramPayloadSize: int64(tl.Max)}
	case errors.Is(err, core.ErrNotNegotiated):
		return ErrNotNegotiated
	case errors.Is(err, core.ErrNotEstablished):
		return ErrNotEstablished
	case errors.Is(err, core.ErrStreamLimit):
		return &StreamLimitReachedError{}
	case errors.Is(err, core.ErrTooLate):
		return ErrTooLate
	}
	return err
}
