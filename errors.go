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
	CodeNoError       StreamErrorCode = 0
	CodeCancelled     StreamErrorCode = 0x5a4d0001
	CodeRejected      StreamErrorCode = 0x5a4d0002
	CodeCarrierFailed StreamErrorCode = 0x5a4d0003
)

// Transport error codes.
const (
	NoError                 TransportErrorCode = 0x00
	InternalError           TransportErrorCode = 0x01
	ConnectionRefused       TransportErrorCode = 0x02
	FlowControlError        TransportErrorCode = 0x03
	StreamLimitError        TransportErrorCode = 0x04
	StreamStateError        TransportErrorCode = 0x05
	FinalSizeError          TransportErrorCode = 0x06
	FrameEncodingError      TransportErrorCode = 0x07
	TransportParameterError TransportErrorCode = 0x08
	ProtocolViolation       TransportErrorCode = 0x0a
	NoViablePath            TransportErrorCode = 0x10
	ExcessiveLoad           TransportErrorCode = 0x25ff75ef
	RoleConflict            TransportErrorCode = 0x277db2c3
	ResumeTimeoutCode       TransportErrorCode = 0x0eb87167
	ResumeFailed            TransportErrorCode = 0x0f05f74e
	IdleTimeoutCode         TransportErrorCode = 0x2736d55c
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
// expired.
type IdleTimeoutError struct{ Remote bool }

func (e *IdleTimeoutError) Error() string   { return "zmux: idle timeout (" + remoteStr(e.Remote) + ")" }
func (e *IdleTimeoutError) Is(t error) bool { return t == net.ErrClosed || t == os.ErrDeadlineExceeded }
func (e *IdleTimeoutError) Timeout() bool   { return true }

// HandshakeTimeoutError reports that the peer's parameters did not arrive.
type HandshakeTimeoutError struct{}

func (e *HandshakeTimeoutError) Error() string { return "zmux: handshake timeout" }
func (e *HandshakeTimeoutError) Is(t error) bool {
	return t == net.ErrClosed || t == os.ErrDeadlineExceeded
}
func (e *HandshakeTimeoutError) Timeout() bool { return true }

// EarlyDataRejectedError reports that the peer could not take early data
// and processed none of it; retry on a new session without early data.
type EarlyDataRejectedError struct{}

func (e *EarlyDataRejectedError) Error() string   { return "zmux: early data rejected" }
func (e *EarlyDataRejectedError) Is(t error) bool { return t == net.ErrClosed }

// ResumeTimeoutError reports that the resume grace period expired.
type ResumeTimeoutError struct{ LastPipeErr error }

func (e *ResumeTimeoutError) Error() string   { return "zmux: resume timeout" }
func (e *ResumeTimeoutError) Unwrap() error   { return e.LastPipeErr }
func (e *ResumeTimeoutError) Is(t error) bool { return t == net.ErrClosed }

// PipesLostError reports that the carrier failed or every pipe of a
// direction was lost.
type PipesLostError struct{ LastPipeErr error }

func (e *PipesLostError) Error() string {
	if e.LastPipeErr != nil {
		return "zmux: pipes lost: " + e.LastPipeErr.Error()
	}
	return "zmux: pipes lost"
}
func (e *PipesLostError) Unwrap() error   { return e.LastPipeErr }
func (e *PipesLostError) Is(t error) bool { return t == net.ErrClosed }

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
	return t == ErrRejected && e.Remote && e.ErrorCode == CodeRejected
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

var (
	// ErrStreamClosed is returned for operations on a direction the
	// application closed itself.
	ErrStreamClosed = errors.New("zmux: stream closed")
	// ErrWouldBlock is returned by non-blocking calls that would wait.
	ErrWouldBlock = errors.New("zmux: operation would block")
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
	// ErrListenerClosed is returned by a closed Listener.
	ErrListenerClosed = errors.New("zmux: listener closed")
	// ErrInvalidConfig wraps configuration errors.
	ErrInvalidConfig = errors.New("zmux: invalid configuration")
)

// IsRetryable reports whether err says that the peer did not process the
// stream, so it can be retried on this or another session.
func IsRetryable(err error) bool {
	var e *EarlyDataRejectedError
	return errors.Is(err, ErrRejected) || errors.Is(err, ErrGoingAway) || errors.As(err, &e)
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
	return &TransportError{ErrorCode: InternalError, ErrorMessage: e.Error()}
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
