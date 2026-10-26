package core

import (
	"errors"
	"fmt"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// CloseKind is the kind of a session termination (API.md 5.3).
type CloseKind uint8

const (
	CloseApplication CloseKind = iota + 1
	CloseTransport
	CloseIdle
	CloseHandshakeTimeout
	CloseEarlyRejected
	CloseResumeTimeout
	ClosePipesLost
)

// SessionError is the terminal error of a session.
type SessionError struct {
	Kind      CloseKind
	Code      uint64
	FrameType uint64
	Reason    string
	Details   string
	Remote    bool
	Cause     error // last pipe error for PipesLost/ResumeTimeout
}

func (e *SessionError) Error() string {
	from := "local"
	if e.Remote {
		from = "remote"
	}
	switch e.Kind {
	case CloseApplication:
		return fmt.Sprintf("zmux: application close (%s) code %#x: %s", from, e.Code, e.Reason)
	case CloseTransport:
		s := fmt.Sprintf("zmux: transport close (%s) %s", from, wire.TransportErrorName(e.Code))
		if e.Reason != "" {
			s += ": " + e.Reason
		}
		return s
	case CloseIdle:
		return "zmux: idle timeout (" + from + ")"
	case CloseHandshakeTimeout:
		return "zmux: handshake timeout"
	case CloseEarlyRejected:
		return "zmux: early data rejected"
	case CloseResumeTimeout:
		return "zmux: resume timeout"
	case ClosePipesLost:
		if e.Cause != nil {
			return "zmux: pipes lost: " + e.Cause.Error()
		}
		return "zmux: pipes lost"
	}
	return "zmux: session closed"
}

func (e *SessionError) Unwrap() error { return e.Cause }

// StreamErrKind classifies stream-level outcomes (STATE_MACHINE.md 2.5).
type StreamErrKind uint8

const (
	StreamReset StreamErrKind = iota + 1
	StreamStopped
	StreamClosedLocally
	StreamRejected
	StreamGoingAway
)

// StreamError is a stream-level outcome.
type StreamError struct {
	Kind     StreamErrKind
	StreamID uint64
	Code     uint64
	Remote   bool
	Reliable uint64
}

func (e *StreamError) Error() string {
	switch e.Kind {
	case StreamReset:
		return fmt.Sprintf("zmux: stream %d reset with code %#x", e.StreamID, e.Code)
	case StreamStopped:
		return fmt.Sprintf("zmux: stream %d stopped with code %#x", e.StreamID, e.Code)
	case StreamRejected:
		return fmt.Sprintf("zmux: stream %d rejected", e.StreamID)
	case StreamGoingAway:
		return "zmux: going away"
	}
	return fmt.Sprintf("zmux: stream %d closed locally", e.StreamID)
}

// API errors that do not end anything.
var (
	ErrStreamLimit    = errors.New("zmux: stream limit reached")
	ErrWouldBlock     = errors.New("zmux: operation would block")
	ErrNotNegotiated  = errors.New("zmux: feature not negotiated")
	ErrNotEstablished = errors.New("zmux: session not established")
	ErrTooLate        = errors.New("zmux: setting must precede the first frame")
	ErrInvalid        = errors.New("zmux: invalid argument")
	ErrDatagramQueue  = errors.New("zmux: datagram queue full")
)

// DatagramTooLargeError reports the largest payload the peer accepts.
type DatagramTooLargeError struct{ Max int }

func (e *DatagramTooLargeError) Error() string {
	return fmt.Sprintf("zmux: datagram too large (max %d)", e.Max)
}
