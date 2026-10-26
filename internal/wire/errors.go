package wire

import (
	"strconv"
	"strings"
)

// Transport error codes (SPEC.md Section 10.2).
const (
	NoError                 uint64 = 0x00
	InternalError           uint64 = 0x01
	ConnectionRefused       uint64 = 0x02
	FlowControlError        uint64 = 0x03
	StreamLimitError        uint64 = 0x04
	StreamStateError        uint64 = 0x05
	FinalSizeError          uint64 = 0x06
	FrameEncodingError      uint64 = 0x07
	TransportParameterError uint64 = 0x08
	ProtocolViolation       uint64 = 0x0a
	NoViablePath            uint64 = 0x10
	ExcessiveLoad           uint64 = 0x25ff75ef
	RoleConflict            uint64 = 0x277db2c3
	ResumeTimeout           uint64 = 0x0eb87167
	ResumeFailed            uint64 = 0x0f05f74e
	PipeFailed              uint64 = 0x296a5bf5
	PipeResourceLimit       uint64 = 0x0b4fa113
	PipeAuthFailed          uint64 = 0x09613938
	IdleTimeout             uint64 = 0x2736d55c
)

// Application error code conventions (EXTENSIONS.md Section 7).
const (
	CodeCancelled     uint64 = 0x5a4d0001
	CodeRejected      uint64 = 0x5a4d0002
	CodeCarrierFailed uint64 = 0x5a4d0003
)

var transportErrorNames = map[uint64]string{
	NoError: "NO_ERROR", InternalError: "INTERNAL_ERROR", ConnectionRefused: "CONNECTION_REFUSED",
	FlowControlError: "FLOW_CONTROL_ERROR", StreamLimitError: "STREAM_LIMIT_ERROR",
	StreamStateError: "STREAM_STATE_ERROR", FinalSizeError: "FINAL_SIZE_ERROR",
	FrameEncodingError: "FRAME_ENCODING_ERROR", TransportParameterError: "TRANSPORT_PARAMETER_ERROR",
	ProtocolViolation: "PROTOCOL_VIOLATION", NoViablePath: "NO_VIABLE_PATH",
	ExcessiveLoad: "ZMUX_EXCESSIVE_LOAD", RoleConflict: "ZMUX_ROLE_CONFLICT",
	ResumeTimeout: "ZMUX_RESUME_TIMEOUT", ResumeFailed: "ZMUX_RESUME_FAILED",
	PipeFailed: "ZMUX_PIPE_FAILED", PipeResourceLimit: "ZMUX_PIPE_RESOURCE_LIMIT",
	PipeAuthFailed: "ZMUX_PIPE_AUTH_FAILED", IdleTimeout: "ZMUX_IDLE_TIMEOUT",
}

// TransportErrorName returns the registry name of a transport error code.
func TransportErrorName(code uint64) string {
	if n, ok := transportErrorNames[code]; ok {
		return n
	}
	return "0x" + strconv.FormatUint(code, 16)
}

// TransportErrorCode returns the code for a registry name.
func TransportErrorCode(name string) (uint64, bool) {
	for c, n := range transportErrorNames {
		if n == name {
			return c, true
		}
	}
	return 0, false
}

// Error is a protocol violation detected while decoding or processing peer
// input. It becomes a CONNECTION_CLOSE of type 0x1c.
type Error struct {
	Code      uint64
	FrameType uint64
	Reason    string
	// Details are the key=value pairs of the diagnostic line (SPEC.md 4.3).
	Details string
}

func (e *Error) Error() string {
	s := TransportErrorName(e.Code)
	if e.Reason != "" {
		s += ": " + e.Reason
	}
	if e.Details != "" {
		s += " (" + e.Details + ")"
	}
	return s
}

// Phrase renders the Reason Phrase with its diagnostic line, at most 1024 bytes.
func (e *Error) Phrase() string {
	s := e.Reason
	if e.Details != "" {
		s += "\n" + e.Details
	}
	if len(s) > 1024 {
		s = s[:1024]
	}
	return s
}

// Errorf builds an Error. kv are key/value pairs for the diagnostic line.
func Errorf(code uint64, reason string, kv ...any) *Error {
	e := &Error{Code: code, Reason: reason}
	if len(kv) > 0 {
		var b strings.Builder
		for i := 0; i+1 < len(kv); i += 2 {
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(kv[i].(string))
			b.WriteByte('=')
			switch v := kv[i+1].(type) {
			case uint64:
				b.WriteString(strconv.FormatUint(v, 10))
			case int:
				b.WriteString(strconv.Itoa(v))
			default:
				b.WriteString("0")
			}
		}
		e.Details = b.String()
	}
	return e
}

// ErrFrame is shorthand for a FRAME_ENCODING_ERROR.
func ErrFrame(reason string) *Error { return &Error{Code: FrameEncodingError, Reason: reason} }

// ErrProto is shorthand for a PROTOCOL_VIOLATION.
func ErrProto(reason string) *Error { return &Error{Code: ProtocolViolation, Reason: reason} }

// ErrParam is shorthand for a TRANSPORT_PARAMETER_ERROR.
func ErrParam(reason string) *Error { return &Error{Code: TransportParameterError, Reason: reason} }
