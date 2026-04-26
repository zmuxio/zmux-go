package wire

import (
	"errors"
	"fmt"
)

type Error struct {
	Code ErrorCode
	Op   string
	Err  error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	switch {
	case e.Op != "" && e.Err != nil:
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Op, e.Err)
	case e.Op != "":
		return fmt.Sprintf("%s: %s", e.Code, e.Op)
	case e.Err != nil:
		return fmt.Sprintf("%s: %v", e.Code, e.Err)
	default:
		return e.Code.String()
	}
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func WrapError(code ErrorCode, op string, err error) error {
	return &Error{Code: code, Op: op, Err: err}
}

func ErrorCodeOf(err error) (ErrorCode, bool) {
	if we, ok := findError[*Error](err); ok {
		return we.Code, true
	}
	return 0, false
}

func IsErrorCode(err error, code ErrorCode) bool {
	got, ok := ErrorCodeOf(err)
	return ok && got == code
}

var (
	ErrInvalidMagic              = errors.New("invalid magic")
	ErrUnsupportedPrefaceVer     = errors.New("unsupported preface version")
	ErrInvalidRole               = errors.New("invalid role")
	ErrNonCanonicalVarint        = errors.New("non-canonical varint62")
	ErrValueTooLarge             = errors.New("varint62 value out of range")
	ErrTruncatedVarint           = errors.New("truncated varint62")
	ErrTruncatedTLV              = errors.New("truncated tlv")
	ErrTLVValueOverrun           = errors.New("tlv value overruns containing payload")
	ErrInvalidFrameType          = errors.New("invalid frame type")
	ErrInvalidFlags              = errors.New("invalid flags for frame type")
	ErrShortFrame                = errors.New("frame too short")
	ErrPayloadTooLarge           = errors.New("payload exceeds configured limit")
	ErrOpenInfoUnavailable       = errors.New("zmux: open_info requires negotiated open_metadata")
	ErrOpenMetadataTooLarge      = errors.New("zmux: opening metadata exceeds peer max_frame_payload")
	ErrPriorityUpdateUnavailable = errors.New("zmux: metadata update requires negotiated priority_update and matching semantic capability")
	ErrPriorityUpdateTooLarge    = errors.New("zmux: priority update exceeds peer max_extension_payload_bytes")
	ErrEmptyMetadataUpdate       = errors.New("zmux: metadata update has no fields")
)

const maxErrorUnwrapDepth = 64

func findError[T any](err error) (T, bool) {
	return findErrorDepth[T](err, 0)
}

func findErrorDepth[T any](err error, depth int) (T, bool) {
	var zero T
	if err == nil || depth > maxErrorUnwrapDepth {
		return zero, false
	}
	if target, ok := any(err).(T); ok {
		return target, true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range wrapped.Unwrap() {
			if target, ok := findErrorDepth[T](child, depth+1); ok {
				return target, true
			}
		}
		return zero, false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return findErrorDepth[T](wrapped.Unwrap(), depth+1)
	}
	return zero, false
}
