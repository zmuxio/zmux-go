package zmux

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/zmuxio/zmux-go/internal/wire"
)

var (
	ErrNilConn                   = errors.New("zmux: nil conn")
	ErrSessionClosed             = errors.New("zmux: session closed")
	ErrReadClosed                = errors.New("zmux: read side closed")
	ErrWriteClosed               = errors.New("zmux: write side closed")
	ErrStreamNotReadable         = errors.New("zmux: stream is not readable")
	ErrStreamNotWritable         = errors.New("zmux: stream is not writable")
	ErrOpenInfoUnavailable       = wire.ErrOpenInfoUnavailable
	ErrOpenMetadataTooLarge      = wire.ErrOpenMetadataTooLarge
	ErrOpenLimited               = errors.New("zmux: too many provisional local opens")
	ErrOpenExpired               = errors.New("zmux: provisional local open expired before first-frame commit")
	ErrAdapterUnsupported        = errors.New("zmux: feature not supported by adapter")
	ErrPriorityUpdateUnavailable = errors.New("zmux: metadata update requires negotiated priority_update and matching semantic capability")
	ErrPriorityUpdateTooLarge    = errors.New("zmux: priority update exceeds peer max_extension_payload_bytes")
	ErrGracefulCloseTimeout      = errors.New("zmux: graceful close drain timed out")
	ErrEmptyMetadataUpdate       = errors.New("zmux: metadata update has no fields")
	ErrKeepaliveTimeout          = errors.New("zmux: keepalive timeout")
)

// ApplicationError carries an application code and optional reason text.
type ApplicationError struct {
	Code   uint64
	Reason string
}

func applicationErr(code uint64, reason string) *ApplicationError {
	return &ApplicationError{Code: code, Reason: reason}
}

func refusedStreamAppErr() *ApplicationError {
	return applicationErr(uint64(CodeRefusedStream), "")
}

func cancelledAppErr(reason string) *ApplicationError {
	return applicationErr(uint64(CodeCancelled), reason)
}

func cloneApplicationError(err *ApplicationError) *ApplicationError {
	if err == nil {
		return nil
	}
	return applicationErr(err.Code, err.Reason)
}

func (e *ApplicationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Reason != "" {
		return fmt.Sprintf("zmux application error %d: %s", e.Code, e.Reason)
	}
	return fmt.Sprintf("zmux application error %d", e.Code)
}

func (e *ApplicationError) Is(err error) bool {
	if e == nil {
		return false
	}
	other, ok := findError[*ApplicationError](err)
	return ok && other.Code == e.Code
}

func (e *ApplicationError) ApplicationCode() uint64 {
	if e == nil {
		return 0
	}
	return e.Code
}

// Scope identifies whether an error belongs to the session or a stream.
type Scope uint8

const (
	ScopeUnknown Scope = iota
	ScopeSession
	ScopeStream
)

func (s Scope) String() string {
	switch s {
	case ScopeSession:
		return "session"
	case ScopeStream:
		return "stream"
	default:
		return "unknown"
	}
}

// Operation identifies the API operation that failed.
type Operation uint8

const (
	OperationUnknown Operation = iota
	OperationOpen
	OperationAccept
	OperationRead
	OperationWrite
	OperationClose
)

func (o Operation) String() string {
	switch o {
	case OperationOpen:
		return "open"
	case OperationAccept:
		return "accept"
	case OperationRead:
		return "read"
	case OperationWrite:
		return "write"
	case OperationClose:
		return "close"
	default:
		return "unknown"
	}
}

// Source identifies where the error came from.
type Source uint8

const (
	SourceUnknown Source = iota
	SourceLocal
	SourceRemote
	SourceTransport
)

func (s Source) String() string {
	switch s {
	case SourceLocal:
		return "local"
	case SourceRemote:
		return "remote"
	case SourceTransport:
		return "transport"
	default:
		return "unknown"
	}
}

// Direction identifies which stream direction the error applies to.
type Direction uint8

const (
	DirectionUnknown Direction = iota
	DirectionRead
	DirectionWrite
	DirectionBoth
)

func (d Direction) String() string {
	switch d {
	case DirectionRead:
		return "read"
	case DirectionWrite:
		return "write"
	case DirectionBoth:
		return "both"
	default:
		return "unknown"
	}
}

// TerminationKind identifies how the operation terminated.
type TerminationKind uint8

const (
	TerminationUnknown TerminationKind = iota
	TerminationGraceful
	TerminationStopped
	TerminationReset
	TerminationAbort
	TerminationSessionTermination
)

func (k TerminationKind) String() string {
	switch k {
	case TerminationGraceful:
		return "graceful"
	case TerminationStopped:
		return "stopped"
	case TerminationReset:
		return "reset"
	case TerminationAbort:
		return "abort"
	case TerminationSessionTermination:
		return "session_termination"
	default:
		return "unknown"
	}
}

// Error is the structured error wrapper returned by public APIs.
type Error struct {
	Scope           Scope
	Operation       Operation
	Source          Source
	WireCode        uint64
	ReasonText      string
	Direction       Direction
	TerminationKind TerminationKind
	Err             error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	if e.Scope != ScopeUnknown || e.Operation != OperationUnknown {
		return fmt.Sprintf("zmux %s %s error", e.Scope, e.Operation)
	}
	return "zmux error"
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// AsStructuredError returns the structured error wrapper when present.
func AsStructuredError(err error) (*Error, bool) {
	out, ok := findError[*Error](err)
	if !ok {
		return nil, false
	}
	return out, true
}

type WireError = wire.Error

func wireError(code ErrorCode, op string, err error) error {
	return wire.WrapError(code, op, err)
}

// ErrorCodeOf returns the wire or application error code when one is present.
func ErrorCodeOf(err error) (ErrorCode, bool) {
	if code, ok := wire.ErrorCodeOf(err); ok {
		return code, true
	}
	appErr, ok := findError[*ApplicationError](err)
	if !ok {
		return 0, false
	}
	return ErrorCode(appErr.Code), true
}

// IsErrorCode reports whether err carries the given wire or application code.
func IsErrorCode(err error, code ErrorCode) bool {
	if wire.IsErrorCode(err, code) {
		return true
	}
	appErr, ok := findError[*ApplicationError](err)
	return ok && appErr.Code == uint64(code)
}

var (
	errInvalidRole        = wire.ErrInvalidRole
	errNonCanonicalVarint = wire.ErrNonCanonicalVarint
	errTruncatedVarint    = wire.ErrTruncatedVarint
	errInvalidFrameType   = wire.ErrInvalidFrameType
	errPayloadTooLarge    = wire.ErrPayloadTooLarge
)

type errorMeta struct {
	scope           Scope
	operation       Operation
	source          Source
	direction       Direction
	terminationKind TerminationKind
}

func reframeStructuredError(err error, meta errorMeta) error {
	if err == nil {
		return nil
	}

	existing, ok := findError[*Error](err)
	if !ok {
		return wrapStructuredError(err, meta)
	}

	cloned := *existing
	if meta.scope != ScopeUnknown {
		cloned.Scope = meta.scope
	}
	if meta.operation != OperationUnknown {
		cloned.Operation = meta.operation
	}
	if meta.source != SourceUnknown {
		cloned.Source = meta.source
	}
	if meta.direction != DirectionUnknown {
		cloned.Direction = meta.direction
	}
	if meta.terminationKind != TerminationUnknown {
		cloned.TerminationKind = meta.terminationKind
	}
	code, reason := errorDetails(err)
	if cloned.WireCode == 0 && code != 0 {
		cloned.WireCode = code
	}
	if cloned.ReasonText == "" {
		cloned.ReasonText = reason
	}
	return &cloned
}

func wrapStructuredError(err error, meta errorMeta) error {
	if err == nil {
		return nil
	}

	code, reason := errorDetails(err)

	if existing, ok := findError[*Error](err); ok {
		cloned := *existing
		if cloned.Scope == ScopeUnknown {
			cloned.Scope = meta.scope
		}
		if cloned.Operation == OperationUnknown {
			cloned.Operation = meta.operation
		}
		if cloned.Source == SourceUnknown {
			cloned.Source = meta.source
		}
		if cloned.Direction == DirectionUnknown {
			cloned.Direction = meta.direction
		}
		if cloned.TerminationKind == TerminationUnknown {
			cloned.TerminationKind = meta.terminationKind
		}
		if cloned.WireCode == 0 && code != 0 {
			cloned.WireCode = code
		}
		if cloned.ReasonText == "" {
			cloned.ReasonText = reason
		}
		return &cloned
	}

	return &Error{
		Scope:           meta.scope,
		Operation:       meta.operation,
		Source:          meta.source,
		WireCode:        code,
		ReasonText:      reason,
		Direction:       meta.direction,
		TerminationKind: meta.terminationKind,
		Err:             err,
	}
}

func errorDetails(err error) (uint64, string) {
	if appErr, ok := findError[*ApplicationError](err); ok {
		return appErr.Code, appErr.Reason
	}
	if code, ok := ErrorCodeOf(err); ok {
		return uint64(code), err.Error()
	}
	return 0, ""
}

func sessionOperationErr(c *Conn, op Operation, err error) error {
	if err == nil {
		return nil
	}
	return reframeStructuredError(err, errorMeta{
		scope:           ScopeSession,
		operation:       op,
		source:          sessionErrorSource(c, err),
		direction:       DirectionBoth,
		terminationKind: TerminationSessionTermination,
	})
}

func sessionOperationErrLocked(c *Conn, op Operation, err error) error {
	if err == nil {
		return nil
	}
	return reframeStructuredError(err, errorMeta{
		scope:           ScopeSession,
		operation:       op,
		source:          sessionErrorSourceLocked(c, err),
		direction:       DirectionBoth,
		terminationKind: TerminationSessionTermination,
	})
}

func readLoopSessionErr(err error) error {
	if err == nil {
		return nil
	}
	wireErr, ok := findError[*wire.Error](err)
	if !ok || !strings.HasPrefix(wireErr.Op, "validate ") {
		return err
	}
	return reframeStructuredError(err, errorMeta{
		scope:           ScopeSession,
		source:          SourceRemote,
		direction:       DirectionRead,
		terminationKind: TerminationSessionTermination,
	})
}

func openOperationErrLocked(c *Conn, err error) error {
	if err == nil {
		return nil
	}
	meta := errorMeta{
		scope:     ScopeSession,
		operation: OperationOpen,
		source:    SourceLocal,
		direction: DirectionBoth,
	}
	appErr, ok := findError[*ApplicationError](err)
	switch {
	case ok && appErr.Code == uint64(CodeRefusedStream):
		meta.source = SourceRemote
	case ok:
		meta.source = SourceLocal
	default:
		if _, ok := findError[*wire.Error](err); ok {
			meta.source = sessionErrorSourceLocked(c, err)
		}
	}
	return reframeStructuredError(err, meta)
}

func sessionWireErrorSource(err error) (Source, bool) {
	wireErr, ok := findError[*wire.Error](err)
	if !ok {
		return SourceUnknown, false
	}
	switch {
	case strings.HasPrefix(wireErr.Op, "send "),
		strings.HasPrefix(wireErr.Op, "marshal "),
		strings.HasPrefix(wireErr.Op, "build "),
		strings.HasPrefix(wireErr.Op, "validate "):
		return SourceLocal, true
	case strings.HasPrefix(wireErr.Op, "parse "),
		strings.HasPrefix(wireErr.Op, "read "),
		strings.HasPrefix(wireErr.Op, "handle "),
		strings.HasPrefix(wireErr.Op, "resolve "),
		strings.HasPrefix(wireErr.Op, "negotiate "):
		return SourceRemote, true
	default:
		return SourceUnknown, false
	}
}

func sessionErrorSourceWithPeerClose(err error, peerClose *ApplicationError) Source {
	switch {
	case err == nil:
		return SourceUnknown
	case isError(err, io.EOF), isError(err, io.ErrClosedPipe):
		return SourceTransport
	}
	if structured, ok := findError[*Error](err); ok && structured.Source != SourceUnknown {
		return structured.Source
	}
	if source, ok := sessionWireErrorSource(err); ok {
		return source
	}

	if appErr, ok := findError[*ApplicationError](err); ok {
		if peerClose != nil && peerClose.Code == appErr.Code && peerClose.Reason == appErr.Reason {
			return SourceRemote
		}
		return SourceLocal
	}
	if _, ok := ErrorCodeOf(err); ok {
		return SourceRemote
	}
	if isError(err, ErrSessionClosed) {
		if peerClose != nil {
			return SourceRemote
		}
		return SourceLocal
	}
	return SourceTransport
}

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

func isError(err, target error) bool {
	if target == nil {
		return err == nil
	}
	return isErrorDepth(err, target, 0)
}

func isErrorDepth(err, target error, depth int) bool {
	if err == nil || depth > maxErrorUnwrapDepth {
		return false
	}
	if sameError(err, target) {
		return true
	}
	if matcher, ok := err.(interface{ Is(error) bool }); ok && matcher.Is(target) {
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range wrapped.Unwrap() {
			if isErrorDepth(child, target, depth+1) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isErrorDepth(wrapped.Unwrap(), target, depth+1)
	}
	return false
}

func sameError(err, target error) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	return err == target
}

func sessionErrorSource(c *Conn, err error) Source {
	var peerClose *ApplicationError
	if c != nil {
		peerClose = c.sessionControl.peerCloseView.Load()
	}
	return sessionErrorSourceWithPeerClose(err, peerClose)
}

func sessionErrorSourceLocked(c *Conn, err error) Source {
	var peerClose *ApplicationError
	if c != nil {
		peerClose = c.sessionControl.peerCloseView.Load()
	}
	return sessionErrorSourceWithPeerClose(err, peerClose)
}

func (s *nativeStream) readSurfaceErrLocked(err error) error {
	if err == nil {
		return nil
	}
	if isError(err, io.EOF) {
		return err
	}
	meta := errorMeta{
		scope:     ScopeStream,
		operation: OperationRead,
		direction: DirectionRead,
	}
	if termination := s.readTerminationMetaLocked(); termination.ready() {
		meta.source = termination.source
		meta.direction = termination.direction
		meta.terminationKind = termination.kind
	} else {
		meta.source = SourceLocal
	}
	return wrapStructuredError(err, meta)
}

func (s *nativeStream) writeSurfaceErrLocked(err error) error {
	if err == nil {
		return nil
	}
	meta := errorMeta{
		scope:     ScopeStream,
		operation: OperationWrite,
		direction: DirectionWrite,
	}
	switch {
	case isError(err, ErrStreamNotWritable):
		meta.source = SourceLocal
	case s != nil:
		if termination := s.writeTerminationMetaLocked(err); termination.ready() {
			meta.source = termination.source
			meta.direction = termination.direction
			meta.terminationKind = termination.kind
			break
		}
		meta.source = SourceLocal
	default:
		meta.source = SourceLocal
	}
	return wrapStructuredError(err, meta)
}

func (s *nativeStream) closeSurfaceErr(err error) error {
	if err == nil {
		return nil
	}
	meta := errorMeta{
		scope:     ScopeStream,
		operation: OperationClose,
		source:    SourceLocal,
	}
	switch {
	case isError(err, ErrStreamNotReadable):
		meta.direction = DirectionRead
	case isError(err, ErrStreamNotWritable):
		meta.direction = DirectionWrite
	default:
		meta.direction = DirectionBoth
	}
	if s != nil {
		if termination := s.closeTerminationMetaLocked(err); termination.ready() {
			meta.source = termination.source
			meta.direction = termination.direction
			meta.terminationKind = termination.kind
		}
	}
	return wrapStructuredError(err, meta)
}

func (s *nativeStream) closeOperationErr(err error) error {
	if err == nil {
		return nil
	}
	if structured, ok := findError[*Error](err); ok && structured.Scope == ScopeSession {
		return sessionOperationErr(s.conn, OperationClose, err)
	}
	return reframeStructuredError(s.closeSurfaceErr(err), errorMeta{
		scope:     ScopeStream,
		operation: OperationClose,
	})
}
