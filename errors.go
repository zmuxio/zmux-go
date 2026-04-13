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
	var other *ApplicationError
	return errors.As(err, &other) && other.Code == e.Code
}

func (e *ApplicationError) ApplicationCode() uint64 {
	if e == nil {
		return 0
	}
	return e.Code
}

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

// Error is the repository-default structured local error shape.
//
// It wraps the underlying exported sentinel or ApplicationError so
// errors.Is/errors.As continue to work through the structured repository error.
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

func AsStructuredError(err error) (*Error, bool) {
	var out *Error
	if !errors.As(err, &out) {
		return nil, false
	}
	return out, true
}

type WireError = wire.Error

func wireError(code ErrorCode, op string, err error) error {
	return wire.WrapError(code, op, err)
}

func ErrorCodeOf(err error) (ErrorCode, bool) {
	return wire.ErrorCodeOf(err)
}

func IsErrorCode(err error, code ErrorCode) bool {
	if wire.IsErrorCode(err, code) {
		return true
	}
	var appErr *ApplicationError
	return errors.As(err, &appErr) && appErr.Code == uint64(code)
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

	var existing *Error
	if !errors.As(err, &existing) {
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

	var existing *Error
	if errors.As(err, &existing) {
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
	var appErr *ApplicationError
	if errors.As(err, &appErr) {
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
	var appErr *ApplicationError
	ok := errors.As(err, &appErr)
	switch {
	case ok && appErr.Code == uint64(CodeRefusedStream):
		meta.source = SourceRemote
	case ok:
		meta.source = SourceLocal
	default:
		var wireErr *wire.Error
		if errors.As(err, &wireErr) {
			meta.source = sessionErrorSourceLocked(c, err)
		}
	}
	return reframeStructuredError(err, meta)
}

func sessionWireErrorSource(err error) (Source, bool) {
	var wireErr *wire.Error
	if !errors.As(err, &wireErr) {
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
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrClosedPipe):
		return SourceTransport
	}
	if source, ok := sessionWireErrorSource(err); ok {
		return source
	}

	var appErr *ApplicationError
	if errors.As(err, &appErr) {
		if peerClose != nil && peerClose.Code == appErr.Code && peerClose.Reason == appErr.Reason {
			return SourceRemote
		}
		return SourceLocal
	}
	if _, ok := ErrorCodeOf(err); ok {
		return SourceRemote
	}
	if errors.Is(err, ErrSessionClosed) {
		return SourceUnknown
	}
	return SourceTransport
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
	if errors.Is(err, io.EOF) {
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
	case errors.Is(err, ErrStreamNotWritable):
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
	case errors.Is(err, ErrStreamNotReadable):
		meta.direction = DirectionRead
	case errors.Is(err, ErrStreamNotWritable):
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
	var structured *Error
	if errors.As(err, &structured) && structured.Scope == ScopeSession {
		return sessionOperationErr(s.conn, OperationClose, err)
	}
	return reframeStructuredError(s.closeSurfaceErr(err), errorMeta{
		scope:     ScopeStream,
		operation: OperationClose,
	})
}
