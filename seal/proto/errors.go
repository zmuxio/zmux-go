package proto

import "github.com/zmuxio/zmux-go/internal/join"

// Failures of parsing and of the handshake. Each has one error class in the
// specification; test with errors.Is. They are the errors of the shared core.
var (
	ErrIncomplete     = join.ErrIncomplete
	ErrMalformed      = join.ErrMalformed
	ErrTrailing       = join.ErrTrailing
	ErrUnknownProfile = join.ErrUnknownProfile
	ErrBadFlags       = join.ErrBadFlags
	ErrBadHandle      = join.ErrBadHandle
	ErrAuth           = join.ErrAuth
	ErrKex            = join.ErrKex
	ErrPeerIsSelf     = join.ErrPeerIsSelf
	ErrTooLarge       = join.ErrTooLarge
	ErrExhausted      = join.ErrExhausted
	ErrInvalidConfig  = join.ErrInvalidConfig
	ErrState          = join.ErrState
	ErrRandomFailed   = join.ErrRandomFailed

	ErrTLVTruncated       = join.ErrTLVTruncated
	ErrTLVNonMinimal      = join.ErrTLVNonMinimal
	ErrTLVOrder           = join.ErrTLVOrder
	ErrTLVUnknownCritical = join.ErrTLVUnknownCritical
	ErrTLVCredentials     = join.ErrTLVCredentials
)
