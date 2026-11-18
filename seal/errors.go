package seal

import "github.com/zmuxio/zmux-go/seal/proto"

// One sentinel per error class of the secure carrier (SPEC Section 9), for
// errors.Is; proto.ErrorClass names the class of an error. A failure of the
// handshake or of a record wraps the sentinel of its class. The classes
// credentials_failed and no reply have ErrCredentialsFailed and ErrNoReply.
var (
	ErrIncomplete         = proto.ErrIncomplete
	ErrMalformed          = proto.ErrMalformed
	ErrTrailing           = proto.ErrTrailing
	ErrUnknownProfile     = proto.ErrUnknownProfile
	ErrUnknownSuite       = proto.ErrUnknownSuite
	ErrBadFlags           = proto.ErrBadFlags
	ErrBadHandle          = proto.ErrBadHandle
	ErrAuth               = proto.ErrAuth
	ErrKex                = proto.ErrKex
	ErrPeerIsSelf         = proto.ErrPeerIsSelf
	ErrTooLarge           = proto.ErrTooLarge
	ErrExhausted          = proto.ErrExhausted
	ErrRandomFailed       = proto.ErrRandomFailed
	ErrTLVTruncated       = proto.ErrTLVTruncated
	ErrTLVNonMinimal      = proto.ErrTLVNonMinimal
	ErrTLVOrder           = proto.ErrTLVOrder
	ErrTLVUnknownCritical = proto.ErrTLVUnknownCritical
	ErrTLVCredentials     = proto.ErrTLVCredentials
	ErrMissingField       = proto.ErrMissingField
	ErrBadField           = proto.ErrBadField
	ErrBadConfirm         = proto.ErrBadConfirm
	ErrInvalidConfig      = proto.ErrInvalidConfig
	ErrState              = proto.ErrState
)
