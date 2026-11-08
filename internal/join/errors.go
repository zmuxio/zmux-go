package join

import "errors"

// Failures of parsing and of the handshakes. Each has one error class in the
// specification; test with errors.Is.
var (
	ErrIncomplete     = errors.New("zmux join: more bytes are needed")
	ErrMalformed      = errors.New("zmux join: malformed message")
	ErrTrailing       = errors.New("zmux join: bytes after a Hello")
	ErrUnknownProfile = errors.New("zmux join: unknown profile")
	ErrUnknownSuite   = errors.New("zmux join: unknown suite")
	ErrBadFlags       = errors.New("zmux join: invalid flags")
	ErrBadHandle      = errors.New("zmux join: invalid handle or locator length")
	ErrAuth           = errors.New("zmux join: authentication failed")
	ErrKex            = errors.New("zmux join: invalid key exchange input")
	ErrPeerIsSelf     = errors.New("zmux join: peer static key equals the local one")
	ErrTooLarge       = errors.New("zmux join: message too large")
	ErrExhausted      = errors.New("zmux join: nonce space exhausted")
	ErrInvalidConfig  = errors.New("zmux join: invalid configuration")
	ErrState          = errors.New("zmux join: call out of turn")
	ErrRandomFailed   = errors.New("zmux join: random source failed")

	ErrTLVTruncated       = errors.New("zmux join: truncated payload item")
	ErrTLVNonMinimal      = errors.New("zmux join: non-shortest varint in payload")
	ErrTLVOrder           = errors.New("zmux join: payload types not strictly increasing")
	ErrTLVUnknownCritical = errors.New("zmux join: unknown payload type that must be understood")
	ErrTLVCredentials     = errors.New("zmux join: empty credential or CredType 0")

	ErrMissingField = errors.New("zmux join: required payload item missing")
	ErrBadField     = errors.New("zmux join: invalid payload item value")
	ErrBadConfirm   = errors.New("zmux join: confirmation record is not empty")
)
