package proto

import (
	"errors"

	"github.com/zmuxio/zmux-go/internal/join"
)

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

// ErrorClass names the class of a handshake or record error (its wire
// vectors' name, a rejection's reason); ok is false for an error of no
// class of this package. The names are incomplete, malformed,
// trailing_bytes, unknown_profile, unknown_suite, peer_is_self,
// random_failed, invalid_config, missing_field, bad_field, bad_confirm,
// bad_flags, bad_handle, auth, kex, too_large, exhausted, tlv_truncated,
// tlv_non_minimal, tlv_order, tlv_unknown_critical, tlv_credentials,
// credentials_failed and state; later versions may add names, so a switch on them
// needs a default.
func ErrorClass(err error) (name string, ok bool) {
	for _, c := range errorClasses {
		if errors.Is(err, c.e) {
			return c.n, true
		}
	}
	return "", false
}

var errorClasses = []struct {
	e error
	n string
}{
	{ErrIncomplete, "incomplete"}, {ErrMalformed, "malformed"}, {ErrTrailing, "trailing_bytes"},
	{ErrUnknownProfile, "unknown_profile"}, {join.ErrUnknownSuite, "unknown_suite"},
	{ErrPeerIsSelf, "peer_is_self"}, {ErrRandomFailed, "random_failed"}, {ErrInvalidConfig, "invalid_config"},
	{join.ErrMissingField, "missing_field"}, {join.ErrBadField, "bad_field"}, {join.ErrBadConfirm, "bad_confirm"},
	{ErrBadFlags, "bad_flags"}, {ErrBadHandle, "bad_handle"}, {ErrAuth, "auth"}, {ErrKex, "kex"},
	{ErrTooLarge, "too_large"}, {ErrExhausted, "exhausted"}, {ErrTLVTruncated, "tlv_truncated"},
	{ErrTLVNonMinimal, "tlv_non_minimal"}, {ErrTLVOrder, "tlv_order"},
	{ErrTLVUnknownCritical, "tlv_unknown_critical"}, {ErrTLVCredentials, "tlv_credentials"},
	{ErrCredentialsFailed, "credentials_failed"}, {ErrState, "state"},
}
