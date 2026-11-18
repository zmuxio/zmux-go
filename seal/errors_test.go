package seal_test

import (
	"testing"

	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

// Every error class of the secure carrier has an exported sentinel, and the
// class a sentinel names is the class proto.ErrorClass reports.
func TestSealSentinelsCoverEveryClass(t *testing.T) {
	want := map[string]error{
		"incomplete": seal.ErrIncomplete, "malformed": seal.ErrMalformed, "trailing_bytes": seal.ErrTrailing,
		"unknown_profile": seal.ErrUnknownProfile, "unknown_suite": seal.ErrUnknownSuite,
		"bad_flags": seal.ErrBadFlags, "bad_handle": seal.ErrBadHandle, "auth": seal.ErrAuth, "kex": seal.ErrKex,
		"peer_is_self": seal.ErrPeerIsSelf, "too_large": seal.ErrTooLarge, "exhausted": seal.ErrExhausted,
		"random_failed": seal.ErrRandomFailed, "tlv_truncated": seal.ErrTLVTruncated,
		"tlv_non_minimal": seal.ErrTLVNonMinimal, "tlv_order": seal.ErrTLVOrder,
		"tlv_unknown_critical": seal.ErrTLVUnknownCritical, "tlv_credentials": seal.ErrTLVCredentials,
		"missing_field": seal.ErrMissingField, "bad_field": seal.ErrBadField, "bad_confirm": seal.ErrBadConfirm,
		"credentials_failed": seal.ErrCredentialsFailed, "invalid_config": seal.ErrInvalidConfig, "state": seal.ErrState,
	}
	if len(want) != 24 { // the 23 classes of SPEC Section 9 and the local state class
		t.Fatalf("%d classes in the table", len(want))
	}
	for class, err := range want {
		if got, ok := proto.ErrorClass(err); !ok || got != class {
			t.Errorf("sentinel of %q is of class %q, %v", class, got, ok)
		}
	}
}
