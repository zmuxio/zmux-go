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
		"incomplete": proto.ErrIncomplete, "malformed": proto.ErrMalformed, "trailing_bytes": proto.ErrTrailing,
		"unknown_profile": proto.ErrUnknownProfile, "unknown_suite": proto.ErrUnknownSuite,
		"bad_flags": proto.ErrBadFlags, "bad_handle": proto.ErrBadHandle, "auth": proto.ErrAuth, "kex": proto.ErrKex,
		"peer_is_self": proto.ErrPeerIsSelf, "too_large": proto.ErrTooLarge, "exhausted": proto.ErrExhausted,
		"random_failed": proto.ErrRandomFailed, "tlv_truncated": proto.ErrTLVTruncated,
		"tlv_non_minimal": proto.ErrTLVNonMinimal, "tlv_order": proto.ErrTLVOrder,
		"tlv_unknown_critical": proto.ErrTLVUnknownCritical, "tlv_credentials": proto.ErrTLVCredentials,
		"missing_field": proto.ErrMissingField, "bad_field": proto.ErrBadField, "bad_confirm": proto.ErrBadConfirm,
		"credentials_failed": seal.ErrCredentialsFailed, "invalid_config": proto.ErrInvalidConfig, "state": proto.ErrState,
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
