package zmuxhttp

import (
	"errors"
	"net/http"
	"testing"

	"github.com/zmuxio/zmux-go"
)

type testHeaderCredential struct{}

func (testHeaderCredential) Prefix(*zmux.CarrierInfo) ([]byte, error) { return nil, nil }
func (testHeaderCredential) Header(*zmux.CarrierInfo) (string, string, error) {
	return "x-carrier-test", "value", nil
}

func TestNextBatchTransportCredentialConflict(t *testing.T) {
	var credential testHeaderCredential
	info := &zmux.CarrierInfo{Stage: zmux.StageHead}
	if _, _, err := CredentialHeader(credential, info, http.Header{"X-CARRIER-TEST": {"existing"}}); !errors.Is(err, zmux.ErrInvalidConfig) {
		t.Fatalf("case-insensitive conflict: %v", err)
	}
	if name, value, err := CredentialHeader(credential, info, nil); err != nil || name != "X-Carrier-Test" || value != "value" {
		t.Fatalf("credential header = %q %q, %v", name, value, err)
	}
}
