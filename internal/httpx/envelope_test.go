package httpx

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticatedEnvelopeBelongsToVerifiedRequest(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	verified := WithAuthenticatedEnvelope(r)
	if !AuthenticatedEnvelope(verified) || AuthenticatedEnvelope(r) ||
		AuthenticatedEnvelope(verified.Clone(verified.Context())) {
		t.Fatal("envelope authentication must apply only to the verified request")
	}
	ctx := WithoutAuthenticatedEnvelope(verified.Context())
	if ctx.Value(authenticatedEnvelopeKey{}) != nil {
		t.Fatal("callback context retained the envelope mark")
	}
}
