package httpx

import (
	"context"
	"net/http"
)

type authenticatedEnvelopeKey struct{}

type authenticatedEnvelope struct {
	request *http.Request
}

// WithAuthenticatedEnvelope marks an exchange request after its complete
// authentication. The mark is valid only on the request returned here.
func WithAuthenticatedEnvelope(r *http.Request) *http.Request {
	envelope := new(authenticatedEnvelope)
	envelope.request = r.WithContext(context.WithValue(r.Context(), authenticatedEnvelopeKey{}, envelope))
	return envelope.request
}

// AuthenticatedEnvelope reports whether the exchange adapter marked r itself.
func AuthenticatedEnvelope(r *http.Request) bool {
	envelope, _ := r.Context().Value(authenticatedEnvelopeKey{}).(*authenticatedEnvelope)
	return envelope != nil && envelope.request == r
}

// WithoutAuthenticatedEnvelope removes the mark from the context before it
// is exposed to admission or fallback callbacks.
func WithoutAuthenticatedEnvelope(ctx context.Context) context.Context {
	if _, ok := ctx.Value(authenticatedEnvelopeKey{}).(*authenticatedEnvelope); !ok {
		return ctx
	}
	return context.WithValue(ctx, authenticatedEnvelopeKey{}, nil)
}
