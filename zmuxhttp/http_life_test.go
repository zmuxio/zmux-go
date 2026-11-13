package zmuxhttp

import "testing"

// Once ServeHTTP has returned, closing a pipe's halves no longer uses the
// response controller (here nil: a use would panic).
func TestHTTPHalvesAfterServeHTTP(t *testing.T) {
	life := &httpLife{}
	life.end()
	body := &httpReqBody{life: life}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	hw := &httpRespWriter{life: life}
	if err := hw.Close(); err != nil {
		t.Fatal(err)
	}
}
