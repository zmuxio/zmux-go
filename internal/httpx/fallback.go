package httpx

import "net/http"

// ServeFallback serves r with next (http.NotFound when nil). An HTTP/1.1
// request with a body is switched to full duplex first: net/http would
// otherwise read the rest of the body before the response header goes
// out, and a streaming body that the client keeps open until it sees a
// response would stall both sides. The error of a writer that cannot
// switch is ignored.
func ServeFallback(next http.Handler, w http.ResponseWriter, r *http.Request) {
	if r.Context().Err() != nil {
		return
	}
	if r.ProtoMajor == 1 && r.ContentLength != 0 && r.Body != nil && r.Body != http.NoBody {
		_ = http.NewResponseController(w).EnableFullDuplex()
	}
	if next == nil {
		http.NotFound(w, r)
		return
	}
	next.ServeHTTP(w, r)
}
