package zmuxhttp

import (
	"net/http"

	"github.com/zmuxio/zmux-go"
)

// Options configure the HTTP adapters (NewHandler, the exchange
// handler, and HTTP upgrades such as WebSocket and WebTransport); the
// admission they run is their listener's (zmux.ListenerConfig.Admission).
type Options struct {
	// Next serves the requests that are not carriers: zmux.VerdictFallback, and
	// requests the adapter does not take (nil: http.NotFound).
	// An HTTP/1.1 request with a body reaches it full duplex
	// (http.ResponseController.EnableFullDuplex), so its response can go
	// out while the client still streams the body.
	Next http.Handler
	// Trusted names the intermediaries whose forwarding header field gives
	// a request's source (nil: the request's peer is its source). Without
	// it zmux takes r.RemoteAddr as it finds it, so a host that resolves
	// the source itself rewrites r.RemoteAddr before the handler runs and
	// leaves Trusted nil.
	Trusted *zmux.TrustedProxies
	// PairField is the request field that pairs the uplink POST and the
	// downlink GET of a split bootstrap (empty: HeaderPair), another name
	// only where an intermediary strips unknown fields. The client side
	// uses the same name (SplitOptions.PairField).
	PairField string
}

// Validate checks o (nil is valid); the HTTP adapters call it when they
// are set up.
func (o *Options) Validate() error {
	if o == nil {
		return nil
	}
	if err := checkPairField(o.PairField); err != nil {
		return err
	}
	return o.Trusted.Validate()
}

// Fallback serves r with Next (http.NotFound without one). An HTTP/1.1
// request with a body reaches Next full duplex (see serveFallback).
func (o *Options) Fallback(w http.ResponseWriter, r *http.Request) {
	var next http.Handler
	if o != nil {
		next = o.Next
	}
	serveFallback(next, w, r)
}

// serveFallback serves r with next (http.NotFound when nil). An HTTP/1.1
// request with a body is switched to full duplex first: net/http would
// otherwise read the rest of the body before the response header goes
// out, and a streaming body that the client keeps open until it sees a
// response would stall both sides. The error of a writer that cannot
// switch is ignored.
func serveFallback(next http.Handler, w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 1 && r.ContentLength != 0 && r.Body != nil && r.Body != http.NoBody {
		_ = http.NewResponseController(w).EnableFullDuplex()
	}
	if next == nil {
		http.NotFound(w, r)
		return
	}
	next.ServeHTTP(w, r)
}
