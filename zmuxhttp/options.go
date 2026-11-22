package zmuxhttp

import (
	"net/http"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/httpx"
)

// Options configure the HTTP adapters (NewHandler, the exchange
// handler, and HTTP upgrades such as WebSocket and WebTransport); the
// admission they run is their listener's (zmux.ListenerConfig.Admission).
type Options struct {
	// Next serves non-carrier requests and VerdictFallback on an HTTP
	// request, including a decision on its body. It receives all headers
	// and all body bytes before the adapter writes a carrier response.
	// When Next is nil, HTTP Fallback responds with 404.
	// VerdictTakeover always selects the byte-stream target.
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
	// uses the same name (Split.PairField).
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
// request with a body reaches Next full duplex .
func (o *Options) Fallback(w http.ResponseWriter, r *http.Request) {
	var next http.Handler
	if o != nil {
		next = o.Next
	}
	httpx.ServeFallback(next, w, r)
}
