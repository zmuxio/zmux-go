package zmux

import (
	"net/http"
	"strings"
)

// HTTPClass is the carrier class of an HTTP request on a unified endpoint,
// decided by its head alone.
type HTTPClass uint8

const (
	// HTTPClassNone is a request that is not a carrier: the host's.
	HTTPClassNone HTTPClass = iota
	// HTTPClassWebSocket is a WebSocket opening handshake: a GET with
	// Upgrade: websocket, or an extended CONNECT for websocket.
	HTTPClassWebSocket
	// HTTPClassWebTransport is an extended CONNECT for webtransport (or
	// a versioned token such as webtransport-h3).
	HTTPClassWebTransport
	// HTTPClassDownlink is a GET with Zmux-Pipe-Pair or Zmux-Pipe-Join.
	HTTPClassDownlink
	// HTTPClassUplink is a POST with Zmux-Pipe-Pair (a split bootstrap's
	// uplink), or one without a Content-Length that carries
	// Zmux-Pipe-Join.
	HTTPClassUplink
	// HTTPClassExchange is a POST with a Content-Length, an exchange carrier.
	HTTPClassExchange
	// HTTPClassBidirectional is a POST without a Content-Length.
	HTTPClassBidirectional
)

var httpClassNames = [...]string{"none", "websocket", "webtransport", "downlink", "uplink", "exchange", "bidirectional"}

func (c HTTPClass) String() string {
	if int(c) < len(httpClassNames) {
		return httpClassNames[c]
	}
	return "HTTPClass(?)"
}

// ClassifyHTTP classifies a request by its head alone, before any body
// byte is read, by the first of its rules that matches. It
// is the one classification of every HTTP adapter: NewHTTPHandler serves
// the downlink, uplink and bidirectional classes and HTTPEndpoint
// dispatches all of them.
func ClassifyHTTP(r *http.Request) HTTPClass {
	switch r.Method {
	case http.MethodGet:
		switch {
		case r.ProtoMajor == 1 && headerHasToken(r.Header, "Upgrade", "websocket"):
			return HTTPClassWebSocket
		case r.Header.Get(HeaderPair) != "" || r.Header.Get(HeaderJoin) != "":
			return HTTPClassDownlink
		}
	case http.MethodConnect:
		// net/http gives an HTTP/2 extended CONNECT's :protocol as a
		// header field, HTTP/3 servers as the request's Proto.
		proto := r.Header.Get(":protocol")
		if proto == "" && !strings.HasPrefix(r.Proto, "HTTP/") {
			proto = r.Proto
		}
		switch {
		case strings.EqualFold(proto, "websocket"):
			return HTTPClassWebSocket
		case strings.EqualFold(proto, "webtransport-h3"), strings.EqualFold(proto, "webtransport"):
			// webtransport is the name of earlier drafts.
			return HTTPClassWebTransport
		}
	case http.MethodPost:
		switch {
		case r.Header.Get(HeaderPair) != "":
			return HTTPClassUplink
		case r.ContentLength >= 0:
			return HTTPClassExchange
		case r.Header.Get(HeaderJoin) != "":
			return HTTPClassUplink
		}
		return HTTPClassBidirectional
	}
	return HTTPClassNone
}

// headerHasToken reports whether a comma-separated field of h holds token
// (case-insensitively).
func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for t := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// HTTPEndpoint serves every HTTP carrier kind on one request target
// (API.md 4.8): it classifies each request with
// ClassifyHTTP and hands it to the adapter of its class, which runs its
// own admission and serves what it does not accept with its own fallback.
// It adds no behaviour of its own: a request of a class without an adapter,
// and every request that is not a carrier, goes to Next. Give each adapter
// the same host handler as its HTTPOptions.Next (HTTPExchangeConfig.Next)
// so that every request that is not taken reaches the host, with the body
// bytes the adapter read replayed.
//
// Behind a reverse proxy that speaks HTTP/2 to the server without TLS,
// serve the endpoint with prior-knowledge HTTP/2 enabled, so that
// bidirectional carriers pass:
//
//	srv := &http.Server{Handler: endpoint, Protocols: new(http.Protocols)}
//	srv.Protocols.SetHTTP1(true)
//	srv.Protocols.SetUnencryptedHTTP2(true)
type HTTPEndpoint struct {
	// Stream serves downlinks, uplinks and bidirectional carriers: the
	// handler of NewHTTPHandler.
	Stream http.Handler
	// Exchange serves exchange requests: an *HTTPExchangeHandler.
	Exchange http.Handler
	// WebSocket serves WebSocket opening handshakes, such as
	// zmuxws.Handler.
	WebSocket http.Handler
	// WebTransport serves WebTransport sessions, such as zmuxh3.Handler.
	WebTransport http.Handler
	// Next is the host's handler (nil: http.NotFound). It is served as
	// HTTPOptions.Fallback serves: an HTTP/1.1 request with a body arrives
	// full duplex.
	Next http.Handler
}

// ServeHTTP dispatches r by its class.
func (e *HTTPEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var h http.Handler
	switch ClassifyHTTP(r) {
	case HTTPClassWebSocket:
		h = e.WebSocket
	case HTTPClassWebTransport:
		h = e.WebTransport
	case HTTPClassDownlink, HTTPClassUplink, HTTPClassBidirectional:
		h = e.Stream
	case HTTPClassExchange:
		h = e.Exchange
	case HTTPClassNone:
		// Not a zmux request: the host's handler.
	}
	if h == nil {
		serveFallback(e.Next, w, r)
		return
	}
	h.ServeHTTP(w, r)
}
