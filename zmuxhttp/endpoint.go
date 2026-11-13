package zmuxhttp

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/httpx"
)

// Class is the carrier class of an HTTP request on a unified endpoint,
// decided by its head alone.
type Class uint8

const (
	// ClassNone is a request that is not a carrier: the host's.
	ClassNone Class = iota
	// ClassWebSocket is a WebSocket opening handshake: a GET with
	// Upgrade: websocket, or an extended CONNECT for websocket.
	ClassWebSocket
	// ClassWebTransport is an extended CONNECT for webtransport (or
	// a versioned token such as webtransport-h3).
	ClassWebTransport
	// ClassDownlink is a GET with the pairing field (HeaderPair).
	ClassDownlink
	// ClassUplink is a POST with the pairing field (a split
	// bootstrap's uplink).
	ClassUplink
	// ClassExchange is a POST with a Content-Length, an exchange carrier.
	ClassExchange
	// ClassBidirectional is a POST without a Content-Length.
	ClassBidirectional
)

var httpClassNames = [...]string{"none", "websocket", "webtransport", "downlink", "uplink", "exchange", "bidirectional"}

func (c Class) String() string {
	if int(c) < len(httpClassNames) {
		return httpClassNames[c]
	}
	return "Class(?)"
}

// Classify classifies a request by its head alone, before any body
// byte is read, by the first of its rules that matches, with the pairing
// field HeaderPair; Options.Classify does so with the configured one.
// It is the one classification of every HTTP adapter: NewHandler
// serves the downlink, uplink and bidirectional classes and Endpoint
// dispatches all of them.
func Classify(r *http.Request) Class { return classifyHTTP(r, HeaderPair) }

// Classify is the package function Classify with o's pairing field
// (Options.PairField); a nil o classifies as it does.
func (o *Options) Classify(r *http.Request) Class {
	return classifyHTTP(r, o.pairField())
}

// classifyHTTP classifies r, pair being the canonical pairing field.
func classifyHTTP(r *http.Request, pair string) Class {
	switch r.Method {
	case http.MethodGet:
		switch {
		case r.ProtoMajor == 1 && headerHasToken(r.Header, "Upgrade", "websocket"):
			return ClassWebSocket
		case r.Header.Get(pair) != "":
			return ClassDownlink
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
			return ClassWebSocket
		case strings.EqualFold(proto, "webtransport-h3"), strings.EqualFold(proto, "webtransport"):
			// webtransport is the name of earlier drafts.
			return ClassWebTransport
		}
	case http.MethodPost:
		switch {
		case r.Header.Get(pair) != "":
			return ClassUplink
		case r.ContentLength >= 0:
			return ClassExchange
		}
		return ClassBidirectional
	}
	return ClassNone
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

// Endpoint serves every HTTP carrier kind on one request target:
// it classifies each request with Options.Classify and hands it to the
// adapter of its class, which runs its own admission and serves what it
// does not accept with its own fallback. It adds no behaviour of its own:
// a request of a class without an adapter, and every request that is not
// a carrier, goes to Options.Fallback. Options is the one value of
// the endpoint: make every adapter with the same value (NewHandler,
// NewExchangeHandler, zmuxws.NewHandler, zmuxh3.HTTP3Handler), so that
// they classify alike and every request that is not taken reaches the
// host, with the body bytes the adapter read replayed.
//
// Behind a reverse proxy that speaks HTTP/2 to the server without TLS,
// serve the endpoint with prior-knowledge HTTP/2 enabled, so that
// bidirectional carriers pass:
//
//	srv := &http.Server{Handler: endpoint, Protocols: new(http.Protocols)}
//	srv.Protocols.SetHTTP1(true)
//	srv.Protocols.SetUnencryptedHTTP2(true)
type Endpoint struct {
	// Options are the endpoint's HTTP options (nil: the defaults): their
	// PairField classifies requests and their Next (Options.Fallback)
	// serves the requests that no adapter takes.
	Options *Options
	// Stream serves downlinks, uplinks and bidirectional carriers: the
	// handler of NewHandler.
	Stream http.Handler
	// Exchange serves exchange requests: an *ExchangeHandler.
	Exchange http.Handler
	// WebSocket serves WebSocket opening handshakes, such as
	// zmuxws.NewHandler.
	WebSocket http.Handler
	// WebTransport serves WebTransport sessions, such as zmuxh3.NewHandler.
	WebTransport http.Handler
}

// ServeHTTP dispatches r by its class.
func (e *Endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var h http.Handler
	switch e.Options.Classify(r) {
	case ClassWebSocket:
		h = e.WebSocket
	case ClassWebTransport:
		h = e.WebTransport
	case ClassDownlink, ClassUplink, ClassBidirectional:
		h = e.Stream
	case ClassExchange:
		h = e.Exchange
	case ClassNone:
		// Not a zmux request: the host's handler.
	}
	if h == nil {
		e.Options.Fallback(w, r)
		return
	}
	h.ServeHTTP(w, r)
}

// pairField returns the canonical request field of o's pairing (HeaderPair
// for a nil o or an empty PairField).
func (o *Options) pairField() string {
	if o == nil || o.PairField == "" {
		return HeaderPair
	}
	return http.CanonicalHeaderKey(o.PairField)
}

// checkPairField checks a pairing field with the rule of
// httpx.CheckField.
func checkPairField(field string) error {
	if why := httpx.CheckField(field); why != "" {
		return fmt.Errorf("%w: pairing field %q %s", zmux.ErrInvalidConfig, field, why)
	}
	return nil
}
