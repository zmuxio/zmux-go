package zmux

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
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
	// HTTPClassDownlink is a GET with the pairing field (HeaderPair).
	HTTPClassDownlink
	// HTTPClassUplink is a POST with the pairing field (a split
	// bootstrap's uplink).
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
// byte is read, by the first of its rules that matches, with the pairing
// field HeaderPair; HTTPOptions.Classify does so with the configured one.
// It is the one classification of every HTTP adapter: NewHTTPHandler
// serves the downlink, uplink and bidirectional classes and HTTPEndpoint
// dispatches all of them.
func ClassifyHTTP(r *http.Request) HTTPClass { return classifyHTTP(r, HeaderPair) }

// Classify is ClassifyHTTP with o's pairing field (HTTPOptions.PairField);
// a nil o is ClassifyHTTP.
func (o *HTTPOptions) Classify(r *http.Request) HTTPClass {
	return classifyHTTP(r, o.pairField())
}

// classifyHTTP classifies r, pair being the canonical pairing field.
func classifyHTTP(r *http.Request, pair string) HTTPClass {
	switch r.Method {
	case http.MethodGet:
		switch {
		case r.ProtoMajor == 1 && headerHasToken(r.Header, "Upgrade", "websocket"):
			return HTTPClassWebSocket
		case r.Header.Get(pair) != "":
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
		case r.Header.Get(pair) != "":
			return HTTPClassUplink
		case r.ContentLength >= 0:
			return HTTPClassExchange
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

// HTTPEndpoint serves every HTTP carrier kind on one request target:
// it classifies each request with
// Options.Classify and hands it to the adapter of its class, which runs its
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
	// Options are the HTTPOptions of handlers that are wrapped in
	// middleware (nil: none), as HandlerOptions cannot see them. The
	// endpoint classifies with the PairField of the handlers it can see and
	// of Options; they must agree, or the endpoint serves 500 and Validate
	// returns ErrInvalidConfig.
	Options *HTTPOptions

	once sync.Once
	pair string
	err  error
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

// HandlerOptions returns the HTTPOptions of a handler made by NewHTTPHandler
// or NewHTTPExchangeHandler; ok is false for any other handler (such as one
// wrapped in middleware).
func HandlerOptions(h http.Handler) (o *HTTPOptions, ok bool) {
	p, ok := h.(interface{ HTTPOptions() *HTTPOptions })
	if !ok {
		return nil, false
	}
	return p.HTTPOptions(), true
}

// Validate checks that the pairing fields of Options, Stream and Exchange
// agree (ErrInvalidConfig otherwise); ServeHTTP checks it on first use.
func (e *HTTPEndpoint) Validate() error {
	_, err := e.pairField()
	return err
}

// pairField returns the pairing field the endpoint classifies by.
func (e *HTTPEndpoint) pairField() (string, error) {
	var pair string
	seen := false
	for _, o := range []*HTTPOptions{e.Options, handlerOpts(e.Stream), handlerOpts(e.Exchange)} {
		if o == nil {
			continue
		}
		if f := o.pairField(); !seen {
			pair, seen = f, true
		} else if f != pair {
			return "", fmt.Errorf("%w: HTTPEndpoint handlers disagree on the pairing field (%q, %q)", ErrInvalidConfig, pair, f)
		}
	}
	if !seen {
		pair = HeaderPair
	}
	return pair, nil
}

func handlerOpts(h http.Handler) *HTTPOptions {
	if h == nil {
		return nil
	}
	o, _ := HandlerOptions(h)
	return o
}

// ServeHTTP dispatches r by its class.
func (e *HTTPEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.once.Do(func() { e.pair, e.err = e.pairField() })
	if e.err != nil {
		http.Error(w, "zmux: misconfigured endpoint", http.StatusInternalServerError)
		return
	}
	var h http.Handler
	switch classifyHTTP(r, e.pair) {
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

// pairField returns the canonical request field of o's pairing (HeaderPair
// for a nil o or an empty PairField).
func (o *HTTPOptions) pairField() string {
	if o == nil || o.PairField == "" {
		return HeaderPair
	}
	return http.CanonicalHeaderKey(o.PairField)
}

// reservedPairFields are fields with a meaning of their own that a pairing
// field must not take over (lower case); prefixes are in reservedPairPrefixes.
var reservedPairFields = map[string]bool{
	"content-length": true, "content-type": true, "content-encoding": true, "transfer-encoding": true,
	"host": true, "connection": true, "upgrade": true, "keep-alive": true, "te": true, "trailer": true,
	"authorization": true, "cookie": true, "expect": true, "date": true, "early-data": true,
	"forwarded": true, "via": true, "location": true, "origin": true,
}

var reservedPairPrefixes = []string{"proxy-", "sec-websocket-", "x-forwarded-"}

// checkPairField reports whether field is empty or a header field name (an
// HTTP token) that no protocol field of HTTP, WebSocket or the adapters
// uses.
func checkPairField(field string) error {
	for i := 0; i < len(field); i++ {
		c := field[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z',
			strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return fmt.Errorf("%w: pairing field %q is not a header field name", ErrInvalidConfig, field)
		}
	}
	l := strings.ToLower(field)
	reserved := reservedPairFields[l] || l == strings.ToLower(HeaderPrefixAuth)
	for _, p := range reservedPairPrefixes {
		reserved = reserved || strings.HasPrefix(l, p)
	}
	if reserved {
		return fmt.Errorf("%w: pairing field %q is reserved", ErrInvalidConfig, field)
	}
	return nil
}
