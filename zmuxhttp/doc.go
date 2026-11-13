// Package zmuxhttp carries zmux sessions over HTTP requests, in three
// carrier kinds: a stream carrier is one request whose body carries the up
// direction and whose response body the down direction (StreamPipe,
// StreamCandidate); a split carrier pairs an uplink POST and a downlink GET
// by a pairing field (SplitPipe, SplitJoin, SplitCandidate); an exchange
// carrier is a sequence of short authenticated requests for paths that
// buffer or cut long ones (ExchangePipe, ExchangeCandidate). On the server,
// NewHandler and NewExchangeHandler hand the carriers to a zmux.Listener,
// whose admission decides on each, and Endpoint serves every kind on one
// request target; Options configures them, and the WebSocket and HTTP/3
// adapters share it. Redirects are followed as CheckRedirect allows.
package zmuxhttp
