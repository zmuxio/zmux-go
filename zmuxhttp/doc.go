// Package zmuxhttp carries zmux sessions over HTTP requests, in three
// carrier kinds, each a struct whose methods are the operations on it: a
// stream carrier is one request whose body carries the up direction and
// whose response body the down direction (Streaming); a split carrier pairs
// an uplink POST and a downlink GET by a pairing field (Split); an exchange
// carrier is a sequence of short authenticated requests for paths that
// buffer or cut long ones (Exchange, with ExchangeConfig for what both ends
// agree on). Each offers Pipe and Candidate (Split also Join). On the
// server, NewHandler and NewExchangeHandler hand the carriers to a
// zmux.Listener, whose admission decides on each, and Mux serves every kind
// on one request target; Options configures them (ExchangeOptions adds the
// exchange server's own settings), and the WebSocket and HTTP/3 adapters
// share it. Redirects are followed as CheckRedirect allows.
package zmuxhttp
