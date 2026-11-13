package zmux

import (
	"net/http"
	"net/netip"
	"testing"
)

// TestTrustedProxiesTrustsSource checks the exported view of the trust
// set: Trusts matches Prefixes (mapped IPv4 included) and Source believes
// the configured header field only from a trusted peer.
func TestTrustedProxiesTrustsSource(t *testing.T) {
	tp := &TrustedProxies{Prefixes: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, Header: "X-Forwarded-For"}
	for a, want := range map[string]bool{"10.1.2.3": true, "::ffff:10.1.2.3": true, "11.0.0.1": false, "2001:db8::1": false} {
		if got := tp.Trusts(netip.MustParseAddr(a)); got != want {
			t.Errorf("Trusts(%s) = %v, want %v", a, got, want)
		}
	}
	var none *TrustedProxies
	if none.Trusts(netip.MustParseAddr("10.1.2.3")) {
		t.Error("a nil TrustedProxies trusts an address")
	}
	r := &http.Request{Header: http.Header{"X-Forwarded-For": {"203.0.113.7"}}}
	for _, c := range []struct {
		tp   *TrustedProxies
		peer string
		want string
	}{
		{tp, "10.0.0.1:443", "203.0.113.7:0"}, // the intermediary reports no port
		{tp, "[::ffff:10.0.0.1]:443", "203.0.113.7:0"},
		{tp, "192.0.2.1:443", "192.0.2.1:443"},                                   // not trusted: the peer itself
		{&TrustedProxies{Prefixes: tp.Prefixes}, "10.0.0.1:443", "10.0.0.1:443"}, // no Header
		{nil, "[::ffff:192.0.2.1]:443", "192.0.2.1:443"},
	} {
		if got := c.tp.Source(r, netip.MustParseAddrPort(c.peer)); got != netip.MustParseAddrPort(c.want) {
			t.Errorf("Source(peer %s) = %v, want %s", c.peer, got, c.want)
		}
	}
	// A reported port is kept.
	fw := &TrustedProxies{Prefixes: tp.Prefixes, Header: "Forwarded"}
	rf := &http.Request{Header: http.Header{"Forwarded": {`for="203.0.113.7:4711"`}}}
	if got := fw.Source(rf, netip.MustParseAddrPort("10.0.0.1:443")); got != netip.MustParseAddrPort("203.0.113.7:4711") {
		t.Errorf("Forwarded with a port: %v", got)
	}
	// An invalid peer (not an IP connection) is returned as it is.
	if got := tp.Source(r, netip.AddrPort{}); got.IsValid() {
		t.Errorf("invalid peer: %v", got)
	}
	// A Unix socket peer has no address: source and peer are zero.
	if src, peer := tp.RequestSource(&http.Request{RemoteAddr: "@", Header: r.Header}); src.IsValid() || peer.IsValid() {
		t.Errorf("requestSource of a Unix peer = %v %v", src, peer)
	}
	src, peer := tp.RequestSource(&http.Request{RemoteAddr: "10.0.0.1:443", Header: r.Header})
	if src != netip.MustParseAddrPort("203.0.113.7:0") || peer != netip.MustParseAddrPort("10.0.0.1:443") {
		t.Errorf("requestSource behind a trusted proxy = %v %v", src, peer)
	}
}
