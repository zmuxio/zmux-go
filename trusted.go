package zmux

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"sync"

	"github.com/djylb/netx/forwarded"
	"github.com/djylb/netx/proxyproto"
)

// TrustedProxies names the intermediaries in front of a server (reverse
// proxies, load balancers, CDNs) whose report of a carrier's client
// address is believed. The source of a carrier, which
// the admission sees as CarrierInfo.Source and the Listener counts in its
// per-source limits, is then the client address the intermediary reports;
// the same report from any other peer is ignored, so that a client cannot
// choose its own source.
//
// Two mechanisms carry the report, each enabled on its own:
//
//   - PROXY protocol: Listener wraps a net.Listener so that connections
//     from a trusted address start with a PROXY protocol header (version 1
//     or 2) and report its source as their RemoteAddr. Serve it with
//     Listener.Serve or an http.Server.
//   - A forwarding header field on HTTP carriers (Header): set
//     HTTPOptions.Trusted for the HTTP
//     adapters to read it on requests from a trusted address.
//
// The fields are read when an adapter is set up; a TrustedProxies must not
// be changed or copied afterwards.
type TrustedProxies struct {
	// Prefixes are the addresses (as /32 or /128 prefixes) and networks of
	// the trusted intermediaries; a peer outside them is its own source.
	Prefixes []netip.Prefix
	// Header is the forwarding header field the trusted intermediaries set
	// on HTTP requests: "X-Forwarded-For", "Forwarded" (RFC 7239),
	// "X-Real-IP", or the name of another field that holds the one client
	// address as X-Real-IP does. Empty: HTTP requests are not read.
	Header string
	// ProxyProtocol makes Listener expect a PROXY protocol header on every
	// connection from a trusted address.
	ProxyProtocol bool

	once sync.Once
	set  *forwarded.PrefixSet
	res  *forwarded.Resolver // nil without Header
}

// init builds the prefix set and the forwarding header resolver once.
func (t *TrustedProxies) init() {
	t.once.Do(func() {
		t.set = forwarded.NewPrefixSet(t.Prefixes...)
		if t.Header == "" {
			return
		}
		r := &forwarded.Resolver{Trusted: t.set}
		switch name := http.CanonicalHeaderKey(t.Header); name {
		case "X-Forwarded-For":
			r.Header = forwarded.HeaderXForwardedFor
		case "Forwarded":
			r.Header = forwarded.HeaderForwarded
		case "X-Real-Ip":
			r.Header = forwarded.HeaderXRealIP
		default:
			r.Header, r.Name = forwarded.HeaderXRealIP, name
		}
		t.res = r
	})
}

// resolver returns the forwarding header resolver, nil without Header.
func (t *TrustedProxies) resolver() *forwarded.Resolver {
	if t == nil {
		return nil
	}
	t.init()
	return t.res
}

// Trusts reports whether a is the address of a trusted intermediary: it
// lies in one of Prefixes (an IPv4-mapped IPv6 address as its IPv4
// address). A nil t trusts nobody.
func (t *TrustedProxies) Trusts(a netip.Addr) bool {
	if t == nil {
		return false
	}
	t.init()
	return t.set.Contains(a)
}

// Source returns the client of r, which arrived from peer: the address
// and port a trusted intermediary in front of the server reports in the
// Header field (port 0 when it reports none), or peer itself when peer is
// not trusted, Header is unset, or the field names no client. IPv4-mapped
// addresses are returned as IPv4, without a zone. A nil t returns peer;
// an invalid peer is returned as it is. It is the one resolver of the
// source of an HTTP carrier: the adapters call it and define none of
// their own.
func (t *TrustedProxies) Source(r *http.Request, peer netip.AddrPort) netip.AddrPort {
	if !peer.IsValid() {
		return peer
	}
	return t.resolver().Resolve(peer, r.Header).Addr // a nil resolver returns peer
}

func (t *TrustedProxies) validate() error {
	if t == nil {
		return nil
	}
	for _, p := range t.Prefixes {
		if !p.IsValid() {
			return fmt.Errorf("%w: TrustedProxies prefix %v", ErrInvalidConfig, p)
		}
	}
	t.init()
	return nil
}

// Listener returns ln with the PROXY protocol expected from the trusted
// addresses when ProxyProtocol is set, and ln itself otherwise. A
// connection from a trusted address that does not start with a header
// fails its first read; other peers are passed through unread.
func (t *TrustedProxies) Listener(ln net.Listener) net.Listener {
	if t == nil || !t.ProxyProtocol {
		return ln
	}
	return &proxyproto.Listener{Listener: ln, PolicyFor: proxyproto.TrustPrefixes(t.Prefixes...)}
}

// requestSource returns the source and the peer of r: the peer is
// r.RemoteAddr (zero when it is not an IP address and port, such as a Unix
// socket), the source what t.Source resolves from it.
func requestSource(t *TrustedProxies, r *http.Request) (source, peer netip.AddrPort) {
	peer, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return netip.AddrPort{}, netip.AddrPort{}
	}
	return t.Source(r, peer), peer
}
