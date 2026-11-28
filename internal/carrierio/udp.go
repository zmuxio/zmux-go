package carrierio

import (
	"context"
	"net"
	"net/netip"
	"strconv"
)

// DialPacket resolves addr (host:port; the host may not be empty, as a
// dial needs a destination) within ctx, opens an unconnected UDP socket
// bound to the wildcard of the destination's family and calls dial with
// it and the destination. A QUIC dial over such a socket never binds a
// dual-stack wildcard, which Darwin may give the port of a local IPv4
// listener, so that handshake replies to a local server return to that
// listener itself. DialPacket owns the socket: it closes it when dial
// fails, else once the returned connection's Context ends (after the
// connection's last write). The returned channel closes after the socket
// closes.
func DialPacket[C interface{ Context() context.Context }](ctx context.Context, addr string, dial func(pc net.PacketConn, dst net.Addr) (C, error)) (C, <-chan struct{}, error) {
	var zero C
	socket, dst, err := udpSocket(ctx, addr)
	if err != nil {
		return zero, nil, err
	}
	c, err := dial(socket, dst)
	if err != nil {
		_ = socket.Close()
		return zero, nil, err
	}
	done := make(chan struct{})
	context.AfterFunc(c.Context(), func() {
		_ = socket.Close()
		close(done)
	})
	return c, done, nil
}

// udpSocket resolves addr and opens the socket DialPacket dials over.
func udpSocket(ctx context.Context, addr string) (*net.UDPConn, *net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, nil, err
	}
	if host == "" {
		return nil, nil, &net.AddrError{Err: "missing host", Addr: addr}
	}
	pn, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		n, lerr := net.DefaultResolver.LookupPort(ctx, "udp", port)
		if lerr != nil {
			return nil, nil, lerr
		}
		pn = uint64(n)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		ips, lerr := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if lerr != nil {
			return nil, nil, lerr
		}
		if len(ips) == 0 {
			return nil, nil, &net.DNSError{Err: "no addresses", Name: host, IsNotFound: true}
		}
		// IPv4 first, as net.ResolveUDPAddr picks for "udp".
		ip = ips[0]
		for _, a := range ips {
			if a.Unmap().Is4() {
				ip = a
				break
			}
		}
	}
	ip = ip.Unmap()
	network, local := "udp4", &net.UDPAddr{IP: net.IPv4zero}
	if ip.Is6() {
		network, local = "udp6", &net.UDPAddr{IP: net.IPv6unspecified}
	}
	socket, err := net.ListenUDP(network, local)
	if err != nil {
		return nil, nil, err
	}
	return socket, net.UDPAddrFromAddrPort(netip.AddrPortFrom(ip, uint16(pn))), nil
}
