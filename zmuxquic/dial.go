package zmuxquic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"github.com/quic-go/quic-go"
)

// dialAddr keeps the destination's UDP family instead of binding a dual-stack
// wildcard. In particular, Darwin may assign that wildcard an IPv4 listener's
// port, causing local handshake replies to return to the listener itself.
func dialAddr(ctx context.Context, addr string, tlsConf *tls.Config, conf *quic.Config, early bool) (*quic.Conn, error) {
	qc, err := dialNative(ctx, addr, tlsConf, conf, early)
	if err != nil || !early {
		return qc, err
	}
	if err = handshakeReady(ctx, qc); err != nil {
		_ = qc.CloseWithError(0, "")
		return nil, err
	}
	return qc, nil
}

// dialNative owns socket allocation and release only. Production stream users
// enter through dialAddr/OpenPipe, which apply the temporary handshake barrier.
func dialNative(ctx context.Context, addr string, tlsConf *tls.Config, conf *quic.Config, early bool) (*quic.Conn, error) {
	remote, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	if tlsConf == nil {
		return nil, errors.New("zmuxquic: TLS config required")
	}
	network := "udp4"
	local := &net.UDPAddr{IP: net.IPv4zero}
	if remote.IP != nil && remote.IP.To4() == nil {
		network = "udp6"
		local.IP = net.IPv6unspecified
	}
	socket, err := net.ListenUDP(network, local)
	if err != nil {
		return nil, err
	}
	if tlsConf.ServerName == "" {
		tlsConf = tlsConf.Clone()
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			host = addr
		}
		tlsConf.ServerName = host
	}
	var qc *quic.Conn
	if early {
		qc, err = quic.DialEarly(ctx, socket, remote, tlsConf, conf)
	} else {
		qc, err = quic.Dial(ctx, socket, remote, tlsConf, conf)
	}
	if err != nil {
		_ = socket.Close()
		return nil, err
	}
	// Conn.Context ends after the native run loop finishes, including any final
	// close write. NextConnection after 0-RTT rejection uses the same connection/context.
	// quic.Dial owns its single-use transport, while we own the supplied socket.
	context.AfterFunc(qc.Context(), func() { _ = socket.Close() })
	return qc, nil
}

// handshakeReady is a temporary quic-go v0.64.1 boundary: opening native streams
// during 0-RTT can race peer transport-parameter updates. Remove this barrier
// only after the preserved upstream rejection reproducer passes under -race.
func handshakeReady(ctx context.Context, qc *quic.Conn) error {
	if qc == nil {
		return errors.New("zmuxquic: nil QUIC connection")
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	select {
	case <-qc.HandshakeComplete():
		return nil
	case <-qc.Context().Done():
		return context.Cause(qc.Context())
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// openStream opens the first bidirectional stream of *qc after the handshake
// barrier. After a 0-RTT rejection the stream lives on the connection's next
// handle, which replaces *qc; both handles share one connection, so the
// caller's close of either covers it.
func openStream(ctx context.Context, qc **quic.Conn) (*quic.Stream, error) {
	if err := handshakeReady(ctx, *qc); err != nil {
		return nil, err
	}
	stream, err := (*qc).OpenStreamSync(ctx)
	if errors.Is(err, quic.Err0RTTRejected) {
		next, nextErr := (*qc).NextConnection(ctx)
		if nextErr != nil {
			return nil, nextErr
		}
		*qc = next
		stream, err = next.OpenStreamSync(ctx)
	}
	return stream, err
}
