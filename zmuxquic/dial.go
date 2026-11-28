package zmuxquic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"github.com/quic-go/quic-go"

	"github.com/zmuxio/zmux-go/internal/carrierio"
)

// dialAddr keeps the destination's UDP family instead of binding a dual-stack
// wildcard. In particular, Darwin may assign that wildcard an IPv4 listener's
// port, causing local handshake replies to return to the listener itself.
func dialAddr(ctx context.Context, addr string, tlsConf *tls.Config, conf *quic.Config, early bool) (*quic.Conn, <-chan struct{}, error) {
	qc, socketDone, err := dialNative(ctx, addr, tlsConf, conf, early)
	if err != nil || !early {
		return qc, socketDone, err
	}
	if err = handshakeReady(ctx, qc); err != nil {
		_ = qc.CloseWithError(0, "")
		return nil, nil, err
	}
	return qc, socketDone, nil
}

// dialNative dials over a socket that carrierio.DialPacket owns. Production
// stream users enter through dialAddr/ConnPipe, which apply the handshake barrier.
func dialNative(ctx context.Context, addr string, tlsConf *tls.Config, conf *quic.Config, early bool) (*quic.Conn, <-chan struct{}, error) {
	if tlsConf == nil {
		return nil, nil, errors.New("zmuxquic: TLS config required")
	}
	if tlsConf.ServerName == "" {
		tlsConf = tlsConf.Clone()
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			host = addr
		}
		tlsConf.ServerName = host
	}
	return carrierio.DialPacket(ctx, addr, func(pc net.PacketConn, dst net.Addr) (*quic.Conn, error) {
		if early {
			return quic.DialEarly(ctx, pc, dst, tlsConf, conf)
		}
		return quic.Dial(ctx, pc, dst, tlsConf, conf)
	})
}

// handshakeReady keeps outgoing streams in 1-RTT. Opening native streams
// during 0-RTT has raced peer transport-parameter updates. Before restoring
// native early streams, run the preserved upstream reproducer under -race:
//
//	go test -race -tags=quicgo_upstream_repro ./zmuxquic -run TestQuicGoUpstreamEarlyStreamRejection -count=20
//
// The reproducer bypasses this barrier and is not a supported path; the
// default rejection test exercises the guarded adapter. The barrier gives up
// native 0-RTT payload latency only: zmux replayable and resumed startup, TLS
// validation, cancellation and socket ownership stay. It covers the default
// dialers and borrowed connections passed to ConnPipe and Client; incoming
// early data is unchanged. qStream.EarlyAccepted relies on it: every outgoing
// stream is created as 1-RTT, so its bytes were not invalidated by a 0-RTT
// rejection. When removing the barrier, decide the verdict per pipe for its
// initial bytes together with stream opening (a connection-wide
// Used0RTT=false does not mean that a pipe created as 1-RTT was rejected), and
// test accepted and rejected resumption, and remembered zmux parameters
// without a reusable TLS ticket.
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
	if err != nil {
		return nil, err
	}
	return stream, nil
}
