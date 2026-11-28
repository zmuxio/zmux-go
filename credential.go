package zmux

import (
	"context"
	"fmt"
	"io"
	"net"
)

// CarrierCredential is what a client presents on a carrier it opens, for the
// server's Admission: the client side of an Admission. info describes
// the carrier as CarrierInfo does on the server: Peer is the server's
// address and TLS the carrier's completed handshake (nil without TLS);
// StageHead is set for Header. admit.PrefixAuth is a CarrierCredential.
//
// The adapters present it: zmux.Client, Dial and DialTLS take
// Config.CarrierCredential, as the QUIC, WebSocket and WebTransport dialers of
// the sub-modules do; zmuxhttp.Streaming and zmuxhttp.Split take one; PresentPipe
// presents it on any other byte-stream pipe. Methods are called
// concurrently.
type CarrierCredential interface {
	// Prefix returns the bytes a carrier starts with, before any zmux
	// record: byte streams, HTTP request bodies, a QUIC connection's
	// first stream.
	Prefix(info *CarrierInfo) ([]byte, error)
	// Header returns the request header field of a carrier decided on its
	// HTTP request head (WebSocket, WebTransport, a request without a
	// body); info.TLS is the state of the connection that carries the
	// request.
	Header(info *CarrierInfo) (name, value string, err error)
}

// PresentPipe writes c's prefix on p, a pipe the client
// opened, before zmux writes on it; a nil c writes nothing. A TLS
// connection is first handshaken (HandshakeContext, bounded by ctx), so
// that its state can bind the prefix. A binary message carrier sends a
// prefix as its own message. A carrier decided on its request head instead
// presents c in the request (Header).
func PresentPipe(ctx context.Context, p PipeIO, c CarrierCredential) error {
	if c == nil {
		return nil
	}
	if p.W == nil {
		return fmt.Errorf("%w: a credential prefix needs a write half", ErrInvalidConfig)
	}
	if h, ok := p.W.(interface{ HandshakeContext(context.Context) error }); ok {
		if err := h.HandshakeContext(ctx); err != nil {
			return err
		}
	}
	info := &CarrierInfo{Framing: p.Framing}
	if ep, ok := p.W.(EarlyClientCarrier); ok {
		info.EarlyData = ep.SendsEarlyData()
	}
	if cs, ok := TerminatedTLS(p); ok {
		info.TLS = &cs
	}
	if ra, ok := p.W.(interface{ RemoteAddr() net.Addr }); ok && ra.RemoteAddr() != nil {
		info.Peer = addrPortOf(ra.RemoteAddr())
	}
	b, err := c.Prefix(info)
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	if p.Framing == FramingRecords {
		w, ok := p.W.(MessageWriter)
		if !ok {
			return fmt.Errorf("%w: a binary credential prefix needs a message writer", ErrInvalidConfig)
		}
		return w.WriteMessage(b)
	}
	n, err := p.W.Write(b)
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}
