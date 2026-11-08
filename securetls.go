package zmux

import (
	"crypto/tls"
	"io"
)

// TLSRecordCarrier is a sending half whose bytes go into a TLS record layer on a byte
// stream, so that records are aligned to TLS records. A *tls.Conn is one
// without saying so; a transform that keeps the raw records of one forwards the hint.
type TLSRecordCarrier interface{ TLSRecords() bool }

func tlsRecords(w io.Writer) bool {
	switch c := w.(type) {
	case *tls.Conn:
		return true
	case TLSRecordCarrier:
		return c.TLSRecords()
	}
	return false
}

// SecureTLS returns c as a carrier that declares the Bound contract (CarrierSecure): the
// host states that this TLS connection ends at the peer, with no proxy that terminates it.
// It is for a connection the host dialled or accepted directly; renegotiation stays off,
// the default of crypto/tls. The declaration is judged again after the handshake: TLS 1.3,
// or TLS 1.2 that did not resume, and a missing extended master secret fails the export.
func SecureTLS(c *tls.Conn) PipeIO {
	s := &secureTLSConn{Conn: c}
	return PipeIO{R: s, W: s}
}

type secureTLSConn struct{ *tls.Conn }

// CarrierSecure declares the contract; before the handshake it is only the host's word.
func (c *secureTLSConn) CarrierSecure() bool {
	st := c.ConnectionState()
	return !st.HandshakeComplete || st.Version >= tls.VersionTLS13 || st.Version == tls.VersionTLS12 && !st.DidResume
}

// TLSRecords is true: this is a TLS record layer.
func (c *secureTLSConn) TLSRecords() bool { return true }

// Abort closes the connection below TLS, which interrupts a blocked write.
func (c *secureTLSConn) Abort(error) { _ = c.NetConn().Close() }
