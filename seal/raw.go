package seal

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// rawCarrier makes the carrier of Bound or Trusted: raw records on the lower halves,
// with the mode and the handshake's exporter in place of the carrier's own TLS state.
// base is the carrier as dialled, whose capabilities the halves offer; lower is base with
// any bytes read beyond the handshake replayed.
func rawCarrier(base, lower zmux.PipeIO, mode zmux.RecordMode, exp *resultExporter) zmux.PipeIO {
	common := rawCommon{exp: exp, mode: mode, base: base}
	q := lower
	if lower.R != nil {
		r := &rawReader{ReadCloser: lower.R, rawCommon: common, same: any(base.R) == any(base.W)}
		q.R = r
		for _, x := range []any{base.W, base.R} {
			if ch, ok := x.(zmux.DatagramChannel); ok {
				q.R = &channelReader{rawReader: r, DatagramChannel: ch}
				break
			}
		}
	}
	if lower.W != nil {
		q.W = newRawWriter(lower.W, common)
	}
	return q
}

// rawCommon is what both halves report.
type rawCommon struct {
	exp  *resultExporter
	mode zmux.RecordMode
	base zmux.PipeIO
}

func (c *rawCommon) RecordMode() zmux.RecordMode { return c.mode }
func (c *rawCommon) CarrierPeer() any            { return c.exp.CarrierPeer() }
func (c *rawCommon) ExportKeyingMaterial(l string, ctx []byte, n int) ([]byte, error) {
	return c.exp.ExportKeyingMaterial(l, ctx, n)
}

func (c *rawCommon) addr(get func(any) (net.Addr, bool)) net.Addr {
	for _, x := range []any{c.base.R, c.base.W} {
		if a, ok := get(x); ok {
			return a
		}
	}
	return nil
}

// RemoteAddr is the carrier's peer address, nil without one.
func (c *rawCommon) RemoteAddr() net.Addr {
	return c.addr(func(x any) (net.Addr, bool) {
		a, ok := x.(interface{ RemoteAddr() net.Addr })
		if !ok {
			return nil, false
		}
		return a.RemoteAddr(), true
	})
}

// LocalAddr is the carrier's local address, nil without one.
func (c *rawCommon) LocalAddr() net.Addr {
	return c.addr(func(x any) (net.Addr, bool) {
		a, ok := x.(interface{ LocalAddr() net.Addr })
		if !ok {
			return nil, false
		}
		return a.LocalAddr(), true
	})
}

type rawReader struct {
	io.ReadCloser
	rawCommon
	same bool // one object with the writing half, which closes it
}

func (r *rawReader) Close() error {
	if r.same {
		return nil
	}
	return r.ReadCloser.Close()
}

// SetReadDeadline is the carrier's, for the admission's peek and a nested connection.
func (r *rawReader) SetReadDeadline(t time.Time) error {
	for _, x := range []any{r.ReadCloser, r.base.R} {
		if d, ok := x.(interface{ SetReadDeadline(time.Time) error }); ok {
			return d.SetReadDeadline(t)
		}
	}
	return errors.ErrUnsupported
}

// ReadMessage reads a message of a message carrier.
func (r *rawReader) ReadMessage() ([]byte, error) {
	mr, ok := r.ReadCloser.(zmux.MessageReader)
	if !ok {
		return nil, errNotMessages
	}
	return mr.ReadMessage()
}

// CarrierConn names the carrier's connection, nil without one.
func (r *rawReader) CarrierConn() any {
	for _, x := range []any{r.base.R, r.base.W} {
		if c, ok := x.(zmux.CarrierConner); ok {
			return c.CarrierConn()
		}
	}
	return nil
}

// channelReader adds the carrier's datagram channel.
type channelReader struct {
	*rawReader
	zmux.DatagramChannel
}

// rawWriter offers what every carrier can answer. The driver falls back to the TCP socket
// through NetConn, so the optional reports are on the types below.
type rawWriter struct {
	io.WriteCloser
	rawCommon
}

func newRawWriter(w io.WriteCloser, c rawCommon) io.WriteCloser {
	base := &rawWriter{WriteCloser: w, rawCommon: c}
	_, lanes := w.(zmux.LaneOpener)
	_, unsent := w.(zmux.UnsentReporter)
	_, retrans := w.(zmux.RetransReporter)
	switch {
	case lanes:
		return &laneWriter{reportingWriter{base}}
	case unsent || retrans:
		return &reportingWriter{base}
	}
	return base
}

// NetConn is the connection below a TLS or TCP carrier, for the socket's statistics.
func (w *rawWriter) NetConn() net.Conn {
	switch c := w.WriteCloser.(type) {
	case interface{ NetConn() net.Conn }:
		return c.NetConn()
	case net.Conn:
		return c
	}
	return nil
}

// SetWriteDeadline is the carrier's.
func (w *rawWriter) SetWriteDeadline(t time.Time) error {
	if d, ok := w.WriteCloser.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return d.SetWriteDeadline(t)
	}
	return errors.ErrUnsupported
}

// TLSRecords tells the session that records go into the carrier's TLS record layer.
func (w *rawWriter) TLSRecords() bool {
	switch c := w.WriteCloser.(type) {
	case *tls.Conn:
		return true
	case zmux.TLSRecordCarrier:
		return c.TLSRecords()
	}
	return false
}

// WriteMessage writes a message of a message carrier.
func (w *rawWriter) WriteMessage(m []byte) error {
	mw, ok := w.WriteCloser.(zmux.MessageWriter)
	if !ok {
		return errNotMessages
	}
	return mw.WriteMessage(m)
}

// WriteMessages writes a batch as the carrier does.
func (w *rawWriter) WriteMessages(ms [][]byte) error {
	if bw, ok := w.WriteCloser.(zmux.MessageBatchWriter); ok {
		return bw.WriteMessages(ms)
	}
	for _, m := range ms {
		if err := w.WriteMessage(m); err != nil {
			return err
		}
	}
	return nil
}

// WriteBuffers writes a batch in one write of the carrier.
func (w *rawWriter) WriteBuffers(bufs [][]byte) (int, error) {
	switch c := w.WriteCloser.(type) {
	case zmux.BuffersWriter:
		return c.WriteBuffers(bufs)
	case *tls.Conn:
	case net.Conn:
		nb := net.Buffers(bufs)
		n, err := nb.WriteTo(c)
		return int(n), err
	}
	return carrier.WriteFlat(w.WriteCloser, bufs)
}

// CloseWrite ends the sending direction where the carrier can.
func (w *rawWriter) CloseWrite() error {
	if cw, ok := w.WriteCloser.(zmux.CloseWriter); ok {
		return cw.CloseWrite()
	}
	return w.Close()
}

// Abort fails the carrier abruptly where it can.
func (w *rawWriter) Abort(cause error) {
	switch c := w.WriteCloser.(type) {
	case zmux.Aborter:
		c.Abort(cause)
	case *tls.Conn:
		_ = c.NetConn().Close()
	}
}

// reportingWriter forwards the carrier's own reports.
type reportingWriter struct{ *rawWriter }

func (w *reportingWriter) Unsent() (uint64, bool) {
	if u, ok := w.WriteCloser.(zmux.UnsentReporter); ok {
		return u.Unsent()
	}
	return 0, false
}

func (w *reportingWriter) Retransmitted() (uint64, bool) {
	if r, ok := w.WriteCloser.(zmux.RetransReporter); ok {
		return r.Retransmitted()
	}
	return 0, false
}

// laneWriter forwards lanes, which carry raw records in these modes.
type laneWriter struct{ reportingWriter }

func (w *laneWriter) OpenLane(ctx context.Context) (io.WriteCloser, error) {
	return w.WriteCloser.(zmux.LaneOpener).OpenLane(ctx)
}

var errNotMessages = errors.New("seal: not a message carrier")
