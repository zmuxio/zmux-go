package zmux

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
)

// The views zmux puts over a carrier half to replay what it read
// (PrefixedPipe, a carrier the listener accepted after its prefix, and
// Takeover.Pipe and Takeover.Conn) forward the capabilities of the half they
// view, so that a carrier keeps what it offers through every view. They do
// so through one mechanism, the embedded halfCaps (every view) and sendCaps
// (the sending half and the net.Conn view of Takeover); a capability is added here, and to
// the classification of carrierview_test.go, which fails for an exported
// interface of this package that is not classified.
//
// A view implements every capability of its set whether or not the half it
// views does; where that half lacks one, the method reports the absence in
// the capability's own terms: no TLS (a zero ConnectionState), false
// (CarrierSecure, ReportsFailure, TLSRecords), nil (CarrierConn,
// CarrierPeer, HandshakeContext), 0 (MaxRecord: no bound), not known
// (Unsent), errors.ErrUnsupported (CloseWrite,
// ExportKeyingMaterial), nothing to do (RecordCloseCode, CarrierTaken).
// Abort closes the connection (below TLS for a TLS connection), and
// WriteBuffers and WriteMessages write one buffer or message at a time.
// The framing capabilities (MessageReader, MessageWriter) are a view's
// own, by the carrier's framing. Capabilities that run zmux sessions on
// a carrier and have no neutral absence (LaneOpener, DatagramChannel,
// EarlyClientCarrier, EarlyServerCarrier, CarrierRecordMode, RecordSealer,
// RecordOpener) are not forwarded: a replayed carrier that zmux goes on
// with has its own sending half, where they are found, and a taken-over one
// reaches them through Unwrap, which returns the half under a view.
// Reading through Unwrap before the replayed bytes were read skips them.

// carrierView is a view of a carrier half.
type carrierView interface{ viewedHalf() any }

// unprefixed is the half under the views of x (x when it is none).
func unprefixed(x any) any {
	for {
		v, ok := x.(carrierView)
		if !ok {
			return x
		}
		x = v.viewedHalf()
	}
}

// halfCaps forwards the capabilities of every view to h, the half under
// the view (never a view itself).
type halfCaps struct{ h any }

func viewOf(x any) halfCaps { return halfCaps{h: unprefixed(x)} }

func (c halfCaps) viewedHalf() any { return c.h }

// ConnectionState is the viewed half's (ConnectionStater).
func (c halfCaps) ConnectionState() tls.ConnectionState {
	if s, ok := c.h.(ConnectionStater); ok {
		return s.ConnectionState()
	}
	return tls.ConnectionState{}
}

// HandshakeContext runs the viewed half's handshake (Handshaker); nil at
// once without one.
func (c halfCaps) HandshakeContext(ctx context.Context) error {
	if h, ok := c.h.(Handshaker); ok {
		return h.HandshakeContext(ctx)
	}
	return nil
}

// ExportKeyingMaterial exports from the viewed half's exporter, as
// Exporter finds it on that half.
func (c halfCaps) ExportKeyingMaterial(label string, context []byte, length int) ([]byte, error) {
	if e, ok := exporterOf(c.h); ok {
		return e.ExportKeyingMaterial(label, context, length)
	}
	return nil, errors.ErrUnsupported
}

// CarrierSecure is the viewed half's declaration (CarrierSecure).
func (c halfCaps) CarrierSecure() bool {
	s, ok := c.h.(CarrierSecure)
	return ok && s.CarrierSecure()
}

// ReportsFailure is the viewed half's (FailureReporter).
func (c halfCaps) ReportsFailure() bool {
	f, ok := c.h.(FailureReporter)
	return ok && f.ReportsFailure()
}

// CarrierConn is the viewed half's carrier connection (CarrierConner).
func (c halfCaps) CarrierConn() any {
	if cc, ok := c.h.(CarrierConner); ok {
		return cc.CarrierConn()
	}
	return nil
}

// RecordCloseCode passes the code to the viewed half (CloseCodeRecorder).
func (c halfCaps) RecordCloseCode(code ApplicationErrorCode, reason string) {
	if r, ok := c.h.(CloseCodeRecorder); ok {
		r.RecordCloseCode(code, reason)
	}
}

// MaxRecord is the viewed half's record bound (CarrierRecordLimit).
func (c halfCaps) MaxRecord() int {
	if l, ok := c.h.(CarrierRecordLimit); ok {
		return l.MaxRecord()
	}
	return 0
}

// CarrierPeer is the viewed half's authenticated peer (CarrierPeer).
func (c halfCaps) CarrierPeer() any {
	if p, ok := c.h.(CarrierPeer); ok {
		return p.CarrierPeer()
	}
	return nil
}

// CarrierTaken tells the viewed half that the listener took the carrier
// (CarrierTaker).
func (c halfCaps) CarrierTaken() {
	if t, ok := c.h.(CarrierTaker); ok {
		t.CarrierTaken()
	}
}

// sendCaps forwards, beside halfCaps, the capabilities of a sending half
// to the viewed one (the views of Takeover: its sending half, Conn).
type sendCaps struct{ halfCaps }

func (c sendCaps) writer() io.Writer { return c.h.(io.Writer) }

// CloseWrite half-closes the viewed half (CloseWriter).
func (c sendCaps) CloseWrite() error {
	if cw, ok := c.h.(CloseWriter); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// Abort aborts the viewed half (Aborter); without one it closes the
// connection below TLS for a TLS connection, else the half.
func (c sendCaps) Abort(cause error) {
	switch x := c.h.(type) {
	case Aborter:
		x.Abort(cause)
	case NetConner:
		_ = x.NetConn().Close()
	case io.Closer:
		_ = x.Close()
	}
}

// TLSRecords reports whether the viewed half's bytes go into a TLS
// record layer (TLSRecordCarrier, or a *tls.Conn).
func (c sendCaps) TLSRecords() bool { return tlsRecords(c.writer()) }

// Unsent is the viewed half's (UnsentReporter).
func (c sendCaps) Unsent() (uint64, bool) {
	if u, ok := c.h.(UnsentReporter); ok {
		return u.Unsent()
	}
	return 0, false
}

// CarrierSampler is for internal use by zmux modules.
func (c sendCaps) CarrierSampler() *carrierstats.Sampler {
	if sampler := carrierstats.ProviderOf(c.h); sampler != nil {
		return sampler
	}
	if w, ok := c.h.(io.WriteCloser); ok {
		return carrierstats.SocketSampler(w)
	}
	return nil
}

// WriteBuffers writes through the viewed half's WriteBuffers
// (BuffersWriter), else one Write per buffer.
func (c sendCaps) WriteBuffers(bufs [][]byte) (int, error) {
	if b, ok := c.h.(BuffersWriter); ok {
		return b.WriteBuffers(bufs)
	}
	n := 0
	for _, b := range bufs {
		m, err := c.writer().Write(b)
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// The views. A view of a receiving half is a prefixReader (byte stream),
// a prefixMessages (message-framed) or a readOnly (the receiving side of a
// carrier whose halves are one value: it closes nothing); prefixedConn is
// the net.Conn view of Takeover.Conn, and the owned* views below are the
// halves of Takeover.Pipe.

// prefixReader returns pre before the rest of r.
type prefixReader struct {
	halfCaps
	r   io.ReadCloser
	pre []byte
}

func (p *prefixReader) Read(b []byte) (int, error) {
	if len(p.pre) > 0 {
		n := copy(b, p.pre)
		p.pre = p.pre[n:]
		return n, nil
	}
	return p.r.Read(b)
}

func (p *prefixReader) Close() error { return p.r.Close() }

// Unwrap returns the carrier's own receiving half.
func (p *prefixReader) Unwrap() io.ReadCloser { return p.h.(io.ReadCloser) }

// prefixMessages returns the first message before the rest.
type prefixMessages struct {
	halfCaps
	r     io.ReadCloser
	first []byte
}

func (p *prefixMessages) Read([]byte) (int, error) { return 0, errors.New("zmux: message carrier") }
func (p *prefixMessages) Close() error             { return p.r.Close() }
func (p *prefixMessages) ReadMessage(limit int) ([]byte, error) {
	if p.first != nil {
		m := p.first
		p.first = nil
		if len(m) > limit {
			return nil, ErrMessageTooLarge
		}
		return m, nil
	}
	return p.r.(MessageReader).ReadMessage(limit)
}

// Unwrap returns the carrier's own receiving half.
func (p *prefixMessages) Unwrap() io.ReadCloser { return p.h.(io.ReadCloser) }

// readOnly closes nothing: the carrier is closed through its write half.
type readOnly struct {
	halfCaps
	r io.ReadCloser
}

func (r readOnly) Read(b []byte) (int, error) { return r.r.Read(b) }
func (r readOnly) Close() error               { return nil }

func (r readOnly) ReadMessage(limit int) ([]byte, error) {
	if mr, ok := r.r.(MessageReader); ok {
		return mr.ReadMessage(limit)
	}
	return noMessages{}.ReadMessage(limit)
}

// Unwrap returns the carrier's own receiving half.
func (r readOnly) Unwrap() io.ReadCloser { return r.h.(io.ReadCloser) }

// prefixConn returns the view of c that Takeover.Conn is: prefix read
// before the rest of c, its Close through o (the halves' one owner). Once
// the prefix is consumed, WriteTo and ReadFrom use c's own (splice and
// sendfile where c offers them).
func prefixConn(c net.Conn, prefix []byte, o *halfOwner) *prefixedConn {
	return &prefixedConn{Conn: c, sendCaps: sendCaps{viewOf(c)}, pre: prefix, o: o}
}

type prefixedConn struct {
	net.Conn
	sendCaps
	o   *halfOwner
	pre []byte
}

// Close closes the connection, once, through its owner.
func (p *prefixedConn) Close() error { p.o.closeAll(); return nil }

// NetConn is the connection below the viewed one (as a TLS connection's
// NetConn), or the viewed connection itself when it names none.
func (p *prefixedConn) NetConn() net.Conn {
	if n, ok := p.Conn.(NetConner); ok {
		return n.NetConn()
	}
	return p.Conn
}

func (p *prefixedConn) Read(b []byte) (int, error) {
	if len(p.pre) > 0 {
		n := copy(b, p.pre)
		p.pre = p.pre[n:]
		return n, nil
	}
	return p.Conn.Read(b)
}

// WriteTo writes the prefix, then copies c to w through w's ReadFrom.
func (p *prefixedConn) WriteTo(w io.Writer) (int64, error) {
	var n int64
	if len(p.pre) > 0 {
		m, err := w.Write(p.pre)
		n += int64(m)
		p.pre = p.pre[m:]
		if err != nil {
			return n, err
		}
	}
	m, err := io.Copy(w, p.Conn)
	return n + m, err
}

// ReadFrom writes r to c through c's ReadFrom where it has one.
func (p *prefixedConn) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := p.Conn.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(struct{ io.Writer }{p.Conn}, r)
}

// Unwrap returns the connection under the view; reading it before the
// replayed bytes are read skips them.
func (p *prefixedConn) Unwrap() net.Conn { return p.Conn }

// halfOwner is the one closer of a taken-over carrier's halves (Takeover):
// the views' Close, zmux's close when Fallback returns and the setup's
// closer all close through it, each half once, whoever asks first. A
// receiving half that is the sending half's value is closed with it.
type halfOwner struct {
	r, w         io.Closer // r nil when it is w; nil for a missing half
	rOnce, wOnce sync.Once
	rDone, wDone atomic.Bool
}

func newHalfOwner(p PipeIO) *halfOwner {
	o := &halfOwner{}
	if p.W != nil {
		o.w = p.W
	}
	if p.R != nil && !carrierio.SameHalf(p.R, p.W) {
		o.r = p.R
	}
	return o
}

func (o *halfOwner) closeR() {
	o.rOnce.Do(func() {
		if o.r != nil {
			carrierio.CloseHalf(o.r)
		}
		o.rDone.Store(true)
	})
}

func (o *halfOwner) closeW() {
	o.wOnce.Do(func() {
		if o.w != nil {
			carrierio.CloseHalf(o.w)
		}
		o.wDone.Store(true)
	})
}

func (o *halfOwner) closeAll() { o.closeW(); o.closeR() }

// closed reports that every half was closed: the carrier has ended. A
// receiving half of the sending half's value ends with it.
func (o *halfOwner) closed() bool { return o.wDone.Load() && (o.r == nil || o.rDone.Load()) }

// The halves of Takeover.Pipe: the carrier's own, their Close through the
// owner. A view of the receiving half (prefixReader, prefixMessages) is
// put over ownedReader or ownedMessages.

// ownedReader is a byte-stream receiving half.
type ownedReader struct {
	halfCaps
	r io.Reader
	o *halfOwner
}

func (r *ownedReader) Read(b []byte) (int, error) { return r.r.Read(b) }
func (r *ownedReader) Close() error               { r.o.closeR(); return nil }

// Unwrap returns the carrier's own receiving half.
func (r *ownedReader) Unwrap() io.ReadCloser { return r.h.(io.ReadCloser) }

// ownedMessages is a message-framed receiving half.
type ownedMessages struct {
	halfCaps
	o *halfOwner
}

func (r *ownedMessages) Read([]byte) (int, error) { return 0, errors.New("zmux: message carrier") }
func (r *ownedMessages) Close() error             { r.o.closeR(); return nil }
func (r *ownedMessages) ReadMessage(limit int) ([]byte, error) {
	return r.h.(MessageReader).ReadMessage(limit)
}

// Unwrap returns the carrier's own receiving half.
func (r *ownedMessages) Unwrap() io.ReadCloser { return r.h.(io.ReadCloser) }

// ownedWriter is a byte-stream sending half.
type ownedWriter struct {
	sendCaps
	o *halfOwner
}

func (w *ownedWriter) Write(b []byte) (int, error) { return w.writer().Write(b) }
func (w *ownedWriter) Close() error                { w.o.closeW(); return nil }

// Unwrap returns the carrier's own sending half.
func (w *ownedWriter) Unwrap() io.WriteCloser { return w.h.(io.WriteCloser) }

// ownedMessageWriter is a message-framed sending half.
type ownedMessageWriter struct {
	sendCaps
	o *halfOwner
}

func (w *ownedMessageWriter) Write([]byte) (int, error) {
	return 0, errors.New("zmux: message carrier")
}
func (w *ownedMessageWriter) Close() error { w.o.closeW(); return nil }
func (w *ownedMessageWriter) WriteMessage(b []byte) error {
	return w.h.(MessageWriter).WriteMessage(b)
}

// WriteMessages writes through the half's WriteMessages
// (MessageBatchWriter), else one WriteMessage per message.
func (w *ownedMessageWriter) WriteMessages(msgs [][]byte) error {
	if b, ok := w.h.(MessageBatchWriter); ok {
		return b.WriteMessages(msgs)
	}
	for _, m := range msgs {
		if err := w.WriteMessage(m); err != nil {
			return err
		}
	}
	return nil
}

// Unwrap returns the carrier's own sending half.
func (w *ownedMessageWriter) Unwrap() io.WriteCloser { return w.h.(io.WriteCloser) }

// takeoverPipe is p as Takeover.Pipe gives it: halves closed through o,
// prefix replayed; conn is the view of Takeover.Conn when p is one
// net.Conn byte stream (both halves are then that view).
func takeoverPipe(p PipeIO, prefix []byte, o *halfOwner) (q PipeIO, conn *prefixedConn) {
	q = p
	q.own = nil
	if nc, ok := p.R.(net.Conn); ok && p.Framing != FramingRecords && carrierio.SameHalf(p.W, p.R) {
		conn = prefixConn(nc, prefix, o)
		q.R, q.W = conn, conn
		return q, conn
	}
	records := p.Framing == FramingRecords
	if p.R != nil {
		if records {
			q.R = &ownedMessages{halfCaps: viewOf(p.R), o: o}
		} else {
			q.R = &ownedReader{halfCaps: viewOf(p.R), r: p.R, o: o}
		}
	}
	if p.W != nil {
		if records {
			q.W = &ownedMessageWriter{sendCaps: sendCaps{viewOf(p.W)}, o: o}
		} else {
			q.W = &ownedWriter{sendCaps: sendCaps{viewOf(p.W)}, o: o}
		}
	}
	return withPrefix(q, prefix), nil
}
