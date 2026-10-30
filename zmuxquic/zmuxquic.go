// Package zmuxquic carries zmux pipes over QUIC (quic-go): a pipe pair is
// one bidirectional stream, its further lanes are unidirectional streams
// of the same connection (MULTIPATH.md 2.5), the TLS exporter of the
// connection authenticates joins, and a client may send its first streams
// as replayable early data (SPEC.md 6.1.2). Native stream creation currently
// waits for the QUIC handshake because of the pinned quic-go0-RTT race.
package zmuxquic

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

// carrierFailed is ZMUX_CARRIER_FAILED as a QUIC stream error code.
const carrierFailed = quic.StreamErrorCode(zmux.StreamCarrierFailed)

// QUICConfig returns a QUIC configuration sized for lanes: unidirectional
// stream credit for the lanes the endpoint carries, and a connection window
// above the stream windows of the lanes that can be blocked at once. base may
// be nil.
func QUICConfig(base *quic.Config) *quic.Config {
	c := &quic.Config{}
	if base != nil {
		c = base.Clone()
	}
	if c.MaxIncomingUniStreams == 0 {
		c.MaxIncomingUniStreams = 258
	}
	if c.MaxIncomingStreams == 0 {
		c.MaxIncomingStreams = 64
	}
	if c.MaxStreamReceiveWindow == 0 {
		c.MaxStreamReceiveWindow = 16 << 20
	}
	if c.MaxConnectionReceiveWindow == 0 {
		c.MaxConnectionReceiveWindow = 4 * c.MaxStreamReceiveWindow
	}
	return c
}

// Dial opens a QUIC connection to addr and starts a zmux client session on
// its first bidirectional stream, as zmux.DialTLS does over TCP. With
// cfg.EarlyData.Replayable and Remembered parameters, replayable startup is
// enabled; native streams wait for handshake completion until the quic-go
// early-stream race is fixed. The session owns the connection: it is closed when the session
// ends. qconf may be nil; QUICConfig sizes it for lanes.
func Dial(ctx context.Context, addr string, tlsConf *tls.Config, qconf *quic.Config, cfg *zmux.Config) (*zmux.Conn, error) {
	early := cfg != nil && cfg.EarlyData.Replayable && len(cfg.EarlyData.Remembered) > 0
	qc, err := dialAddr(ctx, addr, tlsConf, QUICConfig(qconf), early)
	if err != nil {
		return nil, err
	}
	cur := qc
	s, err := openStream(ctx, &cur)
	if err != nil {
		_ = qc.CloseWithError(0, "")
		return nil, err
	}
	clientLanes(cur)
	sp := &qStream{qc: cur, s: s}
	c, err := zmux.Start(ctx, zmux.PipeIO{R: (*qReader)(sp), W: sp}, zmux.RoleClient, cfg)
	if err != nil {
		closeDialled(qc, sp)
		return nil, err
	}
	context.AfterFunc(c.Context(), func() { closeDialled(qc, sp) })
	return c, nil
}

// closeDialled closes the connection Dial opened, and the one that
// replaced it after a 0-RTT rejection.
func closeDialled(qc *quic.Conn, sp *qStream) {
	sp.mu.Lock()
	sp.closed = true
	cur := sp.qc
	sp.mu.Unlock()
	_ = cur.CloseWithError(0, "")
	if cur != qc {
		_ = qc.CloseWithError(0, "")
	}
}

// Client starts a client session on the first bidirectional stream of an
// established QUIC connection that the caller dialled, as zmux.Client does
// on a net.Conn; the caller keeps ownership of qc and closes it after the
// session ended.
func Client(ctx context.Context, qc *quic.Conn, cfg *zmux.Config) (*zmux.Conn, error) {
	p, err := OpenPipe(ctx, qc)
	if err != nil {
		return nil, err
	}
	return zmux.Start(ctx, p, zmux.RoleClient, cfg)
}

// OpenPipe opens a bidirectional stream of qc as a pipe input (bootstrap
// or join), with lanes wired: the lanes the peer opens on qc are accepted
// while qc is open (one goroutine per connection).
func OpenPipe(ctx context.Context, qc *quic.Conn) (zmux.PipeIO, error) {
	pio, _, err := openPipe(ctx, &qc)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	clientLanes(qc)
	return pio, nil
}

// Serve accepts connections from ln and hands their streams to l, as
// zmux.Listener.Serve does for a net.Listener: each bidirectional stream
// is a carrier (a bootstrap or a join), each unidirectional stream a lane;
// Accept on l returns the sessions. Run it once per listener; it returns
// when ln is closed or ctx ends, and the connections and sessions it
// accepted continue (close them, or ln, to end them).
func Serve(ctx context.Context, ln *quic.EarlyListener, l *zmux.Listener) error {
	for {
		qc, err := ln.Accept(ctx)
		if err != nil {
			return err
		}
		go acceptLanes(ctx, qc, l)
		go acceptPipes(ctx, qc, l)
	}
}

func acceptPipes(ctx context.Context, qc *quic.Conn, l *zmux.Listener) {
	for {
		s, err := qc.AcceptStream(ctx)
		if err != nil {
			return
		}
		go func() { _ = l.HandlePipe(ctx, streamPipe(qc, s), zmux.JoinHint{Source: hostOf(qc.RemoteAddr())}) }()
	}
}

// acceptLanes routes the unidirectional streams of a connection: lanes of
// pipes on it (by Lane Key, MULTIPATH.md 5.8).
func acceptLanes(ctx context.Context, qc *quic.Conn, l *zmux.Listener) {
	for {
		s, err := qc.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		p := zmux.PipeIO{R: &laneReader{s: s, qc: qc}}
		go func() {
			if l != nil {
				_ = l.HandlePipe(ctx, p, zmux.JoinHint{Source: hostOf(qc.RemoteAddr())})
			} else {
				_ = zmux.HandleLane(ctx, p)
			}
		}()
	}
}

// laneConns are the client connections whose lanes are being accepted.
var laneConns sync.Map // *quic.Conn -> struct{}

// clientLanes accepts the peer's lanes on a client connection, once.
func clientLanes(qc *quic.Conn) {
	if _, loaded := laneConns.LoadOrStore(qc, struct{}{}); loaded {
		return
	}
	go func() {
		acceptLanes(context.Background(), qc, nil)
		laneConns.Delete(qc)
	}()
}

func hostOf(a net.Addr) string {
	if h, _, err := net.SplitHostPort(a.String()); err == nil {
		return h
	}
	return a.String()
}

// streamPipe adapts a bidirectional QUIC stream to a pipe input.
func streamPipe(qc *quic.Conn, s *quic.Stream) zmux.PipeIO {
	sp := &qStream{qc: qc, s: s}
	return zmux.PipeIO{R: (*qReader)(sp), W: sp}
}

// openPipe opens the first bidirectional stream of *qc (see openStream) as a
// pipe input; the returned context ends with the stream.
func openPipe(ctx context.Context, qc **quic.Conn) (zmux.PipeIO, context.Context, error) {
	s, err := openStream(ctx, qc)
	if err != nil {
		return zmux.PipeIO{}, nil, err
	}
	return streamPipe(*qc, s), s.Context(), nil
}

// qStream is the sending half, with the carrier capabilities.
type qStream struct {
	mu     sync.Mutex
	qc     *quic.Conn
	s      *quic.Stream
	closed bool // Dial's session ended: a replacement connection is closed
	wOnce  sync.Once
	rOnce  sync.Once
}

func (q *qStream) cur() (*quic.Conn, *quic.Stream) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.qc, q.s
}

func (q *qStream) Write(p []byte) (int, error) {
	_, s := q.cur()
	return s.Write(p)
}

// Close ends the sending half gracefully (FIN).
func (q *qStream) Close() error {
	var err error
	q.wOnce.Do(func() {
		_, s := q.cur()
		err = s.Close()
	})
	return err
}

// Abort resets both halves (a pipe failure).
func (q *qStream) Abort(error) {
	_, s := q.cur()
	q.wOnce.Do(func() { s.CancelWrite(carrierFailed) })
	q.rOnce.Do(func() { s.CancelRead(carrierFailed) })
}

// CarrierConn is the QUIC connection (lanes are its streams).
func (q *qStream) CarrierConn() any {
	qc, _ := q.cur()
	return qc
}

// ConnectionState exposes the connection's TLS state (the exporter of
// PipeAuth.TLS and carrier-bound joins).
func (q *qStream) ConnectionState() tls.ConnectionState {
	qc, _ := q.cur()
	return qc.ConnectionState().TLS
}

// OpenLane opens a unidirectional stream of the connection as a lane; it
// waits for stream credit (MULTIPATH.md 5.8).
func (q *qStream) OpenLane(ctx context.Context) (io.WriteCloser, error) {
	qc, _ := q.cur()
	if err := handshakeReady(ctx, qc); err != nil {
		return nil, err
	}
	s, err := qc.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &laneWriter{s: s}, nil
}

// EarlyAccepted waits for the handshake's carrier verdict. The temporary
// handshake barrier creates every outgoing stream after that decision, so
// this stream's bytes are delivered as 1-RTT even if native 0-RTT was rejected.
func (q *qStream) EarlyAccepted(ctx context.Context) (bool, error) {
	qc, _ := q.cur()
	select {
	case <-qc.HandshakeComplete():
	case <-ctx.Done():
		return false, ctx.Err()
	case <-qc.Context().Done():
		return false, context.Cause(qc.Context())
	}
	return true, nil
}

// RestartEarly opens the bootstrap stream again on the connection that
// replaced a rejected 0-RTT one.
func (q *qStream) RestartEarly(ctx context.Context) (io.ReadWriteCloser, error) {
	qc, _ := q.cur()
	nc, err := qc.NextConnection(ctx)
	if err != nil {
		return nil, err
	}
	s, err := nc.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	q.mu.Lock()
	closed := q.closed
	if !closed {
		q.qc, q.s = nc, s
	}
	q.mu.Unlock()
	if closed {
		_ = nc.CloseWithError(0, "")
		return nil, net.ErrClosed
	}
	clientLanes(nc)
	return &restarted{q: q}, nil
}

// UsedEarlyData reports a server stream that arrived in 0-RTT.
func (q *qStream) UsedEarlyData() bool {
	qc, _ := q.cur()
	return qc.ConnectionState().Used0RTT
}

// HandshakeComplete is closed once the connection's handshake completed.
func (q *qStream) HandshakeComplete() <-chan struct{} {
	qc, _ := q.cur()
	return qc.HandshakeComplete()
}

// restarted is the byte stream after a 0-RTT rejection.
type restarted struct{ q *qStream }

func (r *restarted) Read(p []byte) (int, error) {
	_, s := r.q.cur()
	return s.Read(p)
}
func (r *restarted) Write(p []byte) (int, error) { return r.q.Write(p) }
func (r *restarted) Close() error {
	r.q.Abort(nil)
	return nil
}

// qReader is the receiving half.
type qReader qStream

func (r *qReader) Read(p []byte) (int, error) {
	_, s := (*qStream)(r).cur()
	n, err := s.Read(p)
	if err != nil && errors.Is(err, io.EOF) {
		return n, io.EOF
	}
	return n, err
}

// Close of the receiving half before its end stops it.
func (r *qReader) Close() error {
	q := (*qStream)(r)
	q.rOnce.Do(func() {
		_, s := q.cur()
		s.CancelRead(carrierFailed)
	})
	return nil
}

// CarrierConn of the receiving half (for lanes routed by HandlePipe).
func (r *qReader) CarrierConn() any { return (*qStream)(r).CarrierConn() }

// laneWriter is an outbound lane.
type laneWriter struct {
	s    *quic.SendStream
	once sync.Once
}

func (l *laneWriter) Write(p []byte) (int, error) { return l.s.Write(p) }
func (l *laneWriter) Close() error {
	var err error
	l.once.Do(func() { err = l.s.Close() })
	return err
}
func (l *laneWriter) Abort(error) { l.once.Do(func() { l.s.CancelWrite(carrierFailed) }) }

// laneReader is an inbound lane (or a stream to be classified).
type laneReader struct {
	s    *quic.ReceiveStream
	qc   *quic.Conn
	once sync.Once
}

func (l *laneReader) Read(p []byte) (int, error) { return l.s.Read(p) }
func (l *laneReader) Close() error {
	l.once.Do(func() { l.s.CancelRead(carrierFailed) })
	return nil
}
func (l *laneReader) CarrierConn() any { return l.qc }

// Pool opens pipes on a pool of QUIC connections to one server: at most
// PoolLimits.MaxPipesPerConnection pipes per connection at once,
// connections retired by lifetime limits or failure; the pipes of one
// connection share a fate group. It is safe for concurrent use.
type Pool struct {
	pool zmux.CarrierPool[*quic.Conn]
}

// Shutdown retires every connection: idle ones close now, the others once
// their pipes end; OpenPipe fails afterwards.
func (p *Pool) Shutdown() { p.pool.Shutdown() }

// NewPool returns a pool that dials addr with tlsConf and qconf (may be
// nil), within lim.
func NewPool(addr string, tlsConf *tls.Config, qconf *quic.Config, lim zmux.PoolLimits) *Pool {
	p := &Pool{}
	p.pool = zmux.CarrierPool[*quic.Conn]{PoolLimits: lim, Close: closePooled,
		Dial: func(ctx context.Context) (*quic.Conn, error) {
			qc, err := dialAddr(ctx, addr, tlsConf, QUICConfig(qconf), false)
			if err != nil {
				return nil, err
			}
			clientLanes(qc)
			context.AfterFunc(qc.Context(), func() { p.pool.RetireConn(qc) })
			return qc, nil
		}}
	return p
}

func closePooled(qc *quic.Conn) { _ = qc.CloseWithError(0, "") }

// OpenPipe opens a pipe (a bidirectional stream with its lanes) on a
// pooled connection; its PipeIO carries the connection's fate group.
func (p *Pool) OpenPipe(ctx context.Context) (zmux.PipeIO, error) {
	if p.pool.Dial == nil {
		return zmux.PipeIO{}, errors.New("zmuxquic: a Pool is made by NewPool")
	}
	l, err := p.pool.Get(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	qc := l.Conn
	pio, done, err := openPipe(ctx, &qc)
	if err != nil {
		l.Release()
		return zmux.PipeIO{}, err
	}
	context.AfterFunc(done, l.Release)
	pio.FateGroup = l.FateGroup
	return pio, nil
}
