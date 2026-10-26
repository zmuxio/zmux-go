// Package zmuxquic carries zmux pipes over QUIC (quic-go): a pipe pair is
// one bidirectional stream, its further lanes are unidirectional streams
// of the same connection (MULTIPATH.md 2.5), the TLS exporter of the
// connection authenticates joins, and a client may send its first streams
// in 0-RTT (SPEC.md 6.1.2).
package zmuxquic

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	zmux "github.com/zmuxio/zmux-go"
)

// carrierFailed is ZMUX_CARRIER_FAILED as a QUIC stream error code.
const carrierFailed = quic.StreamErrorCode(zmux.CodeCarrierFailed)

// QUICConfig returns a QUIC configuration sized for lanes (IMPLEMENTATION.md
// 13.8 N4, N5): unidirectional stream credit for the lanes the endpoint
// carries, and a connection window above the stream windows of the lanes
// that can be blocked at once. base may be nil.
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

// Dial opens a QUIC connection to addr and starts a zmux session on its
// first bidirectional stream. With cfg.EarlyData.Replayable and remembered
// parameters, the first streams go in 0-RTT.
func Dial(ctx context.Context, addr string, tlsConf *tls.Config, qconf *quic.Config, cfg *zmux.Config) (*zmux.Conn, error) {
	early := cfg != nil && cfg.EarlyData.Replayable && len(cfg.EarlyData.Remembered) > 0
	var qc *quic.Conn
	var err error
	if early {
		qc, err = quic.DialAddrEarly(ctx, addr, tlsConf, QUICConfig(qconf))
	} else {
		qc, err = quic.DialAddr(ctx, addr, tlsConf, QUICConfig(qconf))
	}
	if err != nil {
		return nil, err
	}
	c, err := Client(ctx, qc, cfg)
	if err != nil {
		_ = qc.CloseWithError(0, "")
		return nil, err
	}
	return c, nil
}

// Client starts a client session on an established QUIC connection that
// the caller dialled (Dial dials one itself); the caller keeps ownership of
// qc.
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
	s, err := qc.OpenStreamSync(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	clientLanes(qc)
	return streamPipe(qc, s), nil
}

// Serve accepts connections from ln and hands their streams to l: each
// bidirectional stream is a carrier (a bootstrap or a join), each
// unidirectional stream a lane. Run it once per listener; it returns when
// ln is closed or ctx ends, and the connections and sessions it accepted
// continue (close them, or ln, to end them).
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

// qStream is the sending half, with the carrier capabilities.
type qStream struct {
	mu     sync.Mutex
	qc     *quic.Conn
	s      *quic.Stream
	wOnce  sync.Once
	rOnce  sync.Once
	reset  bool
	closed bool
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
	s, err := qc.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &laneWriter{s: s}, nil
}

// EarlyAccepted waits for the handshake and reports whether the server
// accepted 0-RTT.
func (q *qStream) EarlyAccepted(ctx context.Context) (bool, error) {
	qc, _ := q.cur()
	select {
	case <-qc.HandshakeComplete():
	case <-ctx.Done():
		return false, ctx.Err()
	case <-qc.Context().Done():
		return false, context.Cause(qc.Context())
	}
	return qc.ConnectionState().Used0RTT, nil
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
	q.qc, q.s = nc, s
	q.mu.Unlock()
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

// Pool opens pipes on a pool of QUIC connections to one server (a carrier
// pool, API.md Appendix A): at most MaxPipesPerConnection pipes per
// connection at once, connections retired by lifetime limits or failure;
// the pipes of one connection share a fate group.
type Pool struct {
	// MaxPipesPerConnection bounds the pipes on a connection at once (8).
	MaxPipesPerConnection int
	// MaxPipesPerConnectionLifetime bounds the pipes a connection carries over
	// its life (0: unlimited).
	MaxPipesPerConnectionLifetime int
	// MaxConnectionAge stops giving pipes to a connection this old (0:
	// unlimited).
	MaxConnectionAge time.Duration

	once sync.Once
	pool zmux.CarrierPool[*quic.Conn]
	dial func(ctx context.Context) (*quic.Conn, error)
}

func (p *Pool) init() {
	p.once.Do(func() {
		p.pool = zmux.CarrierPool[*quic.Conn]{Dial: p.dial, Close: closePooled,
			MaxPipesPerConnection:         p.MaxPipesPerConnection,
			MaxPipesPerConnectionLifetime: p.MaxPipesPerConnectionLifetime,
			MaxConnectionAge:              p.MaxConnectionAge}
	})
}

// Shutdown retires every connection: idle ones close now, the others once
// their pipes end; OpenPipe fails afterwards.
func (p *Pool) Shutdown() {
	p.init()
	p.pool.Shutdown()
}

// NewPool returns a pool that dials addr with tlsConf and qconf (may be
// nil); its limits are read at the first OpenPipe.
func NewPool(addr string, tlsConf *tls.Config, qconf *quic.Config) *Pool {
	p := &Pool{}
	p.dial = func(ctx context.Context) (*quic.Conn, error) {
		qc, err := quic.DialAddr(ctx, addr, tlsConf, QUICConfig(qconf))
		if err != nil {
			return nil, err
		}
		clientLanes(qc)
		context.AfterFunc(qc.Context(), func() { p.pool.RetireConn(qc) })
		return qc, nil
	}
	return p
}

func closePooled(qc *quic.Conn) { _ = qc.CloseWithError(0, "") }

// OpenPipe opens a pipe (a bidirectional stream with its lanes) on a
// pooled connection; its PipeIO carries the connection's fate group.
func (p *Pool) OpenPipe(ctx context.Context) (zmux.PipeIO, error) {
	if p.dial == nil {
		return zmux.PipeIO{}, errors.New("zmuxquic: a Pool is made by NewPool")
	}
	p.init()
	l, err := p.pool.Get(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	s, err := l.Conn.OpenStreamSync(ctx)
	if err != nil {
		l.Release()
		return zmux.PipeIO{}, err
	}
	context.AfterFunc(s.Context(), l.Release)
	pio := streamPipe(l.Conn, s)
	pio.FateGroup = l.FateGroup
	return pio, nil
}
