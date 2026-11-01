// Package zmuxquic carries zmux pipes over QUIC (quic-go): a pipe pair is
// one bidirectional stream, its further lanes are unidirectional streams
// of the same connection (MULTIPATH.md 2.5), its datagrams may go as QUIC
// datagrams of the connection (EXTENSIONS.md 6.8), the TLS exporter of the
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
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

// carrierFailed is ZMUX_CARRIER_FAILED as a QUIC stream error code.
const carrierFailed = quic.StreamErrorCode(zmux.StreamCarrierFailed)

// QUICConfig returns a QUIC configuration sized for lanes: unidirectional
// stream credit for the lanes the endpoint carries, and a connection window
// above the stream windows of the lanes that can be blocked at once. It
// enables QUIC datagrams, the datagram channel of the connection's pipes
// that sessions with datagrams use (zmux.DatagramChannel); the datagrams of
// a connection carrying pipes belong to zmux. base may be nil.
func QUICConfig(base *quic.Config) *quic.Config {
	c := &quic.Config{}
	if base != nil {
		c = base.Clone()
	}
	c.EnableDatagrams = true
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
// ends. qconf may be nil; QUICConfig sizes it for lanes. With
// cfg.Credential, the first stream starts with its prefix, bound to the
// connection's handshake, and 0-RTT is not used.
func Dial(ctx context.Context, addr string, tlsConf *tls.Config, qconf *quic.Config, cfg *zmux.Config) (*zmux.Conn, error) {
	var cred zmux.Credential
	if cfg != nil {
		cred = cfg.Credential
	}
	early := cfg != nil && cfg.EarlyData.Replayable && len(cfg.EarlyData.Remembered) > 0 && cred == nil
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
	if err := zmux.PresentPipe(ctx, sp.pipe(), cred); err != nil {
		closeDialled(qc, sp)
		return nil, err
	}
	c, err := zmux.Start(ctx, sp.pipe(), zmux.RoleClient, cfg)
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
// on a net.Conn, with cfg.Credential's prefix; the caller keeps ownership
// of qc and closes it after the session ended.
func Client(ctx context.Context, qc *quic.Conn, cfg *zmux.Config) (*zmux.Conn, error) {
	p, err := OpenPipe(ctx, qc)
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		if err := zmux.PresentPipe(ctx, p, cfg.Credential); err != nil {
			p.W.(*qStream).Abort(err)
			return nil, err
		}
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

// ConnListener is what Serve accepts connections from: a *quic.Listener,
// or a *quic.EarlyListener for 0-RTT.
type ConnListener interface {
	Accept(ctx context.Context) (*quic.Conn, error)
}

// Serve accepts connections from ln and hands their streams to l, as
// zmux.Listener.Serve does for a net.Listener: each bidirectional stream
// is a carrier (a bootstrap or a join), each unidirectional stream a lane;
// Accept on l returns the sessions. A connection takes a slot of its
// source as it is accepted (zmux.Listener.Arrive). With an Admission on
// l, the connection is admitted by its first bidirectional stream, once
// its handshake completed (one that used 0-RTT is closed without
// admission): only once that stream is accepted are the connection's
// further streams and lanes handed to l. A connection whose first stream
// is not accepted is closed once the decision has been acted on; fallback,
// when set, receives that stream on VerdictFallback with its bytes
// replayed. Run it once per listener; it returns when ln is closed or ctx
// ends, and the connections and sessions it accepted continue (close
// them, or ln, to end them).
func Serve(ctx context.Context, ln ConnListener, l *zmux.Listener, fallback func(context.Context, *quic.Conn, zmux.PipeIO)) error {
	for {
		qc, err := ln.Accept(ctx)
		if err != nil {
			return err
		}
		go func() { _ = ServeConn(ctx, qc, l, fallback) }()
	}
}

// ServeConn serves one established QUIC connection that the caller
// accepted or dialled, such as one made by hole punching, exactly as Serve
// serves each connection it accepts (Serve calls it): the connection takes
// a slot of its source, its remote address (zmux.Listener.Arrive), is
// admitted by its first bidirectional stream when l has an Admission (one
// that used 0-RTT is then closed), and its streams then go through l
// (ListenerConfig.WrapPipe included) as bootstraps, joins and lanes, so
// that sessions started on it are returned by Accept on l and its pipes
// join sessions of l. A connection refused is closed.
//
// ServeConn returns when the connection ends (nil) or ctx ends
// (ctx.Err()); after ctx ends no further stream of the connection is
// handed to l, while the connection and its sessions continue, as with
// Serve. On a closed listener it closes qc and returns net.ErrClosed.
func ServeConn(ctx context.Context, qc *quic.Conn, l *zmux.Listener, fallback func(context.Context, *quic.Conn, zmux.PipeIO)) error {
	a, err := l.Arrive(netip.AddrPort{}, addrPort(qc.RemoteAddr()))
	if err != nil {
		_ = qc.CloseWithError(0, "")
		if errors.Is(err, net.ErrClosed) {
			return err
		}
		return nil // refused: the connection ended
	}
	// A connection without a stream gives its slot back when it ends.
	go func() {
		<-qc.Context().Done()
		a.Release()
	}()
	if a.Admitted() {
		var used atomic.Bool
		go watchHandshake(qc, a, &used)
		go acceptLanes(ctx, qc, a, &used)
		go acceptPipes(ctx, qc, a, &used)
		return served(ctx, qc)
	}
	select {
	case <-qc.HandshakeComplete():
	case <-qc.Context().Done():
		a.HandshakeFailed()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	if qc.ConnectionState().Used0RTT {
		a.HandshakeFailed()
		_ = qc.CloseWithError(0, "")
		return nil
	}
	// A connection without a stream ends with its idle timeout.
	s, err := qc.AcceptStream(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = qc.CloseWithError(0, "")
		return nil
	}
	var fb func(context.Context, zmux.PipeIO)
	if fallback != nil {
		fb = func(ctx context.Context, p zmux.PipeIO) { fallback(ctx, qc, p) }
	}
	p, ok := a.Admit(ctx, streamPipe(qc, s), fb)
	if !ok {
		_ = qc.CloseWithError(0, "")
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}
	go acceptLanes(ctx, qc, a, nil)
	go acceptPipes(ctx, qc, a, nil)
	go func() { _ = a.HandlePipe(ctx, p, zmux.JoinHint{}) }()
	return served(ctx, qc)
}

// served waits for the end of qc (nil) or of ctx (its error).
func served(ctx context.Context, qc *quic.Conn) error {
	select {
	case <-qc.Context().Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// watchHandshake counts a connection whose handshake failed before any of
// its streams became a pipe input; a stream that arrived in 0-RTT is
// counted by the listener itself.
func watchHandshake(qc *quic.Conn, a *zmux.Arrival, used *atomic.Bool) {
	select {
	case <-qc.HandshakeComplete():
	case <-qc.Context().Done():
		if !used.Load() {
			a.HandshakeFailed()
		}
	}
}

// acceptPipes hands the bidirectional streams of a connection to a's
// listener; used, when set, records that one arrived.
func acceptPipes(ctx context.Context, qc *quic.Conn, a *zmux.Arrival, used *atomic.Bool) {
	for {
		s, err := qc.AcceptStream(ctx)
		if err != nil {
			return
		}
		if used != nil {
			used.Store(true)
		}
		go func() { _ = a.HandlePipe(ctx, streamPipe(qc, s), zmux.JoinHint{}) }()
	}
}

// acceptLanes routes the unidirectional streams of a connection: lanes of
// pipes on it (by Lane Key, MULTIPATH.md 5.8), through a's listener, or
// on a client connection (a nil a) to its own pipes.
func acceptLanes(ctx context.Context, qc *quic.Conn, a *zmux.Arrival, used *atomic.Bool) {
	for {
		s, err := qc.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		if used != nil {
			used.Store(true)
		}
		p := zmux.PipeIO{R: &laneReader{s: s, qc: qc}, Carrier: zmux.CarrierQUIC}
		go func() {
			if a != nil {
				_ = a.HandlePipe(ctx, p, zmux.JoinHint{})
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
		acceptLanes(context.Background(), qc, nil, nil)
		laneConns.Delete(qc)
	}()
}

// addrPort is a's address and port (zero when it is not an IP address).
func addrPort(a net.Addr) netip.AddrPort {
	if u, ok := a.(*net.UDPAddr); ok {
		return u.AddrPort()
	}
	ap, _ := netip.ParseAddrPort(a.String())
	return ap
}

// streamPipe adapts a bidirectional QUIC stream to a pipe input.
func streamPipe(qc *quic.Conn, s *quic.Stream) zmux.PipeIO {
	return (&qStream{qc: qc, s: s}).pipe()
}

// pipe is the pipe input over q.
func (q *qStream) pipe() zmux.PipeIO {
	return zmux.PipeIO{R: (*qReader)(q), W: q, Carrier: zmux.CarrierQUIC}
}

// openPipe opens the first bidirectional stream of *qc (see openStream) as a
// pipe input; the returned context ends with the stream.
func openPipe(ctx context.Context, qc **quic.Conn) (zmux.PipeIO, context.Context, error) {
	s, err := openStream(ctx, qc)
	if err != nil {
		return zmux.PipeIO{}, nil, err
	}
	sp := &qStream{qc: *qc, s: s}
	return sp.pipe(), s.Context(), nil
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

// Retransmitted reports the bytes the connection declared lost, which
// it sends again (zmux.PipeIO capability); the streams of a connection
// share the count.
func (q *qStream) Retransmitted() (uint64, bool) {
	qc, _ := q.cur()
	return qc.ConnectionStats().BytesLost, true
}

func (q *qStream) Write(p []byte) (int, error) {
	_, s := q.cur()
	return s.Write(p)
}

// WriteBuffers writes a batch with one stream write (one locked copy).
func (q *qStream) WriteBuffers(bufs [][]byte) (int, error) { return zmux.WriteFlat(q, bufs) }

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

// WriteBuffers writes a batch with one stream write (one locked copy).
func (r *restarted) WriteBuffers(bufs [][]byte) (int, error) { return zmux.WriteFlat(r.q, bufs) }
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

// WriteBuffers writes a batch with one stream write (one locked copy).
func (l *laneWriter) WriteBuffers(bufs [][]byte) (int, error) { return zmux.WriteFlat(l.s, bufs) }
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
	pool zmux.CarrierPool[*pooled]
	cred zmux.Credential
}

// pooled is a pooled connection; its first stream carries the credential.
type pooled struct {
	qc        *quic.Conn
	mu        sync.Mutex // held while the first stream is opened
	presented bool
}

// Shutdown retires every connection: idle ones close now, the others once
// their pipes end; OpenPipe fails afterwards.
func (p *Pool) Shutdown() { p.pool.Shutdown() }

// NewPool returns a pool that dials addr with tlsConf and qconf (may be
// nil), within lim. The first stream of each connection starts with
// cred's prefix (nil: none), as a server admission reads it.
func NewPool(addr string, tlsConf *tls.Config, qconf *quic.Config, cred zmux.Credential, lim zmux.PoolLimits) *Pool {
	p := &Pool{cred: cred}
	p.pool = zmux.CarrierPool[*pooled]{PoolLimits: lim, Close: closePooled,
		Dial: func(ctx context.Context) (*pooled, error) {
			qc, err := dialAddr(ctx, addr, tlsConf, QUICConfig(qconf), false)
			if err != nil {
				return nil, err
			}
			clientLanes(qc)
			pc := &pooled{qc: qc}
			context.AfterFunc(qc.Context(), func() { p.pool.RetireConn(pc) })
			return pc, nil
		}}
	return p
}

func closePooled(pc *pooled) { _ = pc.qc.CloseWithError(0, "") }

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
	pio, done, err := p.open(ctx, l.Conn)
	if err != nil {
		l.Release()
		return zmux.PipeIO{}, err
	}
	context.AfterFunc(done, l.Release)
	pio.FateGroup = l.FateGroup
	return pio, nil
}

// open opens a stream of pc. The first one (the lowest stream ID) starts
// with the credential's prefix; streams are opened one at a time.
func (p *Pool) open(ctx context.Context, pc *pooled) (zmux.PipeIO, context.Context, error) {
	qc := pc.qc
	if p.cred == nil {
		return openPipe(ctx, &qc)
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.presented {
		return openPipe(ctx, &qc)
	}
	pio, done, err := openPipe(ctx, &qc)
	if err != nil {
		return pio, done, err
	}
	if err := zmux.PresentPipe(ctx, pio, p.cred); err != nil {
		pio.W.(*qStream).Abort(err)
		return zmux.PipeIO{}, nil, err
	}
	pc.presented = true
	return pio, done, nil
}

// SetReadDeadline bounds reads of the receiving half.
func (r *qReader) SetReadDeadline(t time.Time) error {
	_, s := (*qStream)(r).cur()
	return s.SetReadDeadline(t)
}

// Candidate is a QUIC stream to addr for zmux.DialMode, for a client that
// reaches the server directly: each carrier is the first stream of a new
// connection (dialed as NewPool does, without 0-RTT), which starts with
// the session's credential and is closed once the pipe ends.
func Candidate(addr string, tlsConf *tls.Config, qconf *quic.Config) zmux.Candidate {
	return zmux.Candidate{Kind: zmux.CarrierQUIC, Dial: func(ctx context.Context, cred zmux.Credential) (zmux.PipeIO, error) {
		p := NewPool(addr, tlsConf, qconf, cred, zmux.PoolLimits{MaxPipesPerConnectionLifetime: 1})
		return p.OpenPipe(ctx)
	}}
}
