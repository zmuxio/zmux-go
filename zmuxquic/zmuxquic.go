// Package zmuxquic carries zmux pipes over QUIC (quic-go): a pipe pair is
// one bidirectional stream, its further lanes are unidirectional streams
// of the same connection, its datagrams may go as QUIC
// datagrams of the connection, the TLS exporter of the
// connection authenticates joins, and a client may send its first streams
// as replayable early data. Native streams open after the QUIC handshake
// completes.
package zmuxquic

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/zmuxquic/streamio"
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
// enabled. Native streams open after handshake completion. The session owns
// the connection: it is closed once the session
// has wholly ended (the peer read its CONNECTION_CLOSE and ended its side, or
// the linger timeout expired), with the session's application error code
// (0 after any other end). qconf may be nil; QUICConfig sizes it for lanes. With
// cfg.CarrierCredential, the first stream starts with its prefix, bound to the
// connection's handshake, and 0-RTT is not used.
func Dial(ctx context.Context, addr string, tlsConf *tls.Config, qconf *quic.Config, cfg *zmux.Config) (*zmux.Conn, error) {
	var cred zmux.CarrierCredential
	early := false
	if cfg != nil {
		cred = cfg.CarrierCredential
		early = cfg.EarlyData.Replayable && len(cfg.EarlyData.Remembered) > 0 && cred == nil
	}
	qc, _, err := dialAddr(ctx, addr, tlsConf, QUICConfig(qconf), early)
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
	sp := &qStream{qc: cur, s: s, early: early}
	if err := zmux.PresentPipe(ctx, sp.pipe(), cred); err != nil {
		closeDialled(qc, sp, 0, "")
		return nil, err
	}
	c, err := zmux.Start(ctx, sp.pipe(), zmux.RoleClient, cfg)
	if err != nil {
		closeDialled(qc, sp, 0, "")
		return nil, err
	}
	// The session's context ends when it has wholly ended: every pipe's
	// inbound side ended or lingered out.
	context.AfterFunc(c.Context(), func() {
		code, reason := closeCode(context.Cause(c.Context()))
		closeDialled(qc, sp, code, reason)
	})
	return c, nil
}

// closeDialled closes the connection Dial opened, and the one that
// replaced it after a 0-RTT rejection, with code and reason.
func closeDialled(qc *quic.Conn, sp *qStream, code quic.ApplicationErrorCode, reason string) {
	sp.mu.Lock()
	sp.closed = true
	cur := sp.qc
	sp.mu.Unlock()
	_ = cur.CloseWithError(code, reason)
	if cur != qc {
		_ = qc.CloseWithError(0, "") // it carried no session
	}
}

// closeCode is the QUIC application close of a session that ended with
// err: its application error code and reason, 0 and none after any other
// end.
func closeCode(err error) (quic.ApplicationErrorCode, string) {
	if ae, ok := errors.AsType[*zmux.ApplicationError](err); ok {
		return quic.ApplicationErrorCode(ae.ErrorCode), ae.ErrorMessage
	}
	return 0, ""
}

// Client starts a client session on the first bidirectional stream of an
// established QUIC connection that the caller dialled, as zmux.Client does
// on a net.Conn, with cfg.CarrierCredential's prefix; the caller keeps ownership
// of qc and closes it once the session has wholly ended (Wait returned,
// or Done is closed), not sooner: a QUIC close discards the stream data
// the peer has not acknowledged, the zmux close included. Its code is the
// session's: the ErrorCode of the *zmux.ApplicationError that Wait
// returns, 0 after any other end.
func Client(ctx context.Context, qc *quic.Conn, cfg *zmux.Config) (*zmux.Conn, error) {
	p, err := ConnPipe(ctx, qc)
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		if err := zmux.PresentPipe(ctx, p, cfg.CarrierCredential); err != nil {
			p.W.(*qStream).Abort(err)
			return nil, err
		}
	}
	return zmux.Start(ctx, p, zmux.RoleClient, cfg)
}

// ConnPipe opens a bidirectional stream of qc as a pipe input (bootstrap
// or join), with lanes wired: the lanes the peer opens on qc are accepted
// while qc is open (one goroutine per connection). The caller keeps
// ownership of qc, as with Client.
func ConnPipe(ctx context.Context, qc *quic.Conn) (zmux.PipeIO, error) {
	pio, err := openPipe(ctx, &qc)
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
// is not accepted is closed once the decision has been acted on (on
// VerdictFallback, once zmux.ListenerConfig.Fallback returned; it receives
// that stream with its bytes replayed, and TakeoverConn gives its
// connection, which stays open when the host detached the stream). Run it once per listener; it returns when ln is closed or
// ctx ends, and the connections and sessions it accepted continue (close
// them to end them; closing ln leaves established connections alone).
func Serve(ctx context.Context, l *zmux.Listener, ln ConnListener) error {
	for {
		qc, err := ln.Accept(ctx)
		if err != nil {
			return err
		}
		if err := ServeConn(ctx, l, qc); err != nil && !errors.Is(err, errConnectionEnded) {
			return err
		}
	}
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

// openPipe opens the first bidirectional stream of *qc (see openStream) as a
// pipe input.
func openPipe(ctx context.Context, qc **quic.Conn) (zmux.PipeIO, error) {
	s, err := openStream(ctx, qc)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	sp := &qStream{qc: *qc, s: s}
	return sp.pipe(), nil
}

// qStream is the sending half, with the carrier capabilities.
type qStream struct {
	qc *quic.Conn
	s  *quic.Stream
	// rec, when set, records the close code of the pipe's session for its
	// connection (zmux.CloseCodeRecorder); lease, when set, ends the pooled
	// connection's lease once both halves have ended.
	rec    *streamio.CloseRecord
	lease  *carrierio.HalfLease
	send   streamio.Writer
	wEnd   sync.Once
	rOnce  sync.Once
	mu     sync.Mutex
	closed bool // Dial's session ended: a replacement connection is closed
	early  bool // Dial sent the stream in 0-RTT
}

// pipe is the pipe input over q.
func (q *qStream) pipe() zmux.PipeIO {
	return zmux.PipeIO{R: (*qReader)(q), W: q, Carrier: zmux.CarrierQUIC}
}

// halfEnded counts the end of one half (lease).
func (q *qStream) halfEnded() { q.lease.HalfEnded() }

// RecordCloseCode records the code of the pipe's session for the pooled
// connection's close (zmux.CloseCodeRecorder).
func (q *qStream) RecordCloseCode(code zmux.ApplicationErrorCode, reason string) {
	if q.rec != nil {
		q.rec.Set(uint64(code), reason)
	}
}

func (q *qStream) cur() (*quic.Conn, *quic.Stream) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.qc, q.s
}

// CarrierSampler is for internal use by zmux modules.
func (q *qStream) CarrierSampler() *carrierstats.Sampler {
	qc, _ := q.cur()
	return ConnectionSampler(qc)
}

// ConnectionTraffic returns lifetime transport bytes for a pool policy.
func ConnectionTraffic(qc *quic.Conn) (sent, received uint64) {
	if qc == nil {
		return 0, 0
	}
	stats := qc.ConnectionStats()
	return stats.BytesSent, stats.BytesReceived
}

// ConnectionSampler returns the shared native sampler of a QUIC connection.
// It creates no observation until a reader asks the sampler for one.
func ConnectionSampler(qc *quic.Conn) *carrierstats.Sampler {
	return carrierstats.ForQUIC(qc, func(c *quic.Conn) (sent, lost uint64) {
		st := c.ConnectionStats()
		return st.PacketsSent, st.PacketsLost
	})
}

func (q *qStream) Write(p []byte) (int, error) {
	_, s := q.cur()
	return q.send.Write(s, p)
}

// PreferFlatWrite lets the driver reuse its batch's contiguous buffer.
func (q *qStream) PreferFlatWrite() {}

// WriteBuffers writes a batch with one stream write (one locked copy).
func (q *qStream) WriteBuffers(bufs [][]byte) (int, error) { return carrier.WriteFlat(q, bufs) }

// Close ends the sending half gracefully (FIN).
func (q *qStream) Close() error {
	_, s := q.cur()
	err := q.send.Close(s)
	q.wEnd.Do(q.halfEnded)
	return err
}

// Abort resets both halves (a pipe failure).
func (q *qStream) Abort(_ error) {
	_, s := q.cur()
	q.send.Abort(func() { s.CancelWrite(carrierFailed) })
	q.wEnd.Do(q.halfEnded)
	q.rOnce.Do(func() { s.CancelRead(carrierFailed); q.halfEnded() })
}

// CarrierConn is the QUIC connection (lanes are its streams).
func (q *qStream) CarrierConn() any {
	qc, _ := q.cur()
	return qc
}

// ConnectionState exposes the connection's TLS state (the exporter of
// PipeAuth.Exporter and Bound joins).
func (q *qStream) ConnectionState() tls.ConnectionState {
	qc, _ := q.cur()
	return qc.ConnectionState().TLS
}

// CarrierSecure declares the Bound contract: QUIC protects every byte of the stream end
// to end with the handshake this process terminates.
func (q *qStream) CarrierSecure() bool { return true }

// OpenLane opens a unidirectional stream of the connection as a lane; it
// waits for stream credit.
func (q *qStream) OpenLane(ctx context.Context) (io.WriteCloser, error) {
	qc, _ := q.cur()
	if err := handshakeReady(ctx, qc); err != nil {
		return nil, err
	}
	s, err := qc.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &laneWriter{s: s, qc: qc}, nil
}

// SendsEarlyData reports a stream Dial opened in 0-RTT.
func (q *qStream) SendsEarlyData() bool { return q.early }

// EarlyAccepted waits for the handshake's carrier verdict. The handshake
// barrier creates every outgoing stream after that decision, so this
// stream's bytes are delivered as 1-RTT even if native 0-RTT was rejected.
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
func (q *qStream) RestartEarly(ctx context.Context) (zmux.PipeIO, error) {
	qc, _ := q.cur()
	nc, err := qc.NextConnection(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	s, err := nc.OpenStreamSync(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	q.mu.Lock()
	closed := q.closed
	if !closed {
		q.qc, q.s = nc, s
	}
	q.mu.Unlock()
	if closed {
		_ = nc.CloseWithError(0, "")
		return zmux.PipeIO{}, net.ErrClosed
	}
	clientLanes(nc)
	r := &restarted{q: q}
	return zmux.PipeIO{R: r, W: r}, nil
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

// HandshakeFailed is closed when the connection ended; a handshake that
// completed before is not waited for (zmux.EarlyServerCarrier).
func (q *qStream) HandshakeFailed() <-chan struct{} {
	qc, _ := q.cur()
	return qc.Context().Done()
}

// restarted is the byte stream after a 0-RTT rejection.
type restarted struct{ q *qStream }

// CarrierSampler is for internal use by zmux modules.
func (r *restarted) CarrierSampler() *carrierstats.Sampler { return r.q.CarrierSampler() }

func (r *restarted) Read(p []byte) (int, error) {
	_, s := r.q.cur()
	return s.Read(p)
}
func (r *restarted) Write(p []byte) (int, error) { return r.q.Write(p) }

func (r *restarted) PreferFlatWrite() {}

// WriteBuffers writes a batch with one stream write (one locked copy).
func (r *restarted) WriteBuffers(bufs [][]byte) (int, error) { return r.q.WriteBuffers(bufs) }
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
		q.halfEnded()
	})
	return nil
}

// RecordCloseCode of the receiving half (zmux.CloseCodeRecorder).
func (r *qReader) RecordCloseCode(code zmux.ApplicationErrorCode, reason string) {
	(*qStream)(r).RecordCloseCode(code, reason)
}

// CarrierConn of the receiving half (for lanes routed by HandlePipe).
func (r *qReader) CarrierConn() any { return (*qStream)(r).CarrierConn() }

func (r *qReader) CarrierSampler() *carrierstats.Sampler { return (*qStream)(r).CarrierSampler() }

// laneWriter is an outbound lane.
type laneWriter struct {
	s    *quic.SendStream
	qc   *quic.Conn
	send streamio.Writer
}

func (l *laneWriter) Write(p []byte) (int, error) { return l.send.Write(l.s, p) }

func (l *laneWriter) PreferFlatWrite() {}

// WriteBuffers writes a batch with one ordered stream write.
func (l *laneWriter) WriteBuffers(bufs [][]byte) (int, error) { return carrier.WriteFlat(l, bufs) }
func (l *laneWriter) Close() error                            { return l.send.Close(l.s) }
func (l *laneWriter) Abort(_ error)                           { l.send.Abort(func() { l.s.CancelWrite(carrierFailed) }) }
func (l *laneWriter) CarrierSampler() *carrierstats.Sampler   { return ConnectionSampler(l.qc) }

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
func (l *laneReader) CarrierConn() any                      { return l.qc }
func (l *laneReader) CarrierSampler() *carrierstats.Sampler { return ConnectionSampler(l.qc) }

// ConnPool opens pipes on a pool of QUIC connections to one server: at most
// PoolLimits.MaxPipesPerConnection pipes per connection at once,
// connections retired by lifetime limits or failure; the pipes of one
// connection share a fate group. It is safe for concurrent use.
type ConnPool struct {
	cred zmux.CarrierCredential
	pool carrier.ConnPool[*pooled]
}

// pooled is a pooled connection; its first stream carries the credential.
type pooled struct {
	qc         *quic.Conn
	socketDone <-chan struct{}
	rec        streamio.CloseRecord // the close code of the last session it carried
	mu         sync.Mutex           // held while the first stream is opened
	presented  bool
	failed     bool // the credential failed to go out (under mu)
}

// errCredentialFailed reports a connection whose first stream failed to
// carry the credential: the server rejects every later stream of it.
var errCredentialFailed = errors.New("zmuxquic: the connection's credential failed")

// Shutdown retires every connection: idle ones close now, the others once
// their pipes end; OpenPipe fails afterwards.
func (p *ConnPool) Shutdown() { p.pool.Shutdown() }

// NewConnPool returns a pool that dials addr with tlsConf and qconf (may be
// nil), within lim. The first stream of each connection starts with
// cred's prefix (nil: none), as a server admission reads it.
func NewConnPool(addr string, tlsConf *tls.Config, qconf *quic.Config, cred zmux.CarrierCredential, lim carrier.PoolLimits) *ConnPool {
	p := &ConnPool{cred: cred}
	p.pool = carrier.ConnPool[*pooled]{PoolLimits: lim, Close: closePooled,
		Traffic: func(pc *pooled) (uint64, uint64) { return ConnectionTraffic(pc.qc) },
		Dial: func(ctx context.Context) (*pooled, error) {
			qc, socketDone, err := dialAddr(ctx, addr, tlsConf, QUICConfig(qconf), false)
			if err != nil {
				return nil, err
			}
			clientLanes(qc)
			pc := &pooled{qc: qc, socketDone: socketDone}
			context.AfterFunc(qc.Context(), func() { p.pool.RetireConn(pc) })
			return pc, nil
		}}
	return p
}

// closePooled closes a retired connection once none of its pipes has not
// ended (each pipe's inbound side ended or lingered out), with the code of
// the last session it carried.
func closePooled(pc *pooled) {
	code, reason := pc.rec.Get()
	_ = pc.qc.CloseWithError(quic.ApplicationErrorCode(code), reason)
}

// OpenPipe opens a pipe (a bidirectional stream with its lanes) on a
// pooled connection; its PipeIO carries the connection's fate group.
func (p *ConnPool) OpenPipe(ctx context.Context) (zmux.PipeIO, error) {
	if p.pool.Dial == nil {
		return zmux.PipeIO{}, errors.New("zmuxquic: a ConnPool is made by NewConnPool")
	}
	l, pio, err := p.lease(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	// The lease ends with the pipe: once both of its halves have ended,
	// that is once the session ended it after its inbound side ended or
	// lingered out, so that the connection is never closed under a zmux
	// close the peer has not read; or with the connection.
	q := pio.W.(*qStream)
	q.rec, q.lease = &l.Conn.rec, carrierio.NewHalfLease(l.Conn.qc.Context(), l.Release)
	pio.FateGroup, pio.Liveness = l.FateGroup, l.Liveness
	return pio, nil
}

// lease opens a stream on a pooled connection. A call that waited for a
// connection whose credential then failed to go out takes another one.
func (p *ConnPool) lease(ctx context.Context) (*carrier.Lease[*pooled], zmux.PipeIO, error) {
	for {
		l, err := p.pool.Get(ctx)
		if err != nil {
			return nil, zmux.PipeIO{}, err
		}
		pio, err := p.open(ctx, l.Conn)
		if err == nil {
			return l, pio, nil
		}
		l.Release()
		if !errors.Is(err, errCredentialFailed) {
			return nil, zmux.PipeIO{}, err
		}
	}
}

// open opens a stream of pc. The first one (the lowest stream ID) starts
// with the credential's prefix; streams are opened one at a time. When
// the credential fails to go out, the connection is retired under pc.mu,
// so the calls waiting for it fail with errCredentialFailed and no later
// stream goes on it.
func (p *ConnPool) open(ctx context.Context, pc *pooled) (zmux.PipeIO, error) {
	qc := pc.qc
	if p.cred == nil {
		return openPipe(ctx, &qc)
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.failed {
		return zmux.PipeIO{}, errCredentialFailed
	}
	if pc.presented {
		return openPipe(ctx, &qc)
	}
	pio, err := openPipe(ctx, &qc)
	if err != nil {
		return pio, err // nothing went out: the next first stream presents
	}
	if err := zmux.PresentPipe(ctx, pio, p.cred); err != nil {
		pio.W.(*qStream).Abort(err)
		pc.failed = true
		p.pool.RetireConn(pc)
		return zmux.PipeIO{}, err
	}
	pc.presented = true
	return pio, nil
}

// SetReadDeadline bounds reads of the receiving half.
func (r *qReader) SetReadDeadline(t time.Time) error {
	_, s := (*qStream)(r).cur()
	return s.SetReadDeadline(t)
}

// Candidate is a QUIC stream to addr for the session controller, for a client that
// reaches the server directly: each carrier is the first stream of a new
// connection (dialed as NewConnPool does, without 0-RTT), which starts with
// the session's credential and is closed once the pipe has ended, with
// the session's code.
func Candidate(addr string, tlsConf *tls.Config, qconf *quic.Config) zmux.Candidate {
	return zmux.Candidate{Kind: zmux.CarrierQUIC, CanStart: true, Attachments: []zmux.AttachmentSpec{{Directions: zmux.Both}},
		Open: func(ctx context.Context, req zmux.OpenRequest) ([]zmux.PipeIO, error) {
			p := NewConnPool(addr, tlsConf, qconf, req.Credential, carrier.PoolLimits{MaxPipesPerConnectionLifetime: 1})
			pipe, err := p.OpenPipe(ctx)
			return []zmux.PipeIO{pipe}, err
		}}
}

// TakeoverConn returns the QUIC connection of a carrier that the admission
// handed to zmux.ListenerConfig.Fallback, when it is a stream of one
// (Serve, ServeConn); false otherwise. The connection is closed once the
// fallback returned, as the stream is, unless the host detached the
// carrier (zmux.Takeover.Detach): the connection is then the host's too,
// and zmux accepts none of its further streams.
func TakeoverConn(t *zmux.Takeover) (*quic.Conn, bool) {
	cc, ok := t.Pipe().W.(zmux.CarrierConner)
	if !ok {
		return nil, false
	}
	qc, ok := cc.CarrierConn().(*quic.Conn)
	return qc, ok && qc != nil
}
