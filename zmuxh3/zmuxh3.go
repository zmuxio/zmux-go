// Package zmuxh3 carries zmux pipes over HTTP/3 request streams and
// draft-16 WebTransport sessions. An HTTP/3 request's body carries the up
// direction and its response body the down direction; on WebTransport a
// pipe pair is one bidirectional stream and its further lanes are
// unidirectional streams of the session. WebTransport can use session
// datagrams.
//
// An ordinary HTTP/3 POST carrier uses record datagrams exclusively.
// Deploy both ends together. On the client, use HTTP3Candidate or
// zmuxhttp.Streaming with an http.Client. To serve clients that do not
// negotiate HTTP Datagrams, run a separate http3.Server with
// EnableDatagrams set to false and wrap its handler with HTTP3Handler.
package zmuxh3

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/zmuxhttp"
	"github.com/zmuxio/zmux-go/zmuxquic"
	"github.com/zmuxio/zmux-go/zmuxquic/streamio"
)

const carrierFailed = StreamErrorCode(zmux.StreamCarrierFailed)

// ---- HTTP/3 request streams ----

// quicState provides the TLS state and carrier identity of a QUIC connection.
type quicState struct {
	qc  *quic.Conn
	tls *tls.ConnectionState
}

func (s quicState) ConnectionState() tls.ConnectionState {
	if s.tls != nil {
		return *s.tls
	}
	return s.qc.ConnectionState().TLS
}

func (s quicState) CarrierSampleConn() any { return s.qc }

// CarrierSampler is for internal use by zmux modules.
func (s quicState) CarrierSampler() *carrierstats.Sampler {
	return zmuxquic.ConnectionSampler(s.qc)
}

type quicConnKey struct{}

// QUICConnContext makes the concrete connection available to HTTP/3 and
// WebTransport carrier adapters. Use it as http3.Server.ConnContext for a
// caller-configured HTTP/3 server.
func QUICConnContext(ctx context.Context, conn *quic.Conn) context.Context {
	return context.WithValue(ctx, quicConnKey{}, quicState{qc: conn})
}

func quicInContext(ctx context.Context) quicState {
	s, _ := ctx.Value(quicConnKey{}).(quicState)
	return s
}

// NewServer returns an HTTP/3 server that serves h, typically the
// handler of zmuxhttp.NewHandler, which hands requests to a listener as
// carriers; the caller starts it (ListenAndServe or Serve) and closes it.
// It enables HTTP Datagram negotiation and reports carrier identity through
// HTTP3Handler, whose o it takes. qconf may be nil: no
// 0-RTT. With qconf.Allow0RTT a request may arrive in 0-RTT, before the
// handshake completes; the zmux adapters answer such a carrier request 425
// (Too Early).
func NewServer(addr string, tlsConf *tls.Config, qconf *quic.Config, h http.Handler, o *zmuxhttp.Options) *http3.Server {
	if qconf == nil {
		qconf = &quic.Config{} // http3 would otherwise allow 0-RTT
	}
	return &http3.Server{Addr: addr, TLSConfig: http3.ConfigureTLSConfig(tlsConf), QUICConfig: qconf,
		Handler: HTTP3Handler(h, o), EnableDatagrams: true, ConnContext: QUICConnContext}
}

// ---- WebTransport ----

// NewHandler returns an http.Handler that serves WebTransport carriers of l
// through s. Set the handler on s.H3 before calling Server.Serve.
// Each request passes l's admission on
// its head (zmux.Listener.AdmitUpgrade; the body is not read), or defers
// it to the session's first bidirectional stream (zmux.VerdictDefer;
// zmux.Arrival.AdmitFirst bounds the wait for that stream), is upgraded
// to a WebTransport session; nothing else of the session is handed on
// before its first stream is admitted, and a session whose first stream
// is not accepted, or comes too late, is closed once the verdict was
// acted on (unless the host detached the stream it took over,
// zmux.Takeover.Detach: the session, the stream's CarrierConn, is then the
// host's); and the session's bidirectional
// streams go to l as carriers and its unidirectional streams as lanes,
// until the session ends, with the request's source (o.Trusted). A request
// forwarded from early data, or received in 0-RTT, is answered 425. An
// unclaimed request can fall back to o.Next while its response is untouched.
// o may be nil. Invalid options return an error.
func NewHandler(l *zmux.Listener, o *zmuxhttp.Options, s *Server) (http.Handler, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if o == nil {
		o = &zmuxhttp.Options{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, ok := l.Adapter().AdmitUpgrade(w, r, o.Trusted, o.Next)
		if !ok {
			return
		}
		// The first bidirectional stream is the session's first carrier:
		// nothing else of it is handed on before it is decided, and
		// AdmitFirst bounds the wait for it while the decision is deferred.
		var sess *Session
		var abortResponse bool
		p, ok := a.AdmitFirst(r.Context(), func(ctx context.Context) (zmux.PipeIO, error) {
			var err error
			var response upgradeState
			sess, response, err = upgradeWithin(ctx, s, w, r)
			if err == nil {
				var first *Stream
				first, err = sess.AcceptStream(ctx)
				if err == nil {
					return StreamPipe(sess, first), nil
				}
			}
			switch {
			case sess != nil:
				// Finish this CONNECT inside the arrival's reservation.
				_ = sess.CloseWithError(0, "")
				sess = nil
			case !response.started && response.taken == nil && ctx.Err() == nil:
				o.Fallback(w, r)
			default:
				abortResponse = response.taken == nil && !response.started
			}
			return zmux.PipeIO{}, err
		})
		if abortResponse {
			panic(http.ErrAbortHandler)
		}
		if !ok {
			if sess != nil && !a.Detached() { // a detached takeover leaves the session to the host
				_ = sess.CloseWithError(0, "")
			}
			return
		}
		// The first setup belongs to this CONNECT request. Later streams
		// use the session lifetime and obtain their own reservations.
		first, cancel := context.WithCancel(r.Context())
		dispatched := make(chan struct{})
		labels.Go(first, "wt-pipe", func() {
			defer close(dispatched)
			_ = a.HandlePipe(first, p, zmux.PipeHint{})
		})
		_ = serve(sess.Context(), l, sess, a)
		cancel()
		<-dispatched
	}), nil
}

// Serve hands the streams of an accepted WebTransport session to l, as
// zmuxquic.Serve does for a QUIC listener: each bidirectional stream is a
// carrier (a bootstrap or a join), each unidirectional stream a lane,
// each passing l's admission on its own (zmux.Listener.HandlePipe).
//
// The HTTP/3 session is served request by request: a source slot that is
// full, the admission, a refusal at a limit and a closed listener are
// answered on the stream they concern (it is reset or closed), and never end
// the call, unlike zmuxquic.ServeConn, which hands a whole connection over.
// Serve reports only the end of the session: nil when it was closed (by
// either side, with any application code), the connection's error when it
// failed (an idle timeout, a transport error), and ctx's error when ctx ends
// first; the zmux sessions continue on their pipes.
func Serve(ctx context.Context, l *zmux.Listener, sess *Session) error {
	return serve(ctx, l, sess, nil)
}

// serve is Serve, or with a the streams of a session admitted as one
// arrival by its first stream, first.
func serve(ctx context.Context, l *zmux.Listener, sess *Session, a *zmux.Arrival) error {
	handle := func(p zmux.PipeIO) {
		if a != nil {
			_ = a.HandlePipe(ctx, p, zmux.PipeHint{})
		} else {
			_ = l.HandlePipe(ctx, p, zmux.PipeHint{Source: addrPort(sess.RemoteAddr())})
		}
	}
	if a != nil {
		defer a.Release()
	}
	var wg sync.WaitGroup
	wg.Add(1)
	labels.Go(ctx, "wt-accept-lanes", func() {
		defer wg.Done()
		for {
			s, err := sess.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			p := zmux.PipeIO{R: &laneReader{s: s, sess: sess}, Carrier: zmux.CarrierWebTransport}
			labels.Go(ctx, "wt-lane", func() { handle(p) })
		}
	})
	var err error
	for {
		var s *Stream
		s, err = sess.AcceptStream(ctx)
		if err != nil {
			break
		}
		p := StreamPipe(sess, s)
		labels.Go(ctx, "wt-pipe", func() { handle(p) })
	}
	wg.Wait()
	return sessionEnd(err)
}

// sessionEnd translates a clean session close into the Serve result.
func sessionEnd(err error) error {
	if _, ok := errors.AsType[*SessionError](err); ok {
		return nil
	}
	if ae, ok := errors.AsType[*quic.ApplicationError](err); ok && ae.ErrorCode == quic.ApplicationErrorCode(http3.ErrCodeNoError) {
		return nil
	}
	if he, ok := errors.AsType[*http3.Error](err); ok && he.ErrorCode == http3.ErrCodeNoError {
		return nil
	}
	return err
}

// Dial opens a WebTransport session to url with t and the headers h and
// starts a zmux client session on its first bidirectional stream, as
// zmuxquic.Dial does on a QUIC connection; the lanes the server opens are
// accepted. The CONNECT request carries cfg.CarrierCredential (DialSession). The
// caller, which may start further zmux sessions on the returned
// WebTransport session (SessionPipe), closes it once the last of them has
// wholly ended (Wait returned, or Done is closed), not sooner: closing it
// resets its streams, the zmux close included. Its code is the last
// session's (SessionCloseCode).
func Dial(ctx context.Context, t *Transport, url string, h http.Header, cfg *zmux.Config) (*zmux.Conn, *Session, error) {
	var cred zmux.CarrierCredential
	if cfg != nil {
		cred = cfg.CarrierCredential
	}
	sess, err := DialSession(ctx, t, url, h, cred)
	if err != nil {
		return nil, nil, err
	}
	p, err := SessionPipe(ctx, sess)
	if err != nil {
		_ = sess.CloseWithError(0, "")
		return nil, nil, err
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, cfg)
	if err != nil {
		_ = sess.CloseWithError(0, "")
		return nil, nil, err
	}
	labels.Go(context.Background(), "wt-client-lanes", func() { acceptLanes(sess) })
	return c, sess, nil
}

// lingerTimeout is the zmux linger timeout for a smoothed RTT:
// min(2 s, max(250 ms, 3 x SRTT)).
func lingerTimeout(srtt time.Duration) time.Duration {
	return min(2*time.Second, max(250*time.Millisecond, 3*srtt))
}

// SessionCloseCode is the WebTransport session close code and message for
// a zmux session that ended with err: its application error code and
// reason, 0 and none after any other end or for a code beyond the 32 bits
// of a WebTransport session code.
func SessionCloseCode(err error) (SessionErrorCode, string) {
	if ae, ok := errors.AsType[*zmux.ApplicationError](err); ok {
		return sessionCloseCode(uint64(ae.ErrorCode), ae.ErrorMessage)
	}
	return 0, ""
}

func sessionCloseCode(code uint64, reason string) (SessionErrorCode, string) {
	if code > math.MaxUint32 {
		return 0, ""
	}
	return SessionErrorCode(code), reason
}

// acceptLanes routes the lanes the server opens on a client session.
func acceptLanes(sess *Session) {
	for {
		s, err := sess.AcceptUniStream(sess.Context())
		if err != nil {
			return
		}
		// HandleLane closes a refused or failed lane itself; there is no
		// caller to report its error to.
		labels.Go(context.Background(), "wt-lane", func() {
			_ = zmux.HandleLane(sess.Context(), zmux.PipeIO{R: &laneReader{s: s, sess: sess}, Carrier: zmux.CarrierWebTransport})
		})
	}
}

// ConnPool opens pipes on a pool of WebTransport sessions to one URL: at most
// PoolLimits.MaxPipesPerConnection pipes per session at once, sessions
// retired by lifetime limits or failure; the pipes of one session share a
// fate group. It is safe for concurrent use.
type ConnPool struct {
	pool carrier.ConnPool[*wtPooled]
}

// wtPooled is a pooled WebTransport session.
type wtPooled struct {
	sess *Session
	rec  streamio.CloseRecord // the close code of the last zmux session it carried
}

// Shutdown retires every session: idle ones close now, the others once
// their pipes end; OpenPipe fails afterwards.
func (p *ConnPool) Shutdown() { p.pool.Shutdown() }

// NewConnPool returns a pool that dials url with t, the headers h and cred
// (DialSession), within lim.
func NewConnPool(t *Transport, url string, h http.Header, cred zmux.CarrierCredential, lim carrier.PoolLimits) *ConnPool {
	p := &ConnPool{}
	p.pool = carrier.ConnPool[*wtPooled]{PoolLimits: lim, Close: closePooled,
		Traffic: func(wp *wtPooled) (uint64, uint64) {
			return zmuxquic.ConnectionTraffic(quicInContext(wp.sess.Context()).qc)
		},
		Dial: func(ctx context.Context) (*wtPooled, error) {
			sess, err := DialSession(ctx, t, url, h, cred)
			if err != nil {
				return nil, err
			}
			labels.Go(context.Background(), "wt-client-lanes", func() { acceptLanes(sess) })
			wp := &wtPooled{sess: sess}
			context.AfterFunc(sess.Context(), func() { p.pool.RetireConn(wp) })
			return wp, nil
		}}
	return p
}

// closePooled closes a retired session once none of its pipes has not
// ended (each pipe's inbound side ended or lingered out), with the code of
// the last zmux session it carried.
func closePooled(wp *wtPooled) {
	raw, reason := wp.rec.Get()
	code, reason := sessionCloseCode(raw, reason)
	_ = wp.sess.CloseWithError(code, reason)
}

// OpenPipe opens a pipe on a pooled session; its PipeIO carries the
// session's fate group.
func (p *ConnPool) OpenPipe(ctx context.Context) (zmux.PipeIO, error) {
	if p.pool.Dial == nil {
		return zmux.PipeIO{}, errors.New("zmuxh3: a ConnPool is made by NewConnPool")
	}
	l, err := p.pool.Get(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	s, err := l.Conn.sess.OpenStreamSync(ctx)
	if err != nil {
		l.Release()
		return zmux.PipeIO{}, err
	}
	// The lease ends once both halves of the pipe have ended, after its
	// inbound side ended or lingered out, or with the session.
	ws := newWTStream(l.Conn.sess, s)
	ws.rec = &l.Conn.rec
	ws.lease = carrierio.NewHalfLease(l.Conn.sess.Context(), l.Release)
	pio := ws.pipe()
	pio.FateGroup, pio.Liveness = l.FateGroup, l.Liveness
	return pio, nil
}

// SessionPipe opens a bidirectional stream of a WebTransport session as a
// pipe input (a bootstrap or a join).
func SessionPipe(ctx context.Context, sess *Session) (zmux.PipeIO, error) {
	s, err := sess.OpenStreamSync(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	return StreamPipe(sess, s), nil
}

// StreamPipe adapts a bidirectional stream of a WebTransport session
// to a pipe input (with lanes on the session's unidirectional streams).
func StreamPipe(sess *Session, s *Stream) zmux.PipeIO {
	return newWTStream(sess, s).pipe()
}

func newWTStream(sess *Session, s *Stream) *wtStream {
	w := &wtStream{sess: sess}
	w.s = s
	w.state = quicInContext(sess.Context())
	return w
}

type wtStream struct {
	sess *Session
	// rec records the close code for the pooled WebTransport session.
	rec *streamio.CloseRecord
	wtWriter[*Stream]
	rOnce sync.Once
}

// pipe is the pipe input over w.
func (w *wtStream) pipe() zmux.PipeIO {
	return zmux.PipeIO{R: (*wtReader)(w), W: w, Carrier: zmux.CarrierWebTransport}
}

// RecordCloseCode records the code of the pipe's session
// (zmux.CloseCodeRecorder).
func (w *wtStream) RecordCloseCode(code zmux.ApplicationErrorCode, reason string) {
	if w.rec != nil {
		w.rec.Set(uint64(code), reason)
	}
}

// Abort fails both halves of the WebTransport stream.
func (w *wtStream) Abort(err error) {
	w.wtWriter.Abort(err)
	_ = (*wtReader)(w).Close()
}

// CarrierConn is the WebTransport session (lanes are its streams).
func (w *wtStream) CarrierConn() any     { return w.sess }
func (w *wtStream) PhysicalCarrier() any { return quicInContext(w.sess.Context()).qc }

// ConnectionState exposes the QUIC connection's TLS state (exporter).
func (w *wtStream) ConnectionState() tls.ConnectionState {
	return w.sess.state.TLS
}

// CarrierSecure declares the Bound contract: a WebTransport stream is a QUIC stream of a
// connection this process terminates.
func (w *wtStream) CarrierSecure() bool { return true }

// OpenLane opens a unidirectional stream of the session as a lane.
func (w *wtStream) OpenLane(ctx context.Context) (io.WriteCloser, error) {
	s, err := w.sess.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &wtWriter[*Stream]{s: s, state: w.state}, nil
}

type wtReader wtStream

func (r *wtReader) Read(p []byte) (int, error) { return r.s.Read(p) }

func (r *wtReader) Close() error {
	r.rOnce.Do(func() { r.s.CancelRead(carrierFailed); r.lease.HalfEnded() })
	return nil
}

// RecordCloseCode of the receiving half (zmux.CloseCodeRecorder).
func (r *wtReader) RecordCloseCode(code zmux.ApplicationErrorCode, reason string) {
	(*wtStream)(r).RecordCloseCode(code, reason)
}

func (r *wtReader) CarrierConn() any                      { return r.sess }
func (r *wtReader) PhysicalCarrier() any                  { return quicInContext(r.sess.Context()).qc }
func (r *wtReader) CarrierSampler() *carrierstats.Sampler { return (*wtStream)(r).CarrierSampler() }

type laneReader struct {
	s    *Stream
	sess *Session
	once sync.Once
}

func (l *laneReader) Read(p []byte) (int, error) { return l.s.Read(p) }
func (l *laneReader) Close() error {
	l.once.Do(func() { l.s.CancelRead(carrierFailed) })
	return nil
}
func (l *laneReader) CarrierConn() any { return l.sess }
func (l *laneReader) CarrierSampler() *carrierstats.Sampler {
	return quicInContext(l.sess.Context()).CarrierSampler()
}

// WebTransportCandidate is a WebTransport stream to url for the session controller,
// for a client that reaches the server directly: each carrier is the first
// stream of a new WebTransport session (DialSession with the headers h and
// the session's credential), which is closed once the pipe has ended, with
// the zmux session's code.
func WebTransportCandidate(t *Transport, url string, h http.Header) zmux.Candidate {
	return zmux.Candidate{Kind: zmux.CarrierWebTransport, CanStart: true, Attachments: []zmux.AttachmentSpec{{Directions: zmux.Both}},
		Open: func(ctx context.Context, req zmux.OpenRequest) ([]zmux.PipeIO, error) {
			p := NewConnPool(t, url, h, req.Credential, carrier.PoolLimits{MaxPipesPerConnectionLifetime: 1})
			pipe, err := p.OpenPipe(ctx)
			return []zmux.PipeIO{pipe}, err
		}}
}

// HTTP3Candidate is a bidirectional streaming request over HTTP/3 to url
// for the session controller (zmuxhttp.Streaming.Candidate with an HTTP/3 client, such
// as HTTPClient's). Its kind describes this end's hop only: the first
// peer may be an HTTP/3 intermediary in front of the server.
func HTTP3Candidate(client *http.Client, url string, h http.Header) zmux.Candidate {
	c := zmuxhttp.Streaming{Client: client, URL: url, Header: h}.Candidate()
	c.Kind = zmux.CarrierHTTP3
	open := c.Open
	c.Open = func(ctx context.Context, req zmux.OpenRequest) ([]zmux.PipeIO, error) {
		pipes, err := open(ctx, req)
		for i := range pipes {
			pipes[i].Carrier = zmux.CarrierHTTP3
		}
		return pipes, err
	}
	return c
}

// addrPort is a's address and port (zero when it is not an IP address).
func addrPort(a net.Addr) netip.AddrPort {
	ap, _ := netip.ParseAddrPort(a.String())
	return ap
}
