// Package zmuxh3 carries zmux pipes over HTTP/3 request streams and
// WebTransport sessions (quic-go): an HTTP/3 request's body carries the up
// direction and its response body the down direction; on WebTransport a
// pipe pair is one bidirectional stream and its further lanes are
// unidirectional streams of the session. Datagrams may
// go as HTTP Datagrams of a bidirectional carrier's request stream (served
// by NewServer) or as datagrams of the WebTransport session.
package zmuxh3

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

const carrierFailed = webtransport.StreamErrorCode(zmux.StreamCarrierFailed)

// ---- HTTP/3 request streams ----

// HTTPClient returns an http.Client whose requests run over HTTP/3, for
// zmuxhttp.Streaming and zmuxhttp.Split, and its transport, which the
// caller closes when done. qconf may be nil. A carrier's request names its
// QUIC connection to the carrier (zmuxhttp.CarrierBody), which then declares
// the Bound contract with the connection's exporter. The transport dials each
// connection on a socket of its own; do not replace its Dial.
func HTTPClient(tlsConf *tls.Config, qconf *quic.Config) (*http.Client, *http3.Transport) {
	c := &h3Conns{byLocal: map[string]*quic.Conn{}}
	t := &http3.Transport{TLSClientConfig: tlsConf, QUICConfig: qconf, Dial: c.dial}
	return &http.Client{Transport: &h3RoundTripper{t: t, conns: c}}, t
}

// h3Conns are the client's QUIC connections by local address, each on its
// own socket, so that the connection a request went out on is found from
// what httptrace reports.
type h3Conns struct {
	byLocal map[string]*quic.Conn
	mu      sync.Mutex
}

func (c *h3Conns) dial(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
	qc, err := quic.DialAddrEarly(ctx, addr, tlsCfg, cfg)
	if err != nil {
		return nil, err
	}
	key := qc.LocalAddr().String()
	c.mu.Lock()
	c.byLocal[key] = qc
	c.mu.Unlock()
	context.AfterFunc(qc.Context(), func() {
		c.mu.Lock()
		if c.byLocal[key] == qc {
			delete(c.byLocal, key)
		}
		c.mu.Unlock()
	})
	return qc, nil
}

func (c *h3Conns) find(local net.Addr) *quic.Conn {
	if local == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byLocal[local.String()]
}

// h3RoundTripper names a carrier request's connection to its body before
// the transport writes the request's headers.
type h3RoundTripper struct {
	t     *http3.Transport
	conns *h3Conns
}

func (r *h3RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if b, ok := req.Body.(zmuxhttp.CarrierBody); ok {
		trace := &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) {
			if i.Conn == nil {
				return
			}
			if qc := r.conns.find(i.Conn.LocalAddr()); qc != nil {
				b.SetSecureConnection(quicState{qc})
			}
		}}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	}
	return r.t.RoundTrip(req)
}

// quicState is the TLS state of a QUIC connection, read when asked.
type quicState struct{ qc *quic.Conn }

func (s quicState) ConnectionState() tls.ConnectionState { return s.qc.ConnectionState().TLS }

// NewServer returns an HTTP/3 server that serves h, typically the
// handler of zmuxhttp.NewHandler, which hands requests to a listener as
// carriers; the caller starts it (ListenAndServe or Serve) and closes it.
// It enables HTTP Datagrams, which bidirectional carriers use as their
// datagram channel (HTTP3Handler, whose o it takes). qconf may be nil. With
// qconf.Allow0RTT a request may arrive in 0-RTT, before the handshake
// completes; the zmux adapters answer such a carrier request 425 (Too Early).
func NewServer(addr string, tlsConf *tls.Config, qconf *quic.Config, h http.Handler, o *zmuxhttp.Options) *http3.Server {
	return &http3.Server{Addr: addr, TLSConfig: http3.ConfigureTLSConfig(tlsConf), QUICConfig: qconf,
		Handler: HTTP3Handler(h, o), EnableDatagrams: true}
}

// ---- WebTransport ----

// NewHandler returns an http.Handler that serves WebTransport carriers of l
// through s, a WebTransport server whose HTTP/3 server is configured with
// webtransport.ConfigureHTTP3Server: each request passes l's admission on
// its head (zmux.Listener.AdmitUpgrade; the body is not read), is
// upgraded to a WebTransport session, and the session's bidirectional
// streams go to l as carriers and its unidirectional streams as lanes,
// until the session ends, with the request's source (o.Trusted). A request
// forwarded from early data, or received in 0-RTT, is answered 425; one that cannot be upgraded
// is served by o.Next. o may be nil; an invalid o is an error.
func NewHandler(l *zmux.Listener, o *zmuxhttp.Options, s *webtransport.Server) (http.Handler, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	if o == nil {
		o = &zmuxhttp.Options{}
	}
	fallback := http.HandlerFunc(o.Fallback)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, ok := l.Adapter().AdmitUpgrade(w, r, o.Trusted, fallback)
		if !ok {
			return
		}
		sess, err := s.Upgrade(w, r)
		if err != nil {
			a.HandshakeFailed() // the upgrade failed: no pipe input
			o.Fallback(w, r)
			return
		}
		_ = serve(sess.Context(), l, sess, a)
	}), nil
}

// Serve hands the streams of an accepted WebTransport session to l, as
// zmuxquic.Serve does for a QUIC listener: each bidirectional stream is a
// carrier (a bootstrap or a join), each unidirectional stream a lane,
// each passing l's admission on its own (zmux.Listener.HandlePipe). It
// returns, with the error that ended the accept loop, when the session
// ends or ctx ends; the zmux sessions continue on their pipes.
func Serve(ctx context.Context, l *zmux.Listener, sess *webtransport.Session) error {
	return serve(ctx, l, sess, nil)
}

// serve is Serve, or with a the streams of a session admitted as one
// arrival.
func serve(ctx context.Context, l *zmux.Listener, sess *webtransport.Session, a *zmux.Arrival) error {
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
		var s *webtransport.Stream
		s, err = sess.AcceptStream(ctx)
		if err != nil {
			break
		}
		p := StreamPipe(sess, s)
		labels.Go(ctx, "wt-pipe", func() { handle(p) })
	}
	wg.Wait()
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
func Dial(ctx context.Context, t *webtransport.Transport, url string, h http.Header, cfg *zmux.Config) (*zmux.Conn, *webtransport.Session, error) {
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

// DialSession opens a WebTransport session to url with t and the headers
// h on a new QUIC connection, which is closed with the session, with its
// session close code. With
// cred, the CONNECT request carries its Header form, bound to the
// connection's handshake: the connection is dialed with t.DialAddr (nil:
// quic.DialAddr), the request is sent after the handshake completed, and
// t.QUICConfig and t.TLSClientConfig are completed as t.Dial does. Without
// cred it is t.Dial.
func DialSession(ctx context.Context, t *webtransport.Transport, rawURL string, h http.Header, cred zmux.CarrierCredential) (*webtransport.Session, error) {
	if cred == nil {
		_, sess, err := t.Dial(ctx, rawURL, h)
		return sess, err
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	qconf := t.QUICConfig
	if qconf == nil {
		qconf = &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}
	}
	tlsConf := &tls.Config{}
	if t.TLSClientConfig != nil {
		tlsConf = t.TLSClientConfig.Clone()
	}
	if len(tlsConf.NextProtos) == 0 {
		tlsConf.NextProtos = []string{http3.NextProtoH3}
	}
	dial := t.DialAddr
	if dial == nil {
		dial = quic.DialAddr
	}
	qc, err := dial(ctx, u.Host, tlsConf, qconf)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*webtransport.Session, error) {
		_ = qc.CloseWithError(0, "")
		return nil, err
	}
	select {
	case <-qc.HandshakeComplete():
	case <-qc.Context().Done():
		return fail(context.Cause(qc.Context()))
	case <-ctx.Done():
		return fail(context.Cause(ctx))
	}
	cs := qc.ConnectionState().TLS
	name, value, err := cred.Header(&zmux.CarrierInfo{Peer: addrPort(qc.RemoteAddr()), TLS: &cs, Head: true})
	if err != nil {
		return fail(err)
	}
	hh := h.Clone()
	if hh == nil {
		hh = http.Header{}
	}
	hh.Add(name, value)
	cc, err := t.NewClientConn(qc)
	if err != nil {
		return fail(err)
	}
	_, sess, err := cc.Dial(ctx, rawURL, hh)
	if err != nil {
		return fail(err)
	}
	// The connection carries this session only: it closes with the
	// session's code once the session ended, after a linger in which the
	// session's close capsule reaches the peer (a QUIC close discards
	// stream data the peer has not acknowledged), unless the peer closes
	// it first.
	context.AfterFunc(sess.Context(), func() {
		var code quic.ApplicationErrorCode
		var reason string
		var se *webtransport.SessionError
		if errors.As(closeError(sess), &se) {
			code, reason = quic.ApplicationErrorCode(se.ErrorCode), se.Message
		}
		t := time.NewTimer(lingerTimeout(qc.ConnectionStats().SmoothedRTT))
		defer t.Stop()
		select {
		case <-t.C:
		case <-qc.Context().Done():
		}
		_ = qc.CloseWithError(code, reason)
	})
	return sess, nil
}

// lingerTimeout is the zmux linger timeout for a smoothed RTT:
// min(2 s, max(250 ms, 3 x SRTT)).
func lingerTimeout(srtt time.Duration) time.Duration {
	return min(2*time.Second, max(250*time.Millisecond, 3*srtt))
}

// closeError is the error that ended sess (a *webtransport.SessionError
// for a session close), read once its context is done: its context carries
// no cause, its stream maps do.
//
// webtransport-go v0.13 offers no accessor for it: Context has no cause and
// the error is private. Its stream maps return it from Accept once the
// session is closed; an Accept with a context already ended never waits,
// and a stream it could still return (a session not closed yet) is reset
// at once instead of being left open.
func closeError(sess *webtransport.Session) error {
	done, cancel := context.WithCancel(context.Background())
	cancel()
	s, err := sess.AcceptUniStream(done)
	if s != nil {
		s.CancelRead(carrierFailed)
	}
	return err
}

// SessionCloseCode is the WebTransport session close code and message for
// a zmux session that ended with err: its application error code and
// reason, 0 and none after any other end or for a code beyond the 32 bits
// of a WebTransport session code.
func SessionCloseCode(err error) (webtransport.SessionErrorCode, string) {
	var ae *zmux.ApplicationError
	if errors.As(err, &ae) && ae.ErrorCode <= math.MaxUint32 {
		return webtransport.SessionErrorCode(ae.ErrorCode), ae.ErrorMessage
	}
	return 0, ""
}

// acceptLanes routes the lanes the server opens on a client session.
func acceptLanes(sess *webtransport.Session) {
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
	sess *webtransport.Session
	rec  closeRecord // the close code of the last zmux session it carried
}

// closeRecord is the close code of the last zmux session a pooled
// WebTransport session carried.
type closeRecord struct {
	reason string
	code   zmux.ApplicationErrorCode
	mu     sync.Mutex
	set    bool
}

func (r *closeRecord) record(code zmux.ApplicationErrorCode, reason string) {
	r.mu.Lock()
	r.code, r.reason, r.set = code, reason, true
	r.mu.Unlock()
}

func (r *closeRecord) get() (webtransport.SessionErrorCode, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.set {
		return 0, ""
	}
	return SessionCloseCode(&zmux.ApplicationError{ErrorCode: r.code, ErrorMessage: r.reason})
}

// Shutdown retires every session: idle ones close now, the others once
// their pipes end; OpenPipe fails afterwards.
func (p *ConnPool) Shutdown() { p.pool.Shutdown() }

// NewConnPool returns a pool that dials url with t, the headers h and cred
// (DialSession), within lim.
func NewConnPool(t *webtransport.Transport, url string, h http.Header, cred zmux.CarrierCredential, lim carrier.PoolLimits) *ConnPool {
	p := &ConnPool{}
	p.pool = carrier.ConnPool[*wtPooled]{PoolLimits: lim, Close: closePooled,
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
	code, reason := wp.rec.get()
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
	ws := &wtStream{sess: l.Conn.sess, s: s, rec: &l.Conn.rec, ended: pipeRelease(l.Conn.sess.Context(), l.Release)}
	pio := ws.pipe()
	pio.FateGroup = l.FateGroup
	return pio, nil
}

// SessionPipe opens a bidirectional stream of a WebTransport session as a
// pipe input (a bootstrap or a join).
func SessionPipe(ctx context.Context, sess *webtransport.Session) (zmux.PipeIO, error) {
	s, err := sess.OpenStreamSync(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	return StreamPipe(sess, s), nil
}

// StreamPipe adapts a bidirectional stream of a WebTransport session
// to a pipe input (with lanes on the session's unidirectional streams).
func StreamPipe(sess *webtransport.Session, s *webtransport.Stream) zmux.PipeIO {
	return (&wtStream{sess: sess, s: s}).pipe()
}

// pipe is the pipe input over w.
func (w *wtStream) pipe() zmux.PipeIO {
	return zmux.PipeIO{R: (*wtReader)(w), W: w, Carrier: zmux.CarrierWebTransport}
}

type wtStream struct {
	sess *webtransport.Session
	s    *webtransport.Stream
	// rec, when set, records the close code of the pipe's session for its
	// pooled WebTransport session (zmux.CloseCodeRecorder); ended, when
	// set, is called once both halves have ended.
	rec    *closeRecord
	ended  func()
	wOnce  sync.Once
	rOnce  sync.Once
	halves atomic.Int32
}

// halfEnded counts the end of one half; the second calls ended.
func (w *wtStream) halfEnded() {
	if w.halves.Add(1) == 2 && w.ended != nil {
		w.ended()
	}
}

// RecordCloseCode records the code of the pipe's session
// (zmux.CloseCodeRecorder).
func (w *wtStream) RecordCloseCode(code zmux.ApplicationErrorCode, reason string) {
	if w.rec != nil {
		w.rec.record(code, reason)
	}
}

func (w *wtStream) Write(p []byte) (int, error) { return w.s.Write(p) }

// WriteBuffers writes a batch with one stream write (one locked copy).
func (w *wtStream) WriteBuffers(bufs [][]byte) (int, error) { return carrier.WriteFlat(w.s, bufs) }

func (w *wtStream) Close() error {
	var err error
	w.wOnce.Do(func() { err = w.s.Close(); w.halfEnded() })
	return err
}

// Abort fails both halves. A write parked on a WT_SESSION_GONE reset waits
// for the session to end and ignores CancelWrite, so the write deadline is
// what releases it.
func (w *wtStream) Abort(error) {
	_ = w.s.SetWriteDeadline(time.Unix(1, 0))
	w.wOnce.Do(func() { w.s.CancelWrite(carrierFailed); w.halfEnded() })
	w.rOnce.Do(func() { w.s.CancelRead(carrierFailed); w.halfEnded() })
}

// CarrierConn is the WebTransport session (lanes are its streams).
func (w *wtStream) CarrierConn() any { return w.sess }

// ConnectionState exposes the QUIC connection's TLS state (exporter).
func (w *wtStream) ConnectionState() tls.ConnectionState {
	return w.sess.SessionState().ConnectionState.TLS
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
	return &laneWriter{s: s}, nil
}

type wtReader wtStream

func (r *wtReader) Read(p []byte) (int, error) { return r.s.Read(p) }

func (r *wtReader) Close() error {
	r.rOnce.Do(func() { r.s.CancelRead(carrierFailed); (*wtStream)(r).halfEnded() })
	return nil
}

// RecordCloseCode of the receiving half (zmux.CloseCodeRecorder).
func (r *wtReader) RecordCloseCode(code zmux.ApplicationErrorCode, reason string) {
	(*wtStream)(r).RecordCloseCode(code, reason)
}

func (r *wtReader) CarrierConn() any { return r.sess }

type laneWriter struct {
	s    *webtransport.SendStream
	once sync.Once
}

func (l *laneWriter) Write(p []byte) (int, error) { return l.s.Write(p) }

// WriteBuffers writes a batch with one stream write (one locked copy).
func (l *laneWriter) WriteBuffers(bufs [][]byte) (int, error) { return carrier.WriteFlat(l.s, bufs) }
func (l *laneWriter) Close() error {
	var err error
	l.once.Do(func() { err = l.s.Close() })
	return err
}
func (l *laneWriter) Abort(error) {
	_ = l.s.SetWriteDeadline(time.Unix(1, 0)) // releases a write parked on WT_SESSION_GONE
	l.once.Do(func() { l.s.CancelWrite(carrierFailed) })
}

type laneReader struct {
	s    *webtransport.ReceiveStream
	sess *webtransport.Session
	once sync.Once
}

func (l *laneReader) Read(p []byte) (int, error) { return l.s.Read(p) }
func (l *laneReader) Close() error {
	l.once.Do(func() { l.s.CancelRead(carrierFailed) })
	return nil
}
func (l *laneReader) CarrierConn() any { return l.sess }

// WebTransportCandidate is a WebTransport stream to url for zmux.DialChoice,
// for a client that reaches the server directly: each carrier is the first
// stream of a new WebTransport session (DialSession with the headers h and
// the session's credential), which is closed once the pipe has ended, with
// the zmux session's code.
func WebTransportCandidate(t *webtransport.Transport, url string, h http.Header) zmux.Candidate {
	return zmux.Candidate{Kind: zmux.CarrierWebTransport, Dial: func(ctx context.Context, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
		p := NewConnPool(t, url, h, cred, carrier.PoolLimits{MaxPipesPerConnectionLifetime: 1})
		return p.OpenPipe(ctx)
	}}
}

// HTTP3Candidate is a bidirectional streaming request over HTTP/3 to url
// for zmux.DialChoice (zmuxhttp.Streaming.Candidate with an HTTP/3 client, such
// as HTTPClient's). Its kind describes this end's hop only: the first
// peer may be an HTTP/3 intermediary in front of the server.
func HTTP3Candidate(client *http.Client, url string, h http.Header) zmux.Candidate {
	c := zmuxhttp.Streaming{Client: client, URL: url, Header: h}.Candidate()
	c.Kind = zmux.CarrierHTTP3
	return c
}

// addrPort is a's address and port (zero when it is not an IP address).
func addrPort(a net.Addr) netip.AddrPort {
	ap, _ := netip.ParseAddrPort(a.String())
	return ap
}

// CloseIdleConnections passes to the transport (http.Client.CloseIdleConnections).
func (r *h3RoundTripper) CloseIdleConnections() { r.t.CloseIdleConnections() }

// pipeRelease calls release when the connection's ctx ends or when the
// returned function is called, which the pipe calls once both its halves
// ended; it then also withdraws the registration on ctx, so that a
// long-lived connection does not accumulate one per pipe.
func pipeRelease(ctx context.Context, release func()) func() {
	stop := context.AfterFunc(ctx, release)
	return func() {
		stop()
		release()
	}
}
