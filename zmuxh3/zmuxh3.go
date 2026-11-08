// Package zmuxh3 carries zmux pipes over HTTP/3 request streams and
// WebTransport sessions (quic-go): an HTTP/3 request's body carries the up
// direction and its response body the down direction; on WebTransport a
// pipe pair is one bidirectional stream and its further lanes are
// unidirectional streams of the session. Datagrams may
// go as HTTP Datagrams of a bidirectional carrier's request stream (served
// by NewHTTP3Server) or as datagrams of the WebTransport session.
package zmuxh3

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"net/url"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/labels"
)

const carrierFailed = webtransport.StreamErrorCode(zmux.StreamCarrierFailed)

// ---- HTTP/3 request streams ----

// HTTPClient returns an http.Client whose requests run over HTTP/3, for
// zmux.HTTPPipe and zmux.HTTPSplitPipe, and its transport, which the
// caller closes when done. qconf may be nil. A carrier's request names its
// QUIC connection to the carrier (zmux.HTTPCarrierBody), which then declares
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
	mu      sync.Mutex
	byLocal map[string]*quic.Conn
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
	if b, ok := req.Body.(zmux.HTTPCarrierBody); ok {
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

// NewHTTP3Server returns an HTTP/3 server that serves h, typically the
// handler of zmux.NewHTTPHandler, which hands requests to a listener as
// carriers; the caller starts it (ListenAndServe or Serve) and closes it.
// It enables HTTP Datagrams, which bidirectional carriers use as their
// datagram channel (HTTP3Handler). qconf may be nil. With qconf.Allow0RTT
// a request may arrive in 0-RTT, before the handshake completes; the zmux
// adapters answer such a carrier request 425 (Too Early).
func NewHTTP3Server(addr string, tlsConf *tls.Config, qconf *quic.Config, h http.Handler) *http3.Server {
	return &http3.Server{Addr: addr, TLSConfig: http3.ConfigureTLSConfig(tlsConf), QUICConfig: qconf,
		Handler: HTTP3Handler(h), EnableDatagrams: true}
}

// ---- WebTransport ----

// Handler returns an http.Handler for an HTTP/3 server configured with
// webtransport.ConfigureHTTP3Server: each request passes l's admission on
// its head (zmux.Listener.AdmitUpgrade; the body is not read), is
// upgraded to a WebTransport session, and the session's bidirectional
// streams go to l as carriers and its unidirectional streams as lanes,
// until the session ends, with the request's source (o.Trusted). A request
// forwarded from early data, or received in 0-RTT, is answered 425; one that cannot be upgraded
// is served by o.Next. o may be nil; an invalid o is an error.
func Handler(s *webtransport.Server, l *zmux.Listener, o *zmux.HTTPOptions) (http.Handler, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, ok := l.AdmitUpgrade(w, r, o)
		if !ok {
			return
		}
		sess, err := s.Upgrade(w, r)
		if err != nil {
			a.HandshakeFailed() // the upgrade failed: no pipe input
			o.Fallback(w, r)
			return
		}
		_ = serve(sess.Context(), sess, l, a)
	}), nil
}

// Serve hands the streams of an accepted WebTransport session to l, as
// zmuxquic.Serve does for a QUIC listener: each bidirectional stream is a
// carrier (a bootstrap or a join), each unidirectional stream a lane,
// each passing l's admission on its own (zmux.Listener.HandlePipe). It
// returns, with the error that ended the accept loop, when the session
// ends or ctx ends; the zmux sessions continue on their pipes.
func Serve(ctx context.Context, sess *webtransport.Session, l *zmux.Listener) error {
	return serve(ctx, sess, l, nil)
}

// serve is Serve, or with a the streams of a session admitted as one
// arrival.
func serve(ctx context.Context, sess *webtransport.Session, l *zmux.Listener, a *zmux.Arrival) error {
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
		p := WebTransportPipe(sess, s)
		labels.Go(ctx, "wt-pipe", func() { handle(p) })
	}
	wg.Wait()
	return err
}

// Dial opens a WebTransport session to url with t and the headers h and
// starts a zmux client session on its first bidirectional stream, as
// zmuxquic.Dial does on a QUIC connection; the lanes the server opens are
// accepted. The CONNECT request carries cfg.Credential (DialSession). The
// caller closes the returned WebTransport session after the zmux session
// ended.
func Dial(ctx context.Context, t *webtransport.Transport, url string, h http.Header, cfg *zmux.Config) (*zmux.Conn, *webtransport.Session, error) {
	var cred zmux.Credential
	if cfg != nil {
		cred = cfg.Credential
	}
	sess, err := DialSession(ctx, t, url, h, cred)
	if err != nil {
		return nil, nil, err
	}
	p, err := OpenPipe(ctx, sess)
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
// h on a new QUIC connection, which is closed with the session. With
// cred, the CONNECT request carries its Header form, bound to the
// connection's handshake: the connection is dialed with t.DialAddr (nil:
// quic.DialAddr), the request is sent after the handshake completed, and
// t.QUICConfig and t.TLSClientConfig are completed as t.Dial does. Without
// cred it is t.Dial.
func DialSession(ctx context.Context, t *webtransport.Transport, rawURL string, h http.Header, cred zmux.Credential) (*webtransport.Session, error) {
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
	context.AfterFunc(sess.Context(), func() { _ = qc.CloseWithError(0, "") })
	return sess, nil
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

// Pool opens pipes on a pool of WebTransport sessions to one URL: at most
// PoolLimits.MaxPipesPerConnection pipes per session at once, sessions
// retired by lifetime limits or failure; the pipes of one session share a
// fate group. It is safe for concurrent use.
type Pool struct {
	pool zmux.CarrierPool[*webtransport.Session]
}

// Shutdown retires every session: idle ones close now, the others once
// their pipes end; OpenPipe fails afterwards.
func (p *Pool) Shutdown() { p.pool.Shutdown() }

// NewPool returns a pool that dials url with t, the headers h and cred
// (DialSession), within lim.
func NewPool(t *webtransport.Transport, url string, h http.Header, cred zmux.Credential, lim zmux.PoolLimits) *Pool {
	p := &Pool{}
	p.pool = zmux.CarrierPool[*webtransport.Session]{PoolLimits: lim, Close: closePooled,
		Dial: func(ctx context.Context) (*webtransport.Session, error) {
			sess, err := DialSession(ctx, t, url, h, cred)
			if err != nil {
				return nil, err
			}
			labels.Go(context.Background(), "wt-client-lanes", func() { acceptLanes(sess) })
			context.AfterFunc(sess.Context(), func() { p.pool.RetireConn(sess) })
			return sess, nil
		}}
	return p
}

func closePooled(sess *webtransport.Session) { _ = sess.CloseWithError(0, "") }

// OpenPipe opens a pipe on a pooled session; its PipeIO carries the
// session's fate group.
func (p *Pool) OpenPipe(ctx context.Context) (zmux.PipeIO, error) {
	if p.pool.Dial == nil {
		return zmux.PipeIO{}, errors.New("zmuxh3: a Pool is made by NewPool")
	}
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
	pio := WebTransportPipe(l.Conn, s)
	pio.FateGroup = l.FateGroup
	return pio, nil
}

// OpenPipe opens a bidirectional stream of a WebTransport session as a
// pipe input (a bootstrap or a join).
func OpenPipe(ctx context.Context, sess *webtransport.Session) (zmux.PipeIO, error) {
	s, err := sess.OpenStreamSync(ctx)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	return WebTransportPipe(sess, s), nil
}

// WebTransportPipe adapts a bidirectional stream of a WebTransport session
// to a pipe input (with lanes on the session's unidirectional streams).
func WebTransportPipe(sess *webtransport.Session, s *webtransport.Stream) zmux.PipeIO {
	ws := &wtStream{sess: sess, s: s}
	return zmux.PipeIO{R: (*wtReader)(ws), W: ws, Carrier: zmux.CarrierWebTransport}
}

type wtStream struct {
	sess  *webtransport.Session
	s     *webtransport.Stream
	wOnce sync.Once
	rOnce sync.Once
}

func (w *wtStream) Write(p []byte) (int, error) { return w.s.Write(p) }

// WriteBuffers writes a batch with one stream write (one locked copy).
func (w *wtStream) WriteBuffers(bufs [][]byte) (int, error) { return zmux.WriteFlat(w.s, bufs) }

func (w *wtStream) Close() error {
	var err error
	w.wOnce.Do(func() { err = w.s.Close() })
	return err
}

// Abort fails both halves. A write parked on a WT_SESSION_GONE reset waits
// for the session to end and ignores CancelWrite, so the write deadline is
// what releases it.
func (w *wtStream) Abort(error) {
	_ = w.s.SetWriteDeadline(time.Unix(1, 0))
	w.wOnce.Do(func() { w.s.CancelWrite(carrierFailed) })
	w.rOnce.Do(func() { w.s.CancelRead(carrierFailed) })
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
	r.rOnce.Do(func() { r.s.CancelRead(carrierFailed) })
	return nil
}

func (r *wtReader) CarrierConn() any { return r.sess }

type laneWriter struct {
	s    *webtransport.SendStream
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

// WebTransportCandidate is a WebTransport stream to url for zmux.DialMode,
// for a client that reaches the server directly: each carrier is the first
// stream of a new WebTransport session (DialSession with the headers h and
// the session's credential), which is closed once the pipe ends.
func WebTransportCandidate(t *webtransport.Transport, url string, h http.Header) zmux.Candidate {
	return zmux.Candidate{Kind: zmux.CarrierWebTransport, Dial: func(ctx context.Context, cred zmux.Credential) (zmux.PipeIO, error) {
		p := NewPool(t, url, h, cred, zmux.PoolLimits{MaxPipesPerConnectionLifetime: 1})
		return p.OpenPipe(ctx)
	}}
}

// HTTP3Candidate is a bidirectional streaming request over HTTP/3 to url
// for zmux.DialMode (zmux.StreamingCandidate with an HTTP/3 client, such
// as HTTPClient's). Its kind describes this end's hop only: the first
// peer may be an HTTP/3 intermediary in front of the server.
func HTTP3Candidate(client *http.Client, url string, h http.Header) zmux.Candidate {
	c := zmux.StreamingCandidate(client, url, h)
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
