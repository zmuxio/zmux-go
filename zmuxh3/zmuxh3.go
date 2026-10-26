// Package zmuxh3 carries zmux pipes over HTTP/3 request streams and
// WebTransport sessions (quic-go): an HTTP/3 request's body carries the up
// direction and its response body the down direction; on WebTransport a
// pipe pair is one bidirectional stream and its further lanes are
// unidirectional streams of the session (MULTIPATH.md 2.5).
package zmuxh3

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
)

const carrierFailed = webtransport.StreamErrorCode(zmux.CodeCarrierFailed)

// ---- HTTP/3 request streams ----

// Client returns an http.Client whose requests run over HTTP/3, for
// zmux.HTTPPipe and zmux.HTTPSplitPipe, and its transport, which the
// caller closes when done.
func Client(tlsConf *tls.Config, qconf *quic.Config) (*http.Client, *http3.Transport) {
	t := &http3.Transport{TLSClientConfig: tlsConf, QUICConfig: qconf}
	return &http.Client{Transport: t}, t
}

// Server returns an HTTP/3 server that hands its requests to l (an
// http.Handler) as carriers; the caller starts it (ListenAndServe or
// Serve) and closes it.
func Server(addr string, tlsConf *tls.Config, qconf *quic.Config, l *zmux.Listener) *http3.Server {
	return &http3.Server{Addr: addr, TLSConfig: http3.ConfigureTLSConfig(tlsConf), QUICConfig: qconf, Handler: l}
}

// ---- WebTransport ----

// Handler returns an http.Handler for an HTTP/3 server configured with
// webtransport.ConfigureHTTP3Server: it upgrades each request to a
// WebTransport session and hands its bidirectional streams to l as
// carriers and its unidirectional streams as lanes, until the session
// ends.
func Handler(s *webtransport.Server, l *zmux.Listener) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := s.Upgrade(w, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ServeSession(sess.Context(), sess, l)
	})
}

// ServeSession hands the streams of a WebTransport session to l until the
// session ends.
func ServeSession(ctx context.Context, sess *webtransport.Session, l *zmux.Listener) {
	src := hostOf(sess.RemoteAddr())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			s, err := sess.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			p := zmux.PipeIO{R: &laneReader{s: s, sess: sess}}
			go func() { _ = l.HandlePipe(ctx, p, zmux.JoinHint{Source: src}) }()
		}
	}()
	for {
		s, err := sess.AcceptStream(ctx)
		if err != nil {
			break
		}
		go func() { _ = l.HandlePipe(ctx, StreamPipe(sess, s), zmux.JoinHint{Source: src}) }()
	}
	wg.Wait()
}

// DialSession opens a WebTransport session and starts a zmux session on
// its first bidirectional stream (lanes wired).
func DialSession(ctx context.Context, t *webtransport.Transport, url string, h http.Header, cfg *zmux.Config) (*zmux.Conn, *webtransport.Session, error) {
	_, sess, err := t.Dial(ctx, url, h)
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
	go acceptLanes(sess)
	return c, sess, nil
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
		go func() { _ = zmux.HandleLane(sess.Context(), zmux.PipeIO{R: &laneReader{s: s, sess: sess}}) }()
	}
}

// Pool opens pipes on a pool of WebTransport sessions to one URL (a
// carrier pool, API.md Appendix A): at most MaxPipesPerConnection pipes per
// session at once, sessions retired by lifetime limits or failure; the
// pipes of one session share a fate group.
type Pool struct {
	// MaxPipesPerConnection bounds the pipes on a session at once (8).
	MaxPipesPerConnection int
	// MaxPipesPerConnectionLifetime bounds the pipes a session carries over
	// its life (0: unlimited).
	MaxPipesPerConnectionLifetime int
	// MaxConnectionAge stops giving pipes to a session this old (0:
	// unlimited).
	MaxConnectionAge time.Duration

	once sync.Once
	pool zmux.CarrierPool[*webtransport.Session]
	dial func(ctx context.Context) (*webtransport.Session, error)
}

func (p *Pool) init() {
	p.once.Do(func() {
		p.pool = zmux.CarrierPool[*webtransport.Session]{Dial: p.dial, Close: closePooled,
			MaxPipesPerConnection:         p.MaxPipesPerConnection,
			MaxPipesPerConnectionLifetime: p.MaxPipesPerConnectionLifetime,
			MaxConnectionAge:              p.MaxConnectionAge}
	})
}

// Shutdown retires every session: idle ones close now, the others once
// their pipes end; OpenPipe fails afterwards.
func (p *Pool) Shutdown() {
	p.init()
	p.pool.Shutdown()
}

// NewPool returns a pool that dials url with t and the headers h; its
// limits are read at the first OpenPipe.
func NewPool(t *webtransport.Transport, url string, h http.Header) *Pool {
	p := &Pool{}
	p.dial = func(ctx context.Context) (*webtransport.Session, error) {
		_, sess, err := t.Dial(ctx, url, h)
		if err != nil {
			return nil, err
		}
		go acceptLanes(sess)
		context.AfterFunc(sess.Context(), func() { p.pool.RetireConn(sess) })
		return sess, nil
	}
	return p
}

func closePooled(sess *webtransport.Session) { _ = sess.CloseWithError(0, "") }

// OpenPipe opens a pipe on a pooled session; its PipeIO carries the
// session's fate group.
func (p *Pool) OpenPipe(ctx context.Context) (zmux.PipeIO, error) {
	if p.dial == nil {
		return zmux.PipeIO{}, errors.New("zmuxh3: a Pool is made by NewPool")
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
	pio := StreamPipe(l.Conn, s)
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
	return StreamPipe(sess, s), nil
}

// StreamPipe adapts a bidirectional WebTransport stream to a pipe input.
func StreamPipe(sess *webtransport.Session, s *webtransport.Stream) zmux.PipeIO {
	ws := &wtStream{sess: sess, s: s}
	return zmux.PipeIO{R: (*wtReader)(ws), W: ws}
}

type wtStream struct {
	sess  *webtransport.Session
	s     *webtransport.Stream
	wOnce sync.Once
	rOnce sync.Once
}

func (w *wtStream) Write(p []byte) (int, error) { return w.s.Write(p) }

func (w *wtStream) Close() error {
	var err error
	w.wOnce.Do(func() { err = w.s.Close() })
	return err
}

func (w *wtStream) Abort(error) {
	w.wOnce.Do(func() { w.s.CancelWrite(carrierFailed) })
	w.rOnce.Do(func() { w.s.CancelRead(carrierFailed) })
}

// CarrierConn is the WebTransport session (lanes are its streams).
func (w *wtStream) CarrierConn() any { return w.sess }

// ConnectionState exposes the QUIC connection's TLS state (exporter).
func (w *wtStream) ConnectionState() tls.ConnectionState {
	return w.sess.SessionState().ConnectionState.TLS
}

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
func (l *laneWriter) Close() error {
	var err error
	l.once.Do(func() { err = l.s.Close() })
	return err
}
func (l *laneWriter) Abort(error) { l.once.Do(func() { l.s.CancelWrite(carrierFailed) }) }

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

func hostOf(a net.Addr) string {
	if h, _, err := net.SplitHostPort(a.String()); err == nil {
		return h
	}
	return a.String()
}
