package zmuxh3

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Server dispatches WebTransport and HTTP/3 streams on the same QUIC
// listener. H3 supplies the TLS config and handler. Configure it before
// serving, and use this Server to serve its QUIC connections.
type Server struct {
	H3          *http3.Server
	CheckOrigin func(*http.Request) bool

	// ReorderingTimeout bounds reads of stream prefixes and the wait for
	// SETTINGS or CONNECT when they arrive out of order. Zero uses 5 seconds.
	ReorderingTimeout time.Duration
	// MaxPendingStreams bounds streams awaiting CONNECT or acceptance.
	// Zero uses 128.
	MaxPendingStreams int

	mu    sync.Mutex
	ctx   context.Context
	stop  context.CancelFunc
	conns map[*quic.Conn]*wtConn
}

func (s *Server) init() (context.Context, error) {
	if s == nil || s.H3 == nil {
		return nil, errors.New("zmuxh3: a WebTransport server needs an HTTP/3 server")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx != nil {
		if s.ctx.Err() != nil {
			return nil, http.ErrServerClosed
		}
		return s.ctx, nil
	}
	s.ctx, s.stop = context.WithCancel(context.Background())
	s.conns = make(map[*quic.Conn]*wtConn)
	s.H3.EnableDatagrams = true
	s.H3.AdditionalSettings = wtAdvertisedSettings(s.H3.AdditionalSettings)
	orig := s.H3.ConnContext
	s.H3.ConnContext = func(ctx context.Context, qc *quic.Conn) context.Context {
		if orig != nil {
			ctx = orig(ctx, qc)
		}
		s.mu.Lock()
		c := s.conns[qc]
		s.mu.Unlock()
		return context.WithValue(QUICConnContext(ctx, qc), wtConnKey{}, c)
	}
	return s.ctx, nil
}

// Serve serves QUIC and HTTP/3 on packet. The caller owns the socket.
func (s *Server) Serve(packet net.PacketConn) error {
	ctx, err := s.init()
	if err != nil {
		return err
	}
	if packet == nil || s.H3.TLSConfig == nil {
		return errors.New("zmuxh3: a packet connection and TLS config are required")
	}
	qconf := wtQUICConfig(s.H3.QUICConfig)
	if get := qconf.GetConfigForClient; get != nil {
		qconf.GetConfigForClient = func(info *quic.ClientInfo) (*quic.Config, error) {
			config, err := get(info)
			if err != nil {
				return nil, err
			}
			return wtQUICConfig(config), nil
		}
	}
	transport := &quic.Transport{Conn: packet}
	var workers sync.WaitGroup
	defer workers.Wait()
	defer func() { _ = transport.Close() }()
	ln, err := transport.ListenEarly(http3.ConfigureTLSConfig(s.H3.TLSConfig), qconf)
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()
	for {
		qc, err := ln.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return http.ErrServerClosed
			}
			return err
		}
		workers.Go(func() {
			if err := s.ServeQUICConn(qc); err != nil {
				_ = qc.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeInternalError), "")
			}
		})
	}
}

// ServeQUICConn serves a connection from a caller-managed QUIC listener.
// Enable QUIC datagrams and reserve all incoming streams for this method.
// It returns when the connection and its handlers have stopped.
func (s *Server) ServeQUICConn(qc *quic.Conn) error {
	if err := wtConnError(qc); err != nil {
		return err
	}
	ctx, err := s.init()
	if err != nil {
		return err
	}
	c := newWTConn(qc, true, s.ReorderingTimeout, s.MaxPendingStreams)
	s.mu.Lock()
	if ctx.Err() != nil {
		s.mu.Unlock()
		return http.ErrServerClosed
	}
	if s.conns[qc] != nil {
		s.mu.Unlock()
		return errors.New("zmuxh3: QUIC connection is already serving WebTransport")
	}
	s.conns[qc] = c
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, qc)
		s.mu.Unlock()
	}()
	h3, err := s.H3.NewRawServerConn(qc)
	if err != nil {
		return err
	}
	c.run(h3.HandleRequestStream, h3.HandleUnidirectionalStream)
	return nil
}

// Close stops serving and closes active QUIC connections.
func (s *Server) Close() error {
	if s == nil {
		return net.ErrClosed
	}
	s.mu.Lock()
	if s.ctx == nil {
		s.ctx, s.stop = context.WithCancel(context.Background())
	}
	s.stop()
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()
	for qc := range conns {
		_ = qc.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
	}
	return nil
}

func (s *Server) originAllowed(r *http.Request) bool {
	if s != nil && s.CheckOrigin != nil {
		return s.CheckOrigin(r)
	}
	origins := r.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	if len(origins) != 1 {
		return false
	}
	u, err := url.Parse(origins[0])
	if err != nil || u.Scheme != "https" || u.User != nil || u.Path != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return false
	}
	origin, err := httpsAuthority(u.Host)
	target, targetErr := httpsAuthority(r.Host)
	return err == nil && targetErr == nil && origin == target
}

// Upgrade accepts a WebTransport CONNECT request served by s. The caller
// owns the session and may dispatch it with Serve.
func (s *Server) Upgrade(w http.ResponseWriter, r *http.Request) (*Session, error) {
	sess, state, err := upgradeWithin(r.Context(), s, w, r)
	if err != nil && !state.started && state.taken == nil && r.Context().Err() == nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return nil, err
	}
	return sess, err
}
