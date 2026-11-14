// Package zmuxws carries zmux pipes over WebSocket. Every binary message
// is one record's Frames field (FramingRecords), so that
// intermediaries that re-chunk the byte stream cannot break records; a
// text message fails the pipe. The subprotocol (Sec-WebSocket-Protocol)
// is the application protocol identifier, as ALPN is over TLS: DefaultALPN
// ("qmux-02"), "qmux-02.<application>" or an application's own identifier,
// each also accepted as its draft-01 alias (zmux.ALPNAlias).
//
// Handler serves upgrade requests to a zmux.Listener; Dial and DialPipe
// open a WebSocket to a ws:// or wss:// URL; Pipe adapts a connection
// established otherwise. A client's credential travels
// in its header form, such as admit.PrefixAuth's Zmux-Prefix-Auth field.
//
// A carrier has the state of the TLS beneath it that this process
// terminates (ConnectionState): a client's wss connection, or the TLS
// connection of a server's request (r.TLS), so that zmux.PipeAuth.Exporter works over wss. A carrier on a TLS connection of its
// own declares the Bound contract (zmux.CarrierSecure); the WebSocket streams of one
// HTTP/2 connection (extended CONNECT) share its TLS and its exporters, and do not. Where TLS ends elsewhere (a ws URL, or a server behind a
// TLS-terminating proxy) the state is not HandshakeComplete, and both fail
// rather than bind to TLS that is not the peer's. Only the exporter
// binding, or a client that pins the server, proves the TLS end to end.
//
// A close frame is the graceful end of a pipe; a connection that ends
// without one is a failure. A pipe that zmux aborts closes the connection
// beneath without a close frame, so that the peer sees a failure.
package zmuxws

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/djylb/netx/httphead"
	"github.com/djylb/netx/wsconn"
	"github.com/djylb/netx/wsconn/wshttp"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

// options are the connection settings of both ends: messages up to one
// byte above the largest record plus 64 bytes for the per-message
// overhead of a carrier transform (such as an AEAD tag), so that a peer
// cannot make a carrier buffer much more than a record, and a message
// above the session's limit reaches it as the oversized record it is.
var options = wsconn.Options{ReadLimit: zmux.MaxRecordSize + 1 + 64}

// NoSubprotocol in the protocols of Handler accepts an upgrade request that
// has no Sec-WebSocket-Protocol field, and as the only entry of
// DialOptions.Protocols sends none. The pipe's Protocol is then "".
const NoSubprotocol = ""

// accepted returns the subprotocols a server accepts, by preference: each
// identifier followed by its alias, so that the first identifier offered
// in either form is chosen; none reports NoSubprotocol among protocols.
func accepted(protocols []string) (out []string, none bool) {
	if len(protocols) == 0 {
		protocols = []string{zmux.DefaultALPN}
	}
	out = make([]string, 0, 2*len(protocols))
	for _, p := range protocols {
		if p == NoSubprotocol {
			none = true
			continue
		}
		out = append(out, p)
		if a, ok := zmux.ALPNAlias(p); ok {
			out = append(out, a)
		}
	}
	return out, none
}

// offered returns the subprotocols a client offers: the identifiers, then
// their aliases; NoSubprotocol alone offers none, and mixed with others it
// is zmux.ErrInvalidConfig.
func offered(protocols []string) ([]string, error) {
	if len(protocols) == 0 {
		protocols = []string{zmux.DefaultALPN}
	}
	if slices.Contains(protocols, NoSubprotocol) {
		if len(protocols) > 1 {
			return nil, fmt.Errorf("%w: zmuxws.NoSubprotocol cannot be offered with other protocols", zmux.ErrInvalidConfig)
		}
		return nil, nil
	}
	out := append(make([]string, 0, 2*len(protocols)), protocols...)
	for _, p := range protocols {
		if a, ok := zmux.ALPNAlias(p); ok {
			out = append(out, a)
		}
	}
	return out, nil
}

// ---- server ----

// NewHandler returns an http.Handler that upgrades WebSocket requests to
// carriers of l. protocols are the subprotocols it accepts, by preference
// (none: zmux.DefaultALPN), each also as its alias. NoSubprotocol among
// them also accepts a request with no Sec-WebSocket-Protocol field; a
// request that offers subprotocols, none of them accepted, is not
// accepted.
//
// Nothing is answered before a request is accepted: a request that is not
// a valid WebSocket upgrade or offers no accepted subprotocol is served by
// o.Next, and an upgrade request then passes l's admission
// (zmux.Listener.AdmitUpgrade, decided on the request head, without a
// prefix). An accepted request is answered with 101 and its connection is
// handed to the listener for its lifetime (zmux.Arrival.HandlePipe), with
// the request's source (o.Trusted). A request forwarded from
// early data is answered 425 (Too Early). o may be nil; an invalid o is an
// error.
//
// HTTP/1.1 requests are hijacked; an HTTP/2 extended CONNECT (RFC 8441,
// see package wshttp) runs over its stream.
func NewHandler(l *zmux.Listener, o *zmuxhttp.Options, protocols ...string) (http.Handler, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	subs, none := accepted(protocols)
	h := &handler{l: l, none: none, so: wsconn.ServerOptions{
		Options:      options,
		Subprotocols: subs,
		// Origins are the admission's to decide; zmux clients are seldom
		// browsers.
		CheckOrigin: wsconn.AnyOrigin,
	}}
	if o != nil {
		h.o = *o
	}
	return h, nil
}

type handler struct {
	l  *zmux.Listener
	o  zmuxhttp.Options
	so wsconn.ServerOptions
	// none accepts a request without a Sec-WebSocket-Protocol field.
	none bool
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The 101 carries Date, as an origin server's responses do (RFC 9110
	// 6.6.1).
	so := h.so
	so.Header = append(slices.Clip(h.so.Header), httphead.Field{Name: "Date", Value: time.Now().UTC().Format(http.TimeFormat)})
	hs, err := wshttp.Check(w, r, &so)
	// Check reports no subprotocol both for a request that offers none and
	// for one whose offers are all unaccepted.
	if err != nil || hs.Subprotocol == "" && (!h.none || len(r.Header.Values("Sec-WebSocket-Protocol")) != 0) {
		h.o.Fallback(w, r)
		return
	}
	a, ok := h.l.Adapter().AdmitUpgrade(w, r, h.o.Trusted, http.HandlerFunc(h.o.Fallback))
	if !ok {
		return
	}
	c, err := wshttp.Accept(w, r, hs)
	if err != nil {
		if _, ok := errors.AsType[*wsconn.HandshakeError](err); ok {
			// Nothing was answered.
			h.o.Fallback(w, r)
		}
		a.HandshakeFailed() // the upgrade failed: no pipe input
		return
	}
	_ = a.HandlePipe(r.Context(), pipe(c, r.TLS, r.ProtoMajor == 1), zmux.PipeHint{})
	// Over an HTTP/2 stream, the handler outlives the connection.
	select {
	case <-wshttp.Done(c):
	case <-r.Context().Done():
	}
}

// ---- client ----

// DialOptions configure Dial and DialPipe. The zero value offers
// zmux.DefaultALPN and dials with a net.Dialer.
type DialOptions struct {
	// Protocols are the subprotocols offered, by preference (none:
	// zmux.DefaultALPN), followed by their aliases. []string{NoSubprotocol}
	// offers none; NoSubprotocol with other entries is
	// zmux.ErrInvalidConfig.
	Protocols []string
	// Header holds fields added to the upgrade request, such as
	// a credential's. A Host field sets the
	// request's Host; the handshake's own fields are refused.
	Header http.Header
	// CarrierCredential is presented in the upgrade request (its Header form,
	// bound to the TLS connection of a wss URL) for the server's
	// admission; nil: none. Dial takes zmux.Config.CarrierCredential instead.
	CarrierCredential zmux.CarrierCredential
	// TLSConfig configures the TLS of wss URLs; nil verifies the URL's
	// host with the system roots. Without a ServerName, the URL's host is
	// verified.
	TLSConfig *tls.Config
	// Dialer dials the TCP connection, such as through a proxy; nil is a
	// net.Dialer.
	Dialer interface {
		DialContext(ctx context.Context, network, addr string) (net.Conn, error)
	}
	// HandshakeTimeout bounds the dial and the handshakes, of every
	// redirect together, when ctx has no deadline (zero: 10 s; negative:
	// no timeout).
	HandshakeTimeout time.Duration
	// MaxRedirects bounds the redirects of the upgrade followed, each on a
	// new connection with the credential presented anew, as
	// zmuxhttp.CheckRedirect allows. Zero is zmuxhttp.DefaultRedirects
	// (5); a negative value follows none. A refused one is a
	// *zmuxhttp.RedirectError.
	MaxRedirects int
}

// DialPipe opens a WebSocket to rawURL (ws:// or wss://) and returns it as
// a pipe input: for zmux.Start, Conn.AddPipe, or a carrier pool. The
// upgrade request carries o.CarrierCredential.
func DialPipe(ctx context.Context, rawURL string, o *DialOptions) (zmux.PipeIO, error) {
	if o == nil {
		o = &DialOptions{}
	}
	p, _, err := dialPipe(ctx, rawURL, o, o.CarrierCredential)
	return p, err
}

// Dial opens a WebSocket to rawURL and starts a zmux client session on
// it, as zmux.Dial does over TCP; the upgrade request carries
// cfg.CarrierCredential. A Credential in o as well is a zmux.ErrInvalidConfig
// error.
func Dial(ctx context.Context, rawURL string, o *DialOptions, cfg *zmux.Config) (*zmux.Conn, error) {
	if o == nil {
		o = &DialOptions{}
	}
	var cred zmux.CarrierCredential
	if cfg != nil {
		cred = cfg.CarrierCredential
	}
	if o.CarrierCredential != nil {
		return nil, fmt.Errorf("%w: zmuxws.Dial takes Config.CarrierCredential, not DialOptions.CarrierCredential", zmux.ErrInvalidConfig)
	}
	p, _, err := dialPipe(ctx, rawURL, o, cred)
	if err != nil {
		return nil, err
	}
	return zmux.Start(ctx, p, zmux.RoleClient, cfg)
}

// dialPipe dials rawURL, following redirects of the upgrade as
// zmuxhttp.CheckRedirect allows, each over a new connection with the
// credential presented anew; it returns the final URL.
func dialPipe(ctx context.Context, rawURL string, o *DialOptions, cred zmux.CarrierCredential) (zmux.PipeIO, string, error) {
	timeout := o.HandshakeTimeout
	if timeout == 0 {
		timeout = wsconn.DefaultHandshakeTimeout
	}
	if _, ok := ctx.Deadline(); !ok && timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	bound := o.MaxRedirects
	switch {
	case bound < 0:
		bound = 0
	case bound == 0:
		bound = zmuxhttp.DefaultRedirects
	}
	for hops := 0; ; hops++ {
		p, err := dialOnce(ctx, rawURL, o, cred)
		var he *wsconn.HandshakeError
		if !errors.As(err, &he) || he.Response == nil || !zmuxhttp.IsRedirect(he.Status) {
			return p, rawURL, err
		}
		from, perr := url.Parse(rawURL)
		if perr != nil {
			return p, rawURL, err
		}
		loc := he.Response.Header.Get("Location")
		next, rerr := zmuxhttp.CheckRedirect(from, http.MethodGet, he.Status, loc)
		if rerr == nil && hops >= bound {
			rerr = &zmuxhttp.RedirectError{StatusCode: he.Status, Location: loc, Reason: "too many redirects"}
		}
		if rerr != nil {
			return zmux.PipeIO{}, rawURL, rerr
		}
		rawURL = next.String()
	}
}

func dialOnce(ctx context.Context, rawURL string, o *DialOptions, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	secure := u.Scheme == "wss" || u.Scheme == "https"
	if !secure && u.Scheme != "ws" && u.Scheme != "http" || u.Hostname() == "" {
		return zmux.PipeIO{}, fmt.Errorf("zmuxws: not a ws or wss URL: %q", rawURL)
	}
	subs, err := offered(o.Protocols)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	co := wsconn.ClientOptions{
		Options:          options,
		Subprotocols:     subs,
		HandshakeTimeout: o.HandshakeTimeout,
	}
	for name, vs := range o.Header {
		if http.CanonicalHeaderKey(name) == "Host" {
			if len(vs) > 0 {
				co.Host = vs[0]
			}
			continue
		}
		for _, v := range vs {
			co.Header = append(co.Header, httphead.Field{Name: name, Value: v})
		}
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if secure {
			port = "443"
		}
	}
	addr := net.JoinHostPort(u.Hostname(), port)
	var d interface {
		DialContext(ctx context.Context, network, addr string) (net.Conn, error)
	} = &net.Dialer{}
	switch {
	case secure:
		d = &tlsDialer{forward: o.Dialer, config: o.TLSConfig}
	case o.Dialer != nil:
		d = o.Dialer
	}
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	var credErr error
	if cred != nil {
		peer, _ := netip.ParseAddrPort(nc.RemoteAddr().String())
		info := &zmux.CarrierInfo{Peer: peer, Head: true}
		if tc, ok := nc.(*tls.Conn); ok {
			cs := tc.ConnectionState()
			info.TLS = &cs
		}
		name, value, err := cred.Header(info)
		switch {
		case err == nil:
			co.Header = append(co.Header, httphead.Field{Name: name, Value: value})
		case info.TLS != nil:
			_ = nc.Close()
			return zmux.PipeIO{}, err
		default:
			// A credential bound to TLS on a plain connection: the
			// request goes without it, as far as a redirect to wss, and
			// fails with credErr otherwise.
			credErr = err
		}
	}
	return handshake(ctx, nc, rawURL, &co, credErr)
}

// handshake runs the client handshake over nc and returns the carrier,
// which then owns the connection; on every error the connection is closed
// (wsconn.Client closes nc itself when the handshake fails).
func handshake(ctx context.Context, nc net.Conn, rawURL string, co *wsconn.ClientOptions, credErr error) (p zmux.PipeIO, err error) {
	c, _, err := wsconn.Client(ctx, nc, rawURL, co)
	if err != nil {
		_ = nc.Close()
		return zmux.PipeIO{}, handshakeErr(err, credErr)
	}
	defer func() {
		if err != nil {
			_ = c.Close()
		}
	}()
	if credErr != nil {
		return zmux.PipeIO{}, credErr
	}
	return pipe(c, tlsState(nc), true), nil
}

// handshakeErr is the error of a failed handshake: credErr, the credential
// left out of the request, unless the server redirected, which the caller
// follows.
func handshakeErr(err, credErr error) error {
	var he *wsconn.HandshakeError
	if credErr != nil && (!errors.As(err, &he) || !zmuxhttp.IsRedirect(he.Status)) {
		return credErr
	}
	return err
}

// tlsDialer dials TLS over forward (a net.Dialer when nil).
type tlsDialer struct {
	forward interface {
		DialContext(ctx context.Context, network, addr string) (net.Conn, error)
	}
	config *tls.Config
}

func (d *tlsDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d.forward == nil {
		td := &tls.Dialer{Config: d.config}
		return td.DialContext(ctx, network, addr)
	}
	raw, err := d.forward.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	cfg := d.config
	if cfg == nil || cfg.ServerName == "" {
		if cfg == nil {
			cfg = &tls.Config{}
		} else {
			cfg = cfg.Clone()
		}
		if host, _, err := net.SplitHostPort(addr); err == nil {
			cfg.ServerName = host
		}
	}
	tc := tls.Client(raw, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = tc.Close()
		return nil, err
	}
	return tc, nil
}

// ---- carrier ----

// ConnPipe returns a WebSocket connection, client or server, as a pipe input
// with message framing. A connection established by other means should
// have a read limit of at least zmux.MaxRecordSize+1, 64 bytes more under a
// transform (wsconn's default
// is above it). Its ConnectionState is that of c's own connection when
// that is a TLS connection, else not HandshakeComplete.
func ConnPipe(c *wsconn.Conn) zmux.PipeIO { return pipe(c, tlsState(c.NetConn()), true) }

// own says that the connection beneath c carries only c.
func pipe(c *wsconn.Conn, st *tls.ConnectionState, own bool) zmux.PipeIO {
	if st != nil && !st.HandshakeComplete {
		st = nil
	}
	p := &carrier{c: c, tls: st, own: own}
	return zmux.PipeIO{R: p, W: p, Framing: zmux.FramingRecords, Carrier: zmux.CarrierWebSocket, Protocol: c.Subprotocol()}
}

// tlsState is the state of nc's TLS when nc is a TLS connection.
func tlsState(nc net.Conn) *tls.ConnectionState {
	tc, ok := nc.(interface{ ConnectionState() tls.ConnectionState })
	if !ok {
		return nil
	}
	st := tc.ConnectionState()
	return &st
}

// carrier is both halves of a pipe over a WebSocket connection: one
// binary message per record.
type carrier struct {
	c   *wsconn.Conn
	tls *tls.ConnectionState // nil: no TLS terminated here
	own bool                 // the TLS connection carries only this WebSocket
}

// CarrierSecure reports the Bound contract: end to end on a TLS connection of its own.
func (p *carrier) CarrierSecure() bool {
	return p.tls != nil && p.own && p.tls.Version >= tls.VersionTLS12
}

// ConnectionState returns the state of the TLS beneath the connection,
// shared by the streams of one HTTP/2 connection; without TLS terminated
// in this process it is the zero state, not HandshakeComplete.
func (p *carrier) ConnectionState() tls.ConnectionState {
	if p.tls == nil {
		return tls.ConnectionState{}
	}
	return *p.tls
}

var errMessageCarrier = errors.New("zmuxws: a message carrier has no byte stream")

// ReadMessage returns the next binary message. The peer's close frame is
// the end of the pipe (io.EOF); a connection that ends without one, and a
// text message, fail it.
func (p *carrier) ReadMessage() ([]byte, error) {
	return p.message(p.c.AppendMessage(nil))
}

// ReadMessageLimit returns the next binary message as ReadMessage does,
// when it holds at most limit bytes (zmux.MessageLimitReader); a longer
// one, also after decompression, is read no further and fails the
// carrier (status 1009) with zmux.ErrMessageTooLarge.
func (p *carrier) ReadMessageLimit(limit int) ([]byte, error) {
	m, err := p.message(p.c.ReadMessageLimit(int64(max(limit, 0))))
	if errors.Is(err, wsconn.ErrMessageTooLarge) {
		return nil, zmux.ErrMessageTooLarge
	}
	return m, err
}

var _ zmux.MessageLimitReader = (*carrier)(nil)

// message maps a read of the connection to the carrier's result.
func (p *carrier) message(typ wsconn.MessageType, m []byte, err error) ([]byte, error) {
	var ce *wsconn.CloseError
	switch {
	case err == nil && typ != wsconn.Binary:
		return nil, errors.New("zmuxws: a text message on a zmux carrier")
	case err == nil:
		return m, nil
	case errors.As(err, &ce):
		return nil, io.EOF
	case errors.Is(err, io.EOF):
		return nil, io.ErrUnexpectedEOF
	}
	return nil, err
}

// WriteMessage sends m as one binary message.
func (p *carrier) WriteMessage(m []byte) error { return p.c.WriteMessage(wsconn.Binary, m) }

// WriteMessages sends msgs as binary messages in one write to the
// connection beneath (zmux.MessageBatchWriter).
func (p *carrier) WriteMessages(msgs [][]byte) error {
	return p.c.WriteMessages(wsconn.Binary, msgs)
}

var _ zmux.MessageBatchWriter = (*carrier)(nil)

func (p *carrier) Read([]byte) (int, error)  { return 0, errMessageCarrier }
func (p *carrier) Write([]byte) (int, error) { return 0, errMessageCarrier }

// CloseWrite sends a close frame: the graceful end of the pipe.
func (p *carrier) CloseWrite() error { return p.c.CloseWrite() }

// Close ends the closing handshake, within wsconn's close timeout.
func (p *carrier) Close() error { return p.c.Close() }

// Abort closes the connection beneath (the TCP connection under TLS)
// without a close frame.
func (p *carrier) Abort(error) {
	nc := p.c.NetConn()
	if tc, ok := nc.(interface{ NetConn() net.Conn }); ok {
		nc = tc.NetConn()
	}
	_ = nc.Close()
}

// SetReadDeadline bounds reads, as zmux.Admission.AdmitPipe needs.
func (p *carrier) SetReadDeadline(t time.Time) error { return p.c.SetReadDeadline(t) }

// Candidate is a WebSocket to rawURL for zmux.DialChoice, dialed as DialPipe
// does with o (nil: the zero DialOptions) and the session's credential in
// place of o.CarrierCredential. A refused upgrade is reported as a
// *zmuxhttp.StatusError, so that the mode reports the candidate refused.
func Candidate(rawURL string, o *DialOptions) zmux.Candidate {
	var opts DialOptions
	if o != nil {
		opts = *o
	}
	t := zmuxhttp.NewTarget(rawURL)
	return zmux.Candidate{Kind: zmux.CarrierWebSocket, Target: t, Dial: func(ctx context.Context, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
		p, final, err := dialPipe(ctx, t.URL(), &opts, cred)
		if err == nil {
			t.Redirected(final)
		}
		var he *wsconn.HandshakeError
		if errors.As(err, &he) && he.Status != 0 {
			return p, fmt.Errorf("%w: %w", &zmuxhttp.StatusError{StatusCode: he.Status,
				Status: strconv.Itoa(he.Status) + " " + http.StatusText(he.Status)}, err)
		}
		return p, err
	}}
}
