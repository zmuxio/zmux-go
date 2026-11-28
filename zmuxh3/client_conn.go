package zmuxh3

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http/httpguts"
)

// ClientConnTransport uses an established client-side HTTP/3 QUIC connection
// for requests to authority, an HTTPS host with an optional port (default 443).
// The caller supplies the authority it authorized when dialing qc. Both the
// URL and any Host override must match it, including on redirects.
//
// The returned transport can be shared by concurrent requests. Repeating the
// binding with the same transport and authority reuses the HTTP/3 client.
// The caller must reserve qc for this HTTP/3 client, including reception of
// unidirectional streams. t supplies the HTTP/3 settings and must not be
// modified while in use. Dial, TLSClientConfig and QUICConfig are unused.
//
// Request cancellation and http.Client.CloseIdleConnections leave qc open.
// The caller manages the lifetime of qc and its socket.
func ClientConnTransport(t *http3.Transport, qc *quic.Conn, authority string) (http.RoundTripper, error) {
	if t == nil || qc == nil || qc.Context() == nil {
		return nil, errors.New("zmuxh3: an HTTP/3 transport and QUIC connection are required")
	}
	authority, err := httpsAuthority(authority)
	if err != nil {
		return nil, err
	}
	select {
	case <-qc.HandshakeComplete():
	default:
		return nil, errors.New("zmuxh3: QUIC handshake is incomplete")
	}
	state := qc.ConnectionState()
	if state.TLS.NegotiatedProtocol != http3.NextProtoH3 {
		return nil, errors.New("zmuxh3: the QUIC connection did not negotiate HTTP/3")
	}
	return bindClient(t, qc, authority, state)
}

func bindClient(t *http3.Transport, qc *quic.Conn, authority string, state quic.ConnectionState) (*clientConnTransport, error) {
	if t.EnableDatagrams && !state.SupportsDatagrams.Local {
		return nil, errors.New("zmuxh3: HTTP/3 datagrams require local QUIC datagram support")
	}

	boundClients.Lock()
	ctx := qc.Context()
	if err := context.Cause(ctx); err != nil {
		boundClients.Unlock()
		return nil, err
	}
	if existing := boundClients.byConn[qc]; existing != nil {
		boundClients.Unlock()
		if existing.transport != t || existing.authority != authority {
			return nil, errors.New("zmuxh3: QUIC connection already bound to an HTTP/3 client")
		}
		return existing, nil
	}
	// Exporters are fixed by the completed handshake. Snapshot the TLS
	// state here since quic-go's ConnectionState also reads the active path.
	r := &clientConnTransport{transport: t, state: quicState{qc: qc, tls: &state.TLS}, authority: authority, ready: make(chan struct{})}
	boundClients.byConn[qc] = r
	boundClients.Unlock()

	context.AfterFunc(ctx, func() {
		boundClients.Lock()
		delete(boundClients.byConn, qc)
		boundClients.Unlock()
	})
	r.client = t.NewClientConn(qc)
	close(r.ready)
	return r, nil
}

var boundClients = struct {
	byConn map[*quic.Conn]*clientConnTransport
	sync.Mutex
}{byConn: make(map[*quic.Conn]*clientConnTransport)}

type clientConnTransport struct {
	state     quicState
	transport *http3.Transport
	ready     chan struct{}
	client    *http3.ClientConn // published by closing ready
	authority string
}

func (c *clientConnTransport) PhysicalCarrier() any { return c.state.qc }

func (c *clientConnTransport) roundTripBody(req *http.Request) (*http.Response, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return c.client.RoundTrip(req)
	}

	// ClientConn closes the body from its asynchronous sendec. Also close it on
	// cancellation or a failure to start the stream, when that sender may be
	// blocked in Read or may never have started.
	body := &clientRequestBody{ReadCloser: req.Body}
	abort := func() { _ = body.closeBody() }
	body.stopRequest = context.AfterFunc(req.Context(), abort)
	body.stopConn = context.AfterFunc(c.state.qc.Context(), abort)
	out := req.WithContext(req.Context())
	out.Body = body
	resp, err := c.client.RoundTrip(out)
	if err != nil {
		_ = body.Close()
	}
	return resp, err
}

func (c *clientConnTransport) waitReady(req *http.Request) error {
	ctx := req.Context()
	connCtx := c.state.qc.Context()
	select {
	case <-c.ready:
	case <-ctx.Done():
		return ctx.Err()
	case <-connCtx.Done():
		return context.Cause(connCtx)
	}
	// ClientConn waits for SETTINGS on Extended CONNECT using only the
	// connection context. Wait here as well to honor request cancellation.
	if req.Method == http.MethodConnect && req.Proto != "" && req.Proto != "HTTP/1.1" {
		select {
		case <-c.client.ReceivedSettings():
		case <-ctx.Done():
			return ctx.Err()
		case <-connCtx.Done():
			return context.Cause(connCtx)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return context.Cause(connCtx)
}

func requestAuthority(req *http.Request) (string, error) {
	if req.URL == nil || req.URL.Scheme != "https" || req.URL.User != nil || req.URL.Opaque != "" {
		return "", errors.New("zmuxh3: an absolute HTTPS URL is required")
	}
	target, err := httpsAuthority(req.URL.Host)
	if err != nil || req.Host == "" || req.Host == req.URL.Host {
		return target, err
	}
	host, err := httpsAuthority(req.Host)
	if err != nil || host != target {
		return "", errors.New("zmuxh3: Host does not match the request URL")
	}
	return target, nil
}

func failRequest(req *http.Request, err error) (*http.Response, error) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
	return nil, err
}

type clientRequestBody struct {
	io.ReadCloser
	stopRequest func() bool
	stopConn    func() bool
	closed      atomic.Bool
}

func (b *clientRequestBody) closeBody() error {
	if b.closed.CompareAndSwap(false, true) {
		return b.ReadCloser.Close()
	}
	return nil
}

func (b *clientRequestBody) Close() error {
	b.stopRequest()
	b.stopConn()
	return b.closeBody()
}

func httpsAuthority(authority string) (string, error) {
	u, err := url.Parse("https://" + authority)
	if err != nil || u.Host != authority || u.User != nil || u.Hostname() == "" ||
		u.Path != "" || u.Fragment != "" || u.ForceQuery || u.RawQuery != "" || strings.HasSuffix(authority, ":") {
		return "", errors.New("zmuxh3: invalid HTTPS authority")
	}
	host, port := u.Hostname(), u.Port()
	if strings.HasPrefix(authority, "[") {
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is6() || ip.Zone() != "" {
			return "", errors.New("zmuxh3: invalid IPv6 literal in HTTPS authority")
		}
		host = ip.String()
	} else {
		host, err = httpguts.PunycodeHostPort(host)
		if err != nil || strings.ContainsAny(host, ":[]") || !httpguts.ValidHostHeader(host) {
			return "", errors.New("zmuxh3: invalid HTTPS hostname")
		}
	}
	if port == "" {
		port = "443"
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return "", errors.New("zmuxh3: invalid HTTPS port")
	}
	return net.JoinHostPort(strings.ToLower(host), strconv.FormatUint(n, 10)), nil
}
