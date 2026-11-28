package zmuxh3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

// Transport opens one WebTransport session per owned QUIC connection.
// Close cancels its pending dials and closes its owned connections.
type Transport struct {
	TLSClientConfig *tls.Config
	QUICConfig      *quic.Config
	DialAddr        func(context.Context, string, *tls.Config, *quic.Config) (*quic.Conn, error)

	mu    sync.Mutex
	ctx   context.Context
	stop  context.CancelFunc
	conns map[*quic.Conn]struct{}
}

func (t *Transport) init() {
	if t.ctx == nil {
		t.ctx, t.stop = context.WithCancel(context.Background())
		t.conns = make(map[*quic.Conn]struct{})
	}
}

// Close closes the transport's owned connections.
func (t *Transport) Close() error {
	if t == nil {
		return net.ErrClosed
	}
	t.mu.Lock()
	t.init()
	t.stop()
	conns := t.conns
	t.conns = nil
	t.mu.Unlock()
	for qc := range conns {
		_ = qc.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
	}
	return nil
}

// ClientConn establishes a WebTransport session on a caller-owned QUIC
// connection. The caller reserves incoming streams for this binding.
type ClientConn struct {
	conn    *wtConn
	h3      *http3.RawClientConn
	mu      sync.Mutex
	dialing bool
}

// NewClientConn starts HTTP/3 on an established connection. The connection
// must have negotiated ALPN h3 and enabled QUIC datagrams. The caller uses
// one binding for the lifetime of the connection and closes it when done.
func (t *Transport) NewClientConn(qc *quic.Conn) (*ClientConn, error) {
	if t == nil {
		return nil, net.ErrClosed
	}
	if err := wtConnError(qc); err != nil {
		return nil, err
	}
	select {
	case <-qc.HandshakeComplete():
	default:
		return nil, errors.New("zmuxh3: QUIC handshake is incomplete")
	}
	state := qc.ConnectionState()
	if state.TLS.NegotiatedProtocol != http3.NextProtoH3 || !state.SupportsDatagrams.Local {
		return nil, errors.New("zmuxh3: WebTransport needs HTTP/3 and QUIC datagrams")
	}
	tr := &http3.Transport{EnableDatagrams: true, AdditionalSettings: wtAdvertisedSettings(nil)}
	raw := tr.NewRawClientConn(qc)
	c := newWTConn(qc, false, 0, 0)
	c.runClient(raw)
	return &ClientConn{conn: c, h3: raw}, nil
}

// ConnectError reports the final HTTP status of a refused CONNECT.
type ConnectError struct {
	StatusCode int
}

func (e *ConnectError) Error() string {
	return fmt.Sprintf("zmuxh3: WebTransport CONNECT returned HTTP %d %s", e.StatusCode, http.StatusText(e.StatusCode))
}

func wtRequest(ctx context.Context, rawURL string, header http.Header) (*http.Request, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodConnect, rawURL, http.NoBody)
	if err != nil {
		return nil, "", err
	}
	authority, err := requestAuthority(req)
	if err != nil {
		return nil, "", err
	}
	req.Proto = wtProtocol
	req.Header = header.Clone()
	return req, authority, nil
}

// Dial sends Extended CONNECT. The returned response head can be inspected
// for its status and headers; the session owns the CONNECT body.
func (c *ClientConn) Dial(ctx context.Context, rawURL string, header http.Header) (*http.Response, *Session, error) {
	req, _, err := wtRequest(ctx, rawURL, header)
	if err != nil {
		return nil, nil, err
	}
	return c.dial(req)
}

func (c *ClientConn) dial(req *http.Request) (*http.Response, *Session, error) {
	if c == nil {
		return nil, nil, net.ErrClosed
	}
	c.mu.Lock()
	c.conn.mu.Lock()
	used := c.conn.session != nil
	c.conn.mu.Unlock()
	if c.dialing || used {
		c.mu.Unlock()
		return nil, nil, errWTSessionLimit
	}
	c.dialing = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.dialing = false
		c.mu.Unlock()
	}()

	ctx := req.Context()
	wait, cancel := context.WithTimeout(ctx, c.conn.timeout)
	defer cancel()
	if err := c.conn.waitSettings(wait, c.h3, false); err != nil {
		return nil, nil, err
	}
	stream, err := c.h3.OpenRequestStream(ctx)
	if err != nil {
		return nil, nil, err
	}
	stop := context.AfterFunc(ctx, func() { wtCancelControl(stream) })
	var resp *http.Response
	var sess *Session
	defer func() {
		stop()
		if sess == nil {
			wtCancelControl(stream)
		}
	}()
	if err := stream.SendRequestHeader(req); err != nil {
		return nil, nil, wtSetupError(ctx, err)
	}
	for interim := 0; ; interim++ {
		resp, err = stream.ReadResponse()
		if err != nil {
			return nil, nil, wtSetupError(ctx, err)
		}
		resp.Body = http.NoBody
		if resp.StatusCode < 100 || resp.StatusCode >= 200 {
			break
		}
		if interim == 4 {
			return nil, nil, errors.New("zmuxh3: too many informational CONNECT responses")
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, nil, &ConnectError{StatusCode: resp.StatusCode}
	}
	if !stop() || ctx.Err() != nil {
		return resp, nil, wtSetupError(ctx, nil)
	}
	sess, err = c.conn.newSession(stream)
	if err != nil {
		return resp, nil, err
	}
	sess.start()
	return resp, sess, nil
}

// DialSession opens a WebTransport session on a new QUIC connection. The
// session owns the connection, which closes after CONNECT finishes. A
// credential is bound to the completed TLS handshake and carried as a header.
func DialSession(ctx context.Context, t *Transport, rawURL string, h http.Header, cred zmux.CarrierCredential) (*Session, error) {
	if t == nil {
		return nil, net.ErrClosed
	}
	req, authority, err := wtRequest(ctx, rawURL, h)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.init()
	owner := t.ctx
	t.mu.Unlock()
	if owner.Err() != nil {
		return nil, net.ErrClosed
	}
	setup, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := context.AfterFunc(owner, func() { cancel(net.ErrClosed) })
	defer stop()

	tlsConf := &tls.Config{}
	if t.TLSClientConfig != nil {
		tlsConf = t.TLSClientConfig.Clone()
	}
	tlsConf.NextProtos = []string{http3.NextProtoH3}
	dial := t.DialAddr
	if dial == nil {
		dial = quic.DialAddr
	}
	qc, err := dial(setup, authority, tlsConf, wtQUICConfig(t.QUICConfig))
	if err != nil {
		return nil, wtSetupError(setup, err)
	}
	if qc == nil || qc.Context() == nil {
		return nil, errors.New("zmuxh3: WebTransport dial did not return a QUIC connection")
	}
	closeConn := func(code quic.ApplicationErrorCode) { _ = qc.CloseWithError(code, "") }
	t.mu.Lock()
	if owner.Err() != nil {
		t.mu.Unlock()
		closeConn(quic.ApplicationErrorCode(http3.ErrCodeNoError))
		return nil, net.ErrClosed
	}
	t.conns[qc] = struct{}{}
	t.mu.Unlock()
	context.AfterFunc(qc.Context(), func() {
		t.mu.Lock()
		delete(t.conns, qc)
		t.mu.Unlock()
	})
	select {
	case <-qc.HandshakeComplete():
	case <-qc.Context().Done():
		return nil, context.Cause(qc.Context())
	case <-setup.Done():
		closeConn(quic.ApplicationErrorCode(http3.ErrCodeNoError))
		return nil, context.Cause(setup)
	}
	if cred != nil {
		cs := qc.ConnectionState().TLS
		name, value, err := zmuxhttp.CredentialHeader(cred, &zmux.CarrierInfo{Peer: addrPort(qc.RemoteAddr()), TLS: &cs, Stage: zmux.StageHead}, req.Header)
		if err != nil {
			closeConn(quic.ApplicationErrorCode(http3.ErrCodeNoError))
			return nil, err
		}
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		req.Header.Add(name, value)
	}
	client, err := t.NewClientConn(qc)
	if err != nil {
		closeConn(quic.ApplicationErrorCode(http3.ErrCodeNoError))
		return nil, err
	}
	resp, sess, err := client.dial(req.WithContext(setup))
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		code := quic.ApplicationErrorCode(http3.ErrCodeNoError)
		if errors.Is(err, errWTSettings) {
			code = wtRequirementsNotMet
		}
		closeConn(code)
		return nil, err
	}
	context.AfterFunc(sess.Context(), func() {
		if qc.Context().Err() != nil {
			return
		}
		timer := time.NewTimer(lingerTimeout(qc.ConnectionStats().SmoothedRTT))
		defer timer.Stop()
		select {
		case <-sess.controlDone:
		case <-timer.C:
		case <-qc.Context().Done():
		}
		closeConn(quic.ApplicationErrorCode(http3.ErrCodeNoError))
	})
	return sess, nil
}
