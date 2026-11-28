package zmuxh3

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

func (c *clientConnTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("zmuxh3: nil HTTP/3 request")
	}
	target, err := requestAuthority(req)
	if err != nil {
		return failRequest(req, err)
	}
	if target != c.authority {
		return failRequest(req, errors.New("zmuxh3: request authority is outside the connection binding"))
	}
	return c.roundTrip(req)
}

func (c *clientConnTransport) roundTrip(req *http.Request) (*http.Response, error) {
	if err := c.waitReady(req); err != nil {
		return failRequest(req, err)
	}
	if carrier, ok := req.Body.(zmuxhttp.CarrierBody); ok {
		carrier.SetSecureConnection(c.state)
	}
	resp, err := c.roundTripBody(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &carrierBody{ReadCloser: resp.Body, quicState: c.state}
	return resp, nil
}

// carrierBody publishes the actual connection even for requests with no
// request body, such as the downlink of a split carrier.
type carrierBody struct {
	io.ReadCloser
	quicState
	release func()
}

func (b *carrierBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.release != nil && errors.Is(err, io.EOF) {
		b.release()
	}
	return n, err
}

func (b *carrierBody) Close() error {
	if b.release != nil {
		defer b.release()
	}
	return b.ReadCloser.Close()
}

// ClientTransport owns the QUIC connections opened by HTTPClient.
type ClientTransport struct {
	ctx     context.Context
	tls     *tls.Config
	quic    *quic.Config
	h3      *http3.Transport
	stop    context.CancelFunc
	conn    map[string]*clientDial
	dialing sync.WaitGroup
	mu      sync.Mutex
}

type clientDial struct {
	done    chan struct{}
	c       *clientConnTransport
	err     error
	cancel  context.CancelFunc
	waiters int
	users   int
}

// HTTPClient returns a client and its owned HTTP/3 transport for Streaming
// and Split. A nil qconf enables QUIC and HTTP Datagram negotiation. Close the
// transport when done.
func HTTPClient(tlsConf *tls.Config, qconf *quic.Config) (*http.Client, *ClientTransport) {
	if tlsConf == nil {
		tlsConf = &tls.Config{}
	} else {
		tlsConf = tlsConf.Clone()
	}
	tlsConf.NextProtos = []string{http3.NextProtoH3}
	if qconf == nil {
		qconf = &quic.Config{EnableDatagrams: true}
	} else {
		qconf = qconf.Clone()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &ClientTransport{tls: tlsConf, quic: qconf, h3: &http3.Transport{EnableDatagrams: qconf.EnableDatagrams},
		ctx: ctx, stop: cancel, conn: make(map[string]*clientDial)}
	return &http.Client{Transport: t}, t
}

// HTTPDatagrams reports whether streaming HTTP carriers expose a native
// datagram channel. Ordinary HTTP requests carry zmux datagrams in records.
func (t *ClientTransport) HTTPDatagrams() bool { return false }

// RoundTrip selects a connection for the request authority and sends the request.
func (t *ClientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("zmuxh3: nil HTTP/3 request")
	}
	target, err := requestAuthority(req)
	if err != nil {
		return failRequest(req, err)
	}
	d, err := t.acquire(req.Context(), target)
	if err != nil {
		return failRequest(req, err)
	}
	resp, err := d.c.roundTrip(req)
	release := sync.OnceFunc(func() {
		t.mu.Lock()
		d.users--
		t.mu.Unlock()
	})
	if err != nil {
		release()
		return nil, err
	}
	resp.Body.(*carrierBody).release = release
	return resp, nil
}

func (t *ClientTransport) acquire(ctx context.Context, target string) (*clientDial, error) {
	t.mu.Lock()
	if t.ctx.Err() != nil {
		t.mu.Unlock()
		return nil, net.ErrClosed
	}
	d := t.conn[target]
	if d != nil && d.c != nil && d.c.state.qc.Context().Err() != nil {
		d = nil
	}
	if d == nil {
		dial, cancel := context.WithCancel(t.ctx)
		d = &clientDial{done: make(chan struct{}), cancel: cancel}
		t.conn[target] = d
		t.dialing.Add(1)
		labels.Go(dial, "h3-dial", func() {
			defer t.dialing.Done()
			t.dial(dial, target, d)
		})
	}
	d.waiters++
	t.mu.Unlock()
	select {
	case <-d.done:
	case <-ctx.Done():
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	d.waiters--
	if err := ctx.Err(); err != nil {
		if d.waiters == 0 && d.c == nil && t.conn[target] == d {
			delete(t.conn, target)
			d.cancel()
		}
		return nil, err
	}
	if d.err != nil {
		return nil, d.err
	}
	d.users++
	return d, nil
}

func (t *ClientTransport) dial(ctx context.Context, target string, d *clientDial) {
	defer d.cancel()
	tlsCfg := t.tls.Clone()
	if tlsCfg.ServerName == "" {
		tlsCfg.ServerName, _, _ = net.SplitHostPort(target)
	}
	qc, _, err := carrierio.DialPacket(ctx, target, func(pc net.PacketConn, addr net.Addr) (*quic.Conn, error) {
		return quic.Dial(ctx, pc, addr, tlsCfg, t.quic)
	})
	var c *clientConnTransport
	if err == nil {
		c, err = bindClient(t.h3, qc, target, qc.ConnectionState())
	}
	t.mu.Lock()
	removed := t.conn[target] != d
	if removed || err != nil {
		if !removed {
			delete(t.conn, target)
		}
		t.mu.Unlock()
		if qc != nil {
			_ = qc.CloseWithError(0, "")
		}
		if removed {
			err = net.ErrClosed
		}
		d.err = err
		close(d.done)
		return
	}
	d.c = c
	close(d.done)
	t.mu.Unlock()
	context.AfterFunc(qc.Context(), func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.conn[target] == d {
			delete(t.conn, target)
		}
	})
}

// CloseIdleConnections closes connections with no requests using them.
func (t *ClientTransport) CloseIdleConnections() { t.closeConnections(false) }

// Close cancels outstanding dials and closes all owned connections.
func (t *ClientTransport) Close() error {
	t.stop()
	t.closeConnections(true)
	t.dialing.Wait()
	return nil
}

func (t *ClientTransport) closeConnections(all bool) {
	t.mu.Lock()
	var conns []*clientConnTransport
	for key, d := range t.conn {
		if !all && (d.waiters != 0 || d.users != 0 || d.c == nil) {
			continue
		}
		delete(t.conn, key)
		if d.c != nil {
			conns = append(conns, d.c)
		} else {
			d.cancel()
		}
	}
	t.mu.Unlock()
	for _, c := range conns {
		_ = c.state.qc.CloseWithError(0, "")
	}
}
