package zmuxhttp

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/internal/httpx"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/internal/poolhost"
)

// A request body carries the up direction, a response body the down
// direction. HTTP/2 and HTTP/3 requests (and HTTP/1.1 with full duplex)
// carry both; a split bootstrap pairs an uplink POST and a downlink GET by
// a pairing header.

// HeaderPair is the default header that pairs the uplink POST and the
// downlink GET of a split bootstrap (Options.PairField and
// Split.PairField name another).
const HeaderPair = httpx.FieldPair

// Streaming is the client of the streaming carrier: a request whose body
// carries the up direction and whose response body carries the down
// direction. Its methods are the operations on it.
type Streaming struct {
	// CarrierCredential is presented for the server's admission (nil:
	// none), as the documentation of Pipe says.
	CarrierCredential zmux.CarrierCredential
	// Client sends the request (nil: http.DefaultClient).
	Client *http.Client
	// Header is added to the request.
	Header http.Header
	// URL is the request target.
	URL string
}

// Pipe opens a carrier over an HTTP request: dir zmux.DirectionBidirectional sends
// the request body up and reads the response body (HTTP/2, HTTP/3, or an
// HTTP/1.1 server with full duplex); zmux.DirectionSendOnly is a POST whose body is the
// uplink; zmux.DirectionReceiveOnly is a GET whose response body is the downlink. ctx bounds the setting
// up: ending it before the request is under way (its body read by the
// transport, or its response arrived) fails the carrier; after that the
// request lives until the pipe ends, as for any dialled connection.
//
// The zmux.PipeIO has the halves of dir only: W alone for zmux.DirectionSendOnly, R alone
// for zmux.DirectionReceiveOnly. Its Carrier is zmux.CarrierStream for zmux.DirectionBidirectional
// and zmux.CarrierSplit for a one-way half; the request's protocol is not known
// yet, so a caller dialing over HTTP/3 sets zmux.CarrierHTTP3 itself
// (zmuxh3.HTTP3Candidate does). A one-way pipe is not given to zmux.Conn.AddPipe:
// pair an uplink and a downlink with zmux.PairedPipe(up, down), or use Split.
//
// A response status other than 2xx fails the pipe with a *StatusError:
// the read half returns it, and for zmux.DirectionSendOnly the write half does once
// the response arrives. A transport failure is reported the same way.
//
// A redirect is followed as CheckRedirect allows, up to the bound of
// Client's CheckRedirect (DefaultRedirects hops when it is nil, which
// is consulted for every hop); a redirect it refuses fails the pipe with
// a *RedirectError. The request is repeated whole at the new URL,
// with a fresh credential and every uplink byte written so far: until the
// first response that is not a redirect, the uplink is kept (and, past
// 64 KiB, held back), so that no byte is lost or sent twice.
//
// CarrierCredential, when set, is presented for the server's admission: its Prefix
// starts the request body, and a GET, which has none, carries its Header
// form. The connection under Client is not known when the request is
// built, so a credential bound to TLS (admit.PrefixAuth.BindTLS) is an error.
func (s Streaming) Pipe(ctx context.Context, dir zmux.Direction) (zmux.PipeIO, error) {
	return httpPipe(ctx, s.Client, s.URL, nil, s.Header, dir, s.CarrierCredential)
}

// httpPipe is Streaming.Pipe; with t, the carrier starts at t's URL and records
// in t the URL redirects led it to.
func httpPipe(ctx context.Context, client *http.Client, rawURL string, t *Target, h http.Header, dir zmux.Direction, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
	if t != nil {
		rawURL = t.URL()
	}
	nc := noRedirectClient(client)
	// ctx bounds the setting up of the carrier only: once the request is
	// under way (the transport reads its body, or its response arrived)
	// the carrier lives until it is closed.
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, cancel)
	call := &httpCall{done: make(chan struct{}), up: func() { stop() }}
	if client != nil {
		if fixed, ok := client.Transport.(zmux.PhysicalCarrier); ok {
			call.physical = fixed.PhysicalCarrier()
		}
	}
	dial := ctx // still bounds the redirects the request follows
	ctx = life
	method := http.MethodPost
	var s *upStream
	if dir == zmux.DirectionReceiveOnly {
		method = http.MethodGet
	} else {
		s = newUpStream()
	}
	var prev context.CancelFunc
	newReq := func(u *url.URL) (*http.Request, error) {
		if prev != nil && s != nil && !s.replayable() {
			return nil, errUpReplay
		}
		if prev != nil && dial.Err() != nil {
			// A body already read lets the carrier outlive ctx, but not a
			// redirect chain the server drives: each hop is a new setup.
			return nil, context.Cause(dial)
		}
		var body io.ReadCloser = http.NoBody
		var up *httpUpBody
		if s != nil {
			up = &httpUpBody{s: s, call: call, closed: make(chan struct{}), wrote: make(chan struct{}), gone: make(chan struct{})}
			body = up
		}
		actx, acancel := context.WithCancel(ctx)
		hop := new(httpHop)
		actx = context.WithValue(actx, httpHopKey{}, hop)
		trace := &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) {
			if i.Conn != nil {
				conn := i.Conn
				hop.conn.Store(&conn)
			}
			if dir == zmux.DirectionBidirectional && u.Scheme == "https" {
				if tc, ok := i.Conn.(*tls.Conn); ok && zmux.BoundTLS(tc.ConnectionState()) {
					up.SetSecureConnection(tc)
				}
			}
		}}
		if up != nil {
			up.hop = hop
			trace.WroteHeaders = up.headersWritten
		}
		actx = httptrace.WithClientTrace(actx, trace)
		req, err := http.NewRequestWithContext(actx, method, u.String(), body)
		if err != nil {
			acancel()
			return nil, err
		}
		addHeader(req.Header, h)
		if cred != nil {
			peer, _ := netip.ParseAddrPort(req.URL.Host)
			info := &zmux.CarrierInfo{Peer: peer, Encrypted: u.Scheme == "https"}
			if up == nil {
				info.Stage = zmux.StageHead
			}
			if up == nil {
				var name, value string
				if name, value, err = CredentialHeader(cred, info, req.Header); err == nil {
					req.Header.Add(name, value)
				}
			} else {
				up.pre, err = cred.Prefix(info)
			}
			if err != nil {
				acancel()
				return nil, err
			}
		}
		if prev != nil {
			prev() // the redirected request: its body is given up below
		}
		if s != nil {
			call.cur.Store(up) // before setLive ends the body it replaces
			s.setLive(up)
		}
		prev = acancel
		return req, nil
	}
	u0, err := url.Parse(rawURL)
	if err != nil {
		stop()
		cancel()
		return zmux.PipeIO{}, err
	}
	first, err := newReq(u0)
	if err != nil {
		stop()
		cancel()
		return zmux.PipeIO{}, err
	}
	labels.Go(ctx, "http-pipe", func() {
		resp, final, err := doHTTP(ctx, client, nc, rawURL, func(u *url.URL) (*http.Request, error) {
			if first != nil {
				r := first
				first = nil
				return r, nil
			}
			return newReq(u)
		})
		if call.err = httpCarrierErr(resp, err); call.err == nil {
			call.up()
			call.resp = resp
			if t != nil {
				t.Redirected(final.String())
			}
		}
		stop() // settled either way: ctx holds nothing of the carrier now
		if s != nil {
			s.settle()
		}
		close(call.done)
		if dir != zmux.DirectionSendOnly {
			return // the response is the downlink
		}
		// An uplink's response carries no data: its end is the end of
		// the carrier.
		if call.err == nil {
			// Drain so the connection can be reused.
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		s.p.closeRead(call.end())
		cancel()
	})
	p := zmux.PipeIO{Carrier: zmux.CarrierSplit}
	if dir == zmux.DirectionBidirectional {
		p.Carrier = zmux.CarrierStream
	}
	if s != nil {
		w := &httpReqWriter{p: s.p, call: call, cancel: cancel, linger: dir == zmux.DirectionSendOnly}
		p.W = w
		if dir == zmux.DirectionBidirectional && client != nil {
			if tr, ok := client.Transport.(interface{ HTTPDatagrams() bool }); ok && tr.HTTPDatagrams() {
				p.W = &httpClientChanWriter{httpReqWriter: w}
			}
		}
	}
	if dir != zmux.DirectionSendOnly {
		p.R = &httpRespReader{call: call, cancel: cancel}
	}
	return p, nil
}

// httpCall is the outcome of a carrier's request, set before done closes.
type httpCall struct {
	up   func() // the request is under way: ctx no longer ends it
	done chan struct{}
	resp *http.Response // a 2xx response
	err  error

	// The request's two halves share an identity. A transport that is
	// tied to an established connection can also identify that connection
	// across requests. A redirectable request has no fixed connection.
	physical any

	// The request body of the current hop, which holds the connection a
	// transport declared secure for that hop (CarrierBody) and the end
	// of the wait for it, the hop's headers going out. A redirect
	// replaces it, so a hop without a connection is not secure.
	cur atomic.Pointer[httpUpBody]
}

func (c *httpCall) physicalCarrier() any {
	if c.physical != nil {
		return c.physical
	}
	return c
}

// CarrierBody is implemented by the request body of an HTTP carrier. A
// bidirectional carrier on an https TLS connection of net/http's transport
// names that connection itself. Another client transport whose connection
// protects the request end to end, such as HTTP/3 over QUIC
// (zmuxh3.HTTPClient), calls SetSecureConnection with
// that connection before it writes the request's headers: the carrier then
// declares the Bound contract (zmux.CarrierSecure) with the connection's
// exporter, which identifies the connection, not the request stream.
type CarrierBody interface {
	SetSecureConnection(zmux.ConnectionStater)
}

// end is the error the carrier's halves report once the request ended.
func (c *httpCall) end() error {
	if c.err != nil {
		return c.err
	}
	return net.ErrClosed
}

// upReplayMax bounds the uplink bytes sent before the first response that
// is not a redirect that are kept for a repeat at a redirect's URL. Past
// it the uplink goes on unkept (a transport may finish writing the body
// before it returns the response), and a later redirect is refused. A
// redirecting front answers before reading the body, and a server once it
// has read the preface, well within it.
const upReplayMax = 64 << 10

// errUpAbandoned ends the body of a request that a redirect replaced.
var errUpAbandoned = errors.New("zmux: HTTP carrier request redirected")

// errUpReplay refuses a redirect after more uplink than upReplayMax.
var errUpReplay = errors.New("uplink past the replay bound")

// upStream is an uplink across the requests of a carrier's redirects: the
// bodyPipe, read by the live request's body, and while the response is
// pending the bytes read so far (carry), which a repeated request sends
// again first. A replaced request's body that was waiting in the pipe when
// it was replaced adds what it got to carry. rdMu orders reads of p.
type upStream struct {
	p       *bodyPipe
	live    *httpUpBody // under mu
	carry   []byte      // under mu
	rdMu    sync.Mutex
	mu      sync.Mutex
	direct  atomic.Bool // settled and carry drained: the live body reads p only
	pending bool        // no response other than a redirect yet (under mu)
	over    bool        // more than upReplayMax sent while pending: no repeat (under mu)
}

func newUpStream() *upStream {
	return &upStream{p: newBodyPipe(), pending: true}
}

// setLive makes b the request body that carries the uplink; the one
// before reads no more.
func (s *upStream) setLive(b *httpUpBody) {
	s.mu.Lock()
	if s.live != nil {
		s.live.dead.Store(true)
		s.live.goneOnce.Do(func() { close(s.live.gone) })
	}
	s.live = b
	s.mu.Unlock()
}

// settle ends the pending state: a response other than a redirect, or a
// failure, arrived.
func (s *upStream) settle() {
	s.mu.Lock()
	s.pending = false
	s.mu.Unlock()
}

// replayable reports whether a repeated request can send the uplink again.
func (s *upStream) replayable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.over
}

func (s *upStream) isLive(b *httpUpBody) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live == b
}

// httpUpBody is the request body the transport reads the uplink from.
// The transport closes it when it stops reading, also when a refusal
// arrives: the writer then gets the request's outcome rather than
// io.ErrClosedPipe, as soon as it is known.
type httpUpBody struct {
	s    *upStream
	call *httpCall
	// closed closes when the transport closes the body: a Read waiting
	// on the pipe returns at once. An HTTP/2 transport's response body
	// Close waits for the request body's writer, so a refused or
	// redirected request's Close would otherwise wait for the carrier's
	// next write.
	closed chan struct{}
	// wrote closes when this hop's request headers went out; gone, when a
	// redirect replaced the hop.
	wrote, gone chan struct{}
	hop         *httpHop
	conn        atomic.Pointer[zmux.ConnectionStater] // this hop's secure connection
	pre         []byte                                // a credential's prefix, read first
	off         int                                   // of s.carry sent by this body (under s.mu)
	once        sync.Once
	dead        atomic.Bool
	wroteOnce   sync.Once
	goneOnce    sync.Once
}

type httpHopKey struct{}

type httpHop struct {
	conn atomic.Pointer[net.Conn]
}

func (h *httpHop) sampler() *carrierstats.Sampler {
	if h != nil {
		if c := h.conn.Load(); c != nil {
			//goland:noinspection GoResourceLeak // TCPConnOf only borrows the HTTP transport's connection.
			if tcp := carrierstats.TCPConnOf(*c); tcp != nil {
				return carrierstats.SocketSampler(tcp)
			}
		}
	}
	return nil
}

func (b *httpUpBody) headersWritten() { b.wroteOnce.Do(func() { close(b.wrote) }) }

// SetSecureConnection implements CarrierBody; a body a redirect
// replaced declares nothing.
func (b *httpUpBody) SetSecureConnection(c zmux.ConnectionStater) {
	if c == nil || b.dead.Load() {
		return
	}
	if !b.dead.Load() {
		b.conn.Store(&c)
	}
}

func (b *httpUpBody) Read(p []byte) (int, error) {
	b.call.up()
	if len(b.pre) > 0 {
		n := copy(p, b.pre)
		b.pre = b.pre[n:]
		return n, nil
	}
	s := b.s
	for {
		if !s.direct.Load() {
			if n, ok, err := b.fromCarry(p); ok {
				return n, err
			}
		}
		s.rdMu.Lock()
		if b.dead.Load() {
			s.rdMu.Unlock()
			return 0, errUpAbandoned
		}
		if !s.direct.Load() {
			// carry may have grown while this body waited for its turn.
			s.mu.Lock()
			grown := b.off < len(s.carry)
			s.mu.Unlock()
			if grown {
				s.rdMu.Unlock()
				continue
			}
		}
		n, err := s.p.read(p, b.closed)
		if !s.direct.Load() || b.dead.Load() {
			s.mu.Lock()
			if b.dead.Load() {
				// Replaced while it waited: the bytes go to the live body.
				if n > 0 {
					s.carry = append(s.carry, p[:n]...)
					s.direct.Store(false)
				}
				s.mu.Unlock()
				s.rdMu.Unlock()
				return 0, errUpAbandoned
			}
			if s.pending && n > 0 && !s.over {
				s.carry = append(s.carry, p[:n]...)
				b.off += n
				if len(s.carry) >= upReplayMax {
					s.over, s.carry, b.off = true, nil, 0
				}
			}
			s.mu.Unlock()
		}
		s.rdMu.Unlock()
		return n, err
	}
}

// fromCarry serves p from carry; ok is false when the caller reads the
// pipe.
func (b *httpUpBody) fromCarry(p []byte) (n int, ok bool, err error) {
	s := b.s
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case b.dead.Load():
		return 0, true, errUpAbandoned
	case b.off < len(s.carry):
		n = copy(p, s.carry[b.off:])
		b.off += n
		if !s.pending && b.off == len(s.carry) {
			s.carry, b.off = nil, 0
			s.direct.Store(true)
		}
		return n, true, nil
	case !s.pending:
		s.carry, b.off = nil, 0
		s.direct.Store(true)
	}
	return 0, false, nil
}

func (b *httpUpBody) Close() error {
	b.once.Do(func() {
		close(b.closed)
		end := func() {
			<-b.call.done
			if b.s.isLive(b) {
				b.s.p.closeRead(b.call.end())
			}
		}
		select {
		case <-b.call.done:
			end()
		default:
			// Do returns once the transport has given up the body.
			go end()
		}
	})
	return nil
}

// StatusError reports that an HTTP carrier's request was answered with
// a status other than 2xx.
type StatusError struct {
	Status     string // as the response gave it, e.g. "403 Forbidden"
	StatusCode int
}

func (e *StatusError) Error() string { return "zmux: HTTP carrier status " + e.Status }

// Is matches zmux.ErrCarrierRefused.
func (e *StatusError) Is(target error) bool { return target == zmux.ErrCarrierRefused }

// httpCarrierErr is the outcome of a carrier request: Do's error, an
// *StatusError (the unused body is closed) or nil for a 2xx response.
func httpCarrierErr(resp *http.Response, err error) error {
	switch {
	case err != nil:
		return err
	case resp.StatusCode/100 != 2:
		_ = resp.Body.Close() // the status is the error
		return &StatusError{StatusCode: resp.StatusCode, Status: resp.Status}
	}
	return nil
}

// addHeader adds the fields of src to dst without sharing src's value
// slices, which the caller keeps.
func addHeader(dst, src http.Header) {
	for k, v := range src {
		dst[k] = append(dst[k], v...)
	}
}

// Split is the client of the split carrier: a bootstrap over two requests,
// a POST uplink and a GET downlink, paired by a random pairing token. The
// carriers must be confidential end to end (TLS to the server). Its
// methods are the operations on it.
type Split struct {
	// Client sends the requests (nil: http.DefaultClient).
	Client *http.Client
	// URL is the request target of both requests.
	URL string
	// Header is added to both requests.
	Header http.Header
	// CarrierCredential is presented on each of the two requests, as
	// Streaming.Pipe presents it (nil: none).
	CarrierCredential zmux.CarrierCredential
	// PairField is the request field that pairs the uplink POST and the
	// downlink GET (empty: HeaderPair). The server's Options.PairField
	// must be the same.
	PairField string
}

// Validate checks the fields of s that no request is needed to check; Pipe,
// Join and the candidate return the same error.
func (s Split) Validate() error { return checkPairField(s.PairField) }

// pairField returns the canonical pairing field of s.
func (s Split) pairField() string {
	return (&Options{PairField: s.PairField}).pairField()
}

// Pipe opens the split bootstrap: a POST uplink and a GET downlink, paired
// by a random pairing token and each presenting the CarrierCredential as
// Streaming.Pipe does.
func (s Split) Pipe(ctx context.Context) (zmux.PipeIO, error) {
	if err := s.Validate(); err != nil {
		return zmux.PipeIO{}, err
	}
	return httpSplitPipe(ctx, s.Client, s.URL, nil, s.Header, s.CarrierCredential, s.pairField())
}

// Join joins a split carrier to c as one further pipe of the session: one
// Join across a paired POST and GET, a bidi attachment by default. ctx
// bounds the join; the requests then live until the pipe ends.
func (s Split) Join(ctx context.Context, c *zmux.Conn, opts zmux.PipeOptions) (*zmux.Pipe, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return httpSplitJoin(ctx, c, s.Client, s.URL, nil, s.Header, s.CarrierCredential, opts, s.pairField())
}

// httpSplitPipe is Split.Pipe; pair is the canonical pairing field.
func httpSplitPipe(ctx context.Context, client *http.Client, url string, t *Target, h http.Header, cred zmux.CarrierCredential, pair string) (zmux.PipeIO, error) {
	if t != nil {
		// Both requests start where the last carrier ended up.
		url = t.URL()
	}
	var tok [16]byte
	if _, err := rand.Read(tok[:]); err != nil {
		return zmux.PipeIO{}, err
	}
	hh := h.Clone()
	if hh == nil {
		hh = http.Header{}
	}
	hh.Set(pair, base64.RawURLEncoding.EncodeToString(tok[:]))
	up, err := httpPipe(ctx, client, url, t, hh, zmux.DirectionSendOnly, cred)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	down, err := httpPipe(ctx, client, url, t, hh, zmux.DirectionReceiveOnly, cred)
	if err != nil {
		up.W.(*httpReqWriter).Abort(err) // cancel the uplink's request: the server must not hold its half
		return zmux.PipeIO{}, err
	}
	// The pipe fails as soon as either request does: an uplink refused
	// after its first bytes were taken fails the downlink's reads.
	uw, dr := up.W.(*httpReqWriter), down.R.(*httpRespReader)
	dr.peer = uw.call
	labels.Go(ctx, "http-split", func() {
		<-uw.call.done
		if uw.call.err != nil {
			dr.cancel()
		}
	})
	return zmux.PipeIO{R: down.R, W: up.W, Carrier: zmux.CarrierSplit}, nil
}

func httpSplitJoin(ctx context.Context, c *zmux.Conn, client *http.Client, url string, t *Target, h http.Header, cred zmux.CarrierCredential, o zmux.PipeOptions, pair string) (*zmux.Pipe, error) {
	p, err := httpSplitPipe(ctx, client, url, t, h, cred, pair)
	if err != nil {
		return nil, err
	}
	return c.AddPipe(ctx, p, o)
}

// httpReqWriter is the request body: an uplink.
type httpReqWriter struct {
	p      *bodyPipe
	call   *httpCall
	cancel context.CancelFunc
	once   sync.Once
	// linger bounds the wait for an uplink's response once its body
	// ended. A bidirectional carrier's response is its downlink, which
	// its read half ends: a proxy may hold back its header until the
	// request body ended, or longer.
	linger bool
}

func (w *httpReqWriter) Write(p []byte) (int, error) {
	w.p.wrMu.Lock()
	defer w.p.wrMu.Unlock()
	w.p.one[0] = p
	n, err := w.p.write(w.p.one[:], len(p))
	w.p.one[0] = nil
	return n, err
}

// WriteBuffers hands a whole batch to the transport in one turn.
func (w *httpReqWriter) WriteBuffers(bufs [][]byte) (int, error) {
	total := 0
	for _, b := range bufs {
		total += len(b)
	}
	w.p.wrMu.Lock()
	defer w.p.wrMu.Unlock()
	return w.p.write(bufs, total)
}

// CarrierSecure declares the Bound contract once the transport named a
// secure connection (CarrierBody).
func (w *httpReqWriter) CarrierSecure() bool { return w.connection() != nil }

// connection is the current hop's secure connection, if it declared one.
func (w *httpReqWriter) connection() zmux.ConnectionStater {
	if b := w.call.cur.Load(); b != nil {
		if c := b.conn.Load(); c != nil {
			return *c
		}
	}
	return nil
}

// ConnectionState is that connection's TLS state; without one, none.
func (w *httpReqWriter) ConnectionState() tls.ConnectionState {
	if c := w.connection(); c != nil {
		return c.ConnectionState()
	}
	return tls.ConnectionState{}
}

// CarrierSampler is for internal use by zmux modules.
func (w *httpReqWriter) CarrierSampler() *carrierstats.Sampler {
	if s := carrierstats.ProviderOf(w.connection()); s != nil {
		return s
	}
	if b := w.call.cur.Load(); b != nil {
		return b.hop.sampler()
	}
	return nil
}

func (w *httpReqWriter) CarrierSampleConn() any { return w.connection() }
func (w *httpReqWriter) PhysicalCarrier() any   { return w.call.physicalCarrier() }

// HandshakeContext waits until the current hop's request headers went out
// (or the request ended): a transport names the connection before then.
func (w *httpReqWriter) HandshakeContext(ctx context.Context) error {
	for {
		b := w.call.cur.Load()
		select {
		case <-b.wrote:
			if w.call.cur.Load() == b {
				return nil
			}
		case <-b.gone: // redirected: wait for the next hop's headers
		case <-w.call.done:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func (w *httpReqWriter) Close() error {
	w.once.Do(func() {
		w.p.closeWrite(nil)
		if !w.linger {
			return
		}
		// The response ends an uplink's request; a server that never
		// sends it no longer holds the connection and the request's
		// goroutine past the linger bound.
		go func() {
			t := time.NewTimer(reqLinger)
			defer t.Stop()
			select {
			case <-w.call.done:
			case <-t.C:
				w.cancel()
			}
		}()
	})
	return nil
}

// reqLinger bounds how long an uplink whose body ended waits for its
// response (host clock: a bound on carrier IO).
const reqLinger = 30 * time.Second

func (w *httpReqWriter) Abort(err error) {
	w.once.Do(func() { w.p.closeWrite(err) })
	w.cancel()
}

// bodyPipe connects the uplink's writes to the transport reading the
// request body, as io.Pipe does, but a write offers a whole batch of
// buffers: the reader copies from them in turn, as much as each Read
// takes, and the write returns once all of it was copied, so nothing is
// retained and a batch costs one hand-over instead of one per buffer.
type bodyPipe struct {
	rerr error // under emu: the read side's close error
	werr error // under emu: the write side's close error
	wrCh chan struct{}
	rdCh chan int // bytes copied in a turn
	done chan struct{}
	one  [1][]byte // a plain Write's buffer
	bufs [][]byte  // what is left of the write; the reader's while it holds a turn
	once sync.Once
	wrMu sync.Mutex // serializes writes; guards one
	emu  sync.Mutex
}

func newBodyPipe() *bodyPipe {
	return &bodyPipe{wrCh: make(chan struct{}), rdCh: make(chan int), done: make(chan struct{})}
}

// read takes one turn of the current write, or none once closed closes
// (io.ErrClosedPipe); closed may be nil.
func (p *bodyPipe) read(b []byte, closed <-chan struct{}) (int, error) {
	select {
	case <-p.done:
		return 0, p.readCloseError()
	case <-closed:
		return 0, io.ErrClosedPipe
	default:
	}
	select {
	case <-p.wrCh:
		n := 0
		for n < len(b) && len(p.bufs) > 0 {
			c := copy(b[n:], p.bufs[0])
			n += c
			if c == len(p.bufs[0]) {
				p.bufs = p.bufs[1:]
			} else {
				p.bufs[0] = p.bufs[0][c:]
			}
		}
		p.rdCh <- n
		return n, nil
	case <-p.done:
		return 0, p.readCloseError()
	case <-closed:
		return 0, io.ErrClosedPipe
	}
}

// write offers bufs (total bytes) until the reader has copied all of it;
// under wrMu.
func (p *bodyPipe) write(bufs [][]byte, total int) (int, error) {
	select {
	case <-p.done:
		return 0, p.writeCloseError()
	default:
	}
	p.bufs = bufs
	defer func() { p.bufs = nil }()
	n := 0
	for n < total {
		select {
		case p.wrCh <- struct{}{}:
			n += <-p.rdCh
		case <-p.done:
			return n, p.writeCloseError()
		}
	}
	return n, nil
}

// closeRead ends the pipe from the reader's side: writes fail with err.
func (p *bodyPipe) closeRead(err error) {
	if err == nil {
		err = io.ErrClosedPipe
	}
	p.emu.Lock()
	if p.rerr == nil {
		p.rerr = err
	}
	p.emu.Unlock()
	p.once.Do(func() { close(p.done) })
}

// closeWrite ends the pipe from the writer's side: reads end with err,
// io.EOF for nil.
func (p *bodyPipe) closeWrite(err error) {
	if err == nil {
		err = io.EOF
	}
	p.emu.Lock()
	if p.werr == nil {
		p.werr = err
	}
	p.emu.Unlock()
	p.once.Do(func() { close(p.done) })
}

func (p *bodyPipe) readCloseError() error {
	p.emu.Lock()
	defer p.emu.Unlock()
	if p.rerr == nil && p.werr != nil {
		return p.werr
	}
	return io.ErrClosedPipe
}

func (p *bodyPipe) writeCloseError() error {
	p.emu.Lock()
	defer p.emu.Unlock()
	if p.werr == nil && p.rerr != nil {
		return p.rerr
	}
	return io.ErrClosedPipe
}

// httpRespReader is the response body: a downlink.
type httpRespReader struct {
	call   *httpCall
	peer   *httpCall // a split bootstrap's uplink, whose failure is the pipe's
	cancel context.CancelFunc
	once   sync.Once
}

func (r *httpRespReader) PhysicalCarrier() any { return r.call.physicalCarrier() }

// CarrierSampler exposes the provider of the request whose response was accepted.
func (r *httpRespReader) CarrierSampler() *carrierstats.Sampler {
	select {
	case <-r.call.done:
		if r.call.resp != nil {
			if s := carrierstats.ProviderOf(r.call.resp.Body); s != nil {
				return s
			}
			if req := r.call.resp.Request; req != nil {
				hop, _ := req.Context().Value(httpHopKey{}).(*httpHop)
				return hop.sampler()
			}
		}
	default:
	}
	return nil
}

func (r *httpRespReader) Read(p []byte) (int, error) {
	<-r.call.done
	err := r.call.err
	var n int
	if err == nil {
		n, err = r.call.resp.Body.Read(p)
	}
	if err != nil && r.peer != nil {
		select {
		case <-r.peer.done:
			if r.peer.err != nil {
				err = r.peer.err
			}
		default:
		}
	}
	return n, err
}

func (r *httpRespReader) Close() error {
	r.once.Do(func() {
		r.cancel()
		go func() {
			<-r.call.done
			if r.call.resp != nil {
				_ = r.call.resp.Body.Close() // reader closed; nothing to report to
			}
		}()
	})
	return nil
}

type carrierConnKey struct{}

// ConnContext records the accepted connection for carrier statistics. Set
// http.Server.ConnContext to ConnContext, or call it from an existing hook,
// to expose the physical connection and TCP provider for HTTP/1.1 and HTTP/2 carriers.
func ConnContext(ctx context.Context, conn net.Conn) context.Context {
	return context.WithValue(ctx, carrierConnKey{}, conn)
}

// NewHandler returns a handler that hands HTTP requests to l as
// carriers, by their class (Classify): a downlink (the response body),
// an uplink (the request body) or a bidirectional carrier (both, full
// duplex). Requests of other classes are served by o.Next. Each carrier
// first passes l's admission (zmux.ListenerConfig.Admission); a request forwarded from
// early data is answered 425. Nothing is written before a carrier is
// accepted: with an Admission, the response header goes out when it
// accepts the request. Otherwise the listener commits the response when
// classification takes the carrier or its handshake layer first writes. A response that carries a downlink is sent
// with Cache-Control: no-store and X-Accel-Buffering: no and without a
// Content-Length, so that intermediaries neither cache nor buffer it. A request that l does not take and
// to which nothing was written is served by o.Next, with every body byte
// that l read replayed in front of the rest. A carrier with a body that
// the admission decides on its start with zmux.VerdictFallback goes to
// Options.Next with the complete streaming request body, or receives 404
// when Next is nil. VerdictTakeover hands the accepted request and response
// halves to ListenerConfig.Fallback.
// The handler retains a detached carrier until its halves or request end.
// Use ConnContext with the serving http.Server to expose TCP carrier samples.
// An invalid o is a zmux.ErrInvalidConfig error.
func NewHandler(l *zmux.Listener, o *Options) (http.Handler, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	var opts Options
	if o != nil {
		opts = *o
	}
	return &httpHandler{l: l, o: opts}, nil
}

type httpHandler struct {
	l *zmux.Listener
	o Options
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	class := h.o.Classify(r)
	switch class {
	case ClassDownlink, ClassUplink, ClassBidirectional:
	default:
		h.o.Fallback(w, r)
		return
	}
	a, ok := h.l.Adapter().AdmitRequest(w, r, h.o.Trusted, h.o.Next)
	if !ok {
		return
	}
	hint := zmux.PipeHint{Pair: r.Header.Get(h.o.pairField())}
	rc := http.NewResponseController(w)
	// An HTTP/1.1 connection is not reused after a carrier: the server
	// does not read what is left of a full-duplex request body, so a
	// client could otherwise send its next request on a connection that
	// is about to close.
	life := &httpLife{}
	conn, _ := r.Context().Value(carrierConnKey{}).(net.Conn)
	hw := &httpRespWriter{w: w, rc: rc, close: r.ProtoMajor == 1, life: life, conn: conn}
	// A carrier closed after the request ended can no longer use its controller.
	life.tail = new(carrierio.RequestTail)
	if outer, ok := w.(interface{ RequestTail() *carrierio.RequestTail }); ok {
		life.tail = outer.RequestTail()
	} else {
		defer life.tail.Finish()
	}
	defer life.end()
	p := zmux.PipeIO{Carrier: zmux.CarrierSplit}
	var body *httpReqBody
	switch class {
	case ClassDownlink:
		p.W = hw
	case ClassUplink:
		hint.Uplink = true
		// An admitted uplink's response header goes out while its body
		// is read.
		_ = rc.EnableFullDuplex()
		body = &httpReqBody{b: r.Body, rc: rc, life: life, provider: hw}
		p.R = body
	default:
		p.Carrier = zmux.CarrierStream
		if r.ProtoMajor == 3 {
			p.Carrier = zmux.CarrierHTTP3
		}
		_ = rc.EnableFullDuplex()
		body = &httpReqBody{b: r.Body, rc: rc, life: life, provider: hw}
		if r.TLS != nil && zmux.BoundTLS(*r.TLS) {
			// This process terminates the request's TLS (or QUIC)
			// connection: the carrier declares the Bound contract, its
			// Binding from the connection's exporter.
			body.tls = r.TLS
		}
		p.R = body
		p.W = hw
		if dc := httpChannelOf(w); dc != nil {
			// The request stream's datagrams.
			hw.chp, hw.chReady = dc, make(chan struct{})
			p.W = &httpChanWriter{httpRespWriter: hw}
		}
	}
	acceptHead := func(ctx context.Context) error {
		hw.mu.Lock()
		defer hw.mu.Unlock()
		if hw.closed {
			return net.ErrClosed
		}
		if err := commitResponse(ctx, w, func() error { hw.start(); return nil }); err != nil {
			return err
		}
		return hw.openChannel()
	}
	if h.l.Adapter().HasAdmission() {
		// Admitted: the carrier is valid, its header goes out now.
		err := a.PrepareCarrier(r.Context(), acceptHead)
		if err != nil {
			a.Release()
			panic(http.ErrAbortHandler)
		}
	} else {
		life.tail.Accept = acceptHead
	}
	// The handler owns the response until the session or takeover closes
	// every carrier half backed by this request.
	carrierEnd := make(chan struct{})
	halves := new(atomic.Int32)
	halfEnded := func() {
		if halves.Add(-1) == 0 {
			close(carrierEnd)
		}
	}
	if p.W != nil {
		halves.Add(1)
		hw.onClose = sync.OnceFunc(halfEnded)
	}
	if body != nil {
		halves.Add(1)
		body.onClose = sync.OnceFunc(halfEnded)
	}
	setupErr := a.HandlePipe(r.Context(), p, hint)
	if a.Detached() {
		select {
		case <-carrierEnd:
		case <-r.Context().Done():
			// Exclude any host write before returning the ResponseWriter.
			hw.stopWriting()
		}
		return
	}
	// A split request backs just its own half. Its setup context may end
	// while the combined carrier still uses the request.
	<-carrierEnd
	if !hw.started() {
		if read, ok := body.replay(); setupErr != nil && ok {
			// Not taken, nothing written: the request goes to the
			// fallback, whatever ended it (refused, malformed, the
			// listener closed). The pipe's halves were closed with
			// immediate deadlines.
			_ = rc.SetReadDeadline(time.Time{})
			_ = rc.SetWriteDeadline(time.Time{})
			if len(read) > 0 {
				r.Body = httpx.ReplayBody(read, r.Body, r.Body)
			}
			h.o.Fallback(w, r)
			return
		}
		// A taken carrier whose response never committed cannot be replayed.
		panic(http.ErrAbortHandler)
	}
}

// commitResponse joins the write interrupt before handing the response
// to the carrier. A deadline belongs only to this HTTP request/stream.
func commitResponse(ctx context.Context, w http.ResponseWriter, write func() error) error {
	rc := http.NewResponseController(w)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		poolhost.Safe(nil, "HTTP response interrupt", func() { _ = rc.SetWriteDeadline(time.Now()) })
	})
	defer func() {
		if !stop() {
			<-done
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := write(); err != nil {
		return err
	}
	return rc.Flush()
}

// httpRespWriter is the response body as the sending half of a pipe; every
// write, and every batch as a whole, is flushed.
type httpRespWriter struct {
	w    http.ResponseWriter
	conn net.Conn
	// The request stream's datagram channel: its provider (under mu), and
	// the channel once the header went out. The receiver's state has a
	// lock of its own, taken after mu, so that a session registering its
	// receiver under its own lock never waits for a blocked write.
	chp     DatagramProvider
	chErr   error // the provider result, under mu
	rc      *http.ResponseController
	life    *httpLife
	onClose func() // tells the handler that the pipe's sending half was closed
	ch      atomic.Pointer[zmux.DatagramChannel]
	chReady chan struct{} // the provider answered
	deliver func([]byte)  // a receiver waiting for the channel (under chMu)
	chStop  func()        // the receiver's stop (under chMu)
	chGen   uint64        // the current registration's number (under chMu)
	mu      sync.Mutex
	chMu    sync.Mutex
	closed  bool
	written bool // the response header went out
	close   bool // Connection: close
}

func (h *httpRespWriter) CarrierSampleConn() any { return h.conn }

func (h *httpRespWriter) PhysicalCarrier() any {
	if h.conn != nil {
		return h.conn
	}
	if carrier, ok := h.w.(zmux.PhysicalCarrier); ok {
		if key := carrier.PhysicalCarrier(); key != nil {
			return key
		}
	}
	return h
}

// CarrierSampler is for internal use by zmux modules.
func (h *httpRespWriter) CarrierSampler() *carrierstats.Sampler {
	for w := h.w; w != nil; {
		if provider, ok := w.(carrierstats.Provider); ok {
			return provider.CarrierSampler()
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		w = u.Unwrap()
	}
	return nil
}

// start writes the response header, once; under mu or after the pipe ended.
func (h *httpRespWriter) start() {
	if h.written {
		return
	}
	h.written = true
	hh := h.w.Header()
	hh.Set("Content-Type", "application/octet-stream")
	hh.Set("Cache-Control", "no-store")
	// Reverse proxies that buffer responses by default forward this one as
	// it is written.
	hh.Set("X-Accel-Buffering", "no")
	if h.close {
		hh.Set("Connection", "close")
	}
	h.w.WriteHeader(http.StatusOK)
}

func (h *httpRespWriter) started() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.written
}

func (h *httpRespWriter) Write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, net.ErrClosed
	}
	h.start()
	n, err := h.write(p)
	if err == nil {
		err = h.rc.Flush()
	}
	return n, err
}

// WriteBuffers writes a batch through the response's buffer and flushes
// once.
func (h *httpRespWriter) WriteBuffers(bufs [][]byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, net.ErrClosed
	}
	h.start()
	n := 0
	for _, b := range bufs {
		m, err := h.write(b)
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, h.rc.Flush()
}

func (h *httpRespWriter) write(p []byte) (int, error) {
	n, err := h.w.Write(p)
	if len(p) != n && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

// Close ends the response (the handler returns once the pipe has ended);
// a write in progress, such as one blocked on a peer that does not read,
// is interrupted.
func (h *httpRespWriter) Close() error {
	h.stopWriting()
	if h.onClose != nil {
		h.onClose()
	}
	return nil
}

// stopWriting waits for an interrupted write before the response can end.
func (h *httpRespWriter) stopWriting() {
	if !h.mu.TryLock() {
		// A write is in progress: interrupt it.
		h.life.do(func() { _ = h.rc.SetWriteDeadline(time.Now()) })
		h.mu.Lock()
	}
	h.closed = true
	h.mu.Unlock()
}

// httpLife is the lifetime of a carrier's ServeHTTP call: the response
// controller is used only while it runs.
type httpLife struct {
	tail *carrierio.RequestTail
	mu   sync.Mutex
	done bool
}

// do runs f unless ServeHTTP has returned; end waits for a running f.
func (l *httpLife) do(f func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.done {
		f()
	}
}

func (l *httpLife) end() {
	l.mu.Lock()
	l.done = true
	l.mu.Unlock()
}

// httpReqBody is the request body as the receiving half of a pipe; Close
// interrupts a blocked read; the body itself is closed only once the
// listener took the carrier, as a request handed to the fallback still
// reads it. Until the listener takes the
// carrier, it keeps what was read (at most zmux.MaxUntakenRead bytes, what the
// listener reads before it decides) for the fallback.
type httpReqBody struct {
	b        io.ReadCloser
	rc       *http.ResponseController
	life     *httpLife
	tls      *tls.ConnectionState // the request's QUIC connection (HTTP/3)
	provider *httpRespWriter
	// onClose tells the handler that the body was closed.
	onClose func()

	read   []byte
	mu     sync.Mutex
	taken  bool // the listener took the carrier, or read past the bound
	closed bool // the carrier has released its view of the request body
	ended  bool // the body hit its end: net/http now reads the connection itself
}

func (r *httpReqBody) CarrierSampler() *carrierstats.Sampler {
	if r.provider != nil {
		return r.provider.CarrierSampler()
	}
	return nil
}

func (r *httpReqBody) PhysicalCarrier() any {
	if r.provider != nil {
		return r.provider.PhysicalCarrier()
	}
	return nil
}

func (r *httpReqBody) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, net.ErrClosed
	}
	n, err := r.b.Read(p)
	r.ended = r.ended || errors.Is(err, io.EOF)
	if n > 0 && !r.taken {
		if len(r.read)+n > zmux.MaxUntakenRead {
			r.taken, r.read = true, nil
		} else {
			r.read = append(r.read, p[:n]...)
		}
	}
	return n, err
}

// CarrierSecure declares the Bound contract on a request whose TLS this process terminates.
func (r *httpReqBody) CarrierSecure() bool { return r.tls != nil }

// ConnectionState is that connection's TLS state; otherwise none.
func (r *httpReqBody) ConnectionState() tls.ConnectionState {
	if r.tls == nil {
		return tls.ConnectionState{}
	}
	return *r.tls
}

func (r *httpReqBody) CarrierTaken() {
	r.mu.Lock()
	r.taken, r.read = true, nil
	r.mu.Unlock()
}

// replay returns the bytes read, and whether the carrier can be handed on
// with them (it was not taken); a nil r read nothing.
func (r *httpReqBody) replay() ([]byte, bool) {
	if r == nil {
		return nil, true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.read, !r.taken
}

func (r *httpReqBody) Close() error {
	if r.onClose != nil {
		defer r.onClose()
	}
	// Interrupt a carrier read in progress. An idle untaken view can be
	// released without touching the underlying HTTP request at all.
	bodyClosed := false
	if !r.mu.TryLock() {
		r.life.do(func() {
			if err := r.rc.SetReadDeadline(time.Now()); err != nil {
				// Request.Body supports concurrent Close to unblock Read.
				_ = r.b.Close()
				bodyClosed = true
			}
		})
		r.mu.Lock()
	}
	defer r.mu.Unlock()
	if bodyClosed {
		r.taken, r.closed, r.read = true, true, nil
		return nil
	}
	if r.closed {
		return nil
	}
	r.closed = true
	if !r.taken {
		return nil
	}
	if !r.ended {
		r.life.do(func() { _ = r.rc.SetReadDeadline(time.Now()) })
	}
	return r.b.Close()
}

// DatagramProvider is an optional capability of an http.ResponseWriter,
// found through Unwrap: the datagram channel of the request's stream, for
// HTTP versions that have one, such as HTTP/3 with HTTP Datagrams (RFC
// 9297; zmuxh3 provides it). The handler of NewHandler uses it for
// bidirectional carriers.
type DatagramProvider interface {
	// DatagramChannel returns the request stream's datagram channel. The
	// handler calls it once, after the response header was written, while
	// no other write to the response runs. A failure terminates the
	// request without transferring its stream to the carrier.
	DatagramChannel() (zmux.DatagramChannel, error)
}

func httpChannelOf(w http.ResponseWriter) DatagramProvider {
	for {
		if dc, ok := w.(DatagramProvider); ok {
			return dc
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return nil
		}
		w = u.Unwrap()
	}
}

// openChannel takes the request stream's datagram channel once the header
// went out, and starts a receiver that waits for it; under mu.
func (h *httpRespWriter) openChannel() error {
	if h.chp == nil {
		return nil
	}
	select {
	case <-h.chReady:
		return h.chErr
	default:
	}
	defer close(h.chReady)
	ch, err := h.chp.DatagramChannel()
	h.chErr = err
	if err != nil || ch == nil {
		return err
	}
	h.chMu.Lock()
	defer h.chMu.Unlock()
	h.ch.Store(&ch)
	if h.deliver != nil {
		h.chStop = ch.ReceiveChannelDatagrams(h.deliver)
		h.deliver = nil
	}
	return nil
}

// httpChanWriter is the sending half of an HTTP carrier whose request
// stream has a datagram channel: until the response header went out, the
// channel takes nothing.
type httpChanWriter struct{ *httpRespWriter }

func (h *httpRespWriter) RequestTail() *carrierio.RequestTail { return h.life.tail }
func (r *httpReqBody) RequestTail() *carrierio.RequestTail    { return r.life.tail }

func (h *httpChanWriter) channel() zmux.DatagramChannel {
	if p := h.ch.Load(); p != nil {
		return *p
	}
	return nil
}

func (h *httpChanWriter) ChannelStreamID() (uint64, bool) {
	if ch := h.channel(); ch != nil {
		return ch.ChannelStreamID()
	}
	return 0, false
}

func (h *httpChanWriter) ChannelDatagramCapacity() (limit, room int) {
	return channelCapacity(h.chReady, h.channel)
}

func (h *httpChanWriter) SendChannelDatagram(b []byte) error {
	return h.SendChannelDatagramUntil(b, time.Time{})
}

func (h *httpChanWriter) SendChannelDatagramUntil(b []byte, deadline time.Time) error {
	return carrierio.SendChannelDatagram(h.channel(), b, deadline)
}

func (h *httpChanWriter) ChannelCapacityChanged() <-chan struct{} {
	return channelChange(h.chReady, h.channel)
}

func (h *httpChanWriter) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	h.chMu.Lock()
	defer h.chMu.Unlock()
	// A later registration replaces the earlier receiver; the earlier
	// one's stop then does nothing.
	h.stopLocked()
	h.chGen++
	gen := h.chGen
	if ch := h.channel(); ch != nil {
		h.chStop = ch.ReceiveChannelDatagrams(deliver)
	} else {
		h.deliver = deliver
	}
	return func() {
		h.chMu.Lock()
		defer h.chMu.Unlock()
		if h.chGen == gen {
			h.stopLocked()
		}
	}
}

// stopLocked stops the current receiver, or drops the one waiting for the
// channel; under chMu.
func (h *httpChanWriter) stopLocked() {
	h.deliver = nil
	if h.chStop != nil {
		h.chStop()
		h.chStop = nil
	}
}
