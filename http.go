package zmux

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// HTTP carriers: a request body carries the up direction, a
// response body the down direction. HTTP/2 and HTTP/3 requests (and
// HTTP/1.1 with full duplex) carry both; a split bootstrap pairs an uplink
// POST and a downlink GET by the Zmux-Pipe-Pair header (SPEC.md 6.1.1), and
// a pipe that cannot write first joins out of band with Zmux-Pipe-Join
// (MULTIPATH.md 5.3).
const (
	HeaderJoin = "Zmux-Pipe-Join"
	HeaderPair = "Zmux-Pipe-Pair"
)

// HTTPPipe opens a carrier over an HTTP request: dir DirBidirectional sends
// the request body up and reads the response body (HTTP/2, HTTP/3, or an
// HTTP/1.1 server with full duplex); DirSendOnly is a POST whose body is the
// uplink; DirReceiveOnly is a GET whose response body is the downlink. h may
// carry the Zmux-Pipe-Join value of a Reservation. ctx bounds the setting
// up: ending it before the request is under way (its body read by the
// transport, or its response arrived) fails the carrier; after that the
// request lives until the pipe ends, as for any dialled connection.
//
// The PipeIO has the halves of dir only: W alone for DirSendOnly, R alone
// for DirReceiveOnly. Its Carrier is CarrierStreaming for DirBidirectional
// and CarrierSplit for a one-way half; the request's protocol is not known
// yet, so a caller dialing over HTTP/3 sets CarrierHTTP3 itself
// (zmuxh3.HTTP3Candidate does). Pass a one-way pipe to Conn.AddPipe with the same
// PipeOptions.Direction, or combine two as HTTPSplitPipe does.
//
// A response status other than 2xx fails the pipe with an *HTTPStatusError:
// the read half returns it, and for DirSendOnly the write half does once
// the response arrives. A transport failure is reported the same way.
//
// A redirect is followed as CheckHTTPRedirect allows, up to the bound of
// client.CheckRedirect (DefaultHTTPRedirects hops when it is nil, which
// is consulted for every hop); a redirect it refuses fails the pipe with
// an *HTTPRedirectError. The request is repeated whole at the new URL,
// with a fresh credential and every uplink byte written so far: until the
// first response that is not a redirect, the uplink is kept (and, past
// 64 KiB, held back), so that no byte is lost or sent twice.
//
// cred, when set, is presented for the server's admission: its Prefix
// starts the request body, and a GET, which has none, carries its Header
// form. The connection under client is not known when the request is
// built, so a credential bound to TLS (PrefixAuth.BindTLS) is an error.
func HTTPPipe(ctx context.Context, client *http.Client, url string, h http.Header, dir Direction, cred Credential) (PipeIO, error) {
	return httpPipe(ctx, client, url, nil, h, dir, cred)
}

// httpPipe is HTTPPipe; with t, the carrier starts at t's URL and records
// in t the URL redirects led it to.
func httpPipe(ctx context.Context, client *http.Client, rawURL string, t *HTTPTarget, h http.Header, dir Direction, cred Credential) (PipeIO, error) {
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
	ctx = life
	method := http.MethodPost
	var s *upStream
	if dir == DirReceiveOnly {
		method = http.MethodGet
	} else {
		s = newUpStream()
	}
	var prev context.CancelFunc
	newReq := func(u *url.URL) (*http.Request, error) {
		if prev != nil && s != nil && !s.replayable() {
			return nil, errUpReplay
		}
		var body io.ReadCloser = http.NoBody
		var up *httpUpBody
		if s != nil {
			up = &httpUpBody{s: s, call: call}
			body = up
		}
		actx, acancel := context.WithCancel(ctx)
		req, err := http.NewRequestWithContext(actx, method, u.String(), body)
		if err != nil {
			acancel()
			return nil, err
		}
		addHeader(req.Header, h)
		if cred != nil {
			peer, _ := netip.ParseAddrPort(req.URL.Host)
			info := &CarrierInfo{Peer: peer, Head: up == nil}
			if up == nil {
				var name, value string
				if name, value, err = cred.Header(info); err == nil {
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
			s.setLive(up)
		}
		prev = acancel
		return req, nil
	}
	u0, err := url.Parse(rawURL)
	if err != nil {
		cancel()
		return PipeIO{}, err
	}
	first, err := newReq(u0)
	if err != nil {
		cancel()
		return PipeIO{}, err
	}
	go func() {
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
		if s != nil {
			s.settle()
		}
		close(call.done)
		if dir != DirSendOnly {
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
	}()
	p := PipeIO{Carrier: CarrierSplit}
	if dir == DirBidirectional {
		p.Carrier = CarrierStreaming
	}
	if s != nil {
		p.W = &httpReqWriter{p: s.p, call: call, cancel: cancel}
	}
	if dir != DirSendOnly {
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
	p    *bodyPipe
	rdMu sync.Mutex

	mu      sync.Mutex
	live    *httpUpBody
	carry   []byte
	pending bool        // no response other than a redirect yet
	over    bool        // more than upReplayMax sent while pending: no repeat
	direct  atomic.Bool // settled and carry drained: the live body reads p only
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
	pre  []byte // a credential's prefix, read first
	off  int    // of s.carry sent by this body (under s.mu)
	dead atomic.Bool
	once sync.Once
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
			if n, err, ok := b.fromCarry(p); ok {
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
		n, err := s.p.read(p)
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
func (b *httpUpBody) fromCarry(p []byte) (n int, err error, ok bool) {
	s := b.s
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case b.dead.Load():
		return 0, errUpAbandoned, true
	case b.off < len(s.carry):
		n = copy(p, s.carry[b.off:])
		b.off += n
		if !s.pending && b.off == len(s.carry) {
			s.carry, b.off = nil, 0
			s.direct.Store(true)
		}
		return n, nil, true
	case !s.pending:
		s.carry, b.off = nil, 0
		s.direct.Store(true)
	}
	return 0, nil, false
}

func (b *httpUpBody) Close() error {
	b.once.Do(func() {
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

// HTTPStatusError reports that an HTTP carrier's request was answered with
// a status other than 2xx.
type HTTPStatusError struct {
	StatusCode int
	Status     string // as the response gave it, e.g. "403 Forbidden"
}

func (e *HTTPStatusError) Error() string { return "zmux: HTTP carrier status " + e.Status }

// httpCarrierErr is the outcome of a carrier request: Do's error, an
// *HTTPStatusError (the unused body is closed) or nil for a 2xx response.
func httpCarrierErr(resp *http.Response, err error) error {
	switch {
	case err != nil:
		return err
	case resp.StatusCode/100 != 2:
		_ = resp.Body.Close() // the status is the error
		return &HTTPStatusError{StatusCode: resp.StatusCode, Status: resp.Status}
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

// HTTPSplitPipe opens a split bootstrap over two requests, a POST uplink
// and a GET downlink, paired by a random pairing token (SPEC.md 6.1.1),
// each presenting cred as HTTPPipe does. The carriers must be
// confidential end to end (TLS to the server).
func HTTPSplitPipe(ctx context.Context, client *http.Client, url string, h http.Header, cred Credential) (PipeIO, error) {
	return httpSplitPipe(ctx, client, url, nil, h, cred)
}

func httpSplitPipe(ctx context.Context, client *http.Client, url string, t *HTTPTarget, h http.Header, cred Credential) (PipeIO, error) {
	if t != nil {
		// Both requests start where the last carrier ended up.
		url = t.URL()
	}
	var tok [16]byte
	if _, err := rand.Read(tok[:]); err != nil {
		return PipeIO{}, err
	}
	hh := h.Clone()
	if hh == nil {
		hh = http.Header{}
	}
	hh.Set(HeaderPair, base64.RawURLEncoding.EncodeToString(tok[:]))
	up, err := httpPipe(ctx, client, url, t, hh, DirSendOnly, cred)
	if err != nil {
		return PipeIO{}, err
	}
	down, err := httpPipe(ctx, client, url, t, hh, DirReceiveOnly, cred)
	if err != nil {
		_ = up.W.Close() // abandon the uplink; err is the one to report
		return PipeIO{}, err
	}
	// The pipe fails as soon as either request does: an uplink refused
	// after its first bytes were taken fails the downlink's reads.
	uw, dr := up.W.(*httpReqWriter), down.R.(*httpRespReader)
	dr.peer = uw.call
	go func() {
		<-uw.call.done
		if uw.call.err != nil {
			dr.cancel()
		}
	}()
	return PipeIO{R: down.R, W: up.W, Carrier: CarrierSplit}, nil
}

// HTTPSplitJoin joins a split carrier to c as a further pipe of the
// session: a downlink GET joined out of band and an uplink POST joined in
// band, opened at the same time (a split bootstrap's pairing makes pipe
// pair 0 only). Both get o, with their Direction. ctx bounds the joins;
// the requests then live until their pipes end. When either join fails
// the other pipe is aborted.
func HTTPSplitJoin(ctx context.Context, c *Conn, client *http.Client, url string, h http.Header, cred Credential, o PipeOptions) ([]*Pipe, error) {
	return httpSplitJoin(ctx, c, client, url, nil, h, cred, o)
}

func httpSplitJoin(ctx context.Context, c *Conn, client *http.Client, url string, t *HTTPTarget, h http.Header, cred Credential, o PipeOptions) ([]*Pipe, error) {
	if t != nil {
		url = t.URL()
	}
	od, ou := o, o
	od.Direction, ou.Direction = DirReceiveOnly, DirSendOnly
	r, err := c.ReservePipe(DirReceiveOnly, od)
	if err != nil {
		return nil, err
	}
	hh := h.Clone()
	if hh == nil {
		hh = http.Header{}
	}
	hh.Set(HeaderJoin, r.Header())
	type res struct {
		p   *Pipe
		err error
	}
	downc := make(chan res, 1)
	go func() {
		down, err := httpPipe(ctx, client, url, t, hh, DirReceiveOnly, cred)
		if err != nil {
			r.Cancel()
			downc <- res{nil, err}
			return
		}
		p, err := r.Attach(ctx, down)
		downc <- res{p, err}
	}()
	var pu *Pipe
	up, err := httpPipe(ctx, client, url, t, h, DirSendOnly, cred)
	if err == nil {
		if pu, err = c.AddPipe(ctx, up, ou); err != nil {
			closePipeIO(up)
		}
	}
	d := <-downc
	if err == nil {
		err = d.err
	}
	if err != nil {
		if d.p != nil {
			_ = d.p.Abort(err)
		}
		if pu != nil {
			_ = pu.Abort(err)
		}
		return nil, err
	}
	return []*Pipe{d.p, pu}, nil
}

// httpReqWriter is the request body: an uplink.
type httpReqWriter struct {
	p      *bodyPipe
	call   *httpCall
	cancel context.CancelFunc
	once   sync.Once
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

func (w *httpReqWriter) Close() error {
	w.once.Do(func() { w.p.closeWrite(nil) })
	return nil
}
func (w *httpReqWriter) Abort(err error) {
	w.once.Do(func() {
		w.p.closeWrite(err)
		w.cancel()
	})
}

// bodyPipe connects the uplink's writes to the transport reading the
// request body, as io.Pipe does, but a write offers a whole batch of
// buffers: the reader copies from them in turn, as much as each Read
// takes, and the write returns once all of it was copied, so nothing is
// retained and a batch costs one hand-over instead of one per buffer.
type bodyPipe struct {
	wrMu sync.Mutex // serializes writes; guards one
	one  [1][]byte  // a plain Write's buffer
	bufs [][]byte   // what is left of the write; the reader's while it holds a turn
	wrCh chan struct{}
	rdCh chan int // bytes copied in a turn
	once sync.Once
	done chan struct{}

	emu  sync.Mutex
	rerr error // the read side's close error
	werr error // the write side's close error
}

func newBodyPipe() *bodyPipe {
	return &bodyPipe{wrCh: make(chan struct{}), rdCh: make(chan int), done: make(chan struct{})}
}

// read takes one turn of the current write.
func (p *bodyPipe) read(b []byte) (int, error) {
	select {
	case <-p.done:
		return 0, p.readCloseError()
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

// NewHTTPHandler returns a handler that hands HTTP requests to l as
// carriers, by their class (ClassifyHTTP): a downlink (the response body),
// an uplink (the request body) or a bidirectional carrier (both, full
// duplex). Requests of other classes are served by o.Next. Each carrier
// first passes l's admission (ListenerConfig.Admission); a request forwarded from
// early data is answered 425. Nothing is written before a carrier is
// accepted: with an Admission, the response header goes out when it
// accepts the request; without one, with the first downlink bytes, and an
// uplink's once l has taken it. A response that carries a downlink is sent
// with Cache-Control: no-store and X-Accel-Buffering: no and without a
// Content-Length, so that intermediaries neither cache nor buffer it. A request that l does not take and
// to which nothing was written is served by o.Next, with every body byte
// that l read replayed in front of the rest. The
// handler returns when the pipe ends. o may be nil; an invalid o is an
// ErrInvalidConfig error.
func NewHTTPHandler(l *Listener, o *HTTPOptions) (http.Handler, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	var opts HTTPOptions
	if o != nil {
		opts = HTTPOptions{Next: o.Next, Trusted: o.Trusted}
	}
	return &httpHandler{l: l, o: opts}, nil
}

type httpHandler struct {
	l *Listener
	o HTTPOptions
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	class := ClassifyHTTP(r)
	switch class {
	case HTTPClassDownlink, HTTPClassUplink, HTTPClassBidirectional:
	default:
		h.o.Fallback(w, r)
		return
	}
	a, ok := h.l.admitRequest(w, r, &h.o, true)
	if !ok {
		return
	}
	hint := JoinHint{Join: r.Header.Get(HeaderJoin), Pair: r.Header.Get(HeaderPair)}
	rc := http.NewResponseController(w)
	// An HTTP/1.1 connection is not reused after a carrier: the server
	// does not read what is left of a full-duplex request body, so a
	// client could otherwise send its next request on a connection that
	// is about to close.
	hw := &httpRespWriter{w: w, rc: rc, close: r.ProtoMajor == 1}
	p := PipeIO{Carrier: CarrierSplit}
	var body *httpReqBody
	switch class {
	case HTTPClassDownlink:
		p.W = hw
	case HTTPClassUplink:
		hint.Uplink = true
		// An admitted uplink's response header goes out while its body
		// is read.
		_ = rc.EnableFullDuplex()
		body = &httpReqBody{b: r.Body, rc: rc}
		p.R = body
	default:
		p.Carrier = CarrierStreaming
		if r.ProtoMajor == 3 {
			p.Carrier = CarrierHTTP3
		}
		_ = rc.EnableFullDuplex()
		body = &httpReqBody{b: r.Body, rc: rc}
		p.R = body
		p.W = hw
		if dc := httpChannelOf(w); dc != nil {
			// The request stream's datagrams (EXTENSIONS.md 6.8).
			hw.chp = dc
			p.W = &httpChanWriter{httpRespWriter: hw}
		}
	}
	if h.l.cfg.Admission != nil {
		// Admitted: the carrier is valid, its header goes out now.
		hw.mu.Lock()
		hw.start()
		hw.mu.Unlock()
		_ = rc.Flush()
	}
	_ = a.HandlePipe(r.Context(), p, hint)
	if !hw.started() {
		if read, ok := body.replay(); ok {
			// Not taken, nothing written: the request goes to the
			// fallback, whatever ended it (refused, malformed, the
			// listener closed). The pipe's halves were closed with
			// immediate deadlines.
			_ = rc.SetReadDeadline(time.Time{})
			_ = rc.SetWriteDeadline(time.Time{})
			if len(read) > 0 {
				r.Body = &replayBody{Reader: io.MultiReader(bytesReader(read), r.Body), c: r.Body}
			}
			h.o.Fallback(w, r)
			return
		}
		hw.start() // an uplink's empty response
	}
}

// httpRespWriter is the response body as the sending half of a pipe; every
// write, and every batch as a whole, is flushed.
type httpRespWriter struct {
	mu      sync.Mutex
	w       http.ResponseWriter
	rc      *http.ResponseController
	closed  bool
	written bool // the response header went out
	close   bool // Connection: close
	stopped bool // the receiver was stopped (under chMu)

	// The request stream's datagram channel: its provider (under mu), and
	// the channel once the header went out. The receiver's state has a
	// lock of its own, taken after mu, so that a session registering its
	// receiver under its own lock never waits for a blocked write.
	chp     HTTPDatagramChannel
	ch      atomic.Pointer[DatagramChannel]
	chMu    sync.Mutex
	deliver func([]byte) // a receiver waiting for the channel (under chMu)
	chStop  func()       // the receiver's stop (under chMu)
}

// start writes the response header, once; under mu or after the pipe ended.
func (h *httpRespWriter) start() {
	if h.written {
		return
	}
	defer h.openChannel()
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
	n, err := h.w.Write(p)
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
		m, err := h.w.Write(b)
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, h.rc.Flush()
}

// Close ends the response (the handler returns once the pipe has ended);
// a write blocked on a peer that does not read is interrupted.
func (h *httpRespWriter) Close() error {
	_ = h.rc.SetWriteDeadline(time.Now())
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	return nil
}

// httpReqBody is the request body as the receiving half of a pipe; Close
// interrupts a blocked read; the body itself is closed only once the
// listener took the carrier, as a request handed to the fallback still
// reads it. Until the listener takes the
// carrier, it keeps what was read (at most firstReadBound bytes, what the
// listener reads before it decides) for the fallback.
type httpReqBody struct {
	b  io.ReadCloser
	rc *http.ResponseController

	mu    sync.Mutex
	read  []byte
	taken bool // the listener took the carrier, or read past the bound
}

func (r *httpReqBody) Read(p []byte) (int, error) {
	n, err := r.b.Read(p)
	if n > 0 {
		r.mu.Lock()
		if !r.taken {
			if len(r.read)+n > firstReadBound {
				r.taken, r.read = true, nil
			} else {
				r.read = append(r.read, p[:n]...)
			}
		}
		r.mu.Unlock()
	}
	return n, err
}

func (r *httpReqBody) carrierTaken() {
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
	_ = r.rc.SetReadDeadline(time.Now())
	if _, ok := r.replay(); ok {
		return nil
	}
	return r.b.Close()
}

// HTTPDatagramChannel is an optional capability of an http.ResponseWriter,
// found through Unwrap: the datagram channel of the request's stream, for
// HTTP versions that have one, such as HTTP/3 with HTTP Datagrams (RFC
// 9297; zmuxh3 provides it). The handler of NewHTTPHandler uses it for
// bidirectional carriers (EXTENSIONS.md 6.8).
type HTTPDatagramChannel interface {
	// DatagramChannel returns the request stream's datagram channel. The
	// handler calls it once, after the response header was written, while
	// no other write to the response runs.
	DatagramChannel() DatagramChannel
}

func httpChannelOf(w http.ResponseWriter) HTTPDatagramChannel {
	for {
		if dc, ok := w.(HTTPDatagramChannel); ok {
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
func (h *httpRespWriter) openChannel() {
	if h.chp == nil {
		return
	}
	ch := h.chp.DatagramChannel()
	if ch == nil {
		return
	}
	h.chMu.Lock()
	defer h.chMu.Unlock()
	h.ch.Store(&ch)
	if h.deliver != nil && !h.stopped {
		h.chStop = ch.ReceiveChannelDatagrams(h.deliver)
	}
}

// httpChanWriter is the sending half of an HTTP carrier whose request
// stream has a datagram channel: until the response header went out, the
// channel takes nothing.
type httpChanWriter struct{ *httpRespWriter }

func (h *httpChanWriter) channel() DatagramChannel {
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

func (h *httpChanWriter) MaxChannelDatagram() int {
	if ch := h.channel(); ch != nil {
		return ch.MaxChannelDatagram()
	}
	return 0
}

func (h *httpChanWriter) SendChannelDatagram(b []byte) error {
	if ch := h.channel(); ch != nil {
		return ch.SendChannelDatagram(b)
	}
	return net.ErrClosed
}

func (h *httpChanWriter) ReceiveChannelDatagrams(deliver func([]byte)) func() {
	h.chMu.Lock()
	defer h.chMu.Unlock()
	if ch := h.channel(); ch != nil {
		h.chStop = ch.ReceiveChannelDatagrams(deliver)
	} else {
		h.deliver = deliver
	}
	return func() {
		h.chMu.Lock()
		defer h.chMu.Unlock()
		h.stopped = true
		if h.chStop != nil {
			h.chStop()
			h.chStop = nil
		}
	}
}
