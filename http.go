package zmux

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
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
// carry the Zmux-Pipe-Join value of a Reservation. The request lives until
// the pipe ends.
func HTTPPipe(ctx context.Context, client *http.Client, url string, h http.Header, dir Direction) (PipeIO, error) {
	if client == nil {
		client = http.DefaultClient
	}
	ctx, cancel := context.WithCancel(ctx)
	method := http.MethodPost
	var body io.ReadCloser = http.NoBody
	var pw *io.PipeWriter
	if dir == DirReceiveOnly {
		method = http.MethodGet
	} else {
		var pr *io.PipeReader
		pr, pw = io.Pipe()
		body = pr
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		cancel()
		return PipeIO{}, err
	}
	for k, v := range h {
		req.Header[k] = v
	}
	if dir == DirSendOnly {
		// The response is not read; the request ends with the pipe.
		w := &httpReqWriter{pw: pw, cancel: cancel}
		go func() {
			if resp, err := client.Do(req); err == nil {
				// Drain so the connection can be reused; the uplink's
				// outcome is reported through the pipe, not the response.
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			_ = pw.CloseWithError(net.ErrClosed) // never fails
		}()
		return PipeIO{W: w}, nil
	}
	type res struct {
		resp *http.Response
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		resp, err := client.Do(req)
		ch <- res{resp, err}
	}()
	var w io.WriteCloser
	if pw != nil {
		w = &httpReqWriter{pw: pw, cancel: cancel}
	}
	r := &httpRespReader{ch: make(chan struct{}), cancel: cancel}
	go func() {
		x := <-ch
		r.resp, r.err = x.resp, x.err
		if r.err == nil && x.resp.StatusCode/100 != 2 {
			_ = x.resp.Body.Close() // unused body; the status is the error
			r.err = errors.New("zmux: HTTP carrier status " + x.resp.Status)
		}
		close(r.ch)
	}()
	return PipeIO{R: r, W: w}, nil
}

// HTTPSplitPipe opens a split bootstrap over two requests, a POST uplink
// and a GET downlink, paired by a random pairing token (SPEC.md 6.1.1).
// The carriers must be confidential end to end (TLS to the server).
func HTTPSplitPipe(ctx context.Context, client *http.Client, url string, h http.Header) (PipeIO, error) {
	var tok [16]byte
	if _, err := rand.Read(tok[:]); err != nil {
		return PipeIO{}, err
	}
	hh := h.Clone()
	if hh == nil {
		hh = http.Header{}
	}
	hh.Set(HeaderPair, base64.RawURLEncoding.EncodeToString(tok[:]))
	up, err := HTTPPipe(ctx, client, url, hh, DirSendOnly)
	if err != nil {
		return PipeIO{}, err
	}
	down, err := HTTPPipe(ctx, client, url, hh, DirReceiveOnly)
	if err != nil {
		_ = up.W.Close() // abandon the uplink; err is the one to report
		return PipeIO{}, err
	}
	return PipeIO{R: down.R, W: up.W}, nil
}

// httpReqWriter is the request body: an uplink.
type httpReqWriter struct {
	pw     *io.PipeWriter
	cancel context.CancelFunc
	once   sync.Once
}

func (w *httpReqWriter) Write(p []byte) (int, error) { return w.pw.Write(p) }
func (w *httpReqWriter) Close() error {
	w.once.Do(func() { _ = w.pw.Close() }) // never fails
	return nil
}
func (w *httpReqWriter) Abort(err error) {
	w.once.Do(func() {
		_ = w.pw.CloseWithError(err) // never fails
		w.cancel()
	})
}

// httpRespReader is the response body: a downlink.
type httpRespReader struct {
	ch     chan struct{}
	resp   *http.Response
	err    error
	cancel context.CancelFunc
	once   sync.Once
}

func (r *httpRespReader) Read(p []byte) (int, error) {
	<-r.ch
	if r.err != nil {
		return 0, r.err
	}
	return r.resp.Body.Read(p)
}

func (r *httpRespReader) Close() error {
	r.once.Do(func() {
		r.cancel()
		go func() {
			<-r.ch
			if r.resp != nil {
				_ = r.resp.Body.Close() // reader closed; nothing to report to
			}
		}()
	})
	return nil
}

// ServeHTTP hands an HTTP request to the listener as a carrier: a GET is
// a downlink (the response body), a request with a pairing token and a
// body is an uplink, any other request with a body carries both
// directions (full duplex). It returns when the pipe ends.
func (l *Listener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hint := JoinHint{Join: r.Header.Get(HeaderJoin), Pair: r.Header.Get(HeaderPair)}
	if a, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		hint.Source = a
	}
	rc := http.NewResponseController(w)
	var p PipeIO
	switch {
	case r.Method == http.MethodGet:
		p.W = newHTTPRespWriter(w, rc)
	case hint.Pair != "":
		hint.Uplink = true
		p.R = &httpReqBody{b: r.Body, rc: rc}
		w.WriteHeader(http.StatusOK)
	default:
		_ = rc.EnableFullDuplex()
		p.R = &httpReqBody{b: r.Body, rc: rc}
		p.W = newHTTPRespWriter(w, rc)
	}
	if p.W != nil {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_ = rc.Flush()
	}
	_ = l.HandlePipe(r.Context(), p, hint)
}

// httpRespWriter is the response body as the sending half of a pipe; every
// write is flushed.
type httpRespWriter struct {
	mu     sync.Mutex
	w      http.ResponseWriter
	rc     *http.ResponseController
	closed bool
}

func newHTTPRespWriter(w http.ResponseWriter, rc *http.ResponseController) *httpRespWriter {
	return &httpRespWriter{w: w, rc: rc}
}

func (h *httpRespWriter) Write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, net.ErrClosed
	}
	n, err := h.w.Write(p)
	if err == nil {
		err = h.rc.Flush()
	}
	return n, err
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
// interrupts a blocked read.
type httpReqBody struct {
	b  io.ReadCloser
	rc *http.ResponseController
}

func (r *httpReqBody) Read(p []byte) (int, error) { return r.b.Read(p) }
func (r *httpReqBody) Close() error {
	_ = r.rc.SetReadDeadline(time.Now())
	return r.b.Close()
}
