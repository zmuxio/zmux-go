// Package zmuxtest is experimental test support, outside the zmux API
// stability promise: it may change in any commit and is not for
// production use. It uses the standard library only.
//
// Proxy is a reverse proxy that behaves as common deployments in front of
// a server do: a reverse proxy with its default configuration, a CDN that
// buffers whole bodies, and a front that passes WebSocket only; it
// forwards to the upstream over HTTP/1.1. Relay is a TCP relay that loses
// carriers on demand: idle and age cuts, black holes, refusals and
// explicit cuts.
package zmuxtest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Profile is the behaviour of a Proxy.
type Profile struct {
	// BufferRequests reads every request body whole before forwarding it
	// with a Content-Length.
	BufferRequests bool
	// BufferResponses reads every response body whole before forwarding
	// it with a Content-Length, unless HonorAccel and the upstream sent
	// X-Accel-Buffering: no.
	BufferResponses bool
	HonorAccel      bool
	// WebSocket forwards WebSocket upgrades; otherwise the hop-by-hop
	// Upgrade and Connection fields are dropped and the request goes up as
	// a plain GET.
	WebSocket bool
	// WebSocketOnly refuses every request but a WebSocket upgrade (403).
	WebSocketOnly bool
	// ForwardedFor appends the client's address to X-Forwarded-For.
	ForwardedFor bool
	// EarlyData marks every request Early-Data: 1, as a proxy that took it
	// from TLS early data.
	EarlyData bool
	// TLS serves the proxy over TLS (its Server.Client trusts it).
	TLS bool
	// ResponseTimeout answers 504 when the upstream's response has not
	// arrived whole within it (buffered responses only; zero: none).
	ResponseTimeout time.Duration
	// MaxBody answers 413 for a request body above it (zero: no limit).
	MaxBody int64
	// RedirectTo answers every request with RedirectStatus (308 when zero)
	// to this base URL followed by the request's path and query, as a
	// front that upgrades http to https or moves a path.
	RedirectTo     string
	RedirectStatus int
}

// RedirectToHTTPS answers every request with status to the https base
// URL, as a front that upgrades http to https.
func RedirectToHTTPS(httpsURL string, status int) Profile {
	return Profile{RedirectTo: httpsURL, RedirectStatus: status}
}

// NginxDefaults behaves as a reverse proxy with its default
// configuration: request bodies buffered, responses buffered unless the
// upstream asks otherwise, no WebSocket, a 1 MiB body limit, HTTP/1.1 to
// the upstream.
func NginxDefaults() Profile {
	return Profile{BufferRequests: true, BufferResponses: true, HonorAccel: true, MaxBody: 1 << 20, ForwardedFor: true}
}

// BufferingCDN buffers whole request and response bodies and answers 504
// when a response takes longer than timeout.
func BufferingCDN(timeout time.Duration) Profile {
	return Profile{BufferRequests: true, BufferResponses: true, ResponseTimeout: timeout, ForwardedFor: true}
}

// WebSocketFront passes WebSocket upgrades and refuses everything else.
func WebSocketFront() Profile {
	return Profile{WebSocket: true, WebSocketOnly: true, ForwardedFor: true}
}

// Proxy is a running proxy; URL is its address.
type Proxy struct {
	*httptest.Server
	p   Profile
	up  *url.URL
	tr  *http.Transport
	mu  sync.Mutex
	hij map[net.Conn]struct{} // tunnels, closed with the proxy

	redirects atomic.Int64
}

// Redirects is the number of requests answered with a redirect.
func (x *Proxy) Redirects() int64 { return x.redirects.Load() }

// New starts a proxy to the upstream base URL (http://host:port).
func New(upstream string, p Profile) *Proxy {
	u, err := url.Parse(upstream)
	if err != nil {
		panic(err)
	}
	x := &Proxy{p: p, up: u, tr: &http.Transport{}, hij: map[net.Conn]struct{}{}}
	if p.TLS {
		x.Server = httptest.NewTLSServer(http.HandlerFunc(x.serve))
	} else {
		x.Server = httptest.NewServer(http.HandlerFunc(x.serve))
	}
	return x
}

// Close stops the proxy and its tunnels.
func (x *Proxy) Close() {
	x.mu.Lock()
	for c := range x.hij {
		_ = c.Close()
	}
	x.mu.Unlock()
	x.CloseClientConnections()
	x.Server.Close()
	x.tr.CloseIdleConnections()
}

var hopByHop = []string{"Connection", "Keep-Alive", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade"}

func isUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func (x *Proxy) serve(w http.ResponseWriter, r *http.Request) {
	if x.p.RedirectTo != "" {
		x.redirects.Add(1)
		st := x.p.RedirectStatus
		if st == 0 {
			st = http.StatusPermanentRedirect
		}
		w.Header().Set("Location", strings.TrimSuffix(x.p.RedirectTo, "/")+r.URL.RequestURI())
		w.Header().Set("Connection", "close")
		w.WriteHeader(st)
		return
	}
	up := isUpgrade(r)
	if x.p.WebSocketOnly && !up {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if x.p.ForwardedFor {
		if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			r.Header.Add("X-Forwarded-For", h)
		}
	}
	if x.p.EarlyData {
		r.Header.Set("Early-Data", "1")
	}
	if up && x.p.WebSocket {
		x.tunnel(w, r)
		return
	}
	out := &http.Request{Method: r.Method, URL: x.target(r), Header: r.Header.Clone(),
		Host: r.Host, ContentLength: r.ContentLength}
	for _, h := range hopByHop {
		out.Header.Del(h)
	}
	if r.ContentLength > 0 || r.ContentLength < 0 {
		out.Body = r.Body
	}
	if x.p.MaxBody > 0 && r.ContentLength > x.p.MaxBody {
		http.Error(w, "too large", http.StatusRequestEntityTooLarge)
		return
	}
	if x.p.BufferRequests && out.Body != nil {
		var rd io.Reader = r.Body
		if x.p.MaxBody > 0 {
			rd = io.LimitReader(r.Body, x.p.MaxBody+1)
		}
		b, err := io.ReadAll(rd)
		if err != nil {
			return
		}
		if x.p.MaxBody > 0 && int64(len(b)) > x.p.MaxBody {
			http.Error(w, "too large", http.StatusRequestEntityTooLarge)
			return
		}
		out.Body, out.ContentLength = io.NopCloser(bytes.NewReader(b)), int64(len(b))
	} else if out.Body != nil {
		_ = http.NewResponseController(w).EnableFullDuplex()
	}
	ctx := r.Context()
	if x.p.ResponseTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, x.p.ResponseTimeout)
		defer cancel()
	}
	resp, err := x.tr.RoundTrip(out.WithContext(ctx))
	if err != nil {
		x.gatewayError(w, ctx)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	buffer := x.p.BufferResponses && (!x.p.HonorAccel || !strings.EqualFold(resp.Header.Get("X-Accel-Buffering"), "no"))
	hh := w.Header()
	maps.Copy(hh, resp.Header)
	for _, h := range hopByHop {
		hh.Del(h)
	}
	hh.Del("X-Accel-Buffering")
	if buffer {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			x.gatewayError(w, ctx)
			return
		}
		hh.Set("Content-Length", strconv.Itoa(len(b)))
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(b)
		return
	}
	hh.Del("Content-Length")
	if resp.ContentLength >= 0 {
		hh.Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	w.WriteHeader(resp.StatusCode)
	rc := http.NewResponseController(w)
	_ = rc.Flush()
	buf := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			_ = rc.Flush()
		}
		if err != nil {
			return
		}
	}
}

// target is the upstream URL of r.
func (x *Proxy) target(r *http.Request) *url.URL {
	u := *x.up
	u.Path, u.RawPath, u.RawQuery = r.URL.Path, r.URL.RawPath, r.URL.RawQuery
	return &u
}

func (x *Proxy) gatewayError(w http.ResponseWriter, ctx context.Context) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
		return
	}
	http.Error(w, "bad gateway", http.StatusBadGateway)
}

// tunnel forwards a WebSocket upgrade and then the bytes both ways.
func (x *Proxy) tunnel(w http.ResponseWriter, r *http.Request) {
	uc, err := net.Dial("tcp", x.up.Host)
	if err != nil {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	cc, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		_ = uc.Close()
		return
	}
	x.mu.Lock()
	x.hij[cc], x.hij[uc] = struct{}{}, struct{}{}
	x.mu.Unlock()
	defer func() {
		x.mu.Lock()
		delete(x.hij, cc)
		delete(x.hij, uc)
		x.mu.Unlock()
		_ = cc.Close()
		_ = uc.Close()
	}()
	out := r.Clone(context.Background())
	out.URL = x.target(r)
	out.RequestURI = ""
	if err := out.Write(uc); err != nil {
		return
	}
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(uc, brw.Reader)
		close(done)
		_ = uc.Close()
	}()
	_, _ = io.Copy(cc, bufio.NewReader(uc))
	_ = cc.Close()
	<-done
}
