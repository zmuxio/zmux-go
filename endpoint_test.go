package zmux

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djylb/netx/proxyproto"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestClassifyHTTP checks the classification rules in order.
func TestClassifyHTTP(t *testing.T) {
	req := func(method string, h map[string]string, body io.Reader, cl int64) *http.Request {
		r := httptest.NewRequest(method, "http://example.test/zmux", body)
		for k, v := range h {
			r.Header.Set(k, v)
		}
		r.ContentLength = cl
		return r
	}
	h3 := req(http.MethodConnect, nil, nil, -1)
	h3.Proto, h3.ProtoMajor = "webtransport", 3
	h3draft := req(http.MethodConnect, nil, nil, -1)
	h3draft.Proto, h3draft.ProtoMajor = "webtransport-h3", 3
	h3other := req(http.MethodConnect, nil, nil, -1)
	h3other.Proto, h3other.ProtoMajor = "webtransport-other", 3
	h2ws := req(http.MethodConnect, map[string]string{":protocol": "websocket"}, nil, -1)
	h2ws.Proto, h2ws.ProtoMajor = "HTTP/2.0", 2
	body := strings.NewReader("x")
	for _, c := range []struct {
		name string
		r    *http.Request
		want HTTPClass
	}{
		{"websocket", req(http.MethodGet, map[string]string{"Upgrade": "WebSocket", "Connection": "Upgrade"}, nil, 0), HTTPClassWebSocket},
		{"websocket with a join", req(http.MethodGet, map[string]string{"Upgrade": "websocket", HeaderJoin: "x"}, nil, 0), HTTPClassWebSocket},
		{"extended connect websocket", h2ws, HTTPClassWebSocket},
		{"webtransport", h3, HTTPClassWebTransport},
		{"webtransport draft", h3draft, HTTPClassWebTransport},
		{"other webtransport-*", h3other, HTTPClassNone},
		{"downlink pair", req(http.MethodGet, map[string]string{HeaderPair: "x"}, nil, 0), HTTPClassDownlink},
		{"downlink join", req(http.MethodGet, map[string]string{HeaderJoin: "x"}, nil, 0), HTTPClassDownlink},
		{"plain get", req(http.MethodGet, nil, nil, 0), HTTPClassNone},
		{"split uplink", req(http.MethodPost, map[string]string{HeaderPair: "x"}, body, -1), HTTPClassUplink},
		{"split uplink with a length", req(http.MethodPost, map[string]string{HeaderPair: "x"}, body, 1), HTTPClassUplink},
		{"exchange", req(http.MethodPost, nil, body, 1), HTTPClassExchange},
		{"exchange with a join", req(http.MethodPost, map[string]string{HeaderJoin: "x"}, body, 1), HTTPClassExchange},
		{"bidirectional", req(http.MethodPost, nil, body, -1), HTTPClassBidirectional},
		{"join uplink", req(http.MethodPost, map[string]string{HeaderJoin: "x"}, body, -1), HTTPClassUplink},
		{"put", req(http.MethodPut, nil, body, -1), HTTPClassNone},
		{"plain connect", req(http.MethodConnect, nil, nil, -1), HTTPClassNone},
	} {
		if got := ClassifyHTTP(c.r); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// unifiedEnv is a listener behind an HTTPEndpoint that serves stream and
// exchange carriers on one target, with a host handler that records what
// it was given.
type unifiedEnv struct {
	lst  *Listener
	ex   *HTTPExchangeHandler
	srv  *httptest.Server
	mu   sync.Mutex
	host []string // "METHOD body" of the complete requests the host served
}

func newUnifiedEnv(t *testing.T, adm *Admission, trusted *TrustedProxies) *unifiedEnv {
	t.Helper()
	e := &unifiedEnv{lst: NewListener(&ListenerConfig{Config: mpConfig(), Admission: adm})}
	host := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			// A request its client aborted before its body was complete,
			// such as a carrier request a closing session cancels: it
			// cannot be authenticated, so it reaches the host like any
			// broken request, but nobody waits for an answer.
			return
		}
		e.mu.Lock()
		e.host = append(e.host, r.Method+" "+string(b))
		e.mu.Unlock()
		http.Error(w, "host", http.StatusTeapot)
	})
	stream, err := NewHTTPHandler(e.lst, &HTTPOptions{Next: host, Trusted: trusted})
	if err != nil {
		t.Fatal(err)
	}
	e.ex, err = NewHTTPExchangeHandler(e.lst, &HTTPExchangeConfig{PSK: bytes.Repeat([]byte{5}, 32),
		HoldTime: time.Second, Next: host, Trusted: trusted})
	if err != nil {
		t.Fatal(err)
	}
	e.srv = httptest.NewServer(&HTTPEndpoint{Stream: stream, Exchange: e.ex, Next: host})
	return e
}

func (e *unifiedEnv) close() {
	e.srv.Close()
	_ = e.ex.Close()
	_ = e.lst.Close()
}

func (e *unifiedEnv) hostRequests() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.host...)
}

// TestHTTPEndpoint serves a bidirectional carrier, a split bootstrap and
// an exchange carrier on one target, and gives the host the requests that
// are not carriers, an exchange request that fails authentication with its
// body.
func TestHTTPEndpoint(t *testing.T) {
	leakcheck.Check(t)
	e := newUnifiedEnv(t, nil, nil)
	defer e.close()
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	hc := &http.Client{Transport: tr}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dials := map[string]func() (PipeIO, error){
		"streaming": func() (PipeIO, error) { return HTTPPipe(ctx, hc, e.srv.URL, nil, DirBidirectional, nil) },
		"split":     func() (PipeIO, error) { return HTTPSplitPipe(ctx, hc, e.srv.URL, nil, nil) },
		"exchange": func() (PipeIO, error) {
			return HTTPExchangePipe(ctx, hc, e.srv.URL, &HTTPExchangeConfig{PSK: bytes.Repeat([]byte{5}, 32)})
		},
	}
	for name, dial := range dials {
		t.Run(name, func(t *testing.T) {
			p, err := dial()
			if err != nil {
				t.Fatal(err)
			}
			c, err := Start(ctx, p, RoleClient, mpConfig())
			if err != nil {
				t.Fatal(err)
			}
			s, err := e.lst.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			echoOnce(t, ctx, c, s)
			want := map[string]CarrierKind{"streaming": CarrierStreaming, "split": CarrierSplit,
				"exchange": CarrierExchange}[name]
			for _, x := range []*Conn{c, s} {
				if st := x.PipeStats(); len(st) == 0 || st[0].Carrier != want {
					t.Fatalf("carrier kind %+v, want %v", st, want)
				}
			}
			_ = c.Close()
			_ = s.Close()
			waitDone(t, c)
			waitDone(t, s)
		})
	}
	resp, err := hc.Get(e.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	resp, err = hc.Post(e.srv.URL, "text/plain", strings.NewReader("form data"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	got := e.hostRequests()
	if len(got) != 2 || got[0] != "GET " || got[1] != "POST form data" {
		t.Fatalf("host served %q", got)
	}
}

// echoOnce echoes one message over a new stream of c served by s.
func echoOnce(t *testing.T, ctx context.Context, c, s *Conn) {
	t.Helper()
	go func() {
		st, err := s.AcceptStream(ctx)
		if err == nil {
			_, _ = io.Copy(st, st)
			_ = st.Close()
		}
	}()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msg := bytes.Repeat([]byte("zmux"), 4096)
	go func() { _, _ = st.Write(msg); _ = st.Close() }()
	got, err := io.ReadAll(st)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("echo: %d bytes, %v", len(got), err)
	}
}

// TestHTTPEarlyData checks that a carrier request forwarded from early data
// is answered 425 before any admission and reaches no listener, on every
// HTTP adapter, while a request that is no carrier goes to the host.
func TestHTTPEarlyData(t *testing.T) {
	leakcheck.Check(t)
	var decided sync.Map
	adm := &Admission{Decide: func(_ context.Context, info *CarrierInfo, _ []byte) Decision {
		decided.Store(info.Request.Method, true)
		return Decision{Verdict: VerdictAccept}
	}}
	e := newUnifiedEnv(t, adm, nil)
	defer e.close()
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	hc := &http.Client{Transport: tr}
	for _, c := range []struct {
		method string
		h      map[string]string
		body   string
		want   int
	}{
		{http.MethodPost, nil, "", http.StatusTooEarly},                                  // exchange
		{http.MethodGet, map[string]string{HeaderPair: "AAAA"}, "", http.StatusTooEarly}, // downlink
		{http.MethodGet, nil, "", http.StatusTeapot},                                     // the host's
	} {
		var body io.Reader = http.NoBody
		if c.body != "" {
			body = strings.NewReader(c.body)
		}
		req, _ := http.NewRequest(c.method, e.srv.URL, body)
		req.Header.Set("Early-Data", "1")
		for k, v := range c.h {
			req.Header.Set(k, v)
		}
		resp, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s %v: %d, want %d", c.method, c.h, resp.StatusCode, c.want)
		}
	}
	// A streaming request (no Content-Length) with early data.
	pr, pw := io.Pipe()
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL, pr)
	req.Header.Set("Early-Data", "1")
	go func() { _, _ = pw.Write([]byte("x")); _ = pw.Close() }()
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTooEarly {
		t.Errorf("streaming POST: %d, want 425", resp.StatusCode)
	}
	if _, ok := decided.Load(http.MethodPost); ok {
		t.Error("an early-data POST reached the admission")
	}
	if _, ok := decided.Load(http.MethodGet); ok {
		t.Error("an early-data GET reached the admission")
	}
}

// TestHTTPTrustedSource checks the source an admission sees: the address
// a trusted intermediary reports in the forwarding header field or with
// the PROXY protocol, and the peer itself when the report comes from an
// address that is not trusted.
func TestHTTPTrustedSource(t *testing.T) {
	leakcheck.Check(t)
	loop := netip.MustParsePrefix("127.0.0.0/8")
	other := netip.MustParsePrefix("10.0.0.0/8")
	for _, c := range []struct {
		name    string
		trusted *TrustedProxies
		proxy   bool // send a PROXY protocol header
		want    string
	}{
		{"header trusted", &TrustedProxies{Prefixes: []netip.Prefix{loop}, Header: "X-Forwarded-For"}, false, "203.0.113.7:0"},
		{"header untrusted", &TrustedProxies{Prefixes: []netip.Prefix{other}, Header: "X-Forwarded-For"}, false, "127.0.0.1"},
		{"header not configured", nil, false, "127.0.0.1"},
		{"forwarded trusted", &TrustedProxies{Prefixes: []netip.Prefix{loop}, Header: "Forwarded"}, false, "198.51.100.9:0"},
		{"proxy protocol trusted", &TrustedProxies{Prefixes: []netip.Prefix{loop}, ProxyProtocol: true}, true, "192.0.2.4:4242"},
		{"proxy protocol untrusted", &TrustedProxies{Prefixes: []netip.Prefix{other}, ProxyProtocol: true}, true, "bad request"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := make(chan string, 1)
			adm := &Admission{Decide: func(_ context.Context, info *CarrierInfo, _ []byte) Decision {
				if !c.proxy && info.Peer.Addr() != netip.MustParseAddr("127.0.0.1") {
					got <- "peer " + info.Peer.String() // the peer is the connection's
				} else {
					got <- info.Source.String()
				}
				return Decision{Verdict: VerdictFallback}
			}}
			lst := NewListener(&ListenerConfig{Admission: adm})
			defer func() { _ = lst.Close() }()
			h, err := NewHTTPHandler(lst, &HTTPOptions{Trusted: c.trusted})
			if err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
			done := make(chan struct{})
			go func() { _ = srv.Serve(c.trusted.Listener(ln)); close(done) }()
			defer func() { _ = srv.Close(); <-done }()
			nc, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = nc.Close() }()
			_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
			if c.proxy {
				ph, _ := proxyproto.NewHeader(proxyproto.V1, &net.TCPAddr{IP: net.IPv4(192, 0, 2, 4), Port: 4242},
					&net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443})
				if _, err := ph.WriteTo(nc); err != nil {
					t.Fatal(err)
				}
			}
			// A downlink GET, decided on its head.
			_, _ = io.WriteString(nc, "GET / HTTP/1.1\r\nHost: x\r\n"+HeaderPair+": AAAA\r\n"+
				"X-Forwarded-For: 203.0.113.7\r\nForwarded: for=198.51.100.9\r\nConnection: close\r\n\r\n")
			b, _ := io.ReadAll(nc)
			select {
			case r := <-got:
				if !strings.HasPrefix(r, c.want) {
					t.Fatalf("source %q, want %q", r, c.want)
				}
			default:
				if c.want != "bad request" {
					t.Fatalf("no admission; response %q", b)
				}
			}
		})
	}
}
