package zmuxws

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/netx/wsconn"
	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/prefixtest"
	"github.com/zmuxio/zmux-go/internal/teststream"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

func mpConfig() *zmux.Config {
	return &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}}, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32), EndToEnd: true}}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type handshakeCloseCounter struct {
	net.Conn
	closes int
}

func (c *handshakeCloseCounter) Close() error {
	c.closes++
	return c.Conn.Close()
}

func TestHandshakeClosesFailedConnectionOnce(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { _ = server.Close() })
	conn := &handshakeCloseCounter{Conn: client}
	_, err := handshake(t.Context(), conn, "", nil)
	if err == nil {
		t.Fatal("handshake accepted an empty URL")
	}
	if conn.closes != 1 {
		t.Fatalf("connection closed %d times, want 1", conn.closes)
	}
}

// next is the application's handler of requests that are not carriers.
var next = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusTeapot)
	_, _ = io.WriteString(w, "next "+r.URL.Path)
})

// server serves the handler of l and o on a test server and returns its
// ws:// URL.
func server(t *testing.T, l *zmux.Listener, o *zmuxhttp.Options, protocols ...string) string {
	t.Helper()
	h, err := NewHandler(l, o, protocols...)
	if err != nil {
		t.Fatal(err)
	}
	return wsTestServer(t, l, h)
}

func wsTestServer(t *testing.T, l *zmux.Listener, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		_ = l.Close()
		srv.Close()
	})
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// echo checks a round trip of n bytes over a stream that s echoes.
func echo(t *testing.T, ctx context.Context, c, s *zmux.Conn, n int) {
	t.Helper()
	msg := make([]byte, n)
	_, _ = rand.Read(msg)
	teststream.Echo(t, ctx, c.OpenStreamSync, s.AcceptStream, msg)
}

func closeBoth(c, s *zmux.Conn) {
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

func TestSession(t *testing.T) {
	ctx := testContext(t)
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	u := server(t, l, nil)
	c, err := Dial(ctx, u+"/zmux", nil, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, ctx, c, s, 1<<20)

	// A second pipe.
	p, err := DialPipe(ctx, u+"/zmux", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddPipe(ctx, p, zmux.PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	if n := len(c.Pipes()); n != 2 {
		t.Fatalf("%d pipes", n)
	}
	echo(t, ctx, c, s, 256<<10)
	checkKind(t, zmux.CarrierWebSocket, c, s)
	closeBoth(c, s)
}

// wss://, with TLS over the default dialer and over the application's,
// admitted by a token bound to the TLS connection: Dial presents
// Config.CarrierCredential, DialPipe DialOptions.CarrierCredential for a join.
func TestTLS(t *testing.T) {
	ctx := testContext(t)
	auth, v, id := prefixtest.New(t, admit.PrefixCarrierOptions{BindTLS: true}, zmux.VerdictFallback)
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(),
		Admission: v.Admission()})
	h, err := NewHandler(l, &zmuxhttp.Options{Next: next})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(h)
	t.Cleanup(func() {
		_ = l.Close()
		srv.Close()
	})
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	u := "wss" + strings.TrimPrefix(srv.URL, "https")
	cfg := mpConfig()
	cfg.CarrierCredential = auth
	for _, d := range []*net.Dialer{nil, {}} {
		o := &DialOptions{TLSConfig: &tls.Config{RootCAs: roots, ServerName: "example.com"}}
		if d != nil {
			o.Dialer = d
		}
		c, err := Dial(ctx, u, o, cfg)
		if err != nil {
			t.Fatal(err)
		}
		s, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := admit.BoundPrincipal(s); !ok || got != id {
			t.Fatalf("bound principal = %x, %v", got, ok)
		}
		jo := *o
		jo.CarrierCredential = auth
		p, err := DialPipe(ctx, u, &jo)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.AddPipe(ctx, p, zmux.PipeOptions{}); err != nil {
			t.Fatal(err)
		}
		echo(t, ctx, c, s, 256<<10)
		for _, x := range []*zmux.Conn{c, s} {
			if st := x.ConnectionState(); st.ApplicationProtocol != zmux.DefaultALPN || st.TLS == nil || st.TLS.NegotiatedProtocol == zmux.DefaultALPN {
				t.Fatalf("subprotocol %q, TLS %v", st.ApplicationProtocol, st.TLS)
			}
		}
		if n := len(c.PipeStats()); n != 2 {
			t.Fatalf("%d pipes", n)
		}
		closeBoth(c, s)
	}
	// Without a credential, the upgrade is not admitted.
	_, err = DialPipe(ctx, u, &DialOptions{TLSConfig: &tls.Config{RootCAs: roots, ServerName: "example.com"}})
	var he *wsconn.HandshakeError
	if !errors.As(err, &he) || he.Status != http.StatusTeapot {
		t.Fatalf("without a credential: %v", err)
	}
}

// The subprotocols: identifiers of both forms, accepted also as their
// aliases; a request without an accepted one, and any request that is not
// an upgrade, reaches the next handler, the admission undecided.
func TestSubprotocols(t *testing.T) {
	ctx := testContext(t)
	var decided atomic.Int32
	o := &zmuxhttp.Options{Next: next}
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: &zmux.Admission{
		Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
			decided.Add(1)
			return zmux.Decision{}
		}}})
	u := server(t, l, o, "qmux-02.chat", "moq-transport-18")
	for _, tc := range []struct {
		want  string // "" refused
		offer []string
	}{
		{offer: []string{"qmux-02.chat"}, want: "qmux-02.chat"},
		{offer: []string{"qmux-01.chat"}, want: "qmux-01.chat"},
		{offer: []string{"moq-transport-18"}, want: "moq-transport-18"},
		{offer: []string{"moq-transport-18", "qmux-01.chat"}, want: "qmux-01.chat"}, // the server's preference
		{offer: []string{"qmux-02"}, want: ""},
		{offer: []string{"moq-transport-17"}, want: ""},
	} {
		d := &wsconn.Dialer{Subprotocols: tc.offer}
		wc, resp, err := d.Dial(ctx, u+"/x")
		if tc.want == "" {
			if err == nil || resp == nil || resp.Code != http.StatusTeapot {
				t.Fatalf("offer %v: %v %+v", tc.offer, err, resp)
			}
			continue
		}
		if err != nil {
			t.Fatalf("offer %v: %v", tc.offer, err)
		}
		if wc.Subprotocol() != tc.want {
			t.Fatalf("offer %v: chose %q", tc.offer, wc.Subprotocol())
		}
		_ = wc.Close()
	}
	if n := decided.Load(); n != 4 {
		t.Fatalf("%d decisions for 4 upgrades", n)
	}
	// A plain request.
	resp, err := http.Get("http" + strings.TrimPrefix(u, "ws") + "/plain")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot || string(b) != "next /plain" || decided.Load() != 4 {
		t.Fatalf("plain request: %d %q", resp.StatusCode, b)
	}
}

// An admission on the upgrade request: a valid token in the
// Zmux-Prefix-Auth field is accepted, and a replayed or missing one falls
// back to the next handler, which gets the request as it came.
func TestAdmissionAcceptAndFallback(t *testing.T) {
	ctx := testContext(t)
	auth, v, _ := prefixtest.New(t, admit.PrefixCarrierOptions{}, zmux.VerdictFallback)
	o := &zmuxhttp.Options{Next: next}
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(),
		Admission: v.Admission()})
	handler, err := NewHandler(l, o)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(handler)
	t.Cleanup(func() {
		_ = l.Close()
		srv.Close()
	})
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	tlsConfig := &tls.Config{RootCAs: roots}
	u := "wss" + strings.TrimPrefix(srv.URL, "https")
	name, value, err := auth.Header(&zmux.CarrierInfo{Stage: zmux.StageHead, Encrypted: true})
	if err != nil || name != admit.HeaderPrefixAuth {
		t.Fatal(name, err)
	}
	h := http.Header{name: {value}}
	c, err := Dial(ctx, u+"/z", &DialOptions{Header: h, TLSConfig: tlsConfig}, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := admit.BoundPrincipal(s); ok {
		t.Fatal("unbound ticket exposed a principal")
	}
	echo(t, ctx, c, s, 64<<10)
	closeBoth(c, s)

	for name, h := range map[string]http.Header{"replayed": h, "missing": nil} {
		_, err := DialPipe(ctx, u+"/z", &DialOptions{Header: h, TLSConfig: tlsConfig})
		var he *wsconn.HandshakeError
		if !errors.As(err, &he) || he.Status != http.StatusTeapot {
			t.Fatalf("%s token: %v", name, err)
		}
	}
	// One credential source per call.
	if _, err := Dial(ctx, u+"/z", &DialOptions{CarrierCredential: auth}, mpConfig()); !errors.Is(err, zmux.ErrInvalidConfig) {
		t.Fatalf("Dial with DialOptions.CarrierCredential: %v", err)
	}
}

// rawUpgrade sends an upgrade request on a new connection and returns what
// the server answers until it ends the connection.
func rawUpgrade(t *testing.T, u string) []byte {
	t.Helper()
	addr := strings.TrimPrefix(u, "ws://")
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	_ = nc.SetDeadline(time.Now().Add(10 * time.Second))
	_, err = io.WriteString(nc, "GET / HTTP/1.1\r\nHost: "+addr+"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: qmux-02\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(bufio.NewReader(nc))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return b
}

// Stall and Close answer nothing: a stalled request ends after the stall
// duration, a closed one at once.
func TestAdmissionStallAndClose(t *testing.T) {
	ctx := testContext(t)
	for _, verdict := range []zmux.AdmissionVerdict{zmux.VerdictStall, zmux.VerdictClose} {
		cfg := mpConfig()
		clk := timeshim.NewFake(time.Now())
		cfg.ClockDomain = zmux.NewClockDomain(clk)
		const hold = time.Second
		l := mustNewListener(&zmux.ListenerConfig{Config: cfg, Admission: &zmux.Admission{
			StallMin: hold, StallMax: hold,
			Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
				return zmux.Decision{Verdict: verdict}
			}}})
		u := server(t, l, &zmuxhttp.Options{Next: next})
		answer := make(chan []byte, 1)
		go func() { answer <- rawUpgrade(t, u) }()
		if verdict == zmux.VerdictStall {
			if err := clk.WaitDeadline(ctx, clk.Now().Add(hold)); err != nil {
				t.Fatal(err)
			}
			clk.Advance(hold)
		}
		select {
		case b := <-answer:
			if len(b) != 0 {
				t.Fatalf("verdict %d: answered %q", verdict, b)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

// pipePair returns the pipe inputs of a server and a client WebSocket.
func pipePair(t *testing.T) (srv, cli zmux.PipeIO) {
	t.Helper()
	ctx := testContext(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ch := make(chan *wsconn.Conn, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			ch <- nil
			return
		}
		c, _ := wsconn.Accept(ctx, nc, &wsconn.ServerOptions{Options: options})
		ch <- c
	}()
	cc, _, err := (&wsconn.Dialer{}).Dial(ctx, "ws://"+ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sc := <-ch
	if sc == nil {
		t.Fatal("no server connection")
	}
	t.Cleanup(func() {
		_ = cc.NetConn().Close()
		_ = sc.NetConn().Close()
	})
	return ConnPipe(sc), ConnPipe(cc)
}

// A message is a record as it was written; a close frame is the end, a
// connection that ends without one, and a text message, a failure.
func TestCarrier(t *testing.T) {
	srv, cli := pipePair(t)
	w, r := cli.W.(zmux.MessageWriter), srv.R.(zmux.MessageReader)
	for _, m := range [][]byte{[]byte("one"), {}, bytes.Repeat([]byte{9}, zmux.MaxRecordSize)} {
		if err := w.WriteMessage(m); err != nil {
			t.Fatal(err)
		}
		got, err := r.ReadMessage(zmux.MaxRecordSize)
		if err != nil || !bytes.Equal(got.Bytes(), m) {
			t.Fatalf("read %d bytes, %v", got.Len(), err)
		}
		got.Release()
	}
	// Above the largest record by more than the room for a transform's
	// overhead: the carrier fails.
	if err := w.WriteMessage(make([]byte, zmux.MaxRecordSize+66)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadMessage(zmux.MaxRecordSize); err == nil {
		t.Fatal("oversized message read")
	}

	// A bounded read: a message of the limit is read, a longer one fails
	// the carrier with zmux.ErrMessageTooLarge and close status 1009.
	srv, cli = pipePair(t)
	w, lr := cli.W.(zmux.MessageWriter), srv.R.(zmux.MessageReader)
	for _, n := range []int{64, 65} {
		if err := w.WriteMessage(make([]byte, n)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := lr.ReadMessage(64)
	if err != nil || got.Len() != 64 {
		t.Fatalf("read %d bytes, %v", got.Len(), err)
	}
	got.Release()
	if _, err := lr.ReadMessage(64); !errors.Is(err, zmux.ErrMessageTooLarge) {
		t.Fatalf("over the limit: %v", err)
	}
	var ce *wsconn.CloseError
	if _, _, err := cli.W.(*carrier).c.ReadMessage(); !errors.As(err, &ce) || ce.Code != wsconn.StatusMessageTooBig {
		t.Fatalf("peer saw %v", err)
	}

	srv, cli = pipePair(t)
	if err := cli.W.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.R.(zmux.MessageReader).ReadMessage(zmux.MaxRecordSize); !errors.Is(err, io.EOF) {
		t.Fatalf("after a close frame: %v", err)
	}
	if _, err := cli.R.(zmux.MessageReader).ReadMessage(zmux.MaxRecordSize); !errors.Is(err, io.EOF) {
		t.Fatalf("after the echoed close frame: %v", err)
	}

	srv, cli = pipePair(t)
	cli.W.(interface{ Abort(error) }).Abort(nil)
	if _, err := srv.R.(zmux.MessageReader).ReadMessage(zmux.MaxRecordSize); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("after an abort: %v", err)
	}

	srv, cli = pipePair(t)
	if err := cli.W.(*carrier).c.WriteMessage(wsconn.Text, []byte("text")); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.R.(zmux.MessageReader).ReadMessage(zmux.MaxRecordSize); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("after a text message: %v", err)
	}
}

// checkKind fails the test unless every pipe of each session reports
// carrier kind k.
func checkKind(t *testing.T, k zmux.CarrierKind, cs ...*zmux.Conn) {
	t.Helper()
	for _, c := range cs {
		st := c.PipeStats()
		if len(st) == 0 {
			t.Fatal("no pipes")
		}
		for _, p := range st {
			if p.Carrier != k {
				t.Fatalf("pipe %d: carrier %v, want %v", p.PipeID, p.Carrier, k)
			}
		}
	}
}

// TestUpgradeFailureCounted checks that a WebSocket upgrade that passes
// the checks but cannot be completed (here: a response writer that cannot
// be taken over) is a setup failure of the listener and nothing else.
func TestUpgradeFailureCounted(t *testing.T) {
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	h, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	r.Header.Set("Sec-WebSocket-Protocol", zmux.DefaultALPN)
	h.ServeHTTP(httptest.NewRecorder(), r)
	if st := lst.Stats(); !reflect.DeepEqual(st, zmux.ListenerStats{SetupFailures: 1}) {
		t.Fatalf("stats %+v", st)
	}
}

// NoSubprotocol accepts an upgrade request without a Sec-WebSocket-Protocol
// field; a request that offers subprotocols, none accepted, still reaches
// the next handler, and a server without NoSubprotocol refuses the bare
// request.
func TestNoSubprotocol(t *testing.T) {
	ctx := testContext(t)
	o := &zmuxhttp.Options{Next: next}
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	u := server(t, l, o, "qmux-02.chat", NoSubprotocol)

	c, err := Dial(ctx, u, &DialOptions{Protocols: []string{NoSubprotocol}}, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, ctx, c, s, 4096)
	closeBoth(c, s)

	p, err := DialPipe(ctx, u, &DialOptions{Protocols: []string{NoSubprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	if p.Protocol != "" {
		t.Fatalf("Protocol = %q", p.Protocol)
	}
	_ = p.W.Close()

	c, err = Dial(ctx, u, &DialOptions{Protocols: []string{"qmux-02.chat"}}, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err = l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, ctx, c, s, 4096)
	closeBoth(c, s)

	// Offered but not accepted: the next handler.
	_, resp, err := (&wsconn.Dialer{Subprotocols: []string{"qmux-02"}}).Dial(ctx, u+"/x")
	if err == nil || resp == nil || resp.Code != http.StatusTeapot {
		t.Fatalf("unmatched offer: %v %+v", err, resp)
	}

	// Mixed with others on the client.
	_, err = DialPipe(ctx, u, &DialOptions{Protocols: []string{NoSubprotocol, "qmux-02.chat"}})
	if !errors.Is(err, zmux.ErrInvalidConfig) {
		t.Fatalf("mixed protocols: %v", err)
	}

	// A server that does not accept it.
	l2 := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	u2 := server(t, l2, o, "qmux-02.chat")
	_, resp, err = (&wsconn.Dialer{}).Dial(ctx, u2+"/x")
	if err == nil || resp == nil || resp.Code != http.StatusTeapot {
		t.Fatalf("bare request to a server without NoSubprotocol: %v %+v", err, resp)
	}
}
