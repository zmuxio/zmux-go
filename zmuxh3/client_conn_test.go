package zmuxh3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

func TestClientConnTransportSamples(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	lst := mustNewListener(&zmux.ListenerConfig{Config: pskCfg()})
	t.Cleanup(func() { _ = lst.Close() })
	handler, err := zmuxhttp.NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	type requestInfo struct {
		conn      *quic.Conn
		method    string
		datagrams bool
	}
	seen := make(chan requestInfo, 1)
	dial, authority := boundClientServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		settings := w.(*h3CarrierWriter).ResponseWriter.(http3.Settingser)
		select {
		case <-settings.ReceivedSettings():
		case <-r.Context().Done():
			return
		}
		select {
		case seen <- requestInfo{conn: quicInContext(r.Context()).qc, datagrams: settings.Settings().EnableDatagrams, method: r.Method}:
		case <-r.Context().Done():
			return
		}
		handler.ServeHTTP(w, r)
	}))

	first := dial(ctx, true)
	second := dial(ctx, false)
	if first.LocalAddr().String() != second.LocalAddr().String() {
		t.Fatal("QUIC connections must share a local address")
	}
	clientExporters := make(map[*quic.Conn][]byte)
	newClient := func(qc *quic.Conn, datagrams bool) *http.Client {
		tr := &http3.Transport{EnableDatagrams: datagrams}
		rt, err := ClientConnTransport(tr, qc, authority)
		if err != nil {
			t.Fatal(err)
		}
		again, err := ClientConnTransport(tr, qc, authority)
		if err != nil || again != rt {
			t.Fatalf("repeated binding changed transport: %v", err)
		}
		if _, err := ClientConnTransport(&http3.Transport{EnableDatagrams: datagrams}, qc, authority); err == nil {
			t.Fatal("second HTTP/3 client accepted on the same QUIC connection")
		}
		if rt.(*clientConnTransport).state.tls == nil {
			t.Fatal("the HTTP/3 transport did not snapshot the completed handshake")
		}
		clientExporters[qc] = boundClientExporter(t, qc)
		return &http.Client{Transport: rt}
	}
	if _, err := ClientConnTransport(&http3.Transport{EnableDatagrams: true}, second, authority); err == nil {
		t.Fatal("HTTP/3 datagrams accepted with local QUIC datagrams disabled")
	}
	client1, client2 := newClient(first, true), newClient(second, false)
	open := func(client *http.Client, qc *quic.Conn, datagrams bool) zmux.CarrierID {
		c, s := boundClientSession(t, ctx, lst, client, authority, pskCfg())
		checkKind(t, zmux.CarrierHTTP3, c, s)
		var actual requestInfo
		select {
		case actual = <-seen:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if actual.method != http.MethodPost || actual.datagrams != datagrams || actual.conn == nil {
			t.Fatalf("unexpected streaming request: %+v", actual)
		}
		if !bytes.Equal(clientExporters[qc], boundClientExporter(t, actual.conn)) {
			t.Fatal("the streaming request did not use the caller's QUIC connection")
		}
		for _, ep := range []struct {
			session *zmux.Conn
			conn    *quic.Conn
		}{{c, qc}, {s, actual.conn}} {
			sampler := (quicState{qc: ep.conn}).CarrierSampler()
			carrierstats.Prime(sampler)
			checkQUICCarrierSamples(t, ep.session)
			want := carrierstats.Cached(sampler).ID
			if got := ep.session.PipeStats()[0].CarrierSample.CarrierID; got != want {
				t.Fatalf("carrier ID %v, want %v", got, want)
			}
		}
		echo(t, c, s)
		return c.PipeStats()[0].CarrierSample.CarrierID
	}

	id1 := open(client1, first, true)
	if id := open(client1, first, true); id != id1 {
		t.Fatal("streaming requests on the same connection did not share the sampler")
	}
	id2 := open(client2, second, false)
	if id1 == id2 {
		t.Fatal("connections sharing a socket reported the same carrier ID")
	}
	pathSocket, err := net.ListenPacket("udp", net.JoinHostPort(first.LocalAddr().(*net.UDPAddr).IP.String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pathSocket.Close() })
	pathTransport := &quic.Transport{Conn: pathSocket}
	t.Cleanup(func() {
		_ = first.CloseWithError(0, "")
		_ = pathTransport.Close()
	})
	path, err := first.AddPath(pathTransport)
	if err != nil {
		t.Fatal(err)
	}
	if err := path.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	if err := path.Switch(); err != nil {
		t.Fatal(err)
	}
	if id := open(client1, first, true); id != id1 {
		t.Fatal("migration changed the carrier ID")
	}
	if err := first.CloseWithError(0, ""); err != nil {
		t.Fatal(err)
	}
	reconnect := dial(ctx, true)
	if id := open(newClient(reconnect, true), reconnect, true); id == id1 || id == id2 {
		t.Fatal("reconnect reused a carrier ID")
	}
}

func boundClientExporter(t *testing.T, qc *quic.Conn) []byte {
	t.Helper()
	state := qc.ConnectionState().TLS
	exporter, err := state.ExportKeyingMaterial("EXPORTER-zmuxh3-test", nil, 32)
	if err != nil {
		t.Fatal(err)
	}
	return exporter
}

func TestClientConnTransportRequestScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var requests atomic.Int32
	dial, authority := boundClientServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/hold":
			_ = http.NewResponseController(w).Flush()
			<-r.Context().Done()
		case "/redirect":
			w.Header().Set("Location", (&url.URL{Scheme: "https", Host: otherHTTPSAuthority(r.Host)}).String())
			w.WriteHeader(http.StatusTemporaryRedirect)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	qc := dial(ctx, false)
	tr := &http3.Transport{}
	rt, err := ClientConnTransport(tr, qc, authority)
	if err != nil {
		t.Fatal(err)
	}
	other := otherHTTPSAuthority(authority)
	if _, err := ClientConnTransport(tr, qc, other); err == nil {
		t.Fatal("connection rebound to another authority")
	}

	client := &http.Client{Transport: rt}
	base := "https://" + authority
	for _, tc := range []struct {
		name string
		url  string
		host string
	}{
		{"other URL", "https://" + other, ""},
		{"other Host", base, other},
		{"insecure scheme", "http://" + authority, ""},
		{"cross-authority redirect", base + "/redirect", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, tc.url, http.NoBody)
			if err != nil {
				t.Fatal(err)
			}
			req.Host = tc.host
			resp, err := client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
				t.Fatalf("request to %s with Host %q was accepted", tc.url, tc.host)
			}
		})
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("peer saw %d requests, want only the original redirect request", n)
	}

	body := &blockingClientBody{reading: make(chan struct{}), closed: make(chan struct{})}
	reqCtx, cancelReq := context.WithCancel(ctx)
	defer cancelReq()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+"/hold", body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	select {
	case <-body.reading:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancelReq()
	select {
	case <-body.closed:
	case <-ctx.Done():
		t.Fatal("request cancellation did not close the blocked body")
	}
	client.CloseIdleConnections()
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, base, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	next, err := client.Do(req)
	if err != nil {
		t.Fatalf("caller connection unusable after cancellation and transport close: %v", err)
	}
	_ = next.Body.Close()
	if next.StatusCode != http.StatusNoContent {
		t.Fatal(next.Status)
	}
	sampler := carrierstats.ProviderOf(next.Body)
	if sampler == nil || sampler != (quicState{qc: qc}).CarrierSampler() {
		t.Fatal("the response to a request without a body lost the QUIC sampler")
	}
	carrierstats.Prime(sampler)
	if sample := carrierstats.Cached(sampler); !sample.Known || sample.Source != carrierstats.SourceQUICGo || sample.Sent == 0 {
		t.Fatalf("missing actual QUIC counters: %+v", sample)
	}
	if err := qc.CloseWithError(0, ""); err != nil {
		t.Fatal(err)
	}
	after, err := client.Do(req)
	if err == nil {
		_ = after.Body.Close()
		t.Fatal("request succeeded after its bound QUIC connection closed")
	}
}

func otherHTTPSAuthority(authority string) string {
	host, _, _ := net.SplitHostPort(authority)
	return net.JoinHostPort(host, "443")
}

type blockingClientBody struct {
	reading chan struct{}
	closed  chan struct{}
}

func (b *blockingClientBody) Read([]byte) (int, error) {
	close(b.reading)
	<-b.closed
	return 0, io.ErrClosedPipe
}

func (b *blockingClientBody) Close() error {
	close(b.closed)
	return nil
}

func TestClientConnTransportWithoutBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	lst := mustNewListener(&zmux.ListenerConfig{Config: pskCfg()})
	t.Cleanup(func() { _ = lst.Close() })
	handler, err := zmuxhttp.NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	dial, authority := boundClientServer(t, handler)
	qc := dial(ctx, false)
	tr := &http3.Transport{}
	c, s := boundClientSession(t, ctx, lst, &http.Client{Transport: tr.NewClientConn(qc)}, authority, pskCfg())
	if sample := c.PipeStats()[0].CarrierSample; sample != nil {
		t.Fatalf("HTTP/3 client without a provider reported a sample: %+v", sample)
	}
	checkQUICCarrierSamples(t, s)
}

func boundClientSession(t *testing.T, ctx context.Context, lst *zmux.Listener, client *http.Client, authority string, config *zmux.Config) (*zmux.Conn, *zmux.Conn) {
	t.Helper()
	p, err := zmuxhttp.Streaming{Client: client, URL: "https://" + authority}.Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	p.Carrier = zmux.CarrierHTTP3
	c, err := zmux.Start(ctx, p, zmux.RoleClient, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); _ = c.Wait() })
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = s.Wait() })
	return c, s
}

func boundClientServer(t *testing.T, handler http.Handler) (func(context.Context, bool) *quic.Conn, string) {
	t.Helper()
	srvTLS, cliTLS := tlsConfigs(t)
	loopback, err := net.ResolveUDPAddr("udp4", net.JoinHostPort(cliTLS.ServerName, "0"))
	if err != nil {
		t.Fatal(err)
	}
	serverSocket, err := net.ListenUDP("udp4", loopback)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverSocket.Close() })
	srv := NewServer("", srvTLS, &quic.Config{EnableDatagrams: true}, handler, nil)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(serverSocket) }()
	t.Cleanup(func() {
		_ = srv.Close()
		err := <-done
		if !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	})
	clientSocket, err := net.ListenUDP("udp4", loopback)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientSocket.Close() })
	tr := &quic.Transport{Conn: clientSocket}
	t.Cleanup(func() { _ = tr.Close() })
	addr := serverSocket.LocalAddr()
	dial := func(ctx context.Context, datagrams bool) *quic.Conn {
		t.Helper()
		qc, err := tr.Dial(ctx, addr, cliTLS, &quic.Config{EnableDatagrams: datagrams})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = qc.CloseWithError(0, "") })
		return qc
	}
	return dial, addr.String()
}
