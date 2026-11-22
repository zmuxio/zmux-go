package zmuxh3

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/carrier"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/zmuxhttp"
	"github.com/zmuxio/zmux-go/zmuxquic"
)

func tlsConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
		&tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{http3.NextProtoH3}}
}

func echo(t *testing.T, c, s *zmux.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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
	msg := make([]byte, 512<<10)
	_, _ = rand.Read(msg)
	go func() { _, _ = st.Write(msg); _ = st.Close() }()
	got, err := io.ReadAll(st)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("echo %d bytes %v", len(got), err)
	}
}

func cfg() *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{Exporter: true, EndToEnd: true}}
}

// pskCfg: HTTP request carriers expose no TLS exporter to the session.
func pskCfg() *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{9}, 32), EndToEnd: true}}
}

func TestHTTP3RequestStream(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: pskCfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }() // a server does not close what it was given
	hh, err := zmuxhttp.NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer("", srvTLS, nil, hh, nil)
	go func() { _ = srv.Serve(pc) }()
	defer func() { _ = srv.Close() }()
	client, tr := HTTPClient(cliTLS, nil)
	defer func() { _ = tr.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := "https://" + pc.LocalAddr().String() + "/"
	p, err := zmuxhttp.Streaming{Client: client, URL: url}.Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	p.Carrier = zmux.CarrierHTTP3 // zmuxhttp.Streaming.Pipe cannot know the protocol yet
	c, err := zmux.Start(ctx, p, zmux.RoleClient, pskCfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	checkKind(t, zmux.CarrierHTTP3, c, s)
	checkQUICCarrierSamples(t, c, s)
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

func TestWebTransport(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }() // a server does not close what it was given
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS), ConnContext: QUICConnContext,
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	ws := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	// Sessions are admitted by a token bound to the QUIC connection, in
	// the CONNECT request's Zmux-Prefix-Auth field.
	auth := &admit.PrefixAuth{Key: []byte("k"), Slot: 30 * time.Second, BindTLS: true}
	v, err := admit.NewPrefixAuthVerifier(auth, admit.PrefixAuthVerifierOptions{Skew: 1})
	if err != nil {
		t.Fatal(err)
	}
	lst := mustNewListener(&zmux.ListenerConfig{Config: cfg(), Admission: &zmux.Admission{Peek: admit.PrefixTokenLen, Decide: v.Decide}})
	defer func() { _ = lst.Close() }()
	wh, err := NewHandler(lst, nil, ws)
	if err != nil {
		t.Fatal(err)
	}
	h3.Handler = wh
	go func() { _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tr := &webtransport.Transport{TLSClientConfig: cliTLS}
	if _, err := DialSession(ctx, tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, nil); err == nil {
		t.Fatal("a session without a credential was admitted")
	}
	ccfg := cfg()
	ccfg.CarrierCredential = auth
	c, sess, err := Dial(ctx, tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	checkKind(t, zmux.CarrierWebTransport, c, s)
	checkQUICCarrierSamples(t, c, s)
	// Pooled pipes: two per WebTransport session, then a new session.
	pool := NewConnPool(tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, auth, carrier.PoolLimits{MaxPipesPerConnection: 2})
	defer pool.Shutdown()
	fates := make([]uint64, 0, 3)
	for range 3 {
		// The dial's context bounds the dial only: the pipes outlive it.
		dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
		pp, err := pool.OpenPipe(dctx)
		if err != nil {
			t.Fatal(err)
		}
		fates = append(fates, pp.FateGroup)
		if _, err := c.AddPipe(dctx, pp, zmux.PipeOptions{}); err != nil {
			t.Fatal(err)
		}
		dcancel()
	}
	time.Sleep(50 * time.Millisecond)
	for _, st := range c.PipeStats() {
		if st.Outbound != zmux.OutboundActive {
			t.Fatalf("pipe %d %v after its dial's context ended", st.PipeID, st.Outbound)
		}
	}
	if fates[0] == 0 || fates[0] != fates[1] || fates[2] == fates[0] {
		t.Fatalf("fate groups %v", fates)
	}
	echo(t, c, s)
	// A second pipe joined over another stream of the session.
	p2, err := SessionPipe(ctx, sess)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := c.AddPipe(ctx, p2, zmux.PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	checkQUICCarrierSamples(t, c, s)
	for _, endpoint := range []*zmux.Conn{c, s} {
		first, _ := endpoint.Pipe(0)
		other, _ := endpoint.Pipe(joined.ID())
		a, _ := first.Stats()
		b, _ := other.Stats()
		if a.CarrierSample.CarrierID != b.CarrierSample.CarrierID {
			t.Fatal("pipes on the same WebTransport session reported different carrier IDs")
		}
	}
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

func checkQUICCarrierSamples(t *testing.T, endpoints ...*zmux.Conn) {
	t.Helper()
	for _, endpoint := range endpoints {
		for _, st := range endpoint.PipeStats() {
			sample := st.CarrierSample
			if sample == nil || sample.Source != zmux.CarrierSourceQUICGo || sample.Counts.QUIC == nil ||
				sample.Counts.QUIC.PacketsSent == 0 || sample.Generation != 0 {
				t.Fatalf("pipe %d (out=%v, in=%v, failure=%v): unexpected carrier sample %+v", st.PipeID, st.Outbound, st.Inbound, st.Failure, sample)
			}
		}
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

// TestUpgradeFailureCounted checks that an admitted request whose
// WebTransport upgrade fails (here: not an HTTP/3 extended CONNECT) is a
// setup failure of the listener and nothing else.
func TestUpgradeFailureCounted(t *testing.T) {
	lst := mustNewListener(&zmux.ListenerConfig{})
	defer func() { _ = lst.Close() }()
	h, err := NewHandler(lst, nil, &webtransport.Server{})
	if err != nil {
		t.Fatal(err)
	}
	r, _ := http.NewRequest(http.MethodGet, "https://localhost/", http.NoBody)
	r.RemoteAddr = "192.0.2.1:443"
	h.ServeHTTP(&discardWriter{h: http.Header{}}, r)
	if st := lst.Stats(); !reflect.DeepEqual(st, zmux.ListenerStats{SetupFailures: 1}) {
		t.Fatalf("stats %+v", st)
	}
}

type discardWriter struct{ h http.Header }

func (w *discardWriter) Header() http.Header         { return w.h }
func (w *discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *discardWriter) WriteHeader(_ int)           {}

// Serve runs the accept loop of a WebTransport session the application
// upgraded itself: its streams carry a zmux session to the listener, and
// Serve returns once the session ends.
func TestServe(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: cfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }() // a server does not close what it was given
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	ws := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	served := make(chan error, 1)
	h3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := ws.Upgrade(w, r)
		if err != nil {
			served <- err
			return
		}
		served <- Serve(ctx, lst, sess)
	})
	go func() { _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close() }()
	tr := &webtransport.Transport{TLSClientConfig: cliTLS}
	c, sess, err := Dial(ctx, tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, cfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	checkKind(t, zmux.CarrierWebTransport, c, s)
	for _, endpoint := range []*zmux.Conn{c, s} {
		sample := endpoint.PipeStats()[0].CarrierSample
		if sample == nil || sample.Source != zmux.CarrierSourceUnknown || sample.Counts != (zmux.CarrierCounts{}) {
			t.Fatalf("external WebTransport session: unexpected carrier sample %+v", sample)
		}
	}
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
	_ = sess.CloseWithError(0, "")
	select {
	case err := <-served:
		_ = err // the session ended
	case <-ctx.Done():
		t.Fatal("Serve did not return when the session ended")
	}
}

// A join in Bound over an HTTP/3 request stream: both ends declare the
// carrier secure and take the Binding from the QUIC connection's exporter.
func TestBoundOverHTTP3(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	scfg := pskCfg()
	scfg.AllowRecordMode = func(c zmux.RecordPolicyInput, _ zmux.RecordMode, _ any) bool { return c.Secure && c.HasExporter }
	lst := mustNewListener(&zmux.ListenerConfig{Config: scfg})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	hh, err := zmuxhttp.NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer("", srvTLS, nil, hh, nil)
	go func() { _ = srv.Serve(pc) }()
	defer func() { _ = srv.Close() }()
	client, tr := HTTPClient(cliTLS, nil)
	defer func() { _ = tr.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := "https://" + pc.LocalAddr().String() + "/"
	p, err := zmuxhttp.Streaming{Client: client, URL: url}.Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	p.Carrier = zmux.CarrierHTTP3
	c, err := zmux.Start(ctx, p, zmux.RoleClient, pskCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = c.Wait() }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(); _ = s.Wait() }()
	echo(t, c, s)
	q, err := zmuxhttp.Streaming{Client: client, URL: url}.Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	q.Carrier = zmux.CarrierHTTP3
	bound := zmux.RecordBound
	pp, err := c.AddPipe(ctx, q, zmux.PipeOptions{RecordMode: zmux.Set(bound)})
	if err != nil {
		t.Fatal(err)
	}
	if st, ok := pp.Stats(); !ok || st.RecordMode != zmux.RecordBound {
		t.Fatalf("record mode %v %v", st.RecordMode, ok)
	}
	echo(t, c, s)
}

// HS-13, HS-15, L06.
func TestHTTP3TakeoverAndDelegatedTrailer(t *testing.T) {
	t.Run("next", func(t *testing.T) { testHTTP3FallbackAndTakeover(t, true) })
	t.Run("no-next", func(t *testing.T) { testHTTP3FallbackAndTakeover(t, false) })
}

func testHTTP3FallbackAndTakeover(t *testing.T, withNext bool) {
	t.Helper()
	srvTLS, cliTLS := tlsConfigs(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	l := mustNewListener(&zmux.ListenerConfig{Admission: &zmux.Admission{Peek: 4, Decide: func(_ context.Context, _ zmux.CarrierInfo, p []byte) zmux.Decision {
		v := zmux.VerdictFallback
		if string(p) == "TAKE" {
			v = zmux.VerdictTakeover
		}
		return zmux.Decision{Verdict: v}
	}}, Fallback: func(_ context.Context, tk *zmux.Takeover) { p := tk.Pipe(); _, _ = io.Copy(p.W, p.R) }})
	defer func() { _ = l.Close() }()
	opts := &zmuxhttp.Options{}
	if withNext {
		opts.Next = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, err := io.ReadAll(r.Body)
			if err != nil || string(b) != "NEXTdata" || r.Trailer.Get("X-Final") != "trail" {
				t.Errorf("body %q trailers %v err %v", b, r.Trailer, err)
			}
			w.WriteHeader(http.StatusTeapot)
		})
	}
	h, err := zmuxhttp.NewHandler(l, opts)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	srv := NewServer("", srvTLS, nil, h, opts)
	go func() { _ = srv.Serve(pc) }()
	defer func() { _ = srv.Close() }()
	client, tr := HTTPClient(cliTLS, nil)
	defer func() { _ = tr.Close() }()
	url := "https://" + pc.LocalAddr().String() + "/"
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, pr)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = pw.Write([]byte("TAKE")) }()
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK || res.ProtoMajor != 3 {
		t.Fatal(res.Status, res.Proto)
	}
	go func() { _, _ = pw.Write([]byte("data")); _ = pw.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil || string(b) != "TAKEdata" {
		t.Fatalf("takeover %q %v", b, err)
	}
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, url, io.NopCloser(bytes.NewBufferString("NEXTdata")))
	if err != nil {
		t.Fatal(err)
	}
	req.Trailer = http.Header{"X-Final": []string{"trail"}}
	res, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	wantStatus := http.StatusNotFound
	if withNext {
		wantStatus = http.StatusTeapot
	}
	if res.StatusCode != wantStatus {
		t.Fatal(res.Status)
	}
}

func TestQUICSamplerSharedWithNativeStream(t *testing.T) {
	serverTLS, clientTLS := tlsConfigs(t)
	serverTLS.NextProtos = clientTLS.NextProtos
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	l, err := quic.ListenAddr("localhost:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	qc, err := quic.DialAddr(ctx, l.Addr().String(), clientTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	p, err := zmuxquic.ConnPipe(ctx, qc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.W.Close() }()
	h3 := (quicState{qc: qc}).CarrierSampler()
	if native := carrierstats.ProviderOf(p.W); native == nil || native != h3 {
		t.Fatal("native and HTTP/3 carriers on one QUIC connection must share a sampler")
	}
}
