package zmuxh3

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/prefixtest"
)

func TestWebTransportReusesQUICStreamCredit(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		name := "owned"
		if borrowed {
			name = "borrowed"
		}
		t.Run(name, func(t *testing.T) { testWebTransportStreamCredit(t, borrowed) })
	}
}

func testWebTransportStreamCredit(t *testing.T, borrowed bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	serverTLS, clientTLS := tlsConfigs(t)
	qconf := &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true, MaxIncomingStreams: 2}
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(serverTLS), QUICConfig: qconf}
	server := &Server{H3: h3}
	accepted := make(chan *Session, 1)
	h3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, state, err := upgradeWithin(r.Context(), server, w, r)
		if err != nil {
			if sess != nil {
				_ = sess.CloseWithError(0, "")
			} else if state.taken == nil {
				panic(http.ErrAbortHandler)
			}
			return
		}
		accepted <- sess
		<-sess.Context().Done()
	})
	packet, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = packet.Close() }()
	var socket *quic.Transport
	serve := func() error { return server.Serve(packet) }
	if borrowed {
		socket = &quic.Transport{Conn: packet}
		defer func() { _ = socket.Close() }()
		ln, err := socket.Listen(h3.TLSConfig, h3.QUICConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ln.Close() }()
		serve = func() error {
			qc, err := ln.Accept(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = qc.CloseWithError(0, "") }()
			return server.ServeQUICConn(qc)
		}
	}
	serving := make(chan struct{})
	go func() { defer close(serving); _ = serve() }()
	defer func() {
		cancel()
		_ = server.Close()
		<-serving
	}()

	clientConfig := qconf.Clone()
	clientConfig.MaxIncomingStreams = 1
	transport := &Transport{QUICConfig: clientConfig, TLSClientConfig: clientTLS}
	defer func() { _ = transport.Close() }()
	url := "https://" + packet.LocalAddr().String() + "/"
	var client *Session
	if borrowed {
		qc, err := quic.DialAddr(ctx, packet.LocalAddr().String(), clientTLS, clientConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = qc.CloseWithError(0, "") }()
		cc, err := transport.NewClientConn(qc)
		if err != nil {
			t.Fatal(err)
		}
		resp, sess, err := cc.Dial(ctx, url, nil)
		if resp != nil {
			defer func() { _ = resp.Body.Close() }()
		}
		if err != nil {
			t.Fatal(err)
		}
		client = sess
	} else {
		// A credential selects the adapter's owned QUIC dialer.
		auth, _, _ := prefixtest.New(t, admit.PrefixCarrierOptions{BindTLS: true}, zmux.VerdictClose)
		client, err = DialSession(ctx, transport, url, nil, auth)
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() { _ = client.CloseWithError(0, "") }()
	var peer *Session
	select {
	case peer = <-accepted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if client.state.RequireStreamCreditRelease || peer.state.RequireStreamCreditRelease {
		t.Fatal("the pinned WebTransport version requires automatic QUIC stream credit")
	}
	for range 3 {
		checkWebTransportStreamCredit(t, ctx, client, peer)
		checkWebTransportStreamCredit(t, ctx, peer, client)
	}
}

func checkWebTransportStreamCredit(t *testing.T, ctx context.Context, sender, receiver *Session) {
	t.Helper()
	out, err := sender.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	_ = out.SetDeadline(deadline)
	if _, err := out.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	in, err := receiver.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = in.SetDeadline(deadline)
	got, err := io.ReadAll(io.LimitReader(in, 2))
	if err != nil || string(got) != "x" {
		t.Fatalf("receive: %q, %v", got, err)
	}
	if err := in.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(io.LimitReader(out, 1))
	if err != nil || len(got) != 0 {
		t.Fatalf("finish: %q, %v", got, err)
	}
}
