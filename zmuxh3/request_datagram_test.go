package zmuxh3

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

func TestHTTP3DatagramAbortsOnlyOrdinaryRequest(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodConnect} {
		for _, receiver := range []string{"server", "client"} {
			t.Run(method+"/"+receiver, func(t *testing.T) {
				testHTTP3DatagramAbort(t, method, receiver == "server")
			})
		}
	}
}

func testHTTP3DatagramAbort(t *testing.T, method string, serverReceives bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	serverTLS, clientTLS := tlsConfigs(t)
	active := make(chan *http.Request, 1)
	server := NewServer("", serverTLS, &quic.Config{EnableDatagrams: true}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var body [1]byte
		if _, err := io.ReadFull(r.Body, body[:]); err != nil {
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
		active <- r
		<-r.Context().Done()
	}), nil)
	packet, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	serving := make(chan struct{})
	go func() {
		defer close(serving)
		_ = server.Serve(packet)
	}()
	defer func() {
		_ = server.Close()
		_ = packet.Close()
		<-serving
	}()
	url := "https://" + packet.LocalAddr().String() + "/"
	qc, err := quic.DialAddr(ctx, packet.LocalAddr().String(), clientTLS, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	transport := &http3.Transport{EnableDatagrams: true}
	cc := transport.NewClientConn(qc)
	select {
	case <-cc.ReceivedSettings():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !cc.Settings().EnableDatagrams {
		t.Fatal("peer did not negotiate HTTP Datagrams")
	}
	reader, writer := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	// RoundTrip uploads the body concurrently. Keep it open until the
	// request receives a datagram and both stream directions are canceled.
	written := make(chan struct{})
	go func() {
		defer close(written)
		_, _ = writer.Write([]byte("x"))
	}()
	defer func() { _ = writer.Close(); <-written }()
	resp, err := cc.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var incoming *http.Request
	select {
	case incoming = <-active:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if incoming.ContentLength > 0 || incoming.Proto != "HTTP/3.0" {
		t.Fatalf("request reached the server as %s length %d", incoming.Proto, incoming.ContentLength)
	}

	// This request is the first client-initiated bidirectional stream, so
	// its Quarter Stream ID is zero. Send a framed HTTP Datagram directly
	// to emulate a peer that used the connection's negotiated capability.
	sender := qc
	if !serverReceives {
		sender = quicInContext(incoming.Context()).qc
	}
	if err := sender.SendDatagram(append(quicvarint.Append(nil, 0), 'd')); err != nil {
		t.Fatal(err)
	}
	var data [1]byte
	_, err = resp.Body.Read(data[:])
	var h3err *http3.Error
	if !errors.As(err, &h3err) || h3err.ErrorCode != http3.ErrCodeDatagramError || h3err.Remote != serverReceives {
		t.Fatalf("read returned %v, want H3_DATAGRAM_ERROR (remote=%t)", err, serverReceives)
	}
	select {
	case <-incoming.Context().Done():
	case <-ctx.Done():
		t.Fatal("request's write side remained open")
	}
	next, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	health, err := cc.RoundTrip(next)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = health.Body.Close() }()
	if health.StatusCode != http.StatusNoContent {
		t.Fatalf("second request on the same QUIC connection: %s", health.Status)
	}
}
