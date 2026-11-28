package zmuxh3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

func dgCfg() *zmux.Config {
	// These tests exercise the carrier path, without the wall-clock CoDel policy.
	return &zmux.Config{EnableDatagrams: true, DatagramQueueDelay: -1, PipeAuth: zmux.PipeAuth{PSK: make([]byte, 32), EndToEnd: true}}
}

func waitNativeDatagrams(t *testing.T, ctx context.Context, c *zmux.Conn) {
	t.Helper()
	for !c.ConnectionState().NativeDatagramAvailable {
		if _, err := c.NextEvent(ctx); err != nil {
			t.Fatalf("waiting for native datagrams: %v", err)
		}
	}
}

// Receive each datagram before sending the next. This checks routing and
// representation without depending on queue bursts or sender-side accounting,
// which may run after the receiver delivers the datagram.
func roundTrip(t *testing.T, ctx context.Context, c, s *zmux.Conn, tag string) {
	t.Helper()
	for _, direction := range []struct {
		from, to *zmux.Conn
		name     string
	}{{c, s, "up"}, {s, c, "down"}} {
		waitNativeDatagrams(t, ctx, direction.from)
		before := direction.to.ConnectionStats()
		const n = 20
		for i := range n {
			want := fmt.Sprintf("%s %s %d", tag, direction.name, i)
			if err := direction.from.SendDatagramWaitWith(ctx, []byte(want), zmux.DatagramSendOptions{Carrier: zmux.DatagramRequireNative}); err != nil {
				t.Fatalf("send %q: %v", want, err)
			}
			if got, err := direction.to.ReceiveDatagram(ctx); err != nil || string(got) != want {
				t.Fatalf("receive %q: got %q, %v", want, got, err)
			}
		}
		if got := direction.to.ConnectionStats().DatagramsReceivedChannel - before.DatagramsReceivedChannel; got != n {
			t.Fatalf("%s %s: received %d native datagrams, want %d", tag, direction.name, got, n)
		}
	}
}

func TestWebTransportChannel(t *testing.T) {
	lst := mustNewListener(&zmux.ListenerConfig{Config: dgCfg()})
	t.Cleanup(func() { _ = lst.Close() })
	tr, url := startWTFixture(t, lst, nil)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	c, sess, err := Dial(ctx, tr, url, nil, dgCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A second zmux session on another stream of the same WebTransport
	// session: the session's datagrams are routed by Pipe Stream ID.
	p2, err := SessionPipe(ctx, sess)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := zmux.Start(ctx, p2, zmux.RoleClient, dgCfg())
	if err != nil {
		t.Fatal(err)
	}
	s2, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, ctx, c, s, "first")
	roundTrip(t, ctx, c2, s2, "second")
	if ChannelDropped(sess) != 0 {
		t.Fatal("datagrams dropped")
	}
	for _, x := range []*zmux.Conn{c, c2, s, s2} {
		_ = x.Close()
	}
}

func startHTTP3PostFixture(t *testing.T) (*zmux.Listener, string, *tls.Config) {
	t.Helper()
	srvTLS, cliTLS := tlsConfigs(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: dgCfg()})
	t.Cleanup(func() { _ = lst.Close() })
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	hh, err := zmuxhttp.NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer("", srvTLS, testQUIC(), hh, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(pc)
	}()
	t.Cleanup(func() {
		_ = srv.Close()
		<-done
	})
	return lst, pc.LocalAddr().String(), cliTLS
}

func TestNextBatchTransportHTTP3PostUsesRecords(t *testing.T) {
	lst, address, cliTLS := startHTTP3PostFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	qc, err := quic.DialAddr(ctx, address, cliTLS, testQUIC())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	tr, err := ClientConnTransport(&http3.Transport{EnableDatagrams: true}, qc, address)
	if err != nil {
		t.Fatal(err)
	}
	p, err := (zmuxhttp.Streaming{Client: &http.Client{Transport: tr}, URL: "https://" + address + "/"}).Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, dgCfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s)
	for _, pair := range [][2]*zmux.Conn{{c, s}, {s, c}} {
		if err := pair[0].SendDatagramWait(ctx, []byte("post")); err != nil {
			t.Fatal(err)
		}
		if got, err := pair[1].ReceiveDatagram(ctx); err != nil || string(got) != "post" {
			t.Fatalf("datagram = %q, %v", got, err)
		}
		err := pair[0].SendDatagramWaitWith(ctx, []byte("native"), zmux.DatagramSendOptions{Carrier: zmux.DatagramRequireNative})
		if drop, ok := errors.AsType[zmux.DatagramDroppedError](err); !ok || drop.Reason != zmux.DatagramDropNoNativePath {
			t.Fatalf("native-only POST datagram returned %v", err)
		}
		sent, received := pair[0].ConnectionStats(), pair[1].ConnectionStats()
		if sent.DatagramNativeDropped != 1 {
			t.Fatalf("native-only POST datagrams dropped: %d, want 1", sent.DatagramNativeDropped)
		}
		if received.DatagramsReceived != 1 || received.DatagramsReceivedChannel != 0 {
			t.Fatalf("ordinary POST received %+v", received)
		}
	}
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

// A client without the channel gets datagrams in records.
func TestNextBatchTransportHTTP3ChannelFallback(t *testing.T) {
	lst, address, cliTLS := startHTTP3PostFixture(t)
	client, tr := HTTPClient(cliTLS, &quic.Config{HandshakeIdleTimeout: 30 * time.Second})
	defer func() { _ = tr.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	p, err := zmuxhttp.Streaming{Client: client, URL: "https://" + address + "/"}.Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, dgCfg())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if err := s.SendDatagramWait(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if p, err := c.ReceiveDatagram(ctx); err != nil || len(p) != 1 || p[0] != byte(i) {
			t.Fatalf("%v %v", p, err)
		}
	}
	if st := c.ConnectionStats(); st.DatagramsReceivedChannel != 0 || st.DatagramsReceived != 5 {
		t.Fatalf("%+v", st)
	}
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

// testQUIC is a QUIC configuration with a handshake timeout that tolerates
// a loaded test host: the default 5 s fails handshakes under -race with
// many packages running at once.
func testQUIC() *quic.Config {
	return &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true,
		HandshakeIdleTimeout: 30 * time.Second}
}
