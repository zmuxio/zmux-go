package zmuxh3

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
)

// TestCandidates serves WebTransport and HTTP/3 request carriers on one
// target (zmux.HTTPEndpoint) and dials each in auto and explicit mode.
func TestCandidates(t *testing.T) {
	srvTLS, cliTLS := tlsConfigs(t)
	lst := mustNewListener(&zmux.ListenerConfig{Config: pskCfg()})
	defer func() { _ = lst.Close() }()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }() // a server does not close what it was given
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	wts := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	wh, err := Handler(wts, lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	sh, err := zmux.NewHTTPHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	h3.Handler = &zmux.HTTPEndpoint{Stream: sh, WebTransport: wh}
	go func() { _ = wts.Serve(pc) }()
	defer func() { _ = wts.Close() }()
	url := "https://" + pc.LocalAddr().String() + "/zmux"
	client, tr := HTTPClient(cliTLS, nil)
	defer func() { _ = tr.Close() }()
	wt := &webtransport.Transport{TLSClientConfig: cliTLS}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, c := range []struct {
		mode zmux.Mode
		want zmux.CarrierKind
	}{
		{zmux.AutoMode(WebTransportCandidate(wt, url, nil), HTTP3Candidate(client, url, nil)), zmux.CarrierWebTransport},
		{zmux.ExplicitMode(HTTP3Candidate(client, url, nil)), zmux.CarrierHTTP3},
	} {
		conn, err := zmux.DialMode(ctx, c.mode, pskCfg(), &zmux.ModePolicy{NoJoin: true})
		if err != nil {
			t.Fatal(err)
		}
		if st := conn.ConnectionStats(); st.Carrier != c.want || len(st.CarrierCandidates) != 1 {
			t.Fatalf("%v %+v, want %v", st.Carrier, st.CarrierCandidates, c.want)
		}
		s, err := lst.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		echo(t, conn, s)
		_ = conn.Close()
		_ = s.Close()
		_ = conn.Wait()
		_ = s.Wait()
	}
}
