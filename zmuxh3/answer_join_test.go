package zmuxh3

import (
	"context"
	"crypto/rand"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/varint"
)

// unknownJoin is a byte-stream record holding a PIPE_JOIN whose locator
// starts with id.
func unknownJoin(id []byte) []byte {
	loc := make([]byte, 16)
	_, _ = rand.Read(loc[copy(loc, id):])
	f := varint.AppendVarint(nil, 0x1a24de36) // PIPE_JOIN
	f = append(f, byte(len(loc)))
	f = append(f, loc...)
	f = append(f, 1, 2, 0, 0) // pipe 1, receive-only, no fate group, priority 0
	f = append(f, make([]byte, 16)...)
	return append(varint.AppendVarint(nil, uint64(len(f))), f...)
}

// TestUniStreamUnknownJoin: a WebTransport unidirectional stream (no
// sending half) carrying an unknown join is closed without an answer and
// counted as rejected, with AnswerUnknownJoins on.
func TestUniStreamUnknownJoin(t *testing.T) {
	id := []byte("inst")
	srvTLS, cliTLS := tlsConfigs(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h3 := &http3.Server{TLSConfig: http3.ConfigureTLSConfig(srvTLS),
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}}
	webtransport.ConfigureHTTP3Server(h3)
	ws := &webtransport.Server{H3: h3, CheckOrigin: func(*http.Request) bool { return true }}
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: cfg(), InstanceID: id, AnswerUnknownJoins: true})
	defer func() { _ = lst.Close() }()
	wh, err := Handler(ws, lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	h3.Handler = wh
	go func() { _ = ws.Serve(pc) }()
	defer func() { _ = ws.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tr := &webtransport.Transport{TLSClientConfig: cliTLS}
	sess, err := DialSession(ctx, tr, "https://"+pc.LocalAddr().String()+"/zmux", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sess.CloseWithError(0, "") }()
	s, err := sess.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(unknownJoin(id)); err != nil {
		t.Fatal(err)
	}
	for lst.Stats().Rejected != 1 {
		if ctx.Err() != nil {
			t.Fatalf("stats %+v", lst.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
