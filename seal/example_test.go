package seal_test

import (
	"context"
	"net"
	"net/netip"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
)

// A server that accepts only protected carriers, and a client that pins
// its key and sends its first stream in msg1.
func Example() {
	serverKey, _ := seal.GenerateKey()
	clientKey, _ := seal.GenerateKey()
	psk := []byte("a PSK of at least 16 random bytes")

	srv, _ := seal.NewServer(&seal.ServerConfig{
		Keys: []seal.PrivateKey{serverKey},
		Authorize: func(_ context.Context, peer seal.PublicKey, _ netip.AddrPort) ([]byte, any, bool) {
			return psk, "client 1", peer == clientKey.Public()
		},
	})
	lst := zmux.NewListener(&zmux.ListenerConfig{
		Config:    &zmux.Config{EarlyData: zmux.EarlyData{Replayable: true}},
		Admission: srv.Admission(seal.SealedOnly),
		WrapPipe:  srv.WrapPipe,
	})
	defer func() { _ = lst.Close() }()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = lst.Serve(context.Background(), ln) }()

	ctx := context.Background()
	conn, err := seal.Dial(ctx, "tcp", ln.Addr().String(),
		&seal.ClientConfig{Peers: []seal.PublicKey{serverKey.Public()}, Key: clientKey, PSK: psk},
		&zmux.Config{EarlyData: zmux.EarlyData{ZmuxPeer: true, Replayable: true, HoldFirstFlight: true}})
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	st, _ := conn.OpenStream()
	_, _ = st.Write([]byte("HELLO"))
	conn.ReleaseFirstFlight() // msg1 carries the parameters and HELLO

	sc, _ := lst.Accept(ctx)
	_ = sc.Admitted().(*seal.Peer).Value // "client 1"
}
