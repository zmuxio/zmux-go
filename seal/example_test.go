package seal_test

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"fmt"
	"net"
	"net/netip"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
)

type oneToken struct{ handle, psk []byte }

func (g oneToken) Resolve(context.Context, []byte, netip.AddrPort) ([]byte, error) {
	return g.psk, nil
}

func (g oneToken) Credential(context.Context, seal.CredentialRequest) (*seal.Credential, error) {
	return nil, nil
}

func (g oneToken) Commit(context.Context, seal.Admission) (any, error) {
	return "client 1", nil
}

// A server that takes only Sealed carriers, and a client that pins its key.
func Example() {
	ctx := context.Background()
	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	handle, psk, _ := seal.DeriveHandlePSK(secret, []byte("example"), nil)
	node, _ := ecdh.X25519().GenerateKey(rand.Reader)

	server := &seal.ServerConfig{Gate: oneToken{handle, psk}, Key: node}
	lst, err := zmux.NewListener(&zmux.ListenerConfig{WrapPipe: server})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = lst.Close() }()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = lst.Serve(ctx, ln) }()

	nc, _ := net.Dial("tcp", ln.Addr().String())
	p, err := seal.Establish(ctx, zmux.NetPipe(nc), &seal.ClientConfig{
		Anchor: seal.PinAnchor(seal.NodeKeyID(node.PublicKey().Bytes())),
		Handle: handle,
		PSK:    psk,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	conn, err := zmux.Start(ctx, p, zmux.RoleClient, &zmux.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = conn.Close() }()

	sc, _ := lst.Accept(ctx)
	fmt.Println(sc.Admitted().(*seal.Peer).Value)
	// Output: client 1
}
