package seal_test

import (
	"context"
	"crypto/ecdh"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
)

// Example_readme is the Go code of the root README.md "Protected carriers",
// kept here so that it compiles against the current API. It only compiles
// (no Output); Example runs a complete exchange.
func Example_readme() {
	_ = func(ctx context.Context, gate seal.Gate, nodeKey *ecdh.PrivateKey, nodePub []byte,
		carrier zmux.PipeIO, handle, psk []byte, cfg *zmux.Config) (*zmux.Conn, error) {
		server := &seal.ServerConfig{Gate: gate, Key: nodeKey}
		l, err := zmux.NewListener(&zmux.ListenerConfig{WrapPipe: server}) // validates server
		if err != nil {
			return nil, err
		}
		defer func() { _ = l.Close() }()

		ctx, cancel := context.WithTimeout(ctx, 10*time.Second) // a refused handshake gets no reply
		defer cancel()
		p, err := seal.Establish(ctx, carrier, &seal.ClientConfig{
			Anchor: seal.PinAnchor(seal.NodeKeyID(nodePub)),
			Handle: handle,
			PSK:    psk,
		})
		if err != nil {
			return nil, err
		}
		conn, err := zmux.Start(ctx, p, zmux.RoleClient, cfg)
		return conn, err
	}
}
