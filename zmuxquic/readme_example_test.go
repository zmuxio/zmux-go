package zmuxquic_test

import (
	"context"
	"crypto/tls"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxquic"
)

// Example_readmeQUIC is the Go code of the root README.md "QUIC", kept here
// so that it compiles against the current API. It only compiles (no
// Output): it dials an example host.
func Example_readmeQUIC() {
	_ = func(ctx context.Context, tlsConf *tls.Config) (*zmux.Conn, error) {
		conn, err := zmuxquic.Dial(ctx, "example.com:4433", tlsConf, nil, nil)
		return conn, err
	}
}
