package zmuxws_test

import (
	"context"
	"net/http"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxhttp"
	"github.com/zmuxio/zmux-go/zmuxws"
)

// The examples in this file hold the Go code of the root README.md that
// uses this module, line for line, so that it keeps compiling against the
// current API. They only compile (no Output): they dial example hosts.

// Example_readmeWebSocket is README.md "WebSocket".
func Example_readmeWebSocket() {
	_ = func(ctx context.Context, l *zmux.Listener, site http.Handler) (*zmux.Conn, error) {
		// server: upgrades go to l; other requests, and those the admission
		// does not accept, to Next
		h, err := zmuxws.NewHandler(l, &zmuxhttp.Options{Next: site})
		if err != nil {
			return nil, err
		}
		http.Handle("/zmux", h)

		// client
		conn, err := zmuxws.Dial(ctx, "wss://example.com/zmux", nil, nil)
		return conn, err
	}
}
