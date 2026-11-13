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

// Example_readmeProxy is README.md "Behind a reverse proxy or CDN".
func Example_readmeProxy() {
	_ = func(l *zmux.Listener, site http.Handler, trusted *zmux.TrustedProxies, psk []byte) error {
		o := &zmuxhttp.Options{Next: site, Trusted: trusted}
		stream, err := zmuxhttp.NewHandler(l, o)
		if err != nil {
			return err
		}
		ex, err := zmuxhttp.NewExchangeHandler(l, o, &zmuxhttp.ExchangeConfig{PSK: psk})
		if err != nil {
			return err
		}
		ws, err := zmuxws.NewHandler(l, o)
		if err != nil {
			return err
		}
		http.Handle("/zmux", &zmuxhttp.Endpoint{Options: o, Stream: stream, Exchange: ex, WebSocket: ws})
		return nil
	}
	_ = func(ctx context.Context, client *http.Client, psk []byte, cfg *zmux.Config,
		cache *zmux.CarrierCache, network string) (*zmux.Conn, error) {
		conn, err := zmux.DialChoice(ctx, zmux.AutoChoice(
			zmuxws.Candidate("wss://example.com/zmux", nil),
			zmuxhttp.StreamCandidate(client, "https://example.com/zmux", nil),
			zmuxhttp.SplitCandidate(client, "https://example.com/zmux", nil),
			zmuxhttp.ExchangeCandidate(client, "https://example.com/zmux", &zmuxhttp.ExchangeConfig{PSK: psk}),
		), cfg, &zmux.CarrierChoicePolicy{Cache: cache, Key: "example.com|" + network})
		return conn, err
	}
}
