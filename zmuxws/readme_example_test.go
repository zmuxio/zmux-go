package zmuxws_test

import (
	"context"
	"net/http"

	"github.com/zmuxio/zmux-go"
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
		h, err := zmuxws.Handler(l, &zmux.HTTPOptions{Next: site})
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
		stream, err := zmux.NewHTTPHandler(l, &zmux.HTTPOptions{Next: site, Trusted: trusted})
		if err != nil {
			return err
		}
		ex, err := zmux.NewHTTPExchangeHandler(l, &zmux.HTTPExchangeConfig{PSK: psk, Next: site, Trusted: trusted})
		if err != nil {
			return err
		}
		ws, err := zmuxws.Handler(l, &zmux.HTTPOptions{Next: site, Trusted: trusted})
		if err != nil {
			return err
		}
		http.Handle("/zmux", &zmux.HTTPEndpoint{Stream: stream, Exchange: ex, WebSocket: ws, Next: site})
		return nil
	}
	_ = func(ctx context.Context, client *http.Client, psk []byte, cfg *zmux.Config,
		cache *zmux.CarrierCache, network string) (*zmux.Conn, error) {
		conn, err := zmux.DialMode(ctx, zmux.AutoMode(
			zmuxws.Candidate("wss://example.com/zmux", nil),
			zmux.StreamingCandidate(client, "https://example.com/zmux", nil),
			zmux.SplitCandidate(client, "https://example.com/zmux", nil),
			zmux.ExchangeCandidate(client, "https://example.com/zmux", &zmux.HTTPExchangeConfig{PSK: psk}),
		), cfg, &zmux.ModePolicy{Cache: cache, Key: "example.com|" + network})
		return conn, err
	}
}
