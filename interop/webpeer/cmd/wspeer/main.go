// Command wspeer is a scripted WebSocket peer: `server|client|bad ADDR`.
// The server prints READY, serves ws://ADDR/ with prefix authentication
// "ws-interop" in the upgrade's header field, answers anything else 418
// and prints FALLBACK; the client prints OK ping, bad (no credential)
// prints REFUSED with the error.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/interop/webpeer"
	"github.com/zmuxio/zmux-go/zmuxhttp"
	"github.com/zmuxio/zmux-go/zmuxws"
)

func main() {
	if len(os.Args) != 3 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: wspeer server|client|bad ADDR")
		os.Exit(2)
	}
	mode, addr := os.Args[1], os.Args[2]
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	switch mode {
	case "server":
		l, err := peer.NewListener("ws-interop")
		check(err)
		h, err := zmuxws.NewHandler(l, &zmuxhttp.Options{Next: peer.Teapot})
		check(err)
		ln, err := net.Listen("tcp", addr)
		check(err)
		go peer.Serve(ctx, l)
		fmt.Println("READY")
		check((&http.Server{Handler: h}).Serve(ln))
	case "client", "bad":
		cfg := &zmux.Config{}
		if mode == "client" {
			cfg.CarrierCredential = peer.Auth("ws-interop")
		}
		c, err := zmuxws.Dial(ctx, "ws://"+addr+"/zmux", nil, cfg)
		if err != nil {
			fmt.Printf("REFUSED %v\n", err)
			if mode == "bad" {
				return
			}
			os.Exit(1)
		}
		defer func() { _ = c.Close() }()
		if mode == "bad" {
			fmt.Println("ACCEPTED")
			os.Exit(1)
		}
		check(peer.Ping(ctx, c))
	default:
		check(fmt.Errorf("unknown mode %q", mode))
	}
}

func check(err error) {
	if err != nil {
		fmt.Printf("ERROR %v\n", err)
		os.Exit(1)
	}
}
