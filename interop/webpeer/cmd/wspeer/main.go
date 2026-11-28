// Command wspeer is a scripted WebSocket peer: `server|client|bad ADDR`.
// The server prints `READY <bound address>` (port 0 picks a free port), serves wss://ADDR/ with prefix authentication
// "ws-interop" in the upgrade's header field, answers anything else 418
// and prints FALLBACK; the client prints OK ping, bad (no credential)
// prints REFUSED with the error.
package main

import (
	"context"
	"crypto/tls"
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
		peer.Ready(os.Stdout, ln)
		tlsConfig, err := peer.SelfSignedTLS()
		check(err)
		check((&http.Server{Handler: h, TLSConfig: tlsConfig}).ServeTLS(ln, "", ""))
	case "client", "bad":
		cfg := &zmux.Config{}
		if mode == "client" {
			auth, err := peer.Auth("ws-interop")
			check(err)
			cfg.CarrierCredential = auth
		}
		o := &zmuxws.DialOptions{TLSConfig: &tls.Config{InsecureSkipVerify: true}} // ephemeral interop fixture
		c, err := zmuxws.Dial(ctx, "wss://"+addr+"/zmux", o, cfg)
		if err != nil {
			if mode == "bad" {
				fmt.Printf("REFUSED %v\n", err)
				return
			}
			exit1(fmt.Sprintf("REFUSED %v", err))
		}
		if mode == "bad" {
			_ = c.Close()
			exit1("ACCEPTED")
		}
		err = peer.Ping(ctx, c)
		_ = c.Close()
		check(err)
	default:
		check(fmt.Errorf("unknown mode %q", mode))
	}
}

// exit1 prints line and exits with status 1, without running defers: the
// process ends.
func exit1(line string) {
	fmt.Println(line)
	os.Exit(1)
}

func check(err error) {
	if err != nil {
		fmt.Printf("ERROR %v\n", err)
		os.Exit(1)
	}
}
