// Command httppeer is a scripted HTTP streaming peer:
// `server|client|bad KIND ADDR`, KIND stream, split, stream2 or split2
// (2: HTTP/2 cleartext). Authentication is prefix authentication "right".
// The server prints READY, prints FALLBACK for a refused request (answered
// 418) and SESSION for an echoed stream; the client prints OK ping; bad
// (key "wrong") prints REFUSED with the error.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/interop/webpeer"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: httppeer server|client|bad KIND ADDR")
		os.Exit(2)
	}
	mode, kind, addr := os.Args[1], os.Args[2], os.Args[3]
	h2 := strings.HasSuffix(kind, "2")
	split := strings.HasPrefix(kind, "split")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var p http.Protocols
	p.SetHTTP1(!h2)
	p.SetUnencryptedHTTP2(h2)
	switch mode {
	case "server":
		l, err := peer.NewListener("right")
		check(err)
		h, err := zmuxhttp.NewHandler(l, &zmuxhttp.Options{Next: peer.Teapot})
		check(err)
		ln, err := net.Listen("tcp", addr)
		check(err)
		go peer.Serve(ctx, l)
		fmt.Println("READY")
		srv := &http.Server{Handler: h, Protocols: &p}
		if h2 {
			p.SetHTTP1(true) // h2c with prior knowledge still shares the listener
		}
		check(srv.Serve(ln))
	case "client", "bad":
		key := "right"
		if mode == "bad" {
			key = "wrong"
		}
		client := &http.Client{Transport: &http.Transport{Protocols: &p}}
		url := "http://" + addr + "/zmux"
		var cand zmux.Candidate
		if split {
			cand = zmuxhttp.SplitCandidate(client, url, nil)
		} else {
			cand = zmuxhttp.StreamCandidate(client, url, nil)
		}
		pipe, err := cand.Dial(ctx, peer.Auth(key))
		var c *zmux.Conn
		if err == nil {
			c, err = zmux.Start(ctx, pipe, zmux.RoleClient, &zmux.Config{})
		}
		if err == nil {
			defer func() { _ = c.Close() }()
			err = peer.Ping(ctx, c)
		}
		if err != nil {
			fmt.Printf("REFUSED %v\n", err)
			os.Exit(1)
		}
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
