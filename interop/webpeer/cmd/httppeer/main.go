// Command httppeer is a scripted HTTP streaming peer:
// `server|client|bad KIND ADDR`, KIND stream, split, stream2 or split2
// (2: HTTP/2, split uses TLS). Authentication is prefix authentication "right".
//
// KIND tls1 and tls2 are bidirectional carriers over TLS 1.3 (HTTP/1.1 and
// HTTP/2), with multipath and no admission: the server serves both on a
// self-signed certificate, WebSocket upgrades too (HTTP/1.1 and HTTP/2
// extended CONNECT); the client skips verification, pings, joins a second
// carrier (Bound, else Sealed: MODE), pings again and prints OK ping.
// KIND behind is such a server in cleartext (HTTP/1.1 and h2c), for a front
// that terminates TLS. The server prints MODE for each joined pipe after a
// stream. WebSocket over HTTP/2 needs GODEBUG=http2xconnect=1 in the
// server's environment.
// The server prints `READY <bound address>` (port 0 picks a free port), prints FALLBACK for a refused request (answered
// 418) and SESSION for an echoed stream; the client prints OK ping; bad
// (key "wrong") prints REFUSED with the error.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/interop/webpeer"
	"github.com/zmuxio/zmux-go/zmuxhttp"
	"github.com/zmuxio/zmux-go/zmuxws"
)

func main() {
	if len(os.Args) != 4 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: httppeer server|client|bad KIND ADDR")
		os.Exit(2)
	}
	mode, kind, addr := os.Args[1], os.Args[2], os.Args[3]
	if kind == "tls1" || kind == "tls2" || kind == "behind" {
		mp(mode, kind, addr)
		return
	}
	h2 := strings.HasSuffix(kind, "2")
	split := strings.HasPrefix(kind, "split")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var p http.Protocols
	p.SetHTTP1(!h2)
	p.SetUnencryptedHTTP2(h2 && !split)
	p.SetHTTP2(h2 && split)
	switch mode {
	case "server":
		l, err := peer.NewListener("right")
		check(err)
		h, err := zmuxhttp.NewHandler(l, &zmuxhttp.Options{Next: peer.Teapot})
		check(err)
		ln, err := net.Listen("tcp", addr)
		check(err)
		go peer.Serve(ctx, l)
		peer.Ready(os.Stdout, ln)
		srv := &http.Server{Handler: h, Protocols: &p}
		if split {
			tlsConfig, err := peer.SelfSignedTLS()
			check(err)
			srv.TLSConfig = tlsConfig
			check(srv.ServeTLS(ln, "", ""))
		} else {
			if h2 {
				p.SetHTTP1(true) // h2c with prior knowledge still shares the listener
			}
			check(srv.Serve(ln))
		}
	case "client", "bad":
		key := "right"
		if mode == "bad" {
			key = "wrong"
		}
		transport := &http.Transport{Protocols: &p}
		url := "http://" + addr + "/zmux"
		if split {
			transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // ephemeral interop fixture
			url = "https://" + addr + "/zmux"
		}
		client := &http.Client{Transport: transport}
		var cand zmux.Candidate
		if split {
			cand = zmuxhttp.Split{Client: client, URL: url}.Candidate()
		} else {
			cand = zmuxhttp.Streaming{Client: client, URL: url}.Candidate()
		}
		auth, err := peer.Auth(key)
		check(err)
		pipes, err := cand.Open(ctx, zmux.OpenRequest{Credential: auth, Stage: zmux.OpenBootstrap, Need: zmux.Both, Lifetime: ctx})
		var c *zmux.Conn
		if err == nil {
			c, err = zmux.Start(ctx, pipes[0], zmux.RoleClient, &zmux.Config{})
		}
		if err == nil {
			err = peer.Ping(ctx, c)
			_ = c.Close()
		}
		refuse(err)
	default:
		check(fmt.Errorf("unknown mode %q", mode))
	}
}

func mp(mode, kind, addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	switch mode {
	case "server":
		l, err := zmux.NewListener(&zmux.ListenerConfig{Config: peer.MPConfig()})
		check(err)
		hh, err := zmuxhttp.NewHandler(l, nil)
		check(err)
		wh, err := zmuxws.NewHandler(l, nil)
		check(err)
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodConnect || strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
				wh.ServeHTTP(w, r)
				return
			}
			hh.ServeHTTP(w, r)
		})
		ln, err := net.Listen("tcp", addr)
		check(err)
		go peer.ServeEcho(ctx, l)
		srv := &http.Server{Handler: h}
		if kind == "behind" {
			var p http.Protocols
			p.SetHTTP1(true)
			p.SetUnencryptedHTTP2(true)
			srv.Protocols = &p
			peer.Ready(os.Stdout, ln)
			check(srv.Serve(ln))
		}
		srv.TLSConfig, err = peer.SelfSignedTLS()
		check(err)
		peer.Ready(os.Stdout, ln)
		check(srv.ServeTLS(ln, "", ""))
	case "client":
		var p http.Protocols
		p.SetHTTP1(kind == "tls1")
		p.SetHTTP2(kind == "tls2")
		client := &http.Client{Transport: &http.Transport{Protocols: &p,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}}}
		s := zmuxhttp.Streaming{Client: client, URL: "https://" + addr + "/zmux"}
		dial := func() (zmux.PipeIO, error) { return s.Pipe(ctx, zmux.DirectionBidirectional) }
		pipe, err := dial()
		check(err)
		c, err := zmux.Start(ctx, pipe, zmux.RoleClient, peer.MPConfig())
		check(err)
		defer func() { _ = c.Close() }()
		check(peer.Ping(ctx, c))
		check(peer.Join(ctx, c, dial))
		check(peer.Ping(ctx, c))
	default:
		check(fmt.Errorf("unknown mode %q", mode))
	}
}

// refuse reports a refused client and exits, without running defers: the
// process ends.
func refuse(err error) {
	if err != nil {
		fmt.Printf("REFUSED %v\n", err)
		os.Exit(1)
	}
}

func check(err error) {
	if err != nil {
		fmt.Printf("ERROR %v\n", err)
		os.Exit(1)
	}
}
