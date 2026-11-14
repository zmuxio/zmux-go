// Command tlsfront is a reverse proxy that terminates TLS: `tlsfront ADDR
// UPSTREAM` serves HTTP/1.1 and HTTP/2 over TLS 1.3 on ADDR (self-signed)
// and forwards every request to http://UPSTREAM over h2c, streaming both
// bodies. It prints READY.
package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	"github.com/zmuxio/zmux-go/interop/webpeer"
)

func main() {
	if len(os.Args) != 3 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: tlsfront ADDR UPSTREAM")
		os.Exit(2)
	}
	var up http.Protocols
	up.SetUnencryptedHTTP2(true)
	target := &url.URL{Scheme: "http", Host: os.Args[2]}
	rp := &httputil.ReverseProxy{
		Rewrite:       func(r *httputil.ProxyRequest) { r.SetURL(target); r.SetXForwarded() },
		Transport:     &http.Transport{Protocols: &up},
		FlushInterval: -1,
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// An HTTP/1.1 request body keeps flowing after the response starts.
		_ = http.NewResponseController(w).EnableFullDuplex()
		rp.ServeHTTP(w, r)
	})
	cfg, err := peer.SelfSignedTLS()
	check(err)
	ln, err := net.Listen("tcp", os.Args[1])
	check(err)
	fmt.Println("READY")
	check((&http.Server{Handler: h, TLSConfig: cfg}).ServeTLS(ln, "", ""))
}

func check(err error) {
	if err != nil {
		fmt.Printf("ERROR %v\n", err)
		os.Exit(1)
	}
}
