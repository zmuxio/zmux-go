package zmux_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"time"

	"github.com/zmuxio/zmux-go"
)

// The examples in this file hold the Go code of README.md that uses this
// module alone, line for line, so that it keeps compiling against the
// current API. They only compile (no Output): they dial example hosts.
// The sub-modules hold the README code that uses them.

// Example_readmeClient is README.md "Client".
func Example_readmeClient() {
	_ = func(ctx context.Context) ([]byte, error) {
		conn, err := zmux.Dial(ctx, "tcp", "example.com:4433", nil)
		// or: zmux.DialTLS(ctx, "tcp", "example.com:4433", tlsConf, nil)
		if err != nil {
			return nil, err
		}
		defer func() { _ = conn.Close() }()

		st, err := conn.OpenStreamSync(ctx)
		if err != nil {
			return nil, err
		}
		if _, err := st.Write([]byte("hello")); err != nil {
			return nil, err
		}
		if err := st.Close(); err != nil { // FIN: the write side is done
			return nil, err
		}
		reply, err := io.ReadAll(st)
		return reply, err
	}
}

// Example_readmeServer is README.md "Server".
func Example_readmeServer() {
	_ = func(ctx context.Context, cfg *zmux.Config) error {
		ln, err := net.Listen("tcp", ":4433")
		if err != nil {
			return err
		}
		l, err := zmux.NewListener(&zmux.ListenerConfig{Config: cfg})
		if err != nil {
			return err // the whole configuration is checked here
		}
		defer func() { _ = l.Close() }()
		go func() { _ = l.Serve(ctx, ln) }() // for TLS: l.Serve(ctx, tls.NewListener(ln, tlsConf))

		for {
			conn, err := l.Accept(ctx)
			if err != nil {
				return err
			}
			go func() {
				for {
					st, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func() {
						_, _ = io.Copy(st, st) // echo
						_ = st.Close()
					}()
				}
			}()
		}
	}
}

// Example_readmeClosing is README.md "Closing".
func Example_readmeClosing() {
	_ = func(conn *zmux.Conn) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := conn.Shutdown(ctx, 0, "")
		return err
	}
}

// Example_readmeErrors is README.md "Errors".
func Example_readmeErrors() {
	_ = func(st *zmux.Stream, buf []byte) {
		_, err := st.Read(buf)
		var app *zmux.ApplicationError
		switch {
		case zmux.IsRetryable(err):
			// the peer did not process the stream: send it again
		case errors.Is(err, zmux.ErrIdleTimeout):
			// the session timed out
		case errors.Is(err, zmux.ErrResumeTimeout), errors.Is(err, zmux.ErrPipesLost):
			// every pipe was lost (and not resumed in time); errors.Unwrap(err)
			// is the error of the last pipe lost
			log.Println("paths down:", errors.Unwrap(err))
		case errors.As(err, &app) && app.Remote:
			// the peer closed the session with app.ErrorCode
		case errors.Is(err, net.ErrClosed):
			// the session ended for another reason
		}
	}
}

// Example_readmeMultipath is README.md "Multipath".
func Example_readmeMultipath() {
	_ = func(ctx context.Context, psk []byte, primaryAddr, backupAddr string) error {
		cfg := &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: psk, EndToEnd: true}}
		conn, err := zmux.Dial(ctx, "tcp", primaryAddr, cfg)
		if err != nil {
			return err
		}
		nc, err := net.Dial("tcp", backupAddr)
		if err != nil {
			return err
		}
		p, err := conn.AddPipe(ctx, zmux.NetPipe(nc), zmux.PipeOptions{Label: "backup", SendStandby: new(true)})
		if err != nil {
			return err
		}
		for {
			ev, err := conn.NextEvent(ctx)
			if err != nil {
				return err
			}
			if ev.Kind == zmux.EventPipeFailed && ev.Pipe == p.ID() {
				log.Println("backup path failed")
			}
		}
	}
}

// Example_readmeRedial is README.md "Reconnecting after an outage".
func Example_readmeRedial() {
	_ = func(ctx context.Context, psk []byte, addr string) (*zmux.Conn, error) {
		cfg := &zmux.Config{
			ResumeTimeout: 30 * time.Second,
			PipeAuth:      zmux.PipeAuth{PSK: psk, EndToEnd: true},
			Redial: func(ctx context.Context, need zmux.Direction) (zmux.PipeIO, error) {
				var d net.Dialer
				nc, err := d.DialContext(ctx, "tcp", addr)
				if err != nil {
					return zmux.PipeIO{}, err
				}
				return zmux.NetPipe(nc), nil
			},
		}
		conn, err := zmux.Dial(ctx, "tcp", addr, cfg)
		if err != nil {
			return nil, err
		}
		// The server's Listener uses the same ResumeTimeout and PipeAuth. Streams
		// survive a carrier that fails and is redialled within 30 s.
		return conn, nil
	}
}

// Example_readmeHTTPExchange is README.md "HTTP exchanges".
func Example_readmeHTTPExchange() {
	_ = func(l *zmux.Listener, psk []byte) error {
		h, err := zmux.NewHTTPExchangeHandler(l, nil, &zmux.HTTPExchangeConfig{PSK: psk})
		if err != nil {
			return err
		}
		defer func() { _ = h.Close() }()
		http.Handle("/x/", h)
		return nil
	}
	_ = func(ctx context.Context, client *http.Client, psk []byte, cfg *zmux.Config) (*zmux.Conn, error) {
		p, err := zmux.HTTPExchangePipe(ctx, client, "https://example.com/x/{instance}/{nonce}",
			&zmux.HTTPExchangeConfig{PSK: psk})
		if err != nil {
			return nil, err
		}
		conn, err := zmux.Start(ctx, p, zmux.RoleClient, cfg) // or conn.AddPipe(ctx, p, opts)
		return conn, err
	}
}

// Example_readmeProxy is README.md "Behind a reverse proxy or CDN" (the
// parts without WebSocket; zmuxws holds the others).
func Example_readmeProxy() {
	trusted := &zmux.TrustedProxies{Prefixes: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")},
		Header: "X-Forwarded-For"}
	_ = trusted

	_ = func(endpoint http.Handler) *http.Server {
		srv := &http.Server{Addr: "127.0.0.1:8080", Handler: endpoint, Protocols: new(http.Protocols)}
		srv.Protocols.SetHTTP1(true)
		srv.Protocols.SetUnencryptedHTTP2(true)
		return srv
	}
}

// Example_readmeAdmission is README.md "Admission".
func Example_readmeAdmission() {
	_ = func(ctx context.Context, key []byte, cfg *zmux.Config, fallback func(context.Context, net.Conn),
		ln net.Listener, tlsConf *tls.Config, addr string) (*zmux.Conn, error) {
		auth := &zmux.PrefixAuth{Key: key, Slot: 30 * time.Second, Skew: 1, BindTLS: true}
		v, err := zmux.NewPrefixAuthVerifier(auth)
		if err != nil {
			return nil, err
		}
		l, err := zmux.NewListener(&zmux.ListenerConfig{Config: cfg,
			Admission: &zmux.Admission{Peek: zmux.PrefixTokenLen, Decide: v.Decide}, Fallback: fallback})
		if err != nil {
			return nil, err
		}
		go func() { _ = l.Serve(ctx, tls.NewListener(ln, tlsConf)) }()

		// client: Config.Credential is presented by the dialers
		conn, err := zmux.DialTLS(ctx, "tcp", addr, tlsConf, &zmux.Config{Credential: auth})
		return conn, err
	}
}
