package zmux_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"math"
	"net"
	"net/http"

	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/zmuxhttp"
)

// The examples in this file hold the Go code of README.md that uses this
// module alone, line for line, so that it keeps compiling against the
// current API. They only compile (no Output): they dial example hosts.
// The sub-modules hold the README code that uses them.

// Example_readmeClient is README.md "Client".
func Example_readmeClient() {
	_ = func(ctx context.Context) ([]byte, error) {
		conn, err := zmux.Dial(ctx, "tcp", "example.com:4433", nil)
		// or zmux.DialTLS(ctx, "tcp", "example.com:4433", tlsConf, nil)
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
		cfg := &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}}, PipeAuth: zmux.PipeAuth{PSK: psk, EndToEnd: true}}
		conn, err := zmux.Dial(ctx, "tcp", primaryAddr, cfg)
		if err != nil {
			return err
		}
		nc, err := net.Dial("tcp", backupAddr)
		if err != nil {
			return err
		}
		p, err := conn.AddPipe(ctx, zmux.NetPipe(nc), zmux.PipeOptions{Label: "backup", SendStandby: zmux.Set(true)})
		if err != nil {
			return err
		}
		for {
			ev, err := conn.NextEvent(ctx)
			if err != nil {
				return err
			}
			if ev.Kind == zmux.EventPipeFailed && ev.PipeID == p.ID() {
				log.Println("backup path failed")
			}
		}
	}
}

// Example_readmePipeUpdate is README.md "Multipath" (Pipe.Update).
func Example_readmePipeUpdate() {
	_ = func(p *zmux.Pipe) error {
		err := p.Update(zmux.PipeUpdate{SendStandby: zmux.Set(false), Weight: zmux.Clear[uint64]()})
		return err
	}
}

// Example_readmeHTTPExchange is README.md "HTTP exchanges".
func Example_readmeHTTPExchange() {
	_ = func(l *zmux.Listener, psk []byte) error {
		h, err := zmuxhttp.NewExchangeHandler(l, nil, zmuxhttp.ExchangeConfig{PSK: psk})
		if err != nil {
			return err
		}
		defer func() { _ = h.Close() }()
		http.Handle("/x/", h)
		return nil
	}
	_ = func(ctx context.Context, client *http.Client, psk []byte, cfg *zmux.Config) (*zmux.Conn, error) {
		p, err := zmuxhttp.Exchange{Client: client, URL: "https://example.com/x/{instance}/{nonce}",
			Config: zmuxhttp.ExchangeConfig{PSK: psk}}.Pipe(ctx)
		if err != nil {
			return nil, err
		}
		conn, err := zmux.Start(ctx, p, zmux.RoleClient, cfg) // or conn.AddPipe(ctx, p, opts)
		return conn, err
	}
}

// Example_readmeAdmission is README.md "Admission".
func Example_readmeAdmission() {
	_ = func(ctx context.Context, ticket admit.PrefixCredential, outer [32]byte, ticketEnd uint64, cfg *zmux.Config,
		ln net.Listener, tlsConf *tls.Config, addr string) (*zmux.Conn, error) {
		window := admit.PrefixWindow{Slot: 30 * time.Second, Skew: 1}
		generations := []admit.PrefixGeneration{{Key: outer, LastSlot: math.MaxUint64}}
		store, err := admit.NewMemoryPrefixStore(window, generations, 0)
		if err != nil {
			return nil, err
		}
		if err := store.AddTicket(admit.PrefixTicket{PrefixCredential: ticket, LastSlot: ticketEnd, SingleUse: true}); err != nil {
			return nil, err
		}
		carrier := admit.PrefixCarrierOptions{BindTLS: true}
		v, err := admit.NewPrefixAuthVerifier(admit.PrefixAuthVerifierOptions{PrefixCarrierOptions: carrier, Store: store})
		if err != nil {
			return nil, err
		}
		l, err := zmux.NewListener(&zmux.ListenerConfig{Config: cfg, Admission: v.Admission()})
		if err != nil {
			return nil, err
		}
		go func() { _ = l.Serve(ctx, tls.NewListener(ln, tlsConf)) }()

		// Client configuration uses the ticket and outer key provisioned for its entrance.
		auth, err := admit.NewPrefixAuth(admit.PrefixAuthOptions{PrefixCarrierOptions: carrier, Slot: window.Slot}, ticket, generations...)
		if err != nil {
			return nil, err
		}
		conn, err := zmux.DialTLS(ctx, "tcp", addr, tlsConf, &zmux.Config{CarrierCredential: auth})
		return conn, err
	}
}
