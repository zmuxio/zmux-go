// Package peer is the shared part of the scripted WebSocket and HTTP
// streaming peers that the Java HttpInteropTest drives: a listener whose
// admission is prefix authentication, a fallback that answers 418, and a
// session that echoes one stream.
package peer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
)

// Ready prints the ready line `READY <address>` with the address ln is bound
// to, so a caller that listens on port 0 learns the port it got.
func Ready(w io.Writer, ln net.Listener) {
	_, _ = fmt.Fprintf(w, "READY %s\n", ln.Addr())
}

const ticketSlot = 30 * time.Second

func fixtureTicket(key string) (admit.PrefixCredential, [32]byte) {
	id := sha256.Sum256([]byte("interop ticket id " + key))
	ticket := admit.PrefixCredential{ID: [16]byte(id[:16]), Key: sha256.Sum256([]byte("interop ticket key " + key))}
	return ticket, sha256.Sum256([]byte("interop outer key " + key))
}

// Auth is the scripted client's reusable unbound presentation.
func Auth(key string) (*admit.PrefixAuth, error) {
	ticket, outer := fixtureTicket(key)
	carrier := admit.PrefixCarrierOptions{AllowCleartext: true}
	return admit.NewPrefixAuth(admit.PrefixAuthOptions{PrefixCarrierOptions: carrier, Slot: ticketSlot}, ticket,
		admit.PrefixGeneration{Key: outer, LastSlot: math.MaxUint64})
}

// NewListener returns a listener that admits carriers presenting Auth(key).
func NewListener(key string) (*zmux.Listener, error) {
	ticket, outer := fixtureTicket(key)
	store, err := admit.NewMemoryPrefixStore(admit.PrefixWindow{Slot: ticketSlot, Skew: 1},
		[]admit.PrefixGeneration{{Key: outer, LastSlot: math.MaxUint64}}, 0)
	if err != nil {
		return nil, err
	}
	if err := store.AddTicket(admit.PrefixTicket{PrefixCredential: ticket, LastSlot: math.MaxUint64}); err != nil {
		return nil, err
	}
	carrier := admit.PrefixCarrierOptions{AllowCleartext: true}
	v, err := admit.NewPrefixAuthVerifier(admit.PrefixAuthVerifierOptions{
		PrefixCarrierOptions: carrier, Store: store, Reject: zmux.VerdictFallback,
	})
	if err != nil {
		return nil, err
	}
	a := v.Admission()
	a.PeekTimeout, a.StallMax = 5*time.Second, time.Second
	return zmux.NewListener(&zmux.ListenerConfig{Config: &zmux.Config{}, Admission: a})
}

// Teapot answers every request 418 and prints FALLBACK with its target.
var Teapot = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("FALLBACK %s\n", r.URL.RequestURI())
	w.WriteHeader(http.StatusTeapot)
	_, _ = fmt.Fprintf(w, "fallback %s", r.URL.RequestURI())
})

// Serve echoes one stream of every session l accepts until l closes, and
// prints SESSION with the stream's content.
func Serve(ctx context.Context, l *zmux.Listener) {
	for {
		c, err := l.Accept(ctx)
		if err != nil {
			return
		}
		go func() {
			defer func() { _ = c.Close() }()
			st, err := c.AcceptStream(ctx)
			if err != nil {
				return
			}
			b, err := io.ReadAll(st)
			if err != nil {
				return
			}
			_, _ = st.Write(b)
			_ = st.Close()
			fmt.Printf("SESSION %s\n", b)
			_ = c.Wait()
		}()
	}
}

// Ping sends "ping" on a stream of c and checks the echo.
func Ping(ctx context.Context, c *zmux.Conn) error {
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := st.Write([]byte("ping")); err != nil {
		return err
	}
	if err := st.Close(); err != nil {
		return err
	}
	b, err := io.ReadAll(st)
	if err != nil {
		return err
	}
	if string(b) != "ping" {
		return fmt.Errorf("echo %q", b)
	}
	fmt.Println("OK ping")
	return nil
}

// JoinPSK is the pre-shared join key of the TLS peers.
var JoinPSK = bytes.Repeat([]byte{7}, 32)

// MPConfig is the session configuration of the TLS peers: multipath with
// JoinPSK, and joins in Bound only on a carrier that declares the contract.
func MPConfig() *zmux.Config {
	return &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}}, PipeAuth: zmux.PipeAuth{PSK: JoinPSK, EndToEnd: true},
		AllowRecordMode: func(in zmux.RecordPolicyInput, m zmux.RecordMode, _ any) bool {
			return m == zmux.RecordSealed || m == zmux.RecordBound && in.Secure && in.HasExporter
		}}
}

// SelfSignedTLS is a TLS 1.3 server configuration with a fresh certificate
// for localhost and 127.0.0.1, offering h2 and http/1.1.
func SelfSignedTLS() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h2", "http/1.1"},
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, nil
}

// ServeEcho echoes every stream of every session l accepts until l closes.
// It prints SESSION with each stream's content, and MODE with the record
// mode of each pipe other than pipe 0 once that pipe has joined.
func ServeEcho(ctx context.Context, l *zmux.Listener) {
	serveEcho(ctx, l, os.Stdout)
}

func serveEcho(ctx context.Context, l *zmux.Listener, w io.Writer) {
	for {
		c, err := l.Accept(ctx)
		if err != nil {
			return
		}
		go reportJoins(ctx, c, w)
		go func() {
			defer func() { _ = c.Close() }()
			for {
				st, err := c.AcceptStream(ctx)
				if err != nil {
					return
				}
				b, err := io.ReadAll(st)
				if err != nil {
					return
				}
				_, _ = st.Write(b)
				_ = st.Close()
				_, _ = fmt.Fprintf(w, "SESSION %s\n", b)
			}
		}()
	}
}

// reportJoins prints MODE for every pipe other than pipe 0 as it joins c,
// until c ends. Reporting on the join event, not after a stream, covers a
// pipe that joins after the last stream.
func reportJoins(ctx context.Context, c *zmux.Conn, w io.Writer) {
	for {
		ev, err := c.NextEvent(ctx)
		if err != nil {
			return
		}
		if ev.Kind != zmux.EventPipeJoined || ev.PipeID == 0 {
			continue
		}
		for _, p := range c.Pipes() {
			if s, ok := p.Stats(); ok && p.ID() == ev.PipeID {
				_, _ = fmt.Fprintf(w, "MODE %v\n", s.RecordMode)
			}
		}
	}
}

// Join adds a pipe from dial to c, first in Bound and, when that fails, on a
// new carrier in Sealed. It prints MODE with the mode of the joined pipe.
func Join(ctx context.Context, c *zmux.Conn, dial func() (zmux.PipeIO, error)) error {
	for _, m := range []zmux.RecordMode{zmux.RecordBound, zmux.RecordSealed} {
		q, err := dial()
		if err != nil {
			return err
		}
		p, err := c.AddPipe(ctx, q, zmux.PipeOptions{RecordMode: zmux.Set(m)})
		if err != nil {
			fmt.Printf("JOIN %v refused: %v\n", m, err)
			continue
		}
		s, _ := p.Stats()
		fmt.Printf("MODE %v\n", s.RecordMode)
		return nil
	}
	return errors.New("no join")
}
