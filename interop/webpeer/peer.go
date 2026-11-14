// Package peer is the shared part of the scripted WebSocket and HTTP
// streaming peers that the Java HttpInteropTest drives: a listener whose
// admission is prefix authentication, a fallback that answers 418, and a
// session that echoes one stream.
package peer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
)

// Auth is the prefix authentication of key: 30 s slots, no TLS binding; the listener accepts one slot of skew.
func Auth(key string) *admit.PrefixAuth {
	return &admit.PrefixAuth{Key: []byte(key), Slot: 30 * time.Second}
}

// NewListener returns a listener that admits carriers presenting Auth(key).
func NewListener(key string) (*zmux.Listener, error) {
	v, err := admit.NewPrefixAuthVerifier(Auth(key), admit.PrefixAuthVerifierOptions{Skew: 1})
	if err != nil {
		return nil, err
	}
	return zmux.NewListener(&zmux.ListenerConfig{Config: &zmux.Config{},
		Admission: &zmux.Admission{Peek: admit.PrefixTokenLen, PeekTimeout: 5 * time.Second, Decide: v.Decide, StallMax: time.Second}})
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
