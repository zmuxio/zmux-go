package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestHTTPCarriers(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig()})
	srv := httptest.NewServer(lst)
	defer srv.Close()
	defer lst.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, mode := range []string{"duplex", "split"} {
		t.Run(mode, func(t *testing.T) {
			var p PipeIO
			var err error
			if mode == "duplex" {
				p, err = HTTPPipe(ctx, srv.Client(), srv.URL, nil, Bidirectional)
			} else {
				p, err = HTTPSplitPipe(ctx, srv.Client(), srv.URL, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			c, err := Start(ctx, p, RoleClient, mpConfig())
			if err != nil {
				t.Fatal(err)
			}
			s, err := lst.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				c.Close()
				s.Close()
				waitDone(t, c)
				waitDone(t, s)
			}()
			go func() {
				st, err := s.AcceptStream(ctx)
				if err == nil {
					io.Copy(st, st)
					st.Close()
				}
			}()
			st, err := c.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			msg := make([]byte, 256<<10)
			rand.Read(msg)
			go func() { st.Write(msg); st.Close() }()
			got, err := io.ReadAll(st)
			if err != nil || !bytes.Equal(got, msg) {
				t.Fatalf("echo over HTTP %s: %d bytes, %v", mode, len(got), err)
			}
			// A downlink joined out of band.
			if mode == "duplex" {
				r, err := c.ReservePipe(ReceiveOnly, PipeOptions{})
				if err != nil {
					t.Fatal(err)
				}
				h := map[string][]string{HeaderJoin: {r.Header()}}
				dp, err := HTTPPipe(ctx, srv.Client(), srv.URL, h, ReceiveOnly)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := r.Attach(ctx, dp); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
