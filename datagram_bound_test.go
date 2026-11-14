package zmux

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestDatagramAtMaxPayload: over TCP and TLS, a session and a stream
// datagram of exactly MaxDatagramPayload are delivered, one byte more is
// refused locally, and the session keeps delivering after it.
func TestDatagramAtMaxPayload(t *testing.T) {
	for _, carrier := range []string{"tcp", "tls"} {
		t.Run(carrier, func(t *testing.T) {
			leakcheck.Check(t)
			a, b := tcpPair(t)
			if carrier == "tls" {
				srv, cli := selfSignedTLS(t)
				a, b = tls.Client(a, cli), tls.Server(b, srv)
			}
			cfg := &Config{EnableDatagrams: true}
			client, server := pair(t, a, b, cfg, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cs, ss := openPackets(t, ctx, client, server)
			m := cs.MaxDatagramPayload(0)
			if carrier == "tls" && m >= DefaultMaxRecordSize {
				t.Fatalf("TLS stream bound %d", m)
			}
			var tl *DatagramTooLargeError
			if err := cs.SendDatagram(0, make([]byte, m+1)); !errors.As(err, &tl) || tl.MaxDatagramPayloadSize != int64(m) {
				t.Fatalf("one byte above: %v", err)
			}
			for _, n := range []int{m, 1} {
				if err := cs.SendDatagram(0, make([]byte, n)); err != nil {
					t.Fatal(err)
				}
				if _, p, err := ss.ReceiveDatagram(ctx); err != nil || len(p) != n {
					t.Fatalf("stream datagram of %d: %d bytes, %v", n, len(p), err)
				}
			}
			sm := client.ConnectionState().MaxDatagramPayload
			if err := client.SendDatagram(make([]byte, sm+1)); !errors.As(err, &tl) {
				t.Fatalf("session datagram one byte above: %v", err)
			}
			if err := client.SendDatagram(make([]byte, sm)); err != nil {
				t.Fatal(err)
			}
			if p, err := server.ReceiveDatagram(ctx); err != nil || len(p) != sm {
				t.Fatalf("session datagram of %d: %d bytes, %v", sm, len(p), err)
			}
			_ = cs.Close()
			_ = ss.Close()
		})
	}
}
