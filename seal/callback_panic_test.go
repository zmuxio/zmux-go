package seal_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
)

// TestHostCallbackPanicRejects: a panic in a host callback of either end
// (here the AppData sources) fails that handshake with an error, the
// server's as a gate_error rejection, and ends nothing else.
func TestHostCallbackPanicRejects(t *testing.T) {
	for _, side := range []string{"server", "client"} {
		t.Run(side, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			f := newFixture(t)
			boom := func(context.Context, seal.AppDataRequest) ([]byte, error) { panic("host bug") }
			if side == "server" {
				f.server.AppData = boom
			} else {
				f.client.AppData = boom
			}
			a, b := net.Pipe()
			_, _, cerr, serr := establishPair(t, ctx, &f.client, &f.server, zmux.NetPipe(a), zmux.NetPipe(b))
			if side == "server" {
				var rej *zmux.RejectError
				if !errors.As(serr, &rej) || rej.Reason != "gate_error" {
					t.Fatalf("server: %v", serr)
				}
			} else if cerr == nil {
				t.Fatal("client handshake with a panicking source succeeded")
			}
		})
	}
}
