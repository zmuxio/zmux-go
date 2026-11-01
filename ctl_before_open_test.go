package zmux

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// Control frames of a local stream queued before its opening STREAM
// frame (STOP_SENDING after CancelRead, MAX_STREAM_DATA after
// SetReceiveWindow) wait for it and carry the stream's own ID: stream 0
// stays untouched and the session lives, for either opener.
func TestStreamCtlBeforeOpen(t *testing.T) {
	for _, opener := range []string{"responder", "initiator"} {
		for _, op := range []string{"CancelRead", "SetReceiveWindow"} {
			t.Run(opener+"/"+op, func(t *testing.T) {
				leakcheck.Check(t)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				a, b := net.Pipe()
				c, s := pair(t, a, b, nil, nil)
				// Stream 0, the initiator's first.
				cs, err := c.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := cs.Write([]byte("hello")); err != nil {
					t.Fatal(err)
				}
				ss, err := s.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				buf := make([]byte, 5)
				if _, err := io.ReadFull(ss, buf); err != nil {
					t.Fatal(err)
				}
				o, peer := s, c
				if opener == "initiator" {
					o, peer = c, s
				}
				ns, err := o.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				if op == "CancelRead" {
					ns.CancelRead(7)
				} else {
					ns.SetReceiveWindow(1<<20, 1<<20)
				}
				if _, err := ns.Write([]byte("grant")); err != nil {
					t.Fatal(err)
				}
				_ = ns.Close()
				ps, err := peer.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if ps.StreamID() == 0 || ps.StreamID() != ns.StreamID() {
					t.Fatalf("peer stream %d, opener's %d", ps.StreamID(), ns.StreamID())
				}
				if got, err := io.ReadAll(ps); err != nil || string(got) != "grant" {
					t.Fatalf("read %q, %v", got, err)
				}
				if op == "CancelRead" {
					// The STOP_SENDING reached this stream: its writer
					// learns it.
					waitUntil(t, 5*time.Second, "STOP_SENDING", func() bool {
						_, err := ps.Write([]byte("x"))
						return err != nil
					})
				}
				// Stream 0 works both ways.
				if _, err := cs.Write([]byte("again")); err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadFull(ss, buf); err != nil || string(buf) != "again" {
					t.Fatalf("stream 0 read %q, %v", buf, err)
				}
				if _, err := ss.Write([]byte("reply")); err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadFull(cs, buf); err != nil || string(buf) != "reply" {
					t.Fatalf("stream 0 reply %q, %v", buf, err)
				}
				for _, x := range []*Conn{c, s} {
					select {
					case <-x.Context().Done():
						t.Fatalf("session ended: %v", context.Cause(x.Context()))
					default:
					}
				}
			})
		}
	}
}
