package zmux

import (
	"context"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

// A failed close write on a joined pipe must not keep the session in
// CLOSING. The peer still receives the close on the healthy pipe.
func TestCloseWhileJoinedPipeFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		nc, err := e.dial(ctx)
		if err != nil {
			t.Fatal(err)
		}
		carrier := &failCloseWrite{Conn: nc}
		if _, err := e.client.AddPipe(ctx, NetPipe(carrier), PipeOptions{SendStandby: Set(true)}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		carrier.armed = true
		_ = e.client.CloseWithError(7, "bye")
		synctest.Wait()
		if !carrier.failed {
			t.Fatal("the joined carrier did not attempt its close write")
		}
		waitTestSignal(t, e.client.Done(), "client session end")
		waitTestSignal(t, e.server.Done(), "server session end")
		wantAppClose(t, "the peer", e.server.Wait(), 7, "bye", true)
	})
}

type failCloseWrite struct {
	net.Conn
	armed, failed bool // accessed between synctest quiescence points
}

func (c *failCloseWrite) Write(b []byte) (int, error) {
	if c.armed {
		c.failed = true
		return 0, net.ErrClosed
	}
	return c.Conn.Write(b)
}
