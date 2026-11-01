package zmux

import (
	"context"
	"testing"
	"time"
)

// panicChan is a chanConn whose channel size query panics once armed,
// under the session lock in the driver's loops.
type panicChan struct {
	*chanConn
	armed chan struct{}
}

func (c *panicChan) MaxChannelDatagram() int {
	select {
	case <-c.armed:
		panic("test: channel size")
	default:
		return c.chanConn.MaxChannelDatagram()
	}
}

// TestDriverPanicClosesSession: a panic under the session lock in a
// driver loop closes the session instead of leaving the lock held.
func TestDriverPanicClosesSession(t *testing.T) {
	a, b := chanPair(t, 1200)
	pa := &panicChan{chanConn: a, armed: make(chan struct{})}
	c, s := pair(t, pa, b, dgramCfg(), dgramCfg())
	if got := exchangeDatagrams(t, c, s, 1, 100); got != 1 {
		t.Fatalf("%d of 1 datagram", got)
	}
	close(pa.armed)
	_ = c.SendDatagram([]byte("boom"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = c.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("the session did not close after the panic")
	}
	_ = s.Close()
}
