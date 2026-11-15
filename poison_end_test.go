package zmux

import (
	"net"
	"testing"
)

// TestPoisonedSessionEnds: a panic under the lock ends the session even
// when the core did not reach CLOSED (its FailInternal panicked in turn):
// done is published and Wait returns.
func TestPoisonedSessionEnds(t *testing.T) {
	a, b := tcpPair(t)
	c, _ := pair(t, a, b, nil, nil)
	func() {
		defer func() { _ = recover() }()
		c.locked(func() {
			c.poisonSt = poisonTorn // as if FailInternal had panicked: the core is not closed
			panic("torn")
		})
	}()
	waitDone(t, c)
	if c.Wait() == nil {
		t.Fatal("poisoned session ended without an error")
	}
}

type panicCloseConn struct{ net.Conn }

func (p panicCloseConn) Close() error { _ = p.Conn.Close(); panic("carrier close") }

// TestCarrierPanicStillEnds: a carrier whose Close panics while the session
// ends does not keep done from being published.
func TestCarrierPanicStillEnds(t *testing.T) {
	a, b := tcpPair(t)
	c, _ := pair(t, panicCloseConn{a}, b, nil, nil)
	c.Abort(1, "x")
	waitDone(t, c)
}
