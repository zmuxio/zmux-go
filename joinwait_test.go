package zmux

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// A join waiting for establishment or the join keys holds a pending slot
// of the listener: its wait is bounded even without a handshake timeout.
func TestJoinWaitTimeoutBounded(t *testing.T) {
	def := time.Duration(core.DefaultHandshakeTimeout)
	for _, c := range []struct {
		hto, want time.Duration
	}{{0, def}, {-1, def}, {time.Second, time.Second}} {
		if got := (&Config{HandshakeTimeout: c.hto}).joinWaitTimeout(); got != c.want {
			t.Errorf("HandshakeTimeout %v: %v, want %v", c.hto, got, c.want)
		}
	}
}
