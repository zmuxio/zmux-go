package zmux

import (
	"errors"
	"testing"
	"time"
)

// The join timeout is its own setting: the handshake timeout neither
// bounds nor defaults it, and it cannot be no timeout.
func TestJoinTimeoutIndependent(t *testing.T) {
	for _, c := range []struct {
		hto, jto, want time.Duration
	}{{0, 0, DefaultJoinTimeout}, {-1, 0, DefaultJoinTimeout}, {time.Second, 0, DefaultJoinTimeout},
		{-1, 2 * time.Second, 2 * time.Second}, {time.Second, 3 * time.Second, 3 * time.Second}} {
		cfg := &Config{HandshakeTimeout: c.hto, JoinTimeout: c.jto}
		cc, err := cfg.coreConfig(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := time.Duration(cc.JoinTimeout); got != c.want || cfg.joinTimeout() != c.want {
			t.Errorf("HandshakeTimeout %v JoinTimeout %v: %v, want %v", c.hto, c.jto, got, c.want)
		}
	}
	if _, err := (&Config{JoinTimeout: -1}).coreConfig(0, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("negative JoinTimeout: %v", err)
	}
}
