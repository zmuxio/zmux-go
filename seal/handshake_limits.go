package seal

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/zmuxio/zmux-go/seal/proto"
)

// HandshakeLimits bounds the work of building Establish replies. Multiple
// listeners can share one limit when they share the same signer or resources.
type HandshakeLimits struct {
	mu                    sync.Mutex
	totalLimit, anonLimit int
	total, anon           int
}

// NewHandshakeLimits sets the maximum number of concurrent replies and the
// share available for anonymous Hellos. It requires 0 <= anonymous < total.
func NewHandshakeLimits(total, anonymous int) (*HandshakeLimits, error) {
	if anonymous < 0 || anonymous >= total {
		return nil, fmt.Errorf("%w: invalid Establish handshake limits", proto.ErrInvalidConfig)
	}
	return &HandshakeLimits{totalLimit: total, anonLimit: anonymous}, nil
}

// Protects only lazy initialization of each configuration's default limits.
var defaultHandshakeLimitsMu sync.Mutex

func (l *HandshakeLimits) acquire(anonymous bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= l.totalLimit || anonymous && l.anon >= l.anonLimit {
		return false
	}
	l.total++
	if anonymous {
		l.anon++
	}
	return true
}

func (l *HandshakeLimits) release(anonymous bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total--
	if anonymous {
		l.anon--
	}
}

func (c *ServerConfig) handshakeLimits() *HandshakeLimits {
	if c.HandshakeLimits != nil {
		return c.HandshakeLimits
	}
	defaultHandshakeLimitsMu.Lock()
	defer defaultHandshakeLimitsMu.Unlock()
	if c.defaultHandshakeLimits == nil {
		total := max(2, runtime.GOMAXPROCS(0))
		c.defaultHandshakeLimits = &HandshakeLimits{totalLimit: total, anonLimit: total/2 + total%2}
	}
	return c.defaultHandshakeLimits
}
