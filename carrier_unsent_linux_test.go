package zmux

import (
	"testing"
	"time"
)

// TestTCPUnsentLinux checks the unsent count of a Linux TCP carrier: it
// is reported, and it rises when the peer does not read.
func TestTCPUnsentLinux(t *testing.T) {
	a, b := tcpPair(t)
	defer func() { _ = a.Close(); _ = b.Close() }()
	ti := tcpOf(a)
	if n, ok := ti.unsent(); !ok || n != 0 {
		t.Fatalf("idle socket: unsent %d, %v", n, ok)
	}
	_ = a.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 1<<20)
	for {
		if _, err := a.Write(buf); err != nil {
			break // the peer's window and our buffer are full
		}
	}
	if n, ok := ti.unsent(); !ok || n == 0 {
		t.Fatalf("full socket: unsent %d, %v", n, ok)
	}
}
