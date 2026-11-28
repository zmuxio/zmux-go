package zmux

import (
	"errors"
	"syscall"
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
	if err := a.SetWriteDeadline(time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	for {
		var (
			sendErr error
			queued  uint64
			known   bool
		)
		// Sample after each nonblocking send, including when the socket
		// cannot write. The peer never reads from its end.
		err := ti.rc.Write(func(fd uintptr) bool {
			_, sendErr = syscall.Write(int(fd), buf)
			queued, known = ti.unsent()
			return !known || queued != 0 || !errors.Is(sendErr, syscall.EAGAIN)
		})
		if err != nil {
			t.Fatal(err)
		}
		if !known {
			t.Fatal("unsent bytes unavailable after write")
		}
		if queued != 0 {
			return
		}
		if sendErr != nil && !errors.Is(sendErr, syscall.EINTR) {
			t.Fatal(sendErr)
		}
	}
}
