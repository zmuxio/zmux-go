package carrierio

import (
	"context"
	"sync"
	"sync/atomic"
)

// HalfLease ends a pooled pipe's lease once both of the pipe's halves have
// ended (the session ended it after its inbound side ended or lingered
// out, so that a connection is never closed under a close the peer has
// not read), or once the connection's ctx ends, whichever is first; the
// registration on ctx is withdrawn then, so that a long-lived connection
// keeps none per pipe. A nil HalfLease does nothing. It is safe for
// concurrent use.
type HalfLease struct {
	stop    func() bool
	release func()
	once    sync.Once
	halves  atomic.Int32
}

// NewHalfLease returns the lease end of a pipe on the connection whose
// context is ctx; release is called once.
func NewHalfLease(ctx context.Context, release func()) *HalfLease {
	h := &HalfLease{release: release}
	h.stop = context.AfterFunc(ctx, h.end)
	return h
}

// HalfEnded records the end of one half; the second ends the lease.
func (h *HalfLease) HalfEnded() {
	if h != nil && h.halves.Add(1) == 2 {
		h.stop()
		h.end()
	}
}

func (h *HalfLease) end() { h.once.Do(h.release) }
