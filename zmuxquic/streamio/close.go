package streamio

import "sync"

// CloseRecord stores the application close of the most recent pipe on a
// pooled connection or session. The carrier converts its own code width.
type CloseRecord struct {
	reason string
	code   uint64
	mu     sync.Mutex
}

// Set records the application close for the carrier's eventual release.
func (r *CloseRecord) Set(code uint64, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.code, r.reason = code, reason
}

// Get returns the close code and reason together.
func (r *CloseRecord) Get() (uint64, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.code, r.reason
}
