package seal

import (
	"net/http"
	"sync"
	"time"

	"github.com/djylb/timeshim"
)

// ClockSkew holds a client's estimate of the servers' clock, for the
// stamps of msg1 and the buckets of bootstrap tags. It learns from the
// server time in each msg2 (authenticated), and before the first one from
// a carrier's HTTP Date response field (a hint: unauthenticated, used
// only while nothing authenticated is known and only when it differs from
// the local clock by more than half a minute). Once learnt, the estimate
// runs on the monotonic clock, so later changes of the local wall clock
// do not move it. The zero value is ready; it is safe for concurrent use.
// Share one among the carriers of the servers that share a clock.
type ClockSkew struct {
	mu     sync.Mutex
	set    bool
	auth   bool
	base   time.Time // the local reading when server was observed
	server int64     // the server's Unix ms at base
}

// defaultSkew is the ClockSkew of a ClientConfig without one.
var defaultSkew ClockSkew

// hintSlack is how far a hint must be from the local clock to be taken.
const hintSlack = 30 * time.Second

// now is the estimated server time at clk's now, in Unix milliseconds.
func (s *ClockSkew) now(clk timeshim.Clock) int64 {
	t := clk.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.set {
		return t.UnixMilli()
	}
	return s.server + t.Sub(s.base).Milliseconds()
}

// observe records server time serverMs seen at local reading at;
// authenticated: from a msg2.
func (s *ClockSkew) observe(serverMs int64, at time.Time, authenticated bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !authenticated {
		if s.auth {
			return
		}
		d := time.Duration(serverMs-at.UnixMilli()) * time.Millisecond
		if d < hintSlack && d > -hintSlack {
			return
		}
	}
	s.set, s.base, s.server = true, at, serverMs
	s.auth = s.auth || authenticated
}

// hint takes the Date field of h as a hint, at local reading at.
func (s *ClockSkew) hint(h http.Header, at time.Time) {
	if h == nil {
		return
	}
	d, err := http.ParseTime(h.Get("Date"))
	if err != nil {
		return
	}
	s.observe(d.UnixMilli(), at, false)
}

// Offset reports the estimated server clock minus the local wall clock
// at clk's now, and whether it was learnt from a server (false: nothing,
// or a hint only).
func (s *ClockSkew) Offset(clk timeshim.Clock) (time.Duration, bool) {
	clk = timeshim.Or(clk)
	t := clk.Now()
	est := s.now(clk)
	s.mu.Lock()
	auth := s.auth
	s.mu.Unlock()
	return time.Duration(est-t.UnixMilli()) * time.Millisecond, auth
}
