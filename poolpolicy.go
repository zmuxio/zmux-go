package zmux

import (
	"fmt"
	"time"
)

// PoolPolicy decides the members a pool keeps and when it dials. Decide
// must not block; the pool clamps values outside its limits.
type PoolPolicy interface {
	Decide(v *PoolView, d *PoolDecision)
}

// PoolView is the state of a pool.
type PoolView struct {
	Now      time.Time
	Members  []PoolMember
	Failures int       // consecutive failed dials
	LastDial time.Time // zero: none
}

// PoolMember is a pipe of a PipePool, or a connection of a CarrierPool.
type PoolMember struct {
	ID  uint64 // pipe ID, or the connection's fate group
	Age time.Duration
	// Idle: for a pipe, its PipeStats.SinceLastTraffic (probes,
	// acknowledgements and padding do not end it); for a connection, 0
	// while it carries pipes, otherwise since its last pipe ended, or
	// since it was established when it never carried one.
	Idle          time.Duration
	Pipes         int    // pipes on the connection; 1 for a pipe
	BytesSent     uint64 // 0 for a connection
	BytesReceived uint64 // 0 for a connection
}

// PoolDecision is the answer to Decide. Its zero value keeps the pool's
// own behaviour.
type PoolDecision struct {
	// Count is the members to keep. Zero is the pool's value (a pool
	// keeps one at least); a negative value is invalid.
	Count int
	// DialAfter is the earliest next dial after the start of the last
	// dial attempt (the start of the pool before the first), in place of
	// the pool's backoff (at most its maximum). Zero is the pool's
	// backoff; a negative value is invalid.
	DialAfter time.Duration
	// Retire lists the members to retire now (a pipe is replaced before
	// it closes).
	Retire []uint64
	// Recheck asks for the next Decide within this time even when nothing
	// else happens. Zero is none; a negative value is invalid. Pools wait
	// at least MinRecheck.
	Recheck time.Duration
	// ProbeInterval, ProbeJitter, DisableIdleProbe and NoProbeJitter are
	// set on the pipes a pool dials after this decision, over its
	// PipeOptions: the zero values keep the pool's (else the session's)
	// settings; DisableIdleProbe turns their idle probes off and
	// NoProbeJitter makes their jitter 0; ProbeInterval is otherwise at
	// least 1 s and ProbeJitter from 0 to below 1. Unlike
	// PipeOptions.ProbeInterval, a negative ProbeInterval is invalid here,
	// as the pool decisions shared by every implementation have it. A pool that dials no pipes ignores
	// valid ones. Validate reports values outside these ranges, which a
	// pool (or Config.RedialPolicy's session) does not clamp: it rejects
	// the whole decision and reports the error to its DecisionRejected
	// callback.
	ProbeInterval    time.Duration
	ProbeJitter      float64
	DisableIdleProbe bool
	NoProbeJitter    bool
}

// Validate reports, with ErrInvalidConfig, a decision with a negative
// Count, DialAfter or Recheck, a ProbeInterval neither 0 nor at least
// 1 s, or a ProbeJitter outside [0, 1).
func (d *PoolDecision) Validate() error {
	if d.Count < 0 || d.DialAfter < 0 || d.Recheck < 0 {
		return fmt.Errorf("%w: PoolDecision with a negative Count, DialAfter or Recheck", ErrInvalidConfig)
	}
	if d.ProbeInterval < 0 {
		return fmt.Errorf("%w: negative PoolDecision.ProbeInterval", ErrInvalidConfig)
	}
	if err := checkProbeInterval(d.ProbeInterval, "PoolDecision.ProbeInterval"); err != nil {
		return err
	}
	return checkProbeJitter(d.ProbeJitter, "PoolDecision.ProbeJitter")
}

// MinRecheck is the shortest wait a pool applies to PoolDecision.Recheck.
const MinRecheck = 10 * time.Millisecond

// decide asks pol, containing a panic (the zero decision then applies),
// and returns the decision's Validate error, the decision left as it is.
func decide(pol PoolPolicy, v *PoolView, d *PoolDecision) error {
	*d = PoolDecision{Retire: d.Retire[:0]}
	if !safeCall(func() { pol.Decide(v, d) }) {
		*d = PoolDecision{}
	}
	return d.Validate()
}

// reportRejected hands the error of a rejected decision to a pool's
// DecisionRejected callback f, if set, containing a panic. Callers hold no
// lock.
func reportRejected(f func(error), err error) {
	if f != nil {
		safeCall(func() { f(err) })
	}
}

// probeOptions returns o with d's probe settings over it.
func (d *PoolDecision) probeOptions(o PipeOptions) PipeOptions {
	switch {
	case d.DisableIdleProbe:
		o.ProbeInterval = -1
	case d.ProbeInterval > 0:
		o.ProbeInterval = d.ProbeInterval
	}
	switch {
	case d.NoProbeJitter:
		o.ProbeJitter, o.NoProbeJitter = 0, true
	case d.ProbeJitter > 0:
		o.ProbeJitter, o.NoProbeJitter = d.ProbeJitter, false
	}
	return o
}

// recheckAfter is the wait d asks for before the next Decide; 0: none.
func recheckAfter(d *PoolDecision) time.Duration {
	if d.Recheck <= 0 {
		return 0
	}
	return max(d.Recheck, MinRecheck)
}
