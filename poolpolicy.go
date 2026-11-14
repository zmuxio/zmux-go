package zmux

import (
	"fmt"
	"log/slog"
	"time"
)

// PoolPolicy decides the members a pool keeps and when it dials. Decide
// must not block; the pool clamps values outside its limits.
type PoolPolicy interface {
	Decide(v *PoolView, d *PoolDecision)
}

// PoolView is the state of a pool.
type PoolView struct {
	Now time.Time
	// Connections reports a carrier.ConnPool: its members are connections,
	// named by FateGroup and retired with RetireConnections. Otherwise they
	// are the pipes of a PipePool, named by PipeID and retired with
	// RetirePipes.
	Connections bool
	Members     []PoolMember
	Failures    int       // consecutive failed dials
	LastDial    time.Time // zero: none
}

// PoolMember is a pipe of a PipePool, or a connection of a carrier.ConnPool.
type PoolMember struct {
	// PipeID is the pipe of a PipePool member; FateGroup the fate group
	// of a carrier.ConnPool connection. The other is zero.
	PipeID    PipeID
	FateGroup uint64
	Age       time.Duration
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
	// RetirePipes lists the pipes of a PipePool to retire now (a pipe is
	// replaced before it closes); RetireConnections lists the connections
	// of a carrier.ConnPool to retire, by fate group.
	RetirePipes       []PipeID
	RetireConnections []uint64
	// Recheck asks for the next Decide within this time even when nothing
	// else happens. Zero is none; a negative value is invalid. Pools wait
	// at least MinRecheck.
	Recheck time.Duration
	// Liveness is set on the pipes a pool dials after this decision, over
	// its PipeOptions.Liveness: a zero field keeps the pool's (else the
	// session's) setting, and the others take the forms and bounds of
	// PipeLiveness (a negative ProbeInterval turns their idle probes off).
	// A pool that dials no pipes ignores it. Validate reports values
	// outside these bounds, which a pool (or Config.RedialPolicy's session)
	// does not clamp: it rejects the whole decision and reports the error
	// to its DecisionRejected callback.
	Liveness PipeLiveness
}

// Validate reports, with ErrInvalidConfig, a decision with a negative
// Count, DialAfter or Recheck, or a Liveness outside the bounds of
// PipeLiveness.
func (d *PoolDecision) Validate() error {
	if d.Count < 0 || d.DialAfter < 0 || d.Recheck < 0 {
		return fmt.Errorf("%w: PoolDecision with a negative Count, DialAfter or Recheck", ErrInvalidConfig)
	}
	if _, err := d.Liveness.core(); err != nil {
		return fmt.Errorf("PoolDecision.Liveness: %w", err)
	}
	return nil
}

// MinRecheck is the shortest wait a pool applies to PoolDecision.Recheck.
const MinRecheck = 10 * time.Millisecond

// Ask fills d with pol's decision on v, as every pool asks its policy: a
// panic in Decide is contained (the zero decision then applies), and the
// error is the decision's Validate error, the decision left as it is; a
// pool that rejects the decision goes on as with the zero one.
func (d *PoolDecision) Ask(pol PoolPolicy, v *PoolView) error { return decide(nil, pol, v, d) }

// decide asks pol, containing a panic (the zero decision then applies,
// and the panic is logged to l), and returns the decision's Validate
// error, the decision left as it is.
func decide(l *slog.Logger, pol PoolPolicy, v *PoolView, d *PoolDecision) error {
	*d = PoolDecision{RetirePipes: d.RetirePipes[:0], RetireConnections: d.RetireConnections[:0]}
	if !safeLog(l, "PoolPolicy.Decide", func() { pol.Decide(v, d) }) {
		*d = PoolDecision{}
	}
	return d.Validate()
}

// reportRejected hands the error of a rejected decision to a pool's
// DecisionRejected callback f, if set, containing a panic (logged to l).
// Callers hold no lock.
func reportRejected(l *slog.Logger, f func(error), err error) {
	if f != nil {
		safeLog(l, "DecisionRejected", func() { f(err) })
	}
}

// probeOptions returns o with d's liveness settings over it.
func (d *PoolDecision) probeOptions(o PipeOptions) PipeOptions {
	dl := &d.Liveness
	if *dl == (PipeLiveness{}) {
		return o
	}
	l, _ := o.Liveness.get()
	if dl.SuspectFloor != 0 {
		l.SuspectFloor = dl.SuspectFloor
	}
	if dl.DeadTimeout != 0 {
		l.DeadTimeout = dl.DeadTimeout
	}
	if dl.ProbeInterval != 0 {
		l.ProbeInterval = dl.ProbeInterval
	}
	if dl.ProbeJitter != 0 {
		l.ProbeJitter = dl.ProbeJitter
	}
	o.Liveness = Set(l)
	return o
}

// RecheckAfter is the wait d asks for before the next Decide, Recheck
// raised to MinRecheck; 0: none.
func (d *PoolDecision) RecheckAfter() time.Duration {
	if d.Recheck <= 0 {
		return 0
	}
	return max(d.Recheck, MinRecheck)
}
