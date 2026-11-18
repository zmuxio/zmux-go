package zmux

import (
	"fmt"
	"time"
)

// PoolPolicy decides the members a pool keeps and when it dials. Decide
// runs on the pool's goroutine, must return promptly and must not block or
// call into the session. A non-nil error rejects the look: the pool goes
// on as with the zero decision and reports the error to its
// DecisionRejected callback, else logs it at warning level (a PipePool to
// Config.Logger, a carrier.ConnPool to its Logger). A decision with a value
// out of range is rejected the same way (PoolDecision.Validate), and so is
// a Decide that panics; no value is corrected into range.
type PoolPolicy interface {
	Decide(v *PoolView, d *PoolDecision) error
}

// PoolView is the state of a pool. It holds durations, not points in
// time, so that a policy reads no clock of its own.
type PoolView struct {
	// Members are the pool's members; a replacement being joined is not
	// one until it replaces its member.
	Members []PoolMember
	// Failures counts the consecutive failed dial attempts of the pool
	// (redials and replacements alike); 0 once a dial gives a usable
	// member.
	Failures int
	// SinceLastDial is the time since the start of the pool's last dial
	// attempt; negative before the first.
	SinceLastDial time.Duration
	// Dialing counts the pool's own growth dials in progress: the dial
	// attempts it started to reach its count (first dials and redials),
	// and a replacement whose member has gone meanwhile, which then adds
	// one. A replacement of a member still there is not counted, and
	// neither is a dial on demand, one a carrier.ConnPool.Get makes because
	// no connection has room. A pool that dials inside its looks, so that
	// none is in progress when it asks its policy, always reports 0: a
	// PipePool (MaintainPipes) does.
	Dialing int
}

// PoolMember is a member of a PipePool (a pipe, or the pipes a Join joined
// together), or a connection of a carrier.ConnPool. One key identifies it,
// and a decision names it by that key: a PipePool member by PipeID, a
// connection by FateGroup. A member is a connection exactly when its
// FateGroup is non-zero (a carrier pool never gives a connection fate
// group 0); PipeID cannot tell, since pipe 0 is a pipe.
type PoolMember struct {
	// PipeID is the ID of the first pipe of a PipePool member, pipe 0
	// included; it means nothing for a connection.
	PipeID PipeID
	// FateGroup is the fate group of a carrier.ConnPool connection, never
	// 0; 0 for a PipePool member.
	FateGroup uint64
	// Age is the time since its oldest pipe was attached, or since the
	// connection was established.
	Age time.Duration
	// Idle: for a member, the least PipeStats.SinceLastTraffic of its
	// pipes (probes, acknowledgements and padding do not end it); for a
	// connection, 0 while it carries pipes, otherwise since its last pipe
	// ended, or since it was established when it never carried one.
	Idle          time.Duration
	Pipes         int    // its pipes: 1 for a dialled pipe, the group's size for a joined member, the pipes on a connection
	BytesSent     uint64 // over its pipes; 0 for a connection
	BytesReceived uint64 // over its pipes; 0 for a connection
	// Usable: every pipe serves its direction and the peer attached it;
	// for a connection, it takes new pipes (it is not retired).
	Usable bool
	// Replacing: a replacement of it is being dialled or joined; always
	// false for a connection.
	Replacing bool
}

// PoolDecision is the answer to Decide. Its zero value keeps the pool's
// own behaviour.
type PoolDecision struct {
	// RetirePipes lists the pipes of a PipePool to retire now (a pipe is
	// replaced before it closes); RetireConnections lists the connections
	// of a carrier.ConnPool to retire, by fate group.
	RetirePipes       []PipeID
	RetireConnections []uint64
	// Liveness is set on the pipes a pool dials after this decision, over
	// its PipeOptions.Liveness: a zero field keeps the pool's (else the
	// session's) setting, and the others take the forms and bounds of
	// PipeLiveness (a negative ProbeInterval turns their idle probes off).
	// A pool that dials no pipes ignores it. Validate reports values
	// outside these bounds, which a pool (or Config.RedialPolicy's session)
	// does not clamp: it rejects the whole decision and reports the error
	// to its DecisionRejected callback.
	Liveness PipeLiveness
	// Count is the members to keep. Zero keeps the pool's own behaviour:
	// a PipePool keeps its Count (at least one), a carrier.ConnPool dials
	// a connection only when a pipe finds none with room. A Count below
	// the pool's size, its members and Dialing together, shrinks the pool
	// one step at a time: the growth dials under way are cancelled first,
	// the most recently started first (a dial on demand is not one, is
	// not counted and is never cancelled); then members close, those in
	// RetirePipes (RetireConnections) first, then the longest Idle, ties
	// broken by the fewest Pipes, the least Age, then the highest key. A
	// PipePool member closes gracefully, with its replacement if one is
	// being joined. A connection is retired: it takes no new pipe at once,
	// no longer counts, is reported with Usable false and closes once its
	// last pipe ends (at once when it carries none). A negative value is
	// invalid.
	Count int
	// DialAfter is the earliest next dial after the start of the last
	// dial attempt (the start of the pool before the first), in place of
	// the pool's backoff (at most its maximum). Zero leaves the pool's
	// backoff in force where it has one (a PipePool or a redial policy) and
	// asks for no delay of a carrier.ConnPool, which has none; a negative
	// value is invalid.
	DialAfter time.Duration
	// Recheck asks for the next Decide within this time even when nothing
	// else happens; zero asks for none beyond the pool's own looks (its
	// events, dials and the maintenance interval of 100 ms), and a
	// negative value is invalid. A positive value is lengthened to
	// MinRecheck at least, so the shortest recheck is MinRecheck; zero is
	// never "at once". It has no upper bound of its own.
	Recheck time.Duration
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

// poolLook is the maintenance interval of a pool with a policy or a byte
// rotation limit, the same in every language: byte counts change with no
// event of their own.
const poolLook = 100 * time.Millisecond

// MinRecheck is the shortest wait a pool applies to PoolDecision.Recheck.
const MinRecheck = 10 * time.Millisecond

// Ask fills d with pol's decision on v, as every pool asks its policy. A
// Decide that returns an error or panics rejects the look: d is then the
// zero decision and the error is Decide's, or names the panic. Otherwise
// the error is the decision's Validate error, the decision left as it is. A pool that gets an error rejects the
// decision and goes on as with the zero one.
func (d *PoolDecision) Ask(pol PoolPolicy, v *PoolView) error { return decide(pol, v, d) }

// decide is Ask. The panic is contained without logging: the error that
// names it is reported as the rejection.
func decide(pol PoolPolicy, v *PoolView, d *PoolDecision) error {
	*d = PoolDecision{RetirePipes: d.RetirePipes[:0], RetireConnections: d.RetireConnections[:0]}
	if err := hostCall(nil, "PoolPolicy.Decide", func() error { return pol.Decide(v, d) }); err != nil {
		*d = PoolDecision{RetirePipes: d.RetirePipes[:0], RetireConnections: d.RetireConnections[:0]}
		return err
	}
	return d.Validate()
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

// RecheckAfter is the wait before a pool asks its policy again after d:
// d's Recheck, raised to MinRecheck, when it comes before the pool's
// maintenance interval (100 ms), else that interval.
func (d *PoolDecision) RecheckAfter() time.Duration {
	if d.Recheck <= 0 {
		return poolLook
	}
	return min(max(d.Recheck, MinRecheck), poolLook)
}
