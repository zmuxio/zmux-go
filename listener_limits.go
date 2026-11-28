package zmux

import (
	"fmt"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/setting"
)

// ListenerLimits are the bounds of a Listener that SetLimits changes while
// it runs; ListenerConfig embeds them for their initial values.
//
// The other settings are fixed for the listener's life: Backlog (the
// capacity of the accept queue), SourcePrefixV4 and SourcePrefixV6 (they
// key the slots live carriers hold), InstanceID, the callbacks and
// Config, whose HandshakeTimeout bounds the carriers.
// The bounds of a session itself are its Config: SessionConfig, called for
// every new session, returns the Config current at the time; an established
// session keeps the Config it started with.
type ListenerLimits struct {
	// MaxSuspended bounds the suspended sessions it holds. Zero is 1024;
	// the longest-suspended are closed with ZMUX_RESUME_TIMEOUT.
	MaxSuspended int
	// MaxPending bounds the carriers of the listener before they belong to
	// a session: a carrier holds one of its slots from its arrival, through
	// its handshake, the admission, the inbound transform, a split half's
	// wait for its partner, the classification, join verification and the
	// exchange of transport parameters, and through a stall, until it
	// enters a session, is closed or is handed to a fallback. Zero is 1024;
	// a negative value is unlimited. When every slot is taken, a new
	// carrier closes the oldest stalled carrier to take its slot, or, with
	// none stalled, is closed at once (ListenerStats.Rejected). Stalled
	// carriers hold at most half of MaxPending; a carrier to be stalled
	// beyond that is closed (AdmissionStats.StallOverflow).
	MaxPending int
	// MaxPendingPerSource bounds those carriers per source (zero: 8),
	// stalled ones included: a new carrier beyond it is refused, and a
	// source holds at most MaxPendingPerSource - 1 stalled carriers, a
	// carrier to be stalled beyond that being closed. A source is a client
	// address aggregated to its source prefix (SourcePrefixV4,
	// SourcePrefixV6); deployments behind a carrier-grade NAT raise it.
	// The minimum is 2. Each half of a split bootstrap holds a slot
	// until the pair is complete.
	MaxPendingPerSource int
	// FirstRecordTimeout closes a carrier whose first record does not
	// arrive in time. Zero is 2 s; a negative value is no timeout.
	FirstRecordTimeout time.Duration
	// MemoryBudget bounds the listener's reserved receive credit, in-flight
	// I/O and retained storage. Zero is 1 GiB; a negative value is unlimited.
	// It replaces the sessions' Config.MemoryBudget. The initial connection
	// window may be reduced to fit. Establishment fails if 16 KiB cannot be
	// granted. SetLimits rejects any limit below existing commitments.
	MemoryBudget int64
}

// resolve validates the limits and fills in the defaults.
func (r ListenerLimits) resolve() (ListenerLimits, error) {
	if r.MaxSuspended < 0 {
		return r, fmt.Errorf("%w: negative ListenerConfig count", ErrInvalidConfig)
	}
	if r.MaxSuspended == 0 {
		r.MaxSuspended = defaultMaxSuspended
	}
	if r.MaxPending == 0 {
		r.MaxPending = core.DefaultMaxPending
	} else if r.MaxPending < 0 {
		r.MaxPending = -1
	}
	if r.MaxPendingPerSource == 0 {
		r.MaxPendingPerSource = core.DefaultMaxPendingPerSource
	} else if r.MaxPendingPerSource < 2 {
		return r, fmt.Errorf("%w: MaxPendingPerSource must be at least 2", ErrInvalidConfig)
	}
	if r.FirstRecordTimeout == 0 {
		r.FirstRecordTimeout = DefaultFirstRecordTimeout
	} else if r.FirstRecordTimeout < 0 {
		r.FirstRecordTimeout = -1
	}
	if r.MemoryBudget == 0 {
		r.MemoryBudget = defaultMemoryBudget
	} else if r.MemoryBudget < 0 {
		r.MemoryBudget = -1
	}
	return r, nil
}

// firstRecord is the effective FirstRecordTimeout of resolved limits.
func (r ListenerLimits) firstRecord() time.Duration {
	return setting.Timeout(r.FirstRecordTimeout, DefaultFirstRecordTimeout)
}

// Limits returns the limits in effect, defaults filled in: a zero count
// or timeout reads as its default, no timeout as -1, unlimited
// MaxPending as -1, so that a changed copy keeps the others' meaning.
func (l *Listener) Limits() ListenerLimits { return *l.lim.Load() }

// SetLimits replaces the listener's limits; invalid limits change nothing
// and return an error wrapping ErrInvalidConfig. New carriers and the next
// decisions on held ones use them at once: lowered pending bounds refuse or
// displace new carriers until the count is below them, but close no
// carrier already held; a lowered MaxSuspended closes the
// longest-suspended sessions beyond it with ZMUX_RESUME_TIMEOUT; a changed
// FirstRecordTimeout applies to carriers that arrive after the call; a
// MemoryBudget below already committed memory rejects the entire update.
// Otherwise the new budget applies to subsequent charges immediately.
func (l *Listener) SetLimits(lim ListenerLimits) error {
	r, err := lim.resolve()
	if err != nil {
		return err
	}
	over, err := l.applyLimits(&r)
	if err != nil {
		return err
	}
	if len(over) > 0 {
		go closeSuspended(over)
	}
	return nil
}

// applyLimits stores resolved limits and returns the suspended sessions
// beyond the new MaxSuspended, for the caller to close.
func (l *Listener) applyLimits(r *ListenerLimits) ([]suspension, error) {
	l.pmu.Lock()
	var mem uint64 // unlimited
	if r.MemoryBudget > 0 {
		mem = uint64(r.MemoryBudget)
	}
	if !l.budget.SetLimit(mem) {
		l.pmu.Unlock()
		return nil, fmt.Errorf("%w: MemoryBudget below committed memory", ErrInvalidConfig)
	}
	l.storeAdmissionLimits(r)
	l.pmu.Unlock()
	if l.served.Load() {
		l.startWatch()
	}
	l.suspMu.Lock()
	defer l.suspMu.Unlock()
	return l.trimSuspended(), nil
}

// storeAdmissionLimits is used during construction and under pmu thereafter.
func (l *Listener) storeAdmissionLimits(r *ListenerLimits) {
	l.lim.Store(r)
	l.pending.MaxTotal, l.pending.MaxPerSource = r.MaxPending, r.MaxPendingPerSource
}
