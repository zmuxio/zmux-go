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
	// Each half of a split bootstrap holds a slot until the pair is
	// complete, so split bootstraps need at least 2.
	MaxPendingPerSource int
	// MaxPendingPerSession bounds the Unconfirmed carriers of one session:
	// joins that passed authentication and wait for the joiner's
	// confirmation (a join in Bound Mode is confirmed by its verification
	// and holds no place). Zero is 4. A new authenticated Hello is never
	// refused for lack of room: once the bound is reached, the carrier
	// that has been Unconfirmed longest is closed first, as a failed join;
	// below it, none is.
	MaxPendingPerSession int
	// FirstRecordTimeout closes a carrier whose first record does not
	// arrive in time. Zero is 2 s; a negative value is no timeout.
	FirstRecordTimeout time.Duration
	// MemoryBudget bounds the sum over the listener's sessions of buffered
	// receive data, retained data and session window growth. Zero is
	// 1 GiB; a negative value is unlimited. It replaces the sessions' own
	// Config.MemoryBudget. Above it sessions grant no new credit, refuse
	// new peer streams, new sessions and pipes are refused, and suspended
	// sessions are closed, the largest first; normal operation resumes
	// below three quarters of it.
	MemoryBudget int64
}

// resolve validates the limits and fills in the defaults.
func (r ListenerLimits) resolve() (ListenerLimits, error) {
	if r.MaxSuspended < 0 || r.MaxPendingPerSource < 0 || r.MaxPendingPerSession < 0 {
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
	}
	if r.MaxPendingPerSession == 0 {
		r.MaxPendingPerSession = core.DefaultMaxUnconfirmed
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
// carrier already held; a lowered MaxPendingPerSession applies to the
// next join of a session; a lowered MaxSuspended closes the
// longest-suspended sessions beyond it with ZMUX_RESUME_TIMEOUT; a changed
// FirstRecordTimeout applies to carriers that arrive after the call; a
// changed MemoryBudget starts or ends memory pressure at once, as if the
// charged sum had crossed the new bounds (OnMemoryPressure, suspended
// sessions closed the largest first).
func (l *Listener) SetLimits(lim ListenerLimits) error {
	r, err := lim.resolve()
	if err != nil {
		return err
	}
	if over := l.applyLimits(&r); len(over) > 0 {
		go closeSuspended(over)
	}
	return nil
}

// applyLimits stores resolved limits and returns the suspended sessions
// beyond the new MaxSuspended, for the caller to close.
func (l *Listener) applyLimits(r *ListenerLimits) []*Conn {
	l.pmu.Lock()
	l.lim.Store(r)
	l.pending.MaxTotal = r.MaxPending
	l.pending.MaxPerSource = r.MaxPendingPerSource
	l.pending.Timeout = core.Duration(r.firstRecord())
	l.pmu.Unlock()
	var mem uint64 // unlimited
	if r.MemoryBudget > 0 {
		mem = uint64(r.MemoryBudget)
	}
	l.budget.SetLimit(mem)
	if l.served.Load() {
		l.startWatch()
	}
	l.regMu.Lock()
	defer l.regMu.Unlock()
	return l.trimSuspended()
}
