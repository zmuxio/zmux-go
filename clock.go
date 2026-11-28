package zmux

import (
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
)

// ClockDomain identifies a clock shared by sessions and controller state.
// A nil domain uses the process-wide default and the host monotonic clock.
type ClockDomain struct {
	tick   time.Duration
	source timeshim.Clock
}

var defaultClockDomain ClockDomain

// NewClockDomain creates a distinct domain. A nil source uses the host clock.
// A clock implementing Resolution() time.Duration supplies the tick used for
// strict grace deadlines and recovery jitter. The default is one nanosecond.
func NewClockDomain(source timeshim.Clock) *ClockDomain {
	domain := &ClockDomain{source: source}
	if r, ok := source.(interface{ Resolution() time.Duration }); ok {
		domain.tick = max(time.Nanosecond, r.Resolution())
	}
	return domain
}

func (d *ClockDomain) resolved() *ClockDomain {
	if d == nil {
		return &defaultClockDomain
	}
	return d
}

func (d *ClockDomain) resolution() time.Duration { return max(time.Nanosecond, d.resolved().tick) }

func (d *ClockDomain) clockSource() timeshim.Clock { return timeshim.Or(d.resolved().source) }

// clock maps a domain to the session-relative protocol time.
type clock struct {
	base   time.Time
	domain *ClockDomain
	ext    timeshim.Clock // nil: the host clock
	origin int64          // monoNow at base (the host clock)
}

func newClock(domain *ClockDomain) clock {
	domain = domain.resolved()
	if domain.source == nil {
		return clock{base: time.Now(), origin: monoNow(), domain: domain}
	}
	return clock{base: domain.source.Now(), ext: domain.source, domain: domain}
}

func (c clock) now() core.Time {
	if c.ext == nil {
		return core.Time(monoNow() - c.origin)
	}
	return core.Time(max(c.ext.Now().Sub(c.base), 0))
}

// until converts a core time to a duration from now.
func (c clock) until(t core.Time) time.Duration {
	if t == core.Never {
		return time.Duration(1<<63 - 1)
	}
	d := time.Duration(t - c.now())
	if d < 0 {
		return 0
	}
	return d
}

// wall converts a core time to wall-clock time.
func (c clock) wall(t core.Time) time.Time { return c.base.Add(time.Duration(t)) }

// source is the clock as a timeshim.Clock, for wall-clock timers.
func (c clock) source() timeshim.Clock { return timeshim.Or(c.ext) }

// afterFunc runs f after d on this clock.
func (c clock) afterFunc(d time.Duration, f func()) timeshim.Timer {
	if c.ext == nil {
		return time.AfterFunc(d, f)
	}
	return c.ext.AfterFunc(d, f)
}
