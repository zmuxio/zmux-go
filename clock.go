package zmux

import (
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
)

// clock maps the session's clock to core.Time: the host's monotonic clock
// (QueryPerformanceCounter on Windows) when Config.Clock is nil, the
// configured clock otherwise.
type clock struct {
	base time.Time
	ext  timeshim.Clock // nil: the host clock
}

func newClock(ext timeshim.Clock) clock {
	if ext == nil {
		return clock{base: time.Now()}
	}
	return clock{base: ext.Now(), ext: ext}
}

func (c clock) now() core.Time {
	if c.ext == nil {
		return core.Time(monoSince(c.base))
	}
	return core.Time(c.ext.Now().Sub(c.base))
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
