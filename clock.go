package zmux

import (
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// clock maps the monotonic clock to core.Time.
type clock struct{ base time.Time }

func newClock() clock { return clock{base: time.Now()} }

func (c clock) now() core.Time { return core.Time(monoSince(c.base)) }

// at converts a core time to a duration from now.
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
