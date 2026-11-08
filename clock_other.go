//go:build !windows

package zmux

import "time"

var monoBase = time.Now()

// monoNow reads the host's monotonic clock, from an arbitrary origin.
func monoNow() int64 { return int64(time.Since(monoBase)) }
