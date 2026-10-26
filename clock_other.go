//go:build !windows

package zmux

import "time"

func monoSince(base time.Time) int64 { return int64(time.Since(base)) }
