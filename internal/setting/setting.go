// Package setting resolves configuration values by the conventions that
// the zmux packages share.
package setting

import "time"

// NoTimeout stands for "no timeout" (a negative timeout setting): long
// enough never to pass, short enough to add to any clock reading.
const NoTimeout = 100 * 365 * 24 * time.Hour

// Timeout resolves a timeout setting: 0 is def, a negative value none.
func Timeout(v, def time.Duration) time.Duration {
	switch {
	case v < 0:
		return NoTimeout
	case v == 0:
		return def
	}
	return v
}
