package zmux

import "strings"

// connRefused reports whether the dial was refused by the peer. Plan 9
// reports it as an error string, not an errno.
func connRefused(err error) bool {
	return err != nil && strings.Contains(err.Error(), "connection refused")
}
