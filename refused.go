//go:build !plan9

package zmux

import (
	"errors"
	"syscall"
)

// connRefused reports whether the dial was refused by the peer.
func connRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }
