//go:build !linux && !darwin && !windows

package zmux

import (
	"errors"
	"time"
)

func setNotsentLowat(uintptr, int) error { return errors.ErrUnsupported }

func tcpMinRTT(_ uintptr) (time.Duration, bool) { return 0, false }

// tcpUnsent: the platform reports no count of unsent bytes alone.
func tcpUnsent(uintptr) (uint64, bool) { return 0, false }

const tcpRetransBits = 64

func tcpRetrans(uintptr) (uint64, bool) { return 0, false }
