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

// retransCounter widens the count tcpRetrans reads: nil, it is 64 bits.
func retransCounter() *counter32 { return nil }

func tcpRetrans(uintptr) (uint64, bool) { return 0, false }
