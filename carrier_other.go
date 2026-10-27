//go:build !linux && !darwin && !windows

package zmux

import (
	"errors"
	"time"
)

func setNotsentLowat(uintptr, int) error { return errors.ErrUnsupported }

func tcpMinRTT(_ uintptr) (time.Duration, bool) { return 0, false }
