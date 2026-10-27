package zmux

import (
	"syscall"
	"time"
)

const tcpNotsentLowat = 0x201 // TCP_NOTSENT_LOWAT

func setNotsentLowat(fd uintptr, n int) error {
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotsentLowat, n)
}

// tcpMinRTT: Darwin's TCP_CONNECTION_INFO reports only a smoothed RTT in
// milliseconds, which is not a minimum.
func tcpMinRTT(_ uintptr) (time.Duration, bool) { return 0, false }
