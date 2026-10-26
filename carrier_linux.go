package zmux

import (
	"syscall"
	"time"
	"unsafe"
)

const tcpNotsentLowat = 25 // TCP_NOTSENT_LOWAT

func setNotsentLowat(fd uintptr, n int) error {
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotsentLowat, n)
}

// tcpMinRTT reads tcpi_min_rtt (microseconds, offset 148 of struct tcp_info).
func tcpMinRTT(fd uintptr) (time.Duration, bool) {
	var buf [232]byte
	l := uint32(len(buf))
	_, _, e := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, syscall.TCP_INFO,
		uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(&l)), 0)
	if e != 0 || l < 152 {
		return 0, false
	}
	us := *(*uint32)(unsafe.Pointer(&buf[148]))
	return time.Duration(us) * time.Microsecond, us > 0
}
