package zmux

import (
	"encoding/binary"
	"syscall"
	"time"
	"unsafe"

	"github.com/zmuxio/zmux-go/internal/carrierstats"
)

const tcpNotsentLowat = 25 // TCP_NOTSENT_LOWAT

func setNotsentLowat(fd uintptr, n int) error {
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotsentLowat, n)
}

// tcpMinRTT reads tcpi_min_rtt (microseconds, offset 148 of struct tcp_info).
func tcpMinRTT(fd uintptr) (time.Duration, bool) {
	var buf [232]byte
	l, e := carrierstats.TCPInfo(fd, buf[:])
	if e != 0 || l < 152 {
		return 0, false
	}
	us := binary.NativeEndian.Uint32(buf[148:])
	return time.Duration(us) * time.Microsecond, us > 0
}

const siocOutqNSD = 0x894B // SIOCOUTQNSD: bytes in the send queue not yet sent

// tcpUnsent reads the bytes the socket holds unsent (SIOCOUTQNSD).
func tcpUnsent(fd uintptr) (uint64, bool) {
	var n int32
	_, _, e := syscall.Syscall(syscall.SYS_IOCTL, fd, siocOutqNSD, uintptr(unsafe.Pointer(&n)))
	return uint64(max(n, 0)), e == 0
}
