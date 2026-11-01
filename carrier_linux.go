package zmux

import (
	"encoding/binary"
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
	e := getsockopt(fd, syscall.IPPROTO_TCP, syscall.TCP_INFO, unsafe.Pointer(&buf[0]), &l)
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

// retransCounter widens the count tcpRetrans reads: nil, it is 64 bits.
func retransCounter() *counter32 { return nil }

// tcpRetrans reads tcpi_bytes_retrans (offset 208 of struct tcp_info,
// Linux 4.19 and later).
func tcpRetrans(fd uintptr) (uint64, bool) {
	var buf [232]byte
	l := uint32(len(buf))
	e := getsockopt(fd, syscall.IPPROTO_TCP, syscall.TCP_INFO, unsafe.Pointer(&buf[0]), &l)
	if e != 0 || l < 216 {
		return 0, false
	}
	return binary.NativeEndian.Uint64(buf[208:]), true
}
