package zmux

import (
	"syscall"
	"time"
	"unsafe"
)

const tcpNotsentLowat = 0x201 // TCP_NOTSENT_LOWAT

func setNotsentLowat(fd uintptr, n int) error {
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotsentLowat, n)
}

// tcpMinRTT: Darwin's TCP_CONNECTION_INFO reports only a smoothed RTT in
// milliseconds, which is not a minimum.
func tcpMinRTT(_ uintptr) (time.Duration, bool) { return 0, false }

// tcpUnsent: the platform reports no count of unsent bytes alone.
func tcpUnsent(_ uintptr) (uint64, bool) { return 0, false }

const tcpConnectionInfo = 0x106 // TCP_CONNECTION_INFO

// tcpRetransBits is the width of the count tcpRetrans reads.
const tcpRetransBits = 64

// tcpRetrans reads tcpi_txretransmitbytes (offset 72 of struct
// tcp_connection_info).
func tcpRetrans(fd uintptr) (uint64, bool) {
	var buf [112]byte
	l := uint32(len(buf))
	_, _, e := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, tcpConnectionInfo,
		uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(&l)), 0)
	if e != 0 || l < 80 {
		return 0, false
	}
	return *(*uint64)(unsafe.Pointer(&buf[72])), true
}
