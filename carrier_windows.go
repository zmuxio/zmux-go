package zmux

import (
	"encoding/binary"
	"errors"
	"syscall"
	"time"
	"unsafe"
)

// Windows has no TCP_NOTSENT_LOWAT: the carrier queue limiter paces such
// carriers.
func setNotsentLowat(uintptr, int) error { return errors.ErrUnsupported }

const sioTCPInfo = 0xD8000027 // SIO_TCP_INFO

// tcpMinRTT reads MinRttUs of TCP_INFO_v0 through WSAIoctl.
func tcpMinRTT(fd uintptr) (time.Duration, bool) {
	var version uint32 // TCP_INFO_v0
	var info [88]byte
	var n uint32
	err := syscall.WSAIoctl(syscall.Handle(fd), sioTCPInfo, (*byte)(unsafe.Pointer(&version)), 4,
		&info[0], uint32(len(info)), &n, nil, 0)
	if err != nil || n < 28 {
		return 0, false
	}
	us := binary.NativeEndian.Uint32(info[24:])
	return time.Duration(us) * time.Microsecond, us > 0 && us != ^uint32(0)
}

// tcpUnsent: the platform reports no count of unsent bytes alone.
func tcpUnsent(uintptr) (uint64, bool) { return 0, false }

// tcpRetransBits is the width of the count tcpRetrans reads.
const tcpRetransBits = 32

// tcpRetrans reads BytesRetrans of TCP_INFO_v0 through WSAIoctl, a 32-bit
// count that tcpInfo widens. TCP_INFO_v1 and v2 keep the same 32-bit
// field and need newer Windows, so v0 is read.
func tcpRetrans(fd uintptr) (uint64, bool) {
	var version uint32 // TCP_INFO_v0
	var info [88]byte
	var n uint32
	err := syscall.WSAIoctl(syscall.Handle(fd), sioTCPInfo, (*byte)(unsafe.Pointer(&version)), 4,
		&info[0], uint32(len(info)), &n, nil, 0)
	if err != nil || n < 72 {
		return 0, false
	}
	return uint64(binary.NativeEndian.Uint32(info[68:])), true
}
