package carrierstats

import (
	"syscall"
	"unsafe"
)

const TCPSource = SourceLinuxTCPInfo

func nativeTCPPair(fd uintptr) Readout {
	var buf [160]byte
	n, err := TCPInfo(fd, buf[:])
	if err != 0 {
		return Readout{}
	}
	return decodeLinuxPair(buf[:n])
}

// TCPInfo reads up to len(buf) bytes of Linux TCP_INFO.
func TCPInfo(fd uintptr, buf []byte) (uint32, syscall.Errno) {
	n := uint32(len(buf))
	//nolint:gosec // getsockopt writes at most the buffer length supplied in n.
	err := getsockopt(fd, syscall.IPPROTO_TCP, syscall.TCP_INFO, unsafe.Pointer(&buf[0]), &n)
	return min(n, uint32(len(buf))), err
}
