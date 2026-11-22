package carrierstats

import (
	"syscall"
	"unsafe"
)

const tcpConnectionInfo = 0x106 // TCP_CONNECTION_INFO

const TCPSource = SourceDarwinTCPConnectionInfo

func nativeTCPPair(fd uintptr) Readout {
	var buf [112]byte
	n := uint32(len(buf))
	//nolint:gosec // getsockopt writes into the bounded stack buffer.
	_, _, err := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, tcpConnectionInfo,
		uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(&n)), 0)
	if err != 0 {
		return Readout{}
	}
	return decodeDarwinPair(buf[:min(n, uint32(len(buf)))])
}
