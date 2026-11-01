package zmux

import (
	"crypto/tls"
	"net"
	"syscall"
	"time"
)

// Low-water marks of TCP carriers:
// 128 KiB on a single pipe, 32 KiB per pipe of a multipath session.
const (
	singleLowWater = 128 << 10
	multiLowWater  = 32 << 10
)

// tcpInfo is the TCP socket under a carrier, when there is one: its
// low-water mark and its minimum RTT are optional carrier capabilities.
type tcpInfo struct {
	rc syscall.RawConn
}

// tcpOf finds the TCP socket under a carrier half (a TCP connection, or a
// TLS connection over one).
func tcpOf(x any) tcpInfo {
	if tc, ok := x.(*tls.Conn); ok {
		x = tc.NetConn()
	}
	if c, ok := x.(*net.TCPConn); ok {
		if rc, err := c.SyscallConn(); err == nil {
			return tcpInfo{rc: rc}
		}
	}
	return tcpInfo{}
}

// setLowWater sets TCP_NOTSENT_LOWAT where the platform has it; true when
// the carrier now bounds its unsent bytes.
func (t tcpInfo) setLowWater(n int) bool {
	if t.rc == nil {
		return false
	}
	ok := false
	_ = t.rc.Control(func(fd uintptr) { ok = setNotsentLowat(fd, n) == nil })
	return ok
}

// minRTT is the carrier's own minimum RTT (Linux tcpi_min_rtt, Windows
// MinRttUs); Darwin reports only a smoothed value, which is not used.
func (t tcpInfo) minRTT() (time.Duration, bool) {
	if t.rc == nil {
		return 0, false
	}
	var d time.Duration
	ok := false
	_ = t.rc.Control(func(fd uintptr) { d, ok = tcpMinRTT(fd) })
	return d, ok && d > 0
}

// unsent is the carrier's unsent count, an optional capability: the bytes
// the socket holds and has not sent (Linux SIOCOUTQNSD).
func (t tcpInfo) unsent() (uint64, bool) {
	if t.rc == nil {
		return 0, false
	}
	var n uint64
	ok := false
	_ = t.rc.Control(func(fd uintptr) { n, ok = tcpUnsent(fd) })
	return n, ok
}

func (t tcpInfo) hasMinRTT() bool {
	_, ok := t.minRTT()
	return ok
}
