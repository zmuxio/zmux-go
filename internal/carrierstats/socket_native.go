//go:build linux || darwin

package carrierstats

import "net"

// ReadTCPPair queries the pair using the TCP socket lifetime guard.
func ReadTCPPair(c *net.TCPConn) (pair Readout) {
	rc, err := c.SyscallConn()
	if err != nil {
		return pair
	}
	if rc.Control(func(fd uintptr) { pair = nativeTCPPair(fd) }) != nil {
		return Readout{}
	}
	return pair
}
