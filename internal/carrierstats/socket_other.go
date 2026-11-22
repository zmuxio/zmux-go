//go:build !linux && !darwin && !windows

package carrierstats

import "net"

const TCPSource = SourceUnknown

// ReadTCPPair reports no pair: this platform has no matching native counters.
func ReadTCPPair(*net.TCPConn) Readout { return Readout{} }
