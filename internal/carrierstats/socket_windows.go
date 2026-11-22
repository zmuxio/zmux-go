package carrierstats

import "net"

const TCPSource = SourceWindowsSIOTCPInfo

// ReadTCPPair reports no pair: this platform has no matching native counters.
func ReadTCPPair(*net.TCPConn) Readout { return Readout{} }
