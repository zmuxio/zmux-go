package carrierstats

import (
	"encoding/binary"
	"net"
)

// TCPConnOf finds the TCP socket under a carrier half: a TCP connection, or
// any stack of NetConn layers (TLS, wrappers) over one.
func TCPConnOf(x any) *net.TCPConn {
	// Each NetConn layer (TLS, a host's wrapper, a transform's half over a
	// connection) names the connection beneath it; a bound stops a wrapper
	// that names itself.
	for range maxConnLayers {
		if c, ok := x.(*net.TCPConn); ok {
			return c
		}
		nc, ok := x.(interface{ NetConn() net.Conn })
		if !ok {
			return nil
		}
		next := nc.NetConn()
		if next == nil || next == x {
			return nil
		}
		x = next
	}
	return nil
}

// maxConnLayers bounds the NetConn layers TCPConnOf goes through.
const maxConnLayers = 16

// SocketSampler resolves the connection beneath a sending half.
func SocketSampler(w any) *Sampler {
	//goland:noinspection GoResourceLeak // borrowed view of the caller's connection; the caller owns its close
	if tcp := TCPConnOf(w); tcp != nil {
		return For(tcp, TCPSource, ReadTCPPair)
	}
	var id any
	if cc, ok := w.(interface{ CarrierSampleConn() any }); ok {
		id = cc.CarrierSampleConn()
	}
	if id == nil {
		if cc, ok := w.(interface{ CarrierConn() any }); ok {
			id = cc.CarrierConn()
		}
	}
	if id != nil {
		//goland:noinspection GoResourceLeak // borrowed view of the caller's connection; the caller owns its close
		if tcp := TCPConnOf(id); tcp != nil {
			return For(tcp, TCPSource, ReadTCPPair)
		}
		if sampler := Unknown(id); sampler != nil {
			return sampler
		}
	}
	if nc, ok := w.(interface{ NetConn() net.Conn }); ok {
		if sampler := Unknown(nc.NetConn()); sampler != nil {
			return sampler
		}
	}
	if nc, ok := w.(net.Conn); ok {
		return Unknown(nc)
	}
	return nil
}

// TCP_INFO fields tcpi_total_retrans and tcpi_data_segs_out.
func decodeLinuxPair(buf []byte) Readout {
	if len(buf) < 160 {
		return Readout{}
	}
	return Readout{
		Sent:  uint64(binary.NativeEndian.Uint32(buf[156:])),
		Lost:  uint64(binary.NativeEndian.Uint32(buf[100:])),
		Known: true,
	}
}

// TCP_CONNECTION_INFO fields tcpi_txpackets and tcpi_txretransmitpackets.
// Older kernels end the layout before the retransmitted packet counter.
func decodeDarwinPair(buf []byte) Readout {
	if len(buf) < 112 {
		return Readout{}
	}
	return Readout{
		Sent:  binary.NativeEndian.Uint64(buf[56:]),
		Lost:  binary.NativeEndian.Uint64(buf[104:]),
		Known: true,
	}
}
