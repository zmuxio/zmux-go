package zmux

import (
	"io"
	"runtime"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierstats"
)

// CarrierID is an opaque, process-local connection identity. Its zero value is invalid.
type CarrierID = carrierstats.CarrierID

// CarrierDirection identifies the sampled sender relative to the local process.
type CarrierDirection = carrierstats.Direction

// CarrierOutbound is the local sending direction.
const CarrierOutbound = carrierstats.Outbound

// CarrierSource identifies a native provider and its counter semantics.
type CarrierSource = carrierstats.Source

// Native carrier source tags.
const (
	CarrierSourceUnknown                 = carrierstats.SourceUnknown
	CarrierSourceLinuxTCPInfo            = carrierstats.SourceLinuxTCPInfo
	CarrierSourceDarwinTCPConnectionInfo = carrierstats.SourceDarwinTCPConnectionInfo
	CarrierSourceWindowsSIOTCPInfo       = carrierstats.SourceWindowsSIOTCPInfo
	CarrierSourceQUICGo                  = carrierstats.SourceQUICGo
)

// CarrierSample reports an observation of the local sender across all pipes
// and sessions sharing one connection. A reset advances Generation.
type CarrierSample struct {
	CarrierID  CarrierID
	Generation uint64
	Direction  CarrierDirection
	Source     CarrierSource
	SampledAt  time.Time
	Counts     CarrierCounts
}

// CarrierCounts contains one native pair, or all nil when unavailable.
type CarrierCounts struct {
	TCPSegments *TCPSegmentCounters
	TCPPackets  *TCPPacketCounters
	QUIC        *QUICCounters
}

// TCPSegmentCounters report Linux data segments and retransmitted segments.
type TCPSegmentCounters struct {
	DataSegsOut, TotalRetrans uint64
}

// TCPPacketCounters report Darwin outbound packets including retransmissions.
type TCPPacketCounters struct {
	PacketsSent, PacketsRetransmitted uint64
}

// QUICCounters report QUIC sent and lost packets. PacketsLost can decrease
// when the transport corrects a loss declaration after a late ACK.
type QUICCounters struct {
	PacketsSent, PacketsLost uint64
}

func outboundCarrierSample(w io.WriteCloser, refresh bool) *CarrierSample {
	sampler := samplerOf(w)
	if sampler == nil {
		return nil
	}
	var o carrierstats.Observation
	if refresh {
		o = carrierstats.Sample(sampler)
	} else {
		o = carrierstats.Cached(sampler)
	}
	runtime.KeepAlive(w)
	if o.At.IsZero() {
		return nil
	}
	s := &CarrierSample{CarrierID: o.ID, Generation: o.Generation, Direction: CarrierOutbound, Source: o.Source, SampledAt: o.At}
	if o.Known {
		switch o.Source {
		case CarrierSourceLinuxTCPInfo:
			s.Counts.TCPSegments = &TCPSegmentCounters{o.Sent, o.Lost}
		case CarrierSourceDarwinTCPConnectionInfo:
			s.Counts.TCPPackets = &TCPPacketCounters{o.Sent, o.Lost}
		case CarrierSourceQUICGo:
			s.Counts.QUIC = &QUICCounters{o.Sent, o.Lost}
		case CarrierSourceUnknown, CarrierSourceWindowsSIOTCPInfo:
			// These sources have no native pair.
		}
	}
	return s
}

// primeCarrierSample runs before committing an outbound attachment, outside
// the session and listener locks.
func primeCarrierSample(w io.WriteCloser) {
	if sampler := samplerOf(w); sampler != nil {
		carrierstats.Prime(sampler)
	}
	runtime.KeepAlive(w)
}

func samplerOf(w io.WriteCloser) *carrierstats.Sampler {
	if sampler := carrierstats.ProviderOf(w); sampler != nil {
		return sampler
	}
	return carrierstats.SocketSampler(w)
}
