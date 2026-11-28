package zmux

import (
	"io"
	"runtime"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierstats"
)

// CarrierID is an opaque, process-local connection identity. Its zero value is invalid.
type CarrierID = carrierstats.CarrierID

// NewCarrierID creates an identity for a physical connection whose adapter
// supplies its own observation metadata. Share it across that connection's halves.
func NewCarrierID() CarrierID { return carrierstats.NewCarrierID() }

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
	SampledAt  time.Time
	Counts     CarrierCounts
	Loss       *CarrierLoss
	Direction  CarrierDirection
	Source     CarrierSource
	CarrierID  CarrierID
	Generation uint64
}

// CarrierLoss reports counter deltas scoped to one physical sender. The
// source identifies the unit and counter semantics.
type CarrierLoss struct {
	version               uint64
	SampledAt, ValidUntil time.Time
	Source                CarrierSource
	CarrierID             CarrierID
	Generation            uint64
	Interval              time.Duration
	Sent, Lost            uint64
}

const carrierLossValidFor = 60 * time.Second

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
	defer runtime.KeepAlive(w)
	return carrierObservation(samplerOf(w), refresh)
}

func carrierObservation(sampler *carrierstats.Sampler, refresh bool) *CarrierSample {
	if sampler == nil {
		return nil
	}
	var o carrierstats.Observation
	if refresh {
		o = carrierstats.Sample(sampler)
	} else {
		o = carrierstats.Cached(sampler)
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
	s.Loss = physicalLoss(o, carrierLossValidFor, time.Now())
	return s
}

func physicalLoss(o carrierstats.Observation, window time.Duration, now time.Time) *CarrierLoss {
	expiry := o.At.Add(window)
	if o.Interval <= 0 || !now.Before(expiry) {
		return nil
	}
	return &CarrierLoss{version: o.LossVersion, CarrierID: o.ID, Generation: o.Generation, Source: o.Source, SampledAt: o.At, ValidUntil: expiry, Interval: o.Interval, Sent: o.DeltaSent, Lost: o.DeltaLost}
}

// primeCarrierSample runs before committing an outbound attachment, outside
// the session and listener locks.
func primeCarrierSample(w io.WriteCloser) *carrierstats.Sampler {
	sampler := samplerOf(w)
	if sampler != nil {
		carrierstats.Prime(sampler)
	}
	runtime.KeepAlive(w)
	return sampler
}

func samplerOf(w any) *carrierstats.Sampler {
	if sampler := carrierstats.ProviderOf(w); sampler != nil {
		return sampler
	}
	return carrierstats.SocketSampler(w)
}
