package runtime

import (
	"math"

	"github.com/zmuxio/zmux-go/internal/wire"
)

const (
	RepoDefaultPerStreamDataHWMMin = 256 * 1024
	RepoDefaultSessionDataHWMMin   = 4 * 1024 * 1024
	RepoDefaultUrgentLaneCapMin    = 64 * 1024
)

func QuarterThreshold(v uint64) uint64 {
	if v <= 4 {
		return 1
	}
	return v / 4
}

func WindowRemaining(limit, received uint64) uint64 {
	if received >= limit {
		return 0
	}
	return limit - received
}

func NegotiatedFramePayload(local, peer wire.Settings) uint64 {
	payload := MinNonZeroUint64(local.MaxFramePayload, peer.MaxFramePayload)
	if payload == 0 {
		payload = wire.DefaultSettings().MaxFramePayload
	}
	return payload
}

func RepoDefaultPerStreamDataHWM(maxFramePayload uint64) uint64 {
	return MaxUint64(RepoDefaultPerStreamDataHWMMin, SaturatingMul(maxFramePayload, 16))
}

func RepoDefaultSessionDataHWM(perStreamDataHWM uint64) uint64 {
	return MaxUint64(RepoDefaultSessionDataHWMMin, SaturatingMul(perStreamDataHWM, 4))
}

func RepoDefaultUrgentLaneCap(maxControlPayload uint64) uint64 {
	return MaxUint64(RepoDefaultUrgentLaneCapMin, SaturatingMul(maxControlPayload, 8))
}

func MinNonZeroUint64(a, b uint64) uint64 {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func SaturatingAdd(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func SaturatingMul(v uint64, n uint64) uint64 {
	if v == 0 || n == 0 {
		return 0
	}
	if math.MaxUint64/v < n {
		return math.MaxUint64
	}
	return v * n
}

func LowWatermark(high uint64) uint64 {
	return high / 2
}

func QueueWouldBlock(sessionMemoryBlocked bool, sessionQueued, streamQueued, reqBytes, sessionHWM, streamHWM uint64) bool {
	if sessionMemoryBlocked || reqBytes == 0 {
		return sessionMemoryBlocked
	}
	if SaturatingAdd(sessionQueued, reqBytes) > sessionHWM {
		return true
	}
	if SaturatingAdd(streamQueued, reqBytes) > streamHWM {
		return true
	}
	return false
}

func CrossedLowWatermark(prev, next, lowWatermark uint64) bool {
	return prev > lowWatermark && next <= lowWatermark
}

func GainedCredit(prev, next uint64) bool {
	return prev == 0 && next > 0
}

func MemoryWakeNeeded(prevTracked, nextTracked, threshold uint64) bool {
	if prevTracked < threshold {
		return false
	}
	return nextTracked < threshold
}

func ProjectedExceedsThreshold(current, additional, threshold uint64) bool {
	return SaturatingAdd(current, additional) > threshold
}

const (
	DefaultVisibleAcceptBacklogLimit = 128

	visibleAcceptBacklogBytesMin    = 4 << 20
	visibleAcceptPerStreamHWMMin    = 256 << 10
	visibleAcceptPerStreamHWMFrames = 16
	visibleAcceptSessionHWMFactor   = 4
)

func VisibleAcceptBacklogBytesHardCap(maxFramePayload uint64) uint64 {
	if maxFramePayload == 0 {
		maxFramePayload = wire.DefaultSettings().MaxFramePayload
	}
	perStream := uint64(visibleAcceptPerStreamHWMFrames) * maxFramePayload
	if perStream < visibleAcceptPerStreamHWMMin {
		perStream = visibleAcceptPerStreamHWMMin
	}
	limit := uint64(visibleAcceptSessionHWMFactor) * perStream
	if limit < visibleAcceptBacklogBytesMin {
		limit = visibleAcceptBacklogBytesMin
	}
	return limit
}

func LateDataPerStreamCap(initialStreamWindow, maxFramePayload uint64) uint64 {
	if maxFramePayload == 0 {
		maxFramePayload = wire.DefaultSettings().MaxFramePayload
	}
	limit := 2 * maxFramePayload
	if windowCap := initialStreamWindow / 8; windowCap < limit {
		limit = windowCap
	}
	return limit
}

func AggregateLateDataCap(maxFramePayload uint64) uint64 {
	limit := 4 * maxFramePayload
	const minCap = 64 * 1024
	if limit < minCap {
		limit = minCap
	}
	return limit
}

type ReleaseWakePlan struct {
	Broadcast  bool
	StreamWake bool
	Control    bool
	MemoryWake bool
}

func PlanQueueReleaseWake(
	prevTracked, nextTracked, memoryThreshold uint64,
	prevSessionQueued, nextSessionQueued, sessionLWM uint64,
	prevStreamQueued, nextStreamQueued, streamLWM uint64,
	urgentReleased bool,
) ReleaseWakePlan {
	memoryWake := MemoryWakeNeeded(prevTracked, nextTracked, memoryThreshold)
	sessionWake := CrossedLowWatermark(prevSessionQueued, nextSessionQueued, sessionLWM)
	broadcast := sessionWake || memoryWake
	streamWake := !broadcast && CrossedLowWatermark(prevStreamQueued, nextStreamQueued, streamLWM)
	return ReleaseWakePlan{
		Broadcast:  broadcast,
		StreamWake: streamWake,
		Control:    memoryWake || urgentReleased,
		MemoryWake: memoryWake,
	}
}

func PlanPreparedReleaseWake(
	prevTracked, nextTracked, memoryThreshold uint64,
	prevSessionQueued, nextSessionQueued, sessionLWM uint64,
	prevStreamQueued, nextStreamQueued, streamLWM uint64,
	prevSessionCredit, nextSessionCredit uint64,
	prevStreamCredit, nextStreamCredit uint64,
	urgentReleased bool,
) ReleaseWakePlan {
	memoryWake := MemoryWakeNeeded(prevTracked, nextTracked, memoryThreshold)
	sessionWake := CrossedLowWatermark(prevSessionQueued, nextSessionQueued, sessionLWM) ||
		GainedCredit(prevSessionCredit, nextSessionCredit)
	broadcast := sessionWake || memoryWake
	streamWake := !broadcast && (CrossedLowWatermark(prevStreamQueued, nextStreamQueued, streamLWM) ||
		GainedCredit(prevStreamCredit, nextStreamCredit))
	return ReleaseWakePlan{
		Broadcast:  broadcast,
		StreamWake: streamWake,
		Control:    memoryWake || urgentReleased,
		MemoryWake: memoryWake,
	}
}

func PlanLaneReleaseWake(prevTracked, nextTracked, memoryThreshold uint64, controlReleased bool) ReleaseWakePlan {
	memoryWake := MemoryWakeNeeded(prevTracked, nextTracked, memoryThreshold)
	return ReleaseWakePlan{
		Broadcast:  memoryWake,
		StreamWake: false,
		Control:    memoryWake || controlReleased,
		MemoryWake: memoryWake,
	}
}
