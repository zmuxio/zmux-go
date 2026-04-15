package runtime

import (
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

const (
	DefaultWriteBurstFrames   = 16
	MildWriteBurstFrames      = 8
	StrongWriteBurstFrames    = 4
	SaturatedWriteBurstFrames = 2

	DefaultFragmentTimeBudget   = 200 * time.Millisecond
	MildFragmentTimeBudget      = 150 * time.Millisecond
	StrongFragmentTimeBudget    = 100 * time.Millisecond
	SaturatedFragmentTimeBudget = 50 * time.Millisecond
)

func WriteBurstLimit(priority uint64, hint wire.SchedulerHint) int {
	switch {
	case priority >= 16:
		return SaturatedWriteBurstFrames
	case priority >= 4:
		return StrongWriteBurstFrames
	case priority >= 1:
		return MildWriteBurstFrames
	}

	switch hint {
	case wire.SchedulerLatency:
		return MildWriteBurstFrames
	default:
		return DefaultWriteBurstFrames
	}
}

func FragmentCap(maxPayload, prefixLen, priority uint64, hint wire.SchedulerHint) uint64 {
	if maxPayload == 0 {
		maxPayload = wire.DefaultSettings().MaxFramePayload
	}
	if prefixLen >= maxPayload {
		return 0
	}

	available := maxPayload - prefixLen
	switch {
	case priority >= 16:
		return ScaledFragmentCap(available, 1, 4)
	case priority >= 4:
		return ScaledFragmentCap(available, 1, 2)
	case priority >= 1:
		return ScaledFragmentCap(available, 3, 4)
	}

	switch hint {
	case wire.SchedulerLatency:
		return ScaledFragmentCap(available, 1, 2)
	default:
		return available
	}
}

func FragmentTimeBudget(priority uint64, hint wire.SchedulerHint) time.Duration {
	switch {
	case priority >= 16:
		return SaturatedFragmentTimeBudget
	case priority >= 4:
		return StrongFragmentTimeBudget
	case priority >= 1:
		return MildFragmentTimeBudget
	}

	switch hint {
	case wire.SchedulerLatency:
		return StrongFragmentTimeBudget
	default:
		return DefaultFragmentTimeBudget
	}
}

func RateLimitedFragmentCap(baseCap, estimatedSendRateBps, priority uint64, hint wire.SchedulerHint) uint64 {
	if baseCap == 0 || estimatedSendRateBps == 0 {
		return baseCap
	}
	budget := FragmentTimeBudget(priority, hint)
	if budget <= 0 {
		return baseCap
	}
	rateCap := SaturatingMulDivFloor(estimatedSendRateBps, uint64(budget), uint64(time.Second))
	if rateCap == 0 {
		rateCap = 1
	}
	if rateCap < baseCap {
		return rateCap
	}
	return baseCap
}

func ScaledFragmentCap(max uint64, num uint64, den uint64) uint64 {
	if max == 0 {
		return 0
	}
	if den == 0 {
		return max
	}
	v := SaturatingMulDivFloor(max, num, den)
	if v == 0 {
		return 1
	}
	if v > max {
		return max
	}
	return v
}
