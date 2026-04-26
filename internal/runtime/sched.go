package runtime

import "github.com/zmuxio/zmux-go/internal/wire"

// BatchScheduler owns retained batch-ordering state plus the explicit-group
// tracking needed by the current repository-default sender model.
//
// This keeps writer/runtime scheduling ownership inside internal/runtime while
// allowing root-package tests and adapters to inspect the retained state during
// the ongoing structural refactor.
type BatchScheduler struct {
	State           BatchState
	ActiveGroupRefs map[uint64]uint32
}

func NewBatchScheduler() BatchScheduler {
	return BatchScheduler{}
}

func (s *BatchScheduler) EnsureState() {
	if s == nil {
		return
	}
	normalizeBatchState(&s.State)
}

func (s *BatchScheduler) Order(cfg BatchConfig, items []BatchItem) []int {
	if s == nil {
		return OrderBatchIndices(cfg, nil, items)
	}
	return OrderBatchIndices(cfg, &s.State, items)
}

func (s *BatchScheduler) TrackedExplicitGroupCount() int {
	if s == nil || len(s.ActiveGroupRefs) == 0 {
		return 0
	}
	count := len(s.ActiveGroupRefs)
	if _, ok := s.ActiveGroupRefs[0]; ok {
		count--
	}
	if _, ok := s.ActiveGroupRefs[FallbackGroupBucket]; ok {
		count--
	}
	if count < 0 {
		return 0
	}
	return count
}

func (s *BatchScheduler) TrackExplicitGroup(groupID uint64) {
	if s == nil || groupID == 0 {
		return
	}
	if s.ActiveGroupRefs == nil {
		s.ActiveGroupRefs = make(map[uint64]uint32)
	}
	s.ActiveGroupRefs[groupID]++
}

func (s *BatchScheduler) UntrackExplicitGroup(groupID uint64) {
	if s == nil || groupID == 0 || s.ActiveGroupRefs == nil {
		return
	}
	if refs := s.ActiveGroupRefs[groupID]; refs > 1 {
		s.ActiveGroupRefs[groupID] = refs - 1
		return
	}
	delete(s.ActiveGroupRefs, groupID)
	delete(s.State.GroupVirtualTime, GroupKey{Kind: 1, Value: groupID})
	delete(s.State.GroupFinishTag, GroupKey{Kind: 1, Value: groupID})
	delete(s.State.GroupLastService, GroupKey{Kind: 1, Value: groupID})
	delete(s.State.GroupLag, GroupKey{Kind: 1, Value: groupID})
	delete(s.State.PreferredStreamHead, GroupKey{Kind: 1, Value: groupID})
	s.maybeClearIdleHeadState()
}

func (s *BatchScheduler) DropStream(streamID uint64, explicitGroup bool, groupID uint64) {
	if s == nil || streamID == 0 {
		return
	}
	delete(s.State.StreamFinishTag, streamID)
	delete(s.State.StreamLastService, streamID)
	delete(s.State.StreamLag, streamID)
	delete(s.State.StreamClass, streamID)
	delete(s.State.StreamLastSeenBatch, streamID)
	delete(s.State.SmallBurstDisarmed, streamID)
	if !explicitGroup || groupID == 0 {
		delete(s.State.GroupVirtualTime, GroupKey{Kind: 0, Value: streamID})
		delete(s.State.GroupFinishTag, GroupKey{Kind: 0, Value: streamID})
		delete(s.State.GroupLastService, GroupKey{Kind: 0, Value: streamID})
		delete(s.State.GroupLag, GroupKey{Kind: 0, Value: streamID})
		delete(s.State.PreferredStreamHead, GroupKey{Kind: 0, Value: streamID})
	}
	s.maybeClearIdleHeadState()
}

func (s *BatchScheduler) Clear() {
	if s == nil {
		return
	}
	s.State = BatchState{}
	s.ActiveGroupRefs = nil
}

func (s *BatchScheduler) maybeClearIdleHeadState() {
	if s == nil {
		return
	}
	if len(s.State.StreamFinishTag) != 0 ||
		len(s.State.StreamLastService) != 0 ||
		len(s.State.StreamLag) != 0 ||
		len(s.State.StreamClass) != 0 ||
		len(s.State.StreamLastSeenBatch) != 0 ||
		len(s.State.SmallBurstDisarmed) != 0 ||
		len(s.State.GroupVirtualTime) != 0 ||
		len(s.State.GroupFinishTag) != 0 ||
		len(s.State.GroupLastService) != 0 ||
		len(s.State.GroupLag) != 0 ||
		len(s.State.PreferredStreamHead) != 0 ||
		s.State.HasPreferredGroupHead {
		return
	}
	scrubIdleRetainedBatchState(&s.State)
	releaseIdleBatchStateStorage(&s.State)
}

const wfqTagScale uint64 = 256
const maxSignedInt64 = int64(^uint64(0) >> 1)

func serviceTag(cost int64, weight uint64) uint64 {
	c := uint64(normalizeCost(cost))
	if weight == 0 {
		weight = 1
	}
	total := SaturatingMulDivCeil(c, wfqTagScale, weight)
	if total == 0 {
		return 1
	}
	return total
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func SchedulerQuantum(maxPayload uint64) uint64 {
	if maxPayload == 0 {
		return wire.DefaultSettings().MaxFramePayload
	}
	return maxPayload
}

func StreamWeight(priority, queuedBytes uint64, hint wire.SchedulerHint, maxPayload uint64) uint64 {
	base := priorityWeight(priority, hint)
	shortWindow := SchedulerQuantum(maxPayload)
	if shortWindow == 0 {
		return max64(base, 1)
	}

	switch hint {
	case wire.SchedulerLatency:
		switch {
		case queuedBytes <= shortWindow:
			base *= 4
		case queuedBytes <= shortWindow*2:
			base *= 2
		}
	case wire.SchedulerBalancedFair, wire.SchedulerUnspecifiedOrBalanced, wire.SchedulerGroupFair:
		if queuedBytes <= shortWindow {
			base *= 2
		}
	case wire.SchedulerBulkThroughput:
		if queuedBytes <= shortWindow/2 && shortWindow > 1 {
			base += base / 2
		}
	}

	if base == 0 {
		return 1
	}
	return base
}

func FeedbackWindow(hint wire.SchedulerHint, maxPayload uint64) int64 {
	window := SchedulerQuantum(maxPayload)
	switch hint {
	case wire.SchedulerLatency:
		window = SaturatingMul(window, 6)
	case wire.SchedulerBulkThroughput:
		window = SaturatingMul(window, 2)
	default:
		window = SaturatingMul(window, 4)
	}
	if window == 0 {
		return 1
	}
	if window > uint64(maxSignedInt64) {
		return maxSignedInt64
	}
	return int64(window)
}

func AdjustWeightForLag(base uint64, lag int64, window int64, fresh bool) uint64 {
	if base == 0 {
		base = 1
	}
	if fresh {
		base = SaturatingAdd(base, max64(base/2, 1))
	}
	if window <= 0 || lag == 0 {
		return max64(base, 1)
	}

	if lag > 0 {
		boost := lagScaledWeight(base, min64(lag, window), uint64(window))
		return max64(SaturatingAdd(base, max64(boost, 1)), 1)
	}
	penalty := lagScaledWeight(base, min64(-lag, window), SaturatingMul(uint64(window), 2))
	return base - penalty
}

func lagScaledWeight(base uint64, magnitude int64, divisor uint64) uint64 {
	if base == 0 || magnitude <= 0 || divisor == 0 {
		return 0
	}
	return SaturatingMulDivFloor(base, uint64(magnitude), divisor)
}

func GroupWeight(groupKey GroupKey, streamWeight uint64, hint wire.SchedulerHint) uint64 {
	if groupKey.Kind != 1 {
		return max64(streamWeight, 1)
	}
	switch hint {
	case wire.SchedulerLatency:
		return 32
	case wire.SchedulerBulkThroughput:
		return 16
	default:
		return 24
	}
}

func priorityWeight(priority uint64, hint wire.SchedulerHint) uint64 {
	switch hint {
	case wire.SchedulerLatency:
		return bandedWeight(priority, 16, 24, 32, 48, 64, 96)
	case wire.SchedulerBulkThroughput:
		return bandedWeight(priority, 16, 18, 20, 24, 28, 32)
	default:
		return bandedWeight(priority, 16, 20, 24, 32, 48, 72)
	}
}

func bandedWeight(priority uint64, base, mild, medium, strong, xstrong, saturated uint64) uint64 {
	switch {
	case priority >= 32:
		return saturated
	case priority >= 16:
		return xstrong
	case priority >= 8:
		return strong
	case priority >= 4:
		return medium
	case priority >= 1:
		return mild
	default:
		return base
	}
}
