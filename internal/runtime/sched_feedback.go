package runtime

func fairShare(cost int64, weight uint64, totalWeight uint64) int64 {
	if cost <= 0 || weight == 0 || totalWeight == 0 {
		return 0
	}
	share := SaturatingMulDivCeil(uint64(cost), weight, totalWeight)
	const maxInt64 = ^uint64(0) >> 1
	if share > maxInt64 {
		return int64(maxInt64)
	}
	return int64(share)
}

func clampLag(value int64, window int64) int64 {
	if window <= 0 {
		return 0
	}
	limit := maxSignedInt64
	if window <= maxSignedInt64/2 {
		limit = window * 2
	}
	switch {
	case value > limit:
		return limit
	case value < -limit:
		return -limit
	default:
		return value
	}
}

func applyLagFeedback(current int64, expected int64, actual int64, window int64) int64 {
	if window <= 0 {
		return 0
	}
	if expected >= actual {
		delta := expected - actual
		if current > maxSignedInt64-delta {
			return clampLag(maxSignedInt64, window)
		}
		return clampLag(current+delta, window)
	}

	delta := actual - expected
	floor := -maxSignedInt64
	if current < floor+delta {
		return clampLag(floor, window)
	}
	return clampLag(current-delta, window)
}

func isFreshStream(state *BatchState, streamID uint64) bool {
	if state == nil || isSyntheticStreamKey(streamID) {
		return false
	}
	if _, ok := state.StreamFinishTag[streamID]; ok {
		return false
	}
	if _, ok := state.StreamLastService[streamID]; ok {
		return false
	}
	if _, ok := state.StreamLag[streamID]; ok {
		return false
	}
	return true
}

func isFreshGroup(state *BatchState, groupKey GroupKey) bool {
	if state == nil || isTransientGroupKey(groupKey) {
		return false
	}
	if _, ok := state.GroupFinishTag[groupKey]; ok {
		return false
	}
	if _, ok := state.GroupLastService[groupKey]; ok {
		return false
	}
	if _, ok := state.GroupLag[groupKey]; ok {
		return false
	}
	return true
}
