package runtime

import "sort"

type batchBuildResult struct {
	groupOrder          []GroupKey
	groupState          map[GroupKey]map[uint64][]int
	streamOrder         map[GroupKey][]uint64
	queuedBytes         map[uint64]uint64
	streamMeta          map[uint64]StreamMeta
	hasRealStreamScoped bool
	hasPriorityUpdate   bool
}

func transientBatchState(state *BatchState, capHint int) batchTransientState {
	return batchTransientState{
		streamFinish:     transientStreamFinishMap(state, capHint),
		streamLastServed: transientStreamLastServedMap(state, capHint),
		groupVirtual:     transientGroupVirtualMap(state, capHint),
		groupFinish:      transientGroupFinishMap(state, capHint),
		groupLastServed:  transientGroupLastServedMap(state, capHint),
	}
}

func buildBatchGroups(state *BatchState, items []BatchItem) batchBuildResult {
	result := batchBuildResult{
		groupOrder:  groupOrderSlice(state, 0, len(items)),
		groupState:  groupStateMap(state, len(items)),
		streamOrder: streamOrderMap(state, len(items)),
		queuedBytes: queuedBytesMap(state),
	}

	for i, item := range items {
		req := item.Request
		queues, ok := result.groupState[req.GroupKey]
		if !ok {
			result.groupOrder = append(result.groupOrder, req.GroupKey)
			queues = nextGroupQueueMap(state)
			result.groupState[req.GroupKey] = queues
		}
		streamKey := syntheticStreamKey(req, i)
		if _, ok := queues[streamKey]; !ok {
			result.streamOrder[req.GroupKey] = append(result.streamOrder[req.GroupKey], streamKey)
		}
		queues[streamKey] = append(queues[streamKey], i)
		result.queuedBytes[streamKey] = SaturatingAdd(result.queuedBytes[streamKey], uint64(normalizeCost(req.Cost)))
		if req.StreamScoped {
			if result.streamMeta == nil {
				result.streamMeta = batchStreamMetaMap(state, len(items))
			}
			result.hasRealStreamScoped = true
			result.streamMeta[req.StreamID] = item.Stream
		}
		if req.IsPriorityUpdate {
			result.hasPriorityUpdate = true
		}
	}
	return result
}

func syntheticStreamKey(req RequestMeta, idx int) uint64 {
	if req.StreamScoped {
		return req.StreamID
	}
	return syntheticStreamKeyBit | uint64(idx)
}

func isSyntheticStreamKey(streamID uint64) bool {
	return streamID&syntheticStreamKeyBit != 0
}

func batchStreamMetaMap(state *BatchState, capHint int) map[uint64]StreamMeta {
	if state == nil {
		return make(map[uint64]StreamMeta, capHint)
	}
	if state.scratch.streamMeta == nil {
		state.scratch.streamMeta = make(map[uint64]StreamMeta, capHint)
	} else {
		clear(state.scratch.streamMeta)
	}
	return state.scratch.streamMeta
}

func pickNextTransientOrdinaryHead(groupOrder []GroupKey, groupState map[GroupKey]map[uint64][]int, streamOrder map[GroupKey][]uint64) (groupIdx int, streamID uint64, reqIdx int, rest []int, ok bool) {
	for idx, groupKey := range groupOrder {
		if !isTransientGroupKey(groupKey) {
			continue
		}
		streams := streamOrder[groupKey]
		for _, streamID := range streams {
			queue := groupState[groupKey][streamID]
			if len(queue) == 0 {
				continue
			}
			return idx, streamID, queue[0], queue[1:], true
		}
	}
	return 0, 0, 0, nil, false
}

func isTransientGroupKey(groupKey GroupKey) bool {
	return groupKey.Kind == 2
}

func peekStreamCandidate(queue []int, items []BatchItem) (reqIdx int, pos int, cost int64, isPriorityUpdate bool, ok bool) {
	for i, idx := range queue {
		if items[idx].Request.IsPriorityUpdate {
			return idx, i, normalizeCost(items[idx].Request.Cost), true, true
		}
	}
	if len(queue) == 0 {
		return 0, 0, 0, false, false
	}
	idx := queue[0]
	return idx, 0, normalizeCost(items[idx].Request.Cost), false, true
}

func removeQueueEntry(queue []int, pos int) []int {
	if pos < 0 || pos >= len(queue) {
		return queue
	}
	copy(queue[pos:], queue[pos+1:])
	return queue[:len(queue)-1]
}

func normalizeCost(cost int64) int64 {
	if cost <= 0 {
		return 1
	}
	return cost
}

func OrderBatchIndices(cfg BatchConfig, state *BatchState, items []BatchItem) []int {
	if cfg.Urgent {
		return orderUrgentBatch(items, state)
	}

	state = normalizeBatchState(state)
	retainedRealState := hasRetainedRealBatchState(state)
	if !retainedRealState {
		scrubIdleRetainedBatchState(state)
	}

	prepared := buildBatchGroups(state, items)
	if !prepared.hasRealStreamScoped {
		return identityOrder(state, len(items))
	}

	transient := transientBatchState(state, len(items))
	tiePrefs := snapshotBatchTiePrefs(state)
	wfqActive := newBatchWFQActive(cfg, state, transient, prepared, items, tiePrefs)
	ordered := orderedSlice(state, 0, len(items))
	advisoryHeadArmed := true
	seenRealOpportunity := false
	transientHeadUsed := false
	recordedBatchHead := false
	recordedGroupHead := selectedSlice(state, len(prepared.groupOrder))

	for len(ordered) < len(items) {
		if !seenRealOpportunity && !transientHeadUsed {
			if idx, streamID, reqIdx, rest, ok := pickNextTransientOrdinaryHead(prepared.groupOrder, prepared.groupState, prepared.streamOrder); ok {
				groupKey := prepared.groupOrder[idx]
				ordered = append(ordered, reqIdx)
				prepared.groupState[groupKey][streamID] = rest
				transientHeadUsed = true
				continue
			}
		}

		candidate, ok := wfqActive.next(advisoryHeadArmed)
		if !ok {
			return appendRemainingInInputOrder(state, ordered, len(items))
		}

		ordered = append(ordered, candidate.stream.reqIdx)
		recordPreferredHeads(state, candidate, prepared.groupOrder, prepared.streamOrder, recordedBatchHead, recordedGroupHead)
		if !recordedBatchHead {
			recordedBatchHead = true
		}
		wfqActive.commit(candidate)
		if candidate.stream.isPriorityUpdate {
			advisoryHeadArmed = false
		}
		if !isSyntheticStreamKey(candidate.stream.streamID) {
			seenRealOpportunity = true
		}
	}

	maybeRebaseWFQState(state)
	return ordered
}

func appendRemainingInInputOrder(state *BatchState, ordered []int, size int) []int {
	if len(ordered) >= size {
		return ordered
	}
	selected := selectedSlice(state, size)
	for _, idx := range ordered {
		if idx >= 0 && idx < len(selected) {
			selected[idx] = true
		}
	}
	for idx := 0; idx < size; idx++ {
		if selected[idx] {
			continue
		}
		ordered = append(ordered, idx)
	}
	return ordered
}

func identityOrder(state *BatchState, n int) []int {
	out := orderedSlice(state, n, n)
	for i := range out {
		out[i] = i
	}
	return out
}

func orderUrgentBatch(items []BatchItem, state *BatchState) []int {
	order := identityOrder(state, len(items))
	sort.SliceStable(order, func(i, j int) bool {
		left := items[order[i]].Request
		right := items[order[j]].Request
		if left.UrgencyRank != right.UrgencyRank {
			return left.UrgencyRank < right.UrgencyRank
		}
		if left.StreamScoped != right.StreamScoped {
			return left.StreamScoped
		}
		if left.StreamScoped && left.StreamID != right.StreamID {
			return left.StreamID < right.StreamID
		}
		return false
	})
	return order
}

type batchTiePrefs struct {
	hasGroup bool
	group    GroupKey
	streams  map[GroupKey]uint64
}

func betterGroupCandidate(prefs batchTiePrefs, left, right wfqGroupCandidate) bool {
	if left.eligible != right.eligible {
		return left.eligible
	}
	if !left.eligible {
		if left.groupStart != right.groupStart {
			return left.groupStart < right.groupStart
		}
		if left.groupFinish != right.groupFinish {
			return left.groupFinish < right.groupFinish
		}
	} else {
		if left.groupFinish != right.groupFinish {
			return left.groupFinish < right.groupFinish
		}
		if left.groupStart != right.groupStart {
			return left.groupStart < right.groupStart
		}
	}
	if prefs.hasGroup {
		leftPreferred := left.groupKey == prefs.group
		rightPreferred := right.groupKey == prefs.group
		if leftPreferred != rightPreferred {
			return leftPreferred
		}
	}
	if left.stream.streamFinish != right.stream.streamFinish {
		return left.stream.streamFinish < right.stream.streamFinish
	}
	if left.stream.streamStart != right.stream.streamStart {
		return left.stream.streamStart < right.stream.streamStart
	}
	if left.groupLastServed != right.groupLastServed {
		return left.groupLastServed < right.groupLastServed
	}
	if left.stream.streamLastServed != right.stream.streamLastServed {
		return left.stream.streamLastServed < right.stream.streamLastServed
	}
	if left.groupOrder != right.groupOrder {
		return left.groupOrder < right.groupOrder
	}
	return left.stream.streamOrder < right.stream.streamOrder
}

func betterStreamCandidate(preferred uint64, left, right wfqStreamCandidate) bool {
	if left.eligible != right.eligible {
		return left.eligible
	}
	if !left.eligible {
		if left.streamStart != right.streamStart {
			return left.streamStart < right.streamStart
		}
		if left.streamFinish != right.streamFinish {
			return left.streamFinish < right.streamFinish
		}
	} else {
		if left.streamFinish != right.streamFinish {
			return left.streamFinish < right.streamFinish
		}
		if left.streamStart != right.streamStart {
			return left.streamStart < right.streamStart
		}
	}
	if preferred != 0 {
		leftPreferred := left.streamID == preferred
		rightPreferred := right.streamID == preferred
		if leftPreferred != rightPreferred {
			return leftPreferred
		}
	}
	if left.streamLastServed != right.streamLastServed {
		return left.streamLastServed < right.streamLastServed
	}
	return left.streamOrder < right.streamOrder
}

func recordPreferredHeads(state *BatchState, candidate wfqGroupCandidate, groupOrder []GroupKey, streamOrder map[GroupKey][]uint64, recordedBatchHead bool, recordedGroupHead []bool) {
	if state == nil {
		return
	}
	if !recordedBatchHead {
		if nextGroup, ok := nextGroupHead(groupOrder, candidate.groupOrder); ok {
			state.PreferredGroupHead = nextGroup
			state.HasPreferredGroupHead = true
		} else {
			state.PreferredGroupHead = GroupKey{}
			state.HasPreferredGroupHead = false
		}
	}
	if candidate.groupOrder < 0 || candidate.groupOrder >= len(recordedGroupHead) {
		return
	}
	if recordedGroupHead[candidate.groupOrder] {
		return
	}
	recordedGroupHead[candidate.groupOrder] = true
	if nextStream, ok := nextStreamHead(streamOrder[candidate.groupKey], candidate.stream.streamOrder); ok && !isSyntheticStreamKey(nextStream) {
		state.PreferredStreamHead[candidate.groupKey] = nextStream
		return
	}
	delete(state.PreferredStreamHead, candidate.groupKey)
}

func snapshotBatchTiePrefs(state *BatchState) batchTiePrefs {
	prefs := batchTiePrefs{}
	if state == nil {
		return prefs
	}
	prefs.hasGroup = state.HasPreferredGroupHead
	prefs.group = state.PreferredGroupHead
	if len(state.PreferredStreamHead) == 0 {
		return prefs
	}
	prefs.streams = tiePrefStreamsMap(state, len(state.PreferredStreamHead))
	for key, streamID := range state.PreferredStreamHead {
		prefs.streams[key] = streamID
	}
	return prefs
}

func nextGroupHead(groupOrder []GroupKey, selected int) (GroupKey, bool) {
	if len(groupOrder) < 2 || selected < 0 {
		return GroupKey{}, false
	}
	return groupOrder[(selected+1)%len(groupOrder)], true
}

func nextStreamHead(streams []uint64, selected int) (uint64, bool) {
	if len(streams) < 2 || selected < 0 {
		return 0, false
	}
	return streams[(selected+1)%len(streams)], true
}
