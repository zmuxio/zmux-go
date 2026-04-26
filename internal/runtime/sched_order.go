package runtime

import (
	"sort"

	"github.com/zmuxio/zmux-go/internal/wire"
)

type batchBuildResult struct {
	groupOrder          []GroupKey
	groupState          map[GroupKey]map[uint64][]int
	groups              []batchBuiltGroup
	streamOrder         map[GroupKey][]uint64
	queuedBytes         map[uint64]uint64
	streamMeta          map[uint64]StreamMeta
	preparedStreams     map[uint64]batchPreparedStream
	hasRealStreamScoped bool
	hasPriorityUpdate   bool
}

type batchBuiltGroup struct {
	key     GroupKey
	queues  map[uint64][]int
	streams []uint64
}

type batchPreparedStream struct {
	meta            StreamMeta
	selection       batchStreamSelection
	queuedBytes     uint64
	class           trafficClass
	selectionEpoch  uint32
	smallBurstArmed bool
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
	prepareBatchScratchForBuild(state, len(items))
	result := batchBuildResult{
		groupOrder:  groupOrderSlice(state, 0, len(items)),
		groupState:  groupStateMap(state, len(items)),
		groups:      groupBuildSlice(state, len(items)),
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
			result.streamOrder[req.GroupKey] = nextStreamOrderSlice(state)
		}
		streamKey := syntheticStreamKey(req, i)
		queue, ok := queues[streamKey]
		if !ok {
			result.streamOrder[req.GroupKey] = append(result.streamOrder[req.GroupKey], streamKey)
			queue = nextGroupQueueEntrySlice(state)
		}
		queues[streamKey] = append(queue, i)
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
	for _, groupKey := range result.groupOrder {
		result.groups = append(result.groups, batchBuiltGroup{
			key:     groupKey,
			queues:  result.groupState[groupKey],
			streams: result.streamOrder[groupKey],
		})
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

func pickNextTransientOrdinaryHead(groups []batchBuiltGroup) (groupIdx int, streamID uint64, reqIdx int, rest []int, ok bool) {
	for idx, group := range groups {
		if !isTransientGroupKey(group.key) {
			continue
		}
		for _, streamID := range group.streams {
			queue := group.queues[streamID]
			if len(queue) == 0 {
				continue
			}
			return idx, streamID, queue[0], removeQueueEntry(queue, 0), true
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
		order := identityOrder(state, len(items))
		if !retainedRealState {
			releaseIdleBatchStateStorage(state)
		}
		return order
	}

	transient := transientBatchState(state, len(items))
	tiePrefs := snapshotBatchTiePrefs(state)
	interactiveQuantum := SchedulerQuantum(cfg.MaxFramePayload)
	feedbackWindow := FeedbackWindow(cfg.SchedulerHint, cfg.MaxFramePayload)
	batchSeq := state.BatchSeq + 1
	preparedStreams := applyBatchStreamClasses(state, prepared, cfg.SchedulerHint, interactiveQuantum, batchSeq)
	prepared.preparedStreams = preparedStreams
	ordered := orderedSlice(state, 0, len(items))
	advisoryHeadArmed := true
	seenRealOpportunity := false
	transientHeadUsed := false
	recordedBatchHead := false
	recordedGroupHead := selectedSlice(state, len(prepared.groups))
	interactiveStreak := state.InteractiveStreak
	classSelectionsSinceBulk := state.ClassSelectionsSinceBulk
	bypassSelections := bypassSelectionsMap(state, len(prepared.streamMeta))
	var selectionEpoch uint32

	for len(ordered) < len(items) {
		if !seenRealOpportunity && !transientHeadUsed {
			if idx, streamID, reqIdx, rest, ok := pickNextTransientOrdinaryHead(prepared.groups); ok {
				ordered = append(ordered, reqIdx)
				prepared.groups[idx].queues[streamID] = rest
				transientHeadUsed = true
				continue
			}
		}

		advisoryOnly := advisoryHeadArmed && prepared.hasPriorityUpdate
		selectionEpoch++
		if selectionEpoch == 0 {
			selectionEpoch++
		}
		interactiveActive, bulkActive := refreshActiveStreamSelections(state, prepared, items, cfg, advisoryOnly, selectionEpoch)
		interactiveCandidates := interactiveCandidateSlice(state, len(prepared.groups))
		bulkCandidates := bulkCandidateSlice(state, len(prepared.groups))
		var interactiveGroupWeight uint64
		var bulkGroupWeight uint64
		var interactiveBest wfqGroupCandidate
		var bulkBest wfqGroupCandidate
		for groupOrder, group := range prepared.groups {
			pair := topCandidatesForGroupClasses(cfg, state, transient, prepared, tiePrefs, group, groupOrder, interactiveQuantum, feedbackWindow, interactiveActive, bulkActive, selectionEpoch, bypassSelections)
			if pair.hasInteractive {
				interactiveCandidates = append(interactiveCandidates, pair.interactive)
				interactiveGroupWeight = SaturatingAdd(interactiveGroupWeight, pair.interactive.groupWeight)
				if interactiveBest.stream.streamID == 0 || betterGroupCandidate(tiePrefs, pair.interactive, interactiveBest) {
					interactiveBest = pair.interactive
				}
			}
			if pair.hasBulk {
				bulkCandidates = append(bulkCandidates, pair.bulk)
				bulkGroupWeight = SaturatingAdd(bulkGroupWeight, pair.bulk.groupWeight)
				if bulkBest.stream.streamID == 0 || betterGroupCandidate(tiePrefs, pair.bulk, bulkBest) {
					bulkBest = pair.bulk
				}
			}
		}
		if len(interactiveCandidates) == 0 && len(bulkCandidates) == 0 {
			ordered = appendRemainingInInputOrder(state, ordered, len(items))
			break
		}

		selectedClass := chooseTrafficClass(tiePrefs, cfg.SchedulerHint, interactiveBest, bulkBest, interactiveStreak, classSelectionsSinceBulk)
		candidate := interactiveBest
		var candidates []wfqGroupCandidate
		var totalGroupWeight uint64
		var activeClassStreams []uint64
		switch selectedClass {
		case trafficClassBulk:
			if bulkBest.stream.streamID == 0 && interactiveBest.stream.streamID != 0 {
				candidate = interactiveBest
				candidates = interactiveCandidates
				totalGroupWeight = interactiveGroupWeight
				activeClassStreams = interactiveActive
			} else {
				candidate = bulkBest
				candidates = bulkCandidates
				totalGroupWeight = bulkGroupWeight
				activeClassStreams = bulkActive
			}
		default:
			if interactiveBest.stream.streamID == 0 && bulkBest.stream.streamID != 0 {
				candidate = bulkBest
				candidates = bulkCandidates
				totalGroupWeight = bulkGroupWeight
				activeClassStreams = bulkActive
			} else {
				candidate = interactiveBest
				candidates = interactiveCandidates
				totalGroupWeight = interactiveGroupWeight
				activeClassStreams = interactiveActive
			}
		}
		if candidate.stream.streamID == 0 {
			ordered = appendRemainingInInputOrder(state, ordered, len(items))
			break
		}

		ordered = append(ordered, candidate.stream.reqIdx)
		recordPreferredHeads(state, candidate, prepared.groups, recordedBatchHead, recordedGroupHead)
		if !recordedBatchHead {
			recordedBatchHead = true
		}
		updateLagFeedback(state, prepared, candidate, candidates, totalGroupWeight, feedbackWindow, selectionEpoch)
		updateBypassSelections(activeClassStreams, candidate.stream.streamID, bypassSelections)
		if preparedStream := prepared.preparedStreams[candidate.stream.streamID]; preparedStream.smallBurstArmed && preparedStream.queuedBytes <= interactiveQuantum {
			preparedStream.smallBurstArmed = false
			prepared.preparedStreams[candidate.stream.streamID] = preparedStream
			state.SmallBurstDisarmed[candidate.stream.streamID] = struct{}{}
		}
		prepared.groups[candidate.groupOrder].queues[candidate.stream.streamID] = removeQueueEntry(prepared.groups[candidate.groupOrder].queues[candidate.stream.streamID], candidate.stream.queuePos)
		consumePreparedQueuedBytes(prepared.queuedBytes, prepared.preparedStreams, candidate.stream.streamID, candidate.stream.cost)
		commitWFQSelection(state, transient, candidate, max64(totalGroupWeight, 1), max64(candidate.totalStreamWeight, 1))
		if candidate.stream.isPriorityUpdate {
			advisoryHeadArmed = false
		}
		if !isSyntheticStreamKey(candidate.stream.streamID) {
			seenRealOpportunity = true
			if candidate.class == trafficClassBulk {
				interactiveStreak = 0
				classSelectionsSinceBulk = 0
			} else {
				if interactiveStreak < ^uint32(0) {
					interactiveStreak++
				}
				if interactiveBest.stream.streamID != 0 && bulkBest.stream.streamID != 0 && classSelectionsSinceBulk < ^uint32(0) {
					classSelectionsSinceBulk++
				} else if interactiveBest.stream.streamID == 0 || bulkBest.stream.streamID == 0 {
					classSelectionsSinceBulk = 0
				}
			}
		}
	}

	retainBatchStreamClasses(state, prepared.preparedStreams, batchSeq, interactiveStreak, classSelectionsSinceBulk)
	maybeRebaseWFQState(state)
	return ordered
}

func applyBatchStreamClasses(state *BatchState, prepared batchBuildResult, hint wire.SchedulerHint, interactiveQuantum, batchSeq uint64) map[uint64]batchPreparedStream {
	preparedStreams := preparedStreamMap(state, len(prepared.streamMeta))
	for streamID, meta := range prepared.streamMeta {
		previous, ok := state.StreamClass[streamID]
		class := classifyStreamClass(prepared.queuedBytes[streamID], meta.Priority, hint, previous, ok, interactiveQuantum)
		lastSeen, seen := state.StreamLastSeenBatch[streamID]
		if !seen || batchSeq-lastSeen >= 2 {
			delete(state.SmallBurstDisarmed, streamID)
		}
		_, disarmed := state.SmallBurstDisarmed[streamID]
		preparedStreams[streamID] = batchPreparedStream{
			meta:            meta,
			queuedBytes:     prepared.queuedBytes[streamID],
			class:           class,
			smallBurstArmed: !disarmed,
		}
	}
	return preparedStreams
}

func retainBatchStreamClasses(state *BatchState, preparedStreams map[uint64]batchPreparedStream, batchSeq uint64, interactiveStreak, classSelectionsSinceBulk uint32) {
	for streamID, preparedStream := range preparedStreams {
		state.StreamClass[streamID] = preparedStream.class
		state.StreamLastSeenBatch[streamID] = batchSeq
	}
	state.BatchSeq = batchSeq
	state.InteractiveStreak = interactiveStreak
	state.ClassSelectionsSinceBulk = classSelectionsSinceBulk
}

func refreshActiveStreamSelections(state *BatchState, prepared batchBuildResult, items []BatchItem, cfg BatchConfig, advisoryOnly bool, selectionEpoch uint32) ([]uint64, []uint64) {
	interactiveActive := interactiveActiveStreamSlice(state, len(prepared.streamMeta))
	bulkActive := bulkActiveStreamSlice(state, len(prepared.streamMeta))
	for _, group := range prepared.groups {
		for _, streamID := range group.streams {
			preparedStream, ok := prepared.preparedStreams[streamID]
			if !ok {
				continue
			}
			reqIdx, pos, cost, isPriorityUpdate, ok := selectStreamCandidate(group.queues[streamID], items, advisoryOnly)
			if !ok {
				continue
			}
			preparedStream.selection = batchStreamSelection{
				reqIdx:           reqIdx,
				queuePos:         pos,
				cost:             cost,
				baseWeight:       StreamWeight(preparedStream.meta.Priority, preparedStream.queuedBytes, cfg.SchedulerHint, cfg.MaxFramePayload),
				isPriorityUpdate: isPriorityUpdate,
			}
			preparedStream.selectionEpoch = selectionEpoch
			prepared.preparedStreams[streamID] = preparedStream
			if preparedStream.class == trafficClassBulk {
				bulkActive = append(bulkActive, streamID)
			} else {
				interactiveActive = append(interactiveActive, streamID)
			}
		}
	}
	return interactiveActive, bulkActive
}

type groupClassCandidatePair struct {
	interactive    wfqGroupCandidate
	bulk           wfqGroupCandidate
	hasInteractive bool
	hasBulk        bool
}

func topCandidatesForGroupClasses(cfg BatchConfig, state *BatchState, transient batchTransientState, prepared batchBuildResult, prefs batchTiePrefs, group batchBuiltGroup, groupOrder int, interactiveQuantum uint64, feedbackWindow int64, interactiveActive, bulkActive []uint64, selectionEpoch uint32, bypassSelections map[uint64]int) groupClassCandidatePair {
	groupVirtual := groupVirtualTime(state, transient, group.key)
	groupFinishBase := groupFinishTag(state, transient, group.key)
	groupLagValue := groupLag(state, group.key)
	groupLastServedValue := groupLastServed(state, transient, group.key)
	freshGroup := isFreshGroup(state, group.key)
	rootVirtual := state.RootVirtualTime
	preferredStream := uint64(0)
	if prefs.streams != nil {
		preferredStream = prefs.streams[group.key]
	}
	var interactiveTop wfqStreamCandidate
	var bulkTop wfqStreamCandidate
	var totalInteractiveBase uint64
	var totalInteractiveWeight uint64
	var totalBulkBase uint64
	var totalBulkWeight uint64
	var hasInteractiveTop bool
	var hasBulkTop bool
	for streamIdx, streamID := range group.streams {
		preparedStream, ok := prepared.preparedStreams[streamID]
		if !ok || preparedStream.selectionEpoch != selectionEpoch {
			continue
		}
		class := preparedStream.class
		selection := preparedStream.selection
		baseWeight := selection.baseWeight
		lagAdjustedWeight := AdjustWeightForLag(baseWeight, streamLag(state, streamID), feedbackWindow, isFreshStream(state, streamID))
		activeClassStreams := len(interactiveActive)
		if class == trafficClassBulk {
			activeClassStreams = len(bulkActive)
		}
		effectiveWeight := classAdjustedWeight(baseWeight, lagAdjustedWeight, preparedStream.queuedBytes, preparedStream.smallBurstArmed, interactiveQuantum, shouldApplyAging(streamID, activeClassStreams, bypassSelections))
		streamStart := max64(streamFinishTag(state, transient, streamID), groupVirtual)
		streamFinish := SaturatingAdd(streamStart, serviceTag(selection.cost, max64(effectiveWeight, 1)))
		candidate := wfqStreamCandidate{
			streamID:         streamID,
			reqIdx:           selection.reqIdx,
			queuePos:         selection.queuePos,
			cost:             selection.cost,
			baseWeight:       baseWeight,
			weight:           effectiveWeight,
			streamVirtual:    groupVirtual,
			streamStart:      streamStart,
			streamFinish:     streamFinish,
			streamLastServed: streamLastServed(state, transient, streamID),
			eligible:         streamStart <= groupVirtual,
			isPriorityUpdate: selection.isPriorityUpdate,
			streamOrder:      streamIdx,
		}
		if class == trafficClassBulk {
			totalBulkBase = SaturatingAdd(totalBulkBase, baseWeight)
			totalBulkWeight = SaturatingAdd(totalBulkWeight, effectiveWeight)
			if !hasBulkTop || betterStreamCandidate(preferredStream, candidate, bulkTop) {
				bulkTop = candidate
				hasBulkTop = true
			}
			continue
		}
		totalInteractiveBase = SaturatingAdd(totalInteractiveBase, baseWeight)
		totalInteractiveWeight = SaturatingAdd(totalInteractiveWeight, effectiveWeight)
		if !hasInteractiveTop || betterStreamCandidate(preferredStream, candidate, interactiveTop) {
			interactiveTop = candidate
			hasInteractiveTop = true
		}
	}
	return groupClassCandidatePair{
		interactive:    buildGroupClassCandidateWithGroupState(cfg, group.key, groupOrder, trafficClassInteractive, interactiveTop, totalInteractiveBase, totalInteractiveWeight, feedbackWindow, rootVirtual, groupFinishBase, groupLastServedValue, groupLagValue, freshGroup, hasInteractiveTop),
		bulk:           buildGroupClassCandidateWithGroupState(cfg, group.key, groupOrder, trafficClassBulk, bulkTop, totalBulkBase, totalBulkWeight, feedbackWindow, rootVirtual, groupFinishBase, groupLastServedValue, groupLagValue, freshGroup, hasBulkTop),
		hasInteractive: hasInteractiveTop,
		hasBulk:        hasBulkTop,
	}
}

func buildGroupClassCandidateWithGroupState(cfg BatchConfig, groupKey GroupKey, groupOrder int, class trafficClass, top wfqStreamCandidate, totalBaseStreamWeight, totalStreamWeight uint64, feedbackWindow int64, rootVirtual, groupFinishBase, groupLastServedValue uint64, groupLagValue int64, freshGroup bool, hasTop bool) wfqGroupCandidate {
	if !hasTop {
		return wfqGroupCandidate{}
	}
	baseGroupWeight := GroupWeight(groupKey, top.baseWeight, cfg.SchedulerHint)
	groupWeight := AdjustWeightForLag(baseGroupWeight, groupLagValue, feedbackWindow, freshGroup)
	groupStart := max64(groupFinishBase, rootVirtual)
	groupFinish := SaturatingAdd(groupStart, serviceTag(top.cost, max64(groupWeight, 1)))
	return wfqGroupCandidate{
		groupKey:              groupKey,
		groupVirtual:          rootVirtual,
		groupStart:            groupStart,
		groupFinish:           groupFinish,
		groupLastServed:       groupLastServedValue,
		eligible:              groupStart <= rootVirtual && top.eligible,
		groupOrder:            groupOrder,
		class:                 class,
		baseGroupWeight:       max64(baseGroupWeight, 1),
		groupWeight:           max64(groupWeight, 1),
		totalBaseStreamWeight: max64(totalBaseStreamWeight, 1),
		totalStreamWeight:     max64(totalStreamWeight, 1),
		stream:                top,
	}
}

func updateLagFeedback(state *BatchState, prepared batchBuildResult, chosen wfqGroupCandidate, candidates []wfqGroupCandidate, totalGroupWeight uint64, feedbackWindow int64, selectionEpoch uint32) {
	if state == nil || feedbackWindow <= 0 || chosen.stream.streamID == 0 {
		return
	}
	cost := normalizeCost(chosen.stream.cost)
	for _, candidate := range candidates {
		if candidate.baseGroupWeight == 0 {
			continue
		}
		expected := fairShare(cost, candidate.baseGroupWeight, max64(totalGroupWeight, 1))
		actual := int64(0)
		if candidate.groupKey == chosen.groupKey {
			actual = cost
		}
		setGroupLag(state, candidate.groupKey, clampLag(groupLag(state, candidate.groupKey)+expected-actual, feedbackWindow))
	}

	for _, streamID := range prepared.groups[chosen.groupOrder].streams {
		preparedStream, ok := prepared.preparedStreams[streamID]
		if !ok || preparedStream.selectionEpoch != selectionEpoch || preparedStream.class != chosen.class {
			continue
		}
		expected := fairShare(cost, preparedStream.selection.baseWeight, max64(chosen.totalBaseStreamWeight, 1))
		actual := int64(0)
		if streamID == chosen.stream.streamID {
			actual = cost
		}
		setStreamLag(state, streamID, clampLag(streamLag(state, streamID)+expected-actual, feedbackWindow))
	}
}

func updateBypassSelections(activeStreams []uint64, selectedStreamID uint64, bypassSelections map[uint64]int) {
	for _, streamID := range activeStreams {
		if streamID == selectedStreamID {
			bypassSelections[streamID] = 0
			continue
		}
		bypassSelections[streamID]++
	}
}

func shouldApplyAging(streamID uint64, activeClassStreams int, bypassSelections map[uint64]int) bool {
	if activeClassStreams <= 1 {
		return false
	}
	return bypassSelections[streamID] >= activeClassStreams*agingRoundThreshold
}

func classAdjustedWeight(baseWeight, effectiveWeight, queuedBytes uint64, smallBurstArmed bool, interactiveQuantum uint64, ageBoost bool) uint64 {
	adjusted := max64(effectiveWeight, 1)
	if smallBurstArmed && queuedBytes <= interactiveQuantum {
		adjusted = SaturatingAdd(adjusted, max64(baseWeight, 1))
	}
	if ageBoost {
		boost := max64(baseWeight, max64(adjusted/2, 1))
		adjusted = SaturatingAdd(adjusted, boost)
	}
	return max64(adjusted, 1)
}

func consumeQueuedBytes(queuedBytes map[uint64]uint64, streamID uint64, cost int64) {
	delta := uint64(normalizeCost(cost))
	if queuedBytes[streamID] <= delta {
		delete(queuedBytes, streamID)
		return
	}
	queuedBytes[streamID] -= delta
}

func consumePreparedQueuedBytes(queuedBytes map[uint64]uint64, preparedStreams map[uint64]batchPreparedStream, streamID uint64, cost int64) {
	consumeQueuedBytes(queuedBytes, streamID, cost)
	preparedStream, ok := preparedStreams[streamID]
	if !ok {
		return
	}
	preparedStream.queuedBytes = queuedBytes[streamID]
	preparedStreams[streamID] = preparedStream
}

func selectStreamCandidate(queue []int, items []BatchItem, advisoryOnly bool) (reqIdx int, pos int, cost int64, isPriorityUpdate bool, ok bool) {
	reqIdx, pos, cost, isPriorityUpdate, ok = peekStreamCandidate(queue, items)
	if !ok {
		return 0, 0, 0, false, false
	}
	if advisoryOnly && !isPriorityUpdate {
		return 0, 0, 0, false, false
	}
	return reqIdx, pos, cost, isPriorityUpdate, true
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

func betterEligibleWindow(leftEligible bool, leftStart, leftFinish, rightStart, rightFinish uint64) (bool, bool) {
	if !leftEligible {
		if leftStart != rightStart {
			return leftStart < rightStart, true
		}
		if leftFinish != rightFinish {
			return leftFinish < rightFinish, true
		}
		return false, false
	}
	if leftFinish != rightFinish {
		return leftFinish < rightFinish, true
	}
	if leftStart != rightStart {
		return leftStart < rightStart, true
	}
	return false, false
}

func betterGroupCandidate(prefs batchTiePrefs, left, right wfqGroupCandidate) bool {
	if left.eligible != right.eligible {
		return left.eligible
	}
	if better, ok := betterEligibleWindow(left.eligible, left.groupStart, left.groupFinish, right.groupStart, right.groupFinish); ok {
		return better
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
	if better, ok := betterEligibleWindow(left.eligible, left.streamStart, left.streamFinish, right.streamStart, right.streamFinish); ok {
		return better
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

func recordPreferredHeads(state *BatchState, candidate wfqGroupCandidate, groups []batchBuiltGroup, recordedBatchHead bool, recordedGroupHead []bool) {
	if state == nil || isSyntheticStreamKey(candidate.stream.streamID) {
		return
	}
	if !recordedBatchHead {
		if nextGroup, ok := nextRealGroupHead(groups, candidate.groupOrder); ok {
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
	if nextStream, ok := nextRealStreamHead(groups[candidate.groupOrder].streams, candidate.stream.streamOrder); ok {
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

func nextRealGroupHead(groups []batchBuiltGroup, selected int) (GroupKey, bool) {
	if len(groups) < 2 || selected < 0 {
		return GroupKey{}, false
	}
	for offset := 1; offset < len(groups); offset++ {
		next := groups[(selected+offset)%len(groups)].key
		if !isTransientGroupKey(next) {
			return next, true
		}
	}
	return GroupKey{}, false
}

func nextRealStreamHead(streams []uint64, selected int) (uint64, bool) {
	if len(streams) < 2 || selected < 0 {
		return 0, false
	}
	for offset := 1; offset < len(streams); offset++ {
		next := streams[(selected+offset)%len(streams)]
		if !isSyntheticStreamKey(next) {
			return next, true
		}
	}
	return 0, false
}
