package runtime

type wfqActiveGroup struct {
	key                   GroupKey
	order                 int
	queues                map[uint64][]int
	streams               []uint64
	top                   wfqStreamCandidate
	hasTop                bool
	baseGroupWeight       uint64
	groupWeight           uint64
	totalBaseStreamWeight uint64
	totalStreamWeight     uint64
}

type batchWFQActive struct {
	state             *BatchState
	cfg               BatchConfig
	transient         batchTransientState
	items             []BatchItem
	queuedBytes       map[uint64]uint64
	streamMeta        map[uint64]StreamMeta
	tiePrefs          batchTiePrefs
	groupHeap         wfqGroupHeap
	activeGroups      []wfqActiveGroup
	totalGroupWeight  uint64
	hasPriorityUpdate bool
	advisoryOnly      bool
}

type wfqGroupHeap struct {
	items []wfqGroupCandidate
	prefs batchTiePrefs
}

func (h *wfqGroupHeap) push(candidate wfqGroupCandidate) {
	h.items = append(h.items, candidate)
	h.up(len(h.items) - 1)
}

func (h *wfqGroupHeap) pop() (wfqGroupCandidate, bool) {
	if len(h.items) == 0 {
		return wfqGroupCandidate{}, false
	}
	best := h.items[0]
	last := len(h.items) - 1
	h.items[0] = h.items[last]
	h.items = h.items[:last]
	if len(h.items) != 0 {
		h.down(0)
	}
	return best, true
}

func (h *wfqGroupHeap) less(i, j int) bool {
	return betterGroupCandidate(h.prefs, h.items[i], h.items[j])
}

func (h *wfqGroupHeap) up(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !h.less(idx, parent) {
			return
		}
		h.items[idx], h.items[parent] = h.items[parent], h.items[idx]
		idx = parent
	}
}

func (h *wfqGroupHeap) down(idx int) {
	for {
		left := idx*2 + 1
		if left >= len(h.items) {
			return
		}
		best := left
		right := left + 1
		if right < len(h.items) && h.less(right, best) {
			best = right
		}
		if !h.less(best, idx) {
			return
		}
		h.items[idx], h.items[best] = h.items[best], h.items[idx]
		idx = best
	}
}

func activeGroupSlice(state *BatchState, capHint int) []wfqActiveGroup {
	if state == nil {
		return make([]wfqActiveGroup, 0, capHint)
	}
	if batchScratchOversized(cap(state.scratch.activeGroups), capHint) {
		state.scratch.activeGroups = nil
	}
	if cap(state.scratch.activeGroups) < capHint {
		state.scratch.activeGroups = make([]wfqActiveGroup, 0, capHint)
	} else {
		if len(state.scratch.activeGroups) > 0 {
			clear(state.scratch.activeGroups)
		}
		state.scratch.activeGroups = state.scratch.activeGroups[:0]
	}
	return state.scratch.activeGroups
}

func groupCandidateSlice(state *BatchState, n int, capHint int) []wfqGroupCandidate {
	if state == nil {
		return make([]wfqGroupCandidate, n, capHint)
	}
	if batchScratchOversized(cap(state.scratch.groupCandidates), capHint) {
		state.scratch.groupCandidates = nil
	}
	if cap(state.scratch.groupCandidates) < capHint {
		state.scratch.groupCandidates = make([]wfqGroupCandidate, n, capHint)
	} else {
		state.scratch.groupCandidates = state.scratch.groupCandidates[:n]
	}
	return state.scratch.groupCandidates
}

func newBatchWFQActive(
	cfg BatchConfig,
	state *BatchState,
	transient batchTransientState,
	prepared batchBuildResult,
	items []BatchItem,
	tiePrefs batchTiePrefs,
) *batchWFQActive {
	activeGroups := activeGroupSlice(state, len(prepared.groupOrder))
	for i, groupKey := range prepared.groupOrder {
		activeGroups = append(activeGroups, wfqActiveGroup{
			key:     groupKey,
			order:   i,
			queues:  prepared.groupState[groupKey],
			streams: prepared.streamOrder[groupKey],
		})
	}

	scheduler := &batchWFQActive{
		state:        state,
		cfg:          cfg,
		transient:    transient,
		items:        items,
		queuedBytes:  prepared.queuedBytes,
		streamMeta:   prepared.streamMeta,
		tiePrefs:     tiePrefs,
		groupHeap:    wfqGroupHeap{prefs: tiePrefs, items: groupCandidateSlice(state, 0, len(prepared.groupOrder))},
		activeGroups: activeGroups,
	}
	scheduler.hasPriorityUpdate = prepared.hasPriorityUpdate
	scheduler.refreshAdvisoryMode(true)
	return scheduler
}

func (s *batchWFQActive) next(advisoryHeadArmed bool) (wfqGroupCandidate, bool) {
	if s == nil {
		return wfqGroupCandidate{}, false
	}
	s.refreshAdvisoryMode(advisoryHeadArmed)
	s.rebuildGroupHeap()
	return s.groupHeap.pop()
}

func (s *batchWFQActive) commit(candidate wfqGroupCandidate) {
	if s == nil {
		return
	}
	group := &s.activeGroups[candidate.groupOrder]
	previousStreamWeight := group.totalStreamWeight
	commitWFQSelection(s.state, s.transient, candidate, s.totalGroupWeight, previousStreamWeight)
	s.updateFeedback(candidate)
	group.queues[candidate.stream.streamID] = removeQueueEntry(group.queues[candidate.stream.streamID], candidate.stream.queuePos)
	s.consumeQueuedBytes(candidate.stream.streamID, candidate.stream.cost)
	rebuildGroupTop(s, group)
	s.refreshGroupWeights()
}

func (s *batchWFQActive) refreshAdvisoryMode(advisoryHeadArmed bool) {
	advisoryOnly := advisoryHeadArmed && s.hasPriorityUpdate
	if advisoryOnly == s.advisoryOnly && len(s.groupHeap.items) != 0 {
		return
	}
	s.advisoryOnly = advisoryOnly
	for i := range s.activeGroups {
		rebuildGroupTop(s, &s.activeGroups[i])
	}
	s.refreshGroupWeights()
}

func (s *batchWFQActive) rebuildGroupHeap() {
	if s == nil {
		return
	}
	s.groupHeap.items = s.groupHeap.items[:0]
	for i := range s.activeGroups {
		group := &s.activeGroups[i]
		if !group.hasTop {
			continue
		}
		s.groupHeap.push(groupCandidateFor(s.state, s.transient, s.cfg, *group))
	}
}

func rebuildGroupTop(s *batchWFQActive, group *wfqActiveGroup) {
	if s == nil || group == nil {
		return
	}
	group.hasTop = false
	group.top = wfqStreamCandidate{}
	group.baseGroupWeight = 0
	group.groupWeight = 0
	group.totalBaseStreamWeight = 0
	group.totalStreamWeight = 0

	groupVirtual := groupVirtualTime(s.state, s.transient, group.key)
	preferredStream := uint64(0)
	if s.tiePrefs.streams != nil {
		preferredStream = s.tiePrefs.streams[group.key]
	}

	for streamIdx, streamID := range group.streams {
		queue := group.queues[streamID]
		if len(queue) == 0 {
			continue
		}
		reqIdx, pos, cost, isPriorityUpdate, ok := peekStreamCandidate(queue, s.items)
		if !ok {
			continue
		}
		if s.advisoryOnly && !isPriorityUpdate {
			continue
		}
		meta := s.streamMeta[streamID]
		baseWeight := StreamWeight(meta.Priority, s.queuedBytes[streamID], s.cfg.SchedulerHint, s.cfg.MaxFramePayload)
		effectiveWeight := AdjustWeightForLag(baseWeight, streamLag(s.state, streamID), FeedbackWindow(s.cfg.SchedulerHint, s.cfg.MaxFramePayload), isFreshStream(s.state, streamID))
		group.totalBaseStreamWeight = SaturatingAdd(group.totalBaseStreamWeight, baseWeight)
		group.totalStreamWeight = SaturatingAdd(group.totalStreamWeight, effectiveWeight)
		streamStart := max64(streamFinishTag(s.state, s.transient, streamID), groupVirtual)
		streamFinish := SaturatingAdd(streamStart, serviceTag(cost, effectiveWeight))
		candidate := wfqStreamCandidate{
			streamID:         streamID,
			reqIdx:           reqIdx,
			queuePos:         pos,
			cost:             cost,
			baseWeight:       baseWeight,
			weight:           effectiveWeight,
			streamVirtual:    groupVirtual,
			streamStart:      streamStart,
			streamFinish:     streamFinish,
			streamLastServed: streamLastServed(s.state, s.transient, streamID),
			eligible:         streamStart <= groupVirtual,
			isPriorityUpdate: isPriorityUpdate,
			streamOrder:      streamIdx,
		}
		if !group.hasTop || betterStreamCandidate(preferredStream, candidate, group.top) {
			group.top = candidate
			group.hasTop = true
		}
	}
	if group.hasTop {
		group.baseGroupWeight = GroupWeight(group.key, group.top.baseWeight, s.cfg.SchedulerHint)
		group.groupWeight = AdjustWeightForLag(group.baseGroupWeight, groupLag(s.state, group.key), FeedbackWindow(s.cfg.SchedulerHint, s.cfg.MaxFramePayload), isFreshGroup(s.state, group.key))
	}
}

func (s *batchWFQActive) refreshGroupWeights() {
	if s == nil {
		return
	}
	window := FeedbackWindow(s.cfg.SchedulerHint, s.cfg.MaxFramePayload)
	s.totalGroupWeight = 0
	for i := range s.activeGroups {
		group := &s.activeGroups[i]
		if !group.hasTop {
			group.baseGroupWeight = 0
			group.groupWeight = 0
			continue
		}
		group.baseGroupWeight = GroupWeight(group.key, group.top.baseWeight, s.cfg.SchedulerHint)
		group.groupWeight = AdjustWeightForLag(group.baseGroupWeight, groupLag(s.state, group.key), window, isFreshGroup(s.state, group.key))
		s.totalGroupWeight = SaturatingAdd(s.totalGroupWeight, group.groupWeight)
	}
}

func groupCandidateFor(state *BatchState, transient batchTransientState, cfg BatchConfig, group wfqActiveGroup) wfqGroupCandidate {
	groupVirtual := state.RootVirtualTime
	groupWeight := group.groupWeight
	if groupWeight == 0 {
		groupWeight = GroupWeight(group.key, group.top.weight, cfg.SchedulerHint)
	}
	groupStart := max64(groupFinishTag(state, transient, group.key), groupVirtual)
	groupFinish := SaturatingAdd(groupStart, serviceTag(group.top.cost, groupWeight))
	return wfqGroupCandidate{
		groupKey:        group.key,
		groupVirtual:    groupVirtual,
		groupStart:      groupStart,
		groupFinish:     groupFinish,
		groupLastServed: groupLastServed(state, transient, group.key),
		eligible:        groupStart <= groupVirtual && group.top.eligible,
		groupOrder:      group.order,
		stream:          group.top,
	}
}

func (s *batchWFQActive) consumeQueuedBytes(streamID uint64, cost int64) {
	if s == nil {
		return
	}
	delta := uint64(normalizeCost(cost))
	current := s.queuedBytes[streamID]
	if current <= delta {
		delete(s.queuedBytes, streamID)
		return
	}
	s.queuedBytes[streamID] = current - delta
}

func (s *batchWFQActive) updateFeedback(candidate wfqGroupCandidate) {
	if s == nil || s.state == nil {
		return
	}
	window := FeedbackWindow(s.cfg.SchedulerHint, s.cfg.MaxFramePayload)
	if window <= 0 {
		return
	}
	cost := normalizeCost(candidate.stream.cost)
	selectedGroup := &s.activeGroups[candidate.groupOrder]

	totalGroupWeight := max64(s.totalGroupWeight, 1)
	for i := range s.activeGroups {
		group := &s.activeGroups[i]
		if group.groupWeight == 0 {
			continue
		}
		expected := fairShare(cost, group.baseGroupWeight, totalGroupWeight)
		actual := int64(0)
		if i == candidate.groupOrder {
			actual = cost
		}
		setGroupLag(s.state, group.key, clampLag(groupLag(s.state, group.key)+expected-actual, window))
	}

	totalStreamWeight := max64(selectedGroup.totalBaseStreamWeight, 1)
	for _, streamID := range selectedGroup.streams {
		queue := selectedGroup.queues[streamID]
		if len(queue) == 0 {
			continue
		}
		meta := s.streamMeta[streamID]
		baseWeight := StreamWeight(meta.Priority, s.queuedBytes[streamID], s.cfg.SchedulerHint, s.cfg.MaxFramePayload)
		expected := fairShare(cost, baseWeight, totalStreamWeight)
		actual := int64(0)
		if streamID == candidate.stream.streamID {
			actual = cost
		}
		setStreamLag(s.state, streamID, clampLag(streamLag(s.state, streamID)+expected-actual, window))
	}
}

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
