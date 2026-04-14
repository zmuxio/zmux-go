package runtime

import "testing"

func TestOrderUrgentBatchOrdersSameRankStreamScopedByAscendingStreamID(t *testing.T) {
	t.Parallel()

	reqs := []RequestMeta{
		{UrgencyRank: 2, StreamScoped: true, StreamID: 8, Cost: 1},
		{UrgencyRank: 2, StreamScoped: false, Cost: 1},
		{UrgencyRank: 2, StreamScoped: true, StreamID: 4, Cost: 1},
		{UrgencyRank: 1, StreamScoped: true, StreamID: 12, Cost: 1},
	}

	order := orderBatchIndices(BatchConfig{Urgent: true}, &BatchState{}, reqs, nil)
	if got := order; len(got) != 4 || got[0] != 3 || got[1] != 2 || got[2] != 0 || got[3] != 1 {
		t.Fatalf("urgent order = %v, want [3 2 0 1]", got)
	}
}

func TestOrderBatchIndicesDoesNotRetainSessionScopedSyntheticState(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	reqs := []RequestMeta{
		{GroupKey: GroupKey{Kind: 2, Value: 0}, Cost: 1},
		{GroupKey: GroupKey{Kind: 2, Value: 1}, Cost: 2},
	}

	order := orderBatchIndices(BatchConfig{GroupFair: true, MaxFramePayload: 16384}, state, reqs, nil)
	if len(order) != len(reqs) {
		t.Fatalf("order len = %d, want %d", len(order), len(reqs))
	}
	if len(state.StreamFinishTag) != 0 || len(state.StreamLastService) != 0 {
		t.Fatalf("retained synthetic stream state = %#v, want empty", state)
	}
	if len(state.GroupVirtualTime) != 0 || len(state.GroupFinishTag) != 0 || len(state.GroupLastService) != 0 {
		t.Fatalf("retained synthetic group state = %#v, want empty", state)
	}
	if state.RootVirtualTime != 0 || state.ServiceSeq != 0 {
		t.Fatalf("retained scheduler clock = (%d,%d), want (0,0)", state.RootVirtualTime, state.ServiceSeq)
	}
}

func TestOrderBatchIndicesSessionScopedOrdinaryPreservesRetainedWFQState(t *testing.T) {
	t.Parallel()

	state := &BatchState{
		RootVirtualTime: 77,
		ServiceSeq:      9,
		GroupVirtualTime: map[GroupKey]uint64{
			{Kind: 0, Value: 4}: 21,
		},
		GroupFinishTag: map[GroupKey]uint64{
			{Kind: 0, Value: 4}: 33,
		},
		GroupLastService: map[GroupKey]uint64{
			{Kind: 0, Value: 4}: 5,
		},
		StreamFinishTag:   map[uint64]uint64{4: 21},
		StreamLastService: map[uint64]uint64{4: 5},
	}
	reqs := []RequestMeta{
		{GroupKey: GroupKey{Kind: 2, Value: 0}, Cost: 1},
		{GroupKey: GroupKey{Kind: 2, Value: 1}, Cost: 2},
	}

	order := orderBatchIndices(BatchConfig{MaxFramePayload: 16384}, state, reqs, nil)
	if len(order) != len(reqs) {
		t.Fatalf("order len = %d, want %d", len(order), len(reqs))
	}
	if got := state.RootVirtualTime; got != 77 {
		t.Fatalf("root virtual time = %d, want 77", got)
	}
	if got := state.ServiceSeq; got != 9 {
		t.Fatalf("service seq = %d, want 9", got)
	}
}

func TestOrderBatchIndicesInterleavesEqualStreamsWithinBatch(t *testing.T) {
	t.Parallel()

	reqs := []RequestMeta{
		streamReq(4, 1),
		streamReq(4, 1),
		streamReq(8, 1),
	}

	order := orderBatchIndices(BatchConfig{MaxFramePayload: 16384}, &BatchState{}, reqs, map[uint64]StreamMeta{
		4: {},
		8: {},
	})
	if got := []uint64{reqs[order[0]].StreamID, reqs[order[1]].StreamID, reqs[order[2]].StreamID}; !equalUint64s(got, []uint64{4, 8, 4}) {
		t.Fatalf("equal-stream order = %v, want [4 8 4]", got)
	}
}

func TestOrderBatchIndicesAdvancesRootVirtualTimeByActiveGroupWeight(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	reqs := []RequestMeta{
		streamReq(4, 1),
		streamReq(8, 1),
	}

	order := orderBatchIndices(BatchConfig{MaxFramePayload: 16384}, state, reqs, map[uint64]StreamMeta{
		4: {},
		8: {},
	})
	if got := []uint64{reqs[order[0]].StreamID, reqs[order[1]].StreamID}; !equalUint64s(got, []uint64{4, 8}) {
		t.Fatalf("equal-stream order = %v, want [4 8]", got)
	}
	if got := state.RootVirtualTime; got != 9 {
		t.Fatalf("root virtual time = %d, want 9", got)
	}
	if got := state.ServiceSeq; got != 2 {
		t.Fatalf("service seq = %d, want 2", got)
	}
}

func TestOrderBatchIndicesAdvancesGroupVirtualTimeByActiveStreamWeight(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	groupKey := GroupKey{Kind: 1, Value: 7}
	reqs := []RequestMeta{
		{GroupKey: groupKey, StreamID: 4, StreamScoped: true, Cost: 1},
		{GroupKey: groupKey, StreamID: 8, StreamScoped: true, Cost: 1},
	}

	order := orderBatchIndices(BatchConfig{GroupFair: true, MaxFramePayload: 16384}, state, reqs, map[uint64]StreamMeta{
		4: {},
		8: {},
	})
	if got := []uint64{reqs[order[0]].StreamID, reqs[order[1]].StreamID}; !equalUint64s(got, []uint64{4, 8}) {
		t.Fatalf("single-group order = %v, want [4 8]", got)
	}
	if got := state.RootVirtualTime; got != 19 {
		t.Fatalf("root virtual time = %d, want 19", got)
	}
	if got := state.GroupVirtualTime[groupKey]; got != 11 {
		t.Fatalf("group virtual time = %d, want 11", got)
	}
}

func TestOrderBatchIndicesFeedbackLetsFreshPeerBeatStalePreferredHead(t *testing.T) {
	t.Parallel()

	state := &BatchState{
		GroupVirtualTime: map[GroupKey]uint64{
			{Kind: 0, Value: 4}: 0,
		},
		GroupFinishTag: map[GroupKey]uint64{
			{Kind: 0, Value: 4}: 0,
		},
		GroupLastService: map[GroupKey]uint64{
			{Kind: 0, Value: 4}: 1,
		},
		StreamFinishTag: map[uint64]uint64{
			4: 0,
		},
		StreamLastService: map[uint64]uint64{
			4: 1,
		},
		HasPreferredGroupHead: true,
		PreferredGroupHead:    GroupKey{Kind: 0, Value: 4},
	}
	reqs := []RequestMeta{
		streamReq(4, 1),
		streamReq(8, 1),
	}

	order := orderBatchIndices(BatchConfig{MaxFramePayload: 16384}, state, reqs, map[uint64]StreamMeta{
		4: {},
		8: {},
	})
	if got := reqs[order[0]].StreamID; got != 8 {
		t.Fatalf("first selected stream = %d, want fresh stream 8", got)
	}
}

func TestOrderBatchIndicesKeepsSessionScopedHeadOutOfWFQCompetition(t *testing.T) {
	t.Parallel()

	reqs := []RequestMeta{
		{GroupKey: GroupKey{Kind: 2, Value: 0}, Cost: 1},
		streamReq(4, 40000),
		streamReq(8, 40000),
	}

	order := orderBatchIndices(BatchConfig{MaxFramePayload: 16384}, &BatchState{}, reqs, map[uint64]StreamMeta{
		4: {},
		8: {Priority: 20},
	})
	if got := []int{order[0], order[1], order[2]}; got[0] != 0 || reqs[got[1]].StreamID != 8 || reqs[got[2]].StreamID != 4 {
		t.Fatalf("mixed session/data order = %v, want session head then [8 4]", got)
	}
}

func TestOrderBatchIndicesGivesPriorityUpdateOneCrossStreamHeadOpportunity(t *testing.T) {
	t.Parallel()

	reqs := []RequestMeta{
		streamReq(8, 1),
		streamReq(4, 1),
		{
			GroupKey:         GroupKey{Kind: 0, Value: 4},
			StreamID:         4,
			StreamScoped:     true,
			IsPriorityUpdate: true,
			Cost:             1,
		},
	}

	order := orderBatchIndices(BatchConfig{MaxFramePayload: 16384}, &BatchState{}, reqs, map[uint64]StreamMeta{
		4: {},
		8: {},
	})
	if got := order; len(got) != 3 || got[0] != 2 || reqs[got[1]].StreamID != 8 || reqs[got[2]].StreamID != 4 {
		t.Fatalf("priority-update order = %v, want [2 0 1]", got)
	}
}

func TestOrderBatchIndicesPrefersEligibleStreamOverLowerFinishIneligibleStream(t *testing.T) {
	t.Parallel()

	state := &BatchState{
		GroupVirtualTime: map[GroupKey]uint64{
			{Kind: 0, Value: 4}: 0,
			{Kind: 0, Value: 8}: 0,
		},
		StreamFinishTag: map[uint64]uint64{
			4: 12,
		},
	}
	reqs := []RequestMeta{
		streamReq(4, 1),
		streamReq(8, 8),
	}

	order := orderBatchIndices(BatchConfig{MaxFramePayload: 16384}, state, reqs, map[uint64]StreamMeta{
		4: {Priority: 20},
		8: {},
	})
	if got := reqs[order[0]].StreamID; got != 8 {
		t.Fatalf("first selected stream = %d, want eligible stream 8", got)
	}
}

func TestOrderBatchIndicesPrioritizesPriorityUpdateOnceAtBatchHead(t *testing.T) {
	t.Parallel()

	reqs := []RequestMeta{
		streamReq(8, 1),
		streamReq(4, 1),
		{
			GroupKey:         GroupKey{Kind: 0, Value: 4},
			StreamID:         4,
			StreamScoped:     true,
			IsPriorityUpdate: true,
			Cost:             1,
		},
	}

	order := orderBatchIndices(BatchConfig{MaxFramePayload: 16384}, &BatchState{}, reqs, map[uint64]StreamMeta{
		4: {},
		8: {},
	})
	if got := reqs[order[0]]; got.StreamID != 4 || !got.IsPriorityUpdate {
		t.Fatalf("first selected request = %+v, want stream 4 priority update", got)
	}
}

func TestOrderBatchIndicesPrefersEligibleGroupOverIneligibleGroup(t *testing.T) {
	t.Parallel()

	groupA := GroupKey{Kind: 1, Value: 7}
	groupB := GroupKey{Kind: 1, Value: 9}
	state := &BatchState{
		RootVirtualTime: 4,
		GroupFinishTag: map[GroupKey]uint64{
			groupA: 12,
			groupB: 4,
		},
	}
	reqs := []RequestMeta{
		{GroupKey: groupA, StreamID: 4, StreamScoped: true, Cost: 1},
		{GroupKey: groupB, StreamID: 8, StreamScoped: true, Cost: 1},
	}

	order := orderBatchIndices(BatchConfig{GroupFair: true, MaxFramePayload: 16384}, state, reqs, map[uint64]StreamMeta{
		4: {},
		8: {},
	})
	if got := reqs[order[0]].StreamID; got != 8 {
		t.Fatalf("first selected stream = %d, want eligible group stream 8", got)
	}
}

func TestOrderBatchIndicesPrefersHigherPriorityShortFlow(t *testing.T) {
	t.Parallel()

	reqs := []RequestMeta{
		streamReq(4, 40000),
		streamReq(8, 512),
	}

	order := orderBatchIndices(BatchConfig{MaxFramePayload: 16384}, &BatchState{}, reqs, map[uint64]StreamMeta{
		4: {},
		8: {Priority: 20},
	})
	if got := reqs[order[0]].StreamID; got != 8 {
		t.Fatalf("first selected stream = %d, want 8", got)
	}
}

func TestOrderBatchIndicesInterleavesExplicitGroupsWhenGroupFair(t *testing.T) {
	t.Parallel()

	reqs := []RequestMeta{
		{GroupKey: GroupKey{Kind: 1, Value: 7}, StreamID: 4, StreamScoped: true, Cost: 1},
		{GroupKey: GroupKey{Kind: 1, Value: 7}, StreamID: 8, StreamScoped: true, Cost: 1},
		{GroupKey: GroupKey{Kind: 1, Value: 9}, StreamID: 12, StreamScoped: true, Cost: 1},
		{GroupKey: GroupKey{Kind: 1, Value: 9}, StreamID: 16, StreamScoped: true, Cost: 1},
	}

	order := orderBatchIndices(BatchConfig{GroupFair: true, MaxFramePayload: 16384}, &BatchState{}, reqs, map[uint64]StreamMeta{
		4:  {},
		8:  {},
		12: {},
		16: {},
	})
	if got := []uint64{
		reqs[order[0]].StreamID,
		reqs[order[1]].StreamID,
		reqs[order[2]].StreamID,
		reqs[order[3]].StreamID,
	}; !equalUint64s(got, []uint64{4, 12, 8, 16}) {
		t.Fatalf("group-fair order = %v, want [4 12 8 16]", got)
	}
}

func TestOrderBatchIndicesKeepsPerGroupHeadAsBoundedNextBatchBias(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	firstBatch := []RequestMeta{
		{GroupKey: GroupKey{Kind: 1, Value: 7}, StreamID: 4, StreamScoped: true, Cost: 1},
		{GroupKey: GroupKey{Kind: 1, Value: 7}, StreamID: 8, StreamScoped: true, Cost: 1},
		{GroupKey: GroupKey{Kind: 1, Value: 9}, StreamID: 12, StreamScoped: true, Cost: 1},
		{GroupKey: GroupKey{Kind: 1, Value: 9}, StreamID: 16, StreamScoped: true, Cost: 1},
	}
	streams := map[uint64]StreamMeta{
		4:  {},
		8:  {},
		12: {},
		16: {},
	}

	order := orderBatchIndices(BatchConfig{GroupFair: true, MaxFramePayload: 16384}, state, firstBatch, streams)
	if got := []uint64{
		firstBatch[order[0]].StreamID,
		firstBatch[order[1]].StreamID,
		firstBatch[order[2]].StreamID,
		firstBatch[order[3]].StreamID,
	}; !equalUint64s(got, []uint64{4, 12, 8, 16}) {
		t.Fatalf("first group-fair order = %v, want [4 12 8 16]", got)
	}

	secondBatch := []RequestMeta{
		{GroupKey: GroupKey{Kind: 1, Value: 7}, StreamID: 4, StreamScoped: true, Cost: 1},
		{GroupKey: GroupKey{Kind: 1, Value: 7}, StreamID: 8, StreamScoped: true, Cost: 1},
		{GroupKey: GroupKey{Kind: 1, Value: 9}, StreamID: 12, StreamScoped: true, Cost: 1},
		{GroupKey: GroupKey{Kind: 1, Value: 9}, StreamID: 16, StreamScoped: true, Cost: 1},
	}
	order = orderBatchIndices(BatchConfig{GroupFair: true, MaxFramePayload: 16384}, state, secondBatch, streams)
	if got := []uint64{
		secondBatch[order[0]].StreamID,
		secondBatch[order[1]].StreamID,
		secondBatch[order[2]].StreamID,
		secondBatch[order[3]].StreamID,
	}; !equalUint64s(got, []uint64{16, 8, 12, 4}) {
		t.Fatalf("second group-fair order = %v, want [16 8 12 4]", got)
	}
}

func TestOrderBatchIndicesRecordsRetainedWFQStateForRealStreamsOnly(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	reqs := []RequestMeta{
		streamReq(4, 1),
		{GroupKey: GroupKey{Kind: 2, Value: 1}, Cost: 1},
	}

	order := orderBatchIndices(BatchConfig{GroupFair: true, MaxFramePayload: 16384}, state, reqs, map[uint64]StreamMeta{
		4: {},
	})
	if len(order) != len(reqs) {
		t.Fatalf("order len = %d, want %d", len(order), len(reqs))
	}
	if len(state.StreamFinishTag) != 1 {
		t.Fatalf("retained stream finish tags = %#v, want one real-stream entry", state.StreamFinishTag)
	}
	if _, ok := state.StreamFinishTag[4]; !ok {
		t.Fatalf("real stream finish tag missing: %#v", state.StreamFinishTag)
	}
	for key := range state.GroupVirtualTime {
		if key.Kind == 2 {
			t.Fatalf("retained transient group state for kind=2: %#v", state.GroupVirtualTime)
		}
	}
	if state.ServiceSeq == 0 {
		t.Fatal("service sequence did not advance for real stream traffic")
	}
}

func TestBuildBatchGroupsReusesAndClearsNestedQueueMaps(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	firstItems := []BatchItem{
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 4, StreamScoped: true, Cost: 1}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 8, StreamScoped: true, Cost: 1}},
	}
	first := buildBatchGroups(state, firstItems)
	firstQueues := first.groupState[GroupKey{Kind: 0, Value: 4}]
	if len(firstQueues) != 2 {
		t.Fatalf("first queue count = %d, want 2", len(firstQueues))
	}

	secondItems := []BatchItem{
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 4, StreamScoped: true, Cost: 1}},
	}
	second := buildBatchGroups(state, secondItems)
	secondQueues := second.groupState[GroupKey{Kind: 0, Value: 4}]
	if len(secondQueues) != 1 {
		t.Fatalf("second queue count = %d, want 1", len(secondQueues))
	}
	if _, ok := secondQueues[8]; ok {
		t.Fatal("reused nested queue map retained stale stream queue for stream 8")
	}
	if firstQueues == nil || secondQueues == nil {
		t.Fatal("expected non-nil nested queue maps")
	}
}

func TestBuildBatchGroupsRecyclesNestedQueueAndOrderSlices(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	firstItems := []BatchItem{
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 4, StreamScoped: true, Cost: 1}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 8, StreamScoped: true, Cost: 1}},
	}
	buildBatchGroups(state, firstItems)

	secondItems := []BatchItem{
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 4, StreamScoped: true, Cost: 1}},
	}
	buildBatchGroups(state, secondItems)

	if len(state.scratch.groupQueueEntries) == 0 {
		t.Fatal("expected recycled nested queue slices after second build")
	}
	if state.scratch.groupQueueEntryCount == 0 {
		t.Fatal("expected second build to reuse a recycled nested queue slice")
	}
	if len(state.scratch.streamOrderEntries) == 0 {
		t.Fatal("expected recycled stream-order slices after second build")
	}
	if state.scratch.streamOrderEntryCount == 0 {
		t.Fatal("expected second build to reuse a recycled stream-order slice")
	}
}

func TestBuildBatchGroupsAccumulatesQueuedBytesDuringGrouping(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	groupA := GroupKey{Kind: 1, Value: 7}
	groupB := GroupKey{Kind: 1, Value: 9}
	items := []BatchItem{
		{Request: RequestMeta{GroupKey: groupA, StreamID: 4, StreamScoped: true, Cost: 2}},
		{Request: RequestMeta{GroupKey: groupA, StreamID: 4, StreamScoped: true, Cost: 3}},
		{Request: RequestMeta{GroupKey: groupB, StreamID: 4, StreamScoped: true, Cost: 5}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 2, Value: 0}, Cost: 7}},
	}

	prepared := buildBatchGroups(state, items)

	if !prepared.hasRealStreamScoped {
		t.Fatal("hasRealStreamScoped = false, want true")
	}
	if got := prepared.queuedBytes[4]; got != 10 {
		t.Fatalf("queuedBytes[4] = %d, want 10", got)
	}
}

func TestBuildBatchGroupsCapturesStreamMetaAndPriorityUpdate(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	meta := StreamMeta{Priority: 9}
	items := []BatchItem{
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 4, StreamScoped: true, Cost: 1}, Stream: meta},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 4, StreamScoped: true, IsPriorityUpdate: true, Cost: 1}, Stream: meta},
	}

	prepared := buildBatchGroups(state, items)

	if !prepared.hasPriorityUpdate {
		t.Fatal("hasPriorityUpdate = false, want true")
	}
	if got := prepared.streamMeta[4]; got.Priority != meta.Priority {
		t.Fatalf("streamMeta[4] = %#v, want priority %d", got, meta.Priority)
	}
}

func TestActiveGroupSliceClearsRetainedNestedReferences(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	state.scratch.activeGroups = make([]wfqActiveGroup, 1, 3)
	state.scratch.activeGroups[0] = wfqActiveGroup{
		queues:  map[uint64][]int{4: {0, 1}},
		streams: []uint64{4, 8},
		hasTop:  true,
	}
	backing := state.scratch.activeGroups[:cap(state.scratch.activeGroups)]
	backing[1] = wfqActiveGroup{
		queues:  map[uint64][]int{9: {2}},
		streams: []uint64{9},
		hasTop:  true,
	}
	backing[2] = wfqActiveGroup{
		queues:  map[uint64][]int{10: {3}},
		streams: []uint64{10},
		hasTop:  true,
	}

	got := activeGroupSlice(state, 1)
	if len(got) != 0 {
		t.Fatalf("active group scratch len = %d, want 0", len(got))
	}

	if len(backing) == 0 {
		t.Fatal("expected retained scratch backing")
	}
	for i := range backing {
		if backing[i].queues != nil {
			t.Fatalf("active group scratch[%d] retained queues: %#v", i, backing[i].queues)
		}
		if backing[i].streams != nil {
			t.Fatalf("active group scratch[%d] retained streams: %#v", i, backing[i].streams)
		}
		if backing[i].hasTop {
			t.Fatalf("active group scratch[%d] retained top marker", i)
		}
	}
}

func TestActiveGroupSliceDropsOversizedBacking(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	oversizedCap := batchScratchRetainLimit(1) + 1
	state.scratch.activeGroups = make([]wfqActiveGroup, 0, oversizedCap)

	got := activeGroupSlice(state, 1)
	if len(got) != 0 {
		t.Fatalf("active group scratch len = %d, want 0", len(got))
	}
	if cap(got) >= oversizedCap {
		t.Fatalf("active group scratch cap = %d, want drop below %d", cap(got), oversizedCap)
	}
}

func TestBuildBatchGroupsDropsOversizedGroupQueueCache(t *testing.T) {
	t.Parallel()

	state := &BatchState{}
	oversizedLen := batchScratchRetainLimit(1) + 1
	state.scratch.lastBuildCapHint = oversizedLen
	state.scratch.groupQueues = make([]map[uint64][]int, oversizedLen)
	for i := range state.scratch.groupQueues {
		state.scratch.groupQueues[i] = map[uint64][]int{uint64(i + 1): {i}}
	}

	prepared := buildBatchGroups(state, []BatchItem{
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 4, StreamScoped: true, Cost: 1}},
	})

	if len(prepared.groupOrder) != 1 {
		t.Fatalf("groupOrder len = %d, want 1", len(prepared.groupOrder))
	}
	if got := len(state.scratch.groupQueues); got != 1 {
		t.Fatalf("retained group queue cache len = %d, want 1 after oversized cache drop", got)
	}
}

func streamReq(streamID uint64, cost int64) RequestMeta {
	return RequestMeta{
		GroupKey:     GroupKey{Kind: 0, Value: streamID},
		StreamID:     streamID,
		StreamScoped: true,
		Cost:         cost,
	}
}

func orderBatchIndices(cfg BatchConfig, state *BatchState, reqs []RequestMeta, streams map[uint64]StreamMeta) []int {
	items := make([]BatchItem, len(reqs))
	for i, req := range reqs {
		item := BatchItem{Request: req}
		if req.StreamScoped && streams != nil {
			item.Stream = streams[req.StreamID]
		}
		items[i] = item
	}
	return OrderBatchIndices(cfg, state, items)
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
