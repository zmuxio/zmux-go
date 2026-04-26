package runtime

import "github.com/zmuxio/zmux-go/internal/wire"

const syntheticStreamKeyBit uint64 = 1 << 63

type GroupKey struct {
	Kind  uint8
	Value uint64
}

const (
	MaxExplicitGroups = 16

	// FallbackGroupBucket is the local overflow bucket used when too many
	// explicit non-zero groups are simultaneously active in the current sender
	// decision scope.
	FallbackGroupBucket = ^uint64(0)
)

type RequestMeta struct {
	GroupKey         GroupKey
	StreamID         uint64
	StreamScoped     bool
	IsPriorityUpdate bool
	Cost             int64
	UrgencyRank      int
}

type StreamMeta struct {
	Priority uint64
}

type BatchItem struct {
	Request RequestMeta
	Stream  StreamMeta
}

type BatchConfig struct {
	Urgent          bool
	GroupFair       bool
	SchedulerHint   wire.SchedulerHint
	MaxFramePayload uint64
}

type BatchState struct {
	RootVirtualTime          uint64
	GroupVirtualTime         map[GroupKey]uint64
	GroupFinishTag           map[GroupKey]uint64
	GroupLastService         map[GroupKey]uint64
	GroupLag                 map[GroupKey]int64
	StreamFinishTag          map[uint64]uint64
	StreamLastService        map[uint64]uint64
	StreamLag                map[uint64]int64
	StreamClass              map[uint64]trafficClass
	StreamLastSeenBatch      map[uint64]uint64
	SmallBurstDisarmed       map[uint64]struct{}
	PreferredGroupHead       GroupKey
	HasPreferredGroupHead    bool
	PreferredStreamHead      map[GroupKey]uint64
	ServiceSeq               uint64
	BatchSeq                 uint64
	InteractiveStreak        uint32
	ClassSelectionsSinceBulk uint32
	scratch                  batchScratch
}

type batchScratch struct {
	groupOrder                []GroupKey
	groupState                map[GroupKey]map[uint64][]int
	groups                    []batchBuiltGroup
	groupQueues               []map[uint64][]int
	groupQueueCount           int
	groupQueueEntries         [][]int
	groupQueueEntryCount      int
	streamOrder               map[GroupKey][]uint64
	streamOrderEntries        [][]uint64
	streamOrderEntryCount     int
	queuedBytes               map[uint64]uint64
	streamMeta                map[uint64]StreamMeta
	preparedStreams           map[uint64]batchPreparedStream
	bypassSelections          map[uint64]int
	interactiveActiveStreams  []uint64
	bulkActiveStreams         []uint64
	interactiveCandidates     []wfqGroupCandidate
	bulkCandidates            []wfqGroupCandidate
	transientStreamFinish     map[uint64]uint64
	transientStreamLastServed map[uint64]uint64
	transientGroupVirtual     map[GroupKey]uint64
	transientGroupFinish      map[GroupKey]uint64
	transientGroupLastServed  map[GroupKey]uint64
	tiePrefStreams            map[GroupKey]uint64
	ordered                   []int
	selected                  []bool
	lastBuildCapHint          int
}

const (
	batchScratchRetainMinCap = 256
	batchScratchRetainFactor = 4
)

func batchScratchRetainLimit(hint int) int {
	if hint <= 0 {
		return batchScratchRetainMinCap
	}
	if hint < batchScratchRetainMinCap {
		return batchScratchRetainMinCap
	}
	maxInt := int(^uint(0) >> 1)
	if hint > maxInt/batchScratchRetainFactor {
		return maxInt
	}
	return hint * batchScratchRetainFactor
}

func batchScratchOversized(retainedCap, hint int) bool {
	if retainedCap <= 0 {
		return false
	}
	return retainedCap > batchScratchRetainLimit(hint)
}

type batchTransientState struct {
	streamFinish     map[uint64]uint64
	streamLastServed map[uint64]uint64
	groupVirtual     map[GroupKey]uint64
	groupFinish      map[GroupKey]uint64
	groupLastServed  map[GroupKey]uint64
}

type batchStreamSelection struct {
	reqIdx           int
	queuePos         int
	cost             int64
	baseWeight       uint64
	isPriorityUpdate bool
}

type wfqStreamCandidate struct {
	streamID         uint64
	reqIdx           int
	queuePos         int
	cost             int64
	baseWeight       uint64
	weight           uint64
	streamVirtual    uint64
	streamStart      uint64
	streamFinish     uint64
	streamLastServed uint64
	eligible         bool
	isPriorityUpdate bool
	streamOrder      int
}

type wfqGroupCandidate struct {
	groupKey              GroupKey
	groupVirtual          uint64
	groupStart            uint64
	groupFinish           uint64
	groupLastServed       uint64
	eligible              bool
	groupOrder            int
	class                 trafficClass
	baseGroupWeight       uint64
	groupWeight           uint64
	totalBaseStreamWeight uint64
	totalStreamWeight     uint64
	stream                wfqStreamCandidate
}

func groupVirtualTime(state *BatchState, transient batchTransientState, groupKey GroupKey) uint64 {
	if isTransientGroupKey(groupKey) {
		return transient.groupVirtual[groupKey]
	}
	return state.GroupVirtualTime[groupKey]
}

func setGroupVirtualTime(state *BatchState, transient batchTransientState, groupKey GroupKey, value uint64) {
	if isTransientGroupKey(groupKey) {
		transient.groupVirtual[groupKey] = value
		return
	}
	state.GroupVirtualTime[groupKey] = value
}

func groupFinishTag(state *BatchState, transient batchTransientState, groupKey GroupKey) uint64 {
	if isTransientGroupKey(groupKey) {
		return transient.groupFinish[groupKey]
	}
	return state.GroupFinishTag[groupKey]
}

func setGroupFinishTag(state *BatchState, transient batchTransientState, groupKey GroupKey, value uint64) {
	if isTransientGroupKey(groupKey) {
		transient.groupFinish[groupKey] = value
		return
	}
	state.GroupFinishTag[groupKey] = value
}

func groupLastServed(state *BatchState, transient batchTransientState, groupKey GroupKey) uint64 {
	if isTransientGroupKey(groupKey) {
		return transient.groupLastServed[groupKey]
	}
	return state.GroupLastService[groupKey]
}

func setGroupLastServed(state *BatchState, transient batchTransientState, groupKey GroupKey, value uint64) {
	if isTransientGroupKey(groupKey) {
		transient.groupLastServed[groupKey] = value
		return
	}
	state.GroupLastService[groupKey] = value
}

func streamFinishTag(state *BatchState, transient batchTransientState, streamID uint64) uint64 {
	if isSyntheticStreamKey(streamID) {
		return transient.streamFinish[streamID]
	}
	return state.StreamFinishTag[streamID]
}

func setStreamFinishTag(state *BatchState, transient batchTransientState, streamID uint64, value uint64) {
	if isSyntheticStreamKey(streamID) {
		transient.streamFinish[streamID] = value
		return
	}
	state.StreamFinishTag[streamID] = value
}

func streamLastServed(state *BatchState, transient batchTransientState, streamID uint64) uint64 {
	if isSyntheticStreamKey(streamID) {
		return transient.streamLastServed[streamID]
	}
	return state.StreamLastService[streamID]
}

func setStreamLastServed(state *BatchState, transient batchTransientState, streamID uint64, value uint64) {
	if isSyntheticStreamKey(streamID) {
		transient.streamLastServed[streamID] = value
		return
	}
	state.StreamLastService[streamID] = value
}

func groupLag(state *BatchState, groupKey GroupKey) int64 {
	if state == nil || state.GroupLag == nil {
		return 0
	}
	return state.GroupLag[groupKey]
}

func setGroupLag(state *BatchState, groupKey GroupKey, value int64) {
	if state == nil || state.GroupLag == nil || isTransientGroupKey(groupKey) {
		return
	}
	state.GroupLag[groupKey] = value
}

func streamLag(state *BatchState, streamID uint64) int64 {
	if state == nil || state.StreamLag == nil {
		return 0
	}
	return state.StreamLag[streamID]
}

func setStreamLag(state *BatchState, streamID uint64, value int64) {
	if state == nil || state.StreamLag == nil || isSyntheticStreamKey(streamID) {
		return
	}
	state.StreamLag[streamID] = value
}

func commitWFQSelection(state *BatchState, transient batchTransientState, candidate wfqGroupCandidate, activeGroupWeight uint64, activeStreamWeight uint64) {
	state.ServiceSeq++
	seq := state.ServiceSeq

	rootVirtual := state.RootVirtualTime
	if candidate.groupStart > rootVirtual {
		rootVirtual = candidate.groupStart
	}
	state.RootVirtualTime = SaturatingAdd(rootVirtual, serviceTag(candidate.stream.cost, max64(activeGroupWeight, 1)))

	groupVirtual := groupVirtualTime(state, transient, candidate.groupKey)
	if candidate.stream.streamStart > groupVirtual {
		groupVirtual = candidate.stream.streamStart
	}
	setGroupVirtualTime(state, transient, candidate.groupKey, SaturatingAdd(groupVirtual, serviceTag(candidate.stream.cost, max64(activeStreamWeight, 1))))
	setGroupFinishTag(state, transient, candidate.groupKey, candidate.groupFinish)
	setGroupLastServed(state, transient, candidate.groupKey, seq)
	setStreamFinishTag(state, transient, candidate.stream.streamID, candidate.stream.streamFinish)
	setStreamLastServed(state, transient, candidate.stream.streamID, seq)
}

func normalizeBatchState(state *BatchState) *BatchState {
	if state == nil {
		state = &BatchState{}
	}
	if state.GroupVirtualTime == nil {
		state.GroupVirtualTime = make(map[GroupKey]uint64)
	}
	if state.GroupFinishTag == nil {
		state.GroupFinishTag = make(map[GroupKey]uint64)
	}
	if state.GroupLastService == nil {
		state.GroupLastService = make(map[GroupKey]uint64)
	}
	if state.GroupLag == nil {
		state.GroupLag = make(map[GroupKey]int64)
	}
	if state.StreamFinishTag == nil {
		state.StreamFinishTag = make(map[uint64]uint64)
	}
	if state.StreamLastService == nil {
		state.StreamLastService = make(map[uint64]uint64)
	}
	if state.StreamLag == nil {
		state.StreamLag = make(map[uint64]int64)
	}
	if state.StreamClass == nil {
		state.StreamClass = make(map[uint64]trafficClass)
	}
	if state.StreamLastSeenBatch == nil {
		state.StreamLastSeenBatch = make(map[uint64]uint64)
	}
	if state.SmallBurstDisarmed == nil {
		state.SmallBurstDisarmed = make(map[uint64]struct{})
	}
	if state.PreferredStreamHead == nil {
		state.PreferredStreamHead = make(map[GroupKey]uint64)
	}
	return state
}

func prepareBatchScratchForBuild(state *BatchState, capHint int) {
	if state == nil {
		return
	}
	if batchScratchOversized(state.scratch.lastBuildCapHint, capHint) {
		state.scratch.groupState = nil
		state.scratch.groups = nil
		state.scratch.groupQueues = nil
		state.scratch.groupQueueCount = 0
		state.scratch.groupQueueEntries = nil
		state.scratch.groupQueueEntryCount = 0
		state.scratch.streamOrder = nil
		state.scratch.streamOrderEntries = nil
		state.scratch.streamOrderEntryCount = 0
		state.scratch.queuedBytes = nil
		state.scratch.streamMeta = nil
		state.scratch.preparedStreams = nil
		state.scratch.bypassSelections = nil
		state.scratch.interactiveActiveStreams = nil
		state.scratch.bulkActiveStreams = nil
		state.scratch.interactiveCandidates = nil
		state.scratch.bulkCandidates = nil
		state.scratch.transientStreamFinish = nil
		state.scratch.transientStreamLastServed = nil
		state.scratch.transientGroupVirtual = nil
		state.scratch.transientGroupFinish = nil
		state.scratch.transientGroupLastServed = nil
		state.scratch.tiePrefStreams = nil
	}
	state.scratch.lastBuildCapHint = capHint
}

func hasRetainedRealBatchState(state *BatchState) bool {
	if state == nil {
		return false
	}
	return len(state.GroupVirtualTime) != 0 ||
		len(state.GroupFinishTag) != 0 ||
		len(state.GroupLastService) != 0 ||
		len(state.StreamFinishTag) != 0 ||
		len(state.StreamLastService) != 0 ||
		len(state.StreamClass) != 0 ||
		len(state.StreamLastSeenBatch) != 0 ||
		len(state.SmallBurstDisarmed) != 0 ||
		len(state.PreferredStreamHead) != 0 ||
		state.HasPreferredGroupHead
}

func scrubIdleRetainedBatchState(state *BatchState) {
	if state == nil {
		return
	}
	state.RootVirtualTime = 0
	state.PreferredGroupHead = GroupKey{}
	state.HasPreferredGroupHead = false
	if state.PreferredStreamHead != nil {
		clear(state.PreferredStreamHead)
	}
	if state.GroupLag != nil {
		clear(state.GroupLag)
	}
	if state.StreamLag != nil {
		clear(state.StreamLag)
	}
	if state.StreamClass != nil {
		clear(state.StreamClass)
	}
	if state.StreamLastSeenBatch != nil {
		clear(state.StreamLastSeenBatch)
	}
	if state.SmallBurstDisarmed != nil {
		clear(state.SmallBurstDisarmed)
	}
	state.ServiceSeq = 0
	state.BatchSeq = 0
	state.InteractiveStreak = 0
	state.ClassSelectionsSinceBulk = 0
}

func releaseIdleBatchStateStorage(state *BatchState) {
	if state == nil {
		return
	}
	state.GroupVirtualTime = nil
	state.GroupFinishTag = nil
	state.GroupLastService = nil
	state.GroupLag = nil
	state.StreamFinishTag = nil
	state.StreamLastService = nil
	state.StreamLag = nil
	state.StreamClass = nil
	state.StreamLastSeenBatch = nil
	state.SmallBurstDisarmed = nil
	state.PreferredStreamHead = nil
	state.scratch = batchScratch{}
}

func maybeRebaseWFQState(state *BatchState) {
	if state == nil || state.RootVirtualTime < 1<<48 {
		return
	}
	floor := state.RootVirtualTime
	for _, tag := range state.GroupVirtualTime {
		if tag < floor {
			floor = tag
		}
	}
	for _, tag := range state.GroupFinishTag {
		if tag < floor {
			floor = tag
		}
	}
	for _, tag := range state.StreamFinishTag {
		if tag < floor {
			floor = tag
		}
	}
	if floor == 0 {
		return
	}

	state.RootVirtualTime -= floor
	for key, tag := range state.GroupVirtualTime {
		state.GroupVirtualTime[key] = tag - floor
	}
	for key, tag := range state.GroupFinishTag {
		state.GroupFinishTag[key] = tag - floor
	}
	for key, tag := range state.StreamFinishTag {
		state.StreamFinishTag[key] = tag - floor
	}
}

func groupStateMap(state *BatchState, capHint int) map[GroupKey]map[uint64][]int {
	if state == nil {
		return make(map[GroupKey]map[uint64][]int, capHint)
	}
	state.scratch.groupQueueCount = 0
	state.scratch.groupQueueEntryCount = 0
	if batchScratchOversized(cap(state.scratch.groupQueues), capHint) {
		state.scratch.groupQueues = nil
	}
	if batchScratchOversized(cap(state.scratch.groupQueueEntries), capHint) {
		state.scratch.groupQueueEntries = nil
	} else if state.scratch.groupQueueEntries != nil {
		state.scratch.groupQueueEntries = state.scratch.groupQueueEntries[:0]
	}
	if state.scratch.groupState == nil {
		state.scratch.groupState = make(map[GroupKey]map[uint64][]int, capHint)
	} else {
		clear(state.scratch.groupState)
	}
	return state.scratch.groupState
}

func groupBuildSlice(state *BatchState, capHint int) []batchBuiltGroup {
	if state == nil {
		return make([]batchBuiltGroup, 0, capHint)
	}
	if batchScratchOversized(cap(state.scratch.groups), capHint) {
		state.scratch.groups = nil
	} else if cap(state.scratch.groups) > 0 {
		clear(state.scratch.groups[:cap(state.scratch.groups)])
	}
	if cap(state.scratch.groups) < capHint {
		state.scratch.groups = make([]batchBuiltGroup, 0, capHint)
	} else {
		state.scratch.groups = state.scratch.groups[:0]
	}
	return state.scratch.groups
}

func nextGroupQueueMap(state *BatchState) map[uint64][]int {
	if state == nil {
		return make(map[uint64][]int)
	}
	if state.scratch.groupQueueCount < len(state.scratch.groupQueues) {
		out := state.scratch.groupQueues[state.scratch.groupQueueCount]
		recycleGroupQueueEntrySlices(state, out)
		clear(out)
		state.scratch.groupQueueCount++
		return out
	}
	out := make(map[uint64][]int)
	state.scratch.groupQueues = append(state.scratch.groupQueues, out)
	state.scratch.groupQueueCount++
	return out
}

func clearUnusedGroupQueueMaps(state *BatchState) {
	if state == nil {
		return
	}
	for i := state.scratch.groupQueueCount; i < len(state.scratch.groupQueues); i++ {
		queues := state.scratch.groupQueues[i]
		if len(queues) == 0 {
			continue
		}
		recycleGroupQueueEntrySlices(state, queues)
		clear(queues)
	}
}

func streamOrderMap(state *BatchState, capHint int) map[GroupKey][]uint64 {
	if state == nil {
		return make(map[GroupKey][]uint64, capHint)
	}
	state.scratch.streamOrderEntryCount = 0
	if batchScratchOversized(cap(state.scratch.streamOrderEntries), capHint) {
		state.scratch.streamOrderEntries = nil
	} else if state.scratch.streamOrderEntries != nil {
		state.scratch.streamOrderEntries = state.scratch.streamOrderEntries[:0]
	}
	if state.scratch.streamOrder == nil {
		state.scratch.streamOrder = make(map[GroupKey][]uint64, capHint)
	} else {
		recycleStreamOrderSlices(state, state.scratch.streamOrder)
		clear(state.scratch.streamOrder)
	}
	return state.scratch.streamOrder
}

func recycleGroupQueueEntrySlices(state *BatchState, queues map[uint64][]int) {
	if state == nil {
		return
	}
	for _, queue := range queues {
		if batchScratchOversized(cap(queue), state.scratch.lastBuildCapHint) {
			continue
		}
		state.scratch.groupQueueEntries = append(state.scratch.groupQueueEntries, queue[:0])
	}
}

func nextGroupQueueEntrySlice(state *BatchState) []int {
	if state == nil {
		return nil
	}
	if state.scratch.groupQueueEntryCount >= len(state.scratch.groupQueueEntries) {
		return nil
	}
	out := state.scratch.groupQueueEntries[state.scratch.groupQueueEntryCount]
	state.scratch.groupQueueEntryCount++
	return out[:0]
}

func recycleStreamOrderSlices(state *BatchState, orders map[GroupKey][]uint64) {
	if state == nil {
		return
	}
	for _, order := range orders {
		if batchScratchOversized(cap(order), state.scratch.lastBuildCapHint) {
			continue
		}
		state.scratch.streamOrderEntries = append(state.scratch.streamOrderEntries, order[:0])
	}
}

func nextStreamOrderSlice(state *BatchState) []uint64 {
	if state == nil {
		return nil
	}
	if state.scratch.streamOrderEntryCount >= len(state.scratch.streamOrderEntries) {
		return nil
	}
	out := state.scratch.streamOrderEntries[state.scratch.streamOrderEntryCount]
	state.scratch.streamOrderEntryCount++
	return out[:0]
}

func queuedBytesMap(state *BatchState) map[uint64]uint64 {
	if state == nil {
		return make(map[uint64]uint64)
	}
	if state.scratch.queuedBytes == nil {
		state.scratch.queuedBytes = make(map[uint64]uint64)
	} else {
		clear(state.scratch.queuedBytes)
	}
	return state.scratch.queuedBytes
}

func preparedStreamMap(state *BatchState, capHint int) map[uint64]batchPreparedStream {
	if state == nil {
		return make(map[uint64]batchPreparedStream, capHint)
	}
	if state.scratch.preparedStreams == nil {
		state.scratch.preparedStreams = make(map[uint64]batchPreparedStream, capHint)
	} else {
		clear(state.scratch.preparedStreams)
	}
	return state.scratch.preparedStreams
}

func bypassSelectionsMap(state *BatchState, capHint int) map[uint64]int {
	if state == nil {
		return make(map[uint64]int, capHint)
	}
	if state.scratch.bypassSelections == nil {
		state.scratch.bypassSelections = make(map[uint64]int, capHint)
	} else {
		clear(state.scratch.bypassSelections)
	}
	return state.scratch.bypassSelections
}

func interactiveActiveStreamSlice(state *BatchState, capHint int) []uint64 {
	if state == nil {
		return make([]uint64, 0, capHint)
	}
	if batchScratchOversized(cap(state.scratch.interactiveActiveStreams), capHint) {
		state.scratch.interactiveActiveStreams = nil
	}
	if cap(state.scratch.interactiveActiveStreams) < capHint {
		state.scratch.interactiveActiveStreams = make([]uint64, 0, capHint)
	} else {
		state.scratch.interactiveActiveStreams = state.scratch.interactiveActiveStreams[:0]
	}
	return state.scratch.interactiveActiveStreams
}

func bulkActiveStreamSlice(state *BatchState, capHint int) []uint64 {
	if state == nil {
		return make([]uint64, 0, capHint)
	}
	if batchScratchOversized(cap(state.scratch.bulkActiveStreams), capHint) {
		state.scratch.bulkActiveStreams = nil
	}
	if cap(state.scratch.bulkActiveStreams) < capHint {
		state.scratch.bulkActiveStreams = make([]uint64, 0, capHint)
	} else {
		state.scratch.bulkActiveStreams = state.scratch.bulkActiveStreams[:0]
	}
	return state.scratch.bulkActiveStreams
}

func transientStreamFinishMap(state *BatchState, capHint int) map[uint64]uint64 {
	if state == nil {
		return make(map[uint64]uint64, capHint)
	}
	if state.scratch.transientStreamFinish == nil {
		state.scratch.transientStreamFinish = make(map[uint64]uint64, capHint)
	} else {
		clear(state.scratch.transientStreamFinish)
	}
	return state.scratch.transientStreamFinish
}

func transientStreamLastServedMap(state *BatchState, capHint int) map[uint64]uint64 {
	if state == nil {
		return make(map[uint64]uint64, capHint)
	}
	if state.scratch.transientStreamLastServed == nil {
		state.scratch.transientStreamLastServed = make(map[uint64]uint64, capHint)
	} else {
		clear(state.scratch.transientStreamLastServed)
	}
	return state.scratch.transientStreamLastServed
}

func transientGroupVirtualMap(state *BatchState, capHint int) map[GroupKey]uint64 {
	if state == nil {
		return make(map[GroupKey]uint64, capHint)
	}
	if state.scratch.transientGroupVirtual == nil {
		state.scratch.transientGroupVirtual = make(map[GroupKey]uint64, capHint)
	} else {
		clear(state.scratch.transientGroupVirtual)
	}
	return state.scratch.transientGroupVirtual
}

func transientGroupFinishMap(state *BatchState, capHint int) map[GroupKey]uint64 {
	if state == nil {
		return make(map[GroupKey]uint64, capHint)
	}
	if state.scratch.transientGroupFinish == nil {
		state.scratch.transientGroupFinish = make(map[GroupKey]uint64, capHint)
	} else {
		clear(state.scratch.transientGroupFinish)
	}
	return state.scratch.transientGroupFinish
}

func transientGroupLastServedMap(state *BatchState, capHint int) map[GroupKey]uint64 {
	if state == nil {
		return make(map[GroupKey]uint64, capHint)
	}
	if state.scratch.transientGroupLastServed == nil {
		state.scratch.transientGroupLastServed = make(map[GroupKey]uint64, capHint)
	} else {
		clear(state.scratch.transientGroupLastServed)
	}
	return state.scratch.transientGroupLastServed
}

func tiePrefStreamsMap(state *BatchState, capHint int) map[GroupKey]uint64 {
	if state == nil {
		return make(map[GroupKey]uint64, capHint)
	}
	if state.scratch.tiePrefStreams == nil {
		state.scratch.tiePrefStreams = make(map[GroupKey]uint64, capHint)
	} else {
		clear(state.scratch.tiePrefStreams)
	}
	return state.scratch.tiePrefStreams
}

func orderedSlice(state *BatchState, n int, capHint int) []int {
	if state == nil {
		return make([]int, n, capHint)
	}
	if batchScratchOversized(cap(state.scratch.ordered), capHint) {
		state.scratch.ordered = nil
	}
	if cap(state.scratch.ordered) < capHint {
		state.scratch.ordered = make([]int, n, capHint)
	} else {
		state.scratch.ordered = state.scratch.ordered[:n]
	}
	return state.scratch.ordered
}

func interactiveCandidateSlice(state *BatchState, capHint int) []wfqGroupCandidate {
	if state == nil {
		return make([]wfqGroupCandidate, 0, capHint)
	}
	if batchScratchOversized(cap(state.scratch.interactiveCandidates), capHint) {
		state.scratch.interactiveCandidates = nil
	}
	if cap(state.scratch.interactiveCandidates) < capHint {
		state.scratch.interactiveCandidates = make([]wfqGroupCandidate, 0, capHint)
	} else {
		state.scratch.interactiveCandidates = state.scratch.interactiveCandidates[:0]
	}
	return state.scratch.interactiveCandidates
}

func bulkCandidateSlice(state *BatchState, capHint int) []wfqGroupCandidate {
	if state == nil {
		return make([]wfqGroupCandidate, 0, capHint)
	}
	if batchScratchOversized(cap(state.scratch.bulkCandidates), capHint) {
		state.scratch.bulkCandidates = nil
	}
	if cap(state.scratch.bulkCandidates) < capHint {
		state.scratch.bulkCandidates = make([]wfqGroupCandidate, 0, capHint)
	} else {
		state.scratch.bulkCandidates = state.scratch.bulkCandidates[:0]
	}
	return state.scratch.bulkCandidates
}

func groupOrderSlice(state *BatchState, n int, capHint int) []GroupKey {
	if state == nil {
		return make([]GroupKey, n, capHint)
	}
	if batchScratchOversized(cap(state.scratch.groupOrder), capHint) {
		state.scratch.groupOrder = nil
	}
	if cap(state.scratch.groupOrder) < capHint {
		state.scratch.groupOrder = make([]GroupKey, n, capHint)
	} else {
		state.scratch.groupOrder = state.scratch.groupOrder[:n]
	}
	return state.scratch.groupOrder
}

func selectedSlice(state *BatchState, n int) []bool {
	if state == nil {
		return make([]bool, n)
	}
	if batchScratchOversized(cap(state.scratch.selected), n) {
		state.scratch.selected = nil
	}
	if cap(state.scratch.selected) < n {
		state.scratch.selected = make([]bool, n)
	} else {
		state.scratch.selected = state.scratch.selected[:n]
		clear(state.scratch.selected)
	}
	return state.scratch.selected
}
