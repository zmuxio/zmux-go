package zmux

import (
	"sort"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
)

type lateDataAction = state.LateDataAction

const (
	lateDataIgnore      = state.LateDataIgnore
	lateDataAbortClosed = state.LateDataAbortClosed
	lateDataAbortState  = state.LateDataAbortState
)

type usedStreamMarker struct {
	action lateDataAction
	cause  lateDataCause
}

type usedStreamRange struct {
	start  uint64
	end    uint64
	marker usedStreamMarker
}

type terminalDataDisposition struct {
	action lateDataAction
	cause  lateDataCause
}

const maxTombstones = 4096

const (
	// Repository-default v1 hidden control-opened retention is currently realized
	// as hidden terminal tombstones from ABORT-first peer streams.
	hiddenControlRetainedHardCap = state.DefaultAdmissionHardCap
	hiddenControlRetainedMaxAge  = time.Second
)

type streamTombstone struct {
	state.StreamTombstone
	Hidden        bool
	CreatedAt     time.Time
	OrderIndex    int
	HiddenIndex   int
	LateDataCause lateDataCause
}

func (t *streamTombstone) queueIndex(hidden bool) int {
	if t == nil {
		return -1
	}
	if hidden {
		return t.HiddenIndex
	}
	return t.OrderIndex
}

func (t *streamTombstone) setQueueIndex(hidden bool, idx int) {
	if t == nil {
		return
	}
	if hidden {
		t.HiddenIndex = idx
		return
	}
	t.OrderIndex = idx
}

type streamTombstoneLookup struct {
	tombstone streamTombstone
	present   bool
}

func (l streamTombstoneLookup) found() bool {
	return l.present
}

func (c *Conn) tombstoneForLocked(streamID uint64) streamTombstoneLookup {
	if c.registry.tombstones == nil {
		return streamTombstoneLookup{}
	}
	ts, ok := c.registry.tombstones[streamID]
	return streamTombstoneLookup{tombstone: ts, present: ok}
}

type terminalDataLookup struct {
	disposition terminalDataDisposition
	present     bool
}

func (l terminalDataLookup) found() bool {
	return l.present
}

func (c *Conn) terminalDataDispositionForLocked(streamID uint64) terminalDataLookup {
	if tombstone := c.tombstoneForLocked(streamID); tombstone.found() {
		return terminalDataLookup{
			disposition: terminalDataDisposition{action: tombstone.tombstone.DataAction, cause: tombstone.tombstone.LateDataCause},
			present:     true,
		}
	}
	marker, ok := c.usedStreamMarkerForLocked(streamID)
	if !ok {
		return terminalDataLookup{}
	}
	return terminalDataLookup{disposition: terminalDataDisposition(marker), present: true}
}

func (c *Conn) hasTerminalMarkerLocked(streamID uint64) bool {
	return c.terminalDataDispositionForLocked(streamID).found()
}

type queueIndexLookup struct {
	index   int
	present bool
}

func (l queueIndexLookup) found() bool {
	return l.present
}

type tombstoneOrderLookup struct {
	streamID  uint64
	tombstone streamTombstone
	present   bool
}

func (l tombstoneOrderLookup) found() bool {
	return l.present
}

type tombstoneIDLookup struct {
	streamID uint64
	present  bool
}

func (l tombstoneIDLookup) found() bool {
	return l.present
}

func (c *Conn) markUsedStreamLocked(streamID uint64, marker usedStreamMarker) {
	if c == nil {
		return
	}
	if c.registry.usedStreamRangeMode {
		c.registry.usedStreamRanges = shrinkCompactedQueueBacking(upsertUsedStreamRange(c.registry.usedStreamRanges, streamID, marker))
		c.dropUsedStreamMapEntryLocked(streamID)
		return
	}
	if c.registry.usedStreamData == nil {
		c.registry.usedStreamData = make(map[uint64]usedStreamMarker)
	}
	c.registry.usedStreamData[streamID] = marker
	c.compactMarkerOnlyRangesLocked()
}

func (c *Conn) dropUsedStreamMapEntryLocked(streamID uint64) {
	if c == nil || c.registry.usedStreamData == nil {
		return
	}
	delete(c.registry.usedStreamData, streamID)
	if len(c.registry.usedStreamData) == 0 {
		c.registry.usedStreamData = nil
	}
}

func sameUsedStreamMarker(a, b usedStreamMarker) bool {
	return a.action == b.action && a.cause == b.cause
}

func usedStreamRangeContains(r usedStreamRange, streamID uint64) bool {
	if streamID < r.start || streamID > r.end {
		return false
	}
	return (streamID-r.start)%4 == 0
}

func usedStreamRangeMergeable(a, b usedStreamRange) bool {
	if !sameUsedStreamMarker(a.marker, b.marker) {
		return false
	}
	if a.start%4 != b.start%4 {
		return false
	}
	return a.end+4 >= b.start && b.end+4 >= a.start
}

func mergeUsedStreamRangeAround(ranges []usedStreamRange, idx int) []usedStreamRange {
	if idx < 0 || idx >= len(ranges) {
		return ranges
	}
	for idx > 0 && usedStreamRangeMergeable(ranges[idx-1], ranges[idx]) {
		if ranges[idx].start < ranges[idx-1].start {
			ranges[idx-1].start = ranges[idx].start
		}
		if ranges[idx].end > ranges[idx-1].end {
			ranges[idx-1].end = ranges[idx].end
		}
		copy(ranges[idx:], ranges[idx+1:])
		ranges = ranges[:len(ranges)-1]
		idx--
	}
	for idx+1 < len(ranges) && usedStreamRangeMergeable(ranges[idx], ranges[idx+1]) {
		if ranges[idx+1].end > ranges[idx].end {
			ranges[idx].end = ranges[idx+1].end
		}
		copy(ranges[idx+1:], ranges[idx+2:])
		ranges = ranges[:len(ranges)-1]
	}
	return ranges
}

func setContainedUsedStreamMarker(ranges []usedStreamRange, idx int, streamID uint64, marker usedStreamMarker) []usedStreamRange {
	if idx < 0 || idx >= len(ranges) {
		return ranges
	}
	current := ranges[idx]
	if sameUsedStreamMarker(current.marker, marker) {
		return ranges
	}
	out := append([]usedStreamRange(nil), ranges[:idx]...)
	insertIdx := len(out)
	if current.start < streamID {
		out = append(out, usedStreamRange{
			start:  current.start,
			end:    streamID - 4,
			marker: current.marker,
		})
		insertIdx = len(out)
	}
	out = append(out, usedStreamRange{
		start:  streamID,
		end:    streamID,
		marker: marker,
	})
	if streamID < current.end {
		out = append(out, usedStreamRange{
			start:  streamID + 4,
			end:    current.end,
			marker: current.marker,
		})
	}
	out = append(out, ranges[idx+1:]...)
	return mergeUsedStreamRangeAround(out, insertIdx)
}

func upsertUsedStreamRange(ranges []usedStreamRange, streamID uint64, marker usedStreamMarker) []usedStreamRange {
	if len(ranges) == 0 {
		return append(ranges, usedStreamRange{start: streamID, end: streamID, marker: marker})
	}
	idx := sort.Search(len(ranges), func(i int) bool {
		return ranges[i].start > streamID
	})
	if idx > 0 && usedStreamRangeContains(ranges[idx-1], streamID) {
		return setContainedUsedStreamMarker(ranges, idx-1, streamID, marker)
	}
	ranges = append(ranges, usedStreamRange{})
	copy(ranges[idx+1:], ranges[idx:])
	ranges[idx] = usedStreamRange{
		start:  streamID,
		end:    streamID,
		marker: marker,
	}
	return mergeUsedStreamRangeAround(ranges, idx)
}

func (c *Conn) usedStreamMarkerForLocked(streamID uint64) (usedStreamMarker, bool) {
	if c == nil {
		return usedStreamMarker{}, false
	}
	if c.registry.usedStreamData != nil {
		if marker, ok := c.registry.usedStreamData[streamID]; ok {
			return marker, true
		}
	}
	ranges := c.registry.usedStreamRanges
	if len(ranges) == 0 {
		return usedStreamMarker{}, false
	}
	idx := sort.Search(len(ranges), func(i int) bool {
		return ranges[i].start > streamID
	})
	if idx > 0 {
		r := ranges[idx-1]
		if usedStreamRangeContains(r, streamID) {
			return r.marker, true
		}
	}
	return usedStreamMarker{}, false
}

func (c *Conn) compactMarkerOnlyRangesLocked() {
	if c == nil || len(c.registry.usedStreamData) == 0 {
		return
	}
	markerOnlyCount := c.markerOnlyMapCountLocked()
	if markerOnlyCount <= c.markerOnlyHardCapLocked() && markerOnlyCount < 64 {
		return
	}
	ids := make([]uint64, 0, markerOnlyCount)
	for streamID := range c.registry.usedStreamData {
		if _, ok := c.registry.tombstones[streamID]; ok {
			continue
		}
		ids = append(ids, streamID)
	}
	if len(ids) == 0 {
		return
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ranges := append([]usedStreamRange(nil), c.registry.usedStreamRanges...)
	for _, streamID := range ids {
		marker := c.registry.usedStreamData[streamID]
		ranges = upsertUsedStreamRange(ranges, streamID, marker)
		c.dropUsedStreamMapEntryLocked(streamID)
	}
	c.registry.usedStreamRanges = shrinkCompactedQueueBacking(ranges)
	c.registry.usedStreamRangeMode = true
}

func tombstoneStateForStream(stream *nativeStream) state.StreamTombstone {
	return stream.tombstoneStateLocked()
}

func usedStreamMarkerFromTombstone(tombstone state.StreamTombstone, cause lateDataCause) usedStreamMarker {
	return usedStreamMarker{
		action: tombstone.DataAction,
		cause:  cause,
	}
}

func (c *Conn) closeSessionAsyncLocked(err error) {
	if c == nil || err == nil || c.lifecycle.closeErr != nil {
		return
	}
	c.shutdown.asyncCloseOnce.Do(func() {
		go c.closeSession(err)
	})
}

func (c *Conn) enforceTerminalBookkeepingMemoryCapLocked() {
	if c == nil || c.lifecycle.closeErr != nil {
		return
	}
	c.compactMarkerOnlyRangesLocked()
	if err := c.markerOnlyCapErrorLocked("compact terminal state"); err != nil {
		c.closeSessionAsyncLocked(err)
		return
	}
	if err := c.sessionMemoryCapErrorLocked("compact terminal state"); err != nil {
		c.closeSessionAsyncLocked(err)
	}
}

func (c *Conn) maybeCompactTerminalLocked(stream *nativeStream) {
	if stream == nil {
		return
	}
	c.removeUnseenLocalLocked(stream)
	_, ok := c.registry.streams[stream.id]
	if !stream.shouldCompactTerminalLocked(terminalTrackingStateFrom(ok)) {
		return
	}
	if c.registry.tombstones == nil {
		c.registry.tombstones = make(map[uint64]streamTombstone)
	}
	tombstone := tombstoneStateForStream(stream)
	lateDataCause := stream.lateDataCauseLocked()
	now := time.Now()
	hidden := !stream.applicationVisible
	c.markUsedStreamLocked(stream.id, usedStreamMarkerFromTombstone(tombstone, lateDataCause))
	if hidden {
		c.noteHiddenStreamReapedLocked()
	}
	c.registry.tombstones[stream.id] = streamTombstone{
		StreamTombstone: tombstone,
		Hidden:          hidden,
		CreatedAt:       now,
		OrderIndex:      -1,
		HiddenIndex:     -1,
		LateDataCause:   lateDataCause,
	}
	c.appendTombstoneLocked(stream.id)
	if hidden {
		c.appendHiddenTombstoneLocked(stream.id)
	}
	c.dropLiveStreamLocked(stream.id)
	c.releaseTerminalStreamStateLocked(stream, transientStreamReleaseOptions{
		scheduler: schedulerReleaseDrop,
		send:      true,
		receive:   streamReceiveReleaseAndClearReadBuf,
	})
	c.reapExcessTombstonesLocked()
	c.enforceHiddenControlStateBudgetLocked(now)
	c.enforceTerminalBookkeepingMemoryCapLocked()
	notify(c.signals.livenessCh)
	c.broadcastStateWakeLocked()
}

func (c *Conn) reapExcessTombstonesLocked() {
	limit := c.registry.tombstoneLimit
	if limit <= 0 {
		limit = maxTombstones
	}
	for c.tombstoneCountLocked() > limit {
		head := c.tombstoneHeadLocked()
		if !head.found() || !c.removeTombstoneLocked(head.streamID) {
			return
		}
	}
	for c.tombstoneCountLocked() > 0 && c.trackedSessionMemoryLocked() > c.sessionMemoryHardCapLocked() {
		head := c.tombstoneHeadLocked()
		if !head.found() {
			return
		}
		tombstone, ok := c.registry.tombstones[head.streamID]
		if !ok {
			return
		}
		if !tombstone.Hidden && c.visibleTombstoneRetainedLocked() <= 1 {
			return
		}
		if !c.removeTombstoneLocked(head.streamID) {
			return
		}
	}
	c.maybeCompactTombstoneQueueLocked()
}

func (c *Conn) tombstoneOrderContainsLocked(streamID uint64, idx int) bool {
	if c == nil || idx < 0 || idx >= len(c.registry.tombstoneOrder) || c.registry.tombstoneOrder[idx] != streamID {
		return false
	}
	tombstone, ok := c.registry.tombstones[streamID]
	return ok && tombstone.OrderIndex == idx
}

func (c *Conn) tombstoneIndexLocked(streamID uint64, hint int) queueIndexLookup {
	if c == nil {
		return queueIndexLookup{index: -1}
	}
	if c.tombstoneOrderContainsLocked(streamID, hint) {
		return queueIndexLookup{index: hint, present: true}
	}
	if _, ok := c.registry.tombstones[streamID]; !ok {
		return queueIndexLookup{index: -1}
	}
	for i := c.registry.tombstoneHead; i < len(c.registry.tombstoneOrder); i++ {
		if c.registry.tombstoneOrder[i] == streamID {
			return queueIndexLookup{index: i, present: true}
		}
	}
	return queueIndexLookup{index: -1}
}

func (c *Conn) tombstoneOrderEntryLocked(idx int) tombstoneOrderLookup {
	if c == nil || idx < 0 || idx >= len(c.registry.tombstoneOrder) {
		return tombstoneOrderLookup{}
	}
	streamID := c.registry.tombstoneOrder[idx]
	tombstone, ok := c.registry.tombstones[streamID]
	if !ok || tombstone.OrderIndex != idx {
		return tombstoneOrderLookup{}
	}
	return tombstoneOrderLookup{streamID: streamID, tombstone: tombstone, present: true}
}

func (c *Conn) advanceTombstoneHeadLocked() {
	if c == nil {
		return
	}
	for c.registry.tombstoneHead < len(c.registry.tombstoneOrder) {
		if c.tombstoneOrderEntryLocked(c.registry.tombstoneHead).found() {
			return
		}
		c.registry.tombstoneHead++
	}
}

func (c *Conn) ensureTombstoneQueueLocked() {
	if c == nil || c.registry.tombstonesInit {
		return
	}
	c.registry.tombstoneHead = 0
	c.registry.tombstoneCount = 0
	if len(c.registry.tombstones) == 0 || len(c.registry.tombstoneOrder) == 0 {
		c.registry.tombstonesInit = true
		return
	}
	for i, streamID := range c.registry.tombstoneOrder {
		tombstone, ok := c.registry.tombstones[streamID]
		if !ok {
			continue
		}
		tombstone.OrderIndex = i
		c.registry.tombstones[streamID] = tombstone
		c.registry.tombstoneCount++
	}
	c.registry.tombstonesInit = true
}

func (c *Conn) tombstoneCountLocked() int {
	if c == nil {
		return 0
	}
	c.ensureTombstoneQueueLocked()
	return c.registry.tombstoneCount
}

func (c *Conn) tombstoneHeadLocked() tombstoneIDLookup {
	if c == nil {
		return tombstoneIDLookup{}
	}
	c.ensureTombstoneQueueLocked()
	c.advanceTombstoneHeadLocked()
	entry := c.tombstoneOrderEntryLocked(c.registry.tombstoneHead)
	if entry.found() {
		return tombstoneIDLookup{streamID: entry.streamID, present: true}
	}
	return tombstoneIDLookup{}
}

func (c *Conn) tombstoneTailLocked() tombstoneIDLookup {
	if c == nil {
		return tombstoneIDLookup{}
	}
	c.ensureTombstoneQueueLocked()
	for i := len(c.registry.tombstoneOrder) - 1; i >= c.registry.tombstoneHead; i-- {
		entry := c.tombstoneOrderEntryLocked(i)
		if entry.found() {
			return tombstoneIDLookup{streamID: entry.streamID, present: true}
		}
	}
	return tombstoneIDLookup{}
}

func (c *Conn) appendTombstoneLocked(streamID uint64) {
	if c == nil {
		return
	}
	c.ensureTombstoneQueueLocked()
	c.appendIndexedTombstoneLocked(
		streamID,
		false,
		&c.registry.tombstoneOrder,
		&c.registry.tombstoneCount,
		c.tombstoneIndexLocked,
	)
}

func (c *Conn) removeTombstoneOrderSlotLocked(i int) bool {
	if c == nil {
		return false
	}
	c.ensureTombstoneQueueLocked()
	if i < c.registry.tombstoneHead || i >= len(c.registry.tombstoneOrder) {
		return false
	}
	entry := c.tombstoneOrderEntryLocked(i)
	if !entry.found() {
		return false
	}
	prevTracked := c.trackedSessionMemoryLocked()
	c.registry.tombstoneOrder[i] = 0
	c.registry.tombstoneCount--
	if c.registry.tombstoneCount < 0 {
		c.registry.tombstoneCount = 0
	}
	c.markUsedStreamLocked(entry.streamID, usedStreamMarkerFromTombstone(entry.tombstone.StreamTombstone, entry.tombstone.LateDataCause))
	entry.tombstone.OrderIndex = -1
	c.removeHiddenTombstoneLocked(entry.streamID, entry.tombstone)
	delete(c.registry.tombstones, entry.streamID)
	if i == c.registry.tombstoneHead {
		c.advanceTombstoneHeadLocked()
	}
	c.maybeCompactTombstoneQueueLocked()
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleased)
	return true
}

func (c *Conn) maybeCompactTombstoneQueueLocked() {
	if c == nil {
		return
	}
	c.ensureTombstoneQueueLocked()
	c.compactIndexedTombstoneQueueLocked(
		false,
		&c.registry.tombstoneOrder,
		&c.registry.tombstoneHead,
		&c.registry.tombstoneCount,
		&c.registry.tombstonesInit,
		c.tombstoneOrderEntryLocked,
	)
}

func (c *Conn) clearTombstoneQueueLocked() {
	if c == nil {
		return
	}
	c.registry.tombstoneOrder = nil
	c.registry.tombstoneHead = 0
	c.registry.tombstoneCount = 0
	c.registry.tombstonesInit = false
}

func (c *Conn) hiddenTombstoneOrderContainsLocked(streamID uint64, idx int) bool {
	if c == nil || idx < 0 || idx >= len(c.registry.hiddenTombstoneOrder) || c.registry.hiddenTombstoneOrder[idx] != streamID {
		return false
	}
	tombstone, ok := c.registry.tombstones[streamID]
	return ok && tombstone.Hidden && tombstone.HiddenIndex == idx
}

func (c *Conn) hiddenTombstoneIndexLocked(streamID uint64, hint int) queueIndexLookup {
	if c == nil {
		return queueIndexLookup{index: -1}
	}
	if c.hiddenTombstoneOrderContainsLocked(streamID, hint) {
		return queueIndexLookup{index: hint, present: true}
	}
	for i, id := range c.registry.hiddenTombstoneOrder {
		if id == streamID {
			return queueIndexLookup{index: i, present: true}
		}
	}
	return queueIndexLookup{index: -1}
}

func (c *Conn) ensureHiddenTombstonesLocked() {
	if c == nil || c.registry.hiddenTombstonesInit {
		return
	}
	c.ensureTombstoneQueueLocked()
	c.registry.hiddenTombstoneHead = 0
	c.registry.hiddenTombstoneCount = 0
	c.registry.hiddenTombstoneOrder = c.registry.hiddenTombstoneOrder[:0]
	if len(c.registry.tombstones) == 0 || c.tombstoneCountLocked() == 0 {
		c.registry.hiddenTombstoneOrder = nil
		c.registry.hiddenTombstonesInit = true
		return
	}
	for i := c.registry.tombstoneHead; i < len(c.registry.tombstoneOrder); i++ {
		entry := c.tombstoneOrderEntryLocked(i)
		if !entry.found() {
			continue
		}
		if !entry.tombstone.Hidden {
			entry.tombstone.HiddenIndex = -1
			c.registry.tombstones[entry.streamID] = entry.tombstone
			continue
		}
		entry.tombstone.HiddenIndex = len(c.registry.hiddenTombstoneOrder)
		c.registry.tombstones[entry.streamID] = entry.tombstone
		c.registry.hiddenTombstoneOrder = append(c.registry.hiddenTombstoneOrder, entry.streamID)
		c.registry.hiddenTombstoneCount++
	}
	c.registry.hiddenTombstoneOrder = shrinkCompactedQueueBacking(c.registry.hiddenTombstoneOrder)
	c.registry.hiddenTombstonesInit = true
}

func (c *Conn) hiddenTombstoneCountLocked() int {
	if c == nil {
		return 0
	}
	c.ensureHiddenTombstonesLocked()
	return c.registry.hiddenTombstoneCount
}

func (c *Conn) hiddenTombstoneOrderEntryLocked(idx int) tombstoneOrderLookup {
	if c == nil || idx < 0 || idx >= len(c.registry.hiddenTombstoneOrder) {
		return tombstoneOrderLookup{}
	}
	streamID := c.registry.hiddenTombstoneOrder[idx]
	if streamID == 0 {
		return tombstoneOrderLookup{}
	}
	tombstone, ok := c.registry.tombstones[streamID]
	if !ok || !tombstone.Hidden || tombstone.HiddenIndex != idx {
		return tombstoneOrderLookup{}
	}
	return tombstoneOrderLookup{streamID: streamID, tombstone: tombstone, present: true}
}

func (c *Conn) advanceHiddenTombstoneHeadLocked() {
	if c == nil {
		return
	}
	for c.registry.hiddenTombstoneHead < len(c.registry.hiddenTombstoneOrder) {
		if c.hiddenTombstoneOrderEntryLocked(c.registry.hiddenTombstoneHead).found() {
			return
		}
		c.registry.hiddenTombstoneHead++
	}
}

func (c *Conn) hiddenTombstoneHeadLocked() tombstoneIDLookup {
	if c == nil {
		return tombstoneIDLookup{}
	}
	c.ensureHiddenTombstonesLocked()
	c.advanceHiddenTombstoneHeadLocked()
	entry := c.hiddenTombstoneOrderEntryLocked(c.registry.hiddenTombstoneHead)
	if entry.found() {
		return tombstoneIDLookup{streamID: entry.streamID, present: true}
	}
	return tombstoneIDLookup{}
}

func (c *Conn) hiddenTombstoneTailLocked() tombstoneIDLookup {
	if c == nil {
		return tombstoneIDLookup{}
	}
	c.ensureHiddenTombstonesLocked()
	for i := len(c.registry.hiddenTombstoneOrder) - 1; i >= c.registry.hiddenTombstoneHead; i-- {
		entry := c.hiddenTombstoneOrderEntryLocked(i)
		if entry.found() {
			return tombstoneIDLookup{streamID: entry.streamID, present: true}
		}
	}
	return tombstoneIDLookup{}
}

func (c *Conn) appendHiddenTombstoneLocked(streamID uint64) {
	if c == nil {
		return
	}
	c.ensureHiddenTombstonesLocked()
	c.appendIndexedTombstoneLocked(
		streamID,
		true,
		&c.registry.hiddenTombstoneOrder,
		&c.registry.hiddenTombstoneCount,
		c.hiddenTombstoneIndexLocked,
	)
}

func (c *Conn) removeHiddenTombstoneLocked(streamID uint64, tombstone streamTombstone) {
	if c == nil || !tombstone.Hidden {
		return
	}
	c.ensureHiddenTombstonesLocked()
	idx := c.hiddenTombstoneIndexLocked(streamID, tombstone.HiddenIndex)
	if !idx.found() {
		return
	}
	c.registry.hiddenTombstoneOrder[idx.index] = 0
	tombstone.HiddenIndex = -1
	c.registry.tombstones[streamID] = tombstone
	if c.registry.hiddenTombstoneCount > 0 {
		c.registry.hiddenTombstoneCount--
	}
	if idx.index == c.registry.hiddenTombstoneHead {
		c.advanceHiddenTombstoneHeadLocked()
	}
	c.maybeCompactHiddenTombstoneQueueLocked()
}

func (c *Conn) maybeCompactHiddenTombstoneQueueLocked() {
	if c == nil {
		return
	}
	c.compactIndexedTombstoneQueueLocked(
		true,
		&c.registry.hiddenTombstoneOrder,
		&c.registry.hiddenTombstoneHead,
		&c.registry.hiddenTombstoneCount,
		&c.registry.hiddenTombstonesInit,
		c.hiddenTombstoneOrderEntryLocked,
	)
}

func (c *Conn) clearHiddenTombstonesLocked() {
	if c == nil {
		return
	}
	c.registry.hiddenTombstoneOrder = nil
	c.registry.hiddenTombstoneHead = 0
	c.registry.hiddenTombstoneCount = 0
	c.registry.hiddenTombstonesInit = false
}

func (c *Conn) appendIndexedTombstoneLocked(
	streamID uint64,
	hidden bool,
	order *[]uint64,
	count *int,
	indexLookup func(uint64, int) queueIndexLookup,
) {
	tombstone, ok := c.registry.tombstones[streamID]
	if !ok || (hidden && !tombstone.Hidden) {
		return
	}
	if idx := indexLookup(streamID, tombstone.queueIndex(hidden)); idx.found() {
		if tombstone.queueIndex(hidden) != idx.index {
			tombstone.setQueueIndex(hidden, idx.index)
			c.registry.tombstones[streamID] = tombstone
		}
		return
	}
	tombstone.setQueueIndex(hidden, len(*order))
	c.registry.tombstones[streamID] = tombstone
	*order = append(*order, streamID)
	*count++
}

func (c *Conn) compactIndexedTombstoneQueueLocked(
	hidden bool,
	order *[]uint64,
	head *int,
	count *int,
	init *bool,
	entryLookup func(int) tombstoneOrderLookup,
) {
	if *count == 0 {
		*order = nil
		*head = 0
		*count = 0
		*init = true
		return
	}
	if *head == 0 && len(*order) <= 2*(*count) {
		return
	}
	writeIdx := 0
	for i := *head; i < len(*order); i++ {
		entry := entryLookup(i)
		if !entry.found() {
			continue
		}
		entry.tombstone.setQueueIndex(hidden, writeIdx)
		c.registry.tombstones[entry.streamID] = entry.tombstone
		(*order)[writeIdx] = entry.streamID
		writeIdx++
	}
	clear((*order)[writeIdx:])
	*order = shrinkCompactedQueueBacking((*order)[:writeIdx])
	*head = 0
	*count = writeIdx
	*init = true
}

func (c *Conn) enforceHiddenControlStateBudgetLocked(now time.Time) {
	if c == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	c.reapExpiredHiddenControlStateLocked(now)
	for c.hiddenControlStateRetainedLocked() > state.AdmissionHardCap(c.pendingInboundLimitLocked()) {
		if !c.reapNewestHiddenControlStateLocked() {
			return
		}
	}
	for c.hiddenControlStateBytesLocked() > c.sessionMemoryHardCapLocked() {
		if !c.reapNewestHiddenControlStateLocked() {
			return
		}
	}
}

func (c *Conn) hiddenControlStateRetainedLocked() int {
	return c.hiddenTombstoneCountLocked()
}

func (c *Conn) hiddenControlStateBytesLocked() uint64 {
	if c == nil {
		return 0
	}
	return saturatingMul(uint64(c.hiddenControlStateRetainedLocked()), c.retainedStateUnitLocked())
}

func (c *Conn) reapExpiredHiddenControlStateLocked(now time.Time) {
	if c == nil {
		return
	}
	for {
		head := c.hiddenTombstoneHeadLocked()
		if !head.found() {
			return
		}
		tombstone, ok := c.registry.tombstones[head.streamID]
		if !ok || !tombstone.Hidden {
			if !ok {
				c.registry.hiddenTombstoneOrder[c.registry.hiddenTombstoneHead] = 0
				if c.registry.hiddenTombstoneCount > 0 {
					c.registry.hiddenTombstoneCount--
				}
			}
			c.advanceHiddenTombstoneHeadLocked()
			c.maybeCompactHiddenTombstoneQueueLocked()
			continue
		}
		if tombstone.CreatedAt.IsZero() || now.Sub(tombstone.CreatedAt) <= hiddenControlRetainedMaxAge {
			return
		}
		if !c.removeTombstoneLocked(head.streamID) {
			return
		}
	}
}

func (c *Conn) reapNewestHiddenControlStateLocked() bool {
	if c == nil {
		return false
	}
	tail := c.hiddenTombstoneTailLocked()
	if !tail.found() {
		return false
	}
	return c.removeTombstoneLocked(tail.streamID)
}

func (c *Conn) removeTombstoneLocked(streamID uint64) bool {
	if c == nil {
		return false
	}
	c.ensureTombstoneQueueLocked()
	tombstone, ok := c.registry.tombstones[streamID]
	if !ok {
		return false
	}
	idx := c.tombstoneIndexLocked(streamID, tombstone.OrderIndex)
	if !idx.found() {
		return false
	}
	return c.removeTombstoneOrderSlotLocked(idx.index)
}
