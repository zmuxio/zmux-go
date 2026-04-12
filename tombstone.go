package zmux

import (
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
	if c.registry.usedStreamData == nil {
		return terminalDataLookup{}
	}
	marker, ok := c.registry.usedStreamData[streamID]
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
	if c.registry.usedStreamData == nil {
		c.registry.usedStreamData = make(map[uint64]usedStreamMarker)
	}
	c.registry.usedStreamData[streamID] = marker
}

func tombstoneStateForStream(stream *Stream) state.StreamTombstone {
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
	if err := c.markerOnlyCapErrorLocked("compact terminal state"); err != nil {
		c.closeSessionAsyncLocked(err)
		return
	}
	if err := c.sessionMemoryCapErrorLocked("compact terminal state"); err != nil {
		c.closeSessionAsyncLocked(err)
	}
}

func (c *Conn) maybeCompactTerminalLocked(stream *Stream) {
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
	tombstone, ok := c.registry.tombstones[streamID]
	if !ok {
		return
	}
	if idx := c.tombstoneIndexLocked(streamID, tombstone.OrderIndex); idx.found() {
		if tombstone.OrderIndex != idx.index {
			tombstone.OrderIndex = idx.index
			c.registry.tombstones[streamID] = tombstone
		}
		return
	}
	tombstone.OrderIndex = len(c.registry.tombstoneOrder)
	c.registry.tombstones[streamID] = tombstone
	c.registry.tombstoneOrder = append(c.registry.tombstoneOrder, streamID)
	c.registry.tombstoneCount++
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
	c.enforceTerminalBookkeepingMemoryCapLocked()
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleased)
	return true
}

func (c *Conn) maybeCompactTombstoneQueueLocked() {
	if c == nil {
		return
	}
	c.ensureTombstoneQueueLocked()
	if c.registry.tombstoneCount == 0 {
		c.registry.tombstoneOrder = nil
		c.registry.tombstoneHead = 0
		c.registry.tombstoneCount = 0
		c.registry.tombstonesInit = true
		return
	}
	if c.registry.tombstoneHead == 0 && len(c.registry.tombstoneOrder) <= 2*c.registry.tombstoneCount {
		return
	}
	writeIdx := 0
	for i := c.registry.tombstoneHead; i < len(c.registry.tombstoneOrder); i++ {
		entry := c.tombstoneOrderEntryLocked(i)
		if !entry.found() {
			continue
		}
		entry.tombstone.OrderIndex = writeIdx
		c.registry.tombstones[entry.streamID] = entry.tombstone
		c.registry.tombstoneOrder[writeIdx] = entry.streamID
		writeIdx++
	}
	c.registry.tombstoneOrder = c.registry.tombstoneOrder[:writeIdx]
	c.registry.tombstoneHead = 0
	c.registry.tombstoneCount = writeIdx
	c.registry.tombstonesInit = true
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
	c.registry.hiddenTombstoneOrder = c.registry.hiddenTombstoneOrder[:0]
	if len(c.registry.tombstones) == 0 || c.tombstoneCountLocked() == 0 {
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
	}
	c.registry.hiddenTombstonesInit = true
}

func (c *Conn) hiddenTombstoneCountLocked() int {
	if c == nil {
		return 0
	}
	c.ensureHiddenTombstonesLocked()
	return len(c.registry.hiddenTombstoneOrder)
}

func (c *Conn) hiddenTombstoneHeadLocked() tombstoneIDLookup {
	if c == nil {
		return tombstoneIDLookup{}
	}
	c.ensureHiddenTombstonesLocked()
	if len(c.registry.hiddenTombstoneOrder) == 0 {
		return tombstoneIDLookup{}
	}
	return tombstoneIDLookup{streamID: c.registry.hiddenTombstoneOrder[0], present: true}
}

func (c *Conn) hiddenTombstoneTailLocked() tombstoneIDLookup {
	if c == nil {
		return tombstoneIDLookup{}
	}
	c.ensureHiddenTombstonesLocked()
	if len(c.registry.hiddenTombstoneOrder) == 0 {
		return tombstoneIDLookup{}
	}
	return tombstoneIDLookup{streamID: c.registry.hiddenTombstoneOrder[len(c.registry.hiddenTombstoneOrder)-1], present: true}
}

func (c *Conn) appendHiddenTombstoneLocked(streamID uint64) {
	if c == nil {
		return
	}
	c.ensureHiddenTombstonesLocked()
	tombstone, ok := c.registry.tombstones[streamID]
	if !ok || !tombstone.Hidden {
		return
	}
	if idx := c.hiddenTombstoneIndexLocked(streamID, tombstone.HiddenIndex); idx.found() {
		if tombstone.HiddenIndex != idx.index {
			tombstone.HiddenIndex = idx.index
			c.registry.tombstones[streamID] = tombstone
		}
		return
	}
	tombstone.HiddenIndex = len(c.registry.hiddenTombstoneOrder)
	c.registry.tombstones[streamID] = tombstone
	c.registry.hiddenTombstoneOrder = append(c.registry.hiddenTombstoneOrder, streamID)
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
	copy(c.registry.hiddenTombstoneOrder[idx.index:], c.registry.hiddenTombstoneOrder[idx.index+1:])
	c.registry.hiddenTombstoneOrder = c.registry.hiddenTombstoneOrder[:len(c.registry.hiddenTombstoneOrder)-1]
	for i := idx.index; i < len(c.registry.hiddenTombstoneOrder); i++ {
		id := c.registry.hiddenTombstoneOrder[i]
		next := c.registry.tombstones[id]
		next.HiddenIndex = i
		c.registry.tombstones[id] = next
	}
}

func (c *Conn) clearHiddenTombstonesLocked() {
	if c == nil {
		return
	}
	c.registry.hiddenTombstoneOrder = nil
	c.registry.hiddenTombstonesInit = false
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
	for c.trackedSessionMemoryLocked() > c.sessionMemoryHardCapLocked() {
		if !c.reapNewestHiddenControlStateLocked() {
			return
		}
	}
}

func (c *Conn) hiddenControlStateRetainedLocked() int {
	return c.hiddenTombstoneCountLocked()
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
			c.registry.hiddenTombstonesInit = false
			c.ensureHiddenTombstonesLocked()
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
