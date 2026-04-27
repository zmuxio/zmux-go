package zmux

import (
	"errors"
	"fmt"
	"time"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// connPendingControlState owns buffered non-close control/advisory accounting
// and the runtime queues used to drain those stream/session-scoped updates.
type connPendingControlState struct {
	controlNotify         chan struct{}
	terminalNotify        chan struct{}
	pendingControlBudget  uint64
	pendingPriorityBudget uint64
	controlBytes          uint64
	sessionMaxData        uint64
	hasSessionMaxData     bool
	sessionBlocked        uint64
	hasSessionBlocked     bool
	sessionBlockedSet     bool
	streamQueues          [pendingStreamQueueCount]streamSparseQueueState
	priorityBytes         uint64
	preparedPriorityBytes uint64
	sessionBlockedAt      uint64
}

const minPendingControlBudget = 64 << 10

type pendingStreamControlVisitFn func(stream *nativeStream, value uint64) (remove bool, stop bool)

func storePendingControlPayload(existing, payload []byte) []byte {
	return storeRetainedBytes(existing, payload, retainedBytesBorrowed)
}

func (c *Conn) pendingControlBudgetLocked() uint64 {
	if c == nil {
		return 0
	}
	if c.pending.pendingControlBudget > 0 {
		return c.pending.pendingControlBudget
	}
	maxPayload := c.config.peer.Settings.MaxControlPayloadBytes
	if maxPayload == 0 {
		maxPayload = c.config.local.Settings.MaxControlPayloadBytes
	}
	if maxPayload == 0 {
		maxPayload = DefaultSettings().MaxControlPayloadBytes
	}
	budget := saturatingMul(maxPayload, 8)
	if budget < minPendingControlBudget {
		budget = minPendingControlBudget
	}
	return budget
}

func pendingControlVarintBytes(v uint64) uint64 {
	return clampedVarintLen62(v)
}

func pendingSessionControlBytes(v uint64) uint64 {
	return pendingControlVarintBytes(v)
}

func pendingStreamControlBytes(streamID, v uint64) uint64 {
	return pendingControlVarintBytes(streamID) + pendingControlVarintBytes(v)
}

type sessionControlKind uint8

const (
	sessionControlMaxData sessionControlKind = iota
	sessionControlBlocked
)

type streamControlKind uint8

const (
	streamControlMaxData streamControlKind = iota
	streamControlBlocked
	streamControlCount
)

type pendingSessionControlValue struct {
	value   uint64
	present bool
}

func (v pendingSessionControlValue) bytes() uint64 {
	if !v.present {
		return 0
	}
	return pendingSessionControlBytes(v.value)
}

func (c *Conn) pendingSessionControlValueLocked(kind sessionControlKind) pendingSessionControlValue {
	switch kind {
	case sessionControlMaxData:
		return pendingSessionControlValue{value: c.pending.sessionMaxData, present: c.pending.hasSessionMaxData}
	case sessionControlBlocked:
		return pendingSessionControlValue{value: c.pending.sessionBlocked, present: c.pending.hasSessionBlocked}
	default:
		return pendingSessionControlValue{}
	}
}

func (c *Conn) storePendingSessionControlValueLocked(kind sessionControlKind, v uint64) bool {
	if c == nil {
		return false
	}
	switch kind {
	case sessionControlMaxData:
		c.pending.sessionMaxData = v
		c.pending.hasSessionMaxData = true
	case sessionControlBlocked:
		c.pending.sessionBlocked = v
		c.pending.hasSessionBlocked = true
	default:
		return false
	}
	return true
}

func (c *Conn) clearPendingSessionControlValueLocked(kind sessionControlKind) bool {
	if c == nil {
		return false
	}
	switch kind {
	case sessionControlMaxData:
		if !c.pending.hasSessionMaxData && c.pending.sessionMaxData == 0 {
			return false
		}
		c.pending.sessionMaxData = 0
		c.pending.hasSessionMaxData = false
	case sessionControlBlocked:
		if !c.pending.hasSessionBlocked && c.pending.sessionBlocked == 0 {
			return false
		}
		c.pending.sessionBlocked = 0
		c.pending.hasSessionBlocked = false
	default:
		return false
	}
	return true
}

func sessionControlFrameType(kind sessionControlKind) FrameType {
	switch kind {
	case sessionControlMaxData:
		return FrameTypeMAXDATA
	case sessionControlBlocked:
		return FrameTypeBLOCKED
	default:
		return 0
	}
}

func (c *Conn) setPendingSessionControlLocked(kind sessionControlKind, v uint64) bool {
	if c == nil {
		return false
	}
	current := c.pendingSessionControlValueLocked(kind)
	oldBytes := current.bytes()
	newBytes := pendingSessionControlBytes(v)
	if !c.replacePendingControlBytesLocked(oldBytes, newBytes, pendingControlReplaceOptions{
		mode:   pendingControlReplaceChecked,
		notify: pendingControlNotifyAvailable,
	}) {
		return false
	}
	return c.storePendingSessionControlValueLocked(kind, v)
}

func (c *Conn) dropPendingSessionControlLocked(kind sessionControlKind) bool {
	if c == nil {
		return false
	}
	current := c.pendingSessionControlValueLocked(kind)
	if !current.present {
		return false
	}
	c.replacePendingControlBytesLocked(current.bytes(), 0, pendingControlReplaceOptions{
		mode:   pendingControlReplaceForced,
		notify: pendingControlNotifySkip,
	})
	return c.clearPendingSessionControlValueLocked(kind)
}

type pendingStreamControlValue struct {
	value   uint64
	present bool
}

func (v pendingStreamControlValue) bytes(streamID uint64) uint64 {
	if !v.present {
		return 0
	}
	return pendingStreamControlBytes(streamID, v.value)
}

func shouldFlushPendingStreamControlLocked(kind streamControlKind, stream *nativeStream) (flush bool, keep bool) {
	if stream == nil {
		return false, false
	}
	return stream.pendingControlFlushStateLocked(kind)
}

func streamControlFrameType(kind streamControlKind) FrameType {
	switch kind {
	case streamControlMaxData:
		return FrameTypeMAXDATA
	case streamControlBlocked:
		return FrameTypeBLOCKED
	default:
		return 0
	}
}

type pendingControlReplaceMode uint8

const (
	pendingControlReplaceChecked pendingControlReplaceMode = iota
	pendingControlReplaceForced
)

type pendingControlNotifyPolicy uint8

const (
	pendingControlNotifyAvailable pendingControlNotifyPolicy = iota
	pendingControlNotifySkip
)

type pendingControlReplaceOptions struct {
	mode   pendingControlReplaceMode
	notify pendingControlNotifyPolicy
}

func (o pendingControlReplaceOptions) force() bool {
	return o.mode == pendingControlReplaceForced
}

func (o pendingControlReplaceOptions) shouldNotify() bool {
	return o.notify != pendingControlNotifySkip
}

func (c *Conn) replacePendingControlBytesLocked(oldBytes, newBytes uint64, opts pendingControlReplaceOptions) bool {
	if c == nil {
		return false
	}
	prevTracked := c.trackedSessionMemoryLocked()
	currentBucket := c.pending.controlBytes
	projected := c.pending.controlBytes
	if oldBytes > 0 {
		projected = csub(projected, oldBytes)
	}
	projected = saturatingAdd(projected, newBytes)
	if !opts.force() && projected > c.pendingControlBudgetLocked() {
		return false
	}
	if !opts.force() && c.projectedTrackedSessionMemoryLocked(currentBucket, projected) > c.sessionMemoryHardCapLocked() {
		return false
	}
	c.pending.controlBytes = projected
	if opts.shouldNotify() {
		c.notifySessionMemoryAvailableLocked(prevTracked)
	}
	return true
}

func (c *Conn) setPendingGoAwayPayloadLocked(payload []byte) bool {
	if c == nil {
		return false
	}
	oldBytes := uint64(0)
	if c.sessionControl.hasPendingGoAway {
		oldBytes = uint64(len(c.sessionControl.pendingGoAwayPayload))
	}
	newBytes := uint64(len(payload))
	if !c.replacePendingControlBytesLocked(oldBytes, newBytes, pendingControlReplaceOptions{
		mode:   pendingControlReplaceForced,
		notify: pendingControlNotifyAvailable,
	}) {
		return false
	}
	c.sessionControl.pendingGoAwayPayload = storePendingControlPayload(c.sessionControl.pendingGoAwayPayload, payload)
	c.sessionControl.hasPendingGoAway = true
	return true
}

func (c *Conn) clearPendingGoAwayLocked() {
	if c == nil || !c.sessionControl.hasPendingGoAway {
		return
	}
	c.replacePendingControlBytesLocked(uint64(len(c.sessionControl.pendingGoAwayPayload)), 0, pendingControlReplaceOptions{
		mode:   pendingControlReplaceForced,
		notify: pendingControlNotifyAvailable,
	})
	c.sessionControl.pendingGoAwayPayload = nil
	c.sessionControl.hasPendingGoAway = false
}

func (c *Conn) pendingTargetStreamLocked(streamID uint64) *nativeStream {
	if c == nil || c.registry.streams == nil {
		return nil
	}
	stream := c.registry.streams[streamID]
	if !streamMatchesID(stream, streamID) {
		return nil
	}
	return stream
}

type pendingStreamQueueKind uint8

const (
	pendingStreamQueueMaxData pendingStreamQueueKind = iota
	pendingStreamQueueBlocked
	pendingStreamQueuePriority
	pendingStreamQueueTerminal
	pendingStreamQueueCount
)

func pendingStreamQueueKindForControl(kind streamControlKind) pendingStreamQueueKind {
	switch kind {
	case streamControlBlocked:
		return pendingStreamQueueBlocked
	default:
		return pendingStreamQueueMaxData
	}
}

func (kind pendingStreamQueueKind) valid() bool {
	return kind < pendingStreamQueueCount
}

func (p *connPendingControlState) streamQueueState(kind pendingStreamQueueKind) *streamSparseQueueState {
	if p == nil || !kind.valid() {
		return nil
	}
	return &p.streamQueues[kind]
}

func (p *connPendingControlState) streamQueueCount(kind pendingStreamQueueKind) int {
	queueState := p.streamQueueState(kind)
	if queueState == nil {
		return 0
	}
	return queueState.count
}

func findQueuedStreamByID(q indexedStreamQueue, streamID uint64) *nativeStream {
	if !q.ready() || streamID == 0 {
		return nil
	}
	for i := q.state.head; i < len(q.state.items); i++ {
		stream := q.state.items[i]
		if streamMatchesID(stream, streamID) {
			return stream
		}
	}
	return nil
}

type pendingStreamQueueSpec struct {
	getIndex func(*nativeStream) int32
	setIndex func(*nativeStream, int32)
}

var pendingStreamQueueSpecs = [...]pendingStreamQueueSpec{
	pendingStreamQueueMaxData: {
		getIndex: getPendingMaxDataIndex,
		setIndex: setPendingMaxDataIndex,
	},
	pendingStreamQueueBlocked: {
		getIndex: getPendingBlockedIndex,
		setIndex: setPendingBlockedIndex,
	},
	pendingStreamQueuePriority: {
		getIndex: getPendingPriorityIndex,
		setIndex: setPendingPriorityIndex,
	},
	pendingStreamQueueTerminal: {
		getIndex: getPendingTerminalIndex,
		setIndex: setPendingTerminalIndex,
	},
}

func (c *Conn) pendingStreamQueueLocked(kind pendingStreamQueueKind) indexedStreamQueue {
	if c == nil || !kind.valid() {
		return indexedStreamQueue{}
	}
	spec := pendingStreamQueueSpecs[kind]
	return newIndexedStreamQueue(c.pending.streamQueueState(kind), spec.getIndex, spec.setIndex)
}

func (c *Conn) ensurePendingStreamQueueLocked(kind pendingStreamQueueKind) {
	if c == nil {
		return
	}
	queue := c.pendingStreamQueueLocked(kind)
	if !queue.ready() {
		return
	}
	ensureIndexedStreamQueueFromStreams(queue, c.registry.streams, func(stream *nativeStream) bool {
		return stream.inPendingQueueLocked(kind)
	})
}

func (c *Conn) pendingQueuedTargetStreamLocked(kind pendingStreamQueueKind, streamID uint64) *nativeStream {
	if c == nil || streamID == 0 {
		return nil
	}
	if stream := c.pendingTargetStreamLocked(streamID); stream != nil {
		return stream
	}
	return findQueuedStreamByID(c.pendingStreamQueueLocked(kind), streamID)
}

func (c *Conn) pendingStreamControlQueueLocked(kind streamControlKind) indexedStreamQueue {
	return c.pendingStreamQueueLocked(pendingStreamQueueKindForControl(kind))
}

func (c *Conn) ensurePendingStreamControlQueueLocked(kind streamControlKind) {
	c.ensurePendingStreamQueueLocked(pendingStreamQueueKindForControl(kind))
}

func (c *Conn) pendingStreamControlHasEntriesLocked(kind streamControlKind) bool {
	if c == nil {
		return false
	}
	c.ensurePendingStreamControlQueueLocked(kind)
	return c.pendingStreamControlQueueLocked(kind).countValue() > 0
}

func (c *Conn) pendingStreamControlCountLocked(kind streamControlKind) int {
	if c == nil {
		return 0
	}
	c.ensurePendingStreamControlQueueLocked(kind)
	return c.pendingStreamControlQueueLocked(kind).countValue()
}

func (c *Conn) pendingStreamControlValueLocked(kind streamControlKind, streamID uint64) pendingStreamControlValue {
	if c == nil {
		return pendingStreamControlValue{}
	}
	stream := c.pendingQueuedTargetStreamLocked(pendingStreamQueueKindForControl(kind), streamID)
	return stream.pendingControlValueLocked(kind)
}

func (c *Conn) setPendingStreamControlLocked(kind streamControlKind, streamID, v uint64) bool {
	if c == nil {
		return false
	}
	stream := c.pendingTargetStreamLocked(streamID)
	if stream == nil {
		return false
	}
	existing := stream.pendingControlValueLocked(kind)
	oldBytes := existing.bytes(streamID)
	newBytes := pendingStreamControlBytes(streamID, v)
	if !c.replacePendingControlBytesLocked(oldBytes, newBytes, pendingControlReplaceOptions{
		mode:   pendingControlReplaceChecked,
		notify: pendingControlNotifyAvailable,
	}) {
		return false
	}
	stream.setPendingControlValueLocked(kind, v)
	queue := c.pendingStreamControlQueueLocked(kind)
	if queue.ready() {
		queue.append(stream)
	}
	return true
}

func (c *Conn) dropPendingStreamControlEntryLocked(kind streamControlKind, streamID uint64) bool {
	if c == nil {
		return false
	}
	c.ensurePendingStreamControlQueueLocked(kind)
	prevBytes := c.pending.controlBytes
	value := c.pendingStreamControlValueLocked(kind, streamID)
	queue := c.pendingStreamControlQueueLocked(kind)
	queueKind := pendingStreamQueueKindForControl(kind)
	removed := false
	if live := c.registry.streams[streamID]; streamMatchesID(live, streamID) {
		live.clearPendingControlValueLocked(kind)
		removed = queue.remove(live, live.pendingQueueIndex(queueKind), streamQueueEntryNonNil)
	} else {
		for i := queue.state.head; i < len(queue.state.items); i++ {
			queued := queue.state.items[i]
			if !streamMatchesID(queued, streamID) {
				continue
			}
			queued.clearPendingControlValueLocked(kind)
			removed = queue.remove(queued, int32(i), streamQueueEntryNonNil)
			break
		}
	}
	if !value.present {
		if removed {
			c.recomputePendingControlBytesLocked()
		}
		return c.pending.controlBytes < prevBytes
	}
	c.replacePendingControlBytesLocked(value.bytes(streamID), 0, pendingControlReplaceOptions{
		mode:   pendingControlReplaceForced,
		notify: pendingControlNotifySkip,
	})
	return true
}

func (c *Conn) clearPendingStreamControlEntryLocked(kind streamControlKind, streamID uint64) {
	if c == nil {
		return
	}
	prevTracked := c.trackedSessionMemoryLocked()
	released := c.dropPendingStreamControlEntryLocked(kind, streamID)
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
}

func (c *Conn) visitPendingStreamControlLocked(kind streamControlKind, visit pendingStreamControlVisitFn) bool {
	if c == nil || visit == nil || !c.pendingStreamControlHasEntriesLocked(kind) {
		return false
	}
	queue := c.pendingStreamControlQueueLocked(kind)
	released := false
	for i := queue.state.head; i < len(queue.state.items); {
		stream := queue.state.items[i]
		if stream == nil || stream.pendingQueueIndex(pendingStreamQueueKindForControl(kind)) != int32(i) {
			i++
			continue
		}
		id := stream.id
		if live := c.pendingTargetStreamLocked(id); live != stream {
			if c.dropPendingStreamControlEntryLocked(kind, id) {
				released = true
			}
			continue
		}
		flush, keep := shouldFlushPendingStreamControlLocked(kind, stream)
		if keep {
			i++
			continue
		}
		if !flush {
			if c.dropPendingStreamControlEntryLocked(kind, id) {
				released = true
			}
			continue
		}
		value := stream.pendingControlValueLocked(kind)
		if !value.present {
			if c.dropPendingStreamControlEntryLocked(kind, id) {
				released = true
			}
			continue
		}
		remove, stop := visit(stream, value.value)
		if remove {
			if c.dropPendingStreamControlEntryLocked(kind, id) {
				released = true
			}
		} else {
			i++
		}
		if stop {
			break
		}
	}
	return released
}

func makePendingVarintControlTxFrame(frameType FrameType, streamID uint64, value uint64) txFrame {
	frame := makeTxFrame(frameType, 0, streamID)
	payload := encodeClampedVarint62(value)
	frame.setFlatPayload(payload)
	return frame
}

func (c *Conn) visitPendingStreamControlTxFramesLocked(kind streamControlKind, stop *bool, appendFrame func(txFrame) bool) bool {
	if c == nil || appendFrame == nil {
		return false
	}
	if stop == nil {
		stop = new(bool)
	}
	if *stop || !c.pendingStreamControlHasEntriesLocked(kind) {
		return false
	}
	frameType := streamControlFrameType(kind)
	if frameType == 0 {
		return false
	}
	return c.visitPendingStreamControlLocked(kind, func(stream *nativeStream, value uint64) (bool, bool) {
		if *stop {
			return false, true
		}
		if !appendFrame(makePendingVarintControlTxFrame(frameType, stream.id, value)) {
			return false, true
		}
		return true, false
	})
}

func (c *Conn) releasePendingStreamControlLocked(kind streamControlKind) {
	if c == nil {
		return
	}
	c.forEachKnownStreamLocked(func(stream *nativeStream) {
		stream.clearPendingControlValueLocked(kind)
	})
	queue := c.pendingStreamControlQueueLocked(kind)
	queue.clear(func(stream *nativeStream) {
		stream.setPendingQueueIndex(pendingStreamQueueKindForControl(kind), invalidStreamQueueIndex)
	})
	queue.state.items = nil
	queue.state.init = false
}

func (c *Conn) recomputePendingControlBytesLocked() {
	if c == nil {
		return
	}
	total := uint64(0)
	if c.pending.hasSessionMaxData {
		total = saturatingAdd(total, pendingSessionControlBytes(c.pending.sessionMaxData))
	}
	if c.pending.hasSessionBlocked {
		total = saturatingAdd(total, pendingSessionControlBytes(c.pending.sessionBlocked))
	}
	if c.sessionControl.hasPendingGoAway {
		total = saturatingAdd(total, uint64(len(c.sessionControl.pendingGoAwayPayload)))
	}
	for id, stream := range c.registry.streams {
		if !streamMatchesID(stream, id) {
			continue
		}
		if value := stream.pendingControlValueLocked(streamControlMaxData); value.present {
			total = saturatingAdd(total, value.bytes(id))
		}
		if value := stream.pendingControlValueLocked(streamControlBlocked); value.present {
			total = saturatingAdd(total, value.bytes(id))
		}
	}
	c.pending.controlBytes = total
}

func (c *Conn) recomputePendingPriorityBytesLocked() {
	if c == nil {
		return
	}
	total := uint64(0)
	for id, stream := range c.registry.streams {
		if !streamMatchesID(stream, id) {
			continue
		}
		if stream.hasPendingPriorityUpdateLocked() {
			total = saturatingAdd(total, uint64(len(stream.pending.priority)))
		}
	}
	c.pending.priorityBytes = total
}

func (c *Conn) pendingPriorityQueueLocked() indexedStreamQueue {
	return c.pendingStreamQueueLocked(pendingStreamQueuePriority)
}

func (c *Conn) pendingTerminalQueueLocked() indexedStreamQueue {
	return c.pendingStreamQueueLocked(pendingStreamQueueTerminal)
}

func (c *Conn) pendingPriorityQueueCountLocked() int {
	return c.pending.streamQueueCount(pendingStreamQueuePriority)
}

func (c *Conn) pendingTerminalQueueCountLocked() int {
	return c.pending.streamQueueCount(pendingStreamQueueTerminal)
}

func (c *Conn) ensurePendingPriorityUpdateQueueLocked() {
	c.ensurePendingStreamQueueLocked(pendingStreamQueuePriority)
}

func (c *Conn) ensurePendingTerminalControlQueueLocked() {
	c.ensurePendingStreamQueueLocked(pendingStreamQueueTerminal)
}

func (c *Conn) pendingPriorityTargetStreamLocked(streamID uint64) *nativeStream {
	return c.pendingQueuedTargetStreamLocked(pendingStreamQueuePriority, streamID)
}

func (c *Conn) pendingTerminalTargetStreamLocked(streamID uint64) *nativeStream {
	return c.pendingQueuedTargetStreamLocked(pendingStreamQueueTerminal, streamID)
}

func (c *Conn) dropPendingPriorityUpdateEntryLocked(streamID uint64) bool {
	if c == nil || streamID == 0 {
		return false
	}
	c.ensurePendingPriorityUpdateQueueLocked()
	prevBytes := c.pending.priorityBytes
	released := false
	stream := c.pendingPriorityTargetStreamLocked(streamID)
	currentIndex := invalidStreamQueueIndex
	if stream != nil {
		currentIndex = stream.pendingQueueIndex(pendingStreamQueuePriority)
		if stream.hasPendingPriorityUpdateLocked() {
			c.pending.priorityBytes = csub(c.pending.priorityBytes, uint64(len(stream.pending.priority)))
			released = true
		}
		stream.clearPendingPriorityUpdateLocked()
		stream.setPendingQueueIndex(pendingStreamQueuePriority, invalidStreamQueueIndex)
	}
	if removed := c.pendingPriorityQueueLocked().remove(stream, currentIndex, streamQueueEntryNonNil); removed && !released {
		c.recomputePendingPriorityBytesLocked()
	}
	return released || c.pending.priorityBytes < prevBytes
}

func (c *Conn) pendingTerminalControlHasEntriesLocked() bool {
	if c == nil {
		return false
	}
	c.ensurePendingTerminalControlQueueLocked()
	return c.pendingTerminalQueueLocked().countValue() > 0
}

func (c *Conn) pendingTerminalControlBytesLocked() uint64 {
	if c == nil {
		return 0
	}
	c.ensurePendingTerminalControlQueueLocked()
	queue := c.pendingTerminalQueueLocked()
	total := uint64(0)
	for i := queue.state.head; i < len(queue.state.items); i++ {
		stream := queue.state.items[i]
		if stream == nil || stream.pendingQueueIndex(pendingStreamQueueTerminal) != int32(i) {
			continue
		}
		total = saturatingAdd(total, stream.pendingTerminalControlBytesLocked())
	}
	return total
}

func (c *Conn) dropPendingTerminalControlEntryLocked(streamID uint64) bool {
	if c == nil || streamID == 0 {
		return false
	}
	queue := c.pendingTerminalQueueLocked()
	stream := c.pendingTerminalTargetStreamLocked(streamID)
	if stream == nil {
		return false
	}
	currentIndex := invalidStreamQueueIndex
	if streamBelongsToConn(stream, c) {
		currentIndex = stream.pendingQueueIndex(pendingStreamQueueTerminal)
	}
	if currentIndex == invalidStreamQueueIndex {
		currentIndex = queue.getIndex(stream)
	}
	if currentIndex == invalidStreamQueueIndex || !stream.hasPendingTerminalControlLocked() {
		stream.setPendingQueueIndex(pendingStreamQueueTerminal, invalidStreamQueueIndex)
		return false
	}

	oldBytes := stream.pendingTerminalControlBytesLocked()
	removed := queue.remove(stream, currentIndex, streamQueueEntryNonNil)
	if !removed {
		stream.setPendingQueueIndex(pendingStreamQueueTerminal, invalidStreamQueueIndex)
		return false
	}
	stream.clearPendingTerminalControlLocked()
	c.replacePendingControlBytesLocked(oldBytes, 0, pendingControlReplaceOptions{
		mode:   pendingControlReplaceForced,
		notify: pendingControlNotifySkip,
	})
	stream.setPendingQueueIndex(pendingStreamQueueTerminal, invalidStreamQueueIndex)
	return true
}

func (c *Conn) releasePendingTerminalControlLocked() {
	if c == nil {
		return
	}
	c.ensurePendingTerminalControlQueueLocked()
	queue := c.pendingTerminalQueueLocked()
	queue.clear(func(stream *nativeStream) {
		if stream == nil {
			return
		}
		stream.clearPendingTerminalControlLocked()
		stream.setPendingQueueIndex(pendingStreamQueueTerminal, invalidStreamQueueIndex)
	})
}

type pendingTerminalControlResult struct {
	changed  bool
	accepted bool
}

type pendingPriorityQueueStatus uint8

const (
	pendingPriorityQueueAccepted pendingPriorityQueueStatus = iota + 1
	pendingPriorityQueueDroppedBudget
	pendingPriorityQueueDroppedMemory
	pendingPriorityQueueDroppedUnavailable
)

type pendingPriorityQueueResult struct {
	status           pendingPriorityQueueStatus
	projectedTracked uint64
}

func (r pendingPriorityQueueResult) accepted() bool {
	return r.status == pendingPriorityQueueAccepted
}

func (r pendingPriorityQueueResult) structuredErr(c *Conn) error {
	switch r.status {
	case pendingPriorityQueueDroppedBudget:
		return wireError(CodeInternal, "queue PRIORITY_UPDATE", fmt.Errorf("pending priority update budget exceeded"))
	case pendingPriorityQueueDroppedMemory:
		hardCap := uint64(0)
		if c != nil {
			hardCap = c.sessionMemoryHardCapLocked()
		}
		return wireError(CodeInternal, "queue PRIORITY_UPDATE", errSessionMemoryCapExceeded(r.projectedTracked, hardCap))
	case pendingPriorityQueueDroppedUnavailable:
		return ErrSessionClosed
	default:
		return wireError(CodeInternal, "queue PRIORITY_UPDATE", fmt.Errorf("pending priority update rejected"))
	}
}

func (c *Conn) setPendingTerminalControlLocked(stream *nativeStream, apply func(*nativeStream) (changed bool, coalesced bool, superseded bool)) pendingTerminalControlResult {
	if c == nil || stream == nil || apply == nil {
		return pendingTerminalControlResult{}
	}
	oldFlags := stream.pending.flags & (streamPendingTerminalStop | streamPendingTerminalReset | streamPendingTerminalAbort)
	oldState := stream.pending.terminal
	oldBytes := stream.pendingTerminalControlBytesLocked()
	changed, coalesced, superseded := apply(stream)
	if coalesced {
		c.metrics.coalescedTerminalSignals = saturatingAdd(c.metrics.coalescedTerminalSignals, 1)
	}
	if superseded {
		c.metrics.droppedSupersededControls = saturatingAdd(c.metrics.droppedSupersededControls, 1)
	}
	if !changed {
		return pendingTerminalControlResult{accepted: true}
	}
	stream.recomputePendingTerminalControlBytesLocked()
	newBytes := stream.pendingTerminalControlBytesLocked()
	if !c.replacePendingControlBytesLocked(oldBytes, newBytes, pendingControlReplaceOptions{
		mode:   pendingControlReplaceChecked,
		notify: pendingControlNotifyAvailable,
	}) {
		stream.pending.terminal = oldState
		stream.pending.flags &^= streamPendingTerminalStop | streamPendingTerminalReset | streamPendingTerminalAbort
		stream.pending.flags |= oldFlags
		return pendingTerminalControlResult{}
	}
	c.pendingTerminalQueueLocked().append(stream)
	return pendingTerminalControlResult{changed: true, accepted: true}
}

func (c *Conn) clearPendingPriorityUpdateStateLocked() {
	if c == nil {
		return
	}
	c.forEachKnownStreamLocked(func(stream *nativeStream) {
		stream.clearPendingPriorityUpdateLocked()
		stream.setPendingQueueIndex(pendingStreamQueuePriority, invalidStreamQueueIndex)
	})
	queue := c.pendingPriorityQueueLocked()
	queue.clear(func(stream *nativeStream) {
		stream.setPendingQueueIndex(pendingStreamQueuePriority, invalidStreamQueueIndex)
	})
	queue.state.items = nil
	queue.state.init = false
	c.pending.priorityBytes = 0
}

func (c *Conn) ensurePendingNonCloseControlLocked() bool {
	if c == nil {
		return false
	}
	if c.allowLocalNonCloseControlLocked() {
		return true
	}
	c.clearPendingNonCloseControlStateLocked()
	return false
}

func (c *Conn) clearPendingNonCloseControlStateLocked() {
	if c == nil {
		return
	}
	keepControlBytes := uint64(0)
	if c.sessionControl.hasPendingGoAway {
		keepControlBytes = uint64(len(c.sessionControl.pendingGoAwayPayload))
	}
	c.clearPendingSessionControlValueLocked(sessionControlMaxData)
	c.releasePendingStreamControlLocked(streamControlMaxData)
	c.clearPendingSessionControlValueLocked(sessionControlBlocked)
	c.releasePendingStreamControlLocked(streamControlBlocked)
	c.clearPendingPriorityUpdateStateLocked()
	c.releasePendingTerminalControlLocked()
	c.pending.controlBytes = keepControlBytes
	c.clearSessionBlockedStateLocked()
}

func (c *Conn) queueResolvedStreamControlAsync(kind streamControlKind, stream *nativeStream, v uint64, drop func()) bool {
	if c == nil {
		return false
	}
	if stream == nil {
		if drop != nil {
			drop()
		}
		return false
	}
	flush, keep := stream.pendingControlFlushStateLocked(kind)
	if !flush && !keep {
		if drop != nil {
			drop()
		}
		return false
	}
	if stream.skipPendingControlQueueLocked(kind, v) {
		return true
	}
	if !c.setPendingStreamControlLocked(kind, stream.id, v) {
		if drop != nil {
			drop()
		}
		return false
	}
	stream.notePendingControlQueuedLocked(kind, v)
	notify(c.pending.controlNotify)
	return true
}

func (c *Conn) queueStreamMaxDataAsync(streamID, v uint64) bool {
	if !c.ensurePendingNonCloseControlLocked() {
		return false
	}
	v = clampVarint62(v)
	stream := c.registry.streams[streamID]
	if !streamMatchesID(stream, streamID) {
		c.clearPendingStreamControlEntryLocked(streamControlMaxData, streamID)
		return false
	}
	return c.queueResolvedStreamControlAsync(streamControlMaxData, stream, v, func() {
		c.clearPendingStreamControlEntryLocked(streamControlMaxData, streamID)
	})
}

func (c *Conn) queueStreamBlockedAsync(stream *nativeStream, v uint64) {
	if stream == nil {
		return
	}
	if !c.ensurePendingNonCloseControlLocked() {
		return
	}
	if !stream.idSet || !streamBelongsToConn(stream, c) {
		c.releaseStreamRuntimeStateLocked(stream, streamRuntimeBlocked)
		return
	}
	if c.registry.streams != nil {
		if live := c.registry.streams[stream.id]; live != nil && live != stream {
			c.releaseStreamRuntimeStateLocked(stream, streamRuntimeBlocked)
			return
		}
	}
	c.queueResolvedStreamControlAsync(streamControlBlocked, stream, v, func() {
		c.releaseStreamRuntimeStateLocked(stream, streamRuntimeBlocked)
	})
}

func (c *Conn) visitPendingTerminalControlTxFramesLocked(stop *bool, collector *pendingTxFrameCollector) bool {
	if c == nil || collector == nil {
		return false
	}
	if stop == nil {
		stop = new(bool)
	}
	c.ensurePendingTerminalControlQueueLocked()
	queue := c.pendingTerminalQueueLocked()
	if !queue.ready() || queue.countValue() == 0 {
		return false
	}

	released := false
	for i := queue.state.head; i < len(queue.state.items); {
		if *stop {
			break
		}
		stream := queue.state.items[i]
		if stream == nil || stream.pendingQueueIndex(pendingStreamQueueTerminal) != int32(i) {
			i++
			continue
		}
		id := stream.id
		target := c.pendingTerminalTargetStreamLocked(id)
		if target == nil {
			if c.dropPendingTerminalControlEntryLocked(id) {
				released = true
			}
			continue
		}
		flush, keep := target.pendingTerminalFlushStateLocked()
		if !flush {
			if !keep && c.dropPendingTerminalControlEntryLocked(id) {
				released = true
			}
			i++
			continue
		}
		frames := target.pendingTerminalFramesLocked()
		if len(frames) == 0 {
			if c.dropPendingTerminalControlEntryLocked(id) {
				released = true
			}
			continue
		}
		if !collector.appendFrames(frames) {
			*stop = true
			break
		}
		if c.dropPendingTerminalControlEntryLocked(id) {
			released = true
		}
	}
	return released
}

func (c *Conn) visitPendingUrgentControlTxFramesLocked(stop *bool, collector *pendingTxFrameCollector) bool {
	if c == nil || collector == nil {
		return false
	}
	if stop == nil {
		stop = new(bool)
	}

	released := false
	if c.pendingTerminalControlHasEntriesLocked() && !*stop {
		if c.visitPendingTerminalControlTxFramesLocked(stop, collector) {
			released = true
		}
	}

	if c.pending.hasSessionMaxData && !*stop {
		if c.visitPendingSessionControlTxFramesLocked(sessionControlMaxData, stop, collector.append) {
			released = true
		}
	}

	if c.pendingStreamControlHasEntriesLocked(streamControlMaxData) && !*stop {
		if c.visitPendingStreamControlTxFramesLocked(streamControlMaxData, stop, collector.append) {
			released = true
		}
	}

	if c.pending.hasSessionBlocked && !*stop {
		if c.visitPendingSessionControlTxFramesLocked(sessionControlBlocked, stop, collector.append) {
			released = true
		}
	}

	if c.pendingStreamControlHasEntriesLocked(streamControlBlocked) && !*stop {
		if c.visitPendingStreamControlTxFramesLocked(streamControlBlocked, stop, collector.append) {
			released = true
		}
	}
	return released
}

func (c *Conn) drainPendingUrgentControlTxFramesLocked() (urgent []txFrame) {
	prevTracked := c.trackedSessionMemoryLocked()
	var frameBuf [4]txFrame
	collector := newPendingTxFrameCollector(frameBuf[:0], 0, 0)
	released := c.visitPendingUrgentControlTxFramesLocked(&collector.stopped, &collector)
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
	return collector.frames
}

func (c *Conn) queuePendingSessionControlAsync(kind sessionControlKind, v uint64) bool {
	if c == nil || !c.ensurePendingNonCloseControlLocked() {
		return false
	}
	current := c.pendingSessionControlValueLocked(kind)
	switch kind {
	case sessionControlMaxData:
		if current.present && v <= current.value {
			return false
		}
	case sessionControlBlocked:
		if current.present && current.value == v {
			return false
		}
		if c.pending.sessionBlockedSet && c.pending.sessionBlockedAt == v {
			return false
		}
	default:
		return false
	}
	if !c.setPendingSessionControlLocked(kind, v) {
		return false
	}
	if kind == sessionControlBlocked {
		c.markSessionBlockedStateLocked(v)
	}
	notify(c.pending.controlNotify)
	return true
}

func (c *Conn) ensurePendingSessionMaxDataLocked(v uint64) bool {
	if c == nil || !c.ensurePendingNonCloseControlLocked() {
		return false
	}
	v = clampVarint62(v)
	current := c.pendingSessionControlValueLocked(sessionControlMaxData)
	if current.present && v <= current.value {
		return true
	}
	if !c.setPendingSessionControlLocked(sessionControlMaxData, v) {
		return false
	}
	notify(c.pending.controlNotify)
	return true
}

func (c *Conn) ensurePendingStreamMaxDataLocked(stream *nativeStream, v uint64) bool {
	if c == nil || stream == nil || !c.ensurePendingNonCloseControlLocked() {
		return false
	}
	if !stream.idSet {
		return false
	}
	v = clampVarint62(v)
	live := c.registry.streams[stream.id]
	if !streamMatchesID(live, stream.id) {
		c.clearPendingStreamControlEntryLocked(streamControlMaxData, stream.id)
		return false
	}
	stream = live
	flush, keep := stream.pendingControlFlushStateLocked(streamControlMaxData)
	if !flush && !keep {
		c.clearPendingStreamControlEntryLocked(streamControlMaxData, stream.id)
		return false
	}
	if stream.skipPendingControlQueueLocked(streamControlMaxData, v) {
		return true
	}
	if !c.setPendingStreamControlLocked(streamControlMaxData, stream.id, v) {
		return false
	}
	stream.notePendingControlQueuedLocked(streamControlMaxData, v)
	notify(c.pending.controlNotify)
	return true
}

func (c *Conn) visitPendingSessionControlTxFramesLocked(kind sessionControlKind, stop *bool, appendFrame func(txFrame) bool) bool {
	if c == nil || appendFrame == nil {
		return false
	}
	current := c.pendingSessionControlValueLocked(kind)
	if !current.present {
		return false
	}
	if stop != nil && *stop {
		return false
	}
	frameType := sessionControlFrameType(kind)
	if frameType == 0 || !appendFrame(makePendingVarintControlTxFrame(frameType, 0, current.value)) {
		return false
	}
	return c.dropPendingSessionControlLocked(kind)
}

type pendingWriteRequestResult struct {
	request writeRequest
	err     error
	ready   bool
}

func (r pendingWriteRequestResult) hasRequest() bool {
	return r.ready
}

type pendingTxFrameCollector struct {
	frames      []txFrame
	queuedBytes uint64
	maxFrames   int
	maxBytes    uint64
	stopped     bool
}

func newPendingTxFrameCollector(frames []txFrame, maxFrames int, maxBytes uint64) pendingTxFrameCollector {
	if maxFrames < 0 {
		maxFrames = 0
	}
	return pendingTxFrameCollector{
		frames:    frames[:0],
		maxFrames: maxFrames,
		maxBytes:  maxBytes,
	}
}

func (c *pendingTxFrameCollector) append(frame txFrame) bool {
	if c == nil {
		return false
	}
	frameBytes := txFrameBufferedBytes(frame)
	if len(c.frames) > 0 {
		if c.maxFrames > 0 && len(c.frames) >= c.maxFrames {
			c.stopped = true
			return false
		}
		if c.maxBytes > 0 && saturatingAdd(c.queuedBytes, frameBytes) > c.maxBytes {
			c.stopped = true
			return false
		}
	}
	c.frames = append(c.frames, frame)
	c.queuedBytes = saturatingAdd(c.queuedBytes, frameBytes)
	if c.maxFrames > 0 && len(c.frames) >= c.maxFrames {
		c.stopped = true
	}
	return true
}

func (c *pendingTxFrameCollector) canAppendFrames(frames []txFrame) bool {
	if c == nil {
		return false
	}
	if len(frames) == 0 {
		return true
	}
	if len(c.frames) > 0 && c.maxFrames > 0 && len(c.frames)+len(frames) > c.maxFrames {
		return false
	}
	if len(c.frames) > 0 && c.maxBytes > 0 {
		total := c.queuedBytes
		for _, frame := range frames {
			total = saturatingAdd(total, txFrameBufferedBytes(frame))
		}
		if total > c.maxBytes {
			return false
		}
	}
	return true
}

func (c *pendingTxFrameCollector) appendFrames(frames []txFrame) bool {
	if c == nil {
		return false
	}
	if !c.canAppendFrames(frames) {
		c.stopped = true
		return false
	}
	for _, frame := range frames {
		if !c.append(frame) {
			return false
		}
	}
	return true
}

func (c *pendingTxFrameCollector) empty() bool {
	return c == nil || len(c.frames) == 0
}

func buildPendingControlWriteRequest(frames []txFrame, queuedBytes uint64, lane writeLane) writeRequest {
	req := writeRequest{
		frames:                frames,
		origin:                writeRequestOriginProtocol,
		terminalPolicy:        terminalWriteReject,
		cloneFramesBeforeSend: false,
		queuedBytes:           queuedBytes,
	}
	switch lane {
	case writeLaneUrgent:
		req.urgentReserved = true
	case writeLaneAdvisory:
		req.advisoryReserved = true
	default:
	}
	return req
}

func collectPendingWriteRequestsLocked(capHint int, take func() pendingWriteRequestResult) ([]writeRequest, error) {
	if take == nil {
		return nil, nil
	}
	var reqBuf [4]writeRequest
	reqs := reqBuf[:0]
	if capHint > cap(reqBuf) {
		reqs = make([]writeRequest, 0, capHint)
	}
	for {
		result := take()
		if result.err != nil {
			return nil, result.err
		}
		if !result.hasRequest() {
			return reqs, nil
		}
		reqs = append(reqs, result.request)
	}
}

func (c *Conn) takePendingControlWriteRequestLocked() pendingWriteRequestResult {
	if !c.ensurePendingNonCloseControlLocked() {
		return pendingWriteRequestResult{}
	}

	result := c.takePendingUrgentControlRequestLocked()
	if result.hasRequest() || result.err != nil {
		return result
	}
	return c.takePendingPriorityUpdateRequestLocked()
}

func (c *Conn) hasPendingControlWorkLocked() bool {
	if c == nil {
		return false
	}
	c.ensurePendingPriorityUpdateQueueLocked()
	return c.pendingTerminalControlHasEntriesLocked() ||
		c.pending.hasSessionMaxData ||
		c.pendingStreamControlHasEntriesLocked(streamControlMaxData) ||
		c.pending.hasSessionBlocked ||
		c.pendingStreamControlHasEntriesLocked(streamControlBlocked) ||
		c.pendingPriorityQueueCountLocked() > 0
}

func (c *Conn) takePendingUrgentControlRequestLocked() pendingWriteRequestResult {
	if c == nil {
		return pendingWriteRequestResult{}
	}
	if !c.pendingTerminalControlHasEntriesLocked() &&
		!c.pending.hasSessionMaxData &&
		!c.pendingStreamControlHasEntriesLocked(streamControlMaxData) &&
		!c.pending.hasSessionBlocked &&
		!c.pendingStreamControlHasEntriesLocked(streamControlBlocked) {
		return pendingWriteRequestResult{}
	}

	maxBytes := c.urgentLaneCapLocked()
	prevTracked := c.trackedSessionMemoryLocked()
	var frameBuf [maxWriteBatchFrames]txFrame
	collector := newPendingTxFrameCollector(frameBuf[:0], maxWriteBatchFrames, maxBytes)
	released := c.visitPendingUrgentControlTxFramesLocked(&collector.stopped, &collector)

	if collector.empty() {
		c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
		return pendingWriteRequestResult{}
	}
	if err := c.sessionMemoryCapErrorWithAdditionalLocked("queue urgent control", collector.queuedBytes); err != nil {
		c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
		return pendingWriteRequestResult{err: err}
	}
	c.flow.urgentQueuedBytes = saturatingAdd(c.flow.urgentQueuedBytes, collector.queuedBytes)
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
	return pendingWriteRequestResult{
		request: buildPendingControlWriteRequest(collector.frames, collector.queuedBytes, writeLaneUrgent),
		ready:   true,
	}
}

func (c *Conn) takePendingPriorityUpdateRequestLocked() pendingWriteRequestResult {
	if c == nil {
		return pendingWriteRequestResult{}
	}
	c.ensurePendingPriorityUpdateQueueLocked()
	for c.pendingPriorityQueueCountLocked() > 0 {
		var flushIDs [maxWriteBatchFrames]uint64
		var chunkBuf [maxWriteBatchFrames]txFrame
		prevTracked := c.trackedSessionMemoryLocked()
		batch := c.collectPendingPriorityUpdateBatchLocked(flushIDs[:0], chunkBuf[:0], maxWriteBatchFrames)
		if len(batch.ids) == 0 {
			c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(batch.released))
			return pendingWriteRequestResult{}
		}

		plan := rt.PlanPriorityAdvisoryHandoff(c.trackedSessionMemoryLocked(), batch.removedPendingBytes, batch.queuedBytes, c.sessionMemoryHardCapLocked())
		released := batch.released
		if !plan.Accept {
			for _, id := range batch.ids {
				if c.dropPendingPriorityUpdateEntryLocked(id) {
					released = true
				}
			}
			c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
			continue
		}

		for _, id := range batch.ids {
			if c.dropPendingPriorityUpdateEntryLocked(id) {
				released = true
			}
		}
		c.flow.advisoryQueuedBytes = saturatingAdd(c.flow.advisoryQueuedBytes, batch.queuedBytes)
		c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
		return pendingWriteRequestResult{
			request: buildPendingControlWriteRequest(batch.frames, batch.queuedBytes, writeLaneAdvisory),
			ready:   true,
		}
	}
	return pendingWriteRequestResult{}
}

const minPendingPriorityBudget = 64 << 10

func buildPriorityUpdatePayload(caps Capabilities, update MetadataUpdate, maxPayload uint64) ([]byte, error) {
	payload, err := wire.BuildPriorityUpdatePayload(caps, update.Priority, update.Group, maxPayload)
	switch {
	case errors.Is(err, wire.ErrEmptyMetadataUpdate):
		return nil, ErrEmptyMetadataUpdate
	case errors.Is(err, wire.ErrPriorityUpdateUnavailable):
		return nil, ErrPriorityUpdateUnavailable
	case errors.Is(err, wire.ErrPriorityUpdateTooLarge):
		return nil, ErrPriorityUpdateTooLarge
	default:
		return payload, err
	}
}

func appendPriorityUpdatePayload(dst []byte, caps Capabilities, update MetadataUpdate, maxPayload uint64) ([]byte, error) {
	payload, err := wire.AppendPriorityUpdatePayload(dst, caps, update.Priority, update.Group, maxPayload)
	switch {
	case errors.Is(err, wire.ErrEmptyMetadataUpdate):
		return nil, ErrEmptyMetadataUpdate
	case errors.Is(err, wire.ErrPriorityUpdateUnavailable):
		return nil, ErrPriorityUpdateUnavailable
	case errors.Is(err, wire.ErrPriorityUpdateTooLarge):
		return nil, ErrPriorityUpdateTooLarge
	default:
		return payload, err
	}
}

func parsePriorityUpdatePayload(payload []byte) (streamMetadata, bool, error) {
	return wire.ParsePriorityUpdatePayload(payload)
}

func (c *Conn) handlePriorityUpdatePayloadError(err error) error {
	c.mu.Lock()
	ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
	c.mu.Unlock()
	if ignore {
		return nil
	}
	return frameSizeError("handle PRIORITY_UPDATE", err)
}

func (c *Conn) handleDroppedPriorityUpdateFrame() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		return nil
	}
	c.ingress.droppedPriorityUpdate = saturatingAdd(c.ingress.droppedPriorityUpdate, 1)
	return nil
}

func (c *Conn) finishPriorityUpdateLocked(stream *nativeStream, meta streamMetadata, oldGroup uint64, oldExplicit bool, now time.Time) error {
	if !c.applyReceivedMetadataLocked(stream, meta, receivedMetadataOnUpdate) {
		return c.recordNoOpPriorityUpdateLocked(now)
	}
	if meta.HasGroup && c.shouldRecordGroupRebucketChurnLocked(stream, oldGroup, oldExplicit) {
		if err := c.recordGroupRebucketChurnLocked(now); err != nil {
			return err
		}
	}
	c.clearNoOpPriorityUpdateLocked()
	return nil
}

func (c *Conn) applyPriorityUpdateLocked(streamID uint64, meta streamMetadata, now time.Time) error {
	stream := c.registry.streams[streamID]
	if stream == nil {
		return nil
	}
	if stream.ignoreLateNonOpeningControlLocked() {
		return c.recordNoOpPriorityUpdateLocked(now)
	}
	if stream.awaitingPeerVisibilityLocked() {
		return nil
	}
	oldGroup := stream.group
	oldExplicit := stream.groupExplicit
	c.markPeerVisibleLocked(stream)
	return c.finishPriorityUpdateLocked(stream, meta, oldGroup, oldExplicit, now)
}

func (c *Conn) handlePriorityUpdateFrame(streamID uint64, payload []byte) error {
	if !c.config.negotiated.Capabilities.Has(CapabilityPriorityUpdate) {
		return nil
	}

	meta, ok, err := parsePriorityUpdatePayload(payload)
	if err != nil {
		return c.handlePriorityUpdatePayloadError(err)
	}
	if !ok {
		return c.handleDroppedPriorityUpdateFrame()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		return nil
	}
	return c.applyPriorityUpdateLocked(streamID, meta, time.Now())
}

func (c *Conn) pendingPriorityBudgetLocked() uint64 {
	if c == nil {
		return minPendingPriorityBudget
	}
	if c.pending.pendingPriorityBudget > 0 {
		return c.pending.pendingPriorityBudget
	}
	maxExt := c.config.peer.Settings.MaxExtensionPayloadBytes
	if maxExt == 0 {
		maxExt = c.config.local.Settings.MaxExtensionPayloadBytes
	}
	if maxExt == 0 {
		maxExt = DefaultSettings().MaxExtensionPayloadBytes
	}
	return maxUint64(minPendingPriorityBudget, saturatingMul(maxExt, 8))
}

type pendingPriorityUpdateBatch struct {
	ids                 []uint64
	frames              []txFrame
	removedPendingBytes uint64
	queuedBytes         uint64
	released            bool
}

func buildPendingPriorityUpdateFrame(streamID uint64, payload []byte) txFrame {
	frame := makeTxFrame(FrameTypeEXT, 0, streamID)
	frame.setFlatPayload(payload)
	return frame
}

func (c *Conn) resolvePendingPriorityTargetLocked(streamID uint64, requireIdentified bool) (target *nativeStream, flush bool, keep bool, release sessionMemoryRelease) {
	if c == nil || c.registry.streams == nil || streamID == 0 {
		return nil, false, false, sessionMemoryUnchanged
	}
	target = c.pendingTargetStreamLocked(streamID)
	if target == nil && !requireIdentified {
		target = c.pendingPriorityTargetStreamLocked(streamID)
	}
	if target == nil || (requireIdentified && !streamMatchesID(target, streamID)) {
		released := c.dropPendingPriorityUpdateEntryLocked(streamID)
		return nil, false, false, sessionMemoryReleaseFrom(released)
	}
	flush, keep = target.pendingPriorityFlushStateLocked()
	if flush || keep {
		return target, flush, keep, sessionMemoryUnchanged
	}
	released := c.dropPendingPriorityUpdateEntryLocked(streamID)
	return target, false, false, sessionMemoryReleaseFrom(released)
}

func (c *Conn) replacePendingPriorityUpdateLocked(streamID uint64, payload []byte, ownership retainedBytesOwnership) uint64 {
	if c == nil || streamID == 0 || len(payload) == 0 {
		return 0
	}
	target := c.pendingTargetStreamLocked(streamID)
	if target == nil {
		return 0
	}
	existing := []byte(nil)
	oldLen := uint64(0)
	if target.hasPendingPriorityUpdateLocked() {
		existing = target.pending.priority
		oldLen = uint64(len(existing))
	}
	c.pending.priorityBytes = csub(c.pending.priorityBytes, oldLen)
	stored := storeRetainedBytes(existing, payload, ownership)
	target.setPendingPriorityUpdateLocked(stored)
	c.pendingPriorityQueueLocked().append(target)
	storedLen := uint64(len(stored))
	c.pending.priorityBytes = saturatingAdd(c.pending.priorityBytes, storedLen)
	return storedLen
}

func (c *Conn) collectPendingPriorityUpdateBatchLocked(ids []uint64, frames []txFrame, limit int) pendingPriorityUpdateBatch {
	batch := pendingPriorityUpdateBatch{
		ids: ids[:0],
	}
	if c == nil {
		return batch
	}
	c.ensurePendingPriorityUpdateQueueLocked()
	if c.pendingPriorityQueueCountLocked() == 0 {
		return batch
	}
	collector := newPendingTxFrameCollector(frames, limit, 0)
	queue := c.pendingPriorityQueueLocked()
	for i := queue.state.head; i < len(queue.state.items); {
		stream := queue.state.items[i]
		if stream == nil || stream.pendingQueueIndex(pendingStreamQueuePriority) != int32(i) {
			i++
			continue
		}
		if collector.stopped {
			break
		}
		id := stream.id
		target, flush, keep, release := c.resolvePendingPriorityTargetLocked(id, false)
		if !flush && !keep {
			batch.released = release.released() || batch.released
			continue
		}
		if keep {
			i++
			continue
		}
		if target == nil || !target.hasPendingPriorityUpdateLocked() {
			batch.released = c.dropPendingPriorityUpdateEntryLocked(id) || batch.released
			continue
		}
		payload := target.pending.priority
		frame := buildPendingPriorityUpdateFrame(id, payload)
		if !collector.append(frame) {
			break
		}
		batch.ids = append(batch.ids, id)
		batch.removedPendingBytes = saturatingAdd(batch.removedPendingBytes, uint64(len(payload)))
		i++
	}
	batch.frames = collector.frames
	batch.queuedBytes = collector.queuedBytes
	return batch
}

func (c *Conn) takePendingPriorityUpdateRequestsLocked() ([]writeRequest, error) {
	if c == nil {
		return nil, nil
	}
	c.ensurePendingPriorityUpdateQueueLocked()
	count := c.pendingPriorityQueueCountLocked()
	if count == 0 {
		return nil, nil
	}
	capHint := (count + maxWriteBatchFrames - 1) / maxWriteBatchFrames
	return collectPendingWriteRequestsLocked(capHint, c.takePendingPriorityUpdateRequestLocked)
}

func (c *Conn) queuePriorityUpdateAsync(streamID uint64, payload []byte, ownership retainedBytesOwnership) pendingPriorityQueueResult {
	if len(payload) == 0 {
		return pendingPriorityQueueResult{}
	}
	if !c.ensurePendingNonCloseControlLocked() {
		return pendingPriorityQueueResult{status: pendingPriorityQueueDroppedUnavailable}
	}
	stream, flush, keep, _ := c.resolvePendingPriorityTargetLocked(streamID, true)
	if stream == nil || (!flush && !keep) {
		return pendingPriorityQueueResult{status: pendingPriorityQueueDroppedUnavailable}
	}
	prevTracked := c.trackedSessionMemoryLocked()
	budget := c.pendingPriorityBudgetLocked()
	if budget == 0 {
		return pendingPriorityQueueResult{status: pendingPriorityQueueDroppedBudget}
	}
	oldLen := uint64(0)
	if stream.hasPendingPriorityUpdateLocked() {
		oldLen = uint64(len(stream.pending.priority))
	}
	plan := rt.PlanPendingPriorityUpdate(
		prevTracked,
		c.pending.priorityBytes,
		oldLen,
		uint64(len(payload)),
		budget,
		c.sessionMemoryHardCapLocked(),
	)
	if !plan.Accept {
		if plan.NextPendingBytes > budget {
			return pendingPriorityQueueResult{status: pendingPriorityQueueDroppedBudget, projectedTracked: plan.ProjectedTracked}
		}
		return pendingPriorityQueueResult{status: pendingPriorityQueueDroppedMemory, projectedTracked: plan.ProjectedTracked}
	}
	c.replacePendingPriorityUpdateLocked(streamID, payload, ownership)
	c.pending.priorityBytes = plan.NextPendingBytes
	c.notifySessionMemoryAvailableLocked(prevTracked)
	notify(c.pending.controlNotify)
	return pendingPriorityQueueResult{status: pendingPriorityQueueAccepted, projectedTracked: plan.ProjectedTracked}
}

func (c *Conn) takePendingPriorityUpdateFrameLocked(streamID uint64) preparedPriorityUpdate {
	if !c.ensurePendingNonCloseControlLocked() {
		return preparedPriorityUpdate{}
	}
	stream, flush, keep, _ := c.resolvePendingPriorityTargetLocked(streamID, false)
	if !flush && !keep {
		return preparedPriorityUpdate{}
	}
	if keep {
		return preparedPriorityUpdate{}
	}
	if stream == nil || !stream.hasPendingPriorityUpdateLocked() {
		c.dropPendingPriorityUpdateEntryLocked(streamID)
		return preparedPriorityUpdate{}
	}
	priority := makePreparedPriorityUpdate(streamID, stream.pending.priority)
	if !c.movePendingPriorityUpdateToPreparedLocked(priority) {
		return preparedPriorityUpdate{}
	}
	return priority
}

func (c *Conn) movePendingPriorityUpdateToPreparedLocked(priority preparedPriorityUpdate) bool {
	if c == nil || !priority.hasFrame() {
		return false
	}
	if c.projectedTrackedSessionMemoryLocked(uint64(len(priority.payload)), priority.frameBytes) > c.sessionMemoryHardCapLocked() {
		return false
	}
	c.dropPendingPriorityUpdateEntryLocked(priority.streamID)
	c.pending.preparedPriorityBytes = saturatingAdd(c.pending.preparedPriorityBytes, priority.frameBytes)
	return true
}

func (c *Conn) ensurePreparedPriorityRollbackLocked(req *writeRequest) {
	if c == nil || req == nil || req.preparedPriorityBytes > 0 {
		return
	}
	priority := preparedPriorityUpdateFromFrames(req.frames)
	if !priority.hasFrame() {
		return
	}
	req.setPreparedPriorityUpdate(priority)
}

func (c *Conn) releasePreparedPriorityBytesLocked(n uint64) {
	if c == nil || n == 0 {
		return
	}
	c.pending.preparedPriorityBytes = csub(c.pending.preparedPriorityBytes, n)
}

func (c *Conn) restorePreparedPriorityUpdateLocked(req *writeRequest) {
	if c == nil || req == nil {
		return
	}
	c.ensurePreparedPriorityRollbackLocked(req)
	priority := req.preparedPriorityUpdate()
	if !priority.hasFrame() {
		return
	}

	if !req.preparedPriorityQueued {
		c.releasePreparedPriorityBytesLocked(priority.frameBytes)
	}
	req.preparedPriorityQueued = false

	if !c.allowLocalNonCloseControlLocked() {
		clearPreparedPriorityRollback(req)
		return
	}

	if c.pendingTargetStreamLocked(priority.streamID) == nil {
		clearPreparedPriorityRollback(req)
		return
	}
	c.replacePendingPriorityUpdateLocked(priority.streamID, priority.payload, retainedBytesOwned)
	notify(c.pending.controlNotify)
	clearPreparedPriorityRollback(req)
}
