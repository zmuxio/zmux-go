package zmux

import (
	"sync"
	"time"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/wire"
)

type writeBatchScratch struct {
	batch            []writeRequest
	items            []rt.BatchItem
	ordered          []writeRequest
	rejected         []rejectedWriteRequest
	encoded          []byte
	encodedHandle    *writeBatchEncodedBuffer
	explicitGroups   map[uint64]struct{}
	explicitGroupIDs []uint64
	queuedByStream   map[*nativeStream]uint64
	queuedStreams    []*nativeStream
}

type rejectedWriteRequest struct {
	req writeRequest
	err error
}

type dequeuedWriteWorkKind uint8

const (
	dequeuedWriteWorkRequest dequeuedWriteWorkKind = 1 + iota
	dequeuedWriteWorkControl
	dequeuedWriteWorkClosed
)

type dequeuedWriteWork struct {
	req  writeRequest
	lane writeLane
	kind dequeuedWriteWorkKind
}

const maxControlBatchesPerWake = 4

var maxRetainedWriteBatchBytes = maxWriteBatchFrames * int(DefaultSettings().MaxFramePayload)

type streamValueAccumulator struct {
	scratch      *writeBatchScratch
	capHint      int
	singleStream *nativeStream
	singleValue  uint64
	values       map[*nativeStream]uint64
}

type writeBatchEncodedBuffer struct {
	bucket int
	buf    []byte
}

type writeBatchEncodedBufferBucket struct {
	size int
	pool sync.Pool
}

var writeBatchEncodedBufferBuckets = newWriteBatchEncodedBufferBuckets()

func clearWriteRequests(reqs []writeRequest) {
	clear(reqs)
}

func clearRejectedWriteRequests(reqs []rejectedWriteRequest) {
	clear(reqs)
}

func (s *writeBatchScratch) itemSlice(n int) []rt.BatchItem {
	if cap(s.items) < n {
		s.items = make([]rt.BatchItem, n)
	}
	s.items = s.items[:n]
	return s.items
}

func (s *writeBatchScratch) batchSlice(n int, capHint int) []writeRequest {
	if capHint < n {
		capHint = n
	}
	if cap(s.batch) > 0 {
		clearWriteRequests(s.batch[:cap(s.batch)])
	}
	if cap(s.batch) < capHint {
		s.batch = make([]writeRequest, n, capHint)
	} else {
		s.batch = s.batch[:n]
	}
	return s.batch
}

func (s *writeBatchScratch) orderedSlice(n int) []writeRequest {
	if cap(s.ordered) > 0 {
		clearWriteRequests(s.ordered[:cap(s.ordered)])
	}
	if cap(s.ordered) < n {
		s.ordered = make([]writeRequest, n)
	}
	s.ordered = s.ordered[:n]
	return s.ordered
}

func (s *writeBatchScratch) rejectedSlice(capHint int) []rejectedWriteRequest {
	if cap(s.rejected) > 0 {
		clearRejectedWriteRequests(s.rejected[:cap(s.rejected)])
	}
	if cap(s.rejected) < capHint {
		s.rejected = make([]rejectedWriteRequest, 0, capHint)
	} else {
		s.rejected = s.rejected[:0]
	}
	return s.rejected
}

func (s *writeBatchScratch) encodedBuffer(n int) []byte {
	s.releaseEncodedBuffer(s.encoded)
	s.encoded, s.encodedHandle = acquireWriteBatchEncodedBuffer(n)
	return s.encoded
}

func (s *writeBatchScratch) releaseEncodedBuffer(buf []byte) {
	releaseWriteBatchEncodedBuffer(buf, s.encodedHandle)
	s.encoded = nil
	s.encodedHandle = nil
}

func (s *writeBatchScratch) reset() {
	s.releaseEncodedBuffer(s.encoded)
	s.batch = nil
	s.items = nil
	s.ordered = nil
	s.rejected = nil
	s.explicitGroups = nil
	s.explicitGroupIDs = nil
	s.queuedByStream = nil
	s.queuedStreams = nil
}

func (s *writeBatchScratch) clearRetainedBatchRefs() {
	if s == nil {
		return
	}
	if len(s.batch) > 0 {
		clearWriteRequests(s.batch[:len(s.batch)])
	}
	if len(s.ordered) > 0 {
		clearWriteRequests(s.ordered[:len(s.ordered)])
	}
	if len(s.rejected) > 0 {
		clearRejectedWriteRequests(s.rejected[:len(s.rejected)])
	}
	s.clearQueuedStreamRefs()
}

func (s *writeBatchScratch) dataScratch(n int) ([]rt.BatchItem, map[uint64]struct{}) {
	items := s.itemSlice(n)
	if s.explicitGroups == nil {
		s.explicitGroups = make(map[uint64]struct{}, n)
	} else {
		for _, groupID := range s.explicitGroupIDs {
			delete(s.explicitGroups, groupID)
		}
	}
	s.explicitGroupIDs = s.explicitGroupIDs[:0]
	return items, s.explicitGroups
}

func (s *writeBatchScratch) addExplicitGroup(groupID uint64) {
	if _, ok := s.explicitGroups[groupID]; ok {
		return
	}
	s.explicitGroups[groupID] = struct{}{}
	s.explicitGroupIDs = append(s.explicitGroupIDs, groupID)
}

func (s *writeBatchScratch) queuedStreamScratch(capHint int) map[*nativeStream]uint64 {
	if s.queuedByStream == nil {
		s.queuedByStream = make(map[*nativeStream]uint64, capHint)
	} else {
		s.clearQueuedStreamRefs()
	}
	return s.queuedByStream
}

func (s *writeBatchScratch) clearQueuedStreamRefs() {
	if s == nil {
		return
	}
	for _, stream := range s.queuedStreams {
		delete(s.queuedByStream, stream)
	}
	clear(s.queuedStreams)
	s.queuedStreams = s.queuedStreams[:0]
}

func (s *writeBatchScratch) addQueuedStream(stream *nativeStream, queued uint64) {
	if stream == nil || queued == 0 {
		return
	}
	if _, ok := s.queuedByStream[stream]; !ok {
		s.queuedStreams = append(s.queuedStreams, stream)
	}
	s.queuedByStream[stream] = saturatingAdd(s.queuedByStream[stream], queued)
}

func (s *writeBatchScratch) streamValueAccumulator(capHint int) streamValueAccumulator {
	return streamValueAccumulator{
		scratch: s,
		capHint: capHint,
	}
}

func (a *streamValueAccumulator) promote() {
	if a == nil || a.scratch == nil || a.values != nil {
		return
	}
	a.values = a.scratch.queuedStreamScratch(a.capHint)
	if a.singleStream != nil && a.singleValue > 0 {
		a.values[a.singleStream] = a.singleValue
		a.scratch.queuedStreams = append(a.scratch.queuedStreams, a.singleStream)
	}
	a.singleStream = nil
	a.singleValue = 0
}

func (a *streamValueAccumulator) Add(stream *nativeStream, value uint64) {
	if a == nil || stream == nil || value == 0 {
		return
	}
	if a.values != nil {
		a.scratch.addQueuedStream(stream, value)
		return
	}
	if a.singleStream == nil {
		a.singleStream = stream
		a.singleValue = value
		return
	}
	if a.singleStream == stream {
		a.singleValue = saturatingAdd(a.singleValue, value)
		return
	}
	a.promote()
	a.scratch.addQueuedStream(stream, value)
}

func (a *streamValueAccumulator) RememberFirst(stream *nativeStream, value uint64) {
	if a == nil || stream == nil || value == 0 {
		return
	}
	if a.values != nil {
		if _, ok := a.values[stream]; ok {
			return
		}
		a.values[stream] = value
		a.scratch.queuedStreams = append(a.scratch.queuedStreams, stream)
		return
	}
	if a.singleStream == nil {
		a.singleStream = stream
		a.singleValue = value
		return
	}
	if a.singleStream == stream {
		return
	}
	a.promote()
	a.values[stream] = value
	a.scratch.queuedStreams = append(a.scratch.queuedStreams, stream)
}

func (a *streamValueAccumulator) Range(fn func(stream *nativeStream, value uint64)) {
	if fn == nil {
		return
	}
	if a == nil {
		return
	}
	if a.values == nil {
		if a.singleStream != nil && a.singleValue > 0 {
			fn(a.singleStream, a.singleValue)
		}
		return
	}
	for _, stream := range a.scratch.queuedStreams {
		if stream == nil {
			continue
		}
		fn(stream, a.values[stream])
	}
}

func newWriteBatchEncodedBufferBuckets() []writeBatchEncodedBufferBucket {
	const minBucketSize = 64
	if maxRetainedWriteBatchBytes < minBucketSize {
		return nil
	}

	buckets := make([]writeBatchEncodedBufferBucket, 0, 16)
	for size := minBucketSize; size <= maxRetainedWriteBatchBytes; size <<= 1 {
		bucketIndex := len(buckets)
		bucketSize := size
		buckets = append(buckets, writeBatchEncodedBufferBucket{
			size: bucketSize,
			pool: sync.Pool{
				New: func() any {
					return &writeBatchEncodedBuffer{
						bucket: bucketIndex,
						buf:    make([]byte, 0, bucketSize),
					}
				},
			},
		})
		if size > maxRetainedWriteBatchBytes/2 {
			break
		}
	}
	return buckets
}

func writeBatchEncodedBufferBucketIndex(n int) int {
	if n <= 0 {
		return -1
	}
	for i := range writeBatchEncodedBufferBuckets {
		if n <= writeBatchEncodedBufferBuckets[i].size {
			return i
		}
	}
	return -1
}

func acquireWriteBatchEncodedBuffer(n int) ([]byte, *writeBatchEncodedBuffer) {
	if n <= 0 {
		return nil, nil
	}
	if idx := writeBatchEncodedBufferBucketIndex(n); idx >= 0 {
		handle := writeBatchEncodedBufferBuckets[idx].pool.Get().(*writeBatchEncodedBuffer)
		return handle.buf[:0], handle
	}
	return make([]byte, 0, n), nil
}

func releaseWriteBatchEncodedBuffer(buf []byte, handle *writeBatchEncodedBuffer) {
	if handle == nil {
		return
	}
	if handle.bucket < 0 || handle.bucket >= len(writeBatchEncodedBufferBuckets) {
		return
	}
	bucket := &writeBatchEncodedBufferBuckets[handle.bucket]
	handle.buf = retainedWriteBatchEncodedBuffer(buf, handle, bucket.size)
	bucket.pool.Put(handle)
}

func retainedWriteBatchEncodedBuffer(buf []byte, handle *writeBatchEncodedBuffer, bucketSize int) []byte {
	if handle == nil || bucketSize <= 0 {
		return nil
	}
	if cap(handle.buf) != bucketSize {
		return make([]byte, 0, bucketSize)
	}
	if cap(buf) == bucketSize && sameWriteBufferBackingStart(buf, handle.buf) {
		return buf[:0]
	}
	return handle.buf[:0]
}

func sameWriteBufferBackingStart(a, b []byte) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return false
	}
	return &a[:1][0] == &b[:1][0]
}

func (c *Conn) writeLoop() {
	defer c.writer.scratch.reset()
	for {
		work := c.dequeueWriteWork()
		switch work.kind {
		case dequeuedWriteWorkClosed:
			return
		case dequeuedWriteWorkControl:
			if !c.handlePendingControlWake() {
				return
			}
			continue
		case dequeuedWriteWorkRequest:
			batch := c.collectWriteBatch(work.req, work.lane)
			if !c.handleWriteBatch(batch) {
				return
			}
		}
	}
}

func (c *Conn) handlePendingControlWake() bool {
	for handled := 0; ; handled++ {
		c.mu.Lock()
		c.retryReceiveReplenishLocked()
		result := c.takePendingControlWriteRequestLocked()
		c.mu.Unlock()
		if result.err != nil {
			c.closeSessionWithOptions(result.err, closeOriginWriteLoop, closeFrameDefault)
			return false
		}
		if !result.hasRequest() {
			return true
		}
		batch := c.writer.scratch.batchSlice(1, 1)
		batch[0] = result.request
		if !c.handleWriteBatch(batch) {
			return false
		}
		if handled+1 >= maxControlBatchesPerWake {
			c.mu.Lock()
			more := c.hasPendingControlWorkLocked()
			c.mu.Unlock()
			if more {
				c.writer.yieldOrdinary = true
				notify(c.pending.terminalNotify)
				notify(c.pending.controlNotify)
			}
			return true
		}
	}
}

func (c *Conn) completeWriteBatch(batch []writeRequest, err error) {
	if c == nil || len(batch) == 0 {
		return
	}
	c.releaseBatchReservations(batch)
	for i := range batch {
		completeWriteRequest(&batch[i], err)
	}
}

func (c *Conn) handleWriteBatch(batch []writeRequest) bool {
	if c != nil {
		defer c.writer.scratch.clearRetainedBatchRefs()
	}
	batch = c.suppressWriteBatch(batch)
	if len(batch) == 0 {
		return true
	}
	select {
	case <-c.lifecycle.closedCh:
		err := queueVisibleSessionErr(c, c.err())
		c.completeWriteBatch(batch, err)
		return false
	default:
	}
	err := c.writeBatch(batch)
	if err != nil {
		c.completeWriteBatch(batch, err)
		c.closeSessionWithOptions(err, closeOriginWriteLoop, closeFrameDefault)
		return false
	}
	c.completeWriteBatch(batch, nil)
	return true
}

func (c *Conn) tryDequeueWriteWork() (dequeuedWriteWork, bool) {
	select {
	case <-c.lifecycle.closedCh:
		return dequeuedWriteWork{kind: dequeuedWriteWorkClosed}, true
	case req := <-c.writer.urgentWriteCh:
		return dequeuedWriteWork{req: req, lane: writeLaneUrgent, kind: dequeuedWriteWorkRequest}, true
	default:
	}
	if c.writer.yieldOrdinary {
		select {
		case req := <-c.writer.writeCh:
			c.writer.yieldOrdinary = false
			return dequeuedWriteWork{req: req, lane: writeLaneOrdinary, kind: dequeuedWriteWorkRequest}, true
		default:
			c.writer.yieldOrdinary = false
		}
	}
	select {
	case <-c.pending.terminalNotify:
		return dequeuedWriteWork{kind: dequeuedWriteWorkControl}, true
	case <-c.pending.controlNotify:
		return dequeuedWriteWork{kind: dequeuedWriteWorkControl}, true
	default:
	}
	advisory := c.writer.advisoryWriteCh
	select {
	case req := <-advisory:
		return dequeuedWriteWork{req: req, lane: writeLaneAdvisory, kind: dequeuedWriteWorkRequest}, true
	default:
		return dequeuedWriteWork{}, false
	}
}

func (c *Conn) waitDequeueWriteWork() dequeuedWriteWork {
	advisory := c.writer.advisoryWriteCh
	select {
	case <-c.lifecycle.closedCh:
		return dequeuedWriteWork{kind: dequeuedWriteWorkClosed}
	case <-c.pending.terminalNotify:
		return dequeuedWriteWork{kind: dequeuedWriteWorkControl}
	case <-c.pending.controlNotify:
		return dequeuedWriteWork{kind: dequeuedWriteWorkControl}
	case req := <-c.writer.urgentWriteCh:
		return dequeuedWriteWork{req: req, lane: writeLaneUrgent, kind: dequeuedWriteWorkRequest}
	case req := <-advisory:
		return dequeuedWriteWork{req: req, lane: writeLaneAdvisory, kind: dequeuedWriteWorkRequest}
	case req := <-c.writer.writeCh:
		return dequeuedWriteWork{req: req, lane: writeLaneOrdinary, kind: dequeuedWriteWorkRequest}
	}
}

func (c *Conn) dequeueWriteWork() dequeuedWriteWork {
	if work, ready := c.tryDequeueWriteWork(); ready {
		return work
	}
	return c.waitDequeueWriteWork()
}

func (c *Conn) suppressWriteRequest(req writeRequest) error {
	if !req.origin.isStreamGenerated() {
		return nil
	}
	c.mu.Lock()
	err := c.suppressWriteRequestForStreamLocked(&req, req.reservedStream)
	c.mu.Unlock()
	return err
}

func (c *Conn) suppressWriteRequestForStreamLocked(req *writeRequest, stream *nativeStream) error {
	if req == nil || !req.origin.isStreamGenerated() {
		return nil
	}
	if stream == nil {
		streamID, ok := batchStreamID(req)
		if !ok {
			return nil
		}
		stream = c.registry.streams[streamID]
	}
	if stream == nil || !stream.localSend {
		return nil
	}
	return stream.suppressWriteRequestErrLocked(req)
}

func (c *Conn) suppressWriteBatch(batch []writeRequest) []writeRequest {
	filtered := batch[:0]
	rejected := c.writer.scratch.rejectedSlice(len(batch))

	c.mu.Lock()
	inflightQueued := c.writer.scratch.streamValueAccumulator(len(batch))
	for i := range batch {
		req := &batch[i]
		if err := c.suppressWriteRequestForStreamLocked(req, req.reservedStream); err != nil {
			rejected = append(rejected, rejectedWriteRequest{req: *req, err: err})
			continue
		}
		if req.queueReserved && req.queuedBytes > 0 && req.reservedStream != nil {
			inflightQueued.Add(req.reservedStream, req.queuedBytes)
		}
		filtered = append(filtered, *req)
	}
	inflightQueued.Range(func(stream *nativeStream, queued uint64) {
		stream.inflightQueued = saturatingAdd(stream.inflightQueued, queued)
	})
	c.mu.Unlock()

	c.releaseRejectedPreparedRequests(rejected)
	for _, item := range rejected {
		req := item.req
		completeWriteRequest(&req, item.err)
	}
	if len(filtered) < len(batch) {
		clearWriteRequests(batch[len(filtered):])
	}
	return filtered
}

func (c *Conn) writeBatch(batch []writeRequest) error {
	if len(batch) == 0 {
		return nil
	}

	totalEncoded, _, frameCount, err := prepareWriteBatchSize(batch)
	if err != nil {
		return err
	}

	start := time.Now()
	written, err := 0, error(nil)
	buf := c.writer.scratch.encodedBuffer(totalEncoded)
	defer func() {
		c.writer.scratch.releaseEncodedBuffer(buf)
	}()
	for _, req := range batch {
		for _, frame := range req.frames {
			buf, err = appendFrameBinaryTrusted(buf, frame)
			if err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		written = len(buf)
		err = rt.WriteAll(c.io.conn, buf)
	}
	if err != nil {
		return err
	}
	now := time.Now()
	c.mu.Lock()
	c.noteFlushAndRateLocked(frameCount, written, now, now.Sub(start))
	c.mu.Unlock()
	notify(c.signals.livenessCh)
	return nil
}

func prepareWriteBatchSize(batch []writeRequest) (totalEncoded int, payloadBytes int, frameCount int, err error) {
	maxInt := int(^uint(0) >> 1)
	maxUintInt := uint64(maxInt)
	encodedTotal := uint64(0)
	payloadTotal := uint64(0)

	for i := range batch {
		req := &batch[i]
		classifyWriteRequest(req)
		if len(req.frames) > maxInt-frameCount {
			return 0, 0, 0, frameSizeError("send batch", errPayloadTooLarge)
		}
		frameCount += len(req.frames)
		if frameCount > maxInt/maxEncodedFrameOverhead {
			return 0, 0, 0, frameSizeError("send batch", errPayloadTooLarge)
		}
		for _, frame := range req.frames {
			encoded := txFrameEncodedBytes(frame)
			if encoded > maxUintInt || encodedTotal > maxUintInt-encoded {
				return 0, 0, 0, frameSizeError("send batch", errPayloadTooLarge)
			}
			encodedTotal += encoded

			payloadLen := uint64(frame.payloadLength())
			if payloadTotal > maxUintInt-payloadLen {
				return 0, 0, 0, frameSizeError("send batch", errPayloadTooLarge)
			}
			payloadTotal += payloadLen
		}
	}
	return int(encodedTotal), int(payloadTotal), frameCount, nil
}

const maxWriteBatchFrames = 32
const maxEncodedFrameOverhead = 17

func (c *Conn) collectWriteBatch(first writeRequest, lane writeLane) []writeRequest {
	if lane == writeLaneOrdinary || lane == writeLaneAdvisory {
		return c.collectOrdinaryWriteBatch(first, lane)
	}
	batch := c.writer.scratch.batchSlice(1, maxWriteBatchFrames)
	batch[0] = first
	return rt.CollectReadyBatchInto(batch, c.writeLaneChan(lane), maxWriteBatchFrames, func(batch []writeRequest) []writeRequest {
		return c.orderWriteBatch(batch, lane)
	})
}

func (c *Conn) collectOrdinaryWriteBatch(first writeRequest, firstLane writeLane) []writeRequest {
	batch := c.writer.scratch.batchSlice(1, maxWriteBatchFrames)
	batch[0] = first
	return rt.CollectAlternatingReadyBatchInto(
		batch,
		c.writeLaneChan(writeLaneOrdinary),
		c.advisoryReadChan(),
		firstLane == writeLaneOrdinary,
		maxWriteBatchFrames,
		func(batch []writeRequest) []writeRequest {
			return c.orderWriteBatch(batch, writeLaneOrdinary)
		},
	)
}

func (c *Conn) orderWriteBatch(batch []writeRequest, lane writeLane) []writeRequest {
	if len(batch) < 2 {
		return batch
	}
	if sameStreamBurstKeepsOrder(batch, lane) {
		return batch
	}

	order := c.batchOrder(batch, lane)
	if len(order) != len(batch) || batchOrderIsIdentity(order) {
		return batch
	}

	ordered := c.writer.scratch.orderedSlice(len(batch))
	for i, idx := range order {
		if idx < 0 || idx >= len(batch) {
			return batch
		}
		ordered[i] = batch[idx]
	}
	return ordered
}

func appendFrameBinaryTrusted(dst []byte, frame txFrame) ([]byte, error) {
	dst, err := wire.AppendFrameHeaderTrustedCachedStreamID(
		dst,
		frame.Code(),
		frame.StreamID,
		frame.streamIDPacked,
		frame.streamIDLen,
		uint64(frame.payloadLength()),
	)
	if err != nil {
		return nil, err
	}
	return frame.appendPayload(dst), nil
}

func sameStreamBurstKeepsOrder(batch []writeRequest, lane writeLane) bool {
	if lane != writeLaneOrdinary && lane != writeLaneAdvisory {
		return false
	}
	if len(batch) == 0 {
		return false
	}
	first := &batch[0]
	classifyWriteRequest(first)
	if !first.requestStreamScoped || first.requestIsPriorityUpdate {
		return false
	}
	streamID := first.requestStreamID
	for i := 1; i < len(batch); i++ {
		req := &batch[i]
		classifyWriteRequest(req)
		if !req.requestStreamScoped || req.requestIsPriorityUpdate || req.requestStreamID != streamID {
			return false
		}
	}
	return true
}

func batchOrderIsIdentity(order []int) bool {
	for i, idx := range order {
		if idx != i {
			return false
		}
	}
	return true
}

func (c *Conn) batchOrder(batch []writeRequest, lane writeLane) []int {
	switch lane {
	case writeLaneUrgent:
		return rt.OrderBatchIndices(
			rt.BatchConfig{Urgent: true},
			nil,
			c.urgentBatchItems(batch),
		)
	case writeLaneOrdinary, writeLaneAdvisory:
		items := c.dataBatchItems(batch)
		return c.writer.scheduler.Order(
			rt.BatchConfig{
				GroupFair:       c.config.peer.Settings.SchedulerHints == SchedulerGroupFair,
				SchedulerHint:   c.config.peer.Settings.SchedulerHints,
				MaxFramePayload: c.config.peer.Settings.MaxFramePayload,
			},
			items,
		)
	default:
		return rt.OrderBatchIndices(rt.BatchConfig{}, nil, nil)
	}
}

func (c *Conn) advisoryReadChan() <-chan writeRequest {
	if c.writer.advisoryWriteCh != nil {
		return c.writer.advisoryWriteCh
	}
	return c.writer.writeCh
}

func (c *Conn) schedulerTracksExplicitGroupsLocked() bool {
	return c != nil && c.config.peer.Settings.SchedulerHints == SchedulerGroupFair
}

func (c *Conn) trackedExplicitGroupCountLocked() int {
	if c == nil {
		return 0
	}
	return c.writer.scheduler.TrackedExplicitGroupCount()
}

func (c *Conn) trackedGroupBucketLocked(stream *nativeStream) uint64 {
	if stream == nil {
		return 0
	}
	if stream.trackedGroup != 0 {
		return stream.trackedGroup
	}
	if stream.groupTracked && stream.groupExplicit && stream.group != 0 {
		return stream.group
	}
	return 0
}

func (c *Conn) selectTrackedGroupBucketLocked(stream *nativeStream) uint64 {
	if !c.tracksExplicitGroupLocked(stream) {
		return 0
	}
	if tracked := c.trackedGroupBucketLocked(stream); tracked != 0 {
		return tracked
	}
	if refs := c.writer.scheduler.ActiveGroupRefs[stream.group]; refs > 0 {
		return stream.group
	}
	if c.trackedExplicitGroupCountLocked() < rt.MaxExplicitGroups {
		return stream.group
	}
	return rt.FallbackGroupBucket
}

func (c *Conn) tracksExplicitGroupLocked(stream *nativeStream) bool {
	if c == nil || stream == nil {
		return false
	}
	return c.schedulerTracksExplicitGroupsLocked() &&
		stream.idSet &&
		stream.localSend &&
		!state.SendTerminal(stream.effectiveSendHalfStateLocked()) &&
		stream.groupExplicit &&
		stream.group != 0
}

func (c *Conn) maybeTrackStreamGroupLocked(stream *nativeStream) {
	if c == nil || stream == nil || stream.groupTracked {
		return
	}
	groupBucket := c.selectTrackedGroupBucketLocked(stream)
	if groupBucket == 0 {
		return
	}
	c.writer.scheduler.TrackExplicitGroup(groupBucket)
	stream.groupTracked = true
	stream.trackedGroup = groupBucket
}

func (c *Conn) untrackStreamGroupLocked(stream *nativeStream) {
	if c == nil || stream == nil || !stream.groupTracked {
		return
	}
	groupBucket := c.trackedGroupBucketLocked(stream)
	stream.groupTracked = false
	stream.trackedGroup = 0
	if groupBucket != 0 {
		c.writer.scheduler.UntrackExplicitGroup(groupBucket)
	}
}

func (c *Conn) setStreamGroupLocked(stream *nativeStream, group uint64, explicit bool) {
	if stream == nil {
		return
	}
	c.untrackStreamGroupLocked(stream)
	stream.group = group
	stream.groupExplicit = explicit
	stream.trackedGroup = 0
	c.maybeTrackStreamGroupLocked(stream)
}

func (c *Conn) dropWriteBatchStateLocked(stream *nativeStream) {
	if c == nil || stream == nil {
		return
	}
	groupBucket := c.trackedGroupBucketLocked(stream)
	c.untrackStreamGroupLocked(stream)
	c.writer.scheduler.DropStream(stream.id, groupBucket != 0, groupBucket)
}

func (c *Conn) clearWriteBatchStateLocked() {
	if c == nil {
		return
	}
	c.writer.scheduler.Clear()
}

func (c *Conn) streamBatchGroupKeyLocked(stream *nativeStream, streamID uint64, explicitGroups map[uint64]struct{}, groupFair bool) rt.GroupKey {
	groupKey := rt.GroupKey{Kind: 0, Value: streamID}
	if stream == nil || !groupFair || !stream.groupExplicit || stream.group == 0 {
		return groupKey
	}
	if tracked := c.trackedGroupBucketLocked(stream); tracked != 0 {
		return rt.GroupKey{Kind: 1, Value: tracked}
	}
	group := stream.group
	if _, ok := explicitGroups[group]; ok {
		return rt.GroupKey{Kind: 1, Value: group}
	}
	if len(explicitGroups) < rt.MaxExplicitGroups {
		c.writer.scratch.addExplicitGroup(group)
		return rt.GroupKey{Kind: 1, Value: group}
	}
	return rt.GroupKey{Kind: 1, Value: rt.FallbackGroupBucket}
}

func (c *Conn) urgentBatchItems(batch []writeRequest) []rt.BatchItem {
	items := c.writer.scratch.itemSlice(len(batch))
	for i := range batch {
		req := &batch[i]
		classifyWriteRequest(req)
		items[i] = rt.BatchItem{
			Request: rt.RequestMeta{
				GroupKey:     rt.GroupKey{Kind: 2, Value: uint64(i)},
				StreamID:     req.requestStreamID,
				StreamScoped: req.requestStreamScoped,
				UrgencyRank:  req.requestUrgencyRank,
				Cost:         req.requestCost,
			},
		}
	}
	return items
}

func (c *Conn) dataBatchItems(batch []writeRequest) []rt.BatchItem {
	items, explicitGroups := c.writer.scratch.dataScratch(len(batch))
	groupFair := c.config.peer.Settings.SchedulerHints == SchedulerGroupFair

	c.mu.Lock()
	for i := range batch {
		req := &batch[i]
		classifyWriteRequest(req)
		item := rt.BatchItem{
			Request: rt.RequestMeta{
				GroupKey:         rt.GroupKey{Kind: 2, Value: uint64(i)},
				Cost:             req.requestCost,
				IsPriorityUpdate: req.requestStreamScoped && req.requestIsPriorityUpdate,
			},
		}

		if req.requestStreamScoped {
			item.Request.StreamScoped = true
			item.Request.StreamID = req.requestStreamID
			item.Request.GroupKey = rt.GroupKey{Kind: 0, Value: req.requestStreamID}
			if s := c.registry.streams[req.requestStreamID]; s != nil {
				item.Stream.Priority = s.priority
				item.Request.GroupKey = c.streamBatchGroupKeyLocked(s, req.requestStreamID, explicitGroups, groupFair)
			}
		}

		items[i] = item
	}
	c.mu.Unlock()

	return items
}
