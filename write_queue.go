package zmux

import (
	"os"
	"runtime"
	"sync"
	"time"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/wire"
)

type writeRequest struct {
	frames []txFrame
	done   chan error
	// donePooled marks completion channels acquired from the internal pool so
	// the hot path can reuse them after normal completion without changing the
	// external request API that tests and control paths observe.
	donePooled   bool
	doneReusable bool
	// preparedNotify is an optional one-shot synchronization hook used by tests
	// that need to observe the "prepared for writer admission" boundary without
	// racing on caller-owned request fields.
	preparedNotify chan struct{}
	// origin identifies whether the request came from the stream state machine
	// or from protocol/control paths that only reuse the writer machinery.
	origin writeRequestOrigin
	// terminalPolicy permits sending an already-terminal request that was
	// intentionally created by a local state transition (for example, the
	// terminal FIN DATA for CloseWrite, RESET for Reset, or ABORT for
	// CloseWithError).
	terminalPolicy terminalWritePolicy
	// cloneFramesBeforeSend delays defensive frame cloning for non-owned
	// requests until they are ready to enter the single writer path.
	cloneFramesBeforeSend bool
	// queueReserved tracks ordinary data-queue reservation held for this
	// request while it waits to enter or leave the writer path.
	queueReserved    bool
	queuedBytes      uint64
	reservedStream   *nativeStream
	urgentReserved   bool
	advisoryReserved bool
	// requestMetaReady guards cached stream/terminal classification derived
	// from frames so hot suppress/admission paths do not rescan the batch.
	requestMetaReady        bool
	requestStreamID         uint64
	requestStreamIDKnown    bool
	requestStreamScoped     bool
	requestUrgencyRank      int
	requestCost             int64
	requestBufferedBytes    uint64
	requestIsPriorityUpdate bool
	requestAllUrgent        bool
	terminalDataPriority    bool
	terminalResetOnly       bool
	terminalAbortOnly       bool
	terminalHasFIN          bool
	// preparedSendBytes / preparedSendFin track local send-side state that was
	// reserved while building this request but is still withdrawable until the
	// request enters the single writer path.
	preparedSendBytes uint64
	preparedSendFin   bool
	// preparedOpenerVisibility marks requests that carry the local opener frame
	// for a local-opened stream and therefore hold the temporary advisory
	// ordering barrier until writer admission or rollback.
	preparedOpenerVisibility openerVisibilityMark
	// preparedPriority* tracks a same-stream piggybacked PRIORITY_UPDATE that
	// was lifted out of the pending advisory bucket while this request is still
	// withdrawable. The request may later restore that update if local admission
	// fails before writer admission.
	preparedPriorityStreamID uint64
	preparedPriorityPayload  []byte
	preparedPriorityBytes    uint64
	preparedPriorityQueued   bool
}

type txFrame struct {
	Type     FrameType
	Flags    byte
	StreamID uint64
	Payload  []byte

	streamIDPacked uint64
	streamIDLen    uint8

	payloadKind    txPayloadKind
	payloadPrefix  []byte
	payloadParts   [][]byte
	payloadPartIdx int
	payloadPartOff int
	payloadPartLen int
	payloadLen     int
}

const postCloseQueueDrainWindow = 5 * time.Millisecond

type txPayloadKind uint8

const (
	txPayloadFlat txPayloadKind = iota
	txPayloadPrefixFlat
	txPayloadParts
	txPayloadPrefixParts
)

var writeRequestDoneChanPool = sync.Pool{
	New: func() any {
		return make(chan error, 1)
	},
}

func drainDoneChan(done chan error) {
	if done == nil {
		return
	}
	select {
	case <-done:
	default:
	}
}

func (req *writeRequest) ensureDoneChan() {
	if req == nil || req.done != nil {
		return
	}
	done := writeRequestDoneChanPool.Get().(chan error)
	drainDoneChan(done)
	req.done = done
	req.donePooled = true
	req.doneReusable = false
}

func (req *writeRequest) markDoneReusable() {
	if req == nil || req.done == nil {
		return
	}
	req.doneReusable = true
}

func (req *writeRequest) tryTakeDoneErr() (error, bool) {
	if req == nil || req.done == nil {
		return nil, false
	}
	select {
	case err := <-req.done:
		req.markDoneReusable()
		return err, true
	default:
		return nil, false
	}
}

func (req *writeRequest) releaseDoneChan() {
	if req == nil || req.done == nil {
		return
	}
	done := req.done
	pooled := req.donePooled && req.doneReusable
	req.done = nil
	req.donePooled = false
	req.doneReusable = false
	if pooled {
		drainDoneChan(done)
		writeRequestDoneChanPool.Put(done)
	}
}

func completeWriteRequest(req *writeRequest, err error) {
	if req == nil || req.done == nil {
		return
	}
	req.done <- err
}

func encodeStreamIDCached(streamID uint64) (uint64, uint8) {
	packed, n, err := wire.PackVarint(streamID)
	if err != nil {
		return 0, 0
	}
	return packed, n
}

func makeTxFrame(frameType FrameType, flags byte, streamID uint64) txFrame {
	packed, n := encodeStreamIDCached(streamID)
	return txFrame{
		Type:           frameType,
		Flags:          flags,
		StreamID:       streamID,
		streamIDPacked: packed,
		streamIDLen:    n,
	}
}

func flatTxFrame(frame Frame) txFrame {
	tx := makeTxFrame(frame.Type, frame.Flags, frame.StreamID)
	tx.setFlatPayload(frame.Payload)
	tx.payloadLen = len(frame.Payload)
	return tx
}

func (f *txFrame) Code() byte {
	if f == nil {
		return 0
	}
	return byte(f.Type) | f.Flags
}

func (f *txFrame) hasPayloadPrefix() bool {
	if f == nil {
		return false
	}
	return f.payloadKind == txPayloadPrefixFlat || f.payloadKind == txPayloadPrefixParts
}

func (f *txFrame) hasPayloadParts() bool {
	if f == nil {
		return false
	}
	return f.payloadKind == txPayloadParts || f.payloadKind == txPayloadPrefixParts
}

func (f *txFrame) payloadLength() int {
	if f == nil {
		return 0
	}
	if f.payloadLen < 0 {
		return 0
	}
	return f.payloadLen
}

func (f *txFrame) appendPayload(dst []byte) []byte {
	if f == nil {
		return dst
	}
	if f.hasPayloadPrefix() {
		dst = append(dst, f.payloadPrefix...)
	}
	if f.hasPayloadParts() {
		idx := f.payloadPartIdx
		off := f.payloadPartOff
		remaining := f.payloadPartLen
		for remaining > 0 && idx < len(f.payloadParts) {
			part := f.payloadParts[idx]
			if off >= len(part) {
				idx++
				off = 0
				continue
			}
			take := len(part) - off
			if take > remaining {
				take = remaining
			}
			dst = append(dst, part[off:off+take]...)
			remaining -= take
			off += take
			if off >= len(part) {
				idx++
				off = 0
			}
		}
		return dst
	}
	return append(dst, f.Payload...)
}

func (f *txFrame) clonedPayload() []byte {
	if f.payloadLength() == 0 {
		return nil
	}
	dst := make([]byte, 0, f.payloadLength())
	return f.appendPayload(dst)
}

func (f *txFrame) payloadForValidation() []byte {
	if f == nil {
		return nil
	}
	if f.hasPayloadPrefix() {
		return f.payloadPrefix
	}
	return f.Payload
}

func (f *txFrame) resetPayloadView() {
	if f == nil {
		return
	}
	f.Payload = nil
	f.payloadPrefix = nil
	f.payloadParts = nil
	f.payloadPartIdx = 0
	f.payloadPartOff = 0
	f.payloadPartLen = 0
	f.payloadKind = txPayloadFlat
}

func (f *txFrame) setFlatPayload(payload []byte) {
	if f == nil {
		return
	}
	f.resetPayloadView()
	f.Payload = payload
}

func (f *txFrame) setPrefixedFlatPayload(prefix, payload []byte) {
	if f == nil {
		return
	}
	if len(payload) == 0 {
		f.setFlatPayload(prefix)
		return
	}
	f.resetPayloadView()
	f.payloadKind = txPayloadPrefixFlat
	f.payloadPrefix = prefix
	f.Payload = payload
}

func (f *txFrame) setPartsPayload(parts [][]byte, idx, off, n int) {
	if f == nil {
		return
	}
	f.resetPayloadView()
	f.payloadKind = txPayloadParts
	f.payloadParts = parts
	f.payloadPartIdx = idx
	f.payloadPartOff = off
	f.payloadPartLen = n
}

func (f *txFrame) setPrefixedPartsPayload(prefix []byte, parts [][]byte, idx, off, n int) {
	if f == nil {
		return
	}
	f.resetPayloadView()
	f.payloadKind = txPayloadPrefixParts
	f.payloadPrefix = prefix
	f.payloadParts = parts
	f.payloadPartIdx = idx
	f.payloadPartOff = off
	f.payloadPartLen = n
}

// txFrameQueueCost is the coarse queued-byte accounting used by admission,
// HWM/LWM thresholds, and queue reservation. It intentionally tracks only the
// type byte plus payload bytes so queue pressure remains stable across later
// encoding strategy changes.
func txFrameQueueCost(frame txFrame) uint64 {
	return uint64(1 + frame.payloadLength())
}

func txFrameBufferedBytes(frame txFrame) uint64 {
	return txFrameQueueCost(frame)
}

func txFramesQueueCost(frames []txFrame) uint64 {
	total := uint64(0)
	for _, frame := range frames {
		total = saturatingAdd(total, txFrameQueueCost(frame))
	}
	return total
}

func txFramesBufferedBytes(frames []txFrame) uint64 {
	return txFramesQueueCost(frames)
}

func txFrameEncodedBytes(frame txFrame) uint64 {
	streamIDLen := int(frame.streamIDLen)
	if streamIDLen == 0 {
		n, err := wire.VarintLen(frame.StreamID)
		if err != nil {
			return txFrameQueueCost(frame)
		}
		streamIDLen = n
	}
	bodyLen := uint64(1+streamIDLen) + uint64(frame.payloadLength())
	frameLenLen, err := wire.VarintLen(bodyLen)
	if err != nil {
		return bodyLen
	}
	return uint64(frameLenLen) + bodyLen
}

func cloneTxFramesIfNeeded(frames []txFrame, clone bool) ([]txFrame, bool) {
	if !clone {
		return frames, false
	}
	if len(frames) == 0 {
		return nil, false
	}
	cloned := make([]txFrame, len(frames))
	copy(cloned, frames)
	for i := range cloned {
		payload := cloned[i].clonedPayload()
		cloned[i].setFlatPayload(payload)
		cloned[i].payloadLen = len(payload)
	}
	return cloned, false
}

type preparedPriorityUpdate struct {
	frame      txFrame
	streamID   uint64
	payload    []byte
	frameBytes uint64
}

func (u preparedPriorityUpdate) hasFrame() bool {
	return u.streamID != 0 && len(u.payload) > 0 && u.frameBytes > 0
}

func (u preparedPriorityUpdate) append(frames []txFrame) []txFrame {
	if !u.hasFrame() {
		return frames
	}
	return append(frames, u.frame)
}

func makePreparedPriorityUpdate(streamID uint64, payload []byte) preparedPriorityUpdate {
	if streamID == 0 || len(payload) == 0 {
		return preparedPriorityUpdate{}
	}
	frame := buildPendingPriorityUpdateFrame(streamID, payload)
	return preparedPriorityUpdate{
		frame:      frame,
		streamID:   streamID,
		payload:    payload,
		frameBytes: txFrameBufferedBytes(frame),
	}
}

func preparedPriorityUpdateFromFrames(frames []txFrame) preparedPriorityUpdate {
	if len(frames) < 2 {
		return preparedPriorityUpdate{}
	}

	first := frames[0]
	if first.Type != FrameTypeEXT || first.StreamID == 0 || len(first.Payload) == 0 {
		return preparedPriorityUpdate{}
	}
	subtype, _, ok := parseExtFrame(first.Payload)
	if !ok || subtype != EXTPriorityUpdate {
		return preparedPriorityUpdate{}
	}
	for _, frame := range frames[1:] {
		if frame.Type != FrameTypeDATA || frame.StreamID != first.StreamID {
			return preparedPriorityUpdate{}
		}
	}
	return preparedPriorityUpdate{
		frame:      first,
		streamID:   first.StreamID,
		payload:    first.Payload,
		frameBytes: txFrameBufferedBytes(first),
	}
}

type openerVisibilityMark uint8

const (
	openerVisibilityUnchanged openerVisibilityMark = iota
	openerVisibilityPeerVisible
)

func (m openerVisibilityMark) marksPeerVisible() bool {
	return m == openerVisibilityPeerVisible
}

type terminalWritePolicy uint8

const (
	terminalWriteReject terminalWritePolicy = iota
	terminalWriteAllow
)

func (p terminalWritePolicy) allowsTerminal() bool {
	return p == terminalWriteAllow
}

type frameOwnership uint8

const (
	frameBorrowed frameOwnership = iota
	frameImmutable
	frameOwned
)

func (o frameOwnership) requiresClone() bool {
	return o == frameBorrowed
}

func (o frameOwnership) ownsFrames() bool {
	return !o.requiresClone()
}

type queuedWriteOptions struct {
	terminalPolicy   terminalWritePolicy
	deadlineOverride time.Time
	ownership        frameOwnership
	queuedBytes      uint64
	deadlinePolicy   writeDeadlinePolicy
	openerVisibility openerVisibilityMark
}

type queuedWriteCommit struct {
	progress         int
	openerVisibility openerVisibilityMark
	finalize         bool
}

func (c queuedWriteCommit) empty() bool {
	return c.progress <= 0 && !c.openerVisibility.marksPeerVisible() && !c.finalize
}

func (c queuedWriteCommit) burstFinalState() writeBurstFinalState {
	if c.finalize {
		return writeBurstFinalized
	}
	return writeBurstNotFinalized
}

type writeRequestOrigin uint8

const (
	writeRequestOriginProtocol writeRequestOrigin = iota
	writeRequestOriginStream
)

func (o writeRequestOrigin) isStreamGenerated() bool {
	return o == writeRequestOriginStream
}

type writeLane uint8

const (
	writeLaneOrdinary writeLane = iota
	writeLaneAdvisory
	writeLaneUrgent
)

func (l writeLane) isUrgent() bool {
	return l == writeLaneUrgent
}

func (l writeLane) isAdvisory() bool {
	return l == writeLaneAdvisory
}

type writeUrgencyProfile uint8

const (
	writeUrgencyMixed writeUrgencyProfile = iota
	writeUrgencyAllUrgent
)

func writeUrgencyProfileFrom(allUrgent bool) writeUrgencyProfile {
	if allUrgent {
		return writeUrgencyAllUrgent
	}
	return writeUrgencyMixed
}

func (p writeUrgencyProfile) allUrgent() bool {
	return p == writeUrgencyAllUrgent
}

func (l writeLane) promote(profile writeUrgencyProfile) writeLane {
	if l == writeLaneOrdinary && profile.allUrgent() {
		return writeLaneUrgent
	}
	return l
}

type writeDeadlinePolicy uint8

const (
	writeDeadlineUseStream writeDeadlinePolicy = iota
	writeDeadlineOverrideOnly
)

func (p writeDeadlinePolicy) usesStreamDeadline() bool {
	return p != writeDeadlineOverrideOnly
}

type preparedQueueDispatchOptions struct {
	lane      writeLane
	ownership frameOwnership
}

type frameLaneRequestOptions struct {
	lane           writeLane
	terminalPolicy terminalWritePolicy
	origin         writeRequestOrigin
	stream         *nativeStream
}

type streamWriteDispatchOptions struct {
	lane             writeLane
	deadlineOverride time.Time
	deadlinePolicy   writeDeadlinePolicy
}

func clearWriteRequestClassification(req *writeRequest) {
	if req == nil {
		return
	}
	req.requestMetaReady = false
	req.requestStreamID = 0
	req.requestStreamIDKnown = false
	req.requestStreamScoped = false
	req.requestUrgencyRank = 0
	req.requestCost = 0
	req.requestBufferedBytes = 0
	req.requestIsPriorityUpdate = false
	req.requestAllUrgent = false
	req.terminalDataPriority = false
	req.terminalResetOnly = false
	req.terminalAbortOnly = false
	req.terminalHasFIN = false
}

func clearPreparedWriteRequestState(req *writeRequest) {
	if req == nil {
		return
	}
	req.preparedSendBytes = 0
	req.preparedSendFin = false
	req.preparedOpenerVisibility = openerVisibilityUnchanged
	clearPreparedPriorityRollback(req)
	req.preparedPriorityQueued = false
}

func clearPreparedPriorityRollback(req *writeRequest) {
	if req == nil {
		return
	}
	req.setPreparedPriorityUpdate(preparedPriorityUpdate{})
}

func (req *writeRequest) preparedPriorityUpdate() preparedPriorityUpdate {
	if req == nil {
		return preparedPriorityUpdate{}
	}
	return preparedPriorityUpdate{
		streamID:   req.preparedPriorityStreamID,
		payload:    req.preparedPriorityPayload,
		frameBytes: req.preparedPriorityBytes,
	}
}

func (req *writeRequest) setPreparedPriorityUpdate(priority preparedPriorityUpdate) {
	if req == nil {
		return
	}
	req.preparedPriorityStreamID = priority.streamID
	req.preparedPriorityPayload = priority.payload
	req.preparedPriorityBytes = priority.frameBytes
}

func (req *writeRequest) clearRetainedRefs() {
	if req == nil {
		return
	}
	req.frames = nil
	req.releaseDoneChan()
	req.preparedNotify = nil
	req.queueReserved = false
	req.queuedBytes = 0
	req.reservedStream = nil
	req.urgentReserved = false
	req.advisoryReserved = false
	req.cloneFramesBeforeSend = false
	clearWriteRequestClassification(req)
	clearPreparedWriteRequestState(req)
}

func (req *writeRequest) notifyPrepared() {
	if req == nil || req.preparedNotify == nil {
		return
	}
	close(req.preparedNotify)
	req.preparedNotify = nil
}

func prepareWriteRequestForSend(req *writeRequest) {
	if req == nil {
		return
	}
	req.ensureDoneChan()
	req.frames, req.cloneFramesBeforeSend = cloneTxFramesIfNeeded(req.frames, req.cloneFramesBeforeSend)
}

func clearPreparedQueueRequests(reqs []writeRequest) {
	for i := range reqs {
		reqs[i].clearRetainedRefs()
	}
}

func initTerminalClassification(req *writeRequest) {
	if req == nil {
		return
	}
	req.terminalDataPriority = true
	req.terminalResetOnly = true
	req.terminalAbortOnly = true
	req.terminalHasFIN = false
}

func clearTerminalClassification(req *writeRequest) {
	if req == nil {
		return
	}
	req.terminalDataPriority = false
	req.terminalResetOnly = false
	req.terminalAbortOnly = false
}

func clearTerminalClassificationAndFIN(req *writeRequest) {
	if req == nil {
		return
	}
	clearTerminalClassification(req)
	req.terminalHasFIN = false
}

func setRequestStreamScope(req *writeRequest, streamID uint64) {
	if req == nil {
		return
	}
	req.requestStreamID = streamID
	req.requestStreamIDKnown = true
	req.requestStreamScoped = true
}

func clearRequestStreamScope(req *writeRequest) {
	if req == nil {
		return
	}
	req.requestStreamIDKnown = false
	req.requestStreamScoped = false
}

func ensureMinimumRequestCost(req *writeRequest) {
	if req == nil {
		return
	}
	if req.requestCost <= 0 {
		req.requestCost = 1
	}
}

func frameIsPriorityUpdate(frame txFrame) bool {
	if frame.Type != FrameTypeEXT {
		return false
	}
	subtype, _, ok := parseExtFrame(frame.Payload)
	return ok && subtype == EXTPriorityUpdate
}

type terminalFINScan uint8

const (
	terminalFINNotSeen terminalFINScan = iota
	terminalFINSeen
)

func (s terminalFINScan) seen() bool {
	return s == terminalFINSeen
}

func classifyTerminalFrame(req *writeRequest, frame txFrame, fin terminalFINScan) terminalFINScan {
	switch frame.Type {
	case FrameTypeDATA:
		if fin.seen() {
			req.terminalDataPriority = false
		}
		if frame.Flags&FrameFlagFIN != 0 {
			fin = terminalFINSeen
			req.terminalHasFIN = true
		}
		req.terminalResetOnly = false
		req.terminalAbortOnly = false
	case FrameTypeEXT:
		if fin.seen() || !frameIsPriorityUpdate(frame) {
			req.terminalDataPriority = false
		}
		req.terminalResetOnly = false
		req.terminalAbortOnly = false
	case FrameTypeRESET:
		req.terminalDataPriority = false
		req.terminalAbortOnly = false
	case FrameTypeABORT:
		req.terminalDataPriority = false
		req.terminalResetOnly = false
	default:
		clearTerminalClassification(req)
	}
	return fin
}

func classifyWriteRequest(req *writeRequest) {
	if req == nil || req.requestMetaReady {
		return
	}

	clearWriteRequestClassification(req)
	req.requestMetaReady = true
	if len(req.frames) == 1 {
		classifySingleFrameWriteRequest(req, req.frames[0])
		return
	}
	req.requestAllUrgent = len(req.frames) > 0
	req.requestUrgencyRank = rt.DefaultUrgencyRank
	initTerminalClassification(req)

	seen := false
	mixed := false
	fin := terminalFINNotSeen
	for _, frame := range req.frames {
		if !rt.IsUrgentType(frame.Type) {
			req.requestAllUrgent = false
		}
		if got := rt.UrgencyRank(frame.Type); got < req.requestUrgencyRank {
			req.requestUrgencyRank = got
		}
		bytes := txFrameBufferedBytes(frame)
		req.requestBufferedBytes = saturatingAdd(req.requestBufferedBytes, bytes)
		req.requestCost += int64(bytes)

		if !batchFrameIsStreamScoped(frame) {
			continue
		}
		if !seen {
			req.requestStreamID = frame.StreamID
			seen = true
		} else if frame.StreamID != req.requestStreamID {
			mixed = true
		}
		if mixed {
			clearTerminalClassificationAndFIN(req)
			continue
		}

		fin = classifyTerminalFrame(req, frame, fin)
	}

	ensureMinimumRequestCost(req)
	if seen && !mixed {
		setRequestStreamScope(req, req.requestStreamID)
	}
	if !seen {
		clearRequestStreamScope(req)
		clearTerminalClassificationAndFIN(req)
	}
}

func classifySingleFrameWriteRequest(req *writeRequest, frame txFrame) {
	if req == nil {
		return
	}
	clearWriteRequestClassification(req)
	req.requestMetaReady = true
	req.requestAllUrgent = rt.IsUrgentType(frame.Type)
	req.requestUrgencyRank = rt.UrgencyRank(frame.Type)
	req.requestBufferedBytes = txFrameBufferedBytes(frame)
	req.requestCost = int64(req.requestBufferedBytes)
	initTerminalClassification(req)
	req.requestIsPriorityUpdate = frameIsPriorityUpdate(frame)

	if !batchFrameIsStreamScoped(frame) {
		clearRequestStreamScope(req)
		clearTerminalClassificationAndFIN(req)
		ensureMinimumRequestCost(req)
		return
	}

	setRequestStreamScope(req, frame.StreamID)
	classifyTerminalFrame(req, frame, terminalFINNotSeen)
	ensureMinimumRequestCost(req)
}

func batchStreamID(req *writeRequest) (uint64, bool) {
	classifyWriteRequest(req)
	return req.requestStreamID, req.requestStreamIDKnown
}

func batchFrameIsStreamScoped(frame txFrame) bool {
	if frame.StreamID == 0 {
		return false
	}
	switch frame.Type {
	case FrameTypeDATA,
		FrameTypeMAXDATA,
		FrameTypeStopSending,
		FrameTypeBLOCKED,
		FrameTypeRESET,
		FrameTypeABORT,
		FrameTypeEXT:
		return true
	default:
		return false
	}
}

func (req *writeRequest) targetsStreamID(streamID uint64) bool {
	if req == nil {
		return false
	}
	classifyWriteRequest(req)
	if !req.requestStreamIDKnown {
		return false
	}
	return streamID == 0 || req.requestStreamID == streamID
}

func (req *writeRequest) allowsTerminalSendHalf(sendHalf state.SendHalfState, streamID uint64) bool {
	if req == nil || !req.targetsStreamID(streamID) {
		return false
	}
	switch sendHalf {
	case state.SendHalfFin, state.SendHalfStopSeen:
		if !req.terminalDataPriority {
			return false
		}
		return sendHalf != state.SendHalfFin || req.terminalHasFIN
	case state.SendHalfReset:
		return req.terminalResetOnly
	case state.SendHalfAborted:
		return req.terminalAbortOnly
	default:
		return false
	}
}

func (req *writeRequest) allowsQueuedGracefulFinDrainForStream(streamID uint64) bool {
	if req == nil || !req.queueReserved || !req.targetsStreamID(streamID) {
		return false
	}
	return req.terminalDataPriority && req.terminalHasFIN && !req.terminalResetOnly && !req.terminalAbortOnly
}

type preparedReleaseSnapshot struct {
	tracked       uint64
	sessionQueued uint64
	sessionCredit uint64
	streamQueued  uint64
	streamCredit  uint64
}

func (c *Conn) preparedRequestStreamLocked(req *writeRequest) *nativeStream {
	if c == nil || req == nil {
		return nil
	}
	if req.reservedStream != nil {
		return req.reservedStream
	}
	if req.requestStreamIDKnown {
		return c.registry.streams[req.requestStreamID]
	}
	streamID, ok := batchStreamID(req)
	if !ok {
		return nil
	}
	return c.registry.streams[streamID]
}

func (c *Conn) rollbackPreparedSendLocked(stream *nativeStream, req *writeRequest) {
	if c == nil || stream == nil || req == nil {
		return
	}

	if req.preparedSendBytes > 0 {
		stream.sendSent = csub(stream.sendSent, req.preparedSendBytes)
		c.flow.sendSessionUsed = csub(c.flow.sendSessionUsed, req.preparedSendBytes)
		req.preparedSendBytes = 0
	}
	if req.preparedSendFin {
		if stream.sendFinReachedLocked() {
			stream.clearSendFin()
		}
		req.preparedSendFin = false
	}
	if req.preparedOpenerVisibility.marksPeerVisible() {
		stream.clearOpeningBarrierLocked()
		req.preparedOpenerVisibility = openerVisibilityUnchanged
	}
}

func (c *Conn) capturePreparedReleaseSnapshotLocked(stream *nativeStream) preparedReleaseSnapshot {
	snapshot := preparedReleaseSnapshot{
		tracked:       c.trackedSessionMemoryLocked(),
		sessionQueued: c.flow.queuedDataBytes,
		sessionCredit: csub(c.flow.sendSessionMax, c.flow.sendSessionUsed),
	}
	if stream != nil {
		snapshot.streamQueued = stream.queuedDataBytes
		snapshot.streamCredit = csub(stream.sendMax, stream.sendSent)
	}
	return snapshot
}

func (c *Conn) releasePreparedWriteRequestLocked(req *writeRequest) (*nativeStream, rt.ReleaseWakePlan) {
	if c == nil || req == nil {
		return nil, rt.ReleaseWakePlan{}
	}
	stream := c.preparedRequestStreamLocked(req)
	prev := c.capturePreparedReleaseSnapshotLocked(stream)

	c.rollbackPreparedSendLocked(stream, req)
	if prev.sessionCredit == 0 && csub(c.flow.sendSessionMax, c.flow.sendSessionUsed) > 0 {
		c.clearSessionBlockedStateLocked()
		c.dropPendingSessionControlLocked(sessionControlBlocked)
	}
	if stream != nil && prev.streamCredit == 0 && csub(stream.sendMax, stream.sendSent) > 0 {
		stream.clearBlockedState()
		if stream.idSet {
			c.dropPendingStreamControlEntryLocked(streamControlBlocked, stream.id)
		}
	}
	c.releaseWriteQueueReservationLocked(req)
	c.restorePreparedPriorityUpdateLocked(req)
	urgentReleased := req.urgentReserved
	c.releaseUrgentQueueReservationLocked(req)
	c.releaseAdvisoryQueueReservationLocked(req)
	next := c.capturePreparedReleaseSnapshotLocked(stream)
	plan := rt.PlanPreparedReleaseWake(
		prev.tracked,
		next.tracked,
		c.sessionMemoryHighThresholdLocked(),
		prev.sessionQueued,
		next.sessionQueued,
		c.sessionDataLWMLocked(),
		prev.streamQueued,
		next.streamQueued,
		c.perStreamDataLWMLocked(),
		prev.sessionCredit,
		next.sessionCredit,
		prev.streamCredit,
		next.streamCredit,
		urgentReleased,
	)
	return stream, plan
}

func (c *Conn) releasePreparedWriteRequest(req *writeRequest) {
	if c == nil || req == nil {
		return
	}

	urgentReleased := req.urgentReserved && req.queuedBytes > 0
	c.mu.Lock()
	stream, plan := c.releasePreparedWriteRequestLocked(req)
	if plan.Broadcast {
		c.broadcastWriteWakeLocked()
	}
	if urgentReleased {
		c.broadcastUrgentWakeLocked()
	}
	c.mu.Unlock()

	if plan.Control {
		notify(c.pending.controlNotify)
	}
	if plan.StreamWake && stream != nil {
		notify(stream.writeNotify)
	}
}

func (c *Conn) releaseRejectedPreparedRequests(rejected []rejectedWriteRequest) {
	if c == nil || len(rejected) == 0 {
		return
	}

	var (
		streamWakes   []*nativeStream
		needBroadcast bool
		needControl   bool
		needUrgent    bool
	)
	c.mu.Lock()
	for i := range rejected {
		needUrgent = needUrgent || (rejected[i].req.urgentReserved && rejected[i].req.queuedBytes > 0)
		stream, plan := c.releasePreparedWriteRequestLocked(&rejected[i].req)
		needBroadcast = needBroadcast || plan.Broadcast
		needControl = needControl || plan.Control
		if plan.StreamWake && stream != nil {
			streamWakes = append(streamWakes, stream)
		}
	}
	if needBroadcast {
		c.broadcastWriteWakeLocked()
	}
	if needUrgent {
		c.broadcastUrgentWakeLocked()
	}
	c.mu.Unlock()

	if needControl {
		notify(c.pending.controlNotify)
	}
	for _, stream := range streamWakes {
		notify(stream.writeNotify)
	}
}

func (c *Conn) enqueuePreparedQueueRequest(req *writeRequest, opts preparedQueueDispatchOptions) error {
	if err := c.dispatchPreparedQueueRequest(req, opts); err != nil {
		return err
	}
	return c.waitPreparedQueueRequest(req)
}

func (c *Conn) dispatchPreparedQueueRequests(reqs []writeRequest, opts preparedQueueDispatchOptions) error {
	dispatched := 0
	for i := range reqs {
		if err := c.dispatchPreparedQueueRequest(&reqs[i], opts); err != nil {
			for j := 0; j < dispatched; j++ {
				_ = c.waitPreparedQueueRequest(&reqs[j])
			}
			clearPreparedQueueRequests(reqs[i:])
			return err
		}
		dispatched++
	}
	var firstErr error
	for i := range reqs {
		if err := c.waitPreparedQueueRequest(&reqs[i]); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (c *Conn) dispatchPreparedQueueRequest(req *writeRequest, opts preparedQueueDispatchOptions) (err error) {
	if c == nil || req == nil {
		return ErrSessionClosed
	}
	preparedForAdmission := false
	defer func() {
		if err != nil && preparedForAdmission {
			req.markDoneReusable()
			req.clearRetainedRefs()
		}
	}()
	select {
	case <-c.lifecycle.closedCh:
		return queueVisibleSessionErr(c, c.err())
	default:
	}

	queuedBytes := req.queuedBytes
	urgentReserved := false
	advisoryReserved := false
	if opts.lane.isUrgent() && c.pending.controlNotify != nil {
		for {
			var wakeCh <-chan struct{}
			c.mu.Lock()
			preflight := c.preflightUrgentWriteRequestLocked(req)
			if preflight.reservation.blocked() {
				wakeCh = c.currentUrgentWakeLocked()
			}
			c.mu.Unlock()
			if preflight.closeErr != nil {
				c.releaseUrgentQueueReservation(req)
				return queueVisibleSessionErr(c, preflight.closeErr)
			}
			if preflight.reservation.memoryErr != nil {
				c.releaseUrgentQueueReservation(req)
				c.closeSession(preflight.reservation.memoryErr)
				return queueVisibleSessionErr(c, c.err())
			}
			if !preflight.reservation.blocked() {
				urgentReserved = req.urgentReserved
				queuedBytes = req.queuedBytes
				break
			}
			select {
			case <-c.lifecycle.closedCh:
				return queueVisibleSessionErr(c, c.err())
			case <-wakeCh:
			}
		}
	}
	if opts.lane.isAdvisory() {
		c.mu.Lock()
		preflight := c.preflightAdvisoryWriteRequestLocked(req)
		c.mu.Unlock()
		if preflight.closeErr != nil {
			c.releaseAdvisoryQueueReservation(req)
			return queueVisibleSessionErr(c, preflight.closeErr)
		}
		if preflight.memoryErr != nil {
			c.releaseAdvisoryQueueReservation(req)
			c.closeSession(preflight.memoryErr)
			return queueVisibleSessionErr(c, c.err())
		}
		advisoryReserved = req.advisoryReserved
		queuedBytes = req.queuedBytes
	}

	req.cloneFramesBeforeSend = opts.ownership.requiresClone()
	req.queuedBytes = queuedBytes
	req.urgentReserved = urgentReserved
	req.advisoryReserved = advisoryReserved
	select {
	case <-c.lifecycle.closedCh:
		c.releaseUrgentQueueReservation(req)
		c.releaseAdvisoryQueueReservation(req)
		return queueVisibleSessionErr(c, c.err())
	default:
	}
	prepareWriteRequestForSend(req)
	preparedForAdmission = true
	req.notifyPrepared()
	select {
	case <-c.lifecycle.closedCh:
		c.releaseUrgentQueueReservation(req)
		c.releaseAdvisoryQueueReservation(req)
		return queueVisibleSessionErr(c, c.err())
	case c.writeLaneChan(opts.lane) <- *req:
	}
	return nil
}

func (c *Conn) waitPreparedQueueRequest(req *writeRequest) error {
	if c == nil || req == nil {
		return ErrSessionClosed
	}
	defer req.clearRetainedRefs()
	allowEarlyCloseErr := !req.carriesOnlyCloseFrames()
	notifyCh, closeErr := c.refreshPreparedQueueRequestCompletion()
	if allowEarlyCloseErr && closeErr != nil {
		if err, ok := req.tryTakeDoneErr(); ok && err != nil {
			return c.queueRequestDoneErr(err)
		}
		return queueVisibleSessionErr(c, closeErr)
	}
	select {
	case <-c.lifecycle.closedCh:
		if err, ok := req.tryTakeDoneErr(); ok && err != nil {
			return c.queueRequestDoneErr(err)
		}
		return queueVisibleSessionErr(c, c.err())
	default:
	}
	for {
		select {
		case <-c.lifecycle.closedCh:
			if err, ok := req.tryTakeDoneErr(); ok && err != nil {
				return c.queueRequestDoneErr(err)
			}
			return queueVisibleSessionErr(c, c.err())
		case err := <-req.done:
			req.markDoneReusable()
			return c.queueRequestDoneErr(err)
		case <-notifyCh:
			notifyCh, closeErr = c.refreshPreparedQueueRequestCompletion()
			if allowEarlyCloseErr && closeErr != nil {
				if err, ok := req.tryTakeDoneErr(); ok && err != nil {
					return c.queueRequestDoneErr(err)
				}
				return queueVisibleSessionErr(c, closeErr)
			}
		}
	}
}

func (c *Conn) refreshPreparedQueueRequestCompletion() (<-chan struct{}, error) {
	if c == nil {
		return nil, ErrSessionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	notifyCh := c.lifecycle.terminalCh
	if notifyCh == nil {
		notifyCh = c.lifecycle.closedCh
	}
	return notifyCh, c.lifecycle.closeErr
}

func (req *writeRequest) carriesOnlyCloseFrames() bool {
	if req == nil || len(req.frames) == 0 {
		return false
	}
	for _, frame := range req.frames {
		if frame.Type != FrameTypeCLOSE {
			return false
		}
	}
	return true
}

type queueReservationState uint8

const queueReservationBlocked queueReservationState = 1

type queueReservationResult struct {
	state     queueReservationState
	memoryErr error
}

func (r queueReservationResult) blocked() bool {
	return r.state == queueReservationBlocked
}

type urgentWritePreflight struct {
	closeErr    error
	reservation queueReservationResult
}

type advisoryWritePreflight struct {
	closeErr  error
	memoryErr error
}

type streamWritePreflight struct {
	notifyCh    <-chan struct{}
	writeWake   <-chan struct{}
	deadline    time.Time
	err         error
	reservation queueReservationResult
}

type queuedWriteCompletionRefresh struct {
	notifyCh <-chan struct{}
	deadline time.Time
	closeErr error
}

type queuedWriteRequestRefresh struct {
	deadline time.Time
	err      error
}

func (c *Conn) preflightUrgentWriteRequestLocked(req *writeRequest) urgentWritePreflight {
	if c == nil {
		return urgentWritePreflight{closeErr: ErrSessionClosed}
	}
	if c.lifecycle.closeErr != nil {
		return urgentWritePreflight{closeErr: c.lifecycle.closeErr}
	}
	return urgentWritePreflight{reservation: c.reserveUrgentQueueLocked(req)}
}

func (c *Conn) preflightAdvisoryWriteRequestLocked(req *writeRequest) advisoryWritePreflight {
	if c == nil {
		return advisoryWritePreflight{closeErr: ErrSessionClosed}
	}
	if c.lifecycle.closeErr != nil {
		return advisoryWritePreflight{closeErr: c.lifecycle.closeErr}
	}
	return advisoryWritePreflight{memoryErr: c.reserveAdvisoryQueueLocked(req)}
}

func (c *Conn) preflightStreamWriteRequestLocked(stream *nativeStream, req *writeRequest, opts streamWriteDispatchOptions) streamWritePreflight {
	preflight := streamWritePreflight{}
	refresh := c.refreshQueuedWriteRequestLocked(stream, req, opts)
	preflight.deadline = refresh.deadline
	preflight.err = refresh.err
	if refresh.err != nil {
		return preflight
	}
	if stream != nil {
		preflight.notifyCh = stream.ensureWriteNotifyLocked()
	}

	preflight.reservation = c.reserveWriteQueueLocked(stream, req, opts.lane)
	if preflight.reservation.blocked() {
		preflight.writeWake = c.currentWriteWakeLocked()
	}
	return preflight
}

func (c *Conn) queueRequestDoneErr(err error) error {
	if err == nil {
		return nil
	}
	if cerr := c.err(); cerr != nil {
		return queueVisibleSessionErr(c, cerr)
	}
	return queueVisibleSessionErr(c, err)
}

func (s *nativeStream) releasePreparedWriteRequestForFailure(req *writeRequest) {
	if s == nil || s.conn == nil || req == nil {
		return
	}
	s.conn.releasePreparedWriteRequest(req)
	req.markDoneReusable()
	req.clearRetainedRefs()
}

func (s *nativeStream) rollbackPreparedWriteRequest(req *writeRequest, err error) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	s.releasePreparedWriteRequestForFailure(req)
	return err
}

func (s *nativeStream) closePreparedWriteRequestForMemory(req *writeRequest, memoryErr error) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	s.releasePreparedWriteRequestForFailure(req)
	s.conn.closeSession(memoryErr)
	return queueVisibleSessionErr(s.conn, s.conn.err())
}

func (s *nativeStream) refreshQueuedWriteCompletion() (notifyCh <-chan struct{}, deadline time.Time, cerr error) {
	if s == nil || s.conn == nil {
		return nil, time.Time{}, ErrSessionClosed
	}
	s.conn.mu.Lock()
	refresh := s.conn.refreshQueuedWriteCompletionLocked(s)
	s.conn.mu.Unlock()
	notifyCh = refresh.notifyCh
	deadline = refresh.deadline
	cerr = refresh.closeErr
	return
}

func (s *nativeStream) enqueueWriteRequestUntilDeadline(req *writeRequest, opts streamWriteDispatchOptions) error {
	if s == nil || s.conn == nil || req == nil {
		return ErrSessionClosed
	}
	lane := s.conn.writeLaneChan(opts.lane)
	for {
		s.conn.mu.Lock()
		preflight := s.conn.preflightStreamWriteRequestLocked(s, req, opts)
		s.conn.mu.Unlock()
		if preflight.err != nil {
			return s.rollbackPreparedWriteRequest(req, preflight.err)
		}
		if preflight.reservation.memoryErr != nil {
			return s.closePreparedWriteRequestForMemory(req, preflight.reservation.memoryErr)
		}
		if preflight.reservation.blocked() {
			start := time.Now()
			err := s.waitWithDeadlineAndWake(preflight.notifyCh, preflight.writeWake, preflight.deadline, OperationWrite)
			s.conn.noteBlockedWrite(time.Since(start))
			if err != nil {
				return s.rollbackPreparedWriteRequest(req, err)
			}
			continue
		}
		return s.sendQueuedWriteRequestUntilDeadline(req, lane, preflight.notifyCh, preflight.deadline, opts)
	}
}

func (s *nativeStream) waitQueuedWriteCompletion(req *writeRequest) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	defer req.clearRetainedRefs()
	completion := queuedWriteCompletionRefresh{}
	notifyCh, deadline, cerr := s.refreshQueuedWriteCompletion()
	completion.notifyCh = notifyCh
	completion.deadline = deadline
	completion.closeErr = cerr
	if completion.closeErr != nil {
		if err, ok := req.tryTakeDoneErr(); ok && err != nil {
			return s.conn.queueRequestDoneErr(err)
		}
		return queueVisibleSessionErr(s.conn, completion.closeErr)
	}
	var timer *time.Timer
	defer stopTimer(timer)
	for {
		var timeout <-chan time.Time
		if !completion.deadline.IsZero() {
			delay := time.Until(completion.deadline)
			if delay <= 0 {
				if err, ok := req.tryTakeDoneErr(); ok && err != nil {
					return s.conn.queueRequestDoneErr(err)
				}
				return os.ErrDeadlineExceeded
			}
			timer = resetTimer(timer, delay)
			timeout = timer.C
		}

		select {
		case <-s.conn.lifecycle.closedCh:
			if err, ok := req.tryTakeDoneErr(); ok && err != nil {
				return s.conn.queueRequestDoneErr(err)
			}
			return queueVisibleSessionErr(s.conn, s.conn.err())
		case err := <-req.done:
			req.markDoneReusable()
			return s.conn.queueRequestDoneErr(err)
		case <-completion.notifyCh:
			completion.notifyCh, completion.deadline, completion.closeErr = s.refreshQueuedWriteCompletion()
			if completion.closeErr != nil {
				if err, ok := req.tryTakeDoneErr(); ok && err != nil {
					return s.conn.queueRequestDoneErr(err)
				}
				return queueVisibleSessionErr(s.conn, completion.closeErr)
			}
			continue
		case <-timeout:
			if err, ok := req.tryTakeDoneErr(); ok && err != nil {
				return s.conn.queueRequestDoneErr(err)
			}
			return os.ErrDeadlineExceeded
		}
	}
}

func (c *Conn) streamWriteDeadlineLocked(stream *nativeStream, opts streamWriteDispatchOptions) time.Time {
	if !opts.deadlinePolicy.usesStreamDeadline() {
		return opts.deadlineOverride
	}
	if stream != nil {
		deadline := time.Time{}
		if ws := stream.waitStateLocked(); ws != nil {
			deadline = ws.writeDeadline
		}
		return rt.EffectiveDeadline(deadline, opts.deadlineOverride)
	}
	return opts.deadlineOverride
}

func (c *Conn) refreshQueuedWriteCompletionLocked(stream *nativeStream) queuedWriteCompletionRefresh {
	refresh := queuedWriteCompletionRefresh{
		deadline: c.streamWriteDeadlineLocked(stream, streamWriteDispatchOptions{}),
	}
	if stream != nil {
		refresh.notifyCh = stream.ensureWriteNotifyLocked()
	}
	if c != nil {
		refresh.closeErr = c.lifecycle.closeErr
	}
	return refresh
}

func (c *Conn) refreshQueuedWriteRequestLocked(stream *nativeStream, req *writeRequest, opts streamWriteDispatchOptions) queuedWriteRequestRefresh {
	refresh := queuedWriteRequestRefresh{
		deadline: c.streamWriteDeadlineLocked(stream, opts),
	}
	if c.lifecycle.closeErr != nil {
		refresh.err = sessionOperationErrLocked(c, OperationWrite, visibleSessionErrLocked(c, c.lifecycle.closeErr))
		return refresh
	}
	refresh.err = c.suppressWriteRequestForStreamLocked(req, stream)
	return refresh
}

func (s *nativeStream) sendQueuedWriteRequestUntilDeadline(req *writeRequest, lane chan writeRequest, notifyCh <-chan struct{}, deadline time.Time, opts streamWriteDispatchOptions) error {
	if s == nil || s.conn == nil || req == nil {
		return ErrSessionClosed
	}
	prepareWriteRequestForSend(req)
	var timer *time.Timer
	defer stopTimer(timer)
	for {
		var timeout <-chan time.Time
		if !deadline.IsZero() {
			delay := time.Until(deadline)
			if delay <= 0 {
				return s.rollbackPreparedWriteRequest(req, os.ErrDeadlineExceeded)
			}
			timer = resetTimer(timer, delay)
			timeout = timer.C
		}

		select {
		case <-s.conn.lifecycle.closedCh:
			return s.rollbackPreparedWriteRequest(req, queueVisibleSessionErr(s.conn, s.conn.err()))
		case lane <- *req:
			return nil
		case <-notifyCh:
			s.conn.mu.Lock()
			refresh := s.conn.refreshQueuedWriteRequestLocked(s, req, opts)
			s.conn.mu.Unlock()
			if refresh.err != nil {
				return s.rollbackPreparedWriteRequest(req, refresh.err)
			}
			deadline = refresh.deadline
			continue
		case <-timeout:
			return s.rollbackPreparedWriteRequest(req, os.ErrDeadlineExceeded)
		}
	}
}

func queueVisibleSessionErr(c *Conn, err error) error {
	current := connStateClosed
	if c != nil {
		c.mu.Lock()
		current = c.lifecycle.sessionState
		c.mu.Unlock()
	}
	if state.IsBenignSessionError(current, err, ErrSessionClosed) {
		err = ErrSessionClosed
	}
	return sessionOperationErr(c, OperationWrite, state.VisibleSessionError(current, err, ErrSessionClosed))
}

func (c *Conn) writeLaneChan(lane writeLane) chan writeRequest {
	if lane.isUrgent() {
		return c.writer.urgentWriteCh
	}
	if lane.isAdvisory() && c.writer.advisoryWriteCh != nil {
		return c.writer.advisoryWriteCh
	}
	return c.writer.writeCh
}

func (c *Conn) queueTxFrame(frame txFrame, opts frameLaneRequestOptions, ownership frameOwnership) error {
	var frameBuf [1]txFrame
	frames := frameBuf[:1]
	frames[0] = frame
	req, err := c.buildTxLaneRequest(frames, opts)
	if err != nil {
		return err
	}
	return c.enqueuePreparedQueueRequest(&req, preparedQueueDispatchOptions{
		lane:      opts.lane,
		ownership: ownership,
	})
}

func (c *Conn) queueImmutableFrame(frame txFrame) error {
	opts := frameLaneRequestOptions{
		lane: writeLaneOrdinary,
	}
	if rt.IsUrgentType(frame.Type) {
		opts.lane = writeLaneUrgent
	}
	return c.queueTxFrame(frame, opts, frameImmutable)
}

func txFrameChunkSpans(frames []txFrame, maxFrames int, maxBytes uint64) []rt.ChunkSpan {
	if len(frames) == 0 {
		return nil
	}
	if maxFrames <= 0 {
		maxFrames = len(frames)
	}

	spans := make([]rt.ChunkSpan, 0, len(frames))
	start := 0
	chunkBytes := uint64(0)
	for i, frame := range frames {
		frameBytes := txFrameBufferedBytes(frame)
		overFrameCap := i-start >= maxFrames
		overByteCap := maxBytes > 0 && i > start && saturatingAdd(chunkBytes, frameBytes) > maxBytes
		if overFrameCap || overByteCap {
			spans = append(spans, rt.ChunkSpan{Start: start, End: i})
			start = i
			chunkBytes = 0
		}
		chunkBytes = saturatingAdd(chunkBytes, frameBytes)
	}
	spans = append(spans, rt.ChunkSpan{Start: start, End: len(frames)})
	return spans
}

func (c *Conn) queueTxFrameChunksToLane(frames []txFrame, opts frameLaneRequestOptions, ownership frameOwnership, maxBytes uint64) error {
	if len(frames) == 0 {
		return nil
	}
	opts.origin = writeRequestOriginProtocol
	localLimits := c.localLimitsView()
	peerLimits := c.peerLimitsView()
	spans := txFrameChunkSpans(frames, maxWriteBatchFrames, maxBytes)
	var reqBuf [4]writeRequest
	reqs := reqBuf[:0]
	if capHint := len(spans); capHint > cap(reqBuf) {
		reqs = make([]writeRequest, 0, capHint)
	}
	for _, span := range spans {
		req, err := c.buildTxLaneRequestWithLimits(frames[span.Start:span.End], opts, localLimits, peerLimits)
		if err != nil {
			return err
		}
		reqs = append(reqs, req)
	}
	return c.dispatchPreparedQueueRequests(reqs, preparedQueueDispatchOptions{
		lane:      opts.lane,
		ownership: ownership,
	})
}

func (c *Conn) buildTxLaneRequest(frames []txFrame, opts frameLaneRequestOptions) (writeRequest, error) {
	return c.buildTxLaneRequestWithLimits(frames, opts, c.localLimitsView(), c.peerLimitsView())
}

func (c *Conn) buildTxLaneRequestWithLimits(frames []txFrame, opts frameLaneRequestOptions, localLimits Limits, peerLimits Limits) (writeRequest, error) {
	if len(frames) == 0 {
		return writeRequest{}, nil
	}
	select {
	case <-c.lifecycle.closedCh:
		return writeRequest{}, queueVisibleSessionErr(c, c.err())
	default:
	}
	if err := validateOutboundTxFramesWithLimits(frames, localLimits, peerLimits); err != nil {
		return writeRequest{}, err
	}

	req := writeRequest{
		frames:         frames,
		origin:         opts.origin,
		terminalPolicy: opts.terminalPolicy,
	}
	if opts.origin.isStreamGenerated() {
		classifyWriteRequest(&req)
		req.queuedBytes = req.requestBufferedBytes
		if opts.stream != nil && req.requestStreamScoped && req.requestStreamIDKnown {
			if !opts.stream.idSet || opts.stream.id == req.requestStreamID {
				req.reservedStream = opts.stream
			}
		}
	} else {
		req.queuedBytes = txFramesBufferedBytes(req.frames)
	}
	return req, nil
}

func validateOutboundTxFramesWithLimits(frames []txFrame, localLimits, peerLimits Limits) error {
	if len(frames) == 0 {
		return nil
	}
	if len(frames) == 1 {
		return validateOutboundTxFrameWithLimits(frames[0], localLimits, peerLimits)
	}
	for _, frame := range frames {
		if err := validateOutboundTxFrameWithLimits(frame, localLimits, peerLimits); err != nil {
			return err
		}
	}
	return nil
}

func validateOutboundTxFrameWithLimits(frame txFrame, localLimits, peerLimits Limits) error {
	validation := Frame{
		Type:     frame.Type,
		Flags:    frame.Flags,
		StreamID: frame.StreamID,
		Payload:  frame.payloadForValidation(),
	}
	if err := validateFrame(validation, defaultNormalizedLimits, false); err != nil {
		return err
	}

	var (
		limit uint64
		op    string
	)
	switch frame.Type {
	case FrameTypeDATA:
		limit = peerLimits.MaxFramePayload
		op = "send DATA payload"
	case FrameTypeMAXDATA, FrameTypeBLOCKED, FrameTypePONG,
		FrameTypeABORT, FrameTypeGOAWAY, FrameTypeCLOSE:
		limit = peerLimits.MaxControlPayloadBytes
		op = "send control payload"
	case FrameTypePING:
		limit = min(localLimits.MaxControlPayloadBytes, peerLimits.MaxControlPayloadBytes)
		op = "send PING payload"
	case FrameTypeEXT:
		limit = peerLimits.MaxExtensionPayloadBytes
		op = "send EXT payload"
	default:
		return nil
	}
	if uint64(frame.payloadLength()) > limit {
		return frameSizeError(op, errPayloadTooLarge)
	}
	return nil
}

func (c *Conn) sessionDataLWMLocked() uint64 {
	return rt.LowWatermark(c.sessionDataHWMLocked())
}

func (c *Conn) perStreamDataLWMLocked() uint64 {
	return rt.LowWatermark(c.perStreamDataHWMLocked())
}

func requestBufferedBytes(req *writeRequest) uint64 {
	if req == nil {
		return 0
	}
	classifyWriteRequest(req)
	return req.requestBufferedBytes
}

func ensureRequestQueuedBytes(req *writeRequest) uint64 {
	if req == nil {
		return 0
	}
	if req.queuedBytes == 0 {
		req.queuedBytes = requestBufferedBytes(req)
	}
	return req.queuedBytes
}

func (c *Conn) writeQueueBlockedLocked(stream *nativeStream, reqBytes uint64, trackedAdditional uint64) bool {
	if c == nil || stream == nil || reqBytes == 0 {
		return false
	}
	return rt.QueueWouldBlock(
		c.sessionWriteMemoryBlockedLocked(trackedAdditional),
		c.flow.queuedDataBytes,
		stream.queuedDataBytes,
		reqBytes,
		c.sessionDataHWMLocked(),
		c.perStreamDataHWMLocked(),
	)
}

func (c *Conn) reserveWriteQueueLocked(stream *nativeStream, req *writeRequest, lane writeLane) queueReservationResult {
	if c == nil || stream == nil || req == nil || lane.isUrgent() || !req.origin.isStreamGenerated() || req.queueReserved {
		return queueReservationResult{}
	}
	c.ensurePreparedPriorityRollbackLocked(req)

	bytes := ensureRequestQueuedBytes(req)
	if bytes == 0 {
		return queueReservationResult{}
	}
	trackedAdditional := bytes
	trackedAdditional = csub(trackedAdditional, req.preparedPriorityBytes)
	if err := c.sessionMemoryCapErrorWithAdditionalLocked("queue stream write", trackedAdditional); err != nil {
		return queueReservationResult{memoryErr: err}
	}
	if c.writeQueueBlockedLocked(stream, bytes, trackedAdditional) {
		return queueReservationResult{state: queueReservationBlocked}
	}

	c.flow.queuedDataBytes = saturatingAdd(c.flow.queuedDataBytes, bytes)
	stream.queuedDataBytes = saturatingAdd(stream.queuedDataBytes, bytes)
	c.trackQueuedDataStreamLocked(stream)
	c.handoffPreparedPriorityToQueueLocked(req)
	req.queueReserved = true
	req.reservedStream = stream
	return queueReservationResult{}
}

func (c *Conn) urgentLaneCapLocked() uint64 {
	if c != nil && c.flow.urgentQueueCap > 0 {
		return c.flow.urgentQueueCap
	}
	localLimits := c.localLimitsView()
	peerLimits := c.peerLimitsView()
	payload := minNonZeroUint64(localLimits.MaxControlPayloadBytes, peerLimits.MaxControlPayloadBytes)
	return rt.RepoDefaultUrgentLaneCap(payload)
}

func (c *Conn) reserveUrgentQueueLocked(req *writeRequest) queueReservationResult {
	if c == nil || req == nil || req.urgentReserved {
		return queueReservationResult{}
	}

	bytes := ensureRequestQueuedBytes(req)
	if bytes == 0 {
		return queueReservationResult{}
	}
	if err := c.sessionMemoryCapErrorWithAdditionalLocked("queue urgent control", bytes); err != nil {
		return queueReservationResult{memoryErr: err}
	}
	if saturatingAdd(c.flow.urgentQueuedBytes, bytes) > c.urgentLaneCapLocked() {
		return queueReservationResult{state: queueReservationBlocked}
	}

	c.flow.urgentQueuedBytes = saturatingAdd(c.flow.urgentQueuedBytes, bytes)
	req.urgentReserved = true
	return queueReservationResult{}
}

func (c *Conn) reserveAdvisoryQueueLocked(req *writeRequest) error {
	if c == nil || req == nil || req.advisoryReserved {
		return nil
	}

	bytes := ensureRequestQueuedBytes(req)
	if bytes == 0 {
		return nil
	}
	if err := c.sessionMemoryCapErrorWithAdditionalLocked("queue advisory control", bytes); err != nil {
		return err
	}

	c.flow.advisoryQueuedBytes = saturatingAdd(c.flow.advisoryQueuedBytes, bytes)
	req.advisoryReserved = true
	return nil
}

func (c *Conn) releaseWriteQueueReservation(req *writeRequest) {
	if c == nil || req == nil || !req.queueReserved || req.queuedBytes == 0 {
		return
	}

	c.mu.Lock()
	prevTracked := c.trackedSessionMemoryLocked()
	prevSession := c.flow.queuedDataBytes
	stream := req.reservedStream
	prevStream := uint64(0)
	if stream != nil {
		prevStream = stream.queuedDataBytes
	}
	c.releaseWriteQueueReservationLocked(req)
	nextStream := uint64(0)
	if stream != nil {
		nextStream = stream.queuedDataBytes
	}
	plan := rt.PlanQueueReleaseWake(
		prevTracked,
		c.trackedSessionMemoryLocked(),
		c.sessionMemoryHighThresholdLocked(),
		prevSession,
		c.flow.queuedDataBytes,
		c.sessionDataLWMLocked(),
		prevStream,
		nextStream,
		c.perStreamDataLWMLocked(),
		false,
	)

	if plan.Broadcast {
		c.broadcastWriteWakeLocked()
		if plan.MemoryWake {
			notify(c.pending.controlNotify)
		}
	} else if plan.StreamWake && stream != nil {
		notify(stream.writeNotify)
	}
	c.mu.Unlock()
}

func (c *Conn) releaseAdvisoryQueueReservation(req *writeRequest) {
	if c == nil || req == nil || !req.advisoryReserved || req.queuedBytes == 0 {
		return
	}

	c.mu.Lock()
	prevTracked := c.trackedSessionMemoryLocked()
	c.releaseAdvisoryQueueReservationLocked(req)
	plan := rt.PlanLaneReleaseWake(prevTracked, c.trackedSessionMemoryLocked(), c.sessionMemoryHighThresholdLocked(), false)
	if plan.Broadcast {
		c.broadcastWriteWakeLocked()
	}
	c.mu.Unlock()

	if plan.Control {
		notify(c.pending.controlNotify)
	}
}

func (c *Conn) releaseUrgentQueueReservation(req *writeRequest) {
	if c == nil || req == nil || !req.urgentReserved || req.queuedBytes == 0 {
		return
	}

	c.mu.Lock()
	prevTracked := c.trackedSessionMemoryLocked()
	c.releaseUrgentQueueReservationLocked(req)
	plan := rt.PlanLaneReleaseWake(prevTracked, c.trackedSessionMemoryLocked(), c.sessionMemoryHighThresholdLocked(), true)
	if plan.Broadcast {
		c.broadcastWriteWakeLocked()
	}
	c.broadcastUrgentWakeLocked()
	if plan.Control {
		notify(c.pending.controlNotify)
	}
	c.mu.Unlock()
}

func (c *Conn) releaseWriteQueueReservationLocked(req *writeRequest) {
	if c == nil || req == nil || !req.queueReserved || req.queuedBytes == 0 {
		return
	}

	c.flow.queuedDataBytes = csub(c.flow.queuedDataBytes, req.queuedBytes)

	stream := req.reservedStream
	if stream != nil {
		stream.queuedDataBytes = csub(stream.queuedDataBytes, req.queuedBytes)
		if stream.queuedDataBytes == 0 {
			c.untrackQueuedDataStreamLocked(stream)
		}
	}

	req.queueReserved = false
	req.reservedStream = nil
	if !req.urgentReserved && !req.advisoryReserved {
		req.queuedBytes = 0
	}
	if stream != nil && stream.queuedDataBytes == 0 && stream.inflightQueued == 0 {
		c.maybeCompactTerminalLocked(stream)
	}
}

func (c *Conn) releaseUrgentQueueReservationLocked(req *writeRequest) {
	if c == nil || req == nil || !req.urgentReserved || req.queuedBytes == 0 {
		return
	}

	c.flow.urgentQueuedBytes = csub(c.flow.urgentQueuedBytes, req.queuedBytes)
	req.urgentReserved = false
	if !req.queueReserved && !req.advisoryReserved {
		req.queuedBytes = 0
		req.reservedStream = nil
	}
}

func (c *Conn) releaseAdvisoryQueueReservationLocked(req *writeRequest) {
	if c == nil || req == nil || !req.advisoryReserved || req.queuedBytes == 0 {
		return
	}

	c.flow.advisoryQueuedBytes = csub(c.flow.advisoryQueuedBytes, req.queuedBytes)
	req.advisoryReserved = false
	if !req.queueReserved && !req.urgentReserved {
		req.queuedBytes = 0
		req.reservedStream = nil
	}
}

func (c *Conn) handoffPreparedPriorityToQueueLocked(req *writeRequest) {
	if c == nil || req == nil || req.preparedPriorityQueued {
		return
	}
	c.ensurePreparedPriorityRollbackLocked(req)
	priority := req.preparedPriorityUpdate()
	if !priority.hasFrame() {
		return
	}
	c.releasePreparedPriorityBytesLocked(priority.frameBytes)
	req.preparedPriorityQueued = true
}

func (c *Conn) releaseBatchReservations(batch []writeRequest) {
	if c == nil || len(batch) == 0 {
		return
	}

	c.mu.Lock()
	prevTracked := c.trackedSessionMemoryLocked()
	prevSession := c.flow.queuedDataBytes
	var (
		streamPrev     = c.writer.scratch.streamValueAccumulator(len(batch))
		urgentReleased bool
	)

	for i := range batch {
		req := &batch[i]
		if req.queueReserved && req.queuedBytes > 0 && req.reservedStream != nil {
			streamPrev.RememberFirst(req.reservedStream, req.reservedStream.queuedDataBytes)
			req.reservedStream.inflightQueued = csub(req.reservedStream.inflightQueued, req.queuedBytes)
		}
		c.releaseWriteQueueReservationLocked(req)
		if req.urgentReserved {
			urgentReleased = true
		}
		c.releaseUrgentQueueReservationLocked(req)
		c.releaseAdvisoryQueueReservationLocked(req)
	}

	plan := rt.PlanLaneReleaseWake(prevTracked, c.trackedSessionMemoryLocked(), c.sessionMemoryHighThresholdLocked(), urgentReleased)
	sessionWake := rt.CrossedLowWatermark(prevSession, c.flow.queuedDataBytes, c.sessionDataLWMLocked())
	plan.Broadcast = plan.Broadcast || sessionWake
	var streamWakes []*nativeStream
	if !sessionWake {
		lowWatermark := c.perStreamDataLWMLocked()
		streamPrev.Range(func(stream *nativeStream, prev uint64) {
			if rt.CrossedLowWatermark(prev, stream.queuedDataBytes, lowWatermark) {
				streamWakes = append(streamWakes, stream)
			}
		})
	}
	if plan.Broadcast {
		c.broadcastWriteWakeLocked()
	}
	if urgentReleased {
		c.broadcastUrgentWakeLocked()
	}
	c.mu.Unlock()

	if plan.Control {
		notify(c.pending.controlNotify)
	}
	for _, stream := range streamWakes {
		if stream != nil {
			notify(stream.writeNotify)
		}
	}
}

func (c *Conn) currentWriteWakeLocked() <-chan struct{} {
	if c == nil {
		return nil
	}
	if c.signals.writeWakeCh == nil {
		c.signals.writeWakeCh = make(chan struct{})
	}
	return c.signals.writeWakeCh
}

func (c *Conn) currentUrgentWakeLocked() <-chan struct{} {
	if c == nil {
		return nil
	}
	if c.signals.urgentWakeCh == nil {
		c.signals.urgentWakeCh = make(chan struct{})
	}
	return c.signals.urgentWakeCh
}

func (c *Conn) broadcastWriteWakeLocked() {
	if c == nil || c.signals.writeWakeCh == nil {
		return
	}
	close(c.signals.writeWakeCh)
	c.signals.writeWakeCh = make(chan struct{})
}

func (c *Conn) broadcastUrgentWakeLocked() {
	if c == nil || c.signals.urgentWakeCh == nil {
		return
	}
	close(c.signals.urgentWakeCh)
	c.signals.urgentWakeCh = make(chan struct{})
}

func (c *Conn) clearWriteQueueReservationsLocked() {
	c.forEachKnownStreamLocked(func(stream *nativeStream) {
		c.clearQueuedDataStreamStateLocked(stream)
	})
	c.flow.queuedDataBytes = 0
	c.flow.advisoryQueuedBytes = 0
	c.flow.urgentQueuedBytes = 0
	c.pending.preparedPriorityBytes = 0
	c.flow.queuedDataStreams = nil
}

func drainDetachedWriteLane(ch chan writeRequest, err error) bool {
	if ch == nil {
		return false
	}
	drained := false
	for {
		select {
		case req := <-ch:
			drained = true
			completeWriteRequest(&req, err)
			req.clearRetainedRefs()
		default:
			return drained
		}
	}
}

func drainDetachedWriteLanes(err error, lanes ...chan writeRequest) {
	if len(lanes) == 0 {
		return
	}
	deadline := time.Now().Add(postCloseQueueDrainWindow)
	idlePasses := 0
	for {
		progressed := false
		for _, lane := range lanes {
			if drainDetachedWriteLane(lane, err) {
				progressed = true
			}
		}
		if progressed {
			idlePasses = 0
			continue
		}
		idlePasses++
		if idlePasses >= 2 || time.Now().After(deadline) {
			return
		}
		runtime.Gosched()
	}
}

func (c *Conn) trackQueuedDataStreamLocked(stream *nativeStream) {
	if c == nil || stream == nil || stream.queuedDataBytes == 0 {
		return
	}
	if c.flow.queuedDataStreams == nil {
		c.flow.queuedDataStreams = make(map[*nativeStream]struct{})
	}
	c.flow.queuedDataStreams[stream] = struct{}{}
}

func (c *Conn) untrackQueuedDataStreamLocked(stream *nativeStream) {
	if c == nil || stream == nil || c.flow.queuedDataStreams == nil {
		return
	}
	delete(c.flow.queuedDataStreams, stream)
	if len(c.flow.queuedDataStreams) == 0 {
		c.flow.queuedDataStreams = nil
	}
}

type queuedDataWakePolicy uint8

const queuedDataWakeOnRelease queuedDataWakePolicy = 1

func (p queuedDataWakePolicy) shouldWake() bool {
	return p == queuedDataWakeOnRelease
}

func (c *Conn) releaseQueuedDataStreamStateLocked(stream *nativeStream, wake queuedDataWakePolicy) {
	if c == nil || stream == nil {
		return
	}

	prevTracked := c.trackedSessionMemoryLocked()
	prevSessionQueued := c.flow.queuedDataBytes
	releasedQueued := stream.queuedDataBytes
	if releasedQueued > 0 {
		c.flow.queuedDataBytes = csub(c.flow.queuedDataBytes, releasedQueued)
	}
	c.clearQueuedDataStreamStateLocked(stream)

	if !wake.shouldWake() || releasedQueued == 0 {
		return
	}

	memoryWake := c.sessionMemoryWakeNeededLocked(prevTracked)
	sessionWake := rt.CrossedLowWatermark(prevSessionQueued, c.flow.queuedDataBytes, c.sessionDataLWMLocked())
	if sessionWake || memoryWake {
		c.broadcastWriteWakeLocked()
	}
	if memoryWake {
		notify(c.pending.controlNotify)
	}
}

func (c *Conn) clearQueuedDataStreamStateLocked(stream *nativeStream) {
	if c == nil || stream == nil {
		return
	}
	stream.queuedDataBytes = 0
	stream.inflightQueued = 0
	c.untrackQueuedDataStreamLocked(stream)
}
