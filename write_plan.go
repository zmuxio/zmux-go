package zmux

import (
	"errors"
	"os"
	"time"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
)

type writePrepareWindow struct {
	openerVisibility openerVisibilityMark
	prefixLen        uint64
	availableSession uint64
	availableStream  uint64
	frameCap         uint64
}

func totalPartLen(parts [][]byte) (int, bool) {
	return totalPartLenWithin(parts, int(^uint(0)>>1))
}

func totalPartLenWithin(parts [][]byte, limit int) (int, bool) {
	if limit < 0 {
		return 0, false
	}
	total := 0
	for _, part := range parts {
		if len(part) > limit-total {
			return 0, false
		}
		total += len(part)
	}
	return total, true
}

func advanceParts(parts [][]byte, idx, off, n int) (int, int) {
	for n > 0 && idx < len(parts) {
		part := parts[idx]
		remain := len(part) - off
		if remain <= 0 {
			idx++
			off = 0
			continue
		}
		if n < remain {
			return idx, off + n
		}
		n -= remain
		idx++
		off = 0
	}
	for idx < len(parts) && off >= len(parts[idx]) {
		idx++
		off = 0
	}
	return idx, off
}

func writeDeadlinePolicyAfterErr(err error) writeDeadlinePolicy {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return writeDeadlineOverrideOnly
	}
	return writeDeadlineUseStream
}

type localOpenerPrepareStatus uint8

const localOpenerRetry localOpenerPrepareStatus = 1

type localOpenerPrepareResult struct {
	visibility openerVisibilityMark
	status     localOpenerPrepareStatus
	wait       provisionalOpenTurnWait
}

func (r localOpenerPrepareResult) shouldRetry() bool {
	return r.status == localOpenerRetry
}

func (s *nativeStream) prepareRetriableLocalOpenerLocked() (localOpenerPrepareResult, error) {
	if s == nil || s.conn == nil {
		return localOpenerPrepareResult{}, ErrSessionClosed
	}
	result := localOpenerPrepareResult{}
	if s.needsLocalOpenerLocked() {
		openState, err := s.conn.prepareLocalOpeningLocked(s)
		if err != nil {
			return localOpenerPrepareResult{}, err
		}
		if openState.awaitingTurn() {
			wait, blocked, waitErr := s.provisionalOpenTurnWaitLocked()
			if waitErr != nil {
				return localOpenerPrepareResult{}, waitErr
			}
			if blocked {
				result.status = localOpenerRetry
				result.wait = wait
			}
			return result, nil
		}
		if openState.opened() {
			result.visibility = openerVisibilityPeerVisible
		}
	}
	if s.shouldEmitOpenerFrameLocked() {
		result.visibility = openerVisibilityPeerVisible
	}
	return result, nil
}

func (s *nativeStream) openerPrefixLenLocked(visibility openerVisibilityMark) (uint64, error) {
	if s == nil || s.conn == nil || !visibility.marksPeerVisible() {
		return 0, nil
	}
	prefixLen := uint64(len(s.openMetadataPrefix))
	if prefixLen > s.conn.config.peer.Settings.MaxFramePayload {
		return 0, ErrOpenMetadataTooLarge
	}
	return prefixLen, nil
}

func (s *nativeStream) validateOpenedFramesLocked(frames []txFrame, visibility openerVisibilityMark) error {
	if s == nil || s.conn == nil || !visibility.marksPeerVisible() {
		return nil
	}
	if _, err := s.openerPrefixLenLocked(visibility); err != nil {
		return err
	}
	return validateOutboundTxFramesWithLimits(frames, s.conn.localLimitsView(), s.conn.peerLimitsView())
}

func (s *nativeStream) markSendCommittedAndMaybeBarrierLocked(visibility openerVisibilityMark) {
	if s == nil {
		return
	}
	s.markSendCommittedLocked()
	if visibility.marksPeerVisible() {
		s.markOpenerQueuedLocked()
	}
}

func (s *nativeStream) currentWritePrepareWindowLocked(visibility openerVisibilityMark, prefixLen uint64) writePrepareWindow {
	if s == nil || s.conn == nil {
		return writePrepareWindow{}
	}
	return writePrepareWindow{
		openerVisibility: visibility,
		prefixLen:        prefixLen,
		availableSession: csub(s.conn.flow.sendSessionMax, s.conn.flow.sendSessionUsed),
		availableStream:  csub(s.sendMax, s.sendSent),
		frameCap:         s.txFragmentCapLocked(prefixLen),
	}
}

func (s *nativeStream) validateWriteAdmissionLocked(policy writeAdmissionPolicy) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	if !s.localSend {
		s.conn.mu.Unlock()
		return s.writeSurfaceErrLocked(ErrStreamNotWritable)
	}
	if s.conn.lifecycle.closeErr != nil {
		err := sessionOperationErrLocked(s.conn, OperationWrite, visibleSessionErrLocked(s.conn, s.conn.lifecycle.closeErr))
		s.conn.mu.Unlock()
		return err
	}
	if _, err := s.writeAdmissionStateLocked(policy); err != nil {
		s.conn.mu.Unlock()
		return err
	}
	return nil
}

type writePrepareWindowMode uint8

const (
	writePrepareWindowStep writePrepareWindowMode = iota
	writePrepareWindowBurst
)

type writePrepareOutcome uint8

const (
	writePrepareReady writePrepareOutcome = iota
	writePrepareRetry
	writePrepareBurstFallback
)

type writePrepareAttempt struct {
	window  writePrepareWindow
	outcome writePrepareOutcome
}

// acquireWritePrepareWindowLocked returns with conn.mu still held on success.
// Any retry/fallback/error path returns with the mutex already released.
func (s *nativeStream) acquireWritePrepareWindowLocked(mode writePrepareWindowMode, policy writeAdmissionPolicy) (writePrepareAttempt, error) {
	if s == nil || s.conn == nil {
		return writePrepareAttempt{}, ErrSessionClosed
	}

	s.conn.mu.Lock()
	phase := s.visibilityPhaseLocked()
	if mode == writePrepareWindowBurst && (phase.NeedsLocalOpener() || phase.ShouldEmitOpenerFrame()) {
		s.conn.mu.Unlock()
		return writePrepareAttempt{outcome: writePrepareBurstFallback}, nil
	}
	if err := s.validateWriteAdmissionLocked(policy); err != nil {
		return writePrepareAttempt{}, err
	}

	visibility := openerVisibilityUnchanged
	if mode == writePrepareWindowStep {
		openerResult, err := s.prepareRetriableLocalOpenerLocked()
		if err != nil {
			s.conn.mu.Unlock()
			return writePrepareAttempt{}, s.writeSurfaceErrLocked(err)
		}
		if openerResult.shouldRetry() {
			wait := openerResult.wait
			s.conn.mu.Unlock()
			if err := wait.wait(s, s.writeSurfaceErrLocked); err != nil {
				return writePrepareAttempt{}, err
			}
			return writePrepareAttempt{outcome: writePrepareRetry}, nil
		}
		visibility = openerResult.visibility
	}

	prefixLen, err := s.openerPrefixLenLocked(visibility)
	if err != nil {
		s.conn.mu.Unlock()
		return writePrepareAttempt{}, s.writeSurfaceErrLocked(err)
	}

	return writePrepareAttempt{
		window:  s.currentWritePrepareWindowLocked(visibility, prefixLen),
		outcome: writePrepareReady,
	}, nil
}

func boundedWriteChunk(remaining, availableSession, availableStream, frameCap uint64) uint64 {
	chunk := remaining
	if chunk > frameCap {
		chunk = frameCap
	}
	if chunk > availableSession {
		chunk = availableSession
	}
	if chunk > availableStream {
		chunk = availableStream
	}
	return chunk
}

func writePrepareBlocked(window writePrepareWindow) bool {
	return window.availableSession == 0 || window.availableStream == 0 || window.frameCap == 0
}

type writeChunkMode uint8

const (
	writeChunkStreaming writeChunkMode = iota
	writeChunkFinal
)

func (m writeChunkMode) isFinal() bool {
	return m == writeChunkFinal
}

func (m writeChunkMode) admissionPolicy() writeAdmissionPolicy {
	if m.isFinal() {
		return writeAdmissionTerminalChunk
	}
	return writeAdmissionStrict
}

type writeFinReservation uint8

const (
	writeFinDefer writeFinReservation = iota
	writeFinReserve
)

func (r writeFinReservation) reservesFIN() bool {
	return r == writeFinReserve
}

type writeBurstFinalState uint8

const (
	writeBurstNotFinalized writeBurstFinalState = iota
	writeBurstFinalized
)

func (s writeBurstFinalState) finalized() bool {
	return s == writeBurstFinalized
}

type preparedWriteStepBuild struct {
	step  writeStep
	ready bool
}

func (r preparedWriteStepBuild) hasStep() bool {
	return r.ready
}

func (s *nativeStream) buildPreparedWriteStepLocked(
	parts [][]byte,
	idx, off, totalRemaining int,
	mode writeChunkMode,
	window writePrepareWindow,
) preparedWriteStepBuild {
	if s == nil || s.conn == nil {
		return preparedWriteStepBuild{}
	}
	if window.openerVisibility.marksPeerVisible() && mode.isFinal() && totalRemaining == 0 {
		frame := s.dataFrameLocked(nil, dataFrameTraitFIN|dataFrameTraitOpenMetadata)
		return preparedWriteStepBuild{
			step:  s.finishPreparedWriteStepLocked(frame, 0, 0, window.openerVisibility, writeFinDefer),
			ready: true,
		}
	}
	if window.openerVisibility.marksPeerVisible() && writePrepareBlocked(window) {
		frame := s.dataFrameLocked(nil, dataFrameTraitOpenMetadata)
		return preparedWriteStepBuild{
			step:  s.finishPreparedWriteStepLocked(frame, 0, 0, window.openerVisibility, writeFinDefer),
			ready: true,
		}
	}

	chunk := boundedWriteChunk(uint64(totalRemaining), window.availableSession, window.availableStream, window.frameCap)
	if chunk == 0 {
		return preparedWriteStepBuild{}
	}
	finalized := mode.isFinal() && int(chunk) == totalRemaining
	traits := dataFrameTraitNone
	if finalized {
		traits |= dataFrameTraitFIN
	}
	if window.openerVisibility.marksPeerVisible() {
		traits |= dataFrameTraitOpenMetadata
	}
	frame := s.dataFrameFromPartsLocked(parts, idx, off, int(chunk), traits)
	finReservation := writeFinDefer
	if finalized {
		finReservation = writeFinReserve
	}
	return preparedWriteStepBuild{
		step:  s.finishPreparedWriteStepLocked(frame, int(chunk), chunk, window.openerVisibility, finReservation),
		ready: true,
	}
}

func (s *nativeStream) prepareWritePartsLocked(parts [][]byte, idx, off, totalRemaining int, mode writeChunkMode) (writeStep, error) {
	if s == nil || s.conn == nil {
		return writeStep{}, ErrSessionClosed
	}

	for {
		attempt, err := s.acquireWritePrepareWindowLocked(writePrepareWindowStep, mode.admissionPolicy())
		if err != nil {
			return writeStep{}, err
		}
		if attempt.outcome == writePrepareRetry {
			continue
		}
		window := attempt.window

		remainingLen := uint64(totalRemaining)
		if mode.isFinal() && s.stopSeenWriteFinalNeedsImmediateCompletionLocked(remainingLen, window.frameCap, window.availableSession, window.availableStream) {
			s.conn.mu.Unlock()
			return writeStep{}, s.resetStopSeenWriteFinal()
		}

		step := s.buildPreparedWriteStepLocked(parts, idx, off, totalRemaining, mode, window)
		if step.hasStep() {
			return step.step, nil
		}

		if err := s.waitForWriteCreditLocked(window.availableSession, window.availableStream); err != nil {
			return writeStep{}, err
		}
	}
}

func (s *nativeStream) reserveWriteChunkLocked(chunk uint64) {
	if s == nil || s.conn == nil || chunk == 0 {
		return
	}
	s.markSendCommittedLocked()
	s.sendSent = saturatingAdd(s.sendSent, chunk)
	s.conn.flow.sendSessionUsed = saturatingAdd(s.conn.flow.sendSessionUsed, chunk)
	s.clearBlockedState()
	s.conn.clearSessionBlockedStateLocked()
}

func (s *nativeStream) finishPreparedWriteStepLocked(frame txFrame, appN int, chunk uint64, visibility openerVisibilityMark, finReservation writeFinReservation) writeStep {
	if s != nil {
		if chunk > 0 {
			s.reserveWriteChunkLocked(chunk)
		}
		s.markSendCommittedAndMaybeBarrierLocked(visibility)
		if finReservation.reservesFIN() {
			s.setSendFin()
		}
		if s.conn != nil {
			s.conn.mu.Unlock()
		}
	}
	return writeStep{frame: frame, appN: appN, openerVisibility: visibility}
}

func (s *nativeStream) waitForWriteCreditLocked(availableSession, availableStream uint64) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}

	var writeWake <-chan struct{}
	if availableSession == 0 {
		s.conn.queuePendingSessionControlAsync(sessionControlBlocked, s.conn.flow.sendSessionMax)
		writeWake = s.conn.currentWriteWakeLocked()
	}
	if s.shouldQueueStreamBlockedLocked(availableStream) {
		s.conn.queueStreamBlockedAsync(s, s.sendMax)
	}
	notifyCh, deadline := s.writeWaitSnapshotLocked()
	s.conn.mu.Unlock()
	return s.waitWithDeadlineAndWakeTracked(notifyCh, writeWake, deadline, OperationWrite)
}

func (s *nativeStream) prepareOwnedWriteRequestFallback(req *writeRequest, localLimits Limits, peerLimits Limits) error {
	if err := validateOutboundTxFramesWithLimits(req.frames, localLimits, peerLimits); err != nil {
		return err
	}
	classifyWriteRequest(req)
	s.annotatePreparedOwnedRollback(req)
	return nil
}

func (s *nativeStream) prepareOwnedWriteRequest(req *writeRequest) error {
	if req != nil {
		clearWriteRequestClassification(req)
		clearPreparedWriteRequestState(req)
	}
	if s == nil || s.conn == nil || req == nil || len(req.frames) == 0 {
		return nil
	}

	localLimits := s.conn.localLimitsView()
	peerLimits := s.conn.peerLimitsView()
	prepared := s.prepareOwnedDataWriteRequest(req, peerLimits.MaxFramePayload)
	if prepared.handled {
		return prepared.err
	}
	return s.prepareOwnedWriteRequestFallback(req, localLimits, peerLimits)
}

type ownedDataWritePreparation struct {
	handled bool
	err     error
}

func (s *nativeStream) prepareOwnedDataWriteRequest(req *writeRequest, maxPayload uint64) ownedDataWritePreparation {
	if s == nil || req == nil || len(req.frames) == 0 {
		return ownedDataWritePreparation{handled: true}
	}

	bufferedBytes := uint64(0)
	requestCost := int64(0)
	preparedSendBytes := uint64(0)
	terminalDataPriority := true
	hasFIN := false
	for _, frame := range req.frames {
		if frame.Type != FrameTypeDATA ||
			frame.StreamID == 0 ||
			frame.StreamID != s.id ||
			frame.Flags&FrameFlagOpenMetadata != 0 {
			return ownedDataWritePreparation{}
		}
		switch frame.Flags {
		case 0, FrameFlagFIN:
		default:
			return ownedDataWritePreparation{}
		}
		if uint64(frame.payloadLength()) > maxPayload {
			return ownedDataWritePreparation{handled: true, err: frameSizeError("send DATA payload", errPayloadTooLarge)}
		}

		bytes := txFrameBufferedBytes(frame)
		bufferedBytes = saturatingAdd(bufferedBytes, bytes)
		requestCost = addRequestCost(requestCost, bytes)
		preparedSendBytes = saturatingAdd(preparedSendBytes, uint64(frame.payloadLength()))
		if hasFIN {
			terminalDataPriority = false
		}
		if frame.Flags&FrameFlagFIN != 0 {
			hasFIN = true
		}
	}

	req.requestMetaReady = true
	req.requestStreamID = s.id
	req.requestStreamIDKnown = true
	req.requestStreamScoped = true
	req.requestUrgencyRank = rt.DefaultUrgencyRank
	req.requestBufferedBytes = bufferedBytes
	req.requestCost = requestCost
	req.requestIsPriorityUpdate = false
	req.requestAllUrgent = false
	req.terminalDataPriority = terminalDataPriority
	req.terminalResetOnly = false
	req.terminalAbortOnly = false
	req.terminalHasFIN = hasFIN
	req.preparedSendBytes = preparedSendBytes
	req.preparedSendFin = hasFIN
	if req.requestCost <= 0 {
		req.requestCost = 1
	}
	return ownedDataWritePreparation{handled: true}
}

func (s *nativeStream) annotatePreparedOwnedRollback(req *writeRequest) {
	if s == nil || req == nil || len(req.frames) == 0 {
		return
	}

	prefixLen := len(s.openMetadataPrefix)
	req.preparedSendBytes = 0
	req.preparedSendFin = false
	clearPreparedPriorityRollback(req)
	req.preparedPriorityQueued = false

	if priority := preparedPriorityUpdateFromFrames(req.frames); priority.hasFrame() && priority.streamID == s.id {
		req.setPreparedPriorityUpdate(priority)
	}

	for _, frame := range req.frames {
		if frame.Type != FrameTypeDATA || frame.StreamID != s.id {
			continue
		}

		payloadLen := frame.payloadLength()
		if frame.Flags&FrameFlagOpenMetadata != 0 && prefixLen > 0 {
			if prefixLen >= payloadLen {
				payloadLen = 0
			} else {
				payloadLen -= prefixLen
			}
		}
		req.preparedSendBytes = saturatingAdd(req.preparedSendBytes, uint64(payloadLen))
		if frame.Flags&FrameFlagFIN != 0 {
			req.preparedSendFin = true
		}
	}
}

func (s *nativeStream) commitQueuedWrite(commit queuedWriteCommit) {
	if s == nil || s.conn == nil || commit.empty() {
		return
	}

	var now time.Time
	if commit.progress > 0 {
		now = time.Now()
	}

	var dispatch streamEventDispatch
	s.conn.mu.Lock()
	if commit.openerVisibility.marksPeerVisible() {
		s.conn.markPeerVisibleLocked(s)
		dispatch = s.conn.takeStreamEventLocked(s, EventStreamOpened, nil)
	}
	if commit.progress > 0 {
		s.conn.noteStreamProgressLocked(now)
		s.conn.noteAppProgressLocked(now)
	}
	if commit.finalize {
		s.conn.maybeFinalizePeerActiveLocked(s)
	}
	s.conn.mu.Unlock()

	if dispatch.shouldEmit() {
		s.conn.emitEvent(dispatch.event)
	}
}

func (s *nativeStream) queueFramesUntilDeadlineAndOptionsOwned(frames []txFrame, opts queuedWriteOptions) error {
	if s == nil || s.conn == nil || len(frames) == 0 {
		return nil
	}
	if !opts.ownership.ownsFrames() {
		if err := validateOutboundTxFramesWithLimits(frames, s.conn.localLimitsView(), s.conn.peerLimitsView()); err != nil {
			return err
		}
	}

	req := writeRequest{
		frames:         frames,
		origin:         writeRequestOriginStream,
		terminalPolicy: opts.terminalPolicy,
		queuedBytes:    opts.queuedBytes,
	}
	if opts.ownership.ownsFrames() {
		if err := s.prepareOwnedWriteRequest(&req); err != nil {
			return err
		}
	} else {
		classifyWriteRequest(&req)
	}
	if req.requestStreamScoped && req.requestStreamIDKnown && (!s.idSet || req.requestStreamID == s.id) {
		req.reservedStream = s
	}
	req.preparedOpenerVisibility = opts.openerVisibility
	if req.queuedBytes == 0 {
		req.queuedBytes = req.requestBufferedBytes
	}
	lane := writeLaneOrdinary.promote(writeUrgencyProfileFrom(req.requestAllUrgent))
	req.cloneFramesBeforeSend = !opts.ownership.ownsFrames()

	// deadlineOverride only bounds local admission into the writer path. Once the
	// request is queued to the single writer, completion waits follow the normal
	// stream write deadline surface so internal convergence helpers do not race a
	// request that is already in flight with a second fallback terminal action.
	if err := s.enqueueWriteRequestUntilDeadline(&req, streamWriteDispatchOptions{
		lane:             lane,
		deadlineOverride: opts.deadlineOverride,
		deadlinePolicy:   opts.deadlinePolicy,
	}); err != nil {
		return err
	}
	return s.waitQueuedWriteCompletion(&req)
}

const (
	defaultWriteBurstFrames   = rt.DefaultWriteBurstFrames
	mildWriteBurstFrames      = rt.MildWriteBurstFrames
	strongWriteBurstFrames    = rt.StrongWriteBurstFrames
	saturatedWriteBurstFrames = rt.SaturatedWriteBurstFrames
)

type writeBatchStart struct {
	burstLimit   int
	priority     preparedPriorityUpdate
	queueByteCap uint64
}

func (s *nativeStream) beginWriteBatchStartLocked() writeBatchStart {
	start := writeBatchStart{burstLimit: defaultWriteBurstFrames}
	if s == nil || s.conn == nil {
		return start
	}
	start.burstLimit = s.writeBurstLimitLocked()
	start.queueByteCap = s.writeRequestQueueCapLocked()
	if s.idSet {
		start.priority = s.conn.takePendingPriorityUpdateFrameLocked(s.id)
	}
	return start
}

func (s *nativeStream) writeBurstLimitLocked() int {
	return rt.WriteBurstLimit(s.priority, s.conn.config.peer.Settings.SchedulerHints)
}

func (s *nativeStream) writeRequestQueueCapLocked() uint64 {
	if s == nil || s.conn == nil {
		return 0
	}
	limit := s.conn.perStreamDataHWMLocked()
	if sessionCap := s.conn.sessionDataHWMLocked(); limit == 0 || (sessionCap > 0 && sessionCap < limit) {
		limit = sessionCap
	}
	return limit
}

func (start writeBatchStart) allowsNextQueuedFrame(currentQueued uint64, nextFrameBytes uint64) bool {
	if start.queueByteCap == 0 || currentQueued == 0 {
		return true
	}
	return saturatingAdd(currentQueued, nextFrameBytes) <= start.queueByteCap
}

func (s *nativeStream) txFragmentCapLocked(prefixLen uint64) uint64 {
	baseCap := rt.FragmentCap(s.conn.config.peer.Settings.MaxFramePayload, prefixLen, s.priority, s.conn.config.peer.Settings.SchedulerHints)
	return rt.RateLimitedFragmentCap(baseCap, s.conn.metrics.sendRateEstimate, s.priority, s.conn.config.peer.Settings.SchedulerHints)
}

func scaledFragmentCap(max uint64, num uint64, den uint64) uint64 {
	return rt.ScaledFragmentCap(max, num, den)
}

type writeBurstState struct {
	frames      []txFrame
	queuedBytes uint64
	commit      queuedWriteCommit
	dataFrames  int
}

func (state *writeBurstState) initFrameBuffer(start writeBatchStart, frames []txFrame) {
	state.frames = frames[:0]
	if start.priority.hasFrame() {
		state.frames = start.priority.append(state.frames)
		state.queuedBytes = start.priority.frameBytes
	}
}

func (state *writeBurstState) appendPrepared(frames []txFrame, queuedBytes uint64, progress int, finalState writeBurstFinalState) {
	if len(frames) > 0 {
		state.frames = append(state.frames, frames...)
	}
	state.queuedBytes = saturatingAdd(state.queuedBytes, queuedBytes)
	state.commit.progress = progress
	state.commit.finalize = finalState.finalized()
}

func (state *writeBurstState) appendStep(step writeStep) {
	state.frames = append(state.frames, step.frame)
	state.queuedBytes = saturatingAdd(state.queuedBytes, txFrameBufferedBytes(step.frame))
	state.commit.progress += step.appN
	state.dataFrames++
	if step.openerVisibility.marksPeerVisible() {
		state.commit.openerVisibility = step.openerVisibility
	}
	if step.frame.Flags&FrameFlagFIN != 0 {
		state.commit.finalize = true
	}
}

func (state *writeBurstState) hasFrames() bool {
	if state == nil {
		return false
	}
	return len(state.frames) > 0
}

func (s *nativeStream) commitBurstPeerVisible(state writeBurstState) {
	if state.commit.openerVisibility.marksPeerVisible() {
		s.commitQueuedWrite(queuedWriteCommit{openerVisibility: state.commit.openerVisibility})
	}
}

func (s *nativeStream) commitBurstProgress(state writeBurstState) {
	if state.commit.progress > 0 {
		s.commitQueuedWrite(queuedWriteCommit{progress: state.commit.progress})
	}
}

func (s *nativeStream) commitBurstSuccess(state writeBurstState) {
	s.commitQueuedWrite(state.commit)
}

type writeBurstBatchPreparation struct {
	start       writeBatchStart
	frames      []txFrame
	queuedBytes uint64
	progress    int
	finalState  writeBurstFinalState
	err         error
	handled     bool
}

func (s *nativeStream) prepareWritePartsBurstBatch(parts [][]byte, idx, off, totalRemaining int, mode writeChunkMode) writeBurstBatchPreparation {
	prepared := writeBurstBatchPreparation{
		start:      writeBatchStart{burstLimit: defaultWriteBurstFrames},
		finalState: writeBurstNotFinalized,
	}
	if s == nil || s.conn == nil || totalRemaining <= 0 {
		return prepared
	}

	if mode.isFinal() {
		if totalRemaining == 0 {
			return prepared
		}
	}

	var frameBuf [defaultWriteBurstFrames]txFrame
	startReady := false
	for {
		attempt, err := s.acquireWritePrepareWindowLocked(writePrepareWindowBurst, mode.admissionPolicy())
		if attempt.outcome == writePrepareBurstFallback {
			return prepared
		}
		if err != nil {
			prepared.err = err
			prepared.handled = true
			return prepared
		}
		window := attempt.window
		if !startReady {
			prepared.start = s.beginWriteBatchStartLocked()
			startReady = true
		}
		if mode.isFinal() && s.stopSeenLocked() && !s.stopSeenWriteFinalBurstEligibleLocked(totalRemaining) {
			s.conn.mu.Unlock()
			return prepared
		}
		if prepared.frames == nil {
			prepared.frames = frameBuf[:0]
		}
		requestQueuedBytes := prepared.start.priority.frameBytes
		queueCapHit := false

		frameIdx, frameOff := advanceParts(parts, idx, off, prepared.progress)
		availableSession := window.availableSession
		availableStream := window.availableStream
		frameCap := window.frameCap
		remainingLen := uint64(totalRemaining - prepared.progress)
		if mode.isFinal() && s.stopSeenWriteFinalNeedsImmediateCompletionLocked(remainingLen, frameCap, availableSession, availableStream) {
			s.conn.mu.Unlock()
			prepared.err = s.resetStopSeenWriteFinal()
			prepared.handled = true
			return prepared
		}

		for len(prepared.frames) < prepared.start.burstLimit && prepared.progress < totalRemaining && availableSession > 0 && availableStream > 0 && frameCap > 0 {
			chunk := boundedWriteChunk(uint64(totalRemaining-prepared.progress), availableSession, availableStream, frameCap)
			if chunk == 0 {
				break
			}
			frameFinalized := mode.isFinal() && prepared.progress+int(chunk) == totalRemaining
			traits := dataFrameTraitNone
			if frameFinalized {
				traits |= dataFrameTraitFIN
			}
			frame := s.dataFrameFromPartsLocked(parts, frameIdx, frameOff, int(chunk), traits)
			frameBytes := txFrameBufferedBytes(frame)
			if !prepared.start.allowsNextQueuedFrame(requestQueuedBytes, frameBytes) {
				queueCapHit = true
				break
			}
			prepared.frames = append(prepared.frames, frame)
			prepared.queuedBytes = saturatingAdd(prepared.queuedBytes, frameBytes)
			requestQueuedBytes = saturatingAdd(requestQueuedBytes, frameBytes)
			prepared.progress += int(chunk)
			frameIdx, frameOff = advanceParts(parts, frameIdx, frameOff, int(chunk))
			s.reserveWriteChunkLocked(chunk)
			if frameFinalized {
				s.setSendFin()
				prepared.finalState = writeBurstFinalized
			}
			availableSession -= chunk
			availableStream -= chunk
		}

		if queueCapHit || len(prepared.frames) >= prepared.start.burstLimit || prepared.progress >= totalRemaining {
			s.conn.mu.Unlock()
			prepared.handled = true
			return prepared
		}
		if prepared.progress > 0 {
			// Flush partial progress promptly instead of stalling it behind a later credit wait.
			s.conn.mu.Unlock()
			prepared.handled = true
			return prepared
		}

		if err := s.waitForWriteCreditLocked(availableSession, availableStream); err != nil {
			prepared.err = err
			prepared.handled = true
			return prepared
		}
	}
}

func (s *nativeStream) resetStopSeenWriteFinal() error {
	if err := s.CancelWrite(uint64(CodeCancelled)); err != nil {
		return err
	}
	s.conn.mu.Lock()
	err := s.writeSurfaceErrLocked(s.terminalErrLocked())
	s.conn.mu.Unlock()
	return err
}

type writeBurstResult struct {
	progress   int
	finalState writeBurstFinalState
	stop       bool
	err        error
}

type writeBurstFlushMode uint8

const (
	writeBurstFlushPrepared writeBurstFlushMode = iota
	writeBurstFlushAccumulatedErr
	writeBurstFlushReady
)

func (s *nativeStream) queueBurstFramesWithAdmission(state writeBurstState, mode writeChunkMode, deadlinePolicy writeDeadlinePolicy) error {
	opts := queuedWriteOptions{
		terminalPolicy:   terminalWriteReject,
		ownership:        frameOwned,
		queuedBytes:      state.queuedBytes,
		deadlinePolicy:   deadlinePolicy,
		openerVisibility: state.commit.openerVisibility,
	}
	if mode.isFinal() && state.commit.finalize {
		opts.terminalPolicy = terminalWriteAllow
	}
	if mode.isFinal() {
		err := s.queueFramesUntilDeadlineAndOptionsOwned(state.frames, opts)
		if err != nil {
			if state.commit.finalize {
				s.conn.mu.Lock()
				s.clearSendFin()
				s.conn.mu.Unlock()
			}
			return err
		}
		return nil
	}
	return s.queueFramesUntilDeadlineAndOptionsOwned(state.frames, opts)
}

func (s *nativeStream) finishBurstQueueErr(state writeBurstState, err error) writeBurstResult {
	if state.commit.progress > 0 {
		s.commitBurstProgress(state)
		return writeBurstResult{progress: state.commit.progress, err: err}
	}
	return writeBurstResult{err: err}
}

func (s *nativeStream) flushWriteBurst(state writeBurstState, chunkMode writeChunkMode, mode writeBurstFlushMode, burstErr error) writeBurstResult {
	if burstErr != nil {
		if !state.hasFrames() {
			return writeBurstResult{err: burstErr}
		}
		queueErr := s.queueBurstFramesWithAdmission(state, chunkMode, writeDeadlinePolicyAfterErr(burstErr))
		if queueErr != nil {
			return s.finishBurstQueueErr(state, queueErr)
		}
		if mode == writeBurstFlushAccumulatedErr {
			s.commitBurstPeerVisible(state)
		}
		if state.commit.progress > 0 {
			s.commitBurstProgress(state)
			return writeBurstResult{progress: state.commit.progress, err: burstErr}
		}
		return writeBurstResult{err: burstErr}
	}
	if !state.hasFrames() {
		return writeBurstResult{stop: true}
	}
	if err := s.queueBurstFramesWithAdmission(state, chunkMode, writeDeadlineUseStream); err != nil {
		return s.finishBurstQueueErr(state, err)
	}
	switch mode {
	case writeBurstFlushPrepared:
		if chunkMode.isFinal() {
			s.commitQueuedWrite(queuedWriteCommit{
				progress: state.commit.progress,
				finalize: state.commit.finalize,
			})
			return writeBurstResult{
				progress:   state.commit.progress,
				finalState: state.commit.burstFinalState(),
			}
		}
		s.commitBurstProgress(state)
		return writeBurstResult{progress: state.commit.progress}
	case writeBurstFlushReady:
		s.commitBurstSuccess(state)
		return writeBurstResult{
			progress:   state.commit.progress,
			finalState: state.commit.burstFinalState(),
		}
	default:
		s.commitBurstSuccess(state)
		return writeBurstResult{
			progress:   state.commit.progress,
			finalState: state.commit.burstFinalState(),
		}
	}
}

func (s *nativeStream) executeWriteBurst(parts [][]byte, idx, off, totalRemaining int, mode writeChunkMode) writeBurstResult {
	prepared := s.prepareWritePartsBurstBatch(parts, idx, off, totalRemaining, mode)
	var (
		state    writeBurstState
		frameBuf [defaultWriteBurstFrames + 1]txFrame
	)
	state.initFrameBuffer(prepared.start, frameBuf[:0])

	if prepared.handled {
		state.appendPrepared(prepared.frames, prepared.queuedBytes, prepared.progress, prepared.finalState)
		return s.flushWriteBurst(state, mode, writeBurstFlushPrepared, prepared.err)
	}

	batchIdx, batchOff := idx, off
	for state.dataFrames < prepared.start.burstLimit && state.commit.progress < totalRemaining {
		step, err := s.prepareWritePartsLocked(parts, batchIdx, batchOff, totalRemaining-state.commit.progress, mode)
		if err != nil {
			return s.flushWriteBurst(state, mode, writeBurstFlushAccumulatedErr, err)
		}

		frameBytes := txFrameBufferedBytes(step.frame)
		if !prepared.start.allowsNextQueuedFrame(state.queuedBytes, frameBytes) {
			break
		}
		state.appendStep(step)
		if step.appN > 0 {
			batchIdx, batchOff = advanceParts(parts, batchIdx, batchOff, step.appN)
		}
		// Flush the opener frame before attempting later chunks so flow-control
		// BLOCKED signaling cannot overtake the first peer-visible DATA frame.
		if step.openerVisibility.marksPeerVisible() {
			break
		}
		if mode.isFinal() && state.commit.finalize {
			break
		}
	}

	return s.flushWriteBurst(state, mode, writeBurstFlushReady, nil)
}

func (s *nativeStream) writeBurst(remaining []byte) (progress int, stop bool, err error) {
	var parts [1][]byte
	parts[0] = remaining
	result := s.executeWriteBurst(parts[:], 0, 0, len(remaining), writeChunkStreaming)
	return result.progress, result.stop, result.err
}

func (s *nativeStream) writeFinalBurst(parts [][]byte, idx, off, totalRemaining int) (progress int, finalState writeBurstFinalState, stop bool, err error) {
	result := s.executeWriteBurst(parts, idx, off, totalRemaining, writeChunkFinal)
	return result.progress, result.finalState, result.stop, result.err
}
