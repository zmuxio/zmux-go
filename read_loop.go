package zmux

import (
	"errors"
	"fmt"
	"os"
	"time"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// Ingress ownership lives here:
// - frame dispatch and read loop
// - session-level GOAWAY/CLOSE/EXT handling
// - stream MAX_DATA/BLOCKED plus receive accounting/replenish
// - stream STOP_SENDING/RESET/ABORT handling
// - inbound abuse/flood guard budgets

func (c *Conn) handleFrame(frame Frame) error {
	_, err := c.handleFrameBuffered(frame, nil, nil)
	return err
}

func (c *Conn) handleFrameBuffered(frame Frame, backing []byte, handle *wire.FrameReadBufferHandle) (bool, error) {
	c.mu.Lock()
	if frame.Type == FrameTypeCLOSE {
		if state.PlanPeerClose(c.lifecycle.closeErr, c.sessionControl.peerCloseErr != nil, ErrSessionClosed).Ignore {
			c.mu.Unlock()
			return false, nil
		}
	} else if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Unlock()

	now := time.Now()
	switch frame.Type {
	case FrameTypeEXT:
		c.mu.Lock()
		err := c.recordInboundExtBudgetLocked(now, uint64(len(frame.Payload)), "handle EXT")
		if err == nil {
			err = c.recordInboundMixedBudgetLocked(now, uint64(len(frame.Payload)), "handle frame")
		}
		c.mu.Unlock()
		if err != nil {
			return false, err
		}
	case FrameTypeDATA:
	default:
		c.mu.Lock()
		err := c.recordInboundControlBudgetLocked(now, uint64(len(frame.Payload)), "handle frame")
		if err == nil {
			err = c.recordInboundMixedBudgetLocked(now, uint64(len(frame.Payload)), "handle frame")
		}
		c.mu.Unlock()
		if err != nil {
			return false, err
		}
	}

	switch frame.Type {
	case FrameTypeDATA:
		return c.handleDataFrameBuffered(frame, backing, handle)
	case FrameTypeMAXDATA:
		return false, c.handleMaxDataFrame(frame)
	case FrameTypeStopSending:
		return false, c.handleStopSendingFrame(frame)
	case FrameTypeRESET:
		return false, c.handleResetFrame(frame)
	case FrameTypeABORT:
		return false, c.handleAbortFrame(frame)
	case FrameTypePING:
		c.mu.Lock()
		if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
			c.mu.Unlock()
			return false, nil
		}
		err := c.recordInboundPingFloodLocked(now)
		c.mu.Unlock()
		if err != nil {
			return false, err
		}
		replyPayload := clonePayloadBytes(frame.Payload)
		c.queueReadLoopFrameAsync(flatTxFrame(Frame{Type: FrameTypePONG, Payload: replyPayload}))
		return false, nil
	case FrameTypePONG:
		return false, c.handlePongFrame(frame)
	case FrameTypeBLOCKED:
		return false, c.handleBlockedFrame(frame)
	case FrameTypeGOAWAY:
		return false, c.handleGoAwayFrame(frame)
	case FrameTypeCLOSE:
		return false, c.handleCloseFrame(frame)
	case FrameTypeEXT:
		return false, c.handleExtFrame(frame)
	default:
		return false, wireError(CodeProtocol, "handle frame", errInvalidFrameType)
	}
}

func (c *Conn) readLoop() {
	limits := c.localLimitsView()
	var scratch []byte
	for {
		// Reuse one bounded frame buffer per active connection to avoid
		// per-frame pool churn without pinning oversized backings indefinitely.
		frame, buf, handle, err := readFrameBuffered(c.io.reader, limits, scratch)
		scratch = nil
		if err != nil {
			scratch = retainReadFrameBufferForPayloadLimit(buf, limits.MaxFramePayload)
			if scratch == nil && handle != nil {
				releaseReadFrameBuffer(buf, handle)
			}
			c.closeSession(err)
			return
		}
		c.noteInboundFrame(time.Now())
		retained, err := c.handleFrameBuffered(frame, buf, handle)
		if !retained {
			scratch = retainReadFrameBufferForPayloadLimit(buf, limits.MaxFramePayload)
			if scratch == nil && handle != nil {
				releaseReadFrameBuffer(buf, handle)
			}
		}
		if err != nil {
			c.closeSession(err)
			return
		}
	}
}

func (c *Conn) runReadLoopProtocolActionAsync(action func() error) {
	if c == nil || action == nil {
		return
	}
	go func() {
		if err := action(); err != nil {
			c.mu.Lock()
			current := c.lifecycle.sessionState
			c.mu.Unlock()
			if state.IsBenignSessionError(current, err, ErrSessionClosed) {
				return
			}
			c.closeSession(err)
		}
	}()
}

func (c *Conn) queueReadLoopFrameAsync(frame txFrame) {
	c.runReadLoopProtocolActionAsync(func() error {
		return c.queueImmutableFrame(frame)
	})
}

func (c *Conn) abortWithCodeAsync(streamID uint64, code ErrorCode) error {
	payload, err := buildCodePayload(uint64(code), "", 0)
	if err != nil {
		return wireError(CodeInternal, "queue ABORT", err)
	}
	c.queueReadLoopFrameAsync(flatTxFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: streamID,
		Payload:  payload,
	}))
	return nil
}

func (c *Conn) abortStreamStateAsync(streamID uint64) error {
	return c.abortWithCodeAsync(streamID, CodeStreamState)
}

func (c *Conn) validatePeerGoAwayPayloadLocked(payload goAwayPayload) error {
	if err := validateGoAwayWatermarkForDirection(payload.LastAcceptedBidi, true); err != nil {
		return err
	}
	if err := validateGoAwayWatermarkForDirection(payload.LastAcceptedUni, false); err != nil {
		return err
	}
	if payload.LastAcceptedBidi > c.sessionControl.peerGoAwayBidi || payload.LastAcceptedUni > c.sessionControl.peerGoAwayUni {
		return fmt.Errorf("GOAWAY watermarks must be non-increasing")
	}
	return nil
}

func (c *Conn) storePeerGoAwayLocked(payload goAwayPayload) {
	reason, reasonBytes := c.retainPeerReasonLocked(c.sessionControl.peerGoAwayReasonBytes, payload.Reason)
	c.sessionControl.peerGoAwayReasonBytes = reasonBytes
	c.sessionControl.peerGoAwayErr = applicationErr(payload.Code, reason)
}

func (c *Conn) applyPeerGoAwayChangeLocked(payload goAwayPayload, nextState connState) {
	if payload.LastAcceptedBidi < c.sessionControl.peerGoAwayBidi {
		c.sessionControl.peerGoAwayBidi = payload.LastAcceptedBidi
	}
	if payload.LastAcceptedUni < c.sessionControl.peerGoAwayUni {
		c.sessionControl.peerGoAwayUni = payload.LastAcceptedUni
	}
	c.clearNoOpControlBudgetsLocked()
	c.reclaimUnseenLocalStreamsLocked()
	c.lifecycle.sessionState = nextState
}

func (c *Conn) handleGoAwayFrame(frame Frame) error {
	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	c.mu.Unlock()

	payload, err := parseGOAWAYPayload(frame.Payload)
	if err != nil {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle GOAWAY", err)
	}
	c.mu.Lock()
	plan := state.PlanPeerGoAway(
		c.lifecycle.sessionState,
		c.lifecycle.closeErr != nil,
		c.sessionControl.peerGoAwayBidi,
		c.sessionControl.peerGoAwayUni,
		payload.LastAcceptedBidi,
		payload.LastAcceptedUni,
	)
	if plan.Ignore {
		c.mu.Unlock()
		return nil
	}
	if err := c.validatePeerGoAwayPayloadLocked(payload); err != nil {
		c.mu.Unlock()
		return wireError(CodeProtocol, "handle GOAWAY", err)
	}
	c.storePeerGoAwayLocked(payload)
	if plan.Changed {
		c.applyPeerGoAwayChangeLocked(payload, plan.NextState)
	} else if err := c.recordNoOpControlLocked(time.Now(), "handle GOAWAY"); err != nil {
		c.mu.Unlock()
		return err
	}
	notify(c.pending.controlNotify)
	c.mu.Unlock()
	return nil
}

func (c *Conn) handleCloseFrame(frame Frame) error {
	c.mu.Lock()
	if state.PlanPeerClose(c.lifecycle.closeErr, c.sessionControl.peerCloseErr != nil, ErrSessionClosed).Ignore {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		c.mu.Lock()
		ignore := state.PlanPeerClose(c.lifecycle.closeErr, c.sessionControl.peerCloseErr != nil, ErrSessionClosed).Ignore
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle CLOSE", err)
	}
	c.mu.Lock()
	if state.PlanPeerClose(c.lifecycle.closeErr, c.sessionControl.peerCloseErr != nil, ErrSessionClosed).Ignore {
		c.mu.Unlock()
		return nil
	}
	reason, c.sessionControl.peerCloseReasonBytes = c.retainPeerReasonLocked(c.sessionControl.peerCloseReasonBytes, reason)
	closeErr := applicationErr(code, reason)
	c.sessionControl.peerCloseErr = closeErr
	c.sessionControl.peerCloseView.Store(cloneApplicationError(closeErr))
	c.mu.Unlock()

	c.closeSession(closeErr)
	notify(c.pending.controlNotify)
	return closeErr
}

func (c *Conn) handleExtFrame(frame Frame) error {
	c.mu.Lock()
	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	extType, n, err := ParseVarint(frame.Payload)
	if err != nil {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle EXT", err)
	}
	c.mu.Lock()
	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	switch EXTSubtype(extType) {
	case EXTPriorityUpdate:
		return c.handlePriorityUpdateFrame(frame.StreamID, frame.Payload[n:])
	default:
		return nil
	}
}

// PeerCloseError returns the last CLOSE payload from the peer, if any.
//
// It does not alter session state. The returned value is a copy so callers can
// inspect code and reason safely without mutating connection internals.
func (c *Conn) PeerCloseError() *ApplicationError {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionControl.peerCloseErr == nil {
		return nil
	}
	return cloneApplicationError(c.sessionControl.peerCloseErr)
}

type zeroDataObservation uint8

const (
	zeroDataProgress zeroDataObservation = iota
	zeroDataNoOp
)

func (o zeroDataObservation) isNoOp() bool {
	return o == zeroDataNoOp
}

func (c *Conn) updateNoOpZeroDataLocked(observation zeroDataObservation, now time.Time) error {
	if !observation.isNoOp() {
		c.clearNoOpZeroDataLocked()
		return nil
	}
	return c.recordNoOpZeroDataLocked(now)
}

type lateDataTracking uint8

const (
	lateDataTrackingIgnore lateDataTracking = iota
	lateDataTrackingPerStream
)

func lateDataTrackingFrom(trackPerStream bool) lateDataTracking {
	if trackPerStream {
		return lateDataTrackingPerStream
	}
	return lateDataTrackingIgnore
}

type lateDiscardPlan struct {
	stream *nativeStream
	cause  lateDataCause
}

func ignoredPeerDataLateDiscard(stream *nativeStream, tracking lateDataTracking) lateDiscardPlan {
	if stream == nil || tracking != lateDataTrackingPerStream {
		return lateDiscardPlan{}
	}
	return lateDiscardPlan{stream: stream, cause: stream.lateDataCauseLocked()}
}

func (c *Conn) discardPeerDataLocked(stream *nativeStream, appLen uint64, cause lateDataCause) error {
	if c.sessionReceiveLimitExceededLocked(appLen) {
		return wireError(CodeFlowControl, "handle DATA", fmt.Errorf("session receive window exceeded"))
	}
	c.accountDiscardedSessionReceiveLocked(appLen)
	c.releaseLateDiscardLocked(stream, appLen, cause)
	if c.lateDataCapExceededLocked(stream) {
		return wireError(CodeProtocol, "handle DATA", fmt.Errorf("late-data cap exceeded"))
	}
	return nil
}

func (c *Conn) bufferPeerDataLocked(stream *nativeStream, appData []byte) {
	if c == nil || stream == nil {
		return
	}
	appLen := uint64(len(appData))
	c.accountBufferedSessionReceiveLocked(appLen)
	stream.recvBuffer = saturatingAdd(stream.recvBuffer, appLen)
	c.addAcceptQueuedBytesLocked(stream, appLen)
	if appLen > 0 {
		if stream.readHead != nil {
			stream.appendReadChunkLocked(c, clonePayloadBytes(appData), nil, nil)
			return
		}
		stream.readBuf = append(stream.readBuf, appData...)
	}
}

func (c *Conn) retainPeerDataLocked(stream *nativeStream, appData, backing []byte, handle *wire.FrameReadBufferHandle) {
	if c == nil || stream == nil {
		releaseReadFrameBuffer(backing, handle)
		return
	}
	appLen := uint64(len(appData))
	if appLen == 0 {
		releaseReadFrameBuffer(backing, handle)
		return
	}
	c.accountBufferedSessionReceiveLocked(appLen)
	stream.recvBuffer = saturatingAdd(stream.recvBuffer, appLen)
	c.addAcceptQueuedBytesLocked(stream, appLen)
	stream.appendReadChunkLocked(c, appData, backing, handle)
}

func (c *Conn) handleDataFrame(frame Frame) error {
	_, err := c.handleDataFrameBuffered(frame, nil, nil)
	return err
}

func (c *Conn) handleDataFrameBuffered(frame Frame, backing []byte, handle *wire.FrameReadBufferHandle) (bool, error) {
	retained := false
	if !c.lockPeerNonCloseFrameHandling() {
		return false, nil
	}
	stream := c.registry.streams[frame.StreamID]
	if stream == nil {
		if terminal := c.terminalDataDispositionForLocked(frame.StreamID); terminal.found() {
			c.mu.Unlock()
			return false, c.handleTerminalDataFrame(frame, terminal.disposition)
		}
	}
	isFirst := stream == nil
	if isFirst {
		var err error
		stream, err = c.openPeerStreamLocked(frame.StreamID, peerStreamVisibleOnData)
		if err != nil {
			c.mu.Unlock()
			return false, err
		}
		if stream == nil {
			c.mu.Unlock()
			return false, nil
		}
	}
	arrival := peerDataArrivalContinue
	if frame.Flags&FrameFlagFIN != 0 {
		arrival = peerDataArrivalFinal
	}
	dataPlan := stream.peerDataPlanLocked(arrival)
	switch dataPlan.Outcome {
	case state.PeerDataAbortState:
		return false, c.abortLiveStreamLocked(stream, CodeStreamState, "")
	case state.PeerDataAbortClosed:
		return false, c.abortLiveStreamLocked(stream, CodeStreamClosed, "")
	case state.PeerDataIgnore:
		if frame.Flags&FrameFlagOpenMetadata != 0 {
			c.mu.Unlock()
			return false, wireError(CodeProtocol, "handle DATA", fmt.Errorf("OPEN_METADATA on already-open stream %d", frame.StreamID))
		}
		appBytes, err := dataFrameAppData(frame)
		if err != nil {
			c.mu.Unlock()
			return false, frameSizeError("handle DATA", err)
		}
		appLen := uint64(len(appBytes))
		discardPlan := ignoredPeerDataLateDiscard(stream, lateDataTrackingFrom(dataPlan.TrackLatePerStream))
		if err := c.discardPeerDataLocked(discardPlan.stream, appLen, discardPlan.cause); err != nil {
			c.mu.Unlock()
			return false, err
		}
		if dataPlan.AdvanceRecvFin {
			stream.setRecvFin()
			c.maybeFinalizePeerActiveLocked(stream)
		}
		c.mu.Unlock()
		return false, nil
	default:
	}
	c.mu.Unlock()

	parsed, err := parseDataPayloadView(frame.Payload, frame.Flags)
	if err != nil {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return false, nil
		}
		return false, frameSizeError("handle DATA", err)
	}

	c.mu.Lock()
	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		c.mu.Unlock()
		return false, nil
	}
	// The payload parse runs without c.mu held, so the stream may have been
	// finalized or removed before we can safely mutate receive state.
	stream = c.registry.streams[frame.StreamID]
	if stream == nil {
		if terminal := c.terminalDataDispositionForLocked(frame.StreamID); terminal.found() {
			c.mu.Unlock()
			return false, c.handleTerminalDataPayload(frame, parsed.AppData, terminal.disposition)
		}
		c.mu.Unlock()
		return false, nil
	}
	if frame.Flags&FrameFlagOpenMetadata != 0 && !isFirst {
		c.mu.Unlock()
		return false, wireError(CodeProtocol, "handle DATA", fmt.Errorf("OPEN_METADATA on already-open stream %d", frame.StreamID))
	}
	if frame.Flags&FrameFlagOpenMetadata != 0 && !c.config.negotiated.Capabilities.Has(CapabilityOpenMetadata) {
		c.mu.Unlock()
		return false, wireError(CodeProtocol, "handle DATA", fmt.Errorf("OPEN_METADATA without negotiated capability"))
	}

	appLen := uint64(len(parsed.AppData))
	now := time.Now()
	observation := zeroDataProgress
	if !isFirst && appLen == 0 && frame.Flags&FrameFlagFIN == 0 && frame.Flags&FrameFlagOpenMetadata == 0 {
		observation = zeroDataNoOp
	}
	if err := c.updateNoOpZeroDataLocked(observation, now); err != nil {
		c.mu.Unlock()
		return false, err
	}
	if receiveWindowExceeded(stream.recvReceived, stream.recvAdvertised, appLen) {
		return false, c.abortLiveStreamLocked(stream, CodeFlowControl, "")
	}
	if c.sessionReceiveLimitExceededLocked(appLen) {
		c.mu.Unlock()
		return false, wireError(CodeFlowControl, "handle DATA", fmt.Errorf("session receive window exceeded"))
	}
	c.markPeerVisibleLocked(stream)
	stream.recvReceived = saturatingAdd(stream.recvReceived, appLen)
	if isFirst {
		c.applyReceivedMetadataLocked(stream, parsed.Metadata, receivedMetadataOnOpen)
	}
	if stream.readStopSentLocked() {
		if err := c.discardPeerDataLocked(stream, appLen, lateDataCauseCloseRead); err != nil {
			c.mu.Unlock()
			return false, err
		}
	} else if len(backing) > 0 && len(parsed.AppData) > 0 {
		c.retainPeerDataLocked(stream, parsed.AppData, backing, handle)
		retained = true
	} else {
		c.bufferPeerDataLocked(stream, parsed.AppData)
	}
	if isFirst && !stream.isLocalOpenedLocked() {
		c.markApplicationVisibleLocked(stream)
		c.enqueueAcceptedLocked(stream)
	}
	if c.flow.sessionMemoryCap > 0 {
		if memoryErr := c.sessionMemoryCapErrorLocked("handle DATA"); memoryErr != nil {
			c.mu.Unlock()
			c.closeSession(memoryErr)
			return retained, memoryErr
		}
	}
	var refusedVisibleStreamIDs []uint64
	if stream.applicationVisible && !stream.acceptedFlag() {
		refusedVisibleStreamIDs = c.enforceVisibleAcceptBacklogLocked()
		for _, streamID := range refusedVisibleStreamIDs {
			if streamID != stream.id {
				continue
			}
			c.mu.Unlock()
			for _, refusedID := range refusedVisibleStreamIDs {
				if err := c.abortWithCodeAsync(refusedID, CodeRefusedStream); err != nil {
					return retained, err
				}
			}
			return retained, nil
		}
	}
	if memoryErr := c.sessionMemoryCapErrorLocked("handle DATA"); memoryErr != nil {
		c.mu.Unlock()
		c.closeSession(memoryErr)
		return retained, memoryErr
	}
	if frame.Flags&FrameFlagFIN != 0 {
		stream.setRecvFin()
	}
	notify(stream.readNotify)
	c.maybeFinalizePeerActiveLocked(stream)
	c.mu.Unlock()
	for _, refusedID := range refusedVisibleStreamIDs {
		if err := c.abortWithCodeAsync(refusedID, CodeRefusedStream); err != nil {
			return retained, err
		}
	}
	return retained, nil
}

func (c *Conn) handleTerminalDataFrame(frame Frame, disposition terminalDataDisposition) error {
	appBytes, err := dataFrameAppData(frame)
	if err != nil {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle DATA", err)
	}
	return c.handleTerminalDataPayload(frame, appBytes, disposition)
}

func (c *Conn) handleTerminalDataPayload(frame Frame, appData []byte, disposition terminalDataDisposition) error {
	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	c.mu.Unlock()

	if frame.Flags&FrameFlagOpenMetadata != 0 {
		return wireError(CodeProtocol, "handle DATA", fmt.Errorf("OPEN_METADATA on previously used stream %d", frame.StreamID))
	}
	switch disposition.action {
	case lateDataAbortClosed:
		return c.abortWithCodeAsync(frame.StreamID, CodeStreamClosed)
	case lateDataAbortState:
		return c.abortStreamStateAsync(frame.StreamID)
	default:
		if !c.lockPeerNonCloseFrameHandling() {
			return nil
		}
		defer c.mu.Unlock()
		appLen := uint64(len(appData))
		return c.discardPeerDataLocked(nil, appLen, disposition.cause)
	}
}

func dataFrameAppData(frame Frame) ([]byte, error) {
	parsed, err := parseDataPayloadView(frame.Payload, frame.Flags)
	if err != nil {
		return nil, err
	}
	return parsed.AppData, nil
}

func (c *Conn) recordIgnoredPeerTerminalControlLocked(op string, now time.Time) error {
	return c.recordNoOpControlLocked(now, "handle "+op)
}

func (c *Conn) finishPeerVisibleTerminalControlLocked(stream *nativeStream, op string, now time.Time, notifyMask streamNotifyMask) error {
	if c == nil || stream == nil {
		return nil
	}
	c.clearNoOpControlBudgetsLocked()
	if err := c.recordVisibleTerminalChurnLocked(stream, now, "handle "+op); err != nil {
		return err
	}
	notifyStreamLocked(stream, notifyMask)
	c.maybeFinalizePeerActiveLocked(stream)
	return nil
}

func (c *Conn) handleStopSendingFrame(frame Frame) error {
	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	c.mu.Unlock()

	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle STOP_SENDING", err)
	}

	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	stream, err := c.lookupExistingPeerStreamLocked("STOP_SENDING", frame.StreamID)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if stream == nil {
		c.mu.Unlock()
		return nil
	}
	if !stream.localSend {
		return c.abortLiveStreamLocked(stream, CodeStreamState, "")
	}
	now := time.Now()
	c.markPeerVisibleLocked(stream)
	plan := state.PlanPeerStopSending(
		stream.localSend,
		stream.localReceive,
		stream.effectiveSendHalfStateLocked(),
		stream.effectiveRecvHalfStateLocked(),
	)
	gracefulFinish := false
	if plan.Ignore {
		if err := c.recordIgnoredPeerTerminalControlLocked("STOP_SENDING", now); err != nil {
			c.mu.Unlock()
			return err
		}
		c.mu.Unlock()
		return nil
	}
	if plan.RecordStop {
		reason, stream.sendStopReasonBytes = c.retainPeerReasonLocked(stream.sendStopReasonBytes, reason)
		stream.setSendStopSeen(applicationErr(code, reason))
		c.clearNoOpControlBudgetsLocked()
		if plan.Outcome == state.StopSendingFinish {
			var (
				sendRateEstimate uint64
				stopTailCap      uint64
				drainWindow      time.Duration
			)
			if stream.conn != nil {
				sendRateEstimate = stream.conn.metrics.sendRateEstimate
				stopTailCap = stream.conn.terminalPolicy.stopSendingTailCap
				drainWindow = stream.conn.terminalPolicy.stopSendingDrainWindow
			}
			gracefulFinish = rt.EvaluateStopSendingGraceful(rt.StopSendingGracefulInput{
				RecvAbortive:     stream.recvAbortiveLocked(),
				NeedsLocalOpener: stream.needsLocalOpenerLocked(),
				LocalOpened:      stream.isLocalOpenedLocked(),
				SendCommitted:    stream.isSendCommittedLocked(),
				QueuedDataBytes:  stream.queuedDataBytes,
				InflightQueued:   stream.inflightQueued,
				FragmentCap:      stream.txFragmentCapLocked(0),
				SendRateEstimate: sendRateEstimate,
				ExplicitTailCap:  stopTailCap,
				DrainWindow:      drainWindow,
			}).Attempt
		}
	}
	notify(stream.writeNotify)
	c.mu.Unlock()

	if gracefulFinish {
		deadline := time.Now().Add(rt.StopSendingDrainWindow(0))
		if conn := stream.conn; conn != nil {
			deadline = time.Now().Add(rt.StopSendingDrainWindow(conn.terminalPolicy.stopSendingDrainWindow))
		}
		err := stream.closeWriteUntil(deadline)
		if err == nil || errors.Is(err, ErrWriteClosed) {
			return nil
		}
		if conn := stream.conn; conn != nil {
			conn.mu.Lock()
			sendTerminal := state.SendTerminal(stream.effectiveSendHalfStateLocked())
			conn.mu.Unlock()
			if sendTerminal {
				return nil
			}
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return err
		}
		return stream.resetAfterStopSending(uint64(CodeCancelled))
	}
	return stream.resetAfterStopSending(uint64(CodeCancelled))
}

func (c *Conn) handleResetFrame(frame Frame) error {
	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	c.mu.Unlock()

	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle RESET", err)
	}

	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	stream, err := c.lookupExistingPeerStreamLocked("RESET", frame.StreamID)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if stream == nil {
		c.mu.Unlock()
		return nil
	}
	if !stream.localReceive {
		return c.abortLiveStreamLocked(stream, CodeStreamState, "")
	}
	now := time.Now()
	c.markPeerVisibleLocked(stream)
	plan := state.PlanPeerReset(
		stream.localSend,
		stream.localReceive,
		stream.effectiveSendHalfStateLocked(),
		stream.effectiveRecvHalfStateLocked(),
	)
	if plan.Ignore {
		if err := c.recordIgnoredPeerTerminalControlLocked("RESET", now); err != nil {
			c.mu.Unlock()
			return err
		}
		c.mu.Unlock()
		return nil
	}
	if plan.RecordReset {
		reason, stream.recvResetReasonBytes = c.retainPeerReasonLocked(stream.recvResetReasonBytes, reason)
		c.noteResetReasonLocked(code)
		stream.setRecvReset(applicationErr(code, reason))
	}
	releaseTraits := streamReceiveReleaseTraitNone
	if plan.ReleaseReceive {
		releaseTraits |= streamReceiveReleaseTraitBudget
	}
	if plan.ClearReadBuf {
		releaseTraits |= streamReceiveReleaseTraitClearReadBuf
	}
	c.applyReceiveReleasePlanLocked(stream, releaseTraits.releaseMode())
	if err := c.finishPeerVisibleTerminalControlLocked(stream, "RESET", now, streamNotifyRead); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	return nil
}

func (c *Conn) handleAbortFrame(frame Frame) error {
	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	c.mu.Unlock()

	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle ABORT", err)
	}

	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	stream := c.registry.streams[frame.StreamID]
	isFirst := stream == nil
	if stream == nil {
		if c.hasTerminalMarkerLocked(frame.StreamID) {
			c.mu.Unlock()
			return nil
		}
		stream, err = c.openPeerStreamLocked(frame.StreamID, peerStreamHidden)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		if stream == nil {
			c.mu.Unlock()
			return nil
		}
	}
	now := time.Now()
	if isFirst {
		if err := c.recordHiddenAbortChurnLocked(now); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	plan := state.PlanPeerAbort(
		stream.localSend,
		stream.localReceive,
		stream.effectiveSendHalfStateLocked(),
		stream.effectiveRecvHalfStateLocked(),
	)
	if plan.Ignore {
		if err := c.recordIgnoredPeerTerminalControlLocked("ABORT", now); err != nil {
			c.mu.Unlock()
			return err
		}
		c.mu.Unlock()
		return nil
	}
	c.markPeerVisibleLocked(stream)
	reason, stream.remoteAbortReasonBytes = c.retainPeerReasonLocked(stream.remoteAbortReasonBytes, reason)
	appErr := applicationErr(code, reason)
	if plan.RecordAbort {
		c.noteAbortReasonLocked(code)
		stream.setAbortedWithSource(appErr, terminalAbortFromPeer)
		c.releaseTerminalStreamStateLocked(stream, transientStreamReleaseOptions{
			send:    true,
			receive: streamReceiveReleaseAndClearReadBuf,
		})
	} else {
		if plan.ReleaseSend {
			c.releaseSendLocked(stream)
		}
		releaseTraits := streamReceiveReleaseTraitNone
		if plan.ReleaseReceive {
			releaseTraits |= streamReceiveReleaseTraitBudget
		}
		if plan.ClearReadBuf {
			releaseTraits |= streamReceiveReleaseTraitClearReadBuf
		}
		c.applyReceiveReleasePlanLocked(stream, releaseTraits.releaseMode())
	}
	if err := c.finishPeerVisibleTerminalControlLocked(stream, "ABORT", now, streamNotifyBoth); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	return nil
}

type lateDataCause uint8

const (
	lateDataCauseNone lateDataCause = iota
	lateDataCauseCloseRead
	lateDataCauseReset
	lateDataCauseAbort
)

func receiveWindowExceeded(received, advertised, n uint64) bool {
	if n == 0 {
		return false
	}
	return n > csub(advertised, received)
}

func (c *Conn) sessionReceiveLimitExceededLocked(n uint64) bool {
	return receiveWindowExceeded(c.flow.recvSessionReceived, c.flow.recvSessionAdvertised, n)
}

func (c *Conn) accountBufferedSessionReceiveLocked(n uint64) {
	if n == 0 {
		return
	}
	c.flow.recvSessionReceived = saturatingAdd(c.flow.recvSessionReceived, n)
	c.flow.recvSessionUsed = saturatingAdd(c.flow.recvSessionUsed, n)
}

func (c *Conn) accountDiscardedSessionReceiveLocked(n uint64) {
	if n == 0 {
		return
	}
	c.flow.recvSessionReceived = saturatingAdd(c.flow.recvSessionReceived, n)
}

func (c *Conn) releaseReceiveLocked(stream *nativeStream, n uint64) {
	if n == 0 {
		return
	}
	prevTracked := c.trackedSessionMemoryLocked()
	sessionN := n
	if sessionN > c.flow.recvSessionUsed {
		sessionN = c.flow.recvSessionUsed
	}
	c.flow.recvSessionUsed -= sessionN
	c.flow.recvSessionAdvertised = clampVarint62(rt.SaturatingAdd(c.flow.recvSessionAdvertised, sessionN))
	c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(c.flow.recvSessionAdvertised))

	if stream == nil {
		c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(sessionN > 0))
		return
	}
	if !stream.applicationVisible {
		c.noteHiddenUnreadDiscardLocked(sessionN)
	}
	stream.recvPending = 0
	streamN := sessionN
	if streamN > stream.recvBuffer {
		streamN = stream.recvBuffer
	}
	c.releaseAcceptQueuedBytesLocked(stream, streamN)
	stream.recvBuffer = csub(stream.recvBuffer, streamN)
	if stream.recvAdvertised == 0 {
		c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(sessionN > 0))
		return
	}
	if stream.localReceive && !stream.readStopSentLocked() && !state.RecvTerminal(stream.effectiveRecvHalfStateLocked()) {
		stream.recvAdvertised = clampVarint62(rt.SaturatingAdd(stream.recvAdvertised, streamN))
		c.queueStreamMaxDataAsync(stream.id, stream.recvAdvertised)
	}
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(sessionN > 0))
}

func (c *Conn) releaseLateDiscardLocked(stream *nativeStream, n uint64, cause lateDataCause) {
	if n == 0 {
		return
	}
	c.flow.recvSessionAdvertised = clampVarint62(rt.SaturatingAdd(c.flow.recvSessionAdvertised, n))
	c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(c.flow.recvSessionAdvertised))
	c.ingress.aggregateLateData = rt.SaturatingAdd(c.ingress.aggregateLateData, n)
	c.recordLateDiscardCauseLocked(cause, n)
	if stream != nil {
		if !stream.applicationVisible {
			c.noteHiddenUnreadDiscardLocked(n)
		}
		stream.lateDataReceived = rt.SaturatingAdd(stream.lateDataReceived, n)
		stream.recvPending = 0
	}
}

func (c *Conn) recordLateDiscardCauseLocked(cause lateDataCause, n uint64) {
	switch cause {
	case lateDataCauseCloseRead:
		c.ingress.lateDataAfterClose = rt.SaturatingAdd(c.ingress.lateDataAfterClose, n)
	case lateDataCauseReset:
		c.ingress.lateDataAfterReset = rt.SaturatingAdd(c.ingress.lateDataAfterReset, n)
	case lateDataCauseAbort:
		c.ingress.lateDataAfterAbort = rt.SaturatingAdd(c.ingress.lateDataAfterAbort, n)
	default:
	}
}

func (c *Conn) lateDataCapExceededLocked(stream *nativeStream) bool {
	if c.ingress.aggregateLateDataCap > 0 && c.ingress.aggregateLateData > c.ingress.aggregateLateDataCap {
		return true
	}
	if stream == nil {
		return false
	}
	if limit := c.effectiveLateDataPerStreamCapLocked(stream); limit.exceeded(stream.lateDataReceived) {
		return true
	}
	return false
}

type lateDataPerStreamCap struct {
	value   uint64
	enabled bool
}

func (c lateDataPerStreamCap) exceeded(received uint64) bool {
	return c.enabled && received > c.value
}

func (c *Conn) effectiveLateDataPerStreamCapLocked(stream *nativeStream) lateDataPerStreamCap {
	if c == nil || stream == nil || !stream.localReceive || !stream.idSet {
		return lateDataPerStreamCap{}
	}
	if c.ingress.lateDataPerStreamCap > 0 {
		return lateDataPerStreamCap{value: c.ingress.lateDataPerStreamCap, enabled: true}
	}
	maxFramePayload := c.config.local.Settings.MaxFramePayload
	if maxFramePayload == 0 {
		maxFramePayload = DefaultSettings().MaxFramePayload
	}
	initialWindow := state.InitialReceiveWindow(c.config.negotiated.LocalRole, c.config.local.Settings, stream.id)
	return lateDataPerStreamCap{value: lateDataPerStreamCapFor(initialWindow, maxFramePayload), enabled: true}
}

func lateDataPerStreamCapFor(initialStreamWindow, maxFramePayload uint64) uint64 {
	return rt.LateDataPerStreamCap(initialStreamWindow, maxFramePayload)
}

func aggregateLateDataCapFor(maxFramePayload uint64) uint64 {
	return rt.AggregateLateDataCap(maxFramePayload)
}

type peerMaxDataUpdate uint8

const (
	peerMaxDataUnchanged peerMaxDataUpdate = iota
	peerMaxDataExpanded
)

func (u peerMaxDataUpdate) changed() bool {
	return u == peerMaxDataExpanded
}

type windowReplenishPolicy uint8

const (
	windowReplenishIfNeeded windowReplenishPolicy = iota
	windowReplenishForce
)

func (p windowReplenishPolicy) forcesReplenish() bool {
	return p == windowReplenishForce
}

func (c *Conn) finishPeerMaxDataFrameLocked(update peerMaxDataUpdate, now time.Time) error {
	if update.changed() {
		c.clearNoOpMaxDataLocked()
		return nil
	}
	return c.recordNoOpMaxDataLocked(now)
}

func (c *Conn) finishPeerBlockedFrameLocked(stream *nativeStream, now time.Time) error {
	hasPending := c.flow.recvSessionPending != 0
	if stream != nil {
		hasPending = hasPending || stream.recvPending != 0
	}
	if hasPending {
		c.clearNoOpBlockedLocked()
	} else if err := c.recordNoOpBlockedLocked(now); err != nil {
		return err
	}
	c.maybeReplenishSessionLockedWithPolicy(windowReplenishForce)
	if stream != nil {
		c.maybeReplenishStreamLockedWithPolicy(stream, windowReplenishForce)
	}
	return nil
}

func (c *Conn) handleSessionMaxDataFrame(value uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		return nil
	}
	now := time.Now()
	update := peerMaxDataUnchanged
	if value > c.flow.sendSessionMax {
		update = peerMaxDataExpanded
		prevTracked := c.trackedSessionMemoryLocked()
		c.flow.sendSessionMax = value
		c.clearSessionBlockedStateLocked()
		released := c.dropPendingSessionControlLocked(sessionControlBlocked)
		c.broadcastWriteWakeLocked()
		if released && c.sessionMemoryWakeNeededLocked(prevTracked) {
			notify(c.pending.controlNotify)
		}
	}
	return c.finishPeerMaxDataFrameLocked(update, now)
}

func (c *Conn) handleSessionBlockedFrame() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		return nil
	}
	return c.finishPeerBlockedFrameLocked(nil, time.Now())
}

func (c *Conn) handleMaxDataFrame(frame Frame) error {
	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	c.mu.Unlock()

	value, _, err := ParseVarint(frame.Payload)
	if err != nil {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle MAX_DATA", err)
	}

	if frame.StreamID == 0 {
		return c.handleSessionMaxDataFrame(value)
	}
	return c.handleStreamMaxDataFrame(frame.StreamID, value)
}

func (c *Conn) handleStreamMaxDataFrame(streamID uint64, value uint64) error {
	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	stream, err := c.lookupExistingPeerStreamLocked("MAX_DATA", streamID)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if stream == nil {
		c.mu.Unlock()
		return nil
	}
	now := time.Now()
	switch state.PeerMaxDataAction(
		stream.localSend,
		stream.localReceive,
		stream.effectiveSendHalfStateLocked(),
		stream.effectiveRecvHalfStateLocked(),
	) {
	case state.PeerStreamControlIgnore:
		if err := c.recordNoOpMaxDataLocked(now); err != nil {
			c.mu.Unlock()
			return err
		}
		c.mu.Unlock()
		return nil
	case state.PeerStreamControlAbortState:
		return c.abortLiveStreamLocked(stream, CodeStreamState, "")
	default:
	}
	c.markPeerVisibleLocked(stream)
	update := peerMaxDataUnchanged
	if value > stream.sendMax {
		update = peerMaxDataExpanded
		stream.sendMax = value
		c.releaseStreamRuntimeStateLocked(stream, streamRuntimeBlocked)
		notify(stream.writeNotify)
	}
	if err := c.finishPeerMaxDataFrameLocked(update, now); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	return nil
}

func (c *Conn) handleBlockedFrame(frame Frame) error {
	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	c.mu.Unlock()

	if _, _, err := ParseVarint(frame.Payload); err != nil {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle BLOCKED", err)
	}
	if frame.StreamID == 0 {
		return c.handleSessionBlockedFrame()
	}
	return c.handleStreamBlockedFrame(frame.StreamID)
}

func (c *Conn) handleStreamBlockedFrame(streamID uint64) error {
	if !c.lockPeerNonCloseFrameHandling() {
		return nil
	}
	stream, err := c.lookupExistingPeerStreamLocked("BLOCKED", streamID)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if stream == nil {
		c.mu.Unlock()
		return nil
	}
	now := time.Now()
	switch state.PeerBlockedAction(
		stream.localSend,
		stream.localReceive,
		stream.effectiveSendHalfStateLocked(),
		stream.effectiveRecvHalfStateLocked(),
	) {
	case state.PeerStreamControlIgnore:
		if err := c.recordNoOpBlockedLocked(now); err != nil {
			c.mu.Unlock()
			return err
		}
		c.mu.Unlock()
		return nil
	case state.PeerStreamControlAbortState:
		return c.abortLiveStreamLocked(stream, CodeStreamState, "")
	default:
	}
	c.markPeerVisibleLocked(stream)
	if err := c.finishPeerBlockedFrameLocked(stream, now); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	return nil
}

func (c *Conn) consumeReceiveLocked(stream *nativeStream, n uint64) {
	if n == 0 {
		return
	}

	sessionN := n
	if sessionN > c.flow.recvSessionUsed {
		sessionN = c.flow.recvSessionUsed
	}
	c.flow.recvSessionUsed -= sessionN
	c.flow.recvSessionPending = saturatingAdd(c.flow.recvSessionPending, sessionN)

	if stream != nil {
		streamN := sessionN
		if streamN > stream.recvBuffer {
			streamN = stream.recvBuffer
		}
		c.releaseAcceptQueuedBytesLocked(stream, streamN)
		stream.recvBuffer -= streamN
		if stream.localReceive && !stream.readStopSentLocked() && !state.RecvTerminal(stream.effectiveRecvHalfStateLocked()) {
			stream.recvPending = saturatingAdd(stream.recvPending, streamN)
		}
	}

	c.maybeReplenishReceiveLocked(stream)
}

func (c *Conn) maybeReplenishReceiveLocked(stream *nativeStream) {
	c.maybeReplenishSessionLocked()
	c.maybeReplenishStreamLocked(stream)
}

func (c *Conn) maybeReplenishSessionLocked() {
	c.maybeReplenishSessionLockedWithPolicy(windowReplenishIfNeeded)
}

func (c *Conn) maybeReplenishSessionLockedWithPolicy(policy windowReplenishPolicy) {
	if c.flow.recvSessionPending == 0 {
		return
	}
	target := c.sessionWindowTargetLocked()
	if !policy.forcesReplenish() && !shouldReplenishPendingWindow(
		rt.WindowRemaining(c.flow.recvSessionAdvertised, c.flow.recvSessionReceived),
		target,
		c.flow.recvSessionAdvertised,
		c.flow.recvSessionPending,
		c.sessionEmergencyThresholdLocked(),
		c.sessionReplenishMinPendingLocked(target),
	) {
		return
	}
	c.replenishSessionLocked(target)
}

func (c *Conn) replenishSessionLocked(target uint64) {
	floor := rt.SaturatingAdd(c.flow.recvSessionAdvertised, c.flow.recvSessionPending)
	desired := floor
	if c.sessionStandingGrowthAllowedLocked() {
		targetDesired := rt.SaturatingAdd(c.flow.recvSessionReceived, target)
		if targetDesired > desired {
			desired = targetDesired
		}
	}
	desired = clampVarint62(desired)
	c.flow.recvSessionAdvertised = desired
	c.flow.recvSessionPending = 0
	c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(c.flow.recvSessionAdvertised))
}

func (c *Conn) maybeReplenishStreamLocked(stream *nativeStream) {
	c.maybeReplenishStreamLockedWithPolicy(stream, windowReplenishIfNeeded)
}

func (c *Conn) maybeReplenishStreamLockedWithPolicy(stream *nativeStream, policy windowReplenishPolicy) {
	if stream == nil || stream.recvPending == 0 {
		return
	}
	if !stream.idSet || !stream.localReceive || stream.readStopSentLocked() || state.RecvTerminal(stream.effectiveRecvHalfStateLocked()) {
		stream.recvPending = 0
		return
	}

	target := c.streamWindowTargetLocked(stream)
	if !policy.forcesReplenish() && !shouldReplenishPendingWindow(
		rt.WindowRemaining(stream.recvAdvertised, stream.recvReceived),
		target,
		stream.recvAdvertised,
		stream.recvPending,
		c.streamEmergencyThresholdLocked(target),
		c.streamReplenishMinPendingLocked(target),
	) {
		return
	}
	c.replenishStreamLocked(stream, target)
}

func (c *Conn) replenishStreamLocked(stream *nativeStream, target uint64) {
	floor := rt.SaturatingAdd(stream.recvAdvertised, stream.recvPending)
	desired := floor
	if c.streamStandingGrowthAllowedLocked(stream) {
		targetDesired := rt.SaturatingAdd(stream.recvReceived, target)
		if targetDesired > desired {
			desired = targetDesired
		}
	}
	desired = clampVarint62(desired)
	stream.recvAdvertised = desired
	stream.recvPending = 0
	c.queueStreamMaxDataAsync(stream.id, stream.recvAdvertised)
}

func (c *Conn) sessionWindowTargetLocked() uint64 {
	return rt.MaxUint64(c.config.local.Settings.InitialMaxData, rt.SaturatingMul(c.sessionDataHWMLocked(), 4))
}

func (c *Conn) streamWindowTargetLocked(stream *nativeStream) uint64 {
	if stream == nil || !stream.idSet {
		return 0
	}
	return rt.MaxUint64(state.InitialReceiveWindow(c.config.negotiated.LocalRole, c.config.local.Settings, stream.id), rt.SaturatingMul(c.perStreamDataHWMLocked(), 2))
}

func (c *Conn) sessionEmergencyThresholdLocked() uint64 {
	payload := negotiatedFramePayloadForReplenish(c.config.local.Settings, c.config.peer.Settings)
	return rt.SaturatingMul(payload, 2)
}

func (c *Conn) streamEmergencyThresholdLocked(target uint64) uint64 {
	payload := negotiatedFramePayloadForReplenish(c.config.local.Settings, c.config.peer.Settings)
	if payload == 0 {
		return 0
	}
	return minNonZeroUint64(payload, quarterThreshold(target))
}

func (c *Conn) sessionReplenishMinPendingLocked(target uint64) uint64 {
	return replenishMinPending(target, negotiatedFramePayloadForReplenish(c.config.local.Settings, c.config.peer.Settings))
}

func (c *Conn) streamReplenishMinPendingLocked(target uint64) uint64 {
	return replenishMinPending(target, negotiatedFramePayloadForReplenish(c.config.local.Settings, c.config.peer.Settings))
}

func quarterThreshold(v uint64) uint64 {
	return rt.QuarterThreshold(v)
}

func shouldReplenishPendingWindow(remaining, target, advertised, pending, emergencyThreshold, minPending uint64) bool {
	if remaining <= emergencyThreshold {
		return true
	}
	if remaining <= quarterThreshold(target) && advertised >= target {
		return true
	}
	return pending >= minPending
}

func replenishMinPending(target, payload uint64) uint64 {
	minPending := quarterThreshold(target)
	if minPending == 0 {
		minPending = 1
	}
	if payload > 0 && payload < minPending {
		minPending = payload
	}
	return minPending
}

func negotiatedFramePayloadForReplenish(local, peer Settings) uint64 {
	return rt.NegotiatedFramePayload(local, peer)
}

func repoDefaultPerStreamDataHWM(maxFramePayload uint64) uint64 {
	return rt.RepoDefaultPerStreamDataHWM(maxFramePayload)
}

func repoDefaultSessionDataHWM(perStreamDataHWM uint64) uint64 {
	return rt.RepoDefaultSessionDataHWM(perStreamDataHWM)
}

func (c *Conn) perStreamDataHWMLocked() uint64 {
	if c == nil {
		settings := DefaultSettings()
		payload := negotiatedFramePayloadForReplenish(settings, settings)
		return repoDefaultPerStreamDataHWM(payload)
	}
	if c.flow.perStreamDataHWM > 0 {
		return c.flow.perStreamDataHWM
	}
	payload := negotiatedFramePayloadForReplenish(c.config.local.Settings, c.config.peer.Settings)
	return repoDefaultPerStreamDataHWM(payload)
}

func (c *Conn) sessionDataHWMLocked() uint64 {
	if c == nil {
		settings := DefaultSettings()
		payload := negotiatedFramePayloadForReplenish(settings, settings)
		return repoDefaultSessionDataHWM(repoDefaultPerStreamDataHWM(payload))
	}
	if c.flow.sessionDataHWM > 0 {
		return c.flow.sessionDataHWM
	}
	return repoDefaultSessionDataHWM(c.perStreamDataHWMLocked())
}

func (c *Conn) sessionStandingGrowthAllowedLocked() bool {
	if c.sessionMemoryPressureHighLocked() {
		return false
	}
	hwm := c.sessionDataHWMLocked()
	if c.flow.recvSessionUsed >= hwm {
		return false
	}
	return saturatingAdd(c.flow.recvSessionUsed, c.flow.recvSessionPending) < hwm
}

func (c *Conn) streamStandingGrowthAllowedLocked(stream *nativeStream) bool {
	if stream == nil {
		return false
	}
	if c.sessionMemoryPressureHighLocked() {
		return false
	}
	hwm := c.perStreamDataHWMLocked()
	if stream.recvBuffer >= hwm {
		return false
	}
	return saturatingAdd(stream.recvBuffer, stream.recvPending) < hwm
}

func minNonZeroUint64(a, b uint64) uint64 {
	return rt.MinNonZeroUint64(a, b)
}

func maxUint64(a, b uint64) uint64 {
	return rt.MaxUint64(a, b)
}

func saturatingAdd(a, b uint64) uint64 {
	return rt.SaturatingAdd(a, b)
}

func saturatingMul(v uint64, n uint64) uint64 {
	return rt.SaturatingMul(v, n)
}

// windowStamp stores an internal rolling-window anchor in a compact form.
// These anchors are only compared against other process-local timestamps, so
// they do not need the full time.Time representation.
type windowStamp int64

func windowStampAt(now time.Time) windowStamp {
	if now.IsZero() {
		return 0
	}
	return windowStamp(now.UnixNano())
}

func windowStampElapsed(now time.Time, start windowStamp) time.Duration {
	if start == 0 || now.IsZero() {
		return 0
	}
	elapsed := now.UnixNano() - int64(start)
	if elapsed <= 0 {
		return 0
	}
	return time.Duration(elapsed)
}

const (
	localAbuseWindow                 = 5 * time.Second
	inboundControlFrameBudget        = 2048
	inboundExtFrameBudget            = 1024
	minInboundControlByteBudget      = 256 << 10
	minInboundExtByteBudget          = 256 << 10
	noOpControlFloodThreshold        = 128
	noOpMaxDataFloodThreshold        = 128
	noOpBlockedFloodThreshold        = 128
	noOpZeroDataFloodThreshold       = 128
	noOpPriorityUpdateFloodThreshold = 128
	groupRebucketChurnThreshold      = 256
	inboundPingFloodThreshold        = 128

	hiddenAbortChurnWindow    = 1 * time.Second
	hiddenAbortChurnThreshold = 128
	visibleChurnWindow        = 1 * time.Second
	visibleChurnThreshold     = 128
)

func (c *Conn) abuseWindowLocked() time.Duration {
	if c != nil && c.abuse.abuseWindow > 0 {
		return c.abuse.abuseWindow
	}
	return localAbuseWindow
}

func (c *Conn) inboundControlFrameBudgetLocked() uint32 {
	if c != nil && c.abuse.controlFrameBudget > 0 {
		return c.abuse.controlFrameBudget
	}
	return inboundControlFrameBudget
}

func (c *Conn) inboundExtFrameBudgetLocked() uint32 {
	if c != nil && c.abuse.extFrameBudget > 0 {
		return c.abuse.extFrameBudget
	}
	return inboundExtFrameBudget
}

func (c *Conn) noOpControlFloodThresholdLocked() uint32 {
	if c != nil && c.abuse.noOpControlFloodLimit > 0 {
		return c.abuse.noOpControlFloodLimit
	}
	return noOpControlFloodThreshold
}

func (c *Conn) noOpMaxDataFloodThresholdLocked() uint32 {
	if c != nil && c.abuse.noOpMaxDataFloodLimit > 0 {
		return c.abuse.noOpMaxDataFloodLimit
	}
	return noOpMaxDataFloodThreshold
}

func (c *Conn) noOpBlockedFloodThresholdLocked() uint32 {
	if c != nil && c.abuse.noOpBlockedFloodLimit > 0 {
		return c.abuse.noOpBlockedFloodLimit
	}
	return noOpBlockedFloodThreshold
}

func (c *Conn) noOpZeroDataFloodThresholdLocked() uint32 {
	if c != nil && c.abuse.noOpZeroDataFloodLimit > 0 {
		return c.abuse.noOpZeroDataFloodLimit
	}
	return noOpZeroDataFloodThreshold
}

func (c *Conn) noOpPriorityUpdateFloodThresholdLocked() uint32 {
	if c != nil && c.abuse.noOpPriorityFloodLimit > 0 {
		return c.abuse.noOpPriorityFloodLimit
	}
	return noOpPriorityUpdateFloodThreshold
}

func (c *Conn) groupRebucketChurnThresholdLocked() uint32 {
	if c != nil && c.abuse.groupRebucketFloodLimit > 0 {
		return c.abuse.groupRebucketFloodLimit
	}
	return groupRebucketChurnThreshold
}

func (c *Conn) hiddenAbortChurnWindowLocked() time.Duration {
	if c != nil && c.abuse.hiddenAbortChurnWindow > 0 {
		return c.abuse.hiddenAbortChurnWindow
	}
	return hiddenAbortChurnWindow
}

func (c *Conn) hiddenAbortChurnThresholdLocked() uint32 {
	if c != nil && c.abuse.hiddenAbortChurnLimit > 0 {
		return c.abuse.hiddenAbortChurnLimit
	}
	return hiddenAbortChurnThreshold
}

func (c *Conn) visibleChurnWindowLocked() time.Duration {
	if c != nil && c.abuse.visibleChurnWindow > 0 {
		return c.abuse.visibleChurnWindow
	}
	return visibleChurnWindow
}

func (c *Conn) visibleChurnThresholdLocked() uint32 {
	if c != nil && c.abuse.visibleChurnLimit > 0 {
		return c.abuse.visibleChurnLimit
	}
	return visibleChurnThreshold
}

func (c *Conn) inboundPingFloodThresholdLocked() uint32 {
	if c != nil && c.abuse.pingFloodLimit > 0 {
		return c.abuse.pingFloodLimit
	}
	return inboundPingFloodThreshold
}

func (c *Conn) recordInboundControlBudgetLocked(now time.Time, payloadLen uint64, op string) error {
	return c.recordTrafficBudgetLocked(
		now,
		&c.abuse.controlBudgetWindowStart,
		&c.abuse.controlBudgetFrames,
		&c.abuse.controlBudgetBytes,
		c.inboundControlFrameBudgetLocked(),
		c.inboundControlByteBudgetLocked(),
		payloadLen,
		op,
		"high-rate inbound control flood exceeded local threshold",
	)
}

func (c *Conn) recordInboundMixedBudgetLocked(now time.Time, payloadLen uint64, op string) error {
	return c.recordTrafficBudgetLocked(
		now,
		&c.abuse.mixedBudgetWindowStart,
		&c.abuse.mixedBudgetFrames,
		&c.abuse.mixedBudgetBytes,
		c.inboundMixedFrameBudgetLocked(),
		c.inboundMixedByteBudgetLocked(),
		payloadLen,
		op,
		"high-rate inbound mixed control/EXT flood exceeded local threshold",
	)
}

func (c *Conn) recordInboundExtBudgetLocked(now time.Time, payloadLen uint64, op string) error {
	return c.recordTrafficBudgetLocked(
		now,
		&c.abuse.extBudgetWindowStart,
		&c.abuse.extBudgetFrames,
		&c.abuse.extBudgetBytes,
		c.inboundExtFrameBudgetLocked(),
		c.inboundExtByteBudgetLocked(),
		payloadLen,
		op,
		"high-rate inbound EXT flood exceeded local threshold",
	)
}

func (c *Conn) recordInboundPingFloodLocked(now time.Time) error {
	return c.recordAbuseCountLocked(
		now,
		&c.abuse.pingFloodWindowStart,
		&c.abuse.pingFloodCount,
		c.inboundPingFloodThresholdLocked(),
		"handle PING",
		"high-rate inbound PING flood exceeded local threshold",
	)
}

func (c *Conn) recordHiddenAbortChurnLocked(now time.Time) error {
	return c.recordChurnLocked(
		now,
		&c.abuse.hiddenAbortWindowStart,
		&c.abuse.hiddenAbortCount,
		c.hiddenAbortChurnWindowLocked(),
		c.hiddenAbortChurnThresholdLocked(),
		"handle ABORT",
		"rapid hidden open-then-abort churn exceeded local threshold",
	)
}

func (c *Conn) recordVisibleTerminalChurnLocked(stream *nativeStream, now time.Time, op string) error {
	if c == nil || !shouldRecordVisibleTerminalChurnLocked(stream) {
		return nil
	}
	stream.markChurnCounted()
	return c.recordChurnLocked(
		now,
		&c.abuse.visibleChurnWindowStart,
		&c.abuse.visibleChurnCount,
		c.visibleChurnWindowLocked(),
		c.visibleChurnThresholdLocked(),
		op,
		"rapid open-then-reset/abort churn exceeded local threshold",
	)
}

func shouldRecordVisibleTerminalChurnLocked(stream *nativeStream) bool {
	if stream == nil {
		return false
	}
	return !stream.isLocalOpenedLocked() && stream.applicationVisible && !stream.acceptedFlag() && !stream.churnCountedFlag() &&
		state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked())
}

func (c *Conn) recordChurnLocked(now time.Time, windowStart *windowStamp, count *uint32, window time.Duration, threshold uint32, op string, msg string) error {
	if c == nil || windowStart == nil || count == nil {
		return nil
	}
	if *windowStart == 0 || windowStampElapsed(now, *windowStart) >= window {
		*windowStart = windowStampAt(now)
		*count = 0
	}
	*count = saturatingInc32(*count)
	if *count <= threshold {
		return nil
	}
	return wireError(CodeProtocol, op, errors.New(msg))
}

func (c *Conn) recordNoOpZeroDataLocked(now time.Time) error {
	return c.recordAbuseCountLocked(
		now,
		&c.abuse.noopDataWindowStart,
		&c.abuse.noopDataCount,
		c.noOpZeroDataFloodThresholdLocked(),
		"handle DATA",
		"repeated no-op zero-length DATA flood exceeded local threshold",
	)
}

func (c *Conn) recordNoOpBlockedLocked(now time.Time) error {
	if err := c.recordNoOpControlLocked(now, "handle BLOCKED"); err != nil {
		return err
	}
	return c.recordAbuseCountLocked(
		now,
		&c.abuse.noopBlockedWindowStart,
		&c.abuse.noopBlockedCount,
		c.noOpBlockedFloodThresholdLocked(),
		"handle BLOCKED",
		"repeated no-op BLOCKED flood exceeded local threshold",
	)
}

func (c *Conn) recordNoOpMaxDataLocked(now time.Time) error {
	if err := c.recordNoOpControlLocked(now, "handle MAX_DATA"); err != nil {
		return err
	}
	return c.recordAbuseCountLocked(
		now,
		&c.abuse.noopMaxDataWindowStart,
		&c.abuse.noopMaxDataCount,
		c.noOpMaxDataFloodThresholdLocked(),
		"handle MAX_DATA",
		"repeated no-op MAX_DATA flood exceeded local threshold",
	)
}

func (c *Conn) clearNoOpMaxDataLocked() {
	if c == nil {
		return
	}
	c.clearNoOpControlBudgetsLocked()
}

func (c *Conn) clearNoOpBlockedLocked() {
	if c == nil {
		return
	}
	c.clearNoOpControlBudgetsLocked()
}

func (c *Conn) clearNoOpZeroDataLocked() {
	if c == nil {
		return
	}
	c.abuse.noopDataWindowStart = 0
	c.abuse.noopDataCount = 0
}

func (c *Conn) recordNoOpPriorityUpdateLocked(now time.Time) error {
	if err := c.recordNoOpControlLocked(now, "handle PRIORITY_UPDATE"); err != nil {
		return err
	}
	return c.recordAbuseCountLocked(
		now,
		&c.abuse.noopPriorityWindowStart,
		&c.abuse.noopPriorityCount,
		c.noOpPriorityUpdateFloodThresholdLocked(),
		"handle PRIORITY_UPDATE",
		"repeated no-op PRIORITY_UPDATE flood exceeded local threshold",
	)
}

func (c *Conn) clearNoOpPriorityUpdateLocked() {
	if c == nil {
		return
	}
	c.clearNoOpControlBudgetsLocked()
}

func (c *Conn) recordGroupRebucketChurnLocked(now time.Time) error {
	return c.recordAbuseCountLocked(
		now,
		&c.abuse.groupRebucketWindowStart,
		&c.abuse.groupRebucketCount,
		c.groupRebucketChurnThresholdLocked(),
		"handle PRIORITY_UPDATE",
		"high-rate effective stream_group rebucketing churn exceeded local threshold",
	)
}

func (c *Conn) recordNoOpControlLocked(now time.Time, op string) error {
	return c.recordAbuseCountLocked(
		now,
		&c.abuse.noopControlWindowStart,
		&c.abuse.noopControlCount,
		c.noOpControlFloodThresholdLocked(),
		op,
		"repeated mixed no-op control flood exceeded local threshold",
	)
}

func (c *Conn) clearNoOpControlLocked() {
	if c == nil {
		return
	}
	c.abuse.noopControlWindowStart = 0
	c.abuse.noopControlCount = 0
}

func (c *Conn) clearNoOpControlBudgetsLocked() {
	if c == nil {
		return
	}
	c.clearNoOpControlLocked()
	c.abuse.noopMaxDataWindowStart = 0
	c.abuse.noopMaxDataCount = 0
	c.abuse.noopBlockedWindowStart = 0
	c.abuse.noopBlockedCount = 0
	c.abuse.noopPriorityWindowStart = 0
	c.abuse.noopPriorityCount = 0
}

func (c *Conn) recordAbuseCountLocked(now time.Time, windowStart *windowStamp, count *uint32, threshold uint32, op string, msg string) error {
	if c == nil || windowStart == nil || count == nil {
		return nil
	}
	if threshold == 0 {
		return nil
	}
	window := c.abuseWindowLocked()
	if *windowStart == 0 || windowStampElapsed(now, *windowStart) > window {
		*windowStart = windowStampAt(now)
		*count = 0
	}
	*count = saturatingInc32(*count)
	if *count <= threshold {
		return nil
	}
	return wireError(CodeProtocol, op, errors.New(msg))
}

func (c *Conn) recordTrafficBudgetLocked(now time.Time, windowStart *windowStamp, frames *uint32, bytes *uint64, frameBudget uint32, byteBudget uint64, payloadLen uint64, op string, msg string) error {
	if c == nil || windowStart == nil || frames == nil || bytes == nil {
		return nil
	}
	window := c.abuseWindowLocked()
	if *windowStart == 0 || windowStampElapsed(now, *windowStart) > window {
		*windowStart = windowStampAt(now)
		*frames = 0
		*bytes = 0
	}
	*frames = saturatingInc32(*frames)
	*bytes = saturatingAdd(*bytes, payloadLen)
	if (frameBudget == 0 || *frames <= frameBudget) && (byteBudget == 0 || *bytes <= byteBudget) {
		return nil
	}
	return wireError(CodeProtocol, op, errors.New(msg))
}

func saturatingInc32(v uint32) uint32 {
	if v == ^uint32(0) {
		return v
	}
	return v + 1
}

func (c *Conn) inboundControlByteBudgetLocked() uint64 {
	if c == nil {
		maxPayload := DefaultSettings().MaxControlPayloadBytes
		budget := saturatingMul(maxPayload, 64)
		if budget < minInboundControlByteBudget {
			return minInboundControlByteBudget
		}
		return budget
	}
	if c.abuse.controlByteBudget > 0 {
		return c.abuse.controlByteBudget
	}
	maxPayload := c.config.local.Settings.MaxControlPayloadBytes
	if maxPayload == 0 {
		maxPayload = DefaultSettings().MaxControlPayloadBytes
	}
	budget := saturatingMul(maxPayload, 64)
	if budget < minInboundControlByteBudget {
		return minInboundControlByteBudget
	}
	return budget
}

func (c *Conn) inboundMixedFrameBudgetLocked() uint32 {
	if c != nil && c.abuse.mixedFrameBudget > 0 {
		return c.abuse.mixedFrameBudget
	}
	control := c.inboundControlFrameBudgetLocked()
	ext := c.inboundExtFrameBudgetLocked()
	if control >= ext {
		return control
	}
	return ext
}

func (c *Conn) inboundMixedByteBudgetLocked() uint64 {
	if c == nil {
		controlMaxPayload := DefaultSettings().MaxControlPayloadBytes
		control := saturatingMul(controlMaxPayload, 64)
		if control < minInboundControlByteBudget {
			control = minInboundControlByteBudget
		}
		extMaxPayload := DefaultSettings().MaxExtensionPayloadBytes
		ext := saturatingMul(extMaxPayload, 64)
		if ext < minInboundExtByteBudget {
			ext = minInboundExtByteBudget
		}
		if control >= ext {
			return control
		}
		return ext
	}
	if c.abuse.mixedByteBudget > 0 {
		return c.abuse.mixedByteBudget
	}
	control := c.inboundControlByteBudgetLocked()
	ext := c.inboundExtByteBudgetLocked()
	if control >= ext {
		return control
	}
	return ext
}

func (c *Conn) inboundExtByteBudgetLocked() uint64 {
	if c == nil {
		maxPayload := DefaultSettings().MaxExtensionPayloadBytes
		budget := saturatingMul(maxPayload, 64)
		if budget < minInboundExtByteBudget {
			return minInboundExtByteBudget
		}
		return budget
	}
	if c.abuse.extByteBudget > 0 {
		return c.abuse.extByteBudget
	}
	maxPayload := c.config.local.Settings.MaxExtensionPayloadBytes
	if maxPayload == 0 {
		maxPayload = DefaultSettings().MaxExtensionPayloadBytes
	}
	budget := saturatingMul(maxPayload, 64)
	if budget < minInboundExtByteBudget {
		return minInboundExtByteBudget
	}
	return budget
}

func (c *Conn) shouldRecordGroupRebucketChurnLocked(stream *nativeStream, oldGroup uint64, oldExplicit bool) bool {
	if c == nil || stream == nil {
		return false
	}
	if c.config.peer.Settings.SchedulerHints != SchedulerGroupFair {
		return false
	}
	if !c.config.negotiated.Capabilities.Has(CapabilityStreamGroups) {
		return false
	}
	if !stream.localSend || state.SendTerminal(stream.effectiveSendHalfStateLocked()) {
		return false
	}
	return stream.group != oldGroup || stream.groupExplicit != oldExplicit
}
