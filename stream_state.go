package zmux

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// StreamMetadata exposes the repository-default advisory metadata currently
// known for a stream.
//
// Priority defaults to 0. Group is nil when the stream has no explicit group
// assignment. OpenInfo is the opener's opaque byte string when present.
type StreamMetadata struct {
	Priority uint64
	Group    *uint64
	OpenInfo []byte
}

// MetadataUpdate carries post-open advisory metadata updates for a stream.
//
// Only Priority and Group have standardized update semantics in zmux v1.
// Open-time opaque metadata remains carried only through OPEN_METADATA.
type MetadataUpdate struct {
	Priority *uint64
	Group    *uint64
}

const (
	streamPendingMaxData uint8 = 1 << iota
	streamPendingBlocked
	streamPendingPriorityUpdate
)

type streamRuntimeStateMask uint8

const (
	streamRuntimeBlocked streamRuntimeStateMask = 1 << iota
	streamRuntimePriority
	streamRuntimeRecvFlow
)

const (
	streamRuntimeSendPending = streamRuntimeBlocked | streamRuntimePriority
	streamRuntimeAllPending  = streamRuntimeSendPending | streamRuntimeRecvFlow
)

func (s *Stream) localSendActionErrLocked(action state.LocalSendAction) error {
	switch action {
	case state.LocalSendActionNotWritable:
		return ErrStreamNotWritable
	case state.LocalSendActionClosed:
		return ErrWriteClosed
	case state.LocalSendActionTerminal:
		return s.terminalErrLocked()
	default:
		return nil
	}
}

func (s *Stream) localRecvActionErrLocked(action state.LocalRecvAction) error {
	switch action {
	case state.LocalRecvActionNotReadable:
		return ErrStreamNotReadable
	case state.LocalRecvActionClosed:
		return ErrReadClosed
	case state.LocalRecvActionTerminal:
		return s.terminalErrLocked()
	default:
		return nil
	}
}

func (s *Stream) readErrLocked() error {
	if s == nil {
		return nil
	}
	if !s.localReceive {
		return ErrStreamNotReadable
	}
	choice := state.ReadErrorChoice(s.localReceive, s.localReadStop, s.effectiveRecvHalfStateLocked())
	switch choice {
	case state.TerminalErrorRecvAbort:
		return s.recvAbortErrLocked()
	case state.TerminalErrorRecvReset:
		return s.recvResetErrLocked()
	case state.TerminalErrorRecvClosed:
		if s.readStopSentLocked() {
			return ErrReadClosed
		}
		if s.effectiveRecvHalfStateLocked() == state.RecvHalfFin {
			return io.EOF
		}
		return ErrReadClosed
	default:
		return nil
	}
}

func (s *Stream) readClosedTerminationLocked() (Source, TerminationKind) {
	if s == nil {
		return SourceLocal, TerminationStopped
	}
	if s.readStopSentLocked() && state.ReadErrorChoice(s.localReceive, s.localReadStop, s.effectiveRecvHalfStateLocked()) == state.TerminalErrorRecvClosed {
		return SourceLocal, TerminationStopped
	}
	switch s.effectiveRecvHalfStateLocked() {
	case state.RecvHalfFin:
		return SourceRemote, TerminationGraceful
	case state.RecvHalfStopSent:
		return SourceLocal, TerminationStopped
	default:
		return SourceLocal, TerminationStopped
	}
}

type streamTerminationMeta struct {
	source    Source
	direction Direction
	kind      TerminationKind
	present   bool
}

func makeStreamTerminationMeta(source Source, direction Direction, kind TerminationKind) streamTerminationMeta {
	return streamTerminationMeta{
		source:    source,
		direction: direction,
		kind:      kind,
		present:   true,
	}
}

func (m streamTerminationMeta) ready() bool {
	return m.present
}

func (s *Stream) readTerminationMetaLocked() streamTerminationMeta {
	if s == nil {
		return streamTerminationMeta{}
	}
	switch state.ReadErrorChoice(s.localReceive, s.localReadStop, s.effectiveRecvHalfStateLocked()) {
	case state.TerminalErrorRecvAbort:
		if s.recvAbortFromPeerLocked() {
			return makeStreamTerminationMeta(SourceRemote, DirectionBoth, TerminationAbort)
		}
		return makeStreamTerminationMeta(SourceLocal, DirectionBoth, TerminationAbort)
	case state.TerminalErrorRecvReset:
		return makeStreamTerminationMeta(SourceRemote, DirectionRead, TerminationReset)
	case state.TerminalErrorRecvClosed:
		source, kind := s.readClosedTerminationLocked()
		return makeStreamTerminationMeta(source, DirectionRead, kind)
	default:
		return streamTerminationMeta{}
	}
}

func (s *Stream) writeTerminationMetaLocked(err error) streamTerminationMeta {
	if s == nil || err == nil {
		return streamTerminationMeta{}
	}
	if errors.Is(err, ErrWriteClosed) {
		source, kind := s.writeClosedTerminationLocked()
		return makeStreamTerminationMeta(source, DirectionWrite, kind)
	}
	return s.abortOrResetTerminationLocked(err)
}

func (s *Stream) closeTerminationMetaLocked(err error) streamTerminationMeta {
	if s == nil || err == nil {
		return streamTerminationMeta{}
	}
	switch {
	case errors.Is(err, ErrReadClosed):
		source, kind := s.readClosedTerminationLocked()
		return makeStreamTerminationMeta(source, DirectionRead, kind)
	case errors.Is(err, ErrWriteClosed):
		source, kind := s.writeClosedTerminationLocked()
		return makeStreamTerminationMeta(source, DirectionWrite, kind)
	default:
		return s.abortOrResetTerminationLocked(err)
	}
}

func (s *Stream) writeClosedTerminationLocked() (Source, TerminationKind) {
	if s == nil {
		return SourceLocal, TerminationStopped
	}
	switch s.effectiveSendHalfStateLocked() {
	case state.SendHalfStopSeen:
		return SourceRemote, TerminationStopped
	case state.SendHalfReset:
		return SourceLocal, TerminationReset
	case state.SendHalfFin:
		return SourceLocal, TerminationGraceful
	default:
		return SourceLocal, TerminationStopped
	}
}

type writeAdmissionPolicy uint8

const (
	writeAdmissionStrict writeAdmissionPolicy = iota
	writeAdmissionTerminalChunk
)

func (p writeAdmissionPolicy) allowsStopSeenFinal() bool {
	return p == writeAdmissionTerminalChunk
}

func (s *Stream) writeAdmissionRawLocked(policy writeAdmissionPolicy) (state.SendHalfState, error) {
	if s == nil {
		return state.SendHalfAbsent, nil
	}
	sendHalf := s.effectiveSendHalfStateLocked()
	switch sendHalf {
	case state.SendHalfAborted, state.SendHalfReset:
		return sendHalf, s.terminalErrLocked()
	case state.SendHalfFin:
		return sendHalf, ErrWriteClosed
	case state.SendHalfStopSeen:
		if !policy.allowsStopSeenFinal() {
			return sendHalf, ErrWriteClosed
		}
	default:
	}
	return sendHalf, nil
}

func (s *Stream) writeAdmissionStateLocked(policy writeAdmissionPolicy) (state.SendHalfState, error) {
	sendHalf, err := s.writeAdmissionRawLocked(policy)
	if err != nil {
		return sendHalf, s.writeSurfaceErrLocked(err)
	}
	return sendHalf, nil
}

func (s *Stream) metadataUpdateErrLocked() error {
	if s == nil {
		return ErrSessionClosed
	}
	if _, err := s.writeAdmissionRawLocked(writeAdmissionStrict); err != nil {
		return err
	}
	if state.FullyTerminal(s.localSend, s.localReceive, s.effectiveSendHalfStateLocked(), s.effectiveRecvHalfStateLocked()) {
		if err := s.terminalErrLocked(); err != nil {
			return err
		}
		return ErrSessionClosed
	}
	return nil
}

type peerDataArrival uint8

const (
	peerDataArrivalContinue peerDataArrival = iota
	peerDataArrivalFinal
)

func (a peerDataArrival) hasFIN() bool {
	return a == peerDataArrivalFinal
}

func (s *Stream) peerDataPlanLocked(arrival peerDataArrival) state.PeerDataPlan {
	if s == nil {
		return state.PeerDataPlan{Outcome: state.PeerDataIgnore}
	}
	recvHalf := s.effectiveRecvHalfStateLocked()
	// Local read-stop keeps late peer DATA on the discard path even after a
	// trailing FIN converges the explicit recv half to recv_fin.
	if s.readStopSentLocked() && recvHalf == state.RecvHalfFin {
		recvHalf = state.RecvHalfStopSent
	}
	return state.PeerDataTransition(
		s.localSend,
		s.localReceive,
		s.effectiveSendHalfStateLocked(),
		recvHalf,
		arrival.hasFIN(),
	)
}

func (s *Stream) shouldReclaimUnseenLocalLocked(peerGoAwayBidi, peerGoAwayUni uint64) bool {
	if s == nil {
		return false
	}
	return state.ShouldReclaimUnseenLocalStream(
		s.visibilityPhaseLocked(),
		s.bidi,
		s.id,
		peerGoAwayBidi,
		peerGoAwayUni,
		s.localSend,
		s.localReceive,
		s.effectiveSendHalfStateLocked(),
		s.effectiveRecvHalfStateLocked(),
	)
}

func (s *Stream) shouldFinalizePeerActiveLocked() bool {
	if s == nil {
		return false
	}
	return state.ShouldFinalizePeerActive(
		s.activeCountedFlag(),
		s.isLocalOpenedLocked(),
		s.localSend,
		s.localReceive,
		s.effectiveSendHalfStateLocked(),
		s.effectiveRecvHalfStateLocked(),
	)
}

func (s *Stream) recvAbortiveLocked() bool {
	switch s.effectiveRecvHalfStateLocked() {
	case state.RecvHalfReset, state.RecvHalfAborted:
		return true
	default:
		return false
	}
}

func (s *Stream) stopSeenLocked() bool {
	return s != nil && s.effectiveSendHalfStateLocked() == state.SendHalfStopSeen
}

func (s *Stream) stopSeenWriteFinalBurstEligibleLocked(totalRemaining, currentRemaining int) bool {
	return s.stopSeenLocked() && totalRemaining == currentRemaining
}

func (s *Stream) stopSeenWriteFinalNeedsImmediateCompletionLocked(totalRemaining, frameCap, availableSession, availableStream uint64) bool {
	if !s.stopSeenLocked() {
		return false
	}
	return frameCap < totalRemaining || availableSession < totalRemaining || availableStream < totalRemaining
}

func (s *Stream) allowsTerminalWriteRequestLocked(req *writeRequest) bool {
	if s == nil {
		return true
	}
	sendHalf := s.effectiveSendHalfStateLocked()
	if sendHalf == state.SendHalfAbsent {
		return true
	}
	return req != nil && req.allowsTerminalSendHalf(sendHalf, s.id)
}

func (s *Stream) allowsQueuedGracefulFinDrainLocked(req *writeRequest) bool {
	if s == nil {
		return false
	}
	return req != nil && req.allowsQueuedGracefulFinDrainForStream(s.id)
}

func (s *Stream) allowsCloseWriteNoOpAfterStopResetLocked() bool {
	if s == nil {
		return false
	}
	sendHalf := s.effectiveSendHalfStateLocked()
	if sendHalf == state.SendHalfFin && s.sendStop != nil {
		return true
	}
	if !s.sendResetFromStopLocked() {
		return false
	}
	if sendHalf != state.SendHalfReset {
		return false
	}
	return s.sendStop != nil
}

func (s *Stream) suppressWriteRequestErrLocked(req *writeRequest) error {
	if s == nil || !s.localSend {
		return nil
	}
	sendHalf := s.effectiveSendHalfStateLocked()
	if sendHalf != state.SendHalfStopSeen && !state.SendTerminal(sendHalf) {
		return nil
	}
	if req != nil && req.terminalPolicy.allowsTerminal() && s.allowsTerminalWriteRequestLocked(req) {
		return nil
	}
	if sendHalf == state.SendHalfFin && s.allowsQueuedGracefulFinDrainLocked(req) {
		return nil
	}
	if _, err := s.writeAdmissionRawLocked(writeAdmissionStrict); err != nil {
		return err
	}
	return s.terminalErrLocked()
}

func (s *Stream) ignoreLateNonOpeningControlLocked() bool {
	if s == nil {
		return false
	}
	return state.IgnoreLateNonOpeningControl(
		s.localSend,
		s.localReceive,
		s.effectiveSendHalfStateLocked(),
		s.effectiveRecvHalfStateLocked(),
	)
}

func (s *Stream) abortOrResetTerminationLocked(err error) streamTerminationMeta {
	switch {
	case s.sendAbortErrLocked() != nil && errors.Is(err, s.sendAbortErrLocked()):
		if s.sendAbortFromPeerLocked() {
			return makeStreamTerminationMeta(SourceRemote, DirectionBoth, TerminationAbort)
		}
		return makeStreamTerminationMeta(SourceLocal, DirectionBoth, TerminationAbort)
	case s.recvAbortErrLocked() != nil && errors.Is(err, s.recvAbortErrLocked()):
		if s.recvAbortFromPeerLocked() {
			return makeStreamTerminationMeta(SourceRemote, DirectionBoth, TerminationAbort)
		}
		return makeStreamTerminationMeta(SourceLocal, DirectionBoth, TerminationAbort)
	case s.sendResetErrLocked() != nil && errors.Is(err, s.sendResetErrLocked()):
		return makeStreamTerminationMeta(SourceLocal, DirectionWrite, TerminationReset)
	case s.recvResetErrLocked() != nil && errors.Is(err, s.recvResetErrLocked()):
		return makeStreamTerminationMeta(SourceRemote, DirectionRead, TerminationReset)
	default:
		return streamTerminationMeta{}
	}
}

func (s *Stream) clearOpeningBarrierLocked() {
	if s == nil {
		return
	}
	if s.localOpen.phase == state.LocalOpenPhaseQueued {
		s.localOpen.phase = state.LocalOpenPhaseNone
	}
}

func (s *Stream) isLocalOpenedLocked() bool {
	return s != nil && s.visibilityPhaseLocked().IsLocal()
}

func (s *Stream) isSendCommittedLocked() bool {
	return s != nil && s.localOpen.committed
}

func (s *Stream) isPeerVisibleLocked() bool {
	return s != nil && s.visibilityPhaseLocked() == state.LocalOpenPhasePeerVisible
}

func (s *Stream) markSendCommittedLocked() {
	if s == nil {
		return
	}
	s.localOpen.committed = true
}

func (s *Stream) markOpenerQueuedLocked() {
	if s == nil || s.localOpen.phase == state.LocalOpenPhasePeerVisible {
		return
	}
	s.localOpen.phase = state.LocalOpenPhaseQueued
}

func (s *Stream) setPeerVisibleLocked() {
	if s == nil {
		return
	}
	s.localOpen.phase = state.LocalOpenPhasePeerVisible
}

func (s *Stream) localOpenVisibilityLocked() state.LocalOpenVisibility {
	if s == nil {
		return state.LocalOpenVisibility{}
	}
	return state.LocalOpenVisibility{
		LocalOpened:   s.localOpen.opened,
		SendCommitted: s.localOpen.committed,
		PeerVisible:   s.localOpen.phase == state.LocalOpenPhasePeerVisible,
		OpenerQueued:  s.localOpen.phase == state.LocalOpenPhaseQueued,
	}
}

func (s *Stream) visibilityPhaseLocked() state.LocalOpenPhase {
	return s.localOpenVisibilityLocked().Phase()
}

func (s *Stream) needsLocalOpenerLocked() bool {
	return s.visibilityPhaseLocked().NeedsLocalOpener()
}

func (s *Stream) awaitingPeerVisibilityLocked() bool {
	return s.visibilityPhaseLocked().AwaitingPeerVisibility()
}

func (s *Stream) shouldEmitOpenerFrameLocked() bool {
	return s.visibilityPhaseLocked().ShouldEmitOpenerFrame()
}

func (s *Stream) shouldMarkPeerVisibleLocked() bool {
	return s.visibilityPhaseLocked().ShouldMarkPeerVisible()
}

func (s *Stream) shouldQueueStreamBlockedLocked(availableStream uint64) bool {
	return s.visibilityPhaseLocked().ShouldQueueStreamBlocked(availableStream)
}

func (s *Stream) readStopSentLocked() bool {
	return (s != nil && s.localReadStop) || state.ReadStopped(s.effectiveRecvHalfStateLocked())
}

func (s *Stream) sendResetMatchesCodeLocked(code uint64) bool {
	resetCode := s.sendResetCodeLocked()
	return s != nil &&
		s.effectiveSendHalfStateLocked() == state.SendHalfReset &&
		resetCode != nil &&
		*resetCode == code
}

type terminalTrackingState uint8

const (
	terminalTrackingReleased terminalTrackingState = iota
	terminalTrackingRetained
)

func terminalTrackingStateFrom(stillTracked bool) terminalTrackingState {
	if stillTracked {
		return terminalTrackingRetained
	}
	return terminalTrackingReleased
}

func (t terminalTrackingState) stillTracked() bool {
	return t == terminalTrackingRetained
}

func (s *Stream) shouldCompactTerminalLocked(tracking terminalTrackingState) bool {
	if s == nil {
		return false
	}
	if s.queuedDataBytes != 0 || s.inflightQueued != 0 {
		return false
	}
	return state.ShouldCompactTerminal(
		s.idSet,
		state.FullyTerminal(s.localSend, s.localReceive, s.effectiveSendHalfStateLocked(), s.effectiveRecvHalfStateLocked()),
		s.recvBuffer,
		s.bufferedReadLenLocked(),
		tracking.stillTracked(),
	)
}

func (s *Stream) tombstoneLateDataActionLocked() lateDataAction {
	if s == nil {
		return lateDataIgnore
	}
	return state.TombstoneLateDataAction(s.localReceive, s.effectiveRecvHalfStateLocked())
}

func (s *Stream) tombstoneStateLocked() state.StreamTombstone {
	if s == nil {
		return state.StreamTombstone{DataAction: lateDataIgnore}
	}
	return state.BuildStreamTombstone(
		s.localReceive,
		s.effectiveSendHalfStateLocked(),
		s.effectiveRecvHalfStateLocked(),
		s.sendResetCodeLocked(),
		s.sendAbortCodeLocked(),
		s.recvResetCodeLocked(),
		s.recvAbortCodeLocked(),
	)
}

func (s *Stream) lateDataCauseLocked() lateDataCause {
	if s == nil {
		return lateDataCauseNone
	}
	switch s.effectiveRecvHalfStateLocked() {
	case state.RecvHalfStopSent:
		return lateDataCauseCloseRead
	case state.RecvHalfReset:
		return lateDataCauseReset
	case state.RecvHalfAborted:
		return lateDataCauseAbort
	default:
		return lateDataCauseNone
	}
}

func (s *Stream) terminalErrLocked() error {
	switch state.TerminalErrorPriority(s.effectiveSendHalfStateLocked(), s.effectiveRecvHalfStateLocked()) {
	case state.TerminalErrorSendAbort:
		return s.sendAbortErrLocked()
	case state.TerminalErrorRecvAbort:
		return s.recvAbortErrLocked()
	case state.TerminalErrorSendReset:
		return s.sendResetErrLocked()
	case state.TerminalErrorRecvReset:
		return s.recvResetErrLocked()
	case state.TerminalErrorSendClosed:
		return ErrWriteClosed
	case state.TerminalErrorRecvClosed:
		return ErrReadClosed
	default:
		return nil
	}
}

func (c *Conn) releaseStreamRetainedStateLocked(stream *Stream) {
	if c == nil || stream == nil {
		return
	}
	if len(stream.openInfo) > 0 {
		prevTracked := c.trackedSessionMemoryLocked()
		c.retention.retainedOpenInfoBytes = csub(c.retention.retainedOpenInfoBytes, uint64(len(stream.openInfo)))
		stream.openInfo = nil
		c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleased)
	}
	c.releaseStreamOpenMetadataPrefixLocked(stream)
	c.releaseStreamPeerReasonBudgetLocked(stream)
	c.releaseStreamRuntimeStateLocked(stream, streamRuntimeAllPending)
}

type schedulerReleasePolicy uint8

const schedulerReleaseDrop schedulerReleasePolicy = 1

type transientStreamReleaseOptions struct {
	queuedWake queuedDataWakePolicy
	scheduler  schedulerReleasePolicy
	send       bool
	receive    streamReceiveReleaseMode
}

func (c *Conn) releaseTerminalStreamStateLocked(stream *Stream, opts transientStreamReleaseOptions) {
	if c == nil || stream == nil {
		return
	}
	if opts.send {
		c.releaseSendLocked(stream)
	}
	if opts.receive != streamReceiveRetain {
		c.applyReceiveReleasePlanLocked(stream, opts.receive)
	}
	if opts.scheduler == schedulerReleaseDrop {
		c.dropWriteBatchStateLocked(stream)
	}
	c.releaseQueuedDataStreamStateLocked(stream, opts.queuedWake)
	c.releaseStreamRetainedStateLocked(stream)
}

func (c *Conn) finalizeTerminalStreamLocked(stream *Stream, releaseOpts transientStreamReleaseOptions, notifyMask streamNotifyMask, finalizePeer bool) {
	if c == nil || stream == nil {
		return
	}
	c.releaseTerminalStreamStateLocked(stream, releaseOpts)
	notifyStreamLocked(stream, notifyMask)
	if finalizePeer {
		c.maybeFinalizePeerActiveLocked(stream)
	}
}

func (u MetadataUpdate) empty() bool {
	return u.Priority == nil && u.Group == nil
}

type metadataUpdateRoute uint8

const (
	metadataUpdateRoutePriorityFrame metadataUpdateRoute = iota
	metadataUpdateRouteOpenMetadata
)

func (r metadataUpdateRoute) usesOpenMetadata() bool {
	return r == metadataUpdateRouteOpenMetadata
}

type optionalUint64 struct {
	value   uint64
	present bool
}

func maybeOptionalUint64(value uint64, present bool) optionalUint64 {
	return optionalUint64{value: value, present: present}
}

func (o optionalUint64) ptr() *uint64 {
	if !o.present {
		return nil
	}
	return boxedUint64(o.value)
}

func wrapMetadataUpdateStructuredError(err error) error {
	return wrapStructuredError(err, errorMeta{
		scope:     ScopeStream,
		operation: OperationWrite,
		source:    SourceLocal,
		direction: DirectionWrite,
	})
}

func (s *Stream) Metadata() StreamMetadata {
	if s == nil || s.conn == nil {
		return StreamMetadata{}
	}
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	var group *uint64
	if s.groupExplicit {
		group = boxedUint64(s.group)
	}
	openInfo := append([]byte(nil), s.openInfo...)
	return StreamMetadata{
		Priority: s.priority,
		Group:    group,
		OpenInfo: openInfo,
	}
}

func (s *Stream) metadataUpdateRouteLocked(caps Capabilities, update MetadataUpdate) (metadataUpdateRoute, error) {
	if s == nil {
		return metadataUpdateRoutePriorityFrame, nil
	}
	phase := s.visibilityPhaseLocked()
	if phase.NeedsLocalOpener() {
		return metadataUpdateRouteOpenMetadata, validateOpenMetadataUpdateCapability(caps, update)
	}
	if phase.ShouldEmitOpenerFrame() && validateOpenMetadataUpdateCapability(caps, update) == nil {
		return metadataUpdateRouteOpenMetadata, nil
	}
	return metadataUpdateRoutePriorityFrame, nil
}

func (s *Stream) applyOpenMetadataUpdateLocked(caps Capabilities, update MetadataUpdate) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}

	nextInitialPriority := s.initialPriority
	nextInitialPrioritySet := s.initialPrioritySet
	nextPriority := s.priority
	if update.Priority != nil {
		nextInitialPriority = *update.Priority
		nextInitialPrioritySet = true
		nextPriority = nextInitialPriority
	}
	nextInitialGroup := s.initialGroup
	nextInitialGroupSet := s.initialGroupSet
	nextGroup := s.group
	nextGroupExplicit := s.groupExplicit
	if update.Group != nil {
		nextInitialGroup = *update.Group
		nextInitialGroupSet = true
		nextGroup = nextInitialGroup
		nextGroupExplicit = nextInitialGroup != 0
	}
	prefix, err := appendOpenMetadataPrefix(
		s.openMetadataPrefix[:0],
		caps,
		maybeOptionalUint64(nextInitialPriority, nextInitialPrioritySet).ptr(),
		maybeOptionalUint64(nextInitialGroup, nextInitialGroupSet).ptr(),
		s.openInfo,
		s.conn.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		return err
	}
	s.openMetadataPrefix = storeOpenMetadataPrefixBytes(s.openMetadataPrefix, prefix, retainedBytesOwned)
	if update.Priority != nil {
		s.initialPriority = nextInitialPriority
		s.initialPrioritySet = nextInitialPrioritySet
		s.priority = nextPriority
	}
	if update.Group != nil {
		s.initialGroup = nextInitialGroup
		s.initialGroupSet = nextInitialGroupSet
		s.conn.setStreamGroupLocked(s, nextGroup, nextGroupExplicit)
	}
	return nil
}

func (s *Stream) queuePendingMetadataUpdateLocked(caps Capabilities, update MetadataUpdate) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	reuse := []byte(nil)
	if s.hasPendingPriorityUpdateLocked() {
		reuse = s.pending.priority[:0]
	}
	payload, err := appendPriorityUpdatePayload(
		reuse,
		caps,
		update,
		s.conn.config.peer.Settings.MaxExtensionPayloadBytes,
	)
	if err != nil {
		return err
	}
	s.conn.queuePriorityUpdateAsync(s.id, payload, retainedBytesOwned)
	if update.Priority != nil {
		s.priority = *update.Priority
	}
	if update.Group != nil {
		s.conn.setStreamGroupLocked(s, *update.Group, *update.Group != 0)
	}
	return nil
}

func (s *Stream) UpdateMetadata(update MetadataUpdate) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	if update.empty() {
		return wrapMetadataUpdateStructuredError(ErrEmptyMetadataUpdate)
	}

	s.conn.mu.Lock()
	if s.conn.lifecycle.closeErr != nil {
		err := sessionOperationErrLocked(s.conn, OperationWrite, visibleSessionErrLocked(s.conn, s.conn.lifecycle.closeErr))
		s.conn.mu.Unlock()
		return err
	}
	if err := s.metadataUpdateErrLocked(); err != nil {
		err = s.writeSurfaceErrLocked(err)
		s.conn.mu.Unlock()
		return err
	}

	caps := s.conn.config.negotiated.Capabilities
	route, err := s.metadataUpdateRouteLocked(caps, update)
	if err != nil {
		s.conn.mu.Unlock()
		return wrapMetadataUpdateStructuredError(err)
	}
	if route.usesOpenMetadata() {
		err = s.applyOpenMetadataUpdateLocked(caps, update)
	} else {
		err = s.queuePendingMetadataUpdateLocked(caps, update)
	}
	s.conn.mu.Unlock()
	if err != nil {
		return wrapMetadataUpdateStructuredError(err)
	}
	return nil
}

func validateOpenMetadataUpdateCapability(caps Capabilities, update MetadataUpdate) error {
	if update.Priority != nil && !caps.CanCarryPriorityOnOpen() {
		return ErrPriorityUpdateUnavailable
	}
	if update.Group != nil && !caps.CanCarryGroupOnOpen() {
		return ErrPriorityUpdateUnavailable
	}
	return nil
}

func (c *Conn) releaseStreamRuntimeStateLocked(stream *Stream, mask streamRuntimeStateMask) bool {
	if c == nil || stream == nil || mask == 0 {
		return false
	}
	if mask&streamRuntimeBlocked != 0 {
		stream.clearBlockedState()
		stream.clearPendingControlValueLocked(streamControlBlocked)
	}
	if mask&streamRuntimePriority != 0 {
		stream.clearPendingPriorityUpdateLocked()
	}
	if mask&streamRuntimeRecvFlow != 0 {
		stream.clearPendingControlValueLocked(streamControlMaxData)
	}
	if !stream.idSet {
		return false
	}
	prevTracked := c.trackedSessionMemoryLocked()
	released := false
	if mask&streamRuntimeBlocked != 0 {
		if c.dropPendingStreamControlEntryLocked(streamControlBlocked, stream.id) {
			released = true
		}
	}
	if mask&streamRuntimePriority != 0 {
		if c.dropPendingPriorityUpdateEntryLocked(stream.id) {
			released = true
		}
	}
	if mask&streamRuntimeRecvFlow != 0 {
		if c.dropPendingStreamControlEntryLocked(streamControlMaxData, stream.id) {
			released = true
		}
	}
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
	return released
}

func (c *Conn) lookupExistingPeerStreamLocked(frameName string, streamID uint64) (*Stream, error) {
	if c == nil {
		return nil, nil
	}
	if stream := c.registry.streams[streamID]; stream != nil {
		return stream, nil
	}
	if c.hasTerminalMarkerLocked(streamID) {
		return nil, nil
	}
	return nil, wireError(
		CodeProtocol,
		"handle "+frameName,
		fmt.Errorf("%s on unknown stream %d", frameName, streamID),
	)
}

func (c *Conn) newPeerStreamLocked(id uint64) *Stream {
	localSend, localRecv := state.StreamKindForLocal(c.config.negotiated.LocalRole, id)
	stream := &Stream{
		conn:             c,
		id:               id,
		idSet:            true,
		bidi:             state.StreamIsBidi(id),
		localOpen:        streamLocalOpenState{},
		pending:          newStreamPendingState(),
		localSend:        localSend,
		localReceive:     localRecv,
		sendMax:          state.InitialSendWindow(c.config.negotiated.LocalRole, c.config.peer.Settings, id),
		recvAdvertised:   state.InitialReceiveWindow(c.config.negotiated.LocalRole, c.config.local.Settings, id),
		provisionalIndex: invalidStreamQueueIndex,
		acceptIndex:      invalidStreamQueueIndex,
		unseenLocalIndex: invalidStreamQueueIndex,
	}
	stream.initHalfStates()
	return stream
}

type peerStreamVisibility uint8

const (
	peerStreamHidden peerStreamVisibility = iota
	peerStreamVisibleOnData
)

func (v peerStreamVisibility) isVisibleOnData() bool {
	return v == peerStreamVisibleOnData
}

func (c *Conn) refusePeerStreamLocked(streamID uint64, stream *Stream, visibility peerStreamVisibility) error {
	if c == nil {
		return nil
	}
	if !visibility.isVisibleOnData() {
		c.noteHiddenStreamRefusedLocked()
	}
	c.noteAbortReasonLocked(uint64(CodeRefusedStream))
	if stream != nil {
		stream.setAbortedWithSource(refusedStreamAppErr(), terminalAbortLocal)
		notify(stream.readNotify)
		notify(stream.writeNotify)
		c.maybeCompactTerminalLocked(stream)
	}
	c.mu.Unlock()
	err := c.abortWithCode(streamID, CodeRefusedStream)
	c.mu.Lock()
	return err
}

func (c *Conn) openPeerStreamLocked(streamID uint64, visibility peerStreamVisibility) (*Stream, error) {
	if err := state.ValidateStreamIDForRole(c.config.negotiated.LocalRole, streamID); err != nil {
		return nil, wireError(CodeProtocol, "open peer stream", err)
	}
	if state.StreamIsLocal(c.config.negotiated.LocalRole, streamID) {
		return nil, wireError(CodeProtocol, "open peer stream", fmt.Errorf("peer used local-owned stream id %d", streamID))
	}
	if state.PeerOpenRefusedByGoAway(streamID, c.sessionControl.localGoAwayBidi, c.sessionControl.localGoAwayUni) {
		if err := c.refusePeerStreamLocked(streamID, nil, visibility); err != nil {
			return nil, err
		}
		return nil, nil
	}
	expected := state.ExpectedNextPeerStreamID(streamID, c.registry.nextPeerBidi, c.registry.nextPeerUni)
	if streamID != expected {
		return nil, wireError(CodeProtocol, "open peer stream", fmt.Errorf("unexpected peer stream id %d, want %d", streamID, expected))
	}

	stream := c.newPeerStreamLocked(streamID)
	c.storeLiveStreamLocked(stream)
	if state.StreamIsBidi(streamID) {
		c.registry.nextPeerBidi += 4
	} else {
		c.registry.nextPeerUni += 4
	}

	if stream.bidi {
		if !state.PeerStreamWithinLimit(true, c.registry.activePeerBidi, c.registry.activePeerUni, c.config.local.Settings.MaxIncomingStreamsBidi, c.config.local.Settings.MaxIncomingStreamsUni) {
			if err := c.refusePeerStreamLocked(streamID, stream, visibility); err != nil {
				return nil, err
			}
			return nil, nil
		}
		c.registry.activePeerBidi++
		stream.markActiveCounted()
	} else {
		if !state.PeerStreamWithinLimit(false, c.registry.activePeerBidi, c.registry.activePeerUni, c.config.local.Settings.MaxIncomingStreamsBidi, c.config.local.Settings.MaxIncomingStreamsUni) {
			if err := c.refusePeerStreamLocked(streamID, stream, visibility); err != nil {
				return nil, err
			}
			return nil, nil
		}
		c.registry.activePeerUni++
		stream.markActiveCounted()
	}

	if !visibility.isVisibleOnData() {
		stream.applicationVisible = false
	}
	return stream, nil
}

type receivedMetadataCarriage uint8

const (
	receivedMetadataOnUpdate receivedMetadataCarriage = iota
	receivedMetadataOnOpen
)

func (m receivedMetadataCarriage) allowsOpenInfo() bool {
	return m == receivedMetadataOnOpen
}

type receivedMetadataPolicy struct {
	allowPriority        bool
	allowGroup           bool
	allowOpenInfoPayload bool
}

func (c *Conn) receivedMetadataPolicyLocked(carriage receivedMetadataCarriage) receivedMetadataPolicy {
	if c == nil {
		return receivedMetadataPolicy{}
	}
	caps := c.config.negotiated.Capabilities
	if carriage.allowsOpenInfo() {
		return receivedMetadataPolicy{
			allowPriority:        caps.CanCarryPriorityOnOpen(),
			allowGroup:           caps.CanCarryGroupOnOpen(),
			allowOpenInfoPayload: caps.CanCarryOpenInfo(),
		}
	}
	return receivedMetadataPolicy{
		allowPriority: caps.CanCarryPriorityInUpdate(),
		allowGroup:    caps.CanCarryGroupInUpdate(),
	}
}

func (c *Conn) applyReceivedMetadataLocked(stream *Stream, meta streamMetadata, carriage receivedMetadataCarriage) bool {
	if stream == nil {
		return false
	}
	policy := c.receivedMetadataPolicyLocked(carriage)
	changed := false
	if policy.allowPriority && meta.HasPriority {
		if stream.priority != meta.Priority {
			changed = true
		}
		stream.priority = meta.Priority
	}
	if policy.allowGroup && meta.HasGroup {
		if stream.group != meta.Group || stream.groupExplicit != (meta.Group != 0) {
			changed = true
		}
		c.setStreamGroupLocked(stream, meta.Group, meta.Group != 0)
	}
	if policy.allowOpenInfoPayload && len(meta.OpenInfo) > 0 {
		if c.setStreamOpenInfoLocked(stream, meta.OpenInfo) {
			changed = true
		}
	}
	return changed
}

func (c *Conn) enqueueAcceptedLocked(stream *Stream) {
	if stream == nil || !state.ShouldEnqueueAccepted(stream.applicationVisible, stream.acceptedFlag(), stream.enqueued) {
		return
	}
	stream.enqueued = true
	c.addAcceptQueuedBytesLocked(stream, stream.recvBuffer)
	c.appendAcceptedLocked(stream)
	notify(c.signals.acceptCh)
}

func (c *Conn) markPeerVisibleLocked(stream *Stream) {
	if stream == nil || !stream.shouldMarkPeerVisibleLocked() {
		return
	}
	stream.setPeerVisibleLocked()
	c.releaseStreamOpenMetadataPrefixLocked(stream)
	c.removeUnseenLocalLocked(stream)
}

func (s *Stream) sendHalfState() state.SendHalfState {
	if s == nil {
		return state.SendHalfAbsent
	}
	return state.NormalizeSendHalfState(s.localSend, s.loadSendHalf())
}

func (s *Stream) recvHalfState() state.RecvHalfState {
	if s == nil {
		return state.RecvHalfAbsent
	}
	return state.NormalizeRecvHalfState(s.localReceive, s.loadRecvHalf())
}

func (s *Stream) effectiveSendHalfStateLocked() state.SendHalfState {
	if s == nil {
		return state.SendHalfAbsent
	}
	return state.NormalizeSendHalfState(s.localSend, s.loadSendHalf())
}

func (s *Stream) effectiveRecvHalfStateLocked() state.RecvHalfState {
	if s == nil {
		return state.RecvHalfAbsent
	}
	return state.NormalizeRecvHalfState(s.localReceive, s.loadRecvHalf())
}

func (s *Stream) sendFinReached() bool {
	return s.sendFinReachedLocked()
}

func (s *Stream) recvFinReached() bool {
	return s.recvFinReachedLocked()
}

func (s *Stream) sendFinReachedLocked() bool {
	return s != nil && s.effectiveSendHalfStateLocked() == state.SendHalfFin
}

func (s *Stream) recvFinReachedLocked() bool {
	return s != nil && s.effectiveRecvHalfStateLocked() == state.RecvHalfFin
}

func (s *Stream) loadSendHalf() state.SendHalfState {
	if s == nil {
		return state.SendHalfAbsent
	}
	return state.SendHalfState(atomic.LoadUint32((*uint32)(&s.sendHalf)))
}

func (s *Stream) storeSendHalf(v state.SendHalfState) {
	if s == nil {
		return
	}
	atomic.StoreUint32((*uint32)(&s.sendHalf), uint32(v))
}

func (s *Stream) loadRecvHalf() state.RecvHalfState {
	if s == nil {
		return state.RecvHalfAbsent
	}
	return state.RecvHalfState(atomic.LoadUint32((*uint32)(&s.recvHalf)))
}

func (s *Stream) storeRecvHalf(v state.RecvHalfState) {
	if s == nil {
		return
	}
	atomic.StoreUint32((*uint32)(&s.recvHalf), uint32(v))
}

func (s *Stream) sendAbortErrLocked() error {
	if s == nil || s.sendAbort == nil {
		return nil
	}
	return s.sendAbort
}

func (s *Stream) recvAbortErrLocked() error {
	if s == nil || s.recvAbort == nil {
		return nil
	}
	return s.recvAbort
}

func (s *Stream) sendResetErrLocked() error {
	if s == nil || s.sendReset == nil {
		return nil
	}
	return s.sendReset
}

func (s *Stream) recvResetErrLocked() error {
	if s == nil || s.recvReset == nil {
		return nil
	}
	return s.recvReset
}

func (s *Stream) sendAbortCodeLocked() *uint64 {
	if s == nil {
		return nil
	}
	return appErrorCodePtr(s.sendAbort)
}

func (s *Stream) recvAbortCodeLocked() *uint64 {
	if s == nil {
		return nil
	}
	return appErrorCodePtr(s.recvAbort)
}

func (s *Stream) sendResetCodeLocked() *uint64 {
	if s == nil {
		return nil
	}
	return appErrorCodePtr(s.sendReset)
}

func (s *Stream) recvResetCodeLocked() *uint64 {
	if s == nil {
		return nil
	}
	return appErrorCodePtr(s.recvReset)
}

type terminalAbortSource uint8

const (
	terminalAbortLocal terminalAbortSource = iota
	terminalAbortFromPeer
)

func (s *Stream) sendAbortFromPeerLocked() bool {
	return s != nil && s.sendAbort != nil && s.sendAbortSource == terminalAbortFromPeer
}

func (s *Stream) recvAbortFromPeerLocked() bool {
	return s != nil && s.recvAbort != nil && s.recvAbortSource == terminalAbortFromPeer
}

func (s *Stream) sendResetFromStopLocked() bool {
	return s != nil && s.sendReset != nil && s.sendResetSource == terminalResetFromStopSending
}

func (s *Stream) initHalfStates() {
	if s == nil {
		return
	}
	s.storeSendHalf(state.BaseSendHalfState(s.localSend))
	s.storeRecvHalf(state.BaseRecvHalfState(s.localReceive))
}

func (s *Stream) setSendStopSeen(err *ApplicationError) {
	if s == nil {
		return
	}
	s.sendStop = err
	s.storeSendHalf(state.SendHalfStopSeen)
	if s.conn != nil {
		s.conn.releaseStreamRuntimeStateLocked(s, streamRuntimeSendPending)
	}
}

func (s *Stream) setSendFin() {
	if s == nil {
		return
	}
	if s.conn != nil {
		s.conn.untrackStreamGroupLocked(s)
	}
	s.sendResetSource = terminalResetDirect
	s.storeSendHalf(state.SendHalfFin)
	if s.conn != nil {
		s.conn.releaseStreamRuntimeStateLocked(s, streamRuntimeSendPending)
	}
}

func (s *Stream) clearSendFin() {
	if s == nil {
		return
	}
	s.sendResetSource = terminalResetDirect
	if s.sendStop != nil {
		s.storeSendHalf(state.SendHalfStopSeen)
	} else {
		s.storeSendHalf(state.BaseSendHalfState(s.localSend))
	}
	if s.loadSendHalf() == state.SendHalfOpen && s.conn != nil {
		s.conn.maybeTrackStreamGroupLocked(s)
	}
}

func (s *Stream) setSendResetWithSource(err *ApplicationError, source terminalResetSource) {
	if s == nil {
		return
	}
	if s.conn != nil {
		s.conn.untrackStreamGroupLocked(s)
	}
	s.sendReset = err
	s.sendResetSource = source
	s.storeSendHalf(state.SendHalfReset)
	if s.conn != nil {
		s.conn.releaseStreamRuntimeStateLocked(s, streamRuntimeSendPending)
	}
}

func (s *Stream) setSendAbortWithSource(err *ApplicationError, source terminalAbortSource) {
	if s == nil {
		return
	}
	if s.conn != nil {
		s.conn.untrackStreamGroupLocked(s)
	}
	s.sendAbort = err
	s.sendAbortSource = source
	s.sendResetSource = terminalResetDirect
	s.storeSendHalf(state.SendHalfAborted)
	if s.conn != nil {
		s.conn.releaseStreamRuntimeStateLocked(s, streamRuntimeSendPending)
	}
}

func (s *Stream) setRecvStopSent() {
	if s == nil {
		return
	}
	s.localReadStop = true
	s.storeRecvHalf(state.RecvHalfStopSent)
	if s.conn != nil {
		s.conn.releaseStreamRuntimeStateLocked(s, streamRuntimeRecvFlow)
	}
}

func (s *Stream) setRecvFin() {
	if s == nil {
		return
	}
	s.storeRecvHalf(state.RecvHalfFin)
	if s.conn != nil {
		s.conn.releaseStreamRuntimeStateLocked(s, streamRuntimeRecvFlow)
	}
}

func (s *Stream) setRecvReset(err *ApplicationError) {
	if s == nil {
		return
	}
	s.recvReset = err
	s.storeRecvHalf(state.RecvHalfReset)
	if s.conn != nil {
		s.conn.releaseStreamRuntimeStateLocked(s, streamRuntimeRecvFlow)
	}
}

func (s *Stream) setRecvAbortWithSource(err *ApplicationError, source terminalAbortSource) {
	if s == nil {
		return
	}
	s.recvAbort = err
	s.recvAbortSource = source
	s.storeRecvHalf(state.RecvHalfAborted)
	if s.conn != nil {
		s.conn.releaseStreamRuntimeStateLocked(s, streamRuntimeRecvFlow)
	}
}

func (s *Stream) setAbortedWithSource(err *ApplicationError, source terminalAbortSource) {
	if s == nil {
		return
	}
	s.setSendAbortWithSource(err, source)
	s.setRecvAbortWithSource(err, source)
}

type streamReadChunk struct {
	data          []byte
	backing       []byte
	handle        *wire.FrameReadBufferHandle
	retainedBytes uint64
	overheadBytes uint64
	next          *streamReadChunk
}

var streamReadChunkPool = sync.Pool{
	New: func() any {
		return &streamReadChunk{}
	},
}

func acquireStreamReadChunk() *streamReadChunk {
	return streamReadChunkPool.Get().(*streamReadChunk)
}

func releaseStreamReadChunk(chunk *streamReadChunk) {
	if chunk == nil {
		return
	}
	*chunk = streamReadChunk{}
	streamReadChunkPool.Put(chunk)
}

type Stream struct {
	conn *Conn

	sendReset *ApplicationError
	sendAbort *ApplicationError
	sendStop  *ApplicationError
	recvReset *ApplicationError
	recvAbort *ApplicationError

	openMetadataPrefix []byte
	readBuf            []byte
	openInfo           []byte
	readHead           *streamReadChunk
	readTail           *streamReadChunk

	readNotify  chan struct{}
	writeNotify chan struct{}
	waitState   *streamWaitState
	provisional *streamProvisionalState

	id                     uint64
	visibilitySeq          uint64
	sendStopReasonBytes    uint64
	recvResetReasonBytes   uint64
	remoteAbortReasonBytes uint64

	sendMax         uint64
	sendSent        uint64
	queuedDataBytes uint64
	inflightQueued  uint64
	blockedAt       uint64

	initialPriority uint64
	initialGroup    uint64
	priority        uint64
	group           uint64
	trackedGroup    uint64

	recvAdvertised   uint64
	recvReceived     uint64
	recvBuffer       uint64
	recvPending      uint64
	lateDataReceived uint64

	provisionalIndex int32
	acceptIndex      int32
	unseenLocalIndex int32

	sendHalf state.SendHalfState
	recvHalf state.RecvHalfState

	lifecycleFlags uint8
	pending        streamPendingState

	idSet bool

	bidi         bool
	localOpen    streamLocalOpenState
	localSend    bool
	localReceive bool

	applicationVisible bool
	enqueued           bool

	sendResetSource terminalResetSource
	sendAbortSource terminalAbortSource
	recvAbortSource terminalAbortSource
	localReadStop   bool
	blockedSet      bool

	initialPrioritySet bool
	initialGroupSet    bool
	groupExplicit      bool
	groupTracked       bool
}

type streamProvisionalState struct {
	created time.Time
}

type streamLocalOpenState struct {
	opened    bool
	committed bool
	phase     state.LocalOpenPhase
}

type streamPendingState struct {
	priority []byte
	control  [streamControlCount]uint64

	queueIndex [pendingStreamQueueCount]int32

	flags uint8
}

const invalidStreamQueueIndex int32 = -1

func newStreamPendingState() streamPendingState {
	pending := streamPendingState{}
	for i := range pending.queueIndex {
		pending.queueIndex[i] = invalidStreamQueueIndex
	}
	return pending
}

var streamPendingControlFlags = [...]uint8{
	streamControlMaxData: streamPendingMaxData,
	streamControlBlocked: streamPendingBlocked,
}

func streamPendingControlFlag(kind streamControlKind) uint8 {
	if kind >= streamControlCount {
		return 0
	}
	return streamPendingControlFlags[kind]
}

const (
	streamFlagAccepted uint8 = 1 << iota
	streamFlagChurnCounted
	streamFlagOpenedEventSent
	streamFlagAcceptedEventSent
	streamFlagActiveCounted
	streamFlagLocalReadSignalPending
)

func (s *Stream) clearQueueMembershipState() {
	if s == nil {
		return
	}
	s.provisionalIndex = invalidStreamQueueIndex
	s.acceptIndex = invalidStreamQueueIndex
	s.unseenLocalIndex = invalidStreamQueueIndex
	s.enqueued = false
}

func (s *Stream) clearBlockedState() {
	if s == nil {
		return
	}
	s.blockedAt = 0
	s.blockedSet = false
}

func (s *Stream) markBlockedState(v uint64) {
	if s == nil {
		return
	}
	s.blockedAt = v
	s.blockedSet = true
}

func (s *Stream) provisionalCreatedAt() time.Time {
	if s == nil || s.provisional == nil {
		return time.Time{}
	}
	return s.provisional.created
}

func (s *Stream) setProvisionalCreated(t time.Time) {
	if s == nil {
		return
	}
	if t.IsZero() {
		s.provisional = nil
		return
	}
	if s.provisional == nil {
		s.provisional = &streamProvisionalState{}
	}
	s.provisional.created = t
}

func (s *Stream) clearProvisionalState() {
	if s == nil {
		return
	}
	s.provisional = nil
}

func (s *Stream) streamFlag(mask uint8) bool {
	return s != nil && s.lifecycleFlags&mask != 0
}

func (s *Stream) markStreamFlag(mask uint8) {
	if s == nil {
		return
	}
	s.lifecycleFlags |= mask
}

func (s *Stream) clearStreamFlag(mask uint8) {
	if s == nil {
		return
	}
	s.lifecycleFlags &^= mask
}

func (s *Stream) acceptedFlag() bool {
	return s.streamFlag(streamFlagAccepted)
}

func (s *Stream) markAccepted() {
	s.markStreamFlag(streamFlagAccepted)
}

func (s *Stream) churnCountedFlag() bool {
	return s.streamFlag(streamFlagChurnCounted)
}

func (s *Stream) markChurnCounted() {
	s.markStreamFlag(streamFlagChurnCounted)
}

func (s *Stream) openedEventSentFlag() bool {
	return s.streamFlag(streamFlagOpenedEventSent)
}

func (s *Stream) markOpenedEventSent() {
	s.markStreamFlag(streamFlagOpenedEventSent)
}

func (s *Stream) acceptedEventSentFlag() bool {
	return s.streamFlag(streamFlagAcceptedEventSent)
}

func (s *Stream) markAcceptedEventSent() {
	s.markStreamFlag(streamFlagAcceptedEventSent)
}

func (s *Stream) activeCountedFlag() bool {
	return s.streamFlag(streamFlagActiveCounted)
}

func (s *Stream) markActiveCounted() {
	s.markStreamFlag(streamFlagActiveCounted)
}

func (s *Stream) clearActiveCounted() {
	s.clearStreamFlag(streamFlagActiveCounted)
}

func (s *Stream) localReadSignalPendingFlag() bool {
	return s.streamFlag(streamFlagLocalReadSignalPending)
}

func (s *Stream) markLocalReadSignalPending() {
	s.markStreamFlag(streamFlagLocalReadSignalPending)
}

func (s *Stream) clearLocalReadSignalPending() {
	s.clearStreamFlag(streamFlagLocalReadSignalPending)
}

func setProvisionalIndex(stream *Stream, idx int32) {
	if stream == nil {
		return
	}
	stream.provisionalIndex = idx
}

func getProvisionalIndex(stream *Stream) int32 {
	if stream == nil {
		return invalidStreamQueueIndex
	}
	return stream.provisionalIndex
}

func setAcceptIndex(stream *Stream, idx int32) {
	if stream == nil {
		return
	}
	stream.acceptIndex = idx
}

func getAcceptIndex(stream *Stream) int32 {
	if stream == nil {
		return invalidStreamQueueIndex
	}
	return stream.acceptIndex
}

func setUnseenLocalIndex(stream *Stream, idx int32) {
	if stream == nil {
		return
	}
	stream.unseenLocalIndex = idx
}

func getUnseenLocalIndex(stream *Stream) int32 {
	if stream == nil {
		return invalidStreamQueueIndex
	}
	return stream.unseenLocalIndex
}

func (s *Stream) pendingControlValueLocked(kind streamControlKind) pendingStreamControlValue {
	if s == nil {
		return pendingStreamControlValue{}
	}
	flag := streamPendingControlFlag(kind)
	if flag == 0 || s.pending.flags&flag == 0 {
		return pendingStreamControlValue{}
	}
	return pendingStreamControlValue{value: s.pending.control[kind], present: true}
}

func (s *Stream) setPendingControlValueLocked(kind streamControlKind, v uint64) {
	if s == nil {
		return
	}
	flag := streamPendingControlFlag(kind)
	if flag == 0 {
		return
	}
	s.pending.control[kind] = v
	s.pending.flags |= flag
}

func (s *Stream) clearPendingControlValueLocked(kind streamControlKind) {
	if s == nil {
		return
	}
	flag := streamPendingControlFlag(kind)
	if flag == 0 {
		return
	}
	s.pending.control[kind] = 0
	s.pending.flags &^= flag
}

func (s *Stream) pendingControlFlushStateLocked(kind streamControlKind) (flush bool, keep bool) {
	if s == nil {
		return false, false
	}
	phase := s.visibilityPhaseLocked()
	switch kind {
	case streamControlMaxData:
		return state.ShouldFlushStreamMaxData(
			s.idSet,
			s.localReceive,
			phase,
			s.readStopSentLocked(),
			state.RecvTerminal(s.effectiveRecvHalfStateLocked()),
		)
	case streamControlBlocked:
		return state.ShouldFlushStreamBlocked(
			s.idSet,
			s.localSend,
			phase,
			s.effectiveSendHalfStateLocked(),
		)
	default:
		return false, false
	}
}

func (s *Stream) skipPendingControlQueueLocked(kind streamControlKind, v uint64) bool {
	if s == nil {
		return true
	}
	pending := s.pendingControlValueLocked(kind)
	switch kind {
	case streamControlMaxData:
		return pending.present && v <= pending.value
	case streamControlBlocked:
		return (pending.present && pending.value == v) || (s.blockedSet && s.blockedAt == v)
	default:
		return true
	}
}

func (s *Stream) notePendingControlQueuedLocked(kind streamControlKind, v uint64) {
	if s == nil {
		return
	}
	if kind == streamControlBlocked {
		s.markBlockedState(v)
	}
}

func (s *Stream) pendingPriorityFlushStateLocked() (flush bool, keep bool) {
	if s == nil {
		return false, false
	}
	return state.ShouldFlushPriorityUpdate(
		s.visibilityPhaseLocked(),
		s.effectiveSendHalfStateLocked(),
	)
}

func (s *Stream) hasPendingPriorityUpdateLocked() bool {
	return s != nil && s.pending.flags&streamPendingPriorityUpdate != 0 && len(s.pending.priority) > 0
}

func (s *Stream) setPendingPriorityUpdateLocked(payload []byte) {
	if s == nil {
		return
	}
	s.pending.priority = payload
	if len(payload) == 0 {
		s.pending.flags &^= streamPendingPriorityUpdate
		return
	}
	s.pending.flags |= streamPendingPriorityUpdate
}

func (s *Stream) clearPendingPriorityUpdateLocked() {
	s.setPendingPriorityUpdateLocked(nil)
}

func (s *Stream) pendingQueueIndex(kind pendingStreamQueueKind) int32 {
	if s == nil || kind >= pendingStreamQueueCount {
		return invalidStreamQueueIndex
	}
	return s.pending.queueIndex[kind]
}

func (s *Stream) setPendingQueueIndex(kind pendingStreamQueueKind, idx int32) {
	if s == nil || kind >= pendingStreamQueueCount {
		return
	}
	s.pending.queueIndex[kind] = idx
}

func (s *Stream) inPendingQueueLocked(kind pendingStreamQueueKind) bool {
	if s == nil {
		return false
	}
	switch kind {
	case pendingStreamQueueMaxData:
		return s.pending.flags&streamPendingControlFlag(streamControlMaxData) != 0
	case pendingStreamQueueBlocked:
		return s.pending.flags&streamPendingControlFlag(streamControlBlocked) != 0
	case pendingStreamQueuePriority:
		return s.hasPendingPriorityUpdateLocked()
	default:
		return false
	}
}

func setPendingMaxDataIndex(stream *Stream, idx int32) {
	stream.setPendingQueueIndex(pendingStreamQueueMaxData, idx)
}

func getPendingMaxDataIndex(stream *Stream) int32 {
	return stream.pendingQueueIndex(pendingStreamQueueMaxData)
}

func setPendingBlockedIndex(stream *Stream, idx int32) {
	stream.setPendingQueueIndex(pendingStreamQueueBlocked, idx)
}

func getPendingBlockedIndex(stream *Stream) int32 {
	return stream.pendingQueueIndex(pendingStreamQueueBlocked)
}

func setPendingPriorityIndex(stream *Stream, idx int32) {
	stream.setPendingQueueIndex(pendingStreamQueuePriority, idx)
}

func getPendingPriorityIndex(stream *Stream) int32 {
	return stream.pendingQueueIndex(pendingStreamQueuePriority)
}

func streamMatchesID(stream *Stream, streamID uint64) bool {
	return stream != nil && stream.idSet && stream.id == streamID
}

func streamBelongsToConn(stream *Stream, c *Conn) bool {
	return stream != nil && (stream.conn == nil || stream.conn == c)
}

type writeStep struct {
	frame            txFrame
	appN             int
	openerVisibility openerVisibilityMark
}

func (s *Stream) StreamID() uint64 {
	if s == nil || s.conn == nil {
		return 0
	}
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if !s.idSet {
		return 0
	}
	return s.id
}

func (s *Stream) ID() uint64 {
	return s.StreamID()
}

func (s *Stream) OpenInfo() []byte {
	if s == nil || s.conn == nil {
		return nil
	}
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return clonePayloadBytes(s.openInfo)
}

func appErrorCodePtr(err *ApplicationError) *uint64 {
	if err == nil {
		return nil
	}
	return boxedUint64(err.Code)
}

func boxedUint64(v uint64) *uint64 {
	ptr := new(uint64)
	*ptr = v
	return ptr
}

func appendDebugTextTLVCapped(payload []byte, reason string, maxPayload uint64) []byte {
	return wire.AppendDebugTextTLVCapped(payload, reason, maxPayload)
}

func clonePayloadBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func singlePartPayloadView(parts [][]byte, idx, off, n int) ([]byte, bool) {
	if n < 0 {
		return nil, false
	}
	for idx < len(parts) {
		part := parts[idx]
		if off >= len(part) {
			idx++
			off = 0
			continue
		}
		available := len(part) - off
		if n > available {
			return nil, false
		}
		return part[off : off+n], true
	}
	return nil, n == 0
}

type dataFrameTraits uint8

const (
	dataFrameTraitNone dataFrameTraits = 0
	dataFrameTraitFIN  dataFrameTraits = 1 << iota
	dataFrameTraitOpenMetadata
)

func (t dataFrameTraits) sendsFIN() bool {
	return t&dataFrameTraitFIN != 0
}

func (t dataFrameTraits) includesOpenMetadata() bool {
	return t&dataFrameTraitOpenMetadata != 0
}

func (s *Stream) dataFrameLocked(app []byte, traits dataFrameTraits) txFrame {
	frame := makeTxFrame(FrameTypeDATA, 0, s.id)
	if traits.includesOpenMetadata() && len(s.openMetadataPrefix) > 0 {
		frame.Flags |= FrameFlagOpenMetadata
		frame.setPrefixedFlatPayload(s.openMetadataPrefix, app)
		frame.payloadLen = len(s.openMetadataPrefix) + len(app)
	} else {
		frame.setFlatPayload(app)
		frame.payloadLen = len(app)
	}
	if traits.sendsFIN() {
		frame.Flags |= FrameFlagFIN
	}
	return frame
}

func (s *Stream) dataFrameFromPartsLocked(parts [][]byte, idx, off, n int, traits dataFrameTraits) txFrame {
	frame := makeTxFrame(FrameTypeDATA, 0, s.id)
	payloadLen := n
	if traits.includesOpenMetadata() && len(s.openMetadataPrefix) > 0 {
		frame.Flags |= FrameFlagOpenMetadata
		payloadLen += len(s.openMetadataPrefix)
	}
	if traits.sendsFIN() {
		frame.Flags |= FrameFlagFIN
	}
	frame.payloadLen = payloadLen

	if frame.Flags&FrameFlagOpenMetadata != 0 && n == 0 {
		frame.setFlatPayload(s.openMetadataPrefix)
		return frame
	}
	if frame.Flags&FrameFlagOpenMetadata == 0 {
		if payload, ok := singlePartPayloadView(parts, idx, off, n); ok {
			frame.setFlatPayload(payload)
			return frame
		}
	}
	if frame.Flags&FrameFlagOpenMetadata != 0 {
		frame.setPrefixedPartsPayload(s.openMetadataPrefix, parts, idx, off, n)
		return frame
	}
	frame.setPartsPayload(parts, idx, off, n)
	return frame
}

func csub(max, used uint64) uint64 {
	if used >= max {
		return 0
	}
	return max - used
}

func derefUint64(v *uint64) uint64 {
	if v == nil {
		return 0
	}
	return *v
}
