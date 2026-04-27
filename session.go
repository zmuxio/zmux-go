package zmux

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
	"github.com/zmuxio/zmux-go/internal/state"
)

// SessionState is the public session lifecycle state.
type SessionState uint8

const (
	SessionStateInvalid SessionState = iota
	SessionStateReady
	SessionStateDraining
	SessionStateClosing
	SessionStateClosed
	SessionStateFailed
)

type connState = state.SessionState

const (
	connStateReady    = state.SessionStateReady
	connStateDraining = state.SessionStateDraining
	connStateClosing  = state.SessionStateClosing
	connStateClosed   = state.SessionStateClosed
	connStateFailed   = state.SessionStateFailed
)

// EventHandler receives opt-in stream and session events.
type EventHandler func(Event)

// EventType identifies an event kind.
type EventType int

const (
	// EventStreamOpened is emitted when a local stream becomes peer-visible.
	EventStreamOpened EventType = iota + 1
	// EventStreamAccepted is emitted when a peer-opened stream is accepted.
	EventStreamAccepted
	// EventSessionClosed is emitted when the session terminates.
	EventSessionClosed
)

// Event is delivered to EventHandler.
type Event struct {
	Type               EventType
	SessionState       SessionState
	StreamID           uint64
	Stream             Stream
	Local              bool
	Bidi               bool
	Time               time.Time
	Err                error
	ApplicationVisible bool
}

func (s SessionState) String() string {
	switch s {
	case SessionStateReady:
		return "ready"
	case SessionStateDraining:
		return "draining"
	case SessionStateClosing:
		return "closing"
	case SessionStateClosed:
		return "closed"
	case SessionStateFailed:
		return "failed"
	default:
		return "invalid"
	}
}

// Valid reports whether s is a defined public session state.
func (s SessionState) Valid() bool {
	switch s {
	case SessionStateReady,
		SessionStateDraining,
		SessionStateClosing,
		SessionStateClosed,
		SessionStateFailed:
		return true
	default:
		return false
	}
}

// Terminal reports whether s is final.
func (s SessionState) Terminal() bool {
	return s == SessionStateClosed || s == SessionStateFailed
}

// State returns the current public session state.
func (c *Conn) State() SessionState {
	if c == nil {
		return SessionStateInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.publicLifecycleReadyLocked() {
		return SessionStateInvalid
	}
	return publicSessionState(c.lifecycle.sessionState)
}

// Closed reports whether the session has terminated.
func (c *Conn) Closed() bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	current := c.lifecycle.sessionState
	closedCh := c.lifecycle.closedCh
	c.mu.Unlock()
	if state.IsSessionFinished(current) {
		return true
	}
	if closedCh == nil {
		return true
	}
	select {
	case <-closedCh:
		return true
	default:
		return false
	}
}

func (c *Conn) publicLifecycleReadyLocked() bool {
	return c != nil && c.lifecycle.closedCh != nil
}

func (c *Conn) publicWritePathReadyLocked() bool {
	return c.publicLifecycleReadyLocked() && c.writer.writeCh != nil && c.writer.urgentWriteCh != nil && c.pending.controlNotify != nil
}

func publicSessionState(current connState) SessionState {
	switch current {
	case connStateReady:
		return SessionStateReady
	case connStateDraining:
		return SessionStateDraining
	case connStateClosing:
		return SessionStateClosing
	case connStateClosed:
		return SessionStateClosed
	case connStateFailed:
		return SessionStateFailed
	default:
		return SessionStateInvalid
	}
}

type streamArity uint8

const (
	streamArityUni streamArity = iota
	streamArityBidi
)

func streamArityFromBidi(bidi bool) streamArity {
	if bidi {
		return streamArityBidi
	}
	return streamArityUni
}

func (a streamArity) isBidi() bool {
	return a == streamArityBidi
}

func (a streamArity) nextLocalID(reg *connRegistryState) uint64 {
	if reg == nil {
		return 0
	}
	if a.isBidi() {
		return reg.nextLocalBidi
	}
	return reg.nextLocalUni
}

func (a streamArity) advanceNextLocalID(reg *connRegistryState) {
	if reg == nil {
		return
	}
	if a.isBidi() {
		reg.nextLocalBidi += 4
		return
	}
	reg.nextLocalUni += 4
}

func (a streamArity) provisionalQueueState(queues *connQueueState) *streamSparseQueueState {
	if queues == nil {
		return nil
	}
	if a.isBidi() {
		return &queues.provisionalBidi
	}
	return &queues.provisionalUni
}

func (a streamArity) unseenLocalQueueState(queues *connQueueState) *streamSparseQueueState {
	if queues == nil {
		return nil
	}
	if a.isBidi() {
		return &queues.unseenLocalBidi
	}
	return &queues.unseenLocalUni
}

func (a streamArity) acceptQueueState(queues *connQueueState) *streamSparseQueueState {
	if queues == nil {
		return nil
	}
	if a.isBidi() {
		return &queues.acceptBidi
	}
	return &queues.acceptUni
}

func (a streamArity) acceptQueuedBytes(queues *connQueueState) *uint64 {
	if queues == nil {
		return nil
	}
	if a.isBidi() {
		return &queues.acceptBidiBytes
	}
	return &queues.acceptUniBytes
}

func (s *nativeStream) streamArity() streamArity {
	if s != nil && s.bidi {
		return streamArityBidi
	}
	return streamArityUni
}

type sessionMemoryRelease uint8

const (
	sessionMemoryUnchanged sessionMemoryRelease = iota
	sessionMemoryReleased
)

func sessionMemoryReleaseFrom(released bool) sessionMemoryRelease {
	if released {
		return sessionMemoryReleased
	}
	return sessionMemoryUnchanged
}

func (r sessionMemoryRelease) released() bool {
	return r == sessionMemoryReleased
}

func visibleSessionErrLocked(c *Conn, err error) error {
	current := connStateClosed
	if c != nil {
		current = c.lifecycle.sessionState
	}
	return state.VisibleSessionError(current, err, ErrSessionClosed)
}

func (c *Conn) advanceSessionOnLocalGoAwayLocked(advanced bool) {
	if c == nil {
		return
	}
	c.lifecycle.sessionState = state.AdvanceSessionOnGoAway(c.lifecycle.sessionState, advanced)
}

func (c *Conn) beginSessionClosingLocked() {
	if c == nil {
		return
	}
	c.lifecycle.sessionState = state.BeginSessionClosing(c.lifecycle.sessionState)
}

func (c *Conn) lockPeerNonCloseFrameHandling() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		c.mu.Unlock()
		return false
	}
	return true
}

func (c *Conn) AcceptStream(ctx context.Context) (NativeStream, error) {
	stream, err := c.acceptStream(ctx, streamArityBidi)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (c *Conn) AcceptUniStream(ctx context.Context) (NativeRecvStream, error) {
	stream, err := c.acceptStream(ctx, streamArityUni)
	if err != nil {
		return nil, err
	}
	return &nativeRecvStream{stream: stream}, nil
}

func (c *Conn) acceptStream(ctx context.Context, arity streamArity) (*nativeStream, error) {
	if c == nil {
		return nil, ErrSessionClosed
	}
	ctx = contextOrBackground(ctx)

	for {
		c.mu.Lock()
		if !c.publicLifecycleReadyLocked() {
			c.mu.Unlock()
			return nil, ErrSessionClosed
		}
		if c.lifecycle.closeErr != nil {
			err := sessionOperationErrLocked(c, OperationAccept, visibleSessionErrLocked(c, c.lifecycle.closeErr))
			c.mu.Unlock()
			return nil, err
		}
		if c.acceptCountLocked(arity) > 0 {
			stream := c.dequeueAcceptedLocked(arity)
			if stream == nil {
				c.mu.Unlock()
				continue
			}
			if c.acceptCountLocked(arity) > 0 {
				notify(c.ensureAcceptNotifyLocked(arity))
			}
			stream.markAccepted()
			dispatch := c.takeStreamEventLocked(stream, EventStreamAccepted, nil)
			c.mu.Unlock()
			if dispatch.shouldEmit() {
				c.emitEvent(dispatch.event)
			}
			return stream, nil
		}
		acceptCh := c.ensureAcceptNotifyLocked(arity)
		closedCh := c.lifecycle.closedCh
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-closedCh:
			if err := c.err(); err != nil {
				c.mu.Lock()
				current := c.lifecycle.sessionState
				c.mu.Unlock()
				return nil, sessionOperationErr(c, OperationAccept, state.VisibleSessionError(current, err, ErrSessionClosed))
			}
			return nil, sessionOperationErr(c, OperationAccept, ErrSessionClosed)
		case <-acceptCh:
		}
	}
}

// emitEvent invokes the configured handler and ignores handler panics.
func (c *Conn) emitEvent(ev Event) {
	if c == nil || c.observer.eventHandler == nil {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	handler := c.observer.eventHandler
	defer func() {
		_ = recover()
	}()
	handler(ev)
}

type streamEventDispatch struct {
	event Event
	emit  bool
}

func (d streamEventDispatch) shouldEmit() bool {
	return d.emit
}

func (c *Conn) takeStreamEventLocked(stream *nativeStream, typ EventType, err error) streamEventDispatch {
	if c == nil || c.observer.eventHandler == nil || stream == nil {
		return streamEventDispatch{}
	}
	switch typ {
	case EventStreamOpened:
		if !stream.isLocalOpenedLocked() || !stream.idSet || stream.openedEventSentFlag() {
			return streamEventDispatch{}
		}
		stream.markOpenedEventSent()
	case EventStreamAccepted:
		if !stream.acceptedFlag() || stream.acceptedEventSentFlag() {
			return streamEventDispatch{}
		}
		stream.markAcceptedEventSent()
	default:
	}

	return streamEventDispatch{
		event: Event{
			Type:               typ,
			SessionState:       publicSessionState(c.lifecycle.sessionState),
			StreamID:           stream.id,
			Stream:             stream,
			Local:              stream.isLocalOpenedLocked(),
			Bidi:               stream.bidi,
			Err:                err,
			Time:               time.Now(),
			ApplicationVisible: stream.applicationVisible,
		},
		emit: true,
	}
}

func (c *Conn) OpenStream(ctx context.Context) (NativeStream, error) {
	return c.OpenStreamWithOptions(ctx, OpenOptions{})
}

func (c *Conn) OpenUniStream(ctx context.Context) (NativeSendStream, error) {
	return c.OpenUniStreamWithOptions(ctx, OpenOptions{})
}

func (c *Conn) OpenStreamWithOptions(ctx context.Context, opts OpenOptions) (NativeStream, error) {
	stream, err := c.openStream(ctx, streamArityBidi, opts)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (c *Conn) OpenUniStreamWithOptions(ctx context.Context, opts OpenOptions) (NativeSendStream, error) {
	stream, err := c.openStream(ctx, streamArityUni, opts)
	if err != nil {
		return nil, err
	}
	return &nativeSendStream{stream: stream}, nil
}

func (c *Conn) OpenAndSend(ctx context.Context, p []byte) (NativeStream, int, error) {
	return c.OpenAndSendWithOptions(ctx, OpenOptions{}, p)
}

func (c *Conn) OpenAndSendWithOptions(ctx context.Context, opts OpenOptions, p []byte) (NativeStream, int, error) {
	stream, err := c.OpenStreamWithOptions(ctx, opts)
	if err != nil {
		return nil, 0, err
	}
	if len(p) == 0 {
		return stream, 0, nil
	}
	writeCtx, err := beginContextWrite(ctx, stream)
	if err != nil {
		return stream, 0, err
	}
	defer writeCtx.clear()
	n, err := stream.Write(p)
	err = writeCtx.err(err)
	return stream, n, err
}

func (c *Conn) OpenUniAndSend(ctx context.Context, p []byte) (NativeSendStream, int, error) {
	return c.OpenUniAndSendWithOptions(ctx, OpenOptions{}, p)
}

func (c *Conn) OpenUniAndSendWithOptions(ctx context.Context, opts OpenOptions, p []byte) (NativeSendStream, int, error) {
	stream, err := c.OpenUniStreamWithOptions(ctx, opts)
	if err != nil {
		return nil, 0, err
	}
	writeCtx, err := beginContextWrite(ctx, stream)
	if err != nil {
		return stream, 0, err
	}
	defer writeCtx.clear()
	n, err := stream.WriteFinal(p)
	err = writeCtx.err(err)
	return stream, n, err
}

type contextWrite struct {
	ctx         context.Context
	setter      writeDeadlineSetter
	stop        func() bool
	done        chan struct{}
	deadline    time.Time
	deadlineSet bool
}

func beginContextWrite(ctx context.Context, setter writeDeadlineSetter) (contextWrite, error) {
	ctx = contextOrBackground(ctx)
	if setter == nil {
		return contextWrite{}, ErrSessionClosed
	}
	select {
	case <-ctx.Done():
		return contextWrite{}, ctx.Err()
	default:
	}

	cw := contextWrite{
		ctx:    ctx,
		setter: setter,
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := setter.SetWriteDeadline(deadline); err != nil {
			return contextWrite{}, err
		}
		cw.deadline = deadline
		cw.deadlineSet = true
	}
	if ctx.Done() == nil {
		return cw, nil
	}

	cw.done = make(chan struct{})
	cw.stop = context.AfterFunc(ctx, func() {
		_ = setter.SetWriteDeadline(time.Now())
		close(cw.done)
	})
	select {
	case <-ctx.Done():
		cw.clear()
		return contextWrite{}, ctx.Err()
	default:
		return cw, nil
	}
}

func (w contextWrite) clear() {
	callbackRan := false
	if w.stop != nil {
		if !w.stop() {
			<-w.done
			callbackRan = true
		}
	}
	if w.setter != nil && (w.deadlineSet || callbackRan) {
		_ = w.setter.SetWriteDeadline(time.Time{})
	}
}

func (w contextWrite) err(err error) error {
	if err == nil || !errors.Is(err, os.ErrDeadlineExceeded) {
		return err
	}
	if ctxErr := w.ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if w.deadlineSet && !time.Now().Before(w.deadline) {
		return context.DeadlineExceeded
	}
	return err
}

func (c *Conn) openStream(ctx context.Context, arity streamArity, opts OpenOptions) (*nativeStream, error) {
	if c == nil {
		return nil, ErrSessionClosed
	}
	ctx = contextOrBackground(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.publicWritePathReadyLocked() {
		return nil, sessionOperationErrLocked(c, OperationOpen, ErrSessionClosed)
	}
	switch state.PlanLocalOpen(c.lifecycle.sessionState, c.shutdown.gracefulCloseActive, c.lifecycle.closeErr != nil) {
	case state.LocalOpenReturnExisting:
		return nil, sessionOperationErrLocked(c, OperationOpen, visibleSessionErrLocked(c, c.lifecycle.closeErr))
	case state.LocalOpenReturnClosed:
		return nil, sessionOperationErrLocked(c, OperationOpen, visibleSessionErrLocked(c, ErrSessionClosed))
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	prefix, err := buildOpenMetadataPrefix(c.config.negotiated.Capabilities, opts, c.config.peer.Settings.MaxFramePayload)
	if err != nil {
		return nil, wrapStructuredError(err, errorMeta{
			scope:     ScopeSession,
			operation: OperationOpen,
			source:    SourceLocal,
			direction: DirectionBoth,
		})
	}

	if err := c.checkLocalOpenPossibleWithOpenInfoLocked(arity, uint64(len(opts.OpenInfo))); err != nil {
		return nil, openOperationErrLocked(c, err)
	}

	stream := c.newProvisionalLocalStreamOwnedLocked(arity, opts, prefix)
	return stream, nil
}

const (
	minSendRateSampleBytes    = 4 << 10
	minSendRateSampleDuration = 25 * time.Millisecond
	maxDuration               = time.Duration(1<<63 - 1)
)

// QueueStats reports writer queue depths.
type QueueStats struct {
	Urgent   int
	Advisory int
	Ordinary int
	Total    int
}

// ActiveStreamStats reports active stream slots by opener and direction.
type ActiveStreamStats struct {
	// LocalBidi counts active bidirectional streams opened by this endpoint.
	LocalBidi uint64
	// LocalUni counts active send-only streams opened by this endpoint.
	LocalUni uint64
	// PeerBidi counts active bidirectional streams opened by the peer.
	PeerBidi uint64
	// PeerUni counts active receive-only streams opened by the peer.
	PeerUni uint64
	// Total is the saturating sum of all active stream counters.
	Total uint64
}

// FlushStats reports the most recent writer flush.
type FlushStats struct {
	Count      uint64
	LastAt     time.Time
	LastFrames int
	LastBytes  int
}

// ProgressStats reports recent transport, mux, stream, and app activity times.
type ProgressStats struct {
	TransportProgressAt   time.Time
	MuxControlProgressAt  time.Time
	StreamProgressAt      time.Time
	ApplicationProgressAt time.Time
}

// HiddenStats reports hidden-stream counters and limits.
type HiddenStats struct {
	Refused              uint64
	Reaped               uint64
	UnreadBytesDiscarded uint64
	Retained             int
	SoftCap              int
	HardCap              int
	AtSoftCap            bool
	AtHardCap            bool
}

// AcceptBacklogStats reports accepted-stream backlog usage.
type AcceptBacklogStats struct {
	Count      int
	CountLimit int
	AtCountCap bool
	Bytes      uint64
	BytesLimit uint64
	AtBytesCap bool
	Refused    uint64
}

// ProvisionalStats reports provisional-open counts and limits.
type ProvisionalStats struct {
	Bidi       int
	Uni        int
	SoftCap    int
	HardCap    int
	BidiAtSoft bool
	UniAtSoft  bool
	BidiAtHard bool
	UniAtHard  bool
	Limited    uint64
	Expired    uint64
}

// ReasonStats counts observed reset and abort codes. Overflow counters count
// events folded after the distinct-code map cap is reached.
type ReasonStats struct {
	Reset         map[uint64]uint64
	ResetOverflow uint64
	Abort         map[uint64]uint64
	AbortOverflow uint64
}

// DiagnosticStats reports ingress, drop, and late-data counters.
type DiagnosticStats struct {
	DroppedPriorityUpdates      uint64
	DroppedLocalPriorityUpdates uint64
	LateDataAfterCloseRead      uint64
	LateDataAfterReset          uint64
	LateDataAfterAbort          uint64
	CoalescedTerminal           uint64
	SupersededTerminal          uint64
	SkippedCloseOnDeadIO        uint64
	CloseFrameAdmissionTimeouts uint64
	CloseFrameFlushTimeouts     uint64
	CloseFrameFlushErrors       uint64
	ProtocolBacklogBlocked      uint64
	MarkerOnlyRangeCount        int
}

// RetainedBucketStats reports retained item and byte counts.
type RetainedBucketStats struct {
	Count int
	Bytes uint64
}

// RetainedStateBreakdown reports retained bytes by category.
type RetainedStateBreakdown struct {
	HiddenControl    RetainedBucketStats
	AcceptBacklog    RetainedBucketStats
	Provisionals     RetainedBucketStats
	VisibleTombstone RetainedBucketStats
	MarkerOnly       RetainedBucketStats
}

// PressureStats reports queue, memory, and retention pressure.
type PressureStats struct {
	ReceiveBacklog         uint64
	ReceiveBacklogHigh     bool
	AggregateLateData      uint64
	AggregateLateDataAtCap bool
	RetainedState          uint64
	RetainedBuckets        RetainedStateBreakdown
	RetainedOpenInfo       uint64
	RetainedPeerReasons    uint64
	OutstandingPingBytes   uint64
	TrackedBuffered        uint64
	TrackedBufferedLimit   uint64
	TrackedBufferedHigh    bool
	TrackedBufferedAtCap   bool
	OrdinaryQueued         uint64
	AdvisoryQueued         uint64
	UrgentQueued           uint64
	PendingControl         uint64
	PendingAdvisory        uint64
	PreparedAdvisory       uint64
	PendingTerminal        uint64
	PendingTerminalCount   int
	PendingProtocolJobs    int
}

// SessionStats is a snapshot of session state and runtime counters.
type SessionStats struct {
	State                    SessionState
	KeepaliveInterval        time.Duration
	KeepaliveMaxPingInterval time.Duration
	KeepaliveTimeout         time.Duration
	PingOutstanding          bool
	PingStalled              bool
	Progress                 ProgressStats
	LastInboundFrameAt       time.Time
	LastControlProgress      time.Time
	LastTransportWrite       time.Time
	LastStreamProgress       time.Time
	LastAppProgress          time.Time
	LastPingSentAt           time.Time
	LastPongAt               time.Time
	LastPingRTT              time.Duration

	ActiveStreams     ActiveStreamStats
	Queues            QueueStats
	Flush             FlushStats
	BlockedWriteTotal time.Duration
	LastOpenLatency   time.Duration
	Pressure          PressureStats
	Hidden            HiddenStats
	AcceptBacklog     AcceptBacklogStats
	Provisionals      ProvisionalStats
	Reasons           ReasonStats
	Diagnostics       DiagnosticStats
}

func makeActiveStreamStats(localBidi, localUni, peerBidi, peerUni uint64) ActiveStreamStats {
	return ActiveStreamStats{
		LocalBidi: localBidi,
		LocalUni:  localUni,
		PeerBidi:  peerBidi,
		PeerUni:   peerUni,
		Total: saturatingAdd(
			saturatingAdd(localBidi, localUni),
			saturatingAdd(peerBidi, peerUni),
		),
	}
}

func saturatingDurationAdd(a, b time.Duration) time.Duration {
	if b <= 0 {
		return a
	}
	if a >= maxDuration-b {
		return maxDuration
	}
	return a + b
}

func averageUint64Floor(a, b uint64) uint64 {
	if a <= b {
		return a + (b-a)/2
	}
	return b + (a-b)/2
}

func (c *Conn) noteStreamProgressLocked(now time.Time) {
	if c == nil {
		return
	}
	c.liveness.lastStreamProgressAt = now
}

func (c *Conn) noteAppProgressLocked(now time.Time) {
	if c == nil {
		return
	}
	c.liveness.lastAppProgressAt = now
}

func (c *Conn) noteFlushAndRateLocked(frames int, bytes int, now time.Time, writeDuration time.Duration) {
	if c == nil {
		return
	}
	c.noteTransportWriteLocked(now)
	c.metrics.flushCount = saturatingAdd(c.metrics.flushCount, 1)
	c.metrics.lastFlushAt = now
	c.metrics.lastBatchFrames = frames
	c.metrics.lastBatchBytes = bytes
	c.noteSendRateEstimateLocked(bytes, writeDuration)
}

func (c *Conn) noteSendRateEstimateLocked(bytes int, writeDuration time.Duration) {
	if c == nil || bytes <= 0 || writeDuration <= 0 {
		return
	}
	if bytes < minSendRateSampleBytes && writeDuration < minSendRateSampleDuration {
		return
	}

	sample := rt.SaturatingMulDivFloor(uint64(bytes), uint64(time.Second), uint64(writeDuration))
	if sample == 0 {
		sample = 1
	}
	if c.metrics.sendRateEstimate == 0 {
		c.metrics.sendRateEstimate = sample
		return
	}
	c.metrics.sendRateEstimate = averageUint64Floor(c.metrics.sendRateEstimate, sample)
}

func (c *Conn) noteBlockedWrite(d time.Duration) {
	if c == nil || d <= 0 {
		return
	}
	c.mu.Lock()
	c.metrics.blockedWriteTime = saturatingDurationAdd(c.metrics.blockedWriteTime, d)
	c.mu.Unlock()
	notify(c.signals.livenessCh)
}

func (c *Conn) noteOpenCommitLocked(created time.Time, now time.Time) {
	if c == nil || created.IsZero() {
		return
	}
	if now.Before(created) {
		now = created
	}
	c.metrics.lastOpenLatency = now.Sub(created)
	notify(c.signals.livenessCh)
}

const maxReasonStatsCodes = 1024

func noteReasonLocked(counts map[uint64]uint64, overflow *uint64, code uint64) {
	if _, ok := counts[code]; ok || len(counts) < maxReasonStatsCodes {
		counts[code] = saturatingAdd(counts[code], 1)
		return
	}
	*overflow = saturatingAdd(*overflow, 1)
}

func (c *Conn) noteResetReasonLocked(code uint64) {
	if c == nil {
		return
	}
	if c.retention.resetReasonCount == nil {
		c.retention.resetReasonCount = make(map[uint64]uint64)
	}
	noteReasonLocked(c.retention.resetReasonCount, &c.retention.resetReasonOverflow, code)
}

func (c *Conn) noteAbortReasonLocked(code uint64) {
	if c == nil {
		return
	}
	if c.retention.abortReasonCount == nil {
		c.retention.abortReasonCount = make(map[uint64]uint64)
	}
	noteReasonLocked(c.retention.abortReasonCount, &c.retention.abortReasonOverflow, code)
}

func (c *Conn) noteHiddenStreamRefusedLocked() {
	if c == nil {
		return
	}
	c.ingress.hiddenStreamsRefused = saturatingAdd(c.ingress.hiddenStreamsRefused, 1)
}

func (c *Conn) noteHiddenStreamReapedLocked() {
	if c == nil {
		return
	}
	c.ingress.hiddenStreamsReaped = saturatingAdd(c.ingress.hiddenStreamsReaped, 1)
}

func (c *Conn) noteHiddenUnreadDiscardLocked(n uint64) {
	if c == nil || n == 0 {
		return
	}
	c.ingress.hiddenUnreadBytesDiscarded = saturatingAdd(c.ingress.hiddenUnreadBytesDiscarded, n)
}

func (c *Conn) Stats() SessionStats {
	if c == nil {
		return SessionStats{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.publicLifecycleReadyLocked() {
		return SessionStats{State: SessionStateInvalid}
	}

	stalled := false
	if c.liveness.pingOutstanding {
		timeout := c.effectiveKeepaliveTimeoutLocked()
		stalled = timeout > 0 && time.Since(c.liveness.lastPingSentAt) > timeout/2
	}

	queues := QueueStats{
		Urgent:   queueDepth(c.writer.urgentWriteCh),
		Advisory: queueDepth(c.writer.advisoryWriteCh),
		Ordinary: queueDepth(c.writer.writeCh),
	}
	queues.Total = queues.Urgent + queues.Advisory + queues.Ordinary
	trackedBuffered := c.trackedSessionMemoryLocked()
	trackedLimit := c.sessionMemoryHardCapLocked()
	retainedBuckets := c.retainedStateBreakdownLocked()

	pressure := PressureStats{
		ReceiveBacklog:         c.flow.recvSessionUsed,
		ReceiveBacklogHigh:     c.flow.recvSessionAdvertised > 0 && c.flow.recvSessionUsed >= c.flow.recvSessionAdvertised/2,
		AggregateLateData:      c.ingress.aggregateLateData,
		AggregateLateDataAtCap: c.ingress.aggregateLateDataCap > 0 && c.ingress.aggregateLateData >= c.ingress.aggregateLateDataCap,
		RetainedState:          retainedBuckets.TotalBytes(),
		RetainedBuckets:        retainedBuckets,
		RetainedOpenInfo:       c.retention.retainedOpenInfoBytes,
		RetainedPeerReasons:    c.retention.retainedPeerReasonBytes,
		OutstandingPingBytes:   c.outstandingPingBytesLocked(),
		TrackedBuffered:        trackedBuffered,
		TrackedBufferedLimit:   trackedLimit,
		TrackedBufferedHigh:    c.sessionMemoryPressureHighLocked(),
		TrackedBufferedAtCap:   trackedBuffered >= trackedLimit,
		OrdinaryQueued:         c.flow.queuedDataBytes,
		AdvisoryQueued:         c.flow.advisoryQueuedBytes,
		UrgentQueued:           c.flow.urgentQueuedBytes,
		PendingControl:         c.pending.controlBytes,
		PendingAdvisory:        c.pending.priorityBytes,
		PreparedAdvisory:       c.pending.preparedPriorityBytes,
		PendingTerminal:        c.pendingTerminalControlBytesLocked(),
		PendingTerminalCount:   c.pendingTerminalQueueCountLocked(),
		PendingProtocolJobs:    len(c.protocol.tasks),
	}
	hiddenRetained := c.hiddenControlStateRetainedLocked()
	hiddenSoftCap := state.AdmissionSoftCap(c.pendingInboundLimitLocked())
	hiddenHardCap := state.AdmissionHardCap(c.pendingInboundLimitLocked())
	acceptBacklogCount := c.pendingAcceptedCountLocked()
	acceptBacklogCountLimit := c.visibleAcceptBacklogHardCapLocked()
	acceptBacklogBytes := c.pendingAcceptedBytesLocked()
	acceptBacklogBytesLimit := c.visibleAcceptBacklogBytesHardCapLocked()
	provisionalSoftCap := state.ProvisionalSoftCap(true, c.pendingInboundLimitLocked())
	provisionalHardCap := state.ProvisionalHardCap(true, c.pendingInboundLimitLocked())
	provisionalBidi := c.provisionalCountLocked(streamArityBidi)
	provisionalUni := c.provisionalCountLocked(streamArityUni)
	activeStreams := makeActiveStreamStats(
		c.registry.activeLocalBidi,
		c.registry.activeLocalUni,
		c.registry.activePeerBidi,
		c.registry.activePeerUni,
	)

	return SessionStats{
		State:                    publicSessionState(c.lifecycle.sessionState),
		KeepaliveInterval:        c.liveness.keepaliveInterval,
		KeepaliveMaxPingInterval: c.liveness.keepaliveMaxPingInterval,
		KeepaliveTimeout:         c.liveness.keepaliveTimeout,
		PingOutstanding:          c.liveness.pingOutstanding,
		PingStalled:              stalled,
		Progress: ProgressStats{
			TransportProgressAt:   c.liveness.lastTransportWriteAt,
			MuxControlProgressAt:  c.liveness.lastControlProgressAt,
			StreamProgressAt:      c.liveness.lastStreamProgressAt,
			ApplicationProgressAt: c.liveness.lastAppProgressAt,
		},
		LastInboundFrameAt:  c.liveness.lastInboundFrameAt,
		LastControlProgress: c.liveness.lastControlProgressAt,
		LastTransportWrite:  c.liveness.lastTransportWriteAt,
		LastStreamProgress:  c.liveness.lastStreamProgressAt,
		LastAppProgress:     c.liveness.lastAppProgressAt,
		LastPingSentAt:      c.liveness.lastPingSentAt,
		LastPongAt:          c.liveness.lastPongAt,
		LastPingRTT:         c.liveness.lastPingRTT,
		ActiveStreams:       activeStreams,
		Queues:              queues,
		Flush: FlushStats{
			Count:      c.metrics.flushCount,
			LastAt:     c.metrics.lastFlushAt,
			LastFrames: c.metrics.lastBatchFrames,
			LastBytes:  c.metrics.lastBatchBytes,
		},
		BlockedWriteTotal: c.metrics.blockedWriteTime,
		LastOpenLatency:   c.metrics.lastOpenLatency,
		Pressure:          pressure,
		Hidden: HiddenStats{
			Refused:              c.ingress.hiddenStreamsRefused,
			Reaped:               c.ingress.hiddenStreamsReaped,
			UnreadBytesDiscarded: c.ingress.hiddenUnreadBytesDiscarded,
			Retained:             hiddenRetained,
			SoftCap:              hiddenSoftCap,
			HardCap:              hiddenHardCap,
			AtSoftCap:            hiddenRetained >= hiddenSoftCap,
			AtHardCap:            hiddenRetained >= hiddenHardCap,
		},
		AcceptBacklog: AcceptBacklogStats{
			Count:      acceptBacklogCount,
			CountLimit: acceptBacklogCountLimit,
			AtCountCap: acceptBacklogCountLimit > 0 && acceptBacklogCount >= acceptBacklogCountLimit,
			Bytes:      acceptBacklogBytes,
			BytesLimit: acceptBacklogBytesLimit,
			AtBytesCap: acceptBacklogBytesLimit > 0 && acceptBacklogBytes >= acceptBacklogBytesLimit,
			Refused:    c.queues.visibleAcceptRefused,
		},
		Provisionals: ProvisionalStats{
			Bidi:       provisionalBidi,
			Uni:        provisionalUni,
			SoftCap:    provisionalSoftCap,
			HardCap:    provisionalHardCap,
			BidiAtSoft: provisionalBidi >= provisionalSoftCap,
			UniAtSoft:  provisionalUni >= provisionalSoftCap,
			BidiAtHard: provisionalBidi >= provisionalHardCap,
			UniAtHard:  provisionalUni >= provisionalHardCap,
			Limited:    c.ingress.provisionalLimited,
			Expired:    c.ingress.provisionalExpired,
		},
		Reasons: ReasonStats{
			Reset:         copyReasonCounts(c.retention.resetReasonCount),
			ResetOverflow: c.retention.resetReasonOverflow,
			Abort:         copyReasonCounts(c.retention.abortReasonCount),
			AbortOverflow: c.retention.abortReasonOverflow,
		},
		Diagnostics: DiagnosticStats{
			DroppedPriorityUpdates:      c.ingress.droppedPriorityUpdate,
			DroppedLocalPriorityUpdates: c.ingress.droppedLocalPriority,
			LateDataAfterCloseRead:      c.ingress.lateDataAfterClose,
			LateDataAfterReset:          c.ingress.lateDataAfterReset,
			LateDataAfterAbort:          c.ingress.lateDataAfterAbort,
			CoalescedTerminal:           c.metrics.coalescedTerminalSignals,
			SupersededTerminal:          c.metrics.droppedSupersededControls,
			SkippedCloseOnDeadIO:        c.metrics.skippedCloseOnDeadIO,
			CloseFrameAdmissionTimeouts: c.metrics.closeFrameAdmissionTO,
			CloseFrameFlushTimeouts:     c.metrics.closeFrameFlushTO,
			CloseFrameFlushErrors:       c.metrics.closeFrameFlushErr,
			ProtocolBacklogBlocked:      c.metrics.protocolBacklogBlocked,
			MarkerOnlyRangeCount:        c.markerOnlyRangeCountLocked(),
		},
	}
}

func queueDepth(ch <-chan writeRequest) int {
	if ch == nil {
		return 0
	}
	return len(ch)
}

func copyReasonCounts(src map[uint64]uint64) map[uint64]uint64 {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[uint64]uint64, len(src))
	for code, n := range src {
		dst[code] = n
	}
	return dst
}

const sessionCloseFrameSendTimeout = 100 * time.Millisecond
const sessionCloseFrameSendTimeoutMax = 2 * time.Second

func closeOperationErr(c *Conn, err error) error {
	return sessionOperationErr(c, OperationClose, err)
}

func completeCloseErr(c *Conn, err error) error {
	current := connStateClosed
	if c != nil {
		c.mu.Lock()
		current = c.lifecycle.sessionState
		c.mu.Unlock()
	}
	if state.IsBenignSessionError(current, err, ErrSessionClosed) {
		return nil
	}
	return err
}

func completeCloseOperationErr(c *Conn, err error) error {
	current := connStateClosed
	if c != nil {
		c.mu.Lock()
		current = c.lifecycle.sessionState
		c.mu.Unlock()
	}
	if state.IsBenignSessionError(current, err, ErrSessionClosed) {
		return nil
	}
	return closeOperationErr(c, err)
}

func (c *Conn) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	if !c.publicLifecycleReadyLocked() {
		c.mu.Unlock()
		return nil
	}
	hasOpenStreams := c.hasGracefulCloseLiveStreamsLocked() || c.totalProvisionalCountLocked() > 0
	plan := state.PlanBeginClose(c.lifecycle.sessionState, c.shutdown.gracefulCloseActive, c.lifecycle.closeErr != nil, hasOpenStreams)
	nextState := plan.NextState
	switch plan.Outcome {
	case state.BeginCloseReturnExisting:
		c.mu.Unlock()
		return completeCloseOperationErr(c, visibleSessionErrLocked(c, c.lifecycle.closeErr))
	case state.BeginCloseWaitExisting:
		c.mu.Unlock()
		return completeCloseErr(c, c.Wait(context.Background()))
	default:
	}
	var initialGoAwayBidi, initialGoAwayUni uint64
	initialGoAwayPolicy := goAwayInitialSkip
	if plan.Outcome == state.BeginCloseGraceful {
		c.lifecycle.sessionState = nextState
		c.shutdown.gracefulCloseActive = true
		if c.sessionControl.localGoAwayBidi == MaxVarint62 && c.sessionControl.localGoAwayUni == MaxVarint62 {
			initialGoAwayBidi = effectiveGoAwaySendWatermark(c.config.negotiated.LocalRole, streamArityBidi, c.sessionControl.localGoAwayBidi)
			initialGoAwayUni = effectiveGoAwaySendWatermark(c.config.negotiated.LocalRole, streamArityUni, c.sessionControl.localGoAwayUni)
			initialGoAwayPolicy = goAwayInitialSend
		}
	} else {
		c.lifecycle.sessionState = nextState
	}
	c.mu.Unlock()

	gracefulClose := false
	var gracefulErr error
	if plan.Outcome == state.BeginCloseGraceful {
		if err := c.closeWithGoAwayAndClose(initialGoAwayPolicy, initialGoAwayBidi, initialGoAwayUni); err == nil || errors.Is(err, ErrGracefulCloseTimeout) {
			gracefulClose = true
			gracefulErr = err
			c.closeSessionWithOptions(nil, closeOriginApp, closeFrameDefault)
		} else {
			c.CloseWithError(err)
			_ = c.Wait(context.Background())
			return closeOperationErr(c, err)
		}
	}

	if !gracefulClose {
		c.closeSessionWithOptions(applicationErr(uint64(CodeNoError), ""), closeOriginApp, closeFrameAlways)
		return completeCloseErr(c, c.Wait(context.Background()))
	}
	if err := completeCloseErr(c, c.Wait(context.Background())); err != nil {
		return err
	}
	if gracefulErr != nil {
		return closeOperationErr(c, gracefulErr)
	}
	return nil
}

func (c *Conn) CloseWithError(err error) {
	if c == nil {
		return
	}
	if err == nil {
		err = ErrSessionClosed
	}
	c.closeSessionWithOptions(err, closeOriginApp, closeFrameAlways)
}

func (c *Conn) Wait(ctx context.Context) error {
	if c == nil {
		return nil
	}
	ctx = contextOrBackground(ctx)
	c.mu.Lock()
	if !c.publicLifecycleReadyLocked() {
		c.mu.Unlock()
		return nil
	}
	closedCh := c.lifecycle.closedCh
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-closedCh:
		return c.waitCloseOperationErr()
	}
}

func (c *Conn) waitCloseOperationErr() error {
	current := connStateClosed
	sessionErr := ErrSessionClosed
	if c != nil {
		c.mu.Lock()
		current = c.lifecycle.sessionState
		sessionErr = c.lifecycle.closeErr
		c.mu.Unlock()
	}
	err := closeOperationErr(c, waitSessionErr(c, state.VisibleSessionError(current, sessionErr, ErrSessionClosed)))
	if err == nil || isError(err, ErrSessionClosed) {
		return nil
	}
	return err
}

func waitSessionErr(c *Conn, err error) error {
	if err == nil || isError(err, ErrSessionClosed) {
		return err
	}
	if isError(err, io.EOF) || isError(err, io.ErrClosedPipe) {
		return err
	}
	if appErr := closeSessionStreamErr(waitSessionState(c), err); appErr != nil {
		return appErr
	}
	return err
}

func waitSessionState(c *Conn) connState {
	if c == nil {
		return connStateClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lifecycle.sessionState
}

func (c *Conn) closeSession(err error) {
	c.closeSessionWithOptions(err, closeOriginInternal, closeFrameDefault)
}

type closeFramePolicy uint8

const (
	closeFrameDefault closeFramePolicy = iota
	closeFrameAlways
)

func (p closeFramePolicy) forcesCloseFrame() bool {
	return p == closeFrameAlways
}

type closeOrigin uint8

const (
	closeOriginInternal closeOrigin = iota
	closeOriginApp
	closeOriginReadLoop
	closeOriginWriteLoop
)

func (o closeOrigin) allowsQueuedCloseFrame() bool {
	return o != closeOriginWriteLoop
}

type closeFrameSendResult struct {
	admitted  bool
	completed bool
	timedOut  bool
	err       error
}

func (r closeFrameSendResult) sent() bool {
	return r.completed && r.err == nil
}

func (c *Conn) clearPostCloseProtocolBacklogLocked() {
	if c == nil {
		return
	}
	c.protocol.tasks = nil
}

func (c *Conn) closeSessionWithOptions(err error, origin closeOrigin, closePolicy closeFramePolicy) {
	if err == nil {
		err = ErrSessionClosed
	} else if origin == closeOriginReadLoop {
		err = readLoopSessionErr(err)
	}
	var emitCloseFrame bool
	var closeFramePayload []byte
	var closeEvent *Event
	c.shutdown.closeOnce.Do(func() {
		c.mu.Lock()
		if c.lifecycle.closeErr != nil {
			err = c.lifecycle.closeErr
		}
		shouldQueueCloseFrame := (closePolicy.forcesCloseFrame() || !state.IsBenignSessionError(c.lifecycle.sessionState, err, ErrSessionClosed)) &&
			c.sessionControl.peerCloseErr == nil &&
			!c.closeFrameOutstandingLocked()
		if !origin.allowsQueuedCloseFrame() && shouldQueueCloseFrame {
			c.metrics.skippedCloseOnDeadIO = saturatingAdd(c.metrics.skippedCloseOnDeadIO, 1)
		}
		if origin.allowsQueuedCloseFrame() && shouldQueueCloseFrame {
			payload, payloadErr := buildClosePayload(c, err)
			if payloadErr != nil {
				err = payloadErr
			} else {
				closeFramePayload = payload
				emitCloseFrame = true
				c.shutdown.closeFramePending = true
			}
		}
		c.lifecycle.closeErr = err
		if !state.IsSessionFinished(c.lifecycle.sessionState) {
			c.lifecycle.sessionState = state.CloseSessionState(c.lifecycle.sessionState, err, ErrSessionClosed)
		}
		c.shutdown.gracefulCloseActive = false
		c.broadcastWriteWakeLocked()
		c.broadcastUrgentWakeLocked()

		sessionErr := closeSessionStreamErr(c.lifecycle.sessionState, err)
		c.releaseAllStreamsForSessionCloseLocked(sessionErr)
		c.closeTerminalChLocked()
		c.notifyAcceptControlAndLivenessLocked()
		c.mu.Unlock()

		if emitCloseFrame {
			sendResult := c.emitCloseFrame(closeFramePayload)
			c.mu.Lock()
			c.finishCloseFrameEnqueueLocked(sendResult.sent())
			c.mu.Unlock()
			if sendResult.sent() {
				if delay := closeTransportDrainDelay(err); delay > 0 {
					time.Sleep(delay)
				}
			}
		}

		c.mu.Lock()
		c.closeClosedChLocked()
		c.clearPostCloseProtocolBacklogLocked()
		c.mu.Unlock()
		if c.io.conn != nil {
			_ = c.io.conn.Close()
		}
		drainDetachedWriteLanes(queueVisibleSessionErr(c, c.err()), c.writer.urgentWriteCh, c.writer.advisoryWriteCh, c.writer.writeCh)

		// Clear tombstones after the transport is closed so the ingress
		// goroutine cannot race against a nil tombstone map.
		c.mu.Lock()
		c.registry.tombstones = nil
		c.clearTombstoneQueueLocked()
		c.clearHiddenTombstonesLocked()
		c.registry.usedStreamData = nil
		c.registry.usedStreamRanges = nil
		c.registry.usedStreamRangeMode = false
		c.mu.Unlock()
		if c != nil && c.observer.eventHandler != nil {
			ev := Event{
				Type:         EventSessionClosed,
				SessionState: c.State(),
				Err:          err,
				Time:         time.Now(),
			}
			closeEvent = &ev
		}
	})
	if closeEvent != nil {
		c.emitEvent(*closeEvent)
	}
}

func (c *Conn) closeFrameOutstandingLocked() bool {
	if c == nil {
		return false
	}
	return c.shutdown.closeFramePending || c.shutdown.closeFrameSent
}

func (c *Conn) allowLocalNonCloseControlLocked() bool {
	if c == nil {
		return false
	}
	return state.AllowLocalNonCloseControl(
		c.lifecycle.sessionState,
		c.lifecycle.closeErr != nil,
		c.sessionControl.peerCloseErr != nil,
		c.closeFrameOutstandingLocked(),
	)
}

func (c *Conn) finishCloseFrameEnqueueLocked(sent bool) {
	if c == nil {
		return
	}
	c.shutdown.closeFramePending = false
	if sent {
		c.shutdown.closeFrameSent = true
	}
}

func closeTransportDrainDelay(err error) time.Duration {
	switch {
	case isError(err, ErrKeepaliveTimeout):
		return 100 * time.Millisecond
	case err == nil || isError(err, ErrSessionClosed):
		return 0
	}
	if appErr, ok := findError[*ApplicationError](err); ok && appErr.Code == uint64(CodeNoError) {
		return 0
	}
	// Give the peer a brief chance to observe the emitted CLOSE before a
	// transport without half-close support turns it into bare EOF/closed-pipe.
	return 10 * time.Millisecond
}

func closeMappedApplicationError(err error) *ApplicationError {
	if err == nil {
		return nil
	}
	if isError(err, ErrKeepaliveTimeout) {
		return applicationErr(uint64(CodeIdleTimeout), ErrKeepaliveTimeout.Error())
	}
	if appErr, ok := findError[*ApplicationError](err); ok {
		return cloneApplicationError(appErr)
	}
	if code, ok := ErrorCodeOf(err); ok {
		return applicationErr(uint64(code), err.Error())
	}
	return applicationErr(uint64(CodeInternal), err.Error())
}

func buildClosePayload(c *Conn, err error) ([]byte, error) {
	maxPayload := DefaultSettings().MaxControlPayloadBytes
	if c != nil && c.config.peer.Settings.MaxControlPayloadBytes != 0 {
		maxPayload = c.config.peer.Settings.MaxControlPayloadBytes
	}
	return buildClosePayloadWithMaxPayload(err, maxPayload)
}

func buildClosePayloadWithMaxPayload(err error, maxPayload uint64) ([]byte, error) {
	if err == nil {
		return buildCodePayload(uint64(CodeNoError), "", maxPayload)
	}
	appErr := closeMappedApplicationError(err)
	return buildCodePayload(appErr.Code, appErr.Reason, maxPayload)
}

func establishmentCloseMaxPayload(local Preface, peer *Preface) uint64 {
	switch {
	case peer != nil && peer.Settings.MaxControlPayloadBytes != 0:
		return peer.Settings.MaxControlPayloadBytes
	case local.Settings.MaxControlPayloadBytes != 0:
		return local.Settings.MaxControlPayloadBytes
	default:
		return DefaultSettings().MaxControlPayloadBytes
	}
}

func buildEstablishmentCloseFrame(local Preface, peer *Preface, err error) ([]byte, error) {
	payload, payloadErr := buildClosePayloadWithMaxPayload(err, establishmentCloseMaxPayload(local, peer))
	frame := Frame{
		Type:    FrameTypeCLOSE,
		Payload: payload,
	}
	encoded, frameErr := frame.MarshalBinary()
	if payloadErr != nil {
		return nil, payloadErr
	}
	if frameErr != nil {
		return nil, frameErr
	}
	return encoded, nil
}

func emitEstablishmentClose(conn io.Writer, local Preface, peer *Preface, err error) error {
	if conn == nil {
		return nil
	}
	frame, frameErr := buildEstablishmentCloseFrame(local, peer, err)
	if frameErr != nil {
		return frameErr
	}
	return rt.WriteAll(conn, frame)
}

// establishmentCloseDrainDelay gives fatal pre-ready CLOSE a short transport
// drain window on transports without a half-close primitive. Without this,
// immediate full close can race the peer's first post-preface read and drop the
// only fatal frame establishing state is allowed to emit.
func establishmentCloseDrainDelay(err error) time.Duration {
	if err == nil {
		return 0
	}
	return 10 * time.Millisecond
}

const establishmentFailureWriteWait = 250 * time.Millisecond

// establishmentSuccessWriteWait bounds how long a successful handshake waits
// for the already-started local preface write to finish after the peer preface
// has been parsed. The preface is intentionally tiny; without a bound, a peer
// that writes its own preface but never drains ours can stall Client/Server/New
// indefinitely.
const establishmentSuccessWriteWait = time.Second

var errEstablishmentPrefaceWriteTimeout = errors.New("local preface write stalled during establishment")
var errEstablishmentPrefaceReadTimeout = errors.New("peer preface read stalled during establishment")

type writeDeadlineSetter interface {
	SetWriteDeadline(time.Time) error
}

type readDeadlineSetter interface {
	SetReadDeadline(time.Time) error
}

type establishmentWriteDeadline struct {
	setter writeDeadlineSetter
	armed  bool
}

type establishmentReadDeadline struct {
	setter readDeadlineSetter
	armed  bool
}

func beginEstablishmentWriteDeadline(conn io.ReadWriteCloser, timeout time.Duration) establishmentWriteDeadline {
	if timeout <= 0 || conn == nil {
		return establishmentWriteDeadline{}
	}
	setter, ok := conn.(writeDeadlineSetter)
	if !ok {
		return establishmentWriteDeadline{}
	}
	if err := setter.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return establishmentWriteDeadline{}
	}
	return establishmentWriteDeadline{setter: setter, armed: true}
}

func beginEstablishmentReadDeadline(conn io.ReadWriteCloser, timeout time.Duration) establishmentReadDeadline {
	if timeout <= 0 || conn == nil {
		return establishmentReadDeadline{}
	}
	setter, ok := conn.(readDeadlineSetter)
	if !ok {
		return establishmentReadDeadline{}
	}
	if err := setter.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return establishmentReadDeadline{}
	}
	return establishmentReadDeadline{setter: setter, armed: true}
}

func (d establishmentWriteDeadline) clear() error {
	if !d.armed || d.setter == nil {
		return nil
	}
	return d.setter.SetWriteDeadline(time.Time{})
}

func (d establishmentReadDeadline) clear() error {
	if !d.armed || d.setter == nil {
		return nil
	}
	return d.setter.SetReadDeadline(time.Time{})
}

func (d establishmentWriteDeadline) expedite() {
	if !d.armed || d.setter == nil {
		return
	}
	_ = d.setter.SetWriteDeadline(time.Now())
}

func normalizeEstablishmentWriteErr(err error, deadline establishmentWriteDeadline) error {
	if err == nil {
		return nil
	}
	if deadline.armed && errors.Is(err, os.ErrDeadlineExceeded) {
		return errEstablishmentPrefaceWriteTimeout
	}
	return err
}

func normalizeEstablishmentReadErr(err error, deadline establishmentReadDeadline) error {
	if err == nil {
		return nil
	}
	if deadline.armed && errors.Is(err, os.ErrDeadlineExceeded) {
		return errEstablishmentPrefaceReadTimeout
	}
	return err
}

func waitEstablishmentWrite(writeErrCh <-chan error, timeout time.Duration) error {
	if writeErrCh == nil {
		return nil
	}
	if timeout <= 0 {
		return <-writeErrCh
	}
	timer := time.NewTimer(timeout)
	defer stopTimer(timer)
	select {
	case err := <-writeErrCh:
		return err
	case <-timer.C:
		return errEstablishmentPrefaceWriteTimeout
	}
}

func closeAfterEstablishmentFailure(conn io.ReadWriteCloser, local Preface, peer *Preface, err error) {
	if conn == nil {
		return
	}
	if err != nil {
		closeDeadline := beginEstablishmentWriteDeadline(conn, establishmentFailureWriteWait)
		if closeDeadline.armed {
			emitErr := emitEstablishmentClose(conn, local, peer, err)
			_ = closeDeadline.clear()
			if emitErr == nil {
				if delay := establishmentCloseDrainDelay(err); delay > 0 {
					time.Sleep(delay)
				}
			}
		}
	}
	_ = conn.Close()
}

func finishEstablishmentFailure(conn io.ReadWriteCloser, writeErrCh <-chan error, writeDeadline establishmentWriteDeadline, local Preface, peer *Preface, err error) {
	if conn == nil {
		return
	}
	writeDeadline.expedite()
	if writeErr := waitEstablishmentWrite(writeErrCh, establishmentFailureWriteWait); writeErr == nil {
		_ = writeDeadline.clear()
		closeAfterEstablishmentFailure(conn, local, peer, err)
		return
	}
	_ = conn.Close()
}

func (c *Conn) closeClosedChLocked() {
	if c.lifecycle.closedCh == nil {
		c.lifecycle.closedCh = make(chan struct{})
	}
	defer func() {
		_ = recover()
	}()
	close(c.lifecycle.closedCh)
}

func (c *Conn) closeTerminalChLocked() {
	if c.lifecycle.terminalCh == nil {
		c.lifecycle.terminalCh = make(chan struct{})
	}
	defer func() {
		_ = recover()
	}()
	close(c.lifecycle.terminalCh)
}

func (c *Conn) emitCloseFrame(payload []byte) closeFrameSendResult {
	if c == nil || len(payload) == 0 {
		return closeFrameSendResult{}
	}
	lane := c.writer.urgentWriteCh
	if lane == nil {
		lane = c.writer.writeCh
	}
	if lane == nil || c.lifecycle.closedCh == nil {
		return closeFrameSendResult{}
	}

	req := writeRequest{
		frames: []txFrame{flatTxFrame(Frame{Type: FrameTypeCLOSE, Payload: payload})},
		done:   make(chan error, 1),
	}

	deadline := time.Now().Add(c.closeFrameSendTimeout())
	if !rt.SendByDeadline(deadline, c.lifecycle.closedCh, lane, req) {
		c.mu.Lock()
		c.metrics.closeFrameAdmissionTO = saturatingAdd(c.metrics.closeFrameAdmissionTO, 1)
		c.mu.Unlock()
		return closeFrameSendResult{timedOut: true}
	}
	result := closeFrameSendResult{admitted: true}
	timer := time.NewTimer(time.Until(deadline))
	defer stopTimer(timer)
	select {
	case err := <-req.done:
		result.completed = true
		result.err = err
		if err != nil {
			c.mu.Lock()
			c.metrics.closeFrameFlushErr = saturatingAdd(c.metrics.closeFrameFlushErr, 1)
			c.mu.Unlock()
		}
	case <-c.lifecycle.closedCh:
		if err := c.err(); err != nil {
			result.err = err
		} else {
			result.err = ErrSessionClosed
		}
	case <-timer.C:
		result.timedOut = true
		c.mu.Lock()
		c.metrics.closeFrameFlushTO = saturatingAdd(c.metrics.closeFrameFlushTO, 1)
		c.mu.Unlock()
	}
	return result
}

func closeSessionStreamErr(finalState connState, err error) *ApplicationError {
	if state.IsBenignSessionError(finalState, err, ErrSessionClosed) {
		return nil
	}
	return closeMappedApplicationError(err)
}

func (c *Conn) releaseSendLocked(stream *nativeStream) {
	if stream == nil || stream.sendSent == 0 {
		return
	}
	prevSessionCredit := csub(c.flow.sendSessionMax, c.flow.sendSessionUsed)
	prevStreamCredit := csub(stream.sendMax, stream.sendSent)
	c.flow.sendSessionUsed = csub(c.flow.sendSessionUsed, stream.sendSent)
	stream.sendSent = 0
	sessionWake := prevSessionCredit == 0 && csub(c.flow.sendSessionMax, c.flow.sendSessionUsed) > 0
	streamWake := prevStreamCredit == 0 && csub(stream.sendMax, stream.sendSent) > 0
	if sessionWake {
		c.clearSessionBlockedStateLocked()
		c.dropPendingSessionControlLocked(sessionControlBlocked)
	}
	if streamWake {
		stream.clearBlockedState()
	}
	if sessionWake {
		c.broadcastWriteWakeLocked()
	} else if streamWake {
		notify(stream.writeNotify)
	}
}

func (c *Conn) planAbortLiveStreamLocked(stream *nativeStream, code ErrorCode, reason string) (txFrame, error) {
	if c == nil || stream == nil {
		return txFrame{}, nil
	}

	c.markPeerVisibleLocked(stream)
	appErr := applicationErr(uint64(code), reason)
	c.noteAbortReasonLocked(appErr.Code)
	stream.setAbortedWithSource(appErr, terminalAbortLocal)
	if err := c.recordLocalAbortTerminalChurnLocked(stream, time.Now(), "queue ABORT"); err != nil {
		return txFrame{}, err
	}
	c.finalizeTerminalStreamLocked(stream, transientStreamReleaseOptions{
		send:    true,
		receive: streamReceiveReleaseAndClearReadBuf,
	}, streamNotifyBoth, true)

	payload, payloadErr := buildCodePayload(uint64(code), reason, c.config.peer.Settings.MaxControlPayloadBytes)
	if payloadErr != nil {
		return txFrame{}, wireError(CodeInternal, "queue ABORT", payloadErr)
	}
	return flatTxFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: stream.id,
		Payload:  payload,
	}), nil
}

type sessionCloseOptions struct {
	abortSource terminalAbortSource
	finalize    bool
}

func (s *nativeStream) applySessionCloseStateLocked(appErr *ApplicationError, source terminalAbortSource) {
	if s == nil {
		return
	}
	plan := state.SessionCloseTransition(
		s.localSend,
		s.localReceive,
		s.effectiveSendHalfStateLocked(),
		s.effectiveRecvHalfStateLocked(),
		appErr != nil,
	)
	if plan.FinishSend {
		s.setSendFin()
	}
	if plan.FinishRecv {
		s.setRecvFin()
	}
	if plan.AbortSend {
		s.setSendAbortWithSource(appErr, source)
	}
	if plan.AbortRecv {
		s.setRecvAbortWithSource(appErr, source)
	}
}

func (c *Conn) closeStreamOnSessionWithOptionsLocked(stream *nativeStream, appErr *ApplicationError, opts sessionCloseOptions) {
	if stream == nil {
		return
	}
	if appErr != nil {
		c.noteAbortReasonLocked(appErr.Code)
	}
	stream.applySessionCloseStateLocked(appErr, opts.abortSource)
	releaseOpts := transientStreamReleaseOptions{}
	if opts.finalize {
		releaseOpts.queuedWake = queuedDataWakeOnRelease
	}
	releaseOpts.send = true
	releaseOpts.receive = streamReceiveReleaseAndClearReadBuf
	c.finalizeTerminalStreamLocked(stream, releaseOpts, streamNotifyBoth, opts.finalize)
}

func (c *Conn) releaseAllStreamsForSessionCloseLocked(sessionErr *ApplicationError) {
	source := terminalAbortLocal
	if sessionErr != nil &&
		c != nil &&
		c.sessionControl.peerCloseErr != nil &&
		sessionErr.Code == c.sessionControl.peerCloseErr.Code &&
		sessionErr.Reason == c.sessionControl.peerCloseErr.Reason {
		source = terminalAbortFromPeer
	}
	c.forEachKnownStreamLocked(func(stream *nativeStream) {
		c.closeStreamOnSessionWithOptionsLocked(stream, sessionErr, sessionCloseOptions{
			abortSource: source,
			finalize:    false,
		})
		stream.clearQueueMembershipState()
	})

	c.resetLiveStreamsLocked()
	c.clearProvisionalQueuesLocked()
	c.clearUnseenLocalQueuesLocked()
	c.clearAcceptQueuesLocked()
	c.clearPendingNonCloseControlStateLocked()
	c.clearPendingGoAwayLocked()
	c.sessionControl.pendingGoAwayBidi = 0
	c.sessionControl.pendingGoAwayUni = 0
	c.sessionControl.goAwaySendActive = false
	c.clearWriteQueueReservationsLocked()
	c.clearWriteBatchStateLocked()
	c.registry.activeLocalBidi = 0
	c.registry.activeLocalUni = 0
	c.registry.activePeerBidi = 0
	c.registry.activePeerUni = 0

	c.liveness.keepaliveInterval = 0
	c.liveness.keepaliveMaxPingInterval = 0
	c.liveness.readIdlePingDueAt = time.Time{}
	c.liveness.writeIdlePingDueAt = time.Time{}
	c.liveness.maxPingDueAt = time.Time{}
	c.liveness.pingOutstanding = false
	c.liveness.pingPayload = nil
	c.liveness.canceledPing = pingPayloadFingerprint{}
	c.liveness.lastPingSentAt = time.Time{}
	c.liveness.lastPongAt = time.Time{}
	c.liveness.lastPingRTT = 0
	if c.liveness.pingDone != nil {
		close(c.liveness.pingDone)
		c.liveness.pingDone = nil
	}
}

const sessionGoAwayDrainInterval = 10 * time.Millisecond
const sessionGoAwayDrainIntervalMax = 250 * time.Millisecond
const sessionGracefulCloseDrainTimeout = 500 * time.Millisecond
const sessionGracefulCloseDrainTimeoutMax = 5 * time.Second
const stopSendingAdaptiveDrainWindowMax = 2 * time.Second
const rttAdaptiveSlack = 50 * time.Millisecond
const defaultKeepaliveTimeoutMin = 5 * time.Second
const defaultKeepaliveTimeoutMax = 60 * time.Second
const writerLaneBuffer = 128
const advisoryLaneBuffer = 32

func acceptedPeerGoAwayWatermark(localRole Role, arity streamArity, nextPeerID uint64) uint64 {
	firstPeerID := state.FirstPeerStreamID(localRole, arity.isBidi())
	if nextPeerID <= firstPeerID {
		return 0
	}
	return nextPeerID - 4
}

func maxPeerGoAwayWatermark(localRole Role, arity streamArity) uint64 {
	firstPeerID := state.FirstPeerStreamID(localRole, arity.isBidi())
	if firstPeerID == 0 || firstPeerID > MaxVarint62 {
		return 0
	}
	return firstPeerID + ((MaxVarint62-firstPeerID)/4)*4
}

func effectiveGoAwaySendWatermark(localRole Role, arity streamArity, watermark uint64) uint64 {
	if watermark == MaxVarint62 {
		return maxPeerGoAwayWatermark(localRole, arity)
	}
	return watermark
}

func minGoAwayWatermark(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

type goAwayInitialPolicy uint8

const (
	goAwayInitialSkip goAwayInitialPolicy = iota
	goAwayInitialSend
)

func (p goAwayInitialPolicy) sendsInitial() bool {
	return p == goAwayInitialSend
}

func (c *Conn) closeWithGoAwayAndClose(initialPolicy goAwayInitialPolicy, initialBidi, initialUni uint64) error {
	if c == nil {
		return nil
	}
	var drainErr error
	c.mu.Lock()
	closeFrameOutstanding := c.closeFrameOutstandingLocked()
	c.mu.Unlock()

	if initialPolicy.sendsInitial() && !closeFrameOutstanding {
		if err := c.GoAway(initialBidi, initialUni); err != nil {
			return err
		}
		timer := time.NewTimer(c.goAwayDrainInterval())
		if c.lifecycle.closedCh != nil {
			select {
			case <-c.lifecycle.closedCh:
			case <-timer.C:
			}
		} else {
			<-timer.C
		}
		timer.Stop()
	}

	c.mu.Lock()
	finalBidi := minGoAwayWatermark(c.sessionControl.localGoAwayBidi, acceptedPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityBidi, c.registry.nextPeerBidi))
	finalUni := minGoAwayWatermark(c.sessionControl.localGoAwayUni, acceptedPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityUni, c.registry.nextPeerUni))
	currentBidi, currentUni := c.sessionControl.localGoAwayBidi, c.sessionControl.localGoAwayUni
	closeFrameOutstanding = closeFrameOutstanding || c.closeFrameOutstandingLocked()
	c.mu.Unlock()

	if !closeFrameOutstanding && (finalBidi != currentBidi || finalUni != currentUni) {
		if err := c.GoAway(finalBidi, finalUni); err != nil {
			return err
		}
	}
	c.mu.Lock()
	if c.lifecycle.closeErr != nil {
		err := sessionOperationErrLocked(c, OperationClose, visibleSessionErrLocked(c, c.lifecycle.closeErr))
		c.mu.Unlock()
		return err
	}
	c.reclaimGracefulCloseLocalStreamsLocked()
	c.mu.Unlock()
	if err := c.waitForGracefulCloseDrain(c.gracefulCloseDrainTimeout()); err != nil {
		if !errors.Is(err, ErrGracefulCloseTimeout) {
			return err
		}
		drainErr = err
	}

	c.mu.Lock()
	if c.lifecycle.closeErr != nil {
		err := sessionOperationErrLocked(c, OperationClose, visibleSessionErrLocked(c, c.lifecycle.closeErr))
		c.mu.Unlock()
		return err
	}
	c.beginSessionClosingLocked()
	c.shutdown.gracefulCloseActive = false
	if closeFrameOutstanding || c.closeFrameOutstandingLocked() {
		c.mu.Unlock()
		return drainErr
	}
	c.shutdown.closeFramePending = true
	c.mu.Unlock()

	closePayload, closePayloadErr := buildClosePayload(c, nil)
	if closePayloadErr != nil {
		c.mu.Lock()
		c.finishCloseFrameEnqueueLocked(false)
		c.mu.Unlock()
		return closeOperationErr(c, closePayloadErr)
	}
	sendResult := c.emitCloseFrame(closePayload)
	if !sendResult.admitted {
		c.mu.Lock()
		c.finishCloseFrameEnqueueLocked(false)
		c.mu.Unlock()
		if sendResult.timedOut {
			return closeOperationErr(c, ErrGracefulCloseTimeout)
		}
		return closeOperationErr(c, ErrSessionClosed)
	}
	c.mu.Lock()
	c.finishCloseFrameEnqueueLocked(sendResult.sent())
	c.mu.Unlock()
	if sendResult.err != nil {
		return closeOperationErr(c, sendResult.err)
	}
	if !sendResult.completed {
		return closeOperationErr(c, ErrGracefulCloseTimeout)
	}

	return drainErr
}

func (c *Conn) GoAway(lastAcceptedBidi, lastAcceptedUni uint64) error {
	return c.GoAwayWithError(lastAcceptedBidi, lastAcceptedUni, uint64(CodeNoError), "")
}

func (c *Conn) GoAwayWithError(lastAcceptedBidi, lastAcceptedUni, code uint64, reason string) error {
	if c == nil {
		return ErrSessionClosed
	}
	c.mu.Lock()
	if !c.publicWritePathReadyLocked() {
		c.mu.Unlock()
		return closeOperationErr(c, ErrSessionClosed)
	}
	c.mu.Unlock()
	if err := validateGoAwayWatermarkForDirection(lastAcceptedBidi, true); err != nil {
		return closeOperationErr(c, wireError(CodeProtocol, "send GOAWAY", err))
	}
	if err := validateGoAwayWatermarkCreator(c.config.negotiated.PeerRole, lastAcceptedBidi); err != nil {
		return closeOperationErr(c, wireError(CodeProtocol, "send GOAWAY", err))
	}
	if err := validateGoAwayWatermarkForDirection(lastAcceptedUni, false); err != nil {
		return closeOperationErr(c, wireError(CodeProtocol, "send GOAWAY", err))
	}
	if err := validateGoAwayWatermarkCreator(c.config.negotiated.PeerRole, lastAcceptedUni); err != nil {
		return closeOperationErr(c, wireError(CodeProtocol, "send GOAWAY", err))
	}

	payload, err := buildGoAwayPayload(lastAcceptedBidi, lastAcceptedUni, code, "")
	if err != nil {
		return closeOperationErr(c, err)
	}
	if reason != "" {
		payload = appendDebugTextTLVCapped(payload, reason, c.config.peer.Settings.MaxControlPayloadBytes)
	}

	c.mu.Lock()
	if lastAcceptedBidi > c.sessionControl.localGoAwayBidi || lastAcceptedUni > c.sessionControl.localGoAwayUni {
		if (c.sessionControl.hasSentGoAway &&
			c.sessionControl.sentGoAwayBidi <= lastAcceptedBidi &&
			c.sessionControl.sentGoAwayUni <= lastAcceptedUni) ||
			(c.sessionControl.hasPendingGoAway &&
				c.sessionControl.pendingGoAwayBidi <= lastAcceptedBidi &&
				c.sessionControl.pendingGoAwayUni <= lastAcceptedUni) ||
			c.sessionControl.goAwaySendActive {
			c.mu.Unlock()
			c.notifyControlAndLiveness()
			return c.waitForLocalGoAwaySend(lastAcceptedBidi, lastAcceptedUni)
		}
		c.mu.Unlock()
		return closeOperationErr(c, wireError(CodeProtocol, "send GOAWAY", fmt.Errorf("GOAWAY watermarks must be non-increasing")))
	}
	if c.lifecycle.closeErr != nil {
		err := visibleSessionErrLocked(c, c.lifecycle.closeErr)
		c.mu.Unlock()
		return sessionOperationErrLocked(c, OperationClose, err)
	}
	if !c.allowLocalNonCloseControlLocked() {
		err := visibleSessionErrLocked(c, ErrSessionClosed)
		c.mu.Unlock()
		return sessionOperationErrLocked(c, OperationClose, err)
	}
	c.sessionControl.localGoAwayBidi = lastAcceptedBidi
	c.sessionControl.localGoAwayUni = lastAcceptedUni
	c.advanceSessionOnLocalGoAwayLocked(true)
	if c.sessionControl.hasSentGoAway &&
		c.sessionControl.sentGoAwayBidi <= lastAcceptedBidi &&
		c.sessionControl.sentGoAwayUni <= lastAcceptedUni {
		c.mu.Unlock()
		c.notifyControlAndLiveness()
		return nil
	}
	c.sessionControl.pendingGoAwayBidi = lastAcceptedBidi
	c.sessionControl.pendingGoAwayUni = lastAcceptedUni
	if !c.setPendingGoAwayPayloadLocked(payload) {
		c.mu.Unlock()
		return closeOperationErr(c, ErrSessionClosed)
	}
	sender := !c.sessionControl.goAwaySendActive
	if sender {
		c.sessionControl.goAwaySendActive = true
	}
	c.mu.Unlock()
	c.notifyControlAndLiveness()

	if sender {
		return c.flushPendingLocalGoAway()
	}
	return c.waitForLocalGoAwaySend(lastAcceptedBidi, lastAcceptedUni)
}

// PeerGoAwayError returns the last peer GOAWAY error, if any.
func (c *Conn) PeerGoAwayError() *ApplicationError {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionControl.peerGoAwayErr == nil {
		return nil
	}
	return cloneApplicationError(c.sessionControl.peerGoAwayErr)
}

func (c *Conn) waitForLocalGoAwaySend(lastAcceptedBidi, lastAcceptedUni uint64) error {
	if c == nil {
		return ErrSessionClosed
	}
	for {
		c.mu.Lock()
		if c.lifecycle.closeErr != nil {
			err := sessionOperationErrLocked(c, OperationClose, visibleSessionErrLocked(c, c.lifecycle.closeErr))
			c.mu.Unlock()
			return err
		}
		sent := c.sessionControl.hasSentGoAway &&
			c.sessionControl.sentGoAwayBidi <= lastAcceptedBidi &&
			c.sessionControl.sentGoAwayUni <= lastAcceptedUni
		pending := c.sessionControl.hasPendingGoAway &&
			c.sessionControl.pendingGoAwayBidi <= lastAcceptedBidi &&
			c.sessionControl.pendingGoAwayUni <= lastAcceptedUni
		if sent {
			c.mu.Unlock()
			return nil
		}
		if !sent && !pending && !c.sessionControl.goAwaySendActive {
			if c.lifecycle.closeErr != nil {
				err := visibleSessionErrLocked(c, c.lifecycle.closeErr)
				c.mu.Unlock()
				return sessionOperationErrLocked(c, OperationClose, err)
			}
			if !c.allowLocalNonCloseControlLocked() {
				err := visibleSessionErrLocked(c, ErrSessionClosed)
				c.mu.Unlock()
				return sessionOperationErrLocked(c, OperationClose, err)
			}
		}
		stateWakeCh := c.currentStateWakeLocked()
		closedCh := c.lifecycle.closedCh
		c.mu.Unlock()

		select {
		case <-closedCh:
		case <-stateWakeCh:
		}
	}
}

func (c *Conn) flushPendingLocalGoAway() error {
	if c == nil {
		return ErrSessionClosed
	}

	for {
		c.mu.Lock()
		if c.lifecycle.closeErr != nil {
			err := sessionOperationErrLocked(c, OperationClose, visibleSessionErrLocked(c, c.lifecycle.closeErr))
			c.sessionControl.goAwaySendActive = false
			c.mu.Unlock()
			c.notifyControlAndLiveness()
			return err
		}
		if !c.sessionControl.hasPendingGoAway {
			c.sessionControl.goAwaySendActive = false
			c.mu.Unlock()
			c.notifyControlAndLiveness()
			return nil
		}
		if c.lifecycle.closeErr != nil {
			err := visibleSessionErrLocked(c, c.lifecycle.closeErr)
			c.clearPendingGoAwayLocked()
			c.sessionControl.goAwaySendActive = false
			c.mu.Unlock()
			c.notifyControlAndLiveness()
			return sessionOperationErrLocked(c, OperationClose, err)
		}
		if !c.allowLocalNonCloseControlLocked() {
			err := visibleSessionErrLocked(c, ErrSessionClosed)
			c.clearPendingGoAwayLocked()
			c.sessionControl.goAwaySendActive = false
			c.mu.Unlock()
			c.notifyControlAndLiveness()
			return sessionOperationErrLocked(c, OperationClose, err)
		}

		lastAcceptedBidi := c.sessionControl.pendingGoAwayBidi
		lastAcceptedUni := c.sessionControl.pendingGoAwayUni
		if c.sessionControl.hasSentGoAway && c.sessionControl.sentGoAwayBidi == lastAcceptedBidi && c.sessionControl.sentGoAwayUni == lastAcceptedUni {
			c.clearPendingGoAwayLocked()
			c.mu.Unlock()
			c.notifyControlAndLiveness()
			continue
		}

		payload := c.sessionControl.pendingGoAwayPayload
		c.clearPendingGoAwayLocked()
		c.mu.Unlock()

		if err := c.queueImmutableFrame(flatTxFrame(Frame{Type: FrameTypeGOAWAY, Payload: payload})); err != nil {
			c.mu.Lock()
			c.sessionControl.goAwaySendActive = false
			if c.lifecycle.closeErr != nil {
				err = visibleSessionErrLocked(c, c.lifecycle.closeErr)
			}
			c.mu.Unlock()
			c.notifyControlAndLiveness()
			return sessionOperationErrLocked(c, OperationClose, err)
		}

		c.mu.Lock()
		c.sessionControl.hasSentGoAway = true
		c.sessionControl.sentGoAwayBidi = lastAcceptedBidi
		c.sessionControl.sentGoAwayUni = lastAcceptedUni
		c.mu.Unlock()
		c.notifyControlAndLiveness()
	}
}

func (c *Conn) waitForGracefulCloseDrain(timeout time.Duration) error {
	if c == nil {
		return ErrSessionClosed
	}
	if timeout <= 0 {
		timeout = sessionGracefulCloseDrainTimeout
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		c.mu.Lock()
		if c.lifecycle.closeErr != nil {
			err := sessionOperationErrLocked(c, OperationClose, visibleSessionErrLocked(c, c.lifecycle.closeErr))
			c.mu.Unlock()
			return err
		}
		if !c.hasGracefulCloseLiveStreamsLocked() && c.totalProvisionalCountLocked() == 0 {
			c.mu.Unlock()
			return nil
		}
		stateWakeCh := c.currentStateWakeLocked()
		closedCh := c.lifecycle.closedCh
		c.mu.Unlock()

		select {
		case <-closedCh:
		case <-stateWakeCh:
		case <-timer.C:
			return ErrGracefulCloseTimeout
		}
	}
}

// DuplexByteStream is the minimal transport shape accepted by session constructors.
type DuplexByteStream interface {
	io.Reader
	io.Writer
	io.Closer
}

var closedSignalCh = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// New establishes a native zmux session.
func New(conn io.ReadWriteCloser, cfg *Config) (*Conn, error) {
	return establish(conn, cloneConfig(cfg))
}

// Client establishes a native initiator session.
func Client(conn io.ReadWriteCloser, cfg *Config) (*Conn, error) {
	c := cloneConfig(cfg)
	c.Role = RoleInitiator
	c.TieBreakerNonce = 0
	return establish(conn, c)
}

// Server establishes a native responder session.
func Server(conn io.ReadWriteCloser, cfg *Config) (*Conn, error) {
	c := cloneConfig(cfg)
	c.Role = RoleResponder
	c.TieBreakerNonce = 0
	return establish(conn, c)
}

// connReadBufferSize bounds per-connection buffered-reader residency.
// The frame path already allocates exact-sized frame buffers, so the reader
// only needs enough space to amortize byte-wise varint/preface parsing.
const connReadBufferSize = 512

func establish(conn io.ReadWriteCloser, cfg Config) (*Conn, error) {
	if conn == nil {
		return nil, ErrNilConn
	}
	local, err := cfg.LocalPreface()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	payload, err := local.MarshalBinary()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	reader := bufio.NewReaderSize(conn, connReadBufferSize)
	writeErrCh := make(chan error, 1)
	writeDeadline := beginEstablishmentWriteDeadline(conn, establishmentSuccessWriteWait)
	readDeadline := beginEstablishmentReadDeadline(conn, establishmentSuccessWriteWait)
	go func() { writeErrCh <- rt.WriteAll(conn, payload) }()

	peer, readErr := ReadPreface(reader)
	if readErr != nil {
		if readErr = normalizeEstablishmentReadErr(readErr, readDeadline); errors.Is(readErr, errEstablishmentPrefaceReadTimeout) {
			readErr = wireError(CodeInternal, "read preface", readErr)
		}
		finishEstablishmentFailure(conn, writeErrCh, writeDeadline, local, nil, readErr)
		return nil, readErr
	}
	if err := readDeadline.clear(); err != nil {
		finishEstablishmentFailure(conn, writeErrCh, writeDeadline, local, &peer, err)
		return nil, err
	}

	negotiated, err := NegotiatePrefaces(local, peer)
	if err != nil {
		finishEstablishmentFailure(conn, writeErrCh, writeDeadline, local, &peer, err)
		return nil, err
	}
	if writeErr := normalizeEstablishmentWriteErr(waitEstablishmentWrite(writeErrCh, establishmentSuccessWriteWait), writeDeadline); writeErr != nil {
		_ = conn.Close()
		if errors.Is(writeErr, errEstablishmentPrefaceWriteTimeout) {
			return nil, wireError(CodeInternal, "write preface", writeErr)
		}
		return nil, writeErr
	}
	if err := writeDeadline.clear(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	now := time.Now()
	var advisoryWriteCh chan writeRequest
	if negotiated.Capabilities.SupportsPriorityUpdateCarriage() {
		advisoryWriteCh = make(chan writeRequest, advisoryLaneBuffer)
	}
	var livenessCh chan struct{}
	if cfg.KeepaliveInterval > 0 {
		livenessCh = make(chan struct{}, 1)
	}
	c := &Conn{
		io: connIOState{
			conn:            conn,
			reader:          reader,
			scatterGatherOK: transportSupportsScatterGather(conn),
		},
		config: connConfigState{
			local:       local,
			peer:        peer,
			localLimits: normalizeLimits(local.Settings.Limits()),
			peerLimits:  normalizeLimits(peer.Settings.Limits()),
			negotiated:  negotiated,
		},
		flow: connFlowState{
			recvSessionAdvertised: local.Settings.InitialMaxData,
			sendSessionMax:        peer.Settings.InitialMaxData,
			sessionMemoryCap:      cfg.SessionMemoryCap,
			perStreamDataHWM:      cfg.PerStreamQueuedDataHWM,
			sessionDataHWM:        cfg.SessionQueuedDataHWM,
			urgentQueueCap:        cfg.UrgentQueuedBytesCap,
		},
		queues: connQueueState{
			acceptBacklogLimit:      cfg.AcceptBacklogLimit,
			acceptBacklogBytesLimit: cfg.AcceptBacklogBytesLimit,
		},
		pending: connPendingControlState{
			controlNotify:         make(chan struct{}, 1),
			terminalNotify:        make(chan struct{}, 1),
			pendingControlBudget:  cfg.PendingControlBytesBudget,
			pendingPriorityBudget: cfg.PendingPriorityBytesBudget,
		},
		retention: connRetentionState{
			retainedOpenInfoBudget: cfg.RetainedOpenInfoBytesBudget,
			retainedReasonBudget:   cfg.RetainedPeerReasonBytesBudget,
			markerOnlyLimit:        cfg.MarkerOnlyUsedStreamLimit,
		},
		terminalPolicy: connTerminationPolicyState{
			stopSendingDrainWindow: cfg.StopSendingGracefulDrainWindow,
			stopSendingTailCap:     cfg.StopSendingGracefulTailCap,
			gracefulCloseTimeout:   cfg.GracefulCloseDrainTimeout,
		},
		writer: connWriterRuntimeState{
			scheduler:       rt.NewBatchScheduler(),
			writeCh:         make(chan writeRequest, writerLaneBuffer),
			advisoryWriteCh: advisoryWriteCh,
			urgentWriteCh:   make(chan writeRequest, 1),
		},
		lifecycle: connLifecycleState{
			sessionState: connStateReady,
			closedCh:     make(chan struct{}),
			terminalCh:   make(chan struct{}),
		},
		signals: connRuntimeSignalState{
			livenessCh: livenessCh,
		},
		liveness: connLivenessState{
			keepaliveInterval:        cfg.KeepaliveInterval,
			keepaliveMaxPingInterval: cfg.KeepaliveMaxPingInterval,
			keepaliveTimeout:         cfg.KeepaliveTimeout,
			keepaliveJitterState:     rt.InitKeepaliveJitterState(local.TieBreakerNonce ^ peer.TieBreakerNonce),
			pingNonceState:           rt.InitSessionNonceState((local.TieBreakerNonce << 1) ^ peer.TieBreakerNonce),
			lastInboundFrameAt:       now,
			lastControlProgressAt:    now,
			lastTransportWriteAt:     now,
		},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		observer: connObserverState{
			eventHandler: cfg.EventHandler,
		},
		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(local.Settings.MaxFramePayload),
		}, registry: connRegistryState{
			nextLocalBidi:  state.FirstLocalStreamID(negotiated.LocalRole, true),
			nextLocalUni:   state.FirstLocalStreamID(negotiated.LocalRole, false),
			nextPeerBidi:   state.FirstPeerStreamID(negotiated.LocalRole, true),
			nextPeerUni:    state.FirstPeerStreamID(negotiated.LocalRole, false),
			tombstoneLimit: cfg.TombstoneLimit,
		},
	}
	c.bootstrapRuntimeQueuesLocked()
	c.applyConfigRuntimePolicy(cfg)
	c.resetKeepaliveSchedulesLocked(now)
	keepaliveEnabled := c.liveness.keepaliveInterval > 0
	go c.writeLoop()
	go c.readLoop()
	if keepaliveEnabled {
		go c.keepaliveLoop()
	}
	return c, nil
}

func (c *Conn) LocalPreface() Preface {
	if c == nil {
		return Preface{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.local
}

func (c *Conn) PeerPreface() Preface {
	if c == nil {
		return Preface{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.peer
}

func (c *Conn) Negotiated() Negotiated {
	if c == nil {
		return Negotiated{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.negotiated
}

type connRetentionState struct {
	retainedOpenInfoBudget uint64
	retainedReasonBudget   uint64
	markerOnlyLimit        int

	retainedOpenInfoBytes   uint64
	retainedPeerReasonBytes uint64

	resetReasonCount    map[uint64]uint64
	abortReasonCount    map[uint64]uint64
	resetReasonOverflow uint64
	abortReasonOverflow uint64
}

type connWriterRuntimeState struct {
	scheduler       rt.BatchScheduler
	scratch         writeBatchScratch
	writeCh         chan writeRequest
	advisoryWriteCh chan writeRequest
	urgentWriteCh   chan writeRequest
	yieldOrdinary   bool
}

type connProtocolRuntimeState struct {
	startOnce sync.Once
	notifyCh  chan struct{}
	tasks     []protocolTask
}

type connIngressAccountingState struct {
	aggregateLateData     uint64
	aggregateLateDataCap  uint64
	lateDataPerStreamCap  uint64
	lateDataAfterClose    uint64
	lateDataAfterReset    uint64
	lateDataAfterAbort    uint64
	droppedPriorityUpdate uint64
	droppedLocalPriority  uint64
	readBufferOverhead    uint64

	hiddenStreamsRefused       uint64
	hiddenStreamsReaped        uint64
	hiddenUnreadBytesDiscarded uint64
	provisionalLimited         uint64
	provisionalExpired         uint64
}

type connRuntimeMetricsState struct {
	flushCount       uint64
	lastFlushAt      time.Time
	lastBatchFrames  int
	lastBatchBytes   int
	sendRateEstimate uint64
	blockedWriteTime time.Duration
	lastOpenLatency  time.Duration

	coalescedTerminalSignals  uint64
	droppedSupersededControls uint64
	skippedCloseOnDeadIO      uint64
	protocolBacklogBlocked    uint64
	closeFrameAdmissionTO     uint64
	closeFrameFlushTO         uint64
	closeFrameFlushErr        uint64
}

type connLivenessState struct {
	keepaliveInterval        time.Duration
	keepaliveMaxPingInterval time.Duration
	keepaliveTimeout         time.Duration
	keepaliveJitterState     uint64
	pingNonceState           uint64
	readIdlePingDueAt        time.Time
	writeIdlePingDueAt       time.Time
	maxPingDueAt             time.Time
	lastInboundFrameAt       time.Time
	lastControlProgressAt    time.Time
	lastTransportWriteAt     time.Time
	lastStreamProgressAt     time.Time
	lastAppProgressAt        time.Time
	lastPingSentAt           time.Time
	lastPongAt               time.Time
	lastPingRTT              time.Duration
	pingOutstanding          bool
	pingPayload              []byte
	canceledPing             pingPayloadFingerprint
	pingDone                 chan struct{}
}

type connSessionControlState struct {
	peerGoAwayBidi  uint64
	peerGoAwayUni   uint64
	localGoAwayBidi uint64
	localGoAwayUni  uint64
	peerGoAwayErr   *ApplicationError
	peerCloseErr    *ApplicationError
	peerCloseView   atomic.Pointer[ApplicationError]

	peerGoAwayReasonBytes uint64
	peerCloseReasonBytes  uint64

	sentGoAwayBidi uint64
	sentGoAwayUni  uint64
	hasSentGoAway  bool

	pendingGoAwayBidi    uint64
	pendingGoAwayUni     uint64
	pendingGoAwayPayload []byte
	hasPendingGoAway     bool
	goAwaySendActive     bool
}

type connShutdownRuntimeState struct {
	gracefulCloseActive bool
	closeFramePending   bool
	closeFrameSent      bool
	closeOnce           sync.Once
	asyncCloseOnce      sync.Once
}

type connLifecycleState struct {
	closedCh     chan struct{}
	terminalCh   chan struct{}
	sessionState connState
	closeErr     error
}

type connRuntimeSignalState struct {
	writeWakeCh  chan struct{}
	urgentWakeCh chan struct{}
	stateWakeCh  chan struct{}
	acceptCh     chan struct{}
	acceptBidiCh chan struct{}
	acceptUniCh  chan struct{}
	livenessCh   chan struct{}
}

type connIOState struct {
	conn            io.ReadWriteCloser
	reader          *bufio.Reader
	scatterGatherOK bool
}

type connObserverState struct {
	eventHandler EventHandler
}

type connTerminationPolicyState struct {
	stopSendingDrainWindow time.Duration
	stopSendingTailCap     uint64
	gracefulCloseTimeout   time.Duration
}

type connConfigState struct {
	local       Preface
	peer        Preface
	localLimits Limits
	peerLimits  Limits
	negotiated  Negotiated
}

type connFlowState struct {
	recvSessionAdvertised uint64
	recvSessionReceived   uint64
	recvSessionUsed       uint64
	recvSessionPending    uint64
	recvReplenishRetry    bool
	sendSessionMax        uint64
	sendSessionUsed       uint64
	queuedDataBytes       uint64
	queuedDataStreams     map[*nativeStream]struct{}
	advisoryQueuedBytes   uint64
	urgentQueuedBytes     uint64
	sessionMemoryCap      uint64
	perStreamDataHWM      uint64
	sessionDataHWM        uint64
	urgentQueueCap        uint64
}

type sparseQueueState[T any] struct {
	items []T
	head  int
	count int
	init  bool
}

type streamSparseQueueState = sparseQueueState[*nativeStream]

type connQueueState struct {
	provisionalBidi streamSparseQueueState
	provisionalUni  streamSparseQueueState

	unseenLocalBidi streamSparseQueueState
	unseenLocalUni  streamSparseQueueState

	acceptBidi      streamSparseQueueState
	acceptUni       streamSparseQueueState
	acceptBidiBytes uint64
	acceptUniBytes  uint64

	acceptBacklogLimit      int
	acceptBacklogBytesLimit uint64
	visibleAcceptRefused    uint64
}

type connAbuseState struct {
	abuseWindow             time.Duration
	hiddenAbortChurnWindow  time.Duration
	hiddenAbortChurnLimit   uint32
	visibleChurnWindow      time.Duration
	visibleChurnLimit       uint32
	controlFrameBudget      uint32
	controlByteBudget       uint64
	extFrameBudget          uint32
	extByteBudget           uint64
	mixedFrameBudget        uint32
	mixedByteBudget         uint64
	noOpControlFloodLimit   uint32
	noOpMaxDataFloodLimit   uint32
	noOpBlockedFloodLimit   uint32
	noOpZeroDataFloodLimit  uint32
	noOpPriorityFloodLimit  uint32
	groupRebucketFloodLimit uint32
	pingFloodLimit          uint32

	hiddenAbortWindowStart   windowStamp
	hiddenAbortCount         uint32
	visibleChurnWindowStart  windowStamp
	visibleChurnCount        uint32
	controlBudgetWindowStart windowStamp
	controlBudgetFrames      uint32
	controlBudgetBytes       uint64
	extBudgetWindowStart     windowStamp
	extBudgetFrames          uint32
	extBudgetBytes           uint64
	mixedBudgetWindowStart   windowStamp
	mixedBudgetFrames        uint32
	mixedBudgetBytes         uint64
	noopControlWindowStart   windowStamp
	noopControlCount         uint32
	noopMaxDataWindowStart   windowStamp
	noopMaxDataCount         uint32
	noopBlockedWindowStart   windowStamp
	noopBlockedCount         uint32
	noopDataWindowStart      windowStamp
	noopDataCount            uint32
	noopPriorityWindowStart  windowStamp
	noopPriorityCount        uint32
	groupRebucketWindowStart windowStamp
	groupRebucketCount       uint32
	pingFloodWindowStart     windowStamp
	pingFloodCount           uint32
}

type connRegistryState struct {
	streams              map[uint64]*nativeStream
	liveStreamCount      int
	liveStreamsInit      bool
	tombstones           map[uint64]streamTombstone
	tombstoneOrder       []uint64
	tombstoneHead        int
	tombstoneCount       int
	tombstonesInit       bool
	hiddenTombstoneOrder []uint64
	hiddenTombstoneHead  int
	hiddenTombstoneCount int
	usedStreamData       map[uint64]usedStreamMarker
	usedStreamRanges     []usedStreamRange
	usedStreamRangeMode  bool
	tombstoneLimit       int
	hiddenTombstonesInit bool

	nextLocalBidi uint64
	nextLocalUni  uint64
	nextPeerBidi  uint64
	nextPeerUni   uint64

	activeLocalBidi uint64
	activeLocalUni  uint64
	activePeerBidi  uint64
	activePeerUni   uint64

	nextVisibilitySeq uint64
}

// Conn is the native zmux session implementation.
type Conn struct {
	mu sync.Mutex

	io             connIOState
	observer       connObserverState
	pending        connPendingControlState
	config         connConfigState
	flow           connFlowState
	queues         connQueueState
	abuse          connAbuseState
	registry       connRegistryState
	retention      connRetentionState
	terminalPolicy connTerminationPolicyState
	writer         connWriterRuntimeState
	protocol       connProtocolRuntimeState
	ingress        connIngressAccountingState
	metrics        connRuntimeMetricsState
	liveness       connLivenessState
	lifecycle      connLifecycleState
	signals        connRuntimeSignalState
	sessionControl connSessionControlState
	shutdown       connShutdownRuntimeState
}

func (c *Conn) err() error {
	if c == nil {
		return ErrSessionClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lifecycle.closeErr
}

func notify(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func ensureNotifyChan(ch *chan struct{}) chan struct{} {
	if ch == nil {
		return nil
	}
	if *ch == nil {
		*ch = make(chan struct{}, 1)
	}
	return *ch
}

func (c *Conn) ensureLivenessNotifyLocked() chan struct{} {
	if c == nil {
		return nil
	}
	return ensureNotifyChan(&c.signals.livenessCh)
}

func (c *Conn) currentStateWakeLocked() <-chan struct{} {
	if c == nil {
		return nil
	}
	if c.signals.stateWakeCh == nil {
		c.signals.stateWakeCh = make(chan struct{})
	}
	return c.signals.stateWakeCh
}

func (c *Conn) broadcastStateWakeLocked() {
	if c == nil || c.signals.stateWakeCh == nil {
		return
	}
	close(c.signals.stateWakeCh)
	c.signals.stateWakeCh = make(chan struct{})
}

func (c *Conn) ensureAcceptNotifyLocked(arity streamArity) chan struct{} {
	if c == nil {
		return nil
	}
	ensureNotifyChan(&c.signals.acceptCh)
	if arity.isBidi() {
		return ensureNotifyChan(&c.signals.acceptBidiCh)
	}
	return ensureNotifyChan(&c.signals.acceptUniCh)
}

func (c *Conn) notifyControlAndLiveness() {
	if c == nil {
		return
	}
	notify(c.pending.controlNotify)
	notify(c.signals.livenessCh)
	c.mu.Lock()
	c.broadcastStateWakeLocked()
	c.mu.Unlock()
}

func (c *Conn) notifyControlAndLivenessLocked() {
	if c == nil {
		return
	}
	notify(c.pending.controlNotify)
	notify(c.signals.livenessCh)
	c.broadcastStateWakeLocked()
}

func (c *Conn) notifyAcceptControlAndLivenessLocked() {
	if c == nil {
		return
	}
	notify(c.signals.acceptCh)
	notify(c.signals.acceptBidiCh)
	notify(c.signals.acceptUniCh)
	c.notifyControlAndLivenessLocked()
}

func (c *Conn) clearSessionBlockedStateLocked() {
	if c == nil {
		return
	}
	c.pending.sessionBlockedAt = 0
	c.pending.sessionBlockedSet = false
}

func (c *Conn) markSessionBlockedStateLocked(v uint64) {
	if c == nil {
		return
	}
	c.pending.sessionBlockedAt = v
	c.pending.sessionBlockedSet = true
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func (c *Conn) applyConfigRuntimePolicy(cfg Config) {
	if c == nil {
		return
	}

	c.flow.sessionMemoryCap = cfg.SessionMemoryCap
	c.flow.perStreamDataHWM = cfg.PerStreamQueuedDataHWM
	c.flow.sessionDataHWM = cfg.SessionQueuedDataHWM
	c.flow.urgentQueueCap = cfg.UrgentQueuedBytesCap
	c.pending.pendingControlBudget = cfg.PendingControlBytesBudget
	c.pending.pendingPriorityBudget = cfg.PendingPriorityBytesBudget
	c.retention.retainedOpenInfoBudget = cfg.RetainedOpenInfoBytesBudget
	c.retention.retainedReasonBudget = cfg.RetainedPeerReasonBytesBudget
	c.retention.markerOnlyLimit = cfg.MarkerOnlyUsedStreamLimit
	c.terminalPolicy.stopSendingDrainWindow = cfg.StopSendingGracefulDrainWindow
	c.terminalPolicy.stopSendingTailCap = cfg.StopSendingGracefulTailCap
	c.terminalPolicy.gracefulCloseTimeout = cfg.GracefulCloseDrainTimeout
	c.queues.acceptBacklogLimit = cfg.AcceptBacklogLimit
	c.queues.acceptBacklogBytesLimit = cfg.AcceptBacklogBytesLimit
	c.registry.tombstoneLimit = cfg.TombstoneLimit
	c.abuse.abuseWindow = cfg.AbuseWindow
	c.abuse.hiddenAbortChurnWindow = cfg.HiddenAbortChurnWindow
	c.abuse.hiddenAbortChurnLimit = cfg.HiddenAbortChurnThreshold
	c.abuse.visibleChurnWindow = cfg.VisibleTerminalChurnWindow
	c.abuse.visibleChurnLimit = cfg.VisibleTerminalChurnThreshold
	c.abuse.controlFrameBudget = cfg.InboundControlFrameBudget
	c.abuse.controlByteBudget = cfg.InboundControlBytesBudget
	c.abuse.extFrameBudget = cfg.InboundExtFrameBudget
	c.abuse.extByteBudget = cfg.InboundExtBytesBudget
	c.abuse.mixedFrameBudget = cfg.InboundMixedFrameBudget
	c.abuse.mixedByteBudget = cfg.InboundMixedBytesBudget
	c.abuse.noOpControlFloodLimit = cfg.NoOpControlFloodThreshold
	c.abuse.noOpMaxDataFloodLimit = cfg.NoOpMaxDataFloodThreshold
	c.abuse.noOpBlockedFloodLimit = cfg.NoOpBlockedFloodThreshold
	c.abuse.noOpZeroDataFloodLimit = cfg.NoOpZeroDataFloodThreshold
	c.abuse.noOpPriorityFloodLimit = cfg.NoOpPriorityUpdateFloodThreshold
	c.abuse.groupRebucketFloodLimit = cfg.GroupRebucketChurnThreshold
	c.abuse.pingFloodLimit = cfg.InboundPingFloodThreshold

	if cfg.AggregateLateDataCap > 0 {
		c.ingress.aggregateLateDataCap = cfg.AggregateLateDataCap
	}
}

func (c *Conn) gracefulCloseDrainTimeout() time.Duration {
	if c == nil {
		return sessionGracefulCloseDrainTimeout
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminalPolicy.gracefulCloseTimeout > 0 {
		return c.terminalPolicy.gracefulCloseTimeout
	}
	return adaptiveRTTTimeout(c.liveness.lastPingRTT, sessionGracefulCloseDrainTimeout, sessionGracefulCloseDrainTimeoutMax, 4, 100*time.Millisecond)
}

func (c *Conn) closeFrameSendTimeout() time.Duration {
	if c == nil {
		return sessionCloseFrameSendTimeout
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return adaptiveRTTTimeout(c.liveness.lastPingRTT, sessionCloseFrameSendTimeout, sessionCloseFrameSendTimeoutMax, 4, rttAdaptiveSlack)
}

func (c *Conn) goAwayDrainInterval() time.Duration {
	if c == nil {
		return sessionGoAwayDrainInterval
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	interval := sessionGoAwayDrainInterval
	if c.liveness.lastPingRTT > 0 {
		if candidate := c.liveness.lastPingRTT / 4; candidate > interval {
			interval = candidate
		}
	}
	if interval > sessionGoAwayDrainIntervalMax {
		return sessionGoAwayDrainIntervalMax
	}
	return interval
}

func (c *Conn) stopSendingDrainWindow() time.Duration {
	if c == nil {
		return rt.StopSendingDrainWindow(0)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopSendingDrainWindowLocked()
}

func adaptiveRTTTimeout(rtt, base, max time.Duration, multiplier int, slack time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	timeout := base
	if rtt > 0 && multiplier > 0 {
		candidate := saturatingDurationMulAdd(rtt, multiplier, slack)
		if candidate > timeout {
			timeout = candidate
		}
	}
	if max > 0 && timeout > max {
		return max
	}
	return timeout
}

func saturatingDurationMulAdd(v time.Duration, multiplier int, add time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if v <= 0 || multiplier <= 0 {
		return 0
	}
	m := time.Duration(multiplier)
	if v > maxDuration/m {
		return maxDuration
	}
	out := v * m
	if add <= 0 {
		return out
	}
	if out > maxDuration-add {
		return maxDuration
	}
	return out + add
}

func (c *Conn) stopSendingDrainWindowLocked() time.Duration {
	if c == nil {
		return rt.StopSendingDrainWindow(0)
	}
	if c.terminalPolicy.stopSendingDrainWindow > 0 {
		return c.terminalPolicy.stopSendingDrainWindow
	}
	return adaptiveRTTTimeout(c.liveness.lastPingRTT, rt.StopSendingDrainWindow(0), stopSendingAdaptiveDrainWindowMax, 2, 0)
}

func (c *Conn) localLimitsView() Limits {
	if c == nil {
		return defaultNormalizedLimits
	}
	if c.config.localLimits != (Limits{}) {
		return c.config.localLimits
	}
	return normalizeLimits(c.config.local.Settings.Limits())
}

func (c *Conn) peerLimitsView() Limits {
	if c == nil {
		return defaultNormalizedLimits
	}
	if c.config.peerLimits != (Limits{}) {
		return c.config.peerLimits
	}
	return normalizeLimits(c.config.peer.Settings.Limits())
}

// bootstrapRuntimeQueuesLocked marks runtime-owned incremental queues as ready
// on freshly established sessions. Fixture-style tests that seed slices/maps
// directly can still rely on the zero-value lazy-sync path because they bypass
// establish() and therefore do not call this helper.
func (c *Conn) bootstrapRuntimeQueuesLocked() {
	if c == nil {
		return
	}
	c.queues.provisionalBidi.init = true
	c.queues.provisionalUni.init = true
	c.queues.acceptBidi.init = true
	c.queues.acceptUni.init = true
	c.queues.unseenLocalBidi.init = true
	c.queues.unseenLocalUni.init = true
	c.registry.tombstonesInit = true
	c.registry.hiddenTombstonesInit = true
	for i := range c.pending.streamQueues {
		c.pending.streamQueues[i].init = true
	}
}

const minSessionMemoryHardCap = 8 << 20
const minRetainedOpenInfoBudget = 64 << 10
const minRetainedPeerReasonBudget = 64 << 10

const (
	minRetainedStateUnit        = 4 << 10
	minCompactTerminalStateUnit = 64
)

func (c *Conn) trackedSessionMemoryLocked() uint64 {
	if c == nil {
		return 0
	}

	total := c.flow.recvSessionUsed
	total = saturatingAdd(total, c.flow.queuedDataBytes)
	total = saturatingAdd(total, c.flow.advisoryQueuedBytes)
	total = saturatingAdd(total, c.flow.urgentQueuedBytes)
	total = saturatingAdd(total, c.pending.controlBytes)
	total = saturatingAdd(total, c.pending.priorityBytes)
	total = saturatingAdd(total, c.pending.preparedPriorityBytes)
	total = saturatingAdd(total, c.outstandingPingBytesLocked())
	total = saturatingAdd(total, c.ingress.readBufferOverhead)
	total = saturatingAdd(total, c.retention.retainedOpenInfoBytes)
	total = saturatingAdd(total, c.retention.retainedPeerReasonBytes)
	total = saturatingAdd(total, c.trackedRetainedStateMemoryLocked())
	return total
}

func (c *Conn) outstandingPingBytesLocked() uint64 {
	if c == nil {
		return 0
	}
	return uint64(len(c.liveness.pingPayload))
}

func (c *Conn) projectedTrackedSessionMemoryLocked(oldBucket, newBucket uint64) uint64 {
	return rt.ProjectTrackedMemoryDelta(c.trackedSessionMemoryLocked(), oldBucket, newBucket)
}

func (c *Conn) projectedTrackedSessionMemoryWithAdditionalLocked(additional uint64) uint64 {
	return saturatingAdd(c.trackedSessionMemoryLocked(), additional)
}

func (c *Conn) sessionMemoryHardCapLocked() uint64 {
	if c == nil {
		return minSessionMemoryHardCap
	}
	if c.flow.sessionMemoryCap > 0 {
		return c.flow.sessionMemoryCap
	}

	hardCap := c.sessionWindowTargetLocked()
	hardCap = saturatingAdd(hardCap, c.sessionDataHWMLocked())
	hardCap = saturatingAdd(hardCap, c.urgentLaneCapLocked())
	hardCap = saturatingAdd(hardCap, c.pendingControlBudgetLocked())
	hardCap = saturatingAdd(hardCap, c.pendingPriorityBudgetLocked())
	hardCap = saturatingAdd(hardCap, c.retainedOpenInfoBudgetLocked())
	hardCap = saturatingAdd(hardCap, c.retainedPeerReasonBudgetLocked())
	return maxUint64(hardCap, minSessionMemoryHardCap)
}

func (c *Conn) sessionMemoryHighThresholdLocked() uint64 {
	hardCap := c.sessionMemoryHardCapLocked()
	if hardCap <= 4 {
		return hardCap
	}
	return hardCap - hardCap/4
}

func (c *Conn) sessionMemoryPressureHighLocked() bool {
	return c.trackedSessionMemoryLocked() >= c.sessionMemoryHighThresholdLocked()
}

func (c *Conn) sessionWriteMemoryBlockedLocked(additional uint64) bool {
	if c == nil || additional == 0 {
		return false
	}
	current := c.trackedSessionMemoryLocked()
	return rt.ProjectedExceedsThreshold(current, additional, c.sessionMemoryHighThresholdLocked())
}

func (c *Conn) sessionMemoryWakeNeededLocked(prevTracked uint64) bool {
	if c == nil {
		return false
	}
	return rt.MemoryWakeNeeded(prevTracked, c.trackedSessionMemoryLocked(), c.sessionMemoryHighThresholdLocked())
}

func (c *Conn) notifySessionMemoryAvailableLocked(prevTracked uint64) {
	if c == nil || !c.sessionMemoryWakeNeededLocked(prevTracked) {
		return
	}
	c.broadcastWriteWakeLocked()
	notify(c.pending.controlNotify)
}

func (c *Conn) notifySessionMemoryReleasedLocked(prevTracked uint64, release sessionMemoryRelease) {
	if !release.released() {
		return
	}
	c.notifySessionMemoryAvailableLocked(prevTracked)
}

func (c *Conn) sessionMemoryCapErrorLocked(op string) error {
	return c.sessionMemoryCapErrorWithAdditionalLocked(op, 0)
}

func (c *Conn) sessionMemoryCapErrorWithAdditionalLocked(op string, additional uint64) error {
	if c == nil {
		return nil
	}
	tracked := c.projectedTrackedSessionMemoryWithAdditionalLocked(additional)
	hardCap := c.sessionMemoryHardCapLocked()
	if tracked <= hardCap {
		return nil
	}
	return wireError(CodeInternal, op, errSessionMemoryCapExceeded(tracked, hardCap))
}

func errSessionMemoryCapExceeded(tracked, hardCap uint64) error {
	return fmt.Errorf("session memory cap exceeded: tracked=%d cap=%d", tracked, hardCap)
}

func (c *Conn) retainedOpenInfoBudgetLocked() uint64 {
	if c == nil {
		return 0
	}
	if c.retention.retainedOpenInfoBudget > 0 {
		return c.retention.retainedOpenInfoBudget
	}
	maxPayload := c.config.local.Settings.MaxFramePayload
	if c.config.peer.Settings.MaxFramePayload > maxPayload {
		maxPayload = c.config.peer.Settings.MaxFramePayload
	}
	if maxPayload == 0 {
		maxPayload = DefaultSettings().MaxFramePayload
	}
	budget := saturatingMul(maxPayload, 8)
	if budget < minRetainedOpenInfoBudget {
		budget = minRetainedOpenInfoBudget
	}
	return budget
}

func storeOpenInfoBytes(existing, openInfo []byte) []byte {
	return storeRetainedBytes(existing, openInfo, retainedBytesBorrowed)
}

type retainedBytesOwnership uint8

const (
	retainedBytesBorrowed retainedBytesOwnership = iota
	retainedBytesOwned
)

func (o retainedBytesOwnership) ownsPayload() bool {
	return o == retainedBytesOwned
}

func storeOpenMetadataPrefixBytes(existing, prefix []byte, ownership retainedBytesOwnership) []byte {
	return storeRetainedBytes(existing, prefix, ownership)
}

func storeRetainedBytes(existing, payload []byte, ownership retainedBytesOwnership) []byte {
	if len(payload) == 0 {
		return nil
	}
	if len(existing) == len(payload) && cap(existing) == len(existing) {
		buf := existing[:]
		copy(buf, payload)
		return buf
	}
	if ownership.ownsPayload() && len(payload) == cap(payload) {
		return payload
	}
	return clonePayloadBytes(payload)
}

func (c *Conn) setStreamOpenInfoLocked(stream *nativeStream, openInfo []byte) bool {
	if c == nil || stream == nil {
		return false
	}
	same := bytes.Equal(stream.openInfo, openInfo)
	if same && len(stream.openInfo) == cap(stream.openInfo) {
		return false
	}
	prevTracked := c.trackedSessionMemoryLocked()
	oldLen := uint64(len(stream.openInfo))
	c.retention.retainedOpenInfoBytes = csub(c.retention.retainedOpenInfoBytes, oldLen)
	stored := storeOpenInfoBytes(stream.openInfo, openInfo)
	stream.openInfo = stored
	c.retention.retainedOpenInfoBytes = saturatingAdd(c.retention.retainedOpenInfoBytes, uint64(len(stored)))
	c.notifySessionMemoryAvailableLocked(prevTracked)
	return !same
}

func (c *Conn) releaseStreamOpenMetadataPrefixLocked(stream *nativeStream) {
	if stream == nil || len(stream.openMetadataPrefix) == 0 {
		return
	}
	stream.openMetadataPrefix = nil
}

func (c *Conn) retainedPeerReasonBudgetLocked() uint64 {
	if c == nil {
		return 0
	}
	if c.retention.retainedReasonBudget > 0 {
		return c.retention.retainedReasonBudget
	}
	maxPayload := c.config.local.Settings.MaxControlPayloadBytes
	if maxPayload == 0 {
		maxPayload = DefaultSettings().MaxControlPayloadBytes
	}
	budget := saturatingMul(maxPayload, 8)
	if budget < minRetainedPeerReasonBudget {
		budget = minRetainedPeerReasonBudget
	}
	return budget
}

func truncateStringToBytes(s string, limit uint64) string {
	if uint64(len(s)) <= limit {
		return s
	}
	if limit == 0 {
		return ""
	}
	cut := 0
	for i := range s {
		if uint64(i) > limit {
			break
		}
		cut = i
	}
	if cut == 0 {
		return ""
	}
	return strings.Clone(s[:cut])
}

func (c *Conn) retainPeerReasonLocked(oldBytes uint64, reason string) (string, uint64) {
	if c == nil || reason == "" {
		if c != nil {
			c.releasePeerReasonBytesLocked(oldBytes)
		}
		return "", 0
	}

	prevTracked := c.trackedSessionMemoryLocked()
	prevBytes := c.retention.retainedPeerReasonBytes
	base := csub(c.retention.retainedPeerReasonBytes, oldBytes)
	budget := c.retainedPeerReasonBudgetLocked()
	if budget == 0 || base >= budget {
		c.retention.retainedPeerReasonBytes = base
		c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(base < prevBytes))
		return "", 0
	}

	available := budget - base
	hardCap := c.sessionMemoryHardCapLocked()
	baseTracked := c.projectedTrackedSessionMemoryLocked(c.retention.retainedPeerReasonBytes, base)
	if baseTracked >= hardCap {
		c.retention.retainedPeerReasonBytes = base
		c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(base < prevBytes))
		return "", 0
	}
	availableByCap := hardCap - baseTracked
	if availableByCap < available {
		available = availableByCap
	}
	trimmed := truncateStringToBytes(reason, available)
	newBytes := uint64(len(trimmed))
	c.retention.retainedPeerReasonBytes = saturatingAdd(base, newBytes)
	c.notifySessionMemoryAvailableLocked(prevTracked)
	return trimmed, newBytes
}

func (c *Conn) releasePeerReasonBytesLocked(bytes uint64) {
	if c == nil || bytes == 0 {
		return
	}
	prevTracked := c.trackedSessionMemoryLocked()
	c.retention.retainedPeerReasonBytes = csub(c.retention.retainedPeerReasonBytes, bytes)
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleased)
}

func (c *Conn) releaseStreamPeerReasonBudgetLocked(stream *nativeStream) {
	if c == nil || stream == nil {
		return
	}
	total := saturatingAdd(
		saturatingAdd(stream.sendStopReasonBytes, stream.recvResetReasonBytes),
		stream.remoteAbortReasonBytes,
	)
	c.releasePeerReasonBytesLocked(total)
	stream.sendStopReasonBytes = 0
	stream.recvResetReasonBytes = 0
	stream.remoteAbortReasonBytes = 0
}

func nonNegativeCountDiff(total, removed int) int {
	count := total - removed
	if count < 0 {
		return 0
	}
	return count
}

func (r RetainedStateBreakdown) TotalBytes() uint64 {
	total := r.HiddenControl.Bytes
	total = saturatingAdd(total, r.AcceptBacklog.Bytes)
	total = saturatingAdd(total, r.Provisionals.Bytes)
	total = saturatingAdd(total, r.VisibleTombstone.Bytes)
	total = saturatingAdd(total, r.MarkerOnly.Bytes)
	return total
}

func (c *Conn) retainedStateUnitLocked() uint64 {
	if c == nil {
		return minRetainedStateUnit
	}
	unit := c.config.local.Settings.MaxFramePayload
	unit = maxUint64(unit, c.config.local.Settings.MaxControlPayloadBytes)
	unit = maxUint64(unit, c.config.local.Settings.MaxExtensionPayloadBytes)
	if unit == 0 {
		def := DefaultSettings()
		unit = maxUint64(def.MaxFramePayload, def.MaxControlPayloadBytes)
		unit = maxUint64(unit, def.MaxExtensionPayloadBytes)
	}
	return maxUint64(unit, minRetainedStateUnit)
}

func (c *Conn) compactTerminalStateUnitLocked() uint64 {
	return minCompactTerminalStateUnit
}

func (c *Conn) visibleTombstoneRetainedLocked() int {
	if c == nil || len(c.registry.tombstones) == 0 {
		return 0
	}
	return nonNegativeCountDiff(len(c.registry.tombstones), c.hiddenControlStateRetainedLocked())
}

func (c *Conn) markerOnlyRetainedLocked() int {
	if c == nil {
		return 0
	}
	return saturatingAddInt(c.markerOnlyMapCountLocked(), c.markerOnlyRangeCountLocked())
}

func saturatingAddInt(a, b int) int {
	maxInt := int(^uint(0) >> 1)
	if a >= maxInt || b >= maxInt || a > maxInt-b {
		return maxInt
	}
	return a + b
}

func (c *Conn) markerOnlyMapCountLocked() int {
	if c == nil || len(c.registry.usedStreamData) == 0 {
		return 0
	}
	count := 0
	for streamID := range c.registry.usedStreamData {
		if _, ok := c.registry.tombstones[streamID]; ok {
			continue
		}
		count++
	}
	return count
}

func (c *Conn) markerOnlyRangeCountLocked() int {
	if c == nil {
		return 0
	}
	return len(c.registry.usedStreamRanges)
}

func (c *Conn) markerOnlyHardCapLocked() int {
	if c == nil {
		return 1
	}
	if c.retention.markerOnlyLimit > 0 {
		return c.retention.markerOnlyLimit
	}
	unit := c.compactTerminalStateUnitLocked()
	if unit == 0 {
		return 1
	}
	capCount := c.sessionMemoryHardCapLocked() / unit
	if capCount == 0 {
		return 1
	}
	maxInt := int(^uint(0) >> 1)
	if capCount > uint64(maxInt) {
		return maxInt
	}
	return int(capCount)
}

func (c *Conn) markerOnlyCapErrorLocked(op string) error {
	if c == nil {
		return nil
	}
	count := c.markerOnlyRetainedLocked()
	capCount := c.markerOnlyHardCapLocked()
	if count <= capCount {
		return nil
	}
	return wireError(CodeInternal, op, fmt.Errorf("marker-only used-stream cap exceeded: count=%d cap=%d", count, capCount))
}

func retainedBucketStats(count int, unit uint64) RetainedBucketStats {
	if count <= 0 || unit == 0 {
		return RetainedBucketStats{}
	}
	return RetainedBucketStats{
		Count: count,
		Bytes: saturatingMul(uint64(count), unit),
	}
}

func (c *Conn) retainedStateBreakdownLocked() RetainedStateBreakdown {
	if c == nil {
		return RetainedStateBreakdown{}
	}
	retainedUnit := c.retainedStateUnitLocked()
	compactUnit := c.compactTerminalStateUnitLocked()
	return RetainedStateBreakdown{
		HiddenControl:    retainedBucketStats(c.hiddenControlStateRetainedLocked(), retainedUnit),
		AcceptBacklog:    retainedBucketStats(c.pendingAcceptedCountLocked(), retainedUnit),
		Provisionals:     retainedBucketStats(c.totalProvisionalCountLocked(), retainedUnit),
		VisibleTombstone: retainedBucketStats(c.visibleTombstoneRetainedLocked(), compactUnit),
		MarkerOnly:       retainedBucketStats(c.markerOnlyRetainedLocked(), compactUnit),
	}
}

func (c *Conn) trackedRetainedStateMemoryLocked() uint64 {
	return c.retainedStateBreakdownLocked().TotalBytes()
}

var errPingBusy = errors.New("zmux: ping already outstanding")

func (c *Conn) Ping(ctx context.Context, echo []byte) (time.Duration, error) {
	if c == nil {
		return 0, ErrSessionClosed
	}
	ctx = contextOrBackground(ctx)
	c.mu.Lock()
	if !c.publicWritePathReadyLocked() {
		c.mu.Unlock()
		return 0, ErrSessionClosed
	}
	c.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		done, sentAt, err := c.beginGeneratedPing(echo, "build PING")
		if err == nil {
			select {
			case <-ctx.Done():
				c.failPingDone(done)
				return 0, ctx.Err()
			case <-c.lifecycle.closedCh:
				if err := c.err(); err != nil {
					return 0, err
				}
				return 0, ErrSessionClosed
			case <-done:
				c.mu.Lock()
				rtt := c.liveness.lastPingRTT
				c.mu.Unlock()
				if rtt == 0 {
					return time.Since(sentAt), nil
				}
				return rtt, nil
			}
		}
		if !errors.Is(err, errPingBusy) {
			return 0, err
		}

		c.mu.Lock()
		var wait <-chan struct{} = c.liveness.pingDone
		c.mu.Unlock()
		if wait == nil {
			wait = closedSignalCh
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-c.lifecycle.closedCh:
			if err := c.err(); err != nil {
				return 0, err
			}
			return 0, ErrSessionClosed
		case <-wait:
		}
	}
}

const pingNonceBytes = 8

const (
	pingPayloadHashOffset64 = 14695981039346656037
	pingPayloadHashPrime64  = 1099511628211
)

type pingPayloadFingerprint struct {
	nonce  uint64
	hash   uint64
	length int
	set    bool
}

func newPingPayloadFingerprint(payload []byte) pingPayloadFingerprint {
	if len(payload) < pingNonceBytes {
		return pingPayloadFingerprint{}
	}
	return pingPayloadFingerprint{
		nonce:  binary.BigEndian.Uint64(payload[:pingNonceBytes]),
		hash:   pingPayloadHash(payload),
		length: len(payload),
		set:    true,
	}
}

func (f pingPayloadFingerprint) matches(payload []byte) bool {
	return f.set &&
		len(payload) == f.length &&
		len(payload) >= pingNonceBytes &&
		binary.BigEndian.Uint64(payload[:pingNonceBytes]) == f.nonce &&
		pingPayloadHash(payload) == f.hash
}

func pingPayloadHash(payload []byte) uint64 {
	h := uint64(pingPayloadHashOffset64)
	for _, b := range payload {
		h ^= uint64(b)
		h *= pingPayloadHashPrime64
	}
	return h
}

func pingPayloadLen(echoLen int) (uint64, bool) {
	if echoLen < 0 {
		return 0, false
	}
	maxEchoLen := int(^uint(0)>>1) - pingNonceBytes
	if echoLen > maxEchoLen {
		return 0, false
	}
	return uint64(echoLen) + pingNonceBytes, true
}

func buildPingPayload(echo []byte, nonce uint64) []byte {
	total, ok := pingPayloadLen(len(echo))
	if !ok {
		panic("PING payload length overflows int")
	}
	payload := make([]byte, int(total))
	binary.BigEndian.PutUint64(payload[:pingNonceBytes], nonce)
	copy(payload[pingNonceBytes:], echo)
	return payload
}

func buildPingPayloadCappedWithNonce(echo []byte, maxPayload, nonce uint64) ([]byte, error) {
	total, ok := pingPayloadLen(len(echo))
	if !ok {
		return nil, fmt.Errorf("PING payload length overflows int")
	}
	if total > maxPayload {
		return nil, fmt.Errorf("PING payload %d exceeds control payload limit %d", total, maxPayload)
	}
	return buildPingPayload(echo, nonce), nil
}

func (c *Conn) pingPayloadLimit() uint64 {
	if c == nil {
		return defaultNormalizedLimits.MaxControlPayloadBytes
	}
	local := c.config.local.Settings.MaxControlPayloadBytes
	if local == 0 {
		local = defaultNormalizedLimits.MaxControlPayloadBytes
	}
	peer := c.config.peer.Settings.MaxControlPayloadBytes
	if peer == 0 {
		peer = defaultNormalizedLimits.MaxControlPayloadBytes
	}
	return minNonZeroUint64(local, peer)
}

func (c *Conn) beginPingPayloadLocked(payload []byte, ownership retainedBytesOwnership) (<-chan struct{}, time.Time, []byte, error) {
	if c == nil {
		return nil, time.Time{}, nil, visibleSessionErrLocked(c, ErrSessionClosed)
	}
	if c.lifecycle.closeErr != nil {
		return nil, time.Time{}, nil, visibleSessionErrLocked(c, c.lifecycle.closeErr)
	}
	if !c.allowLocalNonCloseControlLocked() {
		return nil, time.Time{}, nil, visibleSessionErrLocked(c, ErrSessionClosed)
	}
	if c.liveness.pingOutstanding {
		return nil, time.Time{}, nil, errPingBusy
	}

	done := make(chan struct{})
	sentAt := time.Now()
	c.liveness.pingOutstanding = true
	if ownership.ownsPayload() {
		c.liveness.pingPayload = payload
	} else {
		c.liveness.pingPayload = append([]byte(nil), payload...)
	}
	c.liveness.lastPingSentAt = sentAt
	c.resetMaxPingDueLocked(sentAt)
	c.liveness.pingDone = done
	return done, sentAt, c.liveness.pingPayload, nil
}

func (c *Conn) beginGeneratedPing(echo []byte, op string) (<-chan struct{}, time.Time, error) {
	if c == nil {
		return nil, time.Time{}, ErrSessionClosed
	}
	c.mu.Lock()
	if c.lifecycle.closeErr != nil {
		err := visibleSessionErrLocked(c, c.lifecycle.closeErr)
		c.mu.Unlock()
		return nil, time.Time{}, err
	}
	if !c.allowLocalNonCloseControlLocked() {
		err := visibleSessionErrLocked(c, ErrSessionClosed)
		c.mu.Unlock()
		return nil, time.Time{}, err
	}
	if c.liveness.pingOutstanding {
		c.mu.Unlock()
		return nil, time.Time{}, errPingBusy
	}
	payload, err := buildPingPayloadCappedWithNonce(echo, c.pingPayloadLimit(), rt.NextSessionNonce(&c.liveness.pingNonceState))
	if err != nil {
		c.mu.Unlock()
		return nil, time.Time{}, wireError(CodeFrameSize, op, err)
	}
	done, sentAt, retainedPayload, err := c.beginPingPayloadLocked(payload, retainedBytesOwned)
	if err != nil {
		c.mu.Unlock()
		return nil, time.Time{}, err
	}
	notify(c.signals.livenessCh)
	c.mu.Unlock()

	if err := c.queueImmutableFrame(flatTxFrame(Frame{Type: FrameTypePING, Payload: retainedPayload})); err != nil {
		c.failPing(retainedPayload)
		return nil, time.Time{}, err
	}

	return done, sentAt, nil
}

func (c *Conn) beginPing(payload []byte) (<-chan struct{}, time.Time, error) {
	c.mu.Lock()
	done, sentAt, retainedPayload, err := c.beginPingPayloadLocked(payload, retainedBytesBorrowed)
	if err != nil {
		c.mu.Unlock()
		return nil, time.Time{}, err
	}
	notify(c.signals.livenessCh)
	c.mu.Unlock()

	if err := c.queueImmutableFrame(flatTxFrame(Frame{Type: FrameTypePING, Payload: retainedPayload})); err != nil {
		c.failPing(retainedPayload)
		return nil, time.Time{}, err
	}

	return done, sentAt, nil
}

func (c *Conn) sendKeepalivePing() error {
	_, _, err := c.beginGeneratedPing(nil, "build keepalive PING")
	current := connStateClosed
	if c != nil {
		c.mu.Lock()
		current = c.lifecycle.sessionState
		c.mu.Unlock()
	}
	if errors.Is(err, errPingBusy) || errors.Is(err, ErrSessionClosed) || state.IsBenignSessionError(current, err, ErrSessionClosed) {
		return nil
	}
	if err != nil && c != nil {
		c.mu.Lock()
		suppress := !c.allowLocalNonCloseControlLocked()
		c.mu.Unlock()
		if suppress {
			return nil
		}
	}
	return err
}

func (c *Conn) failPing(payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.liveness.pingOutstanding || !bytes.Equal(c.liveness.pingPayload, payload) {
		return
	}
	c.clearPingLocked()
}

func (c *Conn) failPingDone(done <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.liveness.pingOutstanding || c.liveness.pingDone != done {
		return
	}
	c.cancelPingLocked()
}

func (c *Conn) handlePongFrame(frame Frame) error {
	c.mu.Lock()
	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if len(frame.Payload) < 8 {
		c.mu.Lock()
		ignore := state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil)
		c.mu.Unlock()
		if ignore {
			return nil
		}
		return frameSizeError("handle PONG", fmt.Errorf("PONG payload too short: %d bytes", len(frame.Payload)))
	}
	now := time.Now()

	c.mu.Lock()
	if state.IgnorePeerNonCloseFrame(c.lifecycle.sessionState, c.lifecycle.closeErr != nil) {
		c.mu.Unlock()
		return nil
	}
	c.liveness.lastPongAt = now
	if c.liveness.pingOutstanding && bytes.Equal(frame.Payload, c.liveness.pingPayload) {
		c.liveness.lastPingRTT = now.Sub(c.liveness.lastPingSentAt)
		c.clearPingLocked()
		c.clearNoOpControlBudgetsLocked()
		c.mu.Unlock()
		return nil
	}
	if c.liveness.canceledPing.matches(frame.Payload) {
		c.clearCanceledPingLocked()
		c.clearNoOpControlBudgetsLocked()
		c.mu.Unlock()
		return nil
	}
	if err := c.recordNoOpControlLocked(now, "handle PONG"); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	return nil
}

func (c *Conn) clearPingLocked() {
	prevTracked := c.trackedSessionMemoryLocked()
	now := time.Now()
	c.liveness.pingOutstanding = false
	c.liveness.pingPayload = nil
	if c.liveness.pingDone != nil {
		close(c.liveness.pingDone)
		c.liveness.pingDone = nil
	}
	c.resetReadIdlePingDueLocked(now)
	c.resetWriteIdlePingDueLocked(now)
	notify(c.signals.livenessCh)
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleased)
}

func (c *Conn) cancelPingLocked() {
	prevTracked := c.trackedSessionMemoryLocked()
	now := time.Now()
	c.liveness.pingOutstanding = false
	c.liveness.canceledPing = newPingPayloadFingerprint(c.liveness.pingPayload)
	c.liveness.pingPayload = nil
	if c.liveness.pingDone != nil {
		close(c.liveness.pingDone)
		c.liveness.pingDone = nil
	}
	c.resetReadIdlePingDueLocked(now)
	c.resetWriteIdlePingDueLocked(now)
	notify(c.signals.livenessCh)
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleased)
}

func (c *Conn) clearCanceledPingLocked() {
	if c == nil || !c.liveness.canceledPing.set {
		return
	}
	c.liveness.canceledPing = pingPayloadFingerprint{}
}

func (c *Conn) keepaliveLoop() {
	timer := time.NewTimer(c.keepaliveIntervalSnapshot())
	defer timer.Stop()

	for {
		action := c.nextKeepaliveAction(time.Now())
		if action.shouldSendPing() {
			if err := c.sendKeepalivePing(); err != nil {
				c.mu.Lock()
				current := c.lifecycle.sessionState
				c.mu.Unlock()
				if !state.IsBenignSessionError(current, err, ErrSessionClosed) {
					c.closeSession(err)
					return
				}
			}
			action.delay = c.keepaliveIntervalSnapshot()
		}

		if action.delay <= 0 {
			action.delay = c.keepaliveIntervalSnapshot()
		}

		if !c.waitKeepaliveWake(timer, action.delay).shouldContinue() {
			return
		}
	}
}

type keepaliveWaitResult uint8

const (
	keepaliveWaitClosed keepaliveWaitResult = iota
	keepaliveWaitContinue
)

func (r keepaliveWaitResult) shouldContinue() bool {
	return r == keepaliveWaitContinue
}

func (c *Conn) waitKeepaliveWake(timer *time.Timer, delay time.Duration) keepaliveWaitResult {
	stopTimer(timer)
	if delay <= 0 {
		select {
		case <-c.lifecycle.closedCh:
			return keepaliveWaitClosed
		case <-c.signals.livenessCh:
			return keepaliveWaitContinue
		}
	}

	timer.Reset(delay)
	select {
	case <-c.lifecycle.closedCh:
		return keepaliveWaitClosed
	case <-c.signals.livenessCh:
		return keepaliveWaitContinue
	case <-timer.C:
		return keepaliveWaitContinue
	}
}

func (c *Conn) keepaliveIntervalSnapshot() time.Duration {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.liveness.keepaliveInterval
}

type keepaliveAction struct {
	delay    time.Duration
	sendPing bool
}

func (a keepaliveAction) shouldSendPing() bool {
	return a.sendPing
}

func (c *Conn) nextKeepaliveAction(now time.Time) keepaliveAction {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.liveness.keepaliveInterval <= 0 {
		return keepaliveAction{}
	}
	if c.liveness.pingOutstanding {
		timeout := c.effectiveKeepaliveTimeoutLocked()
		if timeout > 0 && now.Sub(c.liveness.lastPingSentAt) > timeout {
			c.mu.Unlock()
			c.CloseWithError(ErrKeepaliveTimeout)
			c.mu.Lock()
			return keepaliveAction{}
		}
		remaining := timeout - now.Sub(c.liveness.lastPingSentAt)
		if remaining <= 0 || timeout <= 0 {
			remaining = c.liveness.keepaliveInterval
		}
		return keepaliveAction{delay: remaining}
	}

	c.ensureKeepaliveSchedulesLocked(now)

	nextDue := earliestNonZeroTime(
		c.liveness.readIdlePingDueAt,
		c.liveness.writeIdlePingDueAt,
		c.liveness.maxPingDueAt,
	)
	if nextDue.IsZero() {
		return keepaliveAction{}
	}
	if !nextDue.After(now) {
		return keepaliveAction{sendPing: true}
	}
	return keepaliveAction{delay: nextDue.Sub(now)}
}

func (c *Conn) noteInboundFrame(now time.Time) {
	c.mu.Lock()
	c.liveness.lastInboundFrameAt = now
	c.liveness.lastControlProgressAt = now
	c.resetReadIdlePingDueLocked(now)
	c.mu.Unlock()
	notify(c.signals.livenessCh)
}

func (c *Conn) noteTransportWriteLocked(now time.Time) {
	if c == nil {
		return
	}
	c.liveness.lastTransportWriteAt = now
	c.resetWriteIdlePingDueLocked(now)
}

func (c *Conn) resetKeepaliveSchedulesLocked(now time.Time) {
	c.resetReadIdlePingDueLocked(now)
	c.resetWriteIdlePingDueLocked(now)
	c.resetMaxPingDueLocked(now)
}

func (c *Conn) ensureKeepaliveSchedulesLocked(now time.Time) {
	if c.liveness.keepaliveInterval <= 0 {
		c.liveness.readIdlePingDueAt = time.Time{}
		c.liveness.writeIdlePingDueAt = time.Time{}
		c.liveness.maxPingDueAt = time.Time{}
		return
	}
	if c.liveness.readIdlePingDueAt.IsZero() {
		base := c.liveness.lastInboundFrameAt
		if base.IsZero() {
			base = now
		}
		c.resetReadIdlePingDueLocked(base)
	}
	if c.liveness.writeIdlePingDueAt.IsZero() {
		base := c.liveness.lastTransportWriteAt
		if base.IsZero() {
			base = now
		}
		c.resetWriteIdlePingDueLocked(base)
	}
	if c.liveness.maxPingDueAt.IsZero() {
		c.resetMaxPingDueLocked(now)
	}
}

func (c *Conn) resetReadIdlePingDueLocked(now time.Time) {
	if c.liveness.keepaliveInterval <= 0 {
		c.liveness.readIdlePingDueAt = time.Time{}
		return
	}
	c.liveness.readIdlePingDueAt = now.Add(keepaliveLeadJitteredDelay(c.liveness.keepaliveInterval, &c.liveness.keepaliveJitterState))
}

func (c *Conn) resetWriteIdlePingDueLocked(now time.Time) {
	if c.liveness.keepaliveInterval <= 0 {
		c.liveness.writeIdlePingDueAt = time.Time{}
		return
	}
	c.liveness.writeIdlePingDueAt = now.Add(keepaliveLeadJitteredDelay(c.liveness.keepaliveInterval, &c.liveness.keepaliveJitterState))
}

func (c *Conn) resetMaxPingDueLocked(now time.Time) {
	if c.liveness.keepaliveInterval <= 0 || c.liveness.keepaliveMaxPingInterval <= 0 {
		c.liveness.maxPingDueAt = time.Time{}
		return
	}
	c.liveness.maxPingDueAt = now.Add(keepaliveLeadJitteredDelay(c.liveness.keepaliveMaxPingInterval, &c.liveness.keepaliveJitterState))
}

func keepaliveLeadJitteredDelay(base time.Duration, state *uint64) time.Duration {
	if base <= 0 {
		return 0
	}
	jitter := rt.NextKeepaliveJitter(base, state)
	delay := base - jitter
	if delay <= 0 {
		return base
	}
	return delay
}

func earliestNonZeroTime(values ...time.Time) time.Time {
	var earliest time.Time
	for _, value := range values {
		if value.IsZero() {
			continue
		}
		if earliest.IsZero() || value.Before(earliest) {
			earliest = value
		}
	}
	return earliest
}

func (c *Conn) effectiveKeepaliveTimeoutLocked() time.Duration {
	if c == nil {
		return 0
	}
	timeout := c.liveness.keepaliveTimeout
	if timeout <= 0 {
		timeout = saturatingDurationMulAdd(c.liveness.keepaliveInterval, 2, 0)
		if timeout < defaultKeepaliveTimeoutMin {
			timeout = defaultKeepaliveTimeoutMin
		}
		return adaptiveRTTTimeout(c.liveness.lastPingRTT, timeout, defaultKeepaliveTimeoutMax, 4, rttAdaptiveSlack)
	}
	if c.liveness.lastPingRTT > 0 {
		floor := saturatingDurationMulAdd(c.liveness.lastPingRTT, 4, rttAdaptiveSlack)
		if floor > timeout {
			timeout = floor
		}
	}
	return timeout
}

const (
	acceptQueueCompactMinHead      = 64
	provisionalQueueCompactMinHead = 64

	provisionalOpenHardCap           = state.DefaultProvisionalOpenHardCap
	provisionalOpenMaxAge            = state.ProvisionalOpenMaxAge
	provisionalOpenMaxAgeAdaptiveCap = 20 * time.Second
)

type provisionalCommitWait struct {
	deadline    time.Time
	blockedFlag bool
}

func (w provisionalCommitWait) blocked() bool {
	return w.blockedFlag
}

type provisionalOpenTurnWait struct {
	notifyCh       <-chan struct{}
	deadline       time.Time
	retryOnTimeout bool
}

func (w provisionalOpenTurnWait) wait(stream *nativeStream, wrap func(error) error) error {
	if stream == nil {
		return ErrSessionClosed
	}
	if err := stream.waitWithDeadline(w.notifyCh, w.deadline, OperationWrite); err != nil {
		if w.retryOnTimeout && errors.Is(err, os.ErrDeadlineExceeded) {
			return nil
		}
		if wrap != nil {
			return wrap(err)
		}
		return err
	}
	return nil
}

type localOpenCommitState uint8

const (
	localOpenUnchanged localOpenCommitState = iota
	localOpenCommitted
	localOpenAwaitingTurn
)

func (s localOpenCommitState) opened() bool {
	return s == localOpenCommitted
}

func (s localOpenCommitState) awaitingTurn() bool {
	return s == localOpenAwaitingTurn
}

type sparseQueueCompactFn func(head, length, count int) bool

type sparseQueueValidFn[T any] func(item T, idx int) bool

type sparseQueueLookup[T any] struct {
	item    T
	present bool
}

func (l sparseQueueLookup[T]) found() bool {
	return l.present
}

func streamQueueEntryNonNil(stream *nativeStream, _ int) bool {
	return stream != nil
}

func appendSparseQueue[T any](items *[]T, head, count *int, item T) int {
	idx := len(*items)
	*items = append(*items, item)
	*count++
	if *count == 1 {
		*head = idx
	}
	return idx
}

func advanceSparseQueueHead[T any](items []T, head int, valid sparseQueueValidFn[T]) int {
	for head < len(items) {
		if valid == nil || valid(items[head], head) {
			break
		}
		head++
	}
	return head
}

func sparseQueueHeadItem[T any](items []T, head *int, count int, valid sparseQueueValidFn[T]) sparseQueueLookup[T] {
	var zero T
	if head == nil || count == 0 {
		return sparseQueueLookup[T]{item: zero}
	}
	*head = advanceSparseQueueHead(items, *head, valid)
	if *head >= len(items) {
		return sparseQueueLookup[T]{item: zero}
	}
	return sparseQueueLookup[T]{item: items[*head], present: true}
}

func sparseQueueTailItem[T any](items []T, head, count int, valid sparseQueueValidFn[T]) sparseQueueLookup[T] {
	var zero T
	if count == 0 {
		return sparseQueueLookup[T]{item: zero}
	}
	for i := len(items) - 1; i >= head; i-- {
		if valid == nil || valid(items[i], i) {
			return sparseQueueLookup[T]{item: items[i], present: true}
		}
	}
	return sparseQueueLookup[T]{item: zero}
}

func removeSparseQueueSlot[T any](items *[]T, head, count *int, idx int, valid sparseQueueValidFn[T]) bool {
	if items == nil || head == nil || count == nil || *count == 0 || idx < 0 || idx >= len(*items) {
		return false
	}
	var zero T
	(*items)[idx] = zero
	*count--
	if *count < 0 {
		*count = 0
	}
	if idx == *head {
		*head = advanceSparseQueueHead(*items, *head, valid)
	}
	return true
}

func compactedQueueRetainLimit(length int) int {
	maxInt := int(^uint(0) >> 1)
	if length > maxInt/2 {
		return maxInt
	}
	return length * 2
}

func shrinkCompactedQueueBacking[T any](items []T) []T {
	if len(items) == 0 {
		return nil
	}
	if cap(items) <= compactedQueueRetainLimit(len(items)) {
		if cap(items) > len(items) {
			clear(items[len(items):cap(items)])
		}
		return items
	}
	return append([]T(nil), items...)
}

func compactSparseQueue[T any](items *[]T, head, count *int, shouldCompact sparseQueueCompactFn, valid sparseQueueValidFn[T], move func(T, int)) {
	if items == nil || head == nil || count == nil {
		return
	}
	if *count == 0 {
		clear(*items)
		*items = nil
		*head = 0
		*count = 0
		return
	}
	if shouldCompact != nil && !shouldCompact(*head, len(*items), *count) {
		return
	}

	writeIdx := 0
	for i := *head; i < len(*items); i++ {
		item := (*items)[i]
		if valid != nil && !valid(item, i) {
			continue
		}
		if move != nil {
			move(item, writeIdx)
		}
		(*items)[writeIdx] = item
		writeIdx++
	}
	clear((*items)[writeIdx:])
	*items = shrinkCompactedQueueBacking((*items)[:writeIdx])
	*head = 0
	*count = writeIdx
}

func clearSparseQueue[T any](items *[]T, head, count *int, visit func(T)) {
	if items == nil || head == nil || count == nil {
		return
	}
	if visit != nil {
		for _, item := range *items {
			visit(item)
		}
	}
	clear(*items)
	*items = nil
	*head = 0
	*count = 0
}

type indexedQueue[T comparable] struct {
	state    *sparseQueueState[T]
	getIndex func(T) int32
	setIndex func(T, int32)
}

type indexedStreamQueue = indexedQueue[*nativeStream]

func newIndexedQueue[T comparable](state *sparseQueueState[T], getIndex func(T) int32, setIndex func(T, int32)) indexedQueue[T] {
	return indexedQueue[T]{
		state:    state,
		getIndex: getIndex,
		setIndex: setIndex,
	}
}

func newIndexedStreamQueue(state *streamSparseQueueState, getIndex func(*nativeStream) int32, setIndex func(*nativeStream, int32)) indexedStreamQueue {
	return newIndexedQueue[*nativeStream](state, getIndex, setIndex)
}

func (q indexedQueue[T]) ready() bool {
	return q.state != nil && q.getIndex != nil && q.setIndex != nil
}

func (q indexedQueue[T]) countValue() int {
	if !q.ready() {
		return 0
	}
	return q.state.count
}

func (q indexedQueue[T]) initFromItems(valid sparseQueueValidFn[T]) {
	if !q.ready() || q.state.init {
		return
	}
	q.state.head = 0
	q.state.count = 0
	foundHead := false
	for i, stream := range q.state.items {
		if valid != nil && !valid(stream, i) {
			continue
		}
		q.setIndex(stream, int32(i))
		q.state.count++
		if !foundHead {
			q.state.head = i
			foundHead = true
		}
	}
	q.state.init = true
}

func (q indexedQueue[T]) append(item T) {
	var zero T
	if !q.ready() || item == zero {
		return
	}
	if q.holds(item, q.getIndex(item)) {
		return
	}
	idx := appendSparseQueue(&q.state.items, &q.state.head, &q.state.count, item)
	q.setIndex(item, int32(idx))
	q.state.init = true
}

func (q indexedQueue[T]) headItem(valid sparseQueueValidFn[T]) T {
	var zero T
	if !q.ready() || q.state.count == 0 {
		return zero
	}
	item := sparseQueueHeadItem(q.state.items, &q.state.head, q.state.count, valid)
	if !item.found() {
		return zero
	}
	return item.item
}

func (q indexedQueue[T]) headItemWithCompaction(valid sparseQueueValidFn[T], compact func()) T {
	var zero T
	if !q.ready() || q.state.count == 0 {
		return zero
	}
	item := q.headItem(valid)
	if item != zero || compact == nil {
		return item
	}
	compact()
	return q.headItem(valid)
}

func (q indexedQueue[T]) tailItem(valid sparseQueueValidFn[T]) T {
	var zero T
	if !q.ready() || q.state.count == 0 {
		return zero
	}
	item := sparseQueueTailItem(q.state.items, q.state.head, q.state.count, valid)
	if !item.found() {
		return zero
	}
	return item.item
}

func (q indexedQueue[T]) holds(item T, currentIndex int32) bool {
	var zero T
	if !q.ready() || item == zero || currentIndex < 0 {
		return false
	}
	idx := int(currentIndex)
	return idx < len(q.state.items) && q.state.items[idx] == item
}

func (q indexedQueue[T]) remove(item T, currentIndex int32, valid sparseQueueValidFn[T]) bool {
	var zero T
	if !q.ready() || item == zero {
		return false
	}
	if q.state.count == 0 {
		q.setIndex(item, invalidStreamQueueIndex)
		return false
	}

	idx := int(currentIndex)
	if currentIndex < int32(q.state.head) || idx >= len(q.state.items) || q.state.items[idx] != item || (valid != nil && !valid(item, idx)) {
		idx = -1
		for i := q.state.head; i < len(q.state.items); i++ {
			if q.state.items[i] == item && (valid == nil || valid(item, i)) {
				idx = i
				break
			}
		}
		if idx < 0 {
			q.setIndex(item, invalidStreamQueueIndex)
			return false
		}
	}

	q.setIndex(item, invalidStreamQueueIndex)
	return removeSparseQueueSlot(&q.state.items, &q.state.head, &q.state.count, idx, valid)
}

func (q indexedQueue[T]) compact(shouldCompact sparseQueueCompactFn, valid sparseQueueValidFn[T]) {
	if !q.ready() {
		return
	}
	compactSparseQueue(&q.state.items, &q.state.head, &q.state.count, shouldCompact, valid, func(item T, idx int) {
		q.setIndex(item, int32(idx))
	})
	q.state.init = true
}

func (q indexedQueue[T]) clear(visit func(T)) {
	if !q.ready() {
		return
	}
	clearSparseQueue(&q.state.items, &q.state.head, &q.state.count, visit)
	q.state.init = false
}

func resetIndexedStreamQueueFromStreams(q indexedStreamQueue, streams map[uint64]*nativeStream, include func(*nativeStream) bool) {
	if !q.ready() {
		return
	}
	q.clear(func(stream *nativeStream) {
		q.setIndex(stream, invalidStreamQueueIndex)
	})
	if include == nil {
		q.state.init = true
		return
	}
	items := make([]*nativeStream, 0, len(streams))
	for id, stream := range streams {
		if !streamMatchesID(stream, id) || !include(stream) {
			continue
		}
		items = append(items, stream)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].id < items[j].id })
	q.state.items = append(q.state.items, items...)
	q.state.head = 0
	q.state.count = len(q.state.items)
	for i, stream := range q.state.items {
		q.setIndex(stream, int32(i))
	}
	q.state.init = true
}

func ensureIndexedStreamQueueFromStreams(q indexedStreamQueue, streams map[uint64]*nativeStream, include func(*nativeStream) bool) {
	if !q.ready() || q.state.init {
		return
	}
	resetIndexedStreamQueueFromStreams(q, streams, include)
}

func (c *Conn) pendingInboundLimitLocked() int {
	if c == nil {
		return rt.DefaultVisibleAcceptBacklogLimit
	}
	return c.visibleAcceptBacklogHardCapLocked()
}

func (c *Conn) liveStreamCountLocked() int {
	if c == nil || len(c.registry.streams) == 0 {
		return 0
	}
	if c.registry.liveStreamsInit {
		return c.registry.liveStreamCount
	}
	count := 0
	for _, stream := range c.registry.streams {
		if stream != nil {
			count++
		}
	}
	c.registry.liveStreamCount = count
	c.registry.liveStreamsInit = true
	return count
}

func (c *Conn) hasLiveStreamsLocked() bool {
	return c.liveStreamCountLocked() > 0
}

func (c *Conn) hasGracefulCloseLiveStreamsLocked() bool {
	if c == nil || len(c.registry.streams) == 0 {
		return false
	}
	for _, stream := range c.registry.streams {
		if stream != nil && stream.blocksGracefulSessionCloseLocked() {
			return true
		}
	}
	return false
}

func (c *Conn) forEachKnownStreamLocked(fn func(*nativeStream)) {
	if c == nil || fn == nil {
		return
	}
	for _, stream := range c.registry.streams {
		if stream != nil {
			fn(stream)
		}
	}
	for _, stream := range c.queues.provisionalBidi.items {
		if stream != nil {
			fn(stream)
		}
	}
	for _, stream := range c.queues.provisionalUni.items {
		if stream != nil {
			fn(stream)
		}
	}
}

func (c *Conn) storeLiveStreamLocked(stream *nativeStream) {
	if c == nil || stream == nil || !stream.idSet {
		return
	}
	if c.registry.streams == nil {
		c.registry.streams = make(map[uint64]*nativeStream)
	}
	if !c.registry.liveStreamsInit {
		c.liveStreamCountLocked()
	}
	if c.registry.streams[stream.id] == nil {
		c.registry.liveStreamCount++
	}
	c.registry.streams[stream.id] = stream
	c.registry.liveStreamsInit = true
}

func (c *Conn) dropLiveStreamLocked(streamID uint64) *nativeStream {
	if c == nil || streamID == 0 || len(c.registry.streams) == 0 {
		return nil
	}
	if !c.registry.liveStreamsInit {
		c.liveStreamCountLocked()
	}
	stream := c.registry.streams[streamID]
	if stream != nil && c.registry.liveStreamCount > 0 {
		c.registry.liveStreamCount--
	}
	delete(c.registry.streams, streamID)
	c.registry.liveStreamsInit = true
	return stream
}

func (c *Conn) resetLiveStreamsLocked() {
	if c == nil {
		return
	}
	c.registry.streams = nil
	c.registry.liveStreamCount = 0
	c.registry.liveStreamsInit = true
}

func (c *Conn) newLocalStreamLocked(id uint64, arity streamArity, opts OpenOptions, openMetadataPrefix []byte) *nativeStream {
	return c.newLocalStreamWithIDAndPrefixRetentionLocked(id, arity, opts, openMetadataPrefix, retainedBytesBorrowed)
}

func (c *Conn) newLocalStreamWithIDLocked(id uint64, arity streamArity, opts OpenOptions, openMetadataPrefix []byte) *nativeStream {
	return c.newLocalStreamWithIDAndPrefixRetentionLocked(id, arity, opts, openMetadataPrefix, retainedBytesBorrowed)
}

func (c *Conn) newLocalStreamWithIDAndPrefixRetentionLocked(id uint64, arity streamArity, opts OpenOptions, openMetadataPrefix []byte, ownership retainedBytesOwnership) *nativeStream {
	initialPriority := derefUint64(opts.InitialPriority)
	initialPrioritySet := opts.InitialPriority != nil
	initialGroup := derefUint64(opts.InitialGroup)
	initialGroupSet := opts.InitialGroup != nil
	prefix := storeOpenMetadataPrefixBytes(nil, openMetadataPrefix, ownership)
	stream := &nativeStream{
		conn:               c,
		id:                 id,
		idSet:              true,
		bidi:               arity.isBidi(),
		localOpen:          streamLocalOpenState{opened: true},
		pending:            newStreamPendingState(),
		localSend:          true,
		localReceive:       arity.isBidi(),
		sendMax:            state.InitialLocalOpenedSendWindow(c.config.peer.Settings, arity.isBidi()),
		openMetadataPrefix: prefix,
		initialPriority:    initialPriority,
		initialPrioritySet: initialPrioritySet,
		initialGroup:       initialGroup,
		initialGroupSet:    initialGroupSet,
		priority:           initialPriority,
		group:              initialGroup,
		groupExplicit:      initialGroupSet && initialGroup != 0,
		openInfo:           storeOpenInfoBytes(nil, opts.OpenInfo),
		provisionalIndex:   invalidStreamQueueIndex,
		acceptIndex:        invalidStreamQueueIndex,
		unseenLocalIndex:   invalidStreamQueueIndex,
	}
	if arity.isBidi() {
		stream.recvAdvertised = c.config.local.Settings.InitialMaxStreamDataBidiLocallyOpened
	}
	stream.initHalfStates()
	if len(stream.openInfo) > 0 {
		c.retention.retainedOpenInfoBytes = saturatingAdd(c.retention.retainedOpenInfoBytes, uint64(len(stream.openInfo)))
	}
	return stream
}

func (c *Conn) newProvisionalLocalStreamLocked(arity streamArity, opts OpenOptions, openMetadataPrefix []byte) *nativeStream {
	stream := c.newLocalStreamWithIDAndPrefixRetentionLocked(0, arity, opts, openMetadataPrefix, retainedBytesBorrowed)
	stream.id = 0
	stream.idSet = false
	stream.setProvisionalCreated(time.Now())
	c.appendProvisionalLocked(stream)
	return stream
}

func (c *Conn) newProvisionalLocalStreamOwnedLocked(arity streamArity, opts OpenOptions, openMetadataPrefix []byte) *nativeStream {
	stream := c.newLocalStreamWithIDAndPrefixRetentionLocked(0, arity, opts, openMetadataPrefix, retainedBytesOwned)
	stream.id = 0
	stream.idSet = false
	stream.setProvisionalCreated(time.Now())
	c.appendProvisionalLocked(stream)
	return stream
}

func (c *Conn) checkLocalOpenAllowedLocked(id uint64, arity streamArity) error {
	if id > MaxVarint62 {
		return wireError(CodeProtocol, "open stream", fmt.Errorf("stream id overflow"))
	}
	if err := state.ValidateLocalOpenID(c.config.negotiated.LocalRole, id, arity.isBidi()); err != nil {
		return wireError(CodeProtocol, "open stream", err)
	}
	if state.LocalOpenRefusedByGoAway(id, arity.isBidi(), c.sessionControl.peerGoAwayBidi, c.sessionControl.peerGoAwayUni) {
		return refusedStreamAppErr()
	}
	if !c.localOpenWithinPeerLimitLocked(arity, 0) {
		return refusedStreamAppErr()
	}
	return nil
}

func (c *Conn) checkLocalOpenPossibleLocked(arity streamArity) error {
	return c.checkLocalOpenPossibleWithOpenInfoLocked(arity, 0)
}

func (c *Conn) checkLocalOpenPossibleWithOpenInfoLocked(arity streamArity, openInfoLen uint64) error {
	c.reapExpiredProvisionalsLocked(arity, time.Now())
	queueLen := c.provisionalCountLocked(arity)
	id := arity.nextLocalID(&c.registry)
	if queueLen >= state.ProvisionalHardCap(arity.isBidi(), c.pendingInboundLimitLocked()) {
		c.ingress.provisionalLimited = saturatingAdd(c.ingress.provisionalLimited, 1)
		return ErrOpenLimited
	}
	if openInfoLen > 0 && c.retainedOpenInfoBudgetLocked() > 0 && saturatingAdd(c.retention.retainedOpenInfoBytes, openInfoLen) > c.retainedOpenInfoBudgetLocked() {
		c.ingress.provisionalLimited = saturatingAdd(c.ingress.provisionalLimited, 1)
		return ErrOpenLimited
	}
	additional := saturatingAdd(c.retainedStateUnitLocked(), openInfoLen)
	if c.projectedTrackedSessionMemoryWithAdditionalLocked(additional) > c.sessionMemoryHardCapLocked() {
		c.ingress.provisionalLimited = saturatingAdd(c.ingress.provisionalLimited, 1)
		return ErrOpenLimited
	}
	if !c.localOpenWithinPeerLimitLocked(arity, queueLen) {
		return refusedStreamAppErr()
	}
	projected := state.ProjectedLocalOpenID(id, queueLen)
	return c.checkLocalOpenAllowedLocked(projected, arity)
}

func (c *Conn) localOpenWithinPeerLimitLocked(arity streamArity, provisionalCount int) bool {
	if c == nil {
		return false
	}
	var (
		active uint64
		limit  uint64
	)
	if arity.isBidi() {
		active = c.registry.activeLocalBidi
		limit = c.config.peer.Settings.MaxIncomingStreamsBidi
	} else {
		active = c.registry.activeLocalUni
		limit = c.config.peer.Settings.MaxIncomingStreamsUni
	}
	if active >= limit {
		return false
	}
	if provisionalCount <= 0 {
		return true
	}
	return saturatingAdd(active, uint64(provisionalCount)) < limit
}

func provisionalQueueShouldCompact(head, length, count int) bool {
	holes := length - head - count
	return head >= provisionalQueueCompactMinHead || holes >= count
}

type streamQueueLookup struct {
	stream *nativeStream
}

func (l streamQueueLookup) found() bool {
	return l.stream != nil
}

func (c *Conn) provisionalQueueLocked(arity streamArity) indexedStreamQueue {
	if c == nil {
		return indexedStreamQueue{}
	}
	return newIndexedStreamQueue(arity.provisionalQueueState(&c.queues), getProvisionalIndex, setProvisionalIndex)
}

func (c *Conn) provisionalCountLocked(arity streamArity) int {
	if c == nil {
		return 0
	}
	c.ensureProvisionalQueueLocked(arity)
	return c.provisionalQueueLocked(arity).countValue()
}

func (c *Conn) totalProvisionalCountLocked() int {
	if c == nil {
		return 0
	}
	return c.provisionalCountLocked(streamArityBidi) + c.provisionalCountLocked(streamArityUni)
}

func (c *Conn) ensureProvisionalQueueLocked(arity streamArity) {
	if c == nil {
		return
	}
	c.provisionalQueueLocked(arity).initFromItems(streamQueueEntryNonNil)
}

func (c *Conn) provisionalHeadLocked(arity streamArity) streamQueueLookup {
	if c == nil {
		return streamQueueLookup{}
	}
	c.ensureProvisionalQueueLocked(arity)
	return streamQueueLookup{stream: c.provisionalQueueLocked(arity).headItemWithCompaction(
		streamQueueEntryNonNil,
		func() { c.maybeCompactProvisionalLocked(arity) },
	)}
}

func (c *Conn) provisionalTailLocked(arity streamArity) streamQueueLookup {
	if c == nil {
		return streamQueueLookup{}
	}
	c.ensureProvisionalQueueLocked(arity)
	return streamQueueLookup{stream: c.provisionalQueueLocked(arity).tailItem(streamQueueEntryNonNil)}
}

func (c *Conn) appendProvisionalLocked(stream *nativeStream) {
	if c == nil || stream == nil {
		return
	}
	arity := stream.streamArity()
	c.ensureProvisionalQueueLocked(arity)
	c.provisionalQueueLocked(arity).append(stream)
}

func (c *Conn) maybeCompactProvisionalLocked(arity streamArity) {
	if c == nil {
		return
	}
	c.ensureProvisionalQueueLocked(arity)
	c.provisionalQueueLocked(arity).compact(provisionalQueueShouldCompact, streamQueueEntryNonNil)
}

func (c *Conn) clearProvisionalQueuesLocked() {
	if c == nil {
		return
	}
	c.provisionalQueueLocked(streamArityBidi).clear(nil)
	c.provisionalQueueLocked(streamArityUni).clear(nil)
}

func (c *Conn) removeProvisionalLocked(stream *nativeStream) bool {
	if stream == nil || stream.idSet {
		return false
	}
	arity := stream.streamArity()
	c.ensureProvisionalQueueLocked(arity)
	queue := c.provisionalQueueLocked(arity)
	prevTracked := c.trackedSessionMemoryLocked()
	if !queue.remove(stream, stream.provisionalIndex, streamQueueEntryNonNil) {
		return false
	}
	queue.compact(provisionalQueueShouldCompact, streamQueueEntryNonNil)
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleased)
	notify(c.signals.livenessCh)
	c.broadcastStateWakeLocked()
	return true
}

func (c *Conn) notifyProvisionalWaitersLocked(arity streamArity) {
	if head := c.provisionalHeadLocked(arity); head.found() {
		notify(head.stream.writeNotify)
	}
}

func (c *Conn) provisionalCommitWaitLocked(stream *nativeStream, now time.Time) (provisionalCommitWait, error) {
	if stream == nil || stream.idSet || !stream.needsLocalOpenerLocked() {
		return provisionalCommitWait{}, nil
	}
	arity := stream.streamArity()
	c.reapExpiredProvisionalsLocked(arity, now)
	if !stream.localSend {
		return provisionalCommitWait{}, ErrStreamNotWritable
	}
	if _, err := stream.writeAdmissionRawLocked(writeAdmissionStrict); err != nil {
		return provisionalCommitWait{}, err
	}
	head := c.provisionalHeadLocked(arity)
	if !head.found() || head.stream == stream {
		return provisionalCommitWait{}, nil
	}
	headCreated := head.stream.provisionalCreatedAt()
	if headCreated.IsZero() {
		return provisionalCommitWait{blockedFlag: true}, nil
	}
	return provisionalCommitWait{
		deadline:    headCreated.Add(c.provisionalOpenMaxAgeLocked()),
		blockedFlag: true,
	}, nil
}

func (s *nativeStream) provisionalOpenTurnWaitLocked() (provisionalOpenTurnWait, bool, error) {
	if s == nil || s.conn == nil {
		return provisionalOpenTurnWait{}, false, ErrSessionClosed
	}

	wait, err := s.conn.provisionalCommitWaitLocked(s, time.Now())
	if err != nil {
		return provisionalOpenTurnWait{}, false, err
	}
	if !wait.blocked() {
		return provisionalOpenTurnWait{}, false, nil
	}

	notifyCh, writeDeadline := s.writeWaitSnapshotLocked()
	deadline := wait.deadline
	retryOnTimeout := !wait.deadline.IsZero()
	if !writeDeadline.IsZero() && (wait.deadline.IsZero() || !wait.deadline.Before(writeDeadline)) {
		deadline = writeDeadline
		retryOnTimeout = false
	}
	return provisionalOpenTurnWait{
		notifyCh:       notifyCh,
		deadline:       deadline,
		retryOnTimeout: retryOnTimeout,
	}, true, nil
}

func (c *Conn) commitLocalOpenLocked(stream *nativeStream) (localOpenCommitState, error) {
	if stream == nil || stream.idSet || !stream.needsLocalOpenerLocked() {
		return localOpenUnchanged, nil
	}
	wait, err := c.provisionalCommitWaitLocked(stream, time.Now())
	if err != nil {
		return localOpenUnchanged, err
	} else if wait.blocked() {
		return localOpenAwaitingTurn, nil
	}
	id := stream.streamArity().nextLocalID(&c.registry)
	if err := c.checkLocalOpenAllowedLocked(id, stream.streamArity()); err != nil {
		source := terminalAbortLocal
		if appErr, ok := findError[*ApplicationError](err); ok && appErr.Code == uint64(CodeRefusedStream) {
			source = terminalAbortFromPeer
		}
		c.failProvisionalWithSourceLocked(stream, err, source)
		return localOpenUnchanged, err
	}
	removed := c.removeProvisionalLocked(stream)
	stream.id = id
	stream.idSet = true
	c.storeLiveStreamLocked(stream)
	if stream.bidi {
		c.registry.activeLocalBidi++
	} else {
		c.registry.activeLocalUni++
	}
	stream.markActiveCounted()
	c.appendUnseenLocalLocked(stream)
	c.maybeTrackStreamGroupLocked(stream)
	createdAt := stream.provisionalCreatedAt()
	c.noteOpenCommitLocked(createdAt, time.Now())
	stream.clearProvisionalState()
	stream.streamArity().advanceNextLocalID(&c.registry)
	if removed {
		c.notifyProvisionalWaitersLocked(stream.streamArity())
	}
	return localOpenCommitted, nil
}

func (c *Conn) prepareLocalOpeningLocked(stream *nativeStream) (localOpenCommitState, error) {
	if stream == nil || !stream.needsLocalOpenerLocked() {
		return localOpenUnchanged, nil
	}
	if stream.idSet {
		if c.registry.streams[stream.id] == nil {
			c.storeLiveStreamLocked(stream)
		}
		c.appendUnseenLocalLocked(stream)
		return localOpenCommitted, nil
	}
	return c.commitLocalOpenLocked(stream)
}

func (c *Conn) failProvisionalLocked(stream *nativeStream, err error) {
	c.failProvisionalWithSourceLocked(stream, err, terminalAbortLocal)
}

func (c *Conn) failProvisionalWithSourceLocked(stream *nativeStream, err error, source terminalAbortSource) {
	if stream == nil || stream.idSet {
		return
	}
	removed := c.removeProvisionalLocked(stream)
	appErr := cancelledAppErr("")
	var surfaceErr error
	if err != nil {
		if existing, ok := findError[*ApplicationError](err); ok {
			appErr = existing
			if _, bare := err.(*ApplicationError); !bare {
				surfaceErr = err
			}
		} else {
			appErr = cancelledAppErr(err.Error())
			surfaceErr = errors.Join(err, appErr)
		}
	}
	stream.setAbortedWithSource(appErr, source)
	if surfaceErr != nil {
		stream.setAbortSurfaceErr(surfaceErr)
	}
	c.finalizeTerminalStreamLocked(stream, transientStreamReleaseOptions{
		send:    true,
		receive: streamReceiveReleaseAndClearReadBuf,
	}, streamNotifyBoth, false)
	stream.clearProvisionalState()
	if removed {
		c.notifyProvisionalWaitersLocked(stream.streamArity())
	}
}

func (c *Conn) reapExpiredProvisionalsLocked(arity streamArity, now time.Time) {
	for {
		head := c.provisionalHeadLocked(arity)
		if !head.found() || !c.provisionalExpiredLocked(head.stream, now) {
			return
		}
		c.ingress.provisionalExpired = saturatingAdd(c.ingress.provisionalExpired, 1)
		c.failProvisionalLocked(head.stream, ErrOpenExpired)
	}
}

func (c *Conn) provisionalExpiredLocked(stream *nativeStream, now time.Time) bool {
	if stream == nil || stream.idSet {
		return false
	}
	created := stream.provisionalCreatedAt()
	if created.IsZero() {
		return false
	}
	return now.Sub(created) > c.provisionalOpenMaxAgeLocked()
}

func (c *Conn) provisionalOpenMaxAgeLocked() time.Duration {
	if c == nil {
		return provisionalOpenMaxAge
	}
	return adaptiveRTTTimeout(c.liveness.lastPingRTT, provisionalOpenMaxAge, provisionalOpenMaxAgeAdaptiveCap, 6, 250*time.Millisecond)
}

func (c *Conn) reclaimUnseenLocalStreamsLocked() {
	c.reclaimUnseenCommittedLocalLocked(streamArityBidi, c.sessionControl.peerGoAwayBidi)
	c.reclaimUnseenCommittedLocalLocked(streamArityUni, c.sessionControl.peerGoAwayUni)
	c.reclaimProvisionalLocked(streamArityBidi, c.registry.nextLocalBidi, c.sessionControl.peerGoAwayBidi)
	c.reclaimProvisionalLocked(streamArityUni, c.registry.nextLocalUni, c.sessionControl.peerGoAwayUni)
}

func (c *Conn) reclaimUnseenCommittedLocalLocked(arity streamArity, maxID uint64) {
	for {
		tail := c.unseenLocalTailLocked(arity)
		if !tail.found() || tail.stream.id <= maxID {
			return
		}
		if !tail.stream.shouldReclaimUnseenLocalLocked(c.sessionControl.peerGoAwayBidi, c.sessionControl.peerGoAwayUni) {
			c.removeUnseenLocalLocked(tail.stream)
			continue
		}
		c.closeStreamOnSessionWithOptionsLocked(tail.stream, refusedStreamAppErr(), sessionCloseOptions{
			abortSource: terminalAbortFromPeer,
			finalize:    true,
		})
	}
}

func (c *Conn) reclaimGracefulCloseLocalStreamsLocked() {
	for {
		tail := c.unseenLocalTailLocked(streamArityBidi)
		if !tail.found() {
			break
		}
		c.closeStreamOnSessionWithOptionsLocked(tail.stream, refusedStreamAppErr(), sessionCloseOptions{
			abortSource: terminalAbortLocal,
			finalize:    true,
		})
	}
	for {
		tail := c.unseenLocalTailLocked(streamArityUni)
		if !tail.found() {
			break
		}
		c.closeStreamOnSessionWithOptionsLocked(tail.stream, refusedStreamAppErr(), sessionCloseOptions{
			abortSource: terminalAbortLocal,
			finalize:    true,
		})
	}

	c.rejectAllProvisionalsLocked(streamArityBidi)
	c.rejectAllProvisionalsLocked(streamArityUni)
}

func (c *Conn) rejectAllProvisionalsLocked(arity streamArity) {
	for c.provisionalCountLocked(arity) > 0 {
		tail := c.provisionalTailLocked(arity)
		if !tail.found() {
			return
		}
		c.failProvisionalLocked(tail.stream, refusedStreamAppErr())
	}
}

func (c *Conn) reclaimProvisionalLocked(arity streamArity, nextID, maxID uint64) {
	available := state.ProvisionalAvailableCount(nextID, maxID)
	for c.provisionalCountLocked(arity) > available {
		tail := c.provisionalTailLocked(arity)
		if !tail.found() {
			return
		}
		c.failProvisionalWithSourceLocked(tail.stream, refusedStreamAppErr(), terminalAbortFromPeer)
	}
}

func (c *Conn) maybeFinalizePeerActiveLocked(stream *nativeStream) {
	if stream == nil {
		return
	}
	if stream.isLocalOpenedLocked() && state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
		c.removeUnseenLocalLocked(stream)
	}
	if stream.shouldFinalizeLocalActiveLocked() {
		stream.clearActiveCounted()
		c.registry.activeLocalBidi, c.registry.activeLocalUni = state.DecrementActiveStreamCount(stream.bidi, c.registry.activeLocalBidi, c.registry.activeLocalUni)
	}
	if stream.shouldFinalizePeerActiveLocked() {
		stream.clearActiveCounted()
		c.registry.activePeerBidi, c.registry.activePeerUni = state.DecrementActiveStreamCount(stream.bidi, c.registry.activePeerBidi, c.registry.activePeerUni)
	}
	c.maybeCompactTerminalLocked(stream)
	notify(c.signals.livenessCh)
	c.broadcastStateWakeLocked()
}

func shouldTrackUnseenLocalStream(stream *nativeStream) bool {
	return stream != nil &&
		stream.idSet &&
		stream.awaitingPeerVisibilityLocked() &&
		!state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked())
}

func unseenLocalQueueEntryValid(stream *nativeStream, idx int) bool {
	return stream != nil && stream.unseenLocalIndex == int32(idx) && shouldTrackUnseenLocalStream(stream)
}

func unseenLocalQueueShouldCompact(head, length, count int) bool {
	return head != 0 || length > 2*count
}

func (c *Conn) unseenLocalQueueLocked(arity streamArity) indexedStreamQueue {
	if c == nil {
		return indexedStreamQueue{}
	}
	return newIndexedStreamQueue(arity.unseenLocalQueueState(&c.queues), getUnseenLocalIndex, setUnseenLocalIndex)
}

func (c *Conn) initUnseenLocalQueueLocked(arity streamArity) {
	if c == nil {
		return
	}
	queue := c.unseenLocalQueueLocked(arity)
	if !queue.ready() || queue.state.init {
		return
	}
	clear(queue.state.items)
	items := queue.state.items[:0]
	for _, stream := range c.registry.streams {
		if stream == nil || stream.streamArity() != arity {
			continue
		}
		stream.unseenLocalIndex = invalidStreamQueueIndex
		if !shouldTrackUnseenLocalStream(stream) {
			continue
		}
		items = append(items, stream)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].id < items[j].id
	})
	for i, stream := range items {
		stream.unseenLocalIndex = int32(i)
	}
	queue.state.items = items
	queue.state.head = 0
	queue.state.count = len(items)
	queue.state.init = true
}

func (c *Conn) ensureUnseenLocalQueueLocked(arity streamArity) {
	if c == nil {
		return
	}
	c.initUnseenLocalQueueLocked(arity)
}

func (c *Conn) unseenLocalCountLocked(arity streamArity) int {
	if c == nil {
		return 0
	}
	c.ensureUnseenLocalQueueLocked(arity)
	return c.unseenLocalQueueLocked(arity).countValue()
}

func (c *Conn) unseenLocalTailLocked(arity streamArity) streamQueueLookup {
	if c == nil {
		return streamQueueLookup{}
	}
	c.ensureUnseenLocalQueueLocked(arity)
	return streamQueueLookup{stream: c.unseenLocalQueueLocked(arity).tailItem(unseenLocalQueueEntryValid)}
}

func (c *Conn) appendUnseenLocalLocked(stream *nativeStream) {
	if c == nil || !shouldTrackUnseenLocalStream(stream) {
		return
	}
	arity := stream.streamArity()
	c.ensureUnseenLocalQueueLocked(arity)
	queue := c.unseenLocalQueueLocked(arity)
	if queue.holds(stream, stream.unseenLocalIndex) {
		return
	}
	queue.append(stream)
}

func (c *Conn) removeUnseenLocalLocked(stream *nativeStream) bool {
	if c == nil || stream == nil {
		return false
	}
	arity := stream.streamArity()
	c.ensureUnseenLocalQueueLocked(arity)
	queue := c.unseenLocalQueueLocked(arity)
	if !queue.remove(stream, stream.unseenLocalIndex, streamQueueEntryNonNil) {
		return false
	}
	queue.compact(unseenLocalQueueShouldCompact, unseenLocalQueueEntryValid)
	return true
}

func (c *Conn) maybeCompactUnseenLocalQueueLocked(arity streamArity) {
	if c == nil {
		return
	}
	c.ensureUnseenLocalQueueLocked(arity)
	c.unseenLocalQueueLocked(arity).compact(unseenLocalQueueShouldCompact, unseenLocalQueueEntryValid)
}

func (c *Conn) clearUnseenLocalQueuesLocked() {
	if c == nil {
		return
	}
	c.unseenLocalQueueLocked(streamArityBidi).clear(nil)
	c.unseenLocalQueueLocked(streamArityUni).clear(nil)
}

func (c *Conn) markApplicationVisibleLocked(stream *nativeStream) {
	if c == nil || stream == nil || stream.applicationVisible {
		return
	}
	stream.applicationVisible = true
	c.registry.nextVisibilitySeq++
	stream.visibilitySeq = c.registry.nextVisibilitySeq
}

func (c *Conn) visibleAcceptBacklogHardCapLocked() int {
	if c == nil {
		return rt.DefaultVisibleAcceptBacklogLimit
	}
	if c.queues.acceptBacklogLimit > 0 {
		return c.queues.acceptBacklogLimit
	}
	return rt.DefaultVisibleAcceptBacklogLimit
}

func visibleAcceptBacklogBytesHardCapFor(maxFramePayload uint64) uint64 {
	return rt.VisibleAcceptBacklogBytesHardCap(maxFramePayload)
}

func (c *Conn) visibleAcceptBacklogBytesHardCapLocked() uint64 {
	if c == nil {
		return rt.VisibleAcceptBacklogBytesHardCap(0)
	}
	if c.queues.acceptBacklogBytesLimit > 0 {
		return c.queues.acceptBacklogBytesLimit
	}
	maxFramePayload := c.config.local.Settings.MaxFramePayload
	if maxFramePayload == 0 {
		maxFramePayload = DefaultSettings().MaxFramePayload
	}
	return visibleAcceptBacklogBytesHardCapFor(maxFramePayload)
}

func (c *Conn) pendingAcceptedCountLocked() int {
	if c == nil {
		return 0
	}
	return c.acceptCountLocked(streamArityBidi) + c.acceptCountLocked(streamArityUni)
}

func (c *Conn) pendingAcceptedBytesLocked() uint64 {
	if c == nil {
		return 0
	}
	return c.queues.acceptBidiBytes + c.queues.acceptUniBytes
}

func (c *Conn) acceptQueuedBytesLocked(arity streamArity) *uint64 {
	return arity.acceptQueuedBytes(&c.queues)
}

func (c *Conn) addAcceptQueuedBytesLocked(stream *nativeStream, n uint64) {
	if c == nil || stream == nil || !stream.enqueued || n == 0 {
		return
	}
	queued := c.acceptQueuedBytesLocked(stream.streamArity())
	*queued = saturatingAdd(*queued, n)
}

func (c *Conn) releaseAcceptQueuedBytesLocked(stream *nativeStream, n uint64) {
	if c == nil || stream == nil || !stream.enqueued || n == 0 {
		return
	}
	queued := c.acceptQueuedBytesLocked(stream.streamArity())
	*queued = csub(*queued, n)
}

func (c *Conn) finishAcceptedRemovalLocked(stream *nativeStream) {
	if c == nil || stream == nil {
		return
	}
	c.releaseAcceptQueuedBytesLocked(stream, stream.recvBuffer)
	stream.acceptIndex = invalidStreamQueueIndex
	stream.enqueued = false
}

func acceptQueueShouldCompact(head, length, count int) bool {
	holes := length - head - count
	return head >= acceptQueueCompactMinHead || holes >= count
}

func (c *Conn) acceptQueueLocked(arity streamArity) indexedStreamQueue {
	if c == nil {
		return indexedStreamQueue{}
	}
	return newIndexedStreamQueue(arity.acceptQueueState(&c.queues), getAcceptIndex, setAcceptIndex)
}

func (c *Conn) acceptCountLocked(arity streamArity) int {
	if c == nil {
		return 0
	}
	c.ensureAcceptQueueLocked(arity)
	return c.acceptQueueLocked(arity).countValue()
}

func (c *Conn) ensureAcceptQueueLocked(arity streamArity) {
	if c == nil {
		return
	}
	c.acceptQueueLocked(arity).initFromItems(streamQueueEntryNonNil)
}

func (c *Conn) appendAcceptedLocked(stream *nativeStream) {
	if c == nil || stream == nil {
		return
	}
	arity := stream.streamArity()
	c.ensureAcceptQueueLocked(arity)
	c.acceptQueueLocked(arity).append(stream)
}

func (c *Conn) acceptHeadLocked(arity streamArity) streamQueueLookup {
	if c == nil {
		return streamQueueLookup{}
	}
	c.ensureAcceptQueueLocked(arity)
	return streamQueueLookup{stream: c.acceptQueueLocked(arity).headItemWithCompaction(
		streamQueueEntryNonNil,
		func() { c.maybeCompactAcceptQueueLocked(arity) },
	)}
}

func (c *Conn) acceptTailLocked(arity streamArity) streamQueueLookup {
	if c == nil {
		return streamQueueLookup{}
	}
	c.ensureAcceptQueueLocked(arity)
	return streamQueueLookup{stream: c.acceptQueueLocked(arity).tailItem(streamQueueEntryNonNil)}
}

func (c *Conn) maybeCompactAcceptQueueLocked(arity streamArity) {
	if c == nil {
		return
	}
	c.ensureAcceptQueueLocked(arity)
	c.acceptQueueLocked(arity).compact(acceptQueueShouldCompact, streamQueueEntryNonNil)
}

func (c *Conn) clearAcceptQueuesLocked() {
	if c == nil {
		return
	}
	c.acceptQueueLocked(streamArityBidi).clear(func(stream *nativeStream) {
		stream.clearQueueMembershipState()
	})
	c.acceptQueueLocked(streamArityUni).clear(func(stream *nativeStream) {
		stream.clearQueueMembershipState()
	})
	c.queues.acceptBidiBytes = 0
	c.queues.acceptUniBytes = 0
}

func (c *Conn) dequeueAcceptedLocked(arity streamArity) *nativeStream {
	if c == nil {
		return nil
	}
	c.ensureAcceptQueueLocked(arity)
	queue := c.acceptQueueLocked(arity)
	if queue.countValue() == 0 {
		return nil
	}
	stream := queue.headItemWithCompaction(
		streamQueueEntryNonNil,
		func() { c.maybeCompactAcceptQueueLocked(arity) },
	)
	if stream == nil {
		return nil
	}
	queue.state.items[queue.state.head] = nil
	queue.state.head++
	queue.state.count--
	queue.compact(acceptQueueShouldCompact, streamQueueEntryNonNil)
	c.finishAcceptedRemovalLocked(stream)
	return stream
}

func (c *Conn) removeAcceptedLocked(stream *nativeStream) bool {
	if c == nil || stream == nil || !stream.enqueued {
		return false
	}
	arity := stream.streamArity()
	c.ensureAcceptQueueLocked(arity)
	queue := c.acceptQueueLocked(arity)
	prevTracked := c.trackedSessionMemoryLocked()
	if !queue.remove(stream, stream.acceptIndex, streamQueueEntryNonNil) {
		return false
	}
	queue.compact(acceptQueueShouldCompact, streamQueueEntryNonNil)
	c.finishAcceptedRemovalLocked(stream)
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleased)
	return true
}

func (c *Conn) newestAcceptedLocked() streamQueueLookup {
	if c == nil {
		return streamQueueLookup{}
	}
	bidiTail := c.acceptTailLocked(streamArityBidi)
	uniTail := c.acceptTailLocked(streamArityUni)
	switch {
	case !bidiTail.found():
		return uniTail
	case !uniTail.found():
		return bidiTail
	case bidiTail.stream.visibilitySeq > uniTail.stream.visibilitySeq:
		return bidiTail
	default:
		return uniTail
	}
}

func (c *Conn) enforceVisibleAcceptBacklogLocked() []uint64 {
	if c == nil {
		return nil
	}
	limitCount := c.visibleAcceptBacklogHardCapLocked()
	limitBytes := c.visibleAcceptBacklogBytesHardCapLocked()
	var refused []uint64
	for {
		overCount := limitCount > 0 && c.pendingAcceptedCountLocked() > limitCount
		overBytes := limitBytes > 0 && c.pendingAcceptedBytesLocked() > limitBytes
		overOpenInfo := c.retainedOpenInfoBudgetLocked() > 0 && c.retention.retainedOpenInfoBytes > c.retainedOpenInfoBudgetLocked()
		overMemory := c.trackedSessionMemoryLocked() > c.sessionMemoryHardCapLocked()
		if !overCount && !overBytes && !overOpenInfo && !overMemory {
			return refused
		}
		stream := c.newestAcceptedLocked()
		if !stream.found() || !c.removeAcceptedLocked(stream.stream) {
			return refused
		}
		c.queues.visibleAcceptRefused = saturatingAdd(c.queues.visibleAcceptRefused, 1)
		c.closeStreamOnSessionWithOptionsLocked(stream.stream, refusedStreamAppErr(), sessionCloseOptions{
			abortSource: terminalAbortLocal,
			finalize:    true,
		})
		refused = append(refused, stream.stream.id)
	}
}
