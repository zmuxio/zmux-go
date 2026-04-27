package zmux

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/testutil"
	"github.com/zmuxio/zmux-go/internal/wire"
)

type testStreamMutator func(*nativeStream)

func testTxFrame(frame Frame) txFrame {
	return flatTxFrame(frame)
}

func testTxFrames(frames ...Frame) []txFrame {
	return testTxFramesFrom(frames)
}

func testTxFramesFrom(frames []Frame) []txFrame {
	if len(frames) == 0 {
		return nil
	}
	lowered := make([]txFrame, len(frames))
	for i := range frames {
		lowered[i] = testTxFrame(frames[i])
	}
	return lowered
}

type recordingWriteDeadlineSetter struct {
	mu        sync.Mutex
	deadline  []time.Time
	deadlineC chan struct{}
}

func newRecordingWriteDeadlineSetter() *recordingWriteDeadlineSetter {
	return &recordingWriteDeadlineSetter{
		deadlineC: make(chan struct{}, 8),
	}
}

func (s *recordingWriteDeadlineSetter) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.deadline = append(s.deadline, t)
	s.mu.Unlock()
	select {
	case s.deadlineC <- struct{}{}:
	default:
	}
	return nil
}

func (s *recordingWriteDeadlineSetter) deadlineCalls() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.deadline...)
}

func (s *recordingWriteDeadlineSetter) waitForNonZeroDeadline(t *testing.T) {
	t.Helper()
	deadline := time.After(testSignalTimeout)
	for {
		calls := s.deadlineCalls()
		if len(calls) > 0 && !calls[len(calls)-1].IsZero() {
			return
		}
		select {
		case <-s.deadlineC:
		case <-deadline:
			t.Fatalf("timed out waiting for non-zero write deadline; calls=%v", calls)
		}
	}
}

func testPublicFrame(frame txFrame) Frame {
	if frame.payloadKind == txPayloadFlat {
		return Frame{
			Type:     frame.Type,
			Flags:    frame.Flags,
			StreamID: frame.StreamID,
			Payload:  frame.Payload,
		}
	}
	return Frame{
		Type:     frame.Type,
		Flags:    frame.Flags,
		StreamID: frame.StreamID,
		Payload:  frame.clonedPayload(),
	}
}

func testPublicFrames(frames []txFrame) []Frame {
	if len(frames) == 0 {
		return nil
	}
	public := make([]Frame, len(frames))
	for i := range frames {
		public[i] = testPublicFrame(frames[i])
	}
	return public
}

func testBuildFrameLaneRequest(c *Conn, frames []Frame, opts frameLaneRequestOptions) (writeRequest, error) {
	if c == nil {
		return writeRequest{}, nil
	}
	return c.buildTxLaneRequest(testTxFramesFrom(frames), opts)
}

func testQueueFrame(c *Conn, frame Frame) error {
	var opts frameLaneRequestOptions
	if rt.IsUrgentType(frame.Type) {
		opts.lane = writeLaneUrgent
	}
	return c.queueTxFrame(testTxFrame(frame), opts, frameBorrowed)
}

func testQueueAdvisoryFrames(c *Conn, frames []Frame) error {
	return c.queueTxFrameChunksToLane(
		testTxFramesFrom(frames),
		frameLaneRequestOptions{lane: writeLaneAdvisory},
		frameBorrowed,
		0,
	)
}

func testQueueUrgentFrames(c *Conn, frames []Frame) error {
	return c.queueTxFrameChunksToLane(
		testTxFramesFrom(frames),
		frameLaneRequestOptions{lane: writeLaneUrgent},
		frameBorrowed,
		c.urgentLaneCapLocked(),
	)
}

func testDrainPendingUrgentControlFrames(c *Conn) []Frame {
	if c == nil {
		return nil
	}
	return testPublicFrames(c.drainPendingUrgentControlTxFramesLocked())
}

func testDrainPendingPriorityUpdateFrames(c *Conn) []Frame {
	if c == nil {
		return nil
	}
	c.ensurePendingPriorityUpdateQueueLocked()
	count := c.pendingPriorityQueueCountLocked()
	if count == 0 {
		return nil
	}
	var idBuf [4]uint64
	var frameBuf [4]txFrame
	ids := idBuf[:0]
	txFrames := frameBuf[:0]
	if count > cap(idBuf) {
		ids = make([]uint64, 0, count)
	}
	if count > cap(frameBuf) {
		txFrames = make([]txFrame, 0, count)
	}
	prevTracked := c.trackedSessionMemoryLocked()
	batch := c.collectPendingPriorityUpdateBatchLocked(ids, txFrames, 0)
	released := batch.released
	for _, id := range batch.ids {
		if c.dropPendingPriorityUpdateEntryLocked(id) {
			released = true
		}
	}
	c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
	return testPublicFrames(batch.frames)
}

func testDrainPendingControlFrames(c *Conn) ([]Frame, []Frame) {
	if c == nil || !c.ensurePendingNonCloseControlLocked() {
		return nil, nil
	}
	return testDrainPendingUrgentControlFrames(c), testDrainPendingPriorityUpdateFrames(c)
}

func testLocalOpenState(opened, committed bool, phase state.LocalOpenPhase) streamLocalOpenState {
	return streamLocalOpenState{
		opened:    opened,
		committed: committed,
		phase:     phase,
	}
}

func testLocalOpenVisibleState() streamLocalOpenState {
	return testLocalOpenState(true, true, state.LocalOpenPhasePeerVisible)
}

func testLocalOpenOpenedState() streamLocalOpenState {
	return testLocalOpenState(true, false, state.LocalOpenPhaseNone)
}

func testLocalOpenOpenedCommittedState() streamLocalOpenState {
	return testLocalOpenState(true, true, state.LocalOpenPhaseNone)
}

func testLocalOpenClosedState() streamLocalOpenState {
	return testLocalOpenState(false, false, state.LocalOpenPhaseNone)
}

func testLocalOpenCommittedHiddenState() streamLocalOpenState {
	return testLocalOpenState(false, true, state.LocalOpenPhaseNone)
}

func testLocalOpenQueuedState() streamLocalOpenState {
	return testLocalOpenState(true, true, state.LocalOpenPhaseQueued)
}

func testBuildStream(c *Conn, id uint64, mutate ...testStreamMutator) *nativeStream {
	return testBuildStreamWithRegistration(c, id, true, mutate...)
}

func testBuildDetachedStream(c *Conn, id uint64, mutate ...testStreamMutator) *nativeStream {
	return testBuildStreamWithRegistration(c, id, false, mutate...)
}

func testBuildStreamWithRegistration(c *Conn, id uint64, register bool, mutate ...testStreamMutator) *nativeStream {
	stream := &nativeStream{conn: c}
	if id != 0 {
		stream.id = id
		stream.idSet = true
	}
	for _, fn := range mutate {
		if fn != nil {
			fn(stream)
		}
	}
	stream.initHalfStates()
	if register && c != nil && stream.idSet {
		if c.registry.streams == nil {
			c.registry.streams = make(map[uint64]*nativeStream)
		}
		c.registry.streams[stream.id] = stream
	}
	return stream
}

func testWithLocalSend() testStreamMutator {
	return func(stream *nativeStream) {
		stream.localSend = true
	}
}

func testWithLocalReceive() testStreamMutator {
	return func(stream *nativeStream) {
		stream.localReceive = true
	}
}

func testWithBidi() testStreamMutator {
	return func(stream *nativeStream) {
		stream.bidi = true
	}
}

func testWithLocalOpen(localOpen streamLocalOpenState) testStreamMutator {
	return func(stream *nativeStream) {
		stream.localOpen = localOpen
	}
}

func testWithVisibleLocalBidi() testStreamMutator {
	return func(stream *nativeStream) {
		testWithBidi()(stream)
		testWithLocalOpen(testLocalOpenVisibleState())(stream)
		testWithLocalSend()(stream)
		testWithLocalReceive()(stream)
	}
}

func testWithReadNotify() testStreamMutator {
	return func(stream *nativeStream) {
		stream.readNotify = make(chan struct{}, 1)
	}
}

func testWithWriteNotify() testStreamMutator {
	return func(stream *nativeStream) {
		stream.writeNotify = make(chan struct{}, 1)
	}
}

func testWithNotifications() testStreamMutator {
	return func(stream *nativeStream) {
		testWithReadNotify()(stream)
		testWithWriteNotify()(stream)
	}
}

func testWithSendMax(v uint64) testStreamMutator {
	return func(stream *nativeStream) {
		stream.sendMax = v
	}
}

func testWithBlockedState(at uint64) testStreamMutator {
	return func(stream *nativeStream) {
		stream.blockedAt = at
		stream.blockedSet = true
	}
}

func testWithExplicitGroup(group uint64) testStreamMutator {
	return func(stream *nativeStream) {
		stream.group = group
		stream.groupExplicit = group != 0
	}
}

func testWithTrackedGroup(group uint64) testStreamMutator {
	return func(stream *nativeStream) {
		stream.groupTracked = true
		stream.trackedGroup = group
	}
}

func testWithApplicationVisible() testStreamMutator {
	return func(stream *nativeStream) {
		stream.applicationVisible = true
	}
}

func testWithRecvWindow(advertised, buffer uint64) testStreamMutator {
	return func(stream *nativeStream) {
		stream.recvAdvertised = advertised
		stream.recvBuffer = buffer
	}
}

func testWithEnqueued(seq uint64) testStreamMutator {
	return func(stream *nativeStream) {
		stream.enqueued = true
		stream.visibilitySeq = seq
	}
}

func testWithOpenInfo(info []byte) testStreamMutator {
	return func(stream *nativeStream) {
		stream.openInfo = info
	}
}

func testWithOpenMetadataPrefix(prefix []byte) testStreamMutator {
	return func(stream *nativeStream) {
		stream.openMetadataPrefix = prefix
	}
}

func testLocalSendStream(c *Conn, id uint64, extra ...testStreamMutator) *nativeStream {
	base := []testStreamMutator{testWithLocalSend(), testWithWriteNotify()}
	return testBuildStream(c, id, append(base, extra...)...)
}

func testVisibleBidiStream(c *Conn, id uint64, extra ...testStreamMutator) *nativeStream {
	base := []testStreamMutator{testWithVisibleLocalBidi(), testWithNotifications()}
	return testBuildStream(c, id, append(base, extra...)...)
}

func testOpenedBidiStream(c *Conn, id uint64, extra ...testStreamMutator) *nativeStream {
	base := []testStreamMutator{
		testWithBidi(),
		testWithLocalOpen(testLocalOpenOpenedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
		testWithNotifications(),
	}
	return testBuildStream(c, id, append(base, extra...)...)
}

func testMarkLocalOpenCommitted(stream *nativeStream) {
	if stream == nil {
		return
	}
	stream.localOpen.committed = true
}

func testMarkLocalOpenVisible(stream *nativeStream) {
	if stream == nil {
		return
	}
	stream.localOpen.committed = true
	stream.localOpen.phase = state.LocalOpenPhasePeerVisible
}

func testResetLocalOpenVisibility(stream *nativeStream) {
	if stream == nil {
		return
	}
	stream.localOpen.committed = false
	stream.localOpen.phase = state.LocalOpenPhaseNone
}

func testApplicationError(err error) *ApplicationError {
	var appErr *ApplicationError
	errors.As(err, &appErr)
	return appErr
}

func testSameError(got, want error) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	gotApp := testApplicationError(got)
	wantApp := testApplicationError(want)
	if gotApp != nil || wantApp != nil {
		return gotApp != nil && wantApp != nil &&
			gotApp.Code == wantApp.Code &&
			gotApp.Reason == wantApp.Reason
	}
	return errors.Is(got, want) && errors.Is(want, got)
}

func TestLocalGoAwayTransitionsSessionToDraining(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	if got := c.lifecycle.sessionState; got != connStateReady {
		t.Fatalf("initial state = %d, want %d", got, connStateReady)
	}

	c.mu.Lock()
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	c.mu.Unlock()

	if err := c.GoAway(state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), 0); err != nil {
		t.Fatalf("GoAway: %v", err)
	}
	_ = awaitQueuedFrame(t, frames)

	c.mu.Lock()
	got := c.lifecycle.sessionState
	c.mu.Unlock()
	if got != connStateDraining {
		t.Fatalf("state after GoAway = %d, want %d", got, connStateDraining)
	}
}

func TestManualGoAwayKeepsLocalAdmissionOpenWhileDraining(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	c.mu.Unlock()

	if err := c.GoAway(state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), 0); err != nil {
		t.Fatalf("GoAway: %v", err)
	}
	_ = awaitQueuedFrame(t, frames)

	c.mu.Lock()
	sessionState := c.lifecycle.sessionState
	active := c.shutdown.gracefulCloseActive
	c.mu.Unlock()
	if sessionState != connStateDraining {
		t.Fatalf("state after GoAway = %d, want %d", sessionState, connStateDraining)
	}
	if active {
		t.Fatal("manual GoAway should not mark graceful close active")
	}

	if _, err := c.OpenStream(context.Background()); err != nil {
		t.Fatalf("OpenStream after manual GoAway = %v, want nil", err)
	}
}

func TestPeerGoAwayTransitionsSessionToDraining(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	if got := c.lifecycle.sessionState; got != connStateReady {
		t.Fatalf("initial state = %d, want %d", got, connStateReady)
	}

	if err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 8, 0, uint64(CodeNoError), ""),
	}); err != nil {
		t.Fatalf("handleGoAwayFrame: %v", err)
	}

	c.mu.Lock()
	got := c.lifecycle.sessionState
	c.mu.Unlock()
	if got != connStateDraining {
		t.Fatalf("state after peer GoAWAY = %d, want %d", got, connStateDraining)
	}
}

func TestPeerGoAwayNotifiesControlPlane(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConnWithOptions(t, false)
	defer stop()

	select {
	case <-c.pending.controlNotify:
	default:
	}

	if err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 8, 0, uint64(CodeNoError), "first"),
	}); err != nil {
		t.Fatalf("handleGoAwayFrame: %v", err)
	}

	select {
	case <-c.pending.controlNotify:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for control notification after peer GOAWAY")
	}
}

func TestPeerGoAwayRetainsErrorPayloadWithoutClosingSession(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	if err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 8, 0, uint64(CodeProtocol), "maintenance"),
	}); err != nil {
		t.Fatalf("first handleGoAwayFrame: %v", err)
	}

	goAwayErr := c.PeerGoAwayError()
	if goAwayErr == nil {
		t.Fatal("expected peer GOAWAY error after first payload")
	}
	if goAwayErr.Code != uint64(CodeProtocol) {
		t.Fatalf("peer goaway code = %d, want %d", goAwayErr.Code, uint64(CodeProtocol))
	}
	if goAwayErr.Reason != "maintenance" {
		t.Fatalf("peer goaway reason = %q, want %q", goAwayErr.Reason, "maintenance")
	}

	c.mu.Lock()
	if c.lifecycle.sessionState != connStateDraining {
		t.Fatalf("session state = %d, want %d", c.lifecycle.sessionState, connStateDraining)
	}
	if c.lifecycle.closeErr != nil {
		c.mu.Unlock()
		t.Fatalf("goaway should not set closeErr = %v", c.lifecycle.closeErr)
	}
	c.mu.Unlock()

	if err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 8, 0, uint64(CodeFrameSize), "closing soon"),
	}); err != nil {
		t.Fatalf("second handleGoAwayFrame: %v", err)
	}
	goAwayErr = c.PeerGoAwayError()
	if goAwayErr == nil {
		t.Fatal("expected peer GOAWAY error after second payload")
	}
	if goAwayErr.Code != uint64(CodeFrameSize) {
		t.Fatalf("updated peer goaway code = %d, want %d", goAwayErr.Code, uint64(CodeFrameSize))
	}
	if goAwayErr.Reason != "closing soon" {
		t.Fatalf("updated peer goaway reason = %q, want %q", goAwayErr.Reason, "closing soon")
	}
}

func TestPeerCloseRetainsErrorPayloadAsSessionError(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	peerCloseErr := c.PeerCloseError()
	if peerCloseErr != nil {
		t.Fatalf("peer close error before peer close = %v, want nil", peerCloseErr)
	}

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustEncodeVarint(uint64(CodeProtocol)),
	})
	if err == nil {
		t.Fatalf("handle peer close err = nil, want non-nil")
	}
	c.closeSession(err)

	peerCloseErr = c.PeerCloseError()
	if peerCloseErr == nil {
		t.Fatal("expected peer close error after handleCloseFrame")
	}
	if peerCloseErr.Code != uint64(CodeProtocol) {
		t.Fatalf("peer close code = %d, want %d", peerCloseErr.Code, uint64(CodeProtocol))
	}

	c.mu.Lock()
	if c.lifecycle.closeErr == nil {
		c.mu.Unlock()
		t.Fatal("closeErr should be set after closeSession")
	}
	c.mu.Unlock()
}

func TestPeerCloseNoErrorStateIsSessionClosed(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeNoError), "done"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = nil, want non-nil")
	}
	c.closeSession(err)

	c.mu.Lock()
	sessionState := c.lifecycle.sessionState
	c.mu.Unlock()
	peerCloseErr := c.PeerCloseError()
	if sessionState != connStateClosed {
		t.Fatalf("session state = %d, want %d", sessionState, connStateClosed)
	}
	if peerCloseErr == nil {
		t.Fatal("expected peer close error")
	}
	if peerCloseErr.Code != uint64(CodeNoError) || peerCloseErr.Reason != "done" {
		t.Fatalf("peer close error = (%d, %q), want (%d, %q)", peerCloseErr.Code, peerCloseErr.Reason, uint64(CodeNoError), "done")
	}
}

func TestPeerCloseFrameAfterSessionCloseIsIgnored(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	firstErr := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "first"),
	})
	if firstErr == nil {
		t.Fatalf("first handle peer close err = nil, want non-nil")
	}
	c.closeSession(firstErr)

	if err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeInternal), "second"),
	}); err != nil {
		t.Fatalf("second handle peer close err = %v, want nil", err)
	}

	peerCloseErr := c.PeerCloseError()
	if peerCloseErr == nil {
		t.Fatal("expected peer close error after duplicate close")
	}
	if peerCloseErr.Code != uint64(CodeProtocol) || peerCloseErr.Reason != "first" {
		t.Fatalf("peer close error = (%d, %q), want (%d, %q)", peerCloseErr.Code, peerCloseErr.Reason, uint64(CodeProtocol), "first")
	}
}

func TestMalformedPeerCloseFrameAfterSessionCloseIsIgnored(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	firstErr := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "first"),
	})
	if firstErr == nil {
		t.Fatalf("first handle peer close err = nil, want non-nil")
	}
	c.closeSession(firstErr)

	if err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: []byte{0xff},
	}); err != nil {
		t.Fatalf("malformed duplicate handle peer close err = %v, want nil", err)
	}

	peerCloseErr := c.PeerCloseError()
	if peerCloseErr == nil {
		t.Fatal("expected peer close error after duplicate close")
	}
	if peerCloseErr.Code != uint64(CodeProtocol) || peerCloseErr.Reason != "first" {
		t.Fatalf("peer close error = (%d, %q), want (%d, %q)", peerCloseErr.Code, peerCloseErr.Reason, uint64(CodeProtocol), "first")
	}
}

func TestPeerCloseViaHandleFrameAfterSessionCloseIsIgnoredWithoutBudgetSideEffects(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	firstErr := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "first"),
	})
	if firstErr == nil {
		t.Fatalf("first handle peer close err = nil, want non-nil")
	}
	c.closeSession(firstErr)

	c.mu.Lock()
	c.abuse.controlBudgetFrames = 3
	c.abuse.mixedBudgetFrames = 5
	c.abuse.noopControlCount = 7
	c.mu.Unlock()

	if err := c.handleFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: []byte{0xff},
	}); err != nil {
		t.Fatalf("handleFrame malformed duplicate CLOSE err = %v, want nil", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abuse.controlBudgetFrames != 3 || c.abuse.mixedBudgetFrames != 5 || c.abuse.noopControlCount != 7 {
		t.Fatalf("budgets changed after ignored duplicate CLOSE = (%d,%d,%d), want (3,5,7)", c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
	}
	if c.sessionControl.peerCloseErr == nil || c.sessionControl.peerCloseErr.Code != uint64(CodeProtocol) || c.sessionControl.peerCloseErr.Reason != "first" {
		t.Fatalf("peer close error = %#v, want original payload", c.sessionControl.peerCloseErr)
	}
}

func TestPeerCloseFrameTransitionsSessionToClosing(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	if got := c.lifecycle.sessionState; got != connStateReady {
		t.Fatalf("initial state = %d, want %d", got, connStateReady)
	}

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "in-flight"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = nil, want non-nil")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.lifecycle.sessionState; got != connStateFailed {
		t.Fatalf("state after peer CLOSE = %d, want %d", got, connStateFailed)
	}
}

func TestPeerCloseFrameTransitionsDrainingToClosing(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateDraining
	c.mu.Unlock()

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "draining"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = nil, want non-nil")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.lifecycle.sessionState; got != connStateFailed {
		t.Fatalf("state after peer CLOSE = %d, want %d", got, connStateFailed)
	}
}

func TestPeerCloseAfterTransportFailureRetainsPeerCloseDiagnostics(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(io.ErrClosedPipe)

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "peer"),
	})
	if err == nil {
		t.Fatal("handle peer close after transport failure err = nil, want non-nil")
	}

	peerCloseErr := c.PeerCloseError()
	if peerCloseErr == nil {
		t.Fatal("expected peer close diagnostics after transport failure")
	}
	if peerCloseErr.Code != uint64(CodeProtocol) || peerCloseErr.Reason != "peer" {
		t.Fatalf("peer close error = (%d, %q), want (%d, %q)", peerCloseErr.Code, peerCloseErr.Reason, uint64(CodeProtocol), "peer")
	}

	if got := c.State(); got != SessionStateFailed {
		t.Fatalf("State() after transport failure + peer close = %v, want %v", got, SessionStateFailed)
	}
	if !errors.Is(c.err(), io.ErrClosedPipe) {
		t.Fatalf("closeErr = %v, want io.ErrClosedPipe", c.err())
	}
}

func TestMalformedPeerCloseIgnoredAfterLocalSessionClose(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "fatal"})

	if err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: []byte{0xff},
	}); err != nil {
		t.Fatalf("handle malformed peer close after local session close = %v, want nil", err)
	}

	if peerCloseErr := c.PeerCloseError(); peerCloseErr != nil {
		t.Fatalf("peer close diagnostics = %#v, want nil", peerCloseErr)
	}
}

func TestPeerCloseViaHandleFrameIgnoredAfterLocalSessionCloseWithoutBudgetSideEffects(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "fatal"})

	c.mu.Lock()
	c.abuse.controlBudgetFrames = 3
	c.abuse.mixedBudgetFrames = 5
	c.abuse.noopControlCount = 7
	c.mu.Unlock()

	if err := c.handleFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: []byte{0xff},
	}); err != nil {
		t.Fatalf("handleFrame malformed peer close after local session close = %v, want nil", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abuse.controlBudgetFrames != 3 || c.abuse.mixedBudgetFrames != 5 || c.abuse.noopControlCount != 7 {
		t.Fatalf("budgets changed after ignored terminal CLOSE = (%d,%d,%d), want (3,5,7)", c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
	}
	if c.sessionControl.peerCloseErr != nil {
		t.Fatalf("peer close diagnostics = %#v, want nil", c.sessionControl.peerCloseErr)
	}
}

func TestPeerGoAwayIgnoredAfterSessionClose(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "fatal"})

	c.mu.Lock()
	c.sessionControl.peerGoAwayBidi = 80
	c.sessionControl.peerGoAwayUni = 99
	c.sessionControl.peerGoAwayErr = nil
	startBidi := c.sessionControl.peerGoAwayBidi
	startUni := c.sessionControl.peerGoAwayUni
	startState := c.lifecycle.sessionState
	startErr := c.sessionControl.peerGoAwayErr
	c.mu.Unlock()

	err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 0, 0, uint64(CodeProtocol), "too-late"),
	})
	if err != nil {
		t.Fatalf("handle goaway after session close = %v, want nil", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionControl.peerGoAwayBidi != startBidi || c.sessionControl.peerGoAwayUni != startUni {
		t.Fatalf("peer GOAWAY watermarks = (%d, %d), want (%d, %d)", c.sessionControl.peerGoAwayBidi, c.sessionControl.peerGoAwayUni, startBidi, startUni)
	}
	if !testSameError(c.sessionControl.peerGoAwayErr, startErr) {
		t.Fatalf("peer GOAWAY error changed after session close")
	}
	if c.lifecycle.sessionState != startState {
		t.Fatalf("session state after peer GOAWAY = %d, want %d", c.lifecycle.sessionState, startState)
	}
}

func TestCloseSessionClassifiesTerminalState(t *testing.T) {
	t.Parallel()
	c1, _, stop1 := newHandlerTestConn(t)
	defer stop1()
	c1.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "graceful"})

	c1.mu.Lock()
	got := c1.lifecycle.sessionState
	c1.mu.Unlock()
	if got != connStateFailed {
		t.Fatalf("non-noerror session error state = %d, want %d", got, connStateFailed)
	}

	c2, _, stop2 := newHandlerTestConn(t)
	defer stop2()
	c2.closeSession(frameSizeError("protocol", errors.New("bad frame")))

	c2.mu.Lock()
	got = c2.lifecycle.sessionState
	c2.mu.Unlock()
	if got != connStateFailed {
		t.Fatalf("protocol close state = %d, want %d", got, connStateFailed)
	}

	c3, _, stop3 := newHandlerTestConn(t)
	defer stop3()
	c3.closeSession(&ApplicationError{Code: uint64(CodeNoError), Reason: "ok"})

	c3.mu.Lock()
	got = c3.lifecycle.sessionState
	c3.mu.Unlock()
	if got != connStateClosed {
		t.Fatalf("noerror close state = %d, want %d", got, connStateClosed)
	}
}

func TestWaitReturnsNilForSessionCloseCodeNoError(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(&ApplicationError{Code: uint64(CodeNoError), Reason: "ok"})
	if err := c.Wait(context.Background()); err != nil {
		t.Fatalf("Wait after no-error close = %v, want nil", err)
	}
}

func TestGoAwayAfterSessionCloseReturnsSessionError(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()
	c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})

	goAwayErr := c.GoAway(0, 0)
	var appErr *ApplicationError
	if !errors.As(goAwayErr, &appErr) || appErr.Code != uint64(CodeInternal) {
		t.Fatalf("GoAway after close err = %v, want ApplicationError(%d)", goAwayErr, uint64(CodeInternal))
	}

	_, err := c.OpenStream(context.Background())
	if err == nil {
		t.Fatal("OpenStream after close returned nil, want session error")
	}
	if !errors.As(err, &appErr) || appErr.Code != uint64(CodeInternal) {
		t.Fatalf("OpenStream after close err = %v, want ApplicationError(%d)", err, uint64(CodeInternal))
	}
}

func TestGoAwayRejectedWhenClosingWithoutCloseErr(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	startBidi := c.sessionControl.localGoAwayBidi
	startUni := c.sessionControl.localGoAwayUni
	c.lifecycle.sessionState = connStateClosing
	c.mu.Unlock()

	err := c.GoAway(0, 0)
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("GoAway while closing err = %v, want %v", err, ErrSessionClosed)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionControl.localGoAwayBidi != startBidi || c.sessionControl.localGoAwayUni != startUni {
		t.Fatalf("local GOAWAY watermarks changed to (%d,%d), want (%d,%d)", c.sessionControl.localGoAwayBidi, c.sessionControl.localGoAwayUni, startBidi, startUni)
	}
	if c.sessionControl.hasPendingGoAway || c.sessionControl.goAwaySendActive {
		t.Fatalf("pending GOAWAY state = (%t,%t), want false/false", c.sessionControl.hasPendingGoAway, c.sessionControl.goAwaySendActive)
	}
}

func TestGoAwayRejectedAfterCloseFrameCommitted(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	startBidi := c.sessionControl.localGoAwayBidi
	startUni := c.sessionControl.localGoAwayUni
	c.shutdown.closeFrameSent = true
	c.mu.Unlock()

	err := c.GoAway(0, 0)
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("GoAway after close frame err = %v, want %v", err, ErrSessionClosed)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionControl.localGoAwayBidi != startBidi || c.sessionControl.localGoAwayUni != startUni {
		t.Fatalf("local GOAWAY watermarks changed to (%d,%d), want (%d,%d)", c.sessionControl.localGoAwayBidi, c.sessionControl.localGoAwayUni, startBidi, startUni)
	}
	if c.sessionControl.hasPendingGoAway || c.sessionControl.goAwaySendActive {
		t.Fatalf("pending GOAWAY state = (%t,%t), want false/false", c.sessionControl.hasPendingGoAway, c.sessionControl.goAwaySendActive)
	}
}

func TestFlushPendingLocalGoAwayRejectedWhenClosingWithoutCloseErr(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.sessionControl.pendingGoAwayBidi = 8
	c.sessionControl.pendingGoAwayUni = 4
	c.sessionControl.hasPendingGoAway = true
	c.sessionControl.pendingGoAwayPayload = mustGoAwayPayload(t, 8, 4, uint64(CodeNoError), "")
	c.sessionControl.goAwaySendActive = true
	c.mu.Unlock()

	err := c.flushPendingLocalGoAway()
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("flushPendingLocalGoAway while closing err = %v, want %v", err, ErrSessionClosed)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionControl.hasPendingGoAway || c.sessionControl.goAwaySendActive || len(c.sessionControl.pendingGoAwayPayload) != 0 {
		t.Fatalf("pending GOAWAY state = (%t,%t,%d), want false/false/0", c.sessionControl.hasPendingGoAway, c.sessionControl.goAwaySendActive, len(c.sessionControl.pendingGoAwayPayload))
	}
}

func TestWaitForLocalGoAwaySendReturnsSessionClosedWhenPendingGoAwayDroppedByClosing(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	waitDone := make(chan error, 1)
	c.mu.Lock()
	c.sessionControl.goAwaySendActive = true
	c.mu.Unlock()
	go func() {
		waitDone <- c.waitForLocalGoAwaySend(8, 4)
	}()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.sessionControl.goAwaySendActive = false
	c.broadcastStateWakeLocked()
	c.mu.Unlock()
	notify(c.pending.controlNotify)

	select {
	case err := <-waitDone:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("waitForLocalGoAwaySend err = %v, want %v", err, ErrSessionClosed)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("waitForLocalGoAwaySend did not return after pending GOAWAY was dropped by closing")
	}
}

func TestWaitForLocalGoAwaySendDoesNotConsumeControlNotify(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	waitDone := make(chan error, 1)
	c.mu.Lock()
	c.sessionControl.goAwaySendActive = true
	c.mu.Unlock()
	go func() {
		waitDone <- c.waitForLocalGoAwaySend(8, 4)
	}()

	c.pending.controlNotify <- struct{}{}

	c.mu.Lock()
	c.sessionControl.goAwaySendActive = false
	c.sessionControl.hasSentGoAway = true
	c.sessionControl.sentGoAwayBidi = 8
	c.sessionControl.sentGoAwayUni = 4
	c.broadcastStateWakeLocked()
	c.mu.Unlock()

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("waitForLocalGoAwaySend err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("waitForLocalGoAwaySend did not return after GOAWAY send state advanced")
	}

	select {
	case <-c.pending.controlNotify:
	default:
		t.Fatal("waitForLocalGoAwaySend consumed conn.controlNotify, want GOAWAY waiter to leave control-flush signal intact")
	}
}

func TestStateWakeBroadcastUnblocksGoAwayAndGracefulDrainWaiters(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()
	c.lifecycle.terminalCh = make(chan struct{})
	c.lifecycle.sessionState = connStateReady

	stream := &nativeStream{conn: c, id: 1, idSet: true, bidi: true}
	c.mu.Lock()
	c.storeLiveStreamLocked(stream)
	c.sessionControl.goAwaySendActive = true
	c.currentStateWakeLocked()
	c.mu.Unlock()

	goAwayDone := make(chan error, 1)
	drainDone := make(chan error, 1)

	go func() {
		goAwayDone <- c.waitForLocalGoAwaySend(8, 4)
	}()
	go func() {
		drainDone <- c.waitForGracefulCloseDrain(5 * testSignalTimeout)
	}()

	time.Sleep(10 * time.Millisecond)

	c.mu.Lock()
	c.sessionControl.goAwaySendActive = false
	c.sessionControl.hasSentGoAway = true
	c.sessionControl.sentGoAwayBidi = 8
	c.sessionControl.sentGoAwayUni = 4
	c.dropLiveStreamLocked(stream.id)
	c.broadcastStateWakeLocked()
	c.mu.Unlock()

	select {
	case err := <-goAwayDone:
		if err != nil {
			t.Fatalf("waitForLocalGoAwaySend err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("waitForLocalGoAwaySend did not return after state wake broadcast")
	}

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("waitForGracefulCloseDrain err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("waitForGracefulCloseDrain did not return after state wake broadcast")
	}
}

func TestCloseSessionErrorCodePreservedInStreamAborts(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	c.registry.streams[stream.id] = stream

	provisional := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)

	c.closeSession(frameSizeError("test", errors.New("payload too large")))

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeFrameSize) {
		t.Fatalf("stream send abort code = %v, want %d", stream.sendAbort, uint64(CodeFrameSize))
	}
	if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeFrameSize) {
		t.Fatalf("stream recv abort code = %v, want %d", stream.recvAbort, uint64(CodeFrameSize))
	}
	if provisional.sendAbort == nil || provisional.sendAbort.Code != uint64(CodeFrameSize) {
		t.Fatalf("provisional send abort code = %v, want %d", provisional.sendAbort, uint64(CodeFrameSize))
	}
	if provisional.recvAbort == nil || provisional.recvAbort.Code != uint64(CodeFrameSize) {
		t.Fatalf("provisional recv abort code = %v, want %d", provisional.recvAbort, uint64(CodeFrameSize))
	}
}

func TestCloseSessionKeepaliveTimeoutErrorCodePreservedInStreamAborts(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	c.registry.streams[stream.id] = stream

	provisional := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)

	c.closeSession(ErrKeepaliveTimeout)

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeIdleTimeout) {
		t.Fatalf("stream send abort = %v, want code %d", stream.sendAbort, uint64(CodeIdleTimeout))
	}
	if stream.sendAbort.Reason != ErrKeepaliveTimeout.Error() {
		t.Fatalf("stream send abort reason = %q, want %q", stream.sendAbort.Reason, ErrKeepaliveTimeout.Error())
	}
	if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeIdleTimeout) {
		t.Fatalf("stream recv abort = %v, want code %d", stream.recvAbort, uint64(CodeIdleTimeout))
	}
	if stream.recvAbort.Reason != ErrKeepaliveTimeout.Error() {
		t.Fatalf("stream recv abort reason = %q, want %q", stream.recvAbort.Reason, ErrKeepaliveTimeout.Error())
	}
	if provisional.sendAbort == nil || provisional.sendAbort.Code != uint64(CodeIdleTimeout) {
		t.Fatalf("provisional send abort = %v, want code %d", provisional.sendAbort, uint64(CodeIdleTimeout))
	}
	if provisional.sendAbort.Reason != ErrKeepaliveTimeout.Error() {
		t.Fatalf("provisional send abort reason = %q, want %q", provisional.sendAbort.Reason, ErrKeepaliveTimeout.Error())
	}
	if provisional.recvAbort == nil || provisional.recvAbort.Code != uint64(CodeIdleTimeout) {
		t.Fatalf("provisional recv abort = %v, want code %d", provisional.recvAbort, uint64(CodeIdleTimeout))
	}
	if provisional.recvAbort.Reason != ErrKeepaliveTimeout.Error() {
		t.Fatalf("provisional recv abort reason = %q, want %q", provisional.recvAbort.Reason, ErrKeepaliveTimeout.Error())
	}
}

func TestWaitReturnsWireErrorCodeAfterWireErrorClose(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(frameSizeError("close", errors.New("bad frame")))

	err := c.Wait(context.Background())
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("Wait after close error = %v, want *ApplicationError", err)
	}
	if appErr.Code != uint64(CodeFrameSize) {
		t.Fatalf("session close code = %d, want %d", appErr.Code, uint64(CodeFrameSize))
	}
}

func TestCloseSessionEmitsCloseFrameForFatalError(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.io.conn = &countingWriteCloser{}

	c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "fatal"})

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("queued frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
	if frame.StreamID != 0 {
		t.Fatalf("close frame stream id = %d, want 0", frame.StreamID)
	}
	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse close payload: %v", err)
	}
	if code != uint64(CodeInternal) {
		t.Fatalf("close code = %d, want %d", code, uint64(CodeInternal))
	}
	if reason != "fatal" {
		t.Fatalf("close reason = %q, want %q", reason, "fatal")
	}
	assertNoQueuedFrame(t, frames)
}

func TestCloseSessionDoesNotMarkCloseFrameSentWhenPayloadBuildFails(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.io.conn = &countingWriteCloser{}

	c.closeSession(&ApplicationError{Code: MaxVarint62 + 1, Reason: "fatal"})

	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shutdown.closeFrameSent {
		t.Fatal("closeFrameSent = true after CLOSE payload build failure, want false")
	}
	if !errors.Is(c.lifecycle.closeErr, wire.ErrValueTooLarge) {
		t.Fatalf("closeErr = %v, want ErrValueTooLarge", c.lifecycle.closeErr)
	}
	var appErr *ApplicationError
	if errors.As(c.lifecycle.closeErr, &appErr) {
		t.Fatalf("closeErr = %v, want payload-build error instead of original ApplicationError", c.lifecycle.closeErr)
	}
}

func TestCloseStreamOnSessionGracefulCloseFinishesStopSeenHalves(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           state.FirstPeerStreamID(c.config.negotiated.LocalRole, true),
		idSet:        true,
		localOpen:    testLocalOpenCommittedHiddenState(),
		localSend:    true,
		localReceive: true,
		readNotify:   make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}
	stream.initHalfStates()
	stream.conn = c
	stream.setSendStopSeen(&ApplicationError{Code: uint64(CodeCancelled)})
	stream.setRecvStopSent()

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.closeStreamOnSessionWithOptionsLocked(stream, nil, sessionCloseOptions{
		abortSource: terminalAbortLocal,
		finalize:    true,
	})
	c.mu.Unlock()

	if stream.sendHalfState() != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want send_fin", stream.sendHalfState())
	}
	if stream.recvHalfState() != state.RecvHalfFin {
		t.Fatalf("recvHalf = %v, want recv_fin", stream.recvHalfState())
	}
}

func TestCloseStreamOnSessionAbortDoesNotOverwriteTerminalHalf(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           state.FirstPeerStreamID(c.config.negotiated.LocalRole, true),
		idSet:        true,
		localOpen:    testLocalOpenCommittedHiddenState(),
		localSend:    true,
		localReceive: true,
		readNotify:   make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}
	stream.initHalfStates()
	stream.conn = c
	stream.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetDirect)
	stream.setRecvStopSent()

	appErr := &ApplicationError{Code: uint64(CodeInternal), Reason: "fatal"}
	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.closeStreamOnSessionWithOptionsLocked(stream, appErr, sessionCloseOptions{
		abortSource: terminalAbortLocal,
		finalize:    true,
	})
	c.mu.Unlock()

	if stream.sendHalfState() != state.SendHalfReset {
		t.Fatalf("sendHalf = %v, want send_reset", stream.sendHalfState())
	}
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.recvHalfState() != state.RecvHalfAborted {
		t.Fatalf("recvHalf = %v, want recv_aborted", stream.recvHalfState())
	}
	if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeInternal) {
		t.Fatalf("recvAbort = %v, want code %d", stream.recvAbort, uint64(CodeInternal))
	}
}

func TestCloseStreamOnSessionRemoteAbortMarksAbortSourceRemote(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           state.FirstLocalStreamID(c.config.negotiated.LocalRole, true),
		idSet:        true,
		localOpen:    testLocalOpenOpenedCommittedState(),
		localSend:    true,
		localReceive: true,
		readNotify:   make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}
	stream.initHalfStates()
	stream.conn = c

	appErr := &ApplicationError{Code: uint64(CodeRefusedStream), Reason: "peer close"}
	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.closeStreamOnSessionWithOptionsLocked(stream, appErr, sessionCloseOptions{
		abortSource: terminalAbortFromPeer,
		finalize:    true,
	})
	c.mu.Unlock()

	if stream.sendHalfState() != state.SendHalfAborted {
		t.Fatalf("sendHalf = %v, want send_aborted", stream.sendHalfState())
	}
	if stream.recvHalfState() != state.RecvHalfAborted {
		t.Fatalf("recvHalf = %v, want recv_aborted", stream.recvHalfState())
	}
	if !stream.sendAbortFromPeerLocked() {
		t.Fatal("send abort source not marked remote")
	}
	if !stream.recvAbortFromPeerLocked() {
		t.Fatal("recv abort source not marked remote")
	}
}

func TestReleaseAllStreamsForPeerCloseMarksAbortSourceRemote(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           state.FirstLocalStreamID(c.config.negotiated.LocalRole, true),
		idSet:        true,
		localOpen:    testLocalOpenOpenedCommittedState(),
		localSend:    true,
		localReceive: true,
		readNotify:   make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}
	stream.initHalfStates()
	stream.conn = c

	peerCloseErr := &ApplicationError{Code: uint64(CodeInternal), Reason: "peer closing"}
	c.mu.Lock()
	c.sessionControl.peerCloseErr = peerCloseErr
	c.registry.streams[stream.id] = stream
	c.releaseAllStreamsForSessionCloseLocked(&ApplicationError{Code: peerCloseErr.Code, Reason: peerCloseErr.Reason})
	c.mu.Unlock()

	if stream.sendAbort == nil || !stream.sendAbortFromPeerLocked() {
		t.Fatalf("send abort = %v, remote=%v, want remote peer-close abort", stream.sendAbort, stream.sendAbortFromPeerLocked())
	}
	if stream.recvAbort == nil || !stream.recvAbortFromPeerLocked() {
		t.Fatalf("recv abort = %v, remote=%v, want remote peer-close abort", stream.recvAbort, stream.recvAbortFromPeerLocked())
	}
	if c.tombstoneCountLocked() != 0 {
		t.Fatalf("tombstoneCountLocked() = %d, want 0 after bulk session-close release", c.tombstoneCountLocked())
	}
}

func TestPublicSessionStateHelpers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		state    SessionState
		name     string
		valid    bool
		terminal bool
	}{
		{state: SessionStateInvalid, name: "invalid", valid: false, terminal: false},
		{state: SessionStateReady, name: "ready", valid: true, terminal: false},
		{state: SessionStateDraining, name: "draining", valid: true, terminal: false},
		{state: SessionStateClosing, name: "closing", valid: true, terminal: false},
		{state: SessionStateClosed, name: "closed", valid: true, terminal: true},
		{state: SessionStateFailed, name: "failed", valid: true, terminal: true},
	}

	for _, tc := range cases {
		if got := tc.state.String(); got != tc.name {
			t.Fatalf("%v.String() = %q, want %q", tc.state, got, tc.name)
		}
		if got := tc.state.Valid(); got != tc.valid {
			t.Fatalf("%v.Valid() = %v, want %v", tc.state, got, tc.valid)
		}
		if got := tc.state.Terminal(); got != tc.terminal {
			t.Fatalf("%v.Terminal() = %v, want %v", tc.state, got, tc.terminal)
		}
	}
}

func TestConnStatePublicSurface(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	if got := c.State(); got != SessionStateReady {
		t.Fatalf("initial State() = %v, want %v", got, SessionStateReady)
	}

	c.mu.Lock()
	c.lifecycle.sessionState = connStateDraining
	c.mu.Unlock()
	if got := c.State(); got != SessionStateDraining {
		t.Fatalf("draining State() = %v, want %v", got, SessionStateDraining)
	}

	c.closeSession(&ApplicationError{Code: uint64(CodeNoError), Reason: "done"})
	if got := c.State(); got != SessionStateClosed {
		t.Fatalf("closed State() = %v, want %v", got, SessionStateClosed)
	}
}

func TestConnClosedPublicSurface(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	if c.Closed() {
		t.Fatal("Closed() on ready session = true, want false")
	}

	c.closeSession(&ApplicationError{Code: uint64(CodeNoError), Reason: "done"})

	if !c.Closed() {
		t.Fatal("Closed() after closeSession = false, want true")
	}
}

func TestSessionClosedPublicSurface(t *testing.T) {
	t.Parallel()

	if !AsSession(nil).Closed() {
		t.Fatal("AsSession(nil).Closed() = false, want true")
	}

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	session := AsSession(c)
	if session.Closed() {
		t.Fatal("session.Closed() on ready session = true, want false")
	}

	c.closeSession(io.ErrClosedPipe)

	if !session.Closed() {
		t.Fatal("session.Closed() after transport failure = false, want true")
	}
}

func TestTransportEOFFromReadyMarksSessionFailed(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(io.EOF)

	if got := c.State(); got != SessionStateFailed {
		t.Fatalf("State() after raw EOF = %v, want %v", got, SessionStateFailed)
	}

	err := c.Wait(context.Background())
	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationClose || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Wait err = %v, want io.EOF compatibility", err)
	}
}

func TestReadLoopValidateFrameErrorIsRemoteReadSessionTermination(t *testing.T) {
	t.Parallel()

	readLoopErr := readLoopSessionErr(wire.WrapError(wire.CodeProtocol, "validate frame scope", errors.New("bad frame")))
	inner := requireStructuredError(t, readLoopErr)
	if inner.Scope != ScopeSession || inner.Source != SourceRemote ||
		inner.Direction != DirectionRead || inner.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured read-loop validation error = %+v", *inner)
	}

	err := sessionOperationErr(nil, OperationOpen, readLoopErr)
	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationOpen || se.Source != SourceRemote ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured operation error = %+v", *se)
	}
}

func TestTransportEOFFromClosingMarksSessionClosed(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.shutdown.closeFrameSent = true
	c.mu.Unlock()

	c.closeSession(io.EOF)

	if got := c.State(); got != SessionStateClosed {
		t.Fatalf("State() after orderly EOF = %v, want %v", got, SessionStateClosed)
	}
	if err := c.Wait(context.Background()); err != nil {
		t.Fatalf("Wait after orderly EOF = %v, want nil", err)
	}
}

func TestOpenAfterTransportFailureReturnsStructuredTransportError(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(io.ErrClosedPipe)

	_, err := c.OpenStream(context.Background())
	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationOpen || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("open err = %v, want io.ErrClosedPipe compatibility", err)
	}
}

func TestBlockedAcceptAfterTransportFailureReturnsStructuredTransportError(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		_, err := c.AcceptStream(context.Background())
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("AcceptStream returned early: %v", err)
	case <-time.After(testSignalTimeout / 5):
	}

	c.closeSession(io.ErrClosedPipe)

	var err error
	select {
	case err = <-errCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked AcceptStream did not return after transport failure")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationAccept || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("AcceptStream err = %v, want io.ErrClosedPipe compatibility", err)
	}
}

func TestBlockedAcceptUniAfterTransportFailureReturnsStructuredTransportError(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		_, err := c.AcceptUniStream(context.Background())
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("AcceptUniStream returned early: %v", err)
	case <-time.After(testSignalTimeout / 5):
	}

	c.closeSession(io.ErrClosedPipe)

	var err error
	select {
	case err = <-errCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked AcceptUniStream did not return after transport failure")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationAccept || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("AcceptUniStream err = %v, want io.ErrClosedPipe compatibility", err)
	}
}

func TestBlockedReadAfterTransportFailureReturnsStructuredTransportError(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Read(make([]byte, 1))
		errCh <- err
	}()
	awaitStreamReadWaiter(t, stream, testSignalTimeout, "blocked Read did not enter wait state before transport failure")

	select {
	case err := <-errCh:
		t.Fatalf("Read returned early: %v", err)
	case <-time.After(testSignalTimeout / 5):
	}

	c.closeSession(io.ErrClosedPipe)

	var err error
	select {
	case err = <-errCh:
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked Read did not return after transport failure")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationRead || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Read err = %v, want io.ErrClosedPipe compatibility", err)
	}
}

func TestBlockedWriteAfterTransportFailureReturnsStructuredTransportError(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := newBlockedWriteCloseWakeStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.flow.sendSessionMax = 1
	c.flow.sendSessionUsed = 1
	c.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("Write returned early: %v", err)
	case <-time.After(testSignalTimeout / 5):
	}

	c.closeSession(io.ErrClosedPipe)

	var err error
	select {
	case err = <-errCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked Write did not return after transport failure")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Write err = %v, want io.ErrClosedPipe compatibility", err)
	}
}

func TestBlockedWriteFinalAfterTransportFailureReturnsStructuredTransportError(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := newBlockedWriteCloseWakeStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.flow.sendSessionMax = 1
	c.flow.sendSessionUsed = 1
	c.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.WriteFinal([]byte("x"))
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("WriteFinal returned early: %v", err)
	case <-time.After(testSignalTimeout / 5):
	}

	c.closeSession(io.ErrClosedPipe)

	var err error
	select {
	case err = <-errCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked WriteFinal did not return after transport failure")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("WriteFinal err = %v, want io.ErrClosedPipe compatibility", err)
	}
}

func TestBlockedProvisionalWriteAfterTransportFailureReturnsStructuredTransportError(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	_ = c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	blocked := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		_, err := blocked.Write([]byte("x"))
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("provisional Write returned early: %v", err)
	case <-time.After(testSignalTimeout / 5):
	}

	c.closeSession(io.ErrClosedPipe)

	var err error
	select {
	case err = <-errCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked provisional Write did not return after transport failure")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("provisional Write err = %v, want io.ErrClosedPipe compatibility", err)
	}
}

func TestActiveStreamReadAfterTransportFailureIsNotEOF(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	c.closeSession(io.EOF)

	_, err := stream.Read(make([]byte, 1))
	if errors.Is(err, io.EOF) {
		t.Fatalf("stream Read after transport failure err = %v, want non-EOF failure", err)
	}

	var appErr *ApplicationError
	if !errors.As(err, &appErr) || appErr.Code != uint64(CodeInternal) {
		t.Fatalf("stream Read err = %v, want ApplicationError(%d)", err, uint64(CodeInternal))
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationRead || se.Source != SourceLocal ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationAbort ||
		se.WireCode != uint64(CodeInternal) {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestConnWaitAcceptsNilContext(t *testing.T) {
	var nilCtx context.Context

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
	}
	close(c.lifecycle.closedCh)

	if err := c.Wait(nilCtx); err != nil {
		t.Fatalf("Wait(nil) err = %v, want nil", err)
	}
}

func TestConnPublicSessionAPIsAcceptNilContext(t *testing.T) {
	var nilCtx context.Context

	client, server := newConnPair(t)

	type bidiResult struct {
		stream *nativeStream
		err    error
	}
	acceptBidiCh := make(chan bidiResult, 1)
	go func() {
		stream, err := server.AcceptStream(nilCtx)
		acceptBidiCh <- bidiResult{stream: mustNativeStreamImpl(stream), err: err}
	}()

	stream, n, err := client.OpenAndSend(nilCtx, []byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("OpenAndSend(nil) = (%v, %d, %v), want (stream, 5, nil)", stream, n, err)
	}
	if stream == nil || stream.StreamID() == 0 {
		t.Fatalf("OpenAndSend(nil) stream = %#v, want committed stream", stream)
	}

	acceptedBidi := <-acceptBidiCh
	if acceptedBidi.err != nil {
		t.Fatalf("AcceptStream(nil) err = %v, want nil", acceptedBidi.err)
	}
	if acceptedBidi.stream == nil {
		t.Fatal("AcceptStream(nil) stream = nil, want stream")
	}

	bidiPayload := make([]byte, 5)
	if _, err := io.ReadFull(acceptedBidi.stream, bidiPayload); err != nil {
		t.Fatalf("accepted bidi ReadFull err = %v", err)
	}
	if string(bidiPayload) != "hello" {
		t.Fatalf("accepted bidi payload = %q, want %q", string(bidiPayload), "hello")
	}

	type uniResult struct {
		stream *nativeRecvStream
		err    error
	}
	acceptUniCh := make(chan uniResult, 1)
	go func() {
		stream, err := server.AcceptUniStream(nilCtx)
		acceptUniCh <- uniResult{stream: mustNativeRecvStreamImpl(stream), err: err}
	}()

	uniStream, n, err := client.OpenUniAndSend(nilCtx, []byte("world"))
	if err != nil || n != 5 {
		t.Fatalf("OpenUniAndSend(nil) = (%v, %d, %v), want (stream, 5, nil)", uniStream, n, err)
	}
	if uniStream == nil || uniStream.StreamID() == 0 {
		t.Fatalf("OpenUniAndSend(nil) stream = %#v, want committed stream", uniStream)
	}

	acceptedUni := <-acceptUniCh
	if acceptedUni.err != nil {
		t.Fatalf("AcceptUniStream(nil) err = %v, want nil", acceptedUni.err)
	}
	if acceptedUni.stream == nil {
		t.Fatal("AcceptUniStream(nil) stream = nil, want stream")
	}

	uniPayload := make([]byte, 5)
	if _, err := io.ReadFull(acceptedUni.stream, uniPayload); err != nil {
		t.Fatalf("accepted uni ReadFull err = %v", err)
	}
	if string(uniPayload) != "world" {
		t.Fatalf("accepted uni payload = %q, want %q", string(uniPayload), "world")
	}

	if _, err := client.Ping(nilCtx, []byte("ok")); err != nil {
		t.Fatalf("Ping(nil) err = %v, want nil", err)
	}
}

func TestBeginContextWriteSetsDeadlineAndClears(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	setter := newRecordingWriteDeadlineSetter()

	writeCtx, err := beginContextWrite(ctx, setter)
	if err != nil {
		t.Fatalf("beginContextWrite err = %v", err)
	}
	if calls := setter.deadlineCalls(); len(calls) != 1 || !calls[0].Equal(deadline) {
		t.Fatalf("deadline calls after begin = %v, want [%v]", calls, deadline)
	}

	writeCtx.clear()
	calls := setter.deadlineCalls()
	if len(calls) != 2 || !calls[1].IsZero() {
		t.Fatalf("deadline calls after clear = %v, want trailing zero deadline", calls)
	}
}

func TestBeginContextWriteCanceledContextDoesNotTouchDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	setter := newRecordingWriteDeadlineSetter()

	if _, err := beginContextWrite(ctx, setter); !errors.Is(err, context.Canceled) {
		t.Fatalf("beginContextWrite canceled err = %v, want %v", err, context.Canceled)
	}
	if calls := setter.deadlineCalls(); len(calls) != 0 {
		t.Fatalf("deadline calls = %v, want none", calls)
	}
}

func TestContextWriteMapsCancellationDeadlineError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	setter := newRecordingWriteDeadlineSetter()

	writeCtx, err := beginContextWrite(ctx, setter)
	if err != nil {
		t.Fatalf("beginContextWrite err = %v", err)
	}
	cancel()
	setter.waitForNonZeroDeadline(t)

	if got := writeCtx.err(os.ErrDeadlineExceeded); !errors.Is(got, context.Canceled) {
		t.Fatalf("contextWrite.err = %v, want %v", got, context.Canceled)
	}
	writeCtx.clear()
	calls := setter.deadlineCalls()
	if len(calls) < 2 || !calls[len(calls)-1].IsZero() {
		t.Fatalf("deadline calls after clear = %v, want trailing zero deadline", calls)
	}
}

func TestConnNilReceiversHandlePublicSessionAPIs(t *testing.T) {
	var nilCtx context.Context

	var c *Conn

	if _, err := c.OpenStream(nilCtx); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("nil OpenStream err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := c.OpenUniStream(nilCtx); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("nil OpenUniStream err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := c.AcceptStream(nilCtx); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("nil AcceptStream err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := c.AcceptUniStream(nilCtx); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("nil AcceptUniStream err = %v, want %v", err, ErrSessionClosed)
	}
	if _, _, err := c.OpenAndSend(nilCtx, []byte("x")); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("nil OpenAndSend err = %v, want %v", err, ErrSessionClosed)
	}
	if _, _, err := c.OpenUniAndSend(nilCtx, []byte("x")); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("nil OpenUniAndSend err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := c.Ping(nilCtx, nil); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("nil Ping err = %v, want %v", err, ErrSessionClosed)
	}
	if err := c.Wait(nilCtx); err != nil {
		t.Fatalf("nil Wait err = %v, want nil", err)
	}
}

func TestConnZeroValuePublicSessionAPIsStayInvalidAndDoNotHang(t *testing.T) {
	t.Parallel()

	var nilCtx context.Context
	c := &Conn{}

	if got := c.State(); got != SessionStateInvalid {
		t.Fatalf("zero-value State() = %v, want %v", got, SessionStateInvalid)
	}
	if !c.Closed() {
		t.Fatal("zero-value Closed() = false, want true")
	}
	if got := c.Stats().State; got != SessionStateInvalid {
		t.Fatalf("zero-value Stats().State = %v, want %v", got, SessionStateInvalid)
	}
	if _, err := c.OpenStream(nilCtx); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("zero-value OpenStream err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := c.AcceptStream(nilCtx); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("zero-value AcceptStream err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := c.Ping(nilCtx, nil); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("zero-value Ping err = %v, want %v", err, ErrSessionClosed)
	}
	if err := c.GoAway(0, 0); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("zero-value GoAway err = %v, want %v", err, ErrSessionClosed)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("zero-value Close err = %v, want nil", err)
	}
	if err := c.Wait(nilCtx); err != nil {
		t.Fatalf("zero-value Wait err = %v, want nil", err)
	}
	if got := AsSession(c).State(); got != SessionStateInvalid {
		t.Fatalf("AsSession(zero).State() = %v, want %v", got, SessionStateInvalid)
	}
	if !AsSession(c).Closed() {
		t.Fatal("AsSession(zero).Closed() = false, want true")
	}
}

func TestConnGettersNilSafe(t *testing.T) {
	t.Parallel()

	var conn *Conn
	if got := conn.LocalPreface(); got != (Preface{}) {
		t.Fatalf("LocalPreface() = %+v, want zero value", got)
	}
	if got := conn.PeerPreface(); got != (Preface{}) {
		t.Fatalf("PeerPreface() = %+v, want zero value", got)
	}
	if got := conn.Negotiated(); got != (Negotiated{}) {
		t.Fatalf("Negotiated() = %+v, want zero value", got)
	}
}

func TestConnGettersConcurrentWithLockedMutation(t *testing.T) {
	t.Parallel()

	conn := &Conn{}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = conn.LocalPreface()
				_ = conn.PeerPreface()
				_ = conn.Negotiated()
			}
		}
	}()

	conn.mu.Lock()
	conn.config.local = Preface{PrefaceVersion: 1, MinProto: 1, MaxProto: 1}
	conn.config.peer = Preface{PrefaceVersion: 1, MinProto: 1, MaxProto: 1}
	conn.config.negotiated = Negotiated{Proto: 1, LocalRole: RoleInitiator, PeerRole: RoleResponder}
	conn.mu.Unlock()

	close(stop)
	<-done

	if got := conn.LocalPreface().PrefaceVersion; got != 1 {
		t.Fatalf("LocalPreface().PrefaceVersion = %d, want 1", got)
	}
	if got := conn.PeerPreface().PrefaceVersion; got != 1 {
		t.Fatalf("PeerPreface().PrefaceVersion = %d, want 1", got)
	}
	if got := conn.Negotiated().Proto; got != 1 {
		t.Fatalf("Negotiated().Proto = %d, want 1", got)
	}
}

func waitForEventType(t *testing.T, events <-chan Event, typ EventType) Event {
	t.Helper()

	timeout := time.After(testSignalTimeout)
	for {
		select {
		case ev := <-events:
			if ev.Type == typ {
				return ev
			}
		case <-timeout:
			t.Fatalf("timed out waiting for event type %v", typ)
			return Event{}
		}
	}
}

func collectEventsOfType(events <-chan Event, typ EventType) []Event {
	var out []Event
	for {
		select {
		case ev := <-events:
			if ev.Type == typ {
				out = append(out, ev)
			}
		default:
			return out
		}
	}
}

func TestEventStreamOpenedAndAccepted(t *testing.T) {
	t.Parallel()

	ctx, cancel := testContext(t)
	defer cancel()

	clientEvents := make(chan Event, 8)
	serverEvents := make(chan Event, 8)
	clientCfg := &Config{
		EventHandler: func(ev Event) {
			select {
			case clientEvents <- ev:
			default:
			}
		},
	}
	serverCfg := &Config{
		EventHandler: func(ev Event) {
			select {
			case serverEvents <- ev:
			default:
			}
		},
	}
	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		s, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(s)
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write stream: %v", err)
	}

	var accepted *nativeStream
	select {
	case accepted = <-acceptCh:
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	}
	if accepted == nil {
		t.Fatal("expected accepted stream")
	}

	openedEvent := waitForEventType(t, clientEvents, EventStreamOpened)
	if openedEvent.StreamID != stream.StreamID() {
		t.Fatalf("opened event StreamID = %d, want %d", openedEvent.StreamID, stream.StreamID())
	}
	if openedEvent.SessionState != SessionStateReady {
		t.Fatalf("opened event session state = %v, want %v", openedEvent.SessionState, SessionStateReady)
	}
	if !openedEvent.Local {
		t.Fatal("expected opened event to be local")
	}
	if openedEvent.ApplicationVisible {
		t.Fatal("expected local stream_opened event to be non-application-visible")
	}
	if openedEvent.Err != nil {
		t.Fatalf("opened event error = %v, want nil", openedEvent.Err)
	}

	acceptedEvent := waitForEventType(t, serverEvents, EventStreamAccepted)
	if acceptedEvent.StreamID != accepted.StreamID() {
		t.Fatalf("accepted event StreamID = %d, want %d", acceptedEvent.StreamID, accepted.StreamID())
	}
	if acceptedEvent.SessionState != SessionStateReady {
		t.Fatalf("accepted event session state = %v, want %v", acceptedEvent.SessionState, SessionStateReady)
	}
	if acceptedEvent.Local {
		t.Fatal("expected accepted event to be peer-opened")
	}
	if !acceptedEvent.ApplicationVisible {
		t.Fatal("expected accepted event to be application-visible")
	}
}

func TestEventStreamOpenedByCloseWrite(t *testing.T) {
	t.Parallel()

	ctx, cancel := testContext(t)
	defer cancel()

	clientEvents := make(chan Event, 8)
	clientCfg := &Config{
		EventHandler: func(ev Event) {
			select {
			case clientEvents <- ev:
			default:
			}
		},
	}
	client, server := newConnPairWithConfig(t, clientCfg, nil)

	acceptedCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		s, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptedCh <- mustNativeStreamImpl(s)
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	openedEvent := waitForEventType(t, clientEvents, EventStreamOpened)
	if openedEvent.StreamID == 0 {
		t.Fatal("expected opened event StreamID != 0")
	}
	if openedEvent.StreamID != stream.StreamID() {
		t.Fatalf("opened event StreamID = %d, want %d", openedEvent.StreamID, stream.StreamID())
	}

	var accepted *nativeStream
	select {
	case accepted = <-acceptedCh:
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	}
	if accepted == nil {
		t.Fatal("expected accepted stream")
	}
	if accepted.StreamID() != stream.StreamID() {
		t.Fatalf("accepted StreamID = %d, want %d", accepted.StreamID(), stream.StreamID())
	}
}

func TestEventStreamOpenedByCloseRead(t *testing.T) {
	t.Parallel()

	ctx, cancel := testContext(t)
	defer cancel()

	clientEvents := make(chan Event, 8)
	clientCfg := &Config{
		EventHandler: func(ev Event) {
			select {
			case clientEvents <- ev:
			default:
			}
		},
	}
	client, _ := newConnPairWithConfig(t, clientCfg, nil)

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := stream.CloseRead(); err != nil {
		t.Fatalf("close read: %v", err)
	}

	openedEvent := waitForEventType(t, clientEvents, EventStreamOpened)
	if openedEvent.StreamID == 0 {
		t.Fatal("expected opened event StreamID != 0")
	}
	if openedEvent.StreamID != stream.StreamID() {
		t.Fatalf("opened event StreamID = %d, want %d", openedEvent.StreamID, stream.StreamID())
	}
	if stream.StreamID() == 0 {
		t.Fatal("expected committed StreamID after CloseRead")
	}
	if !openedEvent.Local {
		t.Fatal("expected opened event to be local")
	}
}

func TestEventStreamOpenedByAbortWithError(t *testing.T) {
	t.Parallel()

	clientEvents := make(chan Event, 8)
	clientCfg := &Config{
		EventHandler: func(ev Event) {
			select {
			case clientEvents <- ev:
			default:
			}
		},
	}
	client := newConfigPolicyTestConn(clientCfg)

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		select {
		case <-client.pending.controlNotify:
			client.mu.Lock()
			result := client.takePendingControlWriteRequestLocked()
			client.mu.Unlock()
			if result.hasRequest() && result.request.done != nil {
				result.request.done <- nil
			}
		case req := <-client.writer.urgentWriteCh:
			if req.done != nil {
				req.done <- nil
			}
		case req := <-client.writer.writeCh:
			if req.done != nil {
				req.done <- nil
			}
		case req := <-client.writer.advisoryWriteCh:
			if req.done != nil {
				req.done <- nil
			}
		case <-time.After(testSignalTimeout):
		}
	}()

	client.mu.Lock()
	stream := client.newLocalStreamWithIDLocked(state.FirstLocalStreamID(client.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	client.registry.streams[stream.id] = stream
	client.appendUnseenLocalLocked(stream)
	client.mu.Unlock()

	if err := stream.CloseWithError(uint64(CodeInternal), "bye"); err != nil {
		t.Fatalf("close with error: %v", err)
	}

	openedEvent := waitForEventType(t, clientEvents, EventStreamOpened)
	if openedEvent.StreamID == 0 {
		t.Fatal("expected opened event StreamID != 0")
	}
	if openedEvent.StreamID != stream.StreamID() {
		t.Fatalf("opened event StreamID = %d, want %d", openedEvent.StreamID, stream.StreamID())
	}
	if !openedEvent.Local {
		t.Fatal("expected opened event to be local")
	}
	if openedEvent.ApplicationVisible {
		t.Fatal("expected opening-abort stream_opened event to be non-application-visible")
	}

	select {
	case <-writeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for local ABORT request drain")
	}
}

func TestEventSessionClosed(t *testing.T) {
	t.Parallel()

	closeEvents := make(chan Event, 4)
	clientCfg := &Config{
		EventHandler: func(ev Event) {
			if ev.Type != EventSessionClosed {
				return
			}
			select {
			case closeEvents <- ev:
			default:
			}
		},
	}
	client, _ := newConnPairWithConfig(t, clientCfg, nil)

	client.CloseWithError(&ApplicationError{Code: uint64(CodeInternal), Reason: "close test"})

	closedEvent := waitForEventType(t, closeEvents, EventSessionClosed)
	var appErr *ApplicationError
	if !errors.As(closedEvent.Err, &appErr) {
		t.Fatalf("session closed event error = %T, want *ApplicationError", closedEvent.Err)
	}
	if closedEvent.SessionState != SessionStateFailed {
		t.Fatalf("session closed event state = %v, want %v", closedEvent.SessionState, SessionStateFailed)
	}
	if appErr.Code != uint64(CodeInternal) {
		t.Fatalf("session close event code = %d, want %d", appErr.Code, uint64(CodeInternal))
	}
}

func TestEventSessionClosedHandlerCanReenterClose(t *testing.T) {
	t.Parallel()

	var client *Conn
	handlerDone := make(chan struct{}, 1)
	clientCfg := &Config{
		EventHandler: func(ev Event) {
			if ev.Type != EventSessionClosed {
				return
			}
			_ = client.Close()
			select {
			case handlerDone <- struct{}{}:
			default:
			}
		},
	}
	client, _ = newConnPairWithConfig(t, clientCfg, nil)

	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		client.CloseWithError(&ApplicationError{Code: uint64(CodeInternal), Reason: "close test"})
	}()

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("session closed handler did not finish after reentering Close")
	}

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseWithError deadlocked while session closed handler reentered Close")
	}
}

func TestEventStreamOpenedEmittedOnce(t *testing.T) {
	t.Parallel()

	ctx, cancel := testContext(t)
	defer cancel()

	clientEvents := make(chan Event, 8)
	clientCfg := &Config{
		EventHandler: func(ev Event) {
			select {
			case clientEvents <- ev:
			default:
			}
		},
	}
	client, server := newConnPairWithConfig(t, clientCfg, nil)

	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		s, err := server.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 1)
		_, _ = s.Read(buf)
		_, _ = s.Read(buf)
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write stream: %v", err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	select {
	case <-acceptDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for peer drain")
	}

	opened := collectEventsOfType(clientEvents, EventStreamOpened)
	if len(opened) != 1 {
		t.Fatalf("stream_opened count = %d, want 1", len(opened))
	}
	if opened[0].StreamID != stream.StreamID() {
		t.Fatalf("stream_opened StreamID = %d, want %d", opened[0].StreamID, stream.StreamID())
	}
}

func TestEventHandlersMayReenterSessionAndStreamAPIs(t *testing.T) {
	t.Parallel()

	ctx, cancel := testContext(t)
	defer cancel()

	handlerDone := make(chan struct{}, 4)
	var client *Conn
	clientCfg := &Config{
		EventHandler: func(ev Event) {
			switch ev.Type {
			case EventStreamOpened:
				if client != nil {
					_ = client.Stats()
				}
				if ev.Stream != nil {
					_ = ev.Stream.Metadata()
					_ = ev.Stream.StreamID()
				}
				select {
				case handlerDone <- struct{}{}:
				default:
				}
			case EventSessionClosed:
				if client != nil {
					_ = client.Stats()
					_ = client.PeerCloseError()
				}
				select {
				case handlerDone <- struct{}{}:
				default:
				}
			default:
			}
		},
	}
	client, _ = newConnPairWithConfig(t, clientCfg, nil)

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write stream: %v", err)
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stream_opened handler")
	}

	client.CloseWithError(&ApplicationError{Code: uint64(CodeInternal), Reason: "reenter"})
	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for session_closed handler")
	}
}

func TestEventSurfaceOptInLeavesFlagsUnsetWithoutHandler(t *testing.T) {
	t.Parallel()

	ctx, cancel := testContext(t)
	defer cancel()

	client, server := newConnPair(t)

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		s, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(s)
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	streamImpl := requireNativeStreamImpl(t, stream)
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write stream: %v", err)
	}

	var accepted *nativeStream
	select {
	case accepted = <-acceptCh:
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for accepted stream")
	}

	client.mu.Lock()
	if streamImpl.openedEventSentFlag() {
		client.mu.Unlock()
		t.Fatal("openedEventSent = true without configured handler")
	}
	client.mu.Unlock()

	server.mu.Lock()
	if accepted.acceptedEventSentFlag() {
		server.mu.Unlock()
		t.Fatal("acceptedEventSent = true without configured handler")
	}
	server.mu.Unlock()

	if err := accepted.Close(); err != nil {
		t.Fatalf("accepted close: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("stream close: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
}

func TestStatsTrackWriteProgress(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	awaitConnState(t, client, testSignalTimeout, func(c *Conn) bool {
		stats := c.Stats()
		return !stats.LastAppProgress.IsZero() && !stats.LastStreamProgress.IsZero()
	}, "stream/application write progress was not recorded")
}

func TestStatsTrackReadProgress(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(stream)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept stream: %v", err)
	case serverStream = <-acceptCh:
	}

	readCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 4)
		_, err := serverStream.Read(buf)
		readCh <- err
	}()

	if err := <-readCh; err != nil {
		t.Fatalf("read: %v", err)
	}

	awaitConnState(t, server, testSignalTimeout, func(c *Conn) bool {
		stats := c.Stats()
		return !stats.LastAppProgress.IsZero() && !stats.LastStreamProgress.IsZero()
	}, "read progress was not recorded")
}

func TestStatsExposeSessionState(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)

	if got := client.Stats().State; got != SessionStateReady {
		t.Fatalf("initial stats state = %v, want %v", got, SessionStateReady)
	}

	client.CloseWithError(&ApplicationError{Code: uint64(CodeInternal), Reason: "fatal"})

	awaitConnState(t, client, testSignalTimeout, func(c *Conn) bool {
		return c.Stats().State == SessionStateFailed
	}, "stats state did not reach failed after abort")
}

func TestStatsExposeExplicitProgressBuckets(t *testing.T) {
	t.Parallel()

	now := time.Now()
	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), sessionState: connStateReady},
		liveness: connLivenessState{
			lastTransportWriteAt:  now.Add(1 * time.Second),
			lastControlProgressAt: now.Add(2 * time.Second),
			lastStreamProgressAt:  now.Add(3 * time.Second),
			lastAppProgressAt:     now.Add(4 * time.Second),
		},
	}

	stats := c.Stats()
	if stats.Progress.TransportProgressAt != c.liveness.lastTransportWriteAt {
		t.Fatalf("TransportProgressAt = %v, want %v", stats.Progress.TransportProgressAt, c.liveness.lastTransportWriteAt)
	}
	if stats.Progress.MuxControlProgressAt != c.liveness.lastControlProgressAt {
		t.Fatalf("MuxControlProgressAt = %v, want %v", stats.Progress.MuxControlProgressAt, c.liveness.lastControlProgressAt)
	}
	if stats.Progress.StreamProgressAt != c.liveness.lastStreamProgressAt {
		t.Fatalf("StreamProgressAt = %v, want %v", stats.Progress.StreamProgressAt, c.liveness.lastStreamProgressAt)
	}
	if stats.Progress.ApplicationProgressAt != c.liveness.lastAppProgressAt {
		t.Fatalf("ApplicationProgressAt = %v, want %v", stats.Progress.ApplicationProgressAt, c.liveness.lastAppProgressAt)
	}
}

func TestNoteReasonAndHiddenCountersSaturate(t *testing.T) {
	t.Parallel()

	c := &Conn{

		ingress: connIngressAccountingState{
			hiddenStreamsRefused:       ^uint64(0),
			hiddenStreamsReaped:        ^uint64(0),
			hiddenUnreadBytesDiscarded: ^uint64(0) - 1,
		}, retention: connRetentionState{resetReasonCount: map[uint64]uint64{7: ^uint64(0)},
			abortReasonCount: map[uint64]uint64{9: ^uint64(0)}},
	}

	c.noteResetReasonLocked(7)
	c.noteAbortReasonLocked(9)
	c.noteHiddenStreamRefusedLocked()
	c.noteHiddenStreamReapedLocked()
	c.noteHiddenUnreadDiscardLocked(8)

	if got := c.retention.resetReasonCount[7]; got != ^uint64(0) {
		t.Fatalf("resetReasonCount[7] = %d, want %d", got, ^uint64(0))
	}
	if got := c.retention.abortReasonCount[9]; got != ^uint64(0) {
		t.Fatalf("abortReasonCount[9] = %d, want %d", got, ^uint64(0))
	}
	if got := c.ingress.hiddenStreamsRefused; got != ^uint64(0) {
		t.Fatalf("hiddenStreamsRefused = %d, want %d", got, ^uint64(0))
	}
	if got := c.ingress.hiddenStreamsReaped; got != ^uint64(0) {
		t.Fatalf("hiddenStreamsReaped = %d, want %d", got, ^uint64(0))
	}
	if got := c.ingress.hiddenUnreadBytesDiscarded; got != ^uint64(0) {
		t.Fatalf("hiddenUnreadBytesDiscarded = %d, want %d", got, ^uint64(0))
	}
}

func TestNoteBlockedWriteSaturatesDuration(t *testing.T) {
	t.Parallel()

	c := &Conn{signals: connRuntimeSignalState{livenessCh: make(chan struct{}, 1)}}
	c.metrics.blockedWriteTime = maxDuration - time.Nanosecond

	c.noteBlockedWrite(2 * time.Nanosecond)

	if got := c.metrics.blockedWriteTime; got != maxDuration {
		t.Fatalf("blockedWriteTime = %v, want %v", got, maxDuration)
	}
}

func TestStatsProgressViewMirrorsLegacyTimestamps(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	awaitConnState(t, client, testSignalTimeout, func(c *Conn) bool {
		stats := c.Stats()
		return !stats.LastTransportWrite.IsZero() &&
			!stats.LastStreamProgress.IsZero() &&
			!stats.LastAppProgress.IsZero() &&
			stats.Progress.TransportProgressAt.Equal(stats.LastTransportWrite) &&
			stats.Progress.StreamProgressAt.Equal(stats.LastStreamProgress) &&
			stats.Progress.ApplicationProgressAt.Equal(stats.LastAppProgress)
	}, "progress view did not mirror legacy timestamps")
}

func TestCloseWithOpenStreamsSendsGoAwayBeforeClose(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	testMarkLocalOpenVisible(stream)
	c.registry.streams[streamID] = stream
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	c.registry.nextPeerBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.registry.nextPeerUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	expectedInitialBidi := maxPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityBidi)
	expectedInitialUni := maxPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityUni)
	expectedBidi := acceptedPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityBidi, c.registry.nextPeerBidi)
	expectedUni := acceptedPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityUni, c.registry.nextPeerUni)
	c.mu.Unlock()

	if err := c.Close(); !errors.Is(err, ErrGracefulCloseTimeout) {
		t.Fatalf("Close() = %v, want %v", err, ErrGracefulCloseTimeout)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}
	parsed, err := parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse GOAWAY payload: %v", err)
	}
	if parsed.LastAcceptedBidi != expectedInitialBidi || parsed.LastAcceptedUni != expectedInitialUni {
		t.Fatalf("GOAWAY watermarks = (%d, %d), want (%d, %d)",
			parsed.LastAcceptedBidi, parsed.LastAcceptedUni, expectedInitialBidi, expectedInitialUni)
	}
	if parsed.Code != uint64(CodeNoError) {
		t.Fatalf("GOAWAY code = %d, want %d", parsed.Code, uint64(CodeNoError))
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("second frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}
	parsed, err = parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse second GOAWAY payload: %v", err)
	}
	if parsed.LastAcceptedBidi != expectedBidi || parsed.LastAcceptedUni != expectedUni {
		t.Fatalf("final GOAWAY watermarks = (%d, %d), want (%d, %d)",
			parsed.LastAcceptedBidi, parsed.LastAcceptedUni, expectedBidi, expectedUni)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("third frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
}

func TestCloseRecomputesFinalGoAwayAfterDrainInterval(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	testMarkLocalOpenVisible(stream)
	c.registry.streams[streamID] = stream
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	initialNextPeerBidi := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	initialNextPeerUni := state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	c.registry.nextPeerBidi = initialNextPeerBidi
	c.registry.nextPeerUni = initialNextPeerUni
	expectedInitialBidi := maxPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityBidi)
	expectedInitialUni := maxPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityUni)
	updatedNextPeerBidi := initialNextPeerBidi + 4
	updatedNextPeerUni := initialNextPeerUni + 4
	expectedFinalBidi := acceptedPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityBidi, updatedNextPeerBidi)
	expectedFinalUni := acceptedPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityUni, updatedNextPeerUni)
	c.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- c.Close()
	}()

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}
	parsed, err := parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse first GOAWAY payload: %v", err)
	}
	if parsed.LastAcceptedBidi != expectedInitialBidi || parsed.LastAcceptedUni != expectedInitialUni {
		t.Fatalf("initial GOAWAY watermarks = (%d, %d), want (%d, %d)",
			parsed.LastAcceptedBidi, parsed.LastAcceptedUni, expectedInitialBidi, expectedInitialUni)
	}

	c.mu.Lock()
	c.registry.nextPeerBidi = updatedNextPeerBidi
	c.registry.nextPeerUni = updatedNextPeerUni
	c.mu.Unlock()

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("second frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}
	parsed, err = parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse second GOAWAY payload: %v", err)
	}
	if parsed.LastAcceptedBidi != expectedFinalBidi || parsed.LastAcceptedUni != expectedFinalUni {
		t.Fatalf("final GOAWAY watermarks = (%d, %d), want (%d, %d)",
			parsed.LastAcceptedBidi, parsed.LastAcceptedUni, expectedFinalBidi, expectedFinalUni)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("third frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrGracefulCloseTimeout) {
			t.Fatalf("Close() = %v, want %v", err, ErrGracefulCloseTimeout)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Close() did not finish after recomputed final GOAWAY")
	}
}

func TestCloseWithoutOpenStreamsOnlySendsClose(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
	if frame.StreamID != 0 {
		t.Fatalf("close frame stream-id = %d, want 0", frame.StreamID)
	}
	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parseErrorPayload err = %v", err)
	}
	if code != uint64(CodeNoError) || reason != "" {
		t.Fatalf("close payload = (%d, %q), want (%d, %q)", code, reason, uint64(CodeNoError), "")
	}
	assertNoQueuedFrame(t, frames)
}

func TestCloseWithOutstandingCloseFrameStillReturnsDrainTimeout(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	testMarkLocalOpenVisible(stream)
	c.registry.streams[streamID] = stream
	c.shutdown.closeFramePending = true
	c.mu.Unlock()

	if err := c.closeWithGoAwayAndClose(goAwayInitialSkip, 0, 0); !errors.Is(err, ErrGracefulCloseTimeout) {
		t.Fatalf("closeWithGoAwayAndClose() = %v, want %v", err, ErrGracefulCloseTimeout)
	}
}

func TestCloseStartsByClosingLocalAdmission(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	c.registry.streams[streamID] = stream
	c.registry.nextPeerBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.registry.nextPeerUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	c.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- c.Close()
	}()

	awaitConnState(t, c, testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		sessState := c.lifecycle.sessionState
		c.mu.Unlock()
		return sessState == connStateDraining || sessState == connStateClosing || state.IsSessionFinished(sessState)
	}, "session did not enter graceful close state")

	if _, err := c.OpenStream(context.Background()); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("OpenStream while closing = %v, want ErrSessionClosed", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close() while closing = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Close() did not finish after entering closing state")
	}
}

func TestOpenAfterCompletedLocalCloseReturnsLocalStructuredError(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	if err := client.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	_, err := client.OpenStream(ctx)
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("OpenStream after local close = %v, want %v", err, ErrSessionClosed)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationOpen || se.Source != SourceLocal ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestGracefulCloseTransitionsThroughDrainingAndBlocksLocalOpen(t *testing.T) {
	t.Parallel()

	frames := make(chan Frame, 64)
	releaseFirst := make(chan struct{})
	stop := make(chan struct{})
	var stopOnce sync.Once

	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1), terminalNotify: make(chan struct{}, 1)},
		signals: connRuntimeSignalState{livenessCh: make(chan struct{}, 1), acceptCh: make(chan struct{}, 1)},
		writer:  connWriterRuntimeState{writeCh: make(chan writeRequest), urgentWriteCh: make(chan writeRequest)},

		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(settings.MaxFramePayload),
		}, config: connConfigState{local: Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			peer:       Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, Capabilities: 0, LocalRole: localRole, PeerRole: peerRole, PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: settings.InitialMaxData,
			sendSessionMax: settings.InitialMaxData}, registry: connRegistryState{streams: make(map[uint64]*nativeStream),
			tombstones: make(map[uint64]streamTombstone),

			nextLocalBidi: state.FirstLocalStreamID(localRole, true),
			nextLocalUni:  state.FirstLocalStreamID(localRole, false),
			nextPeerBidi:  state.FirstPeerStreamID(localRole, true),
			nextPeerUni:   state.FirstPeerStreamID(localRole, false)},
	}
	defer stopOnce.Do(func() { close(stop) })

	streamID := state.FirstLocalStreamID(localRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	testMarkLocalOpenCommitted(stream)
	c.registry.streams[streamID] = stream
	c.registry.nextPeerBidi = state.FirstPeerStreamID(localRole, true) + 4
	c.registry.nextPeerUni = state.FirstPeerStreamID(localRole, false) + 4

	go func() {
		first := true
		for {
			select {
			case <-stop:
				return
			case req := <-c.writer.writeCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				req.done <- nil
			case req := <-c.writer.urgentWriteCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				if first {
					first = false
					<-releaseFirst
				}
				req.done <- nil
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- c.Close()
	}()

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	awaitConnState(t, c, testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.lifecycle.sessionState == connStateDraining && c.shutdown.gracefulCloseActive
	}, "Close() did not stay in draining before CLOSE")

	if _, err := c.OpenStream(context.Background()); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("OpenStream during graceful drain = %v, want ErrSessionClosed", err)
	}

	close(releaseFirst)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close() = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Close() did not finish after releasing graceful drain")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shutdown.gracefulCloseActive {
		t.Fatal("gracefulCloseActive still set after Close() completed")
	}
}

func TestCloseAfterPeerNoErrorCloseIsNoop(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeNoError), "complete"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = %v, want non-nil", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close after peer no-error close = %v, want nil", err)
	}

	assertNoQueuedFrame(t, frames)
}

func TestCloseAfterPeerErrorCloseReturnsError(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "protocol"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = %v, want non-nil", err)
	}

	err = c.Close()
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("Close after peer close err = %v, want ApplicationError(%d)", err, uint64(CodeProtocol))
	}
	if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "protocol" {
		t.Fatalf("peer close code=%d reason=%q, want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeProtocol), "protocol")
	}

	assertNoQueuedFrame(t, frames)
}

func TestCloseConcurrentCallsEmitSingleCloseFrame(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	c.registry.streams[streamID] = stream
	c.registry.nextPeerBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.registry.nextPeerUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	c.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- c.Close()
	}()

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("second observed frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("third observed frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
	assertNoQueuedFrame(t, frames)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first Close() = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("first Close() did not finish after second call")
	}
}

func TestConcurrentLocalGoAwayKeepsOnlyMostRestrictivePendingReplacement(t *testing.T) {
	t.Parallel()

	frames := make(chan Frame, 64)
	releaseFirst := make(chan struct{})
	stop := make(chan struct{})
	var stopOnce sync.Once

	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals: connRuntimeSignalState{livenessCh: make(chan struct{}, 1), acceptCh: make(chan struct{}, 1)},
		writer:  connWriterRuntimeState{writeCh: make(chan writeRequest), urgentWriteCh: make(chan writeRequest)},

		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(settings.MaxFramePayload),
		}, config: connConfigState{local: Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			peer:       Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, Capabilities: 0, LocalRole: localRole, PeerRole: peerRole, PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: settings.InitialMaxData,
			sendSessionMax: settings.InitialMaxData}, registry: connRegistryState{streams: make(map[uint64]*nativeStream),
			tombstones: make(map[uint64]streamTombstone),

			nextLocalBidi: state.FirstLocalStreamID(localRole, true),
			nextLocalUni:  state.FirstLocalStreamID(localRole, false),
			nextPeerBidi:  state.FirstPeerStreamID(localRole, true),
			nextPeerUni:   state.FirstPeerStreamID(localRole, false)},
	}
	defer stopOnce.Do(func() { close(stop) })

	go func() {
		first := true
		for {
			select {
			case <-stop:
				return
			case req := <-c.writer.writeCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				req.done <- nil
			case req := <-c.writer.urgentWriteCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				if first {
					first = false
					<-releaseFirst
				}
				req.done <- nil
			}
		}
	}()

	initialBidi := state.FirstPeerStreamID(localRole, true) + 8
	initialUni := state.FirstPeerStreamID(localRole, false) + 8
	midBidi := initialBidi - 4
	midUni := initialUni - 4
	finalBidi := initialBidi - 8
	finalUni := initialUni - 8

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- c.GoAway(initialBidi, initialUni)
	}()

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}
	parsed, err := parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse first GOAWAY payload: %v", err)
	}
	if parsed.LastAcceptedBidi != initialBidi || parsed.LastAcceptedUni != initialUni {
		t.Fatalf("first GOAWAY watermarks = (%d, %d), want (%d, %d)",
			parsed.LastAcceptedBidi, parsed.LastAcceptedUni, initialBidi, initialUni)
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- c.GoAway(midBidi, midUni)
	}()

	awaitConnState(t, c, testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.sessionControl.goAwaySendActive &&
			c.sessionControl.hasPendingGoAway &&
			c.sessionControl.pendingGoAwayBidi == midBidi &&
			c.sessionControl.pendingGoAwayUni == midUni
	}, "intermediate pending GOAWAY was not recorded")
	select {
	case <-c.pending.controlNotify:
	default:
	}

	thirdDone := make(chan error, 1)
	go func() {
		thirdDone <- c.GoAway(finalBidi, finalUni)
	}()

	deadline := time.Now().Add(testSignalTimeout)
	for {
		c.mu.Lock()
		pendingFinal := c.sessionControl.goAwaySendActive &&
			c.sessionControl.hasPendingGoAway &&
			c.sessionControl.pendingGoAwayBidi == finalBidi &&
			c.sessionControl.pendingGoAwayUni == finalUni
		snapshot := struct {
			active     bool
			hasPending bool
			bidi       uint64
			uni        uint64
		}{
			active:     c.sessionControl.goAwaySendActive,
			hasPending: c.sessionControl.hasPendingGoAway,
			bidi:       c.sessionControl.pendingGoAwayBidi,
			uni:        c.sessionControl.pendingGoAwayUni,
		}
		c.mu.Unlock()
		if pendingFinal {
			break
		}
		select {
		case err := <-thirdDone:
			t.Fatalf("third GoAway finished before pending replacement became visible: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("most restrictive pending GOAWAY was not retained (active=%t pending=%t bidi=%d uni=%d)",
				snapshot.active, snapshot.hasPending, snapshot.bidi, snapshot.uni)
		}
		runtime.Gosched()
	}

	close(releaseFirst)

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("second frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}
	parsed, err = parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse second GOAWAY payload: %v", err)
	}
	if parsed.LastAcceptedBidi != finalBidi || parsed.LastAcceptedUni != finalUni {
		t.Fatalf("replacement GOAWAY watermarks = (%d, %d), want (%d, %d)",
			parsed.LastAcceptedBidi, parsed.LastAcceptedUni, finalBidi, finalUni)
	}

	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first GoAway = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("first GoAway did not finish after releasing first send")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second GoAway = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("second GoAway did not finish after replacement send")
	}
	select {
	case err := <-thirdDone:
		if err != nil {
			t.Fatalf("third GoAway = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("third GoAway did not finish after replacement send")
	}

	assertNoQueuedFrame(t, frames)
}

func TestCloseAfterPriorGoAwaySendsMoreRestrictiveReplacement(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	testMarkLocalOpenCommitted(stream)
	c.registry.streams[streamID] = stream
	c.registry.nextPeerBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 8
	c.registry.nextPeerUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	c.mu.Unlock()

	if err := c.GoAway(state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)+8, state.FirstPeerStreamID(c.config.negotiated.LocalRole, false)+4); err != nil {
		t.Fatalf("initial GoAway = %v", err)
	}
	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("initial frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	c.mu.Lock()
	c.registry.nextPeerBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.registry.nextPeerUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	expectedBidi := acceptedPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityBidi, c.registry.nextPeerBidi)
	expectedUni := acceptedPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityUni, c.registry.nextPeerUni)
	c.mu.Unlock()

	if err := c.Close(); err != nil {
		t.Fatalf("Close() after prior GoAway = %v", err)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("replacement frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}
	parsed, err := parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse replacement GOAWAY payload: %v", err)
	}
	if parsed.LastAcceptedBidi != expectedBidi || parsed.LastAcceptedUni != expectedUni {
		t.Fatalf("replacement GOAWAY watermarks = (%d, %d), want (%d, %d)",
			parsed.LastAcceptedBidi, parsed.LastAcceptedUni, expectedBidi, expectedUni)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("final frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
	assertNoQueuedFrame(t, frames)
}

func TestGracefulCloseWaitsForActiveStreamsBeforeCloseFrame(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	testMarkLocalOpenVisible(stream)
	c.registry.streams[streamID] = stream
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	c.registry.nextPeerBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.registry.nextPeerUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	c.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- c.Close()
	}()

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("second frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	stream.setSendFin()
	stream.setRecvFin()
	c.maybeCompactTerminalLocked(stream)
	c.mu.Unlock()
	notify(c.signals.livenessCh)

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("final frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close() = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Close() did not finish after active stream drained")
	}
}

func TestGracefulCloseIgnoresUnreadPeerUniStream(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, false)
	stream := c.newPeerStreamLocked(streamID)
	stream.recvBuffer = 5
	stream.applicationVisible = true
	stream.setRecvFin()
	c.storeLiveStreamLocked(stream)
	c.enqueueAcceptedLocked(stream)
	c.mu.Unlock()

	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("final frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
	assertNoQueuedFrame(t, frames)
}

func TestGracefulCloseIgnoresPeerOpenedBidiWithoutLocalSend(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newPeerStreamLocked(streamID)
	stream.recvBuffer = 5
	stream.applicationVisible = true
	stream.setRecvFin()
	c.storeLiveStreamLocked(stream)
	c.enqueueAcceptedLocked(stream)
	c.mu.Unlock()

	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("final frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
	assertNoQueuedFrame(t, frames)
}

func TestGracefulCloseWaitsForPeerOpenedBidiWithLocalSend(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newPeerStreamLocked(streamID)
	stream.sendSent = 1
	stream.applicationVisible = true
	stream.setRecvFin()
	c.storeLiveStreamLocked(stream)
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	c.registry.nextPeerBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.registry.nextPeerUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	c.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- c.Close()
	}()

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("second frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	stream.setSendFin()
	c.maybeCompactTerminalLocked(stream)
	c.mu.Unlock()
	notify(c.signals.livenessCh)

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("final frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close() = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Close() did not finish after peer-opened bidi local send drained")
	}
}

func TestGracefulCloseReclaimsCommittedNeverPeerVisibleLocalStream(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	testMarkLocalOpenCommitted(stream)
	c.registry.streams[streamID] = stream
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	c.registry.nextPeerBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.registry.nextPeerUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	c.mu.Unlock()

	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("second frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("final frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.registry.streams[streamID]; ok {
		t.Fatalf("stream %d still present after graceful close reclaim", streamID)
	}
	if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("send abort = %#v, want REFUSED_STREAM", stream.sendAbort)
	}
	if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("recv abort = %#v, want REFUSED_STREAM", stream.recvAbort)
	}
}

func TestGracefulCloseReclaimsProvisionalNeverPeerVisibleLocalStream(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	c.registry.nextPeerBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.registry.nextPeerUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	initialNextLocalBidi := c.registry.nextLocalBidi
	c.mu.Unlock()

	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("first frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("second frame type = %s, want %s", frame.Type, FrameTypeGOAWAY)
	}

	frame = awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("final frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queues.provisionalBidi.items) != 0 {
		t.Fatalf("provisional bidi count = %d, want 0", len(c.queues.provisionalBidi.items))
	}
	if c.registry.nextLocalBidi != initialNextLocalBidi {
		t.Fatalf("nextLocalBidi = %d, want %d", c.registry.nextLocalBidi, initialNextLocalBidi)
	}
	if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("send abort = %#v, want REFUSED_STREAM", stream.sendAbort)
	}
	if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("recv abort = %#v, want REFUSED_STREAM", stream.recvAbort)
	}
}

func TestAbortClearsStreamsAndSessionPendingState(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	stream.sendSent = 11
	stream.recvBuffer = 7
	stream.readBuf = []byte("queued")
	stream.queuedDataBytes = 19
	stream.inflightQueued = 5
	stream.blockedAt = 101
	stream.blockedSet = true
	c.registry.streams[streamID] = stream
	c.appendUnseenLocalLocked(stream)

	provisionalBidi := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	provisionalBidi.sendSent = 13
	provisionalBidi.recvBuffer = 5
	provisionalBidi.readBuf = []byte("pending")
	provisionalBidi.queuedDataBytes = 23
	provisionalBidi.inflightQueued = 7
	provisionalBidi.blockedAt = 202
	provisionalBidi.blockedSet = true

	provisionalUni := c.newProvisionalLocalStreamLocked(streamArityUni, OpenOptions{}, nil)
	provisionalUni.sendSent = 17
	provisionalUni.recvBuffer = 3
	provisionalUni.readBuf = []byte("queued")
	provisionalUni.queuedDataBytes = 29
	provisionalUni.inflightQueued = 11
	provisionalUni.blockedAt = 303
	provisionalUni.blockedSet = true

	c.flow.queuedDataBytes = stream.queuedDataBytes + provisionalBidi.queuedDataBytes + provisionalUni.queuedDataBytes
	c.flow.queuedDataStreams = map[*nativeStream]struct{}{
		stream:          {},
		provisionalBidi: {},
		provisionalUni:  {},
	}

	accept := c.newLocalStreamWithIDLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	accept.idSet = true
	accept.applicationVisible = true
	accept.recvBuffer = 9
	c.enqueueAcceptedLocked(accept)
	acceptUni := c.newLocalStreamWithIDLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, false), streamArityUni, OpenOptions{}, nil)
	acceptUni.applicationVisible = true
	acceptUni.recvBuffer = 4
	c.enqueueAcceptedLocked(acceptUni)

	testSetPendingStreamMaxData(c, streamID, 99)
	testSetPendingStreamBlocked(c, streamID, 11)
	testSetPendingPriorityUpdate(c, streamID, []byte{0xde, 0xad})
	c.pending.sessionBlocked = 33
	c.pending.hasSessionBlocked = true
	c.pending.sessionMaxData = 333
	c.pending.hasSessionMaxData = true
	c.pending.controlBytes = 7
	c.sessionControl.pendingGoAwayBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 8
	c.sessionControl.pendingGoAwayUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 8
	c.sessionControl.pendingGoAwayPayload = []byte{0xaa, 0xbb, 0xcc}
	c.sessionControl.hasPendingGoAway = true
	c.sessionControl.goAwaySendActive = true
	c.pending.sessionBlockedAt = 44
	c.pending.sessionBlockedSet = true
	c.registry.activePeerBidi = 3
	c.registry.activePeerUni = 2
	c.writer.scheduler = rt.BatchScheduler{
		ActiveGroupRefs: map[uint64]uint64{7: 1},
		State: rt.BatchState{
			RootVirtualTime:   23,
			ServiceSeq:        17,
			StreamFinishTag:   map[uint64]uint64{streamID: 3, 999: 7},
			StreamLastService: map[uint64]uint64{streamID: 8, 999: 11},
			GroupVirtualTime:  map[rt.GroupKey]uint64{{Kind: 0, Value: streamID}: 5, {Kind: 1, Value: 7}: 9},
			GroupFinishTag:    map[rt.GroupKey]uint64{{Kind: 0, Value: streamID}: 6, {Kind: 1, Value: 7}: 10},
			GroupLastService:  map[rt.GroupKey]uint64{{Kind: 0, Value: streamID}: 12, {Kind: 1, Value: 7}: 13},
		},
	}

	c.liveness.keepaliveInterval = 500 * time.Millisecond
	c.liveness.keepaliveMaxPingInterval = 2 * time.Second
	c.liveness.readIdlePingDueAt = time.Now().Add(time.Second)
	c.liveness.writeIdlePingDueAt = time.Now().Add(time.Second)
	c.liveness.maxPingDueAt = time.Now().Add(2 * time.Second)
	c.liveness.pingOutstanding = true
	c.liveness.pingPayload = []byte("ping")
	c.liveness.lastPingSentAt = time.Now()
	c.liveness.lastPongAt = time.Now()
	c.liveness.lastPingRTT = 123 * time.Millisecond
	c.liveness.pingDone = make(chan struct{})
	c.mu.Unlock()

	c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})

	c.mu.Lock()
	if c.lifecycle.sessionState != connStateFailed {
		t.Fatalf("sessionState = %d, want %d", c.lifecycle.sessionState, connStateFailed)
	}
	if len(c.registry.streams) != 0 {
		t.Fatalf("streams count = %d, want 0", len(c.registry.streams))
	}
	if len(c.queues.provisionalBidi.items) != 0 {
		t.Fatalf("provisionalBidi count = %d, want 0", len(c.queues.provisionalBidi.items))
	}
	if len(c.queues.provisionalUni.items) != 0 {
		t.Fatalf("provisionalUni count = %d, want 0", len(c.queues.provisionalUni.items))
	}
	if len(c.queues.acceptBidi.items) != 0 {
		t.Fatalf("acceptBidi count = %d, want 0", len(c.queues.acceptBidi.items))
	}
	if len(c.queues.acceptUni.items) != 0 {
		t.Fatalf("acceptUni count = %d, want 0", len(c.queues.acceptUni.items))
	}
	if c.queues.acceptBidiBytes != 0 || c.queues.acceptUniBytes != 0 {
		t.Fatalf("accept queued bytes = (%d,%d), want (0,0)", c.queues.acceptBidiBytes, c.queues.acceptUniBytes)
	}
	if testPendingStreamMaxDataCount(c) != 0 {
		t.Fatalf("pendingStreamMaxData count = %d, want 0", testPendingStreamMaxDataCount(c))
	}
	if testPendingStreamBlockedCount(c) != 0 {
		t.Fatalf("pendingStreamBlocked count = %d, want 0", testPendingStreamBlockedCount(c))
	}
	if testPendingPriorityUpdateCount(c) != 0 {
		t.Fatalf("pendingPriorityUpdate count = %d, want 0", testPendingPriorityUpdateCount(c))
	}
	if c.pending.sessionBlocked != 0 || c.pending.hasSessionBlocked {
		t.Fatalf("pendingSessionBlocked = %d/%t, want 0/false", c.pending.sessionBlocked, c.pending.hasSessionBlocked)
	}
	if c.pending.sessionMaxData != 0 || c.pending.hasSessionMaxData {
		t.Fatalf("pendingSessionMaxData = %d/%t, want 0/false", c.pending.sessionMaxData, c.pending.hasSessionMaxData)
	}
	if c.sessionControl.pendingGoAwayBidi != 0 || c.sessionControl.pendingGoAwayUni != 0 || len(c.sessionControl.pendingGoAwayPayload) != 0 || c.sessionControl.hasPendingGoAway || c.sessionControl.goAwaySendActive {
		t.Fatalf("pending GOAWAY state = (%d,%d,%d,%t,%t), want zero/false",
			c.sessionControl.pendingGoAwayBidi, c.sessionControl.pendingGoAwayUni, len(c.sessionControl.pendingGoAwayPayload), c.sessionControl.hasPendingGoAway, c.sessionControl.goAwaySendActive)
	}
	if c.pending.controlBytes != 0 || c.pending.priorityBytes != 0 {
		t.Fatalf("pending buffered control/advisory bytes = (%d,%d), want (0,0)", c.pending.controlBytes, c.pending.priorityBytes)
	}
	if c.flow.queuedDataBytes != 0 || len(c.flow.queuedDataStreams) != 0 {
		t.Fatalf("queued write reservations = (%d,%d), want (0,0)", c.flow.queuedDataBytes, len(c.flow.queuedDataStreams))
	}
	if c.pending.sessionBlockedAt != 0 || c.pending.sessionBlockedSet {
		t.Fatalf("session blocked state = (%d,%t), want (0,false)", c.pending.sessionBlockedAt, c.pending.sessionBlockedSet)
	}
	if c.registry.activePeerBidi != 0 || c.registry.activePeerUni != 0 {
		t.Fatalf("active peer counts = (%d,%d), want (0,0)", c.registry.activePeerBidi, c.registry.activePeerUni)
	}
	if len(c.writer.scheduler.ActiveGroupRefs) != 0 {
		t.Fatalf("activeGroupRefs count = %d, want 0", len(c.writer.scheduler.ActiveGroupRefs))
	}
	if len(c.writer.scheduler.State.StreamFinishTag) != 0 ||
		len(c.writer.scheduler.State.StreamLastService) != 0 ||
		len(c.writer.scheduler.State.GroupVirtualTime) != 0 ||
		len(c.writer.scheduler.State.GroupFinishTag) != 0 ||
		len(c.writer.scheduler.State.GroupLastService) != 0 ||
		c.writer.scheduler.State.RootVirtualTime != 0 ||
		c.writer.scheduler.State.ServiceSeq != 0 {
		t.Fatalf("writeBatchState not cleared: %#v", c.writer.scheduler.State)
	}
	if c.liveness.keepaliveInterval != 0 {
		t.Fatalf("keepaliveInterval = %v, want 0", c.liveness.keepaliveInterval)
	}
	if c.liveness.keepaliveMaxPingInterval != 0 {
		t.Fatalf("keepaliveMaxPingInterval = %v, want 0", c.liveness.keepaliveMaxPingInterval)
	}
	if !c.liveness.readIdlePingDueAt.IsZero() || !c.liveness.writeIdlePingDueAt.IsZero() || !c.liveness.maxPingDueAt.IsZero() {
		t.Fatalf("keepalive due state = (%v, %v, %v), want zero", c.liveness.readIdlePingDueAt, c.liveness.writeIdlePingDueAt, c.liveness.maxPingDueAt)
	}
	if c.liveness.pingOutstanding {
		t.Fatalf("pingOutstanding = %t, want false", c.liveness.pingOutstanding)
	}
	if len(c.liveness.pingPayload) != 0 {
		t.Fatalf("pingPayload len = %d, want 0", len(c.liveness.pingPayload))
	}
	if c.liveness.lastPingRTT != 0 {
		t.Fatalf("lastPingRTT = %v, want 0", c.liveness.lastPingRTT)
	}
	if c.liveness.pingDone != nil {
		t.Fatalf("pingDone not nil")
	}
	if stream.sendSent != 0 || stream.recvBuffer != 0 || len(stream.readBuf) != 0 || stream.queuedDataBytes != 0 || stream.inflightQueued != 0 || stream.blockedAt != 0 || stream.blockedSet {
		t.Fatalf("stream resources not released = sendSent=%d recvBuffer=%d readBuf=%d queued=%d inflight=%d blocked=(%d,%t)",
			stream.sendSent, stream.recvBuffer, len(stream.readBuf), stream.queuedDataBytes, stream.inflightQueued, stream.blockedAt, stream.blockedSet)
	}
	if provisionalBidi.sendSent != 0 || provisionalBidi.recvBuffer != 0 || len(provisionalBidi.readBuf) != 0 || provisionalBidi.queuedDataBytes != 0 || provisionalBidi.inflightQueued != 0 || provisionalBidi.blockedAt != 0 || provisionalBidi.blockedSet {
		t.Fatalf("provisional bidi resources not released = sendSent=%d recvBuffer=%d readBuf=%d queued=%d inflight=%d blocked=(%d,%t)",
			provisionalBidi.sendSent, provisionalBidi.recvBuffer, len(provisionalBidi.readBuf), provisionalBidi.queuedDataBytes, provisionalBidi.inflightQueued, provisionalBidi.blockedAt, provisionalBidi.blockedSet)
	}
	if provisionalUni.sendSent != 0 || provisionalUni.recvBuffer != 0 || len(provisionalUni.readBuf) != 0 || provisionalUni.queuedDataBytes != 0 || provisionalUni.inflightQueued != 0 || provisionalUni.blockedAt != 0 || provisionalUni.blockedSet {
		t.Fatalf("provisional uni resources not released = sendSent=%d recvBuffer=%d readBuf=%d queued=%d inflight=%d blocked=(%d,%t)",
			provisionalUni.sendSent, provisionalUni.recvBuffer, len(provisionalUni.readBuf), provisionalUni.queuedDataBytes, provisionalUni.inflightQueued, provisionalUni.blockedAt, provisionalUni.blockedSet)
	}
	if stream.unseenLocalIndex != invalidStreamQueueIndex {
		t.Fatalf("stream unseenLocalIndex = %d, want %d", stream.unseenLocalIndex, invalidStreamQueueIndex)
	}
	if provisionalBidi.provisionalIndex != invalidStreamQueueIndex || provisionalUni.provisionalIndex != invalidStreamQueueIndex {
		t.Fatalf("provisional indexes = (%d,%d), want (%d,%d)",
			provisionalBidi.provisionalIndex, provisionalUni.provisionalIndex,
			invalidStreamQueueIndex, invalidStreamQueueIndex)
	}
	if accept.acceptIndex != invalidStreamQueueIndex || acceptUni.acceptIndex != invalidStreamQueueIndex {
		t.Fatalf("accept indexes = (%d,%d), want (%d,%d)",
			accept.acceptIndex, acceptUni.acceptIndex,
			invalidStreamQueueIndex, invalidStreamQueueIndex)
	}
	if accept.enqueued || acceptUni.enqueued {
		t.Fatalf("accepted stream queue flags = (%t,%t), want (false,false)", accept.enqueued, acceptUni.enqueued)
	}
	c.mu.Unlock()

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("queued frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse CLOSE payload: %v", err)
	}
	if code != uint64(CodeInternal) {
		t.Fatalf("CLOSE code = %d, want %d", code, uint64(CodeInternal))
	}
	if reason != "bye" {
		t.Fatalf("CLOSE reason = %q, want %q", reason, "bye")
	}
	assertNoQueuedFrame(t, frames)
	select {
	case <-c.lifecycle.closedCh:
	default:
		t.Fatalf("closedCh not closed by closeSession")
	}
}

func TestPeerSessionMaxDataIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.flow.sendSessionMax = 128
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = &ApplicationError{Code: uint64(CodeInternal)}
	c.mu.Unlock()

	if err := c.handleMaxDataFrame(Frame{
		Type:    FrameTypeMAXDATA,
		Payload: mustEncodeVarint(256),
	}); err != nil {
		t.Fatalf("handle session MAX_DATA while closing: %v", err)
	}

	c.mu.Lock()
	if c.flow.sendSessionMax != 128 {
		c.mu.Unlock()
		t.Fatalf("sendSessionMax = %d, want 128", c.flow.sendSessionMax)
	}
	c.mu.Unlock()
}

func TestPeerSessionBlockedIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.flow.recvSessionAdvertised = 1024
	c.flow.recvSessionReceived = 512
	c.flow.recvSessionPending = 10
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = &ApplicationError{Code: uint64(CodeInternal)}
	c.mu.Unlock()

	if err := c.handleBlockedFrame(Frame{
		Type:    FrameTypeBLOCKED,
		Payload: mustEncodeVarint(1024),
	}); err != nil {
		t.Fatalf("handle session BLOCKED while closing: %v", err)
	}

	c.mu.Lock()
	if c.flow.recvSessionAdvertised != 1024 {
		c.mu.Unlock()
		t.Fatalf("recvSessionAdvertised = %d, want 1024", c.flow.recvSessionAdvertised)
	}
	if c.flow.recvSessionPending != 10 {
		c.mu.Unlock()
		t.Fatalf("recvSessionPending = %d, want 10", c.flow.recvSessionPending)
	}
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 0 || len(advisory) != 0 {
		t.Fatalf("drained control after closing BLOCKED = %d urgent / %d advisory, want 0 / 0", len(urgent), len(advisory))
	}
}

func TestPeerSessionMaxDataStillAppliesWhileDraining(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.flow.sendSessionMax = 128
	c.lifecycle.sessionState = connStateDraining
	c.mu.Unlock()

	if err := c.handleMaxDataFrame(Frame{
		Type:    FrameTypeMAXDATA,
		Payload: mustEncodeVarint(256),
	}); err != nil {
		t.Fatalf("handle session MAX_DATA while draining: %v", err)
	}

	c.mu.Lock()
	if c.flow.sendSessionMax != 256 {
		c.mu.Unlock()
		t.Fatalf("sendSessionMax = %d, want 256", c.flow.sendSessionMax)
	}
	c.mu.Unlock()
}

func TestPeerSessionBlockedStillReplenishesWhileDraining(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	c.mu.Lock()
	c.lifecycle.sessionState = connStateDraining
	target := c.sessionWindowTargetLocked()
	c.flow.recvSessionAdvertised = target
	c.flow.recvSessionReceived = target / 2
	c.flow.recvSessionPending = 10
	c.mu.Unlock()

	if err := c.handleBlockedFrame(Frame{
		Type:    FrameTypeBLOCKED,
		Payload: mustEncodeVarint(target),
	}); err != nil {
		t.Fatalf("handle session BLOCKED while draining: %v", err)
	}

	c.mu.Lock()
	wantAdvertised := c.flow.recvSessionReceived + target
	if c.flow.recvSessionAdvertised != wantAdvertised {
		c.mu.Unlock()
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, wantAdvertised)
	}
	if c.flow.recvSessionPending != 0 {
		c.mu.Unlock()
		t.Fatalf("recvSessionPending = %d, want 0", c.flow.recvSessionPending)
	}
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory frames = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("urgent frames = %d, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(wantAdvertised)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA %d", urgent[0], wantAdvertised)
	}
}

func TestPeerPingStillRepliesWhileDraining(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateDraining
	c.mu.Unlock()

	payload := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	if err := c.handleFrame(Frame{Type: FrameTypePING, Payload: payload}); err != nil {
		t.Fatalf("handleFrame(PING) while draining: %v", err)
	}

	reply := awaitQueuedFrame(t, frames)
	if reply.Type != FrameTypePONG {
		t.Fatalf("queued frame type = %v, want %v", reply.Type, FrameTypePONG)
	}
	if !bytes.Equal(reply.Payload, payload) {
		t.Fatalf("queued PONG payload = %x, want %x", reply.Payload, payload)
	}
}

func TestPeerPingIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.mu.Unlock()

	if err := c.handleFrame(Frame{Type: FrameTypePING, Payload: []byte{0, 1, 2, 3, 4, 5, 6, 7}}); err != nil {
		t.Fatalf("handleFrame(PING) while closing: %v", err)
	}
	assertNoQueuedFrame(t, frames)
}

func TestPeerPongStillClearsOutstandingPingWhileDraining(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	payload := []byte{9, 8, 7, 6, 5, 4, 3, 2}

	c.mu.Lock()
	c.lifecycle.sessionState = connStateDraining
	c.liveness.pingOutstanding = true
	c.liveness.pingPayload = append([]byte(nil), payload...)
	c.liveness.lastPingSentAt = time.Now().Add(-25 * time.Millisecond)
	c.liveness.pingDone = make(chan struct{})
	c.mu.Unlock()

	if err := c.handlePongFrame(Frame{Type: FrameTypePONG, Payload: payload}); err != nil {
		t.Fatalf("handlePongFrame while draining: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.liveness.pingOutstanding {
		t.Fatal("pingOutstanding = true, want false after matching PONG")
	}
	if !c.liveness.lastPongAt.After(time.Time{}) {
		t.Fatal("lastPongAt not updated")
	}
	if c.liveness.lastPingRTT <= 0 {
		t.Fatalf("lastPingRTT = %v, want > 0", c.liveness.lastPingRTT)
	}
}

func TestPeerPongIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	payload := []byte{9, 8, 7, 6, 5, 4, 3, 2}
	done := make(chan struct{})

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.liveness.pingOutstanding = true
	c.liveness.pingPayload = append([]byte(nil), payload...)
	c.liveness.lastPingSentAt = time.Now().Add(-25 * time.Millisecond)
	c.liveness.pingDone = done
	c.mu.Unlock()

	if err := c.handlePongFrame(Frame{Type: FrameTypePONG, Payload: payload}); err != nil {
		t.Fatalf("handlePongFrame while closing: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.liveness.pingOutstanding {
		t.Fatal("pingOutstanding = false, want true when PONG is ignored during closing")
	}
	if c.liveness.lastPongAt.After(time.Time{}) {
		t.Fatal("lastPongAt updated, want unchanged zero time")
	}
	if c.liveness.lastPingRTT != 0 {
		t.Fatalf("lastPingRTT = %v, want 0", c.liveness.lastPingRTT)
	}
	if c.liveness.pingDone != done {
		t.Fatal("pingDone changed while closing")
	}
}

func TestMalformedPeerPongIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.abuse.controlBudgetFrames = 3
	c.abuse.mixedBudgetFrames = 5
	c.abuse.noopControlCount = 7
	c.mu.Unlock()

	if err := c.handlePongFrame(Frame{Type: FrameTypePONG, Payload: []byte{0x01}}); err != nil {
		t.Fatalf("handlePongFrame malformed while closing: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abuse.controlBudgetFrames != 3 || c.abuse.mixedBudgetFrames != 5 || c.abuse.noopControlCount != 7 {
		t.Fatalf("budgets changed after ignored malformed PONG = (%d,%d,%d), want (3,5,7)", c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
	}
}

func TestPriorityUpdateStillAppliesWhileDraining(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
	defer stop()

	payload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(9)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	c.mu.Lock()
	c.lifecycle.sessionState = connStateDraining
	stream.priority = 1
	c.mu.Unlock()

	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}); err != nil {
		t.Fatalf("handleExtFrame while draining: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.priority != 9 {
		t.Fatalf("stream priority = %d, want 9", stream.priority)
	}
}

func TestPriorityUpdateIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
	defer stop()

	payload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(9)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	stream.priority = 1
	c.mu.Unlock()

	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}); err != nil {
		t.Fatalf("handleExtFrame while closing: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.priority != 1 {
		t.Fatalf("stream priority = %d, want unchanged 1", stream.priority)
	}
}

func TestMalformedPriorityUpdateIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.abuse.controlBudgetFrames = 3
	c.abuse.mixedBudgetFrames = 5
	c.abuse.noopControlCount = 7
	c.mu.Unlock()

	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), Payload: []byte{0xff}}); err != nil {
		t.Fatalf("handleExtFrame malformed while closing: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abuse.controlBudgetFrames != 3 || c.abuse.mixedBudgetFrames != 5 || c.abuse.noopControlCount != 7 {
		t.Fatalf("budgets changed after ignored malformed EXT = (%d,%d,%d), want (3,5,7)", c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
	}
}

func TestDirectPriorityUpdateHandlerIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
	defer stop()

	payload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(9)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}
	_, n, err := ParseVarint(payload)
	if err != nil {
		t.Fatalf("parse priority update subtype: %v", err)
	}

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	stream.priority = 1
	c.mu.Unlock()

	if err := c.handlePriorityUpdateFrame(stream.id, payload[n:]); err != nil {
		t.Fatalf("handlePriorityUpdateFrame while closing: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.priority != 1 {
		t.Fatalf("stream priority = %d, want unchanged 1", stream.priority)
	}
}

func TestPeerDataIgnoredWhenClosingWithoutSideEffects(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.abuse.controlBudgetFrames = 7
	c.abuse.mixedBudgetFrames = 9
	c.abuse.noopDataCount = 11
	c.flow.recvSessionUsed = 13
	c.mu.Unlock()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleFrame(Frame{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte("late")}); err != nil {
		t.Fatalf("handleFrame(DATA) while closing: %v", err)
	}

	c.mu.Lock()
	if len(c.registry.streams) != 0 {
		c.mu.Unlock()
		t.Fatalf("streams opened after terminal DATA = %d, want 0", len(c.registry.streams))
	}
	if c.flow.recvSessionUsed != 13 {
		c.mu.Unlock()
		t.Fatalf("recvSessionUsed = %d, want 13", c.flow.recvSessionUsed)
	}
	if c.abuse.controlBudgetFrames != 7 || c.abuse.mixedBudgetFrames != 9 || c.abuse.noopDataCount != 11 {
		c.mu.Unlock()
		t.Fatalf("budgets changed after ignored DATA = (%d,%d,%d), want (7,9,11)", c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopDataCount)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestPeerStreamControlIgnoredWhenClosingWithoutBudgetSideEffects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		frame Frame
	}{
		{
			name: "MAX_DATA",
			frame: Frame{
				Type:     FrameTypeMAXDATA,
				StreamID: state.FirstPeerStreamID(RoleInitiator, true),
				Payload:  mustEncodeVarint(64),
			},
		},
		{
			name: "BLOCKED",
			frame: Frame{
				Type:     FrameTypeBLOCKED,
				StreamID: state.FirstPeerStreamID(RoleInitiator, true),
				Payload:  mustEncodeVarint(64),
			},
		},
		{
			name: "STOP_SENDING",
			frame: Frame{
				Type:     FrameTypeStopSending,
				StreamID: state.FirstPeerStreamID(RoleInitiator, true),
				Payload:  mustEncodeVarint(uint64(CodeCancelled)),
			},
		},
		{
			name: "RESET",
			frame: Frame{
				Type:     FrameTypeRESET,
				StreamID: state.FirstPeerStreamID(RoleInitiator, true),
				Payload:  mustEncodeVarint(uint64(CodeCancelled)),
			},
		},
		{
			name: "ABORT",
			frame: Frame{
				Type:     FrameTypeABORT,
				StreamID: state.FirstPeerStreamID(RoleInitiator, true),
				Payload:  mustEncodeVarint(uint64(CodeCancelled)),
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, frames, stop := newHandlerTestConn(t)
			defer stop()

			c.mu.Lock()
			c.lifecycle.sessionState = connStateClosing
			c.lifecycle.closeErr = ErrSessionClosed
			c.abuse.controlBudgetFrames = 3
			c.abuse.mixedBudgetFrames = 5
			c.abuse.noopControlCount = 7
			c.mu.Unlock()

			if err := c.handleFrame(tc.frame); err != nil {
				t.Fatalf("handleFrame(%s) while closing: %v", tc.name, err)
			}

			c.mu.Lock()
			if len(c.registry.streams) != 0 {
				c.mu.Unlock()
				t.Fatalf("streams changed after ignored %s = %d, want 0", tc.name, len(c.registry.streams))
			}
			if c.abuse.controlBudgetFrames != 3 || c.abuse.mixedBudgetFrames != 5 || c.abuse.noopControlCount != 7 {
				c.mu.Unlock()
				t.Fatalf("budgets changed after ignored %s = (%d,%d,%d), want (3,5,7)", tc.name, c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
			}
			c.mu.Unlock()
			assertNoQueuedFrame(t, frames)
		})
	}
}

func TestPeerGoAwayIgnoredWhenClosingWithoutSideEffects(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.abuse.controlBudgetFrames = 3
	c.abuse.mixedBudgetFrames = 5
	c.abuse.noopControlCount = 7
	c.sessionControl.peerGoAwayBidi = 80
	c.sessionControl.peerGoAwayUni = 99
	c.sessionControl.peerGoAwayErr = &ApplicationError{Code: uint64(CodeProtocol), Reason: "old"}
	c.mu.Unlock()

	if err := c.handleFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 8, 4, uint64(CodeInternal), "too-late"),
	}); err != nil {
		t.Fatalf("handleFrame(GOAWAY) while closing: %v", err)
	}

	c.mu.Lock()
	if c.sessionControl.peerGoAwayBidi != 80 || c.sessionControl.peerGoAwayUni != 99 {
		c.mu.Unlock()
		t.Fatalf("peer GOAWAY watermarks = (%d,%d), want (80,99)", c.sessionControl.peerGoAwayBidi, c.sessionControl.peerGoAwayUni)
	}
	if c.sessionControl.peerGoAwayErr == nil || c.sessionControl.peerGoAwayErr.Code != uint64(CodeProtocol) || c.sessionControl.peerGoAwayErr.Reason != "old" {
		c.mu.Unlock()
		t.Fatalf("peer GOAWAY error = %#v, want original payload", c.sessionControl.peerGoAwayErr)
	}
	if c.abuse.controlBudgetFrames != 3 || c.abuse.mixedBudgetFrames != 5 || c.abuse.noopControlCount != 7 {
		c.mu.Unlock()
		t.Fatalf("budgets changed after ignored GOAWAY = (%d,%d,%d), want (3,5,7)", c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestPeerNonCloseFramesIgnoredAfterTransportFailure(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(io.ErrClosedPipe)
	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeCLOSE {
		t.Fatalf("queued frame after transport failure close = %+v, want CLOSE", queued)
	}

	c.mu.Lock()
	c.abuse.controlBudgetFrames = 4
	c.abuse.mixedBudgetFrames = 6
	c.abuse.noopControlCount = 8
	c.abuse.noopDataCount = 10
	c.mu.Unlock()

	data := Frame{Type: FrameTypeDATA, StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), Payload: []byte("late")}
	if err := c.handleFrame(data); err != nil {
		t.Fatalf("handleFrame(DATA) after transport/session failure: %v", err)
	}
	if err := c.handleFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true),
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handleFrame(RESET) after transport/session failure: %v", err)
	}
	if err := c.handleFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: []byte{0xff},
	}); err != nil {
		t.Fatalf("handleFrame(GOAWAY) after transport/session failure: %v", err)
	}

	c.mu.Lock()
	if len(c.registry.streams) != 0 {
		c.mu.Unlock()
		t.Fatalf("streams changed after terminal ignore = %d, want 0", len(c.registry.streams))
	}
	if c.abuse.controlBudgetFrames != 4 || c.abuse.mixedBudgetFrames != 6 || c.abuse.noopControlCount != 8 || c.abuse.noopDataCount != 10 {
		c.mu.Unlock()
		t.Fatalf("budgets changed after terminal ignore = (%d,%d,%d,%d), want (4,6,8,10)", c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount, c.abuse.noopDataCount)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestDuplicatePeerCloseIgnoredBeforeParseAndBudgetAccounting(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "peer-close"),
	})
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("initial handleFrame(CLOSE) err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "peer-close" {
		t.Fatalf("initial peer close err = (%d,%q), want (%d,%q)", appErr.Code, appErr.Reason, uint64(CodeProtocol), "peer-close")
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	c.abuse.controlBudgetFrames = 7
	c.abuse.mixedBudgetFrames = 9
	c.abuse.noopControlCount = 11
	c.mu.Unlock()

	if err := c.handleFrame(Frame{Type: FrameTypeCLOSE, Payload: []byte{0xff}}); err != nil {
		t.Fatalf("duplicate malformed handleFrame(CLOSE) err = %v, want nil", err)
	}

	c.mu.Lock()
	if c.sessionControl.peerCloseErr == nil || c.sessionControl.peerCloseErr.Code != uint64(CodeProtocol) || c.sessionControl.peerCloseErr.Reason != "peer-close" {
		c.mu.Unlock()
		t.Fatalf("peerCloseErr = %#v, want preserved initial peer close", c.sessionControl.peerCloseErr)
	}
	if c.abuse.controlBudgetFrames != 7 || c.abuse.mixedBudgetFrames != 9 || c.abuse.noopControlCount != 11 {
		c.mu.Unlock()
		t.Fatalf("budgets changed after ignored duplicate CLOSE = (%d,%d,%d), want (7,9,11)", c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestDirectHandleCloseFrameIgnoresDuplicateMalformedPayload(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "peer-close"),
	})
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("initial handleCloseFrame err = %v, want ApplicationError", err)
	}
	assertNoQueuedFrame(t, frames)

	if err := c.handleCloseFrame(Frame{Type: FrameTypeCLOSE, Payload: []byte{0xff}}); err != nil {
		t.Fatalf("duplicate malformed handleCloseFrame err = %v, want nil", err)
	}
	assertNoQueuedFrame(t, frames)
}

func TestPeerCloseAfterTransportFailureStillParsesDiagnostics(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.closeSession(io.ErrClosedPipe)
	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeCLOSE {
		t.Fatalf("queued frame after transport failure = %+v, want CLOSE", queued)
	}

	c.mu.Lock()
	c.abuse.controlBudgetFrames = 4
	c.abuse.mixedBudgetFrames = 6
	c.abuse.noopControlCount = 8
	c.abuse.controlBudgetWindowStart = windowStampAt(time.Now())
	c.abuse.mixedBudgetWindowStart = c.abuse.controlBudgetWindowStart
	c.mu.Unlock()

	err := c.handleFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeFlowControl), "peer-diagnostics"),
	})
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("handleFrame(CLOSE) after transport failure err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeFlowControl) || appErr.Reason != "peer-diagnostics" {
		t.Fatalf("peer close err = (%d,%q), want (%d,%q)", appErr.Code, appErr.Reason, uint64(CodeFlowControl), "peer-diagnostics")
	}

	c.mu.Lock()
	if c.sessionControl.peerCloseErr == nil || c.sessionControl.peerCloseErr.Code != uint64(CodeFlowControl) || c.sessionControl.peerCloseErr.Reason != "peer-diagnostics" {
		c.mu.Unlock()
		t.Fatalf("peerCloseErr = %#v, want retained parsed diagnostics", c.sessionControl.peerCloseErr)
	}
	if c.abuse.controlBudgetFrames != 5 || c.abuse.mixedBudgetFrames != 7 || c.abuse.noopControlCount != 8 {
		c.mu.Unlock()
		t.Fatalf("budgets after parsed transport-failure CLOSE = (%d,%d,%d), want (5,7,8)", c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestDirectNonCloseHandlersIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.mu.Unlock()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte("late")}); err != nil {
		t.Fatalf("handleDataFrame while closing: %v", err)
	}
	if err := c.handleResetFrame(Frame{Type: FrameTypeRESET, StreamID: streamID, Payload: mustEncodeVarint(uint64(CodeCancelled))}); err != nil {
		t.Fatalf("handleResetFrame while closing: %v", err)
	}
	if err := c.handleStopSendingFrame(Frame{Type: FrameTypeStopSending, StreamID: streamID, Payload: mustEncodeVarint(uint64(CodeCancelled))}); err != nil {
		t.Fatalf("handleStopSendingFrame while closing: %v", err)
	}
	if err := c.handleAbortFrame(Frame{Type: FrameTypeABORT, StreamID: streamID, Payload: mustEncodeVarint(uint64(CodeCancelled))}); err != nil {
		t.Fatalf("handleAbortFrame while closing: %v", err)
	}
	if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, StreamID: streamID, Payload: mustEncodeVarint(64)}); err != nil {
		t.Fatalf("handleMaxDataFrame while closing: %v", err)
	}
	if err := c.handleBlockedFrame(Frame{Type: FrameTypeBLOCKED, StreamID: streamID, Payload: mustEncodeVarint(64)}); err != nil {
		t.Fatalf("handleBlockedFrame while closing: %v", err)
	}
	if err := c.handleGoAwayFrame(Frame{Type: FrameTypeGOAWAY, Payload: []byte{0xff}}); err != nil {
		t.Fatalf("handleGoAwayFrame while closing: %v", err)
	}

	c.mu.Lock()
	if len(c.registry.streams) != 0 {
		c.mu.Unlock()
		t.Fatalf("streams changed after direct ignored handlers = %d, want 0", len(c.registry.streams))
	}
	if c.sessionControl.peerGoAwayErr != nil {
		c.mu.Unlock()
		t.Fatalf("peer GOAWAY error changed after direct ignored handler = %#v, want nil", c.sessionControl.peerGoAwayErr)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestDirectTerminalDataHandlerIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.flow.recvSessionUsed = 13
	c.abuse.noopDataCount = 11
	c.mu.Unlock()

	if err := c.handleTerminalDataFrame(
		Frame{Type: FrameTypeDATA, StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), Payload: []byte("late")},
		terminalDataDisposition{action: lateDataIgnore},
	); err != nil {
		t.Fatalf("handleTerminalDataFrame while closing: %v", err)
	}

	c.mu.Lock()
	if c.flow.recvSessionUsed != 13 {
		c.mu.Unlock()
		t.Fatalf("recvSessionUsed = %d, want 13", c.flow.recvSessionUsed)
	}
	if c.abuse.noopDataCount != 11 {
		c.mu.Unlock()
		t.Fatalf("noopDataCount = %d, want 11", c.abuse.noopDataCount)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestPeerPingReplyDroppedWhenSessionClosesDuringReplyQueue(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest)},
		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})}, config: connConfigState{local: Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			peer:       Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, Capabilities: 0, LocalRole: localRole, PeerRole: peerRole, PeerSettings: settings}},
	}

	done := make(chan error, 1)
	go func() {
		done <- c.handleFrame(Frame{Type: FrameTypePING, Payload: []byte{0, 1, 2, 3, 4, 5, 6, 7}})
	}()

	deadline := time.Now().Add(testSignalTimeout)
	for {
		c.mu.Lock()
		blocked := c.flow.urgentQueuedBytes > 0
		c.mu.Unlock()
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for queued PONG reservation")
		}
		runtime.Gosched()
	}

	c.mu.Lock()
	c.lifecycle.closeErr = ErrSessionClosed
	c.lifecycle.sessionState = connStateClosing
	close(c.lifecycle.closedCh)
	c.mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleFrame(PING) err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for PING handler to return after benign close")
	}
}

func TestReadLoopProtocolQueueDropsBestEffortFramesWhenWorkerStalls(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	c := &Conn{
		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
		writer:  connWriterRuntimeState{urgentWriteCh: make(chan writeRequest)},
		lifecycle: connLifecycleState{
			sessionState: connStateReady,
			closedCh:     make(chan struct{}),
		},
		config: connConfigState{
			local: Preface{
				PrefaceVersion: PrefaceVersion,
				Role:           localRole,
				MinProto:       ProtoVersion,
				MaxProto:       ProtoVersion,
				Settings:       settings,
			},
			peer: Preface{
				PrefaceVersion: PrefaceVersion,
				Role:           peerRole,
				MinProto:       ProtoVersion,
				MaxProto:       ProtoVersion,
				Settings:       settings,
			},
			negotiated: Negotiated{
				Proto:        ProtoVersion,
				Capabilities: 0,
				LocalRole:    localRole,
				PeerRole:     peerRole,
				PeerSettings: settings,
			},
		},
	}

	pongFrame := flatTxFrame(Frame{Type: FrameTypePONG, Payload: []byte{0, 1, 2, 3, 4, 5, 6, 7}})
	abortFrame := flatTxFrame(Frame{Type: FrameTypeABORT, StreamID: 4, Payload: mustEncodeVarint(uint64(CodeStreamClosed))})
	firstDone := make(chan struct{}, 1)
	go func() {
		c.queueReadLoopFrameAsync(pongFrame)
		firstDone <- struct{}{}
	}()

	deadline := time.Now().Add(testSignalTimeout)
	for {
		c.mu.Lock()
		blocked := c.flow.urgentQueuedBytes > 0
		c.mu.Unlock()
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for stalled protocol worker")
		}
		runtime.Gosched()
	}

	select {
	case <-firstDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for first protocol task enqueue to return")
	}

	floodDone := make(chan struct{}, 1)
	go func() {
		for i := 0; i < 512; i++ {
			c.queueReadLoopFrameAsync(pongFrame)
			c.queueReadLoopFrameAsync(abortFrame)
		}
		floodDone <- struct{}{}
	}()

	select {
	case <-floodDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for protocol task flood to enqueue without blocking")
	}

	deadline = time.Now().Add(testSignalTimeout)
	for {
		c.mu.Lock()
		backlogBlocked := c.metrics.protocolBacklogBlocked
		closeErr := c.lifecycle.closeErr
		c.mu.Unlock()
		if backlogBlocked > 0 {
			if closeErr != nil {
				t.Fatalf("protocol backlog overflow closeErr = %v, want nil for droppable frames", closeErr)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for protocol backlog overflow to drop best-effort frames")
		}
		runtime.Gosched()
	}

	c.mu.Lock()
	select {
	case <-c.lifecycle.closedCh:
	default:
		c.lifecycle.closeErr = ErrSessionClosed
		c.lifecycle.sessionState = connStateClosing
		close(c.lifecycle.closedCh)
	}
	c.mu.Unlock()
}

func TestReadLoopProtocolQueueReleasesOversizedBackingAfterDrain(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
	}
	c.protocol.notifyCh = make(chan struct{}, 1)
	c.protocol.tasks = make([]protocolTask, 0, maxReusablePendingReadLoopProtocolJobsCap*2)
	for i := 0; i < maxReusablePendingReadLoopProtocolJobsCap*2; i++ {
		c.protocol.tasks = append(c.protocol.tasks, protocolTask{kind: protocolTaskCloseWrite})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.readLoopProtocolLoop(c.protocol.notifyCh, c.lifecycle.closedCh)
	}()

	notify(c.protocol.notifyCh)

	deadline := time.Now().Add(testSignalTimeout)
	for {
		c.mu.Lock()
		pendingJobs := len(c.protocol.tasks)
		taskCap := cap(c.protocol.tasks)
		c.mu.Unlock()
		if pendingJobs == 0 {
			if taskCap != maxPendingReadLoopProtocolJobs {
				t.Fatalf("protocol task backing cap after drain = %d, want %d", taskCap, maxPendingReadLoopProtocolJobs)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for oversized protocol task backlog to drain")
		}
		runtime.Gosched()
	}

	close(c.lifecycle.closedCh)
	select {
	case <-done:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for protocol loop shutdown")
	}
}

type nopCloseSignalStream struct{}

func (nopCloseSignalStream) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (nopCloseSignalStream) Write(p []byte) (int, error) { return len(p), nil }
func (nopCloseSignalStream) Close() error                { return nil }

func newStalledCloseSignalConn(t *testing.T) (*Conn, <-chan struct{}, chan<- struct{}, <-chan struct{}) {
	t.Helper()

	settings := DefaultSettings()
	sent := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan struct{})

	c := &Conn{
		io: connIOState{conn: nopCloseSignalStream{}},

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals:   connRuntimeSignalState{acceptCh: make(chan struct{}, 1), livenessCh: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	go func() {
		defer close(done)
		req := <-c.writer.urgentWriteCh
		sent <- struct{}{}
		<-release
		req.done <- nil
	}()

	return c, sent, release, done
}

func newBlockedWriteCloseWakeStream(c *Conn, streamID uint64) *nativeStream {
	stream := testOpenedBidiStream(c, streamID, testWithSendMax(1))
	stream.sendSent = 1
	return stream
}

func TestCloseSessionSignalsControlNotifyBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case <-c.pending.controlNotify:
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before prompt control notification was observed")
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("closeSession did not signal controlNotify before transport close")
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestCloseSessionDoesNotWaitForeverForBufferedCloseDone(t *testing.T) {
	settings := DefaultSettings()
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals:   connRuntimeSignalState{acceptCh: make(chan struct{}, 1), livenessCh: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 1)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	done := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession waited indefinitely for buffered CLOSE req.done")
	}

	select {
	case <-c.lifecycle.closedCh:
	default:
		t.Fatal("closedCh not closed after bounded buffered CLOSE wait")
	}
	if got := len(c.writer.urgentWriteCh); got != 0 {
		t.Fatalf("urgent close queue depth = %d, want 0 after session close drain", got)
	}
}

func TestBlockedUrgentQueueReturnsSessionErrorBeforeClosedChOnCloseSession(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)
	c.flow.urgentQueueCap = 1
	c.flow.urgentQueuedBytes = 1

	errCh := make(chan error, 1)
	go func() {
		errCh <- testQueueFrame(c, Frame{Type: FrameTypePING, Payload: make([]byte, 8)})
	}()

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-errCh:
		if !IsErrorCode(err, CodeProtocol) {
			t.Fatalf("blocked urgent queue err = %v, want %s", err, CodeProtocol)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("blocked urgent queue did not wake before closedCh")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before blocked urgent queue returned")
	default:
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestCloseSessionBroadcastsUrgentWakeBeforeClosedCh(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	c.mu.Lock()
	urgentWake := c.currentUrgentWakeLocked()
	c.mu.Unlock()

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case <-urgentWake:
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before urgent wake broadcast")
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("closeSession did not broadcast urgent wake before transport close")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before stalled close emission was released")
	default:
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestWaitReturnsSessionErrorAfterClosedChOnCloseSession(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- c.Wait(context.Background())
	}()

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-waitDone:
		if !IsErrorCode(err, CodeInternal) {
			t.Fatalf("Wait err = %v, want %s", err, CodeInternal)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Wait did not return on closeSession")
	}

	select {
	case <-c.lifecycle.closedCh:
	default:
		t.Fatal("closedCh still open when Wait returned on closeSession")
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestAbortReturnsWaitAfterClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- c.Wait(context.Background())
	}()

	abortDone := make(chan struct{})
	go func() {
		c.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(abortDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-waitDone:
		if !IsErrorCode(err, CodeProtocol) {
			t.Fatalf("Wait err = %v, want %s", err, CodeProtocol)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Wait did not return on Abort")
	}

	select {
	case <-c.lifecycle.closedCh:
	default:
		t.Fatal("closedCh still open when Wait returned on Abort")
	}

	close(release)

	select {
	case <-abortDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("Abort did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestAbortReturnsAcceptBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	acceptDone := make(chan error, 1)
	go func() {
		_, err := c.AcceptStream(context.Background())
		acceptDone <- err
	}()
	awaitAcceptWaiter(t, c, testSignalTimeout, "AcceptStream did not enter wait state before Abort")

	abortDone := make(chan struct{})
	go func() {
		c.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(abortDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-acceptDone:
		if !IsErrorCode(err, CodeProtocol) {
			t.Fatalf("AcceptStream err = %v, want %s", err, CodeProtocol)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("AcceptStream did not return before closedCh on Abort")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before AcceptStream returned")
	default:
	}

	close(release)

	select {
	case <-abortDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("Abort did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestAbortReturnsAcceptUniBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	acceptDone := make(chan error, 1)
	go func() {
		_, err := c.AcceptUniStream(context.Background())
		acceptDone <- err
	}()
	awaitAcceptWaiter(t, c, testSignalTimeout, "AcceptUniStream did not enter wait state before Abort")

	abortDone := make(chan struct{})
	go func() {
		c.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(abortDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-acceptDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("AcceptUniStream err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationAccept || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("AcceptUniStream did not return before closedCh on Abort")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before AcceptUniStream returned")
	default:
	}

	close(release)

	select {
	case <-abortDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("Abort did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestAbortReturnsBlockedReadBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	stream := testOpenedBidiStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	readDone := make(chan error, 1)
	go func() {
		_, err := stream.Read(make([]byte, 1))
		readDone <- err
	}()
	awaitStreamReadWaiter(t, stream, testSignalTimeout, "blocked Read did not enter wait state before Abort")

	abortDone := make(chan struct{})
	go func() {
		c.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(abortDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-readDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("Read err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationRead || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("Read did not return before closedCh on Abort")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before Read returned")
	default:
	}

	close(release)

	select {
	case <-abortDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("Abort did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestAbortReturnsBlockedWriteBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)
	stream := newBlockedWriteCloseWakeStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.flow.sendSessionMax = 1
	c.flow.sendSessionUsed = 1
	c.mu.Unlock()

	writeDone := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		writeDone <- err
	}()

	abortDone := make(chan struct{})
	go func() {
		c.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(abortDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-writeDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("Write err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("Write did not return before closedCh on Abort")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before Write returned")
	default:
	}

	close(release)

	select {
	case <-abortDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("Abort did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestAbortNilReturnsBlockedWriteBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)
	stream := newBlockedWriteCloseWakeStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.flow.sendSessionMax = 1
	c.flow.sendSessionUsed = 1
	c.mu.Unlock()

	writeDone := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		writeDone <- err
	}()

	abortDone := make(chan struct{})
	go func() {
		c.CloseWithError(nil)
		close(abortDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-writeDone:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("Write err = %v, want %v", err, ErrSessionClosed)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("Write did not return before closedCh on CloseWithError(nil)")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before Write returned")
	default:
	}

	close(release)

	select {
	case <-abortDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseWithError(nil) did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestAbortReturnsBlockedWriteFinalBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)
	stream := newBlockedWriteCloseWakeStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.flow.sendSessionMax = 1
	c.flow.sendSessionUsed = 1
	c.mu.Unlock()

	writeDone := make(chan error, 1)
	go func() {
		_, err := stream.WriteFinal([]byte("x"))
		writeDone <- err
	}()

	abortDone := make(chan struct{})
	go func() {
		c.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(abortDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-writeDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("WriteFinal err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("WriteFinal did not return before closedCh on Abort")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before WriteFinal returned")
	default:
	}

	close(release)

	select {
	case <-abortDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("Abort did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestAbortReturnsProvisionalCommitWaiterBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	c.mu.Lock()
	_ = c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	blocked := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	writeDone := make(chan error, 1)
	go func() {
		_, err := blocked.Write([]byte("x"))
		writeDone <- err
	}()

	abortDone := make(chan struct{})
	go func() {
		c.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(abortDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-writeDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("blocked provisional Write err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("blocked provisional Write did not return before closedCh on Abort")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before provisional Write returned")
	default:
	}

	close(release)

	select {
	case <-abortDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("Abort did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestCloseSessionReturnsBlockedReadBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	stream := testOpenedBidiStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	readDone := make(chan error, 1)
	go func() {
		_, err := stream.Read(make([]byte, 1))
		readDone <- err
	}()
	awaitStreamReadWaiter(t, stream, testSignalTimeout, "blocked Read did not enter wait state before closeSession")

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-readDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("Read err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationRead || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("Read did not return before closedCh on closeSession")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before Read returned")
	default:
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestCloseSessionReturnsBlockedWriteBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	c, sent, release, handlerDone := newStalledCloseSignalConn(t)
	stream := newBlockedWriteCloseWakeStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.flow.sendSessionMax = 1
	c.flow.sendSessionUsed = 1
	c.mu.Unlock()

	writeDone := make(chan error, 1)
	writeStarted := make(chan struct{}, 1)
	go func() {
		writeStarted <- struct{}{}
		_, err := stream.Write([]byte("x"))
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked Write did not start before closeSession")
	}

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-writeDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("Write err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("Write did not return before closedCh on closeSession")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before Write returned")
	default:
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestCloseSessionReturnsBlockedWriteFinalBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	c, sent, release, handlerDone := newStalledCloseSignalConn(t)
	stream := newBlockedWriteCloseWakeStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.flow.sendSessionMax = 1
	c.flow.sendSessionUsed = 1
	c.mu.Unlock()

	writeDone := make(chan error, 1)
	writeStarted := make(chan struct{}, 1)
	go func() {
		writeStarted <- struct{}{}
		_, err := stream.WriteFinal([]byte("x"))
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked WriteFinal did not start before closeSession")
	}

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-writeDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("WriteFinal err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("WriteFinal did not return before closedCh on closeSession")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before WriteFinal returned")
	default:
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestCloseSessionReturnsProvisionalCommitWaiterBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	c.mu.Lock()
	_ = c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	blocked := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	writeDone := make(chan error, 1)
	writeStarted := make(chan struct{}, 1)
	go func() {
		writeStarted <- struct{}{}
		_, err := blocked.Write([]byte("x"))
		writeDone <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked provisional Write did not start before closeSession")
	}

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-writeDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("blocked provisional Write err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("blocked provisional Write did not return before closedCh on closeSession")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before provisional Write returned")
	default:
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestCloseSessionReturnsAcceptBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	t.Parallel()

	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	acceptDone := make(chan error, 1)
	go func() {
		_, err := c.AcceptStream(context.Background())
		acceptDone <- err
	}()
	awaitAcceptWaiter(t, c, testSignalTimeout, "AcceptStream did not enter wait state before closeSession")

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-acceptDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("AcceptStream err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationAccept || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("AcceptStream did not return before closedCh on closeSession")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before AcceptStream returned")
	default:
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestCloseSessionReturnsAcceptUniBeforeClosedChWhenCloseFrameSendStalls(t *testing.T) {
	c, sent, release, handlerDone := newStalledCloseSignalConn(t)

	acceptDone := make(chan error, 1)
	go func() {
		_, err := c.AcceptUniStream(context.Background())
		acceptDone <- err
	}()
	awaitAcceptWaiter(t, c, testSignalTimeout, "AcceptUniStream did not enter wait state before closeSession")

	closeDone := make(chan struct{})
	go func() {
		c.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		close(closeDone)
	}()

	select {
	case <-sent:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for stalled CLOSE emission")
	}

	select {
	case err := <-acceptDone:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
			t.Fatalf("AcceptUniStream err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
		}
		se := requireStructuredError(t, err)
		if se.Scope != ScopeSession || se.Operation != OperationAccept || se.Source != SourceLocal ||
			se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
			t.Fatalf("structured error = %+v", *se)
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("AcceptUniStream did not return before closedCh on closeSession")
	}

	select {
	case <-c.lifecycle.closedCh:
		t.Fatal("closedCh closed before AcceptUniStream returned")
	default:
	}

	close(release)

	select {
	case <-closeDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("closeSession did not finish after releasing stalled close emission")
	}

	select {
	case <-handlerDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("stalled close handler did not finish")
	}
}

func TestReleaseRejectedPreparedRequestsBroadcastsUrgentWake(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()

	c.mu.Lock()
	urgentWake := c.currentUrgentWakeLocked()
	c.flow.urgentQueuedBytes = 9
	c.mu.Unlock()

	c.releaseRejectedPreparedRequests([]rejectedWriteRequest{
		{
			req: writeRequest{
				urgentReserved: true,
				queuedBytes:    9,
			},
		},
	})

	select {
	case <-urgentWake:
	case <-time.After(testSignalTimeout):
		t.Fatal("releaseRejectedPreparedRequests did not broadcast urgent wake")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.flow.urgentQueuedBytes; got != 0 {
		t.Fatalf("urgentQueuedBytes = %d, want 0 after rejected urgent release", got)
	}
}

func newSessionMemoryTestConn() *Conn {
	settings := DefaultSettings()
	return &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 1), advisoryWriteCh: make(chan writeRequest, 1), writeCh: make(chan writeRequest, 1)}, config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
}

func TestStatsSnapshotIncludesQueueDepths(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), sessionState: connStateReady},
		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest, 4), advisoryWriteCh: make(chan writeRequest, 4), urgentWriteCh: make(chan writeRequest, 4)},
	}
	c.writer.writeCh <- writeRequest{}
	c.writer.advisoryWriteCh <- writeRequest{}
	c.writer.urgentWriteCh <- writeRequest{}

	stats := c.Stats()
	if stats.Queues.Ordinary != 1 || stats.Queues.Advisory != 1 || stats.Queues.Urgent != 1 || stats.Queues.Total != 3 {
		t.Fatalf("queue stats = %+v, want ordinary=1 advisory=1 urgent=1 total=3", stats.Queues)
	}
}

func TestStatsSnapshotIncludesActiveStreams(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), sessionState: connStateReady},
	}
	c.registry.activeLocalBidi = 1
	c.registry.activeLocalUni = 2
	c.registry.activePeerBidi = 3
	c.registry.activePeerUni = 4

	stats := c.Stats().ActiveStreams
	want := ActiveStreamStats{
		LocalBidi: 1,
		LocalUni:  2,
		PeerBidi:  3,
		PeerUni:   4,
		Total:     10,
	}
	if stats != want {
		t.Fatalf("active stream stats = %+v, want %+v", stats, want)
	}
}

func TestActiveStreamStatsTotalSaturates(t *testing.T) {
	t.Parallel()

	stats := makeActiveStreamStats(^uint64(0), 1, 0, 0)
	if stats.Total != ^uint64(0) {
		t.Fatalf("active stream total = %d, want saturated %d", stats.Total, ^uint64(0))
	}
}

func TestStatsTrackFlushAndReceiveBacklog(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}

	awaitConnState(t, client, testSignalTimeout, func(c *Conn) bool {
		stats := c.Stats()
		return stats.Flush.Count > 0 && stats.Flush.LastFrames > 0 && stats.Flush.LastBytes > 0
	}, "flush stats were not recorded")

	awaitConnState(t, server, testSignalTimeout, func(c *Conn) bool {
		return c.Stats().Pressure.ReceiveBacklog > 0
	}, "receive backlog was not observed")
}

func TestNoteFlushTracksSendRateEstimateFromMeaningfulWrites(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	now := time.Now()

	c.noteFlushAndRateLocked(4, 8192, now, 2*time.Second)
	if got := c.metrics.sendRateEstimate; got != 4096 {
		t.Fatalf("send rate estimate after first sample = %d, want 4096", got)
	}

	c.noteFlushAndRateLocked(4, 8192, now.Add(time.Second), time.Second)
	if got := c.metrics.sendRateEstimate; got != 6144 {
		t.Fatalf("send rate estimate after ewma update = %d, want 6144", got)
	}
}

func TestNoteFlushIgnoresTinyFastSamplesForSendRateEstimate(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.noteFlushAndRateLocked(1, 128, time.Now(), 2*time.Millisecond)
	if got := c.metrics.sendRateEstimate; got != 0 {
		t.Fatalf("send rate estimate after tiny fast sample = %d, want 0", got)
	}
}

func TestNoteFlushSendRateEstimateSaturatesWithoutOverflow(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	maxInt := int(^uint(0) >> 1)
	want := rt.SaturatingMulDivFloor(uint64(maxInt), uint64(time.Second), uint64(time.Nanosecond))

	c.noteSendRateEstimateLocked(maxInt, time.Nanosecond)
	if got := c.metrics.sendRateEstimate; got != want {
		t.Fatalf("send rate estimate = %d, want %d", got, want)
	}

	c.noteSendRateEstimateLocked(maxInt, time.Nanosecond)
	if got := c.metrics.sendRateEstimate; got != want {
		t.Fatalf("send rate average = %d, want %d", got, want)
	}
}

func TestStatsTrackReasonCountersAndHiddenReap(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	if err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: stream.id,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle RESET: %v", err)
	}

	hiddenID := c.registry.nextPeerBidi
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: hiddenID,
		Payload:  mustEncodeVarint(uint64(CodeInternal)),
	}); err != nil {
		t.Fatalf("handle ABORT: %v", err)
	}

	stats := c.Stats()
	if got := stats.Reasons.Reset[uint64(CodeCancelled)]; got != 1 {
		t.Fatalf("reset reason count = %d, want 1", got)
	}
	if got := stats.Reasons.Abort[uint64(CodeInternal)]; got != 1 {
		t.Fatalf("abort reason count = %d, want 1", got)
	}
	if stats.Hidden.Reaped == 0 {
		t.Fatalf("hidden reaped = %d, want > 0", stats.Hidden.Reaped)
	}
}

func TestStatsReasonCountersBoundDistinctCodes(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	for i := uint64(0); i < maxReasonStatsCodes+5; i++ {
		c.noteResetReasonLocked(10_000 + i)
		c.noteAbortReasonLocked(20_000 + i)
	}
	c.noteResetReasonLocked(10_000)
	c.noteAbortReasonLocked(20_000)
	c.mu.Unlock()

	stats := c.Stats()
	if got := len(stats.Reasons.Reset); got != maxReasonStatsCodes {
		t.Fatalf("reset reason map size = %d, want %d", got, maxReasonStatsCodes)
	}
	if got := len(stats.Reasons.Abort); got != maxReasonStatsCodes {
		t.Fatalf("abort reason map size = %d, want %d", got, maxReasonStatsCodes)
	}
	if got := stats.Reasons.ResetOverflow; got != 5 {
		t.Fatalf("reset reason overflow = %d, want 5", got)
	}
	if got := stats.Reasons.AbortOverflow; got != 5 {
		t.Fatalf("abort reason overflow = %d, want 5", got)
	}
	if got := stats.Reasons.Reset[10_000]; got != 2 {
		t.Fatalf("reset reason count = %d, want 2", got)
	}
	if got := stats.Reasons.Abort[20_000]; got != 2 {
		t.Fatalf("abort reason count = %d, want 2", got)
	}
}

func TestStatsTrackBlockedWriteAndOpenLatency(t *testing.T) {
	t.Parallel()

	serverCfg := DefaultConfig()
	serverCfg.Settings.InitialMaxStreamDataBidiPeerOpened = 0

	client, _ := newConnPairWithConfig(t, nil, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	streamImpl := requireNativeStreamImpl(t, stream)

	client.mu.Lock()
	streamImpl.setProvisionalCreated(time.Now().Add(-5 * time.Millisecond))
	client.mu.Unlock()

	if err := stream.SetWriteDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("set write deadline: %v", err)
	}
	_, err = stream.Write([]byte("x"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write err = %v, want deadline exceeded", err)
	}

	awaitConnState(t, client, 2*testSignalTimeout, func(c *Conn) bool {
		stats := c.Stats()
		return stats.BlockedWriteTotal > 0 && stats.LastOpenLatency > 0
	}, "blocked write/open latency stats were not recorded")
}

func TestStatsTrackAdmissionPressureAndVisibleBacklogCounters(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.queues.acceptBacklogLimit = 1
	c.queues.acceptBacklogBytesLimit = 3

	hiddenID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: hiddenID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle hidden ABORT: %v", err)
	}

	firstVisibleID := hiddenID + 4
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: firstVisibleID,
		Payload:  []byte("ab"),
	}); err != nil {
		t.Fatalf("handle first visible DATA: %v", err)
	}
	secondVisibleID := firstVisibleID + 4
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: secondVisibleID,
		Payload:  []byte("cd"),
	}); err != nil {
		t.Fatalf("handle second visible DATA: %v", err)
	}

	stats := c.Stats()
	if stats.Hidden.Retained != 1 {
		t.Fatalf("hidden retained = %d, want 1", stats.Hidden.Retained)
	}
	if stats.Hidden.SoftCap != 16 || stats.Hidden.HardCap != 32 {
		t.Fatalf("hidden caps = (%d,%d), want (16,32)", stats.Hidden.SoftCap, stats.Hidden.HardCap)
	}
	if stats.AcceptBacklog.Count != 1 || stats.AcceptBacklog.Bytes != 2 {
		t.Fatalf("accept backlog = %+v, want count=1 bytes=2", stats.AcceptBacklog)
	}
	if stats.AcceptBacklog.CountLimit != 1 || stats.AcceptBacklog.BytesLimit != 3 {
		t.Fatalf("accept backlog limits = %+v, want count=1 bytes=3", stats.AcceptBacklog)
	}
	if !stats.AcceptBacklog.AtCountCap {
		t.Fatal("accept backlog AtCountCap = false, want true")
	}
	if stats.AcceptBacklog.Refused != 1 {
		t.Fatalf("accept backlog refused = %d, want 1", stats.AcceptBacklog.Refused)
	}
}

func TestStatsTrackProvisionalCapsAndCounters(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	client.mu.Lock()
	client.queues.acceptBacklogLimit = 8
	client.mu.Unlock()

	for i := 0; i < 32; i++ {
		if _, err := client.OpenStream(ctx); err != nil {
			t.Fatalf("open provisional stream %d: %v", i, err)
		}
	}

	stats := client.Stats()
	if stats.Provisionals.Bidi != 32 || stats.Provisionals.Uni != 0 {
		t.Fatalf("provisionals = %+v, want bidi=32 uni=0", stats.Provisionals)
	}
	if stats.Provisionals.SoftCap != 16 || stats.Provisionals.HardCap != 32 {
		t.Fatalf("provisional caps = (%d,%d), want (16,32)", stats.Provisionals.SoftCap, stats.Provisionals.HardCap)
	}
	if !stats.Provisionals.BidiAtSoft || !stats.Provisionals.BidiAtHard {
		t.Fatalf("provisional bidi cap flags = %+v, want soft+hard true", stats.Provisionals)
	}

	if _, err := client.OpenStream(ctx); !errors.Is(err, ErrOpenLimited) {
		t.Fatalf("overflow open err = %v, want %v", err, ErrOpenLimited)
	}
	if got := client.Stats().Provisionals.Limited; got != 1 {
		t.Fatalf("provisional limited = %d, want 1", got)
	}

	client.mu.Lock()
	client.queues.provisionalBidi.items[0].setProvisionalCreated(time.Now().Add(-provisionalOpenMaxAge - time.Second))
	client.mu.Unlock()

	if _, err := client.OpenStream(ctx); err != nil {
		t.Fatalf("open after provisional expiry: %v", err)
	}
	stats = client.Stats()
	if stats.Provisionals.Expired == 0 {
		t.Fatalf("provisional expired = %d, want > 0", stats.Provisionals.Expired)
	}
	if stats.Provisionals.Bidi != 32 {
		t.Fatalf("provisional bidi count after expiry/reopen = %d, want 32", stats.Provisionals.Bidi)
	}
}

func TestStatsTrackSessionMemoryPressure(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), sessionState: connStateReady},

		liveness: connLivenessState{
			pingPayload: []byte("ping1234"),
		},

		config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}}, flow: connFlowState{recvSessionUsed: 512,
			queuedDataBytes:     256,
			advisoryQueuedBytes: 96,
			urgentQueuedBytes:   128,

			sessionMemoryCap: 1024}, queues: connQueueState{provisionalBidi: streamSparseQueueState{items: []*nativeStream{{}}}}, registry: connRegistryState{tombstones: map[uint64]streamTombstone{
			1: {Hidden: true},
		},
			tombstoneOrder: []uint64{1}}, pending: connPendingControlState{
			controlBytes:          64,
			priorityBytes:         80,
			preparedPriorityBytes: 48,
		}, retention: connRetentionState{retainedOpenInfoBytes: 24,
			retainedPeerReasonBytes: 32},
	}

	stats := c.Stats()
	retainedState := saturatingMul(2, c.retainedStateUnitLocked())
	if got := stats.Pressure.RetainedState; got != retainedState {
		t.Fatalf("retained state = %d, want %d", got, retainedState)
	}
	if got := stats.Pressure.RetainedBuckets.HiddenControl.Count; got != 1 {
		t.Fatalf("hidden retained count = %d, want 1", got)
	}
	if got := stats.Pressure.RetainedBuckets.HiddenControl.Bytes; got != c.retainedStateUnitLocked() {
		t.Fatalf("hidden retained bytes = %d, want %d", got, c.retainedStateUnitLocked())
	}
	if got := stats.Pressure.RetainedBuckets.Provisionals.Count; got != 1 {
		t.Fatalf("provisional retained count = %d, want 1", got)
	}
	if got := stats.Pressure.RetainedBuckets.Provisionals.Bytes; got != c.retainedStateUnitLocked() {
		t.Fatalf("provisional retained bytes = %d, want %d", got, c.retainedStateUnitLocked())
	}
	if got := stats.Pressure.RetainedBuckets.AcceptBacklog.Count; got != 0 {
		t.Fatalf("accept backlog retained count = %d, want 0", got)
	}
	if got := stats.Pressure.RetainedBuckets.VisibleTombstone.Count; got != 0 {
		t.Fatalf("visible tombstone retained count = %d, want 0", got)
	}
	if got := stats.Pressure.RetainedBuckets.MarkerOnly.Count; got != 0 {
		t.Fatalf("marker-only retained count = %d, want 0", got)
	}
	if got := stats.Pressure.RetainedOpenInfo; got != 24 {
		t.Fatalf("retained open_info = %d, want 24", got)
	}
	if got := stats.Pressure.RetainedPeerReasons; got != 32 {
		t.Fatalf("retained peer reasons = %d, want 32", got)
	}
	if got := stats.Pressure.OutstandingPingBytes; got != 8 {
		t.Fatalf("outstanding ping bytes = %d, want 8", got)
	}
	if got := stats.Pressure.TrackedBuffered; got != 1184+24+32+8+retainedState {
		t.Fatalf("tracked buffered = %d, want %d", got, 1184+24+32+8+retainedState)
	}
	if got := stats.Pressure.TrackedBufferedLimit; got != 1024 {
		t.Fatalf("tracked buffered limit = %d, want 1024", got)
	}
	if !stats.Pressure.TrackedBufferedHigh {
		t.Fatal("tracked buffered high = false, want true")
	}
	if !stats.Pressure.TrackedBufferedAtCap {
		t.Fatal("tracked buffered at cap = false, want true")
	}
	if stats.Pressure.OrdinaryQueued != 256 || stats.Pressure.AdvisoryQueued != 96 || stats.Pressure.UrgentQueued != 128 {
		t.Fatalf("queued pressure = %+v, want ordinary=256 advisory=96 urgent=128", stats.Pressure)
	}
	if stats.Pressure.PendingControl != 64 || stats.Pressure.PendingAdvisory != 80 || stats.Pressure.PreparedAdvisory != 48 {
		t.Fatalf("pending pressure = %+v, want control=64 pending_advisory=80 prepared_advisory=48", stats.Pressure)
	}
}

func TestStatsExposeRetainedStateBreakdown(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()
	unit := c.retainedStateUnitLocked()
	compactUnit := c.compactTerminalStateUnitLocked()

	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		4: {Hidden: true},
		8: {},
	}
	c.registry.tombstoneOrder = []uint64{4, 8}
	c.registry.usedStreamData = map[uint64]usedStreamMarker{
		4:  {action: lateDataAbortClosed},
		8:  {action: lateDataAbortClosed},
		12: {action: lateDataAbortClosed},
	}
	c.queues.acceptBidi.items = []*nativeStream{{enqueued: true}}
	c.queues.acceptBidi.count = 1
	c.queues.provisionalBidi.items = []*nativeStream{{}}
	c.queues.provisionalBidi.count = 1
	c.queues.provisionalBidi.init = true
	c.mu.Unlock()

	stats := c.Stats()
	if got := stats.Pressure.RetainedBuckets.HiddenControl; got != (RetainedBucketStats{Count: 1, Bytes: unit}) {
		t.Fatalf("hidden bucket = %+v, want count=1 bytes=%d", got, unit)
	}
	if got := stats.Pressure.RetainedBuckets.AcceptBacklog; got != (RetainedBucketStats{Count: 1, Bytes: unit}) {
		t.Fatalf("accept backlog bucket = %+v, want count=1 bytes=%d", got, unit)
	}
	if got := stats.Pressure.RetainedBuckets.Provisionals; got != (RetainedBucketStats{Count: 1, Bytes: unit}) {
		t.Fatalf("provisional bucket = %+v, want count=1 bytes=%d", got, unit)
	}
	if got := stats.Pressure.RetainedBuckets.VisibleTombstone; got != (RetainedBucketStats{Count: 1, Bytes: compactUnit}) {
		t.Fatalf("visible tombstone bucket = %+v, want count=1 bytes=%d", got, compactUnit)
	}
	if got := stats.Pressure.RetainedBuckets.MarkerOnly; got != (RetainedBucketStats{Count: 1, Bytes: compactUnit}) {
		t.Fatalf("marker-only bucket = %+v, want count=1 bytes=%d", got, compactUnit)
	}

	wantTotal := saturatingAdd(saturatingMul(3, unit), saturatingMul(2, compactUnit))
	if got := stats.Pressure.RetainedState; got != wantTotal {
		t.Fatalf("retained state total = %d, want %d", got, wantTotal)
	}
}

func TestUnseenStreamMaxDataClosesSession(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	if err := testQueueFrame(client, Frame{
		Type:     FrameTypeMAXDATA,
		StreamID: 4,
		Payload:  mustEncodeVarint(64),
	}); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, ErrSessionClosed) && !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("queue MAX_DATA: %v", err)
	}

	if err := server.Wait(ctx); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("server wait err = %v, want %s", err, CodeProtocol)
	}
}

func TestUnseenStreamBlockedClosesSession(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	if err := testQueueFrame(client, Frame{
		Type:     FrameTypeBLOCKED,
		StreamID: 4,
		Payload:  mustEncodeVarint(64),
	}); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, ErrSessionClosed) && !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("queue BLOCKED: %v", err)
	}

	if err := server.Wait(ctx); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("server wait err = %v, want %s", err, CodeProtocol)
	}
}

func TestUnseenStreamStopSendingClosesSession(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	if err := testQueueFrame(client, Frame{
		Type:     FrameTypeStopSending,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, ErrSessionClosed) && !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("queue STOP_SENDING: %v", err)
	}

	if err := server.Wait(ctx); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("server wait err = %v, want %s", err, CodeProtocol)
	}
}

func TestUnseenStreamResetClosesSession(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	if err := testQueueFrame(client, Frame{
		Type:     FrameTypeRESET,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, ErrSessionClosed) && !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("queue RESET: %v", err)
	}

	if err := server.Wait(ctx); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("server wait err = %v, want %s", err, CodeProtocol)
	}
}

func TestPeerGoAwayWideningIsProtocolError(t *testing.T) {
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.sessionControl.peerGoAwayBidi = 100
	c.sessionControl.peerGoAwayUni = 99

	if err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 80, 99, uint64(CodeNoError), ""),
	}); err != nil {
		t.Fatalf("first non-increasing GOAWAY: %v", err)
	}
	if c.sessionControl.peerGoAwayBidi != 80 || c.sessionControl.peerGoAwayUni != 99 {
		t.Fatalf("stored watermarks = (%d, %d), want (80, 99)", c.sessionControl.peerGoAwayBidi, c.sessionControl.peerGoAwayUni)
	}

	err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 80, 103, uint64(CodeNoError), ""),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("widening GOAWAY err = %v, want %s", err, CodeProtocol)
	}
}

func TestPeerGoAwayBidiRejectsLocalOrWrongDirectionStreamID(t *testing.T) {
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), 0, uint64(CodeNoError), ""),
	})
	if err != nil {
		t.Fatalf("local-owned bidi GOAWAY err = %v, want nil", err)
	}
	expectedBidiAfterFirst := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	expectedUniAfterFirst := uint64(0)
	bidiAfterFirst := c.sessionControl.peerGoAwayBidi
	uniAfterFirst := c.sessionControl.peerGoAwayUni
	if bidiAfterFirst != expectedBidiAfterFirst || uniAfterFirst != expectedUniAfterFirst {
		t.Fatalf("first local GOAWAY stored watermarks = (%d, %d), want (%d, %d)", bidiAfterFirst, uniAfterFirst, expectedBidiAfterFirst, expectedUniAfterFirst)
	}

	err = c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), 0, uint64(CodeNoError), ""),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("wrong-direction bidi GOAWAY err = %v, want %s", err, CodeProtocol)
	}
	if c.sessionControl.peerGoAwayBidi != bidiAfterFirst || c.sessionControl.peerGoAwayUni != uniAfterFirst {
		t.Fatalf("stored watermarks = (%d, %d), want (%d, %d)", c.sessionControl.peerGoAwayBidi, c.sessionControl.peerGoAwayUni, bidiAfterFirst, uniAfterFirst)
	}
}

func TestPeerGoAwayUniRejectsLocalOrWrongDirectionStreamID(t *testing.T) {
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 0, state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), uint64(CodeNoError), ""),
	})
	if err != nil {
		t.Fatalf("local-owned uni GOAWAY err = %v, want nil", err)
	}
	expectedBidiAfterFirst := uint64(0)
	expectedUniAfterFirst := state.FirstLocalStreamID(c.config.negotiated.LocalRole, false)
	bidiAfterFirst := c.sessionControl.peerGoAwayBidi
	uniAfterFirst := c.sessionControl.peerGoAwayUni
	if bidiAfterFirst != expectedBidiAfterFirst || uniAfterFirst != expectedUniAfterFirst {
		t.Fatalf("first local GOAWAY stored watermarks = (%d, %d), want (%d, %d)", bidiAfterFirst, uniAfterFirst, expectedBidiAfterFirst, expectedUniAfterFirst)
	}

	err = c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 0, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), uint64(CodeNoError), ""),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("wrong-direction uni GOAWAY err = %v, want %s", err, CodeProtocol)
	}
	if c.sessionControl.peerGoAwayBidi != bidiAfterFirst || c.sessionControl.peerGoAwayUni != uniAfterFirst {
		t.Fatalf("stored watermarks = (%d, %d), want (%d, %d)", c.sessionControl.peerGoAwayBidi, c.sessionControl.peerGoAwayUni, bidiAfterFirst, uniAfterFirst)
	}
}

func TestLocalGoAwayRejectsInvalidBidiOrUniWatermark(t *testing.T) {
	for _, localRole := range []Role{RoleResponder, RoleInitiator} {
		func() {
			c, _, stop := newInvalidPolicyConn(t)
			defer stop()

			peerRole := RoleResponder
			if localRole == RoleResponder {
				peerRole = RoleInitiator
			}

			c.config.local.Role = localRole
			c.config.peer.Role = peerRole
			c.config.negotiated.LocalRole = localRole
			c.config.negotiated.PeerRole = peerRole

			startBidi := c.sessionControl.localGoAwayBidi
			startUni := c.sessionControl.localGoAwayUni

			err := c.GoAway(state.FirstLocalStreamID(localRole, true), 0)
			if !IsErrorCode(err, CodeProtocol) {
				t.Fatalf("role %s local-owned bidi GOAWAY err = %v, want %s", localRole, err, CodeProtocol)
			}
			if c.sessionControl.localGoAwayBidi != startBidi || c.sessionControl.localGoAwayUni != startUni {
				t.Fatalf("role %s watermark changed after invalid bidi GOAWAY: (%d, %d), want (%d, %d)", localRole, c.sessionControl.localGoAwayBidi, c.sessionControl.localGoAwayUni, startBidi, startUni)
			}

			err = c.GoAway(state.FirstPeerStreamID(localRole, false), 0)
			if !IsErrorCode(err, CodeProtocol) {
				t.Fatalf("role %s wrong-direction bidi GOAWAY err = %v, want %s", localRole, err, CodeProtocol)
			}
			if c.sessionControl.localGoAwayBidi != startBidi || c.sessionControl.localGoAwayUni != startUni {
				t.Fatalf("role %s watermark changed after invalid bidi direction GOAWAY: (%d, %d), want (%d, %d)", localRole, c.sessionControl.localGoAwayBidi, c.sessionControl.localGoAwayUni, startBidi, startUni)
			}

			err = c.GoAway(0, state.FirstLocalStreamID(localRole, false))
			if !IsErrorCode(err, CodeProtocol) {
				t.Fatalf("role %s local-owned uni GOAWAY err = %v, want %s", localRole, err, CodeProtocol)
			}
			if c.sessionControl.localGoAwayBidi != startBidi || c.sessionControl.localGoAwayUni != startUni {
				t.Fatalf("role %s watermark changed after invalid uni GOAWAY: (%d, %d), want (%d, %d)", localRole, c.sessionControl.localGoAwayBidi, c.sessionControl.localGoAwayUni, startBidi, startUni)
			}

			err = c.GoAway(0, state.FirstPeerStreamID(localRole, true))
			if !IsErrorCode(err, CodeProtocol) {
				t.Fatalf("role %s wrong-direction uni GOAWAY err = %v, want %s", localRole, err, CodeProtocol)
			}
			if c.sessionControl.localGoAwayBidi != startBidi || c.sessionControl.localGoAwayUni != startUni {
				t.Fatalf("role %s watermark changed after invalid uni direction GOAWAY: (%d, %d), want (%d, %d)", localRole, c.sessionControl.localGoAwayBidi, c.sessionControl.localGoAwayUni, startBidi, startUni)
			}
		}()
	}
}

func TestLocalGoAwayUsesLocalRoleForOwnership(t *testing.T) {
	for _, localRole := range []Role{RoleResponder, RoleInitiator} {
		peerRole := RoleResponder
		if localRole == RoleResponder {
			peerRole = RoleInitiator
		}

		for _, side := range []struct {
			name        string
			bidi, uni   uint64
			wantErrCode ErrorCode
		}{
			{
				name:        "peer_bidi",
				bidi:        state.FirstPeerStreamID(localRole, true),
				uni:         0,
				wantErrCode: 0,
			},
			{
				name:        "local_bidi_rejected",
				bidi:        state.FirstLocalStreamID(localRole, true),
				uni:         0,
				wantErrCode: CodeProtocol,
			},
			{
				name:        "peer_uni",
				bidi:        0,
				uni:         state.FirstPeerStreamID(localRole, false),
				wantErrCode: 0,
			},
			{
				name:        "local_uni_rejected",
				bidi:        0,
				uni:         state.FirstLocalStreamID(localRole, false),
				wantErrCode: CodeProtocol,
			},
		} {
			func() {
				c, _, stop := newInvalidPolicyConn(t)
				defer stop()

				c.config.local.Role = localRole
				c.config.peer.Role = peerRole
				c.config.negotiated.LocalRole = localRole
				c.config.negotiated.PeerRole = peerRole

				err := c.GoAway(side.bidi, side.uni)
				if side.wantErrCode == 0 {
					if err != nil {
						t.Fatalf("role %s %s err = %v, want nil", localRole, side.name, err)
					}
					if side.bidi != 0 && c.sessionControl.localGoAwayBidi != side.bidi {
						t.Fatalf("role %s %s bidi watermark = %d, want %d", localRole, side.name, c.sessionControl.localGoAwayBidi, side.bidi)
					}
					if side.uni != 0 && c.sessionControl.localGoAwayUni != side.uni {
						t.Fatalf("role %s %s uni watermark = %d, want %d", localRole, side.name, c.sessionControl.localGoAwayUni, side.uni)
					}
					return
				}
				if !IsErrorCode(err, side.wantErrCode) {
					t.Fatalf("role %s %s err = %v, want %s", localRole, side.name, err, side.wantErrCode)
				}
				if c.sessionControl.localGoAwayBidi != MaxVarint62 || c.sessionControl.localGoAwayUni != MaxVarint62 {
					t.Fatalf("role %s %s changed watermarks = (%d,%d), want MaxVarint62", localRole, side.name, c.sessionControl.localGoAwayBidi, c.sessionControl.localGoAwayUni)
				}
			}()
		}
	}
}

func TestTrackedSessionMemoryIncludesRetainedPeerReasonBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.recvSessionUsed = 10
	c.flow.queuedDataBytes = 20
	c.flow.advisoryQueuedBytes = 15
	c.flow.urgentQueuedBytes = 30
	c.pending.controlBytes = 40
	c.pending.priorityBytes = 50
	c.retention.retainedPeerReasonBytes = 60
	got := c.trackedSessionMemoryLocked()
	c.mu.Unlock()

	if got != 225 {
		t.Fatalf("trackedSessionMemoryLocked() = %d, want 225", got)
	}
}

func TestTrackedSessionMemoryIncludesRetainedOpenInfoAndOutstandingPingBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.recvSessionUsed = 10
	c.retention.retainedOpenInfoBytes = 70
	c.retention.retainedPeerReasonBytes = 60
	c.liveness.pingPayload = []byte("hello")
	got := c.trackedSessionMemoryLocked()
	c.mu.Unlock()

	if got != 145 {
		t.Fatalf("trackedSessionMemoryLocked() = %d, want 145", got)
	}
}

func TestTrackedSessionMemoryIncludesPreparedPriorityBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.recvSessionUsed = 10
	c.pending.preparedPriorityBytes = 12
	got := c.trackedSessionMemoryLocked()
	c.mu.Unlock()

	if got != 22 {
		t.Fatalf("trackedSessionMemoryLocked() = %d, want 22", got)
	}
}

func TestWriteQueueBlockedBySessionMemoryPressure(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := testLocalSendStream(c, 4)
	c.mu.Lock()
	c.flow.recvSessionUsed = c.sessionMemoryHighThresholdLocked()
	blocked := c.writeQueueBlockedLocked(stream, 1, 1)
	c.mu.Unlock()

	if !blocked {
		t.Fatal("expected write queue to block when tracked session memory is already at the high threshold")
	}
}

func TestSessionWriteMemoryBlockedProjectsAcrossHighThreshold(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.sessionMemoryCap = 16
	c.flow.recvSessionUsed = 11
	blocked := c.sessionWriteMemoryBlockedLocked(2)
	c.mu.Unlock()

	if !blocked {
		t.Fatal("expected projected tracked memory to block once it crosses the high threshold")
	}
}

func TestSessionMemoryWakeNeededRequiresReleasedMemoryBelowHighThreshold(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.sessionMemoryCap = 16
	prevTracked := c.sessionMemoryHighThresholdLocked()
	c.flow.recvSessionUsed = prevTracked - 1
	wake := c.sessionMemoryWakeNeededLocked(prevTracked)
	c.mu.Unlock()

	if !wake {
		t.Fatal("expected wake when tracked session memory crosses below the high threshold")
	}

	c.mu.Lock()
	c.flow.recvSessionUsed = prevTracked
	wake = c.sessionMemoryWakeNeededLocked(prevTracked)
	c.mu.Unlock()

	if wake {
		t.Fatal("unexpected wake when tracked session memory stays at the high threshold")
	}

	c.mu.Lock()
	c.flow.recvSessionUsed = prevTracked - 2
	wake = c.sessionMemoryWakeNeededLocked(prevTracked - 1)
	c.mu.Unlock()

	if !wake {
		t.Fatal("expected wake when below-threshold release may unblock projected memory waiters")
	}
}

func TestReleaseReceiveLockedWakesWriteWaitersWhenMemoryPressureDrops(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := testBuildStream(
		c,
		8,
		testWithLocalReceive(),
		testWithApplicationVisible(),
		testWithRecvWindow(128, 64),
		testWithWriteNotify(),
	)

	c.mu.Lock()
	wake := c.currentWriteWakeLocked()
	threshold := c.sessionMemoryHighThresholdLocked()
	c.flow.recvSessionUsed = threshold
	c.releaseReceiveLocked(stream, 64)
	c.mu.Unlock()

	select {
	case <-wake:
	default:
		t.Fatal("expected releaseReceiveLocked to wake blocked writers when session memory pressure drops")
	}
}

func TestBroadcastWriteWakeRotatesConnChannel(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	first := c.currentWriteWakeLocked()
	c.broadcastWriteWakeLocked()
	second := c.currentWriteWakeLocked()
	c.mu.Unlock()

	select {
	case <-first:
	default:
		t.Fatal("expected prior write-wake channel to be closed by broadcast")
	}

	select {
	case <-second:
		t.Fatal("new write-wake channel should remain open after rotation")
	default:
	}
}

func TestSessionMemoryHardCapIncludesRetainedOpenInfoAndPeerReasonBudget(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	got := c.sessionMemoryHardCapLocked()
	want := saturatingAdd(
		saturatingAdd(
			saturatingAdd(
				saturatingAdd(
					saturatingAdd(
						saturatingAdd(c.sessionWindowTargetLocked(), c.sessionDataHWMLocked()),
						c.urgentLaneCapLocked(),
					),
					c.pendingControlBudgetLocked(),
				),
				c.pendingPriorityBudgetLocked(),
			),
			c.retainedOpenInfoBudgetLocked(),
		),
		c.retainedPeerReasonBudgetLocked(),
	)
	if want < minSessionMemoryHardCap {
		want = minSessionMemoryHardCap
	}
	c.mu.Unlock()

	if got != want {
		t.Fatalf("sessionMemoryHardCapLocked() = %d, want %d", got, want)
	}
}

func TestClearWriteQueueReservationsClearsPreparedPriorityBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.pending.preparedPriorityBytes = 33
	c.clearWriteQueueReservationsLocked()
	got := c.pending.preparedPriorityBytes
	c.mu.Unlock()

	if got != 0 {
		t.Fatalf("preparedPriorityBytes after clearWriteQueueReservationsLocked = %d, want 0", got)
	}
}

func TestMarkerOnlyHardCapDefaultsToTrackedMemoryDerivedLimit(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.sessionMemoryCap = 3 * c.compactTerminalStateUnitLocked()
	got := c.markerOnlyHardCapLocked()
	c.mu.Unlock()

	if got != 3 {
		t.Fatalf("markerOnlyHardCapLocked() = %d, want 3", got)
	}
}

func TestQueuePriorityUpdateDropsWhenTrackedMemoryHardCapWouldBeExceeded(t *testing.T) {
	c := newPriorityBudgetTestConn()
	c.mu.Lock()
	c.flow.sessionMemoryCap = 8
	c.flow.recvSessionUsed = 8
	ensureTestPendingStream(c, 1)
	c.queuePriorityUpdateAsync(1, []byte("x"), retainedBytesBorrowed)
	ok := testHasPendingPriorityUpdate(c, 1)
	got := c.pending.priorityBytes
	c.mu.Unlock()

	if ok {
		t.Fatal("priority update should have been dropped when tracked session memory hard cap was already exhausted")
	}
	if got != 0 {
		t.Fatalf("pendingPriorityBytes = %d, want 0", got)
	}
}

func TestTakePendingPriorityUpdateRequestsTransfersTrackedBytesToAdvisoryQueue(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := newSessionMemoryTestConn()
	c.config.negotiated.Capabilities = caps
	stream := testVisibleBidiStream(c, 9)
	c.mu.Lock()
	testSetPendingPriorityUpdate(c, stream.id, append([]byte(nil), payload...))
	reqs, err := c.takePendingPriorityUpdateRequestsLocked()
	if err != nil {
		c.mu.Unlock()
		t.Fatalf("takePendingPriorityUpdateRequestsLocked() err = %v, want nil", err)
	}
	if len(reqs) != 1 {
		c.mu.Unlock()
		t.Fatalf("request count = %d, want 1", len(reqs))
	}
	if c.pending.priorityBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityBytes after handoff = %d, want 0", c.pending.priorityBytes)
	}
	req := reqs[0]
	if !req.advisoryReserved {
		c.mu.Unlock()
		t.Fatal("advisory request was not marked reserved during handoff")
	}
	if c.flow.advisoryQueuedBytes != req.queuedBytes {
		got := c.flow.advisoryQueuedBytes
		c.mu.Unlock()
		t.Fatalf("advisoryQueuedBytes after handoff = %d, want %d", got, req.queuedBytes)
	}
	if got := c.trackedSessionMemoryLocked(); got != req.queuedBytes {
		c.mu.Unlock()
		t.Fatalf("trackedSessionMemoryLocked() after handoff = %d, want %d", got, req.queuedBytes)
	}
	c.mu.Unlock()

	c.releaseBatchReservations(reqs)

	c.mu.Lock()
	if c.flow.advisoryQueuedBytes != 0 {
		got := c.flow.advisoryQueuedBytes
		c.mu.Unlock()
		t.Fatalf("advisoryQueuedBytes after release = %d, want 0", got)
	}
	if got := c.trackedSessionMemoryLocked(); got != 0 {
		c.mu.Unlock()
		t.Fatalf("trackedSessionMemoryLocked() after release = %d, want 0", got)
	}
	c.mu.Unlock()
}

func TestTakePendingPriorityUpdateRequestsFailsWhenAdvisoryHandoffWouldExceedTrackedMemoryCap(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := newSessionMemoryTestConn()
	c.config.negotiated.Capabilities = caps
	stream := testVisibleBidiStream(c, 9)
	c.mu.Lock()
	testSetPendingPriorityUpdate(c, stream.id, append([]byte(nil), payload...))
	c.flow.sessionMemoryCap = uint64(len(payload))
	_, err = c.takePendingPriorityUpdateRequestsLocked()
	if err != nil {
		c.mu.Unlock()
		t.Fatalf("takePendingPriorityUpdateRequestsLocked() err = %v, want nil on advisory drop", err)
	}
	if testHasPendingPriorityUpdate(c, stream.id) {
		c.mu.Unlock()
		t.Fatal("pending priority update retained after advisory drop")
	}
	if got := c.pending.priorityBytes; got != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityBytes after advisory drop = %d, want 0", got)
	}
	if c.flow.advisoryQueuedBytes != 0 {
		got := c.flow.advisoryQueuedBytes
		c.mu.Unlock()
		t.Fatalf("advisoryQueuedBytes after advisory drop = %d, want 0", got)
	}
	c.mu.Unlock()
}

func TestControlFlushLoopTransfersPendingPriorityBytesToAdvisoryQueue(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := newSessionMemoryTestConn()
	c.config.negotiated.Capabilities = caps
	stream := testVisibleBidiStream(c, 9)

	c.mu.Lock()
	testSetPendingPriorityUpdate(c, stream.id, append([]byte(nil), payload...))
	c.mu.Unlock()

	flushDone := make(chan struct{})
	go func() {
		c.controlFlushLoop()
		close(flushDone)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		select {
		case <-flushDone:
		case <-time.After(testSignalTimeout):
			t.Fatal("controlFlushLoop did not exit after closedCh close")
		}
	}()

	notify(c.pending.controlNotify)

	var queued writeRequest
	select {
	case queued = <-c.writer.advisoryWriteCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("controlFlushLoop did not enqueue advisory request")
	}

	c.mu.Lock()
	if c.pending.priorityBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityBytes after control flush = %d, want 0", c.pending.priorityBytes)
	}
	if c.flow.advisoryQueuedBytes != queued.queuedBytes {
		got := c.flow.advisoryQueuedBytes
		c.mu.Unlock()
		t.Fatalf("advisoryQueuedBytes after control flush = %d, want %d", got, queued.queuedBytes)
	}
	if got := c.trackedSessionMemoryLocked(); got != queued.queuedBytes {
		c.mu.Unlock()
		t.Fatalf("trackedSessionMemoryLocked() after control flush = %d, want %d", got, queued.queuedBytes)
	}
	c.mu.Unlock()

	c.releaseBatchReservations([]writeRequest{queued})
	queued.done <- nil

	c.mu.Lock()
	if c.flow.advisoryQueuedBytes != 0 {
		got := c.flow.advisoryQueuedBytes
		c.mu.Unlock()
		t.Fatalf("advisoryQueuedBytes after release = %d, want 0", got)
	}
	c.mu.Unlock()
}

func TestRetainPeerReasonRespectsTrackedMemoryHardCap(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.sessionMemoryCap = 8
	c.flow.recvSessionUsed = 7
	got, n := c.retainPeerReasonLocked(0, "abcd")
	c.mu.Unlock()

	if got != "a" || n != 1 {
		t.Fatalf("retainPeerReasonLocked() = (%q, %d), want (%q, %d)", got, n, "a", 1)
	}
}

func TestClearPingLockedWakesWriteWaitersWhenMemoryPressureDrops(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.flow.sessionMemoryCap = 16
	c.flow.recvSessionUsed = 8
	c.liveness.pingOutstanding = true
	c.liveness.pingPayload = []byte("hello")
	c.liveness.pingDone = make(chan struct{})
	wake := c.currentWriteWakeLocked()
	done := c.liveness.pingDone
	c.clearPingLocked()
	c.mu.Unlock()

	select {
	case <-wake:
	default:
		t.Fatal("expected clearPingLocked to wake blocked writers when tracked session memory pressure drops")
	}

	select {
	case <-done:
	default:
		t.Fatal("expected clearPingLocked to close pingDone")
	}
}

func TestReleaseWriteQueueReservationMemoryWakeDoesNotSuppressStreamWake(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()
	stream := &nativeStream{
		conn:        c,
		id:          8,
		idSet:       true,
		localSend:   true,
		writeNotify: make(chan struct{}, 1),
	}
	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		queueReserved:  true,
		queuedBytes:    5,
		reservedStream: stream,
	}

	c.mu.Lock()
	c.flow.sessionMemoryCap = 16
	c.flow.sessionDataHWM = 100
	c.flow.perStreamDataHWM = 8
	c.flow.recvSessionUsed = 4
	c.flow.queuedDataBytes = 10
	stream.queuedDataBytes = 5
	wake := c.currentWriteWakeLocked()
	c.mu.Unlock()

	c.releaseWriteQueueReservation(&req)

	select {
	case <-wake:
	default:
		t.Fatal("expected memory release to broadcast global write wake")
	}
	select {
	case <-stream.writeNotify:
	default:
		t.Fatal("expected memory release not to suppress per-stream wake")
	}
}

func TestRestorePreparedPriorityUpdateReplacesExistingPendingBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := &nativeStream{conn: c, id: 9, idSet: true}
	req := &writeRequest{
		preparedPriorityStreamID: stream.id,
		preparedPriorityPayload:  []byte("abcd"),
		preparedPriorityBytes:    8,
	}

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	testSetPendingPriorityUpdate(c, stream.id, []byte("old"))
	c.pending.preparedPriorityBytes = req.preparedPriorityBytes
	c.restorePreparedPriorityUpdateLocked(req)
	stored, _ := testPendingPriorityUpdatePayload(c, stream.id)
	gotPending := c.pending.priorityBytes
	gotPrepared := c.pending.preparedPriorityBytes
	c.mu.Unlock()

	if string(stored) != "abcd" {
		t.Fatalf("restored pending payload = %q, want %q", stored, "abcd")
	}
	if gotPending != 4 {
		t.Fatalf("pendingPriorityBytes = %d, want 4", gotPending)
	}
	if gotPrepared != 0 {
		t.Fatalf("preparedPriorityBytes = %d, want 0", gotPrepared)
	}
}

func TestReleasePreparedWriteRequestWakesWriteWaitersWhenAdvisoryReleaseDropsMemoryPressure(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.flow.sessionMemoryCap = 16
	c.flow.recvSessionUsed = 11
	c.flow.advisoryQueuedBytes = 1
	wake := c.currentWriteWakeLocked()
	c.mu.Unlock()

	req := &writeRequest{
		advisoryReserved: true,
		queuedBytes:      1,
	}
	c.releasePreparedWriteRequest(req)

	select {
	case <-wake:
	default:
		t.Fatal("expected advisory prepared-release to wake blocked writers when tracked session memory drops below threshold")
	}
}

func TestCloseStreamOnSessionWakesWriteWaitersWhenSendOnlyBufferedReceiveIsReleased(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := &nativeStream{
		conn:         c,
		id:           7,
		idSet:        true,
		localOpen:    testLocalOpenOpenedCommittedState(),
		localSend:    true,
		localReceive: false,
		recvBuffer:   2,
		readNotify:   make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}
	stream.initHalfStates()

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	c.flow.sessionMemoryCap = 16
	c.flow.recvSessionUsed = c.sessionMemoryHighThresholdLocked()
	wake := c.currentWriteWakeLocked()
	c.closeStreamOnSessionWithOptionsLocked(stream, applicationErr(uint64(CodeInternal), "close"), sessionCloseOptions{
		abortSource: terminalAbortLocal,
		finalize:    false,
	})
	c.mu.Unlock()

	select {
	case <-wake:
	default:
		t.Fatal("expected closeStreamOnSessionWithOptionsLocked to wake blocked writers when buffered receive memory is released")
	}
}

func TestStoreRetainedBytesReusesExistingTightBacking(t *testing.T) {
	existing := make([]byte, 3)
	copy(existing, "old")
	payload := []byte("new")

	stored := storeRetainedBytes(existing, payload, retainedBytesBorrowed)
	if !bytes.Equal(stored, payload) {
		t.Fatalf("stored payload = %q, want %q", stored, payload)
	}
	if len(stored) == 0 || &stored[0] != &existing[0] {
		t.Fatal("storeRetainedBytes did not reuse existing backing array")
	}

	payload[0] = 'x'
	if string(stored) != "new" {
		t.Fatalf("stored payload mutated with source = %q, want %q", stored, "new")
	}
}

func TestStoreRetainedBytesTightensOversizedExistingBacking(t *testing.T) {
	existing := make([]byte, 3, 64)
	copy(existing, "old")
	payload := []byte("new")

	stored := storeRetainedBytes(existing, payload, retainedBytesBorrowed)
	if !bytes.Equal(stored, payload) {
		t.Fatalf("stored payload = %q, want %q", stored, payload)
	}
	if got := cap(stored); got != len(stored) {
		t.Fatalf("cap(stored) = %d, want tight cap %d", got, len(stored))
	}
	if len(stored) == 0 || &stored[0] == &existing[0] {
		t.Fatal("storeRetainedBytes reused oversized existing backing array")
	}
}

func TestRapidHiddenAbortChurnTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	start := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	for i := 0; i < hiddenAbortChurnThreshold; i++ {
		streamID := start + uint64(i*4)
		if err := c.handleAbortFrame(Frame{
			Type:     FrameTypeABORT,
			StreamID: streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		}); err != nil {
			t.Fatalf("early churn event %d err = %v, want nil", i+1, err)
		}
	}

	err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: start + uint64(hiddenAbortChurnThreshold*4),
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final churn err = %v, want %s", err, CodeProtocol)
	}
}

func TestHiddenAbortChurnWindowExpiryResetsCounter(t *testing.T) {
	c := &Conn{}
	base := time.Unix(1, 0)

	for i := 0; i < hiddenAbortChurnThreshold; i++ {
		if err := c.recordHiddenAbortChurnLocked(base); err != nil {
			t.Fatalf("early hidden abort churn %d err = %v, want nil", i+1, err)
		}
	}

	if err := c.recordHiddenAbortChurnLocked(base.Add(hiddenAbortChurnWindow)); err != nil {
		t.Fatalf("hidden abort churn after window expiry err = %v, want nil", err)
	}
}

func TestRapidVisibleAbortChurnTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.queues.acceptBacklogLimit = 1024
	c.queues.acceptBacklogBytesLimit = 1 << 20
	start := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	for i := 0; i < visibleChurnThreshold; i++ {
		streamID := start + uint64(i*4)
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID}); err != nil {
			t.Fatalf("open visible stream %d err = %v, want nil", i+1, err)
		}
		if err := c.handleAbortFrame(Frame{
			Type:     FrameTypeABORT,
			StreamID: streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		}); err != nil {
			t.Fatalf("early visible abort churn %d err = %v, want nil", i+1, err)
		}
	}

	lastID := start + uint64(visibleChurnThreshold*4)
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: lastID}); err != nil {
		t.Fatalf("final open visible stream err = %v, want nil", err)
	}
	err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: lastID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final visible abort churn err = %v, want %s", err, CodeProtocol)
	}
}

func TestRapidVisibleUniResetChurnTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.queues.acceptBacklogLimit = 1024
	c.queues.acceptBacklogBytesLimit = 1 << 20
	start := state.FirstPeerStreamID(c.config.negotiated.LocalRole, false)
	for i := 0; i < visibleChurnThreshold; i++ {
		streamID := start + uint64(i*4)
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID}); err != nil {
			t.Fatalf("open visible uni stream %d err = %v, want nil", i+1, err)
		}
		if err := c.handleResetFrame(Frame{
			Type:     FrameTypeRESET,
			StreamID: streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		}); err != nil {
			t.Fatalf("early visible reset churn %d err = %v, want nil", i+1, err)
		}
	}

	lastID := start + uint64(visibleChurnThreshold*4)
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: lastID}); err != nil {
		t.Fatalf("final open visible uni stream err = %v, want nil", err)
	}
	err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: lastID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final visible reset churn err = %v, want %s", err, CodeProtocol)
	}
}

func TestVisibleTerminalChurnWindowExpiryResetsCounter(t *testing.T) {
	c := &Conn{}
	base := time.Unix(1, 0)

	for i := 0; i < visibleChurnThreshold; i++ {
		if err := c.recordChurnLocked(base, &c.abuse.visibleChurnWindowStart, &c.abuse.visibleChurnCount, visibleChurnWindow, visibleChurnThreshold, "handle RESET", "test visible churn"); err != nil {
			t.Fatalf("early visible churn %d err = %v, want nil", i+1, err)
		}
	}

	if err := c.recordChurnLocked(base.Add(visibleChurnWindow), &c.abuse.visibleChurnWindowStart, &c.abuse.visibleChurnCount, visibleChurnWindow, visibleChurnThreshold, "handle RESET", "test visible churn"); err != nil {
		t.Fatalf("visible churn after window expiry err = %v, want nil", err)
	}
}

func TestRecordChurnLockedSaturatesCounterInsteadOfWrapping(t *testing.T) {
	c := &Conn{}
	base := time.Unix(1, 0)
	c.abuse.visibleChurnWindowStart = windowStampAt(base)
	c.abuse.visibleChurnCount = ^uint32(0)

	if err := c.recordChurnLocked(base, &c.abuse.visibleChurnWindowStart, &c.abuse.visibleChurnCount, visibleChurnWindow, ^uint32(0), "handle RESET", "test visible churn"); err != nil {
		t.Fatalf("recordChurnLocked() err = %v, want nil at saturated threshold", err)
	}
	if c.abuse.visibleChurnCount != ^uint32(0) {
		t.Fatalf("visibleChurnCount = %d, want saturation to %d", c.abuse.visibleChurnCount, ^uint32(0))
	}
}

func TestVisibleTerminalChurnIgnoresAcceptedTerminalStream(t *testing.T) {
	stream := &nativeStream{
		localReceive:       true,
		applicationVisible: true,
	}
	stream.markAccepted()
	stream.initHalfStates()
	stream.setRecvReset(&ApplicationError{Code: uint64(CodeCancelled)})
	if shouldRecordVisibleTerminalChurnLocked(stream) {
		t.Fatal("accepted visible terminal stream unexpectedly counted as churn")
	}
}

func TestVisibleTerminalChurnIgnoresBidiResetOnly(t *testing.T) {
	stream := &nativeStream{
		localSend:          true,
		localReceive:       true,
		applicationVisible: true,
	}
	stream.initHalfStates()
	stream.setRecvReset(&ApplicationError{Code: uint64(CodeCancelled)})
	if shouldRecordVisibleTerminalChurnLocked(stream) {
		t.Fatal("bidi recv-only RESET unexpectedly counted as fully terminal churn")
	}
}

func TestRepeatedNoOpZeroLengthDataTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	frame := Frame{Type: FrameTypeDATA, StreamID: stream.id}
	for i := 0; i < noOpZeroDataFloodThreshold; i++ {
		if err := c.handleDataFrame(frame); err != nil {
			t.Fatalf("early no-op DATA %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleDataFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final no-op DATA err = %v, want %s", err, CodeProtocol)
	}
}

func TestRepeatedNoOpSessionBlockedTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	frame := Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}
	for i := 0; i < noOpBlockedFloodThreshold; i++ {
		if err := c.handleBlockedFrame(frame); err != nil {
			t.Fatalf("early no-op session BLOCKED %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleBlockedFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final no-op session BLOCKED err = %v, want %s", err, CodeProtocol)
	}
}

func TestRepeatedNoOpSessionMaxDataTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	frame := Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(c.flow.sendSessionMax)}
	for i := 0; i < noOpMaxDataFloodThreshold; i++ {
		if err := c.handleMaxDataFrame(frame); err != nil {
			t.Fatalf("early no-op session MAX_DATA %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleMaxDataFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final no-op session MAX_DATA err = %v, want %s", err, CodeProtocol)
	}
}

func TestSessionMaxDataIncreaseClearsNoOpBudget(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	noOpFrame := Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(c.flow.sendSessionMax)}
	for i := 0; i < noOpMaxDataFloodThreshold-1; i++ {
		if err := c.handleMaxDataFrame(noOpFrame); err != nil {
			t.Fatalf("pre-reset no-op session MAX_DATA %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(c.flow.sendSessionMax + 1)}); err != nil {
		t.Fatalf("increasing session MAX_DATA err = %v, want nil", err)
	}
	for i := 0; i < noOpMaxDataFloodThreshold; i++ {
		if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(c.flow.sendSessionMax)}); err != nil {
			t.Fatalf("post-reset no-op session MAX_DATA %d err = %v, want nil", i+1, err)
		}
	}
}

func TestSessionReplenishClearsNoOpBlockedBudget(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	frame := Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}
	for i := 0; i < noOpBlockedFloodThreshold-1; i++ {
		if err := c.handleBlockedFrame(frame); err != nil {
			t.Fatalf("pre-reset no-op session BLOCKED %d err = %v, want nil", i+1, err)
		}
	}

	c.mu.Lock()
	c.flow.recvSessionPending = 1
	c.mu.Unlock()

	if err := c.handleBlockedFrame(frame); err != nil {
		t.Fatalf("session BLOCKED with pending credit err = %v, want nil", err)
	}

	for i := 0; i < noOpBlockedFloodThreshold; i++ {
		if err := c.handleBlockedFrame(frame); err != nil {
			t.Fatalf("post-reset no-op session BLOCKED %d err = %v, want nil", i+1, err)
		}
	}
}

func TestRepeatedNoOpStreamBlockedTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	frame := Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
	for i := 0; i < noOpBlockedFloodThreshold; i++ {
		if err := c.handleBlockedFrame(frame); err != nil {
			t.Fatalf("early no-op stream BLOCKED %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleBlockedFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final no-op stream BLOCKED err = %v, want %s", err, CodeProtocol)
	}
}

func TestRepeatedNoOpStreamMaxDataTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	frame := Frame{Type: FrameTypeMAXDATA, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
	for i := 0; i < noOpMaxDataFloodThreshold; i++ {
		if err := c.handleMaxDataFrame(frame); err != nil {
			t.Fatalf("early no-op stream MAX_DATA %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleMaxDataFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final no-op stream MAX_DATA err = %v, want %s", err, CodeProtocol)
	}
}

func TestMixedNoOpControlFloodTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	maxData := Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(c.flow.sendSessionMax)}
	blocked := Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
	for i := 0; i < noOpControlFloodThreshold; i++ {
		var err error
		if i%2 == 0 {
			err = c.handleMaxDataFrame(maxData)
		} else {
			err = c.handleBlockedFrame(blocked)
		}
		if err != nil {
			t.Fatalf("early mixed no-op control %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleBlockedFrame(blocked); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final mixed no-op control err = %v, want %s", err, CodeProtocol)
	}
}

func TestEffectiveControlChangeClearsMixedNoOpControlBudget(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	noOpMaxData := Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(c.flow.sendSessionMax)}
	noOpBlocked := Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}
	for i := 0; i < noOpControlFloodThreshold-1; i++ {
		var err error
		if i%2 == 0 {
			err = c.handleMaxDataFrame(noOpMaxData)
		} else {
			err = c.handleBlockedFrame(noOpBlocked)
		}
		if err != nil {
			t.Fatalf("pre-reset mixed no-op control %d err = %v, want nil", i+1, err)
		}
	}

	if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(c.flow.sendSessionMax + 1)}); err != nil {
		t.Fatalf("effective MAX_DATA change err = %v, want nil", err)
	}

	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handleBlockedFrame(noOpBlocked); err != nil {
			t.Fatalf("post-reset no-op BLOCKED %d err = %v, want nil", i+1, err)
		}
	}
}

func TestRepeatedUnexpectedPongTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	frame := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handlePongFrame(frame); err != nil {
			t.Fatalf("early unexpected PONG %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handlePongFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final unexpected PONG err = %v, want %s", err, CodeProtocol)
	}
}

func TestUnexpectedPongHandlerTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	frame := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handlePongFrame(frame); err != nil {
			t.Fatalf("early unexpected PONG %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handlePongFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final unexpected PONG err = %v, want %s", err, CodeProtocol)
	}
}

func TestMatchingPongClearsMixedNoOpControlBudget(t *testing.T) {
	c, frames, stop := newInvalidPolicyConn(t)
	defer stop()

	noOp := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	for i := 0; i < noOpControlFloodThreshold-1; i++ {
		if err := c.handlePongFrame(noOp); err != nil {
			t.Fatalf("pre-reset unexpected PONG %d err = %v, want nil", i+1, err)
		}
	}

	payload := []byte{9, 8, 7, 6, 5, 4, 3, 2}
	done, _, err := c.beginPing(payload)
	if err != nil {
		t.Fatalf("beginPing: %v", err)
	}
	if done == nil {
		t.Fatal("beginPing done channel = nil, want non-nil")
	}
	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypePING {
		t.Fatalf("queued frame = %+v, want PING", queued)
	}
	if !bytes.Equal(queued.Payload, payload) {
		t.Fatalf("queued PING payload = %x, want %x", queued.Payload, payload)
	}

	if err := c.handlePongFrame(Frame{Type: FrameTypePONG, Payload: payload}); err != nil {
		t.Fatalf("matching PONG err = %v, want nil", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("matching PONG did not complete outstanding ping")
	}

	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handlePongFrame(noOp); err != nil {
			t.Fatalf("post-reset unexpected PONG %d err = %v, want nil", i+1, err)
		}
	}
}

func TestHandleFrameMatchingPongClearsMixedNoOpControlBudget(t *testing.T) {
	c, frames, stop := newInvalidPolicyConn(t)
	defer stop()

	noOp := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	for i := 0; i < noOpControlFloodThreshold-1; i++ {
		if err := c.handleFrame(noOp); err != nil {
			t.Fatalf("pre-reset handleFrame(PONG) %d err = %v, want nil", i+1, err)
		}
	}

	payload := []byte{9, 8, 7, 6, 5, 4, 3, 2}
	done, _, err := c.beginPing(payload)
	if err != nil {
		t.Fatalf("beginPing: %v", err)
	}
	if done == nil {
		t.Fatal("beginPing done channel = nil, want non-nil")
	}
	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypePING {
		t.Fatalf("queued frame = %+v, want PING", queued)
	}
	if !bytes.Equal(queued.Payload, payload) {
		t.Fatalf("queued PING payload = %x, want %x", queued.Payload, payload)
	}

	if err := c.handleFrame(Frame{Type: FrameTypePONG, Payload: payload}); err != nil {
		t.Fatalf("matching handleFrame(PONG) err = %v, want nil", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("matching handleFrame(PONG) did not complete outstanding ping")
	}

	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handleFrame(noOp); err != nil {
			t.Fatalf("post-reset handleFrame(PONG) %d err = %v, want nil", i+1, err)
		}
	}
}

func TestHandleFrameUnexpectedPongTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	frame := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handleFrame(frame); err != nil {
			t.Fatalf("early handleFrame(PONG) %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final handleFrame(PONG) err = %v, want %s", err, CodeProtocol)
	}
}

func TestStateChangingResetClearsMixedNoOpControlBudget(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	noOp := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	for i := 0; i < noOpControlFloodThreshold-1; i++ {
		if err := c.handlePongFrame(noOp); err != nil {
			t.Fatalf("pre-reset unexpected PONG %d err = %v, want nil", i+1, err)
		}
	}

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	if err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: stream.id,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("state-changing RESET err = %v, want nil", err)
	}

	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handlePongFrame(noOp); err != nil {
			t.Fatalf("post-reset unexpected PONG %d err = %v, want nil", i+1, err)
		}
	}
}

func TestRepeatedNoOpGoAwayTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	bidi := maxPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityBidi)
	uni := maxPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityUni)
	frame := Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, bidi, uni, uint64(CodeNoError), ""),
	}
	if err := c.handleGoAwayFrame(frame); err != nil {
		t.Fatalf("initial state-changing GOAWAY err = %v, want nil", err)
	}
	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handleGoAwayFrame(frame); err != nil {
			t.Fatalf("early no-op GOAWAY %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleGoAwayFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final no-op GOAWAY err = %v, want %s", err, CodeProtocol)
	}
}

func TestGoAwayChangeClearsMixedNoOpControlBudget(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	noOpBlocked := Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}
	for i := 0; i < noOpControlFloodThreshold-1; i++ {
		if err := c.handleBlockedFrame(noOpBlocked); err != nil {
			t.Fatalf("pre-reset no-op BLOCKED %d err = %v, want nil", i+1, err)
		}
	}

	bidi := maxPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityBidi)
	uni := maxPeerGoAwayWatermark(c.config.negotiated.LocalRole, streamArityUni)
	frame := Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, bidi, uni, uint64(CodeNoError), ""),
	}
	if err := c.handleGoAwayFrame(frame); err != nil {
		t.Fatalf("state-changing GOAWAY err = %v, want nil", err)
	}

	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handleGoAwayFrame(frame); err != nil {
			t.Fatalf("post-reset no-op GOAWAY %d err = %v, want nil", i+1, err)
		}
	}
}

func TestRepeatedIgnoredStopSendingFloodTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_stop_seen",
		RecvHalf: "recv_open",
	})

	frame := Frame{Type: FrameTypeStopSending, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}
	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handleStopSendingFrame(frame); err != nil {
			t.Fatalf("early ignored STOP_SENDING %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleStopSendingFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final ignored STOP_SENDING err = %v, want %s", err, CodeProtocol)
	}
}

func TestRepeatedIgnoredResetFloodTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_reset",
	})

	frame := Frame{Type: FrameTypeRESET, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}
	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handleResetFrame(frame); err != nil {
			t.Fatalf("early ignored RESET %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleResetFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final ignored RESET err = %v, want %s", err, CodeProtocol)
	}
}

func TestRepeatedIgnoredAbortFloodTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, false), "uni", "peer_owned", stateHalfExpect{
		SendHalf: "send_aborted",
		RecvHalf: "recv_aborted",
	})

	frame := Frame{Type: FrameTypeABORT, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}
	for i := 0; i < noOpControlFloodThreshold; i++ {
		if err := c.handleAbortFrame(frame); err != nil {
			t.Fatalf("early ignored ABORT %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleAbortFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final ignored ABORT err = %v, want %s", err, CodeProtocol)
	}
}

func TestMaterialDataClearsNoOpZeroLengthDataBudget(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	noOp := Frame{Type: FrameTypeDATA, StreamID: stream.id}
	for i := 0; i < noOpZeroDataFloodThreshold-1; i++ {
		if err := c.handleDataFrame(noOp); err != nil {
			t.Fatalf("pre-reset no-op DATA %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("x")}); err != nil {
		t.Fatalf("material DATA err = %v, want nil", err)
	}
	for i := 0; i < noOpZeroDataFloodThreshold; i++ {
		if err := c.handleDataFrame(noOp); err != nil {
			t.Fatalf("post-reset no-op DATA %d err = %v, want nil", i+1, err)
		}
	}
}

func TestRepeatedNoOpPriorityUpdateTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityPriorityHints
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	stream.priority = 7

	update := MetadataUpdate{Priority: uint64ptr(7)}
	payload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, update, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}
	frame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}

	for i := 0; i < noOpPriorityUpdateFloodThreshold; i++ {
		if err := c.handleExtFrame(frame); err != nil {
			t.Fatalf("early no-op PRIORITY_UPDATE %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleExtFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final no-op PRIORITY_UPDATE err = %v, want %s", err, CodeProtocol)
	}
}

func TestRepeatedIgnoredTerminalStreamMaxDataTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	frame := Frame{Type: FrameTypeMAXDATA, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
	for i := 0; i < noOpMaxDataFloodThreshold; i++ {
		if err := c.handleMaxDataFrame(frame); err != nil {
			t.Fatalf("early ignored terminal MAX_DATA %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleMaxDataFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final ignored terminal MAX_DATA err = %v, want %s", err, CodeProtocol)
	}
}

func TestRepeatedIgnoredTerminalStreamBlockedTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	frame := Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
	for i := 0; i < noOpBlockedFloodThreshold; i++ {
		if err := c.handleBlockedFrame(frame); err != nil {
			t.Fatalf("early ignored terminal BLOCKED %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleBlockedFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final ignored terminal BLOCKED err = %v, want %s", err, CodeProtocol)
	}
}

func TestRepeatedIgnoredTerminalStreamPriorityUpdateTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityPriorityHints
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_aborted",
		RecvHalf: "recv_aborted",
	})

	payload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(7)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build terminal priority update payload: %v", err)
	}
	frame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}

	for i := 0; i < noOpPriorityUpdateFloodThreshold; i++ {
		if err := c.handleExtFrame(frame); err != nil {
			t.Fatalf("early ignored terminal PRIORITY_UPDATE %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleExtFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final ignored terminal PRIORITY_UPDATE err = %v, want %s", err, CodeProtocol)
	}
}

func TestEffectivePriorityUpdateClearsNoOpPriorityBudget(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityPriorityHints
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	stream.priority = 7

	noOpPayload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(7)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build no-op priority update payload: %v", err)
	}
	changePayload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(9)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build changing priority update payload: %v", err)
	}
	noOpFrame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: noOpPayload}
	changeFrame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: changePayload}

	for i := 0; i < noOpPriorityUpdateFloodThreshold-1; i++ {
		if err := c.handleExtFrame(noOpFrame); err != nil {
			t.Fatalf("pre-reset no-op PRIORITY_UPDATE %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleExtFrame(changeFrame); err != nil {
		t.Fatalf("effective PRIORITY_UPDATE err = %v, want nil", err)
	}
	if stream.priority != 9 {
		t.Fatalf("stream priority = %d, want 9", stream.priority)
	}
	for i := 0; i < noOpPriorityUpdateFloodThreshold; i++ {
		if err := c.handleExtFrame(changeFrame); err != nil {
			t.Fatalf("post-reset no-op PRIORITY_UPDATE %d err = %v, want nil", i+1, err)
		}
	}
}

func TestRepeatedEffectiveGroupRebucketChurnTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityStreamGroups
	c.config.peer.Settings.SchedulerHints = SchedulerGroupFair
	c.abuse.groupRebucketFloodLimit = 2
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	build := func(group uint64) []byte {
		t.Helper()
		payload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(group)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			t.Fatalf("build group update payload %d: %v", group, err)
		}
		return payload
	}

	first := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: build(1)}
	second := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: build(2)}
	third := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: build(3)}

	if err := c.handleExtFrame(first); err != nil {
		t.Fatalf("first effective group update err = %v, want nil", err)
	}
	if !stream.groupExplicit || stream.group != 1 {
		t.Fatalf("stream group after first update = (%v,%d), want (%v,%d)", stream.groupExplicit, stream.group, true, uint64(1))
	}
	if err := c.handleExtFrame(second); err != nil {
		t.Fatalf("second effective group update err = %v, want nil", err)
	}
	if !stream.groupExplicit || stream.group != 2 {
		t.Fatalf("stream group after second update = (%v,%d), want (%v,%d)", stream.groupExplicit, stream.group, true, uint64(2))
	}
	if err := c.handleExtFrame(third); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("third effective group update err = %v, want %s", err, CodeProtocol)
	}
}

func TestEffectiveGroupUpdatesDoNotTriggerChurnOutsideGroupFair(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityStreamGroups
	c.abuse.groupRebucketFloodLimit = 1
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	firstPayload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(1)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build first group update payload: %v", err)
	}
	secondPayload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(2)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build second group update payload: %v", err)
	}

	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: firstPayload}); err != nil {
		t.Fatalf("first non-group-fair group update err = %v, want nil", err)
	}
	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: secondPayload}); err != nil {
		t.Fatalf("second non-group-fair group update err = %v, want nil", err)
	}
	if !stream.groupExplicit || stream.group != 2 {
		t.Fatalf("stream group after non-group-fair updates = (%v,%d), want (%v,%d)", stream.groupExplicit, stream.group, true, uint64(2))
	}
}

func TestInboundPingFloodTriggersProtocolClose(t *testing.T) {
	c, frames, stop := newInvalidPolicyConn(t)
	defer stop()

	frame := Frame{Type: FrameTypePING, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	for i := 0; i < inboundPingFloodThreshold; i++ {
		if err := c.handleFrame(frame); err != nil {
			t.Fatalf("early inbound PING %d err = %v, want nil", i+1, err)
		}
		queued := awaitQueuedFrame(t, frames)
		if queued.Type != FrameTypePONG {
			t.Fatalf("queued frame = %+v, want PONG", queued)
		}
	}
	if err := c.handleFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final inbound PING err = %v, want %s", err, CodeProtocol)
	}
}

func TestInboundControlFrameBudgetTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	frame := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	for i := 0; i < inboundControlFrameBudget; i++ {
		armMatchingPingForPolicyTest(c, frame.Payload)
		if err := c.handleFrame(frame); err != nil {
			t.Fatalf("early inbound control frame %d err = %v, want nil", i+1, err)
		}
	}
	armMatchingPingForPolicyTest(c, frame.Payload)
	if err := c.handleFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final inbound control frame err = %v, want %s", err, CodeProtocol)
	}
}

func TestInboundControlByteBudgetTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	payload := make([]byte, int(c.config.local.Settings.MaxControlPayloadBytes))
	payload[7] = 1
	frame := Frame{Type: FrameTypePONG, Payload: payload}

	c.mu.Lock()
	byteBudget := c.inboundControlByteBudgetLocked()
	c.mu.Unlock()
	repeat := int(byteBudget/uint64(len(payload))) + 1
	for i := 0; i < repeat-1; i++ {
		if err := c.handleFrame(frame); err != nil {
			t.Fatalf("early inbound control bytes %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final inbound control byte-budget err = %v, want %s", err, CodeProtocol)
	}
}

func TestInboundExtFrameBudgetTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	payload := mustEncodeVarint(63)
	frame := Frame{Type: FrameTypeEXT, Payload: payload}
	for i := 0; i < inboundExtFrameBudget; i++ {
		if err := c.handleFrame(frame); err != nil {
			t.Fatalf("early inbound EXT frame %d err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleFrame(frame); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final inbound EXT frame err = %v, want %s", err, CodeProtocol)
	}
}

func TestInboundMixedControlExtFrameBudgetTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	control := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	ext := Frame{Type: FrameTypeEXT, Payload: mustEncodeVarint(63)}

	c.mu.Lock()
	budget := c.inboundMixedFrameBudgetLocked()
	c.mu.Unlock()

	for i := uint32(0); i < budget; i++ {
		var err error
		if i%2 == 0 {
			armMatchingPingForPolicyTest(c, control.Payload)
			err = c.handleFrame(control)
		} else {
			err = c.handleFrame(ext)
		}
		if err != nil {
			t.Fatalf("early inbound mixed control/EXT frame %d err = %v, want nil", i+1, err)
		}
	}
	armMatchingPingForPolicyTest(c, control.Payload)
	if err := c.handleFrame(control); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final inbound mixed control/EXT frame err = %v, want %s", err, CodeProtocol)
	}
}

func armMatchingPingForPolicyTest(c *Conn, payload []byte) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.liveness.pingOutstanding = true
	c.liveness.pingPayload = append([]byte(nil), payload...)
	c.liveness.lastPingSentAt = time.Now()
	c.liveness.pingDone = make(chan struct{})
	c.mu.Unlock()
}

func TestInboundMixedControlExtByteBudgetTriggersProtocolClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.mu.Lock()
	chunk := c.config.local.Settings.MaxControlPayloadBytes
	if extMax := c.config.local.Settings.MaxExtensionPayloadBytes; extMax < chunk {
		chunk = extMax
	}
	byteBudget := c.inboundMixedByteBudgetLocked()
	c.mu.Unlock()

	controlPayload := make([]byte, int(chunk))
	controlPayload[7] = 1
	extPayload := append(mustEncodeVarint(63), make([]byte, int(chunk)-1)...)
	control := Frame{Type: FrameTypePONG, Payload: controlPayload}
	ext := Frame{Type: FrameTypeEXT, Payload: extPayload}
	pairs := int(byteBudget / saturatingMul(chunk, 2))

	for i := 0; i < pairs; i++ {
		if err := c.handleFrame(control); err != nil {
			t.Fatalf("early inbound mixed control bytes pair %d control err = %v, want nil", i+1, err)
		}
		if err := c.handleFrame(ext); err != nil {
			t.Fatalf("early inbound mixed control bytes pair %d EXT err = %v, want nil", i+1, err)
		}
	}
	if err := c.handleFrame(control); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final inbound mixed control/EXT byte-budget err = %v, want %s", err, CodeProtocol)
	}
}

func TestInboundDataSessionMemoryCapTriggersInternalClose(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.flow.sessionMemoryCap = 3
	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true),
		Payload:  []byte("abcd"),
	})
	if !IsErrorCode(err, CodeInternal) {
		t.Fatalf("inbound DATA memory-cap err = %v, want %s", err, CodeInternal)
	}
	if !IsErrorCode(c.err(), CodeInternal) {
		t.Fatalf("stored session err = %v, want %s", c.err(), CodeInternal)
	}
}

func uint64ptr(v uint64) *uint64 {
	return &v
}

func TestLateDataAfterFinGetsStreamClosedCloseWithError(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(accepted)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	clientStreamImpl := requireNativeStreamImpl(t, clientStream)
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if err := clientStream.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	case serverStream = <-acceptCh:
	}

	buf := make([]byte, 8)
	n, err := serverStream.Read(buf)
	if err != nil {
		t.Fatalf("server read: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("server read = %q, want %q", got, "hi")
	}
	if _, err := serverStream.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("server second read err = %v, want EOF", err)
	}

	if err := testQueueFrame(client, Frame{Type: FrameTypeDATA, StreamID: clientStreamImpl.id, Payload: []byte("x")}); err != nil {
		t.Fatalf("queue late data: %v", err)
	}

	awaitStreamReadState(t, clientStreamImpl, testSignalTimeout, func(stream *nativeStream) bool {
		return stream.recvAbort != nil
	}, "timed out waiting for late DATA after FIN to trigger ABORT(STREAM_CLOSED)")

	client.mu.Lock()
	recvAbort := clientStreamImpl.recvAbort
	client.mu.Unlock()
	if recvAbort == nil {
		t.Fatal("recvAbort not set")
	}
	if recvAbort.Code != uint64(CodeStreamClosed) {
		t.Fatalf("recv abort code = %d, want %d", recvAbort.Code, uint64(CodeStreamClosed))
	}
}

func TestDuplicateResetIgnored(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(accepted)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	case serverStream = <-acceptCh:
	}

	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server read: %v", err)
	}

	if err := clientStream.CancelWrite(uint64(CodeCancelled)); err != nil {
		t.Fatalf("client reset: %v", err)
	}

	awaitStreamReadState(t, serverStream, testSignalTimeout, func(stream *nativeStream) bool {
		return stream.recvReset != nil
	}, "timed out waiting for first RESET")

	if err := server.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: serverStream.id,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("duplicate RESET err = %v, want nil", err)
	}

	server.mu.Lock()
	serverStreamRecvReset := serverStream.recvReset
	server.mu.Unlock()
	if serverStreamRecvReset == nil {
		t.Fatal("recvReset cleared after duplicate RESET")
	}
	if serverStreamRecvReset.Code != uint64(CodeCancelled) {
		t.Fatalf("recvReset code = %d, want %d", serverStreamRecvReset.Code, uint64(CodeCancelled))
	}
}

func TestDuplicateAbortIgnored(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(accepted)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	case serverStream = <-acceptCh:
	}

	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server read: %v", err)
	}

	if err := clientStream.CloseWithError(uint64(CodeRefusedStream), "no"); err != nil {
		t.Fatalf("client abort: %v", err)
	}

	awaitStreamReadState(t, serverStream, testSignalTimeout, func(stream *nativeStream) bool {
		return stream.recvAbort != nil
	}, "timed out waiting for first ABORT")

	if err := server.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: serverStream.id,
		Payload:  mustEncodeVarint(uint64(CodeRefusedStream)),
	}); err != nil {
		t.Fatalf("duplicate ABORT err = %v, want nil", err)
	}

	server.mu.Lock()
	serverStreamRecvAbort := serverStream.recvAbort
	server.mu.Unlock()
	if serverStreamRecvAbort == nil {
		t.Fatal("recvAbort cleared after duplicate ABORT")
	}
	if serverStreamRecvAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("recvAbort code = %d, want %d", serverStreamRecvAbort.Code, uint64(CodeRefusedStream))
	}
}

func TestUnderlyingTransportCloseUnblocksWait(t *testing.T) {
	t.Parallel()
	left, right := net.Pipe()

	type result struct {
		conn *Conn
		err  error
	}
	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)
	go func() { c, err := Client(left, nil); clientCh <- result{c, err} }()
	go func() { c, err := Server(right, nil); serverCh <- result{c, err} }()

	clientRes := <-clientCh
	serverRes := <-serverCh
	if clientRes.err != nil {
		t.Fatalf("client establish: %v", clientRes.err)
	}
	if serverRes.err != nil {
		t.Fatalf("server establish: %v", serverRes.err)
	}

	ctx, cancel := testContext(t)
	defer cancel()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- serverRes.conn.Wait(ctx)
	}()

	_ = left.Close()

	select {
	case err := <-waitCh:
		if err == nil {
			t.Fatal("server Wait returned nil after raw transport close")
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("server Wait did not unblock after raw transport close")
	}

	_ = clientRes.conn.io.conn.Close()
	_ = serverRes.conn.io.conn.Close()
}

func TestUnderlyingTransportCloseUnblocksAcceptStream(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := server.AcceptStream(ctx)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("AcceptStream returned early: %v", err)
	case <-time.After(testSignalTimeout / 5):
	}

	_ = client.io.conn.Close()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked AcceptStream did not return after raw transport close")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationAccept || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !server.Closed() {
		t.Fatal("server Closed() = false after raw transport close")
	}
}

func TestUnderlyingTransportCloseUnblocksBlockedRead(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	errCh := make(chan error, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			errCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(stream)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-errCh:
		t.Fatalf("AcceptStream err = %v, want nil", err)
	case serverStream = <-acceptCh:
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("timed out waiting for accepted stream")
	}
	if serverStream == nil {
		t.Fatal("accepted stream = nil, want stream")
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(serverStream, buf); err != nil {
		t.Fatalf("drain initial payload: %v", err)
	}

	readErrCh := make(chan error, 1)
	go func() {
		_, err := serverStream.Read(make([]byte, 1))
		readErrCh <- err
	}()
	awaitStreamReadWaiter(t, serverStream, testSignalTimeout, "blocked Read did not enter wait state before raw transport close")

	_ = client.io.conn.Close()

	var readErr error
	select {
	case readErr = <-readErrCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked Read did not return after raw transport close")
	}

	se := requireStructuredError(t, readErr)
	if se.Scope != ScopeSession || se.Operation != OperationRead || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !server.Closed() {
		t.Fatal("server Closed() = false after raw transport close")
	}
}

func TestUnderlyingTransportCloseUnblocksBlockedWrite(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)

	stream := newBlockedWriteCloseWakeStream(server, state.FirstLocalStreamID(server.config.negotiated.LocalRole, true))

	server.mu.Lock()
	server.flow.sendSessionMax = 1
	server.flow.sendSessionUsed = 1
	server.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		errCh <- err
	}()
	awaitStreamWriteWaiter(t, stream, testSignalTimeout, "blocked Write did not enter wait state before raw transport close")

	_ = client.io.conn.Close()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked Write did not return after raw transport close")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceTransport ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
	if !server.Closed() {
		t.Fatal("server Closed() = false after raw transport close")
	}
}

func TestPeerCloseTerminatesActiveStreamReads(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		s, _ := server.AcceptStream(ctx)
		acceptCh <- mustNativeStreamImpl(s)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if accepted := <-acceptCh; accepted == nil {
		t.Fatal("expected accepted stream")
	}

	readErrCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := clientStream.Read(buf)
		readErrCh <- err
	}()

	server.CloseWithError(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})

	var readErr error
	select {
	case readErr = <-readErrCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for blocked read to be unblocked by peer CLOSE")
	}

	var appErr *ApplicationError
	if !errors.As(readErr, &appErr) {
		t.Fatalf("read err = %v, want ApplicationError", readErr)
	}
	if appErr.Code != uint64(CodeInternal) || appErr.Reason != "bye" {
		t.Fatalf("read err = (%d, %q), want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeInternal), "bye")
	}

	if waitErr := client.Wait(ctx); !errors.As(waitErr, &appErr) || appErr.Code != uint64(CodeInternal) || appErr.Reason != "bye" {
		t.Fatalf("wait err = %v, want ApplicationError(%d, %q)", waitErr, uint64(CodeInternal), "bye")
	}
}

func TestSessionCloseClearsTombstones(t *testing.T) {
	t.Parallel()
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle first ABORT: %v", err)
	}

	c.mu.Lock()
	if got := len(c.registry.tombstones); got != 1 {
		c.mu.Unlock()
		t.Fatalf("tombstones before close = %d, want 1", got)
	}
	c.mu.Unlock()

	c.closeSession(ErrSessionClosed)

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := len(c.registry.tombstones); got != 0 {
		t.Fatalf("tombstones after close = %d, want 0", got)
	}
}

func TestCloseSessionReleasesStreamBuffersAndBudgets(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	streamA := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	streamA.idSet = true
	streamA.recvBuffer = 4
	streamA.readBuf = []byte("read")
	streamA.sendSent = 3
	c.registry.streams[streamA.id] = streamA

	streamB := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), streamArityUni, OpenOptions{}, nil)
	streamB.idSet = true
	streamB.recvBuffer = 2
	streamB.readBuf = []byte("xy")
	streamB.sendSent = 5
	c.registry.streams[streamB.id] = streamB

	provisional := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	provisional.sendSent = 7
	provisional.recvBuffer = 11
	provisional.readBuf = []byte("provisional")

	c.flow.sendSessionUsed = streamA.sendSent + streamB.sendSent + provisional.sendSent
	c.flow.recvSessionUsed = streamA.recvBuffer + streamB.recvBuffer + provisional.recvBuffer

	c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.flow.sendSessionUsed; got != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", got)
	}
	if got := c.flow.recvSessionUsed; got != 0 {
		t.Fatalf("recvSessionUsed = %d, want 0", got)
	}
	if streamA.sendSent != 0 {
		t.Fatalf("streamA sendSent = %d, want 0", streamA.sendSent)
	}
	if streamA.recvBuffer != 0 || len(streamA.readBuf) != 0 {
		t.Fatalf("streamA recvBuffer = %d, readBuf len = %d, want 0", streamA.recvBuffer, len(streamA.readBuf))
	}
	if streamB.sendSent != 0 {
		t.Fatalf("streamB sendSent = %d, want 0", streamB.sendSent)
	}
	if streamB.recvBuffer != 0 || len(streamB.readBuf) != 0 {
		t.Fatalf("streamB recvBuffer = %d, readBuf len = %d, want 0", streamB.recvBuffer, len(streamB.readBuf))
	}
	if provisional.sendSent != 0 {
		t.Fatalf("provisional sendSent = %d, want 0", provisional.sendSent)
	}
	if provisional.recvBuffer != 0 || len(provisional.readBuf) != 0 {
		t.Fatalf("provisional recvBuffer = %d, readBuf len = %d, want 0", provisional.recvBuffer, len(provisional.readBuf))
	}
	if got := len(c.registry.streams); got != 0 {
		t.Fatalf("stream count after close = %d, want 0", got)
	}
	if c.registry.streams != nil {
		t.Fatal("streams map not released after close")
	}
	if got := len(c.queues.provisionalBidi.items); got != 0 {
		t.Fatalf("provisionalBidi count after close = %d, want 0", got)
	}
}

func TestPeerCloseTerminatesWrites(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		s, _ := server.AcceptStream(ctx)
		acceptCh <- mustNativeStreamImpl(s)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	serverStream := <-acceptCh
	if serverStream == nil {
		t.Fatal("expected accepted stream")
	}

	server.CloseWithError(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})

	select {
	case <-client.lifecycle.closedCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for session close")
	}

	_, err = clientStream.Write([]byte("x"))
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("write err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeInternal) || appErr.Reason != "bye" {
		t.Fatalf("write err = (%d, %q), want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeInternal), "bye")
	}
}

func TestPeerCloseFrameTerminatesSessionAndStreamWrites(t *testing.T) {
	t.Parallel()
	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hello")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	payload := mustClosePayload(t, uint64(CodeProtocol), "bye")
	if err := client.handleCloseFrame(Frame{
		Type:     FrameTypeCLOSE,
		StreamID: 0,
		Payload:  payload,
	}); err == nil {
		t.Fatal("handle peer close err = nil, want non-nil")
	}

	select {
	case <-client.lifecycle.closedCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for client session close")
	}

	_, err = clientStream.Write([]byte("more"))
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("post-close stream write err = %v, want application error", err)
	}
	if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
		t.Fatalf("post-close stream write err = (%d, %q), want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeProtocol), "bye")
	}

	peerErr := client.PeerCloseError()
	if peerErr == nil {
		t.Fatal("expected peer close error")
	}
	if peerErr.Code != uint64(CodeProtocol) || peerErr.Reason != "bye" {
		t.Fatalf("peer close error = (%d, %q), want (%d, %q)", peerErr.Code, peerErr.Reason, uint64(CodeProtocol), "bye")
	}

	waitErr := client.Wait(ctx)
	if !errors.As(waitErr, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
		t.Fatalf("wait err = %v, want application error (%d, %q)", waitErr, uint64(CodeProtocol), "bye")
	}
}

func TestPeerCloseNoErrorCompletesSessionAndWakesBlockedAccept(t *testing.T) {
	t.Parallel()
	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptErrCh := make(chan error, 1)
	go func() {
		_, err := client.AcceptStream(ctx)
		acceptErrCh <- err
	}()

	payload := mustClosePayload(t, uint64(CodeNoError), "complete")
	if err := client.handleCloseFrame(Frame{
		Type:     FrameTypeCLOSE,
		StreamID: 0,
		Payload:  payload,
	}); err == nil {
		t.Fatal("handle peer close err = nil, want non-nil")
	}

	select {
	case <-client.lifecycle.closedCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for peer CLOSE")
	}

	if err := client.Wait(ctx); err != nil {
		t.Fatalf("wait after NO_ERROR close = %v, want nil", err)
	}

	select {
	case err := <-acceptErrCh:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("blocked accept err = %v, want ErrSessionClosed", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for blocked accept to return")
	}

	peerCloseErr := client.PeerCloseError()
	if peerCloseErr == nil {
		t.Fatal("expected peer close error")
	}
	if peerCloseErr.Code != uint64(CodeNoError) || peerCloseErr.Reason != "complete" {
		t.Fatalf("peer close error = (%d, %q), want (%d, %q)", peerCloseErr.Code, peerCloseErr.Reason, uint64(CodeNoError), "complete")
	}

	if _, err := client.OpenStream(ctx); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("OpenStream after session close = %v, want ErrSessionClosed", err)
	}
}

func TestOpenAfterCloseReturnsSessionError(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	server.CloseWithError(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})

	select {
	case <-client.lifecycle.closedCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for session close")
	}

	_, err := client.OpenStream(ctx)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("OpenStream err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeInternal) {
		t.Fatalf("OpenStream error code = %d, want %d", appErr.Code, uint64(CodeInternal))
	}
}

func TestBlockedAcceptUnblockedByPeerCloseFrameWithoutRuntimeClose(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	ctx, cancel := testContext(t)
	defer cancel()

	acceptErr := make(chan error, 1)
	go func() {
		_, err := c.AcceptStream(ctx)
		acceptErr <- err
	}()

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "protocol"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = %v, want non-nil", err)
	}

	select {
	case got := <-acceptErr:
		var appErr *ApplicationError
		if !errors.As(got, &appErr) || appErr.Code != uint64(CodeProtocol) {
			t.Fatalf("blocked accept err = %v, want ApplicationError(%d)", got, uint64(CodeProtocol))
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for blocked accept to return on peer close")
	}
}

func TestPeerCloseErrorIsVisibleFromOpenBeforeSessionClose(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "protocol"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = %v, want non-nil", err)
	}

	_, openErr := c.OpenStream(context.Background())
	var appErr *ApplicationError
	if !errors.As(openErr, &appErr) {
		t.Fatalf("OpenStream err = %v, want ApplicationError", openErr)
	}
	if appErr.Code != uint64(CodeProtocol) {
		t.Fatalf("OpenStream error code = %d, want %d", appErr.Code, uint64(CodeProtocol))
	}
}

func TestBlockedWriteUnblockedByPeerClose(t *testing.T) {
	serverCfg := DefaultConfig()
	serverCfg.Settings.InitialMaxData = 1
	serverCfg.Settings.InitialMaxStreamDataBidiPeerOpened = 1
	serverCfg.Settings.InitialMaxStreamDataBidiLocallyOpened = 1

	client, server := newConnPairWithConfig(t, nil, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	type writeResult struct {
		n   int
		err error
	}
	if n, err := stream.Write([]byte("x")); n != 1 || err != nil {
		t.Fatalf("initial write expected to succeed: err=%v", err)
	}

	done := make(chan writeResult, 1)
	writeStarted := make(chan struct{}, 1)
	go func() {
		writeStarted <- struct{}{}
		n, werr := stream.Write([]byte("y"))
		done <- writeResult{n: n, err: werr}
	}()
	select {
	case <-writeStarted:
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked write did not start before peer close")
	}
	select {
	case got := <-done:
		t.Fatalf("blocked write returned early: n=%d err=%v", got.n, got.err)
	case <-time.After(testSignalTimeout / 5):
	}

	server.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})

	var got writeResult
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for blocked write to wake")
	}
	var appErr *ApplicationError
	if got.err == nil {
		t.Fatalf("blocked write final err = nil, want application error")
	}
	if !errors.As(got.err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
		t.Fatalf("blocked write err = %v, want (%d, %q)", got.err, uint64(CodeProtocol), "bye")
	}
	if got.n != 0 {
		t.Fatalf("blocked write returned n=%d, want 0", got.n)
	}
}

func TestWaitUnblocksAfterPeerCloseFrameWithoutRuntimeClose(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "protocol"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = %v, want non-nil", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- c.Wait(context.Background())
	}()

	select {
	case got := <-done:
		var appErr *ApplicationError
		if !errors.As(got, &appErr) {
			t.Fatalf("Wait err = %v, want ApplicationError", got)
		}
		if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "protocol" {
			t.Fatalf("Wait err = (%d, %q), want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeProtocol), "protocol")
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Wait did not return after peer CLOSE")
	}
}

func TestWriteFailsAfterPeerCloseFrameBeforeSessionClose(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	c.registry.streams[streamID] = stream

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "protocol"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = %v, want non-nil", err)
	}

	n, got := stream.Write([]byte("x"))
	if n != 0 {
		t.Fatalf("write n = %d, want 0", n)
	}
	var appErr *ApplicationError
	if !errors.As(got, &appErr) {
		t.Fatalf("write err = %v, want ApplicationError", got)
	}
	if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "protocol" {
		t.Fatalf("write err = (%d, %q), want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeProtocol), "protocol")
	}
}

func TestReadFailsAfterPeerCloseFrameBeforeSessionClose(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	c.registry.streams[streamID] = stream

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "protocol"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = %v, want non-nil", err)
	}

	n, got := stream.Read(make([]byte, 1))
	if n != 0 {
		t.Fatalf("read n = %d, want 0", n)
	}
	var appErr *ApplicationError
	if !errors.As(got, &appErr) {
		t.Fatalf("read err = %v, want ApplicationError", got)
	}
	if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "protocol" {
		t.Fatalf("read err = (%d, %q), want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeProtocol), "protocol")
	}
}

func TestStreamTerminalOpsReturnPeerCloseErrorWhenSessionClosedWithoutRuntimeClose(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	c.registry.streams[streamID] = stream

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "protocol"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = %v, want non-nil", err)
	}

	ops := []struct {
		name string
		fn   func() error
	}{
		{
			name: "CloseWrite",
			fn:   stream.CloseWrite,
		},
		{
			name: "CloseRead",
			fn:   stream.CloseRead,
		},
		{
			name: "Reset",
			fn:   func() error { return stream.CancelWrite(uint64(CodeProtocol)) },
		},
		{
			name: "CloseWithError",
			fn:   func() error { return stream.CloseWithError(uint64(CodeProtocol), "protocol") },
		},
		{
			name: "Close",
			fn:   stream.Close,
		},
	}

	for _, op := range ops {
		got := op.fn()
		var appErr *ApplicationError
		if !errors.As(got, &appErr) {
			t.Fatalf("%s err = %v, want ApplicationError", op.name, got)
		}
		if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "protocol" {
			t.Fatalf("%s err = (%d, %q), want (%d, %q)", op.name, appErr.Code, appErr.Reason, uint64(CodeProtocol), "protocol")
		}
	}
	assertNoQueuedFrame(t, frames)
}

func TestPeerCloseClearsActiveStateAndPendingControl(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	stream.sendSent = 7
	stream.recvBuffer = 3
	stream.readBuf = []byte("abc")
	c.registry.streams[streamID] = stream

	provisionalBidi := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	provisionalBidi.sendSent = 11
	provisionalBidi.recvBuffer = 5
	provisionalBidi.readBuf = []byte("pending")

	provisionalUni := c.newProvisionalLocalStreamLocked(streamArityUni, OpenOptions{}, nil)
	provisionalUni.sendSent = 13
	provisionalUni.recvBuffer = 7
	provisionalUni.readBuf = []byte("queued")

	accept := c.newLocalStreamWithIDLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	accept.idSet = true
	accept.applicationVisible = true
	c.enqueueAcceptedLocked(accept)
	acceptUni := c.newLocalStreamWithIDLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, false), streamArityUni, OpenOptions{}, nil)
	acceptUni.applicationVisible = true
	c.enqueueAcceptedLocked(acceptUni)

	testSetPendingStreamMaxData(c, stream.id, 9)
	testSetPendingStreamBlocked(c, stream.id, 4)
	testSetPendingPriorityUpdate(c, stream.id, []byte{1, 2, 3})
	c.pending.sessionBlocked = 3
	c.pending.hasSessionBlocked = true
	c.pending.sessionMaxData = 1024
	c.pending.hasSessionMaxData = true

	c.liveness.keepaliveInterval = time.Second
	c.liveness.keepaliveMaxPingInterval = 2 * time.Second
	c.liveness.readIdlePingDueAt = time.Now().Add(time.Second)
	c.liveness.writeIdlePingDueAt = time.Now().Add(time.Second)
	c.liveness.maxPingDueAt = time.Now().Add(2 * time.Second)
	c.liveness.pingOutstanding = true
	c.liveness.pingPayload = []byte("ping")
	c.liveness.lastPingSentAt = time.Now()
	c.liveness.lastPongAt = time.Now()
	c.liveness.lastPingRTT = time.Millisecond
	c.liveness.pingDone = make(chan struct{})
	c.mu.Unlock()

	err := c.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeProtocol), "protocol"),
	})
	if err == nil {
		t.Fatalf("handle peer close err = %v, want non-nil", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionControl.peerCloseErr == nil {
		t.Fatalf("peer close error not recorded")
	}
	if c.lifecycle.closeErr == nil {
		t.Fatalf("closeErr = nil, want non-nil")
	}
	if c.lifecycle.sessionState != connStateFailed {
		t.Fatalf("sessionState = %d, want %d", c.lifecycle.sessionState, connStateFailed)
	}
	if len(c.registry.streams) != 0 {
		t.Fatalf("streams count = %d, want 0", len(c.registry.streams))
	}
	if c.registry.streams != nil {
		t.Fatal("streams map not released after peer CLOSE")
	}
	if len(c.queues.provisionalBidi.items) != 0 {
		t.Fatalf("provisionalBidi count = %d, want 0", len(c.queues.provisionalBidi.items))
	}
	if len(c.queues.provisionalUni.items) != 0 {
		t.Fatalf("provisionalUni count = %d, want 0", len(c.queues.provisionalUni.items))
	}
	if len(c.queues.acceptBidi.items) != 0 {
		t.Fatalf("acceptBidi count = %d, want 0", len(c.queues.acceptBidi.items))
	}
	if len(c.queues.acceptUni.items) != 0 {
		t.Fatalf("acceptUni count = %d, want 0", len(c.queues.acceptUni.items))
	}
	if testPendingStreamMaxDataCount(c) != 0 {
		t.Fatalf("pendingStreamMaxData count = %d, want 0", testPendingStreamMaxDataCount(c))
	}
	if testPendingStreamBlockedCount(c) != 0 {
		t.Fatalf("pendingStreamBlocked count = %d, want 0", testPendingStreamBlockedCount(c))
	}
	if testPendingPriorityUpdateCount(c) != 0 {
		t.Fatalf("pendingPriorityUpdate count = %d, want 0", testPendingPriorityUpdateCount(c))
	}
	if c.pending.sessionBlocked != 0 || c.pending.hasSessionBlocked {
		t.Fatalf("pendingSessionBlocked = %d/%t, want 0/false", c.pending.sessionBlocked, c.pending.hasSessionBlocked)
	}
	if c.pending.sessionMaxData != 0 || c.pending.hasSessionMaxData {
		t.Fatalf("pendingSessionMaxData = %d/%t, want 0/false", c.pending.sessionMaxData, c.pending.hasSessionMaxData)
	}
	if c.liveness.keepaliveInterval != 0 {
		t.Fatalf("keepaliveInterval = %v, want 0", c.liveness.keepaliveInterval)
	}
	if c.liveness.keepaliveMaxPingInterval != 0 {
		t.Fatalf("keepaliveMaxPingInterval = %v, want 0", c.liveness.keepaliveMaxPingInterval)
	}
	if !c.liveness.readIdlePingDueAt.IsZero() || !c.liveness.writeIdlePingDueAt.IsZero() || !c.liveness.maxPingDueAt.IsZero() {
		t.Fatalf("keepalive due state = (%v, %v, %v), want zero", c.liveness.readIdlePingDueAt, c.liveness.writeIdlePingDueAt, c.liveness.maxPingDueAt)
	}
	if c.liveness.pingOutstanding {
		t.Fatalf("pingOutstanding = %t, want false", c.liveness.pingOutstanding)
	}
	if len(c.liveness.pingPayload) != 0 {
		t.Fatalf("pingPayload len = %d, want 0", len(c.liveness.pingPayload))
	}
	if !c.liveness.lastPingSentAt.IsZero() {
		t.Fatalf("lastPingSentAt = %v, want zero", c.liveness.lastPingSentAt)
	}
	if !c.liveness.lastPongAt.IsZero() {
		t.Fatalf("lastPongAt = %v, want zero", c.liveness.lastPongAt)
	}
	if c.liveness.lastPingRTT != 0 {
		t.Fatalf("lastPingRTT = %v, want 0", c.liveness.lastPingRTT)
	}
	if c.liveness.pingDone != nil {
		t.Fatalf("pingDone not nil")
	}
	if stream.sendSent != 0 || stream.recvBuffer != 0 || len(stream.readBuf) != 0 {
		t.Fatalf("stream resources not released = sendSent=%d recvBuffer=%d readBuf=%d", stream.sendSent, stream.recvBuffer, len(stream.readBuf))
	}
	if provisionalBidi.sendSent != 0 || provisionalBidi.recvBuffer != 0 || len(provisionalBidi.readBuf) != 0 {
		t.Fatalf("provisional bidi resources not released = sendSent=%d recvBuffer=%d readBuf=%d", provisionalBidi.sendSent, provisionalBidi.recvBuffer, len(provisionalBidi.readBuf))
	}
	if provisionalUni.sendSent != 0 || provisionalUni.recvBuffer != 0 || len(provisionalUni.readBuf) != 0 {
		t.Fatalf("provisional uni resources not released = sendSent=%d recvBuffer=%d readBuf=%d", provisionalUni.sendSent, provisionalUni.recvBuffer, len(provisionalUni.readBuf))
	}
	select {
	case <-c.lifecycle.closedCh:
	default:
		t.Fatalf("closedCh not closed by peer CLOSE")
	}
}

func TestRecordAbuseCountLockedSaturatesInsteadOfWrapping(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	now := time.Now()
	windowStart := windowStampAt(now)
	count := ^uint32(0) - 1

	err := c.recordAbuseCountLocked(now, &windowStart, &count, ^uint32(0)-1, "test", "overflow")
	if err == nil {
		t.Fatal("recordAbuseCountLocked() err = nil, want threshold error after saturated increment")
	}
	if count != ^uint32(0) {
		t.Fatalf("count = %d, want saturated max uint32", count)
	}
}

func TestRecordTrafficBudgetLockedSaturatesByteCounter(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	now := time.Now()
	windowStart := windowStampAt(now)
	frames := ^uint32(0) - 1
	byteCount := ^uint64(0) - 2

	err := c.recordTrafficBudgetLocked(now, &windowStart, &frames, &byteCount, 0, ^uint64(0)-1, 4, "test", "overflow")
	if err == nil {
		t.Fatal("recordTrafficBudgetLocked() err = nil, want byte-budget error after saturated add")
	}
	if frames != ^uint32(0) {
		t.Fatalf("frames = %d, want saturated max uint32", frames)
	}
	if byteCount != ^uint64(0) {
		t.Fatalf("bytes = %d, want saturated max uint64", byteCount)
	}
}

func TestInboundControlByteBudgetLockedSaturatesLargePayload(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.config.local.Settings.MaxControlPayloadBytes = (^uint64(0) / 2) + 1

	if got := c.inboundControlByteBudgetLocked(); got != ^uint64(0) {
		t.Fatalf("inboundControlByteBudgetLocked() = %d, want saturated max uint64", got)
	}
}

func TestInboundExtByteBudgetLockedSaturatesLargePayload(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.config.local.Settings.MaxExtensionPayloadBytes = (^uint64(0) / 2) + 1

	if got := c.inboundExtByteBudgetLocked(); got != ^uint64(0) {
		t.Fatalf("inboundExtByteBudgetLocked() = %d, want saturated max uint64", got)
	}
}

func newConfigPolicyTestConn(cfg *Config) *Conn {
	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	cloned := cloneConfig(cfg)
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals: connRuntimeSignalState{acceptCh: make(chan struct{}, 1)},
		writer:  connWriterRuntimeState{writeCh: make(chan writeRequest, 1), advisoryWriteCh: make(chan writeRequest, 1), urgentWriteCh: make(chan writeRequest, 1)},

		observer:  connObserverState{eventHandler: cloned.EventHandler},
		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		liveness: connLivenessState{
			keepaliveInterval: cloned.KeepaliveInterval,
			keepaliveTimeout:  cloned.KeepaliveTimeout,
		},
		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(settings.MaxFramePayload),
		}, config: connConfigState{local: Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			peer:       Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, Capabilities: 0, LocalRole: localRole, PeerRole: peerRole, PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: settings.InitialMaxData,
			sendSessionMax: settings.InitialMaxData}, registry: connRegistryState{streams: make(map[uint64]*nativeStream),
			tombstones:     make(map[uint64]streamTombstone),
			usedStreamData: make(map[uint64]usedStreamMarker),

			nextLocalBidi: state.FirstLocalStreamID(localRole, true),
			nextLocalUni:  state.FirstLocalStreamID(localRole, false),
			nextPeerBidi:  state.FirstPeerStreamID(localRole, true),
			nextPeerUni:   state.FirstPeerStreamID(localRole, false)},
	}
	c.applyConfigRuntimePolicy(cloned)
	return c
}

func TestConfigRuntimePolicyOverridesApplied(t *testing.T) {
	clientCfg := &Config{
		SessionMemoryCap:                 12345,
		PerStreamQueuedDataHWM:           111,
		SessionQueuedDataHWM:             222,
		UrgentQueuedBytesCap:             333,
		StopSendingGracefulDrainWindow:   334 * time.Millisecond,
		StopSendingGracefulTailCap:       335,
		PendingControlBytesBudget:        444,
		PendingPriorityBytesBudget:       555,
		RetainedOpenInfoBytesBudget:      5,
		RetainedPeerReasonBytesBudget:    6,
		AggregateLateDataCap:             777,
		AcceptBacklogLimit:               8,
		AcceptBacklogBytesLimit:          999,
		TombstoneLimit:                   10,
		MarkerOnlyUsedStreamLimit:        11,
		AbuseWindow:                      11 * time.Second,
		HiddenAbortChurnWindow:           12 * time.Second,
		HiddenAbortChurnThreshold:        13,
		VisibleTerminalChurnWindow:       14 * time.Second,
		VisibleTerminalChurnThreshold:    15,
		InboundControlFrameBudget:        16,
		InboundControlBytesBudget:        17,
		InboundExtFrameBudget:            18,
		InboundExtBytesBudget:            19,
		InboundMixedFrameBudget:          20,
		InboundMixedBytesBudget:          21,
		NoOpControlFloodThreshold:        22,
		NoOpMaxDataFloodThreshold:        23,
		NoOpBlockedFloodThreshold:        24,
		NoOpZeroDataFloodThreshold:       25,
		NoOpPriorityUpdateFloodThreshold: 26,
		GroupRebucketChurnThreshold:      27,
		InboundPingFloodThreshold:        28,
	}

	client := newConfigPolicyTestConn(clientCfg)

	client.mu.Lock()
	defer client.mu.Unlock()

	if got := client.sessionMemoryHardCapLocked(); got != clientCfg.SessionMemoryCap {
		t.Fatalf("sessionMemoryHardCapLocked() = %d, want %d", got, clientCfg.SessionMemoryCap)
	}
	if got := client.perStreamDataHWMLocked(); got != clientCfg.PerStreamQueuedDataHWM {
		t.Fatalf("perStreamDataHWMLocked() = %d, want %d", got, clientCfg.PerStreamQueuedDataHWM)
	}
	if got := client.sessionDataHWMLocked(); got != clientCfg.SessionQueuedDataHWM {
		t.Fatalf("sessionDataHWMLocked() = %d, want %d", got, clientCfg.SessionQueuedDataHWM)
	}
	if got := client.urgentLaneCapLocked(); got != clientCfg.UrgentQueuedBytesCap {
		t.Fatalf("urgentLaneCapLocked() = %d, want %d", got, clientCfg.UrgentQueuedBytesCap)
	}
	if got := client.terminalPolicy.stopSendingDrainWindow; got != clientCfg.StopSendingGracefulDrainWindow {
		t.Fatalf("stopSendingDrainWindow = %v, want %v", got, clientCfg.StopSendingGracefulDrainWindow)
	}
	if got := client.terminalPolicy.stopSendingTailCap; got != clientCfg.StopSendingGracefulTailCap {
		t.Fatalf("stopSendingTailCap = %d, want %d", got, clientCfg.StopSendingGracefulTailCap)
	}
	if got := client.pendingControlBudgetLocked(); got != clientCfg.PendingControlBytesBudget {
		t.Fatalf("pendingControlBudgetLocked() = %d, want %d", got, clientCfg.PendingControlBytesBudget)
	}
	if got := client.pendingPriorityBudgetLocked(); got != clientCfg.PendingPriorityBytesBudget {
		t.Fatalf("pendingPriorityBudgetLocked() = %d, want %d", got, clientCfg.PendingPriorityBytesBudget)
	}
	if got := client.retainedOpenInfoBudgetLocked(); got != clientCfg.RetainedOpenInfoBytesBudget {
		t.Fatalf("retainedOpenInfoBudgetLocked() = %d, want %d", got, clientCfg.RetainedOpenInfoBytesBudget)
	}
	if got := client.retainedPeerReasonBudgetLocked(); got != clientCfg.RetainedPeerReasonBytesBudget {
		t.Fatalf("retainedPeerReasonBudgetLocked() = %d, want %d", got, clientCfg.RetainedPeerReasonBytesBudget)
	}
	if got := client.ingress.aggregateLateDataCap; got != clientCfg.AggregateLateDataCap {
		t.Fatalf("aggregateLateDataCap = %d, want %d", got, clientCfg.AggregateLateDataCap)
	}
	if got := client.visibleAcceptBacklogHardCapLocked(); got != clientCfg.AcceptBacklogLimit {
		t.Fatalf("visibleAcceptBacklogHardCapLocked() = %d, want %d", got, clientCfg.AcceptBacklogLimit)
	}
	if got := client.visibleAcceptBacklogBytesHardCapLocked(); got != clientCfg.AcceptBacklogBytesLimit {
		t.Fatalf("visibleAcceptBacklogBytesHardCapLocked() = %d, want %d", got, clientCfg.AcceptBacklogBytesLimit)
	}
	if got := client.registry.tombstoneLimit; got != clientCfg.TombstoneLimit {
		t.Fatalf("tombstoneLimit = %d, want %d", got, clientCfg.TombstoneLimit)
	}
	if got := client.markerOnlyHardCapLocked(); got != clientCfg.MarkerOnlyUsedStreamLimit {
		t.Fatalf("markerOnlyHardCapLocked() = %d, want %d", got, clientCfg.MarkerOnlyUsedStreamLimit)
	}
	if got := client.abuseWindowLocked(); got != clientCfg.AbuseWindow {
		t.Fatalf("abuseWindowLocked() = %v, want %v", got, clientCfg.AbuseWindow)
	}
	if got := client.hiddenAbortChurnWindowLocked(); got != clientCfg.HiddenAbortChurnWindow {
		t.Fatalf("hiddenAbortChurnWindowLocked() = %v, want %v", got, clientCfg.HiddenAbortChurnWindow)
	}
	if got := client.hiddenAbortChurnThresholdLocked(); got != clientCfg.HiddenAbortChurnThreshold {
		t.Fatalf("hiddenAbortChurnThresholdLocked() = %d, want %d", got, clientCfg.HiddenAbortChurnThreshold)
	}
	if got := client.visibleChurnWindowLocked(); got != clientCfg.VisibleTerminalChurnWindow {
		t.Fatalf("visibleChurnWindowLocked() = %v, want %v", got, clientCfg.VisibleTerminalChurnWindow)
	}
	if got := client.visibleChurnThresholdLocked(); got != clientCfg.VisibleTerminalChurnThreshold {
		t.Fatalf("visibleChurnThresholdLocked() = %d, want %d", got, clientCfg.VisibleTerminalChurnThreshold)
	}
	if got := client.inboundControlFrameBudgetLocked(); got != clientCfg.InboundControlFrameBudget {
		t.Fatalf("inboundControlFrameBudgetLocked() = %d, want %d", got, clientCfg.InboundControlFrameBudget)
	}
	if got := client.inboundControlByteBudgetLocked(); got != clientCfg.InboundControlBytesBudget {
		t.Fatalf("inboundControlByteBudgetLocked() = %d, want %d", got, clientCfg.InboundControlBytesBudget)
	}
	if got := client.inboundExtFrameBudgetLocked(); got != clientCfg.InboundExtFrameBudget {
		t.Fatalf("inboundExtFrameBudgetLocked() = %d, want %d", got, clientCfg.InboundExtFrameBudget)
	}
	if got := client.inboundExtByteBudgetLocked(); got != clientCfg.InboundExtBytesBudget {
		t.Fatalf("inboundExtByteBudgetLocked() = %d, want %d", got, clientCfg.InboundExtBytesBudget)
	}
	if got := client.inboundMixedFrameBudgetLocked(); got != clientCfg.InboundMixedFrameBudget {
		t.Fatalf("inboundMixedFrameBudgetLocked() = %d, want %d", got, clientCfg.InboundMixedFrameBudget)
	}
	if got := client.inboundMixedByteBudgetLocked(); got != clientCfg.InboundMixedBytesBudget {
		t.Fatalf("inboundMixedByteBudgetLocked() = %d, want %d", got, clientCfg.InboundMixedBytesBudget)
	}
	if got := client.noOpControlFloodThresholdLocked(); got != clientCfg.NoOpControlFloodThreshold {
		t.Fatalf("noOpControlFloodThresholdLocked() = %d, want %d", got, clientCfg.NoOpControlFloodThreshold)
	}
	if got := client.noOpMaxDataFloodThresholdLocked(); got != clientCfg.NoOpMaxDataFloodThreshold {
		t.Fatalf("noOpMaxDataFloodThresholdLocked() = %d, want %d", got, clientCfg.NoOpMaxDataFloodThreshold)
	}
	if got := client.noOpBlockedFloodThresholdLocked(); got != clientCfg.NoOpBlockedFloodThreshold {
		t.Fatalf("noOpBlockedFloodThresholdLocked() = %d, want %d", got, clientCfg.NoOpBlockedFloodThreshold)
	}
	if got := client.noOpZeroDataFloodThresholdLocked(); got != clientCfg.NoOpZeroDataFloodThreshold {
		t.Fatalf("noOpZeroDataFloodThresholdLocked() = %d, want %d", got, clientCfg.NoOpZeroDataFloodThreshold)
	}
	if got := client.noOpPriorityUpdateFloodThresholdLocked(); got != clientCfg.NoOpPriorityUpdateFloodThreshold {
		t.Fatalf("noOpPriorityUpdateFloodThresholdLocked() = %d, want %d", got, clientCfg.NoOpPriorityUpdateFloodThreshold)
	}
	if got := client.groupRebucketChurnThresholdLocked(); got != clientCfg.GroupRebucketChurnThreshold {
		t.Fatalf("groupRebucketChurnThresholdLocked() = %d, want %d", got, clientCfg.GroupRebucketChurnThreshold)
	}
	if got := client.inboundPingFloodThresholdLocked(); got != clientCfg.InboundPingFloodThreshold {
		t.Fatalf("inboundPingFloodThresholdLocked() = %d, want %d", got, clientCfg.InboundPingFloodThreshold)
	}
}

func TestConfigQueueOverridesAffectBackpressure(t *testing.T) {
	clientCfg := &Config{
		PerStreamQueuedDataHWM: 4,
		SessionQueuedDataHWM:   8,
		UrgentQueuedBytesCap:   5,
	}

	client := newConfigPolicyTestConn(clientCfg)
	stream := &nativeStream{
		conn:        client,
		id:          state.FirstLocalStreamID(client.config.negotiated.LocalRole, true),
		idSet:       true,
		localSend:   true,
		writeNotify: make(chan struct{}, 1),
	}

	client.mu.Lock()
	client.registry.streams[stream.id] = stream
	client.flow.queuedDataBytes = 4
	if !client.writeQueueBlockedLocked(stream, 5, 5) {
		client.mu.Unlock()
		t.Fatal("expected session queued-data override to block additional ordinary write bytes")
	}
	client.flow.queuedDataBytes = 1
	stream.queuedDataBytes = 2
	if !client.writeQueueBlockedLocked(stream, 3, 3) {
		client.mu.Unlock()
		t.Fatal("expected per-stream queued-data override to block additional ordinary write bytes")
	}

	tooLargeFirst := &writeRequest{queuedBytes: clientCfg.UrgentQueuedBytesCap + 1}
	tooLargeFirstReserve := client.reserveUrgentQueueLocked(tooLargeFirst)
	if tooLargeFirstReserve.memoryErr != nil {
		client.mu.Unlock()
		t.Fatalf("reserve oversized first urgent request err = %v, want nil", tooLargeFirstReserve.memoryErr)
	}
	if !tooLargeFirstReserve.blocked() {
		client.mu.Unlock()
		t.Fatal("expected first urgent request beyond the configured cap to block")
	}

	first := &writeRequest{queuedBytes: clientCfg.UrgentQueuedBytesCap}
	firstReserve := client.reserveUrgentQueueLocked(first)
	if firstReserve.blocked() || firstReserve.memoryErr != nil {
		client.mu.Unlock()
		t.Fatalf("reserve first urgent request = (%v, %v), want (false, nil)", firstReserve.blocked(), firstReserve.memoryErr)
	}
	second := &writeRequest{queuedBytes: 1}
	secondReserve := client.reserveUrgentQueueLocked(second)
	client.mu.Unlock()
	if secondReserve.memoryErr != nil {
		t.Fatalf("reserve second urgent request err = %v, want nil", secondReserve.memoryErr)
	}
	if !secondReserve.blocked() {
		t.Fatal("expected urgent queue override to block bytes beyond the configured cap")
	}
	client.releaseUrgentQueueReservation(first)
}

func TestConfigReasonAndPendingBudgetOverridesApply(t *testing.T) {
	clientCfg := &Config{
		SessionMemoryCap:              1024,
		PendingControlBytesBudget:     2,
		PendingPriorityBytesBudget:    3,
		RetainedPeerReasonBytesBudget: 2,
	}

	client := newConfigPolicyTestConn(clientCfg)

	client.mu.Lock()
	defer client.mu.Unlock()

	got, n := client.retainPeerReasonLocked(0, "abcd")
	if got != "ab" || n != 2 {
		t.Fatalf("retainPeerReasonLocked() = (%q, %d), want (%q, %d)", got, n, "ab", 2)
	}
	if client.setPendingSessionControlLocked(sessionControlMaxData, 1<<60) {
		t.Fatal("expected oversized pending MAX_DATA update to be rejected by configured control budget")
	}

	client.queuePriorityUpdateAsync(1, []byte("abcd"), retainedBytesBorrowed)
	if testHasPendingPriorityUpdate(client, 1) {
		t.Fatal("priority update larger than configured advisory budget should not be retained")
	}

	ensureTestPendingStream(client, 1)
	client.queuePriorityUpdateAsync(1, []byte("abc"), retainedBytesBorrowed)
	if got := client.pending.priorityBytes; got != 3 {
		t.Fatalf("pendingPriorityBytes = %d, want 3", got)
	}
}

func TestConfigMixedInboundBudgetOverrideTriggersEarly(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		InboundControlFrameBudget: 100,
		InboundExtFrameBudget:     100,
		InboundMixedFrameBudget:   2,
	}))

	pong := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	ext := Frame{Type: FrameTypeEXT, Payload: mustEncodeVarint(99)}

	if err := c.handleFrame(pong); err != nil {
		t.Fatalf("first inbound control frame err = %v, want nil", err)
	}
	if err := c.handleFrame(ext); err != nil {
		t.Fatalf("second inbound EXT frame err = %v, want nil", err)
	}
	if err := c.handleFrame(pong); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("final inbound mixed control/EXT frame err = %v, want %s", err, CodeProtocol)
	}
}

func TestConfigPingFloodThresholdOverrideTriggersEarly(t *testing.T) {
	c, frames, stop := newInvalidPolicyConn(t)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		InboundPingFloodThreshold: 1,
	}))

	ping := Frame{Type: FrameTypePING, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
	if err := c.handleFrame(ping); err != nil {
		t.Fatalf("first inbound PING err = %v, want nil", err)
	}
	if queued := awaitQueuedFrame(t, frames); queued.Type != FrameTypePONG {
		t.Fatalf("queued frame = %+v, want PONG", queued)
	}
	if err := c.handleFrame(ping); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("second inbound PING err = %v, want %s", err, CodeProtocol)
	}
}

func TestConfigGroupRebucketThresholdOverrideTriggersEarly(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		GroupRebucketChurnThreshold: 1,
	}))
	c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityStreamGroups
	c.config.peer.Settings.SchedulerHints = SchedulerGroupFair
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	firstPayload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(1)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build first group update payload: %v", err)
	}
	secondPayload, err := buildPriorityUpdatePayload(c.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(2)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		t.Fatalf("build second group update payload: %v", err)
	}

	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: firstPayload}); err != nil {
		t.Fatalf("first group update err = %v, want nil", err)
	}
	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: secondPayload}); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("second group update err = %v, want %s", err, CodeProtocol)
	}
}

func TestConfigHiddenAbortChurnOverrideTriggersEarly(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		HiddenAbortChurnThreshold: 1,
		HiddenAbortChurnWindow:    time.Hour,
	}))

	start := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: start,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("first hidden ABORT err = %v, want nil", err)
	}
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: start + 4,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("second hidden ABORT err = %v, want %s", err, CodeProtocol)
	}
}

func TestConfigStopSendingTailCapOverrideAffectsConvergence(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		StopSendingGracefulTailCap: 32,
	}))

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 64
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), streamID, CodeCancelled)
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want reset once configured graceful tail cap is below the unavoidable tail")
	}
}

func TestConfigStopSendingDrainWindowOverrideAffectsConvergence(t *testing.T) {
	c, frames, stop := newDelayedStopSendingPolicyConn(t, 20*time.Millisecond)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		StopSendingGracefulDrainWindow: 5 * time.Millisecond,
	}))

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 64
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), streamID, CodeCancelled)
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want reset once configured STOP_SENDING drain window times out before writer admission")
	}
}

func newDelayedStopSendingPolicyConn(t *testing.T, delay time.Duration) (*Conn, <-chan Frame, func()) {
	t.Helper()

	frames := make(chan Frame, 64)
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	flushDone := make(chan struct{})
	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals: connRuntimeSignalState{acceptCh: make(chan struct{}, 1)},
		writer: connWriterRuntimeState{
			writeCh:         make(chan writeRequest),
			urgentWriteCh:   make(chan writeRequest),
			advisoryWriteCh: make(chan writeRequest),
		},

		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(settings.MaxFramePayload),
		}, config: connConfigState{local: Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			peer:       Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, Capabilities: 0, LocalRole: localRole, PeerRole: peerRole, PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: settings.InitialMaxData,
			sendSessionMax: settings.InitialMaxData}, registry: connRegistryState{streams: make(map[uint64]*nativeStream),
			tombstones:     make(map[uint64]streamTombstone),
			usedStreamData: make(map[uint64]usedStreamMarker),

			nextLocalBidi: state.FirstLocalStreamID(localRole, true),
			nextLocalUni:  state.FirstLocalStreamID(localRole, false),
			nextPeerBidi:  state.FirstPeerStreamID(localRole, true),
			nextPeerUni:   state.FirstPeerStreamID(localRole, false)},
	}

	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stop:
				return
			case req := <-c.writer.urgentWriteCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				if req.done != nil {
					req.done <- nil
				}
			case <-time.After(delay):
				select {
				case <-stop:
					return
				case req := <-c.writer.writeCh:
					for _, frame := range req.frames {
						frames <- testPublicFrame(frame)
					}
					if req.done != nil {
						req.done <- nil
					}
				default:
				}
			}
		}
	}()
	go func() {
		defer close(flushDone)
		for {
			select {
			case <-stop:
				return
			case <-c.lifecycle.closedCh:
				return
			case <-c.pending.controlNotify:
				c.mu.Lock()
				result := takePendingTerminalControlRequestForTest(c)
				c.mu.Unlock()
				if result.err != nil {
					c.closeSession(result.err)
					return
				}
				if !result.hasRequest() {
					continue
				}
				select {
				case <-stop:
					return
				case <-c.lifecycle.closedCh:
					return
				case c.writer.urgentWriteCh <- result.request:
				}
			}
		}
	}()

	return c, frames, func() {
		select {
		case <-c.lifecycle.closedCh:
		default:
			close(c.lifecycle.closedCh)
		}
		close(stop)
		select {
		case <-writerDone:
		case <-time.After(testSignalTimeout):
			t.Fatal("delayed stop-sending writer consumer did not exit")
		}
		select {
		case <-flushDone:
		case <-time.After(testSignalTimeout):
			t.Fatal("delayed stop-sending control flush loop did not exit")
		}
	}
}

const (
	testSignalTimeout      = time.Second
	testQueuedFrameTimeout = 2 * testSignalTimeout
	testCloseWakeTimeout   = testSignalTimeout
	testFrameBufferCap     = 1024
)

func newConnPair(t *testing.T) (*Conn, *Conn) {
	return newConnPairWithConfig(t, nil, nil)
}

func newConnPairWithConfig(t *testing.T, clientCfg, serverCfg *Config) (*Conn, *Conn) {
	t.Helper()

	left, right := net.Pipe()
	type result struct {
		conn *Conn
		err  error
	}
	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)

	go func() { c, err := Client(left, clientCfg); clientCh <- result{c, err} }()
	go func() { c, err := Server(right, serverCfg); serverCh <- result{c, err} }()

	client := <-clientCh
	server := <-serverCh

	if client.err != nil {
		t.Fatalf("client establish: %v", client.err)
	}
	if server.err != nil {
		t.Fatalf("server establish: %v", server.err)
	}
	t.Cleanup(func() {
		_ = client.conn.Close()
		_ = server.conn.Close()
	})

	return client.conn, server.conn
}

func TestEstablishedConnBootstrapsIncrementalQueues(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)

	for _, conn := range []*Conn{client, server} {
		conn.mu.Lock()
		if !conn.queues.provisionalBidi.init || !conn.queues.provisionalUni.init {
			conn.mu.Unlock()
			t.Fatal("provisional queues were not bootstrapped on established conn")
		}
		if !conn.queues.acceptBidi.init || !conn.queues.acceptUni.init {
			conn.mu.Unlock()
			t.Fatal("accept queues were not bootstrapped on established conn")
		}
		if !conn.queues.unseenLocalBidi.init || !conn.queues.unseenLocalUni.init {
			conn.mu.Unlock()
			t.Fatal("unseen-local queues were not bootstrapped on established conn")
		}
		if !conn.registry.tombstonesInit || !conn.registry.hiddenTombstonesInit {
			conn.mu.Unlock()
			t.Fatal("tombstone queues were not bootstrapped on established conn")
		}
		if !conn.pending.streamQueueState(pendingStreamQueueMaxData).init || !conn.pending.streamQueueState(pendingStreamQueueBlocked).init {
			conn.mu.Unlock()
			t.Fatal("pending control queues were not bootstrapped on established conn")
		}
		if !conn.pending.streamQueueState(pendingStreamQueuePriority).init {
			conn.mu.Unlock()
			t.Fatal("pending advisory queue was not bootstrapped on established conn")
		}
		conn.mu.Unlock()
	}
}

func testContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 2*time.Second)
}

type acceptStreamResult struct {
	stream *nativeStream
	err    error
}

func acceptStreamAsync(ctx context.Context, conn *Conn) <-chan acceptStreamResult {
	ch := make(chan acceptStreamResult, 1)
	go func() {
		stream, err := conn.AcceptStream(ctx)
		ch <- acceptStreamResult{stream: mustNativeStreamImpl(stream), err: err}
	}()
	return ch
}

func requireAcceptedStream(t *testing.T, ch <-chan acceptStreamResult) *nativeStream {
	t.Helper()

	accepted := <-ch
	if accepted.err != nil {
		t.Fatalf("accept: %v", accepted.err)
	}
	if accepted.stream == nil {
		t.Fatal("expected accepted stream")
	}
	return accepted.stream
}

type acceptUniStreamResult struct {
	stream *nativeRecvStream
	err    error
}

func acceptUniStreamAsync(ctx context.Context, conn *Conn) <-chan acceptUniStreamResult {
	ch := make(chan acceptUniStreamResult, 1)
	go func() {
		stream, err := conn.AcceptUniStream(ctx)
		ch <- acceptUniStreamResult{stream: mustNativeRecvStreamImpl(stream), err: err}
	}()
	return ch
}

func requireAcceptedUniStream(t *testing.T, ch <-chan acceptUniStreamResult) *nativeRecvStream {
	t.Helper()

	accepted := <-ch
	if accepted.err != nil {
		t.Fatalf("accept uni: %v", accepted.err)
	}
	if accepted.stream == nil {
		t.Fatal("expected accepted uni stream")
	}
	return accepted.stream
}

func awaitRequestDone(t *testing.T, ch <-chan error, msg string) error {
	t.Helper()

	ctx, cancel := testContext(t)
	defer cancel()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		t.Fatal(msg)
		return nil
	}
}

func awaitStreamReadState(t *testing.T, stream *nativeStream, timeout time.Duration, shouldMatch func(*nativeStream) bool, msg string) {
	t.Helper()
	if stream == nil || stream.conn == nil {
		t.Fatal("awaitStreamReadState: nil stream")
	}
	deadline := time.Now().Add(timeout)
	for {
		stream.conn.mu.Lock()
		if shouldMatch(stream) {
			stream.conn.mu.Unlock()
			return
		}
		if stream.readNotify == nil {
			stream.readNotify = make(chan struct{}, 1)
		}
		readNotify := stream.readNotify
		stream.conn.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatal(msg)
		}

		select {
		case <-readNotify:
		case <-time.After(remaining):
			t.Fatal(msg)
		}
	}
}

func awaitStreamWriteState(t *testing.T, stream *nativeStream, timeout time.Duration, shouldMatch func(*nativeStream) bool, msg string) {
	t.Helper()
	if stream == nil || stream.conn == nil {
		t.Fatal("awaitStreamWriteState: nil stream")
	}
	deadline := time.Now().Add(timeout)
	for {
		stream.conn.mu.Lock()
		if shouldMatch(stream) {
			stream.conn.mu.Unlock()
			return
		}
		if stream.writeNotify == nil {
			stream.writeNotify = make(chan struct{}, 1)
		}
		writeNotify := stream.writeNotify
		stream.conn.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatal(msg)
		}

		select {
		case <-writeNotify:
		case <-time.After(remaining):
			t.Fatal(msg)
		}
	}
}

func awaitStreamReadWaiter(t *testing.T, stream *nativeStream, timeout time.Duration, msg string) {
	t.Helper()
	if stream == nil {
		t.Fatal("awaitStreamReadWaiter: nil stream")
	}
	deadline := time.Now().Add(timeout)
	for {
		if stream.loadReadWaiters() > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		runtime.Gosched()
	}
}

func awaitStreamWriteWaiter(t *testing.T, stream *nativeStream, timeout time.Duration, msg string) {
	t.Helper()
	if stream == nil {
		t.Fatal("awaitStreamWriteWaiter: nil stream")
	}
	deadline := time.Now().Add(timeout)
	for {
		if stream.loadWriteWaiters() > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		runtime.Gosched()
	}
}

func awaitConnState(t *testing.T, conn *Conn, timeout time.Duration, shouldMatch func(*Conn) bool, msg string) {
	t.Helper()
	if conn == nil {
		t.Fatal("awaitConnState: nil conn")
	}

	deadline := time.Now().Add(timeout)
	for {
		if shouldMatch(conn) {
			return
		}

		conn.mu.Lock()
		livenessCh := conn.ensureLivenessNotifyLocked()
		controlNotify := conn.pending.controlNotify
		closedCh := conn.lifecycle.closedCh
		conn.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatal(msg)
		}

		pollInterval := 5 * time.Millisecond
		if remaining < pollInterval {
			pollInterval = remaining
		}

		select {
		case <-livenessCh:
		case <-controlNotify:
		case <-closedCh:
			if shouldMatch(conn) {
				return
			}
			conn.mu.Lock()
			err := conn.lifecycle.closeErr
			conn.mu.Unlock()
			if err != nil {
				t.Fatalf("%s (session closed: %v)", msg, err)
			}
			t.Fatal(msg)
		case <-time.After(pollInterval):
		}
	}
}

func awaitPeerGoAwayBidi(t *testing.T, conn *Conn, want uint64) {
	t.Helper()
	awaitConnState(t, conn, 4*testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		seen := c.sessionControl.peerGoAwayBidi
		c.mu.Unlock()
		return seen == want
	}, "expected connection to observe peer GOAWAY watermark")
}

func invalidUint64Ptr(v uint64) *uint64 {
	return &v
}

func takePendingTerminalControlRequestForTest(c *Conn) pendingWriteRequestResult {
	if c == nil || !c.pendingTerminalControlHasEntriesLocked() {
		return pendingWriteRequestResult{}
	}

	maxBytes := c.urgentLaneCapLocked()
	prevTracked := c.trackedSessionMemoryLocked()
	var frameBuf [maxWriteBatchFrames]txFrame
	collector := newPendingTxFrameCollector(frameBuf[:0], maxWriteBatchFrames, maxBytes)
	released := c.visitPendingTerminalControlTxFramesLocked(&collector.stopped, &collector)

	if collector.empty() {
		c.notifySessionMemoryReleasedLocked(prevTracked, sessionMemoryReleaseFrom(released))
		return pendingWriteRequestResult{}
	}
	if err := c.sessionMemoryCapErrorWithAdditionalLocked("queue urgent terminal control", collector.queuedBytes); err != nil {
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

func newHandlerTestConnWithOptions(t *testing.T, autoFlushTerminalControl bool) (*Conn, chan Frame, func()) {
	t.Helper()

	frames := make(chan Frame, testFrameBufferCap)
	stop := make(chan struct{})
	var stopOnce sync.Once
	writerDone := make(chan struct{})
	flushDone := make(chan struct{})
	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1), terminalNotify: make(chan struct{}, 1)},
		signals: connRuntimeSignalState{acceptCh: make(chan struct{}, 1)},
		writer: connWriterRuntimeState{
			writeCh:         make(chan writeRequest),
			urgentWriteCh:   make(chan writeRequest),
			advisoryWriteCh: make(chan writeRequest),
		},

		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(settings.MaxFramePayload),
		}, config: connConfigState{local: Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			peer:       Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, Capabilities: 0, LocalRole: localRole, PeerRole: peerRole, PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: settings.InitialMaxData,
			sendSessionMax: settings.InitialMaxData}, registry: connRegistryState{streams: make(map[uint64]*nativeStream),
			tombstones:     make(map[uint64]streamTombstone),
			usedStreamData: make(map[uint64]usedStreamMarker),

			nextLocalBidi: state.FirstLocalStreamID(localRole, true),
			nextLocalUni:  state.FirstLocalStreamID(localRole, false),
			nextPeerBidi:  state.FirstPeerStreamID(localRole, true),
			nextPeerUni:   state.FirstPeerStreamID(localRole, false)},
	}
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stop:
				return
			case req := <-c.writer.writeCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				if req.done != nil {
					req.done <- nil
				}
			case req := <-c.writer.urgentWriteCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				if req.done != nil {
					req.done <- nil
				}
			}
		}
	}()
	if autoFlushTerminalControl {
		go func() {
			defer close(flushDone)
			for {
				select {
				case <-stop:
					return
				case <-c.lifecycle.closedCh:
					return
				case <-c.pending.terminalNotify:
					c.mu.Lock()
					result := takePendingTerminalControlRequestForTest(c)
					c.mu.Unlock()
					if result.err != nil {
						c.closeSession(result.err)
						return
					}
					if !result.hasRequest() {
						continue
					}
					select {
					case <-stop:
						return
					case <-c.lifecycle.closedCh:
						return
					case c.writer.urgentWriteCh <- result.request:
					}
				}
			}
		}()
	} else {
		close(flushDone)
	}
	return c, frames, func() {
		select {
		case <-c.lifecycle.closedCh:
		default:
			close(c.lifecycle.closedCh)
		}
		stopOnce.Do(func() { close(stop) })
		select {
		case <-writerDone:
		case <-time.After(testSignalTimeout):
			t.Fatal("handler test writer consumer did not exit")
		}
		select {
		case <-flushDone:
		case <-time.After(testSignalTimeout):
			t.Fatal("handler test control flush loop did not exit")
		}
	}
}

func newHandlerTestConn(t *testing.T) (*Conn, chan Frame, func()) {
	t.Helper()
	return newHandlerTestConnWithOptions(t, true)
}

func (c *Conn) flushPendingControlBatches() (fatal bool, err error) {
	if c == nil {
		return false, nil
	}
	for {
		c.mu.Lock()
		result := c.takePendingControlWriteRequestLocked()
		c.mu.Unlock()
		if result.err != nil {
			return true, result.err
		}
		if !result.hasRequest() {
			return false, nil
		}
		req := result.request
		prepareWriteRequestForSend(&req)
		switch {
		case req.urgentReserved:
			c.writer.urgentWriteCh <- req
		case req.advisoryReserved:
			c.writer.advisoryWriteCh <- req
		default:
			c.writer.writeCh <- req
		}
	}
}

func (c *Conn) controlFlushLoop() {
	for {
		select {
		case <-c.lifecycle.closedCh:
			return
		case <-c.pending.controlNotify:
			fatal, err := c.flushPendingControlBatches()
			if err != nil {
				if fatal {
					c.closeSession(err)
				}
				return
			}
		}
	}
}

func configureTestConnHandshakeLocked(c *Conn, localRole, peerRole Role, capabilities Capabilities) {
	if c == nil {
		return
	}

	settings := DefaultSettings()
	c.config.local = Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings}
	c.config.peer = Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings}
	c.config.negotiated = Negotiated{
		Proto:        ProtoVersion,
		Capabilities: capabilities,
		LocalRole:    localRole,
		PeerRole:     peerRole,
		PeerSettings: settings,
	}
	c.registry.nextLocalBidi = state.FirstLocalStreamID(localRole, true)
	c.registry.nextLocalUni = state.FirstLocalStreamID(localRole, false)
	c.registry.nextPeerBidi = state.FirstPeerStreamID(localRole, true)
	c.registry.nextPeerUni = state.FirstPeerStreamID(localRole, false)
	c.flow.recvSessionAdvertised = settings.InitialMaxData
	c.flow.sendSessionMax = settings.InitialMaxData
	c.sessionControl.peerGoAwayBidi = MaxVarint62
	c.sessionControl.peerGoAwayUni = MaxVarint62
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	c.ingress.aggregateLateDataCap = aggregateLateDataCapFor(settings.MaxFramePayload)
}

func newInvalidFrameConn(t *testing.T, capabilities Capabilities) (*Conn, <-chan Frame, func()) {
	t.Helper()

	c, frames, stop := newHandlerTestConn(t)
	c.mu.Lock()
	configureTestConnHandshakeLocked(c, RoleResponder, RoleInitiator, capabilities)
	c.mu.Unlock()
	return c, frames, stop
}

func awaitQueuedFrame(t *testing.T, frames <-chan Frame) Frame {
	t.Helper()

	select {
	case frame := <-frames:
		return frame
	case <-time.After(testQueuedFrameTimeout):
		t.Fatal("timed out waiting for queued frame")
		return Frame{}
	}
}

func assertNoQueuedFrame(t *testing.T, frames <-chan Frame) {
	t.Helper()

	select {
	case frame := <-frames:
		t.Fatalf("unexpected queued frame: %+v", frame)
	default:
	}
}

func assertQueuedResetCode(t *testing.T, queued any, streamID uint64, code ErrorCode) {
	t.Helper()

	var frame Frame
	switch v := queued.(type) {
	case Frame:
		frame = v
	case <-chan Frame:
		frame = awaitQueuedFrame(t, v)
	default:
		t.Fatalf("assertQueuedResetCode: unexpected type %T", queued)
	}

	if frame.Type != FrameTypeRESET {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeRESET)
	}
	if frame.StreamID != streamID {
		t.Fatalf("queued frame stream-id = %d, want %d", frame.StreamID, streamID)
	}
	codeValue, _, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse queued RESET payload: %v", err)
	}
	if ErrorCode(codeValue) != code {
		t.Fatalf("queued RESET code = %d, want %d", codeValue, code)
	}
}

func assertInvalidQueuedAbortCode(t *testing.T, frames <-chan Frame, streamID uint64, code ErrorCode) {
	t.Helper()

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeABORT || frame.StreamID != streamID {
		t.Fatalf("queued frame = %+v, want ABORT on stream %d", frame, streamID)
	}
	want := mustEncodeVarint(uint64(code))
	if string(frame.Payload) != string(want) {
		t.Fatalf("ABORT payload = %x, want %x", frame.Payload, want)
	}
}

func assertLocallyAbortedStream(t *testing.T, c *Conn, streamID uint64, codeName string) {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.registry.streams[streamID]; ok {
		t.Fatalf("stream %d remained live after local %s abort", streamID, codeName)
	}

	tombstone, ok := c.registry.tombstones[streamID]
	if !ok {
		t.Fatalf("stream %d missing tombstone after local %s abort", streamID, codeName)
	}
	if tombstone.TerminalKind != state.TerminalKindAborted {
		t.Fatalf("stream %d tombstone kind = %v, want %v", streamID, tombstone.TerminalKind, state.TerminalKindAborted)
	}
	if !tombstone.HasTerminalCode {
		t.Fatalf("stream %d tombstone missing terminal code after local %s abort", streamID, codeName)
	}
	if got := ErrorCode(tombstone.TerminalCode).String(); got != codeName {
		t.Fatalf("stream %d tombstone code = %q, want %q", streamID, got, codeName)
	}
}

func ensureTestPendingStream(c *Conn, streamID uint64) *nativeStream {
	if c == nil || streamID == 0 {
		return nil
	}
	if c.registry.streams == nil {
		c.registry.streams = make(map[uint64]*nativeStream)
	}
	if stream := c.registry.streams[streamID]; streamMatchesID(stream, streamID) {
		if stream.conn == nil {
			stream.conn = c
		}
		return stream
	}
	return testVisibleBidiStream(c, streamID)
}

func testSetPendingStreamMaxData(c *Conn, streamID, v uint64) bool {
	if ensureTestPendingStream(c, streamID) == nil {
		return false
	}
	return c.setPendingStreamControlLocked(streamControlMaxData, streamID, v)
}

func testSetPendingStreamBlocked(c *Conn, streamID, v uint64) bool {
	stream := ensureTestPendingStream(c, streamID)
	if stream == nil {
		return false
	}
	stream.markBlockedState(v)
	return c.setPendingStreamControlLocked(streamControlBlocked, streamID, v)
}

func testSetPendingPriorityUpdate(c *Conn, streamID uint64, payload []byte) uint64 {
	if ensureTestPendingStream(c, streamID) == nil {
		return 0
	}
	return c.replacePendingPriorityUpdateLocked(streamID, payload, retainedBytesOwned)
}

func testPendingStreamMaxDataValue(c *Conn, streamID uint64) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	value := c.pendingStreamControlValueLocked(streamControlMaxData, streamID)
	return value.value, value.present
}

func testPendingStreamBlockedValue(c *Conn, streamID uint64) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	value := c.pendingStreamControlValueLocked(streamControlBlocked, streamID)
	return value.value, value.present
}

func testPendingPriorityUpdatePayload(c *Conn, streamID uint64) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	stream := c.pendingPriorityTargetStreamLocked(streamID)
	if stream == nil || stream.pending.flags&streamPendingPriorityUpdate == 0 || len(stream.pending.priority) == 0 {
		return nil, false
	}
	return stream.pending.priority, true
}

func testPendingStreamMaxDataCount(c *Conn) int {
	if c == nil {
		return 0
	}
	return c.pendingStreamControlCountLocked(streamControlMaxData)
}

func testPendingStreamBlockedCount(c *Conn) int {
	if c == nil {
		return 0
	}
	return c.pendingStreamControlCountLocked(streamControlBlocked)
}

func testPendingPriorityUpdateCount(c *Conn) int {
	if c == nil {
		return 0
	}
	c.ensurePendingPriorityUpdateQueueLocked()
	return c.pendingPriorityQueueCountLocked()
}

func testHasPendingStreamMaxData(c *Conn, streamID uint64) bool {
	_, ok := testPendingStreamMaxDataValue(c, streamID)
	return ok
}

func testHasPendingStreamBlocked(c *Conn, streamID uint64) bool {
	_, ok := testPendingStreamBlockedValue(c, streamID)
	return ok
}

func testHasPendingPriorityUpdate(c *Conn, streamID uint64) bool {
	_, ok := testPendingPriorityUpdatePayload(c, streamID)
	return ok
}

func mustEncodeVarint(v uint64) []byte {
	b, err := EncodeVarint(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestTrackedSessionMemoryIncludesRetainedStateResidency(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.queues.provisionalBidi.items = []*nativeStream{{}}
	c.queues.acceptBidi.items = []*nativeStream{{}}
	c.registry.tombstones = map[uint64]streamTombstone{
		1: {Hidden: true},
		2: {},
	}
	c.registry.tombstoneOrder = []uint64{1, 2}
	c.registry.usedStreamData = map[uint64]usedStreamMarker{
		1: {action: lateDataAbortClosed},
		2: {action: lateDataAbortClosed},
		3: {action: lateDataAbortClosed},
	}
	unit := c.retainedStateUnitLocked()
	compactUnit := c.compactTerminalStateUnitLocked()
	got := c.trackedSessionMemoryLocked()
	c.mu.Unlock()

	want := saturatingAdd(saturatingMul(3, unit), saturatingMul(2, compactUnit))
	if got != want {
		t.Fatalf("trackedSessionMemoryLocked() = %d, want %d", got, want)
	}
}

func TestEnforceVisibleAcceptBacklogShedsNewestUnderTrackedMemoryCap(t *testing.T) {
	c := newSessionMemoryTestConn()
	newest := testBuildDetachedStream(c, 8, testWithBidi(), testWithEnqueued(2))
	oldest := testBuildDetachedStream(c, 4, testWithBidi(), testWithEnqueued(1))

	c.mu.Lock()
	c.queues.acceptBidi.items = []*nativeStream{oldest, newest}
	c.flow.sessionMemoryCap = c.retainedStateUnitLocked()
	refused := c.enforceVisibleAcceptBacklogLocked()
	remaining := len(c.queues.acceptBidi.items)
	left := c.queues.acceptBidi.items[0]
	c.mu.Unlock()

	if len(refused) != 1 || refused[0] != newest.id {
		t.Fatalf("refused = %#v, want newest stream %d", refused, newest.id)
	}
	if remaining != 1 || left != oldest {
		t.Fatalf("remaining accept backlog = %#v, want only oldest stream", c.queues.acceptBidi)
	}
}

func TestCheckLocalOpenPossibleLimitedByTrackedMemoryCap(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.sessionMemoryCap = c.retainedStateUnitLocked()
	stream := &nativeStream{}
	stream.setProvisionalCreated(time.Now())
	c.queues.provisionalBidi.items = []*nativeStream{stream}
	err := c.checkLocalOpenPossibleLocked(streamArityBidi)
	c.mu.Unlock()

	if !errors.Is(err, ErrOpenLimited) {
		t.Fatalf("checkLocalOpenPossibleLocked err = %v, want %v", err, ErrOpenLimited)
	}
}

func TestCheckLocalOpenPossibleRefusesWhenPeerIncomingBidiSlotsAreExhausted(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.config.peer.Settings.MaxIncomingStreamsBidi = 1
	c.registry.activeLocalBidi = 1
	err := c.checkLocalOpenPossibleLocked(streamArityBidi)
	c.mu.Unlock()

	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("checkLocalOpenPossibleLocked err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("checkLocalOpenPossibleLocked code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}
}

func TestCheckLocalOpenPossibleCountsPendingProvisionalsAgainstPeerIncomingLimit(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.config.peer.Settings.MaxIncomingStreamsBidi = 1
	stream := &nativeStream{}
	stream.setProvisionalCreated(time.Now())
	c.queues.provisionalBidi.items = []*nativeStream{stream}
	err := c.checkLocalOpenPossibleLocked(streamArityBidi)
	c.mu.Unlock()

	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("checkLocalOpenPossibleLocked err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("checkLocalOpenPossibleLocked code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}
}

func TestMaybeFinalizePeerActiveLockedReleasesLocalActiveSlots(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := testOpenedBidiStream(c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))
	testMarkLocalOpenVisible(stream)
	stream.markActiveCounted()
	stream.setSendFin()
	stream.setRecvFin()

	c.mu.Lock()
	c.registry.activeLocalBidi = 1
	c.maybeFinalizePeerActiveLocked(stream)
	active := c.registry.activeLocalBidi
	counted := stream.activeCountedFlag()
	c.mu.Unlock()

	if active != 0 {
		t.Fatalf("activeLocalBidi after finalize = %d, want 0", active)
	}
	if counted {
		t.Fatal("local stream remained active-counted after finalize")
	}
}

func TestEnforceHiddenControlStateBudgetShedsHiddenStateUnderTrackedMemoryCap(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.sessionMemoryCap = c.retainedStateUnitLocked()
	c.registry.tombstones = map[uint64]streamTombstone{
		1: {Hidden: true, CreatedAt: time.Now()},
		2: {Hidden: true, CreatedAt: time.Now()},
	}
	c.registry.tombstoneOrder = []uint64{1, 2}
	c.enforceHiddenControlStateBudgetLocked(time.Now())
	retained := c.hiddenControlStateRetainedLocked()
	_, hasNewest := c.registry.tombstones[2]
	c.mu.Unlock()

	if retained != 1 {
		t.Fatalf("hiddenControlStateRetainedLocked() = %d, want 1", retained)
	}
	if hasNewest {
		t.Fatal("newest hidden tombstone should have been shed under tracked session memory pressure")
	}
}

func TestReapExcessTombstonesShedsOldestVisibleTombstoneUnderTrackedMemoryCap(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.sessionMemoryCap = c.compactTerminalStateUnitLocked()
	c.registry.tombstones = map[uint64]streamTombstone{
		4: {},
		8: {},
	}
	c.registry.tombstoneOrder = []uint64{4, 8}
	c.registry.usedStreamData = map[uint64]usedStreamMarker{
		4: {action: lateDataAbortClosed},
		8: {action: lateDataAbortClosed},
	}
	c.reapExcessTombstonesLocked()
	_, hasOldest := c.registry.tombstones[4]
	_, hasNewest := c.registry.tombstones[8]
	_, keptMarker := c.registry.usedStreamData[4]
	c.mu.Unlock()

	if hasOldest {
		t.Fatal("oldest visible tombstone should have been reaped under tracked session memory pressure")
	}
	if !hasNewest {
		t.Fatal("newest visible tombstone should remain after tracked-memory reap trims back to cap")
	}
	if !keptMarker {
		t.Fatal("reaped visible tombstone should preserve its used-stream marker")
	}
}

func TestCompactTerminalStateClosesSessionWhenMarkerOnlyMemoryExceedsCap(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := testOpenedBidiStream(c, 4, testWithApplicationVisible())
	stream.setSendFin()
	stream.setRecvFin()

	c.mu.Lock()
	c.flow.sessionMemoryCap = c.compactTerminalStateUnitLocked() - 1
	c.maybeCompactTerminalLocked(stream)
	c.mu.Unlock()

	select {
	case <-c.lifecycle.closedCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for session close after non-sheddable terminal bookkeeping exceeded cap")
	}

	if err := c.err(); err == nil {
		t.Fatal("expected session close error after terminal bookkeeping exceeded tracked memory cap")
	}
}

func TestMarkerOnlyUsedStreamLimitOverrideClosesSessionWhenEntriesExceeded(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		MarkerOnlyUsedStreamLimit: 1,
	}))

	c.mu.Lock()
	c.registry.usedStreamData = map[uint64]usedStreamMarker{
		4: {action: lateDataAbortClosed, cause: lateDataCauseCloseRead},
		8: {action: lateDataAbortState, cause: lateDataCauseAbort},
	}
	c.enforceTerminalBookkeepingMemoryCapLocked()
	c.mu.Unlock()

	select {
	case <-c.lifecycle.closedCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for session close after marker-only entry limit was exceeded")
	}

	if !IsErrorCode(c.err(), CodeInternal) {
		t.Fatalf("stored session err = %v, want %s", c.err(), CodeInternal)
	}
}

func TestCompactMarkerOnlyRangesAccountMergedRangeAsSingleEntry(t *testing.T) {
	c := newSessionMemoryTestConn()
	marker := usedStreamMarker{action: lateDataAbortClosed, cause: lateDataCauseReset}

	c.mu.Lock()
	c.flow.sessionMemoryCap = c.compactTerminalStateUnitLocked()
	c.registry.usedStreamData = make(map[uint64]usedStreamMarker)
	for i := 0; i < 64; i++ {
		c.registry.usedStreamData[4+uint64(i)*4] = marker
	}
	c.compactMarkerOnlyRangesLocked()
	mapReleased := c.registry.usedStreamData == nil
	rangeMode := c.registry.usedStreamRangeMode
	retained := c.markerOnlyRetainedLocked()
	tracked := c.trackedRetainedStateMemoryLocked()
	capErr := c.markerOnlyCapErrorLocked("test marker-only ranges")
	got, ok := c.usedStreamMarkerForLocked(4 + 63*4)
	c.mu.Unlock()

	if !mapReleased {
		t.Fatal("usedStreamData map retained empty backing after marker-only range compaction")
	}
	if !rangeMode {
		t.Fatal("usedStreamRangeMode = false, want true after compaction")
	}
	if retained != 1 {
		t.Fatalf("markerOnlyRetainedLocked() = %d, want 1 merged range entry", retained)
	}
	if tracked != c.compactTerminalStateUnitLocked() {
		t.Fatalf("tracked retained marker memory = %d, want %d", tracked, c.compactTerminalStateUnitLocked())
	}
	if capErr != nil {
		t.Fatalf("markerOnlyCapErrorLocked() = %v, want nil for one merged range entry", capErr)
	}
	if !ok || !sameUsedStreamMarker(got, marker) {
		t.Fatalf("used marker lookup = (%+v,%v), want %+v,true", got, ok, marker)
	}
}

func TestRangeModeMarkUsedStreamDropsStaleMapEntry(t *testing.T) {
	c := newSessionMemoryTestConn()
	streamID := uint64(4)
	marker := usedStreamMarker{action: lateDataAbortState, cause: lateDataCauseAbort}

	c.mu.Lock()
	c.registry.usedStreamRangeMode = true
	c.registry.usedStreamRanges = []usedStreamRange{{
		start:  streamID,
		end:    streamID,
		marker: usedStreamMarker{action: lateDataAbortClosed, cause: lateDataCauseCloseRead},
	}}
	c.registry.usedStreamData = map[uint64]usedStreamMarker{
		streamID: {action: lateDataAbortClosed, cause: lateDataCauseCloseRead},
	}
	c.markUsedStreamLocked(streamID, marker)
	_, staleMapEntry := c.registry.usedStreamData[streamID]
	mapReleased := c.registry.usedStreamData == nil
	retained := c.markerOnlyRetainedLocked()
	got, ok := c.usedStreamMarkerForLocked(streamID)
	c.mu.Unlock()

	if staleMapEntry {
		t.Fatal("range-mode mark retained stale usedStreamData entry")
	}
	if !mapReleased {
		t.Fatal("range-mode mark retained empty usedStreamData map")
	}
	if retained != 1 {
		t.Fatalf("markerOnlyRetainedLocked() = %d, want 1 range without duplicate map entry", retained)
	}
	if !ok || !sameUsedStreamMarker(got, marker) {
		t.Fatalf("used marker lookup = (%+v,%v), want %+v,true", got, ok, marker)
	}
}

func TestTrackedSessionMemoryIncludesRetainedOpenInfoBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.retention.retainedOpenInfoBytes = 5
	got := c.trackedSessionMemoryLocked()
	c.mu.Unlock()

	if got != 5 {
		t.Fatalf("trackedSessionMemoryLocked() = %d, want 5", got)
	}
}

func TestCheckLocalOpenPossibleWithOpenInfoLimitedByOpenInfoBudget(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.retention.retainedOpenInfoBudget = 4
	c.retention.retainedOpenInfoBytes = 3
	err := c.checkLocalOpenPossibleWithOpenInfoLocked(streamArityBidi, 2)
	c.mu.Unlock()

	if !errors.Is(err, ErrOpenLimited) {
		t.Fatalf("checkLocalOpenPossibleWithOpenInfoLocked err = %v, want %v", err, ErrOpenLimited)
	}
}

func TestSetStreamOpenInfoRetainsBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := &nativeStream{conn: c}

	c.mu.Lock()
	changed := c.setStreamOpenInfoLocked(stream, []byte("ssh"))
	gotBytes := c.retention.retainedOpenInfoBytes
	gotInfo := string(stream.openInfo)
	c.mu.Unlock()

	if !changed {
		t.Fatal("setStreamOpenInfoLocked() = false, want true")
	}
	if gotBytes != 3 {
		t.Fatalf("retainedOpenInfoBytes = %d, want 3", gotBytes)
	}
	if gotInfo != "ssh" {
		t.Fatalf("stream.openInfo = %q, want %q", gotInfo, "ssh")
	}
}

func TestSetStreamOpenInfoTightensOversizedBackingArray(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := &nativeStream{conn: c}
	stream.openInfo = make([]byte, 3, 64)
	copy(stream.openInfo, "ssh")

	c.mu.Lock()
	c.retention.retainedOpenInfoBytes = uint64(len(stream.openInfo))
	changed := c.setStreamOpenInfoLocked(stream, []byte("ssh"))
	gotLen := len(stream.openInfo)
	gotCap := cap(stream.openInfo)
	gotBytes := c.retention.retainedOpenInfoBytes
	c.mu.Unlock()

	if changed {
		t.Fatal("setStreamOpenInfoLocked() = true, want false for unchanged logical content")
	}
	if gotLen != 3 {
		t.Fatalf("len(stream.openInfo) = %d, want 3", gotLen)
	}
	if gotCap != gotLen {
		t.Fatalf("cap(stream.openInfo) = %d, want tight cap %d", gotCap, gotLen)
	}
	if gotBytes != uint64(gotLen) {
		t.Fatalf("retainedOpenInfoBytes = %d, want %d", gotBytes, gotLen)
	}
}

func TestNewProvisionalLocalStreamOwnedTightensOversizedRetainedBackings(t *testing.T) {
	c := newSessionMemoryTestConn()

	openInfoSrc := make([]byte, 3, 64)
	copy(openInfoSrc, "ssh")
	prefixSrc := make([]byte, 4, 64)
	copy(prefixSrc, "meta")

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamOwnedLocked(streamArityBidi, OpenOptions{OpenInfo: openInfoSrc}, prefixSrc)
	gotInfoLen := len(stream.openInfo)
	gotInfoCap := cap(stream.openInfo)
	gotPrefixLen := len(stream.openMetadataPrefix)
	gotPrefixCap := cap(stream.openMetadataPrefix)
	gotRetained := c.retention.retainedOpenInfoBytes
	infoAliased := gotInfoLen > 0 && &stream.openInfo[0] == &openInfoSrc[0]
	prefixAliased := gotPrefixLen > 0 && &stream.openMetadataPrefix[0] == &prefixSrc[0]
	c.mu.Unlock()

	if gotInfoCap != gotInfoLen {
		t.Fatalf("cap(stream.openInfo) = %d, want tight cap %d", gotInfoCap, gotInfoLen)
	}
	if gotPrefixCap != gotPrefixLen {
		t.Fatalf("cap(stream.openMetadataPrefix) = %d, want tight cap %d", gotPrefixCap, gotPrefixLen)
	}
	if gotRetained != uint64(gotInfoLen) {
		t.Fatalf("retainedOpenInfoBytes = %d, want %d", gotRetained, gotInfoLen)
	}
	if infoAliased {
		t.Fatal("stream.openInfo aliased oversized caller backing array")
	}
	if prefixAliased {
		t.Fatal("stream.openMetadataPrefix aliased oversized owned backing array")
	}

	openInfoSrc[0] = 'x'
	prefixSrc[0] = 'x'

	c.mu.Lock()
	gotInfo := string(stream.openInfo)
	gotPrefix := string(stream.openMetadataPrefix)
	c.mu.Unlock()

	if gotInfo != "ssh" {
		t.Fatalf("stream.openInfo = %q, want %q", gotInfo, "ssh")
	}
	if gotPrefix != "meta" {
		t.Fatalf("stream.openMetadataPrefix = %q, want %q", gotPrefix, "meta")
	}
}

func TestNewProvisionalLocalStreamOwnedReusesTightOwnedPrefix(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.config.negotiated.Capabilities = CapabilityOpenMetadata

	prefix, err := buildOpenMetadataPrefix(CapabilityOpenMetadata, OpenOptions{OpenInfo: []byte("ssh")}, c.config.peer.Settings.MaxFramePayload)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamOwnedLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, prefix)
	aliased := len(prefix) > 0 && len(stream.openMetadataPrefix) > 0 && &stream.openMetadataPrefix[0] == &prefix[0]
	c.mu.Unlock()

	if !aliased {
		t.Fatal("stream.openMetadataPrefix did not reuse tight owned prefix backing array")
	}
}

func TestUpdateMetadataTightensOversizedOpenMetadataPrefixBacking(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.config.negotiated.Capabilities = CapabilityOpenMetadata | CapabilityPriorityHints

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, nil)
	oversizedPrefix := make([]byte, 3, 64)
	copy(oversizedPrefix, "old")
	stream.openMetadataPrefix = oversizedPrefix
	c.mu.Unlock()

	priority := uint64(7)
	if err := stream.UpdateMetadata(MetadataUpdate{Priority: &priority}); err != nil {
		t.Fatalf("UpdateMetadata: %v", err)
	}

	wantPrefix, err := buildOpenMetadataPrefix(
		c.config.negotiated.Capabilities,
		OpenOptions{InitialPriority: &priority, OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	c.mu.Lock()
	gotPrefix := clonePayloadBytes(stream.openMetadataPrefix)
	gotCap := cap(stream.openMetadataPrefix)
	aliased := len(stream.openMetadataPrefix) > 0 && &stream.openMetadataPrefix[0] == &oversizedPrefix[0]
	c.mu.Unlock()

	if string(gotPrefix) != string(wantPrefix) {
		t.Fatalf("openMetadataPrefix = %x, want %x", gotPrefix, wantPrefix)
	}
	if gotCap != len(gotPrefix) {
		t.Fatalf("cap(stream.openMetadataPrefix) = %d, want tight cap %d", gotCap, len(gotPrefix))
	}
	if aliased {
		t.Fatal("UpdateMetadata reused oversized openMetadataPrefix backing array")
	}
}

func TestCompactTerminalReleasesOpenInfoBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := testOpenedBidiStream(
		c,
		4,
		testWithApplicationVisible(),
		testWithOpenInfo([]byte("ssh")),
		testWithOpenMetadataPrefix([]byte("meta")),
	)
	stream.setSendFin()
	stream.setRecvFin()

	c.mu.Lock()
	c.retention.retainedOpenInfoBytes = uint64(len(stream.openInfo))
	c.maybeCompactTerminalLocked(stream)
	got := c.retention.retainedOpenInfoBytes
	c.mu.Unlock()

	if got != 0 {
		t.Fatalf("retainedOpenInfoBytes = %d, want 0 after terminal compaction", got)
	}
	if got := len(stream.openMetadataPrefix); got != 0 {
		t.Fatalf("openMetadataPrefix len after terminal compaction = %d, want 0", got)
	}
}

func TestCompactTerminalSkipsUnacceptedQueuedStreamWithOpenInfo(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := testBuildStream(
		c,
		4,
		testWithBidi(),
		testWithLocalSend(),
		testWithLocalReceive(),
		testWithApplicationVisible(),
		testWithOpenInfo([]byte("ssh")),
		testWithNotifications(),
	)
	stream.setSendFin()
	stream.setRecvFin()

	c.mu.Lock()
	c.retention.retainedOpenInfoBytes = uint64(len(stream.openInfo))
	c.enqueueAcceptedLocked(stream)
	c.maybeCompactTerminalLocked(stream)
	gotLive := c.registry.streams[stream.id]
	_, gotTombstone := c.registry.tombstones[stream.id]
	gotInfo := clonePayloadBytes(stream.openInfo)
	gotRetained := c.retention.retainedOpenInfoBytes
	gotAcceptCount := c.acceptCountLocked(streamArityBidi)
	gotAccepted := c.dequeueAcceptedLocked(streamArityBidi)
	c.mu.Unlock()

	if gotLive != stream {
		t.Fatal("terminal compaction dropped an unaccepted queued stream from live registry")
	}
	if gotTombstone {
		t.Fatal("terminal compaction created tombstone for an unaccepted queued stream")
	}
	if string(gotInfo) != "ssh" {
		t.Fatalf("openInfo = %q, want preserved before accept", gotInfo)
	}
	if gotRetained != uint64(len("ssh")) {
		t.Fatalf("retainedOpenInfoBytes = %d, want %d", gotRetained, len("ssh"))
	}
	if gotAcceptCount != 1 {
		t.Fatalf("accept count = %d, want 1", gotAcceptCount)
	}
	if gotAccepted != stream {
		t.Fatal("accept queue did not retain the unaccepted terminal stream")
	}
}

func TestFailProvisionalReleasesOpenInfoBytes(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, nil)
	before := c.retention.retainedOpenInfoBytes
	c.failProvisionalLocked(stream, nil)
	after := c.retention.retainedOpenInfoBytes
	c.mu.Unlock()

	if before != 3 {
		t.Fatalf("retainedOpenInfoBytes before fail = %d, want 3", before)
	}
	if after != 0 {
		t.Fatalf("retainedOpenInfoBytes after fail = %d, want 0", after)
	}
}

func TestFailProvisionalClearsOpenMetadataPrefix(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, []byte("meta"))
	if got := len(stream.openMetadataPrefix); got == 0 {
		c.mu.Unlock()
		t.Fatal("openMetadataPrefix len = 0 before fail, want > 0")
	}
	c.failProvisionalLocked(stream, nil)
	got := len(stream.openMetadataPrefix)
	c.mu.Unlock()

	if got != 0 {
		t.Fatalf("openMetadataPrefix len after fail = %d, want 0", got)
	}
}

func TestFailUnopenedLocalStreamClearsOpenMetadataPrefix(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	stream := c.newLocalStreamLocked(4, streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, []byte("meta"))
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.failUnopenedLocalStreamLocked(stream, &ApplicationError{Code: uint64(CodeCancelled)})
	got := len(stream.openMetadataPrefix)
	c.mu.Unlock()

	if got != 0 {
		t.Fatalf("openMetadataPrefix len after failUnopenedLocalStreamLocked = %d, want 0", got)
	}
}

func TestMarkPeerVisibleClearsOpenMetadataPrefix(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := testOpenedBidiStream(c, 4, testWithOpenMetadataPrefix([]byte("meta")))

	c.mu.Lock()
	c.markPeerVisibleLocked(stream)
	peerVisible := stream.isPeerVisibleLocked()
	got := len(stream.openMetadataPrefix)
	c.mu.Unlock()

	if !peerVisible {
		t.Fatal("peerVisible = false, want true")
	}
	if got != 0 {
		t.Fatalf("openMetadataPrefix len after markPeerVisibleLocked = %d, want 0", got)
	}
}

func TestCloseStreamOnSessionReleasesOpenMetadataPrefix(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	stream := c.newLocalStreamLocked(4, streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, []byte("meta"))
	stream.idSet = true
	c.registry.streams[stream.id] = stream
	c.closeStreamOnSessionWithOptionsLocked(stream, &ApplicationError{Code: uint64(CodeInternal)}, sessionCloseOptions{
		abortSource: terminalAbortLocal,
		finalize:    true,
	})
	got := len(stream.openMetadataPrefix)
	c.mu.Unlock()

	if got != 0 {
		t.Fatalf("openMetadataPrefix len after closeStreamOnSessionWithOptionsLocked = %d, want 0", got)
	}
}

func TestCloseSessionReleasesProvisionalOpenMetadataPrefix(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, []byte("meta"))
	if got := len(stream.openMetadataPrefix); got == 0 {
		c.mu.Unlock()
		t.Fatal("openMetadataPrefix len before close = 0, want > 0")
	}
	c.mu.Unlock()

	c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})

	c.mu.Lock()
	got := len(stream.openMetadataPrefix)
	c.mu.Unlock()

	if got != 0 {
		t.Fatalf("openMetadataPrefix len after closeSession = %d, want 0", got)
	}
}

func TestCheckLocalOpenPossibleWithOpenInfoLimitedByTrackedMemoryCap(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.flow.sessionMemoryCap = saturatingAdd(c.retainedStateUnitLocked(), 2)
	err := c.checkLocalOpenPossibleWithOpenInfoLocked(streamArityBidi, 3)
	c.mu.Unlock()

	if !errors.Is(err, ErrOpenLimited) {
		t.Fatalf("checkLocalOpenPossibleWithOpenInfoLocked err = %v, want %v", err, ErrOpenLimited)
	}
}

func TestEnforceVisibleAcceptBacklogShedsNewestWhenOpenInfoBudgetExceeded(t *testing.T) {
	c := newSessionMemoryTestConn()
	oldest := testBuildStream(c, 4, testWithBidi(), testWithEnqueued(1), testWithOpenInfo([]byte("aa")), testWithNotifications())
	newest := testBuildStream(c, 8, testWithBidi(), testWithEnqueued(2), testWithOpenInfo([]byte("bb")), testWithNotifications())
	oldest.initHalfStates()
	newest.initHalfStates()

	c.mu.Lock()
	c.retention.retainedOpenInfoBudget = 3
	c.retention.retainedOpenInfoBytes = uint64(len(oldest.openInfo) + len(newest.openInfo))
	c.queues.acceptBidi.items = []*nativeStream{oldest, newest}
	c.registry.streams[oldest.id] = oldest
	c.registry.streams[newest.id] = newest
	refused := c.enforceVisibleAcceptBacklogLocked()
	gotBytes := c.retention.retainedOpenInfoBytes
	c.mu.Unlock()

	if len(refused) != 1 || refused[0] != newest.id {
		t.Fatalf("refused = %#v, want newest stream %d", refused, newest.id)
	}
	if gotBytes != uint64(len(oldest.openInfo)) {
		t.Fatalf("retainedOpenInfoBytes = %d, want %d", gotBytes, len(oldest.openInfo))
	}
}

func TestRetainPeerReasonBudgetTrimsToRemainingBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	defer c.mu.Unlock()

	budget := c.retainedPeerReasonBudgetLocked()
	c.retention.retainedPeerReasonBytes = budget - 2
	got, n := c.retainPeerReasonLocked(0, "abcd")
	if got != "ab" || n != 2 {
		t.Fatalf("retainPeerReasonLocked() = (%q, %d), want (%q, %d)", got, n, "ab", 2)
	}
	if c.retention.retainedPeerReasonBytes != budget {
		t.Fatalf("retainedPeerReasonBytes = %d, want %d", c.retention.retainedPeerReasonBytes, budget)
	}
}

func TestRetainPeerReasonBudgetTrimClonesOversizedSource(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.retention.retainedReasonBudget = 2
	source := "ab" + strings.Repeat("x", 1<<20)
	got, n := c.retainPeerReasonLocked(0, source)
	if got != "ab" || n != 2 {
		t.Fatalf("retainPeerReasonLocked() = (%q, %d), want (%q, %d)", got, n, "ab", 2)
	}
	if unsafe.StringData(got) == unsafe.StringData(source) {
		t.Fatal("trimmed retained peer reason aliases oversized source backing")
	}
}

func TestRetainPeerReasonBudgetReplacesOldBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	defer c.mu.Unlock()

	first, firstBytes := c.retainPeerReasonLocked(0, "first")
	if first != "first" || firstBytes != 5 {
		t.Fatalf("first retain = (%q, %d), want (%q, %d)", first, firstBytes, "first", 5)
	}
	second, secondBytes := c.retainPeerReasonLocked(firstBytes, "xy")
	if second != "xy" || secondBytes != 2 {
		t.Fatalf("second retain = (%q, %d), want (%q, %d)", second, secondBytes, "xy", 2)
	}
	if c.retention.retainedPeerReasonBytes != 2 {
		t.Fatalf("retainedPeerReasonBytes = %d, want 2", c.retention.retainedPeerReasonBytes)
	}
}

func TestReleaseStreamPeerReasonBudgetLocked(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := &nativeStream{
		sendStopReasonBytes:    3,
		recvResetReasonBytes:   4,
		remoteAbortReasonBytes: 5,
	}

	c.mu.Lock()
	c.retention.retainedPeerReasonBytes = 12
	c.releaseStreamPeerReasonBudgetLocked(stream)
	c.mu.Unlock()

	if c.retention.retainedPeerReasonBytes != 0 {
		t.Fatalf("retainedPeerReasonBytes = %d, want 0", c.retention.retainedPeerReasonBytes)
	}
	if stream.sendStopReasonBytes != 0 || stream.recvResetReasonBytes != 0 || stream.remoteAbortReasonBytes != 0 {
		t.Fatalf("stream reason counters = (%d,%d,%d), want all zero", stream.sendStopReasonBytes, stream.recvResetReasonBytes, stream.remoteAbortReasonBytes)
	}
}

func TestRetainPeerReasonBudgetSaturatesTrackedBytes(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.flow.sessionMemoryCap = ^uint64(0)
	c.retention.retainedReasonBudget = ^uint64(0)
	c.retention.retainedPeerReasonBytes = ^uint64(0) - 1

	got, n := c.retainPeerReasonLocked(0, "xy")
	if got != "x" || n != 1 {
		t.Fatalf("retainPeerReasonLocked() = (%q, %d), want (%q, %d)", got, n, "x", 1)
	}
	if c.retention.retainedPeerReasonBytes != ^uint64(0) {
		t.Fatalf("retainedPeerReasonBytes = %d, want saturation to %d", c.retention.retainedPeerReasonBytes, ^uint64(0))
	}
}

func TestReleaseStreamPeerReasonBudgetLockedSaturatesSummedBytes(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := &nativeStream{
		sendStopReasonBytes:    ^uint64(0),
		recvResetReasonBytes:   ^uint64(0),
		remoteAbortReasonBytes: ^uint64(0),
	}

	c.mu.Lock()
	c.retention.retainedPeerReasonBytes = ^uint64(0)
	c.releaseStreamPeerReasonBudgetLocked(stream)
	c.mu.Unlock()

	if c.retention.retainedPeerReasonBytes != 0 {
		t.Fatalf("retainedPeerReasonBytes = %d, want 0 after saturated release", c.retention.retainedPeerReasonBytes)
	}
}

func TestCloseStreamOnSessionReleasesPeerReasonBudget(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := testOpenedBidiStream(c, 9)
	stream.sendStopReasonBytes = 3
	stream.recvResetReasonBytes = 4
	stream.remoteAbortReasonBytes = 5

	c.mu.Lock()
	c.retention.retainedPeerReasonBytes = 12
	c.closeStreamOnSessionWithOptionsLocked(stream, applicationErr(uint64(CodeInternal), "close"), sessionCloseOptions{
		abortSource: terminalAbortLocal,
		finalize:    false,
	})
	got := c.retention.retainedPeerReasonBytes
	c.mu.Unlock()

	if got != 0 {
		t.Fatalf("retainedPeerReasonBytes = %d, want 0 after closeStreamOnSessionWithOptionsLocked", got)
	}
	if stream.sendStopReasonBytes != 0 || stream.recvResetReasonBytes != 0 || stream.remoteAbortReasonBytes != 0 {
		t.Fatalf("stream reason counters = (%d,%d,%d), want all zero", stream.sendStopReasonBytes, stream.recvResetReasonBytes, stream.remoteAbortReasonBytes)
	}
}

func TestRetainPeerReasonBudgetFullBranchWakesWriteWaitersWhenBytesAreReleased(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.flow.sessionMemoryCap = 4
	c.retention.retainedReasonBudget = 2
	c.retention.retainedPeerReasonBytes = 4
	wake := c.currentWriteWakeLocked()
	got, n := c.retainPeerReasonLocked(2, "next")
	c.mu.Unlock()

	if got != "" || n != 0 {
		t.Fatalf("retainPeerReasonLocked() = (%q, %d), want empty retained reason", got, n)
	}

	select {
	case <-wake:
	default:
		t.Fatal("expected retainPeerReasonLocked budget-full replacement to wake blocked writers after releasing bytes")
	}
}

func refreshProvisionalCreated(t *testing.T, streams ...NativeStream) {
	t.Helper()

	// Add a small forward skew so broader package load does not accidentally
	// consume the real repository-default provisional max-age before the action
	// under test gets CPU time.
	now := time.Now().Add(testSignalTimeout)
	for _, native := range streams {
		stream := requireNativeStreamImpl(t, native)
		if stream == nil || stream.conn == nil {
			continue
		}
		stream.conn.mu.Lock()
		if !stream.idSet {
			stream.setProvisionalCreated(now)
		}
		stream.conn.mu.Unlock()
	}
}

func TestStreamIDHiddenUntilOpeningFrameCommitted(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		s, _ := server.AcceptStream(ctx)
		acceptCh <- mustNativeStreamImpl(s)
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if got := stream.StreamID(); got != 0 {
		t.Fatalf("provisional StreamID = %d, want 0", got)
	}

	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := stream.StreamID(); got != 4 {
		t.Fatalf("committed StreamID = %d, want 4", got)
	}

	accepted := <-acceptCh
	if accepted == nil {
		t.Fatal("expected accepted stream")
	}
}

func TestMiddleProvisionalRemovalPreservesOpeningOrder(t *testing.T) {
	t.Parallel()
	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	firstStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open first provisional: %v", err)
	}
	first := requireNativeStreamImpl(t, firstStream)
	secondStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open second provisional: %v", err)
	}
	second := requireNativeStreamImpl(t, secondStream)
	thirdStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open third provisional: %v", err)
	}
	third := requireNativeStreamImpl(t, thirdStream)

	client.mu.Lock()
	defer client.mu.Unlock()

	if got := client.provisionalCountLocked(streamArityBidi); got != 3 {
		t.Fatalf("provisional bidi count = %d, want 3", got)
	}
	if head := client.provisionalHeadLocked(streamArityBidi); !head.found() || head.stream != first {
		t.Fatalf("initial provisional head = %p, want first %p", head.stream, first)
	}
	wait, err := client.provisionalCommitWaitLocked(third, time.Now())
	if err != nil {
		t.Fatalf("third provisional commit wait err = %v, want nil", err)
	}
	if !wait.blocked() {
		t.Fatalf("third provisional commit wait blocked = %v, want true", wait.blocked())
	}

	client.failProvisionalLocked(second, ErrOpenExpired)
	if got := client.provisionalCountLocked(streamArityBidi); got != 2 {
		t.Fatalf("provisional bidi count after middle removal = %d, want 2", got)
	}
	if head := client.provisionalHeadLocked(streamArityBidi); !head.found() || head.stream != first {
		t.Fatalf("head after middle removal = %p, want first %p", head.stream, first)
	}
	wait, err = client.provisionalCommitWaitLocked(third, time.Now())
	if err != nil {
		t.Fatalf("third provisional after middle removal err = %v, want nil", err)
	}
	if !wait.blocked() {
		t.Fatalf("third provisional after middle removal blocked = %v, want true", wait.blocked())
	}

	client.failProvisionalLocked(first, ErrOpenExpired)
	if got := client.provisionalCountLocked(streamArityBidi); got != 1 {
		t.Fatalf("provisional bidi count after head removal = %d, want 1", got)
	}
	if head := client.provisionalHeadLocked(streamArityBidi); !head.found() || head.stream != third {
		t.Fatalf("head after head removal = %p, want third %p", head.stream, third)
	}
	wait, err = client.provisionalCommitWaitLocked(third, time.Now())
	if err != nil {
		t.Fatalf("third provisional after head removal err = %v, want nil", err)
	}
	if wait.blocked() {
		t.Fatalf("third provisional after head removal blocked = %v, want false", wait.blocked())
	}
}

func TestProvisionalCommitWaitIgnoresCommittedLocalReadStop(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()

	c.mu.Lock()
	_ = c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	blocked := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	blocked.setRecvStopSent()
	wait, err := c.provisionalCommitWaitLocked(blocked, time.Now())
	c.mu.Unlock()

	if err != nil {
		t.Fatalf("provisionalCommitWaitLocked err = %v, want nil", err)
	}
	if !wait.blocked() {
		t.Fatal("provisionalCommitWaitLocked blocked = false, want true with earlier same-class provisional head")
	}
}

func TestGracefulCloseReclaimsSparseProvisionalQueue(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()
	c.mu.Lock()
	first := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	second := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	third := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.failProvisionalLocked(second, ErrOpenExpired)
	c.reclaimGracefulCloseLocalStreamsLocked()
	count := c.provisionalCountLocked(streamArityBidi)
	firstAbort := first.sendAbort
	thirdAbort := third.sendAbort
	c.mu.Unlock()

	if count != 0 {
		t.Fatalf("provisional bidi count after graceful-close reclaim = %d, want 0", count)
	}
	if firstAbort == nil || firstAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("first sendAbort = %v, want code %d", firstAbort, uint64(CodeRefusedStream))
	}
	if thirdAbort == nil || thirdAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("third sendAbort = %v, want code %d", thirdAbort, uint64(CodeRefusedStream))
	}
}

func TestPrecommitAbortDoesNotConsumeFirstStreamID(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	first, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open first stream: %v", err)
	}
	if err := first.CloseWithError(uint64(CodeCancelled), ""); err != nil {
		t.Fatalf("abort provisional stream: %v", err)
	}
	if got := first.StreamID(); got != 0 {
		t.Fatalf("aborted provisional StreamID = %d, want 0", got)
	}

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		s, _ := server.AcceptStream(ctx)
		acceptCh <- mustNativeStreamImpl(s)
	}()

	second, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open second stream: %v", err)
	}
	if _, err := second.Write([]byte("x")); err != nil {
		t.Fatalf("write second stream: %v", err)
	}
	if got := second.StreamID(); got != 4 {
		t.Fatalf("second committed StreamID = %d, want 4", got)
	}

	accepted := <-acceptCh
	if accepted == nil {
		t.Fatal("expected accepted stream")
	}
	if got := accepted.StreamID(); got != 4 {
		t.Fatalf("accepted StreamID = %d, want 4", got)
	}
}

func TestMaybeCompactProvisionalQueueClearsStaleBackingPointers(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	first := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	second := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	q := make([]*nativeStream, provisionalQueueCompactMinHead+2, provisionalQueueCompactMinHead+2048)
	q[provisionalQueueCompactMinHead] = first
	q[provisionalQueueCompactMinHead+1] = second
	first.provisionalIndex = int32(provisionalQueueCompactMinHead)
	second.provisionalIndex = int32(provisionalQueueCompactMinHead + 1)
	c.queues.provisionalBidi.items = q
	c.queues.provisionalBidi.head = provisionalQueueCompactMinHead
	c.queues.provisionalBidi.count = 2
	c.queues.provisionalBidi.init = true
	backing := c.queues.provisionalBidi.items[:cap(c.queues.provisionalBidi.items)]

	c.maybeCompactProvisionalLocked(streamArityBidi)
	queue := c.queues.provisionalBidi.items
	c.mu.Unlock()

	if len(queue) != 2 {
		t.Fatalf("len(c.queues.provisionalBidi) = %d, want 2", len(queue))
	}
	if queue[0] != first || queue[1] != second {
		t.Fatalf("provisional queue order = %p,%p want %p,%p", queue[0], queue[1], first, second)
	}
	if first.provisionalIndex != 0 || second.provisionalIndex != 1 {
		t.Fatalf("provisional indexes = (%d,%d), want (0,1)", first.provisionalIndex, second.provisionalIndex)
	}
	if cap(queue) > compactedQueueRetainLimit(len(queue)) {
		t.Fatalf("provisional queue cap = %d, want <= %d", cap(queue), compactedQueueRetainLimit(len(queue)))
	}
	if len(queue) > 0 && len(backing) > 0 && &queue[0] == &backing[0] {
		t.Fatal("provisional queue retained oversized backing after compaction")
	}
	for i := len(queue); i < len(backing); i++ {
		if backing[i] != nil {
			t.Fatalf("stale provisional backing entry at %d = %p, want nil", i, backing[i])
		}
	}
}

func TestDropPendingStreamControlCompactsSparseQueue(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	defer c.mu.Unlock()

	total := pendingStreamQueueCompactMinHead + 2
	ids := make([]uint64, total)
	streams := make([]*nativeStream, total)
	for i := range ids {
		ids[i] = 4 + uint64(i)*4
		if !testSetPendingStreamMaxData(c, ids[i], uint64(i+1)) {
			t.Fatalf("set pending MAX_DATA for stream %d failed", ids[i])
		}
		streams[i] = c.registry.streams[ids[i]]
	}

	queueState := c.pending.streamQueueState(pendingStreamQueueMaxData)
	backing := queueState.items[:cap(queueState.items)]
	for i := 0; i < pendingStreamQueueCompactMinHead; i++ {
		if !c.dropPendingStreamControlEntryLocked(streamControlMaxData, ids[i]) {
			t.Fatalf("drop pending MAX_DATA for stream %d failed", ids[i])
		}
	}

	queue := queueState.items
	first := streams[pendingStreamQueueCompactMinHead]
	second := streams[pendingStreamQueueCompactMinHead+1]
	if len(queue) != 2 {
		t.Fatalf("len(pending MAX_DATA queue) = %d, want 2", len(queue))
	}
	if queue[0] != first || queue[1] != second {
		t.Fatalf("pending MAX_DATA queue order = %p,%p want %p,%p", queue[0], queue[1], first, second)
	}
	if first.pendingQueueIndex(pendingStreamQueueMaxData) != 0 || second.pendingQueueIndex(pendingStreamQueueMaxData) != 1 {
		t.Fatalf("pending MAX_DATA indexes = (%d,%d), want (0,1)", first.pendingQueueIndex(pendingStreamQueueMaxData), second.pendingQueueIndex(pendingStreamQueueMaxData))
	}
	if !testHasPendingStreamMaxData(c, first.id) || !testHasPendingStreamMaxData(c, second.id) {
		t.Fatal("remaining pending MAX_DATA entries were not retained")
	}
	if cap(queue) > compactedQueueRetainLimit(len(queue)) {
		t.Fatalf("pending MAX_DATA queue cap = %d, want <= %d", cap(queue), compactedQueueRetainLimit(len(queue)))
	}
	if len(queue) > 0 && len(backing) > 0 && &queue[0] == &backing[0] {
		t.Fatal("pending MAX_DATA queue retained oversized backing after compaction")
	}
	for i := len(queue); i < len(backing); i++ {
		if backing[i] != nil {
			t.Fatalf("stale pending MAX_DATA backing entry at %d = %p, want nil", i, backing[i])
		}
	}
}

func TestDropPendingPriorityUpdateCompactsSparseQueue(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	defer c.mu.Unlock()

	total := pendingStreamQueueCompactMinHead + 2
	ids := make([]uint64, total)
	streams := make([]*nativeStream, total)
	for i := range ids {
		ids[i] = 4 + uint64(i)*4
		if got := testSetPendingPriorityUpdate(c, ids[i], []byte{byte(i + 1)}); got == 0 {
			t.Fatalf("set pending priority update for stream %d failed", ids[i])
		}
		streams[i] = c.registry.streams[ids[i]]
	}

	queueState := c.pending.streamQueueState(pendingStreamQueuePriority)
	backing := queueState.items[:cap(queueState.items)]
	for i := 0; i < pendingStreamQueueCompactMinHead; i++ {
		if !c.dropPendingPriorityUpdateEntryLocked(ids[i]) {
			t.Fatalf("drop pending priority update for stream %d failed", ids[i])
		}
	}

	queue := queueState.items
	first := streams[pendingStreamQueueCompactMinHead]
	second := streams[pendingStreamQueueCompactMinHead+1]
	if len(queue) != 2 {
		t.Fatalf("len(pending priority queue) = %d, want 2", len(queue))
	}
	if queue[0] != first || queue[1] != second {
		t.Fatalf("pending priority queue order = %p,%p want %p,%p", queue[0], queue[1], first, second)
	}
	if first.pendingQueueIndex(pendingStreamQueuePriority) != 0 || second.pendingQueueIndex(pendingStreamQueuePriority) != 1 {
		t.Fatalf("pending priority indexes = (%d,%d), want (0,1)", first.pendingQueueIndex(pendingStreamQueuePriority), second.pendingQueueIndex(pendingStreamQueuePriority))
	}
	if !testHasPendingPriorityUpdate(c, first.id) || !testHasPendingPriorityUpdate(c, second.id) {
		t.Fatal("remaining pending priority updates were not retained")
	}
	if cap(queue) > compactedQueueRetainLimit(len(queue)) {
		t.Fatalf("pending priority queue cap = %d, want <= %d", cap(queue), compactedQueueRetainLimit(len(queue)))
	}
	if len(queue) > 0 && len(backing) > 0 && &queue[0] == &backing[0] {
		t.Fatal("pending priority queue retained oversized backing after compaction")
	}
	for i := len(queue); i < len(backing); i++ {
		if backing[i] != nil {
			t.Fatalf("stale pending priority backing entry at %d = %p, want nil", i, backing[i])
		}
	}
}

func TestDropPendingTerminalControlCompactsSparseQueue(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	defer c.mu.Unlock()

	total := pendingStreamQueueCompactMinHead + 2
	ids := make([]uint64, total)
	streams := make([]*nativeStream, total)
	for i := range ids {
		ids[i] = 4 + uint64(i)*4
		stream := ensureTestPendingStream(c, ids[i])
		result := c.setPendingTerminalControlLocked(stream, func(stream *nativeStream) (bool, bool, bool) {
			stream.pending.terminal.resetPayload = []byte{byte(i + 1)}
			stream.pending.flags |= streamPendingTerminalReset
			return true, false, false
		})
		if !result.accepted || !result.changed {
			t.Fatalf("set pending terminal control for stream %d failed: %+v", ids[i], result)
		}
		streams[i] = stream
	}

	queueState := c.pending.streamQueueState(pendingStreamQueueTerminal)
	backing := queueState.items[:cap(queueState.items)]
	for i := 0; i < pendingStreamQueueCompactMinHead; i++ {
		if !c.dropPendingTerminalControlEntryLocked(ids[i]) {
			t.Fatalf("drop pending terminal control for stream %d failed", ids[i])
		}
	}

	queue := queueState.items
	first := streams[pendingStreamQueueCompactMinHead]
	second := streams[pendingStreamQueueCompactMinHead+1]
	if len(queue) != 2 {
		t.Fatalf("len(pending terminal queue) = %d, want 2", len(queue))
	}
	if queue[0] != first || queue[1] != second {
		t.Fatalf("pending terminal queue order = %p,%p want %p,%p", queue[0], queue[1], first, second)
	}
	if first.pendingQueueIndex(pendingStreamQueueTerminal) != 0 || second.pendingQueueIndex(pendingStreamQueueTerminal) != 1 {
		t.Fatalf("pending terminal indexes = (%d,%d), want (0,1)", first.pendingQueueIndex(pendingStreamQueueTerminal), second.pendingQueueIndex(pendingStreamQueueTerminal))
	}
	if !first.hasPendingTerminalControlLocked() || !second.hasPendingTerminalControlLocked() {
		t.Fatal("remaining pending terminal controls were not retained")
	}
	if cap(queue) > compactedQueueRetainLimit(len(queue)) {
		t.Fatalf("pending terminal queue cap = %d, want <= %d", cap(queue), compactedQueueRetainLimit(len(queue)))
	}
	if len(queue) > 0 && len(backing) > 0 && &queue[0] == &backing[0] {
		t.Fatal("pending terminal queue retained oversized backing after compaction")
	}
	for i := len(queue); i < len(backing); i++ {
		if backing[i] != nil {
			t.Fatalf("stale pending terminal backing entry at %d = %p, want nil", i, backing[i])
		}
	}
}

func TestLiveStreamCountCompatibilityRebuildsFromSeededMap(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	c.mu.Lock()
	c.registry.liveStreamCount = 0
	c.registry.liveStreamsInit = false
	if got := c.liveStreamCountLocked(); got != 1 {
		c.mu.Unlock()
		t.Fatalf("liveStreamCountLocked() = %d, want 1", got)
	}
	if !c.hasLiveStreamsLocked() {
		c.mu.Unlock()
		t.Fatal("hasLiveStreamsLocked() = false, want true")
	}
	c.dropLiveStreamLocked(stream.id)
	if got := c.liveStreamCountLocked(); got != 0 {
		c.mu.Unlock()
		t.Fatalf("liveStreamCountLocked() after drop = %d, want 0", got)
	}
	c.mu.Unlock()
}

func TestStoreLiveStreamLockedAvoidsDoubleCount(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := &nativeStream{
		conn:         c,
		id:           state.FirstLocalStreamID(c.config.negotiated.LocalRole, true),
		idSet:        true,
		bidi:         true,
		localOpen:    testLocalOpenOpenedState(),
		localSend:    true,
		localReceive: true,
		readNotify:   make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}

	c.mu.Lock()
	c.storeLiveStreamLocked(stream)
	c.storeLiveStreamLocked(stream)
	if got := c.liveStreamCountLocked(); got != 1 {
		c.mu.Unlock()
		t.Fatalf("liveStreamCountLocked() after duplicate store = %d, want 1", got)
	}
	c.mu.Unlock()
}

func TestMarkPeerVisibleRemovesUnseenLocalQueueEntry(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	before := c.unseenLocalCountLocked(streamArityBidi)
	c.markPeerVisibleLocked(stream)
	after := c.unseenLocalCountLocked(streamArityBidi)
	c.mu.Unlock()

	if before != 1 {
		t.Fatalf("unseenLocalCountLocked(streamArityBidi) before = %d, want 1", before)
	}
	if after != 0 {
		t.Fatalf("unseenLocalCountLocked(streamArityBidi) after = %d, want 0", after)
	}
	if stream.unseenLocalIndex != -1 {
		t.Fatalf("stream.unseenLocalIndex = %d, want -1", stream.unseenLocalIndex)
	}
}

func TestReclaimUnseenLocalStreamsOnlyDropsOverWatermarkTail(t *testing.T) {
	c := newSessionMemoryTestConn()
	oldest := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)
	newest := c.newLocalStreamWithIDLocked(8, streamArityBidi, OpenOptions{}, nil)

	c.mu.Lock()
	c.registry.streams[oldest.id] = oldest
	c.registry.streams[newest.id] = newest
	c.sessionControl.peerGoAwayBidi = 4
	c.sessionControl.peerGoAwayUni = MaxVarint62
	c.reclaimUnseenLocalStreamsLocked()
	count := c.unseenLocalCountLocked(streamArityBidi)
	tail := c.unseenLocalTailLocked(streamArityBidi)
	newestAbort := newest.sendAbort
	oldestAbort := oldest.sendAbort
	c.mu.Unlock()

	if newestAbort == nil || newestAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("newest.sendAbort = %v, want code %d", newestAbort, uint64(CodeRefusedStream))
	}
	if oldestAbort != nil {
		t.Fatalf("oldest.sendAbort = %v, want nil", oldestAbort)
	}
	if count != 1 {
		t.Fatalf("unseenLocalCountLocked(streamArityBidi) = %d, want 1", count)
	}
	if !tail.found() || tail.stream != oldest {
		t.Fatalf("unseenLocalTailLocked(streamArityBidi) = %#v, want oldest stream", tail)
	}
}

func TestReclaimGracefulCloseLocalStreamsDrainsUnseenLocalQueues(t *testing.T) {
	c := newSessionMemoryTestConn()
	bidi := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)
	uni := c.newLocalStreamWithIDLocked(6, streamArityUni, OpenOptions{}, nil)

	c.mu.Lock()
	c.registry.streams[bidi.id] = bidi
	c.registry.streams[uni.id] = uni
	c.reclaimGracefulCloseLocalStreamsLocked()
	bidiCount := c.unseenLocalCountLocked(streamArityBidi)
	uniCount := c.unseenLocalCountLocked(streamArityUni)
	bidiAbort := bidi.sendAbort
	uniAbort := uni.sendAbort
	c.mu.Unlock()

	if bidiAbort == nil || bidiAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("bidi.sendAbort = %v, want code %d", bidiAbort, uint64(CodeRefusedStream))
	}
	if uniAbort == nil || uniAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("uni.sendAbort = %v, want code %d", uniAbort, uint64(CodeRefusedStream))
	}
	if bidiCount != 0 || uniCount != 0 {
		t.Fatalf("unseen local counts = (%d,%d), want (0,0)", bidiCount, uniCount)
	}
}

func TestEnsureUnseenLocalQueueClearsStaleBackingPointersOnRebuild(t *testing.T) {
	c := newSessionMemoryTestConn()
	first := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)
	second := c.newLocalStreamWithIDLocked(8, streamArityBidi, OpenOptions{}, nil)

	c.mu.Lock()
	c.registry.streams[first.id] = first
	c.registry.streams[second.id] = second
	if got := c.unseenLocalCountLocked(streamArityBidi); got != 2 {
		c.mu.Unlock()
		t.Fatalf("unseenLocalCountLocked(streamArityBidi) = %d, want 2", got)
	}

	delete(c.registry.streams, second.id)
	c.queues.unseenLocalBidi.init = false
	c.ensureUnseenLocalQueueLocked(streamArityBidi)
	queue := c.queues.unseenLocalBidi.items
	if len(queue) != 1 {
		c.mu.Unlock()
		t.Fatalf("len(c.queues.unseenLocalBidi) = %d, want 1", len(queue))
	}
	backing := queue[:cap(queue)]
	c.mu.Unlock()

	for i := len(queue); i < len(backing); i++ {
		if backing[i] != nil {
			t.Fatalf("stale unseen-local backing entry at %d = %p, want nil", i, backing[i])
		}
	}
}

func TestMaybeCompactUnseenLocalQueueClearsStaleBackingPointers(t *testing.T) {
	c := newSessionMemoryTestConn()
	first := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)
	second := c.newLocalStreamWithIDLocked(8, streamArityBidi, OpenOptions{}, nil)

	c.mu.Lock()
	c.registry.streams[first.id] = first
	c.registry.streams[second.id] = second
	q := make([]*nativeStream, acceptQueueCompactMinHead+2)
	q[acceptQueueCompactMinHead] = first
	q[acceptQueueCompactMinHead+1] = second
	first.unseenLocalIndex = int32(acceptQueueCompactMinHead)
	second.unseenLocalIndex = int32(acceptQueueCompactMinHead + 1)
	c.queues.unseenLocalBidi.items = q
	c.queues.unseenLocalBidi.head = acceptQueueCompactMinHead
	c.queues.unseenLocalBidi.count = 2
	c.queues.unseenLocalBidi.init = true
	backing := c.queues.unseenLocalBidi.items[:cap(c.queues.unseenLocalBidi.items)]

	c.maybeCompactUnseenLocalQueueLocked(streamArityBidi)
	queue := c.queues.unseenLocalBidi.items
	c.mu.Unlock()

	if len(queue) != 2 {
		t.Fatalf("len(c.queues.unseenLocalBidi) = %d, want 2", len(queue))
	}
	if queue[0] != first || queue[1] != second {
		t.Fatalf("unseen-local queue order = %p,%p want %p,%p", queue[0], queue[1], first, second)
	}
	if first.unseenLocalIndex != 0 || second.unseenLocalIndex != 1 {
		t.Fatalf("unseen-local indexes = (%d,%d), want (0,1)", first.unseenLocalIndex, second.unseenLocalIndex)
	}
	for i := len(queue); i < len(backing); i++ {
		if backing[i] != nil {
			t.Fatalf("stale unseen-local compacted backing entry at %d = %p, want nil", i, backing[i])
		}
	}
}

func TestVisibleAcceptBacklogRefusesNewestStream(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.queues.acceptBacklogLimit = 2

	firstBidi := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	firstUni := state.FirstPeerStreamID(c.config.negotiated.LocalRole, false)
	secondBidi := firstBidi + 4

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstBidi, Payload: []byte("a")}); err != nil {
		t.Fatalf("handle first bidi DATA: %v", err)
	}
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstUni, Payload: []byte("b")}); err != nil {
		t.Fatalf("handle first uni DATA: %v", err)
	}
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: secondBidi, Payload: []byte("c")}); err != nil {
		t.Fatalf("handle second bidi DATA: %v", err)
	}

	assertInvalidQueuedAbortCode(t, frames, secondBidi, CodeRefusedStream)
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	if got := c.acceptCountLocked(streamArityBidi); got != 1 {
		c.mu.Unlock()
		t.Fatalf("acceptBidi count = %d, want 1", got)
	}
	if got := c.acceptCountLocked(streamArityUni); got != 1 {
		c.mu.Unlock()
		t.Fatalf("acceptUni count = %d, want 1", got)
	}
	if head := c.acceptHeadLocked(streamArityBidi); !head.found() || head.stream.id != firstBidi {
		c.mu.Unlock()
		t.Fatalf("queued bidi head = %#v, want id %d", head, firstBidi)
	}
	if head := c.acceptHeadLocked(streamArityUni); !head.found() || head.stream.id != firstUni {
		c.mu.Unlock()
		t.Fatalf("queued uni head = %#v, want id %d", head, firstUni)
	}
	if _, ok := c.registry.tombstones[secondBidi]; !ok {
		c.mu.Unlock()
		t.Fatalf("refused visible stream %d missing tombstone", secondBidi)
	}
	if got := c.flow.recvSessionUsed; got != 2 {
		c.mu.Unlock()
		t.Fatalf("recvSessionUsed = %d, want 2 after dropping newest visible stream", got)
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	bidi, err := c.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if got := bidi.StreamID(); got != firstBidi {
		t.Fatalf("accepted bidi id = %d, want %d", got, firstBidi)
	}

	uni, err := c.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("AcceptUniStream: %v", err)
	}
	if got := uni.StreamID(); got != firstUni {
		t.Fatalf("accepted uni id = %d, want %d", got, firstUni)
	}
}

func TestVisibleAcceptBacklogCountsOnlyUnacceptedStreams(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.queues.acceptBacklogLimit = 1

	firstID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	secondID := firstID + 4

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstID, Payload: []byte("a")}); err != nil {
		t.Fatalf("handle first DATA: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	first, err := c.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("accept first stream: %v", err)
	}
	if got := first.StreamID(); got != firstID {
		t.Fatalf("accepted first stream id = %d, want %d", got, firstID)
	}

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: secondID, Payload: []byte("b")}); err != nil {
		t.Fatalf("handle second DATA: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	second, err := c.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("accept second stream: %v", err)
	}
	if got := second.StreamID(); got != secondID {
		t.Fatalf("accepted second stream id = %d, want %d", got, secondID)
	}
}

func TestVisibleAcceptBacklogBytesRefuseNewestByVisibilitySequence(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.queues.acceptBacklogLimit = 16
	c.queues.acceptBacklogBytesLimit = 3

	firstID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	secondID := firstID + 4

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstID, Payload: []byte("a")}); err != nil {
		t.Fatalf("handle first DATA: %v", err)
	}
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: secondID, Payload: []byte("b")}); err != nil {
		t.Fatalf("handle second DATA: %v", err)
	}
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstID, Payload: []byte("xx")}); err != nil {
		t.Fatalf("handle additional first-stream DATA: %v", err)
	}

	assertInvalidQueuedAbortCode(t, frames, secondID, CodeRefusedStream)
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	if got := c.acceptCountLocked(streamArityBidi); got != 1 {
		c.mu.Unlock()
		t.Fatalf("acceptBidi count = %d, want 1", got)
	}
	if head := c.acceptHeadLocked(streamArityBidi); !head.found() || head.stream.id != firstID {
		c.mu.Unlock()
		t.Fatalf("remaining queued bidi head = %#v, want id %d", head, firstID)
	}
	stream := c.registry.streams[firstID]
	if stream == nil {
		c.mu.Unlock()
		t.Fatalf("first stream %d missing after bytes-cap enforcement", firstID)
	}
	if got := stream.recvBuffer; got != 3 {
		c.mu.Unlock()
		t.Fatalf("first stream recvBuffer = %d, want 3", got)
	}
	if got := c.flow.recvSessionUsed; got != 3 {
		c.mu.Unlock()
		t.Fatalf("recvSessionUsed = %d, want 3 after dropping newest queued stream", got)
	}
	if got := c.pendingAcceptedBytesLocked(); got != 3 {
		c.mu.Unlock()
		t.Fatalf("pendingAcceptedBytes = %d, want 3", got)
	}
	if _, ok := c.registry.tombstones[secondID]; !ok {
		c.mu.Unlock()
		t.Fatalf("refused newest visible stream %d missing tombstone", secondID)
	}
	c.mu.Unlock()
}

func TestVisibleAcceptBacklogBytesTrackGrowthAndAcceptPop(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte("abc")}); err != nil {
		t.Fatalf("handle first DATA: %v", err)
	}

	c.mu.Lock()
	if got := c.pendingAcceptedBytesLocked(); got != 3 {
		c.mu.Unlock()
		t.Fatalf("pendingAcceptedBytes after first DATA = %d, want 3", got)
	}
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte("de")}); err != nil {
		t.Fatalf("handle second DATA: %v", err)
	}

	c.mu.Lock()
	if got := c.pendingAcceptedBytesLocked(); got != 5 {
		c.mu.Unlock()
		t.Fatalf("pendingAcceptedBytes after second DATA = %d, want 5", got)
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	stream, err := c.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	streamImpl := requireNativeStreamImpl(t, stream)
	if got := stream.StreamID(); got != streamID {
		t.Fatalf("accepted stream id = %d, want %d", got, streamID)
	}

	c.mu.Lock()
	if got := c.pendingAcceptedBytesLocked(); got != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingAcceptedBytes after accept = %d, want 0", got)
	}
	if got := streamImpl.recvBuffer; got != 5 {
		c.mu.Unlock()
		t.Fatalf("accepted stream recvBuffer = %d, want 5", got)
	}
	c.mu.Unlock()
}

func TestAcceptQueueNotificationCoalescesBurst(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	acceptCh := ensureNotifyChan(&c.signals.acceptCh)
	c.mu.Unlock()

	firstID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	secondID := firstID + 4

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstID, Payload: []byte("a")}); err != nil {
		t.Fatalf("handle first DATA: %v", err)
	}
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: secondID, Payload: []byte("b")}); err != nil {
		t.Fatalf("handle second DATA: %v", err)
	}

	if got := len(acceptCh); got != 1 {
		t.Fatalf("len(acceptCh) = %d, want coalesced single notification", got)
	}

	c.mu.Lock()
	if got := c.acceptCountLocked(streamArityBidi); got != 2 {
		c.mu.Unlock()
		t.Fatalf("acceptCountLocked(streamArityBidi) = %d, want 2", got)
	}
	c.mu.Unlock()
}

func TestAcceptStreamDrainsBurstAfterSingleNotification(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	acceptCh := ensureNotifyChan(&c.signals.acceptCh)
	c.mu.Unlock()

	firstID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	secondID := firstID + 4

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstID, Payload: []byte("a")}); err != nil {
		t.Fatalf("handle first DATA: %v", err)
	}
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: secondID, Payload: []byte("b")}); err != nil {
		t.Fatalf("handle second DATA: %v", err)
	}

	select {
	case <-acceptCh:
	default:
		t.Fatal("accept queue did not publish initial notification")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	first, err := c.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("first AcceptStream: %v", err)
	}
	if got := first.StreamID(); got != firstID {
		t.Fatalf("first accepted StreamID = %d, want %d", got, firstID)
	}

	if got := len(acceptCh); got != 0 {
		t.Fatalf("len(acceptCh) after first accept = %d, want 0 before burst drain", got)
	}

	second, err := c.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("second AcceptStream: %v", err)
	}
	if got := second.StreamID(); got != secondID {
		t.Fatalf("second accepted StreamID = %d, want %d", got, secondID)
	}
}

func TestPeerOpenedBidiStreamRetainsIncomingSlotUntilLocalSendHalfTerminates(t *testing.T) {
	t.Parallel()

	serverCfg := DefaultConfig()
	serverCfg.Settings.MaxIncomingStreamsBidi = 1

	client, server := newConnPairWithConfig(t, nil, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	firstAcceptCh := make(chan *nativeStream, 1)
	firstAcceptErrCh := make(chan error, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			firstAcceptErrCh <- err
			return
		}
		firstAcceptCh <- mustNativeStreamImpl(stream)
	}()

	first, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open first bidi stream: %v", err)
	}
	if _, err := first.Write([]byte("hi")); err != nil {
		t.Fatalf("write first bidi opener: %v", err)
	}
	if err := first.CloseWrite(); err != nil {
		t.Fatalf("close first bidi write: %v", err)
	}

	var accepted *nativeStream
	select {
	case err := <-firstAcceptErrCh:
		t.Fatalf("accept first bidi stream: %v", err)
	case accepted = <-firstAcceptCh:
	}

	buf := make([]byte, 8)
	n, err := accepted.Read(buf)
	if err != nil {
		t.Fatalf("read first bidi payload: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("read first bidi payload = %q, want %q", got, "hi")
	}
	if _, err := accepted.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("read first bidi EOF err = %v, want EOF", err)
	}

	server.mu.Lock()
	activeBeforeCloseWrite := server.registry.activePeerBidi
	server.mu.Unlock()
	if activeBeforeCloseWrite != 1 {
		t.Fatalf("activePeerBidi after peer FIN = %d, want 1", activeBeforeCloseWrite)
	}

	_, err = client.OpenStream(ctx)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("open second bidi stream before slot release err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("open second bidi stream error code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}

	server.mu.Lock()
	activeAfterRefusedSecond := server.registry.activePeerBidi
	server.mu.Unlock()
	if activeAfterRefusedSecond != 1 {
		t.Fatalf("activePeerBidi after refused second stream = %d, want 1", activeAfterRefusedSecond)
	}

	if err := accepted.CloseWrite(); err != nil {
		t.Fatalf("server CloseWrite on peer-opened bidi stream: %v", err)
	}

	awaitConnState(t, server, testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.registry.activePeerBidi == 0
	}, "incoming bidi slot was not released after local send half became terminal")

	thirdAcceptCh := make(chan *nativeStream, 1)
	thirdAcceptErrCh := make(chan error, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			thirdAcceptErrCh <- err
			return
		}
		thirdAcceptCh <- mustNativeStreamImpl(stream)
	}()

	third, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open third bidi stream after slot release: %v", err)
	}
	if _, err := third.Write([]byte("ok")); err != nil {
		t.Fatalf("write third bidi opener: %v", err)
	}

	select {
	case err := <-thirdAcceptErrCh:
		t.Fatalf("accept third bidi stream: %v", err)
	case accepted = <-thirdAcceptCh:
	}

	n, err = accepted.Read(buf)
	if err != nil {
		t.Fatalf("read third bidi payload: %v", err)
	}
	if got := string(buf[:n]); got != "ok" {
		t.Fatalf("read third bidi payload = %q, want %q", got, "ok")
	}
}

func TestPeerOpenedUniStreamReleasesIncomingSlotAfterRecvEOF(t *testing.T) {
	t.Parallel()

	serverCfg := DefaultConfig()
	serverCfg.Settings.MaxIncomingStreamsUni = 1

	client, server := newConnPairWithConfig(t, nil, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	firstAcceptCh := make(chan *nativeRecvStream, 1)
	firstAcceptErrCh := make(chan error, 1)
	go func() {
		stream, err := server.AcceptUniStream(ctx)
		if err != nil {
			firstAcceptErrCh <- err
			return
		}
		firstAcceptCh <- mustNativeRecvStreamImpl(stream)
	}()

	first, n, err := client.OpenUniAndSend(ctx, []byte("hi"))
	if err != nil {
		t.Fatalf("open first uni stream: %v", err)
	}
	if n != 2 {
		t.Fatalf("first uni write len = %d, want 2", n)
	}
	if first == nil {
		t.Fatal("first uni stream = nil")
	}

	var accepted *nativeRecvStream
	select {
	case err := <-firstAcceptErrCh:
		t.Fatalf("accept first uni stream: %v", err)
	case accepted = <-firstAcceptCh:
	}

	buf := make([]byte, 8)
	n, err = accepted.Read(buf)
	if err != nil {
		t.Fatalf("read first uni payload: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("read first uni payload = %q, want %q", got, "hi")
	}
	if _, err := accepted.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("read first uni EOF err = %v, want EOF", err)
	}

	awaitConnState(t, server, testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.registry.activePeerUni == 0
	}, "incoming uni slot was not released after recv EOF")

	secondAcceptCh := make(chan *nativeRecvStream, 1)
	secondAcceptErrCh := make(chan error, 1)
	go func() {
		stream, err := server.AcceptUniStream(ctx)
		if err != nil {
			secondAcceptErrCh <- err
			return
		}
		secondAcceptCh <- mustNativeRecvStreamImpl(stream)
	}()

	second, n, err := client.OpenUniAndSend(ctx, []byte("ok"))
	if err != nil {
		t.Fatalf("open second uni stream after slot release: %v", err)
	}
	if n != 2 {
		t.Fatalf("second uni write len = %d, want 2", n)
	}
	if second == nil {
		t.Fatal("second uni stream = nil")
	}

	select {
	case err := <-secondAcceptErrCh:
		t.Fatalf("accept second uni stream: %v", err)
	case accepted = <-secondAcceptCh:
	}

	n, err = accepted.Read(buf)
	if err != nil {
		t.Fatalf("read second uni payload: %v", err)
	}
	if got := string(buf[:n]); got != "ok" {
		t.Fatalf("read second uni payload = %q, want %q", got, "ok")
	}
}

func TestPeerGoAwayLimitsProvisionalOpenAdmission(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := server.GoAway(4, 0); err != nil {
		t.Fatalf("server sessionControl: %v", err)
	}
	awaitPeerGoAwayBidi(t, client, 4)

	first, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open first stream: %v", err)
	}
	if first == nil {
		t.Fatal("expected first provisional stream")
	}

	_, err = client.OpenStream(ctx)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("second open err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("second open error code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}
}

func TestProvisionalOpenHardCapFailsNewest(t *testing.T) {
	t.Parallel()
	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	for i := 0; i < provisionalOpenHardCap; i++ {
		stream, err := client.OpenStream(ctx)
		if err != nil {
			t.Fatalf("open provisional stream %d: %v", i, err)
		}
		if got := stream.StreamID(); got != 0 {
			t.Fatalf("provisional stream %d StreamID = %d, want 0", i, got)
		}
	}

	_, err := client.OpenStream(ctx)
	if !errors.Is(err, ErrOpenLimited) {
		t.Fatalf("overflow open err = %v, want %v", err, ErrOpenLimited)
	}
}

func TestExpiredProvisionalOpenReleasedWithoutConsumingID(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	first, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open first stream: %v", err)
	}
	firstImpl := requireNativeStreamImpl(t, first)
	client.mu.Lock()
	firstImpl.setProvisionalCreated(time.Now().Add(-provisionalOpenMaxAge - time.Second))
	client.mu.Unlock()

	second, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open second stream: %v", err)
	}
	if got := second.StreamID(); got != 0 {
		t.Fatalf("second provisional StreamID = %d, want 0", got)
	}

	_, err = first.Write([]byte("x"))
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expired provisional write err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeCancelled) {
		t.Fatalf("expired provisional write code = %d, want %d", appErr.Code, uint64(CodeCancelled))
	}
	if appErr.Reason != ErrOpenExpired.Error() {
		t.Fatalf("expired provisional write reason = %q, want %q", appErr.Reason, ErrOpenExpired.Error())
	}
	if !errors.Is(err, ErrOpenExpired) {
		t.Fatalf("expired provisional write err = %v, want errors.Is(..., %v)", err, ErrOpenExpired)
	}

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		s, _ := server.AcceptStream(ctx)
		acceptCh <- mustNativeStreamImpl(s)
	}()

	if _, err := second.Write([]byte("y")); err != nil {
		t.Fatalf("write second stream: %v", err)
	}
	if got := second.StreamID(); got != 4 {
		t.Fatalf("second committed StreamID = %d, want 4", got)
	}

	accepted := <-acceptCh
	if accepted == nil {
		t.Fatal("expected accepted stream")
	}
	if got := accepted.StreamID(); got != 4 {
		t.Fatalf("accepted StreamID = %d, want 4", got)
	}
}

func TestLastValidLocalStreamIDCommitsWithoutReuseThenNextOpenFails(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	ctx, cancel := testContext(t)
	defer cancel()

	last := state.MaxStreamIDForClass(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))
	c.mu.Lock()
	c.registry.nextLocalBidi = last
	c.mu.Unlock()

	stream, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open at last valid local stream id: %v", err)
	}
	if stream.StreamID() != 0 {
		t.Fatalf("provisional StreamID at last slot = %d, want 0", stream.StreamID())
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("commit last valid local stream id: %v", err)
	}
	if got := stream.StreamID(); got != last {
		t.Fatalf("committed StreamID = %d, want %d", got, last)
	}
	frame := awaitQueuedFrame(t, frames)
	if frame.StreamID != last {
		t.Fatalf("queued opening frame stream id = %d, want %d", frame.StreamID, last)
	}

	c.mu.Lock()
	next := c.registry.nextLocalBidi
	c.mu.Unlock()
	if next != last+4 {
		t.Fatalf("nextLocalBidi after last-slot commit = %d, want %d", next, last+4)
	}

	_, err = c.OpenStream(ctx)
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("open after last valid local stream id err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	if len(c.queues.provisionalBidi.items) != 0 || c.queues.provisionalBidi.count != 0 {
		t.Fatalf("provisional bidi queue after exhausted open = len(%d) count(%d), want empty", len(c.queues.provisionalBidi.items), c.queues.provisionalBidi.count)
	}
	if c.registry.streams[last] == nil {
		t.Fatalf("committed last-slot stream %d missing after exhausted-open rejection", last)
	}
	c.mu.Unlock()
}

func TestProjectedLocalOpenExhaustionFailsBeforeCreatingGap(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	ctx, cancel := testContext(t)
	defer cancel()

	last := state.MaxStreamIDForClass(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))
	c.mu.Lock()
	c.registry.nextLocalBidi = last - 4
	c.mu.Unlock()

	first, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("first provisional near exhaustion: %v", err)
	}
	second, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("second provisional at last valid projected slot: %v", err)
	}
	if first.StreamID() != 0 || second.StreamID() != 0 {
		t.Fatalf("provisional StreamIDs = (%d,%d), want (0,0)", first.StreamID(), second.StreamID())
	}

	_, err = c.OpenStream(ctx)
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("third provisional beyond projected stream id range err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	if got := c.provisionalCountLocked(streamArityBidi); got != 2 {
		t.Fatalf("provisional bidi count after projected exhaustion = %d, want 2", got)
	}
	if head := c.provisionalHeadLocked(streamArityBidi); !head.found() || head.stream != first {
		t.Fatal("first provisional no longer heads the queue after projected exhaustion rejection")
	}
	if c.registry.nextLocalBidi != last-4 {
		t.Fatalf("nextLocalBidi after projected exhaustion rejection = %d, want %d", c.registry.nextLocalBidi, last-4)
	}
	c.mu.Unlock()
}

func TestCorruptedNextLocalBidiPeerOwnedIDRejectedBeforeCreatingProvisional(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	ctx, cancel := testContext(t)
	defer cancel()

	peerOwnedBidi := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)

	c.mu.Lock()
	c.registry.nextLocalBidi = peerOwnedBidi
	c.mu.Unlock()

	_, err := c.OpenStream(ctx)
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("open with peer-owned nextLocalBidi err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.provisionalCountLocked(streamArityBidi); got != 0 {
		t.Fatalf("provisional bidi count after peer-owned rejection = %d, want 0", got)
	}
	if len(c.registry.streams) != 0 {
		t.Fatalf("live streams after peer-owned rejection = %d, want 0", len(c.registry.streams))
	}
	if c.registry.nextLocalBidi != peerOwnedBidi {
		t.Fatalf("nextLocalBidi after peer-owned rejection = %d, want %d", c.registry.nextLocalBidi, peerOwnedBidi)
	}
}

func TestCorruptedNextLocalUniWrongClassRejectedBeforeCreatingProvisional(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	ctx, cancel := testContext(t)
	defer cancel()

	wrongClassID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)

	c.mu.Lock()
	c.registry.nextLocalUni = wrongClassID
	c.mu.Unlock()

	_, err := c.OpenUniStream(ctx)
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("open uni with bidi nextLocalUni err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.provisionalCountLocked(streamArityUni); got != 0 {
		t.Fatalf("provisional uni count after wrong-class rejection = %d, want 0", got)
	}
	if len(c.registry.streams) != 0 {
		t.Fatalf("live streams after wrong-class rejection = %d, want 0", len(c.registry.streams))
	}
	if c.registry.nextLocalUni != wrongClassID {
		t.Fatalf("nextLocalUni after wrong-class rejection = %d, want %d", c.registry.nextLocalUni, wrongClassID)
	}
}

func TestLaterProvisionalWriteWaitsForEarlierSameClassCommit(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	ctx, cancel := testContext(t)
	defer cancel()

	first, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open first provisional: %v", err)
	}
	second, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open second provisional: %v", err)
	}

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline second: %v", err)
	}
	if _, err := second.Write([]byte("b")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("second write err = %v, want deadline exceeded", err)
	}
	assertNoQueuedFrame(t, frames)
	if got := second.StreamID(); got != 0 {
		t.Fatalf("second provisional StreamID = %d, want 0 while earlier head is pending", got)
	}

	refreshProvisionalCreated(t, first, second)
	if _, err := first.Write([]byte("a")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if got := first.StreamID(); got != 4 {
		t.Fatalf("first StreamID = %d, want 4", got)
	}

	refreshProvisionalCreated(t, second)
	if err := second.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear second write deadline: %v", err)
	}
	if _, err := second.Write([]byte("b")); err != nil {
		t.Fatalf("second write after first commit: %v", err)
	}
	if got := second.StreamID(); got != 8 {
		t.Fatalf("second StreamID = %d, want 8 after earlier commit", got)
	}

	firstFrame := awaitQueuedFrame(t, frames)
	secondFrame := awaitQueuedFrame(t, frames)
	if firstFrame.Type != FrameTypeDATA || firstFrame.StreamID != 4 || string(firstFrame.Payload) != "a" {
		t.Fatalf("first queued frame = %+v, want DATA stream 4 payload a", firstFrame)
	}
	if secondFrame.Type != FrameTypeDATA || secondFrame.StreamID != 8 || string(secondFrame.Payload) != "b" {
		t.Fatalf("second queued frame = %+v, want DATA stream 8 payload b", secondFrame)
	}
}

func TestLaterProvisionalWriteUsesFirstIDAfterEarlierCancel(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	ctx, cancel := testContext(t)
	defer cancel()

	first, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open first provisional: %v", err)
	}
	second, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open second provisional: %v", err)
	}

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline second: %v", err)
	}
	if _, err := second.Write([]byte("b")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("second write err = %v, want deadline exceeded", err)
	}
	assertNoQueuedFrame(t, frames)

	if err := first.CloseWithError(uint64(CodeCancelled), ""); err != nil {
		t.Fatalf("cancel first provisional: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	refreshProvisionalCreated(t, second)
	if err := second.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear second write deadline: %v", err)
	}
	if _, err := second.Write([]byte("b")); err != nil {
		t.Fatalf("second write after earlier cancel: %v", err)
	}
	if got := second.StreamID(); got != 4 {
		t.Fatalf("second StreamID = %d, want 4 after earlier cancel", got)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeDATA || frame.StreamID != 4 || string(frame.Payload) != "b" {
		t.Fatalf("queued frame = %+v, want DATA stream 4 payload b", frame)
	}
}

func TestLaterProvisionalCloseWriteWaitsForEarlierSameClassHead(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	ctx, cancel := testContext(t)
	defer cancel()

	first, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open first provisional: %v", err)
	}
	second, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open second provisional: %v", err)
	}

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline second: %v", err)
	}
	if err := second.CloseWrite(); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("second CloseWrite err = %v, want deadline exceeded", err)
	}
	assertNoQueuedFrame(t, frames)

	refreshProvisionalCreated(t, first, second)
	if _, err := first.Write([]byte("a")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	refreshProvisionalCreated(t, second)
	if err := second.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear second write deadline: %v", err)
	}
	if err := second.CloseWrite(); err != nil {
		t.Fatalf("second CloseWrite after first commit: %v", err)
	}
	if got := second.StreamID(); got != 8 {
		t.Fatalf("second StreamID = %d, want 8 after earlier commit", got)
	}

	firstFrame := awaitQueuedFrame(t, frames)
	secondFrame := awaitQueuedFrame(t, frames)
	if firstFrame.Type != FrameTypeDATA || firstFrame.StreamID != 4 || string(firstFrame.Payload) != "a" {
		t.Fatalf("first queued frame = %+v, want DATA stream 4 payload a", firstFrame)
	}
	if secondFrame.Type != FrameTypeDATA || secondFrame.StreamID != 8 || secondFrame.Flags&FrameFlagFIN == 0 || len(secondFrame.Payload) != 0 {
		t.Fatalf("second queued frame = %+v, want DATA|FIN stream 8 empty payload", secondFrame)
	}
}

func TestPeerDataOpenerRefusedByIncomingLimitDoesNotAccumulateLateData(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)

	c.mu.Lock()
	c.config.local.Settings.MaxIncomingStreamsBidi = 0
	c.ingress.aggregateLateData = 3
	c.ingress.hiddenUnreadBytesDiscarded = 5
	beforeAdvertised := c.flow.recvSessionAdvertised
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("x"),
	}); err != nil {
		t.Fatalf("handle DATA on refused peer opener: %v", err)
	}

	assertInvalidQueuedAbortCode(t, frames, streamID, CodeRefusedStream)
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ingress.aggregateLateData != 3 {
		t.Fatalf("aggregateLateData = %d, want 3", c.ingress.aggregateLateData)
	}
	if c.ingress.hiddenUnreadBytesDiscarded != 5 {
		t.Fatalf("hiddenUnreadBytesDiscarded = %d, want 5", c.ingress.hiddenUnreadBytesDiscarded)
	}
	if c.flow.recvSessionAdvertised != beforeAdvertised {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, beforeAdvertised)
	}
	if len(c.registry.streams) != 0 {
		t.Fatalf("live streams after refused DATA opener = %d, want 0", len(c.registry.streams))
	}
	if c.registry.nextPeerBidi != streamID+4 {
		t.Fatalf("nextPeerBidi = %d, want %d", c.registry.nextPeerBidi, streamID+4)
	}
	if !c.hasTerminalMarkerLocked(streamID) {
		t.Fatalf("refused DATA opener %d missing terminal marker", streamID)
	}
}

func TestPeerAbortOpenerRefusedByIncomingLimitSkipsNoOpControlSideEffects(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)

	c.mu.Lock()
	c.config.local.Settings.MaxIncomingStreamsBidi = 0
	c.abuse.noopControlCount = 7
	c.ingress.hiddenStreamsRefused = 11
	c.mu.Unlock()

	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle ABORT on refused peer opener: %v", err)
	}

	assertInvalidQueuedAbortCode(t, frames, streamID, CodeRefusedStream)
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abuse.noopControlCount != 7 {
		t.Fatalf("noopControlCount = %d, want 7", c.abuse.noopControlCount)
	}
	if c.ingress.hiddenStreamsRefused != 12 {
		t.Fatalf("hiddenStreamsRefused = %d, want 12", c.ingress.hiddenStreamsRefused)
	}
	if len(c.registry.streams) != 0 {
		t.Fatalf("live streams after refused ABORT opener = %d, want 0", len(c.registry.streams))
	}
	if c.registry.nextPeerBidi != streamID+4 {
		t.Fatalf("nextPeerBidi = %d, want %d", c.registry.nextPeerBidi, streamID+4)
	}
	if !c.hasTerminalMarkerLocked(streamID) {
		t.Fatalf("refused ABORT opener %d missing terminal marker", streamID)
	}
}

func TestRefusedPeerDataOpenerSkipsPayloadParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		configure          func(*Conn)
		wantTerminalMarker bool
	}{
		{
			name: "incoming_limit",
			configure: func(c *Conn) {
				c.config.local.Settings.MaxIncomingStreamsBidi = 0
			},
			wantTerminalMarker: true,
		},
		{
			name: "local_goaway",
			configure: func(c *Conn) {
				c.sessionControl.localGoAwayBidi = 0
				c.sessionControl.localGoAwayUni = 0
			},
			wantTerminalMarker: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, frames, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
			defer stop()

			streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)

			c.mu.Lock()
			tc.configure(c)
			c.mu.Unlock()

			if err := c.handleDataFrame(Frame{
				Type:     FrameTypeDATA,
				Flags:    FrameFlagOpenMetadata,
				StreamID: streamID,
				Payload:  []byte{0xff},
			}); err != nil {
				t.Fatalf("handle malformed refused DATA opener: %v", err)
			}

			assertInvalidQueuedAbortCode(t, frames, streamID, CodeRefusedStream)
			assertNoQueuedFrame(t, frames)

			c.mu.Lock()
			defer c.mu.Unlock()
			if len(c.registry.streams) != 0 {
				t.Fatalf("live streams after refused malformed DATA opener = %d, want 0", len(c.registry.streams))
			}
			if got := c.hasTerminalMarkerLocked(streamID); got != tc.wantTerminalMarker {
				t.Fatalf("terminal marker present = %t, want %t", got, tc.wantTerminalMarker)
			}
		})
	}
}

func TestOpenMetadataCarriesPriorityAndGroup(t *testing.T) {
	clientCfg := DefaultConfig()
	serverCfg := DefaultConfig()
	clientCfg.Capabilities |= CapabilityOpenMetadata | CapabilityPriorityHints | CapabilityStreamGroups
	serverCfg.Capabilities |= CapabilityOpenMetadata | CapabilityPriorityHints | CapabilityStreamGroups

	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := acceptStreamAsync(ctx, server)

	priority := uint64(2)
	group := uint64(7)
	stream, err := client.OpenStreamWithOptions(ctx, OpenOptions{
		InitialPriority: &priority,
		InitialGroup:    &group,
		OpenInfo:        []byte("ssh"),
	})
	if err != nil {
		t.Fatalf("open stream with metadata: %v", err)
	}
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	accepted := requireAcceptedStream(t, acceptCh)
	meta := accepted.Metadata()
	if meta.Priority != priority {
		t.Fatalf("priority = %d, want %d", meta.Priority, priority)
	}
	if meta.Group == nil || *meta.Group != group {
		t.Fatalf("group = %v, want %d", meta.Group, group)
	}
	if got := string(meta.OpenInfo); got != "ssh" {
		t.Fatalf("open info = %q, want %q", got, "ssh")
	}

	buf := make([]byte, 8)
	n, err := accepted.Read(buf)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("read payload = %q, want %q", got, "hi")
	}
	if _, err := accepted.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("second read err = %v, want EOF", err)
	}
}

func TestUpdateMetadataBeforeOpenUsesOpeningMetadata(t *testing.T) {
	clientCfg := DefaultConfig()
	serverCfg := DefaultConfig()
	clientCfg.Capabilities |= CapabilityOpenMetadata | CapabilityPriorityHints | CapabilityStreamGroups
	serverCfg.Capabilities |= CapabilityOpenMetadata | CapabilityPriorityHints | CapabilityStreamGroups

	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := acceptStreamAsync(ctx, server)

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	priority := uint64(5)
	group := uint64(11)
	if err := stream.UpdateMetadata(MetadataUpdate{Priority: &priority, Group: &group}); err != nil {
		t.Fatalf("pre-open update metadata: %v", err)
	}
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}

	accepted := requireAcceptedStream(t, acceptCh)
	meta := accepted.Metadata()
	if meta.Priority != priority {
		t.Fatalf("priority = %d, want %d", meta.Priority, priority)
	}
	if meta.Group == nil || *meta.Group != group {
		t.Fatalf("group = %v, want %d", meta.Group, group)
	}
}

func TestUpdateMetadataBeforeOpenOverflowDoesNotMutateLocalState(t *testing.T) {
	caps := CapabilityOpenMetadata | CapabilityPriorityHints | CapabilityStreamGroups
	maxFramePayload := uint64(12)
	priority := uint64(5)
	group := uint64(11)

	var (
		openInfo []byte
		prefix   []byte
	)
	for n := 1; n <= 64; n++ {
		candidate := bytes.Repeat([]byte("x"), n)
		var err error
		prefix, err = buildOpenMetadataPrefix(caps, OpenOptions{OpenInfo: candidate}, maxFramePayload)
		if err != nil {
			continue
		}
		_, err = buildOpenMetadataPrefixFromCurrent(caps, &priority, &group, candidate, maxFramePayload)
		if errors.Is(err, ErrOpenMetadataTooLarge) {
			openInfo = candidate
			break
		}
	}
	if len(openInfo) == 0 {
		t.Fatal("failed to find open metadata setup that overflows only after metadata update")
	}

	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{negotiated: Negotiated{Capabilities: caps},
			peer: Preface{Settings: Settings{
				MaxFramePayload: maxFramePayload,
				SchedulerHints:  SchedulerGroupFair,
			}}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{OpenInfo: openInfo},
		prefix,
	)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	originalPrefix := append([]byte(nil), stream.openMetadataPrefix...)
	c.mu.Unlock()

	err := stream.UpdateMetadata(MetadataUpdate{Priority: &priority, Group: &group})
	if !errors.Is(err, ErrOpenMetadataTooLarge) {
		t.Fatalf("UpdateMetadata err = %v, want %v", err, ErrOpenMetadataTooLarge)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.priority != 0 {
		t.Fatalf("priority = %d, want 0 after failed pre-open update", stream.priority)
	}
	if stream.initialPrioritySet {
		t.Fatal("initialPrioritySet = true, want false after failed pre-open update")
	}
	if stream.group != 0 || stream.groupExplicit {
		t.Fatalf("group state = (%d,%v), want (0,false) after failed pre-open update", stream.group, stream.groupExplicit)
	}
	if stream.initialGroupSet {
		t.Fatal("initialGroupSet = true, want false after failed pre-open update")
	}
	if stream.groupTracked {
		t.Fatal("groupTracked = true, want false after failed pre-open update")
	}
	if len(c.writer.scheduler.ActiveGroupRefs) != 0 {
		t.Fatalf("tracked explicit groups = %d, want 0 after failed pre-open update", len(c.writer.scheduler.ActiveGroupRefs))
	}
	if !bytes.Equal(stream.openMetadataPrefix, originalPrefix) {
		t.Fatalf("openMetadataPrefix = %x, want unchanged %x", stream.openMetadataPrefix, originalPrefix)
	}
}

func TestUpdateMetadataBeforeOpenTightensOversizedOpenMetadataPrefixBacking(t *testing.T) {
	caps := CapabilityOpenMetadata | CapabilityPriorityHints
	c := newSessionMemoryTestConn()
	c.config.negotiated.Capabilities = caps
	c.config.peer.Settings.MaxFramePayload = 1024

	prefix, err := buildOpenMetadataPrefix(caps, OpenOptions{OpenInfo: []byte("ssh")}, c.config.peer.Settings.MaxFramePayload)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix err = %v", err)
	}
	oversized := make([]byte, len(prefix), 64)
	copy(oversized, prefix)

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, oversized)
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	priority := uint64(9)
	if err := stream.UpdateMetadata(MetadataUpdate{Priority: &priority}); err != nil {
		t.Fatalf("UpdateMetadata err = %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := cap(stream.openMetadataPrefix); got != len(stream.openMetadataPrefix) {
		t.Fatalf("cap(openMetadataPrefix) = %d, want tight cap %d", got, len(stream.openMetadataPrefix))
	}
}

func TestPriorityUpdateRoundTrip(t *testing.T) {
	clientCfg := DefaultConfig()
	serverCfg := DefaultConfig()
	clientCfg.Capabilities |= CapabilityPriorityUpdate | CapabilityPriorityHints | CapabilityStreamGroups
	serverCfg.Capabilities |= CapabilityPriorityUpdate | CapabilityPriorityHints | CapabilityStreamGroups

	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := acceptStreamAsync(ctx, server)

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}

	accepted := requireAcceptedStream(t, acceptCh)
	priority := uint64(9)
	group := uint64(13)
	if err := stream.UpdateMetadata(MetadataUpdate{Priority: &priority, Group: &group}); err != nil {
		t.Fatalf("update metadata: %v", err)
	}
	if _, err := stream.Write([]byte("!")); err != nil {
		t.Fatalf("write barrier data: %v", err)
	}
	got := make([]byte, 3)
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(got) != "hi!" {
		t.Fatalf("read payload = %q, want %q", string(got), "hi!")
	}
	meta := accepted.Metadata()
	if meta.Priority != priority || meta.Group == nil || *meta.Group != group {
		t.Fatalf("receiver metadata = %+v, want priority=%d group=%d", meta, priority, group)
	}
}

func TestPriorityUpdateUnavailableAfterOpen(t *testing.T) {
	clientCfg := DefaultConfig()
	serverCfg := DefaultConfig()
	clientCfg.Capabilities |= CapabilityPriorityHints
	serverCfg.Capabilities |= CapabilityPriorityHints

	client, _ := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}
	priority := uint64(3)
	if err := stream.UpdateMetadata(MetadataUpdate{Priority: &priority}); !errors.Is(err, ErrPriorityUpdateUnavailable) {
		t.Fatalf("update err = %v, want %v", err, ErrPriorityUpdateUnavailable)
	}
}

func TestPriorityUpdateIgnoredWhenUnnegotiated(t *testing.T) {
	clientCfg := DefaultConfig()
	serverCfg := DefaultConfig()
	clientCfg.Capabilities |= CapabilityPriorityHints | CapabilityStreamGroups
	serverCfg.Capabilities |= CapabilityPriorityHints | CapabilityStreamGroups

	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := acceptStreamAsync(ctx, server)

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	streamImpl := requireNativeStreamImpl(t, stream)
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}
	accepted := requireAcceptedStream(t, acceptCh)

	priority := uint64(7)
	payload, err := buildPriorityUpdatePayload(
		CapabilityPriorityUpdate|CapabilityPriorityHints,
		MetadataUpdate{Priority: &priority},
		client.config.peer.Settings.MaxExtensionPayloadBytes,
	)
	if err != nil {
		t.Fatalf("build priority update: %v", err)
	}
	if err := testQueueFrame(client, Frame{Type: FrameTypeEXT, StreamID: streamImpl.id, Payload: payload}); err != nil {
		t.Fatalf("queue priority update: %v", err)
	}
	if _, err := stream.Write([]byte("!")); err != nil {
		t.Fatalf("write barrier data: %v", err)
	}
	got := make([]byte, 3)
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(got) != "hi!" {
		t.Fatalf("read payload = %q, want %q", string(got), "hi!")
	}
	meta := accepted.Metadata()
	if meta.Priority != 0 {
		t.Fatalf("priority = %d, want 0", meta.Priority)
	}
	if meta.Group != nil {
		t.Fatalf("group = %v, want nil", *meta.Group)
	}
}

func TestPriorityUpdateDuplicateSingletonIgnored(t *testing.T) {
	clientCfg := DefaultConfig()
	serverCfg := DefaultConfig()
	clientCfg.Capabilities |= CapabilityPriorityUpdate | CapabilityPriorityHints
	serverCfg.Capabilities |= CapabilityPriorityUpdate | CapabilityPriorityHints

	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := acceptStreamAsync(ctx, server)

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	streamImpl := requireNativeStreamImpl(t, stream)
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}
	accepted := requireAcceptedStream(t, acceptCh)

	var payload []byte
	payload, err = AppendVarint(payload, uint64(EXTPriorityUpdate))
	if err != nil {
		t.Fatalf("append ext subtype: %v", err)
	}
	payload, err = AppendTLV(payload, uint64(MetadataStreamPriority), mustEncodeVarint(2))
	if err != nil {
		t.Fatalf("append first priority tlv: %v", err)
	}
	payload, err = AppendTLV(payload, uint64(MetadataStreamPriority), mustEncodeVarint(3))
	if err != nil {
		t.Fatalf("append duplicate priority tlv: %v", err)
	}
	if err := testQueueFrame(client, Frame{Type: FrameTypeEXT, StreamID: streamImpl.id, Payload: payload}); err != nil {
		t.Fatalf("queue duplicate priority update: %v", err)
	}
	if _, err := stream.Write([]byte("!")); err != nil {
		t.Fatalf("write barrier data: %v", err)
	}
	got := make([]byte, 3)
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(got) != "hi!" {
		t.Fatalf("read payload = %q, want %q", string(got), "hi!")
	}
	meta := accepted.Metadata()
	if meta.Priority != 0 {
		t.Fatalf("priority = %d, want 0", meta.Priority)
	}
	if got := server.Stats().Diagnostics.DroppedPriorityUpdates; got != 1 {
		t.Fatalf("dropped priority updates = %d, want 1", got)
	}
}

func TestPriorityUpdateIgnoresOpenInfoTLV(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenVisible(stream)

	var payload []byte
	var err error
	payload, err = AppendVarint(payload, uint64(EXTPriorityUpdate))
	if err != nil {
		t.Fatalf("append ext subtype: %v", err)
	}
	payload, err = AppendTLV(payload, uint64(MetadataOpenInfo), []byte("ssh"))
	if err != nil {
		t.Fatalf("append open_info tlv: %v", err)
	}
	payload, err = AppendTLV(payload, uint64(MetadataStreamPriority), mustEncodeVarint(5))
	if err != nil {
		t.Fatalf("append priority tlv: %v", err)
	}

	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}); err != nil {
		t.Fatalf("handle PRIORITY_UPDATE with open_info: %v", err)
	}

	meta := stream.Metadata()
	if meta.Priority != 5 {
		t.Fatalf("priority = %d, want 5", meta.Priority)
	}
	if meta.Group != nil {
		t.Fatalf("group = %v, want nil", meta.Group)
	}
	if got := string(meta.OpenInfo); got != "" {
		t.Fatalf("open_info = %q, want empty", got)
	}
}

func TestPriorityUpdateIgnoresUnknownTLV(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenVisible(stream)

	var payload []byte
	var err error
	payload, err = AppendVarint(payload, uint64(EXTPriorityUpdate))
	if err != nil {
		t.Fatalf("append ext subtype: %v", err)
	}
	payload, err = AppendTLV(payload, 99, []byte{0xaa, 0xbb})
	if err != nil {
		t.Fatalf("append unknown tlv: %v", err)
	}
	payload, err = AppendTLV(payload, uint64(MetadataStreamPriority), mustEncodeVarint(7))
	if err != nil {
		t.Fatalf("append priority tlv: %v", err)
	}

	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}); err != nil {
		t.Fatalf("handle PRIORITY_UPDATE with unknown tlv: %v", err)
	}

	meta := stream.Metadata()
	if meta.Priority != 7 {
		t.Fatalf("priority = %d, want 7", meta.Priority)
	}
	if meta.Group != nil {
		t.Fatalf("group = %v, want nil", meta.Group)
	}
}

func TestPriorityUpdateIgnoredUntilLocalOpenBecomesPeerVisible(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	c := &Conn{

		config: connConfigState{negotiated: Negotiated{Capabilities: caps},
			peer: Preface{Settings: Settings{
				MaxExtensionPayloadBytes: 4096,
			}}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := &nativeStream{
		conn:         c,
		id:           11,
		idSet:        true,
		bidi:         true,
		localOpen:    testLocalOpenOpenedCommittedState(),
		localSend:    true,
		localReceive: true,
		priority:     1,
	}
	stream.initHalfStates()
	c.registry.streams[stream.id] = stream

	priority := uint64(9)
	payload, err := buildPriorityUpdatePayload(caps, MetadataUpdate{Priority: &priority}, 4096)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}); err != nil {
		t.Fatalf("handleExtFrame(PRIORITY_UPDATE): %v", err)
	}

	if stream.priority != 1 {
		t.Fatalf("priority = %d, want unchanged 1 before peer-visible open", stream.priority)
	}
	if stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = true, want false while peer has not seen any opening frame")
	}
}

func TestOpenMetadataIgnoresUnknownMetadataTLV(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, CapabilityOpenMetadata|CapabilityPriorityHints)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)

	var metadata []byte
	var err error
	metadata, err = AppendTLV(metadata, 99, []byte{0x01, 0x02})
	if err != nil {
		t.Fatalf("append unknown metadata tlv: %v", err)
	}
	metadata, err = AppendTLV(metadata, uint64(MetadataOpenInfo), []byte("ssh"))
	if err != nil {
		t.Fatalf("append open_info tlv: %v", err)
	}
	metadata, err = AppendTLV(metadata, uint64(MetadataStreamPriority), mustEncodeVarint(2))
	if err != nil {
		t.Fatalf("append priority tlv: %v", err)
	}

	var payload []byte
	payload, err = AppendVarint(payload, uint64(len(metadata)))
	if err != nil {
		t.Fatalf("append metadata len: %v", err)
	}
	payload = append(payload, metadata...)
	payload = append(payload, []byte("hi")...)

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagOpenMetadata,
		StreamID: streamID,
		Payload:  payload,
	}); err != nil {
		t.Fatalf("handle DATA|OPEN_METADATA with unknown metadata tlv: %v", err)
	}

	stream := c.registry.streams[streamID]
	if stream == nil {
		t.Fatal("expected stream to open")
	}
	meta := stream.Metadata()
	if meta.Priority != 2 {
		t.Fatalf("priority = %d, want 2", meta.Priority)
	}
	if got := string(meta.OpenInfo); got != "ssh" {
		t.Fatalf("open_info = %q, want %q", got, "ssh")
	}
	if got := string(stream.readBuf); got != "hi" {
		t.Fatalf("readBuf = %q, want %q", got, "hi")
	}
}

func TestQueuePriorityUpdateAsyncKeepsLatestPerStream(t *testing.T) {
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	first, err := buildPriorityUpdatePayload(
		CapabilityPriorityUpdate|CapabilityPriorityHints,
		MetadataUpdate{Priority: invalidUint64Ptr(2)},
		4096,
	)
	if err != nil {
		t.Fatalf("build first priority update: %v", err)
	}
	second, err := buildPriorityUpdatePayload(
		CapabilityPriorityUpdate|CapabilityPriorityHints,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build second priority update: %v", err)
	}

	ensureTestPendingStream(c, 9)
	c.queuePriorityUpdateAsync(9, first, retainedBytesBorrowed)
	c.queuePriorityUpdateAsync(9, second, retainedBytesBorrowed)

	got, _ := testPendingPriorityUpdatePayload(c, 9)
	if !bytes.Equal(got, second) {
		t.Fatalf("pending priority update = %x, want latest %x", got, second)
	}

	urgent, advisory := testDrainPendingControlFrames(c)
	if len(urgent) != 0 {
		t.Fatalf("drained urgent frames = %d, want 0", len(urgent))
	}
	if len(advisory) != 1 {
		t.Fatalf("drained advisory frames = %d, want 1", len(advisory))
	}
	if advisory[0].Type != FrameTypeEXT || advisory[0].StreamID != 9 {
		t.Fatalf("drained frame = %+v, want EXT for stream 9", advisory[0])
	}
	if !bytes.Equal(advisory[0].Payload, second) {
		t.Fatalf("drained payload = %x, want latest %x", advisory[0].Payload, second)
	}
}

func TestUpdateMetadataAfterOpenQueuesPendingPriorityUpdate(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints | CapabilityStreamGroups
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{negotiated: Negotiated{
			Capabilities: caps,
		},
			peer: Preface{
				Settings: Settings{
					MaxExtensionPayloadBytes: 4096,
				},
			}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	s := testVisibleBidiStream(c, 11)

	priority := uint64(9)
	group := uint64(13)
	if err := s.UpdateMetadata(MetadataUpdate{Priority: &priority, Group: &group}); err != nil {
		t.Fatalf("update metadata: %v", err)
	}

	if s.priority != priority {
		t.Fatalf("local priority = %d, want %d", s.priority, priority)
	}
	if !s.groupExplicit || s.group != group {
		t.Fatalf("local group = (%v,%d), want explicit %d", s.groupExplicit, s.group, group)
	}

	payload, ok := testPendingPriorityUpdatePayload(c, s.id)
	if !ok {
		t.Fatal("expected pending priority update")
	}
	want, err := buildPriorityUpdatePayload(caps, MetadataUpdate{Priority: &priority, Group: &group}, 4096)
	if err != nil {
		t.Fatalf("build expected priority update: %v", err)
	}
	if !bytes.Equal(payload, want) {
		t.Fatalf("pending payload = %x, want %x", payload, want)
	}
}

func TestUpdateMetadataWhileOpeningBarrierRetainsPendingPriorityUpdate(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints | CapabilityStreamGroups
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{negotiated: Negotiated{
			Capabilities: caps,
		},
			peer: Preface{
				Settings: Settings{
					MaxExtensionPayloadBytes: 4096,
				},
			}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	s := testBuildStream(
		c,
		11,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenQueuedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)

	priority := uint64(9)
	group := uint64(13)
	if err := s.UpdateMetadata(MetadataUpdate{Priority: &priority, Group: &group}); err != nil {
		t.Fatalf("update metadata while opening barrier: %v", err)
	}

	if s.priority != priority {
		t.Fatalf("local priority = %d, want %d", s.priority, priority)
	}
	if !s.groupExplicit || s.group != group {
		t.Fatalf("local group = (%v,%d), want explicit %d", s.groupExplicit, s.group, group)
	}

	payload, ok := testPendingPriorityUpdatePayload(c, s.id)
	if !ok {
		t.Fatal("expected pending priority update while opening barrier is active")
	}
	want, err := buildPriorityUpdatePayload(caps, MetadataUpdate{Priority: &priority, Group: &group}, 4096)
	if err != nil {
		t.Fatalf("build expected priority update: %v", err)
	}
	if !bytes.Equal(payload, want) {
		t.Fatalf("pending payload = %x, want %x", payload, want)
	}

	c.mu.Lock()
	c.markPeerVisibleLocked(s)
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 0 {
		t.Fatalf("drained urgent frames = %d, want 0", len(urgent))
	}
	if len(advisory) != 1 {
		t.Fatalf("drained advisory frames = %d, want 1", len(advisory))
	}
	if advisory[0].Type != FrameTypeEXT || advisory[0].StreamID != s.id {
		t.Fatalf("drained frame = %+v, want EXT for stream %d", advisory[0], s.id)
	}
	if !bytes.Equal(advisory[0].Payload, want) {
		t.Fatalf("drained payload = %x, want %x", advisory[0].Payload, want)
	}
}

func TestUpdateMetadataAfterCommittedInvisibleOpenUsesOpeningMetadata(t *testing.T) {
	caps := CapabilityOpenMetadata | CapabilityPriorityUpdate | CapabilityPriorityHints
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{negotiated: Negotiated{
			Capabilities: caps,
		},
			peer: Preface{
				Settings: Settings{
					MaxFramePayload:          4096,
					MaxExtensionPayloadBytes: 4096,
				},
			}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	initialOpts := OpenOptions{OpenInfo: []byte("ssh")}
	initialPrefix, err := buildOpenMetadataPrefix(caps, initialOpts, 4096)
	if err != nil {
		t.Fatalf("build initial open metadata prefix: %v", err)
	}

	c.mu.Lock()
	s := c.newLocalStreamLocked(11, streamArityBidi, initialOpts, initialPrefix)
	testMarkLocalOpenCommitted(s)
	s.localOpen.phase = state.LocalOpenPhaseNone
	c.registry.streams[s.id] = s
	c.mu.Unlock()

	priority := uint64(9)
	if err := s.UpdateMetadata(MetadataUpdate{Priority: &priority}); err != nil {
		t.Fatalf("update metadata before first peer-visible frame: %v", err)
	}

	if s.priority != priority {
		t.Fatalf("local priority = %d, want %d", s.priority, priority)
	}
	if got := testPendingPriorityUpdateCount(c); got != 0 {
		t.Fatalf("pendingPriorityUpdate count = %d, want 0 while opener still needs first peer-visible frame", got)
	}

	wantPrefix, err := buildOpenMetadataPrefix(caps, OpenOptions{
		InitialPriority: &priority,
		OpenInfo:        []byte("ssh"),
	}, 4096)
	if err != nil {
		t.Fatalf("build expected open metadata prefix: %v", err)
	}
	if !bytes.Equal(s.openMetadataPrefix, wantPrefix) {
		t.Fatalf("openMetadataPrefix = %x, want %x", s.openMetadataPrefix, wantPrefix)
	}
}

func TestUpdateMetadataAfterCommittedInvisibleOpenFallsBackToPendingPriorityUpdateWithoutOpenCarriage(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{negotiated: Negotiated{
			Capabilities: caps,
		},
			peer: Preface{
				Settings: Settings{
					MaxFramePayload:          4096,
					MaxExtensionPayloadBytes: 4096,
				},
			}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	s := testBuildStream(c, 11,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenOpenedCommittedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)

	priority := uint64(9)
	if err := s.UpdateMetadata(MetadataUpdate{Priority: &priority}); err != nil {
		t.Fatalf("update metadata before first peer-visible frame without open carriage: %v", err)
	}

	if s.priority != priority {
		t.Fatalf("local priority = %d, want %d", s.priority, priority)
	}
	if got := testPendingPriorityUpdateCount(c); got != 1 {
		t.Fatalf("pendingPriorityUpdate count = %d, want 1 when opener cannot carry update", got)
	}

	want, err := buildPriorityUpdatePayload(caps, MetadataUpdate{Priority: &priority}, 4096)
	if err != nil {
		t.Fatalf("build expected priority update payload: %v", err)
	}
	if got, _ := testPendingPriorityUpdatePayload(c, s.id); !bytes.Equal(got, want) {
		t.Fatalf("pendingPriorityUpdate payload = %x, want %x", got, want)
	}
}

func TestPendingPriorityUpdateWaitsForPeerVisibleAfterCommittedInvisibleOpen(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{negotiated: Negotiated{
			Capabilities: caps,
		},
			peer: Preface{
				Settings: Settings{
					MaxExtensionPayloadBytes: 4096,
				},
			}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	s := testBuildStream(c, 11,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenOpenedCommittedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)

	priority := uint64(9)
	payload, err := buildPriorityUpdatePayload(caps, MetadataUpdate{Priority: &priority}, 4096)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c.mu.Lock()
	testSetPendingPriorityUpdate(c, s.id, append([]byte(nil), payload...))
	urgent, advisory := testDrainPendingControlFrames(c)
	stored, _ := testPendingPriorityUpdatePayload(c, s.id)
	gotPayload := append([]byte(nil), stored...)
	c.mu.Unlock()

	if len(urgent) != 0 {
		t.Fatalf("drained urgent frames = %d, want 0", len(urgent))
	}
	if len(advisory) != 0 {
		t.Fatalf("drained advisory frames = %d, want 0 while stream is still peer-invisible", len(advisory))
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("pending payload = %x, want %x", gotPayload, payload)
	}
}

func newControlBudgetTestConn() *Conn {
	settings := DefaultSettings()
	return &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}},
	}
}

func newPriorityBudgetTestConn() *Conn {
	settings := DefaultSettings()
	return &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
}

func newPendingControlTestStream(c *Conn) *nativeStream {
	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	testMarkLocalOpenVisible(stream)
	c.registry.streams[streamID] = stream
	return stream
}

func TestQueuePriorityUpdateDropsWhenBudgetExceeded(t *testing.T) {
	c := newPriorityBudgetTestConn()
	budget := c.pendingPriorityBudgetLocked()
	first := bytes.Repeat([]byte{1}, int(budget/2)+1)
	second := bytes.Repeat([]byte{2}, int(budget/2)+1)

	ensureTestPendingStream(c, 1)
	ensureTestPendingStream(c, 2)
	c.queuePriorityUpdateAsync(1, first, retainedBytesBorrowed)
	c.queuePriorityUpdateAsync(2, second, retainedBytesBorrowed)

	if !testHasPendingPriorityUpdate(c, 1) {
		t.Fatal("first priority update was not queued")
	}
	if testHasPendingPriorityUpdate(c, 2) {
		t.Fatal("second priority update should have been dropped when budget was exceeded")
	}
	if got := c.pending.priorityBytes; got != uint64(len(first)) {
		t.Fatalf("pending priority bytes = %d, want %d", got, len(first))
	}
}

func TestTakePendingPriorityUpdateReleasesBudget(t *testing.T) {
	c := newPriorityBudgetTestConn()
	budget := c.pendingPriorityBudgetLocked()
	payload := bytes.Repeat([]byte{3}, int(budget/2)+1)

	ensureTestPendingStream(c, 7)
	c.queuePriorityUpdateAsync(7, payload, retainedBytesBorrowed)
	frame := c.takePendingPriorityUpdateFrameLocked(7)
	if !frame.hasFrame() {
		t.Fatal("expected pending priority update frame")
	}
	if frame.frame.Type != FrameTypeEXT || frame.frame.StreamID != 7 {
		t.Fatalf("frame = %+v, want EXT for stream 7", testPublicFrame(frame.frame))
	}
	if got := c.pending.priorityBytes; got != 0 {
		t.Fatalf("pending priority bytes after take = %d, want 0", got)
	}

	full := bytes.Repeat([]byte{4}, int(budget))
	ensureTestPendingStream(c, 9)
	c.queuePriorityUpdateAsync(9, full, retainedBytesBorrowed)
	if !testHasPendingPriorityUpdate(c, 9) {
		t.Fatal("priority update should fit after budget release")
	}
}

func TestQueuePriorityUpdateAsyncTightensOversizedOwnedPayload(t *testing.T) {
	c := newPriorityBudgetTestConn()
	streamID := uint64(11)
	payload := make([]byte, 3, 64)
	copy(payload, []byte{1, 2, 3})

	ensureTestPendingStream(c, streamID)
	c.queuePriorityUpdateAsync(streamID, payload, retainedBytesOwned)

	stored, ok := testPendingPriorityUpdatePayload(c, streamID)
	if !ok {
		t.Fatal("owned priority update was not queued")
	}
	if got := len(stored); got != 3 {
		t.Fatalf("len(stored) = %d, want 3", got)
	}
	if got := cap(stored); got != len(stored) {
		t.Fatalf("cap(stored) = %d, want tight cap %d", got, len(stored))
	}
	if got := c.pending.priorityBytes; got != uint64(len(stored)) {
		t.Fatalf("pendingPriorityBytes = %d, want %d", got, len(stored))
	}
}

func TestCloseWriteCarriesPendingPriorityUpdateBeforeFin(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(5)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest, 1), urgentWriteCh: make(chan writeRequest, 1)}, config: connConfigState{negotiated: Negotiated{
			Capabilities: caps,
		},
			peer: Preface{
				Settings: Settings{
					MaxFramePayload:          4096,
					MaxExtensionPayloadBytes: 4096,
				},
			}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	s := testVisibleBidiStream(c, 11)
	testSetPendingPriorityUpdate(c, s.id, append([]byte(nil), payload...))

	done := make(chan error, 1)
	go func() {
		req := <-c.writer.writeCh
		if len(req.frames) != 2 {
			done <- fmt.Errorf("queued frames = %d, want 2", len(req.frames))
			return
		}
		if req.frames[0].Type != FrameTypeEXT || req.frames[0].StreamID != s.id {
			done <- fmt.Errorf("first frame = %+v, want EXT for stream %d", req.frames[0], s.id)
			return
		}
		if !bytes.Equal(req.frames[0].Payload, payload) {
			done <- fmt.Errorf("priority payload = %x, want %x", req.frames[0].Payload, payload)
			return
		}
		if req.frames[1].Type != FrameTypeDATA || req.frames[1].Flags&FrameFlagFIN == 0 || req.frames[1].StreamID != s.id {
			done <- fmt.Errorf("second frame = %+v, want DATA|FIN for stream %d", req.frames[1], s.id)
			return
		}
		req.done <- nil
		done <- nil
	}()

	if err := s.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v, want nil", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if testHasPendingPriorityUpdate(c, s.id) {
		t.Fatal("pending priority update retained after CloseWrite")
	}
	if got := s.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
	}
}

func TestDrainPendingControlFramesDropsPriorityUpdateForSendTerminalStream(t *testing.T) {
	c := &Conn{registry: connRegistryState{streams: make(map[uint64]*nativeStream)}}
	stream := testBuildStream(
		c,
		4,
		testWithLocalOpen(testLocalOpenOpenedCommittedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)
	stream.setSendFin()
	testSetPendingPriorityUpdate(c, stream.id, []byte{1, 2, 3})

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 0 || len(advisory) != 0 {
		t.Fatalf("drained urgent=%d advisory=%d, want 0/0", len(urgent), len(advisory))
	}
	if testHasPendingPriorityUpdate(c, stream.id) {
		t.Fatal("terminal stream PRIORITY_UPDATE was not dropped from pending state")
	}
	if got := stream.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
	}
}

func TestQueueStreamBlockedDropsWhenControlBudgetExceeded(t *testing.T) {
	c := newControlBudgetTestConn()
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.registry.streams == nil {
		c.registry.streams = make(map[uint64]*nativeStream)
	}
	base := uint64(1 << 60)
	var inserted int
	for i := 0; ; i++ {
		stream := testBuildStream(c, base+uint64(i), testWithLocalSend())
		prevCount := testPendingStreamBlockedCount(c)
		prevBytes := c.pending.controlBytes
		c.queueStreamBlockedAsync(stream, base)
		if testPendingStreamBlockedCount(c) == prevCount {
			if c.pending.controlBytes != prevBytes {
				t.Fatalf("pending control bytes changed on dropped blocked entry: got %d, want %d", c.pending.controlBytes, prevBytes)
			}
			break
		}
		inserted++
	}
	if inserted == 0 {
		t.Fatal("expected at least one blocked entry to fit before budget was exceeded")
	}
	if c.pending.controlBytes > c.pendingControlBudgetLocked() {
		t.Fatalf("pending control bytes = %d, want <= %d", c.pending.controlBytes, c.pendingControlBudgetLocked())
	}
}

func TestClearPendingStreamBlockedReleasesControlBudget(t *testing.T) {
	c := newControlBudgetTestConn()
	c.mu.Lock()
	defer c.mu.Unlock()

	ensureTestPendingStream(c, 7).localSend = true
	if !c.setPendingStreamControlLocked(streamControlBlocked, 7, 1<<60) {
		t.Fatal("set pending stream BLOCKED failed")
	}
	if c.pending.controlBytes == 0 {
		t.Fatal("pending control bytes did not increase for stream BLOCKED")
	}
	c.clearPendingStreamControlEntryLocked(streamControlBlocked, 7)
	if c.pending.controlBytes != 0 {
		t.Fatalf("pending control bytes after clearing stream BLOCKED = %d, want 0", c.pending.controlBytes)
	}
}

func TestClearPendingStreamMaxDataReleasesControlBudget(t *testing.T) {
	c := newControlBudgetTestConn()
	c.mu.Lock()
	defer c.mu.Unlock()

	ensureTestPendingStream(c, 9).localReceive = true
	if !c.setPendingStreamControlLocked(streamControlMaxData, 9, 1<<60) {
		t.Fatal("set pending stream MAX_DATA failed")
	}
	if c.pending.controlBytes == 0 {
		t.Fatal("pending control bytes did not increase for stream MAX_DATA")
	}
	c.clearPendingStreamControlEntryLocked(streamControlMaxData, 9)
	if c.pending.controlBytes != 0 {
		t.Fatalf("pending control bytes after clearing stream MAX_DATA = %d, want 0", c.pending.controlBytes)
	}
}

func TestPendingGoAwayPayloadCountsAgainstControlBudget(t *testing.T) {
	c := newControlBudgetTestConn()
	c.mu.Lock()
	defer c.mu.Unlock()

	payload := make([]byte, 64)
	if !c.setPendingGoAwayPayloadLocked(payload) {
		t.Fatal("set pending GOAWAY payload failed")
	}
	if got := c.pending.controlBytes; got != uint64(len(payload)) {
		t.Fatalf("pending control bytes = %d, want %d", got, len(payload))
	}
	c.clearPendingGoAwayLocked()
	if c.pending.controlBytes != 0 {
		t.Fatalf("pending control bytes after clearing GOAWAY = %d, want 0", c.pending.controlBytes)
	}
}

func TestSetPendingGoAwayPayloadCopiesCallerBytes(t *testing.T) {
	c := newControlBudgetTestConn()
	c.mu.Lock()
	defer c.mu.Unlock()

	payload := []byte("bye")
	if !c.setPendingGoAwayPayloadLocked(payload) {
		t.Fatal("setPendingGoAwayPayloadLocked failed")
	}
	payload[0] = 'x'
	if got := string(c.sessionControl.pendingGoAwayPayload); got != "bye" {
		t.Fatalf("pendingGoAwayPayload = %q, want %q", got, "bye")
	}
}

func TestSetPendingGoAwayPayloadTightensOversizedExistingBacking(t *testing.T) {
	c := newControlBudgetTestConn()
	c.mu.Lock()
	defer c.mu.Unlock()

	existing := make([]byte, 3, 64)
	copy(existing, "old")
	c.sessionControl.pendingGoAwayPayload = existing
	c.sessionControl.hasPendingGoAway = true
	c.pending.controlBytes = uint64(len(existing))

	if !c.setPendingGoAwayPayloadLocked([]byte("new")) {
		t.Fatal("setPendingGoAwayPayloadLocked failed")
	}
	if !bytes.Equal(c.sessionControl.pendingGoAwayPayload, []byte("new")) {
		t.Fatalf("pendingGoAwayPayload = %q, want %q", c.sessionControl.pendingGoAwayPayload, "new")
	}
	if got := cap(c.sessionControl.pendingGoAwayPayload); got != len(c.sessionControl.pendingGoAwayPayload) {
		t.Fatalf("cap(pendingGoAwayPayload) = %d, want tight cap %d", got, len(c.sessionControl.pendingGoAwayPayload))
	}
	if &c.sessionControl.pendingGoAwayPayload[0] == &existing[0] {
		t.Fatal("pendingGoAwayPayload reused oversized existing backing array")
	}
}

func TestClearPendingStreamSendRuntimeStateReleasesBlockedAndPriorityBudget(t *testing.T) {
	c := newControlBudgetTestConn()

	stream := testBuildDetachedStream(c, 11, testWithLocalSend(), testWithBlockedState(9))

	c.mu.Lock()
	if c.registry.streams == nil {
		c.registry.streams = make(map[uint64]*nativeStream)
	}
	c.registry.streams[stream.id] = stream
	if !c.setPendingStreamControlLocked(streamControlBlocked, stream.id, stream.blockedAt) {
		c.mu.Unlock()
		t.Fatal("set pending stream BLOCKED failed")
	}
	testSetPendingPriorityUpdate(c, stream.id, []byte{1, 2, 3})
	if c.pending.controlBytes == 0 || c.pending.priorityBytes == 0 {
		c.mu.Unlock()
		t.Fatal("expected pending control and priority bytes before runtime clear")
	}
	c.releaseStreamRuntimeStateLocked(stream, streamRuntimeSendPending)
	if stream.blockedSet || stream.blockedAt != 0 {
		c.mu.Unlock()
		t.Fatalf("blocked state = (%t,%d), want cleared", stream.blockedSet, stream.blockedAt)
	}
	if c.pending.controlBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pending control bytes after runtime clear = %d, want 0", c.pending.controlBytes)
	}
	if c.pending.priorityBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pending priority bytes after runtime clear = %d, want 0", c.pending.priorityBytes)
	}
	if testHasPendingStreamBlocked(c, stream.id) {
		c.mu.Unlock()
		t.Fatal("pending stream BLOCKED retained after runtime clear")
	}
	if testHasPendingPriorityUpdate(c, stream.id) {
		c.mu.Unlock()
		t.Fatal("pending PRIORITY_UPDATE retained after runtime clear")
	}
	c.mu.Unlock()
}

func TestPendingControlBudgetLockedSaturatesLargePayload(t *testing.T) {
	c := &Conn{config: connConfigState{local: Preface{Settings: DefaultSettings()},
		peer: Preface{Settings: Settings{MaxControlPayloadBytes: ^uint64(0)}}},
	}

	c.mu.Lock()
	got := c.pendingControlBudgetLocked()
	c.mu.Unlock()

	if got != ^uint64(0) {
		t.Fatalf("pendingControlBudgetLocked() = %d, want saturation to %d", got, ^uint64(0))
	}
}

func TestReplacePendingControlBytesLockedSaturatesProjectedBytes(t *testing.T) {
	c := newControlBudgetTestConn()

	c.mu.Lock()
	c.pending.controlBytes = ^uint64(0) - 1
	if !c.replacePendingControlBytesLocked(0, 8, pendingControlReplaceOptions{
		mode:   pendingControlReplaceForced,
		notify: pendingControlNotifyAvailable,
	}) {
		c.mu.Unlock()
		t.Fatal("replacePendingControlBytesLocked(pendingControlReplaceForced) rejected saturated update")
	}
	got := c.pending.controlBytes
	c.mu.Unlock()

	if got != ^uint64(0) {
		t.Fatalf("pendingControlBytes = %d, want saturation to %d", got, ^uint64(0))
	}
}

func TestClearPendingNonCloseControlStateResetsPendingStateAndAllowsRequeue(t *testing.T) {
	c := newControlBudgetTestConn()

	c.mu.Lock()
	testSetPendingStreamMaxData(c, 4, 11)
	testSetPendingStreamBlocked(c, 8, 22)
	testSetPendingPriorityUpdate(c, 12, []byte("prio"))
	c.clearPendingNonCloseControlStateLocked()
	if testPendingStreamMaxDataCount(c) != 0 {
		c.mu.Unlock()
		t.Fatal("pending stream MAX_DATA state not released")
	}
	if testPendingStreamBlockedCount(c) != 0 {
		c.mu.Unlock()
		t.Fatal("pending stream BLOCKED state not released")
	}
	if testPendingPriorityUpdateCount(c) != 0 {
		c.mu.Unlock()
		t.Fatal("pending PRIORITY_UPDATE state not released")
	}
	if c.pending.controlBytes != 0 || c.pending.priorityBytes != 0 {
		gotControl := c.pending.controlBytes
		gotPriority := c.pending.priorityBytes
		c.mu.Unlock()
		t.Fatalf("pending bytes after clear = %d/%d, want 0/0", gotControl, gotPriority)
	}
	if !testSetPendingStreamMaxData(c, 4, 33) {
		c.mu.Unlock()
		t.Fatal("requeue MAX_DATA failed after clear")
	}
	if !testSetPendingStreamBlocked(c, 8, 44) {
		c.mu.Unlock()
		t.Fatal("requeue BLOCKED failed after clear")
	}
	if testSetPendingPriorityUpdate(c, 12, []byte("next")) == 0 {
		c.mu.Unlock()
		t.Fatal("requeue PRIORITY_UPDATE failed after clear")
	}
	c.mu.Unlock()

	if got := testPendingStreamMaxDataCount(c); got != 1 {
		t.Fatalf("pending stream MAX_DATA count after requeue = %d, want 1", got)
	}
	if got := testPendingStreamBlockedCount(c); got != 1 {
		t.Fatalf("pending stream BLOCKED count after requeue = %d, want 1", got)
	}
	if got := testPendingPriorityUpdateCount(c); got != 1 {
		t.Fatalf("pending PRIORITY_UPDATE count after requeue = %d, want 1", got)
	}
}

func TestDrainPendingControlFramesAllowsDrainingState(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := newPendingControlTestStream(c)
	if !c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(101)) {
		c.mu.Unlock()
		t.Fatal("queuePendingSessionControlAsync rejected session MAX_DATA")
	}
	c.queueStreamMaxDataAsync(stream.id, 202)
	c.queuePendingSessionControlAsync(sessionControlBlocked, 303)
	c.queueStreamBlockedAsync(stream, 404)
	c.queuePriorityUpdateAsync(stream.id, []byte{1, 2, 3}, retainedBytesBorrowed)
	c.lifecycle.sessionState = connStateDraining
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 4 {
		t.Fatalf("urgent frame count = %d, want 4", len(urgent))
	}
	if len(advisory) != 1 {
		t.Fatalf("advisory frame count = %d, want 1", len(advisory))
	}
	if advisory[0].Type != FrameTypeEXT || advisory[0].StreamID != stream.id {
		t.Fatalf("advisory frame = %+v, want EXT for stream %d", advisory[0], stream.id)
	}
	if c.pending.controlBytes != 0 {
		t.Fatalf("pendingControlBytes = %d, want 0", c.pending.controlBytes)
	}
	if testPendingPriorityUpdateCount(c) != 0 || c.pending.priorityBytes != 0 {
		t.Fatalf("pending PRIORITY_UPDATE retained = %d/%d", testPendingPriorityUpdateCount(c), c.pending.priorityBytes)
	}
}

func TestDrainPendingControlFramesDropsNonCloseControlWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := newPendingControlTestStream(c)
	if !c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(101)) {
		c.mu.Unlock()
		t.Fatal("queuePendingSessionControlAsync rejected session MAX_DATA")
	}
	c.queueStreamMaxDataAsync(stream.id, 202)
	c.queuePendingSessionControlAsync(sessionControlBlocked, 303)
	c.queueStreamBlockedAsync(stream, 404)
	c.queuePriorityUpdateAsync(stream.id, []byte{1, 2, 3}, retainedBytesBorrowed)
	if !c.setPendingGoAwayPayloadLocked([]byte{0xaa, 0xbb}) {
		c.mu.Unlock()
		t.Fatal("setPendingGoAwayPayloadLocked failed")
	}
	c.sessionControl.pendingGoAwayBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.sessionControl.pendingGoAwayUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
	c.lifecycle.sessionState = connStateClosing

	urgent, advisory := testDrainPendingControlFrames(c)
	if len(urgent) != 0 || len(advisory) != 0 {
		c.mu.Unlock()
		t.Fatalf("drained urgent=%d advisory=%d, want 0/0", len(urgent), len(advisory))
	}
	if c.pending.hasSessionMaxData || testPendingStreamMaxDataCount(c) != 0 {
		c.mu.Unlock()
		t.Fatalf("pending MAX_DATA state retained = %t/%d", c.pending.hasSessionMaxData, testPendingStreamMaxDataCount(c))
	}
	if c.pending.hasSessionBlocked || testPendingStreamBlockedCount(c) != 0 {
		c.mu.Unlock()
		t.Fatalf("pending BLOCKED state retained = %t/%d", c.pending.hasSessionBlocked, testPendingStreamBlockedCount(c))
	}
	if testPendingPriorityUpdateCount(c) != 0 || c.pending.priorityBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pending PRIORITY_UPDATE retained = %d/%d", testPendingPriorityUpdateCount(c), c.pending.priorityBytes)
	}
	if !c.sessionControl.hasPendingGoAway || len(c.sessionControl.pendingGoAwayPayload) != 2 {
		c.mu.Unlock()
		t.Fatalf("pending GOAWAY state lost = %t/%d", c.sessionControl.hasPendingGoAway, len(c.sessionControl.pendingGoAwayPayload))
	}
	if c.pending.controlBytes != uint64(len(c.sessionControl.pendingGoAwayPayload)) {
		c.mu.Unlock()
		t.Fatalf("pendingControlBytes = %d, want %d", c.pending.controlBytes, len(c.sessionControl.pendingGoAwayPayload))
	}
	c.mu.Unlock()
}

func TestQueuePendingControlIgnoredWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := newPendingControlTestStream(c)
	c.lifecycle.sessionState = connStateClosing
	if !c.setPendingGoAwayPayloadLocked([]byte{0xaa}) {
		c.mu.Unlock()
		t.Fatal("setPendingGoAwayPayloadLocked failed")
	}
	c.sessionControl.pendingGoAwayBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
	c.sessionControl.pendingGoAwayUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4

	c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(101))
	c.queueStreamMaxDataAsync(stream.id, 202)
	c.queuePendingSessionControlAsync(sessionControlBlocked, 303)
	c.queueStreamBlockedAsync(stream, 404)
	c.queuePriorityUpdateAsync(stream.id, []byte{1, 2, 3}, retainedBytesBorrowed)

	if c.pending.hasSessionMaxData || testPendingStreamMaxDataCount(c) != 0 {
		c.mu.Unlock()
		t.Fatalf("pending MAX_DATA state retained = %t/%d", c.pending.hasSessionMaxData, testPendingStreamMaxDataCount(c))
	}
	if c.pending.hasSessionBlocked || testPendingStreamBlockedCount(c) != 0 {
		c.mu.Unlock()
		t.Fatalf("pending BLOCKED state retained = %t/%d", c.pending.hasSessionBlocked, testPendingStreamBlockedCount(c))
	}
	if testPendingPriorityUpdateCount(c) != 0 || c.pending.priorityBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pending PRIORITY_UPDATE retained = %d/%d", testPendingPriorityUpdateCount(c), c.pending.priorityBytes)
	}
	if !c.sessionControl.hasPendingGoAway || c.pending.controlBytes != uint64(len(c.sessionControl.pendingGoAwayPayload)) {
		c.mu.Unlock()
		t.Fatalf("GOAWAY accounting changed while closing = %t/%d/%d", c.sessionControl.hasPendingGoAway, c.pending.controlBytes, len(c.sessionControl.pendingGoAwayPayload))
	}
	c.mu.Unlock()
}

func TestTakePendingPriorityUpdateFrameDropsWhenClosing(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := newPendingControlTestStream(c)
	testSetPendingPriorityUpdate(c, stream.id, []byte{1, 2, 3})
	c.lifecycle.sessionState = connStateClosing

	frame := c.takePendingPriorityUpdateFrameLocked(stream.id)
	if frame.hasFrame() {
		c.mu.Unlock()
		t.Fatalf("takePendingPriorityUpdateFrameLocked returned frame %+v while closing", testPublicFrame(frame.frame))
	}
	if testHasPendingPriorityUpdate(c, stream.id) {
		c.mu.Unlock()
		t.Fatal("pending PRIORITY_UPDATE was not dropped")
	}
	if c.pending.priorityBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityBytes = %d, want 0", c.pending.priorityBytes)
	}
	c.mu.Unlock()
}

func TestQueueStreamMaxDataAsyncDropsMissingTargetPendingState(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)

	c.mu.Lock()
	ensureTestPendingStream(c, streamID).localReceive = true
	if !c.setPendingStreamControlLocked(streamControlMaxData, streamID, 64) {
		c.mu.Unlock()
		t.Fatal("setPendingStreamMaxDataLocked failed")
	}
	delete(c.registry.streams, streamID)
	c.queueStreamMaxDataAsync(streamID, 96)
	if testHasPendingStreamMaxData(c, streamID) {
		c.mu.Unlock()
		t.Fatal("stale pending stream MAX_DATA retained for missing target")
	}
	c.mu.Unlock()
}

func TestQueueStreamBlockedAsyncDropsIdlessOrNonLiveTarget(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := &nativeStream{
		conn:       c,
		localSend:  true,
		localOpen:  testLocalOpenOpenedState(),
		blockedAt:  7,
		blockedSet: true,
	}

	c.mu.Lock()
	c.queueStreamBlockedAsync(stream, 9)
	if stream.blockedSet || stream.blockedAt != 0 {
		c.mu.Unlock()
		t.Fatalf("blocked state = (%t,%d), want cleared for non-live target", stream.blockedSet, stream.blockedAt)
	}
	c.mu.Unlock()
}

func TestQueuePriorityUpdateAsyncDropsMissingTargetPendingState(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)

	c.mu.Lock()
	ensureTestPendingStream(c, streamID)
	testSetPendingPriorityUpdate(c, streamID, []byte{1, 2, 3})
	delete(c.registry.streams, streamID)
	c.queuePriorityUpdateAsync(streamID, []byte{4, 5, 6}, retainedBytesBorrowed)
	if testHasPendingPriorityUpdate(c, streamID) {
		c.mu.Unlock()
		t.Fatal("stale pending PRIORITY_UPDATE retained for missing target")
	}
	if c.pending.priorityBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityBytes = %d, want 0 after drop", c.pending.priorityBytes)
	}
	c.mu.Unlock()
}

func TestQueuePriorityUpdateAsyncDropsIdlessTargetPendingState(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)

	c.mu.Lock()
	ensureTestPendingStream(c, streamID)
	testSetPendingPriorityUpdate(c, streamID, []byte{1, 2, 3})
	c.registry.streams[streamID] = &nativeStream{
		conn:      c,
		id:        streamID,
		localSend: true,
		localOpen: testLocalOpenOpenedState(),
	}
	c.queuePriorityUpdateAsync(streamID, []byte{4, 5, 6}, retainedBytesBorrowed)
	if testHasPendingPriorityUpdate(c, streamID) {
		c.mu.Unlock()
		t.Fatal("stale pending PRIORITY_UPDATE retained for idless target")
	}
	if c.pending.priorityBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityBytes = %d, want 0 after idless drop", c.pending.priorityBytes)
	}
	c.mu.Unlock()
}

func TestTerminalErrPriority(t *testing.T) {
	t.Parallel()

	t.Run("send_abort_beats_recv_abort", func(t *testing.T) {
		t.Parallel()
		c, _, stop := newInvalidFrameConn(t, 0)
		defer stop()

		stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
		stream.sendAbort = &ApplicationError{Code: uint64(CodeRefusedStream)}
		stream.recvAbort = &ApplicationError{Code: uint64(CodeInternal), Reason: "peer"}
		stream.sendHalf = state.SendHalfAborted
		stream.recvHalf = state.RecvHalfAborted

		err := stream.terminalErrLocked()
		if err == nil {
			t.Fatal("terminalErr = nil, want application error")
		}
		if appErr := testApplicationError(err); appErr == nil || appErr.Code != uint64(CodeRefusedStream) {
			t.Fatalf("terminalErr = %v, want send-side abort code %d", err, CodeRefusedStream)
		}
	})

	t.Run("send_reset_beats_recv_reset", func(t *testing.T) {
		t.Parallel()
		c, _, stop := newInvalidFrameConn(t, 0)
		defer stop()

		stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
		stream.sendReset = &ApplicationError{Code: uint64(CodeCancelled)}
		stream.recvReset = &ApplicationError{Code: uint64(CodeInternal), Reason: "peer"}
		stream.sendHalf = state.SendHalfReset
		stream.recvHalf = state.RecvHalfReset

		err := stream.terminalErrLocked()
		if err == nil {
			t.Fatal("terminalErr = nil, want application error")
		}
		if appErr := testApplicationError(err); appErr == nil || appErr.Code != uint64(CodeCancelled) {
			t.Fatalf("terminalErr = %v, want send-side reset code %d", err, CodeCancelled)
		}
	})

	t.Run("send_stop_seen_maps_to_write_closed", func(t *testing.T) {
		t.Parallel()
		c, _, stop := newInvalidFrameConn(t, 0)
		defer stop()

		stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
		stream.sendStop = &ApplicationError{Code: uint64(CodeCancelled)}
		stream.sendHalf = state.SendHalfStopSeen
		stream.recvHalf = state.RecvHalfFin

		err := stream.terminalErrLocked()
		if !errors.Is(err, ErrWriteClosed) {
			t.Fatalf("terminalErr = %v, want %v", err, ErrWriteClosed)
		}
	})

	t.Run("send_fin_beats_recv_fin_for_terminal_error_priority", func(t *testing.T) {
		t.Parallel()
		c, _, stop := newInvalidFrameConn(t, 0)
		defer stop()

		stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
		stream.sendHalf = state.SendHalfFin
		stream.recvHalf = state.RecvHalfFin

		err := stream.terminalErrLocked()
		if !errors.Is(err, ErrWriteClosed) {
			t.Fatalf("terminalErr = %v, want %v", err, ErrWriteClosed)
		}
	})
}

func TestRecvStopSentRemainsNonTerminalProtocolState(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_stop_sent",
	})

	if !stream.readStopSentLocked() {
		t.Fatal("readStopped = false, want true")
	}
	if state.RecvTerminal(stream.effectiveRecvHalfStateLocked()) {
		t.Fatal("recvTerminal = true, want false for recv_stop_sent")
	}
	if state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
		t.Fatal("fullyTerminal = true, want false for send_fin + recv_stop_sent")
	}
}

func appErrorIs(err error, code ErrorCode) bool {
	appErr := testApplicationError(err)
	return appErr != nil && appErr.Code == uint64(code)
}

func TestLateNonOpeningControlOnTerminalStreamIgnored(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           4,
		localOpen:    testLocalOpenClosedState(),
		localSend:    true,
		localReceive: true,
	}
	stream.initHalfStates()
	stream.setSendFin()
	stream.setRecvFin()
	c.registry.streams[4] = stream

	tests := []struct {
		name  string
		frame Frame
		call  func(Frame) error
	}{
		{
			name:  "MAX_DATA",
			frame: Frame{Type: FrameTypeMAXDATA, StreamID: 4, Payload: mustEncodeVarint(64)},
			call:  c.handleMaxDataFrame,
		},
		{
			name:  "BLOCKED",
			frame: Frame{Type: FrameTypeBLOCKED, StreamID: 4, Payload: mustEncodeVarint(64)},
			call:  c.handleBlockedFrame,
		},
		{
			name:  "STOP_SENDING",
			frame: Frame{Type: FrameTypeStopSending, StreamID: 4, Payload: mustEncodeVarint(uint64(CodeCancelled))},
			call:  c.handleStopSendingFrame,
		},
		{
			name:  "RESET",
			frame: Frame{Type: FrameTypeRESET, StreamID: 4, Payload: mustEncodeVarint(uint64(CodeCancelled))},
			call:  c.handleResetFrame,
		},
		{
			name:  "ABORT",
			frame: Frame{Type: FrameTypeABORT, StreamID: 4, Payload: mustEncodeVarint(uint64(CodeCancelled))},
			call:  c.handleAbortFrame,
		},
	}

	for _, tt := range tests {
		if err := tt.call(tt.frame); err != nil {
			t.Fatalf("%s err = %v, want nil", tt.name, err)
		}
		assertNoQueuedFrame(t, frames)
		if !state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
			t.Fatalf("%s changed terminal stream state", tt.name)
		}
	}
}

func TestTerminalControlIgnoredWhenFullTerminalStreamAborted(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	tests := []struct {
		name  string
		state stateHalfExpect
	}{
		{
			name: "send_aborted_recv_open",
			state: stateHalfExpect{
				SendHalf: "send_aborted",
				RecvHalf: "recv_open",
			},
		},
		{
			name: "send_open_recv_aborted",
			state: stateHalfExpect{
				SendHalf: "send_open",
				RecvHalf: "recv_aborted",
			},
		},
		{
			name: "send_aborted_recv_aborted",
			state: stateHalfExpect{
				SendHalf: "send_aborted",
				RecvHalf: "recv_aborted",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.mu.Lock()
			streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
			c.mu.Unlock()
			stream := seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", tt.state)
			stream.recvBuffer = 3
			stream.readBuf = []byte("abc")

			c.mu.Lock()
			beforeSessionAdvertised := c.flow.recvSessionAdvertised
			beforeRecvBuffer := stream.recvBuffer
			beforeRecvAdvertised := stream.recvAdvertised
			beforeReadBufLen := len(stream.readBuf)
			beforeSendHalf := stream.sendHalf
			beforeRecvHalf := stream.recvHalf
			beforeSendAbort := stream.sendAbort
			beforeRecvAbort := stream.recvAbort
			c.mu.Unlock()

			calls := []struct {
				name  string
				call  func(Frame) error
				frame Frame
			}{
				{
					name: "STOP_SENDING",
					call: c.handleStopSendingFrame,
					frame: Frame{
						Type:     FrameTypeStopSending,
						StreamID: stream.id,
						Payload:  mustEncodeVarint(uint64(CodeCancelled)),
					},
				},
				{
					name: "RESET",
					call: c.handleResetFrame,
					frame: Frame{
						Type:     FrameTypeRESET,
						StreamID: stream.id,
						Payload:  mustEncodeVarint(uint64(CodeCancelled)),
					},
				},
				{
					name: "ABORT",
					call: c.handleAbortFrame,
					frame: Frame{
						Type:     FrameTypeABORT,
						StreamID: stream.id,
						Payload:  mustEncodeVarint(uint64(CodeCancelled)),
					},
				},
			}
			for _, tc := range calls {
				if err := tc.call(tc.frame); err != nil {
					t.Fatalf("terminal control on %s err = %v", tt.name, err)
				}
				assertNoQueuedFrame(t, frames)
			}

			c.mu.Lock()
			if c.flow.recvSessionAdvertised != beforeSessionAdvertised {
				t.Fatalf("terminal control changed recvSessionAdvertised")
			}
			if stream.recvBuffer != beforeRecvBuffer {
				t.Fatalf("terminal control changed recvBuffer: %d, want %d", stream.recvBuffer, beforeRecvBuffer)
			}
			if stream.recvAdvertised != beforeRecvAdvertised {
				t.Fatalf("terminal control changed recvAdvertised: %d, want %d", stream.recvAdvertised, beforeRecvAdvertised)
			}
			if len(stream.readBuf) != beforeReadBufLen {
				t.Fatalf("terminal control changed readBuf len = %d, want %d", len(stream.readBuf), beforeReadBufLen)
			}
			if !testSameError(stream.sendAbort, beforeSendAbort) {
				t.Fatalf("terminal control changed sendAbort: %v -> %v", beforeSendAbort, stream.sendAbort)
			}
			if !testSameError(stream.recvAbort, beforeRecvAbort) {
				t.Fatalf("terminal control changed recvAbort: %v -> %v", beforeRecvAbort, stream.recvAbort)
			}
			if stream.sendHalf != beforeSendHalf {
				t.Fatalf("terminal control changed sendHalf")
			}
			if stream.recvHalf != beforeRecvHalf {
				t.Fatalf("terminal control changed recvHalf")
			}
			c.mu.Unlock()
		})
	}
}

func TestLateStopSendingIgnoredAfterSendReset(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           4,
		localOpen:    testLocalOpenClosedState(),
		localSend:    true,
		localReceive: true,
	}
	stream.initHalfStates()
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	stream.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetDirect)
	c.registry.streams[4] = stream

	if err := c.handleStopSendingFrame(Frame{
		Type:     FrameTypeStopSending,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handleStopSending on send-reset stream err = %v", err)
	}

	if stream.sendReset == nil {
		t.Fatal("sendReset was cleared by late STOP_SENDING")
	}
	if stream.sendStop != nil {
		t.Fatalf("sendStop = %v, want nil", stream.sendStop)
	}
	assertNoQueuedFrame(t, frames)
}

func TestLateResetIgnoredAfterTerminal(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           4,
		localOpen:    testLocalOpenClosedState(),
		localSend:    true,
		localReceive: true,
	}
	stream.initHalfStates()
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	stream.setSendFin()
	stream.setRecvFin()
	stream.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetDirect)
	c.registry.streams[4] = stream

	before := stream.sendStop
	if err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeRefusedStream)),
	}); err != nil {
		t.Fatalf("handle RESET on terminal stream err = %v", err)
	}

	if !state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
		t.Fatal("terminal stream became non-terminal after late RESET")
	}
	if stream.sendReset == nil {
		t.Fatal("sendReset unexpectedly cleared by late RESET")
	}
	if !testSameError(stream.sendStop, before) {
		t.Fatalf("sendStop changed by late RESET from %v to %v", before, stream.sendStop)
	}
	assertNoQueuedFrame(t, frames)
}

func TestLateAbortIgnoredAfterTerminal(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           4,
		localOpen:    testLocalOpenClosedState(),
		localSend:    true,
		localReceive: true,
	}
	stream.initHalfStates()
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	stream.setSendFin()
	stream.setRecvFin()
	stream.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetDirect)
	stream.setRecvAbortWithSource(&ApplicationError{Code: uint64(CodeInternal), Reason: "peer"}, terminalAbortFromPeer)
	c.registry.streams[4] = stream

	beforeSendReset := stream.sendReset
	beforeRecvAbort := stream.recvAbort
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeRefusedStream)),
	}); err != nil {
		t.Fatalf("handle ABORT on terminal stream err = %v", err)
	}

	if !testSameError(stream.sendReset, beforeSendReset) {
		t.Fatalf("sendReset changed by late ABORT: %v -> %v", beforeSendReset, stream.sendReset)
	}
	if !testSameError(stream.recvAbort, beforeRecvAbort) {
		t.Fatalf("recvAbort changed by late ABORT: %v -> %v", beforeRecvAbort, stream.recvAbort)
	}
	assertNoQueuedFrame(t, frames)
}

func TestLateAbortIgnoredAfterSendResetRecvFin(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           4,
		localOpen:    testLocalOpenCommittedHiddenState(),
		localSend:    true,
		localReceive: true,
	}
	stream.initHalfStates()
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	stream.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetDirect)
	stream.setRecvFin()
	c.registry.streams[4] = stream

	beforeSendReset := stream.sendReset
	beforeRecvAbort := stream.recvAbort
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeRefusedStream)),
	}); err != nil {
		t.Fatalf("handle ABORT on send-reset stream after recv FIN err = %v", err)
	}

	if !state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
		t.Fatalf("stream is not terminal after send-reset/recv-fin")
	}
	if !testSameError(stream.sendReset, beforeSendReset) {
		t.Fatalf("sendReset changed from %v to %v", beforeSendReset, stream.sendReset)
	}
	if !testSameError(stream.recvAbort, beforeRecvAbort) {
		t.Fatalf("recvAbort changed from %v to %v", beforeRecvAbort, stream.recvAbort)
	}
	assertNoQueuedFrame(t, frames)
}

func TestLateTerminalControlStatePreservedWhenRecvAndSendReset(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           4,
		idSet:        true,
		localOpen:    testLocalOpenCommittedHiddenState(),
		localSend:    true,
		localReceive: true,
	}
	stream.initHalfStates()
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	stream.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled), Reason: "local reset"}, terminalResetDirect)
	stream.setRecvReset(&ApplicationError{Code: uint64(CodeInternal), Reason: "peer reset"})
	c.registry.streams[4] = stream

	beforeSendReset := stream.sendReset
	beforeRecvReset := stream.recvReset
	beforeSendCommitted := stream.localOpen.committed

	tests := []struct {
		name  string
		frame Frame
		call  func(Frame) error
	}{
		{
			name:  "STOP_SENDING",
			frame: Frame{Type: FrameTypeStopSending, StreamID: 4, Payload: mustEncodeVarint(uint64(CodeCancelled))},
			call:  c.handleStopSendingFrame,
		},
		{
			name:  "RESET",
			frame: Frame{Type: FrameTypeRESET, StreamID: 4, Payload: mustEncodeVarint(uint64(CodeCancelled))},
			call:  c.handleResetFrame,
		},
		{
			name:  "ABORT",
			frame: Frame{Type: FrameTypeABORT, StreamID: 4, Payload: mustEncodeVarint(uint64(CodeRefusedStream))},
			call:  c.handleAbortFrame,
		},
	}

	for _, tt := range tests {
		if err := tt.call(tt.frame); err != nil {
			t.Fatalf("%s err = %v, want nil", tt.name, err)
		}
		if !state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
			t.Fatalf("%s made stream non-terminal", tt.name)
		}
		if !testSameError(stream.sendReset, beforeSendReset) {
			t.Fatalf("%s changed sendReset", tt.name)
		}
		if !testSameError(stream.recvReset, beforeRecvReset) {
			t.Fatalf("%s changed recvReset", tt.name)
		}
		if stream.localOpen.committed != beforeSendCommitted {
			t.Fatalf("%s changed sendCommitted from %v to %v", tt.name, beforeSendCommitted, stream.localOpen.committed)
		}
		assertNoQueuedFrame(t, frames)
	}
}

func TestAbortErrorHasHigherTerminalPriorityThanReset(t *testing.T) {
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           4,
		idSet:        true,
		localOpen:    testLocalOpenOpenedCommittedState(),
		localSend:    true,
		localReceive: true,
	}
	stream.initHalfStates()
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	stream.setSendAbortWithSource(&ApplicationError{Code: uint64(CodeRefusedStream), Reason: "local abort"}, terminalAbortLocal)
	stream.setRecvReset(&ApplicationError{Code: uint64(CodeCancelled)})
	stream.setRecvAbortWithSource(&ApplicationError{Code: uint64(CodeRefusedStream), Reason: "local abort"}, terminalAbortLocal)
	c.registry.streams[4] = stream

	if _, err := stream.Read(make([]byte, 8)); !appErrorIs(err, CodeRefusedStream) {
		t.Fatalf("read terminal err = %v, want %s", err, CodeRefusedStream)
	}
	if _, err := stream.Write([]byte("x")); !appErrorIs(err, CodeRefusedStream) {
		t.Fatalf("write terminal err = %v, want %s", err, CodeRefusedStream)
	}
}

func TestPeerResetDiscardsBufferedInboundDataAndReturnsReset(t *testing.T) {
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:             4,
		localOpen:      testLocalOpenClosedState(),
		localSend:      true,
		localReceive:   true,
		recvBuffer:     3,
		recvAdvertised: 32,
		readBuf:        []byte("abc"),
	}
	stream.initHalfStates()
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	c.registry.streams[4] = stream
	c.flow.recvSessionUsed = 3
	c.flow.recvSessionAdvertised = 100

	if err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle peer RESET err = %v", err)
	}

	c.mu.Lock()
	readErrExpected := stream.recvReset
	readBufLen := len(stream.readBuf)
	recvBuffer := stream.recvBuffer
	recvAdvertised := stream.recvAdvertised
	recvSessionUsed := c.flow.recvSessionUsed
	recvSessionAdvertised := c.flow.recvSessionAdvertised
	c.mu.Unlock()
	if readErrExpected == nil || readErrExpected.Code != uint64(CodeCancelled) {
		t.Fatalf("recvReset = %v, want code %d", stream.recvReset, uint64(CodeCancelled))
	}
	if readBufLen != 0 {
		t.Fatalf("readBuf len = %d, want 0", len(stream.readBuf))
	}
	if recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", stream.recvBuffer)
	}
	if recvAdvertised != 32 {
		t.Fatalf("recvAdvertised = %d, want %d", recvAdvertised, 32)
	}
	if recvSessionUsed != 0 {
		t.Fatalf("recvSessionUsed = %d, want 0", recvSessionUsed)
	}
	if recvSessionAdvertised != 103 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", recvSessionAdvertised, 103)
	}

	_, err := stream.Read(make([]byte, 8))
	if !appErrorIs(err, CodeCancelled) {
		t.Fatalf("read after reset err = %v, want %s", err, CodeCancelled)
	}
}

func TestPeerAbortDiscardsBufferedInboundDataAndReturnsCloseWithError(t *testing.T) {
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:             4,
		localOpen:      testLocalOpenClosedState(),
		localSend:      true,
		localReceive:   true,
		recvBuffer:     4,
		recvAdvertised: 64,
		readBuf:        []byte("abcd"),
	}
	stream.initHalfStates()
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	c.registry.streams[4] = stream
	c.flow.recvSessionUsed = 4
	c.flow.recvSessionAdvertised = 128

	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeRefusedStream)),
	}); err != nil {
		t.Fatalf("handle peer ABORT err = %v", err)
	}

	c.mu.Lock()
	readErrExpected := stream.recvAbort
	sendAbort := stream.sendAbort
	readBufLen := len(stream.readBuf)
	recvBuffer := stream.recvBuffer
	recvAdvertised := stream.recvAdvertised
	recvSessionUsed := c.flow.recvSessionUsed
	recvSessionAdvertised := c.flow.recvSessionAdvertised
	c.mu.Unlock()
	if readErrExpected == nil || readErrExpected.Code != uint64(CodeRefusedStream) {
		t.Fatalf("recvAbort = %v, want code %d", stream.recvAbort, uint64(CodeRefusedStream))
	}
	if sendAbort == nil || sendAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("sendAbort = %v, want code %d", stream.sendAbort, uint64(CodeRefusedStream))
	}
	if readBufLen != 0 {
		t.Fatalf("readBuf len = %d, want 0", readBufLen)
	}
	if recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", recvBuffer)
	}
	if recvAdvertised != 64 {
		t.Fatalf("recvAdvertised = %d, want %d", recvAdvertised, 64)
	}
	if recvSessionUsed != 0 {
		t.Fatalf("recvSessionUsed = %d, want 0", recvSessionUsed)
	}
	if recvSessionAdvertised != 132 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", recvSessionAdvertised, 132)
	}

	_, readErr := stream.Read(make([]byte, 8))
	if !appErrorIs(readErr, CodeRefusedStream) {
		t.Fatalf("read after abort err = %v, want %s", readErr, CodeRefusedStream)
	}
	_, writeErr := stream.Write([]byte("x"))
	if !appErrorIs(writeErr, CodeRefusedStream) {
		t.Fatalf("write after abort err = %v, want %s", writeErr, CodeRefusedStream)
	}
}

func TestStopSendingAfterRecvResetPreservesSendPathAndQueuesReset(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_reset",
	})
	testMarkLocalOpenCommitted(stream)

	if err := c.handleStopSendingFrame(Frame{
		Type:     FrameTypeStopSending,
		StreamID: stream.id,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle STOP_SENDING on recv_reset stream err = %v", err)
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), stream.id, CodeCancelled)

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.sendStop == nil || stream.sendStop.Code != uint64(CodeCancelled) {
		t.Fatalf("sendStop = %v, want %s", stream.sendStop, CodeCancelled)
	}
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want %s", stream.sendReset, CodeCancelled)
	}
	if stream.sendAbort != nil {
		t.Fatalf("sendAbort set by STOP_SENDING")
	}
	if stream.recvReset == nil || stream.recvReset.Code != uint64(CodeCancelled) {
		t.Fatalf("recvReset lost after STOP_SENDING: %v", stream.recvReset)
	}
}

func TestStopSendingIgnoredAfterRecvCloseWithError(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           4,
		localOpen:    testLocalOpenCommittedHiddenState(),
		localSend:    true,
		localReceive: true,
	}
	stream.initHalfStates()
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	stream.setAbortedWithSource(&ApplicationError{Code: uint64(CodeRefusedStream)}, terminalAbortFromPeer)
	c.registry.streams[4] = stream

	beforeSendStop := stream.sendStop
	beforeSendReset := stream.sendReset
	beforeRecvAbort := stream.recvAbort

	if err := c.handleStopSendingFrame(Frame{
		Type:     FrameTypeStopSending,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle STOP_SENDING on recv-aborted stream err = %v", err)
	}

	if !testSameError(stream.sendStop, beforeSendStop) {
		t.Fatalf("sendStop changed from %v to %v", beforeSendStop, stream.sendStop)
	}
	if !testSameError(stream.sendReset, beforeSendReset) {
		t.Fatalf("sendReset changed from %v to %v", beforeSendReset, stream.sendReset)
	}
	if !testSameError(stream.recvAbort, beforeRecvAbort) {
		t.Fatalf("recvAbort changed from %v to %v", beforeRecvAbort, stream.recvAbort)
	}
	assertNoQueuedFrame(t, frames)
	if !state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
		t.Fatalf("stream not terminal after recv ABORT")
	}
}

func TestAbortAfterRecvResetPreservesExistingRecvResetWithoutRequeue(t *testing.T) {
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_reset",
	})
	testMarkLocalOpenCommitted(stream)

	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: stream.id,
		Payload:  mustEncodeVarint(uint64(CodeRefusedStream)),
	}); err != nil {
		t.Fatalf("handle ABORT after recv_reset stream err = %v", err)
	}

	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.sendReset != nil {
		t.Fatalf("sendReset changed by peer ABORT: %v", stream.sendReset)
	}
	if stream.recvReset == nil || stream.recvReset.Code != uint64(CodeCancelled) {
		t.Fatalf("recvReset lost after ABORT: %v", stream.recvReset)
	}
	if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("sendAbort = %v, want %s", stream.sendAbort, CodeRefusedStream)
	}
	if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("recvAbort = %v, want %s", stream.recvAbort, CodeRefusedStream)
	}
	if !state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
		t.Fatalf("stream not terminal after ABORT")
	}
}

func TestResetAfterRecvStopSentAndSendFinStillConverges(t *testing.T) {
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_stop_sent",
	})

	c.mu.Lock()
	if state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
		c.mu.Unlock()
		t.Fatal("precondition failed: send_fin + recv_stop_sent treated as fully terminal")
	}
	c.mu.Unlock()

	if err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle RESET after recv_stop_sent/send_fin err = %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.registry.tombstones[streamID]; !ok {
		t.Fatalf("stream %d missing tombstone after reset convergence", streamID)
	}
	if _, ok := c.registry.streams[streamID]; ok {
		t.Fatalf("stream %d still retained as live state after reset convergence", streamID)
	}
}

func TestStopSendingClearsPendingStreamBlocked(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	c.mu.Lock()
	stream.blockedAt = 7
	stream.blockedSet = true
	testSetPendingStreamBlocked(c, streamID, 7)
	c.mu.Unlock()

	if err := c.handleStopSendingFrame(Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle STOP_SENDING: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.blockedSet || stream.blockedAt != 0 {
		t.Fatalf("blocked state = (%t,%d), want cleared", stream.blockedSet, stream.blockedAt)
	}
	if testHasPendingStreamBlocked(c, streamID) {
		t.Fatal("pending stream BLOCKED retained after STOP_SENDING")
	}
}

func TestDrainPendingControlFramesDropsBlockedForSendTerminalStream(t *testing.T) {
	c := &Conn{registry: connRegistryState{streams: make(map[uint64]*nativeStream)}}
	stream := &nativeStream{
		conn:      c,
		id:        4,
		idSet:     true,
		localSend: true,
	}
	stream.initHalfStates()
	stream.setSendFin()
	c.registry.streams[stream.id] = stream
	if !c.setPendingStreamControlLocked(streamControlBlocked, stream.id, 9) {
		t.Fatal("setPendingStreamBlockedLocked returned false")
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 0 || len(advisory) != 0 {
		t.Fatalf("drained urgent=%d advisory=%d, want 0/0", len(urgent), len(advisory))
	}
	if testHasPendingStreamBlocked(c, stream.id) {
		t.Fatal("terminal stream BLOCKED was not dropped from pending state")
	}
	if got := stream.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
	}
}

func TestDrainPendingControlFramesDropsBlockedForMissingStream(t *testing.T) {
	c := &Conn{registry: connRegistryState{streams: make(map[uint64]*nativeStream)}}
	c.mu.Lock()
	testSetPendingStreamBlocked(c, 5, 9)
	delete(c.registry.streams, 5)
	c.mu.Unlock()

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 0 || len(advisory) != 0 {
		t.Fatalf("drained urgent=%d advisory=%d, want 0/0", len(urgent), len(advisory))
	}
	if testHasPendingStreamBlocked(c, 5) {
		t.Fatal("missing stream BLOCKED was not dropped from pending state")
	}
}

func TestRecvStopSentClearsPendingStreamMaxData(t *testing.T) {
	c := &Conn{}
	s := &nativeStream{
		conn:         c,
		id:           7,
		idSet:        true,
		localReceive: true,
	}
	s.initHalfStates()

	s.setRecvStopSent()

	if testHasPendingStreamMaxData(c, 7) {
		t.Fatalf("pending MAX_DATA for stopped stream was not cleared")
	}
}

func TestDrainPendingControlFramesDropsMaxDataForRecvTerminalStream(t *testing.T) {
	c := &Conn{registry: connRegistryState{streams: make(map[uint64]*nativeStream)}}
	s := &nativeStream{
		conn:         c,
		id:           11,
		idSet:        true,
		localReceive: true,
	}
	s.initHalfStates()
	s.setRecvFin()
	c.registry.streams[11] = s

	urgent, advisory := testDrainPendingControlFrames(c)

	if len(urgent) != 0 || len(advisory) != 0 {
		t.Fatalf("expected stale MAX_DATA to be dropped, got %d urgent and %d advisory frames", len(urgent), len(advisory))
	}
	if testHasPendingStreamMaxData(c, 11) {
		t.Fatalf("pending MAX_DATA for terminal stream was not cleared")
	}
}

func TestDrainPendingControlFramesDropsMaxDataForMissingStream(t *testing.T) {
	c := &Conn{registry: connRegistryState{streams: make(map[uint64]*nativeStream)}}

	urgent, advisory := testDrainPendingControlFrames(c)

	if len(urgent) != 0 || len(advisory) != 0 {
		t.Fatalf("expected stale MAX_DATA to be dropped, got %d urgent and %d advisory frames", len(urgent), len(advisory))
	}
	if testHasPendingStreamMaxData(c, 12) {
		t.Fatal("pending MAX_DATA for missing stream was not cleared")
	}
}

func TestQueueStreamMaxDataAsyncSkipsStoppedStream(t *testing.T) {
	c := &Conn{registry: connRegistryState{streams: make(map[uint64]*nativeStream)}}
	s := &nativeStream{
		conn:         c,
		id:           13,
		idSet:        true,
		localReceive: true,
	}
	s.initHalfStates()
	s.setRecvStopSent()
	c.registry.streams[13] = s

	c.queueStreamMaxDataAsync(13, 16384)
	if testHasPendingStreamMaxData(c, 13) {
		t.Fatalf("MAX_DATA for stopped stream should not be queued")
	}
}

func TestQueueStreamMaxDataAsyncSkipsMissingStream(t *testing.T) {
	c := &Conn{registry: connRegistryState{streams: make(map[uint64]*nativeStream)}}

	c.queueStreamMaxDataAsync(21, 16384)
	if testHasPendingStreamMaxData(c, 21) {
		t.Fatal("MAX_DATA for missing stream should not be queued")
	}
}

func TestDuplicateStopSendingIgnoredAfterResetConvergence(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("first STOP_SENDING err = %v, want nil", err)
	}
	queued := awaitQueuedFrame(t, frames)
	assertQueuedResetCode(t, queued, streamID, CodeCancelled)
	if stream.sendReset == nil {
		t.Fatal("sendReset = nil, want local reset after first STOP_SENDING")
	}
	if stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset code = %d, want %d", stream.sendReset.Code, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want false after RESET follow-up")
	}

	if stream.sendStop == nil {
		t.Fatal("sendStop = nil, want application error after first STOP_SENDING")
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("duplicate STOP_SENDING err = %v, want nil", err)
	}

	assertNoQueuedFrame(t, frames)

	if stream.sendStop == nil {
		t.Fatal("duplicate STOP_SENDING cleared sendStop")
	}
	if stream.sendStop.Code != uint64(CodeCancelled) {
		t.Fatalf("sendStop code = %d, want %d", stream.sendStop.Code, uint64(CodeCancelled))
	}

	if stream.sendReset == nil {
		t.Fatal("duplicate STOP_SENDING cleared sendReset")
	}
	if stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset code = %d, want %d", stream.sendReset.Code, uint64(CodeCancelled))
	}
}

func TestStopSendingWithNoQueuedTailMayGracefullyFinish(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 3
	c.flow.sendSessionUsed = 3

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want immediate graceful conclusion for empty committed tail")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestStopSendingWithNoQueuedTailStillPrefersResetBeforeCommit(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), streamID, CodeCancelled)
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want reset when no committed tail exists")
	}
}

func TestHandleStopSendingQueuesTerminalFollowupWithoutWriterAdmission(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	c := &Conn{
		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals:   connRuntimeSignalState{acceptCh: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest, 1), urgentWriteCh: make(chan writeRequest, 1)},
		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(settings.MaxFramePayload),
		},
		config: connConfigState{
			local:      Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			peer:       Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, Capabilities: 0, LocalRole: localRole, PeerRole: peerRole, PeerSettings: settings},
		},
		flow: connFlowState{
			recvSessionAdvertised: settings.InitialMaxData,
			sendSessionMax:        settings.InitialMaxData,
		},
		registry: connRegistryState{
			streams:        make(map[uint64]*nativeStream),
			tombstones:     make(map[uint64]streamTombstone),
			usedStreamData: make(map[uint64]usedStreamMarker),
			nextLocalBidi:  state.FirstLocalStreamID(localRole, true),
			nextLocalUni:   state.FirstLocalStreamID(localRole, false),
			nextPeerBidi:   state.FirstPeerStreamID(localRole, true),
			nextPeerUni:    state.FirstPeerStreamID(localRole, false),
		},
	}

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 3
	c.flow.sendSessionUsed = 3

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.handleStopSendingFrame(Frame{
			Type:     FrameTypeStopSending,
			StreamID: streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("handle STOP_SENDING err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("handle STOP_SENDING blocked on writer admission")
	}

	select {
	case req := <-c.writer.writeCh:
		if len(req.frames) == 0 {
			t.Fatal("queued request has no frames")
		}
		queued := testPublicFrame(req.frames[len(req.frames)-1])
		if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
			t.Fatalf("queued frame = %+v, want DATA|FIN", queued)
		}
	case req := <-c.writer.urgentWriteCh:
		if len(req.frames) == 0 {
			t.Fatal("queued request has no frames")
		}
		queued := testPublicFrame(req.frames[len(req.frames)-1])
		if queued.Type != FrameTypeRESET {
			t.Fatalf("queued frame = %+v, want RESET", queued)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("terminal follow-up was not queued asynchronously")
	}
}

func TestStopSendingWithCommittedEmptyTailMayGracefullyFinishBeforeAnyBytesSent(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want graceful conclusion for committed empty tail before any bytes sent")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestStopSendingWithSmallInFlightTailMayGracefullyFinish(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 64
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want immediate graceful conclusion for small unavoidable tail")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestStopSendingWithSmallQueuedTailMayGracefullyFinish(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want immediate graceful conclusion for small queued committed tail")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestStopSendingWithSmallInFlightTailButLargeSuppressibleQueuedTailMayGracefullyFinish(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()
	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		StopSendingGracefulDrainWindow: time.Second,
	}))

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 1024
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want graceful conclusion when only the small in-flight slice is unavoidable and the larger queued tail remains suppressible")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestStopSendingWithSmallCombinedCommittedTailMayGracefullyFinish(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		StopSendingGracefulTailCap: 96,
	}))

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 96
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want graceful conclusion when the unavoidable in-flight tail fits within the graceful cap")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestStopSendingWithLargeInFlightTailStillPrefersReset(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 1024
	stream.queuedDataBytes = 1024
	stream.inflightQueued = 1024
	c.flow.sendSessionUsed = 1024

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), streamID, CodeCancelled)
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want reset for non-negligible unavoidable tail")
	}
}

func TestStopSendingWithLargeQueuedTailStillPrefersResetWithoutInflightTail(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 1024
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), streamID, CodeCancelled)
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want reset for large queued-only tail")
	}
}

func TestStopSendingWithLargeCommittedTailMayGracefullyFinishOnFastLink(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.metrics.sendRateEstimate = 16 << 10

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 768
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want fast-link time budget to allow graceful completion for a larger committed tail")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestStopSendingWithSmallInflightTailStillGracefullyFinishesOnSlowLinkDespiteLargeQueuedTail(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.metrics.sendRateEstimate = 1024

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 768
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want graceful conclusion when the unavoidable in-flight tail fits even though the larger queued tail remains above the slow-link budget")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestStopSendingWithLargeQueuedOnlyTailStillPrefersResetOnSlowLink(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.metrics.sendRateEstimate = 1024

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 768
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), streamID, CodeCancelled)
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want reset when only a queued-only tail remains and the slow-link budget is still too small")
	}
}

func TestConfigStopSendingTailCapOverrideWinsForQueuedOnlyTail(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		StopSendingGracefulTailCap: 256,
	}))
	c.metrics.sendRateEstimate = 16 << 10

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 384
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), streamID, CodeCancelled)
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want explicit graceful-tail cap override to remain authoritative even on a fast link")
	}
}

func TestConfigStopSendingTailCapStillAllowsSmallInflightTail(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.applyConfigRuntimePolicy(cloneConfig(&Config{
		StopSendingGracefulTailCap: 256,
	}))
	c.metrics.sendRateEstimate = 16 << 10

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 384
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want explicit tail cap to still allow graceful conclusion when only the small in-flight slice is unavoidable")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestStopSendingGracefulDrainTimeoutFallsBackToReset(t *testing.T) {
	c, frames, stop := newStopSendingDrainTimeoutConn(t)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 64
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), streamID, CodeCancelled)
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want graceful drain timeout to fall back to reset")
	}
}

func TestStopSendingWithSmallQueuedTailMayGracefullyFinishBeforeAnyBytesSent(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.queuedDataBytes = 1

	stopFrame := Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}

	if err := c.handleStopSendingFrame(stopFrame); err != nil {
		t.Fatalf("STOP_SENDING err = %v, want nil", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeDATA || queued.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame = %+v, want zero-length DATA|FIN", queued)
	}
	if len(queued.Payload) != 0 {
		t.Fatalf("queued payload len = %d, want 0", len(queued.Payload))
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want graceful conclusion for small queued committed tail before any bytes sent")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func newStopSendingDrainTimeoutConn(t *testing.T) (*Conn, <-chan Frame, func()) {
	t.Helper()

	frames := make(chan Frame, testFrameBufferCap)
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	flushDone := make(chan struct{})
	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1), terminalNotify: make(chan struct{}, 1)},
		signals: connRuntimeSignalState{acceptCh: make(chan struct{}, 1)},
		writer: connWriterRuntimeState{
			writeCh:         make(chan writeRequest),
			urgentWriteCh:   make(chan writeRequest),
			advisoryWriteCh: make(chan writeRequest),
		},

		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(settings.MaxFramePayload),
		}, config: connConfigState{local: Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			peer:       Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, Capabilities: 0, LocalRole: localRole, PeerRole: peerRole, PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: settings.InitialMaxData,
			sendSessionMax: settings.InitialMaxData}, registry: connRegistryState{streams: make(map[uint64]*nativeStream),
			tombstones:     make(map[uint64]streamTombstone),
			usedStreamData: make(map[uint64]usedStreamMarker),

			nextLocalBidi: state.FirstLocalStreamID(localRole, true),
			nextLocalUni:  state.FirstLocalStreamID(localRole, false),
			nextPeerBidi:  state.FirstPeerStreamID(localRole, true),
			nextPeerUni:   state.FirstPeerStreamID(localRole, false)},
	}
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stop:
				return
			case req := <-c.writer.urgentWriteCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				if req.done != nil {
					req.done <- nil
				}
			}
		}
	}()
	go func() {
		defer close(flushDone)
		for {
			select {
			case <-stop:
				return
			case <-c.lifecycle.closedCh:
				return
			case <-c.pending.controlNotify:
				c.mu.Lock()
				result := takePendingTerminalControlRequestForTest(c)
				c.mu.Unlock()
				if result.err != nil {
					c.closeSession(result.err)
					return
				}
				if !result.hasRequest() {
					continue
				}
				select {
				case <-stop:
					return
				case <-c.lifecycle.closedCh:
					return
				case c.writer.urgentWriteCh <- result.request:
				}
			}
		}
	}()
	return c, frames, func() {
		select {
		case <-c.lifecycle.closedCh:
		default:
			close(c.lifecycle.closedCh)
		}
		close(stop)
		select {
		case <-writerDone:
		case <-time.After(testSignalTimeout):
			t.Fatal("stop-sending drain-timeout writer consumer did not exit")
		}
		select {
		case <-flushDone:
		case <-time.After(testSignalTimeout):
			t.Fatal("stop-sending drain-timeout control flush loop did not exit")
		}
	}
}

func TestCloseReadLateDataIsDiscardedAndSessionBudgetRestored(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	serverStream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	c.flow.recvSessionAdvertised = 100

	if err := serverStream.CloseRead(); err != nil {
		t.Fatalf("CloseRead: %v", err)
	}
	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeStopSending {
		t.Fatalf("queued frame type = %v, want %v", queued.Type, FrameTypeStopSending)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	advertisedBefore := c.flow.recvSessionAdvertised
	c.mu.Unlock()

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: serverStream.id,
		Payload:  []byte("x"),
	})
	if err != nil {
		t.Fatalf("late DATA after CloseRead: %v", err)
	}

	c.mu.Lock()
	recvBuffer := serverStream.recvBuffer
	readBufLen := len(serverStream.readBuf)
	advertisedAfter := c.flow.recvSessionAdvertised
	c.mu.Unlock()
	if recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", recvBuffer)
	}
	if readBufLen != 0 {
		t.Fatalf("readBuf len = %d, want 0", readBufLen)
	}
	if advertisedAfter != advertisedBefore+1 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", advertisedAfter, advertisedBefore+1)
	}

	_, err = serverStream.Read(make([]byte, 8))
	if !errors.Is(err, ErrReadClosed) {
		t.Fatalf("read after CloseRead err = %v, want %v", err, ErrReadClosed)
	}
}

func TestRecvStopSentLateFinMarksRecvFinWithoutBuffering(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(accepted)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	case serverStream = <-acceptCh:
	}

	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("initial read: %v", err)
	}

	server.mu.Lock()
	serverStream.setRecvStopSent()
	server.mu.Unlock()

	err = server.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagFIN,
		StreamID: serverStream.id,
		Payload:  []byte("x"),
	})
	if err != nil {
		t.Fatalf("late DATA|FIN after CloseRead: %v", err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if !serverStream.recvFinReached() {
		t.Fatal("recvFinReached() = false, want true")
	}
	if serverStream.recvHalf != state.RecvHalfFin {
		t.Fatalf("recvHalf = %v, want %v", serverStream.recvHalf, state.RecvHalfFin)
	}
	if serverStream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", serverStream.recvBuffer)
	}
	if len(serverStream.readBuf) != 0 {
		t.Fatalf("readBuf len = %d, want 0", len(serverStream.readBuf))
	}
}

func TestLateDataAfterCloseReadRestoresSessionBudgetOnlyAndDropsStreamBudget(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(accepted)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept stream: %v", err)
	case serverStream = <-acceptCh:
	}
	server.mu.Lock()
	streamRecvAdvertised := serverStream.recvAdvertised
	sessionAdvertisedBefore := server.flow.recvSessionAdvertised
	server.mu.Unlock()

	if err := serverStream.CloseRead(); err != nil {
		t.Fatalf("close read: %v", err)
	}

	if err := server.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: serverStream.id,
		Payload:  []byte("x"),
	}); err != nil {
		t.Fatalf("late data after close read: %v", err)
	}

	server.mu.Lock()
	if server.flow.recvSessionAdvertised != sessionAdvertisedBefore+3 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", server.flow.recvSessionAdvertised, sessionAdvertisedBefore+3)
	}
	if serverStream.recvAdvertised != streamRecvAdvertised {
		t.Fatalf("recvAdvertised = %d, want %d", serverStream.recvAdvertised, streamRecvAdvertised)
	}
	if serverStream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", serverStream.recvBuffer)
	}
	server.mu.Unlock()
}

func TestLateDataAfterRecvResetRestoresSessionBudgetOnly(t *testing.T) {
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := &nativeStream{
		id:           4,
		localOpen:    testLocalOpenClosedState(),
		localSend:    true,
		localReceive: true,
		recvReset:    &ApplicationError{Code: uint64(CodeCancelled)},
		recvBuffer:   0,
	}
	stream.initHalfStates()
	stream.recvHalf = state.RecvHalfReset
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	c.registry.streams[4] = stream
	c.flow.recvSessionAdvertised = 100

	before := c.flow.recvSessionAdvertised
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: 4,
		Payload:  []byte("x"),
	}); err != nil {
		t.Fatalf("late data after recv reset: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionAdvertised != before+1 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, before+1)
	}
	wantRecvAdvertised := stream.recvAdvertised
	if stream.recvAdvertised != wantRecvAdvertised {
		t.Fatalf("recvAdvertised = %d, want %d", stream.recvAdvertised, wantRecvAdvertised)
	}
}

func TestLateDataAfterSendAbortDropsUnbufferedPayloadAndRestoresSessionBudget(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	stream.setAbortedWithSource(&ApplicationError{Code: uint64(CodeRefusedStream), Reason: "peer abort"}, terminalAbortLocal)
	testMarkLocalOpenCommitted(stream)
	stream.recvBuffer = 2
	stream.readBuf = []byte("xy")

	c.mu.Lock()
	c.flow.recvSessionAdvertised = 100
	c.flow.recvSessionReceived = 0
	beforeSessionAdvertised := c.flow.recvSessionAdvertised
	beforeRecvAdvertised := stream.recvAdvertised
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("late"),
	}); err != nil {
		t.Fatalf("late DATA after send abort err = %v", err)
	}

	var recvSessionAdvertised uint64
	var recvBuffer uint64
	var readBufLen int
	var recvAdvertised uint64
	var recvHalfState state.RecvHalfState
	c.mu.Lock()
	recvSessionAdvertised = c.flow.recvSessionAdvertised
	recvBuffer = stream.recvBuffer
	readBufLen = len(stream.readBuf)
	recvAdvertised = stream.recvAdvertised
	recvHalfState = stream.recvHalfState()
	c.mu.Unlock()

	if recvSessionAdvertised != beforeSessionAdvertised+4 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", recvSessionAdvertised, beforeSessionAdvertised+4)
	}
	if recvBuffer != 2 {
		t.Fatalf("recvBuffer = %d, want 2", recvBuffer)
	}
	if readBufLen != 2 {
		t.Fatalf("readBuf len = %d, want 2", readBufLen)
	}
	if recvAdvertised != beforeRecvAdvertised {
		t.Fatalf("recvAdvertised = %d, want %d", recvAdvertised, beforeRecvAdvertised)
	}
	if recvHalfState != state.RecvHalfAborted {
		t.Fatalf("recvHalf = %v, want recv_aborted", recvHalfState)
	}

	n, readErr := stream.Read(make([]byte, 8))
	if n != 2 {
		t.Fatalf("read n = %d, want 2", n)
	}
	if readErr != nil {
		t.Fatalf("read while terminal buffered data err = %v, want nil", readErr)
	}

	_, readErr = stream.Read(make([]byte, 8))
	var appErr *ApplicationError
	if !errors.As(readErr, &appErr) {
		t.Fatalf("read after send abort err = %v, want send-side terminal error", readErr)
	}
	_, writeErr := stream.Write([]byte("z"))
	appErr = nil
	if !errors.As(writeErr, &appErr) {
		t.Fatalf("write after send abort err = %v, want send-side terminal error", writeErr)
	}
}

func TestLateDataAfterSendAbortWhenSessionBudgetExceededReturnsFlowControl(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_aborted",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	c.flow.recvSessionAdvertised = 4
	c.flow.recvSessionReceived = 4

	c.mu.Lock()
	beforeSessionAdvertised := c.flow.recvSessionAdvertised
	beforeSessionReceived := c.flow.recvSessionReceived
	beforeRecvBuffer := stream.recvBuffer
	beforeRecvAdvertised := stream.recvAdvertised
	c.mu.Unlock()

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	})
	if !IsErrorCode(err, CodeFlowControl) {
		t.Fatalf("late DATA after send abort with exhausted session budget err = %v, want %s", err, CodeFlowControl)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionAdvertised != beforeSessionAdvertised {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, beforeSessionAdvertised)
	}
	if c.flow.recvSessionReceived != beforeSessionReceived {
		t.Fatalf("recvSessionReceived = %d, want %d", c.flow.recvSessionReceived, beforeSessionReceived)
	}
	if stream.recvAdvertised != beforeRecvAdvertised {
		t.Fatalf("recvAdvertised = %d, want %d", stream.recvAdvertised, beforeRecvAdvertised)
	}
	if stream.recvBuffer != beforeRecvBuffer {
		t.Fatalf("recvBuffer = %d, want %d", stream.recvBuffer, beforeRecvBuffer)
	}
}

func TestLateDataAfterFullyTerminalLiveStreamIsIgnored(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_reset",
		RecvHalf: "recv_fin",
	})
	testMarkLocalOpenCommitted(stream)
	c.flow.recvSessionAdvertised = 100
	c.flow.recvSessionReceived = 0

	c.mu.Lock()
	beforeSessionAdvertised := c.flow.recvSessionAdvertised
	beforeRecvAdvertised := stream.recvAdvertised
	beforeRecvBuffer := stream.recvBuffer
	beforeReadBufLen := len(stream.readBuf)
	beforeLateData := stream.lateDataReceived
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("late"),
	}); err != nil {
		t.Fatalf("late DATA after fully terminal live stream err = %v", err)
	}

	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionAdvertised != beforeSessionAdvertised+4 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, beforeSessionAdvertised+4)
	}
	if stream.recvAdvertised != beforeRecvAdvertised {
		t.Fatalf("recvAdvertised = %d, want %d", stream.recvAdvertised, beforeRecvAdvertised)
	}
	if stream.recvBuffer != beforeRecvBuffer {
		t.Fatalf("recvBuffer = %d, want %d", stream.recvBuffer, beforeRecvBuffer)
	}
	if len(stream.readBuf) != beforeReadBufLen {
		t.Fatalf("readBuf len = %d, want %d", len(stream.readBuf), beforeReadBufLen)
	}
	if stream.lateDataReceived != beforeLateData {
		t.Fatalf("lateDataReceived = %d, want %d", stream.lateDataReceived, beforeLateData)
	}
	if stream.sendHalfState() != state.SendHalfReset {
		t.Fatalf("sendHalf = %v, want send_reset", stream.sendHalfState())
	}
	if stream.recvHalfState() != state.RecvHalfFin {
		t.Fatalf("recvHalf = %v, want recv_fin", stream.recvHalfState())
	}
}

func TestLateDataPerStreamCapAfterPeerReset(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.ingress.aggregateLateDataCap = 0
	c.ingress.lateDataPerStreamCap = 1
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_reset",
	})
	c.flow.recvSessionAdvertised = 100
	c.flow.recvSessionReceived = 0

	beforeSessionAdvertised := c.flow.recvSessionAdvertised

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("a"),
	}); err != nil {
		t.Fatalf("first late DATA after peer RESET err = %v", err)
	}

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("b"),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("second late DATA after peer RESET err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionAdvertised != beforeSessionAdvertised+2 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, beforeSessionAdvertised+2)
	}
	if stream.lateDataReceived != 2 {
		t.Fatalf("lateDataReceived = %d, want %d", stream.lateDataReceived, 2)
	}
	if c.ingress.aggregateLateData != 2 {
		t.Fatalf("aggregateLateData = %d, want %d", c.ingress.aggregateLateData, 2)
	}
}

func TestLateDataPerStreamCapForRepositoryDefault(t *testing.T) {
	t.Parallel()

	if got := lateDataPerStreamCapFor(64*1024, 16*1024); got != 8*1024 {
		t.Fatalf("lateDataPerStreamCapFor(64KiB, 16KiB) = %d, want %d", got, 8*1024)
	}
	if got := lateDataPerStreamCapFor(1<<20, 16*1024); got != 32*1024 {
		t.Fatalf("lateDataPerStreamCapFor(1MiB, 16KiB) = %d, want %d", got, 32*1024)
	}
	if got := lateDataPerStreamCapFor(0, 16*1024); got != 1024 {
		t.Fatalf("lateDataPerStreamCapFor(0, 16KiB) = %d, want 1024", got)
	}
}

func TestLateDataAfterRecvAbortedWithZeroInitialWindowUsesMinimumCap(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_aborted",
	})
	c.flow.recvSessionAdvertised = 100

	beforeSessionAdvertised := c.flow.recvSessionAdvertised

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	}); err != nil {
		t.Fatalf("late DATA after peer ABORT with zero initial window err = %v, want nil", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionAdvertised != beforeSessionAdvertised+1 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, beforeSessionAdvertised+1)
	}
	if stream.lateDataReceived != 1 {
		t.Fatalf("lateDataReceived = %d, want 1", stream.lateDataReceived)
	}
}

func TestLateDataPerStreamCapAfterPeerCloseWithError(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.ingress.aggregateLateDataCap = 0
	c.ingress.lateDataPerStreamCap = 1
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_aborted",
	})
	c.flow.recvSessionAdvertised = 100
	c.flow.recvSessionReceived = 0

	beforeSessionAdvertised := c.flow.recvSessionAdvertised

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("a"),
	}); err != nil {
		t.Fatalf("first late DATA after peer ABORT err = %v", err)
	}

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("b"),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("second late DATA after peer ABORT err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionAdvertised != beforeSessionAdvertised+2 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, beforeSessionAdvertised+2)
	}
	if stream.lateDataReceived != 2 {
		t.Fatalf("lateDataReceived = %d, want %d", stream.lateDataReceived, 2)
	}
	if c.ingress.aggregateLateData != 2 {
		t.Fatalf("aggregateLateData = %d, want %d", c.ingress.aggregateLateData, 2)
	}
}

func TestLateDataAfterRecvAbortedRestoresSessionBudgetOnly(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_aborted",
	})
	c.flow.recvSessionAdvertised = 100

	beforeSessionAdvertised := c.flow.recvSessionAdvertised
	beforeSessionReceived := c.flow.recvSessionReceived
	beforeRecvAdvertised := stream.recvAdvertised

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	})
	if err != nil {
		t.Fatalf("late DATA after peer ABORT err = %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionAdvertised != beforeSessionAdvertised+1 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, beforeSessionAdvertised+1)
	}
	if c.flow.recvSessionReceived != beforeSessionReceived+1 {
		t.Fatalf("recvSessionReceived = %d, want %d", c.flow.recvSessionReceived, beforeSessionReceived+1)
	}
	if stream.recvAdvertised != beforeRecvAdvertised {
		t.Fatalf("recvAdvertised = %d, want %d", stream.recvAdvertised, beforeRecvAdvertised)
	}
	if stream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", stream.recvBuffer)
	}
}

func TestLateDataAfterRecvAbortedWhenSessionWindowExceededReturnsFlowControl(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_aborted",
	})
	c.flow.recvSessionAdvertised = 100
	c.flow.recvSessionReceived = 100

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	})
	if !IsErrorCode(err, CodeFlowControl) {
		t.Fatalf("late DATA after peer ABORT with session budget exceeded err = %v, want %s", err, CodeFlowControl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionReceived != 100 {
		t.Fatalf("recvSessionReceived = %d, want %d", c.flow.recvSessionReceived, 100)
	}
	if c.flow.recvSessionAdvertised != 100 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, 100)
	}
	if stream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", stream.recvBuffer)
	}
}

func TestLateDataAggregateCapAfterMultipleTerminalDirections(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.ingress.aggregateLateDataCap = 2
	stream1 := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_reset",
	})
	stream2 := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)+4, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_aborted",
	})
	stream3 := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)+8, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_stop_sent",
	})

	c.flow.recvSessionAdvertised = 100
	c.flow.recvSessionReceived = 0
	stream1.recvBuffer = 0
	stream2.recvBuffer = 0
	stream3.recvBuffer = 0
	stream1.readBuf = nil
	stream2.readBuf = nil
	stream3.readBuf = nil
	stream1.recvAdvertised = 0
	stream2.recvAdvertised = 0
	stream3.recvAdvertised = 0

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream1.id,
		Payload:  []byte("x"),
	}); err != nil {
		t.Fatalf("late DATA on stream1 err = %v", err)
	}
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream2.id,
		Payload:  []byte("x"),
	}); err != nil {
		t.Fatalf("late DATA on stream2 err = %v", err)
	}
	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream3.id,
		Payload:  []byte("x"),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("late DATA on stream3 err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ingress.aggregateLateData <= c.ingress.aggregateLateDataCap {
		t.Fatalf("aggregateLateData = %d, want > %d", c.ingress.aggregateLateData, c.ingress.aggregateLateDataCap)
	}
	if c.flow.recvSessionAdvertised != 103 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, 103)
	}
	if stream1.recvAdvertised != 0 || stream2.recvAdvertised != 0 || stream3.recvAdvertised != 0 {
		t.Fatalf("per-stream advertised changed: %d %d %d", stream1.recvAdvertised, stream2.recvAdvertised, stream3.recvAdvertised)
	}
}

func TestLateDataPerStreamCapAfterCloseRead(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.ingress.aggregateLateDataCap = 0
	c.ingress.lateDataPerStreamCap = 1
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	stream.recvBuffer = 0
	stream.readBuf = nil

	if err := stream.CloseRead(); err != nil {
		t.Fatalf("local CloseRead: %v", err)
	}

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("a"),
	}); err != nil {
		t.Fatalf("first late DATA after CloseRead err = %v", err)
	}

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("b"),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("second late DATA after CloseRead err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.lateDataReceived != 2 {
		t.Fatalf("lateDataReceived = %d, want %d", stream.lateDataReceived, 2)
	}
	if c.ingress.lateDataPerStreamCap != 1 {
		t.Fatalf("lateDataPerStreamCap = %d, want %d", c.ingress.lateDataPerStreamCap, 1)
	}
	if c.ingress.aggregateLateData != 2 {
		t.Fatalf("aggregateLateData = %d, want %d", c.ingress.aggregateLateData, 2)
	}
}

func TestLateOpenMetadataAfterCloseReadReturnsProtocolError(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	if err := stream.CloseRead(); err != nil {
		t.Fatalf("local CloseRead: %v", err)
	}
	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeStopSending {
		t.Fatalf("queued frame type = %v, want %v", queued.Type, FrameTypeStopSending)
	}
	assertNoQueuedFrame(t, frames)

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	err = c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagOpenMetadata,
		StreamID: stream.id,
		Payload:  prefix,
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("late DATA|OPEN_METADATA after CloseRead err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.lateDataReceived != 0 {
		t.Fatalf("lateDataReceived = %d, want 0 after rejected OPEN_METADATA", stream.lateDataReceived)
	}
	if stream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", stream.recvBuffer)
	}
	if len(stream.readBuf) != 0 {
		t.Fatalf("readBuf len = %d, want 0", len(stream.readBuf))
	}
}

func TestDefaultLateDataPerStreamCapAfterCloseReadUsesInitialWindowFraction(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.ingress.aggregateLateDataCap = 0
	c.ingress.lateDataPerStreamCap = 0
	c.config.local.Settings.MaxFramePayload = 16
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 24

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	stream.recvBuffer = 0
	stream.readBuf = nil

	if err := stream.CloseRead(); err != nil {
		t.Fatalf("local CloseRead: %v", err)
	}

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  bytes.Repeat([]byte("x"), 1024),
	}); err != nil {
		t.Fatalf("late DATA at default minimum cap err = %v, want nil", err)
	}

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("late DATA beyond default cap err = %v, want %s", err, CodeProtocol)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.lateDataReceived != 1025 {
		t.Fatalf("lateDataReceived = %d, want 1025", stream.lateDataReceived)
	}
}

func TestLateDataAfterPeerResetStillCountsAgainstSessionLimit(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_reset",
	})
	c.flow.recvSessionReceived = c.flow.recvSessionAdvertised

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	})
	if !IsErrorCode(err, CodeFlowControl) {
		t.Fatalf("late DATA after peer RESET err = %v, want %s", err, CodeFlowControl)
	}
}

func TestLateDataAfterRecvStopSentRestoresSessionBudgetOnly(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_stop_sent",
	})
	c.flow.recvSessionReceived = 0
	c.flow.recvSessionAdvertised = 6
	stream.recvAdvertised = 4
	stream.recvBuffer = 0
	stream.readBuf = nil
	beforeSessionAdvertised := c.flow.recvSessionAdvertised
	beforeRecvAdvertised := stream.recvAdvertised

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	})
	if err != nil {
		t.Fatalf("late DATA after STOP_SENDING err = %v", err)
	}

	c.mu.Lock()
	if !stream.readStopSentLocked() {
		c.mu.Unlock()
		t.Fatal("stream became active after STOP_SENDING terminal")
	}
	if c.flow.recvSessionAdvertised != beforeSessionAdvertised+1 {
		c.mu.Unlock()
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, beforeSessionAdvertised+1)
	}
	if stream.recvAdvertised != beforeRecvAdvertised {
		c.mu.Unlock()
		t.Fatalf("recvAdvertised = %d, want %d", stream.recvAdvertised, beforeRecvAdvertised)
	}
	if stream.recvBuffer != 0 {
		c.mu.Unlock()
		t.Fatalf("recvBuffer = %d, want 0", stream.recvBuffer)
	}
	c.mu.Unlock()

	assertNoQueuedFrame(t, frames)
}

func TestLateDataAfterRecvStopSentCountsLateTailAndReturnsReadClosed(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_stop_sent",
	})
	stream.recvAdvertised = 32
	c.flow.recvSessionUsed = 4
	c.flow.recvSessionAdvertised = 100
	beforeSessionAdvertised := c.flow.recvSessionAdvertised

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	}); err != nil {
		t.Fatalf("late DATA on recv_stop_sent stream err = %v", err)
	}

	c.mu.Lock()
	recvAdvertised := stream.recvAdvertised
	sessionAdvertised := c.flow.recvSessionAdvertised
	streamLateData := stream.lateDataReceived
	c.mu.Unlock()
	if recvAdvertised != 32 {
		t.Fatalf("recvAdvertised = %d, want %d", recvAdvertised, 32)
	}
	if sessionAdvertised != beforeSessionAdvertised+1 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", sessionAdvertised, beforeSessionAdvertised+1)
	}
	if streamLateData != 1 {
		t.Fatalf("lateDataReceived = %d, want 1", streamLateData)
	}

	_, err := stream.Read(make([]byte, 8))
	if !errors.Is(err, ErrReadClosed) {
		t.Fatalf("read after late DATA on stopped recv half err = %v, want %v", err, ErrReadClosed)
	}
}

func TestLateDataAfterRecvStopSentWhenSessionBudgetExceededReturnsFlowControl(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_stop_sent",
	})
	stream.recvAdvertised = 4
	c.flow.recvSessionAdvertised = 100
	c.flow.recvSessionReceived = 100

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	})
	if !IsErrorCode(err, CodeFlowControl) {
		t.Fatalf("late DATA after STOP_SENDING with budget exhausted err = %v, want %s", err, CodeFlowControl)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flow.recvSessionReceived != 100 {
		t.Fatalf("recvSessionReceived = %d, want %d", c.flow.recvSessionReceived, 100)
	}
	if c.flow.recvSessionAdvertised != 100 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, 100)
	}
	if stream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", stream.recvBuffer)
	}
}

func TestLateDataAfterPeerFinNoBudgetSideEffects(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_fin",
	})
	beforeSessionAdvertised := c.flow.recvSessionAdvertised
	beforeSessionReceived := c.flow.recvSessionReceived
	beforeRecvAdvertised := stream.recvAdvertised

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	})
	if err != nil {
		t.Fatalf("late DATA after peer FIN err = %v, want nil", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.registry.streams[stream.id]; ok {
		t.Fatalf("stream %d remained live after late DATA past recv_fin", stream.id)
	}
	if !c.hasTerminalMarkerLocked(stream.id) {
		t.Fatalf("stream %d missing terminal marker after late DATA past recv_fin", stream.id)
	}
	if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeStreamClosed) {
		t.Fatalf("sendAbort = %v, want CODE_STREAM_CLOSED", stream.sendAbort)
	}
	if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeStreamClosed) {
		t.Fatalf("recvAbort = %v, want CODE_STREAM_CLOSED", stream.recvAbort)
	}
	if c.flow.recvSessionAdvertised != beforeSessionAdvertised {
		t.Fatalf("recvSessionAdvertised = %d, want %d", c.flow.recvSessionAdvertised, beforeSessionAdvertised)
	}
	if c.flow.recvSessionReceived != beforeSessionReceived {
		t.Fatalf("recvSessionReceived = %d, want %d", c.flow.recvSessionReceived, beforeSessionReceived)
	}
	if stream.recvAdvertised != beforeRecvAdvertised {
		t.Fatalf("recvAdvertised = %d, want %d", stream.recvAdvertised, beforeRecvAdvertised)
	}
	if stream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", stream.recvBuffer)
	}
}

func TestStatsTrackLateDataDiagnosticsByCause(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.flow.recvSessionAdvertised = 100
	resetStream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_reset",
	})
	abortStream := seedStateFixtureStream(t, c, resetStream.id+4, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_aborted",
	})
	closeReadStream := seedStateFixtureStream(t, c, abortStream.id+4, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	if err := closeReadStream.CloseRead(); err != nil {
		t.Fatalf("CloseRead: %v", err)
	}

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: resetStream.id, Payload: []byte("r")}); err != nil {
		t.Fatalf("late DATA after recv_reset: %v", err)
	}
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: abortStream.id, Payload: []byte("a")}); err != nil {
		t.Fatalf("late DATA after recv_aborted: %v", err)
	}
	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: closeReadStream.id, Payload: []byte("c")}); err != nil {
		t.Fatalf("late DATA after CloseRead: %v", err)
	}

	c.mu.Lock()
	c.registry.usedStreamData[closeReadStream.id+4] = usedStreamMarker{
		action: lateDataIgnore,
		cause:  lateDataCauseReset,
	}
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: closeReadStream.id + 4, Payload: []byte("m")}); err != nil {
		t.Fatalf("late DATA on marker-only reset stream: %v", err)
	}

	stats := c.Stats()
	if got := stats.Diagnostics.LateDataAfterCloseRead; got != 1 {
		t.Fatalf("late data after CloseRead = %d, want 1", got)
	}
	if got := stats.Diagnostics.LateDataAfterReset; got != 2 {
		t.Fatalf("late data after RESET = %d, want 2", got)
	}
	if got := stats.Diagnostics.LateDataAfterAbort; got != 1 {
		t.Fatalf("late data after ABORT = %d, want 1", got)
	}
	if got := stats.Pressure.AggregateLateData; got != 4 {
		t.Fatalf("aggregate late data = %d, want 4", got)
	}
}

func TestAbortFirstHiddenStreamCompactsToTombstone(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle first ABORT: %v", err)
	}

	c.mu.Lock()
	if stream := c.registry.streams[streamID]; stream != nil {
		c.mu.Unlock()
		t.Fatalf("stream %d still retained as live state", streamID)
	}
	ts, ok := c.registry.tombstones[streamID]
	if !ok {
		c.mu.Unlock()
		t.Fatalf("stream %d missing tombstone after first ABORT", streamID)
	}
	if ts.TerminalKind != state.TerminalKindAborted {
		c.mu.Unlock()
		t.Fatalf("hidden ABORT tombstone kind = %v, want aborted", ts.TerminalKind)
	}
	if !ts.HasTerminalCode || ts.TerminalCode != uint64(CodeCancelled) {
		c.mu.Unlock()
		t.Fatalf("hidden ABORT tombstone code = (%v,%d), want (%v,%d)", ts.HasTerminalCode, ts.TerminalCode, true, uint64(CodeCancelled))
	}
	if len(c.queues.acceptBidi.items) != 0 || len(c.queues.acceptUni.items) != 0 {
		c.mu.Unlock()
		t.Fatalf("hidden ABORT-first stream was queued to accept")
	}
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("late"),
	}); err != nil {
		t.Fatalf("late DATA on hidden tombstone: %v", err)
	}

	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	if c.flow.recvSessionUsed != 0 {
		c.mu.Unlock()
		t.Fatalf("recvSessionUsed = %d, want 0 after late tombstone discard", c.flow.recvSessionUsed)
	}
	c.mu.Unlock()
}

func TestLateDataOnGracefulTerminalTombstoneAbortsStreamClosed(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	c.mu.Lock()
	c.maybeCompactTerminalLocked(stream)
	if c.registry.streams[streamID] != nil {
		c.mu.Unlock()
		t.Fatalf("stream %d still retained as live state", streamID)
	}
	ts, ok := c.registry.tombstones[streamID]
	if !ok {
		c.mu.Unlock()
		t.Fatalf("stream %d missing tombstone after graceful terminal compaction", streamID)
	}
	if ts.TerminalKind != state.TerminalKindGraceful {
		c.mu.Unlock()
		t.Fatalf("graceful tombstone kind = %v, want graceful", ts.TerminalKind)
	}
	if ts.HasTerminalCode {
		c.mu.Unlock()
		t.Fatalf("graceful tombstone unexpectedly kept terminal code %d", ts.TerminalCode)
	}
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("late"),
	}); err != nil {
		t.Fatalf("late DATA on graceful tombstone: %v", err)
	}

	assertInvalidQueuedAbortCode(t, frames, streamID, CodeStreamClosed)
}

func TestLateDataOnGracefulTerminalTombstoneCountsAgainstSessionLimit(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	c.mu.Lock()
	c.maybeCompactTerminalLocked(stream)
	c.flow.recvSessionReceived = c.flow.recvSessionAdvertised
	c.mu.Unlock()

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("late"),
	})
	if !IsErrorCode(err, CodeFlowControl) {
		t.Fatalf("late DATA on graceful tombstone err = %v, want %s", err, CodeFlowControl)
	}
}

func TestLateDataOnGracefulTerminalTombstoneCountsAggregateCap(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	c.mu.Lock()
	c.maybeCompactTerminalLocked(stream)
	c.ingress.aggregateLateDataCap = 1
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("a"),
	}); err != nil {
		t.Fatalf("first late DATA on graceful tombstone err = %v", err)
	}
	assertInvalidQueuedAbortCode(t, frames, streamID, CodeStreamClosed)

	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("b"),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("second late DATA on graceful tombstone err = %v, want %s", err, CodeProtocol)
	}
	c.mu.Lock()
	aggregateLateData := c.ingress.aggregateLateData
	c.mu.Unlock()
	if aggregateLateData <= 1 {
		t.Fatalf("aggregateLateData = %d, want > 1", aggregateLateData)
	}
}

func TestLateDataOnSendOnlyTombstoneIgnored(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, false)
	stream := &nativeStream{
		id:           streamID,
		conn:         c,
		idSet:        true,
		localOpen:    testLocalOpenClosedState(),
		localSend:    true,
		localReceive: false,
		sendHalf:     state.SendHalfFin,
		recvHalf:     state.RecvHalfAbsent,
	}
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	c.registry.streams[streamID] = stream

	c.mu.Lock()
	c.maybeCompactTerminalLocked(stream)
	if c.registry.streams[streamID] != nil {
		c.mu.Unlock()
		t.Fatalf("stream %d still retained as live state", streamID)
	}
	ts, ok := c.registry.tombstones[streamID]
	if !ok {
		c.mu.Unlock()
		t.Fatalf("stream %d missing tombstone after terminal compaction", streamID)
	}
	if ts.DataAction != lateDataIgnore {
		c.mu.Unlock()
		t.Fatalf("stream %d tombstone data action = %v, want lateDataIgnore", streamID, ts.DataAction)
	}
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("late"),
	}); err != nil {
		t.Fatalf("late DATA on send-only tombstone: %v", err)
	}

	assertNoQueuedFrame(t, frames)
}

func TestRemoveTombstoneByIDKeepsQueueEndpointsStable(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		4:  {},
		8:  {},
		12: {},
	}
	c.registry.tombstoneOrder = []uint64{4, 8, 12}
	removed := c.removeTombstoneLocked(8)
	head := c.tombstoneHeadLocked()
	tail := c.tombstoneTailLocked()
	count := c.tombstoneCountLocked()
	c.mu.Unlock()

	if !removed {
		t.Fatal("removeTombstoneLocked(8) = false, want true")
	}
	if count != 2 {
		t.Fatalf("tombstoneCountLocked() = %d, want 2", count)
	}
	if !head.found() || head.streamID != 4 {
		t.Fatalf("tombstoneHeadLocked() = (%d,%v), want (4,true)", head.streamID, head.found())
	}
	if !tail.found() || tail.streamID != 12 {
		t.Fatalf("tombstoneTailLocked() = (%d,%v), want (12,true)", tail.streamID, tail.found())
	}
}

func TestHiddenQueueRebuildSkipsSparseRemovedTombstones(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		4:  {Hidden: true},
		8:  {},
		12: {Hidden: true},
	}
	c.registry.tombstoneOrder = []uint64{4, 8, 12}
	if !c.removeTombstoneLocked(8) {
		c.mu.Unlock()
		t.Fatal("removeTombstoneLocked(8) = false, want true")
	}
	c.registry.hiddenTombstonesInit = false
	count := c.hiddenControlStateRetainedLocked()
	head := c.hiddenTombstoneHeadLocked()
	tail := c.hiddenTombstoneTailLocked()
	c.mu.Unlock()

	if count != 2 {
		t.Fatalf("hiddenControlStateRetainedLocked() = %d, want 2", count)
	}
	if !head.found() || head.streamID != 4 {
		t.Fatalf("hiddenTombstoneHeadLocked() = (%d,%v), want (4,true)", head.streamID, head.found())
	}
	if !tail.found() || tail.streamID != 12 {
		t.Fatalf("hiddenTombstoneTailLocked() = (%d,%v), want (12,true)", tail.streamID, tail.found())
	}
}

func TestHiddenQueueRebuildDropsBackingWhenNoHiddenTombstones(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		4: {},
	}
	c.registry.tombstoneOrder = []uint64{4}
	c.registry.hiddenTombstoneOrder = make([]uint64, 0, 128)
	c.registry.hiddenTombstonesInit = false
	count := c.hiddenControlStateRetainedLocked()
	order := c.registry.hiddenTombstoneOrder
	c.mu.Unlock()

	if count != 0 {
		t.Fatalf("hiddenControlStateRetainedLocked() = %d, want 0", count)
	}
	if order != nil {
		t.Fatalf("hiddenTombstoneOrder cap = %d, want nil backing", cap(order))
	}
}

func TestRemoveHiddenTombstoneFallsBackWhenHiddenIndexStale(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		4:  {Hidden: true, HiddenIndex: 0},
		8:  {Hidden: true, HiddenIndex: 0},
		12: {Hidden: true, HiddenIndex: 2},
	}
	c.registry.hiddenTombstoneOrder = []uint64{4, 8, 12}
	c.registry.hiddenTombstoneCount = 3
	c.registry.hiddenTombstonesInit = true

	tombstone := c.registry.tombstones[8]
	c.removeHiddenTombstoneLocked(8, tombstone)

	order := append([]uint64(nil), c.registry.hiddenTombstoneOrder...)
	count := c.registry.hiddenTombstoneCount
	head := c.registry.hiddenTombstoneHead
	first := c.registry.tombstones[4]
	last := c.registry.tombstones[12]
	c.mu.Unlock()

	if len(order) != 3 || order[0] != 4 || order[1] != 0 || order[2] != 12 {
		t.Fatalf("hiddenTombstoneOrder = %v, want [4 0 12]", order)
	}
	if count != 2 {
		t.Fatalf("hiddenTombstoneCount = %d, want 2", count)
	}
	if head != 0 {
		t.Fatalf("hiddenTombstoneHead = %d, want 0", head)
	}
	if first.HiddenIndex != 0 || last.HiddenIndex != 2 {
		t.Fatalf("hidden indexes = (%d,%d), want (0,2)", first.HiddenIndex, last.HiddenIndex)
	}
}

func TestAppendTombstoneLockedNormalizesStaleOrderIndexWithoutDuplicate(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		4: {OrderIndex: 0},
		8: {OrderIndex: 0},
	}
	c.registry.tombstoneOrder = []uint64{4, 8}
	c.registry.tombstoneHead = 0
	c.registry.tombstoneCount = 2
	c.registry.tombstonesInit = true

	c.appendTombstoneLocked(8)

	order := append([]uint64(nil), c.registry.tombstoneOrder...)
	count := c.registry.tombstoneCount
	second := c.registry.tombstones[8]
	c.mu.Unlock()

	if len(order) != 2 || order[0] != 4 || order[1] != 8 {
		t.Fatalf("tombstoneOrder = %v, want [4 8]", order)
	}
	if count != 2 {
		t.Fatalf("tombstoneCount = %d, want 2", count)
	}
	if second.OrderIndex != 1 {
		t.Fatalf("tombstone OrderIndex = %d, want normalized index 1", second.OrderIndex)
	}
}

func TestAppendHiddenTombstoneLockedNormalizesStaleHiddenIndexWithoutDuplicate(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		4: {Hidden: true, HiddenIndex: 0},
		8: {Hidden: true, HiddenIndex: 0},
	}
	c.registry.hiddenTombstoneOrder = []uint64{4, 8}
	c.registry.hiddenTombstoneCount = 2
	c.registry.hiddenTombstonesInit = true

	c.appendHiddenTombstoneLocked(8)

	order := append([]uint64(nil), c.registry.hiddenTombstoneOrder...)
	second := c.registry.tombstones[8]
	c.mu.Unlock()

	if len(order) != 2 || order[0] != 4 || order[1] != 8 {
		t.Fatalf("hiddenTombstoneOrder = %v, want [4 8]", order)
	}
	if second.HiddenIndex != 1 {
		t.Fatalf("tombstone HiddenIndex = %d, want normalized index 1", second.HiddenIndex)
	}
}

func TestMaybeCompactHiddenTombstoneQueuePreservesLiveOrderAndIndices(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		12: {Hidden: true, HiddenIndex: 2},
		16: {Hidden: true, HiddenIndex: 4},
	}
	c.registry.hiddenTombstoneOrder = []uint64{0, 0, 12, 0, 16}
	c.registry.hiddenTombstoneHead = 2
	c.registry.hiddenTombstoneCount = 2
	c.registry.hiddenTombstonesInit = true

	c.maybeCompactHiddenTombstoneQueueLocked()

	order := append([]uint64(nil), c.registry.hiddenTombstoneOrder...)
	first := c.registry.tombstones[12]
	second := c.registry.tombstones[16]
	head := c.registry.hiddenTombstoneHead
	count := c.registry.hiddenTombstoneCount
	c.mu.Unlock()

	if len(order) != 2 || order[0] != 12 || order[1] != 16 {
		t.Fatalf("hiddenTombstoneOrder = %v, want [12 16]", order)
	}
	if head != 0 {
		t.Fatalf("hiddenTombstoneHead = %d, want 0", head)
	}
	if count != 2 {
		t.Fatalf("hiddenTombstoneCount = %d, want 2", count)
	}
	if first.HiddenIndex != 0 || second.HiddenIndex != 1 {
		t.Fatalf("hidden indexes = (%d,%d), want (0,1)", first.HiddenIndex, second.HiddenIndex)
	}
}

func TestTombstoneHeadSkipsSparsePrefix(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		8:  {OrderIndex: 1},
		12: {OrderIndex: 2},
	}
	c.registry.tombstoneOrder = []uint64{4, 8, 12}
	c.registry.tombstoneHead = 0
	c.registry.tombstoneCount = 2
	c.registry.tombstonesInit = true

	head := c.tombstoneHeadLocked()
	gotHeadIndex := c.registry.tombstoneHead
	c.mu.Unlock()

	if !head.found() || head.streamID != 8 {
		t.Fatalf("tombstoneHeadLocked() = (%d,%v), want (8,true)", head.streamID, head.found())
	}
	if gotHeadIndex != 1 {
		t.Fatalf("tombstoneHead = %d, want 1 after skipping sparse prefix", gotHeadIndex)
	}
}

func TestMaybeCompactTombstoneQueuePreservesLiveOrderAndIndices(t *testing.T) {
	c := newSessionMemoryTestConn()
	c.mu.Lock()
	c.registry.tombstones = map[uint64]streamTombstone{
		8:  {OrderIndex: 1},
		16: {OrderIndex: 3},
	}
	c.registry.tombstoneOrder = make([]uint64, 4, 128)
	copy(c.registry.tombstoneOrder, []uint64{4, 8, 12, 16})
	c.registry.tombstoneHead = 1
	c.registry.tombstoneCount = 2
	c.registry.tombstonesInit = true

	c.maybeCompactTombstoneQueueLocked()

	order := append([]uint64(nil), c.registry.tombstoneOrder...)
	first := c.registry.tombstones[8]
	second := c.registry.tombstones[16]
	head := c.registry.tombstoneHead
	count := c.registry.tombstoneCount
	c.mu.Unlock()

	if head != 0 {
		t.Fatalf("tombstoneHead = %d, want 0", head)
	}
	if count != 2 {
		t.Fatalf("tombstoneCount = %d, want 2", count)
	}
	if len(order) != 2 || order[0] != 8 || order[1] != 16 {
		t.Fatalf("tombstoneOrder = %v, want [8 16]", order)
	}
	if cap(order) > compactedQueueRetainLimit(len(order)) {
		t.Fatalf("tombstoneOrder cap = %d, want <= %d", cap(order), compactedQueueRetainLimit(len(order)))
	}
	if first.OrderIndex != 0 || second.OrderIndex != 1 {
		t.Fatalf("order indexes = (%d,%d), want (0,1)", first.OrderIndex, second.OrderIndex)
	}
}

func TestLateDataOnReapedGracefulTombstoneStillAbortsStreamClosed(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.registry.tombstoneLimit = 1

	firstID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	first := seedStateFixtureStream(t, c, firstID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	c.mu.Lock()
	c.maybeCompactTerminalLocked(first)
	c.registry.nextPeerBidi = firstID + 4
	c.mu.Unlock()

	secondID := firstID + 4
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: secondID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle second hidden ABORT: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	if _, ok := c.registry.tombstones[firstID]; ok {
		c.mu.Unlock()
		t.Fatalf("first stream %d tombstone still retained after reap", firstID)
	}
	if _, ok := c.registry.usedStreamData[firstID]; !ok {
		c.mu.Unlock()
		t.Fatalf("first stream %d missing used-stream marker after tombstone reap", firstID)
	}
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: firstID,
		Payload:  []byte("late"),
	}); err != nil {
		t.Fatalf("late DATA on reaped graceful tombstone: %v", err)
	}

	assertInvalidQueuedAbortCode(t, frames, firstID, CodeStreamClosed)
}

func TestResetConvergedTombstoneRetainsTerminalIdentity(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_stop_sent",
	})

	if err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle RESET after recv_stop_sent/send_fin err = %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.registry.streams[streamID]; ok {
		t.Fatalf("stream %d still retained as live state after reset convergence", streamID)
	}
	ts, ok := c.registry.tombstones[streamID]
	if !ok {
		t.Fatalf("stream %d missing tombstone after reset convergence", streamID)
	}
	if ts.DataAction != lateDataIgnore {
		t.Fatalf("reset convergence tombstone action = %v, want ignore", ts.DataAction)
	}
	if ts.TerminalKind != state.TerminalKindReset {
		t.Fatalf("reset convergence tombstone kind = %v, want reset", ts.TerminalKind)
	}
	if !ts.HasTerminalCode || ts.TerminalCode != uint64(CodeCancelled) {
		t.Fatalf("reset convergence tombstone code = (%v,%d), want (%v,%d)", ts.HasTerminalCode, ts.TerminalCode, true, uint64(CodeCancelled))
	}
	if !state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
		t.Fatal("stream should be terminal after reset convergence")
	}
}

func TestTombstoneLateDataActionUsesExplicitRecvHalfState(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := &nativeStream{
		conn:         c,
		id:           streamID,
		idSet:        true,
		bidi:         true,
		localOpen:    testLocalOpenClosedState(),
		localSend:    true,
		localReceive: true,
		sendHalf:     state.SendHalfFin,
		recvHalf:     state.RecvHalfFin,
		readNotify:   make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}
	c.registry.streams[streamID] = stream

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := stream.tombstoneLateDataActionLocked(); got != lateDataAbortClosed {
		t.Fatalf("tombstone late-data action = %v, want %v with explicit recv half", got, lateDataAbortClosed)
	}
}

func TestCompactTerminalDropsWriteBatchStateForStream(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	c.mu.Lock()
	stream.setAbortedWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalAbortLocal)
	c.writer.scheduler.State.RootVirtualTime = 19
	c.writer.scheduler.State.ServiceSeq = 9
	c.writer.scheduler.State.StreamFinishTag = map[uint64]uint64{stream.id: 3}
	c.writer.scheduler.State.StreamLastService = map[uint64]uint64{stream.id: 7}
	c.writer.scheduler.State.GroupVirtualTime = map[rt.GroupKey]uint64{{Kind: 0, Value: stream.id}: 5}
	c.writer.scheduler.State.GroupFinishTag = map[rt.GroupKey]uint64{{Kind: 0, Value: stream.id}: 7}
	c.writer.scheduler.State.GroupLastService = map[rt.GroupKey]uint64{{Kind: 0, Value: stream.id}: 8}
	c.maybeCompactTerminalLocked(stream)
	if _, ok := c.writer.scheduler.State.StreamFinishTag[stream.id]; ok {
		c.mu.Unlock()
		t.Fatalf("stream finish tag for %d still retained after compact", stream.id)
	}
	if _, ok := c.writer.scheduler.State.GroupFinishTag[rt.GroupKey{Kind: 0, Value: stream.id}]; ok {
		c.mu.Unlock()
		t.Fatalf("default-group finish tag for %d still retained after compact", stream.id)
	}
	if _, ok := c.writer.scheduler.State.StreamLastService[stream.id]; ok {
		c.mu.Unlock()
		t.Fatalf("stream last-service entry for %d still retained after compact", stream.id)
	}
	if _, ok := c.writer.scheduler.State.GroupVirtualTime[rt.GroupKey{Kind: 0, Value: stream.id}]; ok {
		c.mu.Unlock()
		t.Fatalf("default-group virtual time for %d still retained after compact", stream.id)
	}
	if _, ok := c.writer.scheduler.State.GroupLastService[rt.GroupKey{Kind: 0, Value: stream.id}]; ok {
		c.mu.Unlock()
		t.Fatalf("default-group last-service entry for %d still retained after compact", stream.id)
	}
	if c.writer.scheduler.State.RootVirtualTime != 0 || c.writer.scheduler.State.ServiceSeq != 0 {
		c.mu.Unlock()
		t.Fatalf("scheduler clocks = (%d,%d), want (0,0)", c.writer.scheduler.State.RootVirtualTime, c.writer.scheduler.State.ServiceSeq)
	}
	c.mu.Unlock()
}

func TestCompactTerminalAllowsGracefulStreamWithSentCreditRetained(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	c.mu.Lock()
	stream.sendSent = 5
	c.flow.sendSessionUsed = 5
	c.maybeCompactTerminalLocked(stream)
	_, live := c.registry.streams[stream.id]
	_, tombstoned := c.registry.tombstones[stream.id]
	c.mu.Unlock()

	if live {
		t.Fatalf("stream %d stayed live with only retained sent credit", stream.id)
	}
	if !tombstoned {
		t.Fatalf("stream %d missing tombstone after graceful compaction", stream.id)
	}
}

func TestCompactTerminalKeepsLiveStreamWhileQueuedStateRemains(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	c.mu.Lock()
	stream.queuedDataBytes = 3
	stream.inflightQueued = 2
	c.flow.queuedDataBytes = 3
	c.flow.queuedDataStreams = map[*nativeStream]struct{}{
		stream: {},
	}
	c.maybeCompactTerminalLocked(stream)
	_, live := c.registry.streams[stream.id]
	_, tombstoned := c.registry.tombstones[stream.id]
	c.mu.Unlock()

	if !live {
		t.Fatalf("stream %d was compacted while queued release state remained", stream.id)
	}
	if tombstoned {
		t.Fatalf("stream %d unexpectedly tombstoned while queued release state remained", stream.id)
	}
}

func TestReleaseBatchReservationsCompactsTerminalStreamAfterQueueDrain(t *testing.T) {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	c.mu.Lock()
	stream.sendSent = 5
	c.flow.sendSessionUsed = 5
	stream.queuedDataBytes = 3
	stream.inflightQueued = 3
	c.flow.queuedDataBytes = 3
	c.flow.queuedDataStreams = map[*nativeStream]struct{}{
		stream: {},
	}
	c.maybeCompactTerminalLocked(stream)
	if _, ok := c.registry.streams[stream.id]; !ok {
		c.mu.Unlock()
		t.Fatalf("stream %d compacted before queued state drained", stream.id)
	}
	c.mu.Unlock()

	c.releaseBatchReservations([]writeRequest{{
		queueReserved:  true,
		queuedBytes:    3,
		reservedStream: stream,
	}})

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.registry.streams[stream.id]; ok {
		t.Fatalf("stream %d still live after final queued reservation release", stream.id)
	}
	if _, ok := c.registry.tombstones[stream.id]; !ok {
		t.Fatalf("stream %d missing tombstone after queued state drained", stream.id)
	}
}

func TestHiddenControlOpenedHardCapReapsNewestRetention(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	start := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	for i := 0; i < hiddenControlRetainedHardCap+1; i++ {
		streamID := start + uint64(i*4)
		if err := c.handleAbortFrame(Frame{
			Type:     FrameTypeABORT,
			StreamID: streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		}); err != nil {
			t.Fatalf("handle hidden ABORT %d: %v", i+1, err)
		}
	}
	assertNoQueuedFrame(t, frames)

	newestID := start + uint64(hiddenControlRetainedHardCap*4)

	c.mu.Lock()
	if got := len(c.registry.streams); got != 0 {
		c.mu.Unlock()
		t.Fatalf("hidden live streams = %d, want 0", got)
	}
	if got := len(c.queues.acceptBidi.items) + len(c.queues.acceptUni.items); got != 0 {
		c.mu.Unlock()
		t.Fatalf("hidden streams entered accept queues: %d", got)
	}
	if got := len(c.registry.tombstones); got != hiddenControlRetainedHardCap {
		c.mu.Unlock()
		t.Fatalf("hidden tombstone retention = %d, want %d", got, hiddenControlRetainedHardCap)
	}
	if _, ok := c.registry.tombstones[start]; !ok {
		c.mu.Unlock()
		t.Fatalf("oldest hidden tombstone %d was unexpectedly reaped", start)
	}
	if _, ok := c.registry.tombstones[newestID]; ok {
		c.mu.Unlock()
		t.Fatalf("newest hidden tombstone %d was retained past hard cap", newestID)
	}
	if _, ok := c.registry.usedStreamData[newestID]; !ok {
		c.mu.Unlock()
		t.Fatalf("newest hidden stream %d lost used-stream marker after reap", newestID)
	}
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: newestID,
		Payload:  []byte("late"),
	}); err != nil {
		t.Fatalf("late DATA on reaped hidden marker: %v", err)
	}
	assertNoQueuedFrame(t, frames)
}

func TestHiddenControlOpenedMaxAgeReapsOldRetention(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle hidden ABORT: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	tombstone, ok := c.registry.tombstones[streamID]
	if !ok {
		c.mu.Unlock()
		t.Fatalf("stream %d missing tombstone before TTL reap", streamID)
	}
	tombstone.CreatedAt = time.Now().Add(-hiddenControlRetainedMaxAge - time.Millisecond)
	c.registry.tombstones[streamID] = tombstone
	c.enforceHiddenControlStateBudgetLocked(time.Now())
	if _, ok := c.registry.tombstones[streamID]; ok {
		c.mu.Unlock()
		t.Fatalf("expired hidden tombstone %d still retained after max-age reap", streamID)
	}
	if _, ok := c.registry.usedStreamData[streamID]; !ok {
		c.mu.Unlock()
		t.Fatalf("stream %d lost used-stream marker after max-age reap", streamID)
	}
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("late"),
	}); err != nil {
		t.Fatalf("late DATA on max-age reaped hidden marker: %v", err)
	}
	assertNoQueuedFrame(t, frames)
}

func TestPeerOpenedResetDoesNotAbortWholeStream(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- mustNativeStreamImpl(accepted)
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	case serverStream = <-acceptCh:
	}

	buf := make([]byte, 8)
	n, err := serverStream.Read(buf)
	if err != nil {
		t.Fatalf("server initial read: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("server initial read = %q, want %q", got, "hi")
	}

	if err := serverStream.CancelWrite(uint64(CodeCancelled)); err != nil {
		t.Fatalf("server reset: %v", err)
	}

	_, err = clientStream.Read(buf)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("client read err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeCancelled) {
		t.Fatalf("client read code = %d, want %d", appErr.Code, uint64(CodeCancelled))
	}

	n, err = clientStream.Write([]byte("ok"))
	if err != nil {
		t.Fatalf("client write after peer reset: %v", err)
	}
	if n != 2 {
		t.Fatalf("client write after peer reset n = %d, want 2", n)
	}

	server.mu.Lock()
	if got := server.registry.streams[serverStream.id]; got != serverStream {
		server.mu.Unlock()
		t.Fatalf("server stream %d dropped from registry after local reset", serverStream.id)
	}
	if !serverStream.localReceive || state.RecvTerminal(serverStream.effectiveRecvHalfStateLocked()) {
		sendHalf := serverStream.effectiveSendHalfStateLocked()
		recvHalf := serverStream.effectiveRecvHalfStateLocked()
		server.mu.Unlock()
		t.Fatalf("server stream halves after local reset = (%v,%v), want recv path to remain open", sendHalf, recvHalf)
	}
	server.mu.Unlock()

	n, err = serverStream.Read(buf)
	if err != nil {
		t.Fatalf("server read after peer reset: %v", err)
	}
	if got := string(buf[:n]); got != "ok" {
		t.Fatalf("server read after peer reset = %q, want %q", got, "ok")
	}
}

func TestInitHalfStatesMatchesStreamKind(t *testing.T) {
	tests := []struct {
		name        string
		localSend   bool
		localRecv   bool
		wantSend    state.SendHalfState
		wantReceive state.RecvHalfState
	}{
		{
			name:        "bidi",
			localSend:   true,
			localRecv:   true,
			wantSend:    state.SendHalfOpen,
			wantReceive: state.RecvHalfOpen,
		},
		{
			name:        "local send only",
			localSend:   true,
			localRecv:   false,
			wantSend:    state.SendHalfOpen,
			wantReceive: state.RecvHalfAbsent,
		},
		{
			name:        "peer send only",
			localSend:   false,
			localRecv:   true,
			wantSend:    state.SendHalfAbsent,
			wantReceive: state.RecvHalfOpen,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &nativeStream{localSend: tc.localSend, localReceive: tc.localRecv}
			s.initHalfStates()
			if s.sendHalf != tc.wantSend {
				t.Fatalf("sendHalf = %v, want %v", s.sendHalf, tc.wantSend)
			}
			if s.recvHalf != tc.wantReceive {
				t.Fatalf("recvHalf = %v, want %v", s.recvHalf, tc.wantReceive)
			}
			if got := s.sendHalfState(); got != tc.wantSend {
				t.Fatalf("sendHalfState() = %v, want %v", got, tc.wantSend)
			}
			if got := s.recvHalfState(); got != tc.wantReceive {
				t.Fatalf("recvHalfState() = %v, want %v", got, tc.wantReceive)
			}
		})
	}
}

func TestInitHalfStatesResetsToBaseCapability(t *testing.T) {
	s := &nativeStream{
		localSend:     true,
		localReceive:  true,
		sendHalf:      state.SendHalfReset,
		recvHalf:      state.RecvHalfAborted,
		sendReset:     &ApplicationError{Code: uint64(CodeCancelled)},
		recvAbort:     &ApplicationError{Code: uint64(CodeInternal)},
		localReadStop: true,
	}

	s.initHalfStates()

	if got := s.sendHalfState(); got != state.SendHalfOpen {
		t.Fatalf("sendHalfState() = %v, want %v", got, state.SendHalfOpen)
	}
	if got := s.recvHalfState(); got != state.RecvHalfOpen {
		t.Fatalf("recvHalfState() = %v, want %v", got, state.RecvHalfOpen)
	}
}

func TestHalfStateAccessorsNormalizeUnknownToBaseCapability(t *testing.T) {
	s := &nativeStream{
		localSend:     true,
		localReceive:  false,
		sendReset:     &ApplicationError{Code: uint64(CodeCancelled)},
		recvAbort:     &ApplicationError{Code: uint64(CodeInternal)},
		localReadStop: true,
	}

	if got := s.sendHalfState(); got != state.SendHalfOpen {
		t.Fatalf("sendHalfState() = %v, want %v", got, state.SendHalfOpen)
	}
	if got := s.recvHalfState(); got != state.RecvHalfAbsent {
		t.Fatalf("recvHalfState() = %v, want %v", got, state.RecvHalfAbsent)
	}
	if got := s.effectiveSendHalfStateLocked(); got != state.SendHalfOpen {
		t.Fatalf("effectiveSendHalfStateLocked() = %v, want %v", got, state.SendHalfOpen)
	}
	if got := s.effectiveRecvHalfStateLocked(); got != state.RecvHalfAbsent {
		t.Fatalf("effectiveRecvHalfStateLocked() = %v, want %v", got, state.RecvHalfAbsent)
	}
}

func TestHalfStateSettersPopulateExplicitStateAndMetadata(t *testing.T) {
	appErr := &ApplicationError{Code: uint64(CodeCancelled)}
	s := &nativeStream{localSend: true, localReceive: true}
	s.initHalfStates()

	s.setSendStopSeen(appErr)
	if s.sendStop == nil {
		t.Fatal("sendStop = nil, want application error")
	}
	if got := s.sendHalfState(); got != state.SendHalfStopSeen {
		t.Fatalf("sendHalfState() = %v, want %v", got, state.SendHalfStopSeen)
	}

	s.setSendFin()
	if !s.sendFinReached() {
		t.Fatal("sendFinReached() = false, want true")
	}
	if got := s.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalfState() = %v, want %v", got, state.SendHalfFin)
	}

	s.setRecvStopSent()
	if !s.localReadStop {
		t.Fatal("localReadStop = false, want true")
	}
	if got := s.recvHalfState(); got != state.RecvHalfStopSent {
		t.Fatalf("recvHalfState() = %v, want %v", got, state.RecvHalfStopSent)
	}

	s.setRecvFin()
	if !s.recvFinReached() {
		t.Fatal("recvFinReached() = false, want true")
	}
	if got := s.recvHalfState(); got != state.RecvHalfFin {
		t.Fatalf("recvHalfState() = %v, want %v", got, state.RecvHalfFin)
	}

	s.setSendResetWithSource(appErr, terminalResetFromStopSending)
	if s.sendReset == nil {
		t.Fatal("sendReset = nil, want application error")
	}
	if !s.sendResetFromStopLocked() {
		t.Fatal("sendResetFromStopLocked() = false, want true")
	}
	if got := s.sendHalfState(); got != state.SendHalfReset {
		t.Fatalf("sendHalfState() = %v, want %v", got, state.SendHalfReset)
	}

	s.setAbortedWithSource(appErr, terminalAbortFromPeer)
	if s.sendAbort == nil || s.recvAbort == nil {
		t.Fatal("abort fields not populated")
	}
	if !s.sendAbortFromPeerLocked() || !s.recvAbortFromPeerLocked() {
		t.Fatal("remote abort flags not populated")
	}
	if got := s.sendHalfState(); got != state.SendHalfAborted {
		t.Fatalf("sendHalfState() = %v, want %v", got, state.SendHalfAborted)
	}
	if got := s.recvHalfState(); got != state.RecvHalfAborted {
		t.Fatalf("recvHalfState() = %v, want %v", got, state.RecvHalfAborted)
	}
}

func TestClearSendFinRollsBackToExplicitBaseState(t *testing.T) {
	s := &nativeStream{localSend: true}
	s.initHalfStates()
	appErr := &ApplicationError{Code: uint64(CodeCancelled), Reason: "cancelled"}

	s.sendStop = appErr
	s.sendHalf = state.SendHalfFin
	s.sendResetSource = terminalResetFromStopSending
	s.clearSendFin()
	if got := s.sendHalfState(); got != state.SendHalfStopSeen {
		t.Fatalf("sendHalfState() = %v, want %v", got, state.SendHalfStopSeen)
	}
	if s.sendResetFromStopLocked() {
		t.Fatal("sendResetFromStopLocked() = true, want false after rollback")
	}

	s.sendStop = nil
	s.sendHalf = state.SendHalfFin
	s.clearSendFin()
	if got := s.sendHalfState(); got != state.SendHalfOpen {
		t.Fatalf("sendHalfState() = %v, want %v", got, state.SendHalfOpen)
	}
	if s.sendFinReached() {
		t.Fatal("sendFinReached() = true, want false after rollback to open")
	}

	s.localSend = false
	s.sendHalf = state.SendHalfFin
	s.clearSendFin()
	if got := s.sendHalfState(); got != state.SendHalfAbsent {
		t.Fatalf("sendHalfState() = %v, want %v", got, state.SendHalfAbsent)
	}
}

func TestLateNonOpeningControlOnHiddenTombstoneIgnored(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle first ABORT: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "MAX_DATA",
			call: func() error {
				return c.handleMaxDataFrame(Frame{
					Type:     FrameTypeMAXDATA,
					StreamID: streamID,
					Payload:  mustEncodeVarint(64),
				})
			},
		},
		{
			name: "BLOCKED",
			call: func() error {
				return c.handleBlockedFrame(Frame{
					Type:     FrameTypeBLOCKED,
					StreamID: streamID,
					Payload:  mustEncodeVarint(64),
				})
			},
		},
		{
			name: "STOP_SENDING",
			call: func() error {
				return c.handleStopSendingFrame(Frame{
					Type:     FrameTypeStopSending,
					StreamID: streamID,
					Payload:  mustEncodeVarint(uint64(CodeCancelled)),
				})
			},
		},
		{
			name: "RESET",
			call: func() error {
				return c.handleResetFrame(Frame{
					Type:     FrameTypeRESET,
					StreamID: streamID,
					Payload:  mustEncodeVarint(uint64(CodeCancelled)),
				})
			},
		},
	}

	for _, tt := range tests {
		if err := tt.call(); err != nil {
			t.Fatalf("%s on hidden tombstone err = %v, want nil", tt.name, err)
		}
		assertNoQueuedFrame(t, frames)

		c.mu.Lock()
		if _, ok := c.registry.tombstones[streamID]; !ok {
			c.mu.Unlock()
			t.Fatalf("%s removed hidden tombstone for stream %d", tt.name, streamID)
		}
		if c.registry.streams[streamID] != nil {
			c.mu.Unlock()
			t.Fatalf("%s revived hidden tombstone as live stream state", tt.name)
		}
		c.mu.Unlock()
	}
}

func TestLateNonOpeningControlOnGracefulTombstoneIgnored(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_fin",
	})

	c.mu.Lock()
	c.maybeCompactTerminalLocked(stream)
	c.mu.Unlock()

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "MAX_DATA",
			call: func() error {
				return c.handleMaxDataFrame(Frame{
					Type:     FrameTypeMAXDATA,
					StreamID: streamID,
					Payload:  mustEncodeVarint(64),
				})
			},
		},
		{
			name: "BLOCKED",
			call: func() error {
				return c.handleBlockedFrame(Frame{
					Type:     FrameTypeBLOCKED,
					StreamID: streamID,
					Payload:  mustEncodeVarint(64),
				})
			},
		},
		{
			name: "STOP_SENDING",
			call: func() error {
				return c.handleStopSendingFrame(Frame{
					Type:     FrameTypeStopSending,
					StreamID: streamID,
					Payload:  mustEncodeVarint(uint64(CodeCancelled)),
				})
			},
		},
		{
			name: "RESET",
			call: func() error {
				return c.handleResetFrame(Frame{
					Type:     FrameTypeRESET,
					StreamID: streamID,
					Payload:  mustEncodeVarint(uint64(CodeCancelled)),
				})
			},
		},
	}

	for _, tt := range tests {
		if err := tt.call(); err != nil {
			t.Fatalf("%s on graceful tombstone err = %v, want nil", tt.name, err)
		}
		assertNoQueuedFrame(t, frames)

		c.mu.Lock()
		if _, ok := c.registry.tombstones[streamID]; !ok {
			c.mu.Unlock()
			t.Fatalf("%s removed graceful tombstone for stream %d", tt.name, streamID)
		}
		if c.registry.streams[streamID] != nil {
			c.mu.Unlock()
			t.Fatalf("%s revived graceful tombstone as live stream state", tt.name)
		}
		c.mu.Unlock()
	}
}

func TestLateNonOpeningControlOnReapedTombstoneIgnored(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.registry.tombstoneLimit = 1

	firstID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: firstID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle first hidden ABORT: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	secondID := firstID + 4
	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: secondID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle second hidden ABORT: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	if _, ok := c.registry.tombstones[firstID]; ok {
		c.mu.Unlock()
		t.Fatalf("first tombstone for stream %d still retained after reap", firstID)
	}
	if _, ok := c.registry.usedStreamData[firstID]; !ok {
		c.mu.Unlock()
		t.Fatalf("first stream %d missing used-stream marker after reap", firstID)
	}
	c.mu.Unlock()

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "STOP_SENDING",
			call: func() error {
				return c.handleStopSendingFrame(Frame{
					Type:     FrameTypeStopSending,
					StreamID: firstID,
					Payload:  mustEncodeVarint(uint64(CodeCancelled)),
				})
			},
		},
		{
			name: "RESET",
			call: func() error {
				return c.handleResetFrame(Frame{
					Type:     FrameTypeRESET,
					StreamID: firstID,
					Payload:  mustEncodeVarint(uint64(CodeCancelled)),
				})
			},
		},
		{
			name: "MAX_DATA",
			call: func() error {
				return c.handleMaxDataFrame(Frame{
					Type:     FrameTypeMAXDATA,
					StreamID: firstID,
					Payload:  mustEncodeVarint(64),
				})
			},
		},
	}

	for _, tt := range tests {
		if err := tt.call(); err != nil {
			t.Fatalf("%s on reaped tombstone err = %v, want nil", tt.name, err)
		}
		assertNoQueuedFrame(t, frames)
		c.mu.Lock()
		if c.registry.streams[firstID] != nil {
			c.mu.Unlock()
			t.Fatalf("%s revived reaped tombstone stream %d", tt.name, firstID)
		}
		if _, ok := c.registry.usedStreamData[firstID]; !ok {
			c.mu.Unlock()
			t.Fatalf("%s removed used-stream marker for %d", tt.name, firstID)
		}
		c.mu.Unlock()
	}
}

type stateFixture struct {
	ID           string             `json:"id"`
	Category     string             `json:"category"`
	StreamKind   string             `json:"stream_kind"`
	Ownership    string             `json:"ownership"`
	Scope        string             `json:"scope"`
	InitialState json.RawMessage    `json:"initial_state"`
	Steps        []stateFixtureStep `json:"steps"`
}

type stateFixtureStep struct {
	Event        string           `json:"event"`
	ExpectState  *stateHalfExpect `json:"expect_state"`
	ExpectResult string           `json:"expect_result"`
}

type stateHalfExpect struct {
	SendHalf string `json:"send_half"`
	RecvHalf string `json:"recv_half"`
}

type stateFixtureEnv struct {
	t        *testing.T
	conn     *Conn
	frames   <-chan Frame
	streamID uint64
	stream   *nativeStream
	streams  []*nativeStream

	streamKind string
	ownership  string
	fixtureID  string

	closeSignal      <-chan struct{}
	closeRelease     chan struct{}
	closeResult      <-chan error
	closeHandlerDone <-chan struct{}

	waiterErr        error
	openErr          error
	capturedFrames   []Frame
	capturedAdvisory []Frame
}

func TestStateFixturesSupportedScenarios(t *testing.T) {
	for _, fixture := range loadStateFixtures(t) {
		fixture := fixture
		t.Run(fixture.ID, func(t *testing.T) {
			if !stateFixtureRunsSerial(fixture.ID) {
				t.Parallel()
			}
			if !supportedStateFixtureIDs[fixture.ID] {
				t.Fatalf("state fixture %q missing from support map", fixture.ID)
			}
			runStateFixture(t, fixture)
		})
	}
}

func stateFixtureRunsSerial(id string) bool {
	return strings.Contains(id, "_before_closed_ch_when_close_frame_send_stalls") ||
		strings.Contains(id, "buffered_close_done") ||
		strings.Contains(id, "opens_before_blocked") ||
		strings.Contains(id, "blocked_urgent_queue_returns_session_error_before_closed_ch")
}

var supportedStateFixtureIDs = map[string]bool{
	"bidi_data_then_fin":                                                                            true,
	"peer_first_reset_is_invalid":                                                                   true,
	"peer_first_abort_is_legal":                                                                     true,
	"late_data_after_peer_reset_is_ignored":                                                         true,
	"late_data_after_peer_abort_is_ignored":                                                         true,
	"local_zero_window_write_opens_before_blocked":                                                  true,
	"local_zero_window_write_final_opens_before_blocked":                                            true,
	"local_zero_window_close_read_opens_before_stop_sending":                                        true,
	"local_zero_window_close_write_queues_only_opening_fin":                                         true,
	"local_zero_window_write_open_metadata_opens_before_blocked":                                    true,
	"local_zero_window_write_final_open_metadata_opens_before_blocked":                              true,
	"local_zero_window_close_write_open_metadata_queues_only_opening_fin":                           true,
	"local_concrete_id_close_read_open_metadata_opens_before_stop_sending":                          true,
	"local_concrete_id_close_write_open_metadata_queues_opening_fin":                                true,
	"local_concrete_id_close_with_error_queues_opening_abort":                                       true,
	"session_max_data_ignored_while_closing":                                                        true,
	"session_blocked_ignored_while_closing":                                                         true,
	"session_max_data_applies_while_draining":                                                       true,
	"session_blocked_replenishes_while_draining":                                                    true,
	"session_peer_data_ignored_while_closing_without_side_effects":                                  true,
	"session_peer_stream_controls_ignored_while_closing_without_side_effects":                       true,
	"session_peer_goaway_ignored_while_closing_without_side_effects":                                true,
	"peer_nonclose_frames_ignored_after_transport_failure":                                          true,
	"duplicate_peer_close_ignored_before_parse_and_budget_accounting":                               true,
	"direct_handle_close_frame_ignores_duplicate_malformed_payload":                                 true,
	"peer_close_after_transport_failure_still_parses_diagnostics":                                   true,
	"direct_nonclose_handlers_ignored_when_closing":                                                 true,
	"direct_terminal_data_handler_ignored_when_closing":                                             true,
	"session_noop_session_blocked_flood_triggers_protocol_close":                                    true,
	"session_noop_session_max_data_flood_triggers_protocol_close":                                   true,
	"session_max_data_increase_clears_noop_budget":                                                  true,
	"session_replenish_clears_noop_blocked_budget":                                                  true,
	"session_mixed_noop_control_flood_triggers_protocol_close":                                      true,
	"session_unexpected_pong_flood_triggers_protocol_close":                                         true,
	"session_handle_frame_unexpected_pong_flood_triggers_protocol_close":                            true,
	"session_matching_pong_clears_mixed_noop_control_budget":                                        true,
	"session_handle_frame_matching_pong_clears_mixed_noop_control_budget":                           true,
	"session_reset_change_clears_mixed_noop_control_budget":                                         true,
	"session_noop_goaway_flood_triggers_protocol_close":                                             true,
	"session_goaway_change_clears_mixed_noop_control_budget":                                        true,
	"session_ignored_stop_sending_flood_triggers_protocol_close":                                    true,
	"session_ignored_reset_flood_triggers_protocol_close":                                           true,
	"session_ignored_abort_flood_triggers_protocol_close":                                           true,
	"session_noop_priority_update_flood_triggers_protocol_close":                                    true,
	"session_effective_priority_update_clears_noop_priority_budget":                                 true,
	"session_ignored_terminal_stream_max_data_flood_triggers_protocol_close":                        true,
	"session_ignored_terminal_stream_blocked_flood_triggers_protocol_close":                         true,
	"session_ignored_terminal_stream_priority_update_flood_triggers_protocol_close":                 true,
	"session_material_data_clears_noop_zero_length_data_budget":                                     true,
	"session_repeated_effective_group_rebucket_churn_triggers_protocol_close":                       true,
	"session_effective_group_updates_do_not_trigger_churn_outside_group_fair":                       true,
	"stream_noop_zero_length_data_flood_triggers_protocol_close":                                    true,
	"stream_noop_blocked_flood_triggers_protocol_close":                                             true,
	"stream_noop_max_data_flood_triggers_protocol_close":                                            true,
	"session_effective_control_change_clears_mixed_noop_control_budget":                             true,
	"session_hidden_abort_churn_triggers_protocol_close":                                            true,
	"session_hidden_abort_churn_window_expiry_resets_counter":                                       true,
	"tracked_session_memory_includes_retained_state_residency":                                      true,
	"enforce_hidden_control_state_budget_sheds_hidden_state_under_tracked_memory_cap":               true,
	"reap_excess_tombstones_sheds_oldest_visible_tombstone_under_tracked_memory_cap":                true,
	"session_visible_abort_churn_triggers_protocol_close":                                           true,
	"session_visible_uni_reset_churn_triggers_protocol_close":                                       true,
	"session_visible_terminal_churn_window_expiry_resets_counter":                                   true,
	"session_visible_terminal_churn_ignores_accepted_stream":                                        true,
	"session_visible_terminal_churn_ignores_bidi_reset_only":                                        true,
	"session_peer_ping_replies_while_draining":                                                      true,
	"session_peer_ping_ignored_while_closing":                                                       true,
	"session_peer_pong_clears_outstanding_ping_while_draining":                                      true,
	"session_peer_pong_ignored_while_closing":                                                       true,
	"session_malformed_peer_pong_ignored_while_closing":                                             true,
	"session_priority_update_applies_while_draining":                                                true,
	"session_priority_update_ignored_while_closing":                                                 true,
	"session_malformed_priority_update_ignored_while_closing":                                       true,
	"session_direct_priority_update_handler_ignored_while_closing":                                  true,
	"session_take_pending_priority_update_handoff_transfers_tracked_bytes_to_advisory_queue":        true,
	"session_take_pending_priority_update_handoff_respects_memory_cap":                              true,
	"session_control_flush_transfers_pending_priority_bytes_to_advisory_queue":                      true,
	"session_write_deadline_restores_pending_priority_update_while_blocked_on_writer_queue":         true,
	"session_close_write_deadline_restores_pending_priority_update_while_blocked_on_writer_queue":   true,
	"session_inbound_ping_flood_triggers_protocol_close":                                            true,
	"session_inbound_control_frame_budget_triggers_protocol_close":                                  true,
	"session_inbound_control_byte_budget_triggers_protocol_close":                                   true,
	"session_inbound_ext_frame_budget_triggers_protocol_close":                                      true,
	"session_inbound_mixed_control_ext_frame_budget_triggers_protocol_close":                        true,
	"session_inbound_mixed_control_ext_byte_budget_triggers_protocol_close":                         true,
	"session_inbound_data_session_memory_cap_triggers_internal_close":                               true,
	"session_drain_pending_control_frames_allows_draining_state":                                    true,
	"session_drain_pending_control_frames_drops_nonclose_control_when_closing":                      true,
	"session_queue_pending_control_ignored_when_closing":                                            true,
	"session_take_pending_priority_update_drops_when_closing":                                       true,
	"session_close_without_open_streams_only_sends_close":                                           true,
	"session_close_after_peer_noerror_close_is_noop":                                                true,
	"session_close_after_peer_error_close_returns_error":                                            true,
	"session_close_signals_control_notify_before_closed_ch_when_close_frame_send_stalls":            true,
	"session_close_does_not_wait_forever_for_buffered_close_done":                                   true,
	"session_blocked_urgent_queue_returns_session_error_before_closed_ch_on_closeSession":           true,
	"session_graceful_close_emits_goaway_then_close":                                                true,
	"session_close_recomputes_final_goaway_after_drain_interval":                                    true,
	"session_close_concurrent_calls_emit_single_close_frame":                                        true,
	"session_concurrent_local_goaway_keeps_only_most_restrictive_pending_replacement":               true,
	"session_close_after_prior_goaway_sends_more_restrictive_replacement":                           true,
	"session_graceful_close_blocks_local_open_while_draining":                                       true,
	"session_graceful_close_waits_for_active_streams_before_close_frame":                            true,
	"session_graceful_close_reclaims_committed_never_peer_visible_local_stream":                     true,
	"session_graceful_close_reclaims_provisional_never_peer_visible_local_stream":                   true,
	"session_abort_clears_streams_and_session_pending_state":                                        true,
	"session_close_returns_wait_after_closed_ch_when_close_frame_send_stalls":                       true,
	"session_close_returns_accept_before_closed_ch_when_close_frame_send_stalls":                    true,
	"session_close_returns_blocked_read_before_closed_ch_when_close_frame_send_stalls":              true,
	"session_close_returns_blocked_write_before_closed_ch_when_close_frame_send_stalls":             true,
	"session_close_returns_blocked_write_final_before_closed_ch_when_close_frame_send_stalls":       true,
	"session_close_returns_accept_uni_before_closed_ch_when_close_frame_send_stalls":                true,
	"session_close_returns_provisional_commit_waiter_before_closed_ch_when_close_frame_send_stalls": true,
	"session_abort_returns_wait_after_closed_ch_when_close_frame_send_stalls":                       true,
	"session_abort_returns_accept_before_closed_ch_when_close_frame_send_stalls":                    true,
	"session_abort_returns_accept_uni_before_closed_ch_when_close_frame_send_stalls":                true,
	"session_abort_returns_blocked_read_before_closed_ch_when_close_frame_send_stalls":              true,
	"session_abort_returns_blocked_write_before_closed_ch_when_close_frame_send_stalls":             true,
	"session_abort_returns_blocked_write_final_before_closed_ch_when_close_frame_send_stalls":       true,
	"session_abort_returns_provisional_commit_waiter_before_closed_ch_when_close_frame_send_stalls": true,
	"session_flow_control_noop_controls_do_not_force_flush":                                         true,
	"stream_flow_control_noop_controls_do_not_force_flush":                                          true,
	"session_blocked_force_flushes_pending_credit_below_pacing_threshold":                           true,
	"session_standing_growth_suppressed_while_released_credit_still_reflects_high_usage":            true,
	"stream_standing_growth_suppressed_while_released_credit_still_reflects_high_usage":             true,
	"session_standing_growth_suppressed_under_tracked_memory_pressure":                              true,
	"stream_standing_growth_suppressed_under_tracked_memory_pressure":                               true,
	"open_metadata_prefix_does_not_consume_zero_windows":                                            true,
	"open_metadata_flow_control_charges_only_trailing_application_bytes":                            true,
	"open_metadata_trailing_application_byte_over_stream_window_locally_aborts":                     true,
	"update_metadata_tightens_oversized_open_metadata_prefix_backing":                               true,
	"compact_terminal_releases_open_info_bytes_and_open_metadata_prefix":                            true,
	"fail_provisional_releases_open_info_bytes_and_clears_open_metadata_prefix":                     true,
	"fail_unopened_local_stream_clears_open_metadata_prefix":                                        true,
	"mark_peer_visible_clears_open_metadata_prefix":                                                 true,
	"close_stream_on_session_releases_open_metadata_prefix":                                         true,
	"close_session_releases_provisional_open_metadata_prefix":                                       true,
	"close_read_with_oversized_open_metadata_keeps_read_stop_without_committing_opener":             true,
	"close_read_with_malformed_open_metadata_keeps_read_stop_without_committing_opener":             true,
	"close_write_with_oversized_open_metadata_without_committing_opener":                            true,
	"close_write_with_malformed_open_metadata_without_committing_opener":                            true,
	"session_max_data_increase_broadcasts_conn_write_wake":                                          true,
	"prepare_write_wakes_on_session_max_data_increase":                                              true,
	"prepare_write_final_wakes_on_session_max_data_increase":                                        true,
	"release_receive_zero_window_wakes_write_waiters_when_memory_pressure_drops":                    true,
	"release_write_queue_reservation_wakes_blocked_write_at_low_watermark":                          true,
	"release_batch_reservations_clears_inflight_queued":                                             true,
	"release_batch_reservations_wakes_distinct_streams_crossing_low_watermark":                      true,
	"suppress_write_batch_marks_inflight_queued_for_accepted_requests":                              true,
	"suppress_write_batch_aggregates_inflight_queued_across_streams":                                true,
	"write_deadline_expires_while_blocked_by_session_queued_data_watermark":                         true,
	"write_deadline_expires_while_blocked_by_per_stream_queued_data_watermark":                      true,
	"write_closes_session_when_tracked_memory_cap_would_be_exceeded":                                true,
	"prepare_write_burst_batch_caps_single_request_at_per_stream_hwm":                               true,
	"prepare_write_final_burst_batch_caps_single_request_at_per_stream_hwm":                         true,
	"prepare_write_burst_batch_uses_smaller_session_queue_cap":                                      true,
	"reserve_write_queue_tracks_only_queued_streams":                                                true,
	"release_write_queue_reservation_untracks_drained_stream":                                       true,
	"clear_write_queue_reservations_locked_uses_tracked_set":                                        true,
	"clear_write_queue_reservations_locked_rebuilds_tracked_set_from_seeded_streams":                true,
	"handle_data_frame_open_metadata_retains_open_info_after_payload_mutation":                      true,
	"check_local_open_possible_with_open_info_limited_by_tracked_memory_cap":                        true,
	"enforce_visible_accept_backlog_sheds_newest_when_open_info_budget_exceeded":                    true,
	"visible_accept_backlog_refuses_newest_stream":                                                  true,
	"visible_accept_backlog_counts_only_unaccepted_streams":                                         true,
	"visible_accept_backlog_bytes_refuse_newest_by_visibility_sequence":                             true,
	"visible_accept_backlog_bytes_track_growth_and_accept_pop":                                      true,
	"enforce_visible_accept_backlog_sheds_newest_under_tracked_memory_cap":                          true,
	"check_local_open_possible_limited_by_tracked_memory_cap":                                       true,
	"stream_blocked_force_flushes_pending_credit_below_pacing_threshold":                            true,
	"stream_blocked_after_read_stop_force_flushes_session_only":                                     true,
	"first_max_data_on_unused_stream_is_invalid":                                                    true,
	"first_blocked_on_unused_stream_is_invalid":                                                     true,
	"uni_wrong_direction_data_rejected":                                                             true,
	"uni_wrong_direction_blocked_rejected":                                                          true,
	"uni_wrong_side_max_data_rejected":                                                              true,
	"uni_wrong_side_stop_sending_rejected":                                                          true,
	"terminal_control_ignored":                                                                      true,
	"stop_sending_requires_terminal_sender_followup":                                                true,
	"goaway_monotonic":            true,
	"peer_stream_id_gap_rejected": true,
	"local_open_cancel_before_first_frame_commit_does_not_consume_id": true,
	"provisional_open_limit_prevents_unbounded_head_of_line_stall":    true,
	"read_stop_discard_restores_session_budget_but_not_stream_budget": true,
	"late_data_after_close_read_honors_session_aggregate_cap":         true,
}

func loadStateFixtures(t *testing.T) []stateFixture {
	t.Helper()
	return testutil.LoadFixtureNDJSON[stateFixture](t, "state_cases.ndjson")
}

func runStateFixture(t *testing.T, fixture stateFixture) {
	t.Helper()

	env := newStateFixtureEnv(t, fixture)
	for _, step := range fixture.Steps {
		err := env.applyStep(step.Event)
		assertStateFixtureStep(t, env, step, err)
	}
}

func newStateFixtureStalledCloseSignalConn(t *testing.T) (*Conn, <-chan struct{}, chan struct{}, <-chan struct{}, func()) {
	t.Helper()

	settings := DefaultSettings()
	sent := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	stop := make(chan struct{})
	var stopOnce sync.Once

	c := &Conn{
		io: connIOState{conn: nopCloseSignalStream{}},

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals:   connRuntimeSignalState{acceptCh: make(chan struct{}, 1), livenessCh: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	go func() {
		defer close(done)
		select {
		case <-stop:
			return
		case req := <-c.writer.urgentWriteCh:
			sent <- struct{}{}
			select {
			case <-release:
			case <-stop:
			}
			req.done <- nil
		}
	}()

	cleanup := func() {
		stopOnce.Do(func() { close(stop) })
		select {
		case <-done:
		case <-time.After(testSignalTimeout):
		}
	}

	return c, sent, release, done, cleanup
}

func newStateFixtureStalledGracefulCloseConn(t *testing.T) (*Conn, <-chan Frame, <-chan struct{}, chan struct{}, func()) {
	t.Helper()

	frames := make(chan Frame, testFrameBufferCap)
	sent := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	stop := make(chan struct{})
	var stopOnce sync.Once

	settings := DefaultSettings()
	localRole := RoleInitiator
	peerRole := RoleResponder
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals: connRuntimeSignalState{livenessCh: make(chan struct{}, 1), acceptCh: make(chan struct{}, 1)},
		writer:  connWriterRuntimeState{writeCh: make(chan writeRequest), urgentWriteCh: make(chan writeRequest)},

		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(settings.MaxFramePayload),
		}, config: connConfigState{local: Preface{PrefaceVersion: PrefaceVersion, Role: localRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			peer:       Preface{PrefaceVersion: PrefaceVersion, Role: peerRole, MinProto: ProtoVersion, MaxProto: ProtoVersion, Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, Capabilities: 0, LocalRole: localRole, PeerRole: peerRole, PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: settings.InitialMaxData,
			sendSessionMax: settings.InitialMaxData}, registry: connRegistryState{streams: make(map[uint64]*nativeStream),
			tombstones:     make(map[uint64]streamTombstone),
			usedStreamData: make(map[uint64]usedStreamMarker),

			nextLocalBidi: state.FirstLocalStreamID(localRole, true),
			nextLocalUni:  state.FirstLocalStreamID(localRole, false),
			nextPeerBidi:  state.FirstPeerStreamID(localRole, true),
			nextPeerUni:   state.FirstPeerStreamID(localRole, false)},
	}

	go func() {
		defer close(done)
		firstUrgent := true
		for {
			select {
			case <-stop:
				return
			case req := <-c.writer.writeCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				req.done <- nil
			case req := <-c.writer.urgentWriteCh:
				for _, frame := range req.frames {
					frames <- testPublicFrame(frame)
				}
				if firstUrgent {
					firstUrgent = false
					sent <- struct{}{}
					select {
					case <-release:
					case <-stop:
					}
				}
				req.done <- nil
			}
		}
	}()

	cleanup := func() {
		stopOnce.Do(func() { close(stop) })
		select {
		case <-done:
		case <-time.After(testSignalTimeout):
		}
	}

	return c, frames, sent, release, cleanup
}

func newStateFixtureBufferedCloseDoneConn(t *testing.T) (*Conn, func()) {
	t.Helper()

	settings := DefaultSettings()
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals:   connRuntimeSignalState{acceptCh: make(chan struct{}, 1), livenessCh: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 1)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	return c, func() {}
}

func newStateFixtureEnv(t *testing.T, fixture stateFixture) *stateFixtureEnv {
	t.Helper()

	var (
		c                *Conn
		frames           <-chan Frame
		closeSignal      <-chan struct{}
		closeRelease     chan struct{}
		closeHandlerDone <-chan struct{}
		cleanup          func()
	)
	switch fixture.ID {
	case "session_graceful_close_blocks_local_open_while_draining",
		"session_concurrent_local_goaway_keeps_only_most_restrictive_pending_replacement":
		c, frames, closeSignal, closeRelease, cleanup = newStateFixtureStalledGracefulCloseConn(t)
	case "session_priority_update_applies_while_draining",
		"session_priority_update_ignored_while_closing",
		"session_malformed_priority_update_ignored_while_closing",
		"session_direct_priority_update_handler_ignored_while_closing":
		var stop func()
		c, frames, stop = newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
		cleanup = stop
	case "session_noop_session_blocked_flood_triggers_protocol_close",
		"session_noop_session_max_data_flood_triggers_protocol_close",
		"session_max_data_increase_clears_noop_budget",
		"session_replenish_clears_noop_blocked_budget",
		"session_mixed_noop_control_flood_triggers_protocol_close",
		"session_unexpected_pong_flood_triggers_protocol_close",
		"session_handle_frame_unexpected_pong_flood_triggers_protocol_close",
		"session_matching_pong_clears_mixed_noop_control_budget",
		"session_handle_frame_matching_pong_clears_mixed_noop_control_budget",
		"session_reset_change_clears_mixed_noop_control_budget",
		"session_noop_goaway_flood_triggers_protocol_close",
		"session_goaway_change_clears_mixed_noop_control_budget",
		"session_ignored_stop_sending_flood_triggers_protocol_close",
		"session_ignored_reset_flood_triggers_protocol_close",
		"session_ignored_abort_flood_triggers_protocol_close",
		"session_noop_priority_update_flood_triggers_protocol_close",
		"session_effective_priority_update_clears_noop_priority_budget",
		"session_ignored_terminal_stream_max_data_flood_triggers_protocol_close",
		"session_ignored_terminal_stream_blocked_flood_triggers_protocol_close",
		"session_ignored_terminal_stream_priority_update_flood_triggers_protocol_close",
		"session_material_data_clears_noop_zero_length_data_budget",
		"session_repeated_effective_group_rebucket_churn_triggers_protocol_close",
		"session_effective_group_updates_do_not_trigger_churn_outside_group_fair",
		"stream_noop_zero_length_data_flood_triggers_protocol_close",
		"stream_noop_blocked_flood_triggers_protocol_close",
		"stream_noop_max_data_flood_triggers_protocol_close",
		"session_effective_control_change_clears_mixed_noop_control_budget",
		"session_hidden_abort_churn_triggers_protocol_close",
		"session_hidden_abort_churn_window_expiry_resets_counter",
		"session_visible_abort_churn_triggers_protocol_close",
		"session_visible_uni_reset_churn_triggers_protocol_close",
		"session_visible_terminal_churn_window_expiry_resets_counter",
		"session_visible_terminal_churn_ignores_accepted_stream",
		"session_visible_terminal_churn_ignores_bidi_reset_only",
		"session_inbound_ping_flood_triggers_protocol_close",
		"session_inbound_control_frame_budget_triggers_protocol_close",
		"session_inbound_control_byte_budget_triggers_protocol_close",
		"session_inbound_ext_frame_budget_triggers_protocol_close",
		"session_inbound_mixed_control_ext_frame_budget_triggers_protocol_close",
		"session_inbound_mixed_control_ext_byte_budget_triggers_protocol_close",
		"session_inbound_data_session_memory_cap_triggers_internal_close":
		var stop func()
		c, frames, stop = newInvalidPolicyConn(t)
		cleanup = stop
	case "session_close_does_not_wait_forever_for_buffered_close_done":
		c, cleanup = newStateFixtureBufferedCloseDoneConn(t)
	case "session_close_returns_accept_before_closed_ch_when_close_frame_send_stalls",
		"session_close_signals_control_notify_before_closed_ch_when_close_frame_send_stalls",
		"session_blocked_urgent_queue_returns_session_error_before_closed_ch_on_closeSession",
		"session_close_returns_wait_after_closed_ch_when_close_frame_send_stalls",
		"session_close_returns_blocked_read_before_closed_ch_when_close_frame_send_stalls",
		"session_close_returns_blocked_write_before_closed_ch_when_close_frame_send_stalls",
		"session_close_returns_blocked_write_final_before_closed_ch_when_close_frame_send_stalls",
		"session_close_returns_accept_uni_before_closed_ch_when_close_frame_send_stalls",
		"session_close_returns_provisional_commit_waiter_before_closed_ch_when_close_frame_send_stalls",
		"session_abort_returns_wait_after_closed_ch_when_close_frame_send_stalls",
		"session_abort_returns_accept_before_closed_ch_when_close_frame_send_stalls",
		"session_abort_returns_accept_uni_before_closed_ch_when_close_frame_send_stalls",
		"session_abort_returns_blocked_read_before_closed_ch_when_close_frame_send_stalls",
		"session_abort_returns_blocked_write_before_closed_ch_when_close_frame_send_stalls",
		"session_abort_returns_blocked_write_final_before_closed_ch_when_close_frame_send_stalls",
		"session_abort_returns_provisional_commit_waiter_before_closed_ch_when_close_frame_send_stalls":
		c, closeSignal, closeRelease, closeHandlerDone, cleanup = newStateFixtureStalledCloseSignalConn(t)
	default:
		var stop func()
		c, frames, stop = newHandlerTestConn(t)
		cleanup = stop
	}
	t.Cleanup(cleanup)

	settings := DefaultSettings()
	switch fixture.ID {
	case "session_blocked_force_flushes_pending_credit_below_pacing_threshold":
		settings.InitialMaxData = 256
		settings.MaxFramePayload = 16
	case "stream_blocked_force_flushes_pending_credit_below_pacing_threshold":
		settings.MaxFramePayload = 8
	case "session_blocked_replenishes_while_draining":
		settings.InitialMaxData = 262144
		settings.MaxFramePayload = 16384
	}
	localRole := RoleResponder
	peerRole := RoleInitiator

	c.config.local = Preface{
		PrefaceVersion: PrefaceVersion,
		Role:           localRole,
		MinProto:       ProtoVersion,
		MaxProto:       ProtoVersion,
		Settings:       settings,
	}
	c.config.peer = Preface{
		PrefaceVersion: PrefaceVersion,
		Role:           peerRole,
		MinProto:       ProtoVersion,
		MaxProto:       ProtoVersion,
		Settings:       settings,
	}
	c.config.negotiated = Negotiated{
		Proto:        ProtoVersion,
		Capabilities: 0,
		LocalRole:    localRole,
		PeerRole:     peerRole,
		PeerSettings: settings,
	}
	c.registry.nextLocalBidi = state.FirstLocalStreamID(localRole, true)
	c.registry.nextLocalUni = state.FirstLocalStreamID(localRole, false)
	c.registry.nextPeerBidi = state.FirstPeerStreamID(localRole, true)
	c.registry.nextPeerUni = state.FirstPeerStreamID(localRole, false)
	c.flow.recvSessionAdvertised = settings.InitialMaxData
	c.flow.sendSessionMax = settings.InitialMaxData
	c.sessionControl.peerGoAwayBidi = MaxVarint62
	c.sessionControl.peerGoAwayUni = MaxVarint62
	c.sessionControl.localGoAwayBidi = MaxVarint62
	c.sessionControl.localGoAwayUni = MaxVarint62
	c.signals.acceptCh = make(chan struct{}, 1)
	switch fixture.ID {
	case "session_hidden_abort_churn_triggers_protocol_close":
		c.abuse.hiddenAbortChurnWindow = time.Hour
	case "session_visible_abort_churn_triggers_protocol_close",
		"session_visible_uni_reset_churn_triggers_protocol_close":
		c.abuse.visibleChurnWindow = time.Hour
	}

	env := &stateFixtureEnv{
		t:                t,
		conn:             c,
		frames:           frames,
		streamKind:       fixture.StreamKind,
		ownership:        fixture.Ownership,
		fixtureID:        fixture.ID,
		closeSignal:      closeSignal,
		closeRelease:     closeRelease,
		closeHandlerDone: closeHandlerDone,
	}
	if fixture.ID == "late_data_after_close_read_honors_session_aggregate_cap" {
		env.streams = buildLateTailFixtureStreams(t, c, localRole)
		return env
	}
	switch fixture.ID {
	case "session_blocked_force_flushes_pending_credit_below_pacing_threshold":
		c.flow.recvSessionAdvertised = 256
		c.flow.recvSessionReceived = 192
		c.flow.recvSessionPending = 8
		c.flow.recvSessionUsed = 128
		c.flow.sessionDataHWM = 128
	case "session_max_data_ignored_while_closing":
		c.flow.sendSessionMax = 128
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = &ApplicationError{Code: uint64(CodeInternal)}
	case "session_blocked_ignored_while_closing":
		c.flow.recvSessionAdvertised = 1024
		c.flow.recvSessionReceived = 512
		c.flow.recvSessionPending = 10
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = &ApplicationError{Code: uint64(CodeInternal)}
	case "session_max_data_applies_while_draining":
		c.flow.sendSessionMax = 128
		c.lifecycle.sessionState = connStateDraining
	case "session_peer_ping_replies_while_draining":
		c.lifecycle.sessionState = connStateDraining
	case "session_peer_ping_ignored_while_closing":
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
	case "session_peer_pong_clears_outstanding_ping_while_draining":
		c.lifecycle.sessionState = connStateDraining
		c.liveness.pingOutstanding = true
		c.liveness.pingPayload = []byte{9, 8, 7, 6, 5, 4, 3, 2}
		c.liveness.lastPingSentAt = time.Now().Add(-25 * time.Millisecond)
		c.liveness.pingDone = make(chan struct{})
	case "session_peer_pong_ignored_while_closing":
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
		c.liveness.pingOutstanding = true
		c.liveness.pingPayload = []byte{9, 8, 7, 6, 5, 4, 3, 2}
		c.liveness.lastPingSentAt = time.Now().Add(-25 * time.Millisecond)
		c.liveness.pingDone = make(chan struct{})
	case "session_malformed_peer_pong_ignored_while_closing":
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
		c.abuse.controlBudgetFrames = 3
		c.abuse.mixedBudgetFrames = 5
		c.abuse.noopControlCount = 7
	case "session_blocked_replenishes_while_draining":
		c.lifecycle.sessionState = connStateDraining
		target := c.sessionWindowTargetLocked()
		c.flow.recvSessionAdvertised = target
		c.flow.recvSessionReceived = target / 2
		c.flow.recvSessionPending = 10
	case "session_peer_data_ignored_while_closing_without_side_effects":
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
		c.abuse.controlBudgetFrames = 7
		c.abuse.mixedBudgetFrames = 9
		c.abuse.noopDataCount = 11
		c.flow.recvSessionUsed = 13
	case "session_peer_stream_controls_ignored_while_closing_without_side_effects":
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
		c.abuse.controlBudgetFrames = 3
		c.abuse.mixedBudgetFrames = 5
		c.abuse.noopControlCount = 7
	case "session_peer_goaway_ignored_while_closing_without_side_effects":
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
		c.abuse.controlBudgetFrames = 3
		c.abuse.mixedBudgetFrames = 5
		c.abuse.noopControlCount = 7
		c.sessionControl.peerGoAwayBidi = 80
		c.sessionControl.peerGoAwayUni = 99
		c.sessionControl.peerGoAwayErr = &ApplicationError{Code: uint64(CodeProtocol), Reason: "old"}
	case "session_blocked_urgent_queue_returns_session_error_before_closed_ch_on_closeSession":
		c.flow.urgentQueueCap = 1
		c.flow.urgentQueuedBytes = 1
	case "session_graceful_close_emits_goaway_then_close",
		"session_graceful_close_blocks_local_open_while_draining",
		"session_close_recomputes_final_goaway_after_drain_interval",
		"session_graceful_close_waits_for_active_streams_before_close_frame":
		streamID := state.FirstLocalStreamID(localRole, true)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		testMarkLocalOpenVisible(stream)
		env.stream = stream
		env.streamID = streamID
		c.registry.streams[streamID] = stream
		c.sessionControl.localGoAwayBidi = MaxVarint62
		c.sessionControl.localGoAwayUni = MaxVarint62
		c.registry.nextPeerBidi = state.FirstPeerStreamID(localRole, true) + 4
		c.registry.nextPeerUni = state.FirstPeerStreamID(localRole, false) + 4
	case "session_drain_pending_control_frames_allows_draining_state":
		stream := newPendingControlTestStream(c)
		if !c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(101)) {
			t.Fatal("queuePendingSessionControlAsync rejected session MAX_DATA")
		}
		c.queueStreamMaxDataAsync(stream.id, 202)
		c.queuePendingSessionControlAsync(sessionControlBlocked, 303)
		c.queueStreamBlockedAsync(stream, 404)
		c.queuePriorityUpdateAsync(stream.id, []byte{1, 2, 3}, retainedBytesBorrowed)
		c.lifecycle.sessionState = connStateDraining
		env.stream = stream
		env.streamID = stream.id
	case "session_drain_pending_control_frames_drops_nonclose_control_when_closing",
		"session_queue_pending_control_ignored_when_closing":
		stream := newPendingControlTestStream(c)
		c.lifecycle.sessionState = connStateClosing
		if !c.setPendingGoAwayPayloadLocked([]byte{0xaa, 0xbb}) {
			t.Fatal("setPendingGoAwayPayloadLocked failed")
		}
		c.sessionControl.pendingGoAwayBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4
		c.sessionControl.pendingGoAwayUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4
		if fixture.ID == "session_drain_pending_control_frames_drops_nonclose_control_when_closing" {
			c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(101))
			c.queueStreamMaxDataAsync(stream.id, 202)
			c.queuePendingSessionControlAsync(sessionControlBlocked, 303)
			c.queueStreamBlockedAsync(stream, 404)
			c.queuePriorityUpdateAsync(stream.id, []byte{1, 2, 3}, retainedBytesBorrowed)
		}
		env.stream = stream
		env.streamID = stream.id
	case "session_take_pending_priority_update_drops_when_closing":
		stream := newPendingControlTestStream(c)
		testSetPendingPriorityUpdate(c, stream.id, []byte{1, 2, 3})
		c.lifecycle.sessionState = connStateClosing
		env.stream = stream
		env.streamID = stream.id
	case "session_mixed_noop_control_flood_triggers_protocol_close",
		"session_effective_control_change_clears_mixed_noop_control_budget",
		"session_reset_change_clears_mixed_noop_control_budget":
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
	case "session_ignored_stop_sending_flood_triggers_protocol_close":
		streamID := state.FirstLocalStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
			SendHalf: "send_stop_seen",
			RecvHalf: "recv_open",
		})
	case "session_ignored_reset_flood_triggers_protocol_close":
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_reset",
		})
	case "session_ignored_abort_flood_triggers_protocol_close":
		streamID := state.FirstPeerStreamID(localRole, false)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "uni", "peer_owned", stateHalfExpect{
			SendHalf: "send_aborted",
			RecvHalf: "recv_aborted",
		})
	case "session_noop_priority_update_flood_triggers_protocol_close",
		"session_effective_priority_update_clears_noop_priority_budget":
		c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityPriorityHints
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
		env.stream.priority = 7
	case "session_ignored_terminal_stream_max_data_flood_triggers_protocol_close",
		"session_ignored_terminal_stream_blocked_flood_triggers_protocol_close":
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_fin",
			RecvHalf: "recv_fin",
		})
	case "session_ignored_terminal_stream_priority_update_flood_triggers_protocol_close":
		c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityPriorityHints
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_aborted",
			RecvHalf: "recv_aborted",
		})
	case "session_material_data_clears_noop_zero_length_data_budget":
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
	case "session_repeated_effective_group_rebucket_churn_triggers_protocol_close":
		c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityStreamGroups
		c.config.peer.Settings.SchedulerHints = SchedulerGroupFair
		c.abuse.groupRebucketFloodLimit = 2
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
	case "session_effective_group_updates_do_not_trigger_churn_outside_group_fair":
		c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityStreamGroups
		c.abuse.groupRebucketFloodLimit = 1
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
	case "session_visible_abort_churn_triggers_protocol_close",
		"session_visible_uni_reset_churn_triggers_protocol_close":
		c.queues.acceptBacklogLimit = 1024
		c.queues.acceptBacklogBytesLimit = 1 << 20
	case "session_priority_update_applies_while_draining":
		c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityPriorityHints
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
		c.lifecycle.sessionState = connStateDraining
		env.stream.priority = 1
	case "session_priority_update_ignored_while_closing",
		"session_direct_priority_update_handler_ignored_while_closing":
		c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityPriorityHints
		streamID := state.FirstPeerStreamID(localRole, true)
		env.streamID = streamID
		env.stream = seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
		env.stream.priority = 1
	case "session_malformed_priority_update_ignored_while_closing":
		c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityPriorityHints
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
		c.abuse.controlBudgetFrames = 3
		c.abuse.mixedBudgetFrames = 5
		c.abuse.noopControlCount = 7
	case "session_close_concurrent_calls_emit_single_close_frame":
		streamID := state.FirstLocalStreamID(localRole, true)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		env.stream = stream
		env.streamID = streamID
		c.registry.streams[streamID] = stream
		c.registry.nextPeerBidi = state.FirstPeerStreamID(localRole, true) + 4
		c.registry.nextPeerUni = state.FirstPeerStreamID(localRole, false) + 4
	case "session_close_after_prior_goaway_sends_more_restrictive_replacement":
		streamID := state.FirstLocalStreamID(localRole, true)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		testMarkLocalOpenCommitted(stream)
		env.stream = stream
		env.streamID = streamID
		c.registry.streams[streamID] = stream
		c.registry.nextPeerBidi = state.FirstPeerStreamID(localRole, true) + 8
		c.registry.nextPeerUni = state.FirstPeerStreamID(localRole, false) + 4
	case "session_graceful_close_reclaims_committed_never_peer_visible_local_stream":
		streamID := state.FirstLocalStreamID(localRole, true)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		testMarkLocalOpenCommitted(stream)
		env.stream = stream
		env.streamID = streamID
		c.registry.streams[streamID] = stream
		c.sessionControl.localGoAwayBidi = MaxVarint62
		c.sessionControl.localGoAwayUni = MaxVarint62
		c.registry.nextPeerBidi = state.FirstPeerStreamID(localRole, true) + 4
		c.registry.nextPeerUni = state.FirstPeerStreamID(localRole, false) + 4
	case "session_graceful_close_reclaims_provisional_never_peer_visible_local_stream":
		stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
		env.stream = stream
		c.sessionControl.localGoAwayBidi = MaxVarint62
		c.sessionControl.localGoAwayUni = MaxVarint62
		c.registry.nextPeerBidi = state.FirstPeerStreamID(localRole, true) + 4
		c.registry.nextPeerUni = state.FirstPeerStreamID(localRole, false) + 4
	case "session_abort_clears_streams_and_session_pending_state":
		streamID := state.FirstLocalStreamID(localRole, true)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		stream.sendSent = 11
		stream.recvBuffer = 7
		stream.readBuf = []byte("queued")
		c.registry.streams[streamID] = stream

		provisionalBidi := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
		provisionalBidi.sendSent = 13
		provisionalBidi.recvBuffer = 5
		provisionalBidi.readBuf = []byte("pending")

		provisionalUni := c.newProvisionalLocalStreamLocked(streamArityUni, OpenOptions{}, nil)
		provisionalUni.sendSent = 17
		provisionalUni.recvBuffer = 3
		provisionalUni.readBuf = []byte("queued")

		accept := c.newLocalStreamWithIDLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
		accept.idSet = true
		accept.applicationVisible = true
		accept.recvBuffer = 9
		c.enqueueAcceptedLocked(accept)
		acceptUni := c.newLocalStreamWithIDLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, false), streamArityUni, OpenOptions{}, nil)
		acceptUni.applicationVisible = true
		acceptUni.recvBuffer = 4
		c.enqueueAcceptedLocked(acceptUni)

		testSetPendingStreamMaxData(c, streamID, 99)
		testSetPendingStreamBlocked(c, streamID, 11)
		testSetPendingPriorityUpdate(c, streamID, []byte{0xde, 0xad})
		c.pending.sessionBlocked = 33
		c.pending.hasSessionBlocked = true
		c.pending.sessionMaxData = 333
		c.pending.hasSessionMaxData = true
		c.pending.controlBytes = 7
		c.sessionControl.pendingGoAwayBidi = state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 8
		c.sessionControl.pendingGoAwayUni = state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 8
		c.sessionControl.pendingGoAwayPayload = []byte{0xaa, 0xbb, 0xcc}
		c.sessionControl.hasPendingGoAway = true
		c.sessionControl.goAwaySendActive = true
		c.pending.sessionBlockedAt = 44
		c.pending.sessionBlockedSet = true
		c.registry.activePeerBidi = 3
		c.registry.activePeerUni = 2
		c.writer.scheduler = rt.BatchScheduler{
			ActiveGroupRefs: map[uint64]uint64{7: 1},
			State: rt.BatchState{
				RootVirtualTime:   23,
				ServiceSeq:        17,
				StreamFinishTag:   map[uint64]uint64{streamID: 3, 999: 7},
				StreamLastService: map[uint64]uint64{streamID: 8, 999: 11},
				GroupVirtualTime:  map[rt.GroupKey]uint64{{Kind: 0, Value: streamID}: 5, {Kind: 1, Value: 7}: 9},
				GroupFinishTag:    map[rt.GroupKey]uint64{{Kind: 0, Value: streamID}: 6, {Kind: 1, Value: 7}: 10},
				GroupLastService:  map[rt.GroupKey]uint64{{Kind: 0, Value: streamID}: 12, {Kind: 1, Value: 7}: 13},
			},
		}
		c.liveness.keepaliveInterval = 500 * time.Millisecond
		c.liveness.keepaliveMaxPingInterval = 2 * time.Second
		c.liveness.readIdlePingDueAt = time.Now().Add(time.Second)
		c.liveness.writeIdlePingDueAt = time.Now().Add(time.Second)
		c.liveness.maxPingDueAt = time.Now().Add(2 * time.Second)
		c.liveness.pingOutstanding = true
		c.liveness.pingPayload = []byte("ping")
		c.liveness.lastPingSentAt = time.Now()
		c.liveness.lastPongAt = time.Now()
		c.liveness.lastPingRTT = 123 * time.Millisecond
		c.liveness.pingDone = make(chan struct{})
		env.stream = stream
		env.streamID = streamID
		env.streams = []*nativeStream{stream, provisionalBidi, provisionalUni}
	}
	if fixture.Scope == "session" || fixture.StreamKind == "" {
		return env
	}

	streamID := fixtureStreamID(localRole, fixture.StreamKind, fixture.Ownership)
	if streamID == 0 {
		t.Fatalf("unsupported stream fixture mapping: kind=%q ownership=%q", fixture.StreamKind, fixture.Ownership)
	}
	env.streamID = streamID

	if bytes.Equal(fixture.InitialState, []byte(`"idle"`)) || len(fixture.InitialState) == 0 {
		return env
	}

	var initial stateHalfExpect
	if err := json.Unmarshal(fixture.InitialState, &initial); err != nil {
		t.Fatalf("decode initial_state for %s: %v", fixture.ID, err)
	}
	env.stream = seedStateFixtureStream(t, c, streamID, fixture.StreamKind, fixture.Ownership, initial)
	if fixture.ID == "read_stop_discard_restores_session_budget_but_not_stream_budget" {
		env.stream.recvBuffer = 3
		env.stream.readBuf = []byte("abc")
		env.conn.flow.recvSessionUsed = 3
	}
	switch fixture.ID {
	case "late_data_after_peer_reset_is_ignored", "late_data_after_peer_abort_is_ignored":
		env.stream.recvAdvertised = state.InitialReceiveWindow(env.conn.config.negotiated.LocalRole, env.conn.config.local.Settings, env.stream.id)
	case "stream_blocked_force_flushes_pending_credit_below_pacing_threshold":
		env.conn.flow.perStreamDataHWM = 32
		env.stream.recvAdvertised = 64
		env.stream.recvReceived = 52
		env.stream.recvPending = 4
		env.stream.recvBuffer = 32
	case "stream_blocked_after_read_stop_force_flushes_session_only":
		sessionTarget := env.conn.sessionWindowTargetLocked()
		streamTarget := env.conn.streamWindowTargetLocked(env.stream)
		env.conn.flow.recvSessionAdvertised = sessionTarget
		env.conn.flow.recvSessionReceived = sessionTarget / 2
		env.conn.flow.recvSessionPending = 10
		env.stream.recvAdvertised = streamTarget
		env.stream.recvReceived = streamTarget / 2
		env.stream.recvPending = 10
	}
	return env
}

func seedStateFixtureStream(t *testing.T, c *Conn, streamID uint64, streamKind, ownership string, initial stateHalfExpect) *nativeStream {
	t.Helper()

	bidi := streamKind == "bidi"

	c.mu.Lock()
	defer c.mu.Unlock()

	var stream *nativeStream
	if ownership == "local_owned" {
		stream = c.newLocalStreamLocked(streamID, streamArityFromBidi(bidi), OpenOptions{}, nil)
	} else {
		stream = c.newPeerStreamLocked(streamID)
	}
	stream.conn = c
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	stream.sendHalf = state.BaseSendHalfState(stream.localSend)
	stream.recvHalf = state.BaseRecvHalfState(stream.localReceive)

	switch initial.SendHalf {
	case "", "send_open":
	case "send_fin":
		stream.sendHalf = state.SendHalfFin
	case "send_stop_seen":
		stream.sendStop = &ApplicationError{Code: uint64(CodeCancelled)}
		stream.sendHalf = state.SendHalfStopSeen
	case "send_reset":
		stream.sendReset = &ApplicationError{Code: uint64(CodeCancelled)}
		stream.sendHalf = state.SendHalfReset
	case "send_aborted":
		stream.sendAbort = &ApplicationError{Code: uint64(CodeCancelled)}
		stream.sendHalf = state.SendHalfAborted
	default:
		t.Fatalf("unsupported initial send_half %q", initial.SendHalf)
	}

	switch initial.RecvHalf {
	case "", "recv_open":
	case "recv_fin":
		stream.recvHalf = state.RecvHalfFin
	case "recv_reset":
		stream.recvReset = &ApplicationError{Code: uint64(CodeCancelled)}
		stream.recvHalf = state.RecvHalfReset
	case "recv_aborted":
		stream.recvAbort = &ApplicationError{Code: uint64(CodeCancelled)}
		stream.recvHalf = state.RecvHalfAborted
	case "recv_stop_sent":
		stream.localReadStop = true
		stream.recvHalf = state.RecvHalfStopSent
	default:
		t.Fatalf("unsupported initial recv_half %q", initial.RecvHalf)
	}

	if !stream.localOpen.opened && !state.FullyTerminal(stream.localSend, stream.localReceive, stream.effectiveSendHalfStateLocked(), stream.effectiveRecvHalfStateLocked()) {
		stream.markActiveCounted()
		if bidi {
			c.registry.activePeerBidi++
		} else {
			c.registry.activePeerUni++
		}
	}
	c.registry.streams[streamID] = stream
	return stream
}

func buildLateTailFixtureStreams(t *testing.T, c *Conn, localRole Role) []*nativeStream {
	t.Helper()

	streamIDs := []uint64{
		state.FirstPeerStreamID(localRole, true),
		state.FirstPeerStreamID(localRole, true) + 4,
		state.FirstPeerStreamID(localRole, true) + 8,
	}
	streams := make([]*nativeStream, 0, len(streamIDs))
	for _, streamID := range streamIDs {
		stream := seedStateFixtureStream(t, c, streamID, "bidi", "peer_owned", stateHalfExpect{
			SendHalf: "send_open",
			RecvHalf: "recv_open",
		})
		if err := stream.CloseRead(); err != nil {
			t.Fatalf("CloseRead for late-tail fixture stream %d: %v", streamID, err)
		}
		streams = append(streams, stream)
	}
	return streams
}

func fixtureStreamID(localRole Role, streamKind, ownership string) uint64 {
	bidi := streamKind == "bidi"
	switch streamKind {
	case "bidi", "uni_local_send_only":
	default:
		return 0
	}
	if ownership == "local_owned" {
		return state.FirstLocalStreamID(localRole, bidi)
	}
	if ownership == "peer_owned" {
		return state.FirstPeerStreamID(localRole, bidi)
	}
	return 0
}

func (e *stateFixtureEnv) applyStep(event string) error {
	switch event {
	case "peer_session_same_MAX_DATA":
		return e.conn.handleMaxDataFrame(Frame{
			Type:    FrameTypeMAXDATA,
			Payload: mustEncodeVarint(e.conn.flow.sendSessionMax),
		})
	case "peer_session_higher_MAX_DATA":
		return e.conn.handleMaxDataFrame(Frame{
			Type:    FrameTypeMAXDATA,
			Payload: mustEncodeVarint(e.conn.flow.sendSessionMax + 128),
		})
	case "peer_session_MAX_DATA_plus_one":
		return e.conn.handleMaxDataFrame(Frame{
			Type:    FrameTypeMAXDATA,
			Payload: mustEncodeVarint(e.conn.flow.sendSessionMax + 1),
		})
	case "peer_session_BLOCKED":
		return e.conn.handleBlockedFrame(Frame{
			Type:    FrameTypeBLOCKED,
			Payload: mustEncodeVarint(0),
		})
	case "peer_session_BLOCKED_at_advertised":
		return e.conn.handleBlockedFrame(Frame{
			Type:    FrameTypeBLOCKED,
			Payload: mustEncodeVarint(e.conn.flow.recvSessionAdvertised),
		})
	case "peer_closing_DATA":
		return e.conn.handleFrame(Frame{
			Type:     FrameTypeDATA,
			StreamID: state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true),
			Payload:  []byte("late"),
		})
	case "peer_closing_stream_MAX_DATA":
		return e.conn.handleFrame(Frame{
			Type:     FrameTypeMAXDATA,
			StreamID: state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true),
			Payload:  mustEncodeVarint(64),
		})
	case "peer_closing_stream_BLOCKED":
		return e.conn.handleFrame(Frame{
			Type:     FrameTypeBLOCKED,
			StreamID: state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true),
			Payload:  mustEncodeVarint(64),
		})
	case "peer_closing_STOP_SENDING":
		return e.conn.handleFrame(Frame{
			Type:     FrameTypeStopSending,
			StreamID: state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true),
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	case "peer_closing_RESET":
		return e.conn.handleFrame(Frame{
			Type:     FrameTypeRESET,
			StreamID: state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true),
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	case "peer_closing_ABORT":
		return e.conn.handleFrame(Frame{
			Type:     FrameTypeABORT,
			StreamID: state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true),
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	case "peer_closing_GOAWAY":
		payload, err := buildGoAwayPayload(8, 4, uint64(CodeInternal), "too-late")
		if err != nil {
			return err
		}
		return e.conn.handleFrame(Frame{
			Type:    FrameTypeGOAWAY,
			Payload: payload,
		})
	case "peer_CLOSE_no_error":
		payload, err := buildCodePayload(uint64(CodeNoError), "complete", e.conn.config.peer.Settings.MaxControlPayloadBytes)
		if err != nil {
			return err
		}
		return e.conn.handleCloseFrame(Frame{
			Type:    FrameTypeCLOSE,
			Payload: payload,
		})
	case "peer_CLOSE_protocol":
		payload, err := buildCodePayload(uint64(CodeProtocol), "protocol", e.conn.config.peer.Settings.MaxControlPayloadBytes)
		if err != nil {
			return err
		}
		return e.conn.handleCloseFrame(Frame{
			Type:    FrameTypeCLOSE,
			Payload: payload,
		})
	case "peer_first_DATA", "peer_DATA", "peer_late_DATA":
		return e.conn.handleDataFrame(Frame{
			Type:     FrameTypeDATA,
			StreamID: e.streamID,
			Payload:  []byte("x"),
		})
	case "peer_DATA_FIN":
		return e.conn.handleDataFrame(Frame{
			Type:     FrameTypeDATA,
			Flags:    FrameFlagFIN,
			StreamID: e.streamID,
		})
	case "local_DATA_FIN":
		return e.requireStream().CloseWrite()
	case "peer_first_RESET", "peer_late_RESET":
		return e.conn.handleResetFrame(Frame{
			Type:     FrameTypeRESET,
			StreamID: e.streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	case "peer_first_ABORT":
		return e.conn.handleAbortFrame(Frame{
			Type:     FrameTypeABORT,
			StreamID: e.streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	case "peer_first_MAX_DATA":
		return e.conn.handleMaxDataFrame(Frame{
			Type:     FrameTypeMAXDATA,
			StreamID: e.streamID,
			Payload:  mustEncodeVarint(64),
		})
	case "peer_same_MAX_DATA":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("no stream available for %q", event)
		}
		return e.conn.handleMaxDataFrame(Frame{
			Type:     FrameTypeMAXDATA,
			StreamID: e.streamID,
			Payload:  mustEncodeVarint(stream.sendMax),
		})
	case "peer_first_BLOCKED", "peer_BLOCKED":
		return e.conn.handleBlockedFrame(Frame{
			Type:     FrameTypeBLOCKED,
			StreamID: e.streamID,
			Payload:  mustEncodeVarint(64),
		})
	case "peer_same_BLOCKED":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("no stream available for %q", event)
		}
		return e.conn.handleBlockedFrame(Frame{
			Type:     FrameTypeBLOCKED,
			StreamID: e.streamID,
			Payload:  mustEncodeVarint(stream.sendMax),
		})
	case "peer_late_STOP_SENDING", "peer_STOP_SENDING":
		return e.conn.handleStopSendingFrame(Frame{
			Type:     FrameTypeStopSending,
			StreamID: e.streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	case "local_STOP_SENDING":
		return e.ensureLocalOwnedStream().CloseRead()
	case "local_STOP_SENDING_then_discard_unread_DATA":
		return e.requireStream().CloseRead()
	case "local_zero_window_write_then_deadline":
		stream := e.seedZeroWindowLocalStream(OpenOptions{}, nil)
		if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
			return err
		}
		resultCh := make(chan error, 1)
		go func() {
			_, err := stream.Write([]byte("x"))
			resultCh <- err
		}()
		select {
		case err := <-resultCh:
			return err
		case <-time.After(2 * testSignalTimeout):
			return fmt.Errorf("timed out waiting for local zero-window write deadline")
		}
	case "local_zero_window_write_open_metadata_then_deadline":
		opts, prefix, err := e.fixtureOpenMetadataOptions()
		if err != nil {
			return err
		}
		stream := e.seedZeroWindowLocalStream(opts, prefix)
		if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
			return err
		}
		resultCh := make(chan error, 1)
		go func() {
			_, err := stream.Write([]byte("x"))
			resultCh <- err
		}()
		select {
		case err := <-resultCh:
			return err
		case <-time.After(2 * testSignalTimeout):
			return fmt.Errorf("timed out waiting for local zero-window metadata write deadline")
		}
	case "local_zero_window_write_final_then_deadline":
		stream := e.seedZeroWindowLocalStream(OpenOptions{}, nil)
		if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
			return err
		}
		resultCh := make(chan error, 1)
		go func() {
			_, err := stream.WriteFinal([]byte("x"))
			resultCh <- err
		}()
		select {
		case err := <-resultCh:
			return err
		case <-time.After(2 * testSignalTimeout):
			return fmt.Errorf("timed out waiting for local zero-window WriteFinal deadline")
		}
	case "local_zero_window_write_final_open_metadata_then_deadline":
		opts, prefix, err := e.fixtureOpenMetadataOptions()
		if err != nil {
			return err
		}
		stream := e.seedZeroWindowLocalStream(opts, prefix)
		if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
			return err
		}
		resultCh := make(chan error, 1)
		go func() {
			_, err := stream.WriteFinal([]byte("x"))
			resultCh <- err
		}()
		select {
		case err := <-resultCh:
			return err
		case <-time.After(2 * testSignalTimeout):
			return fmt.Errorf("timed out waiting for local zero-window metadata WriteFinal deadline")
		}
	case "local_zero_window_close_write":
		return e.seedZeroWindowLocalStream(OpenOptions{}, nil).CloseWrite()
	case "local_zero_window_close_write_open_metadata":
		opts, prefix, err := e.fixtureOpenMetadataOptions()
		if err != nil {
			return err
		}
		return e.seedZeroWindowLocalStream(opts, prefix).CloseWrite()
	case "local_zero_window_close_read":
		return e.seedZeroWindowLocalStream(OpenOptions{}, nil).CloseRead()
	case "local_concrete_id_close_read_open_metadata":
		opts, prefix, err := e.fixtureOpenMetadataOptions()
		if err != nil {
			return err
		}
		return e.seedConcreteLocalStream(opts, prefix).CloseRead()
	case "local_concrete_id_close_write_open_metadata":
		opts, prefix, err := e.fixtureOpenMetadataOptions()
		if err != nil {
			return err
		}
		return e.seedConcreteLocalStream(opts, prefix).CloseWrite()
	case "local_concrete_id_close_with_error":
		return e.seedConcreteLocalStream(OpenOptions{}, nil).CloseWithError(uint64(CodeInternal), "bye")
	case "local_Close":
		return e.conn.Close()
	case "repeat_noop_session_BLOCKED_threshold_plus_one":
		frame := Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}
		for i := 0; i < noOpBlockedFloodThreshold; i++ {
			if err := e.conn.handleBlockedFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleBlockedFrame(frame)
	case "repeat_noop_session_MAX_DATA_threshold_plus_one":
		frame := Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(e.conn.flow.sendSessionMax)}
		for i := 0; i < noOpMaxDataFloodThreshold; i++ {
			if err := e.conn.handleMaxDataFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleMaxDataFrame(frame)
	case "repeat_noop_session_MAX_DATA_threshold_minus_one":
		frame := Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(e.conn.flow.sendSessionMax)}
		for i := 0; i < noOpMaxDataFloodThreshold-1; i++ {
			if err := e.conn.handleMaxDataFrame(frame); err != nil {
				return err
			}
		}
		return nil
	case "repeat_noop_session_MAX_DATA_threshold":
		frame := Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(e.conn.flow.sendSessionMax)}
		for i := 0; i < noOpMaxDataFloodThreshold; i++ {
			if err := e.conn.handleMaxDataFrame(frame); err != nil {
				return err
			}
		}
		return nil
	case "repeat_noop_session_BLOCKED_threshold_minus_one":
		frame := Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}
		for i := 0; i < noOpBlockedFloodThreshold-1; i++ {
			if err := e.conn.handleBlockedFrame(frame); err != nil {
				return err
			}
		}
		return nil
	case "repeat_noop_session_BLOCKED_threshold":
		frame := Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}
		for i := 0; i < noOpBlockedFloodThreshold; i++ {
			if err := e.conn.handleBlockedFrame(frame); err != nil {
				return err
			}
		}
		return nil
	case "peer_session_BLOCKED_with_pending_credit":
		e.conn.mu.Lock()
		e.conn.flow.recvSessionPending = 1
		e.conn.mu.Unlock()
		return e.conn.handleBlockedFrame(Frame{
			Type:    FrameTypeBLOCKED,
			Payload: mustEncodeVarint(0),
		})
	case "peer_PING_opaque_01234567":
		return e.conn.handleFrame(Frame{Type: FrameTypePING, Payload: []byte{0, 1, 2, 3, 4, 5, 6, 7}})
	case "peer_matching_PONG_direct_handler":
		return e.conn.handlePongFrame(Frame{Type: FrameTypePONG, Payload: []byte{9, 8, 7, 6, 5, 4, 3, 2}})
	case "peer_malformed_PONG_direct_handler":
		return e.conn.handlePongFrame(Frame{Type: FrameTypePONG, Payload: []byte{0x01}})
	case "repeat_mixed_noop_control_threshold_plus_one":
		stream := e.stream
		if stream == nil {
			return fmt.Errorf("missing seeded stream for mixed no-op control fixture")
		}
		maxData := Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(e.conn.flow.sendSessionMax)}
		blocked := Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			var err error
			if i%2 == 0 {
				err = e.conn.handleMaxDataFrame(maxData)
			} else {
				err = e.conn.handleBlockedFrame(blocked)
			}
			if err != nil {
				return err
			}
		}
		return e.conn.handleBlockedFrame(blocked)
	case "repeat_mixed_noop_control_threshold_minus_one":
		stream := e.stream
		if stream == nil {
			return fmt.Errorf("missing seeded stream for mixed no-op control fixture")
		}
		maxData := Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(e.conn.flow.sendSessionMax)}
		blocked := Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
		for i := 0; i < noOpControlFloodThreshold-1; i++ {
			var err error
			if i%2 == 0 {
				err = e.conn.handleMaxDataFrame(maxData)
			} else {
				err = e.conn.handleBlockedFrame(blocked)
			}
			if err != nil {
				return err
			}
		}
		return nil
	case "repeat_unexpected_PONG_threshold_plus_one":
		frame := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handlePongFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handlePongFrame(frame)
	case "handle_frame_unexpected_PONG_threshold_plus_one":
		frame := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handleFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleFrame(frame)
	case "unexpected_pong_threshold_minus_one_then_matching_pong_then_threshold":
		noOp := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
		for i := 0; i < noOpControlFloodThreshold-1; i++ {
			if err := e.conn.handlePongFrame(noOp); err != nil {
				return err
			}
		}
		payload := []byte{9, 8, 7, 6, 5, 4, 3, 2}
		done, _, err := e.conn.beginPing(payload)
		if err != nil {
			return err
		}
		if done == nil {
			return fmt.Errorf("beginPing returned nil done channel")
		}
		queued, err := waitForQueuedFrame(e.frames, "queued PING")
		if err != nil {
			return err
		}
		if queued.Type != FrameTypePING {
			return fmt.Errorf("queued frame type = %v, want %v", queued.Type, FrameTypePING)
		}
		if !bytes.Equal(queued.Payload, payload) {
			return fmt.Errorf("queued PING payload = %x, want %x", queued.Payload, payload)
		}
		if err := e.conn.handlePongFrame(Frame{Type: FrameTypePONG, Payload: payload}); err != nil {
			return err
		}
		select {
		case <-done:
		default:
			return fmt.Errorf("matching PONG did not complete outstanding ping")
		}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handlePongFrame(noOp); err != nil {
				return err
			}
		}
		return nil
	case "handle_frame_unexpected_pong_threshold_minus_one_then_matching_pong_then_threshold":
		noOp := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
		for i := 0; i < noOpControlFloodThreshold-1; i++ {
			if err := e.conn.handleFrame(noOp); err != nil {
				return err
			}
		}
		payload := []byte{9, 8, 7, 6, 5, 4, 3, 2}
		done, _, err := e.conn.beginPing(payload)
		if err != nil {
			return err
		}
		if done == nil {
			return fmt.Errorf("beginPing returned nil done channel")
		}
		queued, err := waitForQueuedFrame(e.frames, "queued PING")
		if err != nil {
			return err
		}
		if queued.Type != FrameTypePING {
			return fmt.Errorf("queued frame type = %v, want %v", queued.Type, FrameTypePING)
		}
		if !bytes.Equal(queued.Payload, payload) {
			return fmt.Errorf("queued PING payload = %x, want %x", queued.Payload, payload)
		}
		if err := e.conn.handleFrame(Frame{Type: FrameTypePONG, Payload: payload}); err != nil {
			return err
		}
		select {
		case <-done:
		default:
			return fmt.Errorf("matching handleFrame(PONG) did not complete outstanding ping")
		}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handleFrame(noOp); err != nil {
				return err
			}
		}
		return nil
	case "unexpected_pong_threshold_minus_one_then_peer_RESET_then_threshold":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for RESET budget-clear fixture")
		}
		noOp := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
		for i := 0; i < noOpControlFloodThreshold-1; i++ {
			if err := e.conn.handlePongFrame(noOp); err != nil {
				return err
			}
		}
		if err := e.conn.handleResetFrame(Frame{
			Type:     FrameTypeRESET,
			StreamID: stream.id,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		}); err != nil {
			return err
		}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handlePongFrame(noOp); err != nil {
				return err
			}
		}
		return nil
	case "peer_GOAWAY_then_repeat_same_GOAWAY_threshold_plus_one":
		bidi := maxPeerGoAwayWatermark(e.conn.config.negotiated.LocalRole, streamArityBidi)
		uni := maxPeerGoAwayWatermark(e.conn.config.negotiated.LocalRole, streamArityUni)
		payload, err := buildGoAwayPayload(bidi, uni, uint64(CodeNoError), "")
		if err != nil {
			return err
		}
		frame := Frame{
			Type:    FrameTypeGOAWAY,
			Payload: payload,
		}
		if err := e.conn.handleGoAwayFrame(frame); err != nil {
			return err
		}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handleGoAwayFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleGoAwayFrame(frame)
	case "noop_session_BLOCKED_threshold_minus_one_then_peer_GOAWAY_then_repeat_same_GOAWAY_threshold":
		noOpBlocked := Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}
		for i := 0; i < noOpControlFloodThreshold-1; i++ {
			if err := e.conn.handleBlockedFrame(noOpBlocked); err != nil {
				return err
			}
		}
		bidi := maxPeerGoAwayWatermark(e.conn.config.negotiated.LocalRole, streamArityBidi)
		uni := maxPeerGoAwayWatermark(e.conn.config.negotiated.LocalRole, streamArityUni)
		payload, err := buildGoAwayPayload(bidi, uni, uint64(CodeNoError), "")
		if err != nil {
			return err
		}
		frame := Frame{
			Type:    FrameTypeGOAWAY,
			Payload: payload,
		}
		if err := e.conn.handleGoAwayFrame(frame); err != nil {
			return err
		}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handleGoAwayFrame(frame); err != nil {
				return err
			}
		}
		return nil
	case "repeat_ignored_STOP_SENDING_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for ignored STOP_SENDING flood fixture")
		}
		frame := Frame{Type: FrameTypeStopSending, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handleStopSendingFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleStopSendingFrame(frame)
	case "repeat_ignored_RESET_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for ignored RESET flood fixture")
		}
		frame := Frame{Type: FrameTypeRESET, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handleResetFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleResetFrame(frame)
	case "repeat_ignored_ABORT_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for ignored ABORT flood fixture")
		}
		frame := Frame{Type: FrameTypeABORT, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}
		for i := 0; i < noOpControlFloodThreshold; i++ {
			if err := e.conn.handleAbortFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleAbortFrame(frame)
	case "repeat_noop_priority_update_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for PRIORITY_UPDATE fixture")
		}
		payload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(stream.priority)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		frame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}
		for i := 0; i < noOpPriorityUpdateFloodThreshold; i++ {
			if err := e.conn.handleExtFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleExtFrame(frame)
	case "noop_priority_update_threshold_minus_one_then_effective_update_then_threshold":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for PRIORITY_UPDATE budget-clear fixture")
		}
		noOpPayload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(stream.priority)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		changePayload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(9)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		noOpFrame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: noOpPayload}
		changeFrame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: changePayload}
		for i := 0; i < noOpPriorityUpdateFloodThreshold-1; i++ {
			if err := e.conn.handleExtFrame(noOpFrame); err != nil {
				return err
			}
		}
		if err := e.conn.handleExtFrame(changeFrame); err != nil {
			return err
		}
		if stream.priority != 9 {
			return fmt.Errorf("stream priority = %d, want 9", stream.priority)
		}
		for i := 0; i < noOpPriorityUpdateFloodThreshold; i++ {
			if err := e.conn.handleExtFrame(changeFrame); err != nil {
				return err
			}
		}
		return nil
	case "repeat_terminal_stream_MAX_DATA_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for terminal MAX_DATA flood fixture")
		}
		frame := Frame{Type: FrameTypeMAXDATA, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
		for i := 0; i < noOpMaxDataFloodThreshold; i++ {
			if err := e.conn.handleMaxDataFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleMaxDataFrame(frame)
	case "repeat_terminal_stream_BLOCKED_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for terminal BLOCKED flood fixture")
		}
		frame := Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
		for i := 0; i < noOpBlockedFloodThreshold; i++ {
			if err := e.conn.handleBlockedFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleBlockedFrame(frame)
	case "repeat_terminal_stream_PRIORITY_UPDATE_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for terminal PRIORITY_UPDATE flood fixture")
		}
		payload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(7)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		frame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}
		for i := 0; i < noOpPriorityUpdateFloodThreshold; i++ {
			if err := e.conn.handleExtFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleExtFrame(frame)
	case "repeat_noop_zero_length_DATA_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for zero-length DATA flood fixture")
		}
		frame := Frame{Type: FrameTypeDATA, StreamID: stream.id}
		for i := 0; i < noOpZeroDataFloodThreshold; i++ {
			if err := e.conn.handleDataFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleDataFrame(frame)
	case "repeat_noop_stream_BLOCKED_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for stream BLOCKED flood fixture")
		}
		frame := Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
		for i := 0; i < noOpBlockedFloodThreshold; i++ {
			if err := e.conn.handleBlockedFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleBlockedFrame(frame)
	case "repeat_noop_stream_MAX_DATA_threshold_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for stream MAX_DATA flood fixture")
		}
		frame := Frame{Type: FrameTypeMAXDATA, StreamID: stream.id, Payload: mustEncodeVarint(stream.sendMax)}
		for i := 0; i < noOpMaxDataFloodThreshold; i++ {
			if err := e.conn.handleMaxDataFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleMaxDataFrame(frame)
	case "noop_zero_length_DATA_threshold_minus_one_then_material_DATA_then_threshold":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for zero-length DATA budget-clear fixture")
		}
		noOp := Frame{Type: FrameTypeDATA, StreamID: stream.id}
		for i := 0; i < noOpZeroDataFloodThreshold-1; i++ {
			if err := e.conn.handleDataFrame(noOp); err != nil {
				return err
			}
		}
		if err := e.conn.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("x")}); err != nil {
			return err
		}
		for i := 0; i < noOpZeroDataFloodThreshold; i++ {
			if err := e.conn.handleDataFrame(noOp); err != nil {
				return err
			}
		}
		return nil
	case "effective_group_rebucket_limit_plus_one":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for group rebucket churn fixture")
		}
		firstPayload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(1)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		secondPayload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(2)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		thirdPayload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(3)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		if err := e.conn.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: firstPayload}); err != nil {
			return err
		}
		if err := e.conn.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: secondPayload}); err != nil {
			return err
		}
		return e.conn.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: thirdPayload})
	case "effective_group_updates_apply_without_group_fair_churn":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for non-group-fair group update fixture")
		}
		firstPayload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(1)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		secondPayload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Group: uint64ptr(2)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		if err := e.conn.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: firstPayload}); err != nil {
			return err
		}
		return e.conn.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: secondPayload})
	case "repeat_inbound_PING_threshold_plus_one":
		frame := Frame{Type: FrameTypePING, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
		for i := 0; i < inboundPingFloodThreshold; i++ {
			if err := e.conn.handleFrame(frame); err != nil {
				return err
			}
			queued, err := waitForQueuedFrame(e.frames, "queued PONG")
			if err != nil {
				return err
			}
			if queued.Type != FrameTypePONG {
				return fmt.Errorf("queued frame type = %v, want %v", queued.Type, FrameTypePONG)
			}
		}
		return e.conn.handleFrame(frame)
	case "repeat_inbound_control_frame_budget_plus_one":
		frame := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
		for i := 0; i < inboundControlFrameBudget; i++ {
			armMatchingPingForPolicyTest(e.conn, frame.Payload)
			if err := e.conn.handleFrame(frame); err != nil {
				return err
			}
		}
		armMatchingPingForPolicyTest(e.conn, frame.Payload)
		return e.conn.handleFrame(frame)
	case "repeat_inbound_control_byte_budget_plus_one":
		payload := make([]byte, int(e.conn.config.local.Settings.MaxControlPayloadBytes))
		payload[7] = 1
		frame := Frame{Type: FrameTypePONG, Payload: payload}
		e.conn.mu.Lock()
		byteBudget := e.conn.inboundControlByteBudgetLocked()
		e.conn.mu.Unlock()
		repeat := int(byteBudget/uint64(len(payload))) + 1
		for i := 0; i < repeat-1; i++ {
			if err := e.conn.handleFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleFrame(frame)
	case "repeat_inbound_EXT_frame_budget_plus_one":
		frame := Frame{Type: FrameTypeEXT, Payload: mustEncodeVarint(63)}
		for i := 0; i < inboundExtFrameBudget; i++ {
			if err := e.conn.handleFrame(frame); err != nil {
				return err
			}
		}
		return e.conn.handleFrame(frame)
	case "repeat_inbound_mixed_control_ext_frame_budget_plus_one":
		control := Frame{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
		ext := Frame{Type: FrameTypeEXT, Payload: mustEncodeVarint(63)}
		e.conn.mu.Lock()
		budget := e.conn.inboundMixedFrameBudgetLocked()
		e.conn.mu.Unlock()
		for i := uint32(0); i < budget; i++ {
			var err error
			if i%2 == 0 {
				armMatchingPingForPolicyTest(e.conn, control.Payload)
				err = e.conn.handleFrame(control)
			} else {
				err = e.conn.handleFrame(ext)
			}
			if err != nil {
				return err
			}
		}
		armMatchingPingForPolicyTest(e.conn, control.Payload)
		return e.conn.handleFrame(control)
	case "repeat_inbound_mixed_control_ext_byte_budget_plus_one":
		e.conn.mu.Lock()
		chunk := e.conn.config.local.Settings.MaxControlPayloadBytes
		if extMax := e.conn.config.local.Settings.MaxExtensionPayloadBytes; extMax < chunk {
			chunk = extMax
		}
		byteBudget := e.conn.inboundMixedByteBudgetLocked()
		e.conn.mu.Unlock()
		controlPayload := make([]byte, int(chunk))
		controlPayload[7] = 1
		extPayload := append(mustEncodeVarint(63), make([]byte, int(chunk)-1)...)
		control := Frame{Type: FrameTypePONG, Payload: controlPayload}
		ext := Frame{Type: FrameTypeEXT, Payload: extPayload}
		pairs := int(byteBudget / saturatingMul(chunk, 2))
		for i := 0; i < pairs; i++ {
			if err := e.conn.handleFrame(control); err != nil {
				return err
			}
			if err := e.conn.handleFrame(ext); err != nil {
				return err
			}
		}
		return e.conn.handleFrame(control)
	case "inbound_DATA_exceeds_session_memory_cap":
		e.conn.flow.sessionMemoryCap = 3
		return e.conn.handleDataFrame(Frame{
			Type:     FrameTypeDATA,
			StreamID: state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true),
			Payload:  []byte("abcd"),
		})
	case "peer_PRIORITY_UPDATE_priority_9_ext":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for PRIORITY_UPDATE EXT fixture")
		}
		payload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(9)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		return e.conn.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload})
	case "peer_malformed_PRIORITY_UPDATE_ext":
		return e.conn.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true), Payload: []byte{0xff}})
	case "peer_PRIORITY_UPDATE_priority_9_direct_handler":
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing seeded stream for direct PRIORITY_UPDATE fixture")
		}
		payload, err := buildPriorityUpdatePayload(e.conn.config.negotiated.Capabilities, MetadataUpdate{Priority: uint64ptr(9)}, e.conn.config.peer.Settings.MaxExtensionPayloadBytes)
		if err != nil {
			return err
		}
		_, n, err := ParseVarint(payload)
		if err != nil {
			return err
		}
		return e.conn.handlePriorityUpdateFrame(stream.id, payload[n:])
	case "local_take_pending_priority_update_handoff_transfers_tracked_bytes_to_advisory_queue":
		caps := CapabilityPriorityUpdate | CapabilityPriorityHints
		payload, err := buildPriorityUpdatePayload(caps, MetadataUpdate{Priority: uint64ptr(7)}, 4096)
		if err != nil {
			return err
		}
		c := newSessionMemoryTestConn()
		c.config.negotiated.Capabilities = caps
		stream := testVisibleBidiStream(c, 9)
		c.mu.Lock()
		testSetPendingPriorityUpdate(c, stream.id, append([]byte(nil), payload...))
		reqs, err := c.takePendingPriorityUpdateRequestsLocked()
		if err != nil {
			c.mu.Unlock()
			return err
		}
		if len(reqs) != 1 {
			c.mu.Unlock()
			return fmt.Errorf("request count = %d, want 1", len(reqs))
		}
		if c.pending.priorityBytes != 0 {
			got := c.pending.priorityBytes
			c.mu.Unlock()
			return fmt.Errorf("pendingPriorityBytes after handoff = %d, want 0", got)
		}
		req := reqs[0]
		if !req.advisoryReserved {
			c.mu.Unlock()
			return fmt.Errorf("advisory request was not marked reserved during handoff")
		}
		if c.flow.advisoryQueuedBytes != req.queuedBytes {
			got := c.flow.advisoryQueuedBytes
			c.mu.Unlock()
			return fmt.Errorf("advisoryQueuedBytes after handoff = %d, want %d", got, req.queuedBytes)
		}
		if got := c.trackedSessionMemoryLocked(); got != req.queuedBytes {
			c.mu.Unlock()
			return fmt.Errorf("trackedSessionMemoryLocked after handoff = %d, want %d", got, req.queuedBytes)
		}
		c.mu.Unlock()
		errCh := make(chan error, 1)
		go func() {
			errCh <- c.enqueuePreparedQueueRequest(&req, preparedQueueDispatchOptions{
				lane:      writeLaneAdvisory,
				ownership: frameOwned,
			})
		}()
		var queued writeRequest
		select {
		case queued = <-c.writer.advisoryWriteCh:
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("enqueuePreparedQueueRequest did not enqueue advisory request")
		}
		if !queued.advisoryReserved {
			return fmt.Errorf("queued advisory request was not marked reserved")
		}
		if queued.queuedBytes == 0 {
			return fmt.Errorf("queued advisory request queuedBytes = 0, want > 0")
		}
		c.mu.Lock()
		if c.flow.advisoryQueuedBytes != queued.queuedBytes {
			got := c.flow.advisoryQueuedBytes
			c.mu.Unlock()
			return fmt.Errorf("advisoryQueuedBytes = %d, want %d", got, queued.queuedBytes)
		}
		if got := c.trackedSessionMemoryLocked(); got != queued.queuedBytes {
			c.mu.Unlock()
			return fmt.Errorf("trackedSessionMemoryLocked = %d, want %d", got, queued.queuedBytes)
		}
		c.mu.Unlock()
		c.releaseBatchReservations([]writeRequest{queued})
		queued.done <- nil
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("queueAdvisoryFrames did not complete after advisory release")
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.flow.advisoryQueuedBytes != 0 {
			return fmt.Errorf("advisoryQueuedBytes after release = %d, want 0", c.flow.advisoryQueuedBytes)
		}
		return nil
	case "local_take_pending_priority_update_handoff_fails_when_memory_cap_exceeded":
		caps := CapabilityPriorityUpdate | CapabilityPriorityHints
		payload, err := buildPriorityUpdatePayload(caps, MetadataUpdate{Priority: uint64ptr(7)}, 4096)
		if err != nil {
			return err
		}
		c := newSessionMemoryTestConn()
		c.config.negotiated.Capabilities = caps
		stream := testVisibleBidiStream(c, 9)
		c.mu.Lock()
		testSetPendingPriorityUpdate(c, stream.id, append([]byte(nil), payload...))
		c.flow.sessionMemoryCap = uint64(len(payload))
		_, err = c.takePendingPriorityUpdateRequestsLocked()
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("takePendingPriorityUpdateRequestsLocked err = %v, want nil on advisory drop", err)
		}
		if testHasPendingPriorityUpdate(c, stream.id) {
			c.mu.Unlock()
			return fmt.Errorf("pending priority update retained after advisory drop")
		}
		if got := c.pending.priorityBytes; got != 0 {
			c.mu.Unlock()
			return fmt.Errorf("pendingPriorityBytes after advisory drop = %d, want 0", got)
		}
		if got := c.flow.advisoryQueuedBytes; got != 0 {
			c.mu.Unlock()
			return fmt.Errorf("advisoryQueuedBytes after advisory drop = %d, want 0", got)
		}
		c.mu.Unlock()
		return nil
	case "local_control_flush_transfers_pending_priority_bytes_to_advisory_queue":
		caps := CapabilityPriorityUpdate | CapabilityPriorityHints
		payload, err := buildPriorityUpdatePayload(caps, MetadataUpdate{Priority: uint64ptr(7)}, 4096)
		if err != nil {
			return err
		}
		c := newSessionMemoryTestConn()
		c.config.negotiated.Capabilities = caps
		stream := testVisibleBidiStream(c, 9)
		c.mu.Lock()
		testSetPendingPriorityUpdate(c, stream.id, append([]byte(nil), payload...))
		c.mu.Unlock()
		flushDone := make(chan struct{})
		go func() {
			c.controlFlushLoop()
			close(flushDone)
		}()
		notify(c.pending.controlNotify)
		var queued writeRequest
		select {
		case queued = <-c.writer.advisoryWriteCh:
		case <-time.After(testSignalTimeout):
			close(c.lifecycle.closedCh)
			return fmt.Errorf("controlFlushLoop did not enqueue advisory request")
		}
		c.mu.Lock()
		if c.pending.priorityBytes != 0 {
			got := c.pending.priorityBytes
			c.mu.Unlock()
			close(c.lifecycle.closedCh)
			return fmt.Errorf("pendingPriorityBytes after control flush = %d, want 0", got)
		}
		if c.flow.advisoryQueuedBytes != queued.queuedBytes {
			got := c.flow.advisoryQueuedBytes
			c.mu.Unlock()
			close(c.lifecycle.closedCh)
			return fmt.Errorf("advisoryQueuedBytes after control flush = %d, want %d", got, queued.queuedBytes)
		}
		if got := c.trackedSessionMemoryLocked(); got != queued.queuedBytes {
			c.mu.Unlock()
			close(c.lifecycle.closedCh)
			return fmt.Errorf("trackedSessionMemoryLocked after control flush = %d, want %d", got, queued.queuedBytes)
		}
		c.mu.Unlock()
		c.releaseBatchReservations([]writeRequest{queued})
		queued.done <- nil
		c.mu.Lock()
		if c.flow.advisoryQueuedBytes != 0 {
			got := c.flow.advisoryQueuedBytes
			c.mu.Unlock()
			close(c.lifecycle.closedCh)
			return fmt.Errorf("advisoryQueuedBytes after release = %d, want 0", got)
		}
		c.mu.Unlock()
		close(c.lifecycle.closedCh)
		select {
		case <-flushDone:
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("controlFlushLoop did not exit after closedCh close")
		}
		return nil
	case "local_write_deadline_restores_pending_priority_update_while_blocked_on_writer_queue":
		c, first, second, writer := newBlockedWriterConnWithStreams(e.t)
		wantPayload := configurePendingPriorityUpdateForTest(e.t, c, second, 7)
		firstErrCh := make(chan error, 1)
		go func() {
			_, err := first.Write([]byte("hello"))
			firstErrCh <- err
		}()
		select {
		case <-writer.started:
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("writer never became blocked on first write")
		}
		if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
			return err
		}
		_, err := second.Write([]byte("world"))
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return fmt.Errorf("second write err = %v, want deadline exceeded", err)
		}
		c.mu.Lock()
		gotPayload, ok := testPendingPriorityUpdatePayload(c, second.id)
		gotPrepared := c.pending.preparedPriorityBytes
		c.mu.Unlock()
		if !ok {
			return fmt.Errorf("pending priority update missing after writer-queue deadline rollback")
		}
		if !bytes.Equal(gotPayload, wantPayload) {
			return fmt.Errorf("restored priority payload = %x, want %x", gotPayload, wantPayload)
		}
		if gotPrepared != 0 {
			return fmt.Errorf("preparedPriorityBytes = %d, want 0 after rollback", gotPrepared)
		}
		_ = writer.Close()
		select {
		case <-firstErrCh:
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("first write did not unblock during cleanup")
		}
		return nil
	case "local_close_write_deadline_restores_pending_priority_update_while_blocked_on_writer_queue":
		c, first, second, writer := newBlockedWriterConnWithStreams(e.t)
		wantPayload := configurePendingPriorityUpdateForTest(e.t, c, second, 9)
		firstErrCh := make(chan error, 1)
		go func() {
			_, err := first.Write([]byte("hello"))
			firstErrCh <- err
		}()
		select {
		case <-writer.started:
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("writer never became blocked on first write")
		}
		if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
			return err
		}
		err := second.CloseWrite()
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return fmt.Errorf("close write err = %v, want deadline exceeded", err)
		}
		c.mu.Lock()
		gotPayload, ok := testPendingPriorityUpdatePayload(c, second.id)
		gotPrepared := c.pending.preparedPriorityBytes
		sendFin := second.sendFinReached()
		c.mu.Unlock()
		if !ok {
			return fmt.Errorf("pending priority update missing after CloseWrite deadline rollback")
		}
		if !bytes.Equal(gotPayload, wantPayload) {
			return fmt.Errorf("restored priority payload = %x, want %x", gotPayload, wantPayload)
		}
		if gotPrepared != 0 {
			return fmt.Errorf("preparedPriorityBytes = %d, want 0 after CloseWrite rollback", gotPrepared)
		}
		if sendFin {
			return fmt.Errorf("sendFinReached = true, want false after CloseWrite rollback")
		}
		_ = writer.Close()
		select {
		case <-firstErrCh:
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("first write did not unblock during cleanup")
		}
		return nil
	case "local_session_standing_growth_suppressed_while_released_credit_still_reflects_high_usage":
		settings := DefaultSettings()
		settings.InitialMaxData = 256
		settings.MaxFramePayload = 16
		c := &Conn{

			pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
			lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
				peer:       Preface{Settings: settings},
				negotiated: Negotiated{PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: 256,
				recvSessionReceived: 192,
				recvSessionPending:  16,
				recvSessionUsed:     120,
				sessionDataHWM:      128},
		}
		c.mu.Lock()
		c.replenishSessionLocked(c.sessionWindowTargetLocked())
		urgent, advisory := testDrainPendingControlFrames(c)
		c.mu.Unlock()
		if len(advisory) != 0 {
			return fmt.Errorf("advisory len = %d, want 0", len(advisory))
		}
		if len(urgent) != 1 {
			return fmt.Errorf("drained %d urgent frames, want 1", len(urgent))
		}
		if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || !bytes.Equal(urgent[0].Payload, mustEncodeVarint(272)) {
			return fmt.Errorf("session frame = %+v, want MAX_DATA session 272 without standing-growth jump", urgent[0])
		}
		return nil
	case "local_stream_standing_growth_suppressed_while_released_credit_still_reflects_high_usage":
		settings := DefaultSettings()
		settings.MaxFramePayload = 8
		c := &Conn{

			pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
			lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
				peer:       Preface{Settings: settings},
				negotiated: Negotiated{PeerSettings: settings, LocalRole: RoleInitiator}}, flow: connFlowState{perStreamDataHWM: 32}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
		}
		stream := &nativeStream{
			conn:           c,
			id:             4,
			idSet:          true,
			localReceive:   true,
			recvAdvertised: 64,
			recvReceived:   52,
			recvPending:    8,
			recvBuffer:     28,
		}
		stream.initHalfStates()
		c.registry.streams[stream.id] = stream
		c.mu.Lock()
		c.replenishStreamLocked(stream, c.streamWindowTargetLocked(stream))
		urgent, advisory := testDrainPendingControlFrames(c)
		c.mu.Unlock()
		if len(advisory) != 0 {
			return fmt.Errorf("advisory len = %d, want 0", len(advisory))
		}
		if len(urgent) != 1 {
			return fmt.Errorf("drained %d urgent frames, want 1", len(urgent))
		}
		if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 4 || !bytes.Equal(urgent[0].Payload, mustEncodeVarint(72)) {
			return fmt.Errorf("stream frame = %+v, want MAX_DATA stream 4 value 72 without standing-growth jump", urgent[0])
		}
		return nil
	case "local_session_standing_growth_suppressed_under_tracked_memory_pressure":
		settings := DefaultSettings()
		settings.InitialMaxData = 256
		settings.MaxFramePayload = 16
		c := &Conn{

			pending: connPendingControlState{
				controlNotify: make(chan struct{}, 1),
				controlBytes:  352,
			},
			lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
				peer:       Preface{Settings: settings},
				negotiated: Negotiated{PeerSettings: settings}}, flow: connFlowState{sessionMemoryCap: 512,
				recvSessionAdvertised: 256,
				recvSessionReceived:   192,
				recvSessionPending:    16,
				recvSessionUsed:       32,
				sessionDataHWM:        128},
		}
		c.mu.Lock()
		c.replenishSessionLocked(c.sessionWindowTargetLocked())
		urgent, advisory := testDrainPendingControlFrames(c)
		c.mu.Unlock()
		if len(advisory) != 0 {
			return fmt.Errorf("advisory len = %d, want 0", len(advisory))
		}
		if len(urgent) != 1 {
			return fmt.Errorf("drained %d urgent frames, want 1", len(urgent))
		}
		if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || !bytes.Equal(urgent[0].Payload, mustEncodeVarint(272)) {
			return fmt.Errorf("session frame = %+v, want MAX_DATA session 272 under tracked-memory pressure", urgent[0])
		}
		return nil
	case "local_stream_standing_growth_suppressed_under_tracked_memory_pressure":
		settings := DefaultSettings()
		settings.MaxFramePayload = 8
		settings.InitialMaxStreamDataBidiPeerOpened = 64
		streamID := state.FirstPeerStreamID(RoleInitiator, true)
		c := &Conn{

			pending: connPendingControlState{
				controlNotify: make(chan struct{}, 1),
				controlBytes:  352,
			},
			lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
				peer:       Preface{Settings: settings},
				negotiated: Negotiated{PeerSettings: settings, LocalRole: RoleInitiator}}, flow: connFlowState{sessionMemoryCap: 512,
				recvSessionUsed:  32,
				perStreamDataHWM: 32}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
		}
		stream := &nativeStream{
			conn:           c,
			id:             streamID,
			idSet:          true,
			localReceive:   true,
			localSend:      true,
			recvAdvertised: 64,
			recvReceived:   52,
			recvPending:    8,
			recvBuffer:     8,
		}
		stream.initHalfStates()
		c.registry.streams[stream.id] = stream
		c.mu.Lock()
		c.replenishStreamLocked(stream, c.streamWindowTargetLocked(stream))
		urgent, advisory := testDrainPendingControlFrames(c)
		c.mu.Unlock()
		if len(advisory) != 0 {
			return fmt.Errorf("advisory len = %d, want 0", len(advisory))
		}
		if len(urgent) != 1 {
			return fmt.Errorf("drained %d urgent frames, want 1", len(urgent))
		}
		if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != streamID || !bytes.Equal(urgent[0].Payload, mustEncodeVarint(72)) {
			return fmt.Errorf("stream frame = %+v, want MAX_DATA stream %d value 72 under tracked-memory pressure", urgent[0], streamID)
		}
		return nil
	case "local_open_metadata_prefix_does_not_consume_zero_windows":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		prefix, err := buildOpenMetadataPrefix(
			CapabilityOpenMetadata,
			OpenOptions{OpenInfo: []byte("ssh")},
			c.config.peer.Settings.MaxFramePayload,
		)
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.config.negotiated.Capabilities = CapabilityOpenMetadata
		c.config.local.Settings.InitialMaxData = 0
		c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 0
		c.flow.recvSessionAdvertised = 0
		c.mu.Unlock()
		streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
		if err := c.handleDataFrame(Frame{
			Type:     FrameTypeDATA,
			Flags:    FrameFlagOpenMetadata,
			StreamID: streamID,
			Payload:  prefix,
		}); err != nil {
			return fmt.Errorf("handleDataFrame(DATA|OPEN_METADATA, zero-window) = %v, want nil", err)
		}
		c.mu.Lock()
		stream := c.registry.streams[streamID]
		if stream == nil {
			c.mu.Unlock()
			return fmt.Errorf("stream not opened by zero-window OPEN_METADATA")
		}
		if got := string(stream.openInfo); got != "ssh" {
			c.mu.Unlock()
			return fmt.Errorf("stream.openInfo = %q, want %q", got, "ssh")
		}
		if stream.recvReceived != 0 || stream.recvBuffer != 0 {
			c.mu.Unlock()
			return fmt.Errorf("stream recv accounting = (%d,%d), want 0/0", stream.recvReceived, stream.recvBuffer)
		}
		if c.flow.recvSessionReceived != 0 || c.flow.recvSessionUsed != 0 {
			c.mu.Unlock()
			return fmt.Errorf("session recv accounting = (%d,%d), want 0/0", c.flow.recvSessionReceived, c.flow.recvSessionUsed)
		}
		c.mu.Unlock()
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		return nil
	case "local_open_metadata_flow_control_charges_only_trailing_application_bytes":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		prefix, err := buildOpenMetadataPrefix(
			CapabilityOpenMetadata,
			OpenOptions{OpenInfo: []byte("ssh")},
			c.config.peer.Settings.MaxFramePayload,
		)
		if err != nil {
			return err
		}
		payload := append(append([]byte(nil), prefix...), 'x')
		c.mu.Lock()
		c.config.negotiated.Capabilities = CapabilityOpenMetadata
		c.config.local.Settings.InitialMaxData = 1
		c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 1
		c.flow.recvSessionAdvertised = 1
		c.mu.Unlock()
		streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
		if err := c.handleDataFrame(Frame{
			Type:     FrameTypeDATA,
			Flags:    FrameFlagOpenMetadata,
			StreamID: streamID,
			Payload:  payload,
		}); err != nil {
			return fmt.Errorf("handleDataFrame(DATA|OPEN_METADATA with 1 app byte) = %v, want nil", err)
		}
		c.mu.Lock()
		stream := c.registry.streams[streamID]
		if stream == nil {
			c.mu.Unlock()
			return fmt.Errorf("stream not opened by OPEN_METADATA DATA")
		}
		if got := string(stream.openInfo); got != "ssh" {
			c.mu.Unlock()
			return fmt.Errorf("stream.openInfo = %q, want %q", got, "ssh")
		}
		if stream.recvReceived != 1 || stream.recvBuffer != 1 {
			c.mu.Unlock()
			return fmt.Errorf("stream recv accounting = (%d,%d), want 1/1", stream.recvReceived, stream.recvBuffer)
		}
		if c.flow.recvSessionReceived != 1 || c.flow.recvSessionUsed != 1 {
			c.mu.Unlock()
			return fmt.Errorf("session recv accounting = (%d,%d), want 1/1", c.flow.recvSessionReceived, c.flow.recvSessionUsed)
		}
		if string(stream.readBuf) != "x" {
			c.mu.Unlock()
			return fmt.Errorf("stream.readBuf = %q, want %q", string(stream.readBuf), "x")
		}
		c.mu.Unlock()
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		return nil
	case "local_open_metadata_trailing_application_byte_over_stream_window_locally_aborts":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		prefix, err := buildOpenMetadataPrefix(
			CapabilityOpenMetadata,
			OpenOptions{OpenInfo: []byte("ssh")},
			c.config.peer.Settings.MaxFramePayload,
		)
		if err != nil {
			return err
		}
		payload := append(append([]byte(nil), prefix...), 'x')
		c.mu.Lock()
		c.config.negotiated.Capabilities = CapabilityOpenMetadata
		c.config.local.Settings.InitialMaxData = 1
		c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 0
		c.flow.recvSessionAdvertised = 1
		c.mu.Unlock()
		streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
		if err := c.handleDataFrame(Frame{
			Type:     FrameTypeDATA,
			Flags:    FrameFlagOpenMetadata,
			StreamID: streamID,
			Payload:  payload,
		}); err != nil {
			return fmt.Errorf("handleDataFrame(DATA|OPEN_METADATA over stream window) = %v, want nil queued ABORT", err)
		}
		var queued Frame
		select {
		case queued = <-frames:
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("timed out waiting for queued ABORT")
		}
		if queued.Type != FrameTypeABORT {
			return fmt.Errorf("queued frame type = %v, want %v", queued.Type, FrameTypeABORT)
		}
		if queued.StreamID != streamID {
			return fmt.Errorf("queued frame stream = %d, want %d", queued.StreamID, streamID)
		}
		code, _, err := parseErrorPayload(queued.Payload)
		if err != nil {
			return fmt.Errorf("parse queued ABORT payload: %w", err)
		}
		if ErrorCode(code) != CodeFlowControl {
			return fmt.Errorf("queued ABORT code = %d, want %d", code, CodeFlowControl)
		}
		c.mu.Lock()
		if stream := c.registry.streams[streamID]; stream != nil {
			c.mu.Unlock()
			return fmt.Errorf("live stream %d retained after local FLOW_CONTROL abort", streamID)
		}
		if !c.hasTerminalMarkerLocked(streamID) {
			c.mu.Unlock()
			return fmt.Errorf("terminal marker missing for locally aborted stream %d", streamID)
		}
		if c.flow.recvSessionReceived != 0 || c.flow.recvSessionUsed != 0 {
			c.mu.Unlock()
			return fmt.Errorf("session recv accounting = (%d,%d), want 0/0", c.flow.recvSessionReceived, c.flow.recvSessionUsed)
		}
		c.mu.Unlock()
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected extra queued frame: %+v", frame)
		default:
		}
		return nil
	case "local_session_max_data_increase_broadcasts_conn_write_wake":
		c := &Conn{

			lifecycle: connLifecycleState{closedCh: make(chan struct{})}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
		}
		streamA := testLocalSendStream(c, 4)
		streamB := testLocalSendStream(c, 8)
		c.mu.Lock()
		c.flow.sendSessionMax = 0
		wake := c.currentWriteWakeLocked()
		c.mu.Unlock()
		if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(32)}); err != nil {
			return fmt.Errorf("handle session MAX_DATA err = %v, want nil", err)
		}
		select {
		case <-wake:
		default:
			return fmt.Errorf("expected session MAX_DATA increase to close the connection-level write wake channel")
		}
		select {
		case <-streamA.writeNotify:
			return fmt.Errorf("session MAX_DATA increase should not need per-stream writeNotify for streamA")
		default:
		}
		select {
		case <-streamB.writeNotify:
			return fmt.Errorf("session MAX_DATA increase should not need per-stream writeNotify for streamB")
		default:
		}
		return nil
	case "local_prepare_write_wakes_on_session_max_data_increase":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		c.flow.sendSessionMax = 0
		stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
		testMarkLocalOpenVisible(stream)
		c.registry.streams[stream.id] = stream
		c.mu.Unlock()
		resultCh := make(chan struct {
			step writeStep
			err  error
		}, 1)
		go func() {
			var parts [1][]byte
			parts[0] = []byte("x")
			step, err := stream.prepareWritePartsLocked(parts[:], 0, 0, 1, writeChunkStreaming)
			resultCh <- struct {
				step writeStep
				err  error
			}{step: step, err: err}
		}()
		if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(1)}); err != nil {
			return fmt.Errorf("handle session MAX_DATA err = %v, want nil", err)
		}
		select {
		case result := <-resultCh:
			if result.err != nil {
				return fmt.Errorf("prepareWriteLocked err = %v, want nil", result.err)
			}
			if result.step.frame.Type != FrameTypeDATA || result.step.frame.StreamID != stream.id || string(result.step.frame.Payload) != "x" {
				return fmt.Errorf("prepareWriteLocked frame = %+v, want DATA for stream %d with payload x", result.step.frame, stream.id)
			}
			if result.step.appN != 1 {
				return fmt.Errorf("prepareWriteLocked appN = %d, want 1", result.step.appN)
			}
			return nil
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("prepareWriteLocked did not wake after session MAX_DATA increase")
		}
	case "local_prepare_write_final_wakes_on_session_max_data_increase":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		c.flow.sendSessionMax = 0
		stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
		testMarkLocalOpenVisible(stream)
		c.registry.streams[stream.id] = stream
		c.mu.Unlock()
		resultCh := make(chan struct {
			step writeStep
			err  error
		}, 1)
		go func() {
			var parts [1][]byte
			parts[0] = []byte("x")
			step, err := stream.prepareWritePartsLocked(parts[:], 0, 0, 1, writeChunkFinal)
			resultCh <- struct {
				step writeStep
				err  error
			}{step: step, err: err}
		}()
		if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(1)}); err != nil {
			return fmt.Errorf("handle session MAX_DATA err = %v, want nil", err)
		}
		select {
		case result := <-resultCh:
			if result.err != nil {
				return fmt.Errorf("prepareWriteFinalLocked err = %v, want nil", result.err)
			}
			if result.step.frame.Type != FrameTypeDATA || result.step.frame.StreamID != stream.id || string(result.step.frame.Payload) != "x" || result.step.frame.Flags&FrameFlagFIN == 0 {
				return fmt.Errorf("prepareWriteFinalLocked frame = %+v, want DATA|FIN for stream %d with payload x", result.step.frame, stream.id)
			}
			if result.step.appN != 1 {
				return fmt.Errorf("prepareWriteFinalLocked appN = %d, want 1", result.step.appN)
			}
			return nil
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("prepareWriteFinalLocked did not wake after session MAX_DATA increase")
		}
	case "local_release_receive_zero_window_wakes_write_waiters_when_memory_pressure_drops":
		c := newSessionMemoryTestConn()
		stream := testBuildStream(
			c,
			8,
			testWithLocalReceive(),
			testWithApplicationVisible(),
			testWithRecvWindow(0, 64),
			testWithWriteNotify(),
		)
		c.mu.Lock()
		wake := c.currentWriteWakeLocked()
		threshold := c.sessionMemoryHighThresholdLocked()
		c.flow.recvSessionUsed = threshold
		c.releaseReceiveLocked(stream, 64)
		c.mu.Unlock()
		select {
		case <-wake:
			return nil
		default:
			return fmt.Errorf("expected zero-window releaseReceiveLocked to wake blocked writers when session memory pressure drops")
		}
	case "local_update_metadata_tightens_oversized_open_metadata_prefix_backing":
		c := newSessionMemoryTestConn()
		c.config.negotiated.Capabilities = CapabilityOpenMetadata | CapabilityPriorityHints
		c.mu.Lock()
		stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, nil)
		oversizedPrefix := make([]byte, 3, 64)
		copy(oversizedPrefix, "old")
		stream.openMetadataPrefix = oversizedPrefix
		c.mu.Unlock()
		priority := uint64(7)
		if err := stream.UpdateMetadata(MetadataUpdate{Priority: &priority}); err != nil {
			return fmt.Errorf("UpdateMetadata: %v", err)
		}
		wantPrefix, err := buildOpenMetadataPrefix(
			c.config.negotiated.Capabilities,
			OpenOptions{InitialPriority: &priority, OpenInfo: []byte("ssh")},
			c.config.peer.Settings.MaxFramePayload,
		)
		if err != nil {
			return fmt.Errorf("buildOpenMetadataPrefix: %v", err)
		}
		c.mu.Lock()
		gotPrefix := append([]byte(nil), stream.openMetadataPrefix...)
		gotCap := cap(stream.openMetadataPrefix)
		aliased := len(stream.openMetadataPrefix) > 0 && &stream.openMetadataPrefix[0] == &oversizedPrefix[0]
		c.mu.Unlock()
		if !bytes.Equal(gotPrefix, wantPrefix) {
			return fmt.Errorf("openMetadataPrefix = %x, want %x", gotPrefix, wantPrefix)
		}
		if gotCap != len(gotPrefix) {
			return fmt.Errorf("cap(stream.openMetadataPrefix) = %d, want tight cap %d", gotCap, len(gotPrefix))
		}
		if aliased {
			return fmt.Errorf("UpdateMetadata reused oversized openMetadataPrefix backing array")
		}
		return nil
	case "local_compact_terminal_releases_open_info_bytes_and_open_metadata_prefix":
		c := newSessionMemoryTestConn()
		stream := &nativeStream{
			conn:               c,
			id:                 4,
			idSet:              true,
			bidi:               true,
			localOpen:          testLocalOpenOpenedState(),
			localSend:          true,
			localReceive:       true,
			applicationVisible: true,
			openInfo:           []byte("ssh"),
			openMetadataPrefix: []byte("meta"),
			readNotify:         make(chan struct{}, 1),
			writeNotify:        make(chan struct{}, 1),
		}
		stream.initHalfStates()
		stream.setSendFin()
		stream.setRecvFin()
		c.mu.Lock()
		c.registry.streams[stream.id] = stream
		c.retention.retainedOpenInfoBytes = uint64(len(stream.openInfo))
		c.maybeCompactTerminalLocked(stream)
		gotBytes := c.retention.retainedOpenInfoBytes
		gotPrefixLen := len(stream.openMetadataPrefix)
		c.mu.Unlock()
		if gotBytes != 0 {
			return fmt.Errorf("retainedOpenInfoBytes = %d, want 0 after terminal compaction", gotBytes)
		}
		if gotPrefixLen != 0 {
			return fmt.Errorf("openMetadataPrefix len after terminal compaction = %d, want 0", gotPrefixLen)
		}
		return nil
	case "local_fail_provisional_releases_open_info_bytes_and_clears_open_metadata_prefix":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, []byte("meta"))
		before := c.retention.retainedOpenInfoBytes
		c.failProvisionalLocked(stream, nil)
		after := c.retention.retainedOpenInfoBytes
		gotPrefixLen := len(stream.openMetadataPrefix)
		c.mu.Unlock()
		if before != 3 {
			return fmt.Errorf("retainedOpenInfoBytes before fail = %d, want 3", before)
		}
		if after != 0 {
			return fmt.Errorf("retainedOpenInfoBytes after fail = %d, want 0", after)
		}
		if gotPrefixLen != 0 {
			return fmt.Errorf("openMetadataPrefix len after fail = %d, want 0", gotPrefixLen)
		}
		return nil
	case "local_fail_unopened_local_stream_clears_open_metadata_prefix":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		stream := c.newLocalStreamLocked(4, streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, []byte("meta"))
		c.registry.streams[stream.id] = stream
		c.appendUnseenLocalLocked(stream)
		c.failUnopenedLocalStreamLocked(stream, &ApplicationError{Code: uint64(CodeCancelled)})
		gotPrefixLen := len(stream.openMetadataPrefix)
		c.mu.Unlock()
		if gotPrefixLen != 0 {
			return fmt.Errorf("openMetadataPrefix len after failUnopenedLocalStreamLocked = %d, want 0", gotPrefixLen)
		}
		return nil
	case "local_mark_peer_visible_clears_open_metadata_prefix":
		c := newSessionMemoryTestConn()
		stream := &nativeStream{
			conn:               c,
			id:                 4,
			idSet:              true,
			localOpen:          testLocalOpenOpenedState(),
			localSend:          true,
			localReceive:       true,
			openMetadataPrefix: []byte("meta"),
		}
		c.mu.Lock()
		c.registry.streams[stream.id] = stream
		c.markPeerVisibleLocked(stream)
		peerVisible := stream.isPeerVisibleLocked()
		gotPrefixLen := len(stream.openMetadataPrefix)
		c.mu.Unlock()
		if !peerVisible {
			return fmt.Errorf("peerVisible = false, want true")
		}
		if gotPrefixLen != 0 {
			return fmt.Errorf("openMetadataPrefix len after markPeerVisibleLocked = %d, want 0", gotPrefixLen)
		}
		return nil
	case "local_close_stream_on_session_releases_open_metadata_prefix":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		stream := c.newLocalStreamLocked(4, streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, []byte("meta"))
		stream.idSet = true
		c.registry.streams[stream.id] = stream
		c.closeStreamOnSessionWithOptionsLocked(stream, &ApplicationError{Code: uint64(CodeInternal)}, sessionCloseOptions{
			abortSource: terminalAbortLocal,
			finalize:    true,
		})
		gotPrefixLen := len(stream.openMetadataPrefix)
		c.mu.Unlock()
		if gotPrefixLen != 0 {
			return fmt.Errorf("openMetadataPrefix len after closeStreamOnSessionWithOptionsLocked = %d, want 0", gotPrefixLen)
		}
		return nil
	case "local_close_session_releases_provisional_open_metadata_prefix":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, []byte("meta"))
		if got := len(stream.openMetadataPrefix); got == 0 {
			c.mu.Unlock()
			return fmt.Errorf("openMetadataPrefix len before close = 0, want > 0")
		}
		c.mu.Unlock()
		c.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})
		c.mu.Lock()
		gotPrefixLen := len(stream.openMetadataPrefix)
		c.mu.Unlock()
		if gotPrefixLen != 0 {
			return fmt.Errorf("openMetadataPrefix len after closeSession = %d, want 0", gotPrefixLen)
		}
		return nil
	case "local_peer_nonclose_frames_ignored_after_transport_failure":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		c.closeSession(io.ErrClosedPipe)
		queued := awaitQueuedFrame(e.t, frames)
		if queued.Type != FrameTypeCLOSE {
			return fmt.Errorf("queued frame after transport failure close = %+v, want CLOSE", queued)
		}
		c.mu.Lock()
		c.abuse.controlBudgetFrames = 4
		c.abuse.mixedBudgetFrames = 6
		c.abuse.noopControlCount = 8
		c.abuse.noopDataCount = 10
		c.mu.Unlock()
		data := Frame{Type: FrameTypeDATA, StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), Payload: []byte("late")}
		if err := c.handleFrame(data); err != nil {
			return fmt.Errorf("handleFrame(DATA) after transport/session failure = %v, want nil", err)
		}
		if err := c.handleFrame(Frame{
			Type:     FrameTypeRESET,
			StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true),
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		}); err != nil {
			return fmt.Errorf("handleFrame(RESET) after transport/session failure = %v, want nil", err)
		}
		if err := c.handleFrame(Frame{Type: FrameTypeGOAWAY, Payload: []byte{0xff}}); err != nil {
			return fmt.Errorf("handleFrame(GOAWAY) after transport/session failure = %v, want nil", err)
		}
		c.mu.Lock()
		if len(c.registry.streams) != 0 {
			c.mu.Unlock()
			return fmt.Errorf("streams changed after terminal ignore = %d, want 0", len(c.registry.streams))
		}
		if c.abuse.controlBudgetFrames != 4 || c.abuse.mixedBudgetFrames != 6 || c.abuse.noopControlCount != 8 || c.abuse.noopDataCount != 10 {
			c.mu.Unlock()
			return fmt.Errorf("budgets changed after terminal ignore = (%d,%d,%d,%d), want (4,6,8,10)",
				c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount, c.abuse.noopDataCount)
		}
		c.mu.Unlock()
		assertNoQueuedFrame(e.t, frames)
		return nil
	case "local_duplicate_peer_close_ignored_before_parse_and_budget_accounting":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		err := c.handleFrame(Frame{
			Type:    FrameTypeCLOSE,
			Payload: mustClosePayload(e.t, uint64(CodeProtocol), "peer-close"),
		})
		var appErr *ApplicationError
		if !errors.As(err, &appErr) {
			return fmt.Errorf("initial handleFrame(CLOSE) err = %v, want ApplicationError", err)
		}
		if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "peer-close" {
			return fmt.Errorf("initial peer close err = (%d,%q), want (%d,%q)", appErr.Code, appErr.Reason, uint64(CodeProtocol), "peer-close")
		}
		assertNoQueuedFrame(e.t, frames)
		c.mu.Lock()
		c.abuse.controlBudgetFrames = 7
		c.abuse.mixedBudgetFrames = 9
		c.abuse.noopControlCount = 11
		c.mu.Unlock()
		if err := c.handleFrame(Frame{Type: FrameTypeCLOSE, Payload: []byte{0xff}}); err != nil {
			return fmt.Errorf("duplicate malformed handleFrame(CLOSE) err = %v, want nil", err)
		}
		c.mu.Lock()
		if c.sessionControl.peerCloseErr == nil || c.sessionControl.peerCloseErr.Code != uint64(CodeProtocol) || c.sessionControl.peerCloseErr.Reason != "peer-close" {
			c.mu.Unlock()
			return fmt.Errorf("peerCloseErr = %#v, want preserved initial peer close", c.sessionControl.peerCloseErr)
		}
		if c.abuse.controlBudgetFrames != 7 || c.abuse.mixedBudgetFrames != 9 || c.abuse.noopControlCount != 11 {
			c.mu.Unlock()
			return fmt.Errorf("budgets changed after ignored duplicate CLOSE = (%d,%d,%d), want (7,9,11)",
				c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
		}
		c.mu.Unlock()
		assertNoQueuedFrame(e.t, frames)
		return nil
	case "local_direct_handle_close_frame_ignores_duplicate_malformed_payload":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		err := c.handleCloseFrame(Frame{
			Type:    FrameTypeCLOSE,
			Payload: mustClosePayload(e.t, uint64(CodeProtocol), "peer-close"),
		})
		var appErr *ApplicationError
		if !errors.As(err, &appErr) {
			return fmt.Errorf("initial handleCloseFrame err = %v, want ApplicationError", err)
		}
		assertNoQueuedFrame(e.t, frames)
		if err := c.handleCloseFrame(Frame{Type: FrameTypeCLOSE, Payload: []byte{0xff}}); err != nil {
			return fmt.Errorf("duplicate malformed handleCloseFrame err = %v, want nil", err)
		}
		assertNoQueuedFrame(e.t, frames)
		return nil
	case "local_peer_close_after_transport_failure_still_parses_diagnostics":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		c.closeSession(io.ErrClosedPipe)
		queued := awaitQueuedFrame(e.t, frames)
		if queued.Type != FrameTypeCLOSE {
			return fmt.Errorf("queued frame after transport failure = %+v, want CLOSE", queued)
		}
		c.mu.Lock()
		c.abuse.controlBudgetFrames = 4
		c.abuse.mixedBudgetFrames = 6
		c.abuse.noopControlCount = 8
		c.abuse.controlBudgetWindowStart = windowStampAt(time.Now())
		c.abuse.mixedBudgetWindowStart = c.abuse.controlBudgetWindowStart
		c.mu.Unlock()
		err := c.handleFrame(Frame{
			Type:    FrameTypeCLOSE,
			Payload: mustClosePayload(e.t, uint64(CodeFlowControl), "peer-diagnostics"),
		})
		var appErr *ApplicationError
		if !errors.As(err, &appErr) {
			return fmt.Errorf("handleFrame(CLOSE) after transport failure err = %v, want ApplicationError", err)
		}
		if appErr.Code != uint64(CodeFlowControl) || appErr.Reason != "peer-diagnostics" {
			return fmt.Errorf("peer close err = (%d,%q), want (%d,%q)", appErr.Code, appErr.Reason, uint64(CodeFlowControl), "peer-diagnostics")
		}
		c.mu.Lock()
		if c.sessionControl.peerCloseErr == nil || c.sessionControl.peerCloseErr.Code != uint64(CodeFlowControl) || c.sessionControl.peerCloseErr.Reason != "peer-diagnostics" {
			c.mu.Unlock()
			return fmt.Errorf("peerCloseErr = %#v, want retained parsed diagnostics", c.sessionControl.peerCloseErr)
		}
		if c.abuse.controlBudgetFrames != 5 || c.abuse.mixedBudgetFrames != 7 || c.abuse.noopControlCount != 8 {
			c.mu.Unlock()
			return fmt.Errorf("budgets after parsed transport-failure CLOSE = (%d,%d,%d), want (5,7,8)",
				c.abuse.controlBudgetFrames, c.abuse.mixedBudgetFrames, c.abuse.noopControlCount)
		}
		c.mu.Unlock()
		assertNoQueuedFrame(e.t, frames)
		return nil
	case "local_direct_nonclose_handlers_ignored_when_closing":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		c.mu.Lock()
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
		c.mu.Unlock()
		streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte("late")}); err != nil {
			return fmt.Errorf("handleDataFrame while closing = %v, want nil", err)
		}
		if err := c.handleResetFrame(Frame{Type: FrameTypeRESET, StreamID: streamID, Payload: mustEncodeVarint(uint64(CodeCancelled))}); err != nil {
			return fmt.Errorf("handleResetFrame while closing = %v, want nil", err)
		}
		if err := c.handleStopSendingFrame(Frame{Type: FrameTypeStopSending, StreamID: streamID, Payload: mustEncodeVarint(uint64(CodeCancelled))}); err != nil {
			return fmt.Errorf("handleStopSendingFrame while closing = %v, want nil", err)
		}
		if err := c.handleAbortFrame(Frame{Type: FrameTypeABORT, StreamID: streamID, Payload: mustEncodeVarint(uint64(CodeCancelled))}); err != nil {
			return fmt.Errorf("handleAbortFrame while closing = %v, want nil", err)
		}
		if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, StreamID: streamID, Payload: mustEncodeVarint(64)}); err != nil {
			return fmt.Errorf("handleMaxDataFrame while closing = %v, want nil", err)
		}
		if err := c.handleBlockedFrame(Frame{Type: FrameTypeBLOCKED, StreamID: streamID, Payload: mustEncodeVarint(64)}); err != nil {
			return fmt.Errorf("handleBlockedFrame while closing = %v, want nil", err)
		}
		if err := c.handleGoAwayFrame(Frame{Type: FrameTypeGOAWAY, Payload: []byte{0xff}}); err != nil {
			return fmt.Errorf("handleGoAwayFrame while closing = %v, want nil", err)
		}
		c.mu.Lock()
		if len(c.registry.streams) != 0 {
			c.mu.Unlock()
			return fmt.Errorf("streams changed after direct ignored handlers = %d, want 0", len(c.registry.streams))
		}
		if c.sessionControl.peerGoAwayErr != nil {
			c.mu.Unlock()
			return fmt.Errorf("peer GOAWAY error changed after direct ignored handler = %#v, want nil", c.sessionControl.peerGoAwayErr)
		}
		c.mu.Unlock()
		assertNoQueuedFrame(e.t, frames)
		return nil
	case "local_direct_terminal_data_handler_ignored_when_closing":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		c.mu.Lock()
		c.lifecycle.sessionState = connStateClosing
		c.lifecycle.closeErr = ErrSessionClosed
		c.flow.recvSessionUsed = 13
		c.abuse.noopDataCount = 11
		c.mu.Unlock()
		if err := c.handleTerminalDataFrame(
			Frame{Type: FrameTypeDATA, StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), Payload: []byte("late")},
			terminalDataDisposition{action: lateDataIgnore},
		); err != nil {
			return fmt.Errorf("handleTerminalDataFrame while closing = %v, want nil", err)
		}
		c.mu.Lock()
		if c.flow.recvSessionUsed != 13 {
			c.mu.Unlock()
			return fmt.Errorf("recvSessionUsed = %d, want 13", c.flow.recvSessionUsed)
		}
		if c.abuse.noopDataCount != 11 {
			c.mu.Unlock()
			return fmt.Errorf("noopDataCount = %d, want 11", c.abuse.noopDataCount)
		}
		c.mu.Unlock()
		assertNoQueuedFrame(e.t, frames)
		return nil
	case "local_close_read_with_oversized_open_metadata_keeps_read_stop_without_committing_opener":
		c, frames, stop := newInvalidFrameConn(e.t, 0)
		defer stop()
		c.mu.Lock()
		c.config.peer.Settings.MaxFramePayload = 4
		stream := c.newLocalStreamWithIDLocked(
			state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{},
			[]byte("oversized"),
		)
		c.registry.streams[stream.id] = stream
		c.appendUnseenLocalLocked(stream)
		c.mu.Unlock()
		err := stream.CloseRead()
		if !errors.Is(err, ErrOpenMetadataTooLarge) {
			return fmt.Errorf("CloseRead err = %v, want %v", err, ErrOpenMetadataTooLarge)
		}
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if !stream.readStopSentLocked() {
			return fmt.Errorf("readStopped = false, want true after local CloseRead commit")
		}
		if stream.localOpen.committed {
			return fmt.Errorf("sendCommitted = true, want false when opener validation fails before queueing")
		}
		if stream.visibilityPhaseLocked() == state.LocalOpenPhaseQueued {
			return fmt.Errorf("openingBarrier = true, want false when opener validation fails before queueing")
		}
		if stream.isPeerVisibleLocked() {
			return fmt.Errorf("peerVisible = true, want false when opener never enters writer path")
		}
		return nil
	case "local_close_read_with_malformed_open_metadata_keeps_read_stop_without_committing_opener":
		c, frames, stop := newInvalidFrameConn(e.t, 0)
		defer stop()
		c.mu.Lock()
		c.config.peer.Settings.MaxFramePayload = 16
		stream := c.newLocalStreamWithIDLocked(
			state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{},
			[]byte("bad"),
		)
		c.registry.streams[stream.id] = stream
		c.appendUnseenLocalLocked(stream)
		c.mu.Unlock()
		err := stream.CloseRead()
		if err == nil {
			return fmt.Errorf("CloseRead err = nil, want malformed OPEN_METADATA error")
		}
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if !stream.readStopSentLocked() {
			return fmt.Errorf("readStopped = false, want true after local CloseRead commit")
		}
		if stream.localOpen.committed {
			return fmt.Errorf("sendCommitted = true, want false when opener validation fails before queueing")
		}
		if stream.visibilityPhaseLocked() == state.LocalOpenPhaseQueued {
			return fmt.Errorf("openingBarrier = true, want false when opener validation fails before queueing")
		}
		if stream.isPeerVisibleLocked() {
			return fmt.Errorf("peerVisible = true, want false when opener never enters writer path")
		}
		return nil
	case "local_close_write_with_oversized_open_metadata_without_committing_opener":
		c, frames, stop := newInvalidFrameConn(e.t, 0)
		defer stop()
		c.mu.Lock()
		c.config.peer.Settings.MaxFramePayload = 4
		stream := c.newLocalStreamWithIDLocked(
			state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{},
			[]byte("oversized"),
		)
		c.registry.streams[stream.id] = stream
		c.appendUnseenLocalLocked(stream)
		c.mu.Unlock()
		err := stream.CloseWrite()
		if !errors.Is(err, ErrOpenMetadataTooLarge) {
			return fmt.Errorf("close write err = %v, want %v", err, ErrOpenMetadataTooLarge)
		}
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if stream.localOpen.committed {
			return fmt.Errorf("sendCommitted = true, want false when opener validation fails before queueing")
		}
		if stream.sendFinReached() {
			return fmt.Errorf("sendFinReached() = true, want false when opener validation fails before queueing")
		}
		if stream.visibilityPhaseLocked() == state.LocalOpenPhaseQueued {
			return fmt.Errorf("openingBarrier = true, want false when opener validation fails before queueing")
		}
		if stream.isPeerVisibleLocked() {
			return fmt.Errorf("peerVisible = true, want false when opener never enters writer path")
		}
		return nil
	case "local_close_write_with_malformed_open_metadata_without_committing_opener":
		c, frames, stop := newInvalidFrameConn(e.t, 0)
		defer stop()
		c.mu.Lock()
		c.config.peer.Settings.MaxFramePayload = 16
		stream := c.newLocalStreamWithIDLocked(
			state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{},
			[]byte("bad"),
		)
		c.registry.streams[stream.id] = stream
		c.appendUnseenLocalLocked(stream)
		c.mu.Unlock()
		err := stream.CloseWrite()
		if err == nil {
			return fmt.Errorf("close write err = nil, want malformed OPEN_METADATA error")
		}
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if stream.localOpen.committed {
			return fmt.Errorf("sendCommitted = true, want false when opener validation fails before queueing")
		}
		if stream.sendFinReached() {
			return fmt.Errorf("sendFinReached() = true, want false when opener validation fails before queueing")
		}
		if stream.visibilityPhaseLocked() == state.LocalOpenPhaseQueued {
			return fmt.Errorf("openingBarrier = true, want false when opener validation fails before queueing")
		}
		if stream.isPeerVisibleLocked() {
			return fmt.Errorf("peerVisible = true, want false when opener never enters writer path")
		}
		return nil
	case "local_release_write_queue_reservation_wakes_blocked_write_at_low_watermark":
		stream := newQueueBackpressureTestStream()
		holder := &nativeStream{
			conn:        stream.conn,
			id:          8,
			idSet:       true,
			localSend:   true,
			writeNotify: make(chan struct{}, 1),
		}
		stream.conn.registry.streams[holder.id] = holder
		sessionHWM := stream.conn.sessionDataHWMLocked()
		sessionLWM := stream.conn.sessionDataLWMLocked()
		reserved := sessionHWM - sessionLWM
		holder.queuedDataBytes = reserved
		stream.conn.flow.queuedDataBytes = sessionHWM
		req := writeRequest{
			frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: holder.id, Payload: []byte("held")}}),
			done:           make(chan error, 1),
			origin:         writeRequestOriginStream,
			queueReserved:  true,
			queuedBytes:    reserved,
			reservedStream: holder,
		}
		done := make(chan error, 1)
		go func() {
			_, err := stream.Write([]byte("x"))
			done <- err
		}()
		deadline := time.Now().Add(testSignalTimeout)
		for stream.loadWriteWaiters() == 0 {
			if time.Now().After(deadline) {
				return fmt.Errorf("blocked write did not enter wait state")
			}
			runtime.Gosched()
		}
		stream.conn.releaseWriteQueueReservation(&req)
		var queued writeRequest
		select {
		case queued = <-stream.conn.writer.writeCh:
			queued.done <- nil
		case <-time.After(2 * testSignalTimeout):
			return fmt.Errorf("blocked write did not wake after queued bytes crossed low watermark")
		}
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("write err = %v, want nil", err)
			}
		case <-time.After(2 * testSignalTimeout):
			return fmt.Errorf("blocked write did not complete after queued bytes crossed low watermark")
		}
		return nil
	case "local_release_batch_reservations_clears_inflight_queued":
		stream := newQueueBackpressureTestStream()
		stream.queuedDataBytes = 7
		stream.inflightQueued = 7
		batch := []writeRequest{{
			frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
			done:           make(chan error, 1),
			origin:         writeRequestOriginStream,
			queueReserved:  true,
			queuedBytes:    7,
			reservedStream: stream,
		}}
		stream.conn.releaseBatchReservations(batch)
		stream.conn.mu.Lock()
		defer stream.conn.mu.Unlock()
		if stream.inflightQueued != 0 {
			return fmt.Errorf("stream.inflightQueued = %d, want 0", stream.inflightQueued)
		}
		if stream.queuedDataBytes != 0 {
			return fmt.Errorf("stream.queuedDataBytes = %d, want 0", stream.queuedDataBytes)
		}
		return nil
	case "local_release_batch_reservations_wakes_distinct_streams_crossing_low_watermark":
		stream := newQueueBackpressureTestStream()
		other := testVisibleBidiStream(stream.conn, 8, testWithSendMax(1<<20))
		released := stream.conn.perStreamDataLWMLocked() + 1
		stream.queuedDataBytes = released
		stream.inflightQueued = released
		other.queuedDataBytes = released
		other.inflightQueued = released
		stream.conn.flow.queuedDataBytes = stream.conn.sessionDataLWMLocked() + released*2 + 1
		batch := []writeRequest{
			{
				frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
				done:           make(chan error, 1),
				origin:         writeRequestOriginStream,
				queueReserved:  true,
				queuedBytes:    released,
				reservedStream: stream,
			},
			{
				frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: other.id, Payload: []byte("held")}}),
				done:           make(chan error, 1),
				origin:         writeRequestOriginStream,
				queueReserved:  true,
				queuedBytes:    released,
				reservedStream: other,
			},
		}
		stream.conn.releaseBatchReservations(batch)
		stream.conn.mu.Lock()
		if stream.inflightQueued != 0 {
			stream.conn.mu.Unlock()
			return fmt.Errorf("stream.inflightQueued = %d, want 0", stream.inflightQueued)
		}
		if other.inflightQueued != 0 {
			stream.conn.mu.Unlock()
			return fmt.Errorf("other.inflightQueued = %d, want 0", other.inflightQueued)
		}
		stream.conn.mu.Unlock()
		select {
		case <-stream.writeNotify:
		default:
			return fmt.Errorf("stream writeNotify not signaled")
		}
		select {
		case <-other.writeNotify:
		default:
			return fmt.Errorf("other writeNotify not signaled")
		}
		return nil
	case "local_suppress_write_batch_marks_inflight_queued_for_accepted_requests":
		stream := newQueueBackpressureTestStream()
		req := writeRequest{
			frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
			done:           make(chan error, 1),
			origin:         writeRequestOriginStream,
			queueReserved:  true,
			queuedBytes:    7,
			reservedStream: stream,
		}
		filtered := stream.conn.suppressWriteBatch([]writeRequest{req})
		if len(filtered) != 1 {
			return fmt.Errorf("filtered batch len = %d, want 1", len(filtered))
		}
		stream.conn.mu.Lock()
		defer stream.conn.mu.Unlock()
		if stream.inflightQueued != 7 {
			return fmt.Errorf("stream.inflightQueued = %d, want 7", stream.inflightQueued)
		}
		return nil
	case "local_suppress_write_batch_aggregates_inflight_queued_across_streams":
		stream := newQueueBackpressureTestStream()
		other := testLocalSendStream(stream.conn, 8)
		batch := []writeRequest{
			{
				frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("a")}}),
				done:           make(chan error, 1),
				origin:         writeRequestOriginStream,
				queueReserved:  true,
				queuedBytes:    3,
				reservedStream: stream,
			},
			{
				frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("b")}}),
				done:           make(chan error, 1),
				origin:         writeRequestOriginStream,
				queueReserved:  true,
				queuedBytes:    5,
				reservedStream: stream,
			},
			{
				frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: other.id, Payload: []byte("c")}}),
				done:           make(chan error, 1),
				origin:         writeRequestOriginStream,
				queueReserved:  true,
				queuedBytes:    7,
				reservedStream: other,
			},
		}
		filtered := stream.conn.suppressWriteBatch(batch)
		if len(filtered) != len(batch) {
			return fmt.Errorf("filtered batch len = %d, want %d", len(filtered), len(batch))
		}
		stream.conn.mu.Lock()
		defer stream.conn.mu.Unlock()
		if stream.inflightQueued != 8 {
			return fmt.Errorf("stream.inflightQueued = %d, want 8", stream.inflightQueued)
		}
		if other.inflightQueued != 7 {
			return fmt.Errorf("other.inflightQueued = %d, want 7", other.inflightQueued)
		}
		return nil
	case "local_write_deadline_expires_while_blocked_by_session_queued_data_watermark":
		stream := newQueueBackpressureTestStream()
		stream.conn.flow.queuedDataBytes = stream.conn.sessionDataHWMLocked()
		if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
			return fmt.Errorf("set write deadline: %v", err)
		}
		_, err := stream.Write([]byte("x"))
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return fmt.Errorf("write err = %v, want deadline exceeded", err)
		}
		stream.conn.mu.Lock()
		sendSent := stream.sendSent
		sendSessionUsed := stream.conn.flow.sendSessionUsed
		stream.conn.mu.Unlock()
		if sendSent != 0 {
			return fmt.Errorf("stream.sendSent = %d, want 0 after queue-watermark timeout rollback", sendSent)
		}
		if sendSessionUsed != 0 {
			return fmt.Errorf("conn.flow.sendSessionUsed = %d, want 0 after queue-watermark timeout rollback", sendSessionUsed)
		}
		return nil
	case "local_write_deadline_expires_while_blocked_by_per_stream_queued_data_watermark":
		stream := newQueueBackpressureTestStream()
		stream.queuedDataBytes = stream.conn.perStreamDataHWMLocked()
		if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
			return fmt.Errorf("set write deadline: %v", err)
		}
		_, err := stream.Write([]byte("x"))
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return fmt.Errorf("write err = %v, want deadline exceeded", err)
		}
		stream.conn.mu.Lock()
		sendSent := stream.sendSent
		sendSessionUsed := stream.conn.flow.sendSessionUsed
		stream.conn.mu.Unlock()
		if sendSent != 0 {
			return fmt.Errorf("stream.sendSent = %d, want 0 after per-stream queue-watermark timeout rollback", sendSent)
		}
		if sendSessionUsed != 0 {
			return fmt.Errorf("conn.flow.sendSessionUsed = %d, want 0 after per-stream queue-watermark timeout rollback", sendSessionUsed)
		}
		return nil
	case "local_write_closes_session_when_tracked_memory_cap_would_be_exceeded":
		stream := newQueueBackpressureTestStream()
		stream.conn.flow.recvSessionUsed = 8
		stream.conn.flow.sessionMemoryCap = 8
		_, err := stream.Write([]byte("x"))
		if !IsErrorCode(err, CodeInternal) {
			return fmt.Errorf("write err = %v, want %s", err, CodeInternal)
		}
		if !IsErrorCode(stream.conn.err(), CodeInternal) {
			return fmt.Errorf("stored session err = %v, want %s", stream.conn.err(), CodeInternal)
		}
		return nil
	case "local_prepare_write_burst_batch_caps_single_request_at_per_stream_hwm":
		stream := newQueueBackpressureTestStream()
		payload := make([]byte, int(stream.conn.perStreamDataHWMLocked())*2)
		var parts [1][]byte
		parts[0] = payload
		prepared := stream.prepareWritePartsBurstBatch(parts[:], 0, 0, len(payload), writeChunkStreaming)
		if !prepared.handled {
			return fmt.Errorf("prepareWriteBurstBatch handled = false, want true")
		}
		if prepared.err != nil {
			return fmt.Errorf("prepareWriteBurstBatch err = %v, want nil", prepared.err)
		}
		if prepared.progress == 0 || len(prepared.frames) == 0 {
			return fmt.Errorf("prepareWriteBurstBatch returned progress=%d frames=%d, want >0", prepared.progress, len(prepared.frames))
		}
		if got := saturatingAdd(prepared.start.priority.frameBytes, prepared.queuedBytes); got > stream.conn.perStreamDataHWMLocked() {
			return fmt.Errorf("queued bytes = %d, want <= %d", got, stream.conn.perStreamDataHWMLocked())
		}
		return nil
	case "local_prepare_write_final_burst_batch_caps_single_request_at_per_stream_hwm":
		stream := newQueueBackpressureTestStream()
		payload := make([]byte, int(stream.conn.perStreamDataHWMLocked())*2)
		prepared := stream.prepareWritePartsBurstBatch([][]byte{payload}, 0, 0, len(payload), writeChunkFinal)
		if !prepared.handled {
			return fmt.Errorf("prepareWriteFinalBurstBatch handled = false, want true")
		}
		if prepared.err != nil {
			return fmt.Errorf("prepareWriteFinalBurstBatch err = %v, want nil", prepared.err)
		}
		if prepared.finalState.finalized() {
			return fmt.Errorf("prepareWriteFinalBurstBatch finalized = true, want false for oversized payload")
		}
		if prepared.progress == 0 || len(prepared.frames) == 0 {
			return fmt.Errorf("prepareWriteFinalBurstBatch returned progress=%d frames=%d, want >0", prepared.progress, len(prepared.frames))
		}
		if got := saturatingAdd(prepared.start.priority.frameBytes, prepared.queuedBytes); got > stream.conn.perStreamDataHWMLocked() {
			return fmt.Errorf("queued bytes = %d, want <= %d", got, stream.conn.perStreamDataHWMLocked())
		}
		return nil
	case "local_prepare_write_burst_batch_uses_smaller_session_queue_cap":
		stream := newQueueBackpressureTestStream()
		stream.conn.flow.sessionDataHWM = 64 * 1024
		payload := make([]byte, int(stream.conn.perStreamDataHWMLocked())*2)
		var parts [1][]byte
		parts[0] = payload
		prepared := stream.prepareWritePartsBurstBatch(parts[:], 0, 0, len(payload), writeChunkStreaming)
		if !prepared.handled {
			return fmt.Errorf("prepareWriteBurstBatch handled = false, want true")
		}
		if prepared.err != nil {
			return fmt.Errorf("prepareWriteBurstBatch err = %v, want nil", prepared.err)
		}
		if prepared.progress == 0 || len(prepared.frames) == 0 {
			return fmt.Errorf("prepareWriteBurstBatch returned progress=%d frames=%d, want >0", prepared.progress, len(prepared.frames))
		}
		if got := saturatingAdd(prepared.start.priority.frameBytes, prepared.queuedBytes); got > stream.conn.sessionDataHWMLocked() {
			return fmt.Errorf("queued bytes = %d, want <= session cap %d", got, stream.conn.sessionDataHWMLocked())
		}
		return nil
	case "local_reserve_write_queue_tracks_only_queued_streams":
		stream := newQueueBackpressureTestStream()
		req := writeRequest{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
			done:   make(chan error, 1),
			origin: writeRequestOriginStream,
		}
		stream.conn.mu.Lock()
		reservation := stream.conn.reserveWriteQueueLocked(stream, &req, writeLaneOrdinary)
		stream.conn.mu.Unlock()
		if reservation.blocked() {
			return fmt.Errorf("reserveWriteQueueLocked unexpectedly blocked")
		}
		if reservation.memoryErr != nil {
			return fmt.Errorf("reserveWriteQueueLocked err = %v", reservation.memoryErr)
		}
		if req.reservedStream != stream || !req.queueReserved {
			return fmt.Errorf("request reservation = (%v,%v), want reserved for stream", req.queueReserved, req.reservedStream)
		}
		if got := len(stream.conn.flow.queuedDataStreams); got != 1 {
			return fmt.Errorf("len(queuedDataStreams) = %d, want 1", got)
		}
		if _, ok := stream.conn.flow.queuedDataStreams[stream]; !ok {
			return fmt.Errorf("tracked queued-data stream missing reserved stream")
		}
		return nil
	case "local_release_write_queue_reservation_untracks_drained_stream":
		stream := newQueueBackpressureTestStream()
		req := writeRequest{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
			done:   make(chan error, 1),
			origin: writeRequestOriginStream,
		}
		stream.conn.mu.Lock()
		reservation := stream.conn.reserveWriteQueueLocked(stream, &req, writeLaneOrdinary)
		stream.conn.mu.Unlock()
		if reservation.memoryErr != nil {
			return fmt.Errorf("reserveWriteQueueLocked err = %v", reservation.memoryErr)
		}
		stream.conn.releaseWriteQueueReservation(&req)
		stream.conn.mu.Lock()
		defer stream.conn.mu.Unlock()
		if stream.queuedDataBytes != 0 {
			return fmt.Errorf("stream.queuedDataBytes = %d, want 0", stream.queuedDataBytes)
		}
		if stream.conn.flow.queuedDataStreams != nil {
			if _, ok := stream.conn.flow.queuedDataStreams[stream]; ok {
				return fmt.Errorf("drained stream still tracked in queuedDataStreams")
			}
		}
		return nil
	case "local_clear_write_queue_reservations_locked_uses_tracked_set":
		stream := newQueueBackpressureTestStream()
		other := testLocalSendStream(stream.conn, 8)
		stream.conn.mu.Lock()
		stream.queuedDataBytes = 11
		stream.inflightQueued = 5
		other.queuedDataBytes = 0
		other.inflightQueued = 7
		stream.conn.flow.queuedDataStreams = map[*nativeStream]struct{}{
			stream: {},
		}
		stream.conn.clearWriteQueueReservationsLocked()
		stream.conn.mu.Unlock()
		if stream.queuedDataBytes != 0 {
			return fmt.Errorf("tracked stream queuedDataBytes = %d, want 0", stream.queuedDataBytes)
		}
		if stream.inflightQueued != 0 {
			return fmt.Errorf("tracked stream inflightQueued = %d, want 0", stream.inflightQueued)
		}
		if other.queuedDataBytes != 0 {
			return fmt.Errorf("untracked stream queuedDataBytes = %d, want 0", other.queuedDataBytes)
		}
		if other.inflightQueued != 0 {
			return fmt.Errorf("untracked stream inflightQueued = %d, want 0", other.inflightQueued)
		}
		if stream.conn.flow.queuedDataStreams != nil {
			return fmt.Errorf("queuedDataStreams = %#v, want nil", stream.conn.flow.queuedDataStreams)
		}
		return nil
	case "local_clear_write_queue_reservations_locked_rebuilds_tracked_set_from_seeded_streams":
		stream := newQueueBackpressureTestStream()
		provisional := &nativeStream{
			conn:             stream.conn,
			bidi:             true,
			localOpen:        testLocalOpenOpenedState(),
			localSend:        true,
			localReceive:     true,
			provisionalIndex: 0,
			writeNotify:      make(chan struct{}, 1),
			readNotify:       make(chan struct{}, 1),
		}
		stream.conn.mu.Lock()
		stream.queuedDataBytes = 11
		stream.inflightQueued = 4
		provisional.queuedDataBytes = 7
		provisional.inflightQueued = 6
		stream.conn.flow.queuedDataBytes = 18
		stream.conn.queues.provisionalBidi.items = []*nativeStream{provisional}
		stream.conn.clearWriteQueueReservationsLocked()
		stream.conn.mu.Unlock()
		if stream.queuedDataBytes != 0 {
			return fmt.Errorf("live stream queuedDataBytes = %d, want 0", stream.queuedDataBytes)
		}
		if stream.inflightQueued != 0 {
			return fmt.Errorf("live stream inflightQueued = %d, want 0", stream.inflightQueued)
		}
		if provisional.queuedDataBytes != 0 {
			return fmt.Errorf("provisional queuedDataBytes = %d, want 0", provisional.queuedDataBytes)
		}
		if provisional.inflightQueued != 0 {
			return fmt.Errorf("provisional inflightQueued = %d, want 0", provisional.inflightQueued)
		}
		if stream.conn.flow.queuedDataStreams != nil {
			return fmt.Errorf("queuedDataStreams = %#v, want nil after clear", stream.conn.flow.queuedDataStreams)
		}
		return nil
	case "local_handle_data_frame_open_metadata_retains_open_info_after_payload_mutation":
		c, frames, stop := newHandlerTestConn(e.t)
		defer stop()
		prefix, err := buildOpenMetadataPrefix(
			CapabilityOpenMetadata,
			OpenOptions{OpenInfo: []byte("ssh")},
			c.config.peer.Settings.MaxFramePayload,
		)
		if err != nil {
			return fmt.Errorf("buildOpenMetadataPrefix: %v", err)
		}
		payload := append(append([]byte(nil), prefix...), 'x')
		c.mu.Lock()
		c.config.negotiated.Capabilities = CapabilityOpenMetadata
		c.config.local.Settings.InitialMaxData = 1
		c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 1
		c.flow.recvSessionAdvertised = 1
		c.mu.Unlock()
		streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
		if err := c.handleDataFrame(Frame{
			Type:     FrameTypeDATA,
			Flags:    FrameFlagOpenMetadata,
			StreamID: streamID,
			Payload:  payload,
		}); err != nil {
			return fmt.Errorf("handleDataFrame(DATA|OPEN_METADATA) = %v, want nil", err)
		}
		for i := range payload {
			payload[i] ^= 0xff
		}
		c.mu.Lock()
		stream := c.registry.streams[streamID]
		if stream == nil {
			c.mu.Unlock()
			return fmt.Errorf("stream not opened by OPEN_METADATA DATA")
		}
		if got := string(stream.openInfo); got != "ssh" {
			c.mu.Unlock()
			return fmt.Errorf("stream.openInfo after payload mutation = %q, want %q", got, "ssh")
		}
		if got := string(stream.readBuf); got != "x" {
			c.mu.Unlock()
			return fmt.Errorf("stream.readBuf after payload mutation = %q, want %q", got, "x")
		}
		c.mu.Unlock()
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		return nil
	case "local_check_local_open_possible_with_open_info_limited_by_tracked_memory_cap":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		c.flow.sessionMemoryCap = saturatingAdd(c.retainedStateUnitLocked(), 2)
		err := c.checkLocalOpenPossibleWithOpenInfoLocked(streamArityBidi, 3)
		c.mu.Unlock()
		if !errors.Is(err, ErrOpenLimited) {
			return fmt.Errorf("checkLocalOpenPossibleWithOpenInfoLocked err = %v, want %v", err, ErrOpenLimited)
		}
		return nil
	case "local_enforce_visible_accept_backlog_sheds_newest_when_open_info_budget_exceeded":
		c := newSessionMemoryTestConn()
		oldest := testBuildStream(c, 4, testWithBidi(), testWithEnqueued(1), testWithOpenInfo([]byte("aa")), testWithNotifications())
		newest := testBuildStream(c, 8, testWithBidi(), testWithEnqueued(2), testWithOpenInfo([]byte("bb")), testWithNotifications())
		oldest.initHalfStates()
		newest.initHalfStates()
		c.mu.Lock()
		c.retention.retainedOpenInfoBudget = 3
		c.retention.retainedOpenInfoBytes = uint64(len(oldest.openInfo) + len(newest.openInfo))
		c.queues.acceptBidi.items = []*nativeStream{oldest, newest}
		c.registry.streams[oldest.id] = oldest
		c.registry.streams[newest.id] = newest
		refused := c.enforceVisibleAcceptBacklogLocked()
		gotBytes := c.retention.retainedOpenInfoBytes
		c.mu.Unlock()
		if len(refused) != 1 || refused[0] != newest.id {
			return fmt.Errorf("refused = %#v, want newest stream %d", refused, newest.id)
		}
		if gotBytes != uint64(len(oldest.openInfo)) {
			return fmt.Errorf("retainedOpenInfoBytes = %d, want %d", gotBytes, len(oldest.openInfo))
		}
		return nil
	case "local_visible_accept_backlog_refuses_newest_stream":
		c, frames, stop := newInvalidFrameConn(e.t, 0)
		defer stop()
		c.queues.acceptBacklogLimit = 2
		firstBidi := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
		firstUni := state.FirstPeerStreamID(c.config.negotiated.LocalRole, false)
		secondBidi := firstBidi + 4
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstBidi, Payload: []byte("a")}); err != nil {
			return fmt.Errorf("handle first bidi DATA: %v", err)
		}
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstUni, Payload: []byte("b")}); err != nil {
			return fmt.Errorf("handle first uni DATA: %v", err)
		}
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: secondBidi, Payload: []byte("c")}); err != nil {
			return fmt.Errorf("handle second bidi DATA: %v", err)
		}
		var queued Frame
		select {
		case queued = <-frames:
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("timed out waiting for refused-stream ABORT")
		}
		if queued.Type != FrameTypeABORT || queued.StreamID != secondBidi {
			return fmt.Errorf("queued frame = %+v, want ABORT for stream %d", queued, secondBidi)
		}
		code, _, err := parseErrorPayload(queued.Payload)
		if err != nil {
			return fmt.Errorf("parse queued ABORT payload: %w", err)
		}
		if ErrorCode(code) != CodeRefusedStream {
			return fmt.Errorf("queued ABORT code = %d, want %d", code, CodeRefusedStream)
		}
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		c.mu.Lock()
		if got := c.acceptCountLocked(streamArityBidi); got != 1 {
			c.mu.Unlock()
			return fmt.Errorf("acceptBidi count = %d, want 1", got)
		}
		if got := c.acceptCountLocked(streamArityUni); got != 1 {
			c.mu.Unlock()
			return fmt.Errorf("acceptUni count = %d, want 1", got)
		}
		if head := c.acceptHeadLocked(streamArityBidi); !head.found() || head.stream.id != firstBidi {
			c.mu.Unlock()
			return fmt.Errorf("queued bidi head = %#v, want id %d", head, firstBidi)
		}
		if head := c.acceptHeadLocked(streamArityUni); !head.found() || head.stream.id != firstUni {
			c.mu.Unlock()
			return fmt.Errorf("queued uni head = %#v, want id %d", head, firstUni)
		}
		if _, ok := c.registry.tombstones[secondBidi]; !ok {
			c.mu.Unlock()
			return fmt.Errorf("refused visible stream %d missing tombstone", secondBidi)
		}
		if got := c.flow.recvSessionUsed; got != 2 {
			c.mu.Unlock()
			return fmt.Errorf("recvSessionUsed = %d, want 2 after dropping newest visible stream", got)
		}
		c.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		bidi, err := c.AcceptStream(ctx)
		if err != nil {
			return fmt.Errorf("AcceptStream: %v", err)
		}
		if got := bidi.StreamID(); got != firstBidi {
			return fmt.Errorf("accepted bidi id = %d, want %d", got, firstBidi)
		}
		uni, err := c.AcceptUniStream(ctx)
		if err != nil {
			return fmt.Errorf("AcceptUniStream: %v", err)
		}
		if got := uni.StreamID(); got != firstUni {
			return fmt.Errorf("accepted uni id = %d, want %d", got, firstUni)
		}
		return nil
	case "local_visible_accept_backlog_counts_only_unaccepted_streams":
		c, frames, stop := newInvalidFrameConn(e.t, 0)
		defer stop()
		c.queues.acceptBacklogLimit = 1
		firstID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
		secondID := firstID + 4
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstID, Payload: []byte("a")}); err != nil {
			return fmt.Errorf("handle first DATA: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		first, err := c.AcceptStream(ctx)
		if err != nil {
			return fmt.Errorf("accept first stream: %v", err)
		}
		if got := first.StreamID(); got != firstID {
			return fmt.Errorf("accepted first stream id = %d, want %d", got, firstID)
		}
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: secondID, Payload: []byte("b")}); err != nil {
			return fmt.Errorf("handle second DATA: %v", err)
		}
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		second, err := c.AcceptStream(ctx)
		if err != nil {
			return fmt.Errorf("accept second stream: %v", err)
		}
		if got := second.StreamID(); got != secondID {
			return fmt.Errorf("accepted second stream id = %d, want %d", got, secondID)
		}
		return nil
	case "local_visible_accept_backlog_bytes_refuse_newest_by_visibility_sequence":
		c, frames, stop := newInvalidFrameConn(e.t, 0)
		defer stop()
		c.queues.acceptBacklogLimit = 16
		c.queues.acceptBacklogBytesLimit = 3
		firstID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
		secondID := firstID + 4
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstID, Payload: []byte("a")}); err != nil {
			return fmt.Errorf("handle first DATA: %v", err)
		}
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: secondID, Payload: []byte("b")}); err != nil {
			return fmt.Errorf("handle second DATA: %v", err)
		}
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: firstID, Payload: []byte("xx")}); err != nil {
			return fmt.Errorf("handle additional first-stream DATA: %v", err)
		}
		var queued Frame
		select {
		case queued = <-frames:
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("timed out waiting for refused-stream ABORT")
		}
		if queued.Type != FrameTypeABORT || queued.StreamID != secondID {
			return fmt.Errorf("queued frame = %+v, want ABORT for stream %d", queued, secondID)
		}
		code, _, err := parseErrorPayload(queued.Payload)
		if err != nil {
			return fmt.Errorf("parse queued ABORT payload: %w", err)
		}
		if ErrorCode(code) != CodeRefusedStream {
			return fmt.Errorf("queued ABORT code = %d, want %d", code, CodeRefusedStream)
		}
		select {
		case frame := <-frames:
			return fmt.Errorf("unexpected queued frame: %+v", frame)
		default:
		}
		c.mu.Lock()
		if got := c.acceptCountLocked(streamArityBidi); got != 1 {
			c.mu.Unlock()
			return fmt.Errorf("acceptBidi count = %d, want 1", got)
		}
		if head := c.acceptHeadLocked(streamArityBidi); !head.found() || head.stream.id != firstID {
			c.mu.Unlock()
			return fmt.Errorf("remaining queued bidi head = %#v, want id %d", head, firstID)
		}
		stream := c.registry.streams[firstID]
		if stream == nil {
			c.mu.Unlock()
			return fmt.Errorf("first stream %d missing after bytes-cap enforcement", firstID)
		}
		if got := stream.recvBuffer; got != 3 {
			c.mu.Unlock()
			return fmt.Errorf("first stream recvBuffer = %d, want 3", got)
		}
		if got := c.flow.recvSessionUsed; got != 3 {
			c.mu.Unlock()
			return fmt.Errorf("recvSessionUsed = %d, want 3 after dropping newest queued stream", got)
		}
		if got := c.pendingAcceptedBytesLocked(); got != 3 {
			c.mu.Unlock()
			return fmt.Errorf("pendingAcceptedBytes = %d, want 3", got)
		}
		if _, ok := c.registry.tombstones[secondID]; !ok {
			c.mu.Unlock()
			return fmt.Errorf("refused newest visible stream %d missing tombstone", secondID)
		}
		c.mu.Unlock()
		return nil
	case "local_visible_accept_backlog_bytes_track_growth_and_accept_pop":
		c, _, stop := newInvalidFrameConn(e.t, 0)
		defer stop()
		streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte("abc")}); err != nil {
			return fmt.Errorf("handle first DATA: %v", err)
		}
		c.mu.Lock()
		if got := c.pendingAcceptedBytesLocked(); got != 3 {
			c.mu.Unlock()
			return fmt.Errorf("pendingAcceptedBytes after first DATA = %d, want 3", got)
		}
		c.mu.Unlock()
		if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte("de")}); err != nil {
			return fmt.Errorf("handle second DATA: %v", err)
		}
		c.mu.Lock()
		if got := c.pendingAcceptedBytesLocked(); got != 5 {
			c.mu.Unlock()
			return fmt.Errorf("pendingAcceptedBytes after second DATA = %d, want 5", got)
		}
		c.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		stream, err := c.AcceptStream(ctx)
		if err != nil {
			return fmt.Errorf("AcceptStream: %v", err)
		}
		streamImpl := mustNativeStreamImpl(stream)
		if got := stream.StreamID(); got != streamID {
			return fmt.Errorf("accepted stream id = %d, want %d", got, streamID)
		}
		c.mu.Lock()
		if got := c.pendingAcceptedBytesLocked(); got != 0 {
			c.mu.Unlock()
			return fmt.Errorf("pendingAcceptedBytes after accept = %d, want 0", got)
		}
		if got := streamImpl.recvBuffer; got != 5 {
			c.mu.Unlock()
			return fmt.Errorf("accepted stream recvBuffer = %d, want 5", got)
		}
		c.mu.Unlock()
		return nil
	case "local_enforce_visible_accept_backlog_sheds_newest_under_tracked_memory_cap":
		c := newSessionMemoryTestConn()
		newest := testBuildDetachedStream(c, 8, testWithBidi(), testWithEnqueued(2))
		oldest := testBuildDetachedStream(c, 4, testWithBidi(), testWithEnqueued(1))
		c.mu.Lock()
		c.queues.acceptBidi.items = []*nativeStream{oldest, newest}
		c.flow.sessionMemoryCap = c.retainedStateUnitLocked()
		refused := c.enforceVisibleAcceptBacklogLocked()
		remaining := len(c.queues.acceptBidi.items)
		left := c.queues.acceptBidi.items[0]
		c.mu.Unlock()
		if len(refused) != 1 || refused[0] != newest.id {
			return fmt.Errorf("refused = %#v, want newest stream %d", refused, newest.id)
		}
		if remaining != 1 || left != oldest {
			return fmt.Errorf("remaining accept backlog = %#v, want only oldest stream", c.queues.acceptBidi.items)
		}
		return nil
	case "local_check_local_open_possible_limited_by_tracked_memory_cap":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		c.flow.sessionMemoryCap = c.retainedStateUnitLocked()
		stream := &nativeStream{}
		stream.setProvisionalCreated(time.Now())
		c.queues.provisionalBidi.items = []*nativeStream{stream}
		err := c.checkLocalOpenPossibleLocked(streamArityBidi)
		c.mu.Unlock()
		if !errors.Is(err, ErrOpenLimited) {
			return fmt.Errorf("checkLocalOpenPossibleLocked err = %v, want %v", err, ErrOpenLimited)
		}
		return nil
	case "local_tracked_session_memory_includes_retained_state_residency":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		c.queues.provisionalBidi.items = []*nativeStream{{}}
		c.queues.acceptBidi.items = []*nativeStream{{}}
		c.registry.tombstones = map[uint64]streamTombstone{
			1: {Hidden: true},
			2: {},
		}
		c.registry.tombstoneOrder = []uint64{1, 2}
		c.registry.usedStreamData = map[uint64]usedStreamMarker{
			1: {action: lateDataAbortClosed},
			2: {action: lateDataAbortClosed},
			3: {action: lateDataAbortClosed},
		}
		unit := c.retainedStateUnitLocked()
		compactUnit := c.compactTerminalStateUnitLocked()
		got := c.trackedSessionMemoryLocked()
		c.mu.Unlock()
		want := saturatingAdd(saturatingMul(3, unit), saturatingMul(2, compactUnit))
		if got != want {
			return fmt.Errorf("trackedSessionMemoryLocked() = %d, want %d", got, want)
		}
		return nil
	case "local_enforce_hidden_control_state_budget_sheds_hidden_state_under_tracked_memory_cap":
		c := newSessionMemoryTestConn()
		now := time.Now()
		c.mu.Lock()
		c.flow.sessionMemoryCap = c.retainedStateUnitLocked()
		c.registry.tombstones = map[uint64]streamTombstone{
			1: {Hidden: true, CreatedAt: now},
			2: {Hidden: true, CreatedAt: now},
		}
		c.registry.tombstoneOrder = []uint64{1, 2}
		c.enforceHiddenControlStateBudgetLocked(now)
		retained := c.hiddenControlStateRetainedLocked()
		_, hasNewest := c.registry.tombstones[2]
		c.mu.Unlock()
		if retained != 1 {
			return fmt.Errorf("hiddenControlStateRetainedLocked() = %d, want 1", retained)
		}
		if hasNewest {
			return fmt.Errorf("newest hidden tombstone should have been shed under tracked session memory pressure")
		}
		return nil
	case "local_reap_excess_tombstones_sheds_oldest_visible_tombstone_under_tracked_memory_cap":
		c := newSessionMemoryTestConn()
		c.mu.Lock()
		c.flow.sessionMemoryCap = c.compactTerminalStateUnitLocked()
		c.registry.tombstones = map[uint64]streamTombstone{
			4: {},
			8: {},
		}
		c.registry.tombstoneOrder = []uint64{4, 8}
		c.registry.usedStreamData = map[uint64]usedStreamMarker{
			4: {action: lateDataAbortClosed},
			8: {action: lateDataAbortClosed},
		}
		c.reapExcessTombstonesLocked()
		_, hasOldest := c.registry.tombstones[4]
		_, hasNewest := c.registry.tombstones[8]
		_, keptMarker := c.registry.usedStreamData[4]
		c.mu.Unlock()
		if hasOldest {
			return fmt.Errorf("oldest visible tombstone should have been reaped under tracked session memory pressure")
		}
		if !hasNewest {
			return fmt.Errorf("newest visible tombstone should remain after tracked-memory reap trims back to cap")
		}
		if !keptMarker {
			return fmt.Errorf("reaped visible tombstone should preserve its used-stream marker")
		}
		return nil
	case "repeat_hidden_open_then_ABORT_threshold_plus_one":
		start := state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true)
		for i := 0; i < hiddenAbortChurnThreshold; i++ {
			streamID := start + uint64(i*4)
			if err := e.conn.handleAbortFrame(Frame{
				Type:     FrameTypeABORT,
				StreamID: streamID,
				Payload:  mustEncodeVarint(uint64(CodeCancelled)),
			}); err != nil {
				return err
			}
		}
		return e.conn.handleAbortFrame(Frame{
			Type:     FrameTypeABORT,
			StreamID: start + uint64(hiddenAbortChurnThreshold*4),
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	case "record_hidden_abort_churn_threshold_then_after_window":
		base := time.Unix(1, 0)
		for i := 0; i < hiddenAbortChurnThreshold; i++ {
			if err := e.conn.recordHiddenAbortChurnLocked(base); err != nil {
				return err
			}
		}
		return e.conn.recordHiddenAbortChurnLocked(base.Add(hiddenAbortChurnWindow))
	case "repeat_visible_open_then_ABORT_threshold_plus_one":
		start := state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true)
		for i := 0; i < visibleChurnThreshold; i++ {
			streamID := start + uint64(i*4)
			if err := e.conn.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID}); err != nil {
				return err
			}
			if err := e.conn.handleAbortFrame(Frame{
				Type:     FrameTypeABORT,
				StreamID: streamID,
				Payload:  mustEncodeVarint(uint64(CodeCancelled)),
			}); err != nil {
				return err
			}
		}
		lastID := start + uint64(visibleChurnThreshold*4)
		if err := e.conn.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: lastID}); err != nil {
			return err
		}
		return e.conn.handleAbortFrame(Frame{
			Type:     FrameTypeABORT,
			StreamID: lastID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	case "repeat_visible_uni_open_then_RESET_threshold_plus_one":
		start := state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, false)
		for i := 0; i < visibleChurnThreshold; i++ {
			streamID := start + uint64(i*4)
			if err := e.conn.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: streamID}); err != nil {
				return err
			}
			if err := e.conn.handleResetFrame(Frame{
				Type:     FrameTypeRESET,
				StreamID: streamID,
				Payload:  mustEncodeVarint(uint64(CodeCancelled)),
			}); err != nil {
				return err
			}
		}
		lastID := start + uint64(visibleChurnThreshold*4)
		if err := e.conn.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: lastID}); err != nil {
			return err
		}
		return e.conn.handleResetFrame(Frame{
			Type:     FrameTypeRESET,
			StreamID: lastID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
	case "record_visible_terminal_churn_threshold_then_after_window":
		base := time.Unix(1, 0)
		for i := 0; i < visibleChurnThreshold; i++ {
			if err := e.conn.recordChurnLocked(base, &e.conn.abuse.visibleChurnWindowStart, &e.conn.abuse.visibleChurnCount, visibleChurnWindow, visibleChurnThreshold, "handle RESET", "test visible churn"); err != nil {
				return err
			}
		}
		return e.conn.recordChurnLocked(base.Add(visibleChurnWindow), &e.conn.abuse.visibleChurnWindowStart, &e.conn.abuse.visibleChurnCount, visibleChurnWindow, visibleChurnThreshold, "handle RESET", "test visible churn")
	case "visible_terminal_churn_accepted_stream_not_counted":
		stream := &nativeStream{
			localReceive:       true,
			applicationVisible: true,
		}
		stream.markAccepted()
		stream.initHalfStates()
		stream.setRecvReset(&ApplicationError{Code: uint64(CodeCancelled)})
		if shouldRecordVisibleTerminalChurnLocked(stream) {
			return fmt.Errorf("accepted visible terminal stream unexpectedly counted as churn")
		}
		return nil
	case "visible_terminal_churn_bidi_recv_reset_only_not_counted":
		stream := &nativeStream{
			localSend:          true,
			localReceive:       true,
			applicationVisible: true,
		}
		stream.initHalfStates()
		stream.setRecvReset(&ApplicationError{Code: uint64(CodeCancelled)})
		if shouldRecordVisibleTerminalChurnLocked(stream) {
			return fmt.Errorf("bidi recv-only RESET unexpectedly counted as fully terminal churn")
		}
		return nil
	case "local_drain_pending_control_frames":
		e.conn.mu.Lock()
		urgent, advisory := testDrainPendingControlFrames(e.conn)
		e.conn.mu.Unlock()
		e.capturedFrames = append(e.capturedFrames[:0], urgent...)
		e.capturedAdvisory = append(e.capturedAdvisory[:0], advisory...)
		return nil
	case "local_queue_pending_control_while_closing":
		stream := e.stream
		if stream == nil {
			return fmt.Errorf("missing seeded stream for pending control queue fixture")
		}
		e.conn.mu.Lock()
		e.conn.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(101))
		e.conn.queueStreamMaxDataAsync(stream.id, 202)
		e.conn.queuePendingSessionControlAsync(sessionControlBlocked, 303)
		e.conn.queueStreamBlockedAsync(stream, 404)
		e.conn.queuePriorityUpdateAsync(stream.id, []byte{1, 2, 3}, retainedBytesBorrowed)
		e.conn.mu.Unlock()
		return nil
	case "local_take_pending_priority_update_while_closing":
		stream := e.stream
		if stream == nil {
			return fmt.Errorf("missing seeded stream for pending priority fixture")
		}
		e.conn.mu.Lock()
		frame := e.conn.takePendingPriorityUpdateFrameLocked(stream.id)
		e.conn.mu.Unlock()
		e.capturedFrames = e.capturedFrames[:0]
		if frame.hasFrame() {
			e.capturedFrames = append(e.capturedFrames, testPublicFrame(frame.frame))
		}
		return nil
	case "local_Close_recomputes_final_GOAWAY_after_drain_interval":
		done := make(chan error, 1)
		go func() {
			done <- e.conn.Close()
		}()
		first, err := waitForQueuedFrame(e.frames, "initial GOAWAY")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames[:0], first)
		e.conn.mu.Lock()
		e.conn.registry.nextPeerBidi += 4
		e.conn.registry.nextPeerUni += 4
		e.conn.mu.Unlock()
		second, err := waitForQueuedFrame(e.frames, "recomputed GOAWAY")
		if err != nil {
			return err
		}
		third, err := waitForQueuedFrame(e.frames, "final CLOSE")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames, second, third)
		closeErr, err := waitForErr(done, testSignalTimeout, "Close completion")
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		return nil
	case "local_Close_concurrent_second_call":
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- e.conn.Close()
		}()
		first, err := waitForQueuedFrame(e.frames, "first GOAWAY")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames[:0], first)
		if err := e.conn.Close(); err != nil {
			return err
		}
		second, err := waitForQueuedFrame(e.frames, "replacement GOAWAY")
		if err != nil {
			return err
		}
		third, err := waitForQueuedFrame(e.frames, "final CLOSE")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames, second, third)
		firstErr, err := waitForErr(firstDone, testSignalTimeout, "first Close completion")
		if err != nil {
			return err
		}
		if firstErr != nil {
			return firstErr
		}
		return nil
	case "local_concurrent_GOAWAY_replacement":
		initialBidi := state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true) + 8
		initialUni := state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, false) + 8
		midBidi := initialBidi - 4
		midUni := initialUni - 4
		finalBidi := initialBidi - 8
		finalUni := initialUni - 8

		firstDone := make(chan error, 1)
		go func() {
			firstDone <- e.conn.GoAway(initialBidi, initialUni)
		}()
		first, err := waitForQueuedFrame(e.frames, "first GOAWAY")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames[:0], first)

		secondDone := make(chan error, 1)
		go func() {
			secondDone <- e.conn.GoAway(midBidi, midUni)
		}()
		if err := waitForConnPredicate(e.conn, "intermediate pending GOAWAY", func(c *Conn) bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.sessionControl.goAwaySendActive && c.sessionControl.hasPendingGoAway && c.sessionControl.pendingGoAwayBidi == midBidi && c.sessionControl.pendingGoAwayUni == midUni
		}); err != nil {
			return err
		}
		select {
		case <-e.conn.pending.controlNotify:
		default:
		}

		thirdDone := make(chan error, 1)
		go func() {
			thirdDone <- e.conn.GoAway(finalBidi, finalUni)
		}()
		if err := waitForConnPredicate(e.conn, "most restrictive pending GOAWAY", func(c *Conn) bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.sessionControl.goAwaySendActive && c.sessionControl.hasPendingGoAway && c.sessionControl.pendingGoAwayBidi == finalBidi && c.sessionControl.pendingGoAwayUni == finalUni
		}); err != nil {
			return err
		}

		if e.closeRelease == nil {
			return fmt.Errorf("missing GOAWAY release channel")
		}
		close(e.closeRelease)
		e.closeRelease = nil

		second, err := waitForQueuedFrame(e.frames, "replacement GOAWAY")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames, second)

		for _, pair := range []struct {
			ch   <-chan error
			what string
		}{
			{firstDone, "first GoAway completion"},
			{secondDone, "second GoAway completion"},
			{thirdDone, "third GoAway completion"},
		} {
			goAwayErr, waitErr := waitForErr(pair.ch, testSignalTimeout, pair.what)
			if waitErr != nil {
				return waitErr
			}
			if goAwayErr != nil {
				return goAwayErr
			}
		}
		return nil
	case "local_prior_GOAWAY_then_Close":
		initialBidi := state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true) + 8
		initialUni := state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, false) + 4
		if err := e.conn.GoAway(initialBidi, initialUni); err != nil {
			return err
		}
		first, err := waitForQueuedFrame(e.frames, "initial GOAWAY")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames[:0], first)
		e.conn.mu.Lock()
		e.conn.registry.nextPeerBidi = state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, true) + 4
		e.conn.registry.nextPeerUni = state.FirstPeerStreamID(e.conn.config.negotiated.LocalRole, false) + 4
		e.conn.mu.Unlock()
		if err := e.conn.Close(); err != nil {
			return err
		}
		second, err := waitForQueuedFrame(e.frames, "replacement GOAWAY")
		if err != nil {
			return err
		}
		third, err := waitForQueuedFrame(e.frames, "final CLOSE")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames, second, third)
		return nil
	case "local_graceful_Close_waits_for_active_stream_drain":
		done := make(chan error, 1)
		go func() {
			done <- e.conn.Close()
		}()
		first, err := waitForQueuedFrame(e.frames, "initial GOAWAY")
		if err != nil {
			return err
		}
		second, err := waitForQueuedFrame(e.frames, "final GOAWAY")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames[:0], first, second)
		if frame, ok := drainQueuedFrameNow(e.frames); ok {
			return fmt.Errorf("unexpected queued frame before active stream drained: %+v", frame)
		}
		stream := e.requireStream()
		if stream == nil {
			return fmt.Errorf("missing active stream for graceful drain")
		}
		e.conn.mu.Lock()
		stream.setSendFin()
		stream.setRecvFin()
		e.conn.maybeCompactTerminalLocked(stream)
		e.conn.mu.Unlock()
		notify(e.conn.signals.livenessCh)
		third, err := waitForQueuedFrame(e.frames, "final CLOSE after drain")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames, third)
		closeErr, err := waitForErr(done, testSignalTimeout, "graceful Close completion")
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		return nil
	case "local_Close_reclaims_committed_never_peer_visible_stream":
		if err := e.conn.Close(); err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			frame, err := waitForQueuedFrame(e.frames, fmt.Sprintf("graceful reclaim frame %d", i+1))
			if err != nil {
				return err
			}
			e.capturedFrames = append(e.capturedFrames, frame)
		}
		return nil
	case "local_Close_reclaims_provisional_never_peer_visible_stream":
		if err := e.conn.Close(); err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			frame, err := waitForQueuedFrame(e.frames, fmt.Sprintf("provisional reclaim frame %d", i+1))
			if err != nil {
				return err
			}
			e.capturedFrames = append(e.capturedFrames, frame)
		}
		return nil
	case "local_closeSession_internal_bye_clears_pending_runtime_state":
		e.conn.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})
		frame, err := waitForQueuedFrame(e.frames, "termination CLOSE")
		if err != nil {
			return err
		}
		e.capturedFrames = append(e.capturedFrames[:0], frame)
		return nil
	case "local_closeSession_internal_bye_observe_control_notify":
		closeDone := make(chan error, 1)
		e.closeResult = closeDone
		go func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})
			closeDone <- nil
		}()
		return waitForFixtureSignal(e.closeSignal, "stalled CLOSE emission")
	case "local_closeSession_internal_bye_with_buffered_close_done":
		done := make(chan struct{})
		go func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeInternal), Reason: "bye"})
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-time.After(testSignalTimeout):
			return fmt.Errorf("closeSession waited indefinitely for buffered CLOSE req.done")
		}
	case "local_closeSession_protocol_bye_then_blocked_urgent_queue":
		errCh := make(chan error, 1)
		go func() {
			errCh <- testQueueFrame(e.conn, Frame{Type: FrameTypePING, Payload: make([]byte, 8)})
		}()
		closeDone := make(chan error, 1)
		e.closeResult = closeDone
		go func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
			closeDone <- nil
		}()
		if err := waitForFixtureSignal(e.closeSignal, "stalled CLOSE emission"); err != nil {
			return err
		}
		queueErr, err := waitForFixtureErr(errCh, "blocked urgent queue")
		if err != nil {
			return err
		}
		e.waiterErr = queueErr
		return nil
	case "local_Close_then_OpenStream_while_draining":
		closeDone := make(chan error, 1)
		e.closeResult = closeDone
		go func() {
			closeDone <- e.conn.Close()
		}()
		if err := waitForFixtureSignal(e.closeSignal, "stalled initial GOAWAY"); err != nil {
			return err
		}
		if err := waitForConnPredicate(e.conn, "graceful draining state", func(c *Conn) bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.lifecycle.sessionState == connStateDraining && c.shutdown.gracefulCloseActive
		}); err != nil {
			return err
		}
		_, e.openErr = e.conn.OpenStream(context.Background())
		return nil
	case "local_closeSession_then_Wait_waiter":
		return e.runSessionTerminationWaiter(func() error {
			return e.conn.Wait(context.Background())
		}, func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_closeSession_then_Accept_waiter":
		return e.runSessionTerminationWaiter(func() error {
			_, err := e.conn.AcceptStream(context.Background())
			return err
		}, func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_closeSession_then_AcceptUni_waiter":
		return e.runSessionTerminationWaiter(func() error {
			_, err := e.conn.AcceptUniStream(context.Background())
			return err
		}, func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_closeSession_then_blocked_Read_waiter":
		stream := testOpenedBidiStream(e.conn, state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true))
		e.conn.mu.Lock()
		e.conn.mu.Unlock()
		waiterDone := make(chan error, 1)
		go func() {
			_, err := stream.Read(make([]byte, 1))
			waiterDone <- err
		}()
		if err := waitForStreamReadWaiter(stream, testSignalTimeout); err != nil {
			return err
		}
		return e.finishSessionTerminationWaiter(waiterDone, func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_closeSession_then_blocked_Write_waiter":
		stream := newBlockedWriteCloseWakeStream(e.conn, state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true))
		e.conn.mu.Lock()
		e.conn.registry.streams[stream.id] = stream
		e.conn.flow.sendSessionMax = 1
		e.conn.flow.sendSessionUsed = 1
		e.conn.mu.Unlock()
		return e.runSessionTerminationWaiter(func() error {
			_, err := stream.Write([]byte("x"))
			return err
		}, func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_closeSession_then_blocked_WriteFinal_waiter":
		stream := newBlockedWriteCloseWakeStream(e.conn, state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true))
		e.conn.mu.Lock()
		e.conn.registry.streams[stream.id] = stream
		e.conn.flow.sendSessionMax = 1
		e.conn.flow.sendSessionUsed = 1
		e.conn.mu.Unlock()
		return e.runSessionTerminationWaiter(func() error {
			_, err := stream.WriteFinal([]byte("x"))
			return err
		}, func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_closeSession_then_provisional_commit_waiter":
		e.conn.mu.Lock()
		_ = e.conn.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
		blocked := e.conn.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
		e.conn.mu.Unlock()
		return e.runSessionTerminationWaiter(func() error {
			_, err := blocked.Write([]byte("x"))
			return err
		}, func() {
			e.conn.closeSession(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_Abort_then_Wait_waiter":
		return e.runSessionTerminationWaiter(func() error {
			return e.conn.Wait(context.Background())
		}, func() {
			e.conn.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_Abort_then_Accept_waiter":
		return e.runSessionTerminationWaiter(func() error {
			_, err := e.conn.AcceptStream(context.Background())
			return err
		}, func() {
			e.conn.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_Abort_then_AcceptUni_waiter":
		return e.runSessionTerminationWaiter(func() error {
			_, err := e.conn.AcceptUniStream(context.Background())
			return err
		}, func() {
			e.conn.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_Abort_then_blocked_Read_waiter":
		stream := testOpenedBidiStream(e.conn, state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true))
		e.conn.mu.Lock()
		e.conn.mu.Unlock()
		waiterDone := make(chan error, 1)
		go func() {
			_, err := stream.Read(make([]byte, 1))
			waiterDone <- err
		}()
		if err := waitForStreamReadWaiter(stream, testSignalTimeout); err != nil {
			return err
		}
		return e.finishSessionTerminationWaiter(waiterDone, func() {
			e.conn.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_Abort_then_blocked_Write_waiter":
		stream := newBlockedWriteCloseWakeStream(e.conn, state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true))
		e.conn.mu.Lock()
		e.conn.registry.streams[stream.id] = stream
		e.conn.flow.sendSessionMax = 1
		e.conn.flow.sendSessionUsed = 1
		e.conn.mu.Unlock()
		return e.runSessionTerminationWaiter(func() error {
			_, err := stream.Write([]byte("x"))
			return err
		}, func() {
			e.conn.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_Abort_then_blocked_WriteFinal_waiter":
		stream := newBlockedWriteCloseWakeStream(e.conn, state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true))
		e.conn.mu.Lock()
		e.conn.registry.streams[stream.id] = stream
		e.conn.flow.sendSessionMax = 1
		e.conn.flow.sendSessionUsed = 1
		e.conn.mu.Unlock()
		return e.runSessionTerminationWaiter(func() error {
			_, err := stream.WriteFinal([]byte("x"))
			return err
		}, func() {
			e.conn.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_Abort_then_provisional_commit_waiter":
		e.conn.mu.Lock()
		_ = e.conn.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
		blocked := e.conn.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
		e.conn.mu.Unlock()
		return e.runSessionTerminationWaiter(func() error {
			_, err := blocked.Write([]byte("x"))
			return err
		}, func() {
			e.conn.CloseWithError(&ApplicationError{Code: uint64(CodeProtocol), Reason: "bye"})
		})
	case "local_MAX_DATA":
		stream := e.requireStream()
		if stream == nil || !stream.localReceive {
			return ErrStreamNotReadable
		}
		return nil
	case "peer_first_opening_frame_stream_id_gap":
		return e.conn.handleDataFrame(Frame{
			Type:     FrameTypeDATA,
			StreamID: e.streamID + 4,
			Payload:  []byte("x"),
		})
	case "local_open_allocates_provisional_stream_then_cancels_before_first_frame_commit":
		ctx := context.Background()
		stream, err := e.conn.OpenStream(ctx)
		if err != nil {
			return err
		}
		e.stream = mustNativeStreamImpl(stream)
		e.streamID = state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true)
		return stream.CloseWithError(uint64(CodeCancelled), "")
	case "next_local_open_commits_first_frame":
		ctx := context.Background()
		stream, err := e.conn.OpenStream(ctx)
		if err != nil {
			return err
		}
		e.stream = mustNativeStreamImpl(stream)
		e.streamID = state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true)
		_, err = stream.Write([]byte("x"))
		return err
	case "too_many_provisional_uncommitted_opens_exist_for_one_class":
		ctx := context.Background()
		e.streams = e.streams[:0]
		for i := 0; i < provisionalOpenHardCap; i++ {
			stream, err := e.conn.OpenStream(ctx)
			if err != nil {
				return err
			}
			e.streams = append(e.streams, mustNativeStreamImpl(stream))
		}
		e.stream = e.streams[0]
		_, err := e.conn.OpenStream(ctx)
		return err
	case "oldest_provisional_open_exceeds_age_limit":
		if len(e.streams) == 0 {
			return fmt.Errorf("no provisional streams seeded for age-limit event")
		}
		ctx := context.Background()
		e.conn.mu.Lock()
		e.streams[0].setProvisionalCreated(time.Now().Add(-provisionalOpenMaxAge - time.Second))
		e.conn.mu.Unlock()
		stream, err := e.conn.OpenStream(ctx)
		if err != nil {
			return err
		}
		e.stream = mustNativeStreamImpl(stream)
		return nil
	case "many_stopped_directions_receive_late_tail_data":
		for _, stream := range e.streams {
			if err := e.conn.handleDataFrame(Frame{
				Type:     FrameTypeDATA,
				StreamID: stream.id,
				Payload:  []byte("late"),
			}); err != nil {
				return err
			}
		}
		return nil
	case "aggregate_cap_is_exceeded":
		for i := 0; i < 3; i++ {
			for _, stream := range e.streams {
				if err := e.conn.handleDataFrame(Frame{
					Type:     FrameTypeDATA,
					StreamID: stream.id,
					Payload:  []byte("overflow"),
				}); err != nil {
					return err
				}
			}
		}
		return nil
	default:
		var bidi, uni uint64
		if _, err := fmt.Sscanf(event, "peer_GOAWAY_last_accepted_bidi_%d_uni_%d", &bidi, &uni); err == nil {
			payload, buildErr := buildGoAwayPayload(bidi, uni, uint64(CodeNoError), "")
			if buildErr != nil {
				return buildErr
			}
			return e.conn.handleGoAwayFrame(Frame{
				Type:    FrameTypeGOAWAY,
				Payload: payload,
			})
		}
		return fmt.Errorf("unsupported state fixture event %q", event)
	}
}

func (e *stateFixtureEnv) requireStream() *nativeStream {
	if e.stream != nil {
		return e.stream
	}
	e.conn.mu.Lock()
	defer e.conn.mu.Unlock()
	e.stream = e.conn.registry.streams[e.streamID]
	return e.stream
}

func (e *stateFixtureEnv) ensureLocalOwnedStream() *nativeStream {
	if e.stream != nil {
		return e.stream
	}
	e.conn.mu.Lock()
	defer e.conn.mu.Unlock()
	bidi := e.streamKind == "bidi"
	e.stream = e.conn.newLocalStreamLocked(e.streamID, streamArityFromBidi(bidi), OpenOptions{}, nil)
	e.conn.registry.streams[e.streamID] = e.stream
	return e.stream
}

func (e *stateFixtureEnv) runSessionTerminationWaiter(waiter func() error, terminate func()) error {
	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- waiter()
	}()
	return e.finishSessionTerminationWaiter(waiterDone, terminate)
}

func (e *stateFixtureEnv) finishSessionTerminationWaiter(waiterDone <-chan error, terminate func()) error {
	closeDone := make(chan error, 1)
	e.closeResult = closeDone
	go func() {
		terminate()
		closeDone <- nil
	}()

	if err := waitForFixtureSignal(e.closeSignal, "stalled CLOSE emission"); err != nil {
		return err
	}
	waiterErr, err := waitForFixtureErr(waiterDone, "session waiter")
	if err != nil {
		return err
	}
	e.waiterErr = waiterErr
	return nil
}

func waitForStreamReadWaiter(stream *nativeStream, timeout time.Duration) error {
	if stream == nil {
		return fmt.Errorf("nil stream")
	}
	deadline := time.Now().Add(timeout)
	for {
		if stream.loadReadWaiters() > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for blocked Read waiter")
		}
		runtime.Gosched()
	}
}

func awaitAcceptWaiter(t *testing.T, c *Conn, timeout time.Duration, msg string) {
	t.Helper()
	if c == nil {
		t.Fatal("nil conn")
	}
	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		waiting := c.signals.acceptCh != nil
		c.mu.Unlock()
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		runtime.Gosched()
	}
}

func waitForQueuedFrame(frames <-chan Frame, what string) (Frame, error) {
	select {
	case frame := <-frames:
		return frame, nil
	case <-time.After(testQueuedFrameTimeout):
		return Frame{}, fmt.Errorf("timed out waiting for %s", what)
	}
}

func drainQueuedFrameNow(frames <-chan Frame) (Frame, bool) {
	select {
	case frame := <-frames:
		return frame, true
	default:
		return Frame{}, false
	}
}

func waitForFixtureSignal(ch <-chan struct{}, what string) error {
	if ch == nil {
		return fmt.Errorf("missing fixture signal for %s", what)
	}
	select {
	case <-ch:
		return nil
	case <-time.After(testSignalTimeout):
		return fmt.Errorf("timed out waiting for %s", what)
	}
}

func waitForFixtureErr(ch <-chan error, what string) (error, error) {
	return waitForErr(ch, testSignalTimeout, what)
}

func waitForErr(ch <-chan error, timeout time.Duration, what string) (error, error) {
	select {
	case err := <-ch:
		return err, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timed out waiting for %s", what)
	}
}

func waitForConnPredicate(c *Conn, what string, pred func(*Conn) bool) error {
	deadline := time.Now().Add(testSignalTimeout)
	for time.Now().Before(deadline) {
		if pred(c) {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", what)
}

func assertStateFixtureStep(t *testing.T, env *stateFixtureEnv, step stateFixtureStep, err error) {
	t.Helper()

	switch step.ExpectResult {
	case "":
	case "accepted":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
	case "session_max_data_ignored_without_side_effects":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if env.conn.flow.sendSessionMax != 128 {
			t.Fatalf("event %q sendSessionMax = %d, want 128", step.Event, env.conn.flow.sendSessionMax)
		}
		assertNoQueuedFrame(t, env.frames)
	case "session_blocked_ignored_without_side_effects":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if env.conn.flow.recvSessionAdvertised != 1024 {
			t.Fatalf("event %q recvSessionAdvertised = %d, want 1024", step.Event, env.conn.flow.recvSessionAdvertised)
		}
		if env.conn.flow.recvSessionPending != 10 {
			t.Fatalf("event %q recvSessionPending = %d, want 10", step.Event, env.conn.flow.recvSessionPending)
		}
		assertNoQueuedFrame(t, env.frames)
	case "session_data_ignored_without_side_effects":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if len(env.conn.registry.streams) != 0 {
			t.Fatalf("event %q streams opened after ignored DATA = %d, want 0", step.Event, len(env.conn.registry.streams))
		}
		if env.conn.flow.recvSessionUsed != 13 {
			t.Fatalf("event %q recvSessionUsed = %d, want 13", step.Event, env.conn.flow.recvSessionUsed)
		}
		if env.conn.abuse.controlBudgetFrames != 7 || env.conn.abuse.mixedBudgetFrames != 9 || env.conn.abuse.noopDataCount != 11 {
			t.Fatalf("event %q budgets changed after ignored DATA = (%d,%d,%d), want (7,9,11)",
				step.Event, env.conn.abuse.controlBudgetFrames, env.conn.abuse.mixedBudgetFrames, env.conn.abuse.noopDataCount)
		}
		assertNoQueuedFrame(t, env.frames)
	case "session_stream_control_ignored_without_side_effects":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if len(env.conn.registry.streams) != 0 {
			t.Fatalf("event %q streams changed after ignored control = %d, want 0", step.Event, len(env.conn.registry.streams))
		}
		if env.conn.abuse.controlBudgetFrames != 3 || env.conn.abuse.mixedBudgetFrames != 5 || env.conn.abuse.noopControlCount != 7 {
			t.Fatalf("event %q budgets changed after ignored control = (%d,%d,%d), want (3,5,7)",
				step.Event, env.conn.abuse.controlBudgetFrames, env.conn.abuse.mixedBudgetFrames, env.conn.abuse.noopControlCount)
		}
		assertNoQueuedFrame(t, env.frames)
	case "session_goaway_ignored_without_side_effects":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if env.conn.sessionControl.peerGoAwayBidi != 80 || env.conn.sessionControl.peerGoAwayUni != 99 {
			t.Fatalf("event %q peer GOAWAY watermarks = (%d,%d), want (80,99)", step.Event, env.conn.sessionControl.peerGoAwayBidi, env.conn.sessionControl.peerGoAwayUni)
		}
		if env.conn.sessionControl.peerGoAwayErr == nil || env.conn.sessionControl.peerGoAwayErr.Code != uint64(CodeProtocol) || env.conn.sessionControl.peerGoAwayErr.Reason != "old" {
			t.Fatalf("event %q peer GOAWAY error = %#v, want original payload", step.Event, env.conn.sessionControl.peerGoAwayErr)
		}
		if env.conn.abuse.controlBudgetFrames != 3 || env.conn.abuse.mixedBudgetFrames != 5 || env.conn.abuse.noopControlCount != 7 {
			t.Fatalf("event %q budgets changed after ignored GOAWAY = (%d,%d,%d), want (3,5,7)",
				step.Event, env.conn.abuse.controlBudgetFrames, env.conn.abuse.mixedBudgetFrames, env.conn.abuse.noopControlCount)
		}
		assertNoQueuedFrame(t, env.frames)
	case "session_max_data_increased":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		want := env.conn.config.local.Settings.InitialMaxData + 1
		if env.conn.flow.sendSessionMax != want {
			t.Fatalf("event %q sendSessionMax = %d, want %d", step.Event, env.conn.flow.sendSessionMax, want)
		}
		assertNoQueuedFrame(t, env.frames)
	case "ping_replied_with_matching_pong":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		frame := awaitQueuedFrame(t, env.frames)
		if frame.Type != FrameTypePONG {
			t.Fatalf("event %q queued frame type = %v, want %v", step.Event, frame.Type, FrameTypePONG)
		}
		if !bytes.Equal(frame.Payload, []byte{0, 1, 2, 3, 4, 5, 6, 7}) {
			t.Fatalf("event %q queued PONG payload = %x, want %x", step.Event, frame.Payload, []byte{0, 1, 2, 3, 4, 5, 6, 7})
		}
		assertNoQueuedFrame(t, env.frames)
	case "ping_ignored_while_closing":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		assertNoQueuedFrame(t, env.frames)
	case "pong_clears_outstanding_ping":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if env.conn.liveness.pingOutstanding {
			t.Fatalf("event %q pingOutstanding = true, want false", step.Event)
		}
		if !env.conn.liveness.lastPongAt.After(time.Time{}) {
			t.Fatalf("event %q lastPongAt not updated", step.Event)
		}
		if env.conn.liveness.lastPingRTT <= 0 {
			t.Fatalf("event %q lastPingRTT = %v, want > 0", step.Event, env.conn.liveness.lastPingRTT)
		}
		assertNoQueuedFrame(t, env.frames)
	case "pong_ignored_while_closing":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if !env.conn.liveness.pingOutstanding {
			t.Fatalf("event %q pingOutstanding = false, want true", step.Event)
		}
		if env.conn.liveness.lastPongAt.After(time.Time{}) {
			t.Fatalf("event %q lastPongAt updated unexpectedly", step.Event)
		}
		if env.conn.liveness.lastPingRTT != 0 {
			t.Fatalf("event %q lastPingRTT = %v, want 0", step.Event, env.conn.liveness.lastPingRTT)
		}
		assertNoQueuedFrame(t, env.frames)
	case "malformed_pong_ignored_while_closing":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if env.conn.abuse.controlBudgetFrames != 3 || env.conn.abuse.mixedBudgetFrames != 5 || env.conn.abuse.noopControlCount != 7 {
			t.Fatalf("event %q budgets changed after ignored malformed PONG = (%d,%d,%d), want (3,5,7)",
				step.Event, env.conn.abuse.controlBudgetFrames, env.conn.abuse.mixedBudgetFrames, env.conn.abuse.noopControlCount)
		}
		assertNoQueuedFrame(t, env.frames)
	case "peer_close_terminates_session_without_local_echo":
		if err == nil {
			t.Fatalf("event %q err = nil, want peer close error", step.Event)
		}
		var appErr *ApplicationError
		if !errors.As(err, &appErr) {
			t.Fatalf("event %q err = %v, want ApplicationError", step.Event, err)
		}
		if step.Event == "peer_CLOSE_no_error" {
			if appErr.Code != uint64(CodeNoError) || appErr.Reason != "complete" {
				t.Fatalf("event %q peer close = (%d,%q), want (%d,%q)", step.Event, appErr.Code, appErr.Reason, uint64(CodeNoError), "complete")
			}
		} else {
			if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "protocol" {
				t.Fatalf("event %q peer close = (%d,%q), want (%d,%q)", step.Event, appErr.Code, appErr.Reason, uint64(CodeProtocol), "protocol")
			}
		}
		peerErr := env.conn.PeerCloseError()
		if peerErr == nil || peerErr.Code != appErr.Code || peerErr.Reason != appErr.Reason {
			t.Fatalf("event %q PeerCloseError = %#v, want (%d,%q)", step.Event, peerErr, appErr.Code, appErr.Reason)
		}
		select {
		case <-env.conn.lifecycle.closedCh:
		default:
			t.Fatalf("event %q closedCh not closed after peer CLOSE", step.Event)
		}
		assertNoQueuedFrame(t, env.frames)
	case "close_after_peer_close_noerror_is_noop":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		assertNoQueuedFrame(t, env.frames)
	case "close_after_peer_close_returns_peer_error":
		var appErr *ApplicationError
		if !errors.As(err, &appErr) {
			t.Fatalf("event %q err = %v, want ApplicationError", step.Event, err)
		}
		if appErr.Code != uint64(CodeProtocol) || appErr.Reason != "protocol" {
			t.Fatalf("event %q peer close code=%d reason=%q, want (%d,%q)", step.Event, appErr.Code, appErr.Reason, uint64(CodeProtocol), "protocol")
		}
		assertNoQueuedFrame(t, env.frames)
	case "close_only_sends_close":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		frame := awaitQueuedFrame(t, env.frames)
		if frame.Type != FrameTypeCLOSE {
			t.Fatalf("event %q queued frame type = %v, want %v", step.Event, frame.Type, FrameTypeCLOSE)
		}
		if frame.StreamID != 0 {
			t.Fatalf("event %q close frame stream-id = %d, want 0", step.Event, frame.StreamID)
		}
		assertNoQueuedFrame(t, env.frames)
	case "session_close_signals_control_notify_before_closed_ch":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		select {
		case <-env.conn.pending.controlNotify:
		case <-env.conn.lifecycle.closedCh:
			t.Fatalf("event %q closedCh closed before controlNotify", step.Event)
		case <-time.After(testCloseWakeTimeout):
			t.Fatalf("event %q closeSession did not signal controlNotify before transport close", step.Event)
		}
		releaseFixtureClose(t, env)
	case "session_close_buffered_close_done_finishes":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		select {
		case <-env.conn.lifecycle.closedCh:
		default:
			t.Fatalf("event %q closedCh not closed after buffered CLOSE wait", step.Event)
		}
		if got := len(env.conn.writer.urgentWriteCh); got != 0 {
			t.Fatalf("event %q urgent close queue depth = %d, want 0 after close drain", step.Event, got)
		}
	case "session_blocked_urgent_queue_returns_session_error_before_closed_ch":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if !IsErrorCode(env.waiterErr, CodeProtocol) {
			t.Fatalf("event %q blocked urgent queue err = %v, want %s", step.Event, env.waiterErr, CodeProtocol)
		}
		select {
		case <-env.conn.lifecycle.closedCh:
			t.Fatalf("event %q closedCh closed before blocked urgent queue returned", step.Event)
		default:
		}
		releaseFixtureClose(t, env)
	case "drain_pending_control_allows_draining_state":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 4 {
			t.Fatalf("event %q urgent frame count = %d, want 4", step.Event, len(env.capturedFrames))
		}
		if len(env.capturedAdvisory) != 1 {
			t.Fatalf("event %q advisory frame count = %d, want 1", step.Event, len(env.capturedAdvisory))
		}
		if env.capturedAdvisory[0].Type != FrameTypeEXT || env.capturedAdvisory[0].StreamID != env.stream.id {
			t.Fatalf("event %q advisory frame = %+v, want EXT for stream %d", step.Event, env.capturedAdvisory[0], env.stream.id)
		}
		if testPendingPriorityUpdateCount(env.conn) != 0 || env.conn.pending.priorityBytes != 0 {
			t.Fatalf("event %q pending PRIORITY_UPDATE retained = %d/%d", step.Event, testPendingPriorityUpdateCount(env.conn), env.conn.pending.priorityBytes)
		}
	case "drain_pending_control_drops_nonclose_when_closing":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 0 || len(env.capturedAdvisory) != 0 {
			t.Fatalf("event %q drained urgent=%d advisory=%d, want 0/0", step.Event, len(env.capturedFrames), len(env.capturedAdvisory))
		}
		if env.conn.pending.hasSessionMaxData || testPendingStreamMaxDataCount(env.conn) != 0 {
			t.Fatalf("event %q pending MAX_DATA state retained = %t/%d", step.Event, env.conn.pending.hasSessionMaxData, testPendingStreamMaxDataCount(env.conn))
		}
		if env.conn.pending.hasSessionBlocked || testPendingStreamBlockedCount(env.conn) != 0 {
			t.Fatalf("event %q pending BLOCKED state retained = %t/%d", step.Event, env.conn.pending.hasSessionBlocked, testPendingStreamBlockedCount(env.conn))
		}
		if testPendingPriorityUpdateCount(env.conn) != 0 || env.conn.pending.priorityBytes != 0 {
			t.Fatalf("event %q pending PRIORITY_UPDATE retained = %d/%d", step.Event, testPendingPriorityUpdateCount(env.conn), env.conn.pending.priorityBytes)
		}
		if !env.conn.sessionControl.hasPendingGoAway || len(env.conn.sessionControl.pendingGoAwayPayload) != 2 {
			t.Fatalf("event %q pending GOAWAY state lost = %t/%d", step.Event, env.conn.sessionControl.hasPendingGoAway, len(env.conn.sessionControl.pendingGoAwayPayload))
		}
		if env.conn.pending.controlBytes != uint64(len(env.conn.sessionControl.pendingGoAwayPayload)) {
			t.Fatalf("event %q pendingControlBytes = %d, want %d", step.Event, env.conn.pending.controlBytes, len(env.conn.sessionControl.pendingGoAwayPayload))
		}
	case "queue_pending_control_ignored_when_closing":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if env.conn.pending.hasSessionMaxData || testPendingStreamMaxDataCount(env.conn) != 0 {
			t.Fatalf("event %q pending MAX_DATA state retained = %t/%d", step.Event, env.conn.pending.hasSessionMaxData, testPendingStreamMaxDataCount(env.conn))
		}
		if env.conn.pending.hasSessionBlocked || testPendingStreamBlockedCount(env.conn) != 0 {
			t.Fatalf("event %q pending BLOCKED state retained = %t/%d", step.Event, env.conn.pending.hasSessionBlocked, testPendingStreamBlockedCount(env.conn))
		}
		if testPendingPriorityUpdateCount(env.conn) != 0 || env.conn.pending.priorityBytes != 0 {
			t.Fatalf("event %q pending PRIORITY_UPDATE retained = %d/%d", step.Event, testPendingPriorityUpdateCount(env.conn), env.conn.pending.priorityBytes)
		}
		if !env.conn.sessionControl.hasPendingGoAway || env.conn.pending.controlBytes != uint64(len(env.conn.sessionControl.pendingGoAwayPayload)) {
			t.Fatalf("event %q GOAWAY accounting changed while closing = %t/%d/%d", step.Event, env.conn.sessionControl.hasPendingGoAway, env.conn.pending.controlBytes, len(env.conn.sessionControl.pendingGoAwayPayload))
		}
	case "take_pending_priority_update_drops_when_closing":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 0 {
			t.Fatalf("event %q takePendingPriorityUpdateFrameLocked returned frame %+v while closing", step.Event, env.capturedFrames[0])
		}
		if testHasPendingPriorityUpdate(env.conn, env.stream.id) {
			t.Fatalf("event %q pending PRIORITY_UPDATE was not dropped", step.Event)
		}
		if env.conn.pending.priorityBytes != 0 {
			t.Fatalf("event %q pendingPriorityBytes = %d, want 0", step.Event, env.conn.pending.priorityBytes)
		}
	case "graceful_close_recomputes_final_goaway_after_drain_interval_timeout":
		if !errors.Is(err, ErrGracefulCloseTimeout) {
			t.Fatalf("event %q err = %v, want %v", step.Event, err, ErrGracefulCloseTimeout)
		}
		if len(env.capturedFrames) != 3 {
			t.Fatalf("event %q captured frame count = %d, want 3", step.Event, len(env.capturedFrames))
		}
		initialBidi := maxPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityBidi)
		initialUni := maxPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityUni)
		assertQueuedGoAwayFrame(t, env.capturedFrames[0], initialBidi, initialUni)
		finalBidi := acceptedPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityBidi, env.conn.registry.nextPeerBidi)
		finalUni := acceptedPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityUni, env.conn.registry.nextPeerUni)
		assertQueuedGoAwayFrame(t, env.capturedFrames[1], finalBidi, finalUni)
		if env.capturedFrames[2].Type != FrameTypeCLOSE {
			t.Fatalf("event %q final frame type = %v, want %v", step.Event, env.capturedFrames[2].Type, FrameTypeCLOSE)
		}
		assertNoQueuedFrame(t, env.frames)
	case "concurrent_close_calls_emit_single_close_frame":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 3 {
			t.Fatalf("event %q captured frame count = %d, want 3", step.Event, len(env.capturedFrames))
		}
		if env.capturedFrames[0].Type != FrameTypeGOAWAY || env.capturedFrames[1].Type != FrameTypeGOAWAY || env.capturedFrames[2].Type != FrameTypeCLOSE {
			t.Fatalf("event %q frame types = [%v %v %v], want [GOAWAY GOAWAY CLOSE]",
				step.Event, env.capturedFrames[0].Type, env.capturedFrames[1].Type, env.capturedFrames[2].Type)
		}
		assertNoQueuedFrame(t, env.frames)
	case "goaway_replacement_retains_most_restrictive_pending":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 2 {
			t.Fatalf("event %q captured frame count = %d, want 2", step.Event, len(env.capturedFrames))
		}
		initialBidi := state.FirstPeerStreamID(env.conn.config.negotiated.LocalRole, true) + 8
		initialUni := state.FirstPeerStreamID(env.conn.config.negotiated.LocalRole, false) + 8
		finalBidi := initialBidi - 8
		finalUni := initialUni - 8
		assertQueuedGoAwayFrame(t, env.capturedFrames[0], initialBidi, initialUni)
		assertQueuedGoAwayFrame(t, env.capturedFrames[1], finalBidi, finalUni)
		assertNoQueuedFrame(t, env.frames)
	case "close_after_prior_goaway_sends_more_restrictive_replacement":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 3 {
			t.Fatalf("event %q captured frame count = %d, want 3", step.Event, len(env.capturedFrames))
		}
		initialBidi := state.FirstPeerStreamID(env.conn.config.negotiated.LocalRole, true) + 8
		initialUni := state.FirstPeerStreamID(env.conn.config.negotiated.LocalRole, false) + 4
		assertQueuedGoAwayFrame(t, env.capturedFrames[0], initialBidi, initialUni)
		finalBidi := acceptedPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityBidi, env.conn.registry.nextPeerBidi)
		finalUni := acceptedPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityUni, env.conn.registry.nextPeerUni)
		assertQueuedGoAwayFrame(t, env.capturedFrames[1], finalBidi, finalUni)
		if env.capturedFrames[2].Type != FrameTypeCLOSE {
			t.Fatalf("event %q final frame type = %v, want %v", step.Event, env.capturedFrames[2].Type, FrameTypeCLOSE)
		}
		assertNoQueuedFrame(t, env.frames)
	case "session_max_data_applies":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if env.conn.flow.sendSessionMax != 256 {
			t.Fatalf("event %q sendSessionMax = %d, want 256", step.Event, env.conn.flow.sendSessionMax)
		}
		assertNoQueuedFrame(t, env.frames)
	case "graceful_close_emits_goaway_then_close_timeout":
		if !errors.Is(err, ErrGracefulCloseTimeout) {
			t.Fatalf("event %q err = %v, want %v", step.Event, err, ErrGracefulCloseTimeout)
		}
		assertGracefulCloseSequence(t, env)
	case "graceful_close_blocks_local_open_while_draining":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if !errors.Is(env.openErr, ErrSessionClosed) {
			t.Fatalf("event %q OpenStream while draining = %v, want %v", step.Event, env.openErr, ErrSessionClosed)
		}
		select {
		case <-env.conn.lifecycle.closedCh:
			t.Fatalf("event %q closedCh closed before draining release", step.Event)
		default:
		}
		initialBidi := maxPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityBidi)
		initialUni := maxPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityUni)
		assertQueuedGoAwayFrame(t, awaitQueuedFrame(t, env.frames), initialBidi, initialUni)
		if env.closeRelease != nil {
			close(env.closeRelease)
			env.closeRelease = nil
		}
		finalBidi := acceptedPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityBidi, env.conn.registry.nextPeerBidi)
		finalUni := acceptedPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityUni, env.conn.registry.nextPeerUni)
		assertQueuedGoAwayFrame(t, awaitQueuedFrame(t, env.frames), finalBidi, finalUni)
		frame := awaitQueuedFrame(t, env.frames)
		if frame.Type != FrameTypeCLOSE {
			t.Fatalf("event %q trailing frame type = %v, want %v", step.Event, frame.Type, FrameTypeCLOSE)
		}
		assertNoQueuedFrame(t, env.frames)
		if env.closeResult == nil {
			t.Fatalf("event %q missing close result", step.Event)
		}
		select {
		case closeErr := <-env.closeResult:
			if !errors.Is(closeErr, ErrGracefulCloseTimeout) {
				t.Fatalf("event %q close result = %v, want %v", step.Event, closeErr, ErrGracefulCloseTimeout)
			}
		case <-time.After(testSignalTimeout):
			t.Fatalf("event %q timed out waiting for close result", step.Event)
		}
		env.closeResult = nil
		if env.closeHandlerDone != nil {
			select {
			case <-env.closeHandlerDone:
			case <-time.After(testSignalTimeout):
				t.Fatalf("event %q timed out waiting for stalled close handler", step.Event)
			}
			env.closeHandlerDone = nil
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if env.conn.shutdown.gracefulCloseActive {
			t.Fatalf("event %q gracefulCloseActive still set after Close()", step.Event)
		}
	case "graceful_close_waits_for_active_streams_before_close":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 3 {
			t.Fatalf("event %q captured frame count = %d, want 3", step.Event, len(env.capturedFrames))
		}
		if env.capturedFrames[0].Type != FrameTypeGOAWAY || env.capturedFrames[1].Type != FrameTypeGOAWAY || env.capturedFrames[2].Type != FrameTypeCLOSE {
			t.Fatalf("event %q frame types = [%v %v %v], want [GOAWAY GOAWAY CLOSE]",
				step.Event, env.capturedFrames[0].Type, env.capturedFrames[1].Type, env.capturedFrames[2].Type)
		}
		assertNoQueuedFrame(t, env.frames)
	case "graceful_close_reclaims_committed_never_peer_visible_stream":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 3 {
			t.Fatalf("event %q captured frame count = %d, want 3", step.Event, len(env.capturedFrames))
		}
		if env.capturedFrames[0].Type != FrameTypeGOAWAY || env.capturedFrames[1].Type != FrameTypeGOAWAY || env.capturedFrames[2].Type != FrameTypeCLOSE {
			t.Fatalf("event %q frame types = [%v %v %v], want [GOAWAY GOAWAY CLOSE]",
				step.Event, env.capturedFrames[0].Type, env.capturedFrames[1].Type, env.capturedFrames[2].Type)
		}
		stream := env.requireStream()
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if _, ok := env.conn.registry.streams[stream.id]; ok {
			t.Fatalf("event %q stream %d still present after graceful reclaim", step.Event, stream.id)
		}
		if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeRefusedStream) {
			t.Fatalf("event %q send abort = %#v, want REFUSED_STREAM", step.Event, stream.sendAbort)
		}
		if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeRefusedStream) {
			t.Fatalf("event %q recv abort = %#v, want REFUSED_STREAM", step.Event, stream.recvAbort)
		}
	case "graceful_close_reclaims_provisional_never_peer_visible_stream":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 3 {
			t.Fatalf("event %q captured frame count = %d, want 3", step.Event, len(env.capturedFrames))
		}
		if env.capturedFrames[0].Type != FrameTypeGOAWAY || env.capturedFrames[1].Type != FrameTypeGOAWAY || env.capturedFrames[2].Type != FrameTypeCLOSE {
			t.Fatalf("event %q frame types = [%v %v %v], want [GOAWAY GOAWAY CLOSE]",
				step.Event, env.capturedFrames[0].Type, env.capturedFrames[1].Type, env.capturedFrames[2].Type)
		}
		stream := env.requireStream()
		want := state.FirstLocalStreamID(env.conn.config.negotiated.LocalRole, true)
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if len(env.conn.queues.provisionalBidi.items) != 0 {
			t.Fatalf("event %q provisional bidi count = %d, want 0", step.Event, len(env.conn.queues.provisionalBidi.items))
		}
		if env.conn.registry.nextLocalBidi != want {
			t.Fatalf("event %q nextLocalBidi = %d, want %d", step.Event, env.conn.registry.nextLocalBidi, want)
		}
		if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeRefusedStream) {
			t.Fatalf("event %q send abort = %#v, want REFUSED_STREAM", step.Event, stream.sendAbort)
		}
		if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeRefusedStream) {
			t.Fatalf("event %q recv abort = %#v, want REFUSED_STREAM", step.Event, stream.recvAbort)
		}
	case "termination_clears_streams_and_pending_state":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.capturedFrames) != 1 {
			t.Fatalf("event %q captured frame count = %d, want 1", step.Event, len(env.capturedFrames))
		}
		frame := env.capturedFrames[0]
		if frame.Type != FrameTypeCLOSE {
			t.Fatalf("event %q queued frame type = %v, want %v", step.Event, frame.Type, FrameTypeCLOSE)
		}
		code, reason, parseErr := parseErrorPayload(frame.Payload)
		if parseErr != nil {
			t.Fatalf("event %q parse CLOSE payload: %v", step.Event, parseErr)
		}
		if code != uint64(CodeInternal) || reason != "bye" {
			t.Fatalf("event %q CLOSE payload = (%d,%q), want (%d,%q)", step.Event, code, reason, uint64(CodeInternal), "bye")
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if env.conn.lifecycle.sessionState != connStateFailed {
			t.Fatalf("event %q sessionState = %d, want %d", step.Event, env.conn.lifecycle.sessionState, connStateFailed)
		}
		if len(env.conn.registry.streams) != 0 || len(env.conn.queues.provisionalBidi.items) != 0 || len(env.conn.queues.provisionalUni.items) != 0 || len(env.conn.queues.acceptBidi.items) != 0 || len(env.conn.queues.acceptUni.items) != 0 {
			t.Fatalf("event %q stream queues not cleared", step.Event)
		}
		if env.conn.queues.acceptBidiBytes != 0 || env.conn.queues.acceptUniBytes != 0 {
			t.Fatalf("event %q accept queued bytes = (%d,%d), want (0,0)", step.Event, env.conn.queues.acceptBidiBytes, env.conn.queues.acceptUniBytes)
		}
		if testPendingStreamMaxDataCount(env.conn) != 0 || testPendingStreamBlockedCount(env.conn) != 0 || testPendingPriorityUpdateCount(env.conn) != 0 {
			t.Fatalf("event %q pending per-stream state not cleared", step.Event)
		}
		if env.conn.pending.sessionBlocked != 0 || env.conn.pending.hasSessionBlocked || env.conn.pending.sessionMaxData != 0 || env.conn.pending.hasSessionMaxData {
			t.Fatalf("event %q pending session FC state not cleared", step.Event)
		}
		if env.conn.sessionControl.pendingGoAwayBidi != 0 || env.conn.sessionControl.pendingGoAwayUni != 0 || len(env.conn.sessionControl.pendingGoAwayPayload) != 0 || env.conn.sessionControl.hasPendingGoAway || env.conn.sessionControl.goAwaySendActive {
			t.Fatalf("event %q pending GOAWAY state not cleared", step.Event)
		}
		if env.conn.pending.controlBytes != 0 || env.conn.pending.priorityBytes != 0 {
			t.Fatalf("event %q pending buffered bytes = (%d,%d), want (0,0)", step.Event, env.conn.pending.controlBytes, env.conn.pending.priorityBytes)
		}
		if env.conn.pending.sessionBlockedAt != 0 || env.conn.pending.sessionBlockedSet || env.conn.registry.activePeerBidi != 0 || env.conn.registry.activePeerUni != 0 {
			t.Fatalf("event %q session counters not cleared", step.Event)
		}
		if len(env.conn.writer.scheduler.ActiveGroupRefs) != 0 ||
			len(env.conn.writer.scheduler.State.StreamFinishTag) != 0 ||
			len(env.conn.writer.scheduler.State.StreamLastService) != 0 ||
			len(env.conn.writer.scheduler.State.GroupVirtualTime) != 0 ||
			len(env.conn.writer.scheduler.State.GroupFinishTag) != 0 ||
			len(env.conn.writer.scheduler.State.GroupLastService) != 0 ||
			env.conn.writer.scheduler.State.RootVirtualTime != 0 ||
			env.conn.writer.scheduler.State.ServiceSeq != 0 {
			t.Fatalf("event %q write scheduler state not cleared", step.Event)
		}
		if env.conn.liveness.keepaliveInterval != 0 || env.conn.liveness.keepaliveMaxPingInterval != 0 || !env.conn.liveness.readIdlePingDueAt.IsZero() || !env.conn.liveness.writeIdlePingDueAt.IsZero() || !env.conn.liveness.maxPingDueAt.IsZero() || env.conn.liveness.pingOutstanding || len(env.conn.liveness.pingPayload) != 0 || env.conn.liveness.lastPingRTT != 0 || env.conn.liveness.pingDone != nil {
			t.Fatalf("event %q keepalive state not cleared", step.Event)
		}
		for i, stream := range env.streams {
			if stream.sendSent != 0 || stream.recvBuffer != 0 || len(stream.readBuf) != 0 {
				t.Fatalf("event %q tracked stream %d resources not released = sendSent=%d recvBuffer=%d readBuf=%d",
					step.Event, i, stream.sendSent, stream.recvBuffer, len(stream.readBuf))
			}
		}
		select {
		case <-env.conn.lifecycle.closedCh:
		default:
			t.Fatalf("event %q closedCh not closed by termination", step.Event)
		}
		assertNoQueuedFrame(t, env.frames)
	case "session_wait_returns_after_closed_ch":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		select {
		case <-env.conn.lifecycle.closedCh:
		default:
			t.Fatalf("event %q closedCh not closed before Wait returned", step.Event)
		}
		if !IsErrorCode(env.waiterErr, CodeProtocol) {
			t.Fatalf("event %q Wait err = %v, want %s", step.Event, env.waiterErr, CodeProtocol)
		}
		releaseFixtureClose(t, env)
	case "session_accept_waiter_returns_before_closed_ch":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		assertSessionTerminationWaiterErr(t, env.waiterErr, OperationAccept)
		select {
		case <-env.conn.lifecycle.closedCh:
			t.Fatalf("event %q closedCh closed before accept waiter returned", step.Event)
		default:
		}
		releaseFixtureClose(t, env)
	case "session_read_waiter_returns_before_closed_ch":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		assertSessionTerminationWaiterErr(t, env.waiterErr, OperationRead)
		select {
		case <-env.conn.lifecycle.closedCh:
			t.Fatalf("event %q closedCh closed before Read returned", step.Event)
		default:
		}
		releaseFixtureClose(t, env)
	case "session_write_waiter_returns_before_closed_ch":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		assertSessionTerminationWaiterErr(t, env.waiterErr, OperationWrite)
		select {
		case <-env.conn.lifecycle.closedCh:
			t.Fatalf("event %q closedCh closed before Write waiter returned", step.Event)
		default:
		}
		releaseFixtureClose(t, env)
	case "session_provisional_commit_waiter_returns_before_closed_ch":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		assertSessionTerminationWaiterErr(t, env.waiterErr, OperationWrite)
		select {
		case <-env.conn.lifecycle.closedCh:
			t.Fatalf("event %q closedCh closed before provisional waiter returned", step.Event)
		default:
		}
		releaseFixtureClose(t, env)
	case "no_control_flush":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		assertNoQueuedFrame(t, env.frames)
	case "material_data_clears_zero_length_budget":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		if env.conn.flow.recvSessionUsed != 1 {
			t.Fatalf("event %q recvSessionUsed = %d, want 1", step.Event, env.conn.flow.recvSessionUsed)
		}
		if stream.recvBuffer != 1 {
			t.Fatalf("event %q recvBuffer = %d, want 1", step.Event, stream.recvBuffer)
		}
		assertNoQueuedFrame(t, env.frames)
	case "group_updates_apply_without_churn":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		if !stream.groupExplicit || stream.group != 2 {
			t.Fatalf("event %q stream group = (%v,%d), want (%v,%d)", step.Event, stream.groupExplicit, stream.group, true, uint64(2))
		}
		assertNoQueuedFrame(t, env.frames)
	case "priority_update_applies_while_draining":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		if stream.priority != 9 {
			t.Fatalf("event %q stream priority = %d, want 9", step.Event, stream.priority)
		}
		assertNoQueuedFrame(t, env.frames)
	case "priority_update_ignored_while_closing":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		if stream.priority != 1 {
			t.Fatalf("event %q stream priority = %d, want unchanged 1", step.Event, stream.priority)
		}
		assertNoQueuedFrame(t, env.frames)
	case "malformed_priority_update_ignored_while_closing":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if env.conn.abuse.controlBudgetFrames != 3 || env.conn.abuse.mixedBudgetFrames != 5 || env.conn.abuse.noopControlCount != 7 {
			t.Fatalf("event %q budgets changed after ignored malformed EXT = (%d,%d,%d), want (3,5,7)",
				step.Event, env.conn.abuse.controlBudgetFrames, env.conn.abuse.mixedBudgetFrames, env.conn.abuse.noopControlCount)
		}
		assertNoQueuedFrame(t, env.frames)
	case "queues_opening_data_then_pending_blocked":
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("event %q err = %v, want deadline exceeded", step.Event, err)
		}
		frame := awaitQueuedFrame(t, env.frames)
		if frame.Type != FrameTypeDATA {
			t.Fatalf("event %q opening frame type = %v, want %v", step.Event, frame.Type, FrameTypeDATA)
		}
		if frame.Flags&FrameFlagFIN != 0 {
			t.Fatalf("event %q opening frame unexpectedly carried FIN", step.Event)
		}
		if len(frame.Payload) != 0 {
			t.Fatalf("event %q opening frame payload len = %d, want 0", step.Event, len(frame.Payload))
		}
		assertPendingBlockedFrames(t, env, frame.StreamID)
	case "queues_opening_metadata_data_then_pending_blocked":
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("event %q err = %v, want deadline exceeded", step.Event, err)
		}
		frame := awaitQueuedFrame(t, env.frames)
		assertOpeningDataFrame(t, env, frame, false, true)
		assertPendingBlockedFrames(t, env, frame.StreamID)
	case "queues_opening_fin_only_without_blocked":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		frame := awaitQueuedFrame(t, env.frames)
		if frame.Type != FrameTypeDATA {
			t.Fatalf("event %q opening frame type = %v, want %v", step.Event, frame.Type, FrameTypeDATA)
		}
		if frame.Flags&FrameFlagFIN == 0 {
			t.Fatalf("event %q opening frame missing FIN flag", step.Event)
		}
		if len(frame.Payload) != 0 {
			t.Fatalf("event %q opening frame payload len = %d, want 0", step.Event, len(frame.Payload))
		}
		assertNoQueuedFrame(t, env.frames)
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if env.conn.pending.hasSessionBlocked || testPendingStreamBlockedCount(env.conn) != 0 {
			t.Fatalf("event %q pending blocked state = %t/%d, want false/0", step.Event, env.conn.pending.hasSessionBlocked, testPendingStreamBlockedCount(env.conn))
		}
	case "queues_opening_metadata_fin_only_without_blocked":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		frame := awaitQueuedFrame(t, env.frames)
		assertOpeningDataFrame(t, env, frame, true, true)
		assertNoQueuedFrame(t, env.frames)
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if env.conn.pending.hasSessionBlocked || testPendingStreamBlockedCount(env.conn) != 0 {
			t.Fatalf("event %q pending blocked state = %t/%d, want false/0", step.Event, env.conn.pending.hasSessionBlocked, testPendingStreamBlockedCount(env.conn))
		}
	case "queues_opening_data_then_stop_sending":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		first := awaitQueuedFrame(t, env.frames)
		if first.Type != FrameTypeDATA {
			t.Fatalf("event %q first queued frame type = %v, want %v", step.Event, first.Type, FrameTypeDATA)
		}
		if first.Flags&FrameFlagFIN != 0 {
			t.Fatalf("event %q opening DATA unexpectedly carried FIN", step.Event)
		}
		if len(first.Payload) != 0 {
			t.Fatalf("event %q opening DATA payload len = %d, want 0", step.Event, len(first.Payload))
		}
		second := awaitQueuedFrame(t, env.frames)
		if second.Type != FrameTypeStopSending {
			t.Fatalf("event %q second queued frame type = %v, want %v", step.Event, second.Type, FrameTypeStopSending)
		}
		if second.StreamID != first.StreamID {
			t.Fatalf("event %q STOP_SENDING stream = %d, want %d", step.Event, second.StreamID, first.StreamID)
		}
		code, _, parseErr := parseErrorPayload(second.Payload)
		if parseErr != nil {
			t.Fatalf("event %q decode STOP_SENDING payload err = %v", step.Event, parseErr)
		}
		if code != uint64(CodeCancelled) {
			t.Fatalf("event %q STOP_SENDING code = %d, want %d", step.Event, code, uint64(CodeCancelled))
		}
		assertNoQueuedFrame(t, env.frames)
	case "queues_opening_metadata_data_then_stop_sending":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		first := awaitQueuedFrame(t, env.frames)
		assertOpeningDataFrame(t, env, first, false, true)
		second := awaitQueuedFrame(t, env.frames)
		if second.Type != FrameTypeStopSending {
			t.Fatalf("event %q second queued frame type = %v, want %v", step.Event, second.Type, FrameTypeStopSending)
		}
		if second.StreamID != first.StreamID {
			t.Fatalf("event %q STOP_SENDING stream = %d, want %d", step.Event, second.StreamID, first.StreamID)
		}
		code, _, parseErr := parseErrorPayload(second.Payload)
		if parseErr != nil {
			t.Fatalf("event %q decode STOP_SENDING payload err = %v", step.Event, parseErr)
		}
		if code != uint64(CodeCancelled) {
			t.Fatalf("event %q STOP_SENDING code = %d, want %d", step.Event, code, uint64(CodeCancelled))
		}
		assertNoQueuedFrame(t, env.frames)
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if !stream.localOpen.committed {
			t.Fatalf("event %q sendCommitted = false, want true", step.Event)
		}
		if !stream.isPeerVisibleLocked() {
			t.Fatalf("event %q peerVisible = false, want true", step.Event)
		}
	case "queues_opening_abort":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		frame := awaitQueuedFrame(t, env.frames)
		if frame.Type != FrameTypeABORT {
			t.Fatalf("event %q queued frame type = %v, want %v", step.Event, frame.Type, FrameTypeABORT)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		if frame.StreamID != stream.id {
			t.Fatalf("event %q queued frame stream = %d, want %d", step.Event, frame.StreamID, stream.id)
		}
		code, reason, parseErr := parseErrorPayload(frame.Payload)
		if parseErr != nil {
			t.Fatalf("event %q decode ABORT payload err = %v", step.Event, parseErr)
		}
		if code != uint64(CodeInternal) || reason != "bye" {
			t.Fatalf("event %q queued ABORT payload = (%d,%q), want (%d,%q)", step.Event, code, reason, uint64(CodeInternal), "bye")
		}
		assertNoQueuedFrame(t, env.frames)
		env.conn.mu.Lock()
		defer env.conn.mu.Unlock()
		if !stream.localOpen.committed {
			t.Fatalf("event %q sendCommitted = false, want true", step.Event)
		}
		if !stream.isPeerVisibleLocked() {
			t.Fatalf("event %q peerVisible = false, want true", step.Event)
		}
		if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeInternal) {
			t.Fatalf("event %q sendAbort = %v, want code %d", step.Event, stream.sendAbort, uint64(CodeInternal))
		}
	case "force_flush_session_max_data":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if env.conn.flow.recvSessionPending != 0 {
			t.Fatalf("event %q recvSessionPending = %d, want 0", step.Event, env.conn.flow.recvSessionPending)
		}
		assertQueuedMaxDataFrame(t, env, 0, env.conn.flow.recvSessionAdvertised)
	case "force_flush_stream_max_data":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		if stream.recvPending != 0 {
			t.Fatalf("event %q recvPending = %d, want 0", step.Event, stream.recvPending)
		}
		assertQueuedMaxDataFrame(t, env, stream.id, stream.recvAdvertised)
	case "force_flush_session_only":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		if env.conn.flow.recvSessionPending != 0 {
			t.Fatalf("event %q recvSessionPending = %d, want 0", step.Event, env.conn.flow.recvSessionPending)
		}
		if stream.recvPending != 0 {
			t.Fatalf("event %q recvPending = %d, want 0", step.Event, stream.recvPending)
		}
		wantStreamAdvertised := env.conn.streamWindowTargetLocked(stream)
		if stream.recvAdvertised != wantStreamAdvertised {
			t.Fatalf("event %q recvAdvertised = %d, want %d", step.Event, stream.recvAdvertised, wantStreamAdvertised)
		}
		assertQueuedMaxDataFrame(t, env, 0, env.conn.flow.recvSessionAdvertised)
	case "stream_state_violation", "protocol_violation":
		if !IsErrorCode(err, CodeProtocol) {
			t.Fatalf("event %q err = %v, want %s", step.Event, err, CodeProtocol)
		}
	case "internal_close":
		if !IsErrorCode(err, CodeInternal) {
			t.Fatalf("event %q err = %v, want %s", step.Event, err, CodeInternal)
		}
		if !IsErrorCode(env.conn.err(), CodeInternal) {
			t.Fatalf("event %q stored session err = %v, want %s", step.Event, env.conn.err(), CodeInternal)
		}
	case "sender_must_finish_with_reset_or_fin":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q did not produce a stream", step.Event)
		}
		sendHalf := stream.sendHalfState()
		if sendHalf != state.SendHalfReset && sendHalf != state.SendHalfFin {
			t.Fatalf("event %q did not converge local sender to RESET or FIN", step.Event)
		}
		frame := awaitQueuedFrame(t, env.frames)
		if frame.Type != FrameTypeRESET && (frame.Type != FrameTypeDATA || frame.Flags&FrameFlagFIN == 0) {
			t.Fatalf("event %q queued frame = %+v, want RESET or DATA|FIN", step.Event, frame)
		}
	case "local_invalid":
		if !errors.Is(err, ErrStreamNotReadable) && !errors.Is(err, ErrStreamNotWritable) {
			t.Fatalf("event %q err = %v, want local invalid stream-side error", step.Event, err)
		}
	case "stream_id_not_consumed":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if env.stream == nil {
			t.Fatalf("event %q expected provisional stream", step.Event)
		}
		if got := env.stream.StreamID(); got != 0 {
			t.Fatalf("event %q StreamID = %d, want 0", step.Event, got)
		}
		want := state.FirstLocalStreamID(env.conn.config.negotiated.LocalRole, true)
		if env.conn.registry.nextLocalBidi != want {
			t.Fatalf("event %q nextLocalBidi = %d, want %d", step.Event, env.conn.registry.nextLocalBidi, want)
		}
		select {
		case frame := <-env.frames:
			t.Fatalf("event %q queued unexpected frame %+v", step.Event, frame)
		default:
		}
	case "uses_same_next_stream_id_without_gap":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if env.stream == nil {
			t.Fatalf("event %q expected committed stream", step.Event)
		}
		want := state.FirstLocalStreamID(env.conn.config.negotiated.LocalRole, true)
		if got := env.stream.StreamID(); got != want {
			t.Fatalf("event %q StreamID = %d, want %d", step.Event, got, want)
		}
		frame := awaitQueuedFrame(t, env.frames)
		if frame.StreamID != want {
			t.Fatalf("event %q queued frame stream_id = %d, want %d", step.Event, frame.StreamID, want)
		}
	case "later_open_requests_are_locally_failed_or_delayed_by_policy":
		if !errors.Is(err, ErrOpenLimited) {
			t.Fatalf("event %q err = %v, want %v", step.Event, err, ErrOpenLimited)
		}
		if len(env.streams) != provisionalOpenHardCap {
			t.Fatalf("event %q provisional count = %d, want %d", step.Event, len(env.streams), provisionalOpenHardCap)
		}
	case "it_is_failed_and_released_without_consuming_peer_visible_id":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.streams) == 0 {
			t.Fatalf("event %q expected tracked provisional streams", step.Event)
		}
		oldest := env.streams[0]
		if oldest.sendAbort == nil || oldest.recvAbort == nil {
			t.Fatalf("event %q oldest provisional was not failed", step.Event)
		}
		if got := oldest.StreamID(); got != 0 {
			t.Fatalf("event %q oldest provisional StreamID = %d, want 0", step.Event, got)
		}
		want := state.FirstLocalStreamID(env.conn.config.negotiated.LocalRole, true)
		if env.conn.registry.nextLocalBidi != want {
			t.Fatalf("event %q nextLocalBidi = %d, want %d", step.Event, env.conn.registry.nextLocalBidi, want)
		}
		select {
		case frame := <-env.frames:
			t.Fatalf("event %q queued unexpected frame %+v", step.Event, frame)
		default:
		}
	case "restore_session_budget_only":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		if !stream.localReadStop {
			t.Fatalf("event %q did not mark local read stop", step.Event)
		}
		if stream.recvBuffer != 0 {
			t.Fatalf("event %q recvBuffer = %d, want 0", step.Event, stream.recvBuffer)
		}
		if len(stream.readBuf) != 0 {
			t.Fatalf("event %q readBuf len = %d, want 0", step.Event, len(stream.readBuf))
		}
		wantSessionAdvertised := env.conn.config.local.Settings.InitialMaxData + 3
		if env.conn.flow.recvSessionAdvertised != wantSessionAdvertised {
			t.Fatalf("event %q recvSessionAdvertised = %d, want %d", step.Event, env.conn.flow.recvSessionAdvertised, wantSessionAdvertised)
		}
		wantStreamAdvertised := state.InitialReceiveWindow(env.conn.config.negotiated.LocalRole, env.conn.config.local.Settings, stream.id)
		if stream.recvAdvertised != wantStreamAdvertised {
			t.Fatalf("event %q recvAdvertised = %d, want %d", step.Event, stream.recvAdvertised, wantStreamAdvertised)
		}
	case "restore_session_budget_only_after_terminal_data":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		if !stream.localReadStop && stream.recvReset == nil && stream.recvAbort == nil {
			t.Fatalf("event %q did not remain on a discard-or-terminal receive path", step.Event)
		}
		if stream.recvBuffer != 0 {
			t.Fatalf("event %q recvBuffer = %d, want 0", step.Event, stream.recvBuffer)
		}
		if len(stream.readBuf) != 0 {
			t.Fatalf("event %q readBuf len = %d, want 0", step.Event, len(stream.readBuf))
		}
		if env.conn.flow.recvSessionUsed != 0 {
			t.Fatalf("event %q recvSessionUsed = %d, want 0", step.Event, env.conn.flow.recvSessionUsed)
		}
		wantSessionAdvertised := env.conn.config.local.Settings.InitialMaxData + 1
		if env.conn.flow.recvSessionAdvertised != wantSessionAdvertised {
			t.Fatalf("event %q recvSessionAdvertised = %d, want %d", step.Event, env.conn.flow.recvSessionAdvertised, wantSessionAdvertised)
		}
		wantStreamAdvertised := state.InitialReceiveWindow(env.conn.config.negotiated.LocalRole, env.conn.config.local.Settings, stream.id)
		if stream.recvAdvertised != wantStreamAdvertised {
			t.Fatalf("event %q recvAdvertised = %d, want %d", step.Event, stream.recvAdvertised, wantStreamAdvertised)
		}
	case "per_direction_and_aggregate_late_tail_caps_apply", "additional_late_tail_is_discarded_or_local_policy_escalates":
		if err != nil {
			t.Fatalf("event %q err = %v, want nil", step.Event, err)
		}
		if len(env.streams) == 0 {
			t.Fatalf("event %q expected stopped-direction fixture streams", step.Event)
		}
		for _, stream := range env.streams {
			if !stream.localReadStop {
				t.Fatalf("event %q stream %d localReadStop = false, want true", step.Event, stream.id)
			}
			if stream.recvBuffer != 0 {
				t.Fatalf("event %q stream %d recvBuffer = %d, want 0", step.Event, stream.id, stream.recvBuffer)
			}
			if len(stream.readBuf) != 0 {
				t.Fatalf("event %q stream %d readBuf len = %d, want 0", step.Event, stream.id, len(stream.readBuf))
			}
		}
		if env.conn.flow.recvSessionUsed != 0 {
			t.Fatalf("event %q recvSessionUsed = %d, want 0", step.Event, env.conn.flow.recvSessionUsed)
		}
	default:
		t.Fatalf("unsupported expect_result %q", step.ExpectResult)
	}

	if step.ExpectState != nil {
		if err != nil {
			t.Fatalf("event %q err = %v, want nil with state assertion", step.Event, err)
		}
		stream := env.requireStream()
		if stream == nil {
			if stateFixtureExpectMatchesTombstone(env, *step.ExpectState) {
				return
			}
			t.Fatalf("event %q expected stream state, but stream is nil", step.Event)
		}
		gotSend, gotRecv := streamHalfStateNames(stream)
		if gotSend != step.ExpectState.SendHalf {
			t.Fatalf("event %q send_half = %q, want %q", step.Event, gotSend, step.ExpectState.SendHalf)
		}
		if gotRecv != step.ExpectState.RecvHalf {
			t.Fatalf("event %q recv_half = %q, want %q", step.Event, gotRecv, step.ExpectState.RecvHalf)
		}
	}
}

func assertQueuedMaxDataFrame(t *testing.T, env *stateFixtureEnv, streamID, advertised uint64) {
	t.Helper()

	frame, ok := takeStateFixtureMaxDataFrame(t, env, streamID)
	if !ok {
		t.Fatal("timed out waiting for MAX_DATA frame")
	}
	if frame.Type != FrameTypeMAXDATA {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeMAXDATA)
	}
	if frame.StreamID != streamID {
		t.Fatalf("queued frame stream_id = %d, want %d", frame.StreamID, streamID)
	}
	if string(frame.Payload) != string(mustEncodeVarint(advertised)) {
		t.Fatalf("queued frame payload = %x, want %x", frame.Payload, mustEncodeVarint(advertised))
	}
	assertNoQueuedFrame(t, env.frames)
}

func takeStateFixtureMaxDataFrame(t *testing.T, env *stateFixtureEnv, streamID uint64) (Frame, bool) {
	t.Helper()
	if env == nil {
		return Frame{}, false
	}

	select {
	case frame := <-env.frames:
		return frame, true
	default:
	}

	if env.conn == nil {
		return Frame{}, false
	}

	env.conn.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(env.conn)
	env.conn.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("pending advisory frames = %d, want 0", len(advisory))
	}
	if len(urgent) == 1 && urgent[0].Type == FrameTypeMAXDATA && urgent[0].StreamID == streamID {
		return urgent[0], true
	}
	if len(urgent) != 0 {
		t.Fatalf("pending urgent frames = %+v, want single MAX_DATA on stream %d", urgent, streamID)
	}

	select {
	case frame := <-env.frames:
		return frame, true
	case <-time.After(testSignalTimeout):
		return Frame{}, false
	}
}

func assertOpeningDataFrame(t *testing.T, env *stateFixtureEnv, frame Frame, wantFIN, wantMetadata bool) {
	t.Helper()

	if frame.Type != FrameTypeDATA {
		t.Fatalf("opening frame type = %v, want %v", frame.Type, FrameTypeDATA)
	}
	if wantFIN {
		if frame.Flags&FrameFlagFIN == 0 {
			t.Fatal("opening DATA frame missing FIN")
		}
	} else if frame.Flags&FrameFlagFIN != 0 {
		t.Fatal("opening DATA frame unexpectedly carried FIN")
	}
	if wantMetadata {
		if frame.Flags&FrameFlagOpenMetadata == 0 {
			t.Fatal("opening DATA frame missing OPEN_METADATA")
		}
		if string(frame.Payload) != string(fixtureOpenMetadataPrefix(t, env)) {
			t.Fatalf("opening DATA payload = %x, want %x", frame.Payload, fixtureOpenMetadataPrefix(t, env))
		}
		return
	}
	if len(frame.Payload) != 0 {
		t.Fatalf("opening DATA payload len = %d, want 0", len(frame.Payload))
	}
}

func assertQueuedGoAwayFrame(t *testing.T, frame Frame, wantBidi, wantUni uint64) {
	t.Helper()

	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeGOAWAY)
	}
	parsed, err := parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse GOAWAY payload: %v", err)
	}
	if parsed.LastAcceptedBidi != wantBidi || parsed.LastAcceptedUni != wantUni {
		t.Fatalf("GOAWAY watermarks = (%d,%d), want (%d,%d)",
			parsed.LastAcceptedBidi, parsed.LastAcceptedUni, wantBidi, wantUni)
	}
	if parsed.Code != uint64(CodeNoError) {
		t.Fatalf("GOAWAY code = %d, want %d", parsed.Code, uint64(CodeNoError))
	}
}

func assertGracefulCloseSequence(t *testing.T, env *stateFixtureEnv) {
	t.Helper()

	initialBidi := maxPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityBidi)
	initialUni := maxPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityUni)
	assertQueuedGoAwayFrame(t, awaitQueuedFrame(t, env.frames), initialBidi, initialUni)

	finalBidi := acceptedPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityBidi, env.conn.registry.nextPeerBidi)
	finalUni := acceptedPeerGoAwayWatermark(env.conn.config.negotiated.LocalRole, streamArityUni, env.conn.registry.nextPeerUni)
	assertQueuedGoAwayFrame(t, awaitQueuedFrame(t, env.frames), finalBidi, finalUni)

	frame := awaitQueuedFrame(t, env.frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeCLOSE)
	}
	assertNoQueuedFrame(t, env.frames)

	env.conn.mu.Lock()
	defer env.conn.mu.Unlock()
	if env.conn.shutdown.gracefulCloseActive {
		t.Fatal("gracefulCloseActive still set after graceful close completed")
	}
}

func assertSessionTerminationWaiterErr(t *testing.T, err error, operation Operation) {
	t.Helper()

	var appErr *ApplicationError
	if !errors.As(err, &appErr) || appErr.Code != uint64(CodeProtocol) || appErr.Reason != "bye" {
		t.Fatalf("session waiter err = %v, want ApplicationError(%d, %q)", err, uint64(CodeProtocol), "bye")
	}
	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != operation || se.Source != SourceLocal ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination {
		t.Fatalf("structured error = %+v", *se)
	}
}

func releaseFixtureClose(t *testing.T, env *stateFixtureEnv) {
	t.Helper()

	if env.closeRelease != nil {
		close(env.closeRelease)
		env.closeRelease = nil
	}
	if env.closeResult != nil {
		select {
		case err := <-env.closeResult:
			if err != nil {
				t.Fatalf("close result = %v, want nil", err)
			}
		case <-time.After(testSignalTimeout):
			t.Fatal("timed out waiting for close result")
		}
	}
	if env.closeHandlerDone != nil {
		select {
		case <-env.closeHandlerDone:
		case <-time.After(testSignalTimeout):
			t.Fatal("timed out waiting for stalled close handler")
		}
	}
}

func assertPendingBlockedFrames(t *testing.T, env *stateFixtureEnv, streamID uint64) {
	t.Helper()

	deadline := time.Now().Add(testSignalTimeout)
	for {
		if frame, ok := drainQueuedFrameNow(env.frames); ok {
			t.Fatalf("unexpected queued frame while waiting for pending BLOCKED frames: %+v", frame)
		}

		env.conn.mu.Lock()
		sessionPending := env.conn.pending.sessionBlockedSet
		stream := env.conn.registry.streams[streamID]
		streamPending := stream != nil && stream.inPendingQueueLocked(pendingStreamQueueBlocked)
		env.conn.mu.Unlock()

		if sessionPending && streamPending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for pending BLOCKED frames on stream %d", streamID)
		}
		runtime.Gosched()
	}

	env.conn.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(env.conn)
	env.conn.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("pending advisory frames = %d, want 0", len(advisory))
	}
	if len(urgent) != 2 {
		t.Fatalf("pending urgent frames = %d, want 2", len(urgent))
	}
	if urgent[0].Type != FrameTypeBLOCKED || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("session BLOCKED frame = %+v, want session BLOCKED(0)", urgent[0])
	}
	if urgent[1].Type != FrameTypeBLOCKED || urgent[1].StreamID != streamID || string(urgent[1].Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("stream BLOCKED frame = %+v, want stream BLOCKED(%d,0)", urgent[1], streamID)
	}
}

func stateFixtureExpectMatchesTombstone(env *stateFixtureEnv, expect stateHalfExpect) bool {
	if env == nil || env.conn == nil || env.streamID == 0 {
		return false
	}
	env.conn.mu.Lock()
	defer env.conn.mu.Unlock()
	_, ok := env.conn.registry.tombstones[env.streamID]
	if !ok {
		return false
	}
	return expect.SendHalf == "send_aborted" && expect.RecvHalf == "recv_aborted"
}

func streamHalfStateNames(stream *nativeStream) (sendHalf, recvHalf string) {
	sendHalfState := stream.sendHalfState()
	recvHalfState := stream.recvHalfState()

	switch sendHalfState {
	case state.SendHalfAbsent:
		sendHalf = "absent"
	case state.SendHalfAborted:
		sendHalf = "send_aborted"
	case state.SendHalfReset:
		sendHalf = "send_reset"
	case state.SendHalfFin:
		sendHalf = "send_fin"
	case state.SendHalfStopSeen:
		sendHalf = "send_stop_seen"
	case state.SendHalfOpen:
		sendHalf = "send_open"
	default:
		sendHalf = "send_open"
	}

	switch recvHalfState {
	case state.RecvHalfAbsent:
		recvHalf = "absent"
	case state.RecvHalfAborted:
		recvHalf = "recv_aborted"
	case state.RecvHalfReset:
		recvHalf = "recv_reset"
	case state.RecvHalfStopSent:
		recvHalf = "recv_stop_sent"
	case state.RecvHalfFin:
		recvHalf = "recv_fin"
	case state.RecvHalfOpen:
		recvHalf = "recv_open"
	default:
		recvHalf = "recv_open"
	}
	return sendHalf, recvHalf
}

func (e *stateFixtureEnv) seedZeroWindowLocalStream(opts OpenOptions, prefix []byte) *nativeStream {
	e.conn.mu.Lock()
	defer e.conn.mu.Unlock()

	e.conn.flow.sendSessionMax = 0
	e.conn.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	e.stream = e.conn.newProvisionalLocalStreamLocked(streamArityBidi, opts, prefix)
	e.streamID = state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true)
	return e.stream
}

func (e *stateFixtureEnv) seedConcreteLocalStream(opts OpenOptions, prefix []byte) *nativeStream {
	e.conn.mu.Lock()
	defer e.conn.mu.Unlock()

	streamID := state.FirstLocalStreamID(e.conn.config.negotiated.LocalRole, true)
	e.stream = e.conn.newLocalStreamWithIDLocked(streamID, streamArityBidi, opts, prefix)
	e.conn.registry.streams[e.stream.id] = e.stream
	e.conn.appendUnseenLocalLocked(e.stream)
	e.streamID = streamID
	return e.stream
}

func (e *stateFixtureEnv) fixtureOpenMetadataOptions() (OpenOptions, []byte, error) {
	opts := OpenOptions{OpenInfo: []byte("ssh")}
	prefix, err := buildOpenMetadataPrefix(CapabilityOpenMetadata, opts, e.conn.config.peer.Settings.MaxFramePayload)
	if err != nil {
		return OpenOptions{}, nil, err
	}
	e.conn.mu.Lock()
	e.conn.config.negotiated.Capabilities |= CapabilityOpenMetadata
	e.conn.mu.Unlock()
	return opts, prefix, nil
}

func fixtureOpenMetadataPrefix(t *testing.T, env *stateFixtureEnv) []byte {
	t.Helper()

	opts := OpenOptions{OpenInfo: []byte("ssh")}
	prefix, err := buildOpenMetadataPrefix(CapabilityOpenMetadata, opts, env.conn.config.peer.Settings.MaxFramePayload)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}
	return prefix
}

func TestPendingMaxDataCoalescesLatestValues(t *testing.T) {
	t.Parallel()
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
	}
	testBuildStream(c, 4, testWithLocalReceive())

	c.mu.Lock()
	if !c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(128)) {
		t.Fatal("queuePendingSessionControlAsync rejected session MAX_DATA 128")
	}
	if !c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(256)) {
		t.Fatal("queuePendingSessionControlAsync rejected session MAX_DATA 256")
	}
	c.queueStreamMaxDataAsync(4, 64)
	c.queueStreamMaxDataAsync(4, 96)
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 2 {
		t.Fatalf("drained %d urgent frames, want 2", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(256)) {
		t.Fatalf("session frame = %+v, want MAX_DATA session 256", urgent[0])
	}
	if urgent[1].Type != FrameTypeMAXDATA || urgent[1].StreamID != 4 || string(urgent[1].Payload) != string(mustEncodeVarint(96)) {
		t.Fatalf("stream frame = %+v, want MAX_DATA stream 4 value 96", urgent[1])
	}
}

func TestPendingBlockedCoalescesPerOffset(t *testing.T) {
	t.Parallel()
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
	}
	stream := testBuildStream(c, 4, testWithLocalSend())

	c.mu.Lock()
	c.queuePendingSessionControlAsync(sessionControlBlocked, 128)
	c.queuePendingSessionControlAsync(sessionControlBlocked, 128)
	c.queueStreamBlockedAsync(stream, 64)
	c.queueStreamBlockedAsync(stream, 64)
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 2 {
		t.Fatalf("drained %d urgent frames, want 2", len(urgent))
	}
	if urgent[0].Type != FrameTypeBLOCKED || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(128)) {
		t.Fatalf("session blocked frame = %+v, want BLOCKED session 128", urgent[0])
	}
	if urgent[1].Type != FrameTypeBLOCKED || urgent[1].StreamID != 4 || string(urgent[1].Payload) != string(mustEncodeVarint(64)) {
		t.Fatalf("stream blocked frame = %+v, want BLOCKED stream 4 value 64", urgent[1])
	}

	c.mu.Lock()
	c.queuePendingSessionControlAsync(sessionControlBlocked, 128)
	c.queueStreamBlockedAsync(stream, 64)
	nextUrgent, nextAdvisory := testDrainPendingControlFrames(c)
	if len(nextUrgent) != 0 || len(nextAdvisory) != 0 {
		t.Fatalf("duplicate offsets drained urgent=%d advisory=%d, want 0/0", len(nextUrgent), len(nextAdvisory))
	}
	c.queuePendingSessionControlAsync(sessionControlBlocked, 256)
	c.queueStreamBlockedAsync(stream, 96)
	nextUrgent, nextAdvisory = testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(nextAdvisory) != 0 {
		t.Fatalf("advisory len after offset change = %d, want 0", len(nextAdvisory))
	}
	if len(nextUrgent) != 2 {
		t.Fatalf("drained %d urgent frames after offset change, want 2", len(nextUrgent))
	}
	if string(nextUrgent[0].Payload) != string(mustEncodeVarint(256)) {
		t.Fatalf("session blocked payload = %x, want %x", nextUrgent[0].Payload, mustEncodeVarint(256))
	}
	if string(nextUrgent[1].Payload) != string(mustEncodeVarint(96)) {
		t.Fatalf("stream blocked payload = %x, want %x", nextUrgent[1].Payload, mustEncodeVarint(96))
	}
}

func TestPendingBlockedAllowsZeroOffsetOnce(t *testing.T) {
	t.Parallel()
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
	}
	stream := testBuildStream(c, 4, testWithLocalSend())

	c.mu.Lock()
	c.queuePendingSessionControlAsync(sessionControlBlocked, 0)
	c.queuePendingSessionControlAsync(sessionControlBlocked, 0)
	c.queueStreamBlockedAsync(stream, 0)
	c.queueStreamBlockedAsync(stream, 0)
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 2 {
		t.Fatalf("drained %d urgent frames at zero offset, want 2", len(urgent))
	}
	if urgent[0].Type != FrameTypeBLOCKED || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("session blocked frame = %+v, want BLOCKED session 0", urgent[0])
	}
	if urgent[1].Type != FrameTypeBLOCKED || urgent[1].StreamID != 4 || string(urgent[1].Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("stream blocked frame = %+v, want BLOCKED stream 4 value 0", urgent[1])
	}

	c.mu.Lock()
	c.queuePendingSessionControlAsync(sessionControlBlocked, 0)
	c.queueStreamBlockedAsync(stream, 0)
	nextUrgent, nextAdvisory := testDrainPendingControlFrames(c)
	c.pending.sessionBlockedAt = 0
	c.pending.sessionBlockedSet = false
	stream.blockedAt = 0
	stream.blockedSet = false
	c.queuePendingSessionControlAsync(sessionControlBlocked, 0)
	c.queueStreamBlockedAsync(stream, 0)
	afterResetUrgent, afterResetAdvisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(nextUrgent) != 0 || len(nextAdvisory) != 0 {
		t.Fatalf("duplicate zero offsets drained urgent=%d advisory=%d, want 0/0", len(nextUrgent), len(nextAdvisory))
	}
	if len(afterResetAdvisory) != 0 {
		t.Fatalf("advisory len after zero-offset reset = %d, want 0", len(afterResetAdvisory))
	}
	if len(afterResetUrgent) != 2 {
		t.Fatalf("drained %d urgent frames after zero-offset reset, want 2", len(afterResetUrgent))
	}
}

func TestQueueStreamBlockedAsyncSkipsSendTerminalStream(t *testing.T) {
	t.Parallel()
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := testBuildStream(c, 6, testWithLocalSend())
	stream.setSendFin()

	c.mu.Lock()
	c.queueStreamBlockedAsync(stream, 64)
	c.mu.Unlock()

	if testHasPendingStreamBlocked(c, stream.id) {
		t.Fatal("stream BLOCKED queued for send-terminal stream")
	}
	if stream.blockedSet || stream.blockedAt != 0 {
		t.Fatalf("blocked state = (%t,%d), want cleared", stream.blockedSet, stream.blockedAt)
	}
}

func TestQueueStreamBlockedAsyncSkipsIdlessStream(t *testing.T) {
	t.Parallel()
	c := &Conn{
		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
	}
	stream := &nativeStream{
		conn:      c,
		localSend: true,
	}
	stream.initHalfStates()

	c.mu.Lock()
	c.queueStreamBlockedAsync(stream, 64)
	c.mu.Unlock()

	if testPendingStreamBlockedCount(c) != 0 {
		t.Fatalf("pending stream BLOCKED count = %d, want 0", testPendingStreamBlockedCount(c))
	}
}

func TestReceiveWindowExceededDoesNotWrapAtUint64Boundary(t *testing.T) {
	t.Parallel()

	advertised := ^uint64(0) - 1
	received := advertised
	if !receiveWindowExceeded(received, advertised, 1) {
		t.Fatal("receiveWindowExceeded() = false, want true when window is exhausted")
	}

	received = ^uint64(0) - 2
	if !receiveWindowExceeded(received, advertised, 2) {
		t.Fatal("receiveWindowExceeded() = false, want true instead of overflow wrap")
	}
	if receiveWindowExceeded(received, advertised, 1) {
		t.Fatal("receiveWindowExceeded() = true, want false when exactly one byte remains")
	}
}

func TestAccountBufferedSessionReceiveLockedSaturatesCounters(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.mu.Lock()
	c.flow.recvSessionReceived = ^uint64(0) - 1
	c.flow.recvSessionUsed = ^uint64(0) - 2
	c.accountBufferedSessionReceiveLocked(8)
	gotReceived := c.flow.recvSessionReceived
	gotUsed := c.flow.recvSessionUsed
	c.mu.Unlock()

	if gotReceived != ^uint64(0) {
		t.Fatalf("recvSessionReceived = %d, want saturation to %d", gotReceived, ^uint64(0))
	}
	if gotUsed != ^uint64(0) {
		t.Fatalf("recvSessionUsed = %d, want saturation to %d", gotUsed, ^uint64(0))
	}
}

func TestAccountDiscardedSessionReceiveLockedSaturatesCounter(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.mu.Lock()
	c.flow.recvSessionReceived = ^uint64(0) - 1
	c.accountDiscardedSessionReceiveLocked(8)
	gotReceived := c.flow.recvSessionReceived
	c.mu.Unlock()

	if gotReceived != ^uint64(0) {
		t.Fatalf("recvSessionReceived = %d, want saturation to %d", gotReceived, ^uint64(0))
	}
}

func TestPendingPriorityUpdateDeferredUntilLocalOpenCommit(t *testing.T) {
	c := &Conn{registry: connRegistryState{streams: map[uint64]*nativeStream{}}}
	stream := &nativeStream{
		conn:         c,
		id:           4,
		idSet:        true,
		bidi:         true,
		localOpen:    testLocalOpenOpenedState(),
		localSend:    true,
		localReceive: true,
	}
	stream.initHalfStates()
	c.registry.streams[4] = stream

	testSetPendingPriorityUpdate(c, 4, []byte{0x00, 0x01})
	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 0 {
		t.Fatalf("urgent len = %d, want 0", len(urgent))
	}
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0 for pre-commit local stream", len(advisory))
	}
	if !testHasPendingPriorityUpdate(c, 4) {
		t.Fatalf("pending priority update was dropped before stream open")
	}
}

func TestPendingStreamMaxDataDeferredUntilLocalOpenCommit(t *testing.T) {
	t.Parallel()

	stream := &nativeStream{
		id:           4,
		idSet:        true,
		localOpen:    testLocalOpenOpenedState(),
		localSend:    true,
		localReceive: true,
		bidi:         true,
	}
	stream.initHalfStates()
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, registry: connRegistryState{streams: map[uint64]*nativeStream{4: stream}},
	}

	c.mu.Lock()
	c.queueStreamMaxDataAsync(4, 64)
	urgent, advisory := testDrainPendingControlFrames(c)
	if len(urgent) != 0 || len(advisory) != 0 {
		c.mu.Unlock()
		t.Fatalf("pre-open drain urgent=%d advisory=%d, want 0/0", len(urgent), len(advisory))
	}
	if got, _ := testPendingStreamMaxDataValue(c, 4); got != 64 {
		c.mu.Unlock()
		t.Fatalf("pendingStreamMaxData[4] = %d, want 64 retained until opener commit", got)
	}
	testMarkLocalOpenCommitted(stream)
	urgent, advisory = testDrainPendingControlFrames(c)
	if len(urgent) != 0 || len(advisory) != 0 {
		c.mu.Unlock()
		t.Fatalf("post-commit drain urgent=%d advisory=%d, want 0/0 while awaiting peer visibility", len(urgent), len(advisory))
	}
	testMarkLocalOpenVisible(stream)
	urgent, advisory = testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("post-open drain urgent len = %d, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 4 || string(urgent[0].Payload) != string(mustEncodeVarint(64)) {
		t.Fatalf("stream frame = %+v, want MAX_DATA stream 4 value 64", urgent[0])
	}
}

func TestPendingStreamBlockedDeferredUntilLocalOpenCommit(t *testing.T) {
	t.Parallel()

	stream := &nativeStream{
		id:           4,
		idSet:        true,
		localOpen:    testLocalOpenOpenedState(),
		localSend:    true,
		localReceive: true,
		bidi:         true,
	}
	stream.initHalfStates()
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, registry: connRegistryState{streams: map[uint64]*nativeStream{4: stream}},
	}

	c.mu.Lock()
	c.queueStreamBlockedAsync(stream, 64)
	urgent, advisory := testDrainPendingControlFrames(c)
	if len(urgent) != 0 || len(advisory) != 0 {
		c.mu.Unlock()
		t.Fatalf("pre-open drain urgent=%d advisory=%d, want 0/0", len(urgent), len(advisory))
	}
	if got, _ := testPendingStreamBlockedValue(c, 4); got != 64 {
		c.mu.Unlock()
		t.Fatalf("pendingStreamBlocked[4] = %d, want 64 retained until opener commit", got)
	}
	if !stream.blockedSet || stream.blockedAt != 64 {
		c.mu.Unlock()
		t.Fatalf("blocked state = (%t,%d), want (true,64)", stream.blockedSet, stream.blockedAt)
	}
	testMarkLocalOpenCommitted(stream)
	urgent, advisory = testDrainPendingControlFrames(c)
	if len(urgent) != 0 || len(advisory) != 0 {
		c.mu.Unlock()
		t.Fatalf("post-commit drain urgent=%d advisory=%d, want 0/0 while awaiting peer visibility", len(urgent), len(advisory))
	}
	testMarkLocalOpenVisible(stream)
	urgent, advisory = testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("post-open drain urgent len = %d, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeBLOCKED || urgent[0].StreamID != 4 || string(urgent[0].Payload) != string(mustEncodeVarint(64)) {
		t.Fatalf("stream frame = %+v, want BLOCKED stream 4 value 64", urgent[0])
	}
}

func TestSessionReplenishPacesSmallPendingCredit(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.InitialMaxData = 256
	settings.MaxFramePayload = 16
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: 256,
			recvSessionReceived: 192,
			recvSessionPending:  8,
			recvSessionUsed:     128,
			sessionDataHWM:      128},
	}

	c.mu.Lock()
	c.maybeReplenishSessionLockedWithPolicy(windowReplenishIfNeeded)
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 0 || len(advisory) != 0 {
		t.Fatalf("small pending session replenish drained urgent=%d advisory=%d, want 0/0", len(urgent), len(advisory))
	}

	c.mu.Lock()
	c.flow.recvSessionPending = 16
	c.maybeReplenishSessionLockedWithPolicy(windowReplenishIfNeeded)
	urgent, advisory = testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(272)) {
		t.Fatalf("session frame = %+v, want MAX_DATA session 272", urgent[0])
	}
}

func TestSessionBlockedForcesPendingCreditFlushBelowPacingThreshold(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.InitialMaxData = 256
	settings.MaxFramePayload = 16
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: 256,
			recvSessionReceived: 192,
			recvSessionPending:  8,
			recvSessionUsed:     128,
			sessionDataHWM:      128},
	}

	if err := c.handleBlockedFrame(Frame{Type: FrameTypeBLOCKED, Payload: mustEncodeVarint(0)}); err != nil {
		t.Fatalf("handle BLOCKED: %v", err)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(264)) {
		t.Fatalf("session frame = %+v, want MAX_DATA session 264", urgent[0])
	}
}

func TestStreamReplenishPacesSmallPendingCredit(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.MaxFramePayload = 8
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings, LocalRole: RoleInitiator}}, flow: connFlowState{perStreamDataHWM: 32}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := &nativeStream{
		conn:           c,
		id:             4,
		idSet:          true,
		localReceive:   true,
		recvAdvertised: 64,
		recvReceived:   52,
		recvPending:    4,
		recvBuffer:     32,
	}
	stream.initHalfStates()
	c.registry.streams[stream.id] = stream

	c.mu.Lock()
	c.maybeReplenishStreamLockedWithPolicy(stream, windowReplenishIfNeeded)
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 0 || len(advisory) != 0 {
		t.Fatalf("small pending stream replenish drained urgent=%d advisory=%d, want 0/0", len(urgent), len(advisory))
	}

	c.mu.Lock()
	stream.recvPending = 8
	c.maybeReplenishStreamLockedWithPolicy(stream, windowReplenishIfNeeded)
	urgent, advisory = testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 4 || string(urgent[0].Payload) != string(mustEncodeVarint(72)) {
		t.Fatalf("stream frame = %+v, want MAX_DATA stream 4 value 72", urgent[0])
	}
}

func TestStreamBlockedForcesPendingCreditFlushBelowPacingThreshold(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.MaxFramePayload = 8
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings, LocalRole: RoleInitiator}}, flow: connFlowState{perStreamDataHWM: 32}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := &nativeStream{
		conn:           c,
		id:             4,
		idSet:          true,
		localReceive:   true,
		localSend:      true,
		recvAdvertised: 64,
		recvReceived:   52,
		recvPending:    4,
		recvBuffer:     32,
	}
	stream.initHalfStates()
	c.registry.streams[stream.id] = stream

	if err := c.handleBlockedFrame(Frame{Type: FrameTypeBLOCKED, StreamID: stream.id, Payload: mustEncodeVarint(0)}); err != nil {
		t.Fatalf("handle BLOCKED: %v", err)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 4 || string(urgent[0].Payload) != string(mustEncodeVarint(68)) {
		t.Fatalf("stream frame = %+v, want MAX_DATA stream 4 value 68", urgent[0])
	}
}

func TestSessionStandingGrowthSuppressedWhileReleasedCreditStillReflectsHighUsage(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.InitialMaxData = 256
	settings.MaxFramePayload = 16
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: 256,
			recvSessionReceived: 192,
			recvSessionPending:  16,
			recvSessionUsed:     120,
			sessionDataHWM:      128},
	}

	c.mu.Lock()
	c.replenishSessionLocked(c.sessionWindowTargetLocked())
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(272)) {
		t.Fatalf("session frame = %+v, want MAX_DATA session 272 without standing-growth jump", urgent[0])
	}
}

func TestStreamStandingGrowthSuppressedWhileReleasedCreditStillReflectsHighUsage(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.MaxFramePayload = 8
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings, LocalRole: RoleInitiator}}, flow: connFlowState{perStreamDataHWM: 32}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := &nativeStream{
		conn:           c,
		id:             4,
		idSet:          true,
		localReceive:   true,
		recvAdvertised: 64,
		recvReceived:   52,
		recvPending:    8,
		recvBuffer:     28,
	}
	stream.initHalfStates()
	c.registry.streams[stream.id] = stream

	c.mu.Lock()
	c.replenishStreamLocked(stream, c.streamWindowTargetLocked(stream))
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 4 || string(urgent[0].Payload) != string(mustEncodeVarint(72)) {
		t.Fatalf("stream frame = %+v, want MAX_DATA stream 4 value 72 without standing-growth jump", urgent[0])
	}
}

func TestHandleStreamMaxDataFrameIgnoresClosingUnknownStream(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.mu.Unlock()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleStreamMaxDataFrame(streamID, 64); err != nil {
		t.Fatalf("handleStreamMaxDataFrame while closing: %v", err)
	}
}

func TestHandleStreamBlockedFrameIgnoresClosingUnknownStream(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = ErrSessionClosed
	c.mu.Unlock()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleStreamBlockedFrame(streamID); err != nil {
		t.Fatalf("handleStreamBlockedFrame while closing: %v", err)
	}
}

func TestSessionStandingGrowthSuppressedUnderTrackedMemoryPressure(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.InitialMaxData = 256
	settings.MaxFramePayload = 16
	c := &Conn{

		pending: connPendingControlState{
			controlNotify: make(chan struct{}, 1),
			controlBytes:  352,
		},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings}}, flow: connFlowState{sessionMemoryCap: 512,
			recvSessionAdvertised: 256,
			recvSessionReceived:   192,
			recvSessionPending:    16,
			recvSessionUsed:       32,
			sessionDataHWM:        128},
	}

	c.mu.Lock()
	c.replenishSessionLocked(c.sessionWindowTargetLocked())
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(272)) {
		t.Fatalf("session frame = %+v, want MAX_DATA session 272 under tracked-memory pressure", urgent[0])
	}
}

func TestStreamStandingGrowthSuppressedUnderTrackedMemoryPressure(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.MaxFramePayload = 8
	settings.InitialMaxStreamDataBidiPeerOpened = 64
	streamID := state.FirstPeerStreamID(RoleInitiator, true)
	c := &Conn{

		pending: connPendingControlState{
			controlNotify: make(chan struct{}, 1),
			controlBytes:  352,
		},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings, LocalRole: RoleInitiator}}, flow: connFlowState{sessionMemoryCap: 512,
			recvSessionUsed:  32,
			perStreamDataHWM: 32}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := &nativeStream{
		conn:           c,
		id:             streamID,
		idSet:          true,
		localReceive:   true,
		localSend:      true,
		recvAdvertised: 64,
		recvReceived:   52,
		recvPending:    8,
		recvBuffer:     8,
	}
	stream.initHalfStates()
	c.registry.streams[stream.id] = stream

	c.mu.Lock()
	c.replenishStreamLocked(stream, c.streamWindowTargetLocked(stream))
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != streamID || string(urgent[0].Payload) != string(mustEncodeVarint(72)) {
		t.Fatalf("stream frame = %+v, want MAX_DATA stream %d value 72 under tracked-memory pressure", urgent[0], streamID)
	}
}

func TestOpenMetadataPrefixDoesNotConsumeZeroWindows(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	c.mu.Lock()
	c.config.negotiated.Capabilities = CapabilityOpenMetadata
	c.config.local.Settings.InitialMaxData = 0
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	c.flow.recvSessionAdvertised = 0
	c.mu.Unlock()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagOpenMetadata,
		StreamID: streamID,
		Payload:  prefix,
	}); err != nil {
		t.Fatalf("handleDataFrame(DATA|OPEN_METADATA, zero-window) = %v, want nil", err)
	}

	c.mu.Lock()
	stream := c.registry.streams[streamID]
	if stream == nil {
		c.mu.Unlock()
		t.Fatal("stream not opened by zero-window OPEN_METADATA")
	}
	if got := string(stream.openInfo); got != "ssh" {
		c.mu.Unlock()
		t.Fatalf("stream.openInfo = %q, want %q", got, "ssh")
	}
	if stream.recvReceived != 0 || stream.recvBuffer != 0 {
		c.mu.Unlock()
		t.Fatalf("stream recv accounting = (%d,%d), want 0/0", stream.recvReceived, stream.recvBuffer)
	}
	if c.flow.recvSessionReceived != 0 || c.flow.recvSessionUsed != 0 {
		c.mu.Unlock()
		t.Fatalf("session recv accounting = (%d,%d), want 0/0", c.flow.recvSessionReceived, c.flow.recvSessionUsed)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestOpenMetadataFlowControlChargesOnlyTrailingApplicationBytes(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	payload := append(append([]byte(nil), prefix...), 'x')

	c.mu.Lock()
	c.config.negotiated.Capabilities = CapabilityOpenMetadata
	c.config.local.Settings.InitialMaxData = 1
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 1
	c.flow.recvSessionAdvertised = 1
	c.mu.Unlock()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagOpenMetadata,
		StreamID: streamID,
		Payload:  payload,
	}); err != nil {
		t.Fatalf("handleDataFrame(DATA|OPEN_METADATA with 1 app byte) = %v, want nil", err)
	}

	c.mu.Lock()
	stream := c.registry.streams[streamID]
	if stream == nil {
		c.mu.Unlock()
		t.Fatal("stream not opened by OPEN_METADATA DATA")
	}
	if got := string(stream.openInfo); got != "ssh" {
		c.mu.Unlock()
		t.Fatalf("stream.openInfo = %q, want %q", got, "ssh")
	}
	if stream.recvReceived != 1 || stream.recvBuffer != 1 {
		c.mu.Unlock()
		t.Fatalf("stream recv accounting = (%d,%d), want 1/1", stream.recvReceived, stream.recvBuffer)
	}
	if c.flow.recvSessionReceived != 1 || c.flow.recvSessionUsed != 1 {
		c.mu.Unlock()
		t.Fatalf("session recv accounting = (%d,%d), want 1/1", c.flow.recvSessionReceived, c.flow.recvSessionUsed)
	}
	if string(stream.readBuf) != "x" {
		c.mu.Unlock()
		t.Fatalf("stream.readBuf = %q, want %q", string(stream.readBuf), "x")
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestOpenMetadataFlowControlStillRejectsTrailingAppByteOverStreamWindow(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	payload := append(append([]byte(nil), prefix...), 'x')

	c.mu.Lock()
	c.config.negotiated.Capabilities = CapabilityOpenMetadata
	c.config.local.Settings.InitialMaxData = 1
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	c.flow.recvSessionAdvertised = 1
	c.mu.Unlock()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagOpenMetadata,
		StreamID: streamID,
		Payload:  payload,
	}); err != nil {
		t.Fatalf("handleDataFrame(DATA|OPEN_METADATA over stream window) = %v, want nil queued ABORT", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypeABORT {
		t.Fatalf("queued frame type = %v, want %v", queued.Type, FrameTypeABORT)
	}
	if queued.StreamID != streamID {
		t.Fatalf("queued frame stream = %d, want %d", queued.StreamID, streamID)
	}
	code, _, err := parseErrorPayload(queued.Payload)
	if err != nil {
		t.Fatalf("parse queued ABORT payload: %v", err)
	}
	if ErrorCode(code) != CodeFlowControl {
		t.Fatalf("queued ABORT code = %d, want %d", code, CodeFlowControl)
	}

	c.mu.Lock()
	if stream := c.registry.streams[streamID]; stream != nil {
		c.mu.Unlock()
		t.Fatalf("live stream %d retained after local FLOW_CONTROL abort", streamID)
	}
	if !c.hasTerminalMarkerLocked(streamID) {
		c.mu.Unlock()
		t.Fatalf("terminal marker missing for locally aborted stream %d", streamID)
	}
	if c.flow.recvSessionReceived != 0 || c.flow.recvSessionUsed != 0 {
		c.mu.Unlock()
		t.Fatalf("session recv accounting = (%d,%d), want 0/0", c.flow.recvSessionReceived, c.flow.recvSessionUsed)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestDataFrameExceedingSessionAndStreamWindowPrefersSessionFlowControl(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.config.local.Settings.InitialMaxData = 0
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	c.flow.recvSessionAdvertised = 0
	c.mu.Unlock()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("x"),
	})
	if !IsErrorCode(err, CodeFlowControl) {
		t.Fatalf("handleDataFrame over session+stream windows err = %v, want %s", err, CodeFlowControl)
	}
	assertNoQueuedFrame(t, frames)
}

func TestPendingControlQueuesRebuildFromStreamPendingState(t *testing.T) {
	c := &Conn{registry: connRegistryState{streams: map[uint64]*nativeStream{
		4:  {id: 4, idSet: true, localReceive: true, localSend: true},
		8:  {id: 8, idSet: true, localReceive: true, localSend: true},
		12: {id: 12, idSet: true, localOpen: testLocalOpenOpenedState()},
	}},
	}
	for _, stream := range c.registry.streams {
		stream.conn = c
		stream.initHalfStates()
	}
	testSetPendingStreamMaxData(c, 8, 80)
	testSetPendingStreamMaxData(c, 4, 40)
	testSetPendingStreamBlocked(c, 8, 81)
	testSetPendingStreamBlocked(c, 4, 41)
	testSetPendingPriorityUpdate(c, 12, []byte{0x0c})
	testSetPendingPriorityUpdate(c, 4, []byte{0x04})
	c.pending.streamQueueState(pendingStreamQueueMaxData).init = false
	c.pending.streamQueueState(pendingStreamQueueBlocked).init = false
	c.pending.streamQueueState(pendingStreamQueuePriority).init = false

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(urgent) != 4 {
		t.Fatalf("drained %d urgent frames, want 4", len(urgent))
	}
	if len(advisory) != 1 {
		t.Fatalf("drained %d advisory frames, want 1", len(advisory))
	}
	if advisory[0].StreamID != 4 {
		t.Fatalf("advisory stream id = %d, want 4", advisory[0].StreamID)
	}
	if !testHasPendingPriorityUpdate(c, 12) {
		t.Fatal("pre-commit pending priority update should remain queued")
	}
}

func TestHandleDataFrameOpenMetadataRetainsOpenInfoAfterPayloadMutation(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	payload := append(append([]byte(nil), prefix...), 'x')

	c.mu.Lock()
	c.config.negotiated.Capabilities = CapabilityOpenMetadata
	c.config.local.Settings.InitialMaxData = 1
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 1
	c.flow.recvSessionAdvertised = 1
	c.mu.Unlock()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagOpenMetadata,
		StreamID: streamID,
		Payload:  payload,
	}); err != nil {
		t.Fatalf("handleDataFrame(DATA|OPEN_METADATA) = %v, want nil", err)
	}

	for i := range payload {
		payload[i] ^= 0xff
	}

	c.mu.Lock()
	stream := c.registry.streams[streamID]
	if stream == nil {
		c.mu.Unlock()
		t.Fatal("stream not opened by OPEN_METADATA DATA")
	}
	if got := string(stream.openInfo); got != "ssh" {
		c.mu.Unlock()
		t.Fatalf("stream.openInfo after payload mutation = %q, want %q", got, "ssh")
	}
	if got := string(stream.readBuf); got != "x" {
		c.mu.Unlock()
		t.Fatalf("stream.readBuf after payload mutation = %q, want %q", got, "x")
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestReleaseReceiveClampsAdvertisedWindowsToVarint62(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: MaxVarint62 - 2,
			recvSessionUsed: 10}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := &nativeStream{
		conn:           c,
		id:             4,
		idSet:          true,
		localReceive:   true,
		recvAdvertised: MaxVarint62 - 2,
		recvBuffer:     10,
	}
	stream.initHalfStates()
	c.registry.streams[stream.id] = stream

	c.mu.Lock()
	c.releaseReceiveLocked(stream, 10)
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if got := c.flow.recvSessionAdvertised; got != MaxVarint62 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", got, MaxVarint62)
	}
	if got := stream.recvAdvertised; got != MaxVarint62 {
		t.Fatalf("stream recvAdvertised = %d, want %d", got, MaxVarint62)
	}
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 2 {
		t.Fatalf("urgent len = %d, want 2", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(MaxVarint62)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA %d", urgent[0], MaxVarint62)
	}
	if urgent[1].Type != FrameTypeMAXDATA || urgent[1].StreamID != stream.id || string(urgent[1].Payload) != string(mustEncodeVarint(MaxVarint62)) {
		t.Fatalf("stream frame = %+v, want stream MAX_DATA %d => %d", urgent[1], stream.id, MaxVarint62)
	}
}

func TestReleaseLateDiscardClampsSessionAdvertisedWindowToVarint62(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	c := &Conn{

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{sessionState: connStateReady}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings}}, flow: connFlowState{recvSessionAdvertised: MaxVarint62 - 2},
	}
	stream := &nativeStream{id: 4, idSet: true}

	c.mu.Lock()
	c.releaseLateDiscardLocked(stream, 10, lateDataCauseReset)
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if got := c.flow.recvSessionAdvertised; got != MaxVarint62 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", got, MaxVarint62)
	}
	if got := c.ingress.aggregateLateData; got != 10 {
		t.Fatalf("aggregateLateData = %d, want 10", got)
	}
	if got := stream.lateDataReceived; got != 10 {
		t.Fatalf("stream.lateDataReceived = %d, want 10", got)
	}
	if got := c.ingress.lateDataAfterReset; got != 10 {
		t.Fatalf("lateDataAfterReset = %d, want 10", got)
	}
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("urgent len = %d, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(MaxVarint62)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA %d", urgent[0], MaxVarint62)
	}
}

func TestReleaseReceiveMarksRetryWhenMaxDataCannotQueue(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	c := &Conn{
		pending: connPendingControlState{
			controlNotify:        make(chan struct{}, 1),
			pendingControlBudget: 1,
			controlBytes:         1,
		},
		lifecycle: connLifecycleState{sessionState: connStateReady},
		config: connConfigState{
			local:      Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings},
		},
		flow: connFlowState{
			recvSessionAdvertised: 100,
			recvSessionUsed:       10,
		},
		registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := &nativeStream{
		conn:           c,
		id:             4,
		idSet:          true,
		localReceive:   true,
		recvAdvertised: 100,
		recvBuffer:     10,
	}
	stream.initHalfStates()
	c.registry.streams[stream.id] = stream

	c.mu.Lock()
	c.releaseReceiveLocked(stream, 10)
	c.mu.Unlock()

	if got := c.flow.recvSessionAdvertised; got != 100 {
		t.Fatalf("recvSessionAdvertised = %d, want unchanged 100 when MAX_DATA queueing is blocked", got)
	}
	if got := c.flow.recvSessionPending; got != 10 {
		t.Fatalf("recvSessionPending = %d, want 10 when MAX_DATA queueing is blocked", got)
	}
	if got := stream.recvAdvertised; got != 100 {
		t.Fatalf("stream recvAdvertised = %d, want unchanged 100 when MAX_DATA queueing is blocked", got)
	}
	if got := stream.recvPending; got != 10 {
		t.Fatalf("stream recvPending = %d, want 10 when MAX_DATA queueing is blocked", got)
	}
	if !c.flow.recvReplenishRetry {
		t.Fatal("recvReplenishRetry = false, want retry marker when release MAX_DATA queueing is blocked")
	}
}

func TestReleaseLateDiscardMarksRetryWhenMaxDataCannotQueue(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	c := &Conn{
		pending: connPendingControlState{
			controlNotify:        make(chan struct{}, 1),
			pendingControlBudget: 1,
			controlBytes:         1,
		},
		lifecycle: connLifecycleState{sessionState: connStateReady},
		config: connConfigState{
			local:      Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{PeerSettings: settings},
		},
		flow: connFlowState{recvSessionAdvertised: 100},
	}
	stream := &nativeStream{id: 4, idSet: true}

	c.mu.Lock()
	c.releaseLateDiscardLocked(stream, 10, lateDataCauseReset)
	c.mu.Unlock()

	if got := c.flow.recvSessionAdvertised; got != 100 {
		t.Fatalf("recvSessionAdvertised = %d, want unchanged 100 when late-discard MAX_DATA queueing is blocked", got)
	}
	if got := c.flow.recvSessionPending; got != 10 {
		t.Fatalf("recvSessionPending = %d, want 10 when late-discard MAX_DATA queueing is blocked", got)
	}
	if !c.flow.recvReplenishRetry {
		t.Fatal("recvReplenishRetry = false, want retry marker when late-discard MAX_DATA queueing is blocked")
	}
	if got := c.ingress.aggregateLateData; got != 10 {
		t.Fatalf("aggregateLateData = %d, want 10", got)
	}
	if got := stream.lateDataReceived; got != 10 {
		t.Fatalf("stream.lateDataReceived = %d, want 10", got)
	}
}

func TestReleaseReceiveZeroWindowWakesWriteWaitersWhenMemoryPressureDrops(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()
	stream := &nativeStream{
		conn:               c,
		id:                 8,
		idSet:              true,
		localReceive:       true,
		applicationVisible: true,
		recvAdvertised:     0,
		recvBuffer:         64,
		writeNotify:        make(chan struct{}, 1),
	}

	c.mu.Lock()
	wake := c.currentWriteWakeLocked()
	c.registry.streams[stream.id] = stream
	threshold := c.sessionMemoryHighThresholdLocked()
	c.flow.recvSessionUsed = threshold
	c.releaseReceiveLocked(stream, 64)
	c.mu.Unlock()

	select {
	case <-wake:
	default:
		t.Fatal("expected zero-window releaseReceiveLocked to wake blocked writers when session memory pressure drops")
	}
}

func TestReleaseReceiveNilStreamWakesWriteWaitersWhenMemoryPressureDrops(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.flow.sessionMemoryCap = 16
	c.flow.recvSessionUsed = c.sessionMemoryHighThresholdLocked()
	wake := c.currentWriteWakeLocked()
	c.releaseReceiveLocked(nil, 2)
	c.mu.Unlock()

	select {
	case <-wake:
	default:
		t.Fatal("expected nil-stream releaseReceiveLocked to wake blocked writers when session memory pressure drops")
	}
}

func TestStandingTargetsFollowRepositoryDefaults(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	if got := c.sessionWindowTargetLocked(); got != 16*1024*1024 {
		t.Fatalf("session target = %d, want %d", got, 16*1024*1024)
	}
	if got := c.streamWindowTargetLocked(stream); got != 512*1024 {
		t.Fatalf("stream target = %d, want %d", got, 512*1024)
	}
}

func TestReadConsumptionBatchesReplenishmentUntilThreshold(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	sessionTarget := c.sessionWindowTargetLocked()
	streamTarget := c.streamWindowTargetLocked(stream)

	c.flow.recvSessionAdvertised = sessionTarget
	c.flow.recvSessionReceived = sessionTarget / 2
	c.flow.recvSessionUsed = 10
	stream.recvAdvertised = streamTarget
	stream.recvReceived = streamTarget / 2
	stream.recvBuffer = 10

	c.consumeReceiveLocked(stream, 10)

	if c.flow.recvSessionAdvertised != sessionTarget {
		t.Fatalf("recvSessionAdvertised = %d, want %d before threshold", c.flow.recvSessionAdvertised, sessionTarget)
	}
	if c.flow.recvSessionPending != 10 {
		t.Fatalf("recvSessionPending = %d, want 10", c.flow.recvSessionPending)
	}
	if stream.recvAdvertised != streamTarget {
		t.Fatalf("stream recvAdvertised = %d, want %d before threshold", stream.recvAdvertised, streamTarget)
	}
	if stream.recvPending != 10 {
		t.Fatalf("stream recvPending = %d, want 10", stream.recvPending)
	}
	if c.pending.hasSessionMaxData {
		t.Fatal("session MAX_DATA queued before threshold")
	}
	if testPendingStreamMaxDataCount(c) != 0 {
		t.Fatalf("pendingStreamMaxData len = %d, want 0 before threshold", testPendingStreamMaxDataCount(c))
	}

	c.flow.recvSessionReceived = c.flow.recvSessionAdvertised - quarterThreshold(sessionTarget)
	stream.recvReceived = stream.recvAdvertised - quarterThreshold(streamTarget)
	c.maybeReplenishReceiveLocked(stream)

	wantSessionAdvertised := c.flow.recvSessionReceived + sessionTarget
	if c.flow.recvSessionAdvertised != wantSessionAdvertised {
		t.Fatalf("recvSessionAdvertised = %d, want %d after threshold", c.flow.recvSessionAdvertised, wantSessionAdvertised)
	}
	if c.flow.recvSessionPending != 0 {
		t.Fatalf("recvSessionPending = %d, want 0 after replenish", c.flow.recvSessionPending)
	}
	wantStreamAdvertised := stream.recvReceived + streamTarget
	if stream.recvAdvertised != wantStreamAdvertised {
		t.Fatalf("stream recvAdvertised = %d, want %d after threshold", stream.recvAdvertised, wantStreamAdvertised)
	}
	if stream.recvPending != 0 {
		t.Fatalf("stream recvPending = %d, want 0 after replenish", stream.recvPending)
	}

	urgent, advisory := testDrainPendingControlFrames(c)
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 2 {
		t.Fatalf("drained %d urgent frames, want 2", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(wantSessionAdvertised)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA %d", urgent[0], wantSessionAdvertised)
	}
	if urgent[1].Type != FrameTypeMAXDATA || urgent[1].StreamID != stream.id || string(urgent[1].Payload) != string(mustEncodeVarint(wantStreamAdvertised)) {
		t.Fatalf("stream frame = %+v, want stream MAX_DATA %d => %d", urgent[1], stream.id, wantStreamAdvertised)
	}
}

func TestDiscardReceiveStillReplenishesSessionImmediately(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	c.flow.recvSessionAdvertised = 100
	c.flow.recvSessionUsed = 5
	c.flow.recvSessionPending = 7
	stream.recvAdvertised = 100
	stream.recvBuffer = 5
	stream.recvPending = 9
	stream.localReadStop = true

	c.releaseReceiveLocked(stream, 5)

	if c.flow.recvSessionAdvertised != 105 {
		t.Fatalf("recvSessionAdvertised = %d, want 105", c.flow.recvSessionAdvertised)
	}
	if c.flow.recvSessionPending != 7 {
		t.Fatalf("recvSessionPending = %d, want preserved pending 7", c.flow.recvSessionPending)
	}
	if stream.recvAdvertised != 100 {
		t.Fatalf("stream recvAdvertised = %d, want 100", stream.recvAdvertised)
	}
	if stream.recvPending != 0 {
		t.Fatalf("stream recvPending = %d, want 0", stream.recvPending)
	}

	urgent, advisory := testDrainPendingControlFrames(c)
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(105)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA 105", urgent[0])
	}
}

func TestStandingGrowthFallsBackToReleasedCreditUnderSessionPressure(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	sessionTarget := c.sessionWindowTargetLocked()
	c.flow.recvSessionAdvertised = sessionTarget
	c.flow.recvSessionReceived = c.flow.recvSessionAdvertised - quarterThreshold(sessionTarget)
	c.flow.recvSessionUsed = c.sessionDataHWMLocked()
	c.flow.recvSessionPending = 10

	streamTarget := c.streamWindowTargetLocked(stream)
	stream.recvAdvertised = streamTarget
	stream.recvReceived = stream.recvAdvertised - quarterThreshold(streamTarget)
	stream.recvBuffer = 0
	stream.recvPending = 10

	c.maybeReplenishReceiveLocked(stream)

	if got, want := c.flow.recvSessionAdvertised, sessionTarget+10; got != want {
		t.Fatalf("recvSessionAdvertised = %d, want released-credit fallback %d", got, want)
	}
	if got, want := stream.recvAdvertised, stream.recvReceived+streamTarget; got != want {
		t.Fatalf("stream recvAdvertised = %d, want standing target growth %d", got, want)
	}
}

func TestStandingGrowthFallsBackToReleasedCreditUnderStreamPressure(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	sessionTarget := c.sessionWindowTargetLocked()
	c.flow.recvSessionAdvertised = sessionTarget
	c.flow.recvSessionReceived = c.flow.recvSessionAdvertised - quarterThreshold(sessionTarget)
	c.flow.recvSessionUsed = 0
	c.flow.recvSessionPending = 10

	streamTarget := c.streamWindowTargetLocked(stream)
	stream.recvAdvertised = streamTarget
	stream.recvReceived = stream.recvAdvertised - quarterThreshold(streamTarget)
	stream.recvBuffer = c.perStreamDataHWMLocked()
	stream.recvPending = 10

	c.maybeReplenishReceiveLocked(stream)

	if got, want := c.flow.recvSessionAdvertised, c.flow.recvSessionReceived+sessionTarget; got != want {
		t.Fatalf("recvSessionAdvertised = %d, want standing target growth %d", got, want)
	}
	if got, want := stream.recvAdvertised, streamTarget+10; got != want {
		t.Fatalf("stream recvAdvertised = %d, want released-credit fallback %d", got, want)
	}
}

func TestReplenishReceivePreservesCreditWhenMaxDataCannotQueue(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	sessionTarget := c.sessionWindowTargetLocked()
	streamTarget := c.streamWindowTargetLocked(stream)

	c.pending.pendingControlBudget = 1
	c.pending.controlBytes = 1
	c.flow.recvSessionAdvertised = sessionTarget
	c.flow.recvSessionReceived = c.flow.recvSessionAdvertised - quarterThreshold(sessionTarget)
	c.flow.recvSessionPending = 10
	stream.recvAdvertised = streamTarget
	stream.recvReceived = stream.recvAdvertised - quarterThreshold(streamTarget)
	stream.recvPending = 10

	c.maybeReplenishReceiveLocked(stream)

	if got := c.flow.recvSessionAdvertised; got != sessionTarget {
		t.Fatalf("recvSessionAdvertised = %d, want unchanged %d when MAX_DATA queueing is blocked", got, sessionTarget)
	}
	if got := c.flow.recvSessionPending; got != 10 {
		t.Fatalf("recvSessionPending = %d, want preserved 10 when MAX_DATA queueing is blocked", got)
	}
	if got := stream.recvAdvertised; got != streamTarget {
		t.Fatalf("stream recvAdvertised = %d, want unchanged %d when MAX_DATA queueing is blocked", got, streamTarget)
	}
	if got := stream.recvPending; got != 10 {
		t.Fatalf("stream recvPending = %d, want preserved 10 when MAX_DATA queueing is blocked", got)
	}
	if !c.flow.recvReplenishRetry {
		t.Fatal("recvReplenishRetry = false, want retry marker when MAX_DATA queueing is blocked")
	}
	if c.pending.hasSessionMaxData {
		t.Fatal("session MAX_DATA should not be recorded when control budget rejects the replenish")
	}
	if got := testPendingStreamMaxDataCount(c); got != 0 {
		t.Fatalf("pendingStreamMaxData len = %d, want 0 when control budget rejects the replenish", got)
	}
}

func TestReceiveReplenishRetryQueuesPendingCreditAfterControlBudgetFrees(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	sessionTarget := c.sessionWindowTargetLocked()
	streamTarget := c.streamWindowTargetLocked(stream)
	c.pending.pendingControlBudget = 1
	c.pending.controlBytes = 1
	c.flow.recvSessionAdvertised = sessionTarget
	c.flow.recvSessionReceived = sessionTarget - quarterThreshold(sessionTarget)
	c.flow.recvSessionPending = 10
	stream.recvAdvertised = streamTarget
	stream.recvReceived = streamTarget - quarterThreshold(streamTarget)
	stream.recvPending = 10

	c.maybeReplenishReceiveLocked(stream)
	c.pending.pendingControlBudget = 0
	c.pending.controlBytes = 0
	c.retryReceiveReplenishLocked()

	wantSessionAdvertised := c.flow.recvSessionReceived + sessionTarget
	if got := c.flow.recvSessionAdvertised; got != wantSessionAdvertised {
		t.Fatalf("recvSessionAdvertised = %d, want %d after retry", got, wantSessionAdvertised)
	}
	if got := c.flow.recvSessionPending; got != 0 {
		t.Fatalf("recvSessionPending = %d, want 0 after retry", got)
	}
	wantStreamAdvertised := stream.recvReceived + streamTarget
	if got := stream.recvAdvertised; got != wantStreamAdvertised {
		t.Fatalf("stream recvAdvertised = %d, want %d after retry", got, wantStreamAdvertised)
	}
	if got := stream.recvPending; got != 0 {
		t.Fatalf("stream recvPending = %d, want 0 after retry", got)
	}
	if c.flow.recvReplenishRetry {
		t.Fatal("recvReplenishRetry = true, want false after successful retry")
	}

	urgent, advisory := testDrainPendingControlFrames(c)
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 2 {
		t.Fatalf("drained %d urgent frames, want 2", len(urgent))
	}
}

func TestSessionBlockedPromotesPendingCreditImmediately(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	sessionTarget := c.sessionWindowTargetLocked()
	blockedAt := sessionTarget
	c.flow.recvSessionAdvertised = blockedAt
	c.flow.recvSessionReceived = blockedAt / 2
	c.flow.recvSessionPending = 10

	if err := c.handleBlockedFrame(Frame{
		Type:    FrameTypeBLOCKED,
		Payload: mustEncodeVarint(blockedAt),
	}); err != nil {
		t.Fatalf("handle session BLOCKED: %v", err)
	}

	wantAdvertised := c.flow.recvSessionReceived + sessionTarget
	if c.flow.recvSessionAdvertised != wantAdvertised {
		t.Fatalf("recvSessionAdvertised = %d, want %d after BLOCKED", c.flow.recvSessionAdvertised, wantAdvertised)
	}
	if c.flow.recvSessionPending != 0 {
		t.Fatalf("recvSessionPending = %d, want 0 after BLOCKED", c.flow.recvSessionPending)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(wantAdvertised)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA %d", urgent[0], wantAdvertised)
	}
}

func TestSessionEmergencyReplenishUsesNegotiatedFramePayload(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.MaxFramePayload = 65536
	c.config.peer.Settings.MaxFramePayload = 16384
	c.flow.sessionDataHWM = 16384

	target := c.sessionWindowTargetLocked()
	remaining := uint64(70000)

	c.flow.recvSessionAdvertised = target
	c.flow.recvSessionReceived = target - remaining
	c.flow.recvSessionPending = 10

	c.maybeReplenishSessionLocked()

	if c.flow.recvSessionAdvertised != target {
		t.Fatalf("recvSessionAdvertised = %d, want %d when remaining %d is above negotiated emergency threshold", c.flow.recvSessionAdvertised, target, remaining)
	}
	if c.flow.recvSessionPending != 10 {
		t.Fatalf("recvSessionPending = %d, want 10 when no emergency replenish should fire", c.flow.recvSessionPending)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()
	if len(urgent) != 0 || len(advisory) != 0 {
		t.Fatalf("drained control after non-emergency check = %d urgent / %d advisory, want 0 / 0", len(urgent), len(advisory))
	}
}

func TestSessionEmergencyReplenishFiresBelowTwoNegotiatedFramePayloads(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384
	c.flow.sessionDataHWM = 8192

	target := c.sessionWindowTargetLocked()
	remaining := uint64(20000)

	c.flow.recvSessionAdvertised = target
	c.flow.recvSessionReceived = target - remaining
	c.flow.recvSessionPending = 10

	c.maybeReplenishSessionLocked()

	wantAdvertised := c.flow.recvSessionReceived + target
	if c.flow.recvSessionAdvertised != wantAdvertised {
		t.Fatalf("recvSessionAdvertised = %d, want %d after emergency replenish", c.flow.recvSessionAdvertised, wantAdvertised)
	}
	if c.flow.recvSessionPending != 0 {
		t.Fatalf("recvSessionPending = %d, want 0 after emergency replenish", c.flow.recvSessionPending)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("urgent len = %d, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(wantAdvertised)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA %d", urgent[0], wantAdvertised)
	}
}

func TestStreamBlockedPromotesPendingCreditImmediately(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	streamTarget := c.streamWindowTargetLocked(stream)
	blockedAt := streamTarget
	stream.recvAdvertised = blockedAt
	stream.recvReceived = blockedAt / 2
	stream.recvPending = 10

	if err := c.handleBlockedFrame(Frame{
		Type:     FrameTypeBLOCKED,
		StreamID: stream.id,
		Payload:  mustEncodeVarint(blockedAt),
	}); err != nil {
		t.Fatalf("handle stream BLOCKED: %v", err)
	}

	wantAdvertised := stream.recvReceived + streamTarget
	if stream.recvAdvertised != wantAdvertised {
		t.Fatalf("stream recvAdvertised = %d, want %d after BLOCKED", stream.recvAdvertised, wantAdvertised)
	}
	if stream.recvPending != 0 {
		t.Fatalf("stream recvPending = %d, want 0 after BLOCKED", stream.recvPending)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != stream.id || string(urgent[0].Payload) != string(mustEncodeVarint(wantAdvertised)) {
		t.Fatalf("stream frame = %+v, want stream MAX_DATA %d => %d", urgent[0], stream.id, wantAdvertised)
	}
}

func TestStreamBlockedKeepsStreamReplenishSuppressedAfterLocalReadStop(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.InitialMaxData = 262144
	c.config.local.Settings.InitialMaxStreamDataBidiPeerOpened = 65536
	c.config.local.Settings.MaxFramePayload = 16384
	c.config.peer.Settings.MaxFramePayload = 16384

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	sessionTarget := c.sessionWindowTargetLocked()
	streamTarget := c.streamWindowTargetLocked(stream)
	c.flow.recvSessionAdvertised = sessionTarget
	c.flow.recvSessionReceived = sessionTarget / 2
	c.flow.recvSessionPending = 10
	stream.recvAdvertised = streamTarget
	stream.recvReceived = streamTarget / 2
	stream.recvPending = 10
	stream.localReadStop = true

	if err := c.handleBlockedFrame(Frame{
		Type:     FrameTypeBLOCKED,
		StreamID: stream.id,
		Payload:  mustEncodeVarint(streamTarget),
	}); err != nil {
		t.Fatalf("handle stream BLOCKED after read stop: %v", err)
	}

	wantSessionAdvertised := c.flow.recvSessionReceived + sessionTarget
	if c.flow.recvSessionAdvertised != wantSessionAdvertised {
		t.Fatalf("recvSessionAdvertised = %d, want %d after BLOCKED", c.flow.recvSessionAdvertised, wantSessionAdvertised)
	}
	if c.flow.recvSessionPending != 0 {
		t.Fatalf("recvSessionPending = %d, want 0 after BLOCKED", c.flow.recvSessionPending)
	}
	if stream.recvAdvertised != streamTarget {
		t.Fatalf("stream recvAdvertised = %d, want suppressed value %d", stream.recvAdvertised, streamTarget)
	}
	if stream.recvPending != 0 {
		t.Fatalf("stream recvPending = %d, want 0 after suppressed BLOCKED replenish", stream.recvPending)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()

	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("drained %d urgent frames, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(wantSessionAdvertised)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA %d", urgent[0], wantSessionAdvertised)
	}
}

func TestSessionReplenishClampsAdvertisedToVarint62(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.flow.recvSessionAdvertised = MaxVarint62 - 2
	c.flow.recvSessionPending = 10

	c.replenishSessionLocked(0)

	if got := c.flow.recvSessionAdvertised; got != MaxVarint62 {
		t.Fatalf("recvSessionAdvertised = %d, want %d", got, MaxVarint62)
	}
	if got := c.flow.recvSessionPending; got != 0 {
		t.Fatalf("recvSessionPending = %d, want 0", got)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("urgent len = %d, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(MaxVarint62)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA %d", urgent[0], MaxVarint62)
	}
}

func TestStreamReplenishClampsAdvertisedToVarint62(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	stream.recvAdvertised = MaxVarint62 - 2
	stream.recvPending = 10

	c.replenishStreamLocked(stream, 0)

	if got := stream.recvAdvertised; got != MaxVarint62 {
		t.Fatalf("stream recvAdvertised = %d, want %d", got, MaxVarint62)
	}
	if got := stream.recvPending; got != 0 {
		t.Fatalf("stream recvPending = %d, want 0", got)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("urgent len = %d, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != stream.id || string(urgent[0].Payload) != string(mustEncodeVarint(MaxVarint62)) {
		t.Fatalf("stream frame = %+v, want stream MAX_DATA %d => %d", urgent[0], stream.id, MaxVarint62)
	}
}

func TestQueueSessionMaxDataClampsValueAboveVarint62(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	if !c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(MaxVarint62+17)) {
		t.Fatal("queuePendingSessionControlAsync rejected clamped session MAX_DATA")
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("urgent len = %d, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != 0 || string(urgent[0].Payload) != string(mustEncodeVarint(MaxVarint62)) {
		t.Fatalf("session frame = %+v, want session MAX_DATA %d", urgent[0], MaxVarint62)
	}
}

func TestQueueStreamMaxDataClampsValueAboveVarint62(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	c.queueStreamMaxDataAsync(stream.id, MaxVarint62+17)

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	c.mu.Unlock()
	if len(advisory) != 0 {
		t.Fatalf("advisory len = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		t.Fatalf("urgent len = %d, want 1", len(urgent))
	}
	if urgent[0].Type != FrameTypeMAXDATA || urgent[0].StreamID != stream.id || string(urgent[0].Payload) != string(mustEncodeVarint(MaxVarint62)) {
		t.Fatalf("stream frame = %+v, want stream MAX_DATA %d => %d", urgent[0], stream.id, MaxVarint62)
	}
}

func TestReleaseStreamReceiveStateClearsPendingWithoutBufferedBytes(t *testing.T) {
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := &nativeStream{
		conn:         c,
		id:           state.FirstPeerStreamID(c.config.negotiated.LocalRole, true),
		idSet:        true,
		localReceive: true,
		recvPending:  10,
		readBuf:      []byte("stale"),
	}
	stream.initHalfStates()

	c.mu.Lock()
	c.releaseStreamReceiveStateLocked(stream, streamReceiveReleaseAndClearReadBuf)
	c.mu.Unlock()

	if got := stream.recvPending; got != 0 {
		t.Fatalf("stream recvPending = %d, want 0", got)
	}
	if got := stream.recvBuffer; got != 0 {
		t.Fatalf("stream recvBuffer = %d, want 0", got)
	}
	if got := len(stream.readBuf); got != 0 {
		t.Fatalf("stream readBuf len = %d, want 0", got)
	}
}

func TestCloseFrameSendTimeoutUsesObservedRTTCap(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.lastPingRTT = 700 * time.Millisecond

	if got := c.closeFrameSendTimeout(); got != sessionCloseFrameSendTimeoutMax {
		t.Fatalf("closeFrameSendTimeout = %v, want %v", got, sessionCloseFrameSendTimeoutMax)
	}
}

func TestGracefulCloseDrainTimeoutUsesObservedRTTFloorWhenUnset(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.lastPingRTT = 600 * time.Millisecond

	want := 4*(600*time.Millisecond) + 100*time.Millisecond
	if got := c.gracefulCloseDrainTimeout(); got != want {
		t.Fatalf("gracefulCloseDrainTimeout = %v, want %v", got, want)
	}
}

func TestGracefulCloseDrainTimeoutOverrideWinsOverObservedRTT(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.lastPingRTT = 2 * time.Second
	c.terminalPolicy.gracefulCloseTimeout = 200 * time.Millisecond

	if got := c.gracefulCloseDrainTimeout(); got != 200*time.Millisecond {
		t.Fatalf("gracefulCloseDrainTimeout = %v, want 200ms override", got)
	}
}

func TestStopSendingDrainWindowUsesObservedRTTFloorWhenUnset(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.lastPingRTT = 900 * time.Millisecond

	if got := c.stopSendingDrainWindow(); got != 1800*time.Millisecond {
		t.Fatalf("stopSendingDrainWindow = %v, want 1800ms", got)
	}
}

func TestAdaptiveRTTTimeoutSaturatesBeforeApplyingMax(t *testing.T) {
	t.Parallel()

	got := adaptiveRTTTimeout(time.Duration(1<<62), 500*time.Millisecond, 5*time.Second, 4, 50*time.Millisecond)
	if got != 5*time.Second {
		t.Fatalf("adaptiveRTTTimeout overflow case = %v, want 5s cap", got)
	}
}

func TestEffectiveKeepaliveTimeoutSaturatesConfiguredRTTFloor(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.keepaliveTimeout = 500 * time.Millisecond
	c.liveness.lastPingRTT = time.Duration(1 << 62)

	got := c.effectiveKeepaliveTimeoutLocked()
	if got != time.Duration(1<<63-1) {
		t.Fatalf("effectiveKeepaliveTimeout overflow case = %v, want saturated duration", got)
	}
}

func TestEffectiveKeepaliveTimeoutSaturatesDefaultBaseBeforeMax(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.keepaliveInterval = time.Duration(1 << 62)

	got := c.effectiveKeepaliveTimeoutLocked()
	if got != defaultKeepaliveTimeoutMax {
		t.Fatalf("effectiveKeepaliveTimeout default base overflow case = %v, want %v", got, defaultKeepaliveTimeoutMax)
	}
}

func TestGoAwayDrainIntervalUsesObservedRTTQuarter(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.lastPingRTT = 800 * time.Millisecond

	if got := c.goAwayDrainInterval(); got != 200*time.Millisecond {
		t.Fatalf("goAwayDrainInterval = %v, want 200ms", got)
	}

	c.liveness.lastPingRTT = 2 * time.Second
	if got := c.goAwayDrainInterval(); got != sessionGoAwayDrainIntervalMax {
		t.Fatalf("goAwayDrainInterval = %v, want %v cap", got, sessionGoAwayDrainIntervalMax)
	}
}

func TestNextKeepaliveActionUsesObservedRTTFloorBeforeTimeout(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	base := time.Unix(1_700_000_000, 0)
	c.liveness.keepaliveInterval = time.Second
	c.liveness.keepaliveTimeout = 200 * time.Millisecond
	c.liveness.lastPingRTT = 500 * time.Millisecond
	c.liveness.pingOutstanding = true
	c.liveness.lastPingSentAt = base

	action := c.nextKeepaliveAction(base.Add(300 * time.Millisecond))

	if action.sendPing {
		t.Fatal("nextKeepaliveAction requested ping while previous ping is still outstanding")
	}
	want := 4*(500*time.Millisecond) + rttAdaptiveSlack - 300*time.Millisecond
	if action.delay != want {
		t.Fatalf("keepalive delay = %v, want %v", action.delay, want)
	}
	if !c.liveness.pingOutstanding {
		t.Fatal("nextKeepaliveAction cleared outstanding ping before adaptive timeout elapsed")
	}
}

func TestEffectiveKeepaliveTimeoutLockedUsesAdaptiveDefaultWhenUnset(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.keepaliveInterval = 10 * time.Second

	if got := c.effectiveKeepaliveTimeoutLocked(); got != 20*time.Second {
		t.Fatalf("effectiveKeepaliveTimeoutLocked() = %v, want %v", got, 20*time.Second)
	}

	c.liveness.lastPingRTT = 8 * time.Second
	want := 4*(8*time.Second) + rttAdaptiveSlack
	if got := c.effectiveKeepaliveTimeoutLocked(); got != want {
		t.Fatalf("effectiveKeepaliveTimeoutLocked() with RTT floor = %v, want %v", got, want)
	}

	c.liveness.lastPingRTT = 20 * time.Second
	if got := c.effectiveKeepaliveTimeoutLocked(); got != defaultKeepaliveTimeoutMax {
		t.Fatalf("effectiveKeepaliveTimeoutLocked() cap = %v, want %v", got, defaultKeepaliveTimeoutMax)
	}
}

func TestProvisionalOpenMaxAgeUsesObservedRTTFloor(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.lastPingRTT = 2 * time.Second

	want := 6*(2*time.Second) + 250*time.Millisecond
	if got := c.provisionalOpenMaxAgeLocked(); got != want {
		t.Fatalf("provisionalOpenMaxAge = %v, want %v", got, want)
	}
}

func TestProvisionalOpenMaxAgeUsesObservedRTTCap(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.liveness.lastPingRTT = 5 * time.Second

	if got := c.provisionalOpenMaxAgeLocked(); got != provisionalOpenMaxAgeAdaptiveCap {
		t.Fatalf("provisionalOpenMaxAge = %v, want %v", got, provisionalOpenMaxAgeAdaptiveCap)
	}
}
