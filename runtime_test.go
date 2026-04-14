package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/wire"
)

func requireNativeStreamImpl(t *testing.T, stream NativeStream) *nativeStream {
	t.Helper()
	impl, ok := stream.(*nativeStream)
	if !ok {
		t.Fatalf("stream impl = %T, want *nativeStream", stream)
	}
	return impl
}

func mustNativeStreamImpl(stream NativeStream) *nativeStream {
	impl, ok := stream.(*nativeStream)
	if !ok {
		panic("stream impl != *nativeStream")
	}
	return impl
}

func requireNativeSendStreamImpl(t *testing.T, stream NativeSendStream) *nativeSendStream {
	t.Helper()
	impl, ok := stream.(*nativeSendStream)
	if !ok {
		t.Fatalf("stream impl = %T, want *nativeSendStream", stream)
	}
	return impl
}

func mustNativeSendStreamImpl(stream NativeSendStream) *nativeSendStream {
	impl, ok := stream.(*nativeSendStream)
	if !ok {
		panic("stream impl != *nativeSendStream")
	}
	return impl
}

func requireNativeRecvStreamImpl(t *testing.T, stream NativeRecvStream) *nativeRecvStream {
	t.Helper()
	impl, ok := stream.(*nativeRecvStream)
	if !ok {
		t.Fatalf("stream impl = %T, want *nativeRecvStream", stream)
	}
	return impl
}

func mustNativeRecvStreamImpl(stream NativeRecvStream) *nativeRecvStream {
	impl, ok := stream.(*nativeRecvStream)
	if !ok {
		panic("stream impl != *nativeRecvStream")
	}
	return impl
}

func TestBidiStreamWriteReadAndCloseWrite(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	type acceptResult struct {
		stream NativeStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		s, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{s, err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if n, err := stream.Write([]byte("hi")); err != nil || n != 2 {
		t.Fatalf("write = (%d, %v), want (2, nil)", n, err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("accept: %v", accepted.err)
	}
	if accepted.stream.StreamID() != 4 {
		t.Fatalf("accepted stream id = %d, want 4", accepted.stream.StreamID())
	}

	buf := make([]byte, 8)
	n, err := accepted.stream.Read(buf)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("read payload = %q, want %q", got, "hi")
	}
	if _, err := accepted.stream.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("second read err = %v, want EOF", err)
	}
}

func TestUniStreamWriteReadAndCloseWrite(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	type acceptResult struct {
		stream NativeRecvStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		s, err := server.AcceptUniStream(ctx)
		acceptCh <- acceptResult{s, err}
	}()

	stream, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("open uni stream: %v", err)
	}
	if n, err := stream.Write([]byte("hi")); err != nil || n != 2 {
		t.Fatalf("write = (%d, %v), want (2, nil)", n, err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("accept uni: %v", accepted.err)
	}

	buf := make([]byte, 8)
	n, err := accepted.stream.Read(buf)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("read payload = %q, want %q", got, "hi")
	}
	if _, err := accepted.stream.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("second read err = %v, want EOF", err)
	}
}

func TestSendStreamCloseIgnoresAbsentReadHalf(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	type acceptResult struct {
		stream NativeRecvStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		s, err := server.AcceptUniStream(ctx)
		acceptCh <- acceptResult{s, err}
	}()

	stream, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("open uni stream: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close send-only stream: %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("accept uni: %v", accepted.err)
	}
	if _, err := accepted.stream.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("recv-only peer read err = %v, want EOF", err)
	}
}

func TestRecvStreamCloseIgnoresAbsentWriteHalf(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	type acceptResult struct {
		stream NativeRecvStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		s, err := client.AcceptUniStream(ctx)
		acceptCh <- acceptResult{s, err}
	}()

	stream, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("open uni stream: %v", err)
	}
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("accept uni: %v", accepted.err)
	}
	if err := accepted.stream.Close(); err != nil {
		t.Fatalf("close recv-only stream: %v", err)
	}

	awaitStreamWriteState(t, requireNativeSendStreamImpl(t, stream).stream, testSignalTimeout, func(s *nativeStream) bool {
		return s.localReadStop || s.sendStop != nil || s.sendAbort != nil || s.sendReset != nil || s.sendFinReached()
	}, "timed out waiting for send-only peer to observe recv-side close")

	if _, err := stream.Write([]byte("x")); err == nil {
		t.Fatal("expected write to fail after peer recv-only Close")
	}
}

func TestCloseReadStopsPeerWrites(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan NativeStream, 1)
	go func() {
		s, _ := server.AcceptStream(ctx)
		acceptCh <- s
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	accepted := requireNativeStreamImpl(t, <-acceptCh)
	if accepted == nil {
		t.Fatal("expected accepted stream")
	}
	if err := accepted.CloseRead(); err != nil {
		t.Fatalf("close read: %v", err)
	}

	awaitStreamWriteState(t, requireNativeStreamImpl(t, stream), testSignalTimeout, func(s *nativeStream) bool {
		return s.localReadStop || s.sendStop != nil || s.sendAbort != nil || s.sendReset != nil || s.sendFinReached()
	}, "timed out waiting for write stop state after peer CloseRead")

	_, err = stream.Write([]byte("x"))
	if err == nil {
		t.Fatal("expected write to fail after peer CloseRead")
	}
}

func TestCloseWithErrorPropagatesWholeStreamAbort(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan NativeStream, 1)
	go func() {
		s, _ := server.AcceptStream(ctx)
		acceptCh <- s
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	accepted := requireNativeStreamImpl(t, <-acceptCh)
	if accepted == nil {
		t.Fatal("expected accepted stream")
	}
	if err := accepted.CloseWithError(uint64(CodeRefusedStream), "no"); err != nil {
		t.Fatalf("close with error: %v", err)
	}

	awaitStreamWriteState(t, requireNativeStreamImpl(t, stream), testSignalTimeout, func(s *nativeStream) bool {
		return s.sendAbort != nil || s.sendReset != nil
	}, "timed out waiting for peer abort to be observed")

	_, err = stream.Write([]byte("x"))
	if err == nil {
		t.Fatal("expected write to fail after peer abort")
	}
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("write err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("write error code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}
}

func TestPeerAbortMakesReadReturnApplicationError(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan NativeStream, 1)
	go func() {
		s, _ := server.AcceptStream(ctx)
		acceptCh <- s
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	serverStream := requireNativeStreamImpl(t, <-acceptCh)
	if serverStream == nil {
		t.Fatal("expected accepted stream")
	}
	if _, err := serverStream.Read(make([]byte, 2)); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := serverStream.CloseWithError(uint64(CodeRefusedStream), "no"); err != nil {
		t.Fatalf("server abort: %v", err)
	}

	awaitStreamReadState(t, requireNativeStreamImpl(t, clientStream), testSignalTimeout, func(s *nativeStream) bool {
		return s.recvAbort != nil
	}, "timed out waiting for peer abort to be observed")

	buf := make([]byte, 8)
	_, err = clientStream.Read(buf)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("read err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("read err code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}
	if !errors.Is(appErr, &ApplicationError{Code: uint64(CodeRefusedStream), Reason: "no"}) {
		_ = appErr
	}
	_, err = clientStream.Write([]byte("x"))
	if !errors.As(err, &appErr) {
		t.Fatalf("write after peer abort err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("write err code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}
}

func TestPeerAbortReleasesWithdrawnSendBudget(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 9
	c.flow.sendSessionUsed = 9
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	if err := c.handleAbortFrame(Frame{
		Type:     FrameTypeABORT,
		StreamID: stream.id,
		Payload:  mustEncodeVarint(uint64(CodeRefusedStream)),
	}); err != nil {
		t.Fatalf("handle ABORT err = %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	sendSessionUsed := c.flow.sendSessionUsed
	sendSent := stream.sendSent
	sendAbort := stream.sendAbort
	c.mu.Unlock()

	if sendAbort == nil || sendAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("sendAbort = %v, want code %d", sendAbort, uint64(CodeRefusedStream))
	}
	if sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", sendSessionUsed)
	}
	if sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0", sendSent)
	}
}

func TestResetReleasesWithdrawnSendBudget(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 11
	c.flow.sendSessionUsed = 11
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	if err := stream.CancelWrite(uint64(CodeCancelled)); err != nil {
		t.Fatalf("Reset err = %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeRESET {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeRESET)
	}
	if frame.StreamID != stream.id {
		t.Fatalf("queued frame stream = %d, want %d", frame.StreamID, stream.id)
	}
	code, _, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("decode RESET payload err = %v", err)
	}
	if code != uint64(CodeCancelled) {
		t.Fatalf("queued RESET code = %d, want %d", code, uint64(CodeCancelled))
	}

	c.mu.Lock()
	sendSessionUsed := c.flow.sendSessionUsed
	sendSent := stream.sendSent
	sendReset := stream.sendReset
	c.mu.Unlock()

	if sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", sendSessionUsed)
	}
	if sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0", sendSent)
	}
	if sendReset == nil || sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", sendReset, uint64(CodeCancelled))
	}
}

func TestProvisionalResetReleasesWithdrawnSendBudget(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	stream.sendSent = 8
	c.flow.sendSessionUsed = 8
	c.mu.Unlock()

	if err := stream.CancelWrite(uint64(CodeCancelled)); err != nil {
		t.Fatalf("provisional reset err = %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	sendSessionUsed := c.flow.sendSessionUsed
	sendSent := stream.sendSent
	sendAbort := stream.sendAbort
	c.mu.Unlock()

	if sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", sendSessionUsed)
	}
	if sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0", sendSent)
	}
	if sendAbort == nil || sendAbort.Code != uint64(CodeCancelled) {
		t.Fatalf("sendAbort = %v, want code %d", sendAbort, uint64(CodeCancelled))
	}
}

func TestCloseWithErrorReleasesWithdrawnBudgets(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 13
	stream.recvBuffer = 4
	stream.readBuf = []byte("read")
	c.flow.sendSessionUsed = 13
	c.flow.recvSessionUsed = 4
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	if err := stream.CloseWithError(uint64(CodeInternal), "bye"); err != nil {
		t.Fatalf("CloseWithErrorCode err = %v", err)
	}
	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeABORT {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeABORT)
	}
	if frame.StreamID != stream.id {
		t.Fatalf("queued frame stream = %d, want %d", frame.StreamID, stream.id)
	}
	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("decode ABORT payload err = %v", err)
	}
	if code != uint64(CodeInternal) || reason != "bye" {
		t.Fatalf("queued ABORT payload = (%d, %q), want (%d, %q)", code, reason, uint64(CodeInternal), "bye")
	}

	c.mu.Lock()
	sendSessionUsed := c.flow.sendSessionUsed
	recvSessionUsed := c.flow.recvSessionUsed
	sendSent := stream.sendSent
	recvBuffer := stream.recvBuffer
	readBufLen := len(stream.readBuf)
	sendAbort := stream.sendAbort
	c.mu.Unlock()

	if sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", sendSessionUsed)
	}
	if recvSessionUsed != 0 {
		t.Fatalf("recvSessionUsed = %d, want 0", recvSessionUsed)
	}
	if sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0", sendSent)
	}
	if recvBuffer != 0 {
		t.Fatalf("stream recvBuffer = %d, want 0", recvBuffer)
	}
	if readBufLen != 0 {
		t.Fatalf("stream readBuf len = %d, want 0", readBufLen)
	}
	if sendAbort == nil || sendAbort.Code != uint64(CodeInternal) {
		t.Fatalf("sendAbort = %v, want code %d", sendAbort, uint64(CodeInternal))
	}
}

func TestProvisionalCloseWithErrorReleasesWithdrawnBudgets(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	stream.sendSent = 10
	c.flow.sendSessionUsed = 10
	c.mu.Unlock()

	if err := stream.CloseWithError(uint64(CodeInternal), "bye"); err != nil {
		t.Fatalf("provisional close with error err = %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	sendSessionUsed := c.flow.sendSessionUsed
	sendSent := stream.sendSent
	sendAbort := stream.sendAbort
	c.mu.Unlock()

	if sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", sendSessionUsed)
	}
	if sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0", sendSent)
	}
	if sendAbort == nil || sendAbort.Code != uint64(CodeInternal) {
		t.Fatalf("sendAbort = %v, want code %d", sendAbort, uint64(CodeInternal))
	}
}

func TestGoAwayRefusesFutureOpens(t *testing.T) {
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	if err := server.GoAway(0, 0); err != nil {
		t.Fatalf("server sessionControl: %v", err)
	}

	awaitPeerGoAwayBidi(t, client, 0)

	_, err := client.OpenStream(ctx)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("open after GOAWAY err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("open after GOAWAY code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}
}

func TestGoAwayMustBeNonIncreasing(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()
	c.sessionControl.peerGoAwayBidi = 80
	c.sessionControl.peerGoAwayUni = 99

	if err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 80, 99, uint64(CodeNoError), ""),
	}); err != nil {
		t.Fatalf("first non-increasing GOAWAY: %v", err)
	}
	err := c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, 80, 103, uint64(CodeNoError), ""),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("widening GOAWAY err = %v, want %s", err, CodeProtocol)
	}
}

func TestPeerGoAwayReclaimsNeverPeerVisibleLocalStream(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := server.GoAway(0, 0); err != nil {
		t.Fatalf("server sessionControl: %v", err)
	}

	streamImpl := requireNativeStreamImpl(t, stream)
	awaitStreamWriteState(t, streamImpl, testSignalTimeout, func(s *nativeStream) bool {
		return s.sendAbort != nil
	}, "timed out waiting for unseen local stream to be reclaimed by peer GOAWAY")

	client.mu.Lock()
	sendAbort := streamImpl.sendAbort
	client.mu.Unlock()
	if sendAbort == nil {
		t.Fatal("expected reclaimed stream to have sendAbort")
	}
	if sendAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("reclaimed stream code = %d, want %d", sendAbort.Code, uint64(CodeRefusedStream))
	}
}

func TestPeerGoAwayReclaimReleasesSendBudget(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	stream.idSet = true
	stream.sendSent = 9
	c.flow.sendSessionUsed = 9
	c.registry.streams[stream.id] = stream
	testMarkLocalOpenCommitted(stream)

	c.mu.Lock()
	c.sessionControl.peerGoAwayBidi = 0
	c.sessionControl.peerGoAwayUni = 0
	c.reclaimUnseenLocalStreamsLocked()
	sendAbort := stream.sendAbort
	used := c.flow.sendSessionUsed
	c.mu.Unlock()

	if sendAbort == nil || sendAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("reclaimed send abort code = %v, want %d", sendAbort, uint64(CodeRefusedStream))
	}
	if used != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", used)
	}
	if stream.sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0", stream.sendSent)
	}
}

func TestSuppressWriteRequestRejectsTerminalLocalSend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		seed  func(*nativeStream)
		state state.SendHalfState
	}{
		{
			name: "send_fin",
			seed: func(stream *nativeStream) {
				stream.setSendFin()
			},
			state: state.SendHalfFin,
		},
		{
			name: "send_stop_seen",
			seed: func(stream *nativeStream) {
				stream.setSendStopSeen(&ApplicationError{Code: uint64(CodeCancelled)})
			},
			state: state.SendHalfStopSeen,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, _, stop := newHandlerTestConn(t)
			defer stop()

			c.mu.Lock()
			stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
			testMarkLocalOpenCommitted(stream)
			c.registry.streams[stream.id] = stream
			tc.seed(stream)

			req := writeRequest{
				frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("x")}}),
				done:   make(chan error, 1),
				origin: writeRequestOriginStream,
			}
			c.mu.Unlock()

			err := c.suppressWriteRequest(req)
			if !errors.Is(err, ErrWriteClosed) {
				t.Fatalf("suppressWriteRequest err = %v, want %v", err, ErrWriteClosed)
			}
			if got := stream.sendHalfState(); got != tc.state {
				t.Fatalf("sendHalf = %v, want %v", got, tc.state)
			}
		})
	}
}

func TestSuppressWriteRequestAllowsResetAfterLocalReset(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	stream.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetDirect)
	c.registry.streams[stream.id] = stream

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeRESET, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		terminalPolicy: terminalWriteAllow,
	}
	c.mu.Unlock()
	err := c.suppressWriteRequest(req)

	if err != nil {
		t.Fatalf("suppressWriteRequest err = %v, want nil", err)
	}
}

func TestSuppressWriteRequestAllowsAbortAfterLocalCloseWithError(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	stream.setAbortedWithSource(&ApplicationError{Code: uint64(CodeInternal), Reason: "shutdown"}, terminalAbortLocal)
	c.registry.streams[stream.id] = stream

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeABORT, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeInternal))}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		terminalPolicy: terminalWriteAllow,
	}
	c.mu.Unlock()
	err := c.suppressWriteRequest(req)

	if err != nil {
		t.Fatalf("suppressWriteRequest err = %v, want nil", err)
	}
}

func TestSuppressWriteRequestRejectsDataAfterLocalCloseWithError(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	stream.setAbortedWithSource(&ApplicationError{Code: uint64(CodeInternal), Reason: "shutdown"}, terminalAbortLocal)
	c.registry.streams[stream.id] = stream

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Flags: FrameFlagFIN, Payload: []byte("x")}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		terminalPolicy: terminalWriteAllow,
	}
	c.mu.Unlock()
	err := c.suppressWriteRequest(req)

	if err == nil {
		t.Fatalf("suppressWriteRequest err = nil, want terminal ApplicationError")
	}
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("suppressWriteRequest err = %v, want terminal ApplicationError", err)
	}
	if appErr.Code != uint64(CodeInternal) {
		t.Fatalf("suppressWriteRequest err code = %d, want %d", appErr.Code, uint64(CodeInternal))
	}
}

func TestSuppressWriteRequestRejectsWrongTerminalFrameForSendReset(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	stream.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetDirect)
	c.registry.streams[stream.id] = stream

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("x")}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		terminalPolicy: terminalWriteAllow,
	}
	c.mu.Unlock()
	err := c.suppressWriteRequest(req)

	if err == nil {
		t.Fatalf("suppressWriteRequest err = nil, want terminal reset error")
	}
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("suppressWriteRequest err = %v, want terminal ApplicationError", err)
	}
	if appErr.Code != uint64(CodeCancelled) {
		t.Fatalf("suppressWriteRequest err code = %d, want %d", appErr.Code, uint64(CodeCancelled))
	}
}

func TestSuppressWriteRequestAllowsFinFrameWithOpenMetadata(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, []byte("meta"))
	testMarkLocalOpenCommitted(stream)
	stream.setSendFin()
	stream.openMetadataPrefix = []byte("meta")
	c.registry.streams[stream.id] = stream

	req := writeRequest{
		frames: testTxFramesFrom([]Frame{
			{
				Type:     FrameTypeDATA,
				StreamID: stream.id,
				Flags:    FrameFlagFIN,
				Payload:  []byte("meta"),
			},
		}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		terminalPolicy: terminalWriteAllow,
	}
	c.mu.Unlock()
	err := c.suppressWriteRequest(req)

	if err != nil {
		t.Fatalf("suppressWriteRequest err = %v, want nil", err)
	}
}

func TestWriteOnConcreteLocalIDQueuesOpeningDataWithMetadataAndMarksPeerVisible(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
	defer stop()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix err = %v", err)
	}

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")},
		prefix,
	)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	n, err := stream.Write([]byte("x"))
	if err != nil {
		t.Fatalf("Write err = %v, want nil", err)
	}
	if n != 1 {
		t.Fatalf("Write n = %d, want 1", n)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeDATA {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeDATA)
	}
	if frame.StreamID != stream.id {
		t.Fatalf("queued frame stream = %d, want %d", frame.StreamID, stream.id)
	}
	if frame.Flags&FrameFlagOpenMetadata == 0 {
		t.Fatal("queued DATA frame missing OPEN_METADATA flag for concrete-id opener")
	}
	if string(frame.Payload) != string(append(append([]byte(nil), prefix...), 'x')) {
		t.Fatalf("queued DATA payload = %x, want %x", frame.Payload, append(append([]byte(nil), prefix...), 'x'))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !stream.localOpen.committed {
		t.Fatal("sendCommitted = false, want true after first opening DATA")
	}
	if !stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = false, want true after first opening DATA")
	}
}

func TestSuppressWriteRequestAllowsPriorityUpdateBeforeTerminalFin(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	payload, err := buildPriorityUpdatePayload(
		CapabilityPriorityUpdate|CapabilityPriorityHints,
		MetadataUpdate{Priority: uint64ptr(9)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c.mu.Lock()
	c.config.negotiated.Capabilities = CapabilityPriorityUpdate | CapabilityPriorityHints
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	stream.setSendFin()
	c.registry.streams[stream.id] = stream

	req := writeRequest{
		frames: testTxFramesFrom([]Frame{
			{
				Type:     FrameTypeEXT,
				StreamID: stream.id,
				Payload:  payload,
			},
			{
				Type:     FrameTypeDATA,
				StreamID: stream.id,
				Flags:    FrameFlagFIN,
				Payload:  []byte("x"),
			},
		}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		terminalPolicy: terminalWriteAllow,
	}
	c.mu.Unlock()
	err = c.suppressWriteRequest(req)

	if err != nil {
		t.Fatalf("suppressWriteRequest err = %v, want nil", err)
	}
}

func TestSuppressWriteRequestRejectsFramesAfterTerminalFin(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	stream.setSendFin()
	c.registry.streams[stream.id] = stream

	req := writeRequest{
		frames: testTxFramesFrom([]Frame{
			{
				Type:     FrameTypeDATA,
				StreamID: stream.id,
				Flags:    FrameFlagFIN,
				Payload:  []byte("x"),
			},
			{
				Type:     FrameTypeDATA,
				StreamID: stream.id,
				Payload:  []byte("y"),
			},
		}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		terminalPolicy: terminalWriteAllow,
	}
	c.mu.Unlock()
	err := c.suppressWriteRequest(req)

	if !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("suppressWriteRequest err = %v, want ErrWriteClosed", err)
	}
}

func TestWriteLoopSuppressesTerminalRequestsInBatch(t *testing.T) {
	t.Parallel()

	writer := &countingWriteCloser{}
	c := &Conn{
		io: connIOState{conn: writer},

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16}}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	c.mu.Lock()
	normal := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)
	closed := c.newLocalStreamWithIDLocked(8, streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(normal)
	testMarkLocalOpenCommitted(closed)
	closed.setSendFin()
	c.registry.streams[normal.id] = normal
	c.registry.streams[closed.id] = closed
	c.mu.Unlock()

	reqNormal := writeRequest{
		done:   make(chan error, 1),
		origin: writeRequestOriginStream,
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: normal.id, Payload: []byte("hello")}}),
	}
	reqClosed := writeRequest{
		done:   make(chan error, 1),
		origin: writeRequestOriginStream,
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: closed.id, Payload: []byte("bye")}}),
	}

	c.writer.writeCh <- reqNormal
	c.writer.writeCh <- reqClosed
	go c.writeLoop()

	doneNormal := awaitRequestDone(t, reqNormal.done, "timed out waiting for normal request completion")
	doneClosed := awaitRequestDone(t, reqClosed.done, "timed out waiting for closed request completion")

	if doneNormal != nil {
		t.Fatalf("normal request done = %v, want nil", doneNormal)
	}
	if !errors.Is(doneClosed, ErrWriteClosed) {
		t.Fatalf("closed request done = %v, want %v", doneClosed, ErrWriteClosed)
	}
	if got := writer.writeCount(); got != 1 {
		t.Fatalf("underlying write count = %d, want 1", got)
	}

	close(c.lifecycle.closedCh)
}

func TestWriteLoopSuppressesLeadingTerminalRequestWithoutClobberingLaterAcceptedRequest(t *testing.T) {
	t.Parallel()

	writer := &countingWriteCloser{}
	c := &Conn{
		io: connIOState{conn: writer},

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16}}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	c.mu.Lock()
	closed := c.newLocalStreamWithIDLocked(8, streamArityBidi, OpenOptions{}, nil)
	normal := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(closed)
	testMarkLocalOpenCommitted(normal)
	closed.setSendFin()
	c.registry.streams[closed.id] = closed
	c.registry.streams[normal.id] = normal
	c.mu.Unlock()

	reqClosed := writeRequest{
		done:   make(chan error, 1),
		origin: writeRequestOriginStream,
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: closed.id, Payload: []byte("bye")}}),
	}
	reqNormal := writeRequest{
		done:   make(chan error, 1),
		origin: writeRequestOriginStream,
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: normal.id, Payload: []byte("hello")}}),
	}

	c.writer.writeCh <- reqClosed
	c.writer.writeCh <- reqNormal
	go c.writeLoop()

	doneClosed := awaitRequestDone(t, reqClosed.done, "timed out waiting for leading closed request completion")
	doneNormal := awaitRequestDone(t, reqNormal.done, "timed out waiting for trailing normal request completion")

	if !errors.Is(doneClosed, ErrWriteClosed) {
		t.Fatalf("closed request done = %v, want %v", doneClosed, ErrWriteClosed)
	}
	if doneNormal != nil {
		t.Fatalf("normal request done = %v, want nil", doneNormal)
	}
	if got := writer.writeCount(); got != 1 {
		t.Fatalf("underlying write count = %d, want 1", got)
	}

	close(c.lifecycle.closedCh)
}

func TestWriteLoopAllowsTerminalResetAndSuppressesOtherTerminalData(t *testing.T) {
	t.Parallel()

	writer := &countingWriteCloser{}
	c := &Conn{
		io: connIOState{conn: writer},

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16}}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	c.mu.Lock()
	normal := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)
	reset := c.newLocalStreamWithIDLocked(8, streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(normal)
	testMarkLocalOpenCommitted(reset)
	reset.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetDirect)
	c.registry.streams[normal.id] = normal
	c.registry.streams[reset.id] = reset
	c.mu.Unlock()

	reqNormal := writeRequest{
		done:   make(chan error, 1),
		origin: writeRequestOriginStream,
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: normal.id, Payload: []byte("hello")}}),
	}
	reqReset := writeRequest{
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeRESET, StreamID: reset.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}}),
		terminalPolicy: terminalWriteAllow,
	}
	reqClosed := writeRequest{
		done:   make(chan error, 1),
		origin: writeRequestOriginStream,
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: reset.id, Payload: []byte("x")}}),
	}

	c.writer.writeCh <- reqNormal
	c.writer.writeCh <- reqReset
	c.writer.writeCh <- reqClosed
	go c.writeLoop()

	doneNormal := awaitRequestDone(t, reqNormal.done, "timed out waiting for normal request completion")
	doneReset := awaitRequestDone(t, reqReset.done, "timed out waiting for reset request completion")
	doneClosed := awaitRequestDone(t, reqClosed.done, "timed out waiting for closed request completion")

	if doneNormal != nil {
		t.Fatalf("normal request done = %v, want nil", doneNormal)
	}
	if doneReset != nil {
		t.Fatalf("reset request done = %v, want nil", doneReset)
	}
	var appErr *ApplicationError
	if !errors.As(doneClosed, &appErr) {
		t.Fatalf("closed request done = %v, want ApplicationError", doneClosed)
	}
	if appErr.Code != uint64(CodeCancelled) {
		t.Fatalf("closed request code = %d, want %d", appErr.Code, uint64(CodeCancelled))
	}
	if got := writer.writeCount(); got != 1 {
		t.Fatalf("underlying write count = %d, want 1", got)
	}

	close(c.lifecycle.closedCh)
}

func TestWriteLoopSuppressRejectedDataRollsBackPreparedSendCredit(t *testing.T) {
	t.Parallel()

	writer := &countingWriteCloser{}
	c := &Conn{
		io: connIOState{conn: writer},

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 4)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16}}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	c.mu.Lock()
	closed := c.newLocalStreamWithIDLocked(8, streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(closed)
	closed.setSendStopSeen(&ApplicationError{Code: uint64(CodeCancelled)})
	closed.sendSent = 3
	closed.queuedDataBytes = 4
	c.flow.sendSessionUsed = 3
	c.flow.queuedDataBytes = 4
	c.registry.streams[closed.id] = closed
	c.mu.Unlock()

	reqClosed := writeRequest{
		done:              make(chan error, 1),
		origin:            writeRequestOriginStream,
		frames:            testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: closed.id, Payload: []byte("bye")}}),
		queueReserved:     true,
		queuedBytes:       4,
		reservedStream:    closed,
		preparedSendBytes: 3,
	}

	c.writer.writeCh <- reqClosed
	go c.writeLoop()

	doneClosed := awaitRequestDone(t, reqClosed.done, "timed out waiting for suppressed request completion")
	if !errors.Is(doneClosed, ErrWriteClosed) {
		t.Fatalf("closed request done = %v, want %v", doneClosed, ErrWriteClosed)
	}

	c.mu.Lock()
	sendSent := closed.sendSent
	sendSessionUsed := c.flow.sendSessionUsed
	queuedDataBytes := c.flow.queuedDataBytes
	streamQueuedData := closed.queuedDataBytes
	c.mu.Unlock()

	if sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0 after writer-path suppress rollback", sendSent)
	}
	if sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0 after writer-path suppress rollback", sendSessionUsed)
	}
	if queuedDataBytes != 0 {
		t.Fatalf("conn queuedDataBytes = %d, want 0 after writer-path suppress rollback", queuedDataBytes)
	}
	if streamQueuedData != 0 {
		t.Fatalf("stream queuedDataBytes = %d, want 0 after writer-path suppress rollback", streamQueuedData)
	}
	if got := writer.writeCount(); got != 0 {
		t.Fatalf("underlying write count = %d, want 0 for fully suppressed batch", got)
	}

	close(c.lifecycle.closedCh)
}

func TestWriteLoopSuppressRejectedFinalClearsPreparedFin(t *testing.T) {
	t.Parallel()

	writer := &countingWriteCloser{}
	c := &Conn{
		io: connIOState{conn: writer},

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 4)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16}}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	c.mu.Lock()
	reset := c.newLocalStreamWithIDLocked(8, streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(reset)
	reset.sendHalf = state.SendHalfFin
	reset.sendSent = 1
	reset.queuedDataBytes = 2
	reset.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetDirect)
	c.flow.sendSessionUsed = 1
	c.flow.queuedDataBytes = 2
	c.registry.streams[reset.id] = reset
	c.mu.Unlock()

	reqClosed := writeRequest{
		done:              make(chan error, 1),
		origin:            writeRequestOriginStream,
		terminalPolicy:    terminalWriteAllow,
		frames:            testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: reset.id, Flags: FrameFlagFIN, Payload: []byte("x")}}),
		queueReserved:     true,
		queuedBytes:       2,
		reservedStream:    reset,
		preparedSendBytes: 1,
		preparedSendFin:   true,
	}

	c.writer.writeCh <- reqClosed
	go c.writeLoop()

	doneClosed := awaitRequestDone(t, reqClosed.done, "timed out waiting for suppressed final request completion")
	var appErr *ApplicationError
	if !errors.As(doneClosed, &appErr) {
		t.Fatalf("closed request done = %v, want ApplicationError", doneClosed)
	}
	if appErr.Code != uint64(CodeCancelled) {
		t.Fatalf("closed request code = %d, want %d", appErr.Code, uint64(CodeCancelled))
	}

	c.mu.Lock()
	sendFin := reset.sendFinReached()
	sendReset := reset.sendReset
	sendHalf := reset.sendHalfState()
	sendSent := reset.sendSent
	sendSessionUsed := c.flow.sendSessionUsed
	queuedDataBytes := c.flow.queuedDataBytes
	streamQueuedData := reset.queuedDataBytes
	c.mu.Unlock()

	if sendFin {
		t.Fatal("sendFinReached() = true, want false after rejected final request is rolled back")
	}
	if sendReset == nil || sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", sendReset, uint64(CodeCancelled))
	}
	if sendHalf != state.SendHalfReset {
		t.Fatalf("sendHalf = %v, want %v", sendHalf, state.SendHalfReset)
	}
	if sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0 after rejected final request rollback", sendSent)
	}
	if sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0 after rejected final request rollback", sendSessionUsed)
	}
	if queuedDataBytes != 0 {
		t.Fatalf("conn queuedDataBytes = %d, want 0 after rejected final request rollback", queuedDataBytes)
	}
	if streamQueuedData != 0 {
		t.Fatalf("stream queuedDataBytes = %d, want 0 after rejected final request rollback", streamQueuedData)
	}
	if got := writer.writeCount(); got != 0 {
		t.Fatalf("underlying write count = %d, want 0 for fully suppressed final batch", got)
	}

	close(c.lifecycle.closedCh)
}

func TestPeerVisibleLocalStreamSurvivesGoAwayReclaim(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan NativeStream, 1)
	go func() {
		s, _ := server.AcceptStream(ctx)
		acceptCh <- s
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	nativeStream := requireNativeStreamImpl(t, stream)
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if accepted := <-acceptCh; accepted == nil {
		t.Fatal("expected accepted peer-visible stream")
	}
	client.mu.Lock()
	client.sessionControl.peerGoAwayBidi = 0
	client.sessionControl.peerGoAwayUni = 0
	client.reclaimUnseenLocalStreamsLocked()
	sendAbort := nativeStream.sendAbort
	client.mu.Unlock()
	if sendAbort != nil && sendAbort.Code == uint64(CodeRefusedStream) {
		t.Fatal("peer-visible local stream should not be reclaimed by GOAWAY")
	}
}

func TestPeerVisibleFromCloseWriteSurvivesGoAwayReclaim(t *testing.T) {
	t.Parallel()
	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	nativeStream := requireNativeStreamImpl(t, stream)
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	client.mu.Lock()
	client.sessionControl.peerGoAwayBidi = 0
	client.sessionControl.peerGoAwayUni = 0
	client.reclaimUnseenLocalStreamsLocked()
	sendAbort := nativeStream.sendAbort
	client.mu.Unlock()
	if sendAbort != nil && sendAbort.Code == uint64(CodeRefusedStream) {
		t.Fatal("close-write opener should mark local stream peer-visible")
	}
}

func TestPeerVisibleFromCloseReadSurvivesGoAwayReclaim(t *testing.T) {
	t.Parallel()
	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	nativeStream := requireNativeStreamImpl(t, stream)
	if err := stream.CloseRead(); err != nil {
		t.Fatalf("close read: %v", err)
	}

	client.mu.Lock()
	client.sessionControl.peerGoAwayBidi = 0
	client.sessionControl.peerGoAwayUni = 0
	client.reclaimUnseenLocalStreamsLocked()
	sendAbort := nativeStream.sendAbort
	client.mu.Unlock()
	if sendAbort != nil && sendAbort.Code == uint64(CodeRefusedStream) {
		t.Fatal("close-read opener should mark local stream peer-visible")
	}
}

func TestPeerIgnoresGoAwayAndGetsRefused(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.sessionControl.localGoAwayBidi = 0
	c.sessionControl.localGoAwayUni = 0
	c.mu.Unlock()

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true),
		Payload:  []byte("x"),
	}); err != nil {
		t.Fatalf("handle DATA after local GOAWAY: %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeABORT || frame.StreamID != state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) {
		t.Fatalf("queued frame = %+v, want ABORT on first peer stream", frame)
	}
	if string(frame.Payload) != string(mustEncodeVarint(uint64(CodeRefusedStream))) {
		t.Fatalf("ABORT payload = %x, want %x", frame.Payload, mustEncodeVarint(uint64(CodeRefusedStream)))
	}
}

func TestParsePrefaceExamples(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		hex   string
		role  Role
		nonce uint64
	}{
		{"initiator", "5a 4d 55 58 01 00 00 01 01 00 00", RoleInitiator, 0},
		{"responder", "5a 4d 55 58 01 01 00 01 01 00 00", RoleResponder, 0},
		{"auto", "5a 4d 55 58 01 02 05 01 01 00 00", RoleAuto, 5},
	}

	for _, tt := range tests {
		p, err := ParsePreface(mustHex(t, tt.hex))
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if p.PrefaceVersion != PrefaceVersion {
			t.Fatalf("%s: preface version=%d", tt.name, p.PrefaceVersion)
		}
		if p.Role != tt.role {
			t.Fatalf("%s: role=%v want %v", tt.name, p.Role, tt.role)
		}
		if p.TieBreakerNonce != tt.nonce {
			t.Fatalf("%s: nonce=%d want %d", tt.name, p.TieBreakerNonce, tt.nonce)
		}
		if p.MinProto != ProtoVersion || p.MaxProto != ProtoVersion {
			t.Fatalf("%s: protocol range=%d..%d", tt.name, p.MinProto, p.MaxProto)
		}
	}
}

func TestNegotiateAutoRoles(t *testing.T) {
	t.Parallel()
	local := Preface{
		PrefaceVersion:  PrefaceVersion,
		Role:            RoleAuto,
		TieBreakerNonce: 5,
		MinProto:        ProtoVersion,
		MaxProto:        ProtoVersion,
		Settings:        DefaultSettings(),
	}
	peer := local
	peer.TieBreakerNonce = 3

	negotiated, err := NegotiatePrefaces(local, peer)
	if err != nil {
		t.Fatalf("negotiate: %v", err)
	}
	if negotiated.LocalRole != RoleInitiator || negotiated.PeerRole != RoleResponder {
		t.Fatalf("roles = %v/%v, want initiator/responder", negotiated.LocalRole, negotiated.PeerRole)
	}
}

func TestNegotiateEqualAutoNonceFails(t *testing.T) {
	t.Parallel()
	local := Preface{
		PrefaceVersion:  PrefaceVersion,
		Role:            RoleAuto,
		TieBreakerNonce: 5,
		MinProto:        ProtoVersion,
		MaxProto:        ProtoVersion,
		Settings:        DefaultSettings(),
	}
	peer := local

	if _, err := NegotiatePrefaces(local, peer); err == nil {
		t.Fatal("expected equal auto nonce conflict")
	} else if !IsErrorCode(err, CodeRoleConflict) {
		t.Fatalf("got %v, want %s", err, CodeRoleConflict)
	}
}

func TestClientServerEstablish(t *testing.T) {
	t.Parallel()
	left, right := net.Pipe()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	type result struct {
		conn *Conn
		err  error
	}
	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)

	go func() { c, err := Client(left, nil); clientCh <- result{c, err} }()
	go func() { c, err := Server(right, nil); serverCh <- result{c, err} }()

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

	if client.conn.Negotiated().LocalRole != RoleInitiator {
		t.Fatalf("client local role = %v", client.conn.Negotiated().LocalRole)
	}
	if server.conn.Negotiated().LocalRole != RoleResponder {
		t.Fatalf("server local role = %v", server.conn.Negotiated().LocalRole)
	}
}

func TestEstablishedConnUsesTunedReadBufferSize(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)

	for _, conn := range []*Conn{client, server} {
		if conn.io.reader == nil {
			t.Fatal("conn.io.reader = nil")
		}
		if got := conn.io.reader.Size(); got != connReadBufferSize {
			t.Fatalf("conn.io.reader.Size() = %d, want %d", got, connReadBufferSize)
		}
	}
}

func TestEstablishedConnDefersOptionalChannelsUntilNeeded(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)

	for _, conn := range []*Conn{client, server} {
		if conn.signals.acceptCh != nil {
			t.Fatal("conn.signals.acceptCh != nil before AcceptStream waits, want lazy allocation")
		}
		if conn.writer.advisoryWriteCh != nil {
			t.Fatal("conn.writer.advisoryWriteCh != nil without negotiated priority_update, want nil")
		}
		if conn.signals.livenessCh != nil {
			t.Fatal("conn.signals.livenessCh != nil without keepalive or graceful-close waiters, want lazy allocation")
		}
	}
}

func TestEstablishedConnAllocatesAdvisoryChannelWhenPriorityUpdateNegotiated(t *testing.T) {
	t.Parallel()

	clientCfg := &Config{Capabilities: CapabilityPriorityUpdate | CapabilityPriorityHints}
	serverCfg := &Config{Capabilities: CapabilityPriorityUpdate | CapabilityPriorityHints}
	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)

	for _, conn := range []*Conn{client, server} {
		if conn.writer.advisoryWriteCh == nil {
			t.Fatal("conn.writer.advisoryWriteCh = nil with negotiated priority_update, want channel")
		}
	}
}

func TestEstablishedConnAllocatesLivenessChannelWhenKeepaliveEnabled(t *testing.T) {
	t.Parallel()

	clientCfg := &Config{
		KeepaliveInterval: time.Second,
		KeepaliveTimeout:  2 * time.Second,
	}
	serverCfg := &Config{
		KeepaliveInterval: time.Second,
		KeepaliveTimeout:  2 * time.Second,
	}
	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)

	for _, conn := range []*Conn{client, server} {
		if conn.signals.livenessCh == nil {
			t.Fatal("conn.signals.livenessCh = nil with keepalive enabled, want channel")
		}
	}
}

func TestEnsureLivenessNotifyLockedAllocatesOnDemand(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	if c.ensureLivenessNotifyLocked() == nil {
		t.Fatal("ensureLivenessNotifyLocked() = nil, want channel")
	}
	if c.signals.livenessCh == nil {
		t.Fatal("conn.signals.livenessCh = nil after ensureLivenessNotifyLocked")
	}
}

func TestClientEstablishmentOnlyWritesPrefaceBeforePeerPreface(t *testing.T) {
	t.Parallel()

	left, right := net.Pipe()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	cfg := cloneConfig(nil)
	cfg.Role = RoleInitiator
	cfg.TieBreakerNonce = 0
	local, err := cfg.LocalPreface()
	if err != nil {
		t.Fatalf("build local preface: %v", err)
	}
	want, err := local.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal local preface: %v", err)
	}

	type result struct {
		conn *Conn
		err  error
	}
	clientCh := make(chan result, 1)
	go func() {
		conn, err := Client(left, nil)
		clientCh <- result{conn: conn, err: err}
	}()

	got := make([]byte, len(want))
	if _, err := io.ReadFull(right, got); err != nil {
		t.Fatalf("read client preface: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("client preface = %x, want %x", got, want)
	}

	if err := right.SetReadDeadline(time.Now().Add(testSignalTimeout / 5)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	extra := make([]byte, 1)
	_, err = right.Read(extra)
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("extra pre-ready client write err = %v, want timeout with no additional bytes", err)
	}
	_ = right.SetReadDeadline(time.Time{})
	_ = right.Close()

	client := <-clientCh
	if client.conn != nil {
		_ = client.conn.Close()
		t.Fatal("expected client establish to fail after peer close")
	}
	if client.err == nil {
		t.Fatal("expected client establish error after peer close")
	}
}

func testPrefaceBytesForRole(t *testing.T, role Role) []byte {
	t.Helper()
	cfg := cloneConfig(nil)
	cfg.Role = role
	if role != RoleAuto {
		cfg.TieBreakerNonce = 0
	}
	preface, err := cfg.LocalPreface()
	if err != nil {
		t.Fatalf("build preface: %v", err)
	}
	payload, err := preface.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal preface: %v", err)
	}
	return payload
}

func testPrefaceForRole(t *testing.T, role Role) Preface {
	t.Helper()
	cfg := cloneConfig(nil)
	cfg.Role = role
	if role != RoleAuto {
		cfg.TieBreakerNonce = 0
	}
	preface, err := cfg.LocalPreface()
	if err != nil {
		t.Fatalf("build preface: %v", err)
	}
	return preface
}

type scriptedDuplexConn struct {
	readMu  sync.Mutex
	writeMu sync.Mutex
	readBuf *bytes.Reader
	written bytes.Buffer
	closed  bool
}

func newScriptedDuplexConn(inbound []byte) *scriptedDuplexConn {
	return &scriptedDuplexConn{readBuf: bytes.NewReader(inbound)}
}

func (c *scriptedDuplexConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.readBuf == nil {
		return 0, io.EOF
	}
	return c.readBuf.Read(p)
}

func (c *scriptedDuplexConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	return c.written.Write(p)
}

func (c *scriptedDuplexConn) Close() error {
	c.writeMu.Lock()
	c.closed = true
	c.writeMu.Unlock()
	return nil
}

func (c *scriptedDuplexConn) bytes() []byte {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return append([]byte(nil), c.written.Bytes()...)
}

func establishmentFramesAfterPreface(t *testing.T, raw []byte, prefaceLen int) []Frame {
	t.Helper()
	if len(raw) < prefaceLen {
		t.Fatalf("establishment writes len = %d, want at least local preface len %d", len(raw), prefaceLen)
	}
	return decodeFramesForTest(t, raw[prefaceLen:])
}

func TestCloseAfterEstablishmentFailureWritesFatalClose(t *testing.T) {
	t.Parallel()

	conn := newScriptedDuplexConn(nil)
	local := testPrefaceForRole(t, RoleInitiator)

	closeAfterEstablishmentFailure(conn, local, nil, wireError(CodeProtocol, "parse preface", errInvalidRole))

	frames := decodeFramesForTest(t, conn.bytes())
	if len(frames) != 1 {
		t.Fatalf("frame count = %d, want 1 CLOSE", len(frames))
	}
	if frames[0].Type != FrameTypeCLOSE {
		t.Fatalf("frame type = %s, want %s", frames[0].Type, FrameTypeCLOSE)
	}
	code, _, err := parseErrorPayload(frames[0].Payload)
	if err != nil {
		t.Fatalf("parse close payload: %v", err)
	}
	if code != uint64(CodeProtocol) {
		t.Fatalf("close code = %d, want %d", code, uint64(CodeProtocol))
	}
}

func TestFinishEstablishmentFailureWaitsForLocalPrefaceWriteBeforeFatalClose(t *testing.T) {
	t.Parallel()

	conn := newScriptedDuplexConn(nil)
	local := testPrefaceForRole(t, RoleInitiator)
	writeErrCh := make(chan error, 1)
	writeErrCh <- nil

	finishEstablishmentFailure(conn, writeErrCh, local, nil, wireError(CodeProtocol, "parse preface", errInvalidRole))

	frames := decodeFramesForTest(t, conn.bytes())
	if len(frames) != 1 {
		t.Fatalf("frame count = %d, want 1 CLOSE", len(frames))
	}
	if frames[0].Type != FrameTypeCLOSE {
		t.Fatalf("frame type = %s, want %s", frames[0].Type, FrameTypeCLOSE)
	}
	code, _, err := parseErrorPayload(frames[0].Payload)
	if err != nil {
		t.Fatalf("parse close payload: %v", err)
	}
	if code != uint64(CodeProtocol) {
		t.Fatalf("close code = %d, want %d", code, uint64(CodeProtocol))
	}
}

func TestBuildEstablishmentCloseFrameEncodesFatalClose(t *testing.T) {
	t.Parallel()

	local := testPrefaceForRole(t, RoleInitiator)
	raw, err := buildEstablishmentCloseFrame(local, nil, wireError(CodeProtocol, "parse preface", errInvalidRole))
	if err != nil {
		t.Fatalf("buildEstablishmentCloseFrame() err = %v", err)
	}

	frames := decodeFramesForTest(t, raw)
	if len(frames) != 1 {
		t.Fatalf("frame count = %d, want 1 CLOSE", len(frames))
	}
	if frames[0].Type != FrameTypeCLOSE {
		t.Fatalf("frame type = %s, want %s", frames[0].Type, FrameTypeCLOSE)
	}
}

func TestClientEstablishmentInvalidPeerPrefaceEmitsFatalClose(t *testing.T) {
	t.Parallel()

	wantPreface := testPrefaceBytesForRole(t, RoleInitiator)
	invalidPeer := append([]byte(nil), testPrefaceBytesForRole(t, RoleResponder)...)
	invalidPeer[5] = 7
	conn := newScriptedDuplexConn(invalidPeer)
	client, err := Client(conn, nil)
	if client != nil {
		_ = client.Close()
		t.Fatal("expected client establish to fail after invalid peer preface")
	}
	if err == nil {
		t.Fatal("expected client establish error after invalid peer preface")
	}

	written := conn.bytes()
	if len(written) < len(wantPreface) {
		t.Fatalf("establishment writes len = %d, want at least client preface len %d", len(written), len(wantPreface))
	}
	if !bytes.Equal(written[:len(wantPreface)], wantPreface) {
		t.Fatalf("client preface = %x, want %x", written[:len(wantPreface)], wantPreface)
	}
	frames := establishmentFramesAfterPreface(t, written, len(wantPreface))
	if len(frames) != 1 {
		t.Fatalf("establishment frame count = %d, want 1 fatal CLOSE", len(frames))
	}
	frame := frames[0]
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
	if frame.StreamID != 0 {
		t.Fatalf("close frame stream id = %d, want 0", frame.StreamID)
	}
	code, _, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse close payload: %v", err)
	}
	if code != uint64(CodeProtocol) {
		t.Fatalf("close code = %d, want %d", code, uint64(CodeProtocol))
	}
}

func TestClientEstablishmentRoleConflictEmitsFatalClose(t *testing.T) {
	t.Parallel()

	wantPreface := testPrefaceBytesForRole(t, RoleInitiator)
	conflictingPeer := testPrefaceBytesForRole(t, RoleInitiator)
	conn := newScriptedDuplexConn(conflictingPeer)
	client, err := Client(conn, nil)
	if client != nil {
		_ = client.Close()
		t.Fatal("expected client establish to fail after role conflict")
	}
	if err == nil {
		t.Fatal("expected client establish error after role conflict")
	}

	written := conn.bytes()
	if !bytes.Equal(written[:len(wantPreface)], wantPreface) {
		t.Fatalf("client preface = %x, want %x", written[:len(wantPreface)], wantPreface)
	}
	frames := establishmentFramesAfterPreface(t, written, len(wantPreface))
	if len(frames) != 1 {
		t.Fatalf("establishment frame count = %d, want 1 fatal CLOSE", len(frames))
	}
	frame := frames[0]
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("frame type = %s, want %s", frame.Type, FrameTypeCLOSE)
	}
	code, _, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse close payload: %v", err)
	}
	if code != uint64(CodeRoleConflict) {
		t.Fatalf("close code = %d, want %d", code, uint64(CodeRoleConflict))
	}
}

func TestClientEstablishmentInvalidPeerPrefaceDoesNotWaitForBlockedLocalPrefaceWrite(t *testing.T) {
	t.Parallel()

	left, right := net.Pipe()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	invalidPeer := append([]byte(nil), testPrefaceBytesForRole(t, RoleResponder)...)
	invalidPeer[5] = 7

	peerWriteDone := make(chan error, 1)
	go func() {
		_, err := right.Write(invalidPeer)
		peerWriteDone <- err
	}()

	type result struct {
		conn *Conn
		err  error
	}
	clientCh := make(chan result, 1)
	go func() {
		conn, err := Client(left, nil)
		clientCh <- result{conn: conn, err: err}
	}()

	select {
	case err := <-peerWriteDone:
		if err != nil {
			t.Fatalf("write invalid peer preface: %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out delivering invalid peer preface")
	}

	select {
	case res := <-clientCh:
		if res.conn != nil {
			_ = res.conn.Close()
			t.Fatal("expected client establish to fail after invalid peer preface")
		}
		if res.err == nil {
			t.Fatal("expected client establish error after invalid peer preface")
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("client establish blocked waiting for local preface write after invalid peer preface")
	}
}

func TestClientEstablishmentRoleConflictDoesNotWaitForBlockedLocalPrefaceWrite(t *testing.T) {
	t.Parallel()

	left, right := net.Pipe()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	conflictingPeer := testPrefaceBytesForRole(t, RoleInitiator)
	peerWriteDone := make(chan error, 1)
	go func() {
		_, err := right.Write(conflictingPeer)
		peerWriteDone <- err
	}()

	type result struct {
		conn *Conn
		err  error
	}
	clientCh := make(chan result, 1)
	go func() {
		conn, err := Client(left, nil)
		clientCh <- result{conn: conn, err: err}
	}()

	select {
	case err := <-peerWriteDone:
		if err != nil {
			t.Fatalf("write conflicting peer preface: %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out delivering conflicting peer preface")
	}

	select {
	case res := <-clientCh:
		if res.conn != nil {
			_ = res.conn.Close()
			t.Fatal("expected client establish to fail after role conflict")
		}
		if res.err == nil {
			t.Fatal("expected client establish error after role conflict")
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("client establish blocked waiting for local preface write after role conflict")
	}
}

func TestRetainReadFrameBufferKeepsSmallBuffer(t *testing.T) {
	t.Parallel()

	buf := make([]byte, 0, maxRetainedReadFrameBytes)
	retained := retainReadFrameBuffer(buf)
	if retained == nil {
		t.Fatal("retainReadFrameBuffer() = nil, want retained buffer")
	}
	if len(retained) != 0 {
		t.Fatalf("len(retained) = %d, want 0", len(retained))
	}
	if cap(retained) != cap(buf) {
		t.Fatalf("cap(retained) = %d, want %d", cap(retained), cap(buf))
	}
}

func TestRetainReadFrameBufferDropsOversizedBuffer(t *testing.T) {
	t.Parallel()

	buf := make([]byte, 0, maxRetainedReadFrameBytes+1)
	if retained := retainReadFrameBuffer(buf); retained != nil {
		t.Fatalf("retainReadFrameBuffer() = %v, want nil for oversized buffer", retained)
	}
}

func TestReadFrameBufferedReusesProvidedBuffer(t *testing.T) {
	t.Parallel()

	encoded, err := (Frame{
		Type:    FrameTypePING,
		Payload: []byte("12345678"),
	}).MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() err = %v", err)
	}

	_, buf, handle, err := readFrameBuffered(bytes.NewReader(encoded), Limits{}, nil)
	if err != nil {
		t.Fatalf("readFrameBuffered(first) err = %v", err)
	}
	if handle == nil {
		t.Fatal("readFrameBuffered(first) handle = nil, want pooled handle")
	}

	retained := retainReadFrameBuffer(buf)
	if retained == nil {
		t.Fatal("retainReadFrameBuffer() = nil, want retained buffer")
	}

	_, reused, reusedHandle, err := readFrameBuffered(bytes.NewReader(encoded), Limits{}, retained)
	if err != nil {
		t.Fatalf("readFrameBuffered(reuse) err = %v", err)
	}
	if reusedHandle != nil {
		t.Fatal("readFrameBuffered(reuse) returned new handle, want caller-owned retained handle")
	}
	if cap(reused) != cap(retained) {
		t.Fatalf("cap(reused) = %d, want %d", cap(reused), cap(retained))
	}
	if len(reused) == 0 {
		t.Fatal("len(reused) = 0, want frame bytes")
	}
	if &reused[0] != &retained[:len(reused)][0] {
		t.Fatal("readFrameBuffered(reuse) did not reuse retained backing array")
	}

	releaseReadFrameBuffer(reused, handle)
}

func TestWriteLoopClearsBatchScratchOnExit(t *testing.T) {
	t.Parallel()

	encoded, encodedHandle := acquireWriteBatchEncodedBuffer(128)

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer: connWriterRuntimeState{
			scratch: writeBatchScratch{
				batch:          make([]writeRequest, 1),
				items:          make([]rt.BatchItem, 1),
				ordered:        make([]writeRequest, 1),
				rejected:       make([]rejectedWriteRequest, 1),
				encoded:        encoded,
				encodedHandle:  encodedHandle,
				explicitGroups: map[uint64]struct{}{1: {}},
				queuedByStream: map[*nativeStream]uint64{testBuildDetachedStream(nil, 0): 1},
				queuedStreams:  []*nativeStream{testBuildDetachedStream(nil, 0)},
			},
		},
	}
	close(c.lifecycle.closedCh)

	c.writeLoop()

	if c.writer.scratch.batch != nil {
		t.Fatal("writeBatchScratch.batch not cleared")
	}
	if c.writer.scratch.items != nil {
		t.Fatal("writeBatchScratch.items not cleared")
	}
	if c.writer.scratch.ordered != nil {
		t.Fatal("writeBatchScratch.ordered not cleared")
	}
	if c.writer.scratch.rejected != nil {
		t.Fatal("writeBatchScratch.rejected not cleared")
	}
	if c.writer.scratch.encoded != nil {
		t.Fatal("writeBatchScratch.encoded not cleared")
	}
	if c.writer.scratch.encodedHandle != nil {
		t.Fatal("writeBatchScratch.encodedHandle not cleared")
	}
	if c.writer.scratch.explicitGroups != nil {
		t.Fatal("writeBatchScratch.explicitGroups not cleared")
	}
	if c.writer.scratch.queuedByStream != nil {
		t.Fatal("writeBatchScratch.queuedByStream not cleared")
	}
	if c.writer.scratch.queuedStreams != nil {
		t.Fatal("writeBatchScratch.queuedStreams not cleared")
	}
}

func TestWriteBatchScratchBatchSliceClearsRetainedRequestRefsOnReuse(t *testing.T) {
	t.Parallel()

	var scratch writeBatchScratch
	reserved := testBuildDetachedStream(nil, 0)
	old := scratch.batchSlice(2, 4)
	backing := old[:cap(old)]
	backing[0] = writeRequest{
		frames:                  testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("payload")}}),
		done:                    make(chan error, 1),
		reservedStream:          reserved,
		preparedPriorityPayload: []byte("prio"),
	}
	backing[1] = writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePONG, Payload: []byte("pong")}}),
		done:   make(chan error, 1),
	}
	backing[2] = writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("tail")}}),
		done:           make(chan error, 1),
		reservedStream: reserved,
	}
	backing[3] = writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeCLOSE, Payload: []byte("close")}}),
		done:   make(chan error, 1),
	}

	_ = scratch.batchSlice(0, 0)

	for i := range backing {
		if backing[i].frames != nil || backing[i].done != nil || backing[i].reservedStream != nil || backing[i].preparedPriorityPayload != nil {
			t.Fatalf("batchSlice reuse did not clear retained refs from slot %d", i)
		}
	}
}

func TestWriteBatchScratchOrderedSliceClearsRetainedRequestRefsPastLength(t *testing.T) {
	t.Parallel()

	var scratch writeBatchScratch
	reserved := testBuildDetachedStream(nil, 0)
	ordered := scratch.orderedSlice(2)
	backing := ordered[:cap(ordered)]
	if len(backing) < 4 {
		scratch.ordered = make([]writeRequest, 2, 4)
		ordered = scratch.orderedSlice(2)
		backing = ordered[:cap(ordered)]
	}
	backing[0] = writeRequest{frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("a")}}), done: make(chan error, 1)}
	backing[1] = writeRequest{frames: testTxFramesFrom([]Frame{{Type: FrameTypePONG, Payload: []byte("b")}}), done: make(chan error, 1)}
	backing[2] = writeRequest{frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("tail")}}), done: make(chan error, 1), reservedStream: reserved}
	backing[3] = writeRequest{frames: testTxFramesFrom([]Frame{{Type: FrameTypeCLOSE, Payload: []byte("tail-close")}}), done: make(chan error, 1)}

	_ = scratch.orderedSlice(1)

	for i := range backing {
		if backing[i].frames != nil || backing[i].done != nil || backing[i].reservedStream != nil {
			t.Fatalf("orderedSlice reuse did not clear retained refs from slot %d", i)
		}
	}
}

func TestWriteBatchScratchRejectedSliceClearsRetainedRequestRefsOnReuse(t *testing.T) {
	t.Parallel()

	var scratch writeBatchScratch
	reserved := testBuildDetachedStream(nil, 0)
	rejected := scratch.rejectedSlice(2)
	rejected = append(rejected, rejectedWriteRequest{
		req: writeRequest{
			frames:                  testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("payload")}}),
			done:                    make(chan error, 1),
			reservedStream:          reserved,
			preparedPriorityPayload: []byte("prio"),
		},
		err: io.ErrNoProgress,
	})
	rejected = append(rejected, rejectedWriteRequest{
		req: writeRequest{
			frames:         testTxFramesFrom([]Frame{{Type: FrameTypePONG, Payload: []byte("tail")}}),
			done:           make(chan error, 1),
			reservedStream: reserved,
		},
		err: os.ErrDeadlineExceeded,
	})
	scratch.rejected = rejected
	backing := scratch.rejected[:cap(scratch.rejected)]

	_ = scratch.rejectedSlice(0)

	for i := range backing {
		if backing[i].req.frames != nil || backing[i].req.done != nil || backing[i].req.reservedStream != nil || backing[i].req.preparedPriorityPayload != nil {
			t.Fatalf("rejectedSlice reuse did not clear retained refs from slot %d", i)
		}
		if backing[i].err != nil {
			t.Fatalf("rejectedSlice reuse did not clear retained error ref from slot %d", i)
		}
	}
}

func TestWaitPreparedQueueRequestClearsRetainedRefsOnEarlyClose(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{
			closeErr:   ErrSessionClosed,
			closedCh:   make(chan struct{}),
			terminalCh: make(chan struct{}),
		},
	}
	req := writeRequest{
		frames:                  testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("queued")}}),
		done:                    make(chan error, 1),
		preparedNotify:          make(chan struct{}),
		reservedStream:          testBuildDetachedStream(c, 4),
		queuedBytes:             7,
		queueReserved:           true,
		urgentReserved:          true,
		advisoryReserved:        true,
		cloneFramesBeforeSend:   true,
		preparedPriorityPayload: []byte("priority"),
	}

	err := c.waitPreparedQueueRequest(&req)
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("waitPreparedQueueRequest() err = %v, want %v", err, ErrSessionClosed)
	}
	if req.frames != nil || req.done != nil || req.preparedNotify != nil || req.reservedStream != nil || req.preparedPriorityPayload != nil {
		t.Fatal("waitPreparedQueueRequest did not clear retained request refs on early close")
	}
}

func TestWaitQueuedWriteCompletionClearsRetainedRefsOnInitialClose(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{
			closeErr: ErrSessionClosed,
			closedCh: make(chan struct{}),
		},
	}
	stream := testBuildDetachedStream(c, 4)
	req := writeRequest{
		frames:                  testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("queued")}}),
		done:                    make(chan error, 1),
		preparedNotify:          make(chan struct{}),
		reservedStream:          stream,
		preparedPriorityPayload: []byte("priority"),
	}

	err := stream.waitQueuedWriteCompletion(&req)
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("waitQueuedWriteCompletion() err = %v, want %v", err, ErrSessionClosed)
	}
	if req.frames != nil || req.done != nil || req.preparedNotify != nil || req.reservedStream != nil || req.preparedPriorityPayload != nil {
		t.Fatal("waitQueuedWriteCompletion did not clear retained request refs on early close")
	}
}

func TestReadLoopReleasesSupersededScratchHandle(t *testing.T) {
	oldReadFrameBuffered := readLoopReadFrameBuffered
	oldHandleFrameBuffered := readLoopHandleFrameBuffered
	oldRetain := readLoopRetainReadFrameBufferForPayload
	oldRelease := readLoopReleaseReadFrameBuffer
	oldClose := readLoopCloseSessionWithOptionsForReadErr
	defer func() {
		readLoopHooksMu.Lock()
		readLoopReadFrameBuffered = oldReadFrameBuffered
		readLoopHandleFrameBuffered = oldHandleFrameBuffered
		readLoopRetainReadFrameBufferForPayload = oldRetain
		readLoopReleaseReadFrameBuffer = oldRelease
		readLoopCloseSessionWithOptionsForReadErr = oldClose
		readLoopHooksMu.Unlock()
	}()

	oldBuf := []byte("old-buffer")
	newBuf := []byte("new-buffer")
	oldHandle := &wire.FrameReadBufferHandle{}
	newHandle := &wire.FrameReadBufferHandle{}
	errBoom := errors.New("boom")

	var released []*wire.FrameReadBufferHandle
	var closeErr error
	call := 0
	readLoopHooksMu.Lock()
	readLoopReadFrameBuffered = func(_ io.Reader, _ Limits, dst []byte) (Frame, []byte, *wire.FrameReadBufferHandle, error) {
		call++
		switch call {
		case 1:
			if dst != nil {
				t.Fatalf("first read received scratch %v, want nil", dst)
			}
			return Frame{}, oldBuf, oldHandle, nil
		case 2:
			if cap(dst) != cap(oldBuf) {
				t.Fatalf("second read scratch cap = %d, want %d", cap(dst), cap(oldBuf))
			}
			return Frame{}, newBuf, newHandle, errBoom
		default:
			t.Fatalf("unexpected readLoopReadFrameBuffered call %d", call)
			return Frame{}, nil, nil, errBoom
		}
	}
	readLoopHandleFrameBuffered = func(_ *Conn, _ Frame, _ []byte, handle *wire.FrameReadBufferHandle) (bool, error) {
		if handle != oldHandle {
			t.Fatalf("first frame handle = %p, want %p", handle, oldHandle)
		}
		return false, nil
	}
	readLoopRetainReadFrameBufferForPayload = func(buf []byte, _ uint64) []byte {
		if len(buf) == 0 {
			return nil
		}
		if &buf[0] == &oldBuf[0] {
			return buf[:0]
		}
		return nil
	}
	readLoopReleaseReadFrameBuffer = func(_ []byte, handle *wire.FrameReadBufferHandle) {
		released = append(released, handle)
	}
	readLoopCloseSessionWithOptionsForReadErr = func(_ *Conn, err error) {
		closeErr = err
	}
	readLoopHooksMu.Unlock()

	c := &Conn{}
	c.readLoop()

	if !errors.Is(closeErr, errBoom) {
		t.Fatalf("readLoop close err = %v, want %v", closeErr, errBoom)
	}
	if len(released) != 2 {
		t.Fatalf("released handle count = %d, want 2", len(released))
	}
	if released[0] != oldHandle || released[1] != newHandle {
		t.Fatalf("released handles = %p, %p; want %p, %p", released[0], released[1], oldHandle, newHandle)
	}
}

func TestWriteBatchScratchQueuedStreamScratchClearsRetainedStreamRefsOnReuse(t *testing.T) {
	t.Parallel()

	var scratch writeBatchScratch
	first := testBuildDetachedStream(nil, 0)
	second := testBuildDetachedStream(nil, 0)

	queued := scratch.queuedStreamScratch(2)
	scratch.addQueuedStream(first, 1)
	scratch.addQueuedStream(second, 2)
	if len(scratch.queuedStreams) != 2 {
		t.Fatalf("len(scratch.queuedStreams) = %d, want 2", len(scratch.queuedStreams))
	}
	backing := scratch.queuedStreams[:cap(scratch.queuedStreams)]
	if queued[first] != 1 || queued[second] != 2 {
		t.Fatalf("queued map contents = %#v, want both streams tracked", queued)
	}

	_ = scratch.queuedStreamScratch(0)

	if len(scratch.queuedStreams) != 0 {
		t.Fatalf("len(scratch.queuedStreams) = %d, want 0 after reuse", len(scratch.queuedStreams))
	}
	for i := range backing {
		if backing[i] != nil {
			t.Fatalf("stale queued-stream backing entry at %d = %p, want nil", i, backing[i])
		}
	}
	if len(scratch.queuedByStream) != 0 {
		t.Fatalf("len(scratch.queuedByStream) = %d, want 0 after reuse", len(scratch.queuedByStream))
	}
}

func TestSuppressWriteBatchClearsRejectedTailRequestRefs(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := testBuildStream(c, 4, testWithLocalSend(), testWithNotifications())
	stream.setSendFin()
	c.registry.streams[stream.id] = stream

	batch := c.writer.scratch.batchSlice(2, 2)
	batch[0] = writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("keep")}}),
		done:   make(chan error, 1),
	}
	batch[1] = writeRequest{
		frames:               testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("drop")}}),
		done:                 make(chan error, 1),
		origin:               writeRequestOriginStream,
		reservedStream:       stream,
		requestMetaReady:     true,
		requestStreamID:      stream.id,
		requestStreamIDKnown: true,
		requestStreamScoped:  true,
	}

	original := batch
	filtered := c.suppressWriteBatch(batch)
	if len(filtered) != 1 {
		t.Fatalf("len(filtered) = %d, want 1", len(filtered))
	}
	if original[1].frames != nil || original[1].done != nil || original[1].reservedStream != nil {
		t.Fatal("rejected tail request refs not cleared from batch backing array")
	}
}

func TestStreamReadReleasesBufferWhenDrained(t *testing.T) {
	t.Parallel()

	c := &Conn{lifecycle: connLifecycleState{closedCh: make(chan struct{})}}
	stream := testBuildDetachedStream(c, 0)
	stream.readBuf = make([]byte, 64<<10)
	for i := range stream.readBuf {
		stream.readBuf[i] = 'x'
	}
	buf := make([]byte, len(stream.readBuf))

	n, err := stream.Read(buf)
	if err != nil {
		t.Fatalf("Read() err = %v", err)
	}
	if n != len(buf) {
		t.Fatalf("Read() n = %d, want %d", n, len(buf))
	}
	if stream.readBuf != nil {
		t.Fatalf("stream.readBuf = %v, want nil after draining", stream.readBuf)
	}
}

func TestStreamReadTightensOversizedTailBuffer(t *testing.T) {
	t.Parallel()

	const (
		total = 512 << 10
		capN  = 1 << 20
		readN = total - (32 << 10)
	)

	c := &Conn{lifecycle: connLifecycleState{closedCh: make(chan struct{})}}
	stream := testBuildDetachedStream(c, 0)
	stream.readBuf = make([]byte, total, capN)
	for i := range stream.readBuf {
		stream.readBuf[i] = 'x'
	}

	buf := make([]byte, readN)
	n, err := stream.Read(buf)
	if err != nil {
		t.Fatalf("Read() err = %v", err)
	}
	if n != len(buf) {
		t.Fatalf("Read() n = %d, want %d", n, len(buf))
	}
	if got, want := len(stream.readBuf), total-readN; got != want {
		t.Fatalf("len(stream.readBuf) = %d, want %d", got, want)
	}
	if cap(stream.readBuf) != len(stream.readBuf) {
		t.Fatalf("cap(stream.readBuf) = %d, want tightened to len %d", cap(stream.readBuf), len(stream.readBuf))
	}
}

func TestStreamReadKeepsLargeRemainingBuffer(t *testing.T) {
	t.Parallel()

	const (
		total = 512 << 10
		capN  = 1 << 20
		readN = 128 << 10
	)

	c := &Conn{lifecycle: connLifecycleState{closedCh: make(chan struct{})}}
	stream := testBuildDetachedStream(c, 0)
	stream.readBuf = make([]byte, total, capN)
	for i := range stream.readBuf {
		stream.readBuf[i] = 'x'
	}

	beforeCap := cap(stream.readBuf)
	buf := make([]byte, readN)
	n, err := stream.Read(buf)
	if err != nil {
		t.Fatalf("Read() err = %v", err)
	}
	if n != len(buf) {
		t.Fatalf("Read() n = %d, want %d", n, len(buf))
	}
	if len(stream.readBuf) != total-readN {
		t.Fatalf("len(stream.readBuf) = %d, want %d", len(stream.readBuf), total-readN)
	}
	if cap(stream.readBuf) >= beforeCap {
		t.Fatalf("cap(stream.readBuf) = %d, want advanced-slice cap below original %d", cap(stream.readBuf), beforeCap)
	}
	if cap(stream.readBuf) == len(stream.readBuf) {
		t.Fatalf("cap(stream.readBuf) unexpectedly tightened to len for large remaining tail")
	}
}

type delayedWriteConn struct {
	io.ReadWriteCloser
	delayNanos atomic.Int64
}

func (c *delayedWriteConn) SetWriteDelay(d time.Duration) {
	c.delayNanos.Store(int64(d))
}

func (c *delayedWriteConn) Write(p []byte) (int, error) {
	if d := time.Duration(c.delayNanos.Load()); d > 0 {
		time.Sleep(d)
	}
	return c.ReadWriteCloser.Write(p)
}

func newConnPairWithDelayedServer(t *testing.T, clientCfg, serverCfg *Config) (*Conn, *Conn, *delayedWriteConn) {
	t.Helper()

	left, right := net.Pipe()
	delayed := &delayedWriteConn{ReadWriteCloser: right}

	type result struct {
		conn *Conn
		err  error
	}
	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)

	go func() { c, err := Client(left, clientCfg); clientCh <- result{c, err} }()
	go func() { c, err := Server(delayed, serverCfg); serverCh <- result{c, err} }()

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

	return client.conn, server.conn, delayed
}

func TestPingRoundTrip(t *testing.T) {
	t.Parallel()
	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	rtt, err := client.Ping(ctx, []byte("abc"))
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if rtt < 0 {
		t.Fatalf("Ping RTT = %v, want non-negative", rtt)
	}

	stats := client.Stats()
	if stats.PingOutstanding {
		t.Fatal("PingOutstanding = true after successful round-trip")
	}
	if stats.LastPingSentAt.IsZero() {
		t.Fatal("LastPingSentAt = zero, want non-zero")
	}
	if stats.LastPongAt.IsZero() {
		t.Fatal("LastPongAt = zero, want non-zero")
	}
	if stats.LastControlProgress.IsZero() {
		t.Fatal("LastControlProgress = zero, want non-zero")
	}
	if stats.Progress.MuxControlProgressAt != stats.LastControlProgress {
		t.Fatalf("Progress.MuxControlProgressAt = %v, want %v", stats.Progress.MuxControlProgressAt, stats.LastControlProgress)
	}
}

func TestPingWaitsForOutstandingSlot(t *testing.T) {
	client, _, delayed := newConnPairWithDelayedServer(t, nil, nil)
	delayed.SetWriteDelay(80 * time.Millisecond)

	firstDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*testSignalTimeout)
		defer cancel()
		_, err := client.Ping(ctx, nil)
		firstDone <- err
	}()

	awaitConnState(t, client, testSignalTimeout, func(c *Conn) bool {
		return c.Stats().PingOutstanding
	}, "first ping never became outstanding")

	select {
	case err := <-firstDone:
		t.Fatalf("first ping completed before second ping attempt: %v", err)
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_, err := client.Ping(ctx, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Ping err = %v, want deadline exceeded", err)
	}

	delayed.SetWriteDelay(0)

	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Ping err = %v, want nil", err)
		}
	case <-time.After(3 * testSignalTimeout):
		t.Fatal("first Ping did not deliver completion result after delay cleared")
	}

	if client.Stats().PingOutstanding {
		t.Fatal("PingOutstanding = true after first Ping completed")
	}
}

func TestKeepaliveSendsIdlePing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeepaliveInterval = 20 * time.Millisecond

	client, _ := newConnPairWithConfig(t, cfg, nil)

	awaitConnState(t, client, 2*testSignalTimeout, func(c *Conn) bool {
		stats := c.Stats()
		return !stats.LastPingSentAt.IsZero() &&
			!stats.LastPongAt.IsZero() &&
			!stats.PingOutstanding
	}, "keepalive stats still pending; want idle ping and pong")
}

func TestBeginGeneratedPingAllowedWhileDraining(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateDraining
	c.mu.Unlock()

	_, _, err := c.beginGeneratedPing([]byte("ok"), "build PING")
	if err != nil {
		t.Fatalf("beginGeneratedPing while draining: %v", err)
	}

	queued := awaitQueuedFrame(t, frames)
	if queued.Type != FrameTypePING {
		t.Fatalf("queued frame = %+v, want PING", queued)
	}
	if len(queued.Payload) != 10 {
		t.Fatalf("queued payload len = %d, want 10", len(queued.Payload))
	}
	if string(queued.Payload[8:]) != "ok" {
		t.Fatalf("queued echo suffix = %q, want %q", queued.Payload[8:], "ok")
	}

	c.mu.Lock()
	if !c.liveness.pingOutstanding {
		c.mu.Unlock()
		t.Fatal("pingOutstanding = false, want true after draining PING")
	}
	c.mu.Unlock()

	c.failPing(queued.Payload)
}

func TestBeginGeneratedPingRejectedWhenClosing(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = &ApplicationError{Code: uint64(CodeInternal), Reason: "closing"}
	c.mu.Unlock()

	_, _, err := c.beginGeneratedPing(nil, "build PING")
	if !IsErrorCode(err, CodeInternal) {
		t.Fatalf("beginGeneratedPing err = %v, want %s", err, CodeInternal)
	}

	c.mu.Lock()
	if c.liveness.pingOutstanding {
		c.mu.Unlock()
		t.Fatal("pingOutstanding = true, want false after rejected closing PING")
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestSendKeepalivePingIsSilentDuringClosing(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	c.lifecycle.sessionState = connStateClosing
	c.lifecycle.closeErr = &ApplicationError{Code: uint64(CodeInternal), Reason: "closing"}
	c.mu.Unlock()

	if err := c.sendKeepalivePing(); err != nil {
		t.Fatalf("sendKeepalivePing while closing: %v", err)
	}

	c.mu.Lock()
	if c.liveness.pingOutstanding {
		c.mu.Unlock()
		t.Fatal("pingOutstanding = true, want false while closing")
	}
	if !c.liveness.lastPingSentAt.IsZero() {
		c.mu.Unlock()
		t.Fatalf("lastPingSentAt = %v, want zero while closing", c.liveness.lastPingSentAt)
	}
	c.mu.Unlock()
	assertNoQueuedFrame(t, frames)
}

func TestKeepaliveTimeoutSignalsIdleTimeoutError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeepaliveInterval = 10 * time.Millisecond
	cfg.KeepaliveTimeout = 25 * time.Millisecond

	client, server, delayed := newConnPairWithDelayedServer(t, cfg, nil)
	delayed.SetWriteDelay(80 * time.Millisecond)

	awaitConnState(t, client, testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.lifecycle.sessionState == connStateFailed
	}, "keepalive timeout should fail the session")

	err := client.Wait(context.Background())
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("keepalive timeout wait err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeIdleTimeout) {
		t.Fatalf("close code = %d, want %d", appErr.Code, uint64(CodeIdleTimeout))
	}

	awaitConnState(t, server, testSignalTimeout, func(c *Conn) bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.lifecycle.sessionState == connStateFailed
	}, "server should fail after receiving idle-timeout close")

	peerCloseErr := server.PeerCloseError()
	if peerCloseErr == nil {
		t.Fatal("expected server peer close error from client idle-timeout CLOSE")
	}
	if peerCloseErr.Code != uint64(CodeIdleTimeout) {
		t.Fatalf("server peer close code = %d, want %d", peerCloseErr.Code, uint64(CodeIdleTimeout))
	}
	if peerCloseErr.Reason != ErrKeepaliveTimeout.Error() {
		t.Fatalf("server peer close reason = %q, want %q", peerCloseErr.Reason, ErrKeepaliveTimeout.Error())
	}
}

func TestKeepaliveNoTimeoutDoesNotCloseSession(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.KeepaliveInterval = 10 * time.Millisecond
	cfg.KeepaliveTimeout = 0

	client, _, delayed := newConnPairWithDelayedServer(t, cfg, nil)
	delayed.SetWriteDelay(40 * time.Millisecond)

	awaitConnState(t, client, 2*testSignalTimeout, func(c *Conn) bool {
		return !c.Stats().LastPingSentAt.IsZero()
	}, "client never sent keepalive ping")

	select {
	case <-client.lifecycle.closedCh:
		t.Fatal("session closed unexpectedly during keepalive window with timeout disabled")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestOutboundTransportWriteResetsKeepaliveDeadline(t *testing.T) {
	t.Parallel()

	base := time.Unix(123, 0)
	c := &Conn{
		liveness: connLivenessState{
			keepaliveInterval:    50 * time.Millisecond,
			keepaliveJitterState: 1,
			keepaliveDueAt:       base.Add(20 * time.Millisecond),
		},
	}

	c.mu.Lock()
	c.noteTransportWriteLocked(base.Add(10 * time.Millisecond))
	due := c.liveness.keepaliveDueAt
	lastWrite := c.liveness.lastTransportWriteAt
	c.mu.Unlock()

	if !lastWrite.Equal(base.Add(10 * time.Millisecond)) {
		t.Fatalf("lastTransportWriteAt = %v, want %v", lastWrite, base.Add(10*time.Millisecond))
	}
	minDue := base.Add(60 * time.Millisecond)
	if due.Before(minDue) {
		t.Fatalf("keepaliveDueAt = %v, want >= %v after outbound write reset", due, minDue)
	}

	action := c.nextKeepaliveAction(base.Add(55 * time.Millisecond))
	if action.shouldSendPing() {
		t.Fatal("nextKeepaliveAction before reset deadline = send, want wait")
	}
	if action.delay <= 0 {
		t.Fatalf("nextKeepaliveAction delay = %v, want positive wait before reset deadline", action.delay)
	}
}

func TestResetKeepaliveDueAppliesBoundedJitter(t *testing.T) {
	t.Parallel()

	base := time.Unix(456, 0)
	c := &Conn{
		liveness: connLivenessState{
			keepaliveInterval:    80 * time.Millisecond,
			keepaliveJitterState: 1,
		},
	}

	c.mu.Lock()
	c.resetKeepaliveDueLocked(base)
	due := c.liveness.keepaliveDueAt
	c.mu.Unlock()

	minDue := base.Add(80 * time.Millisecond)
	maxDue := minDue.Add(10 * time.Millisecond)
	if due.Before(minDue) || due.After(maxDue) {
		t.Fatalf("keepaliveDueAt = %v, want within [%v, %v]", due, minDue, maxDue)
	}
}

func TestResetKeepaliveDueUsesFreshJitterPerDeadline(t *testing.T) {
	t.Parallel()

	base := time.Unix(789, 0)
	c := &Conn{
		liveness: connLivenessState{
			keepaliveInterval:    64 * time.Millisecond,
			keepaliveJitterState: 1,
		},
	}

	c.mu.Lock()
	c.resetKeepaliveDueLocked(base)
	first := c.liveness.keepaliveDueAt
	c.resetKeepaliveDueLocked(base)
	second := c.liveness.keepaliveDueAt
	c.mu.Unlock()

	if first == second {
		t.Fatalf("keepaliveDueAt repeated identical jitter = %v, want per-deadline resample", first)
	}
}

func TestResetKeepaliveDueDesynchronizesDistinctSessions(t *testing.T) {
	t.Parallel()

	base := time.Unix(900, 0)
	first := &Conn{
		liveness: connLivenessState{
			keepaliveInterval:    time.Second,
			keepaliveJitterState: 1,
		},
	}
	second := &Conn{
		liveness: connLivenessState{
			keepaliveInterval:    time.Second,
			keepaliveJitterState: 2,
		},
	}

	stayedAligned := true
	for i := 0; i < 4; i++ {
		first.mu.Lock()
		first.resetKeepaliveDueLocked(base)
		firstDue := first.liveness.keepaliveDueAt
		first.mu.Unlock()

		second.mu.Lock()
		second.resetKeepaliveDueLocked(base)
		secondDue := second.liveness.keepaliveDueAt
		second.mu.Unlock()

		if firstDue != secondDue {
			stayedAligned = false
			break
		}
	}

	if stayedAligned {
		t.Fatal("distinct sessions kept identical keepalive deadlines across repeated resets, want desynchronization")
	}
}

func TestPingRejectsPayloadOverLimit(t *testing.T) {
	t.Parallel()
	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	_, err := client.Ping(ctx, make([]byte, 4089))
	if !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("Ping err = %v, want %s", err, CodeFrameSize)
	}
}

func pingPayloadEcho(limit uint64) []byte {
	if limit < 8 {
		return nil
	}
	return make([]byte, int(limit-8))
}

func TestPingRejectsPayloadOverLocalLimit(t *testing.T) {
	t.Parallel()

	clientCfg := DefaultConfig()
	clientCfg.Settings.MaxControlPayloadBytes = 4096
	serverCfg := DefaultConfig()
	serverCfg.Settings.MaxControlPayloadBytes = 8192

	client, _ := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	_, err := client.Ping(ctx, make([]byte, 4089))
	if !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("Ping err = %v, want %s", err, CodeFrameSize)
	}
}

func TestPingAcceptsPayloadAtLocalLimit(t *testing.T) {
	t.Parallel()

	clientCfg := DefaultConfig()
	clientCfg.Settings.MaxControlPayloadBytes = 4096
	serverCfg := DefaultConfig()
	serverCfg.Settings.MaxControlPayloadBytes = 8192

	client, _ := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	if _, err := client.Ping(ctx, pingPayloadEcho(clientCfg.Settings.MaxControlPayloadBytes)); err != nil {
		t.Fatalf("Ping err = %v, want nil", err)
	}
}

func TestPingRejectsPayloadOverPeerLimit(t *testing.T) {
	t.Parallel()

	clientCfg := DefaultConfig()
	clientCfg.Settings.MaxControlPayloadBytes = 8192
	serverCfg := DefaultConfig()
	serverCfg.Settings.MaxControlPayloadBytes = 4096

	client, _ := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	_, err := client.Ping(ctx, make([]byte, 4089))
	if !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("Ping err = %v, want %s", err, CodeFrameSize)
	}
}

func TestPingAcceptsPayloadAtPeerLimit(t *testing.T) {
	t.Parallel()

	clientCfg := DefaultConfig()
	clientCfg.Settings.MaxControlPayloadBytes = 8192
	serverCfg := DefaultConfig()
	serverCfg.Settings.MaxControlPayloadBytes = 4096

	client, _ := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	if _, err := client.Ping(ctx, pingPayloadEcho(serverCfg.Settings.MaxControlPayloadBytes)); err != nil {
		t.Fatalf("Ping err = %v, want nil", err)
	}
}

func waitKeepaliveWake(t *testing.T, ch <-chan struct{}, closed <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-closed:
		select {
		case <-ch:
		default:
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for keepalive wake")
	}
}

func TestNotifyKeepaliveWakeCoalescesSignals(t *testing.T) {
	t.Parallel()

	c := &Conn{}

	c.mu.Lock()
	first := c.ensureLivenessNotifyLocked()
	notify(c.signals.livenessCh)
	notify(c.signals.livenessCh)
	c.mu.Unlock()

	select {
	case <-first:
	default:
		t.Fatal("expected keepalive wake signal")
	}

	select {
	case <-first:
		t.Fatal("unexpected second keepalive wake signal for coalesced notifications")
	default:
	}
}

func TestWaitKeepaliveWakeReturnsOnSignalAndClose(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.mu.Lock()
	ch := c.ensureLivenessNotifyLocked()
	closed := make(chan struct{})
	c.lifecycle.closedCh = closed
	notify(c.signals.livenessCh)
	c.mu.Unlock()

	waitKeepaliveWake(t, ch, closed)

	close(closed)
	waitKeepaliveWake(t, ch, closed)
}

func TestHandleFramePingCopiesPayloadBeforeQueueingPong(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	payload := []byte("ping-echo")
	if err := c.handleFrame(Frame{Type: FrameTypePING, Payload: payload}); err != nil {
		t.Fatalf("handleFrame(PING) err = %v", err)
	}

	copy(payload, "mutated!!")

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypePONG {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypePONG)
	}
	if !bytes.Equal(frame.Payload, []byte("ping-echo")) {
		t.Fatalf("queued PONG payload = %q, want %q", frame.Payload, "ping-echo")
	}
}

func TestBeginPingCopiesCallerPayloadBeforeQueueingPing(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	payload := []byte("ping-echo")
	done, _, err := c.beginPing(payload)
	if err != nil {
		t.Fatalf("beginPing() err = %v", err)
	}
	if done == nil {
		t.Fatal("beginPing() done = nil, want channel")
	}

	copy(payload, "mutated!!")

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypePING {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypePING)
	}
	if !bytes.Equal(frame.Payload, []byte("ping-echo")) {
		t.Fatalf("queued PING payload = %q, want %q", frame.Payload, "ping-echo")
	}

	c.mu.Lock()
	retained := append([]byte(nil), c.liveness.pingPayload...)
	c.mu.Unlock()
	if !bytes.Equal(retained, []byte("ping-echo")) {
		t.Fatalf("retained ping payload = %q, want %q", retained, "ping-echo")
	}
}

func TestParseFrameExamples(t *testing.T) {
	t.Parallel()
	limits := DefaultSettings().Limits()
	tests := []struct {
		name     string
		hex      string
		wantType FrameType
		wantCode byte
		streamID uint64
		payload  []byte
	}{
		{
			name:     "data hi",
			hex:      "04 01 04 68 69",
			wantType: FrameTypeDATA,
			wantCode: 0x01,
			streamID: 4,
			payload:  []byte("hi"),
		},
		{
			name:     "data fin empty",
			hex:      "02 41 04",
			wantType: FrameTypeDATA,
			wantCode: 0x41,
			streamID: 4,
			payload:  nil,
		},
		{
			name:     "session max_data",
			hex:      "04 02 00 44 00",
			wantType: FrameTypeMAXDATA,
			wantCode: 0x02,
			streamID: 0,
			payload:  mustHex(t, "44 00"),
		},
		{
			name:     "ping",
			hex:      "0a 04 00 01 02 03 04 05 06 07 08",
			wantType: FrameTypePING,
			wantCode: 0x04,
			streamID: 0,
			payload:  mustHex(t, "01 02 03 04 05 06 07 08"),
		},
		{
			name:     "priority update",
			hex:      "06 0b 04 01 01 01 02",
			wantType: FrameTypeEXT,
			wantCode: 0x0b,
			streamID: 4,
			payload:  mustHex(t, "01 01 01 02"),
		},
		{
			name:     "data open metadata",
			hex:      "08 21 04 03 01 01 02 68 69",
			wantType: FrameTypeDATA,
			wantCode: 0x21,
			streamID: 4,
			payload:  mustHex(t, "03 01 01 02 68 69"),
		},
		{
			name:     "abort with debug_text",
			hex:      "07 08 04 04 01 02 6e 6f",
			wantType: FrameTypeABORT,
			wantCode: 0x08,
			streamID: 4,
			payload:  mustHex(t, "04 01 02 6e 6f"),
		},
		{
			name:     "goaway",
			hex:      "05 09 00 04 00 00",
			wantType: FrameTypeGOAWAY,
			wantCode: 0x09,
			streamID: 0,
			payload:  mustHex(t, "04 00 00"),
		},
	}

	for _, tt := range tests {
		raw := mustHex(t, tt.hex)
		frame, n, err := ParseFrame(raw, limits)
		if err != nil {
			t.Fatalf("%s: parse: %v", tt.name, err)
		}
		if n != len(raw) {
			t.Fatalf("%s: consumed %d bytes, want %d", tt.name, n, len(raw))
		}
		if frame.Type != tt.wantType {
			t.Fatalf("%s: type = %v, want %v", tt.name, frame.Type, tt.wantType)
		}
		if frame.Code() != tt.wantCode {
			t.Fatalf("%s: code = 0x%02x, want 0x%02x", tt.name, frame.Code(), tt.wantCode)
		}
		if frame.StreamID != tt.streamID {
			t.Fatalf("%s: stream id = %d, want %d", tt.name, frame.StreamID, tt.streamID)
		}
		if string(frame.Payload) != string(tt.payload) {
			t.Fatalf("%s: payload = %x, want %x", tt.name, frame.Payload, tt.payload)
		}

		encoded, err := frame.MarshalBinary()
		if err != nil {
			t.Fatalf("%s: marshal: %v", tt.name, err)
		}
		if string(encoded) != string(raw) {
			t.Fatalf("%s: marshal mismatch = %x, want %x", tt.name, encoded, raw)
		}
	}
}

func TestParseFrameRejectsNonCanonicalLength(t *testing.T) {
	t.Parallel()
	raw := mustHex(t, "40 04 01 04 68 69")
	if _, _, err := ParseFrame(raw, DefaultSettings().Limits()); err == nil {
		t.Fatal("expected non-canonical frame_length to fail")
	} else if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("got %v, want %s", err, CodeProtocol)
	}
}

func TestParseFrameRejectsTruncatedStopSending(t *testing.T) {
	t.Parallel()
	raw := mustHex(t, "02 03 04")
	if _, _, err := ParseFrame(raw, DefaultSettings().Limits()); err == nil {
		t.Fatal("expected truncated STOP_SENDING to fail")
	} else if !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("got %v, want %s", err, CodeFrameSize)
	}
}

func TestParseFrameRejectsInvalidFlagsOnNonDATA(t *testing.T) {
	t.Parallel()
	raw := mustHex(t, "03 27 04 08")
	if _, _, err := ParseFrame(raw, DefaultSettings().Limits()); err == nil {
		t.Fatal("expected invalid flags to fail")
	} else if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("got %v, want %s", err, CodeProtocol)
	}
}

func TestReadFrameExamples(t *testing.T) {
	t.Parallel()

	cases := []string{
		"04 01 04 68 69",
		"02 41 04",
		"04 02 00 44 00",
		"0a 04 00 01 02 03 04 05 06 07 08",
		"06 0b 04 01 01 01 02",
	}

	for _, rawHex := range cases {
		raw := mustHex(t, rawHex)
		frame, err := ReadFrame(bytes.NewReader(raw), DefaultSettings().Limits())
		if err != nil {
			t.Fatalf("read %s: %v", rawHex, err)
		}
		encoded, err := frame.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal %s: %v", rawHex, err)
		}
		if string(encoded) != string(raw) {
			t.Fatalf("read/marshal mismatch for %s = %x, want %x", rawHex, encoded, raw)
		}
	}
}

func TestRetainReadFrameBufferForPayloadLimitKeepsConfiguredLargeBuffer(t *testing.T) {
	t.Parallel()

	maxFramePayload := DefaultSettings().MaxFramePayload * 2
	buf := make([]byte, 0, retainedReadFrameBufferLimit(maxFramePayload))

	retained := retainReadFrameBufferForPayloadLimit(buf, maxFramePayload)
	if retained == nil {
		t.Fatal("retainReadFrameBufferForPayloadLimit() = nil, want retained buffer")
	}
	if len(retained) != 0 {
		t.Fatalf("len(retained) = %d, want 0", len(retained))
	}
	if cap(retained) != cap(buf) {
		t.Fatalf("cap(retained) = %d, want %d", cap(retained), cap(buf))
	}
}

func TestRetainReadFrameBufferForPayloadLimitDropsBufferAboveConfiguredLimit(t *testing.T) {
	t.Parallel()

	maxFramePayload := DefaultSettings().MaxFramePayload / 2
	buf := make([]byte, 0, retainedReadFrameBufferLimit(maxFramePayload)+1)

	if retained := retainReadFrameBufferForPayloadLimit(buf, maxFramePayload); retained != nil {
		t.Fatalf("retainReadFrameBufferForPayloadLimit() = %v, want nil", retained)
	}
}

func TestVarintRoundTrip(t *testing.T) {
	t.Parallel()
	values := []uint64{
		0,
		1,
		63,
		64,
		255,
		16383,
		16384,
		1<<20 + 7,
		1073741823,
		1073741824,
		MaxVarint62,
	}

	for _, value := range values {
		encoded, err := EncodeVarint(value)
		if err != nil {
			t.Fatalf("encode %d: %v", value, err)
		}
		decoded, n, err := ParseVarint(encoded)
		if err != nil {
			t.Fatalf("parse %d: %v", value, err)
		}
		if decoded != value {
			t.Fatalf("roundtrip %d -> %d", value, decoded)
		}
		if n != len(encoded) {
			t.Fatalf("roundtrip %d consumed %d bytes, want %d", value, n, len(encoded))
		}
	}
}

func TestVarintRejectsNonCanonical(t *testing.T) {
	t.Parallel()
	if _, _, err := ParseVarint(mustHex(t, "40 04")); err == nil {
		t.Fatal("expected non-canonical varint to fail")
	} else if !errors.Is(err, errNonCanonicalVarint) {
		t.Fatalf("got %v, want %v", err, errNonCanonicalVarint)
	}
}

func TestVarintRejectsTruncated(t *testing.T) {
	t.Parallel()
	if _, _, err := ParseVarint(mustHex(t, "80 00 01")); err == nil {
		t.Fatal("expected truncated varint to fail")
	} else if !errors.Is(err, errTruncatedVarint) {
		t.Fatalf("got %v, want %v", err, errTruncatedVarint)
	}
}

func TestReadVarintRoundTrip(t *testing.T) {
	t.Parallel()

	values := []uint64{
		0,
		1,
		63,
		64,
		255,
		16383,
		16384,
		1<<20 + 7,
		1073741823,
		1073741824,
		MaxVarint62,
	}

	for _, value := range values {
		encoded, err := EncodeVarint(value)
		if err != nil {
			t.Fatalf("encode %d: %v", value, err)
		}
		decoded, n, err := ReadVarint(bytes.NewReader(encoded))
		if err != nil {
			t.Fatalf("read %d: %v", value, err)
		}
		if decoded != value {
			t.Fatalf("read roundtrip %d -> %d", value, decoded)
		}
		if n != len(encoded) {
			t.Fatalf("read roundtrip %d consumed %d bytes, want %d", value, n, len(encoded))
		}
	}
}
