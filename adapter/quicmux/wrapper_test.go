package quicmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/adaptertest"
	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestQUICSessionContract(t *testing.T) {
	adaptertest.RunSessionContract(t, newWrappedPair)
}

func TestWrapSessionNilIsClosedSafeSession(t *testing.T) {
	session := WrapSession(nil)
	var nilCtx context.Context
	if session == nil {
		t.Fatal("WrapSession(nil) = nil, want closed session wrapper")
	}
	if !session.Closed() {
		t.Fatal("WrapSession(nil).Closed() = false, want true")
	}
	if session.State() != zmux.SessionStateInvalid {
		t.Fatalf("WrapSession(nil).State() = %v, want %v", session.State(), zmux.SessionStateInvalid)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("WrapSession(nil).Close() err = %v, want nil", err)
	}
	if err := session.Wait(nilCtx); err != nil {
		t.Fatalf("WrapSession(nil).Wait(nil) err = %v, want nil", err)
	}
	if got := session.Stats().State; got != zmux.SessionStateInvalid {
		t.Fatalf("WrapSession(nil).Stats().State = %v, want %v", got, zmux.SessionStateInvalid)
	}
}

func TestWrapSessionWaitReturnsNilAfterGracefulClose(t *testing.T) {
	client, _ := newWrappedPair(t)

	if err := client.Close(); err != nil {
		t.Fatalf("Close err = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Wait(ctx); err != nil {
		t.Fatalf("Wait err = %v, want nil", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close err = %v, want nil", err)
	}
}

func TestWrapSessionCloseReadUsesCancelledCode(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if _, err := clientStream.Write([]byte("p")); err != nil {
		t.Fatalf("initial Write err = %v", err)
	}
	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}

	if err := accepted.stream.CloseRead(); err != nil {
		t.Fatalf("CloseRead err = %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	_ = clientStream.SetWriteDeadline(deadline)
	for time.Now().Before(deadline) {
		_, err := clientStream.Write([]byte("x"))
		if err == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok {
			t.Fatalf("Write err = %v, want ApplicationError(CodeCancelled)", err)
		}
		if appErr.Code != uint64(zmux.CodeCancelled) {
			t.Fatalf("Write code = %d, want %d", appErr.Code, uint64(zmux.CodeCancelled))
		}
		return
	}
	t.Fatal("client write did not observe CANCELLED after peer CloseRead")
}

func TestWrapSessionFreshCloseReadSubmitsPreludeBeforeStopSending(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if err := clientStream.CloseRead(); err != nil {
		t.Fatalf("CloseRead err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}
	if got := accepted.stream.OpenInfo(); len(got) != 0 {
		t.Fatalf("accepted OpenInfo len = %d, want 0", len(got))
	}

	deadline := time.Now().Add(3 * time.Second)
	_ = accepted.stream.SetWriteDeadline(deadline)
	for time.Now().Before(deadline) {
		_, err := accepted.stream.Write([]byte("x"))
		if err == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok {
			t.Fatalf("accepted Write err = %v, want ApplicationError(CodeCancelled)", err)
		}
		if appErr.Code != uint64(zmux.CodeCancelled) {
			t.Fatalf("accepted Write code = %d, want %d", appErr.Code, uint64(zmux.CodeCancelled))
		}
		return
	}
	t.Fatal("accepted write did not observe CANCELLED after fresh peer CloseRead")
}

func TestWrapSessionUniCloseMatchesAvailableDirection(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.RecvStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptUniStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	send, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("OpenUniStream err = %v", err)
	}
	payload := []byte("uni-close")
	if _, err := send.Write(payload[:1]); err != nil {
		t.Fatalf("initial Write err = %v", err)
	}
	recv := <-acceptCh
	if recv.err != nil {
		t.Fatalf("AcceptUniStream err = %v", recv.err)
	}
	if _, err := send.Write(payload[1:]); err != nil {
		t.Fatalf("Write err = %v", err)
	}
	if err := send.Close(); err != nil {
		t.Fatalf("send Close err = %v", err)
	}

	got, err := io.ReadAll(recv.stream)
	if err != nil {
		t.Fatalf("ReadAll err = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadAll = %q, want %q", got, payload)
	}

	if err := recv.stream.Close(); err != nil {
		t.Fatalf("recv Close err = %v", err)
	}
}

func TestWrapSessionOpenMetadataVisibleOnAccept(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	priority := uint64(7)
	group := uint64(11)

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStreamWithOptions(ctx, zmux.OpenOptions{
		InitialPriority: &priority,
		InitialGroup:    &group,
		OpenInfo:        []byte("ssh"),
	})
	if err != nil {
		t.Fatalf("OpenStreamWithOptions err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}

	meta := accepted.stream.Metadata()
	if meta.Priority != priority {
		t.Fatalf("accepted Priority = %d, want %d", meta.Priority, priority)
	}
	if meta.Group == nil || *meta.Group != group {
		t.Fatalf("accepted Group = %v, want %d", meta.Group, group)
	}
	if got := string(meta.OpenInfo); got != "ssh" {
		t.Fatalf("accepted OpenInfo = %q, want %q", got, "ssh")
	}
	if got := string(accepted.stream.OpenInfo()); got != "ssh" {
		t.Fatalf("accepted OpenInfo() = %q, want %q", got, "ssh")
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionUpdateMetadataBeforeVisibilityUsesPrelude(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	priority := uint64(5)
	group := uint64(9)

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if err := stream.UpdateMetadata(zmux.MetadataUpdate{
		Priority: &priority,
		Group:    &group,
	}); err != nil {
		t.Fatalf("UpdateMetadata err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}

	meta := accepted.stream.Metadata()
	if meta.Priority != priority {
		t.Fatalf("accepted Priority = %d, want %d", meta.Priority, priority)
	}
	if meta.Group == nil || *meta.Group != group {
		t.Fatalf("accepted Group = %v, want %d", meta.Group, group)
	}
	if meta.OpenInfo != nil {
		t.Fatalf("accepted OpenInfo = %v, want nil", meta.OpenInfo)
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionUpdateMetadataAfterVisibilityUnavailable(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("Write err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}

	priority := uint64(13)
	err = stream.UpdateMetadata(zmux.MetadataUpdate{Priority: &priority})
	if !errors.Is(err, zmux.ErrPriorityUpdateUnavailable) {
		t.Fatalf("UpdateMetadata err = %v, want %v", err, zmux.ErrPriorityUpdateUnavailable)
	}
	if !errors.Is(err, zmux.ErrAdapterUnsupported) {
		t.Fatalf("UpdateMetadata err = %v, want %v", err, zmux.ErrAdapterUnsupported)
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionAcceptStreamReturnsReadyStreamAheadOfStalledPrelude(t *testing.T) {
	client, server := newWrappedPairWithOptions(t, SessionOptions{}, SessionOptions{
		AcceptedPreludeReadTimeout: 100 * time.Millisecond,
	})
	clientConn, _ := client.(*quicSession)
	serverConn, _ := server.(*quicSession)
	if clientConn == nil || serverConn == nil {
		t.Fatal("wrapped sessions = nil adapter sessions, want quicSession")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stalled, err := clientConn.conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("raw OpenStreamSync stalled err = %v", err)
	}
	t.Cleanup(func() {
		stalled.CancelRead(0)
		stalled.CancelWrite(0)
		_ = stalled.Close()
	})

	ready, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream ready err = %v", err)
	}
	t.Cleanup(func() {
		_ = ready.Close()
	})
	if _, err := ready.Write([]byte("x")); err != nil {
		t.Fatalf("ready Write err = %v", err)
	}

	accepted, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("first AcceptStream err = %v", err)
	}
	if accepted.StreamID() != ready.StreamID() {
		t.Fatalf("accepted StreamID = %d, want %d", accepted.StreamID(), ready.StreamID())
	}

	buf := make([]byte, 1)
	if _, err := io.ReadFull(accepted, buf); err != nil {
		t.Fatalf("ReadFull err = %v", err)
	}
	if !bytes.Equal(buf, []byte("x")) {
		t.Fatalf("ReadFull = %q, want %q", buf, []byte("x"))
	}
	_ = accepted.Close()

	time.Sleep(250 * time.Millisecond)

	next, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream next err = %v", err)
	}
	t.Cleanup(func() {
		_ = next.Close()
	})
	if _, err := next.Write([]byte("y")); err != nil {
		t.Fatalf("next Write err = %v", err)
	}

	acceptedNext, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("second AcceptStream err = %v", err)
	}
	if acceptedNext.StreamID() != next.StreamID() {
		t.Fatalf("second accepted StreamID = %d, want %d", acceptedNext.StreamID(), next.StreamID())
	}
	_ = acceptedNext.Close()
}

func TestWrapSessionConcurrentAcceptStreamAllowsReadyStreamsToBypassStalledPrelude(t *testing.T) {
	client, server := newWrappedPairWithOptions(t, SessionOptions{}, SessionOptions{
		AcceptedPreludeReadTimeout: 100 * time.Millisecond,
	})
	clientConn, _ := client.(*quicSession)
	serverConn, _ := server.(*quicSession)
	if clientConn == nil || serverConn == nil {
		t.Fatal("wrapped sessions = nil adapter sessions, want quicSession")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stalled, err := clientConn.conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("raw OpenStreamSync stalled err = %v", err)
	}
	t.Cleanup(func() {
		stalled.CancelRead(0)
		stalled.CancelWrite(0)
		_ = stalled.Close()
	})

	ready, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream ready err = %v", err)
	}
	t.Cleanup(func() {
		_ = ready.Close()
	})
	if _, err := ready.Write([]byte("x")); err != nil {
		t.Fatalf("ready Write err = %v", err)
	}
	secondReady, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream second ready err = %v", err)
	}
	t.Cleanup(func() {
		_ = secondReady.Close()
	})
	if _, err := secondReady.Write([]byte("y")); err != nil {
		t.Fatalf("second ready Write err = %v", err)
	}

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 2)
	for range 2 {
		go func() {
			stream, err := server.AcceptStream(ctx)
			acceptCh <- acceptResult{stream: stream, err: err}
		}()
	}

	first := <-acceptCh
	second := <-acceptCh
	results := []acceptResult{first, second}
	wantIDs := map[uint64]struct{}{
		ready.StreamID():       {},
		secondReady.StreamID(): {},
	}
	for i, result := range results {
		if result.err != nil {
			t.Fatalf("accept result %d err = %v", i, result.err)
		}
		if result.stream == nil {
			t.Fatalf("accept result %d stream = nil, want ready stream", i)
		}
		if _, ok := wantIDs[result.stream.StreamID()]; !ok {
			t.Fatalf("accept result %d StreamID = %d, want one of %v", i, result.stream.StreamID(), []uint64{ready.StreamID(), secondReady.StreamID()})
		}
		delete(wantIDs, result.stream.StreamID())
		_ = result.stream.Close()
	}
	if len(wantIDs) != 0 {
		t.Fatalf("missing accepted StreamIDs: %v", wantIDs)
	}
}

func TestWrapSessionAcceptLoopBoundsConcurrentPreludePreparation(t *testing.T) {
	limit := 3
	clientConn, serverConn := newQUICConnPair(t)
	countedServerConn := &countingSessionConn{SessionConn: serverConn}
	serverSession := WrapSessionWithOptions(countedServerConn, SessionOptions{
		AcceptedPreludeReadTimeout:   -1,
		AcceptedPreludeMaxConcurrent: limit,
	})
	serverAdapter, _ := serverSession.(*quicSession)
	if serverAdapter == nil {
		t.Fatal("wrapped server session = nil adapter session, want quicSession")
	}
	_ = serverAdapter.ensureBidiAcceptLoop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	streams := make([]*quic.Stream, 0, limit+8)
	for i := 0; i < limit+8; i++ {
		stream, err := clientConn.OpenStreamSync(ctx)
		if err != nil {
			t.Fatalf("OpenStreamSync %d err = %v", i, err)
		}
		if _, err := stream.Write([]byte{0x40}); err != nil {
			t.Fatalf("stream %d partial prelude write err = %v", i, err)
		}
		streams = append(streams, stream)
	}
	for _, stream := range streams {
		stream := stream
		t.Cleanup(func() {
			stream.CancelRead(0)
			stream.CancelWrite(0)
			_ = stream.Close()
		})
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(serverAdapter.prepareSem) == limit {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := len(serverAdapter.prepareSem); got != limit {
		t.Fatalf("accepted prelude prepare slots in use = %d, want %d", got, limit)
	}

	time.Sleep(150 * time.Millisecond)
	if got := len(serverAdapter.prepareSem); got != limit {
		t.Fatalf("accepted prelude prepare slots after wait = %d, want still %d while stalled preludes occupy all slots", got, limit)
	}
	if got := countedServerConn.bidiAcceptCount(); got > limit+1 {
		t.Fatalf("accepted bidi stream count = %d, want at most %d while prepare slots are saturated", got, limit+1)
	}
}

func TestNormalizeAcceptedPreludeMaxConcurrent(t *testing.T) {
	previous := DefaultAcceptedPreludeMaxConcurrent()
	SetDefaultAcceptedPreludeMaxConcurrent(0)
	t.Cleanup(func() {
		SetDefaultAcceptedPreludeMaxConcurrent(previous)
	})

	if got := normalizeAcceptedPreludeMaxConcurrent(0); got != defaultAcceptedPreludeMaxConcurrent {
		t.Fatalf("normalizeAcceptedPreludeMaxConcurrent(0) = %d, want %d", got, defaultAcceptedPreludeMaxConcurrent)
	}

	SetDefaultAcceptedPreludeMaxConcurrent(5)
	if got := normalizeAcceptedPreludeMaxConcurrent(0); got != 5 {
		t.Fatalf("normalizeAcceptedPreludeMaxConcurrent(0) after default override = %d, want 5", got)
	}
	if got := normalizeAcceptedPreludeMaxConcurrent(2); got != 2 {
		t.Fatalf("normalizeAcceptedPreludeMaxConcurrent(2) = %d, want 2", got)
	}
}

func TestWrapSessionCancelWriteMakesLocalWriteFailImmediately(t *testing.T) {
	client, _ := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if err := stream.CancelWrite(44); err != nil {
		t.Fatalf("CancelWrite err = %v", err)
	}
	if n, err := stream.Write(nil); err != nil || n != 0 {
		t.Fatalf("zero-length Write after CancelWrite = (%d, %v), want (0, nil)", n, err)
	}
	if _, err := stream.Write([]byte("x")); err == nil {
		t.Fatal("Write after CancelWrite err = nil, want application error")
	} else {
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != 44 {
			t.Fatalf("Write after CancelWrite err = %v, want ApplicationError(44)", err)
		}
	}
}

func TestWrapSessionCloseWithErrorMakesLocalBidiOpsFailImmediately(t *testing.T) {
	client, _ := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if err := stream.CloseWithError(55, "bye"); err != nil {
		t.Fatalf("CloseWithError err = %v", err)
	}
	if _, err := stream.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read after CloseWithError err = nil, want application error")
	} else {
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != 55 || appErr.Reason != "bye" {
			t.Fatalf("Read after CloseWithError err = %v, want ApplicationError(55, \"bye\")", err)
		}
	}
	if _, err := stream.Write([]byte("x")); err == nil {
		t.Fatal("Write after CloseWithError err = nil, want application error")
	} else {
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != 55 || appErr.Reason != "bye" {
			t.Fatalf("Write after CloseWithError err = %v, want ApplicationError(55, \"bye\")", err)
		}
	}
}

func TestWrapSessionCloseWithErrorMakesLocalSendWriteFailImmediately(t *testing.T) {
	client, _ := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("OpenUniStream err = %v", err)
	}
	if err := stream.CloseWithError(66, "bye"); err != nil {
		t.Fatalf("CloseWithError err = %v", err)
	}
	if _, err := stream.Write([]byte("x")); err == nil {
		t.Fatal("Write after CloseWithError err = nil, want application error")
	} else {
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != 66 || appErr.Reason != "bye" {
			t.Fatalf("Write after CloseWithError err = %v, want ApplicationError(66, \"bye\")", err)
		}
	}
}

func TestWrapSessionZeroLengthWriteDoesNotSubmitOpenPrelude(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if n, err := stream.Write(nil); err != nil || n != 0 {
		t.Fatalf("zero-length Write = (%d, %v), want (0, nil)", n, err)
	}

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	if accepted, err := server.AcceptStream(shortCtx); err == nil {
		_ = accepted.Close()
		shortCancel()
		t.Fatal("zero-length Write made stream peer-visible before payload/final intent")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		shortCancel()
		t.Fatalf("short AcceptStream err = %v, want context deadline", err)
	}
	shortCancel()

	if n, err := stream.Write([]byte("x")); err != nil || n != 1 {
		t.Fatalf("payload Write = (%d, %v), want (1, nil)", n, err)
	}
	accepted, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream after payload err = %v", err)
	}
	if got, err := io.ReadAll(io.LimitReader(accepted, 1)); err != nil || string(got) != "x" {
		t.Fatalf("accepted payload = %q, %v; want x, nil", got, err)
	}

	_ = accepted.Close()
	_ = stream.Close()
}

func TestEnsureOpenPreludeResumesAfterPartialWriteError(t *testing.T) {
	writer := &partialPreludeWriter{
		failAfter: 3,
		failErr:   io.ErrUnexpectedEOF,
	}
	base := &quicStreamBase{}
	priority := uint64(7)
	group := uint64(9)
	initLocalStreamBase(base, nil, nil, writer, zmux.OpenOptions{
		InitialPriority: &priority,
		InitialGroup:    &group,
		OpenInfo:        []byte("ssh"),
	})

	prelude, err := base.prepareOpenPrelude()
	if err != nil {
		t.Fatalf("prepareOpenPrelude err = %v", err)
	}
	if err := base.ensureOpenPrelude(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("first ensureOpenPrelude err = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if err := base.ensureOpenPrelude(); err != nil {
		t.Fatalf("second ensureOpenPrelude err = %v", err)
	}
	if got := writer.Bytes(); !bytes.Equal(got, prelude) {
		t.Fatalf("written prelude = %x, want %x", got, prelude)
	}
	if base.preludeOffset != len(prelude) {
		t.Fatalf("preludeOffset = %d, want %d", base.preludeOffset, len(prelude))
	}
	if !base.preludeSent {
		t.Fatal("preludeSent = false, want true")
	}
	if base.prelude != nil {
		t.Fatal("prelude retained after successful send, want released buffer")
	}
}

func TestEnsureOpenPreludeKeepsBufferUntilFullySent(t *testing.T) {
	writer := &partialPreludeWriter{
		failAfter: 3,
		failErr:   io.ErrUnexpectedEOF,
	}
	base := &quicStreamBase{}
	priority := uint64(7)
	initLocalStreamBase(base, nil, nil, writer, zmux.OpenOptions{
		InitialPriority: &priority,
		OpenInfo:        bytes.Repeat([]byte("x"), 1024),
	})

	if err := base.ensureOpenPrelude(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ensureOpenPrelude err = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if base.prelude == nil {
		t.Fatal("prelude buffer released after partial write failure, want retained for retry")
	}
	if base.preludeSent {
		t.Fatal("preludeSent = true after partial write failure, want false")
	}
}

func TestEnsureOpenPreludeWithContextCancelsBlockedWrite(t *testing.T) {
	writer := newDeadlineAwareWriter()
	base := &quicStreamBase{}
	initLocalStreamBase(base, nil, nil, writer, zmux.OpenOptions{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- base.ensureOpenPreludeWithContext(ctx, writer)
	}()

	<-writer.started
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("ensureOpenPreludeWithContext err = %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ensureOpenPreludeWithContext did not unblock after context cancellation")
	}

	deadlines := writer.deadlineCalls()
	if len(deadlines) < 2 {
		t.Fatalf("deadline call count = %d, want at least 2", len(deadlines))
	}
	if deadlines[0].IsZero() {
		t.Fatalf("first deadline = %v, want non-zero cancellation deadline", deadlines[0])
	}
	if !deadlines[len(deadlines)-1].IsZero() {
		t.Fatalf("last deadline = %v, want cleared zero deadline", deadlines[len(deadlines)-1])
	}
}

func TestWritePayloadAndCloseWriteSerializeUnderlyingClose(t *testing.T) {
	writer := newConcurrentDetectWriteCloser()
	base := &quicStreamBase{
		sendPrelude: false,
		preludeSent: true,
	}

	writeDone := make(chan error, 1)
	go func() {
		n, err := base.writePayload(writer, []byte("x"))
		if err == nil && n != 1 {
			err = fmt.Errorf("write n = %d, want 1", n)
		}
		writeDone <- err
	}()

	<-writer.writeStarted
	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- base.closeWrite(writer)
	}()
	<-closeStarted

	select {
	case <-writer.closeCalled:
		t.Fatal("underlying Close ran concurrently with Write")
	case <-time.After(50 * time.Millisecond):
	}

	close(writer.releaseWrite)
	if err := <-writeDone; err != nil {
		t.Fatalf("writePayload err = %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("closeWrite err = %v", err)
	}
	if writer.concurrent() {
		t.Fatal("underlying writer observed concurrent Write and Close")
	}
	if !base.localWriteClosed.Load() {
		t.Fatal("localWriteClosed = false, want true")
	}
}

func TestCloseWriteTerminalStateWinsWhileUnderlyingCloseBlocks(t *testing.T) {
	base := &quicStreamBase{}
	writer := newBlockingCloseWriteCloser()
	defer writer.release()

	done := make(chan error, 1)
	go func() {
		done <- base.closeWrite(writer)
	}()

	select {
	case <-writer.closeStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("underlying Close did not start")
	}

	if !base.localWriteClosed.Load() {
		t.Fatal("localWriteClosed = false while underlying Close is blocked")
	}
	if base.markLocalWriteClosed(&zmux.ApplicationError{Code: 99}) {
		t.Fatal("second write terminal action won after graceful CloseWrite started")
	}
	if err := base.loadLocalWriteErr(); err != nil {
		t.Fatalf("localWriteErr = %v, want nil graceful close cause", err)
	}

	writer.release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("closeWrite err = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("closeWrite did not return after releasing underlying Close")
	}
}

func TestInstallContextWriteDeadlineDoesNotLeaveStaleExpiredDeadline(t *testing.T) {
	setter := newBlockedWriteDeadlineSetter()
	ctx, cancel := context.WithCancel(context.Background())
	restore := installContextWriteDeadline(ctx, setter)

	cancel()
	<-setter.nonZeroStarted

	restoreDone := make(chan struct{})
	go func() {
		defer close(restoreDone)
		restore()
	}()

	select {
	case <-restoreDone:
		t.Fatal("restore returned before in-flight cancellation deadline write was released")
	case <-time.After(50 * time.Millisecond):
	}

	close(setter.releaseNonZero)

	select {
	case <-restoreDone:
	case <-time.After(3 * time.Second):
		t.Fatal("restore did not finish after releasing cancellation deadline write")
	}

	deadlines := setter.deadlineCalls()
	if len(deadlines) < 2 {
		t.Fatalf("deadline call count = %d, want at least 2", len(deadlines))
	}
	if !deadlines[len(deadlines)-1].IsZero() {
		t.Fatalf("last deadline = %v, want cleared zero deadline", deadlines[len(deadlines)-1])
	}
}

func TestPreferLocalWriteErrorReturnsLocalApplicationError(t *testing.T) {
	base := &quicStreamBase{}
	appErr := &zmux.ApplicationError{Code: 77, Reason: "local-abort"}
	base.localWriteClosed.Store(true)
	base.storeLocalWriteErr(appErr)

	if got := base.preferLocalWriteError(io.ErrUnexpectedEOF); got != appErr {
		t.Fatalf("preferLocalWriteError = %v, want %v", got, appErr)
	}
}

func TestWrapSessionCloseReadReturnsErrReadClosedLocally(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("Write err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}
	if err := accepted.stream.CloseRead(); err != nil {
		t.Fatalf("CloseRead err = %v", err)
	}
	if _, err := accepted.stream.Read(make([]byte, 1)); !errors.Is(err, zmux.ErrReadClosed) {
		t.Fatalf("Read err = %v, want %v", err, zmux.ErrReadClosed)
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionCloseWriteReturnsErrWriteClosedLocally(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("Write err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v", err)
	}
	if n, err := stream.Write(nil); err != nil || n != 0 {
		t.Fatalf("zero-length Write after CloseWrite = (%d, %v), want (0, nil)", n, err)
	}
	if _, err := stream.Write([]byte("y")); !errors.Is(err, zmux.ErrWriteClosed) {
		t.Fatalf("Write err = %v, want %v", err, zmux.ErrWriteClosed)
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionCancelWriteAfterCloseWriteReturnsErrWriteClosed(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v", err)
	}
	if err := stream.CancelWrite(77); !errors.Is(err, zmux.ErrWriteClosed) {
		t.Fatalf("CancelWrite err = %v, want %v", err, zmux.ErrWriteClosed)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}
	buf := make([]byte, 1)
	if n, err := accepted.stream.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("Read after remote CloseWrite = (%d, %v), want (0, EOF)", n, err)
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionUniOpenMetadataVisibleOnAccept(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	priority := uint64(3)
	group := uint64(21)

	type acceptResult struct {
		stream zmux.RecvStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptUniStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenUniStreamWithOptions(ctx, zmux.OpenOptions{
		InitialPriority: &priority,
		InitialGroup:    &group,
		OpenInfo:        []byte("rpc"),
	})
	if err != nil {
		t.Fatalf("OpenUniStreamWithOptions err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptUniStream err = %v", accepted.err)
	}

	meta := accepted.stream.Metadata()
	if meta.Priority != priority {
		t.Fatalf("accepted Priority = %d, want %d", meta.Priority, priority)
	}
	if meta.Group == nil || *meta.Group != group {
		t.Fatalf("accepted Group = %v, want %d", meta.Group, group)
	}
	if got := string(meta.OpenInfo); got != "rpc" {
		t.Fatalf("accepted OpenInfo = %q, want %q", got, "rpc")
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func newWrappedPair(t *testing.T) (zmux.Session, zmux.Session) {
	t.Helper()

	clientConn, serverConn := newQUICConnPair(t)
	client, server := WrapSession(clientConn), WrapSession(serverConn)
	cleanupWrappedPair(t, client, server)
	return client, server
}

func newWrappedPairWithOptions(t *testing.T, clientOpts, serverOpts SessionOptions) (zmux.Session, zmux.Session) {
	t.Helper()

	clientConn, serverConn := newQUICConnPair(t)
	client, server := WrapSessionWithOptions(clientConn, clientOpts), WrapSessionWithOptions(serverConn, serverOpts)
	cleanupWrappedPair(t, client, server)
	return client, server
}

func cleanupWrappedPair(t *testing.T, client, server zmux.Session) {
	t.Helper()

	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = client.Wait(ctx)
		_ = server.Wait(ctx)
	})
}

func newQUICConnPair(t *testing.T) (*quic.Conn, *quic.Conn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	serverTLS, clientTLS := testTLSConfigs(t)
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatalf("ListenAddr err = %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	type acceptResult struct {
		conn *quic.Conn
		err  error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		acceptCh <- acceptResult{conn: conn, err: err}
	}()

	clientConn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, nil)
	if err != nil {
		t.Fatalf("DialAddr err = %v", err)
	}
	t.Cleanup(func() {
		_ = clientConn.CloseWithError(0, "")
	})

	serverResult := <-acceptCh
	if serverResult.err != nil {
		t.Fatalf("Accept err = %v", serverResult.err)
	}
	t.Cleanup(func() {
		_ = serverResult.conn.CloseWithError(0, "")
	})

	return clientConn, serverResult.conn
}

type countingSessionConn struct {
	SessionConn
	mu              sync.Mutex
	bidiAcceptCalls int
}

func (c *countingSessionConn) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	stream, err := c.SessionConn.AcceptStream(ctx)
	if err == nil {
		c.mu.Lock()
		c.bidiAcceptCalls++
		c.mu.Unlock()
	}
	return stream, err
}

func (c *countingSessionConn) bidiAcceptCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bidiAcceptCalls
}

type partialPreludeWriter struct {
	bytes.Buffer
	failAfter int
	failErr   error
	failed    bool
}

func (w *partialPreludeWriter) Write(p []byte) (int, error) {
	if !w.failed && w.failAfter > 0 {
		n := w.failAfter
		if n > len(p) {
			n = len(p)
		}
		if n > 0 {
			_, _ = w.Buffer.Write(p[:n])
		}
		w.failed = true
		return n, w.failErr
	}
	return w.Buffer.Write(p)
}

type concurrentDetectWriteCloser struct {
	mu           sync.Mutex
	active       bool
	overlap      bool
	writeStarted chan struct{}
	releaseWrite chan struct{}
	closeCalled  chan struct{}
	writeOnce    sync.Once
	closeOnce    sync.Once
}

func newConcurrentDetectWriteCloser() *concurrentDetectWriteCloser {
	return &concurrentDetectWriteCloser{
		writeStarted: make(chan struct{}),
		releaseWrite: make(chan struct{}),
		closeCalled:  make(chan struct{}),
	}
}

func (w *concurrentDetectWriteCloser) Write(p []byte) (int, error) {
	w.enter()
	w.writeOnce.Do(func() {
		close(w.writeStarted)
	})
	<-w.releaseWrite
	w.leave()
	return len(p), nil
}

func (w *concurrentDetectWriteCloser) Close() error {
	w.closeOnce.Do(func() {
		close(w.closeCalled)
	})
	w.enter()
	w.leave()
	return nil
}

func (w *concurrentDetectWriteCloser) enter() {
	w.mu.Lock()
	if w.active {
		w.overlap = true
	}
	w.active = true
	w.mu.Unlock()
}

func (w *concurrentDetectWriteCloser) leave() {
	w.mu.Lock()
	w.active = false
	w.mu.Unlock()
}

func (w *concurrentDetectWriteCloser) concurrent() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.overlap
}

type blockingCloseWriteCloser struct {
	closeStarted chan struct{}
	releaseClose chan struct{}
	startOnce    sync.Once
	releaseOnce  sync.Once
}

func newBlockingCloseWriteCloser() *blockingCloseWriteCloser {
	return &blockingCloseWriteCloser{
		closeStarted: make(chan struct{}),
		releaseClose: make(chan struct{}),
	}
}

func (w *blockingCloseWriteCloser) Close() error {
	w.startOnce.Do(func() {
		close(w.closeStarted)
	})
	<-w.releaseClose
	return nil
}

func (w *blockingCloseWriteCloser) release() {
	w.releaseOnce.Do(func() {
		close(w.releaseClose)
	})
}

type deadlineAwareWriter struct {
	mu          sync.Mutex
	started     chan struct{}
	deadlineSet chan struct{}
	startOnce   sync.Once
	setOnce     sync.Once
	deadlines   []time.Time
}

func newDeadlineAwareWriter() *deadlineAwareWriter {
	return &deadlineAwareWriter{
		started:     make(chan struct{}),
		deadlineSet: make(chan struct{}),
	}
}

func (w *deadlineAwareWriter) Write(_ []byte) (int, error) {
	w.startOnce.Do(func() {
		close(w.started)
	})
	<-w.deadlineSet
	return 0, os.ErrDeadlineExceeded
}

func (w *deadlineAwareWriter) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, t)
	w.mu.Unlock()
	if !t.IsZero() {
		w.setOnce.Do(func() {
			close(w.deadlineSet)
		})
	}
	return nil
}

func (w *deadlineAwareWriter) deadlineCalls() []time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Time(nil), w.deadlines...)
}

type blockedWriteDeadlineSetter struct {
	mu             sync.Mutex
	deadlines      []time.Time
	nonZeroStarted chan struct{}
	releaseNonZero chan struct{}
}

func newBlockedWriteDeadlineSetter() *blockedWriteDeadlineSetter {
	return &blockedWriteDeadlineSetter{
		nonZeroStarted: make(chan struct{}),
		releaseNonZero: make(chan struct{}),
	}
}

func (s *blockedWriteDeadlineSetter) SetWriteDeadline(t time.Time) error {
	if !t.IsZero() {
		select {
		case <-s.nonZeroStarted:
		default:
			close(s.nonZeroStarted)
		}
		<-s.releaseNonZero
	}
	s.mu.Lock()
	s.deadlines = append(s.deadlines, t)
	s.mu.Unlock()
	return nil
}

func (s *blockedWriteDeadlineSetter) deadlineCalls() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.deadlines...)
}

func testTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey err = %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           nil,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate err = %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair err = %v", err)
	}

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"zmux-quicmux-test"},
	}
	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"zmux-quicmux-test"},
	}
	return serverTLS, clientTLS
}

type cyclicWrappedError struct {
	next error
}

func (e *cyclicWrappedError) Error() string {
	return "cyclic wrapped error"
}

func (e *cyclicWrappedError) Unwrap() error {
	return e.next
}

func TestFindErrorHandlesCyclicUnwrap(t *testing.T) {
	cyclic := &cyclicWrappedError{}
	cyclic.next = cyclic

	if appErr, ok := findError[*zmux.ApplicationError](cyclic); ok {
		t.Fatalf("findError returned %v, want no match for cyclic unwrap", appErr)
	}
}

func BenchmarkReadAcceptedStreamMetadata(b *testing.B) {
	priority := uint64(7)
	group := uint64(9)
	openInfo := []byte("bench-open-info")
	prelude, err := wire.BuildOpenMetadataPrefix(
		quicmuxOpenCaps,
		&priority,
		&group,
		openInfo,
		quicmuxStreamPreludeMaxPayload,
	)
	if err != nil {
		b.Fatalf("BuildOpenMetadataPrefix err = %v", err)
	}

	var reader bytes.Reader
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(prelude)
		meta, err := readAcceptedStreamMetadata(&reader)
		if err != nil {
			b.Fatalf("readAcceptedStreamMetadata err = %v", err)
		}
		if !meta.prioritySet || meta.priority != priority {
			b.Fatalf("priority = (%v, %d), want (%v, %d)", meta.prioritySet, meta.priority, true, priority)
		}
		if !meta.groupEncoded || meta.group != group {
			b.Fatalf("group = (%v, %d), want (%v, %d)", meta.groupEncoded, meta.group, true, group)
		}
		if !bytes.Equal(meta.openInfo, openInfo) {
			b.Fatalf("openInfo = %q, want %q", meta.openInfo, openInfo)
		}
	}
}
