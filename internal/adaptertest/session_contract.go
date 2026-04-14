package adaptertest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

const defaultTimeout = 5 * time.Second

type SessionPairFactory func(t *testing.T) (zmux.Session, zmux.Session)

// RunSessionContract executes the repository-default adapter contract against
// fresh connected session pairs supplied by pairFactory.
func RunSessionContract(t *testing.T, pairFactory SessionPairFactory) {
	t.Helper()

	t.Run("bidi", func(t *testing.T) {
		client, server := withSessionPair(t, pairFactory)
		runBidiContract(t, client, server)
	})
	t.Run("uni", func(t *testing.T) {
		client, server := withSessionPair(t, pairFactory)
		runUniContract(t, client, server)
	})
	t.Run("stream_abortive_close", func(t *testing.T) {
		client, server := withSessionPair(t, pairFactory)
		runStreamAbortiveCloseContract(t, client, server)
	})
	t.Run("read_stop", func(t *testing.T) {
		client, server := withSessionPair(t, pairFactory)
		runReadStopContract(t, client, server)
	})
	t.Run("close", func(t *testing.T) {
		client, server := withSessionPair(t, pairFactory)
		runCloseContract(t, client, server)
	})
	t.Run("abort", func(t *testing.T) {
		client, server := withSessionPair(t, pairFactory)
		runAbortContract(t, client, server)
	})
}

func withSessionPair(t *testing.T, pairFactory SessionPairFactory) (zmux.Session, zmux.Session) {
	t.Helper()
	client, server := pairFactory(t)
	if client == nil || server == nil {
		t.Fatal("session factory returned nil session")
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	if client.Closed() {
		t.Fatal("client session unexpectedly reported Closed() before use")
	}
	if server.Closed() {
		t.Fatal("server session unexpectedly reported Closed() before use")
	}
	return client, server
}

func runBidiContract(t *testing.T, client, server zmux.Session) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
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

	payload := []byte("adapter-contract-bidi")
	if n, err := clientStream.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	if err := clientStream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}

	got, err := io.ReadAll(accepted.stream)
	if err != nil {
		t.Fatalf("ReadAll err = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadAll = %q, want %q", got, payload)
	}

	_ = accepted.stream.Close()
	_ = clientStream.Close()
}

func runUniContract(t *testing.T, client, server zmux.Session) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
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

	payload := []byte("adapter-contract-uni")
	if n, err := send.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write = (%d, %v), want (%d, nil)", n, err, len(payload))
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptUniStream err = %v", accepted.err)
	}
	if err := send.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v", err)
	}

	got, err := io.ReadAll(accepted.stream)
	if err != nil {
		t.Fatalf("ReadAll err = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadAll = %q, want %q", got, payload)
	}

	_ = accepted.stream.Close()
	_ = send.Close()
}

func runStreamAbortiveCloseContract(t *testing.T, client, server zmux.Session) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
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
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial Write err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}
	if _, err := io.ReadFull(accepted.stream, make([]byte, 1)); err != nil {
		t.Fatalf("initial peer ReadFull err = %v", err)
	}

	const abortCode = 55
	const abortReason = "adapter-contract-abort"
	if err := clientStream.CloseWithErrorCode(abortCode, abortReason); err != nil {
		t.Fatalf("CloseWithErrorCode err = %v", err)
	}

	buf := make([]byte, 1)
	if _, err := clientStream.Read(buf); !matchesApplicationError(err, abortCode, abortReason) {
		t.Fatalf("local Read err = %v, want ApplicationError(%d, %q)", err, abortCode, abortReason)
	}
	if _, err := clientStream.Write([]byte("y")); !matchesApplicationError(err, abortCode, abortReason) {
		t.Fatalf("local Write err = %v, want ApplicationError(%d, %q)", err, abortCode, abortReason)
	}

	if err := accepted.stream.SetReadDeadline(time.Now().Add(defaultTimeout)); err != nil {
		t.Fatalf("peer SetReadDeadline err = %v", err)
	}
	if _, err := accepted.stream.Read(buf); err == nil {
		t.Fatal("peer Read err = nil, want termination error")
	}
}

func runReadStopContract(t *testing.T, client, server zmux.Session) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
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
	if _, err := io.ReadFull(accepted.stream, make([]byte, 1)); err != nil {
		t.Fatalf("initial peer ReadFull err = %v", err)
	}

	const stopCode = 77
	if err := accepted.stream.CloseReadWithCode(stopCode); err != nil {
		t.Fatalf("CloseReadWithCode err = %v", err)
	}
	if _, err := accepted.stream.Read(make([]byte, 1)); !errors.Is(err, zmux.ErrReadClosed) {
		t.Fatalf("post-stop Read err = %v, want %v", err, zmux.ErrReadClosed)
	}

	deadline := time.Now().Add(defaultTimeout)
	if err := clientStream.SetWriteDeadline(deadline); err != nil {
		t.Fatalf("SetWriteDeadline err = %v", err)
	}
	for time.Now().Before(deadline) {
		_, err := clientStream.Write([]byte("x"))
		if err == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if matchesApplicationErrorCode(err, stopCode) || errors.Is(err, zmux.ErrWriteClosed) {
			return
		}
		t.Fatalf("Write err = %v, want ApplicationError(%d) or ErrWriteClosed", err, stopCode)
	}
	t.Fatalf("Write did not observe stop code %d before deadline", stopCode)
}

func runCloseContract(t *testing.T, client, server zmux.Session) {
	t.Helper()

	if err := client.Close(); err != nil {
		t.Fatalf("Close err = %v", err)
	}
	if _, ok := waitForClosedResult(t, client); !ok {
		t.Fatal("client Wait after Close did not complete")
	}
	if _, ok := waitForClosedResult(t, server); !ok {
		t.Fatal("server Wait after peer Close did not complete")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err := client.OpenStream(ctx)
	if err == nil {
		t.Fatal("OpenStream after Close err = nil, want non-nil")
	}
	if !errors.Is(err, zmux.ErrSessionClosed) && !matchesApplicationErrorCode(err, uint64(zmux.CodeSessionClosing)) {
		t.Fatalf("OpenStream after Close err = %v, want ErrSessionClosed or session-closing application error", err)
	}
}

func runAbortContract(t *testing.T, client, server zmux.Session) {
	t.Helper()

	const abortCode = 91
	const abortReason = "adapter-contract-session-abort"
	client.Abort(&zmux.ApplicationError{Code: abortCode, Reason: abortReason})

	clientWaitErr, ok := waitForClosedResult(t, client)
	if !ok {
		t.Fatal("client Wait after Abort did not complete")
	}
	if clientWaitErr != nil && !errors.Is(clientWaitErr, zmux.ErrSessionClosed) && !matchesApplicationError(clientWaitErr, abortCode, abortReason) {
		t.Fatalf("client Wait after Abort err = %v", clientWaitErr)
	}

	serverWaitErr, ok := waitForClosedResult(t, server)
	if !ok {
		t.Fatal("server Wait after peer Abort did not complete")
	}
	if serverWaitErr != nil && !errors.Is(serverWaitErr, zmux.ErrSessionClosed) && !matchesApplicationErrorCode(serverWaitErr, abortCode) {
		t.Fatalf("server Wait after peer Abort err = %v", serverWaitErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err := client.OpenStream(ctx)
	if err == nil {
		t.Fatal("OpenStream after Abort err = nil, want non-nil")
	}
	if !errors.Is(err, zmux.ErrSessionClosed) && !matchesApplicationErrorCode(err, abortCode) {
		t.Fatalf("OpenStream after Abort err = %v, want ErrSessionClosed or ApplicationError(%d)", err, abortCode)
	}
}

func waitForClosedResult(t *testing.T, session zmux.Session) (error, bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	err := session.Wait(ctx)
	if err != nil && !errors.Is(err, zmux.ErrSessionClosed) {
		if _, ok := findError[*zmux.ApplicationError](err); !ok {
			t.Fatalf("Wait err = %v", err)
		}
	}
	if !session.Closed() {
		t.Fatal("session did not report Closed() after Wait")
	}
	return err, true
}

func matchesApplicationError(err error, code uint64, reason string) bool {
	appErr, ok := findError[*zmux.ApplicationError](err)
	return ok && appErr.Code == code && appErr.Reason == reason
}

func matchesApplicationErrorCode(err error, code uint64) bool {
	appErr, ok := findError[*zmux.ApplicationError](err)
	return ok && appErr.Code == code
}

func findError[T any](err error) (T, bool) {
	var zero T
	if err == nil {
		return zero, false
	}
	if target, ok := any(err).(T); ok {
		return target, true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range wrapped.Unwrap() {
			if target, ok := findError[T](child); ok {
				return target, true
			}
		}
		return zero, false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return findError[T](wrapped.Unwrap())
	}
	return zero, false
}
