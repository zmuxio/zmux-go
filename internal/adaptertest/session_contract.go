package adaptertest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	zmux "github.com/zmuxio/zmux-go"
)

const defaultTimeout = 5 * time.Second

// RunSessionContract executes the repository-default adapter contract against a
// connected session pair.
func RunSessionContract(t *testing.T, client, server zmux.Session) {
	t.Helper()

	if client.Closed() {
		t.Fatal("client session unexpectedly reported Closed() before use")
	}
	if server.Closed() {
		t.Fatal("server session unexpectedly reported Closed() before use")
	}

	runBidiContract(t, client, server)
	runUniContract(t, client, server)
	runCloseContract(t, client)
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

func runCloseContract(t *testing.T, client zmux.Session) {
	t.Helper()

	if err := client.Close(); err != nil {
		t.Fatalf("Close err = %v", err)
	}
	waitForClosed(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err := client.OpenStream(ctx)
	if err == nil {
		t.Fatal("OpenStream after Close err = nil, want non-nil")
	}
	if !errors.Is(err, zmux.ErrSessionClosed) {
		t.Fatalf("OpenStream after Close err = %v, want ErrSessionClosed", err)
	}
}

func waitForClosed(t *testing.T, session zmux.Session) {
	t.Helper()

	deadline := time.Now().Add(defaultTimeout)
	for time.Now().Before(deadline) {
		if session.Closed() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("session did not report Closed() before timeout")
}
