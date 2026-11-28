// Package teststream exercises a stream across the host and adapter tests.
package teststream

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

type deadlineStream interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}

// Echo checks that two live stream endpoints transfer a payload both ways.
func Echo[S deadlineStream](t *testing.T, ctx context.Context, open, accept func(context.Context) (S, error), payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	deadline, _ := ctx.Deadline()
	served := make(chan error, 1)
	go func() {
		stream, err := accept(ctx)
		if err == nil {
			_ = stream.SetDeadline(deadline)
			_, err = io.Copy(stream, stream)
			_ = stream.Close()
		}
		served <- err
	}()
	stream, err := open(ctx)
	if err != nil {
		cancel()
		t.Fatalf("open: %v, server: %v", err, <-served)
	}
	_ = stream.SetDeadline(deadline)
	written := make(chan error, 1)
	go func() {
		_, err := stream.Write(payload)
		_ = stream.Close()
		written <- err
	}()
	got, err := io.ReadAll(stream)
	writeErr, serveErr := <-written, <-served
	if err != nil || writeErr != nil || serveErr != nil || !bytes.Equal(got, payload) {
		t.Fatalf("echo: %d of %d bytes, read %v, write %v, server %v", len(got), len(payload), err, writeErr, serveErr)
	}
}
