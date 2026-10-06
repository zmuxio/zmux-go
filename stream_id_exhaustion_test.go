package zmux

import (
	"errors"
	"io"
	"testing"

	"github.com/zmuxio/zmux-go/internal/state"
)

// Running out of local stream IDs stops new opens of that class without wrap
// or reuse, reports a local open limit rather than a peer violation, and
// starts graceful replacement with one non-tightening GOAWAY (SPEC §3.1, §9.1).
func TestLocalStreamIDExhaustionStartsGracefulReplacement(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	last := state.MaxStreamIDForClass(state.FirstLocalStreamID(client.config.negotiated.LocalRole, true))
	client.mu.Lock()
	client.registry.nextLocalBidi = last
	client.mu.Unlock()
	server.mu.Lock()
	server.registry.nextPeerBidi = last
	server.mu.Unlock()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open at last valid stream ID: %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write at last valid stream ID: %v", err)
	}
	accepted, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("accept last valid stream ID: %v", err)
	}
	if got := accepted.StreamID(); got != last {
		t.Fatalf("accepted stream ID = %d, want %d", got, last)
	}

	_, err = client.OpenStream(ctx)
	if !errors.Is(err, ErrOpenLimited) || !errors.Is(err, errLocalStreamIDsExhausted) {
		t.Fatalf("open after exhaustion err = %v, want %v", err, errLocalStreamIDsExhausted)
	}
	if _, ok := ErrorCodeOf(err); ok {
		t.Fatalf("open after exhaustion err = %v, want a local error without a wire code", err)
	}
	if got := client.State(); got != SessionStateDraining {
		t.Fatalf("client state after exhaustion = %v, want draining", got)
	}
	awaitPeerGoAwayBidi(t, server, maxPeerGoAwayWatermark(client.config.negotiated.LocalRole, streamArityBidi))
	if _, err := client.OpenStream(ctx); !errors.Is(err, errLocalStreamIDsExhausted) {
		t.Fatalf("second open after exhaustion err = %v, want %v", err, errLocalStreamIDsExhausted)
	}

	// The GOAWAY refuses nothing: the server still opens streams, and the
	// client still opens the class that has IDs left.
	serverStream, err := server.OpenStream(ctx)
	if err != nil {
		t.Fatalf("server open after client exhaustion: %v", err)
	}
	if _, err := serverStream.Write([]byte("s")); err != nil {
		t.Fatalf("server write after client exhaustion: %v", err)
	}
	fromServer, err := client.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("client accept after exhaustion: %v", err)
	}
	if _, err := io.ReadFull(fromServer, make([]byte, 1)); err != nil {
		t.Fatalf("client read server stream: %v", err)
	}
	uni, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("client uni open after bidi exhaustion: %v", err)
	}
	if _, err := uni.Write([]byte("u")); err != nil {
		t.Fatalf("client uni write after bidi exhaustion: %v", err)
	}
	if _, err := server.AcceptUniStream(ctx); err != nil {
		t.Fatalf("server accept uni after client exhaustion: %v", err)
	}

	client.mu.Lock()
	sentGoAway := client.sessionControl.hasSentGoAway
	client.mu.Unlock()
	if !sentGoAway {
		t.Fatal("client did not send its GOAWAY")
	}
	if err := client.err(); err != nil {
		t.Fatalf("client session failed: %v", err)
	}
	if err := server.err(); err != nil {
		t.Fatalf("server session failed: %v", err)
	}
}
