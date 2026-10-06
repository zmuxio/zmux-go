package zmux

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

// Several goroutines can wait on one stream's read side at once: concurrent
// Reads, or a Read while another goroutine sets the read deadline. Every
// read-side wake has to reach all of them; a single token taken by one waiter
// leaves the others blocked for good.

func TestBroadcastReadNotifyWakesEveryWaiter(t *testing.T) {
	t.Parallel()

	c := &Conn{lifecycle: connLifecycleState{closedCh: make(chan struct{})}}
	stream := testBuildDetachedStream(c, 4)

	c.mu.Lock()
	stream.broadcastReadNotifyLocked() // nothing to wake yet
	notifyCh, _ := stream.readWaitSnapshotLocked()
	c.mu.Unlock()

	const waiters = 3
	woke := make(chan struct{}, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			<-notifyCh
			woke <- struct{}{}
		}()
	}

	c.mu.Lock()
	stream.broadcastReadNotifyLocked()
	next, _ := stream.readWaitSnapshotLocked()
	stream.broadcastReadNotifyLocked()
	stream.broadcastReadNotifyLocked()
	c.mu.Unlock()

	for i := 0; i < waiters; i++ {
		select {
		case <-woke:
		case <-time.After(testSignalTimeout):
			t.Fatalf("%d of %d read waiters woke", i, waiters)
		}
	}
	if next == notifyCh {
		t.Fatal("a waiter after the wake reused the closed notify channel")
	}
	select {
	case <-next:
	default:
		t.Fatal("a later wake did not reach the waiter that took the new channel")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.readNotify != nil {
		t.Fatal("a wake with no waiter left a notify channel behind")
	}
}

// acceptPeerStream has the raw peer open a bidirectional stream with one byte
// and returns it once the session has accepted it and the byte has been read.
func acceptPeerStream(t *testing.T, c *Conn, peer *rawFramePeer) (NativeStream, *nativeStream) {
	t.Helper()
	streamID := peer.bidiID(0)
	peer.writeData(t, streamID, 0, []byte("a"))
	stream, err := c.AcceptStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	var buf [1]byte
	if _, err := io.ReadFull(stream, buf[:]); err != nil {
		t.Fatalf("read opening byte: %v", err)
	}
	return stream, requireNativeStreamImpl(t, stream)
}

func pollStreamReadWaiters(t *testing.T, stream *nativeStream, want uint32, msg string) {
	t.Helper()
	deadline := time.Now().Add(testSignalTimeout)
	for stream.loadReadWaiters() < want {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(time.Millisecond)
	}
}

type readResult struct {
	data string
	err  error
}

func startBlockedReads(t *testing.T, stream NativeStream, impl *nativeStream, n int) []chan readResult {
	t.Helper()
	results := make([]chan readResult, n)
	for i := range results {
		results[i] = make(chan readResult, 1)
		go func(done chan readResult) {
			buf := make([]byte, 16)
			n, err := stream.Read(buf)
			done <- readResult{string(buf[:n]), err}
		}(results[i])
	}
	pollStreamReadWaiters(t, impl, uint32(n), "Reads did not all wait for data")
	return results
}

// TestReadDeadlineReachesEveryReaderOfAStream sets a read deadline while two
// Reads wait on the same stream. Both must see it.
func TestReadDeadlineReachesEveryReaderOfAStream(t *testing.T) {
	t.Parallel()

	c, peer := newRawPeerConn(t, nil, DefaultSettings())
	stream, impl := acceptPeerStream(t, c, peer)
	results := startBlockedReads(t, stream, impl, 2)

	if err := stream.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	for i, done := range results {
		select {
		case r := <-done:
			if !errors.Is(r.err, os.ErrDeadlineExceeded) {
				t.Fatalf("Read %d = (%q, %v), want %v", i, r.data, r.err, os.ErrDeadlineExceeded)
			}
		case <-time.After(testSignalTimeout):
			t.Fatalf("Read %d ignored the read deadline set while it waited", i)
		}
	}
}

// TestDataAndFINReachEveryReaderOfAStream delivers one byte and FIN in a single
// frame while two Reads wait. One Read takes the byte; the other must still
// wake and see the end of the stream.
func TestDataAndFINReachEveryReaderOfAStream(t *testing.T) {
	t.Parallel()

	c, peer := newRawPeerConn(t, nil, DefaultSettings())
	stream, impl := acceptPeerStream(t, c, peer)
	results := startBlockedReads(t, stream, impl, 2)

	peer.writeData(t, impl.StreamID(), FrameFlagFIN, []byte("x"))
	var gotData, gotEOF int
	for i, done := range results {
		select {
		case r := <-done:
			switch {
			case r.err == nil && r.data == "x":
				gotData++
			case errors.Is(r.err, io.EOF) && r.data == "":
				gotEOF++
			default:
				t.Fatalf("Read %d = (%q, %v), want (\"x\", nil) or EOF", i, r.data, r.err)
			}
		case <-time.After(testSignalTimeout):
			t.Fatalf("Read %d stayed blocked after DATA with FIN arrived", i)
		}
	}
	if gotData != 1 || gotEOF != 1 {
		t.Fatalf("Reads returned data %d times and EOF %d times, want once each", gotData, gotEOF)
	}
}
