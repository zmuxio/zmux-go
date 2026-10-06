package zmux

import (
	"errors"
	"os"
	"testing"
	"time"
)

// Several goroutines can wait on one stream's write side at once: a Write for
// credit, another Write for the write permit, a CloseRead for its
// STOP_SENDING. Every write-side wake has to reach all of them; a single token
// taken by a waiter it does not concern leaves the others blocked for good.

func TestBroadcastWriteNotifyWakesEveryWaiter(t *testing.T) {
	t.Parallel()

	c := &Conn{lifecycle: connLifecycleState{closedCh: make(chan struct{})}}
	stream := testBuildDetachedStream(c, 4)

	c.mu.Lock()
	stream.broadcastWriteNotifyLocked() // nothing to wake yet
	notifyCh := stream.ensureWriteNotifyLocked()
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
	stream.broadcastWriteNotifyLocked()
	next := stream.ensureWriteNotifyLocked()
	stream.broadcastWriteNotifyLocked()
	stream.broadcastWriteNotifyLocked()
	c.mu.Unlock()

	for i := 0; i < waiters; i++ {
		select {
		case <-woke:
		case <-time.After(testSignalTimeout):
			t.Fatalf("%d of %d write waiters woke", i, waiters)
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
	if stream.writeNotify != nil {
		t.Fatal("a wake with no waiter left a notify channel behind")
	}
}

// TestConcurrentWritesOnAStreamBothSeeStreamCredit has a second Write wait for
// the write permit before the first Write, which holds it, starts waiting for
// stream credit. The MAX_DATA that grants credit must still reach the first
// Write, or neither Write ever finishes.
func TestConcurrentWritesOnAStreamBothSeeStreamCredit(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.InitialMaxStreamDataBidiPeerOpened = 0
	c, peer := newStalledRawPeerConn(t, nil, settings)
	stream, err := c.OpenStream(t.Context())
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	impl := requireNativeStreamImpl(t, stream)

	// The zero-length opener blocks in the stalled transport, and the first
	// Write waits for it while it holds the write permit.
	first := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("abc"))
		first <- err
	}()
	pollStreamState(t, impl, func(s *nativeStream) bool {
		return s.idSet && s.writeInProgress
	}, "first Write did not open the stream")

	second := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("def"))
		second <- err
	}()
	awaitStreamWriteWaiter(t, impl, testSignalTimeout, "second Write did not wait for the write permit")

	peer.startReading()
	pollStreamWriteWaiters(t, impl, 2, "first Write did not wait for stream credit")
	peer.write(t, Frame{Type: FrameTypeMAXDATA, StreamID: stream.StreamID(), Payload: mustEncodeVarint(1000)})

	for name, done := range map[string]chan error{"first": first, "second": second} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s Write err = %v", name, err)
			}
		case <-time.After(testSignalTimeout):
			t.Fatalf("%s Write stayed blocked after MAX_DATA granted stream credit", name)
		}
	}
	c.mu.Lock()
	sent := impl.sendSent
	c.mu.Unlock()
	if sent != 6 {
		t.Fatalf("stream sendSent = %d, want 6", sent)
	}
}

// TestCreditWakeReachesWriteWhileCloseReadAwaitsItsOpener has CloseRead open a
// fresh stream (opener + STOP_SENDING, stuck in a stalled transport) and wait
// for that write, while a Write on the same stream waits for stream credit.
// The CloseRead must not swallow the wake of the MAX_DATA meant for the Write.
func TestCreditWakeReachesWriteWhileCloseReadAwaitsItsOpener(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.InitialMaxStreamDataBidiPeerOpened = 0
	c, peer := newStalledRawPeerConn(t, nil, settings)
	stream, err := c.OpenStream(t.Context())
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	impl := requireNativeStreamImpl(t, stream)

	closeRead := make(chan error, 1)
	go func() { closeRead <- stream.CloseRead() }()
	pollStreamState(t, impl, func(s *nativeStream) bool {
		return s.idSet && s.readStopSentLocked()
	}, "CloseRead did not open the stream")
	// Let CloseRead park on its queued write before the Write starts waiting.
	time.Sleep(10 * time.Millisecond)

	write := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("abc"))
		write <- err
	}()
	awaitStreamWriteWaiter(t, impl, testSignalTimeout, "Write did not wait for stream credit")

	peer.write(t, Frame{Type: FrameTypeMAXDATA, StreamID: stream.StreamID(), Payload: mustEncodeVarint(1000)})
	peer.startReading()

	for name, done := range map[string]chan error{"Write": write, "CloseRead": closeRead} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s err = %v", name, err)
			}
		case <-time.After(testSignalTimeout):
			t.Fatalf("%s stayed blocked after MAX_DATA granted stream credit", name)
		}
	}
	peer.await(t, "DATA abc", func(f Frame) bool {
		return f.Type == FrameTypeDATA && f.StreamID == stream.StreamID() && string(f.Payload) == "abc"
	})
}

// TestWriteDeadlineReachesEveryWriterOfAStream sets a write deadline while one
// Write waits for its frame to leave a stalled transport and a second Write
// waits for the write permit behind it. The deadline must reach the second
// Write too.
func TestWriteDeadlineReachesEveryWriterOfAStream(t *testing.T) {
	t.Parallel()

	c, peer := newStalledRawPeerConn(t, nil, DefaultSettings())
	stream, err := c.OpenStream(t.Context())
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	impl := requireNativeStreamImpl(t, stream)

	first := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("abc"))
		first <- err
	}()
	pollStreamState(t, impl, func(s *nativeStream) bool {
		return s.idSet && s.writeInProgress
	}, "first Write did not open the stream")
	// Let the first Write park on its queued frame.
	time.Sleep(10 * time.Millisecond)

	second := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("def"))
		second <- err
	}()
	awaitStreamWriteWaiter(t, impl, testSignalTimeout, "second Write did not wait for the write permit")

	if err := stream.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	select {
	case err := <-second:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("second Write err = %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("second Write ignored the write deadline set while it waited for the write permit")
	}

	// The writer already owns the first Write's frame, so that Write ends
	// once the transport drains.
	peer.startReading()
	select {
	case <-first:
	case <-time.After(testSignalTimeout):
		t.Fatal("first Write did not finish after the transport drained")
	}
}

func pollStreamWriteWaiters(t *testing.T, stream *nativeStream, want uint32, msg string) {
	t.Helper()
	deadline := time.Now().Add(testSignalTimeout)
	for stream.loadWriteWaiters() < want {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(time.Millisecond)
	}
}
