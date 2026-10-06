package zmux

import (
	"testing"
	"time"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
)

// Aborting a stream releases the queued-data bytes its unsent requests held.
// That release can admit a writer of another stream blocked on the session
// high watermark, so it has to wake writers like any other queue release.

// sessionHWMBlockedWriter fills the session queue with a stalled Write on one
// stream and leaves a Write on a second stream blocked on queue admission.
func sessionHWMBlockedWriter(t *testing.T) (c *Conn, peer *rawFramePeer, full, blocked *nativeStream, blockedDone chan error) {
	t.Helper()
	const sessionHWM = 8192
	settings := DefaultSettings()
	settings.InitialMaxData = 1 << 20
	settings.InitialMaxStreamDataBidiPeerOpened = 1 << 20
	c, peer = newStalledRawPeerConn(t, &Config{SessionQueuedDataHWM: sessionHWM}, settings)

	open := func() (NativeStream, *nativeStream) {
		stream, err := c.OpenStream(t.Context())
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		return stream, requireNativeStreamImpl(t, stream)
	}
	fullStream, full := open()
	go func() { _, _ = fullStream.Write(make([]byte, 3*sessionHWM)) }()
	pollStreamState(t, full, func(s *nativeStream) bool {
		return s.conn.flow.queuedDataBytes > sessionHWM/2
	}, "first Write did not fill the session queue")

	blockedStream, blocked := open()
	blockedDone = make(chan error, 1)
	go func() {
		_, err := blockedStream.Write(make([]byte, sessionHWM/2))
		blockedDone <- err
	}()
	awaitStreamWriteWaiter(t, blocked, testSignalTimeout, "second Write did not wait for queue admission")
	c.mu.Lock()
	queued := blocked.queuedDataBytes
	c.mu.Unlock()
	if queued != 0 {
		t.Fatalf("second Write queued %d bytes, want it blocked on the session high watermark", queued)
	}
	return c, peer, full, blocked, blockedDone
}

func awaitQueueAdmission(t *testing.T, stream *nativeStream, what string) {
	t.Helper()
	deadline := time.Now().Add(testSignalTimeout)
	for {
		stream.conn.mu.Lock()
		queued := stream.queuedDataBytes
		sessionQueued := stream.conn.flow.queuedDataBytes
		stream.conn.mu.Unlock()
		if queued > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("blocked Write was not admitted after %s (session queued %d bytes)", what, sessionQueued)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPeerAbortOfQueuedStreamWakesWritersBlockedOnSessionQueue(t *testing.T) {
	t.Parallel()

	_, peer, full, blocked, _ := sessionHWMBlockedWriter(t)
	peer.write(t, Frame{Type: FrameTypeABORT, StreamID: full.StreamID(), Payload: mustEncodeVarint(uint64(CodeCancelled))})
	awaitQueueAdmission(t, blocked, "the peer aborted the stream holding the queue")
}

func TestLocalAbortOfQueuedStreamWakesWritersBlockedOnSessionQueue(t *testing.T) {
	t.Parallel()

	_, _, full, blocked, _ := sessionHWMBlockedWriter(t)
	go func() { _ = full.CloseWithError(uint64(CodeCancelled), "") }()
	awaitQueueAdmission(t, blocked, "the stream holding the queue was aborted")
}

// TestAbortReleasedQueuedBytesAreNotReleasedAgain releases an aborted
// stream's queued bytes, then lets the writer release the request that held
// them. The session queue must still count the other stream's bytes.
func TestAbortReleasedQueuedBytesAreNotReleasedAgain(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		release func(*Conn, *writeRequest)
	}{
		{name: "writer_batch", release: func(c *Conn, req *writeRequest) {
			c.releaseBatchReservations([]writeRequest{*req})
		}},
		{name: "single_request", release: func(c *Conn, req *writeRequest) {
			c.releaseWriteQueueReservation(req)
		}},
		{name: "rejected_request", release: func(c *Conn, req *writeRequest) {
			c.releaseRejectedPreparedRequests([]rejectedWriteRequest{{req: *req, err: ErrSessionClosed}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newSessionMemoryTestConn()
			aborted := testBuildStream(c, 4, testWithLocalSend())
			other := testBuildStream(c, 8, testWithLocalSend())
			req := writeRequest{
				frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: aborted.id, Payload: make([]byte, 99)}}),
				done:           make(chan error, 1),
				origin:         writeRequestOriginStream,
				queueReserved:  true,
				queuedBytes:    100,
				reservedStream: aborted,
			}

			c.mu.Lock()
			c.flow.queuedDataBytes = 150
			aborted.queuedDataBytes = 100
			other.queuedDataBytes = 50
			c.trackQueuedDataStreamLocked(aborted)
			c.trackQueuedDataStreamLocked(other)
			aborted.setAbortedWithSource(applicationErr(uint64(CodeCancelled), ""), terminalAbortLocal)
			c.releaseTerminalStreamStateLocked(aborted, transientStreamReleaseOptions{
				send:    true,
				receive: streamReceiveReleaseAndClearReadBuf,
			})
			if got := c.flow.queuedDataBytes; got != 50 {
				c.mu.Unlock()
				t.Fatalf("session queued after abort = %d, want 50", got)
			}
			c.mu.Unlock()

			tc.release(c, &req)

			c.mu.Lock()
			defer c.mu.Unlock()
			if got := c.flow.queuedDataBytes; got != 50 {
				t.Fatalf("session queued = %d after the aborted stream's request was released, want the other stream's 50", got)
			}
			if got := other.queuedDataBytes; got != 50 {
				t.Fatalf("other stream queued = %d, want 50", got)
			}
		})
	}
}

// TestAbortReleaseWakesSessionQueueWaiters checks the release itself: freeing
// an aborted stream's queued bytes wakes writers blocked on the session queue
// whenever it can admit them.
func TestAbortReleaseWakesSessionQueueWaiters(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()
	aborted := testBuildStream(c, 4, testWithLocalSend())

	c.mu.Lock()
	c.flow.sessionDataHWM = 100
	c.flow.perStreamDataHWM = 100
	c.flow.queuedDataBytes = 100
	aborted.queuedDataBytes = 100
	c.trackQueuedDataStreamLocked(aborted)
	if rt.QueueReleaseWakes(100, 0, c.sessionDataLWMLocked()) != true {
		c.mu.Unlock()
		t.Fatal("test setup: releasing the whole queue should admit blocked writers")
	}
	wake := c.currentWriteWakeLocked()
	aborted.setAbortedWithSource(applicationErr(uint64(CodeCancelled), ""), terminalAbortLocal)
	c.releaseTerminalStreamStateLocked(aborted, transientStreamReleaseOptions{
		send:    true,
		receive: streamReceiveReleaseAndClearReadBuf,
	})
	c.mu.Unlock()

	select {
	case <-wake:
	default:
		t.Fatal("releasing an aborted stream's queued bytes did not wake writers blocked on the session queue")
	}
}
