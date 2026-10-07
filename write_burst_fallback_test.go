package zmux

import (
	"testing"
	"time"
)

// A Write on a stream that still needs its opener leaves the burst path for
// the per-frame step path, which emits the opener. If another operation on the
// stream (here a CloseRead) commits and queues the opener first, the step path
// goes on building plain DATA frames into the same request, and that request
// must still honour the per-stream queued-data limit.

// fallbackRaceStream opens a stream and parks a Write of payload in the step
// path, waiting for its opener turn, then lets a CloseRead win the opener and
// wakes the Write. It returns the stream and the Write's result channel.
func fallbackRaceStream(t *testing.T, c *Conn, write func(NativeStream) error) (*nativeStream, chan error) {
	t.Helper()
	stream, err := c.OpenStream(t.Context())
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	impl := requireNativeStreamImpl(t, stream)

	// An earlier same-class opener the writer has not taken yet holds the
	// opener turn, so the Write parks in the step path.
	c.mu.Lock()
	c.registry.openerTurnBidi = impl.streamArity().nextLocalID(&c.registry) + 4*64
	c.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- write(stream) }()
	awaitStreamWriteWaiter(t, impl, testSignalTimeout, "Write did not wait for its opener turn")

	// The turn is released, and CloseRead runs before the parked Write does.
	c.mu.Lock()
	c.registry.openerTurnBidi = 0
	c.mu.Unlock()
	closeRead := make(chan error, 1)
	go func() { closeRead <- stream.CloseRead() }()
	pollStreamState(t, impl, func(s *nativeStream) bool {
		return s.idSet && s.readStopSentLocked() && !s.needsLocalOpenerLocked() && !s.shouldEmitOpenerFrameLocked()
	}, "CloseRead did not commit the opener")

	// Now the turn release reaches the Write.
	c.mu.Lock()
	impl.broadcastWriteNotifyLocked()
	c.mu.Unlock()
	return impl, done
}

func TestWriteStepFallbackHonoursPerStreamQueueCap(t *testing.T) {
	t.Parallel()

	const hwm = 4096
	settings := DefaultSettings()
	settings.InitialMaxData = 1 << 20
	settings.InitialMaxStreamDataBidiPeerOpened = 1 << 20
	c, peer := newStalledRawPeerConn(t, &Config{PerStreamQueuedDataHWM: hwm}, settings)

	payload := make([]byte, 20000)
	impl, done := fallbackRaceStream(t, c, func(stream NativeStream) error {
		_, err := stream.Write(payload)
		return err
	})

	// The Write's request waits behind CloseRead's opener, or follows it into
	// an empty queue. Drain CloseRead's frames, or the first frame of the
	// Write's data: either way the Write's request ends up admitted and held
	// by the stalled transport.
	pollStreamState(t, impl, func(s *nativeStream) bool { return s.sendSent > 0 }, "Write did not prepare its batch")
	streamID := impl.StreamID()
	peer.readUntil(t, "CloseRead STOP_SENDING or Write DATA", func(f Frame) bool {
		return f.StreamID == streamID && (f.Type == FrameTypeStopSending || (f.Type == FrameTypeDATA && len(f.Payload) > 0))
	})
	pollStreamState(t, impl, func(s *nativeStream) bool { return s.queuedDataBytes > 1 }, "Write's request was not admitted")
	time.Sleep(20 * time.Millisecond)

	c.mu.Lock()
	queued := impl.queuedDataBytes
	c.mu.Unlock()
	if queued > hwm {
		t.Fatalf("stream has %d bytes queued, above its %d-byte high watermark", queued, hwm)
	}

	peer.startReading()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write err = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Write did not finish after the transport drained")
	}
}

// readUntil reads frames straight off the transport, before startReading,
// until one matches, so a test can drain exactly what it needs from a stalled
// session.
func (p *rawFramePeer) readUntil(t *testing.T, what string, match func(Frame) bool) {
	t.Helper()
	found := make(chan error, 1)
	go func() {
		for {
			frame, err := ReadFrame(p.conn, p.limits)
			if err != nil {
				found <- err
				return
			}
			p.mu.Lock()
			p.frames = append(p.frames, frame)
			p.consumed = append(p.consumed, true)
			p.mu.Unlock()
			if match(frame) {
				found <- nil
				return
			}
		}
	}()
	select {
	case err := <-found:
		if err != nil {
			t.Fatalf("reading until %s: %v", what, err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatalf("timed out reading until %s (received %s)", what, p.describe())
	}
}

// TestWriteFinalStepFallbackStopsBeforeReservingWhatTheQueueCannotTake runs
// the same race with WriteFinal, a payload of two frames, and exactly as much
// stream credit as the payload. Once the first frame fills the request's
// queue cap, the second frame (which carries FIN) must not be prepared:
// preparing it reserves its credit and FIN, and dropping it then leaks both,
// so the stream runs out of credit with data unsent, and FIN is never written.
func TestWriteFinalStepFallbackStopsBeforeReservingWhatTheQueueCannotTake(t *testing.T) {
	t.Parallel()

	const (
		hwm  = 4096
		size = 8000
	)
	settings := DefaultSettings()
	settings.InitialMaxData = 1 << 20
	settings.InitialMaxStreamDataBidiPeerOpened = size
	c, peer := newStalledRawPeerConn(t, &Config{PerStreamQueuedDataHWM: hwm}, settings)

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	type result struct {
		n   int
		err error
	}
	results := make(chan result, 1)
	impl, done := fallbackRaceStream(t, c, func(stream NativeStream) error {
		n, err := stream.WriteFinal(payload)
		results <- result{n, err}
		return err
	})
	peer.startReading()

	select {
	case <-done:
	case <-time.After(testSignalTimeout):
		c.mu.Lock()
		sent, sendMax, fin := impl.sendSent, impl.sendMax, impl.sendFinReachedLocked()
		c.mu.Unlock()
		t.Fatalf("WriteFinal did not finish (stream sendSent=%d sendMax=%d finReserved=%v)", sent, sendMax, fin)
	}
	if r := <-results; r.n != size || r.err != nil {
		t.Fatalf("WriteFinal = (%d, %v), want (%d, nil)", r.n, r.err, size)
	}

	streamID := impl.StreamID()
	var got []byte
	for {
		frame := peer.await(t, "WriteFinal DATA", func(f Frame) bool {
			return f.Type == FrameTypeDATA && f.StreamID == streamID && (len(f.Payload) > 0 || f.Flags&FrameFlagFIN != 0)
		})
		got = append(got, frame.Payload...)
		if frame.Flags&FrameFlagFIN != 0 {
			break
		}
	}
	if string(got) != string(payload) {
		t.Fatalf("peer received %d bytes before FIN, want the %d-byte payload", len(got), size)
	}
	c.mu.Lock()
	used := c.flow.sendSessionUsed
	c.mu.Unlock()
	if used != size {
		t.Fatalf("session send credit used = %d, want %d: a dropped frame kept its reservation", used, size)
	}
}

// TestWriteStepFallbackFlushesBeforeWaitingForCredit runs the race with less
// stream credit than the Write needs. The step path must queue the frames it
// has already prepared before it waits for credit: the peer can only grant
// more once it has received them, so holding them back deadlocks the Write.
func TestWriteStepFallbackFlushesBeforeWaitingForCredit(t *testing.T) {
	t.Parallel()

	const (
		credit = 10000
		size   = 20000
	)
	settings := DefaultSettings()
	settings.InitialMaxData = 1 << 20
	settings.InitialMaxStreamDataBidiPeerOpened = credit
	c, peer := newStalledRawPeerConn(t, nil, settings)

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	impl, done := fallbackRaceStream(t, c, func(stream NativeStream) error {
		_, err := stream.Write(payload)
		return err
	})
	peer.startReading()

	streamID := impl.StreamID()
	var got []byte
	for len(got) < credit {
		frame := peer.await(t, "the DATA the stream has credit for", func(f Frame) bool {
			return f.Type == FrameTypeDATA && f.StreamID == streamID && len(f.Payload) > 0
		})
		got = append(got, frame.Payload...)
	}
	peer.write(t, Frame{Type: FrameTypeMAXDATA, StreamID: streamID, Payload: mustEncodeVarint(size)})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write err = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Write did not finish after MAX_DATA granted the rest")
	}
	for len(got) < size {
		frame := peer.await(t, "the rest of the DATA", func(f Frame) bool {
			return f.Type == FrameTypeDATA && f.StreamID == streamID && len(f.Payload) > 0
		})
		got = append(got, frame.Payload...)
	}
	if string(got) != string(payload) {
		t.Fatalf("peer received %d bytes that differ from the %d-byte payload", len(got), size)
	}
}

// testPrepareWritePartsLocked prepares one step with no batch around it.
func testPrepareWritePartsLocked(s *nativeStream, parts [][]byte, idx, off, totalRemaining int, mode writeChunkMode) (writeStep, error) {
	step, _, err := s.prepareWritePartsLocked(parts, idx, off, totalRemaining, mode, writeStepBudget{})
	return step, err
}

func TestWriteStepRefusedByBatchBudgetReservesNothing(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	c := stream.conn
	payload := make([]byte, 50) // one frame costing 51 queued bytes
	parts := [][]byte{payload}
	reservations := func() (uint64, uint64, bool) {
		c.mu.Lock()
		defer c.mu.Unlock()
		return stream.sendSent, c.flow.sendSessionUsed, stream.sendFinReachedLocked()
	}

	budget := writeStepBudget{start: writeBatchStart{burstLimit: 16, queueByteCap: 100}, queued: 60, held: true}
	if _, ready, err := stream.prepareWritePartsLocked(parts, 0, 0, len(payload), writeChunkFinal, budget); ready || err != nil {
		t.Fatalf("over-budget step = (ready %v, %v), want (false, nil)", ready, err)
	}
	if sent, used, fin := reservations(); sent != 0 || used != 0 || fin {
		t.Fatalf("over-budget step reserved stream credit %d, session credit %d, FIN %v; want nothing", sent, used, fin)
	}

	// With frames held and no credit, the step returns instead of waiting.
	c.mu.Lock()
	sendMax := stream.sendMax
	stream.sendMax = 0
	c.mu.Unlock()
	returned := make(chan bool, 1)
	go func() {
		_, ready, _ := stream.prepareWritePartsLocked(parts, 0, 0, len(payload), writeChunkFinal, writeStepBudget{held: true})
		returned <- ready
	}()
	select {
	case ready := <-returned:
		if ready {
			t.Fatal("step without credit was prepared")
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("step waited for credit while its batch held frames")
	}
	c.mu.Lock()
	stream.sendMax = sendMax
	c.mu.Unlock()

	budget.queued = 40
	step, ready, err := stream.prepareWritePartsLocked(parts, 0, 0, len(payload), writeChunkFinal, budget)
	if !ready || err != nil {
		t.Fatalf("step within budget = (ready %v, %v), want (true, nil)", ready, err)
	}
	if got := txFrameBufferedBytes(step.frame); got != 51 || step.frame.Flags&FrameFlagFIN == 0 {
		t.Fatalf("step frame cost %d flags %#x, want 51 with FIN", got, step.frame.Flags)
	}
	if sent, used, fin := reservations(); sent != 50 || used != 50 || !fin {
		t.Fatalf("prepared step reserved stream credit %d, session credit %d, FIN %v; want 50, 50, true", sent, used, fin)
	}
}
