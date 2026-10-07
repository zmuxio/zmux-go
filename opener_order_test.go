package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

type concurrentOpenCase struct {
	name     string
	streams  int
	rounds   int
	payload  int
	uni      bool
	priority bool
}

// TestConcurrentLocalOpenersReachWireInStreamIDOrder opens many local streams
// from separate goroutines and writes on all of them at once. Every opening
// frame must reach the peer in stream-ID order, or the peer fails the whole
// session with PROTOCOL (SPEC §3.1, §9.6).
func TestConcurrentLocalOpenersReachWireInStreamIDOrder(t *testing.T) {
	t.Parallel()

	tests := []concurrentOpenCase{
		{name: "bidi_many_writers", streams: 64, rounds: 20, payload: 100},
		{name: "bidi_eight_writers", streams: 8, rounds: 100, payload: 100},
		{name: "bidi_mixed_priorities", streams: 64, rounds: 20, payload: 100, priority: true},
		{name: "bidi_first_write_exceeds_window", streams: 16, rounds: 5, payload: 100 << 10},
		{name: "uni_many_writers", uni: true, streams: 64, rounds: 20, payload: 100},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for round := 0; round < tc.rounds; round++ {
				runConcurrentOpenRound(t, tc, round)
			}
		})
	}
}

func runConcurrentOpenRound(t *testing.T, tc concurrentOpenCase, round int) {
	t.Helper()

	client, server := newConnPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	accepted := make(chan uint64, tc.streams)
	acceptErr := make(chan error, 1)
	go func() {
		for i := 0; i < tc.streams; i++ {
			var stream interface {
				io.ReadCloser
				StreamID() uint64
			}
			var err error
			if tc.uni {
				stream, err = server.AcceptUniStream(ctx)
			} else {
				stream, err = server.AcceptStream(ctx)
			}
			if err != nil {
				acceptErr <- err
				return
			}
			accepted <- stream.StreamID()
			go func() {
				_, _ = io.Copy(io.Discard, stream)
				_ = stream.Close()
			}()
		}
	}()

	start := make(chan struct{})
	writeErrs := make(chan error, tc.streams)
	var wg sync.WaitGroup
	for i := 0; i < tc.streams; i++ {
		var opts OpenOptions
		if tc.priority {
			priority := uint64(i % 16)
			opts.InitialPriority = &priority
		}
		var (
			writer io.Writer
			closer func() error
		)
		if tc.uni {
			stream, err := client.OpenUniStreamWithOptions(ctx, opts)
			if err != nil {
				t.Fatalf("round %d: OpenUniStream %d: %v", round, i, err)
			}
			writer, closer = stream, stream.CloseWrite
		} else {
			stream, err := client.OpenStreamWithOptions(ctx, opts)
			if err != nil {
				t.Fatalf("round %d: OpenStream %d: %v", round, i, err)
			}
			writer, closer = stream, stream.Close
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := writer.Write(make([]byte, tc.payload)); err != nil {
				writeErrs <- err
				return
			}
			if err := closer(); err != nil {
				writeErrs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(writeErrs)
	for err := range writeErrs {
		t.Fatalf("round %d: write err = %v (client err = %v, server err = %v)", round, err, client.err(), server.err())
	}

	var last uint64
	for i := 0; i < tc.streams; i++ {
		select {
		case id := <-accepted:
			if i > 0 && id <= last {
				t.Fatalf("round %d: accepted stream %d after %d, want ascending IDs", round, id, last)
			}
			last = id
		case err := <-acceptErr:
			t.Fatalf("round %d: accept %d err = %v (server err = %v)", round, i, err, server.err())
		case <-ctx.Done():
			t.Fatalf("round %d: accepted %d/%d streams (server err = %v)", round, i, tc.streams, server.err())
		}
	}
	if err := server.err(); err != nil {
		t.Fatalf("round %d: server session failed: %v", round, err)
	}
	if err := client.err(); err != nil {
		t.Fatalf("round %d: client session failed: %v", round, err)
	}
	_ = client.Close()
	_ = server.Close()
}

// TestWithdrawnLocalOpenerIsConsumedBeforeLaterOpener withdraws a committed
// opener (its first Write times out while the request is still queued) and
// then opens a later stream of the same class. The withdrawn ID must be
// consumed on the wire, with ABORT(CANCELLED), before the later stream's
// opener (SPEC §3.1).
func TestWithdrawnLocalOpenerIsConsumedBeforeLaterOpener(t *testing.T) {
	t.Parallel()

	client, server, gated := newGatedCaptureConnPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		for {
			stream, err := server.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, stream) }()
		}
	}()

	// Open a first stream and get its opener onto the wire, then park the
	// writer inside the transport with an ordinary follow-up write.
	first, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream first: %v", err)
	}
	if _, err := first.Write([]byte("a")); err != nil {
		t.Fatalf("first opener Write: %v", err)
	}
	gated.block()
	go func() { _, _ = first.Write([]byte("b")) }()
	select {
	case <-gated.entered:
	case <-time.After(testSignalTimeout):
		t.Fatal("writer did not block on the gated transport")
	}

	withdrawn, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream withdrawn: %v", err)
	}
	withdrawnImpl := requireNativeStreamImpl(t, withdrawn)
	if err := withdrawn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if _, err := withdrawn.Write([]byte("w")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("withdrawn opener Write err = %v, want deadline", err)
	}
	pollStreamState(t, withdrawnImpl, func(s *nativeStream) bool {
		return s.idSet && s.sendAbortErrLocked() != nil
	}, "withdrawn opener was not consumed with a local ABORT")

	later, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream later: %v", err)
	}
	laterDone := make(chan error, 1)
	go func() {
		_, err := later.Write([]byte("l"))
		laterDone <- err
	}()

	gated.open()
	select {
	case err := <-laterDone:
		if err != nil {
			t.Fatalf("later opener Write err = %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("later opener Write did not complete after the writer was released")
	}
	if _, err := withdrawn.Write([]byte("again")); err == nil {
		t.Fatal("Write on the consumed stream succeeded, want the local abort error")
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	defer pingCancel()
	if _, err := client.Ping(pingCtx, nil); err != nil {
		t.Fatalf("Ping err = %v (server err = %v)", err, server.err())
	}
	if err := server.err(); err != nil {
		t.Fatalf("server session failed: %v", err)
	}

	withdrawnID, laterID := withdrawn.StreamID(), later.StreamID()
	if laterID != withdrawnID+4 {
		t.Fatalf("later stream ID = %d, want %d", laterID, withdrawnID+4)
	}
	firstSeen := map[uint64]int{}
	var withdrawnFrames []Frame
	for i, frame := range gated.framesAfterPreface(t) {
		if _, ok := firstSeen[frame.StreamID]; !ok {
			firstSeen[frame.StreamID] = i
		}
		if frame.StreamID == withdrawnID {
			withdrawnFrames = append(withdrawnFrames, frame)
		}
	}
	if len(withdrawnFrames) == 0 || withdrawnFrames[0].Type != FrameTypeABORT {
		t.Fatalf("withdrawn stream frames = %v, want ABORT first", frameTypes(withdrawnFrames))
	}
	code, _, err := parseErrorPayload(withdrawnFrames[0].Payload)
	if err != nil || code != uint64(CodeCancelled) {
		t.Fatalf("withdrawn stream ABORT code = %d (err %v), want %d", code, err, uint64(CodeCancelled))
	}
	laterIdx, ok := firstSeen[laterID]
	if !ok || laterIdx < firstSeen[withdrawnID] {
		t.Fatalf("stream %d first frame at %d, before stream %d at %d", laterID, laterIdx, withdrawnID, firstSeen[withdrawnID])
	}
}

func frameTypes(frames []Frame) []string {
	out := make([]string, 0, len(frames))
	for _, frame := range frames {
		out = append(out, fmt.Sprint(frame.Type))
	}
	return out
}

// TestInvalidOpenerDoesNotBurnStreamID checks that a local opener validation
// error is reported before a stream ID is committed, so the next stream of the
// class still opens with the first ID and no peer-visible gap appears.
func TestInvalidOpenerDoesNotBurnStreamID(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	invalid, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream invalid: %v", err)
	}
	invalidImpl := requireNativeStreamImpl(t, invalid)
	client.mu.Lock()
	firstID := client.registry.nextLocalBidi
	invalidImpl.openMetadataPrefix = make([]byte, client.config.peer.Settings.MaxFramePayload+1)
	client.mu.Unlock()

	if _, err := invalid.Write([]byte("x")); !errors.Is(err, ErrOpenMetadataTooLarge) {
		t.Fatalf("invalid opener Write err = %v, want %v", err, ErrOpenMetadataTooLarge)
	}
	client.mu.Lock()
	idSet, nextID, provisionals := invalidImpl.idSet, client.registry.nextLocalBidi, client.provisionalCountLocked(streamArityBidi)
	client.mu.Unlock()
	if idSet || nextID != firstID || provisionals != 0 {
		t.Fatalf("after invalid opener: idSet=%v nextLocalBidi=%d provisionals=%d, want false/%d/0", idSet, nextID, provisionals, firstID)
	}

	valid, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream valid: %v", err)
	}
	if _, err := valid.Write([]byte("y")); err != nil {
		t.Fatalf("valid opener Write: %v", err)
	}
	if got := valid.StreamID(); got != firstID {
		t.Fatalf("valid stream ID = %d, want %d", got, firstID)
	}
	accepted, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v (server err = %v)", err, server.err())
	}
	if got := accepted.StreamID(); got != firstID {
		t.Fatalf("accepted stream ID = %d, want %d", got, firstID)
	}
}

// TestProvisionalStreamDoesNotAgeWhileWaitingForOpenerTurn keeps a committed
// opener queued behind a stalled writer. A later provisional stream of the
// same class waits for that opener, and the wait must not make it expire.
func TestProvisionalStreamDoesNotAgeWhileWaitingForOpenerTurn(t *testing.T) {
	t.Parallel()

	client, server, gated := newGatedCaptureConnPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		for {
			stream, err := server.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, stream) }()
		}
	}()

	parked, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream parked: %v", err)
	}
	if _, err := parked.Write([]byte("a")); err != nil {
		t.Fatalf("parked opener Write: %v", err)
	}
	gated.block()
	go func() { _, _ = parked.Write([]byte("b")) }()
	select {
	case <-gated.entered:
	case <-time.After(testSignalTimeout):
		t.Fatal("writer did not block on the gated transport")
	}

	holder, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream holder: %v", err)
	}
	holderImpl := requireNativeStreamImpl(t, holder)
	holderDone := make(chan error, 1)
	go func() {
		_, err := holder.Write([]byte("h"))
		holderDone <- err
	}()
	pollStreamState(t, holderImpl, func(s *nativeStream) bool {
		return s.conn.holdsLocalOpenerTurnLocked(s)
	}, "holder did not take the opener turn")

	waiter, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream waiter: %v", err)
	}
	waiterImpl := requireNativeStreamImpl(t, waiter)
	client.mu.Lock()
	waiterImpl.setProvisionalCreated(time.Now().Add(-provisionalOpenMaxAge - time.Second))
	client.mu.Unlock()
	waiterDone := make(chan error, 1)
	go func() {
		_, err := waiter.Write([]byte("w"))
		waiterDone <- err
	}()

	// Opening another stream reaps expired provisional heads; the waiter's
	// age must not count while the opener turn is held.
	if _, err := client.OpenStream(ctx); err != nil {
		t.Fatalf("OpenStream reaper: %v", err)
	}
	client.mu.Lock()
	expired := client.provisionalExpiredLocked(waiterImpl, time.Now())
	client.mu.Unlock()
	if expired {
		t.Fatal("provisional waiter expired while waiting for the opener turn")
	}
	select {
	case err := <-waiterDone:
		t.Fatalf("waiter Write returned before the opener turn was released: %v", err)
	default:
	}

	gated.open()
	for name, done := range map[string]chan error{"holder": holderDone, "waiter": waiterDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s Write err = %v", name, err)
			}
		case <-time.After(testSignalTimeout):
			t.Fatalf("%s Write did not complete after the writer was released", name)
		}
	}
	if got, want := waiter.StreamID(), holder.StreamID()+4; got != want {
		t.Fatalf("waiter stream ID = %d, want %d", got, want)
	}
	if err := server.err(); err != nil {
		t.Fatalf("server session failed: %v", err)
	}
}

// TestProvisionalWaiterDoesNotAgeBehindAbandonedHead opens two streams at the
// same instant. The first is abandoned and about to expire; the second waits
// behind it to commit. That wait is not idle provisional time, so the second
// stream opens once the head expires instead of expiring with it.
func TestProvisionalWaiterDoesNotAgeBehindAbandonedHead(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	abandoned, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream abandoned: %v", err)
	}
	writer, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream writer: %v", err)
	}
	client.mu.Lock()
	created := time.Now().Add(-client.provisionalOpenMaxAgeLocked() + 200*time.Millisecond)
	requireNativeStreamImpl(t, abandoned).setProvisionalCreated(created)
	requireNativeStreamImpl(t, writer).setProvisionalCreated(created)
	client.mu.Unlock()

	if _, err := writer.Write([]byte("w")); err != nil {
		t.Fatalf("writer Write err = %v, want nil", err)
	}
	if _, err := abandoned.Write([]byte("late")); !errors.Is(err, ErrOpenExpired) {
		t.Fatalf("abandoned Write err = %v, want %v", err, ErrOpenExpired)
	}
	if got := client.Stats().Provisionals.Expired; got != 1 {
		t.Fatalf("provisional expired = %d, want 1", got)
	}

	accepted, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if got, want := accepted.StreamID(), writer.StreamID(); got != want {
		t.Fatalf("accepted stream ID = %d, want %d", got, want)
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(accepted, buf); err != nil || string(buf) != "w" {
		t.Fatalf("accepted read = %q, %v; want \"w\"", buf, err)
	}
}

// TestUnbegunProvisionalOpenTurnWaitRetriesAtOnce pins the "retry now" wait
// that prepareRetriableLocalOpenerLocked returns when a stream awaiting its
// turn is no longer blocked by the time the wait is built: it must neither
// block nor end a commit wait it never began.
func TestUnbegunProvisionalOpenTurnWaitRetriesAtOnce(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()
	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	impl := requireNativeStreamImpl(t, stream)
	client.mu.Lock()
	impl.beginProvisionalCommitWaitLocked(time.Now())
	client.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- provisionalOpenTurnWait{}.wait(impl, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("zero wait err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("zero provisionalOpenTurnWait blocked")
	}
	client.mu.Lock()
	_, waiting := impl.provisionalAgeOriginLocked()
	client.mu.Unlock()
	if !waiting {
		t.Fatal("zero provisionalOpenTurnWait ended a commit wait it did not begin")
	}
}

// TestOpenerTurnReleaseWakesEveryWaiterOfAStream has a Write and a CloseRead
// wait for the opener turn on the same provisional stream. Whichever of them
// opens the stream, the other must not stay blocked once the turn is released.
func TestOpenerTurnReleaseWakesEveryWaiterOfAStream(t *testing.T) {
	t.Parallel()

	for round := 0; round < 10; round++ {
		c, peer := newStalledRawPeerConn(t, nil, DefaultSettings())

		// The writer blocks on the stalled transport with the first stream's
		// opener, so the second stream holds the bidi opener turn until the
		// peer starts reading.
		var holder *nativeStream
		for _, payload := range []string{"a", "b"} {
			stream, err := c.OpenStream(t.Context())
			if err != nil {
				t.Fatalf("round %d: OpenStream: %v", round, err)
			}
			holder = requireNativeStreamImpl(t, stream)
			go func() { _, _ = stream.Write([]byte(payload)) }()
		}
		pollStreamState(t, holder, func(s *nativeStream) bool {
			return s.conn.holdsLocalOpenerTurnLocked(s)
		}, "no opener turn held behind the stalled writer")

		stream, err := c.OpenStream(t.Context())
		if err != nil {
			t.Fatalf("round %d: OpenStream: %v", round, err)
		}
		results := make(chan error, 2)
		go func() {
			_, err := stream.Write([]byte("x"))
			results <- err
		}()
		go func() { results <- stream.CloseRead() }()
		pollStreamWriteWaiters(t, requireNativeStreamImpl(t, stream), 2, "Write and CloseRead did not both wait for the opener turn")

		peer.startReading()
		for i := 0; i < 2; i++ {
			select {
			case err := <-results:
				if err != nil {
					t.Fatalf("round %d: Write/CloseRead err = %v", round, err)
				}
			case <-time.After(testSignalTimeout):
				t.Fatalf("round %d: a Write or CloseRead stayed blocked after the opener turn was released", round)
			}
		}
		c.CloseWithError(nil)
	}
}
