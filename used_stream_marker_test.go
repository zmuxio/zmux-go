package zmux

import (
	"context"
	"io"
	"testing"
)

func newUsedStreamMarkerTestConn(limit int) *Conn {
	c := newSessionMemoryTestConn()
	c.retention.markerOnlyLimit = limit
	c.registry.usedStreamRangeMode = true
	return c
}

// Ranges of different stream ID classes never sit between two ranges of one
// class, so interleaved classes still merge into one range per class.
func TestUsedStreamMarkersMergeWithinInterleavedClasses(t *testing.T) {
	t.Parallel()

	c := newUsedStreamMarkerTestConn(0)
	marker := usedStreamMarker{action: lateDataAbortClosed, cause: lateDataCauseNone}
	const n = 10000

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := uint64(1); i <= n; i++ {
		c.markUsedStreamLocked(4*i, marker)
		c.markUsedStreamLocked(4*i+1, marker)
		c.markUsedStreamLocked(4*i+2, marker)
	}
	if got := c.markerOnlyRangeCountLocked(); got != 3 {
		t.Fatalf("marker ranges after %d interleaved IDs per class = %d, want one per class", n, got)
	}
	for i := uint64(1); i <= n; i++ {
		for _, id := range []uint64{4 * i, 4*i + 1, 4*i + 2} {
			if got, ok := c.usedStreamMarkerForLocked(id); !ok || !sameUsedStreamMarker(got, marker) {
				t.Fatalf("marker for stream %d = (%+v,%v), want %+v,true", id, got, ok, marker)
			}
		}
	}
	if _, ok := c.usedStreamMarkerForLocked(4*n + 3); ok {
		t.Fatalf("unused stream %d reads as used", 4*n+3)
	}
}

// A range of another class inside a range's span must not hide IDs from the
// lookup.
func TestUsedStreamMarkerLookupSurvivesOtherClassInsideSpan(t *testing.T) {
	t.Parallel()

	c := newUsedStreamMarkerTestConn(0)
	graceful := usedStreamMarker{action: lateDataAbortClosed, cause: lateDataCauseNone}
	reset := usedStreamMarker{action: lateDataIgnore, cause: lateDataCauseReset}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.markUsedStreamLocked(4, graceful)
	c.markUsedStreamLocked(8, graceful)
	c.markUsedStreamLocked(5, reset)
	for id, want := range map[uint64]usedStreamMarker{4: graceful, 8: graceful, 5: reset} {
		if got, ok := c.usedStreamMarkerForLocked(id); !ok || !sameUsedStreamMarker(got, want) {
			t.Fatalf("marker for stream %d = (%+v,%v), want %+v,true", id, got, ok, want)
		}
	}
}

// Over budget, the oldest markers are coarsened instead of failing the
// session: the retained count stays bounded, every used ID still reads as
// used, and the newest IDs keep their exact disposition.
func TestUsedStreamMarkerBudgetCoarsensOldestRangesWithoutFailing(t *testing.T) {
	t.Parallel()

	const limit = 16
	c := newUsedStreamMarkerTestConn(limit)
	markers := []usedStreamMarker{
		{action: lateDataAbortClosed, cause: lateDataCauseNone},
		{action: lateDataIgnore, cause: lateDataCauseReset},
		{action: lateDataIgnore, cause: lateDataCauseAbort},
	}
	const n = 4 * limit * 4

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := uint64(1); i <= n; i++ {
		c.markUsedStreamLocked(4*i, markers[i%3])
		c.markUsedStreamLocked(4*i+2, markers[(i+1)%3])
		if got := c.markerOnlyRetainedLocked(); got > limit {
			t.Fatalf("retained markers after %d IDs = %d, want <= %d", i, got, limit)
		}
	}
	select {
	case <-c.lifecycle.closedCh:
		t.Fatalf("session closed by marker budget: %v", c.lifecycle.closeErr)
	default:
	}
	for i := uint64(1); i <= n; i++ {
		for _, id := range []uint64{4 * i, 4*i + 2} {
			if _, ok := c.usedStreamMarkerForLocked(id); !ok {
				t.Fatalf("used stream %d forgotten after coarsening", id)
			}
		}
	}
	if got, ok := c.usedStreamMarkerForLocked(4); !ok || !sameUsedStreamMarker(got, coarsenedUsedStreamMarker) {
		t.Fatalf("oldest marker = (%+v,%v), want coarsened %+v", got, ok, coarsenedUsedStreamMarker)
	}
	if got, ok := c.usedStreamMarkerForLocked(4 * n); !ok || !sameUsedStreamMarker(got, markers[n%3]) {
		t.Fatalf("newest marker = (%+v,%v), want %+v", got, ok, markers[n%3])
	}
	for _, id := range []uint64{4*n + 4, 4*n + 6, 1, 3} {
		if _, ok := c.usedStreamMarkerForLocked(id); ok {
			t.Fatalf("unused stream %d reads as used after coarsening", id)
		}
	}

	// A later reap of an ID inside the coarsened prefix does not re-fragment.
	before := c.markerOnlyRetainedLocked()
	c.markUsedStreamLocked(8, markers[0])
	if got := c.markerOnlyRetainedLocked(); got != before {
		t.Fatalf("retained markers after marking a coarsened ID = %d, want %d", got, before)
	}
	if got, _ := c.usedStreamMarkerForLocked(8); !sameUsedStreamMarker(got, coarsenedUsedStreamMarker) {
		t.Fatalf("marker for coarsened stream 8 = %+v, want %+v", got, coarsenedUsedStreamMarker)
	}
}

// Late frames for a coarsened ID are handled like any forgotten stream:
// DATA is discarded and its session credit released, control is ignored.
func TestLateFramesOnCoarsenedUsedStreamKeepSession(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	first := c.registry.nextPeerBidi
	c.mu.Lock()
	c.retention.markerOnlyLimit = 2
	marker := []usedStreamMarker{
		{action: lateDataAbortClosed, cause: lateDataCauseNone},
		{action: lateDataIgnore, cause: lateDataCauseReset},
	}
	for i := uint64(0); i < 16; i++ {
		c.markUsedStreamLocked(first+4*i, marker[i%2])
	}
	c.registry.nextPeerBidi = first + 4*16
	coarsened, ok := c.usedStreamMarkerForLocked(first)
	beforeReceived := c.flow.recvSessionReceived
	beforeAdvertised := c.flow.recvSessionAdvertised
	c.mu.Unlock()
	if !ok || !sameUsedStreamMarker(coarsened, coarsenedUsedStreamMarker) {
		t.Fatalf("marker for stream %d = (%+v,%v), want coarsened", first, coarsened, ok)
	}

	if err := c.handleDataFrame(Frame{Type: FrameTypeDATA, StreamID: first, Payload: []byte("late")}); err != nil {
		t.Fatalf("late DATA on coarsened stream: %v", err)
	}
	if err := c.handleStopSendingFrame(Frame{Type: FrameTypeStopSending, StreamID: first, Payload: mustEncodeVarint(uint64(CodeCancelled))}); err != nil {
		t.Fatalf("late STOP_SENDING on coarsened stream: %v", err)
	}
	if err := c.handleResetFrame(Frame{Type: FrameTypeRESET, StreamID: first, Payload: mustEncodeVarint(uint64(CodeCancelled))}); err != nil {
		t.Fatalf("late RESET on coarsened stream: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.flow.recvSessionReceived - beforeReceived; got != 4 {
		t.Fatalf("late DATA counted %d session bytes, want 4", got)
	}
	if got := c.flow.recvSessionAdvertised - beforeAdvertised; got != 4 {
		t.Fatalf("late DATA released %d session bytes, want 4", got)
	}
}

// Both peers opening streams and closing them in mixed ways leaves
// interleaved, mixed-disposition markers. Retention stays bounded and the
// sessions stay healthy.
func TestInterleavedStreamClassesKeepMarkerRetentionBounded(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.TombstoneLimit = 2
	cfg.MarkerOnlyUsedStreamLimit = 8
	clientCfg, serverCfg := *cfg, *cfg
	client, server := newConnPairWithConfig(t, &clientCfg, &serverCfg)
	ctx, cancel := context.WithTimeout(context.Background(), 4*testSignalTimeout)
	defer cancel()

	exchange := func(opener, acceptor *Conn, round int) {
		t.Helper()
		out, err := opener.OpenStream(ctx)
		if err != nil {
			t.Fatalf("round %d open: %v", round, err)
		}
		if _, err := out.Write([]byte("x")); err != nil {
			t.Fatalf("round %d write: %v", round, err)
		}
		if err := out.CloseWrite(); err != nil {
			t.Fatalf("round %d close write: %v", round, err)
		}
		in, err := acceptor.AcceptStream(ctx)
		if err != nil {
			t.Fatalf("round %d accept: %v", round, err)
		}
		if _, err := io.ReadAll(in); err != nil {
			t.Fatalf("round %d read: %v", round, err)
		}
		if round%3 == 0 {
			_ = in.CloseWithError(uint64(CodeCancelled), "")
		} else {
			_ = in.Close()
		}
		if _, err := io.ReadAll(out); err != nil && round%3 != 0 {
			t.Fatalf("round %d opener read: %v", round, err)
		}
		_ = out.Close()
	}
	for round := 0; round < 300; round++ {
		exchange(client, server, round)
		exchange(server, client, round)
	}

	for _, c := range []*Conn{client, server} {
		if err := c.err(); err != nil {
			t.Fatalf("session failed: %v", err)
		}
		c.mu.Lock()
		retained := c.markerOnlyRetainedLocked()
		floors := c.registry.usedStreamFloors
		c.mu.Unlock()
		if retained > cfg.MarkerOnlyUsedStreamLimit {
			t.Fatalf("retained markers = %d, want <= %d", retained, cfg.MarkerOnlyUsedStreamLimit)
		}
		if floors == [usedStreamClasses]uint64{} {
			t.Fatal("no marker was ever coarsened; the workload did not exceed the marker budget")
		}
	}
}
