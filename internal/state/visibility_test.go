package state

import "testing"

func testPhase(localOpened, sendCommitted, peerVisible, openingBarrier bool) LocalOpenPhase {
	return LocalOpenVisibility{
		LocalOpened:   localOpened,
		SendCommitted: sendCommitted,
		PeerVisible:   peerVisible,
		OpenerQueued:  openingBarrier,
	}.Phase()
}

func TestVisibilityAndAcceptancePredicates(t *testing.T) {
	t.Parallel()
	if !ShouldEnqueueAccepted(true, false, false) {
		t.Fatal("expected visible unaccepted stream to be enqueued")
	}
	if ShouldEnqueueAccepted(false, false, false) {
		t.Fatal("expected hidden stream to stay out of accept queue")
	}
	if ShouldEnqueueAccepted(true, true, false) {
		t.Fatal("expected accepted stream not to be re-enqueued")
	}
	phase := testPhase(true, false, false, false)
	if !phase.ShouldMarkPeerVisible() {
		t.Fatal("expected unseen local stream to become peer-visible")
	}
	if LocalOpenPhaseNone.ShouldMarkPeerVisible() {
		t.Fatal("expected peer-opened stream not to use local peer-visible marker")
	}
	if !phase.NeedsLocalOpener() {
		t.Fatal("expected local-open visibility to require opener before send commit")
	}
	phase = testPhase(true, true, false, false)
	if !phase.AwaitingPeerVisibility() {
		t.Fatal("expected committed local-open stream to await peer visibility")
	}
	if !phase.ShouldEmitOpenerFrame() {
		t.Fatal("expected committed local-open stream without barrier to emit opener")
	}
	phase = testPhase(true, true, false, true)
	if phase.ShouldEmitOpenerFrame() {
		t.Fatal("expected opening barrier to suppress opener emission")
	}
	phase = testPhase(true, true, true, true)
	if phase.CanTakePendingPriorityUpdate() != true {
		t.Fatal("expected peer-visible stream to accept pending priority updates")
	}
	if !phase.ShouldQueueStreamBlocked(0) {
		t.Fatal("expected peer-visible committed stream to queue BLOCKED at zero credit")
	}
}

func TestGoAwayReclaimAndPeerActivePredicates(t *testing.T) {
	t.Parallel()
	phase := testPhase(true, false, false, false)
	if !ShouldReclaimUnseenLocalStream(phase, true, 12, 8, 99, true, true, SendHalfOpen, RecvHalfOpen) {
		t.Fatal("expected unseen bidi local stream above GOAWAY watermark to be reclaimed")
	}
	phase = testPhase(true, true, true, false)
	if ShouldReclaimUnseenLocalStream(phase, true, 12, 8, 99, true, true, SendHalfOpen, RecvHalfOpen) {
		t.Fatal("expected peer-visible local stream not to be reclaimed")
	}
	phase = testPhase(true, true, false, false)
	if ShouldReclaimUnseenLocalStream(phase, false, 101, 99, 100, true, true, SendHalfReset, RecvHalfFin) {
		t.Fatal("expected terminal local stream not to be reclaimed")
	}
	if !ShouldFinalizePeerActive(true, false, true, true, SendHalfReset, RecvHalfFin) {
		t.Fatal("expected terminal peer-opened active stream to decrement counts")
	}
	if ShouldFinalizePeerActive(true, true, true, true, SendHalfReset, RecvHalfFin) {
		t.Fatal("expected local-opened stream not to decrement peer-active counts")
	}
}

func TestPendingControlFlushPredicates(t *testing.T) {
	t.Parallel()

	phase := testPhase(true, false, false, false)
	if flush, keep := ShouldFlushStreamMaxData(true, true, phase, false, false); flush || !keep {
		t.Fatalf("stream MAX_DATA before opener = (%v,%v), want (false,true)", flush, keep)
	}
	if flush, keep := ShouldFlushStreamBlocked(true, true, phase, SendHalfOpen); flush || !keep {
		t.Fatalf("stream BLOCKED before opener = (%v,%v), want (false,true)", flush, keep)
	}
	if flush, keep := ShouldFlushPriorityUpdate(phase, SendHalfOpen); flush || !keep {
		t.Fatalf("PRIORITY_UPDATE before peer-visible opener = (%v,%v), want (false,true)", flush, keep)
	}

	phase = testPhase(true, true, false, false)
	if flush, keep := ShouldFlushStreamMaxData(true, true, phase, false, false); !flush || keep {
		t.Fatalf("stream MAX_DATA after opener commit = (%v,%v), want (true,false)", flush, keep)
	}
	if flush, keep := ShouldFlushStreamBlocked(true, true, phase, SendHalfOpen); !flush || keep {
		t.Fatalf("stream BLOCKED after opener commit = (%v,%v), want (true,false)", flush, keep)
	}

	if flush, keep := ShouldFlushPriorityUpdate(phase, SendHalfOpen); flush || !keep {
		t.Fatalf("PRIORITY_UPDATE while awaiting peer visibility = (%v,%v), want (false,true)", flush, keep)
	}

	phase = testPhase(true, true, true, false)
	if flush, keep := ShouldFlushPriorityUpdate(phase, SendHalfOpen); !flush || keep {
		t.Fatalf("PRIORITY_UPDATE after peer visibility = (%v,%v), want (true,false)", flush, keep)
	}
	if flush, keep := ShouldFlushPriorityUpdate(phase, SendHalfStopSeen); flush || keep {
		t.Fatalf("PRIORITY_UPDATE after stop-seen = (%v,%v), want (false,false)", flush, keep)
	}
}
