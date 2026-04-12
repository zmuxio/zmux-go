package state

import "testing"

func TestPeerStreamControlActions(t *testing.T) {
	t.Parallel()

	if got := PeerMaxDataAction(false, true, SendHalfAbsent, RecvHalfOpen); got != PeerStreamControlAbortState {
		t.Fatalf("PeerMaxDataAction(recv-only uni) = %v, want abort-state", got)
	}
	if got := PeerMaxDataAction(true, true, SendHalfReset, RecvHalfFin); got != PeerStreamControlIgnore {
		t.Fatalf("PeerMaxDataAction(fully terminal) = %v, want ignore", got)
	}
	if got := PeerMaxDataAction(true, true, SendHalfFin, RecvHalfOpen); got != PeerStreamControlApply {
		t.Fatalf("PeerMaxDataAction(send_fin only) = %v, want apply", got)
	}

	if got := PeerBlockedAction(true, false, SendHalfOpen, RecvHalfAbsent); got != PeerStreamControlAbortState {
		t.Fatalf("PeerBlockedAction(send-only uni) = %v, want abort-state", got)
	}
	if got := PeerBlockedAction(true, true, SendHalfReset, RecvHalfFin); got != PeerStreamControlIgnore {
		t.Fatalf("PeerBlockedAction(fully terminal) = %v, want ignore", got)
	}
	if got := PeerBlockedAction(true, true, SendHalfFin, RecvHalfOpen); got != PeerStreamControlApply {
		t.Fatalf("PeerBlockedAction(recv side still open) = %v, want apply", got)
	}
}

func TestShouldAdvertiseFlowControl(t *testing.T) {
	t.Parallel()

	if !ShouldAdvertiseMaxData(true, RecvHalfOpen) {
		t.Fatal("ShouldAdvertiseMaxData(open recv half) = false, want true")
	}
	if ShouldAdvertiseMaxData(true, RecvHalfStopSent) {
		t.Fatal("ShouldAdvertiseMaxData(recv_stop_sent) = true, want false")
	}
	if ShouldAdvertiseMaxData(true, RecvHalfReset) {
		t.Fatal("ShouldAdvertiseMaxData(recv_reset) = true, want false")
	}
	if ShouldAdvertiseMaxData(false, RecvHalfOpen) {
		t.Fatal("ShouldAdvertiseMaxData(absent recv half) = true, want false")
	}

	if !ShouldAdvertiseBlocked(true, SendHalfOpen) {
		t.Fatal("ShouldAdvertiseBlocked(send_open) = false, want true")
	}
	if ShouldAdvertiseBlocked(true, SendHalfStopSeen) {
		t.Fatal("ShouldAdvertiseBlocked(send_stop_seen) = true, want false")
	}
	if ShouldAdvertiseBlocked(true, SendHalfFin) {
		t.Fatal("ShouldAdvertiseBlocked(send_fin) = true, want false")
	}
	if ShouldAdvertiseBlocked(false, SendHalfOpen) {
		t.Fatal("ShouldAdvertiseBlocked(absent send half) = true, want false")
	}
}
