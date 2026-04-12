package state

import "testing"

func TestIgnorePeerControlPredicates(t *testing.T) {
	t.Parallel()

	if !IgnorePeerStopSending(true, true, SendHalfStopSeen, RecvHalfOpen) {
		t.Fatal("expected repeated STOP_SENDING to be ignored once send_stop_seen")
	}
	if IgnorePeerStopSending(true, true, SendHalfOpen, RecvHalfOpen) {
		t.Fatal("expected first STOP_SENDING on send_open to remain actionable")
	}
	if !IgnorePeerReset(true, true, SendHalfFin, RecvHalfReset) {
		t.Fatal("expected repeated RESET to be ignored once recv_reset")
	}
	if IgnorePeerReset(true, true, SendHalfFin, RecvHalfStopSent) {
		t.Fatal("expected RESET after recv_stop_sent to remain actionable")
	}
	if !IgnorePeerAbort(true, true, SendHalfAborted, RecvHalfOpen) {
		t.Fatal("expected repeated ABORT to be ignored once a half is aborted")
	}
	if IgnorePeerAbort(true, true, SendHalfOpen, RecvHalfReset) {
		t.Fatal("expected ABORT after reset-only state to remain actionable")
	}
}

func TestPeerDataTransitionUsesExplicitHalfState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		localSend   bool
		localRecv   bool
		sendHalf    SendHalfState
		recvHalf    RecvHalfState
		fin         bool
		wantOutcome PeerDataOutcome
		wantAdvance bool
		wantTrack   bool
	}{
		{
			name:        "open_accepts",
			localSend:   true,
			localRecv:   true,
			sendHalf:    SendHalfOpen,
			recvHalf:    RecvHalfOpen,
			wantOutcome: PeerDataAccept,
		},
		{
			name:        "wrong_direction_aborts_state",
			localSend:   true,
			localRecv:   false,
			sendHalf:    SendHalfOpen,
			recvHalf:    RecvHalfAbsent,
			wantOutcome: PeerDataAbortState,
		},
		{
			name:        "recv_fin_aborts_closed",
			localSend:   true,
			localRecv:   true,
			sendHalf:    SendHalfOpen,
			recvHalf:    RecvHalfFin,
			wantOutcome: PeerDataAbortClosed,
		},
		{
			name:        "recv_reset_ignores_with_stream_late_tracking",
			localSend:   true,
			localRecv:   true,
			sendHalf:    SendHalfOpen,
			recvHalf:    RecvHalfReset,
			wantOutcome: PeerDataIgnore,
			wantTrack:   true,
		},
		{
			name:        "recv_stop_sent_can_advance_fin",
			localSend:   true,
			localRecv:   true,
			sendHalf:    SendHalfFin,
			recvHalf:    RecvHalfStopSent,
			fin:         true,
			wantOutcome: PeerDataIgnore,
			wantAdvance: true,
			wantTrack:   true,
		},
		{
			name:        "full_terminal_overrides_recv_fin_abort_closed",
			localSend:   true,
			localRecv:   true,
			sendHalf:    SendHalfReset,
			recvHalf:    RecvHalfFin,
			wantOutcome: PeerDataIgnore,
		},
		{
			name:        "recv_aborted_keeps_per_stream_late_tracking",
			localSend:   true,
			localRecv:   true,
			sendHalf:    SendHalfFin,
			recvHalf:    RecvHalfAborted,
			wantOutcome: PeerDataIgnore,
			wantTrack:   true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := PeerDataTransition(tc.localSend, tc.localRecv, tc.sendHalf, tc.recvHalf, tc.fin)
			if got.Outcome != tc.wantOutcome {
				t.Fatalf("outcome = %v, want %v", got.Outcome, tc.wantOutcome)
			}
			if got.AdvanceRecvFin != tc.wantAdvance {
				t.Fatalf("advanceRecvFin = %v, want %v", got.AdvanceRecvFin, tc.wantAdvance)
			}
			if got.TrackLatePerStream != tc.wantTrack {
				t.Fatalf("trackLatePerStream = %v, want %v", got.TrackLatePerStream, tc.wantTrack)
			}
		})
	}
}

func TestPeerStopSendingOutcome(t *testing.T) {
	t.Parallel()

	if got := PeerStopSendingOutcome(true, true, SendHalfStopSeen, RecvHalfOpen); got != StopSendingIgnore {
		t.Fatalf("repeated STOP_SENDING outcome = %v, want ignore", got)
	}
	if got := PeerStopSendingOutcome(true, true, SendHalfOpen, RecvHalfOpen); got != StopSendingFinish {
		t.Fatalf("send_open/recv_open with no tail outcome = %v, want finish", got)
	}
	if got := PeerStopSendingOutcome(true, true, SendHalfOpen, RecvHalfReset); got != StopSendingReset {
		t.Fatalf("recv_reset outcome = %v, want reset", got)
	}
	if got := PeerStopSendingOutcome(true, true, SendHalfOpen, RecvHalfAborted); got != StopSendingIgnore {
		t.Fatalf("recv_aborted outcome = %v, want ignore", got)
	}
}

func TestPeerControlPlans(t *testing.T) {
	t.Parallel()

	stopIgnore := PlanPeerStopSending(true, true, SendHalfStopSeen, RecvHalfOpen)
	if !stopIgnore.Ignore || stopIgnore.RecordStop || stopIgnore.Outcome != StopSendingIgnore {
		t.Fatalf("repeated STOP_SENDING plan = %#v, want ignore", stopIgnore)
	}

	stopFinish := PlanPeerStopSending(true, true, SendHalfOpen, RecvHalfOpen)
	if stopFinish.Ignore || !stopFinish.RecordStop || stopFinish.Outcome != StopSendingFinish {
		t.Fatalf("first STOP_SENDING plan = %#v, want record+finish", stopFinish)
	}

	stopIgnoreAfterAbort := PlanPeerStopSending(true, true, SendHalfOpen, RecvHalfAborted)
	if !stopIgnoreAfterAbort.Ignore || stopIgnoreAfterAbort.RecordStop || stopIgnoreAfterAbort.Outcome != StopSendingIgnore {
		t.Fatalf("recv_aborted STOP_SENDING plan = %#v, want ignore", stopIgnoreAfterAbort)
	}

	resetIgnore := PlanPeerReset(true, true, SendHalfFin, RecvHalfReset)
	if !resetIgnore.Ignore || resetIgnore.RecordReset || resetIgnore.ReleaseReceive || resetIgnore.ClearReadBuf {
		t.Fatalf("repeated RESET plan = %#v, want ignore", resetIgnore)
	}

	resetApply := PlanPeerReset(true, true, SendHalfOpen, RecvHalfStopSent)
	if resetApply.Ignore || !resetApply.RecordReset || !resetApply.ReleaseReceive || !resetApply.ClearReadBuf {
		t.Fatalf("recv_stop_sent RESET plan = %#v, want reset+release+clear", resetApply)
	}

	abortIgnore := PlanPeerAbort(true, true, SendHalfAborted, RecvHalfOpen)
	if !abortIgnore.Ignore || abortIgnore.RecordAbort || abortIgnore.ReleaseSend || abortIgnore.ReleaseReceive || abortIgnore.ClearReadBuf {
		t.Fatalf("repeated ABORT plan = %#v, want ignore", abortIgnore)
	}

	abortApply := PlanPeerAbort(true, true, SendHalfReset, RecvHalfOpen)
	if abortApply.Ignore || !abortApply.RecordAbort || !abortApply.ReleaseSend || !abortApply.ReleaseReceive || !abortApply.ClearReadBuf {
		t.Fatalf("first ABORT after reset-only state plan = %#v, want abort+release+clear", abortApply)
	}
}

func TestTerminalErrorPriorityHelpers(t *testing.T) {
	t.Parallel()

	if got := TerminalErrorPriority(SendHalfAborted, RecvHalfReset); got != TerminalErrorSendAbort {
		t.Fatalf("send_abort priority = %v, want send-abort", got)
	}
	if got := TerminalErrorPriority(SendHalfReset, RecvHalfAborted); got != TerminalErrorRecvAbort {
		t.Fatalf("recv_abort priority = %v, want recv-abort", got)
	}
	if got := TerminalErrorPriority(SendHalfReset, RecvHalfReset); got != TerminalErrorSendReset {
		t.Fatalf("send_reset priority = %v, want send-reset", got)
	}
	if got := TerminalErrorPriority(SendHalfFin, RecvHalfFin); got != TerminalErrorSendClosed {
		t.Fatalf("send_fin priority = %v, want send-closed", got)
	}
	if got := ReadErrorChoice(true, true, RecvHalfReset); got != TerminalErrorRecvClosed {
		t.Fatalf("local read stop choice = %v, want recv-closed", got)
	}
	if got := ReadErrorChoice(true, false, RecvHalfReset); got != TerminalErrorRecvReset {
		t.Fatalf("recv_reset choice = %v, want recv-reset", got)
	}
}

func TestLocalActionPlans(t *testing.T) {
	t.Parallel()

	sendTests := []struct {
		name      string
		localSend bool
		sendHalf  SendHalfState
		want      LocalSendAction
	}{
		{name: "close-write absent", localSend: false, sendHalf: SendHalfAbsent, want: LocalSendActionNotWritable},
		{name: "close-write open", localSend: true, sendHalf: SendHalfOpen, want: LocalSendActionApply},
		{name: "close-write stop-seen", localSend: true, sendHalf: SendHalfStopSeen, want: LocalSendActionApply},
		{name: "close-write fin", localSend: true, sendHalf: SendHalfFin, want: LocalSendActionClosed},
		{name: "close-write reset", localSend: true, sendHalf: SendHalfReset, want: LocalSendActionTerminal},
		{name: "close-write aborted", localSend: true, sendHalf: SendHalfAborted, want: LocalSendActionTerminal},
	}
	for _, tc := range sendTests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := LocalCloseWriteAction(tc.localSend, tc.sendHalf); got != tc.want {
				t.Fatalf("LocalCloseWriteAction(%v, %v) = %v, want %v", tc.localSend, tc.sendHalf, got, tc.want)
			}
			if got := LocalResetAction(tc.localSend, tc.sendHalf); got != tc.want {
				t.Fatalf("LocalResetAction(%v, %v) = %v, want %v", tc.localSend, tc.sendHalf, got, tc.want)
			}
		})
	}

	recvTests := []struct {
		name         string
		localReceive bool
		recvHalf     RecvHalfState
		want         LocalRecvAction
	}{
		{name: "close-read absent", localReceive: false, recvHalf: RecvHalfAbsent, want: LocalRecvActionNotReadable},
		{name: "close-read open", localReceive: true, recvHalf: RecvHalfOpen, want: LocalRecvActionApply},
		{name: "close-read fin", localReceive: true, recvHalf: RecvHalfFin, want: LocalRecvActionClosed},
		{name: "close-read stop-sent", localReceive: true, recvHalf: RecvHalfStopSent, want: LocalRecvActionClosed},
		{name: "close-read reset", localReceive: true, recvHalf: RecvHalfReset, want: LocalRecvActionTerminal},
		{name: "close-read aborted", localReceive: true, recvHalf: RecvHalfAborted, want: LocalRecvActionTerminal},
	}
	for _, tc := range recvTests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := LocalCloseReadAction(tc.localReceive, tc.recvHalf); got != tc.want {
				t.Fatalf("LocalCloseReadAction(%v, %v) = %v, want %v", tc.localReceive, tc.recvHalf, got, tc.want)
			}
		})
	}

	if got := LocalAbortActionForStream(SendHalfAborted, RecvHalfOpen); got != LocalAbortActionNoOp {
		t.Fatalf("LocalAbortActionForStream(send_aborted, recv_open) = %v, want no-op", got)
	}
	if got := LocalAbortActionForStream(SendHalfFin, RecvHalfReset); got != LocalAbortActionApply {
		t.Fatalf("LocalAbortActionForStream(send_fin, recv_reset) = %v, want apply", got)
	}
}

func TestSessionCloseTransition(t *testing.T) {
	t.Parallel()

	graceful := SessionCloseTransition(true, true, SendHalfStopSeen, RecvHalfStopSent, false)
	if !graceful.FinishSend || !graceful.FinishRecv || graceful.AbortSend || graceful.AbortRecv {
		t.Fatalf("graceful close plan = %#v, want finish send+recv", graceful)
	}

	abortive := SessionCloseTransition(true, true, SendHalfReset, RecvHalfStopSent, true)
	if abortive.AbortSend {
		t.Fatalf("abortive close should not override terminal send half: %#v", abortive)
	}
	if !abortive.AbortRecv {
		t.Fatalf("abortive close should abort non-terminal recv half: %#v", abortive)
	}
	if abortive.FinishSend || abortive.FinishRecv {
		t.Fatalf("abortive close should not plan graceful finish: %#v", abortive)
	}
}

func TestLateNonOpeningControlUsesExplicitHalfState(t *testing.T) {
	t.Parallel()

	if IgnoreLateNonOpeningControl(true, true, SendHalfFin, RecvHalfStopSent) {
		t.Fatal("send_fin + recv_stop_sent must not be treated as fully terminal")
	}
	if !IgnoreLateNonOpeningControl(true, true, SendHalfReset, RecvHalfFin) {
		t.Fatal("send_reset + recv_fin should be treated as fully terminal")
	}
	if !IgnoreLateNonOpeningControl(true, true, SendHalfOpen, RecvHalfAborted) {
		t.Fatal("recv_aborted should make late non-opening control ignorable")
	}
}

func TestVisibilityPredicatesUseHalfState(t *testing.T) {
	t.Parallel()

	phase := LocalOpenVisibility{LocalOpened: true}.Phase()
	if !ShouldReclaimUnseenLocalStream(phase, true, 12, 8, 99, true, true, SendHalfOpen, RecvHalfOpen) {
		t.Fatal("expected unseen bidi local stream above GOAWAY watermark to be reclaimed")
	}
	phase = LocalOpenVisibility{LocalOpened: true, SendCommitted: true, PeerVisible: true}.Phase()
	if ShouldReclaimUnseenLocalStream(phase, false, 101, 99, 100, true, true, SendHalfOpen, RecvHalfOpen) {
		t.Fatal("expected peer-visible local stream not to be reclaimed")
	}
	phase = LocalOpenVisibility{LocalOpened: true, SendCommitted: true}.Phase()
	if ShouldReclaimUnseenLocalStream(phase, false, 101, 99, 100, true, true, SendHalfReset, RecvHalfFin) {
		t.Fatal("expected fully terminal local stream not to be reclaimed")
	}
	if !ShouldFinalizePeerActive(true, false, true, true, SendHalfReset, RecvHalfFin) {
		t.Fatal("expected terminal peer-opened active stream to decrement counts")
	}
	if ShouldFinalizePeerActive(true, false, true, true, SendHalfFin, RecvHalfStopSent) {
		t.Fatal("expected send_fin + recv_stop_sent not to finalize peer-active counts yet")
	}
}

func TestTombstoneLateDataActionUsesRecvHalf(t *testing.T) {
	t.Parallel()

	if got := TombstoneLateDataAction(false, RecvHalfFin); got != LateDataIgnore {
		t.Fatalf("send-only tombstone action = %v, want ignore", got)
	}
	if got := TombstoneLateDataAction(true, RecvHalfFin); got != LateDataAbortClosed {
		t.Fatalf("graceful tombstone action = %v, want abort-closed", got)
	}
	if got := TombstoneLateDataAction(true, RecvHalfReset); got != LateDataIgnore {
		t.Fatalf("reset tombstone action = %v, want ignore", got)
	}
}

func TestTombstoneTerminalIdentity(t *testing.T) {
	t.Parallel()

	sendResetCode := uint64(7)
	sendAbortCode := uint64(9)
	recvResetCode := uint64(11)
	recvAbortCode := uint64(13)

	graceful := BuildStreamTombstone(true, SendHalfFin, RecvHalfFin, nil, nil, nil, nil)
	if graceful.TerminalKind != TerminalKindGraceful {
		t.Fatalf("graceful terminal kind = %v, want graceful", graceful.TerminalKind)
	}
	if graceful.HasTerminalCode {
		t.Fatalf("graceful tombstone unexpectedly kept terminal code %d", graceful.TerminalCode)
	}

	reset := BuildStreamTombstone(true, SendHalfFin, RecvHalfReset, nil, nil, &recvResetCode, nil)
	if reset.TerminalKind != TerminalKindReset {
		t.Fatalf("reset terminal kind = %v, want reset", reset.TerminalKind)
	}
	if !reset.HasTerminalCode || reset.TerminalCode != recvResetCode {
		t.Fatalf("reset terminal code = (%v,%d), want (%v,%d)", reset.HasTerminalCode, reset.TerminalCode, true, recvResetCode)
	}

	aborted := BuildStreamTombstone(true, SendHalfReset, RecvHalfAborted, &sendResetCode, nil, nil, &recvAbortCode)
	if aborted.TerminalKind != TerminalKindAborted {
		t.Fatalf("aborted terminal kind = %v, want aborted", aborted.TerminalKind)
	}
	if !aborted.HasTerminalCode || aborted.TerminalCode != recvAbortCode {
		t.Fatalf("aborted terminal code = (%v,%d), want (%v,%d)", aborted.HasTerminalCode, aborted.TerminalCode, true, recvAbortCode)
	}

	sendAbortWins := BuildStreamTombstone(true, SendHalfAborted, RecvHalfReset, &sendResetCode, &sendAbortCode, &recvResetCode, nil)
	if sendAbortWins.TerminalKind != TerminalKindAborted {
		t.Fatalf("send-abort precedence kind = %v, want aborted", sendAbortWins.TerminalKind)
	}
	if !sendAbortWins.HasTerminalCode || sendAbortWins.TerminalCode != sendAbortCode {
		t.Fatalf("send-abort precedence code = (%v,%d), want (%v,%d)", sendAbortWins.HasTerminalCode, sendAbortWins.TerminalCode, true, sendAbortCode)
	}
}
