package state

import (
	"errors"
	"io"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

type codedError struct {
	code uint64
}

func (e codedError) Error() string           { return "coded" }
func (e codedError) ApplicationCode() uint64 { return e.code }

func TestCloseSessionState(t *testing.T) {
	t.Parallel()

	closedSentinel := errors.New("closed")
	if got := CloseSessionState(SessionStateReady, nil, closedSentinel); got != SessionStateClosed {
		t.Fatalf("CloseSessionState(nil) = %v, want %v", got, SessionStateClosed)
	}
	if got := CloseSessionState(SessionStateReady, closedSentinel, closedSentinel); got != SessionStateClosed {
		t.Fatalf("CloseSessionState(closed sentinel) = %v, want %v", got, SessionStateClosed)
	}
	if got := CloseSessionState(SessionStateReady, io.EOF, closedSentinel); got != SessionStateFailed {
		t.Fatalf("CloseSessionState(ready, io.EOF) = %v, want %v", got, SessionStateFailed)
	}
	if got := CloseSessionState(SessionStateClosing, io.EOF, closedSentinel); got != SessionStateClosed {
		t.Fatalf("CloseSessionState(closing, io.EOF) = %v, want %v", got, SessionStateClosed)
	}
	if got := CloseSessionState(SessionStateDraining, io.ErrClosedPipe, closedSentinel); got != SessionStateFailed {
		t.Fatalf("CloseSessionState(draining, io.ErrClosedPipe) = %v, want %v", got, SessionStateFailed)
	}
	if got := CloseSessionState(SessionStateReady, codedError{code: uint64(wire.CodeNoError)}, closedSentinel); got != SessionStateClosed {
		t.Fatalf("CloseSessionState(no_error app err) = %v, want %v", got, SessionStateClosed)
	}
	if got := CloseSessionState(SessionStateReady, codedError{code: uint64(wire.CodeInternal)}, closedSentinel); got != SessionStateFailed {
		t.Fatalf("CloseSessionState(internal app err) = %v, want %v", got, SessionStateFailed)
	}
}

func TestVisibleSessionError(t *testing.T) {
	t.Parallel()

	closedSentinel := errors.New("closed")
	if got := VisibleSessionError(SessionStateReady, nil, closedSentinel); !errors.Is(got, closedSentinel) {
		t.Fatalf("VisibleSessionError(nil) = %v, want closed sentinel", got)
	}
	if got := VisibleSessionError(SessionStateReady, io.EOF, closedSentinel); !errors.Is(got, io.EOF) {
		t.Fatalf("VisibleSessionError(ready, io.EOF) = %v, want io.EOF", got)
	}
	if got := VisibleSessionError(SessionStateClosing, io.EOF, closedSentinel); !errors.Is(got, closedSentinel) {
		t.Fatalf("VisibleSessionError(closing, io.EOF) = %v, want closed sentinel", got)
	}
	if got := VisibleSessionError(SessionStateReady, codedError{code: uint64(wire.CodeNoError)}, closedSentinel); !errors.Is(got, closedSentinel) {
		t.Fatalf("VisibleSessionError(NO_ERROR app err) = %v, want closed sentinel", got)
	}
}

func TestIsBenignSessionError(t *testing.T) {
	t.Parallel()

	closedSentinel := errors.New("closed")
	if !IsBenignSessionError(SessionStateReady, nil, closedSentinel) {
		t.Fatal("IsBenignSessionError(nil) = false, want true")
	}
	if !IsBenignSessionError(SessionStateClosing, io.EOF, closedSentinel) {
		t.Fatal("IsBenignSessionError(closing, io.EOF) = false, want true")
	}
	if IsBenignSessionError(SessionStateReady, io.EOF, closedSentinel) {
		t.Fatal("IsBenignSessionError(ready, io.EOF) = true, want false")
	}
}

func TestSessionStatePredicates(t *testing.T) {
	t.Parallel()

	if !CanOpenLocally(SessionStateReady) {
		t.Fatal("CanOpenLocally(ready) = false, want true")
	}
	if !CanOpenLocally(SessionStateDraining) {
		t.Fatal("CanOpenLocally(draining) = false, want true")
	}
	if CanOpenLocally(SessionStateClosing) {
		t.Fatal("CanOpenLocally(closing) = true, want false")
	}
	if !IsSessionFinished(SessionStateClosed) {
		t.Fatal("IsSessionFinished(closed) = false, want true")
	}
	if !IsSessionFinished(SessionStateFailed) {
		t.Fatal("IsSessionFinished(failed) = false, want true")
	}
	if IsSessionFinished(SessionStateReady) {
		t.Fatal("IsSessionFinished(ready) = true, want false")
	}
}

func TestPlanLocalOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		current     SessionState
		graceful    bool
		closeErr    bool
		wantOutcome LocalOpenOutcome
	}{
		{
			name:        "ready allows",
			current:     SessionStateReady,
			wantOutcome: LocalOpenAllow,
		},
		{
			name:        "manual draining allows",
			current:     SessionStateDraining,
			wantOutcome: LocalOpenAllow,
		},
		{
			name:        "graceful drain blocks",
			current:     SessionStateDraining,
			graceful:    true,
			wantOutcome: LocalOpenReturnClosed,
		},
		{
			name:        "close error returns existing",
			current:     SessionStateReady,
			closeErr:    true,
			wantOutcome: LocalOpenReturnExisting,
		},
		{
			name:        "closing blocks",
			current:     SessionStateClosing,
			wantOutcome: LocalOpenReturnClosed,
		},
		{
			name:        "closed blocks",
			current:     SessionStateClosed,
			wantOutcome: LocalOpenReturnClosed,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := PlanLocalOpen(tc.current, tc.graceful, tc.closeErr); got != tc.wantOutcome {
				t.Fatalf("PlanLocalOpen(%v, graceful=%v, closeErr=%v) = %v, want %v", tc.current, tc.graceful, tc.closeErr, got, tc.wantOutcome)
			}
		})
	}
}

func TestPlanBeginClose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		current     SessionState
		graceful    bool
		closeErr    bool
		hasStreams  bool
		wantOutcome BeginCloseOutcome
		wantState   SessionState
	}{
		{
			name:        "finished returns existing",
			current:     SessionStateClosed,
			wantOutcome: BeginCloseReturnExisting,
			wantState:   SessionStateClosed,
		},
		{
			name:        "closing waits existing",
			current:     SessionStateClosing,
			wantOutcome: BeginCloseWaitExisting,
			wantState:   SessionStateClosing,
		},
		{
			name:        "graceful active waits existing",
			current:     SessionStateDraining,
			graceful:    true,
			wantOutcome: BeginCloseWaitExisting,
			wantState:   SessionStateDraining,
		},
		{
			name:        "close error returns existing",
			current:     SessionStateReady,
			closeErr:    true,
			wantOutcome: BeginCloseReturnExisting,
			wantState:   SessionStateReady,
		},
		{
			name:        "ready with streams starts graceful",
			current:     SessionStateReady,
			hasStreams:  true,
			wantOutcome: BeginCloseGraceful,
			wantState:   SessionStateDraining,
		},
		{
			name:        "draining with streams stays graceful",
			current:     SessionStateDraining,
			hasStreams:  true,
			wantOutcome: BeginCloseGraceful,
			wantState:   SessionStateDraining,
		},
		{
			name:        "ready without streams abortive",
			current:     SessionStateReady,
			wantOutcome: BeginCloseAbortive,
			wantState:   SessionStateClosing,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := PlanBeginClose(tc.current, tc.graceful, tc.closeErr, tc.hasStreams)
			if got.Outcome != tc.wantOutcome {
				t.Fatalf("Outcome = %v, want %v", got.Outcome, tc.wantOutcome)
			}
			if got.NextState != tc.wantState {
				t.Fatalf("NextState = %v, want %v", got.NextState, tc.wantState)
			}
		})
	}
}

func TestSessionLifecycleTransitionHelpers(t *testing.T) {
	t.Parallel()

	closedSentinel := errors.New("closed")

	if got := AdvanceSessionOnGoAway(SessionStateReady, true); got != SessionStateDraining {
		t.Fatalf("AdvanceSessionOnGoAway(ready, changed) = %v, want %v", got, SessionStateDraining)
	}
	if got := AdvanceSessionOnGoAway(SessionStateDraining, true); got != SessionStateDraining {
		t.Fatalf("AdvanceSessionOnGoAway(draining, changed) = %v, want %v", got, SessionStateDraining)
	}
	if got := AdvanceSessionOnGoAway(SessionStateReady, false); got != SessionStateReady {
		t.Fatalf("AdvanceSessionOnGoAway(ready, unchanged) = %v, want %v", got, SessionStateReady)
	}

	goAwayIgnore := PlanPeerGoAway(SessionStateFailed, true, 20, 24, 16, 20)
	if !goAwayIgnore.Ignore || goAwayIgnore.Changed || goAwayIgnore.NextState != SessionStateFailed {
		t.Fatalf("PlanPeerGoAway(ignore) = %#v, want ignored failed-state plan", goAwayIgnore)
	}
	goAwayClosing := PlanPeerGoAway(SessionStateClosing, false, 20, 24, 16, 20)
	if !goAwayClosing.Ignore || goAwayClosing.Changed || goAwayClosing.NextState != SessionStateClosing {
		t.Fatalf("PlanPeerGoAway(closing) = %#v, want ignored closing-state plan", goAwayClosing)
	}
	goAwayChange := PlanPeerGoAway(SessionStateReady, false, 20, 24, 16, 24)
	if goAwayChange.Ignore || !goAwayChange.Changed || goAwayChange.NextState != SessionStateDraining {
		t.Fatalf("PlanPeerGoAway(change) = %#v, want changed -> draining", goAwayChange)
	}
	goAwayNoOp := PlanPeerGoAway(SessionStateDraining, false, 20, 24, 20, 24)
	if goAwayNoOp.Ignore || goAwayNoOp.Changed || goAwayNoOp.NextState != SessionStateDraining {
		t.Fatalf("PlanPeerGoAway(no-op) = %#v, want unchanged draining plan", goAwayNoOp)
	}

	if got := BeginSessionClosing(SessionStateReady); got != SessionStateClosing {
		t.Fatalf("BeginSessionClosing(ready) = %v, want %v", got, SessionStateClosing)
	}
	if got := BeginSessionClosing(SessionStateClosed); got != SessionStateClosed {
		t.Fatalf("BeginSessionClosing(closed) = %v, want %v", got, SessionStateClosed)
	}

	if !IgnorePeerClose(codedError{code: uint64(wire.CodeInternal)}, false, closedSentinel) {
		t.Fatal("IgnorePeerClose(app error) = false, want true")
	}
	if plan := PlanPeerClose(codedError{code: uint64(wire.CodeInternal)}, false, closedSentinel); !plan.Ignore {
		t.Fatalf("PlanPeerClose(app error) = %#v, want ignore", plan)
	}
	if IgnorePeerClose(io.ErrClosedPipe, false, closedSentinel) {
		t.Fatal("IgnorePeerClose(transport failure) = true, want false")
	}
	if plan := PlanPeerClose(io.ErrClosedPipe, false, closedSentinel); plan.Ignore {
		t.Fatalf("PlanPeerClose(transport failure) = %#v, want actionable", plan)
	}
	if !IgnorePeerClose(nil, true, closedSentinel) {
		t.Fatal("IgnorePeerClose(peerCloseErrPresent) = false, want true")
	}
	if IgnorePeerClose(nil, false, closedSentinel) {
		t.Fatal("IgnorePeerClose(nil,false) = true, want false")
	}
}

func TestIgnorePeerNonCloseFrame(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		current  SessionState
		closeErr bool
		want     bool
	}{
		{name: "ready allowed", current: SessionStateReady, want: false},
		{name: "draining allowed", current: SessionStateDraining, want: false},
		{name: "closing ignored", current: SessionStateClosing, want: true},
		{name: "closed ignored", current: SessionStateClosed, want: true},
		{name: "failed ignored", current: SessionStateFailed, want: true},
		{name: "close error present ignored", current: SessionStateDraining, closeErr: true, want: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IgnorePeerNonCloseFrame(tc.current, tc.closeErr); got != tc.want {
				t.Fatalf("IgnorePeerNonCloseFrame(%v, closeErr=%v) = %v, want %v", tc.current, tc.closeErr, got, tc.want)
			}
		})
	}
}

func TestAllowLocalNonCloseControl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		current        SessionState
		closeErr       bool
		peerCloseErr   bool
		closeFrameSent bool
		want           bool
	}{
		{name: "ready allowed", current: SessionStateReady, want: true},
		{name: "draining allowed", current: SessionStateDraining, want: true},
		{name: "closing denied", current: SessionStateClosing, want: false},
		{name: "closed denied", current: SessionStateClosed, want: false},
		{name: "failed denied", current: SessionStateFailed, want: false},
		{name: "close error denied", current: SessionStateReady, closeErr: true, want: false},
		{name: "peer close denied", current: SessionStateReady, peerCloseErr: true, want: false},
		{name: "close frame sent denied", current: SessionStateReady, closeFrameSent: true, want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := AllowLocalNonCloseControl(tc.current, tc.closeErr, tc.peerCloseErr, tc.closeFrameSent); got != tc.want {
				t.Fatalf("AllowLocalNonCloseControl(%v, closeErr=%v, peerCloseErr=%v, closeFrameSent=%v) = %v, want %v",
					tc.current, tc.closeErr, tc.peerCloseErr, tc.closeFrameSent, got, tc.want)
			}
		})
	}
}
