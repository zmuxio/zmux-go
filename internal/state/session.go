package state

import (
	"io"

	"github.com/zmuxio/zmux-go/internal/wire"
)

type SessionState uint8

const (
	SessionStateReady SessionState = iota
	SessionStateDraining
	SessionStateClosing
	SessionStateClosed
	SessionStateFailed
)

type BeginCloseOutcome uint8

const (
	BeginCloseReturnExisting BeginCloseOutcome = iota
	BeginCloseWaitExisting
	BeginCloseGraceful
	BeginCloseAbortive
)

type BeginClosePlan struct {
	Outcome   BeginCloseOutcome
	NextState SessionState
}

type LocalOpenOutcome uint8

const (
	LocalOpenAllow LocalOpenOutcome = iota
	LocalOpenReturnExisting
	LocalOpenReturnClosed
)

type PeerGoAwayPlan struct {
	Ignore    bool
	Changed   bool
	NextState SessionState
}

type PeerClosePlan struct {
	Ignore bool
}

type applicationCoder interface {
	ApplicationCode() uint64
}

const maxErrorUnwrapDepth = 64

func findError[T any](err error) (T, bool) {
	return findErrorDepth[T](err, 0)
}

func findErrorDepth[T any](err error, depth int) (T, bool) {
	var zero T
	if err == nil || depth > maxErrorUnwrapDepth {
		return zero, false
	}
	if target, ok := any(err).(T); ok {
		return target, true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range wrapped.Unwrap() {
			if target, ok := findErrorDepth[T](child, depth+1); ok {
				return target, true
			}
		}
		return zero, false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return findErrorDepth[T](wrapped.Unwrap(), depth+1)
	}
	return zero, false
}

func isError(err, target error) bool {
	if target == nil {
		return err == nil
	}
	return isErrorDepth(err, target, 0)
}

func isErrorDepth(err, target error, depth int) bool {
	if err == nil || depth > maxErrorUnwrapDepth {
		return false
	}
	if sameError(err, target) {
		return true
	}
	if matcher, ok := err.(interface{ Is(error) bool }); ok && matcher.Is(target) {
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range wrapped.Unwrap() {
			if isErrorDepth(child, target, depth+1) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isErrorDepth(wrapped.Unwrap(), target, depth+1)
	}
	return false
}

func sameError(err, target error) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	return err == target
}

func CanOpenLocally(state SessionState) bool {
	return state == SessionStateReady || state == SessionStateDraining
}

func IsSessionFinished(state SessionState) bool {
	return state == SessionStateClosed || state == SessionStateFailed
}

func IgnorePeerNonCloseFrame(current SessionState, closeErrPresent bool) bool {
	if closeErrPresent {
		return true
	}
	return current == SessionStateClosing || IsSessionFinished(current)
}

func AllowLocalNonCloseControl(current SessionState, closeErrPresent, peerCloseErrPresent, closeFrameSent bool) bool {
	if closeErrPresent || peerCloseErrPresent || closeFrameSent {
		return false
	}
	switch current {
	case SessionStateClosing, SessionStateClosed, SessionStateFailed:
		return false
	default:
		return true
	}
}

func PlanLocalOpen(current SessionState, gracefulCloseActive, closeErrPresent bool) LocalOpenOutcome {
	switch {
	case closeErrPresent:
		return LocalOpenReturnExisting
	case gracefulCloseActive:
		return LocalOpenReturnClosed
	case CanOpenLocally(current):
		return LocalOpenAllow
	default:
		return LocalOpenReturnClosed
	}
}

func PlanBeginClose(current SessionState, gracefulCloseActive, closeErrPresent, hasOpenStreams bool) BeginClosePlan {
	switch {
	case IsSessionFinished(current):
		return BeginClosePlan{Outcome: BeginCloseReturnExisting, NextState: current}
	case current == SessionStateClosing || gracefulCloseActive:
		return BeginClosePlan{Outcome: BeginCloseWaitExisting, NextState: current}
	case closeErrPresent:
		return BeginClosePlan{Outcome: BeginCloseReturnExisting, NextState: current}
	case CanOpenLocally(current) && hasOpenStreams:
		return BeginClosePlan{Outcome: BeginCloseGraceful, NextState: SessionStateDraining}
	default:
		return BeginClosePlan{Outcome: BeginCloseAbortive, NextState: SessionStateClosing}
	}
}

func AdvanceSessionOnGoAway(current SessionState, changed bool) SessionState {
	if changed && current == SessionStateReady {
		return SessionStateDraining
	}
	return current
}

func PlanPeerGoAway(current SessionState, closeErrPresent bool, currentBidi, currentUni, nextBidi, nextUni uint64) PeerGoAwayPlan {
	if IgnorePeerNonCloseFrame(current, closeErrPresent) {
		return PeerGoAwayPlan{
			Ignore:    true,
			NextState: current,
		}
	}
	changed := nextBidi < currentBidi || nextUni < currentUni
	return PeerGoAwayPlan{
		Changed:   changed,
		NextState: AdvanceSessionOnGoAway(current, changed),
	}
}

func BeginSessionClosing(current SessionState) SessionState {
	if IsSessionFinished(current) {
		return current
	}
	return SessionStateClosing
}

func isClosedSentinel(err error, closedSentinel error) bool {
	return closedSentinel != nil && isError(err, closedSentinel)
}

func isOrderlyTransportClose(current SessionState) bool {
	return current == SessionStateClosing || current == SessionStateClosed
}

func isTransportClose(err error) bool {
	return isError(err, io.EOF) || isError(err, io.ErrClosedPipe)
}

func CloseSessionState(current SessionState, err error, closedSentinel error) SessionState {
	if err == nil {
		return SessionStateClosed
	}

	if isClosedSentinel(err, closedSentinel) {
		return SessionStateClosed
	}

	if coded, ok := findError[applicationCoder](err); ok {
		if coded.ApplicationCode() == uint64(wire.CodeNoError) {
			return SessionStateClosed
		}
		return SessionStateFailed
	}

	if isTransportClose(err) {
		if isOrderlyTransportClose(current) {
			return SessionStateClosed
		}
		return SessionStateFailed
	}

	return SessionStateFailed
}

func VisibleSessionError(current SessionState, err error, closedSentinel error) error {
	if err == nil {
		return closedSentinel
	}
	if isClosedSentinel(err, closedSentinel) {
		return closedSentinel
	}

	if coded, ok := findError[applicationCoder](err); ok && coded.ApplicationCode() == uint64(wire.CodeNoError) {
		return closedSentinel
	}

	if isTransportClose(err) && isOrderlyTransportClose(current) {
		return closedSentinel
	}

	return err
}

func IsBenignSessionError(current SessionState, err error, closedSentinel error) bool {
	err = VisibleSessionError(current, err, closedSentinel)
	if err == nil {
		return true
	}
	return isClosedSentinel(err, closedSentinel)
}

func IgnorePeerClose(closeErr error, peerCloseErrPresent bool, closedSentinel error) bool {
	if peerCloseErrPresent {
		return true
	}
	if closeErr == nil {
		return false
	}
	if isClosedSentinel(closeErr, closedSentinel) {
		return true
	}
	if _, ok := findError[applicationCoder](closeErr); ok {
		return true
	}
	if _, ok := wire.ErrorCodeOf(closeErr); ok {
		return true
	}
	return !isTransportClose(closeErr)
}

func PlanPeerClose(closeErr error, peerCloseErrPresent bool, closedSentinel error) PeerClosePlan {
	return PeerClosePlan{
		Ignore: IgnorePeerClose(closeErr, peerCloseErrPresent, closedSentinel),
	}
}
