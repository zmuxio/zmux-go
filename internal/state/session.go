package state

import (
	"errors"
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
	return closedSentinel != nil && errors.Is(err, closedSentinel)
}

func isOrderlyTransportClose(current SessionState) bool {
	return current == SessionStateClosing || current == SessionStateClosed
}

func isTransportClose(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe)
}

func CloseSessionState(current SessionState, err error, closedSentinel error) SessionState {
	if err == nil {
		return SessionStateClosed
	}

	if isClosedSentinel(err, closedSentinel) {
		return SessionStateClosed
	}

	var coded applicationCoder
	if errors.As(err, &coded) {
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

	var coded applicationCoder
	if errors.As(err, &coded) && coded.ApplicationCode() == uint64(wire.CodeNoError) {
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
	var coded applicationCoder
	if errors.As(closeErr, &coded) {
		return true
	}
	var wireErr *wire.Error
	if errors.As(closeErr, &wireErr) {
		return true
	}
	return !isTransportClose(closeErr)
}

func PlanPeerClose(closeErr error, peerCloseErrPresent bool, closedSentinel error) PeerClosePlan {
	return PeerClosePlan{
		Ignore: IgnorePeerClose(closeErr, peerCloseErrPresent, closedSentinel),
	}
}
