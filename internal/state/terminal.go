package state

type TerminalErrorChoice uint8

const (
	TerminalErrorNone TerminalErrorChoice = iota
	TerminalErrorSendAbort
	TerminalErrorRecvAbort
	TerminalErrorSendReset
	TerminalErrorRecvReset
	TerminalErrorSendClosed
	TerminalErrorRecvClosed
)

type LocalSendAction uint8

const (
	LocalSendActionApply LocalSendAction = iota
	LocalSendActionNotWritable
	LocalSendActionClosed
	LocalSendActionTerminal
)

type LocalRecvAction uint8

const (
	LocalRecvActionApply LocalRecvAction = iota
	LocalRecvActionNotReadable
	LocalRecvActionClosed
	LocalRecvActionTerminal
)

type LocalAbortAction uint8

const (
	LocalAbortActionApply LocalAbortAction = iota
	LocalAbortActionNoOp
)

type StopSendingOutcome uint8

const (
	StopSendingIgnore StopSendingOutcome = iota
	StopSendingFinish
	StopSendingReset
)

type PeerDataOutcome uint8

const (
	PeerDataAccept PeerDataOutcome = iota
	PeerDataIgnore
	PeerDataAbortClosed
	PeerDataAbortState
)

type PeerDataPlan struct {
	Outcome            PeerDataOutcome
	AdvanceRecvFin     bool
	TrackLatePerStream bool
}

type SessionClosePlan struct {
	FinishSend bool
	FinishRecv bool
	AbortSend  bool
	AbortRecv  bool
}

type PeerStopSendingPlan struct {
	Ignore     bool
	RecordStop bool
	Outcome    StopSendingOutcome
}

type PeerResetPlan struct {
	Ignore         bool
	RecordReset    bool
	ReleaseReceive bool
	ClearReadBuf   bool
}

type PeerAbortPlan struct {
	Ignore         bool
	RecordAbort    bool
	ReleaseSend    bool
	ReleaseReceive bool
	ClearReadBuf   bool
}

func ReadErrorChoice(localReceive, localReadStop bool, recvHalf RecvHalfState) TerminalErrorChoice {
	if !localReceive {
		return TerminalErrorNone
	}
	if localReadStop {
		return TerminalErrorRecvClosed
	}
	switch recvHalf {
	case RecvHalfAborted:
		return TerminalErrorRecvAbort
	case RecvHalfStopSent:
		return TerminalErrorRecvClosed
	case RecvHalfReset:
		return TerminalErrorRecvReset
	case RecvHalfFin:
		return TerminalErrorRecvClosed
	default:
		return TerminalErrorNone
	}
}

func TerminalErrorPriority(sendHalf SendHalfState, recvHalf RecvHalfState) TerminalErrorChoice {
	switch sendHalf {
	case SendHalfAborted:
		return TerminalErrorSendAbort
	default:
	}
	switch recvHalf {
	case RecvHalfAborted:
		return TerminalErrorRecvAbort
	default:
	}
	switch sendHalf {
	case SendHalfReset:
		return TerminalErrorSendReset
	default:
	}
	switch recvHalf {
	case RecvHalfReset:
		return TerminalErrorRecvReset
	default:
	}
	switch sendHalf {
	case SendHalfFin, SendHalfStopSeen:
		return TerminalErrorSendClosed
	default:
	}
	switch recvHalf {
	case RecvHalfStopSent, RecvHalfFin:
		return TerminalErrorRecvClosed
	default:
		return TerminalErrorNone
	}
}

func LocalCloseWriteAction(localSend bool, sendHalf SendHalfState) LocalSendAction {
	if !localSend {
		return LocalSendActionNotWritable
	}
	switch sendHalf {
	case SendHalfFin:
		return LocalSendActionClosed
	case SendHalfReset, SendHalfAborted:
		return LocalSendActionTerminal
	default:
		return LocalSendActionApply
	}
}

func LocalResetAction(localSend bool, sendHalf SendHalfState) LocalSendAction {
	return LocalCloseWriteAction(localSend, sendHalf)
}

func LocalCloseReadAction(localReceive bool, recvHalf RecvHalfState) LocalRecvAction {
	if !localReceive {
		return LocalRecvActionNotReadable
	}
	switch recvHalf {
	case RecvHalfStopSent, RecvHalfFin:
		return LocalRecvActionClosed
	case RecvHalfReset, RecvHalfAborted:
		return LocalRecvActionTerminal
	default:
		return LocalRecvActionApply
	}
}

func LocalAbortActionForStream(sendHalf SendHalfState, recvHalf RecvHalfState) LocalAbortAction {
	if sendHalf == SendHalfAborted || recvHalf == RecvHalfAborted {
		return LocalAbortActionNoOp
	}
	return LocalAbortActionApply
}

func SessionCloseTransition(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState, abortive bool) SessionClosePlan {
	plan := SessionClosePlan{}
	if abortive {
		plan.AbortSend = localSend && !SendTerminal(sendHalf)
		plan.AbortRecv = localReceive && !RecvTerminal(recvHalf)
		return plan
	}
	plan.FinishSend = localSend && !SendTerminal(sendHalf)
	plan.FinishRecv = localReceive && !RecvTerminal(recvHalf)
	return plan
}

func IgnoreLateNonOpeningControl(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) bool {
	return FullyTerminal(localSend, localReceive, sendHalf, recvHalf)
}

func IgnorePeerStopSending(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) bool {
	if IgnoreLateNonOpeningControl(localSend, localReceive, sendHalf, recvHalf) {
		return true
	}
	switch sendHalf {
	case SendHalfStopSeen, SendHalfFin, SendHalfReset, SendHalfAborted:
		return true
	default:
		return false
	}
}

func IgnorePeerReset(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) bool {
	if IgnoreLateNonOpeningControl(localSend, localReceive, sendHalf, recvHalf) {
		return true
	}
	switch recvHalf {
	case RecvHalfFin, RecvHalfReset, RecvHalfAborted:
		return true
	default:
		return false
	}
}

func IgnorePeerAbort(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) bool {
	if IgnoreLateNonOpeningControl(localSend, localReceive, sendHalf, recvHalf) {
		return true
	}
	return sendHalf == SendHalfAborted || recvHalf == RecvHalfAborted
}

func PeerDataTransition(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState, fin bool) PeerDataPlan {
	if !localReceive {
		if FullyTerminal(localSend, localReceive, sendHalf, recvHalf) {
			return PeerDataPlan{Outcome: PeerDataIgnore}
		}
		return PeerDataPlan{Outcome: PeerDataAbortState}
	}
	switch recvHalf {
	case RecvHalfReset, RecvHalfAborted:
		return PeerDataPlan{
			Outcome:            PeerDataIgnore,
			TrackLatePerStream: true,
		}
	case RecvHalfStopSent:
		return PeerDataPlan{
			Outcome:            PeerDataIgnore,
			AdvanceRecvFin:     fin,
			TrackLatePerStream: true,
		}
	case RecvHalfFin:
		if FullyTerminal(localSend, localReceive, sendHalf, recvHalf) {
			return PeerDataPlan{Outcome: PeerDataIgnore}
		}
		return PeerDataPlan{Outcome: PeerDataAbortClosed}
	default:
		if FullyTerminal(localSend, localReceive, sendHalf, recvHalf) {
			return PeerDataPlan{Outcome: PeerDataIgnore}
		}
		return PeerDataPlan{Outcome: PeerDataAccept}
	}
}

func PeerStopSendingOutcome(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) StopSendingOutcome {
	if IgnorePeerStopSending(localSend, localReceive, sendHalf, recvHalf) {
		return StopSendingIgnore
	}
	if recvHalf == RecvHalfReset || recvHalf == RecvHalfAborted {
		return StopSendingReset
	}
	return StopSendingFinish
}

func PlanPeerStopSending(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) PeerStopSendingPlan {
	outcome := PeerStopSendingOutcome(localSend, localReceive, sendHalf, recvHalf)
	return PeerStopSendingPlan{
		Ignore:     outcome == StopSendingIgnore,
		RecordStop: outcome != StopSendingIgnore,
		Outcome:    outcome,
	}
}

func PlanPeerReset(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) PeerResetPlan {
	if IgnorePeerReset(localSend, localReceive, sendHalf, recvHalf) {
		return PeerResetPlan{Ignore: true}
	}
	return PeerResetPlan{
		RecordReset:    true,
		ReleaseReceive: localReceive,
		ClearReadBuf:   localReceive,
	}
}

func PlanPeerAbort(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) PeerAbortPlan {
	if IgnorePeerAbort(localSend, localReceive, sendHalf, recvHalf) {
		return PeerAbortPlan{Ignore: true}
	}
	return PeerAbortPlan{
		RecordAbort:    true,
		ReleaseSend:    localSend,
		ReleaseReceive: localReceive,
		ClearReadBuf:   localReceive,
	}
}
