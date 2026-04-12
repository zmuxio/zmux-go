package state

type LateDataAction uint8

const (
	LateDataIgnore LateDataAction = iota + 1
	LateDataAbortClosed
	LateDataAbortState
)

type TerminalKind uint8

const (
	TerminalKindUnknown TerminalKind = iota
	TerminalKindGraceful
	TerminalKindReset
	TerminalKindAborted
)

type StreamTombstone struct {
	DataAction      LateDataAction
	TerminalKind    TerminalKind
	HasTerminalCode bool
	TerminalCode    uint64
}

func TombstoneLateDataAction(localReceive bool, recvHalf RecvHalfState) LateDataAction {
	switch {
	case !localReceive:
		return LateDataIgnore
	case recvHalf == RecvHalfFin:
		return LateDataAbortClosed
	default:
		return LateDataIgnore
	}
}

func TombstoneTerminalKind(sendHalf SendHalfState, recvHalf RecvHalfState) TerminalKind {
	switch TerminalErrorPriority(sendHalf, recvHalf) {
	case TerminalErrorSendAbort, TerminalErrorRecvAbort:
		return TerminalKindAborted
	case TerminalErrorSendReset, TerminalErrorRecvReset:
		return TerminalKindReset
	case TerminalErrorSendClosed, TerminalErrorRecvClosed:
		return TerminalKindGraceful
	default:
		return TerminalKindUnknown
	}
}

func TombstoneTerminalCode(sendHalf SendHalfState, recvHalf RecvHalfState, sendResetCode, sendAbortCode, recvResetCode, recvAbortCode *uint64) (uint64, bool) {
	switch TerminalErrorPriority(sendHalf, recvHalf) {
	case TerminalErrorSendAbort:
		if sendAbortCode != nil {
			return *sendAbortCode, true
		}
	case TerminalErrorRecvAbort:
		if recvAbortCode != nil {
			return *recvAbortCode, true
		}
	case TerminalErrorSendReset:
		if sendResetCode != nil {
			return *sendResetCode, true
		}
	case TerminalErrorRecvReset:
		if recvResetCode != nil {
			return *recvResetCode, true
		}
	default:
	}
	return 0, false
}

func BuildStreamTombstone(localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState, sendResetCode, sendAbortCode, recvResetCode, recvAbortCode *uint64) StreamTombstone {
	code, hasCode := TombstoneTerminalCode(sendHalf, recvHalf, sendResetCode, sendAbortCode, recvResetCode, recvAbortCode)
	return StreamTombstone{
		DataAction:      TombstoneLateDataAction(localReceive, recvHalf),
		TerminalKind:    TombstoneTerminalKind(sendHalf, recvHalf),
		HasTerminalCode: hasCode,
		TerminalCode:    code,
	}
}

func ShouldCompactTerminal(idSet, fullyTerminal bool, recvBuffer uint64, readBufLen int, stillTracked bool) bool {
	if !idSet || !fullyTerminal {
		return false
	}
	if recvBuffer != 0 || readBufLen != 0 {
		return false
	}
	return stillTracked
}
