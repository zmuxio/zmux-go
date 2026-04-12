package state

type SendHalfState uint32

const (
	SendHalfUnknown SendHalfState = iota
	SendHalfAbsent
	SendHalfOpen
	SendHalfStopSeen
	SendHalfFin
	SendHalfReset
	SendHalfAborted
)

type RecvHalfState uint32

const (
	RecvHalfUnknown RecvHalfState = iota
	RecvHalfAbsent
	RecvHalfOpen
	RecvHalfFin
	RecvHalfStopSent
	RecvHalfReset
	RecvHalfAborted
)

func BaseSendHalfState(localSend bool) SendHalfState {
	if localSend {
		return SendHalfOpen
	}
	return SendHalfAbsent
}

func NormalizeSendHalfState(localSend bool, sendHalf SendHalfState) SendHalfState {
	if sendHalf == SendHalfUnknown {
		return BaseSendHalfState(localSend)
	}
	return sendHalf
}

func BaseRecvHalfState(localReceive bool) RecvHalfState {
	if localReceive {
		return RecvHalfOpen
	}
	return RecvHalfAbsent
}

func NormalizeRecvHalfState(localReceive bool, recvHalf RecvHalfState) RecvHalfState {
	if recvHalf == RecvHalfUnknown {
		return BaseRecvHalfState(localReceive)
	}
	return recvHalf
}

func SendTerminal(sendHalf SendHalfState) bool {
	switch sendHalf {
	case SendHalfFin, SendHalfReset, SendHalfAborted:
		return true
	default:
		return false
	}
}

func RecvTerminal(recvHalf RecvHalfState) bool {
	switch recvHalf {
	case RecvHalfFin, RecvHalfReset, RecvHalfAborted:
		return true
	default:
		return false
	}
}

func ReadStopped(recvHalf RecvHalfState) bool {
	return recvHalf == RecvHalfStopSent
}

func FullyTerminal(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) bool {
	sendTerm := !localSend || SendTerminal(sendHalf)
	recvTerm := !localReceive || RecvTerminal(recvHalf)
	if sendHalf == SendHalfAborted || recvHalf == RecvHalfAborted {
		return true
	}
	return sendTerm && recvTerm
}
