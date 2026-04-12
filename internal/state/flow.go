package state

type PeerStreamControlAction uint8

const (
	PeerStreamControlApply PeerStreamControlAction = iota
	PeerStreamControlIgnore
	PeerStreamControlAbortState
)

func PeerMaxDataAction(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) PeerStreamControlAction {
	if IgnoreLateNonOpeningControl(localSend, localReceive, sendHalf, recvHalf) {
		return PeerStreamControlIgnore
	}
	if !localSend {
		return PeerStreamControlAbortState
	}
	return PeerStreamControlApply
}

func PeerBlockedAction(localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) PeerStreamControlAction {
	if IgnoreLateNonOpeningControl(localSend, localReceive, sendHalf, recvHalf) {
		return PeerStreamControlIgnore
	}
	if !localReceive {
		return PeerStreamControlAbortState
	}
	return PeerStreamControlApply
}

func ShouldAdvertiseMaxData(localReceive bool, recvHalf RecvHalfState) bool {
	return localReceive && recvHalf == RecvHalfOpen
}

func ShouldAdvertiseBlocked(localSend bool, sendHalf SendHalfState) bool {
	return localSend && sendHalf == SendHalfOpen
}
