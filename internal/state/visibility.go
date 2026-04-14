package state

type LocalOpenPhase uint8

const (
	LocalOpenPhaseNone LocalOpenPhase = iota
	LocalOpenPhaseNeedsCommit
	LocalOpenPhaseNeedsEmit
	LocalOpenPhaseQueued
	LocalOpenPhasePeerVisible
)

type LocalOpenVisibility struct {
	LocalOpened   bool
	SendCommitted bool
	PeerVisible   bool
	OpenerQueued  bool
}

func (v LocalOpenVisibility) Phase() LocalOpenPhase {
	switch {
	case !v.LocalOpened:
		return LocalOpenPhaseNone
	case v.PeerVisible:
		return LocalOpenPhasePeerVisible
	case !v.SendCommitted:
		return LocalOpenPhaseNeedsCommit
	case v.OpenerQueued:
		return LocalOpenPhaseQueued
	default:
		return LocalOpenPhaseNeedsEmit
	}
}

func (p LocalOpenPhase) IsLocal() bool {
	return p != LocalOpenPhaseNone
}

func (p LocalOpenPhase) NeedsLocalOpener() bool {
	return p == LocalOpenPhaseNeedsCommit
}

func (p LocalOpenPhase) AwaitingPeerVisibility() bool {
	switch p {
	case LocalOpenPhaseNeedsCommit, LocalOpenPhaseNeedsEmit, LocalOpenPhaseQueued:
		return true
	default:
		return false
	}
}

func (p LocalOpenPhase) ShouldEmitOpenerFrame() bool {
	return p == LocalOpenPhaseNeedsCommit || p == LocalOpenPhaseNeedsEmit
}

func (p LocalOpenPhase) ShouldMarkPeerVisible() bool {
	return p.IsLocal() && p != LocalOpenPhasePeerVisible
}

func (p LocalOpenPhase) CanTakePendingPriorityUpdate() bool {
	return !p.AwaitingPeerVisibility()
}

func (p LocalOpenPhase) ShouldQueueStreamBlocked(availableStream uint64) bool {
	return availableStream == 0 && p == LocalOpenPhasePeerVisible
}

func ShouldEnqueueAccepted(applicationVisible, accepted, enqueued bool) bool {
	return applicationVisible && !accepted && !enqueued
}

func ShouldFlushStreamMaxData(idSet, localReceive bool, phase LocalOpenPhase, readStopped, recvTerminal bool) (flush bool, keep bool) {
	if !idSet || !localReceive {
		return false, false
	}
	if readStopped || recvTerminal {
		return false, false
	}
	if phase.AwaitingPeerVisibility() {
		return false, true
	}
	return true, false
}

func ShouldFlushStreamBlocked(idSet, localSend bool, phase LocalOpenPhase, sendHalf SendHalfState) (flush bool, keep bool) {
	if !idSet || !localSend {
		return false, false
	}
	if sendHalf != SendHalfOpen {
		return false, false
	}
	if phase.AwaitingPeerVisibility() {
		return false, true
	}
	return true, false
}

func ShouldFlushPriorityUpdate(phase LocalOpenPhase, sendHalf SendHalfState) (flush bool, keep bool) {
	if sendHalf == SendHalfStopSeen || SendTerminal(sendHalf) {
		return false, false
	}
	if !phase.CanTakePendingPriorityUpdate() {
		return false, true
	}
	return true, false
}

func ShouldReclaimUnseenLocalStream(phase LocalOpenPhase, bidi bool, id, peerGoAwayBidi, peerGoAwayUni uint64, localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) bool {
	if !phase.IsLocal() || phase == LocalOpenPhasePeerVisible || FullyTerminal(localSend, localReceive, sendHalf, recvHalf) {
		return false
	}
	if bidi {
		return id > peerGoAwayBidi
	}
	return id > peerGoAwayUni
}

func ShouldFinalizePeerActive(activeCounted, localOpened, localSend, localReceive bool, sendHalf SendHalfState, recvHalf RecvHalfState) bool {
	return activeCounted && !localOpened && FullyTerminal(localSend, localReceive, sendHalf, recvHalf)
}
