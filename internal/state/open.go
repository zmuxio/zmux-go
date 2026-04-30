package state

import (
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

const (
	DefaultAdmissionSoftCap       = 32
	DefaultAdmissionHardCap       = 64
	DefaultProvisionalOpenHardCap = DefaultAdmissionHardCap
	ProvisionalOpenMaxAge         = 5 * time.Second
)

func LocalOpenRefusedByGoAway(id uint64, bidi bool, peerGoAwayBidi, peerGoAwayUni uint64) bool {
	if bidi {
		return id > peerGoAwayBidi
	}
	return id > peerGoAwayUni
}

func MaxStreamIDForClass(nextID uint64) uint64 {
	if nextID == 0 || nextID > wire.MaxVarint62 {
		return 0
	}
	return nextID + ((wire.MaxVarint62-nextID)/4)*4
}

func ProjectedLocalOpenID(nextID uint64, queueLen int) uint64 {
	if queueLen <= 0 || nextID == 0 || nextID > wire.MaxVarint62 {
		return nextID
	}
	remaining := (wire.MaxVarint62 - nextID) / 4
	if uint64(queueLen) > remaining {
		return wire.MaxVarint62 + 1
	}
	return nextID + uint64(queueLen)*4
}

func AdmissionSoftCap(pendingLimit int) int {
	if pendingLimit <= 0 {
		return DefaultAdmissionSoftCap
	}
	limit := pendingLimit / 4
	if limit < 16 {
		limit = 16
	}
	return limit
}

func AdmissionHardCap(pendingLimit int) int {
	if pendingLimit <= 0 {
		return DefaultAdmissionHardCap
	}
	limit := pendingLimit / 2
	if limit < 32 {
		limit = 32
	}
	return limit
}

func ProvisionalSoftCap(bidi bool, pendingLimit int) int {
	_ = bidi
	return AdmissionSoftCap(pendingLimit)
}

func ProvisionalHardCap(bidi bool, pendingLimit int) int {
	_ = bidi
	return AdmissionHardCap(pendingLimit)
}

func ProvisionalExpired(idSet bool, created time.Time, now time.Time) bool {
	if idSet || created.IsZero() {
		return false
	}
	return now.Sub(created) > ProvisionalOpenMaxAge
}

func ProvisionalAvailableCount(nextID, maxID uint64) int {
	if nextID > maxID {
		return 0
	}
	count := ((maxID - nextID) / 4) + 1
	maxInt := uint64(^uint(0) >> 1)
	if count > maxInt {
		return int(maxInt)
	}
	return int(count)
}

func PeerOpenRefusedByGoAway(streamID uint64, localGoAwayBidi, localGoAwayUni uint64) bool {
	if StreamIsBidi(streamID) {
		return streamID > localGoAwayBidi
	}
	return streamID > localGoAwayUni
}

func ExpectedNextPeerStreamID(streamID, nextPeerBidi, nextPeerUni uint64) uint64 {
	if StreamIsBidi(streamID) {
		return nextPeerBidi
	}
	return nextPeerUni
}

func ActiveStreamWithinLimit(bidi bool, activeBidi, activeUni, maxIncomingBidi, maxIncomingUni uint64) bool {
	if bidi {
		return activeBidi < maxIncomingBidi
	}
	return activeUni < maxIncomingUni
}

func PeerStreamWithinLimit(bidi bool, activePeerBidi, activePeerUni, maxIncomingBidi, maxIncomingUni uint64) bool {
	return ActiveStreamWithinLimit(bidi, activePeerBidi, activePeerUni, maxIncomingBidi, maxIncomingUni)
}

func DecrementActiveStreamCount(bidi bool, activeBidi, activeUni uint64) (uint64, uint64) {
	if bidi {
		if activeBidi > 0 {
			activeBidi--
		}
		return activeBidi, activeUni
	}
	if activeUni > 0 {
		activeUni--
	}
	return activeBidi, activeUni
}

func DecrementActivePeerCount(bidi bool, activePeerBidi, activePeerUni uint64) (uint64, uint64) {
	return DecrementActiveStreamCount(bidi, activePeerBidi, activePeerUni)
}

func InitialSendWindow(localRole wire.Role, peer wire.Settings, streamID uint64) uint64 {
	if !StreamIsBidi(streamID) {
		if StreamIsLocal(localRole, streamID) {
			return peer.InitialMaxStreamDataUni
		}
		return 0
	}
	if StreamIsLocal(localRole, streamID) {
		return peer.InitialMaxStreamDataBidiPeerOpened
	}
	return peer.InitialMaxStreamDataBidiLocallyOpened
}

func InitialLocalOpenedSendWindow(peer wire.Settings, bidi bool) uint64 {
	if bidi {
		return peer.InitialMaxStreamDataBidiPeerOpened
	}
	return peer.InitialMaxStreamDataUni
}

func InitialReceiveWindow(localRole wire.Role, local wire.Settings, streamID uint64) uint64 {
	if !StreamIsBidi(streamID) {
		if StreamIsLocal(localRole, streamID) {
			return 0
		}
		return local.InitialMaxStreamDataUni
	}
	if StreamIsLocal(localRole, streamID) {
		return local.InitialMaxStreamDataBidiLocallyOpened
	}
	return local.InitialMaxStreamDataBidiPeerOpened
}
