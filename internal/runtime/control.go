package runtime

import "github.com/zmuxio/zmux-go/internal/wire"

const DefaultUrgencyRank = 100

func IsUrgentType(frameType wire.FrameType) bool {
	switch frameType {
	case wire.FrameTypeCLOSE,
		wire.FrameTypeGOAWAY,
		wire.FrameTypeABORT,
		wire.FrameTypeRESET,
		wire.FrameTypeStopSending,
		wire.FrameTypeMAXDATA,
		wire.FrameTypeBLOCKED,
		wire.FrameTypePONG,
		wire.FrameTypePING:
		return true
	default:
		return false
	}
}

func UrgencyRank(frameType wire.FrameType) int {
	switch frameType {
	case wire.FrameTypeCLOSE:
		return 0
	case wire.FrameTypeGOAWAY:
		return 1
	case wire.FrameTypeABORT:
		return 2
	case wire.FrameTypeRESET:
		return 3
	case wire.FrameTypeStopSending:
		return 4
	case wire.FrameTypeMAXDATA:
		return 5
	case wire.FrameTypeBLOCKED:
		return 6
	case wire.FrameTypePONG:
		return 7
	case wire.FrameTypePING:
		return 8
	default:
		return DefaultUrgencyRank
	}
}

type PriorityUpdatePlan struct {
	NextPendingBytes uint64
	ProjectedTracked uint64
	Accept           bool
}

type AdvisoryHandoffPlan struct {
	ProjectedTracked uint64
	Accept           bool
}

func ProjectTrackedMemoryDelta(tracked, removed, added uint64) uint64 {
	if removed >= tracked {
		tracked = 0
	} else {
		tracked -= removed
	}
	return SaturatingAdd(tracked, added)
}

func PlanPendingPriorityUpdate(tracked, currentPending, previousEntry, nextEntry, budget, hardCap uint64) PriorityUpdatePlan {
	nextPending := ProjectTrackedMemoryDelta(currentPending, previousEntry, nextEntry)
	projectedTracked := ProjectTrackedMemoryDelta(tracked, currentPending, nextPending)
	return PriorityUpdatePlan{
		NextPendingBytes: nextPending,
		ProjectedTracked: projectedTracked,
		Accept:           nextPending <= budget && projectedTracked <= hardCap,
	}
}

func PlanPriorityAdvisoryHandoff(tracked, removedPending, advisoryBytes, hardCap uint64) AdvisoryHandoffPlan {
	projectedTracked := ProjectTrackedMemoryDelta(tracked, removedPending, advisoryBytes)
	return AdvisoryHandoffPlan{
		ProjectedTracked: projectedTracked,
		Accept:           projectedTracked <= hardCap,
	}
}
