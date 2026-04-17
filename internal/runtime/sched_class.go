package runtime

import "github.com/zmuxio/zmux-go/internal/wire"

type trafficClass uint8

const (
	trafficClassInteractive trafficClass = iota
	trafficClassBulk
)

const (
	interactiveBurstLimit uint32 = 8
	bulkReserveWindow     uint32 = 4
	bulkEntryMultiplier   uint64 = 2
	agingRoundThreshold          = 2
	classScoreScale       uint64 = 8
)

func classifyStreamClass(queuedBytes, priority uint64, hint wire.SchedulerHint, previous trafficClass, hasPrevious bool, interactiveQuantum uint64) trafficClass {
	if interactiveQuantum == 0 {
		interactiveQuantum = SchedulerQuantum(0)
	}
	bulkThreshold := SaturatingMul(interactiveQuantum, bulkEntryMultiplier)
	if queuedBytes <= interactiveQuantum {
		return trafficClassInteractive
	}
	if queuedBytes > bulkThreshold {
		return trafficClassBulk
	}
	if hasPrevious {
		return previous
	}
	switch {
	case hint == wire.SchedulerBulkThroughput:
		return trafficClassBulk
	case hint == wire.SchedulerLatency || priority >= 4:
		return trafficClassInteractive
	case WriteBurstLimit(priority, hint) >= DefaultWriteBurstFrames:
		return trafficClassBulk
	default:
		return trafficClassInteractive
	}
}

func chooseTrafficClass(prefs batchTiePrefs, hint wire.SchedulerHint, interactive, bulk wfqGroupCandidate, interactiveStreak, classSelectionsSinceBulk uint32) trafficClass {
	if interactive.stream.streamID == 0 {
		return trafficClassBulk
	}
	if bulk.stream.streamID == 0 {
		return trafficClassInteractive
	}
	if interactiveStreak >= interactiveBurstLimit || classSelectionsSinceBulk >= bulkReserveWindow-1 {
		return trafficClassBulk
	}
	if betterClassCandidate(prefs, interactive, bulk, hint) {
		return trafficClassInteractive
	}
	return trafficClassBulk
}

func betterClassCandidate(prefs batchTiePrefs, left, right wfqGroupCandidate, hint wire.SchedulerHint) bool {
	if left.stream.streamID == 0 {
		return false
	}
	if right.stream.streamID == 0 {
		return true
	}
	if left.eligible != right.eligible {
		return left.eligible
	}
	leftPrimary := scaledClassTag(left, hint, left.groupStart)
	rightPrimary := scaledClassTag(right, hint, right.groupStart)
	if left.eligible {
		leftPrimary = scaledClassTag(left, hint, left.groupFinish)
		rightPrimary = scaledClassTag(right, hint, right.groupFinish)
	}
	if leftPrimary != rightPrimary {
		return leftPrimary < rightPrimary
	}
	leftSecondary := scaledClassTag(left, hint, left.groupFinish)
	rightSecondary := scaledClassTag(right, hint, right.groupFinish)
	if left.eligible {
		leftSecondary = scaledClassTag(left, hint, left.groupStart)
		rightSecondary = scaledClassTag(right, hint, right.groupStart)
	}
	if leftSecondary != rightSecondary {
		return leftSecondary < rightSecondary
	}
	return betterGroupCandidate(prefs, left, right)
}

func scaledClassTag(candidate wfqGroupCandidate, hint wire.SchedulerHint, tag uint64) uint64 {
	weight := classBiasWeight(candidate.class, hint)
	if weight == 0 {
		return tag
	}
	return SaturatingMulDivFloor(tag, classScoreScale, weight)
}

func classBiasWeight(class trafficClass, hint wire.SchedulerHint) uint64 {
	switch hint {
	case wire.SchedulerLatency:
		if class == trafficClassInteractive {
			return 8
		}
		return 2
	case wire.SchedulerBulkThroughput:
		if class == trafficClassInteractive {
			return 2
		}
		return 8
	default:
		if class == trafficClassInteractive {
			return 6
		}
		return 4
	}
}
