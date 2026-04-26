package runtime

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestStreamWeightLatencyHintStronglyBoostsShortQueues(t *testing.T) {
	const maxPayload = 16384

	short := StreamWeight(4, 512, wire.SchedulerLatency, maxPayload)
	long := StreamWeight(4, maxPayload*8, wire.SchedulerLatency, maxPayload)
	if short <= long {
		t.Fatalf("latency short-queue weight = %d, long-queue weight = %d, want short > long", short, long)
	}
}

func TestStreamWeightBulkHintFlattensPrioritySpread(t *testing.T) {
	const maxPayload = 16384

	balancedLow := StreamWeight(0, maxPayload, wire.SchedulerBalancedFair, maxPayload)
	balancedHigh := StreamWeight(20, maxPayload, wire.SchedulerBalancedFair, maxPayload)
	bulkLow := StreamWeight(0, maxPayload, wire.SchedulerBulkThroughput, maxPayload)
	bulkHigh := StreamWeight(20, maxPayload, wire.SchedulerBulkThroughput, maxPayload)

	if balancedHigh-balancedLow <= bulkHigh-bulkLow {
		t.Fatalf("bulk hint did not flatten priority spread: balanced=(%d,%d) bulk=(%d,%d)", balancedLow, balancedHigh, bulkLow, bulkHigh)
	}
}

func TestGroupWeightUsesEqualShareForExplicitGroups(t *testing.T) {
	if got := GroupWeight(GroupKey{Kind: 1, Value: 7}, 96, wire.SchedulerBalancedFair); got != 24 {
		t.Fatalf("explicit-group weight = %d, want 24", got)
	}
	if got := GroupWeight(GroupKey{Kind: 0, Value: 4}, 96, wire.SchedulerBalancedFair); got != 96 {
		t.Fatalf("default-group weight = %d, want 96", got)
	}
}

func TestAdjustWeightForLagBoostsFreshAndUnderServedFlows(t *testing.T) {
	base := uint64(24)
	window := FeedbackWindow(wire.SchedulerBalancedFair, 16384)

	boosted := AdjustWeightForLag(base, window, window, false)
	if boosted <= base {
		t.Fatalf("positive-lag weight = %d, want > %d", boosted, base)
	}
	fresh := AdjustWeightForLag(base, 0, window, true)
	if fresh <= base {
		t.Fatalf("fresh weight = %d, want > %d", fresh, base)
	}
}

func TestAdjustWeightForLagPenalizesOverServedFlows(t *testing.T) {
	base := uint64(24)
	window := FeedbackWindow(wire.SchedulerBalancedFair, 16384)

	penalized := AdjustWeightForLag(base, -window, window, false)
	if penalized >= base {
		t.Fatalf("negative-lag weight = %d, want < %d", penalized, base)
	}
	if penalized == 0 {
		t.Fatalf("negative-lag weight = %d, want >= 1", penalized)
	}
}

func TestApplyLagFeedbackClampsBeforeInt64Overflow(t *testing.T) {
	const window int64 = 10

	positive := applyLagFeedback(maxSignedInt64-1, maxSignedInt64, 0, window)
	if positive != 2*window {
		t.Fatalf("positive overflow lag = %d, want %d", positive, 2*window)
	}

	negative := applyLagFeedback(-maxSignedInt64+1, 0, maxSignedInt64, window)
	if negative != -2*window {
		t.Fatalf("negative overflow lag = %d, want %d", negative, -2*window)
	}
}
