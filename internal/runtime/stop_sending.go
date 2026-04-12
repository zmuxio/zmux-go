package runtime

import (
	"math/bits"
	"time"
)

const RepoDefaultStopSendingDrainWindow = 100 * time.Millisecond

type StopSendingGracefulInput struct {
	RecvAbortive     bool
	NeedsLocalOpener bool
	LocalOpened      bool
	SendCommitted    bool

	QueuedDataBytes uint64
	InflightQueued  uint64

	FragmentCap      uint64
	SendRateEstimate uint64
	ExplicitTailCap  uint64
	DrainWindow      time.Duration
}

type StopSendingGracefulDecision struct {
	Attempt        bool
	TailBudget     uint64
	CommittedTail  uint64
	InflightTail   uint64
	QueuedOnlyTail uint64
}

func EvaluateStopSendingGraceful(input StopSendingGracefulInput) StopSendingGracefulDecision {
	decision := StopSendingGracefulDecision{
		CommittedTail:  StopSendingCommittedTail(input.QueuedDataBytes, input.InflightQueued),
		InflightTail:   input.InflightQueued,
		QueuedOnlyTail: StopSendingQueuedOnlyTail(input.QueuedDataBytes, input.InflightQueued),
		TailBudget:     StopSendingTailBudget(input.FragmentCap, input.ExplicitTailCap, input.SendRateEstimate, input.DrainWindow),
	}

	if input.RecvAbortive || input.NeedsLocalOpener {
		return decision
	}
	if decision.CommittedTail == 0 {
		decision.Attempt = input.LocalOpened && input.SendCommitted
		return decision
	}
	if decision.InflightTail > 0 {
		decision.Attempt = decision.InflightTail <= decision.TailBudget && input.SendCommitted
		return decision
	}
	decision.Attempt = decision.QueuedOnlyTail <= decision.TailBudget && input.SendCommitted
	return decision
}

func StopSendingCommittedTail(queuedDataBytes, inflightQueued uint64) uint64 {
	return MaxUint64(queuedDataBytes, inflightQueued)
}

func StopSendingQueuedOnlyTail(queuedDataBytes, inflightQueued uint64) uint64 {
	if queuedDataBytes <= inflightQueued {
		return 0
	}
	return queuedDataBytes - inflightQueued
}

func StopSendingTailBudget(fragmentCap, explicitTailCap, sendRateEstimate uint64, drainWindow time.Duration) uint64 {
	if explicitTailCap > 0 {
		return explicitTailCap
	}
	budget := StopSendingStaticTailCap(fragmentCap)
	if rateBudget := StopSendingGracefulRateBudget(sendRateEstimate, StopSendingDrainWindow(drainWindow)); rateBudget > budget {
		return rateBudget
	}
	return budget
}

func StopSendingStaticTailCap(fragmentCap uint64) uint64 {
	limit := ScaledFragmentCap(fragmentCap, 1, 4)
	switch {
	case limit == 0:
		return 0
	case limit > 512:
		return 512
	default:
		return limit
	}
}

func StopSendingGracefulRateBudget(rateBytesPerSecond uint64, window time.Duration) uint64 {
	if rateBytesPerSecond == 0 || window <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(rateBytesPerSecond, uint64(window))
	divisor := uint64(time.Second)
	if hi >= divisor {
		return ^uint64(0)
	}
	budget, _ := bits.Div64(hi, lo, divisor)
	if budget == 0 {
		return 1
	}
	return budget
}

func StopSendingDrainWindow(override time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	return RepoDefaultStopSendingDrainWindow
}
