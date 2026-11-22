package zmux

import "github.com/zmuxio/zmux-go/internal/core"

// Scheduler is a scheduling policy of a multipath session. The built-in
// policies are returned by SchedulerAffinity, SchedulerSessionAffinity, SchedulerSpreadETA,
// SchedulerSpreadRoundRobin and SchedulerSpreadRateWeighted.
type Scheduler interface {
	policy() core.Policy
}

type builtin core.Policy

func (b builtin) policy() core.Policy { return core.Policy(b) }

// SchedulerAffinity binds each stream direction to one pipe (the default).
func SchedulerAffinity() Scheduler { return builtin(core.PolicyAffinity) }

// SchedulerSessionAffinity uses one active pipe per tier.
func SchedulerSessionAffinity() Scheduler { return builtin(core.PolicySessionAffinity) }

// SchedulerSpreadETA estimates arrival per chunk. It spreads to a later
// pipe when the stream's ready backlog can hide the delay and receive
// credit can hold the reordering.
func SchedulerSpreadETA() Scheduler { return builtin(core.PolicySpreadETA) }

// SchedulerSpreadRoundRobin shares chunks by the pipes' weights.
func SchedulerSpreadRoundRobin() Scheduler { return builtin(core.PolicySpreadRoundRobin) }

// SchedulerSpreadRateWeighted shares chunks by weight times measured rate.
func SchedulerSpreadRateWeighted() Scheduler { return builtin(core.PolicySpreadRateWeighted) }

// Direction is a pipe direction from the local endpoint's view.
type Direction uint8

// Pipe directions.
const (
	DirectionBidirectional Direction = iota
	DirectionSendOnly
	DirectionReceiveOnly
)
