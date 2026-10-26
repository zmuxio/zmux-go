package zmux

import "github.com/zmuxio/zmux-go/internal/core"

// Scheduler is a scheduling policy of a multipath session (SCHEDULER.md
// Section 3). The built-in policies are returned by Affinity,
// SessionAffinity, SpreadETA, SpreadRoundRobin and SpreadRateWeighted.
type Scheduler interface {
	policy() core.Policy
}

type builtin core.Policy

func (b builtin) policy() core.Policy { return core.Policy(b) }

// Affinity binds each stream direction to one pipe (the default).
func Affinity() Scheduler { return builtin(core.PolicyAffinity) }

// SessionAffinity uses one active pipe per tier.
func SessionAffinity() Scheduler { return builtin(core.PolicySessionAffinity) }

// SpreadETA sends every chunk on the pipe with the earliest arrival.
func SpreadETA() Scheduler { return builtin(core.PolicySpreadETA) }

// SpreadRoundRobin shares chunks by the pipes' weights.
func SpreadRoundRobin() Scheduler { return builtin(core.PolicySpreadRoundRobin) }

// SpreadRateWeighted shares chunks by weight times measured rate.
func SpreadRateWeighted() Scheduler { return builtin(core.PolicySpreadRateWeighted) }

// Direction is a pipe direction from the local endpoint's view.
type Direction uint8

const (
	Bidirectional Direction = iota
	SendOnly
	ReceiveOnly
)
