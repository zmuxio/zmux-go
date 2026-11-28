package zmux

import (
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// PipeHealthState identifies a direction’s service phase.
type PipeHealthState = core.HealthState

// Directional pipe service phases.
const (
	PipeHealthAbsent      = core.HealthAbsent
	PipeHealthJoining     = core.HealthJoining
	PipeHealthUnknown     = core.HealthUnknown
	PipeHealthUsable      = core.HealthUsable
	PipeHealthUnavailable = core.HealthUnavailable
	PipeHealthDraining    = core.HealthDraining
	PipeHealthEnded       = core.HealthEnded
)

// PipeHealthReason identifies the cause of a directional health transition.
type PipeHealthReason = core.HealthReason

// Reasons for directional health transitions.
const (
	PipeHealthAwaitAttachment  = core.HealthAwaitAttachment
	PipeHealthObserveJoin      = core.HealthObserveJoin
	PipeHealthIdle             = core.HealthIdle
	PipeHealthProbation        = core.HealthProbation
	PipeHealthRevalidate       = core.HealthRevalidate
	PipeHealthAckStall         = core.HealthAckStall
	PipeHealthWriteStall       = core.HealthWriteStall
	PipeHealthInboundSilence   = core.HealthInboundSilence
	PipeHealthCarrierError     = core.HealthCarrierError
	PipeHealthJoinTimeout      = core.HealthJoinTimeout
	PipeHealthProgress         = core.HealthProgress
	PipeHealthPlannedDrain     = core.HealthPlannedDrain
	PipeHealthSessionClosing   = core.HealthSessionClosing
	PipeHealthCanceled         = core.HealthCanceled
	PipeHealthTopologyExcluded = core.HealthTopologyExcluded
)

// PipeHealthProof identifies evidence committed for the current generation.
type PipeHealthProof = core.HealthProof

// Evidence recorded by a direction's health generation.
const (
	PipeHealthProofNone   = core.HealthProofNone
	PipeHealthProofProbe  = core.HealthProofProbe
	PipeHealthProofRecord = core.HealthProofRecord
)

// DirectionHealth reports service ability and stability of one direction.
// StableSince is the start of continuous service in the current episode, when known.
type DirectionHealth struct {
	Since, StableSince time.Time
	Generation         uint64
	State              PipeHealthState
	Reason             PipeHealthReason
	CanCarry, Settled  bool
	Proof              PipeHealthProof
}

// PipeHealth is a value snapshot of both directions. Seq is the last
// transition number for this pipe, in the session's sequence space.
type PipeHealth struct {
	JoinedAt, AttachedAt time.Time
	Failure              error
	Send, Receive        DirectionHealth
	Seq                  uint64
	SinceJoin            time.Duration
	PeerAttached         bool
}

// PipeHealthChange reports a committed direction transition.
type PipeHealthChange struct {
	At            time.Time
	Err           error
	Before, After DirectionHealth
	Seq           uint64
	PipeID        PipeID
	SinceJoin     time.Duration
	Dir           Direction
	Reason        PipeHealthReason
}

// StabilityPolicy sets the continuous service hold from attachment or recovery.
// Settlement also requires proof in the current episode. Proof can arrive during
// or after the hold. Omitting HoldFor selects five seconds. Set(0) still requires proof.
type StabilityPolicy struct {
	HoldFor Change[time.Duration]
}

// Health returns the current health of both directions on p.
func (p *Pipe) Health() (PipeHealth, bool) {
	if p == nil || p.c == nil {
		return PipeHealth{}, false
	}
	c := p.c
	var out PipeHealth
	var ok bool
	c.peek(func() {
		var h core.PipeHealth
		if h, ok = c.s.PipeHealth(p.id); ok {
			out = c.pipeHealth(h)
		}
	})
	return out, ok
}

func (c *Conn) pipeHealth(h core.PipeHealth) PipeHealth {
	out := PipeHealth{
		Seq: h.Seq, Send: c.directionHealth(h.Send), Receive: c.directionHealth(h.Receive),
		PeerAttached: h.PeerAttached, JoinedAt: c.clk.wall(h.JoinedAt),
		SinceJoin: time.Duration(c.clk.now().Sub(h.JoinedAt)), Failure: h.Failure,
	}
	if h.PeerAttached {
		out.AttachedAt = c.clk.wall(h.AttachedAt)
	}
	return out
}

func (c *Conn) directionHealth(h core.DirectionHealth) DirectionHealth {
	d := DirectionHealth{State: h.State, Reason: h.Reason, CanCarry: h.CanCarry, Settled: h.Settled, Generation: h.Generation, Proof: h.Proof}
	if h.State != PipeHealthAbsent {
		d.Since = c.clk.wall(h.Since)
	}
	if h.Stable {
		d.StableSince = c.clk.wall(h.StableSince)
	}
	return d
}
