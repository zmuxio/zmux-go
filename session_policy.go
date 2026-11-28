package zmux

import (
	"fmt"
	"net"

	"github.com/zmuxio/zmux-go/internal/core"
)

// CapacityLimits bounds local attachment, carrier and peer Join work.
type CapacityLimits struct {
	// JoinHeadroom defaults to eight. An explicit zero disables headroom.
	JoinHeadroom *int
	// SteadyAttachments defaults to one. Multipath is offered above one.
	SteadyAttachments int
	// LocalCarriers defaults to twice the initial attachment envelope.
	LocalCarriers int
	// PendingPeerJoins defaults to four.
	PendingPeerJoins int
}

func (l CapacityLimits) resolve() (core.CapacityLimits, error) {
	r := core.CapacityLimits{
		SteadyAttachments: l.SteadyAttachments,
		JoinHeadroom:      core.DefaultJoinHeadroom,
		LocalCarriers:     l.LocalCarriers,
		PendingPeerJoins:  l.PendingPeerJoins,
	}
	if l.JoinHeadroom != nil {
		r.JoinHeadroom = *l.JoinHeadroom
	}
	r, ok := r.Resolve()
	if !ok {
		return r, fmt.Errorf("%w: invalid CapacityLimits", ErrInvalidConfig)
	}
	return r, nil
}

// CandidateRule constrains the local directions of a registered candidate.
type CandidateRule struct {
	ID    CandidateID
	Allow Directions
}

// SessionPolicy declares the host's local capacity and Join authorization.
type SessionPolicy struct {
	AuthorizeJoin  JoinAuthorizer
	Topology       []CandidateRule
	Limits         CapacityLimits
	Revision       uint64
	AuthGeneration uint64
}

func (p SessionPolicy) resolve() (core.SessionPolicy, error) {
	limits, err := p.Limits.resolve()
	r := core.SessionPolicy{Revision: p.Revision, AuthGeneration: p.AuthGeneration, Limits: limits}
	if err != nil {
		return r, err
	}
	if len(p.Topology) > 0 {
		r.Topology = make(map[string]core.Directions, len(p.Topology))
	}
	for _, rule := range p.Topology {
		_, duplicate := r.Topology[string(rule.ID)]
		if rule.ID == "" || duplicate || rule.Allow&^Both != 0 {
			return r, fmt.Errorf("%w: invalid candidate topology", ErrInvalidConfig)
		}
		r.Topology[string(rule.ID)] = rule.Allow
	}
	return r, nil
}

// SetSessionPolicy applies a newer local policy revision. The callback may
// change only with a higher authorization generation. An authenticated Join
// must use the same generation at authorization, Claim and confirmation.
func (c *Conn) SetSessionPolicy(policy SessionPolicy) error {
	if c == nil {
		return net.ErrClosed
	}
	next, err := policy.resolve()
	if err != nil {
		return err
	}
	c.lock()
	defer c.unlock()
	if c.closing {
		return c.errLocked()
	}
	current := c.s.SessionPolicy()
	if err := c.s.SetSessionPolicy(next); err != nil {
		return apiErr(err)
	}
	var cause error
	if next.AuthGeneration != current.AuthGeneration {
		c.authorizeJoin = policy.AuthorizeJoin
		cause = ErrJoinAuthorizationChanged
	}
	for len(c.changes) != 0 {
		c.changes[0].cancelWithCauseLocked(cause)
	}
	return nil
}
