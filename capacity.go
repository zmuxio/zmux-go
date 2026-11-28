package zmux

import (
	"net"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Directions is a set of local directions.
type Directions = core.Directions

const (
	// Send is the local outbound direction.
	Send = core.DirectionSend
	// Receive is the local inbound direction.
	Receive = core.DirectionReceive
	// Both includes both local directions.
	Both = core.DirectionBoth
)

// CapacitySnapshot reports the session's locally enforced capacity and
// directional coverage. Changed closes on the next capacity commit.
type CapacitySnapshot struct {
	BindingGeneration, TargetRevision uint64
	Changed                           <-chan struct{}
	Members                           []MemberSnapshot
	Limits                            CapacityLimits
	Openers                           int
	Observed                          uint64
	PolicyRevision                    uint64
	AuthGeneration                    uint64
	Envelope                          int
	OwnAttachments                    int
	PeerAttachments                   int
	Reserved                          int
	LocalCarriers                     int
	PendingPeerJoins                  int
	AuthorizingPeerJoins              int
	Required                          Directions
	Covered                           Directions
	Stable                            Directions
}

// Capacity atomically observes capacity and subscribes to changes future
// callers can use to refresh that snapshot.
func (c *Conn) Capacity() (CapacitySnapshot, error) {
	var v CapacitySnapshot
	if c == nil {
		return v, net.ErrClosed
	}
	c.lock()
	defer c.unlockPeek()
	if c.closing {
		return v, c.errLocked()
	}
	return c.capacityLocked(), nil
}

// MemberSnapshot projects the core membership and directional evidence.
type MemberSnapshot struct {
	Paths                     []AttachmentQuality
	JoinedAt                  time.Time
	Candidate                 CandidateID
	Pipes                     []PipeID
	ID                        MemberID
	Attachments               int
	Rotate                    bool
	Expected, Covered, Stable Directions
	Complete, Draining        bool
}

// MemberOf resolves a currently indexed PipeID to its whole member.
func (c *Conn) MemberOf(pipe PipeID) (MemberID, error) {
	if c == nil {
		return 0, net.ErrClosed
	}
	var id MemberID
	c.peek(func() { id = c.s.MemberOf(uint64(pipe)) })
	if id == 0 {
		return 0, ErrUnknownMember
	}
	return id, nil
}

func (c *Conn) capacityLocked() CapacitySnapshot {
	var v CapacitySnapshot
	if c.control != nil {
		v.BindingGeneration, v.TargetRevision = c.control.view.BindingGeneration, c.control.target.Revision
	}
	p := c.s.SessionPolicy()
	v.Observed, v.Changed = c.s.WatchCapacity()
	v.PolicyRevision, v.AuthGeneration = p.Revision, p.AuthGeneration
	v.Required = c.s.RequiredDirections()
	v.Covered, v.Stable = c.s.Coverage(nil)
	v.OwnAttachments, v.PeerAttachments, v.Reserved, v.Envelope = c.s.ProtocolCapacity()
	v.LocalCarriers = c.s.CarrierCount()
	v.Openers = c.s.Openers()
	v.PendingPeerJoins = c.s.PendingPeerJoins()
	v.AuthorizingPeerJoins = c.s.AuthorizingPeers()
	v.Limits = CapacityLimits{
		SteadyAttachments: p.Limits.SteadyAttachments,
		JoinHeadroom:      new(p.Limits.JoinHeadroom),
		LocalCarriers:     p.Limits.LocalCarriers,
		PendingPeerJoins:  p.Limits.PendingPeerJoins,
	}
	physical := make(map[CarrierID]*CarrierLoss)
	for _, member := range c.s.Members(nil) {
		m := MemberSnapshot{Rotate: member.Rotate, ID: member.ID, Candidate: CandidateID(member.Candidate),
			Expected: member.Expected, Covered: member.Covered, Stable: member.Stable,
			Complete: member.Complete, Draining: member.Draining, Attachments: member.Attachments,
			JoinedAt: c.clk.wall(member.JoinedAt), Pipes: make([]PipeID, len(member.Pipes))}
		for i, id := range member.Pipes {
			m.Pipes[i] = PipeID(id)
			m.Paths = append(m.Paths, c.attachmentQualityLocked(m.ID, PipeID(id), physical))
		}
		v.Members = append(v.Members, m)
	}
	return v
}
