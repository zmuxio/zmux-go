package core

import (
	"slices"

	"github.com/zmuxio/zmux-go/internal/join"
)

// DefaultMaxUnconfirmed is the default per-session bound on Unconfirmed
// carriers (MULTIPATH.md Section 5.6).
const DefaultMaxUnconfirmed = 4

// holdPeerJoin records the claimed Hello and identifies evictions for the
// I/O owner to clean up. Bound has no protocol slot before confirmation.
func (s *Session) holdPeerJoin(pj *PeerJoin, handoff any) (evicted []any) {
	pj.accepted = true
	if pj.Mode == join.Bound {
		return nil
	}
	pj.handoff = handoff
	for len(s.pendingPeers) >= s.policy.Limits.PendingPeerJoins {
		old := s.pendingPeers[0]
		evicted = append(evicted, old.handoff)
		s.DropPeerJoin(old)
	}
	s.pendingPeers = append(s.pendingPeers, pj)
	s.NotifyCapacity()
	return evicted
}

// DropPeerJoin consumes a pending confirmation exactly once.
func (s *Session) DropPeerJoin(pj *PeerJoin) bool {
	if pj.owner != s || !pj.accepted {
		return false
	}
	pj.accepted = false
	pj.handoff = nil
	if pj.Mode != join.Bound {
		i := slices.Index(s.pendingPeers, pj)
		s.pendingPeers = slices.Delete(s.pendingPeers, i, i+1)
		s.NotifyCapacity()
	}
	return true
}
