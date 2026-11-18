package core

import "github.com/zmuxio/zmux-go/internal/join"

// Unconfirmed is the acceptor's record of a session's Unconfirmed
// carriers (MULTIPATH.md Section 5.4 step 7): joins whose Hello was
// claimed and that wait for the joiner's confirmation. C is the caller's
// handle of a carrier. The zero value is empty; the caller serialises use.
type Unconfirmed[C any] struct {
	q []unconfirmed[C]
}

type unconfirmed[C any] struct {
	pj *PeerJoin
	c  C
}

// Hold records the carrier c of the claimed join pj. A join in Bound Mode
// is confirmed by its verification and holds no place (MULTIPATH.md
// Section 5.6). When the session already holds limit Unconfirmed carriers
// (at least 1), the oldest is evicted first and returned with ok true: the
// caller closes it as a failed join. Below the bound nothing is evicted;
// a new carrier is never refused for lack of room.
func (u *Unconfirmed[C]) Hold(pj *PeerJoin, c C, limit int) (evicted C, ok bool) {
	if pj.Mode == join.Bound {
		return evicted, false
	}
	if len(u.q) >= max(limit, 1) {
		evicted, ok = u.q[0].c, true
		u.q[0] = unconfirmed[C]{}
		u.q = u.q[1:]
	}
	u.q = append(u.q, unconfirmed[C]{pj: pj, c: c})
	return evicted, ok
}

// Drop forgets the join pj once it was confirmed or failed, so that no
// later Hello evicts it.
func (u *Unconfirmed[C]) Drop(pj *PeerJoin) {
	for i := range u.q {
		if u.q[i].pj == pj {
			copy(u.q[i:], u.q[i+1:])
			u.q[len(u.q)-1] = unconfirmed[C]{}
			u.q = u.q[:len(u.q)-1]
			return
		}
	}
}

// Len is the number of Unconfirmed carriers held.
func (u *Unconfirmed[C]) Len() int { return len(u.q) }

// DefaultMaxUnconfirmed is the default per-session bound on Unconfirmed
// carriers (MULTIPATH.md Section 5.6).
const DefaultMaxUnconfirmed = 4
