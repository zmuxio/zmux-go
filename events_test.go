package zmux

import (
	"slices"
	"testing"

	"github.com/zmuxio/zmux-go/internal/core"
)

// TestEventQueueRetirement checks the one report of retirement: a retired
// pipe's unread events leave the queue for one EventPipesRetired, which
// accumulates until it is read, counts the failures and ends not yet read,
// keeps the last four IDs and the drops of the bound; a pipe whose end was
// read counts as retired only.
func TestEventQueueRetirement(t *testing.T) {
	var q eventQueue
	q.push(Event{Kind: EventPipeAdded, Pipe: 2})
	q.push(Event{Kind: EventPipeFailed, Pipe: 2})
	q.push(Event{Kind: EventSessionResumed})
	q.push(Event{Kind: EventPipeClosed, Pipe: 4})
	q.retire(2, true)  // its failure unread
	q.retire(4, false) // its end unread
	if len(q.q) != 2 || q.q[0].Kind != EventSessionResumed || q.q[1].Kind != EventPipesRetired {
		t.Fatalf("queue %+v", q.q)
	}
	sum := q.q[1]
	if sum.Retired != 2 || sum.Failed != 1 || sum.Closed != 0+1 || !slices.Equal(sum.Last, []uint64{2, 4}) {
		t.Fatalf("summary %+v", sum)
	}
	q.q = nil // read
	// A pipe whose terminal event was read: retired only.
	q.push(Event{Kind: EventPipeClosed, Pipe: 6})
	q.q = nil
	q.retire(6, false)
	for id := uint64(8); id <= 14; id += 2 {
		q.retire(id, false) // no terminal event was ever queued: an unread end
	}
	if len(q.q) != 1 {
		t.Fatalf("one summary expected: %+v", q.q)
	}
	sum = q.q[0]
	if sum.Retired != 5 || sum.Closed != 4 || sum.Failed != 0 || !slices.Equal(sum.Last, []uint64{8, 10, 12, 14}) {
		t.Fatalf("summary %+v", sum)
	}
	// The bound drops non-terminal events, keeps terminal ones and the
	// summary, and the next summary carries the drops.
	q.q = nil
	q.push(Event{Kind: EventPipeFailed, Pipe: 20})
	for range eventQueueMax + 9 {
		q.push(Event{Kind: EventPipeQualityChanged, Pipe: 22})
	}
	if len(q.q) != eventQueueMax || q.q[0].Kind != EventPipeFailed {
		t.Fatalf("bound: %d events, first %v", len(q.q), q.q[0].Kind)
	}
	q.retire(22, false)
	i := slices.IndexFunc(q.q, func(x Event) bool { return x.Kind == EventPipesRetired })
	if i < 0 || q.q[i].Dropped != 10 {
		t.Fatalf("drops not reported: %+v", q.q[i:])
	}
	if slices.ContainsFunc(q.q, func(x Event) bool { return x.Pipe == 22 && pipeEvent(x.Kind) }) {
		t.Fatal("a retired pipe's events stayed queued")
	}
}

// The bound never drops terminal events or the summary: with only those
// queued, a new non-terminal event is dropped instead, and drops go into
// the unread summary at once.
func TestEventQueueBoundKeepsSummary(t *testing.T) {
	var q eventQueue
	q.retire(1, false)
	for id := uint64(2); len(q.q) < eventQueueMax; id += 2 {
		q.push(Event{Kind: EventPipeClosed, Pipe: id})
	}
	q.push(Event{Kind: EventPipeQualityChanged, Pipe: 3})
	q.push(Event{Kind: EventPipeFailed, Pipe: 5})
	if len(q.q) != eventQueueMax+1 || q.q[0].Kind != EventPipesRetired || q.q[0].Dropped != 1 ||
		q.q[len(q.q)-1].Kind != EventPipeFailed {
		t.Fatalf("%d events, first %+v, last %v", len(q.q), q.q[0], q.q[len(q.q)-1].Kind)
	}
	if slices.ContainsFunc(q.q, func(x Event) bool { return !keptEvent(x.Kind) }) {
		t.Fatal("a non-terminal event was queued past the bound")
	}
	// A drop with a non-terminal event to remove also counts at once.
	q = eventQueue{}
	q.retire(1, false)
	for len(q.q) < eventQueueMax {
		q.push(Event{Kind: EventPipeQualityChanged, Pipe: 3})
	}
	q.push(Event{Kind: EventPipeQualityChanged, Pipe: 3})
	if q.q[0].Kind != EventPipesRetired || q.q[0].Dropped != 1 || q.dropped != 0 {
		t.Fatalf("summary %+v, pending drops %d", q.q[0], q.dropped)
	}
}

// A terminal state that never entered the queue counts as unread per
// direction: a pipe whose inbound end was read but whose outbound
// direction retired without a terminal event counts as closed.
func TestEventQueueRetireUnreadDirection(t *testing.T) {
	var q eventQueue
	q.seenOut = map[uint64]core.OutState{2: core.OutActive, 4: core.OutFailed}
	q.mark(2, dirInSeen)
	q.push(Event{Kind: EventPipeClosed, Pipe: 2, Direction: DirReceiveOnly})
	q.push(Event{Kind: EventPipeFailed, Pipe: 4, Direction: DirSendOnly})
	q.mark(6, dirInSeen) // inbound only, its end never queued
	q.q = nil            // read
	q.retire(2, false)
	q.retire(4, true) // its failure was read
	q.retire(6, false)
	sum := q.q[0]
	if len(q.q) != 1 || sum.Retired != 3 || sum.Closed != 2 || sum.Failed != 0 {
		t.Fatalf("summary %+v", q.q)
	}
	if len(q.dirs) != 0 || len(q.seenOut) != 0 {
		t.Fatalf("state of retired pipes kept: %v %v", q.dirs, q.seenOut)
	}
}

func TestJoinAuthReasonNames(t *testing.T) {
	for _, c := range []struct {
		r    JoinAuthReason
		want string
	}{{JoinAuthReasonMAC, "mac"}, {JoinAuthReasonCarrierBound, "mac_carrier_bound"}} {
		if got := c.r.String(); got != c.want {
			t.Errorf("%d: %q, want %q", c.r, got, c.want)
		}
	}
}
