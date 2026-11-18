package zmux

import (
	"bytes"
	"context"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// drainKinds reads every unread event of q.
func drainEvents(q *eventQueue) []Event {
	var out []Event
	for {
		ev, ok := q.pop()
		if !ok {
			return out
		}
		out = append(out, ev)
	}
}

// snapshot returns q's unread events in reading order without reading them.
func (e *eventQueue) snapshot() []Event {
	var out []Event
	for n := e.head; n != nil; n = n.next {
		out = append(out, n.ev)
	}
	return out
}

func kindsOf(evs []Event) []EventKind {
	k := make([]EventKind, len(evs))
	for i, ev := range evs {
		k[i] = ev.Kind
	}
	return k
}

// TestEventQueueRetirement checks the one report of retirement: a retired
// pipe's unread events leave the queue for one EventPipesRetired, which
// accumulates until it is read, counts the failures and ends not yet read
// and keeps the last four IDs; a pipe whose end was read counts as retired
// only.
func TestEventQueueRetirement(t *testing.T) {
	var q eventQueue
	q.push(Event{Kind: EventPipeAdded, PipeID: 2})
	q.push(Event{Kind: EventPipeFailed, PipeID: 2})
	q.push(Event{Kind: EventSessionResumed})
	q.push(Event{Kind: EventPipeClosed, PipeID: 4})
	q.retire(2, true)  // its failure unread
	q.retire(4, false) // its end unread
	evs := q.snapshot()
	if len(evs) != 2 || evs[0].Kind != EventSessionResumed || evs[1].Kind != EventPipesRetired {
		t.Fatalf("queue %+v", evs)
	}
	sum := evs[1]
	if sum.Retired != 2 || sum.Failed != 1 || sum.Closed != 1 || !slices.Equal(sum.Last, []PipeID{2, 4}) {
		t.Fatalf("summary %+v", sum)
	}
	drainEvents(&q) // read
	// A pipe whose terminal event was read: retired only.
	q.push(Event{Kind: EventPipeClosed, PipeID: 6})
	drainEvents(&q)
	q.retire(6, false)
	for id := uint64(8); id <= 14; id += 2 {
		q.retire(id, false) // no terminal event was ever queued: an unread end
	}
	evs = q.snapshot()
	if len(evs) != 1 {
		t.Fatalf("one summary expected: %+v", evs)
	}
	sum = evs[0]
	if sum.Retired != 5 || sum.Closed != 4 || sum.Failed != 0 || !slices.Equal(sum.Last, []PipeID{8, 10, 12, 14}) {
		t.Fatalf("summary %+v", sum)
	}
}

// Every event of a pipe, of both directions, leaves the queue with the
// pipe: the keys retire removes are those push keys by.
func TestEventQueueRetireRemovesEveryPipeKey(t *testing.T) {
	var q eventQueue
	for _, p := range pipeKeys {
		for _, dir := range []Direction{DirectionSendOnly, DirectionReceiveOnly} {
			q.push(Event{Kind: p.kind, PipeID: 6, Direction: dir})
			q.push(Event{Kind: p.kind, PipeID: 8, Direction: dir}) // another pipe's stay
		}
	}
	q.retire(6, false)
	for _, ev := range q.snapshot() {
		if ev.Kind != EventPipesRetired && ev.PipeID == 6 {
			t.Fatalf("event %+v of the retired pipe stayed", ev)
		}
	}
	other := 0
	for _, ev := range q.snapshot() {
		if ev.PipeID == 8 {
			other++
		}
	}
	// The pipe keyed without a direction once, the others once per direction.
	want := 0
	for _, p := range pipeKeys {
		want++
		if p.dir {
			want++
		}
	}
	if other != want || len(q.idx) != want+1 {
		t.Fatalf("%d events of the other pipe, want %d (%d keys)", other, want, len(q.idx))
	}
}

// The queue counts a retired pipe only when none of its terminal events
// was read, by the pipe's own failure flag: never queued counts by the
// flag, a terminal read leaves the pipe in neither whichever direction
// and whichever event stays unread.
func TestEventQueueRetireCountsUnreadEnd(t *testing.T) {
	type tc struct {
		name           string
		setup          func(q *eventQueue)
		failed         bool
		failedN, clsdN uint64
	}
	read := func(q *eventQueue) { drainEvents(q) }
	for _, c := range []tc{
		{"never queued, closed", func(*eventQueue) {}, false, 0, 1},
		{"never queued, failed", func(*eventQueue) {}, true, 1, 0},
		{"outbound failed unread", func(q *eventQueue) {
			q.push(Event{Kind: EventPipeFailed, PipeID: 2, Direction: DirectionSendOnly})
		}, true, 1, 0},
		{"inbound closed unread, flag failed", func(q *eventQueue) {
			q.push(Event{Kind: EventPipeClosed, PipeID: 2, Direction: DirectionReceiveOnly})
		}, true, 1, 0},
		{"terminal read", func(q *eventQueue) {
			q.push(Event{Kind: EventPipeClosed, PipeID: 2, Direction: DirectionReceiveOnly})
			read(q)
		}, false, 0, 0},
		{"outbound failed read, inbound end never queued", func(q *eventQueue) {
			q.push(Event{Kind: EventPipeFailed, PipeID: 2, Direction: DirectionSendOnly})
			read(q)
		}, true, 0, 0},
		{"outbound failed read, inbound end unread", func(q *eventQueue) {
			q.push(Event{Kind: EventPipeFailed, PipeID: 2, Direction: DirectionSendOnly})
			read(q)
			q.push(Event{Kind: EventPipeClosed, PipeID: 2, Direction: DirectionReceiveOnly})
		}, true, 0, 0},
		{"inbound read, outbound unread", func(q *eventQueue) {
			q.push(Event{Kind: EventPipeClosed, PipeID: 2, Direction: DirectionReceiveOnly})
			read(q)
			q.push(Event{Kind: EventPipeFailed, PipeID: 2, Direction: DirectionSendOnly})
		}, false, 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			var q eventQueue
			c.setup(&q)
			q.fresh = nil
			q.retire(2, c.failed)
			sum := q.snapshot()
			if len(sum) != 1 || sum[0].Retired != 1 || sum[0].Failed != c.failedN || sum[0].Closed != c.clsdN {
				t.Fatalf("summary %+v, want failed %d closed %d", sum, c.failedN, c.clsdN)
			}
			// The tracer counts the outcome whatever was read.
			if len(q.fresh) != 1 || (q.fresh[0].Failed == 1) == (q.fresh[0].Closed == 1) || (q.fresh[0].Failed == 1) != c.failed {
				t.Fatalf("tracer saw %+v", q.fresh)
			}
		})
	}
}

// The tracer gets one PipesRetired per retired pipe, counting its outcome
// whether or not it was read.
func TestEventQueueTracerRetirement(t *testing.T) {
	var q eventQueue
	q.push(Event{Kind: EventPipeFailed, PipeID: 2})
	drainEvents(&q) // read: the queue counts neither
	q.fresh = nil
	q.retire(2, true)
	q.retire(4, false)
	want := []Event{
		{Kind: EventPipesRetired, Retired: 1, Failed: 1, Last: []PipeID{2}},
		{Kind: EventPipesRetired, Retired: 1, Closed: 1, Last: []PipeID{4}},
	}
	if !reflect.DeepEqual(q.fresh, want) {
		t.Fatalf("tracer saw %+v", q.fresh)
	}
	if sum := q.snapshot()[0]; sum.Retired != 2 || sum.Failed != 0 || sum.Closed != 1 {
		t.Fatalf("summary %+v", sum)
	}
}

// Each kind of the event key table: what the queue holds after
// two occurrences of one key, and after one of another.
func TestEventQueueKeyTable(t *testing.T) {
	for _, c := range []struct {
		name         string
		a, b, other  Event
		wantFirst    Event // the one unread event of the key
		sameKeyCount int
	}{
		{"PipeAdded kept", Event{Kind: EventPipeAdded, PipeID: 2, Variant: "a"}, Event{Kind: EventPipeAdded, PipeID: 2, Variant: "b"},
			Event{Kind: EventPipeAdded, PipeID: 4}, Event{Kind: EventPipeAdded, PipeID: 2, Variant: "a"}, 1},
		{"PipeJoined kept", Event{Kind: EventPipeJoined, PipeID: 2, Variant: "a"}, Event{Kind: EventPipeJoined, PipeID: 2, Variant: "b"},
			Event{Kind: EventPipeJoined, PipeID: 4}, Event{Kind: EventPipeJoined, PipeID: 2, Variant: "a"}, 1},
		{"PipeSuspect latest", Event{Kind: EventPipeSuspect, PipeID: 2, Direction: DirectionSendOnly, Variant: "a"},
			Event{Kind: EventPipeSuspect, PipeID: 2, Direction: DirectionSendOnly, Variant: "b"},
			Event{Kind: EventPipeSuspect, PipeID: 2, Direction: DirectionReceiveOnly},
			Event{Kind: EventPipeSuspect, PipeID: 2, Direction: DirectionSendOnly, Variant: "b"}, 1},
		{"PipeRecovered latest", Event{Kind: EventPipeRecovered, PipeID: 2, Direction: DirectionSendOnly, Variant: "a"},
			Event{Kind: EventPipeRecovered, PipeID: 2, Direction: DirectionSendOnly, Variant: "b"},
			Event{Kind: EventPipeRecovered, PipeID: 4, Direction: DirectionSendOnly},
			Event{Kind: EventPipeRecovered, PipeID: 2, Direction: DirectionSendOnly, Variant: "b"}, 1},
		{"PipeFailed kept", Event{Kind: EventPipeFailed, PipeID: 2, Direction: DirectionSendOnly, Variant: "a"},
			Event{Kind: EventPipeFailed, PipeID: 2, Direction: DirectionSendOnly, Variant: "b"},
			Event{Kind: EventPipeFailed, PipeID: 2, Direction: DirectionReceiveOnly},
			Event{Kind: EventPipeFailed, PipeID: 2, Direction: DirectionSendOnly, Variant: "a"}, 1},
		{"PipeClosed kept", Event{Kind: EventPipeClosed, PipeID: 2, Direction: DirectionReceiveOnly, Variant: "a"},
			Event{Kind: EventPipeClosed, PipeID: 2, Direction: DirectionReceiveOnly, Variant: "b"},
			Event{Kind: EventPipeClosed, PipeID: 2, Direction: DirectionSendOnly},
			Event{Kind: EventPipeClosed, PipeID: 2, Direction: DirectionReceiveOnly, Variant: "a"}, 1},
		{"PipePriorityChanged latest", Event{Kind: EventPipePriorityChanged, PipeID: 2, Priority: 1, Weight: 5},
			Event{Kind: EventPipePriorityChanged, PipeID: 2, Priority: 3, Weight: 7, Standby: true},
			Event{Kind: EventPipePriorityChanged, PipeID: 4},
			Event{Kind: EventPipePriorityChanged, PipeID: 2, Priority: 3, Weight: 7, Standby: true}, 1},
		{"PipeQualityChanged latest", Event{Kind: EventPipeQualityChanged, PipeID: 2, DeliveryRate: 1},
			Event{Kind: EventPipeQualityChanged, PipeID: 2, DeliveryRate: 9, SmoothedRTT: time.Second},
			Event{Kind: EventPipeQualityChanged, PipeID: 4},
			Event{Kind: EventPipeQualityChanged, PipeID: 2, DeliveryRate: 9, SmoothedRTT: time.Second}, 1},
		{"CapacityBelow latest", Event{Kind: EventCapacityBelow, DeliveryRate: 5}, Event{Kind: EventCapacityBelow, DeliveryRate: 3},
			Event{Kind: EventCapacityRestored}, Event{Kind: EventCapacityBelow, DeliveryRate: 3}, 1},
		{"CapacityRestored latest", Event{Kind: EventCapacityRestored, DeliveryRate: 5}, Event{Kind: EventCapacityRestored, DeliveryRate: 8},
			Event{Kind: EventCapacityBelow}, Event{Kind: EventCapacityRestored, DeliveryRate: 8}, 1},
		{"TierSaturation latest", Event{Kind: EventTierSaturation, Priority: 1, Saturated: true},
			Event{Kind: EventTierSaturation, Priority: 1},
			Event{Kind: EventTierSaturation, Priority: 2, Saturated: true}, Event{Kind: EventTierSaturation, Priority: 1}, 1},
		{"JoinAuthFailed count", Event{Kind: EventJoinAuthFailed, Reason: JoinAuthReasonAuth, Count: 1},
			Event{Kind: EventJoinAuthFailed, Reason: JoinAuthReasonAuth, Count: 1},
			Event{Kind: EventJoinAuthFailed, Reason: JoinAuthReasonKex, Count: 1},
			Event{Kind: EventJoinAuthFailed, Reason: JoinAuthReasonAuth, Count: 2}, 1},
		{"EpochCommitted latest", Event{Kind: EventEpochCommitted, Epoch: 1}, Event{Kind: EventEpochCommitted, Epoch: 2},
			Event{Kind: EventEpochDeleted, Epoch: 1}, Event{Kind: EventEpochCommitted, Epoch: 2}, 1},
		{"EpochDeleted latest", Event{Kind: EventEpochDeleted, Epoch: 1}, Event{Kind: EventEpochDeleted, Epoch: 2},
			Event{Kind: EventEpochCommitted, Epoch: 1}, Event{Kind: EventEpochDeleted, Epoch: 2}, 1},
		{"CarrierCandidate latest", Event{Kind: EventCarrierCandidate, Carrier: CarrierWebSocket, Variant: "v", Outcome: 1},
			Event{Kind: EventCarrierCandidate, Carrier: CarrierWebSocket, Variant: "v", Outcome: 2},
			Event{Kind: EventCarrierCandidate, Carrier: CarrierWebSocket, Variant: "w"},
			Event{Kind: EventCarrierCandidate, Carrier: CarrierWebSocket, Variant: "v", Outcome: 2}, 1},
		{"CarrierChosen latest", Event{Kind: EventCarrierChosen, Carrier: CarrierWebSocket}, Event{Kind: EventCarrierChosen, Carrier: CarrierQUIC},
			Event{Kind: EventCarrierCandidate}, Event{Kind: EventCarrierChosen, Carrier: CarrierQUIC}, 1},
		{"SessionSuspended latest", Event{Kind: EventSessionSuspended}, Event{Kind: EventSessionSuspended},
			Event{Kind: EventSessionResumed}, Event{Kind: EventSessionSuspended}, 1},
		{"SessionResumed latest", Event{Kind: EventSessionResumed}, Event{Kind: EventSessionResumed},
			Event{Kind: EventSessionSuspended}, Event{Kind: EventSessionResumed}, 1},
		{"ResumeUnattended latest", Event{Kind: EventResumeUnattended}, Event{Kind: EventResumeUnattended},
			Event{Kind: EventSessionSuspended}, Event{Kind: EventResumeUnattended}, 1},
		{"GoAwayReceived latest", Event{Kind: EventGoAwayReceived, GoAway: &GoAway{MaxBidi: 8}},
			Event{Kind: EventGoAwayReceived, GoAway: &GoAway{MaxBidi: 4, ErrorCode: 2}},
			Event{Kind: EventSessionSuspended}, Event{Kind: EventGoAwayReceived, GoAway: &GoAway{MaxBidi: 4, ErrorCode: 2}}, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			var q eventQueue
			q.push(c.a)
			q.push(c.other)
			q.push(c.b)
			evs := q.snapshot()
			if len(evs) != 2 {
				t.Fatalf("%d unread events, want one per key: %+v", len(evs), evs)
			}
			// Replacing moves the key to the tail; keeping leaves it.
			first, second := evs[0], evs[1]
			if keepFirst(c.a.Kind) {
				if !reflect.DeepEqual(first, c.wantFirst) || !reflect.DeepEqual(second, c.other) {
					t.Fatalf("kept: %+v", evs)
				}
			} else if !reflect.DeepEqual(first, c.other) || !reflect.DeepEqual(second, c.wantFirst) {
				t.Fatalf("moved to the tail: %+v", evs)
			}
		})
	}
}

// Events of distinct keys are read in the order of their latest
// occurrences; an update moves its key to the tail.
func TestEventQueueOrder(t *testing.T) {
	var q eventQueue
	q.push(Event{Kind: EventSessionSuspended})
	q.push(Event{Kind: EventPipeAdded, PipeID: 2, Direction: DirectionSendOnly})
	q.push(Event{Kind: EventSessionResumed})
	if got := kindsOf(q.snapshot()); !slices.Equal(got, []EventKind{EventSessionSuspended, EventPipeAdded, EventSessionResumed}) {
		t.Fatalf("order %v", got)
	}
	q.push(Event{Kind: EventSessionSuspended}) // the latest occurrence is now the last
	if got := kindsOf(q.snapshot()); !slices.Equal(got, []EventKind{EventPipeAdded, EventSessionResumed, EventSessionSuspended}) {
		t.Fatalf("order %v", got)
	}
	// A kept key does not move.
	q.push(Event{Kind: EventPipeAdded, PipeID: 2, Direction: DirectionSendOnly})
	if got := kindsOf(q.snapshot()); !slices.Equal(got, []EventKind{EventPipeAdded, EventSessionResumed, EventSessionSuspended}) {
		t.Fatalf("order %v", got)
	}
	// The retirement summary moves to the tail at each retirement.
	q.retire(2, false)
	q.push(Event{Kind: EventCapacityBelow})
	q.retire(4, false)
	if got := kindsOf(q.snapshot()); !slices.Equal(got, []EventKind{EventSessionResumed, EventSessionSuspended, EventCapacityBelow, EventPipesRetired}) {
		t.Fatalf("order %v", got)
	}
	if q.snapshot()[3].Retired != 2 {
		t.Fatalf("summary %+v", q.snapshot()[3])
	}
}

// A SessionResumed removes the unread ResumeUnattended; a read one stays read.
func TestEventQueueResumeRemovesUnattended(t *testing.T) {
	var q eventQueue
	q.push(Event{Kind: EventSessionSuspended})
	q.push(Event{Kind: EventResumeUnattended})
	q.push(Event{Kind: EventSessionResumed})
	if got := kindsOf(q.snapshot()); !slices.Equal(got, []EventKind{EventSessionSuspended, EventSessionResumed}) {
		t.Fatalf("queue %v", got)
	}
	q.pop() // Suspended read
	q.push(Event{Kind: EventSessionSuspended})
	q.push(Event{Kind: EventResumeUnattended})
	q.pop() // Resumed read
	q.pop() // Suspended read
	if ev, _ := q.pop(); ev.Kind != EventResumeUnattended {
		t.Fatalf("unattended lost: %+v", ev)
	}
	q.push(Event{Kind: EventSessionResumed}) // nothing unread to remove
	if got := kindsOf(q.snapshot()); !slices.Equal(got, []EventKind{EventSessionResumed}) {
		t.Fatalf("queue %v", got)
	}
	// The tracer sees every occurrence.
	if n := len(q.fresh); n != 6 {
		t.Fatalf("%d occurrences traced", n)
	}
}

// Once frozen (the session is CLOSED) nothing enters the queue or the
// tracer's view, retirements included; what is queued is still read.
func TestEventQueueFrozen(t *testing.T) {
	var q eventQueue
	q.push(Event{Kind: EventPipeClosed, PipeID: 2, Direction: DirectionSendOnly})
	q.push(Event{Kind: EventCapacityBelow})
	q.fresh = q.fresh[:0]
	q.freeze()
	q.push(Event{Kind: EventSessionSuspended})
	q.push(Event{Kind: EventJoinAuthFailed, Reason: JoinAuthReasonAuth})
	q.retire(2, false)
	if len(q.fresh) != 0 {
		t.Fatalf("traced after the end: %+v", q.fresh)
	}
	if got := kindsOf(q.snapshot()); !slices.Equal(got, []EventKind{EventPipeClosed, EventCapacityBelow}) {
		t.Fatalf("queue %v", got)
	}
	if got := kindsOf(drainEvents(&q)); len(got) != 2 {
		t.Fatalf("read %v", got)
	}
}

// A hostile peer's failed Hellos are one event per reason: counted, never
// displacing others, however many arrive.
func TestEventQueueJoinAuthFailedBounded(t *testing.T) {
	var q eventQueue
	q.push(Event{Kind: EventPipeAdded, PipeID: 2, Direction: DirectionSendOnly})
	q.push(Event{Kind: EventSessionSuspended})
	reasons := []JoinAuthReason{JoinAuthReasonAuth, JoinAuthReasonKex, JoinAuthReasonBadField}
	const rounds = 100000
	for i := range rounds {
		q.push(Event{Kind: EventJoinAuthFailed, Reason: reasons[i%len(reasons)], Count: 1})
		q.fresh = q.fresh[:0] // the tracer's backlog is its own bound
	}
	evs := q.snapshot()
	if len(evs) != 2+len(reasons) {
		t.Fatalf("%d events", len(evs))
	}
	total := uint64(0)
	for _, ev := range evs {
		if ev.Kind == EventJoinAuthFailed {
			total += ev.Count
		}
	}
	if total != rounds {
		t.Fatalf("counted %d of %d", total, rounds)
	}
	if evs[0].Kind != EventPipeAdded || evs[1].Kind != EventSessionSuspended {
		t.Fatalf("other events displaced: %v", kindsOf(evs))
	}
}

// Model test: the queue against a reference of the rule (one unread event
// per key, order of the latest occurrence), over random sequences of
// occurrences, reads and retirements.
func TestEventQueueModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	kinds := []EventKind{EventPipeAdded, EventPipeJoined, EventPipeSuspect, EventPipeRecovered, EventPipePriorityChanged,
		EventTierSaturation, EventCapacityBelow, EventSessionSuspended, EventSessionResumed, EventCarrierChosen, EventGoAwayReceived}
	var q eventQueue
	var model []Event // by latest occurrence; merged like the queue
	same := func(a, b *Event) bool { return keyOf(a) == keyOf(b) }
	for step := range 20000 {
		switch r := rng.IntN(10); {
		case r < 7:
			ev := Event{Kind: kinds[rng.IntN(len(kinds))], PipeID: PipeID(2 * (1 + rng.IntN(4))), Priority: uint64(rng.IntN(3)),
				Direction: Direction(1 + rng.IntN(2)), Weight: uint64(step)}
			q.push(ev)
			if ev.Kind == EventSessionResumed {
				model = slices.DeleteFunc(model, func(x Event) bool { return x.Kind == EventResumeUnattended })
			}
			if i := slices.IndexFunc(model, func(x Event) bool { return same(&x, &ev) }); i >= 0 {
				if keepFirst(ev.Kind) {
					continue
				}
				model = slices.Delete(model, i, i+1)
			}
			model = append(model, ev)
		case r < 9:
			got, ok := q.pop()
			if ok != (len(model) > 0) {
				t.Fatalf("step %d: pop %v with %d modelled", step, ok, len(model))
			}
			if ok {
				if got.Kind == EventPipesRetired { // the outcome counts have their own tests
					got.Failed, got.Closed, got.Last = 0, 0, nil
				}
				if !reflect.DeepEqual(got, model[0]) {
					t.Fatalf("step %d: read %+v, want %+v", step, got, model[0])
				}
				model = model[1:]
			}
		default:
			id := uint64(2 * (1 + rng.IntN(4)))
			q.retire(id, false)
			model = slices.DeleteFunc(model, func(x Event) bool { return pipeEvent(x.Kind) && uint64(x.PipeID) == id })
			sum := Event{Kind: EventPipesRetired}
			if i := slices.IndexFunc(model, func(x Event) bool { return x.Kind == EventPipesRetired }); i >= 0 {
				sum = model[i]
				model = slices.Delete(model, i, i+1)
			}
			sum.Retired++
			model = append(model, sum)
		}
		if q.len() != len(q.snapshot()) {
			t.Fatalf("step %d: index %d, list %d", step, q.len(), len(q.snapshot()))
		}
	}
}

func TestJoinAuthReasonNames(t *testing.T) {
	for _, c := range []struct {
		r    JoinAuthReason
		want string
	}{{JoinAuthReasonAuth, "auth"}, {JoinAuthReasonMissingField, "missing_field"}, {JoinAuthReasonBadField, "bad_field"},
		{JoinAuthReasonTLVTruncated, "tlv_truncated"}, {JoinAuthReasonTLVNonMinimal, "tlv_non_minimal"},
		{JoinAuthReasonTLVOrder, "tlv_order"},
		{JoinAuthReasonTLVUnknownCritical, "tlv_unknown_critical"}, {JoinAuthReasonKex, "kex"}} {
		if got := c.r.String(); got != c.want {
			t.Errorf("%d: %q, want %q", c.r, got, c.want)
		}
	}
}

// A peer that keeps failing joins cannot displace the session's other
// events nor grow its queue: the failures are one event with their count
// (the tracer sees each one), and the events before them are still read, in
// order.
func TestJoinAuthFailedHostilePeer(t *testing.T) {
	leakcheck.Check(t)
	var mu sync.Mutex
	var traced []Event
	scfg := deferredConfig()
	scfg.Tracer = &Tracer{Event: func(_ SessionID, ev Event) {
		if ev.Kind == EventJoinAuthFailed {
			mu.Lock()
			traced = append(traced, ev)
			mu.Unlock()
		}
	}}
	e := newMPEnv(t, deferredConfig(), scfg)
	mustSet(t, e.client, joinSecret)
	mustSet(t, e.server, bytes.Repeat([]byte{8}, 32))
	var before []EventKind
	e.server.locked(func() { before = kindsOf(e.server.events.snapshot()) })
	const attempts = 40
	for i := range attempts {
		if err := e.tryPipe(5 * time.Second); err == nil {
			t.Fatal("join with a different secret succeeded")
		}
		waitUntil(t, 5*time.Second, "auth failure", func() bool {
			return e.server.ConnectionStats().JoinAuthFailures == uint64(i+1)
		})
	}
	waitUntil(t, 5*time.Second, "the tracer saw every failure", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(traced) == attempts
	})
	mu.Lock()
	for _, ev := range traced {
		if ev.Count != 1 || ev.Reason != JoinAuthReasonAuth {
			t.Fatalf("tracer event %+v", ev)
		}
	}
	mu.Unlock()
	var evs []Event
	e.server.locked(func() { evs = e.server.events.snapshot() })
	if len(evs) != len(before)+1 || !slices.Equal(kindsOf(evs[:len(before)]), before) {
		t.Fatalf("events before %v, after %v", before, kindsOf(evs))
	}
	last := evs[len(evs)-1]
	if last.Kind != EventJoinAuthFailed || last.Reason != JoinAuthReasonAuth || last.Count != attempts {
		t.Fatalf("last event %+v", last)
	}
	// What NextEvent reads is the same.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range evs[:len(evs)-1] {
		if _, err := e.server.NextEvent(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if ev, err := e.server.NextEvent(ctx); err != nil || ev.Count != attempts {
		t.Fatalf("%+v, %v", ev, err)
	}
}

// After the session ended nothing more enters the queue: the events read
// are those of before the end, then the session's error.
func TestEventQueueFrozenAtEnd(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	p := e.addPipe(t, PipeOptions{})
	e.client.Abort(1, "end")
	<-e.client.Done()
	var n int
	e.client.locked(func() {
		if !e.client.events.frozen {
			t.Error("the queue is not frozen at the end")
		}
		n = e.client.events.len()
	})
	_ = p.Abort()
	waitUntil(t, 5*time.Second, "the pipe ended", func() bool { return isDone(p.Done()) })
	e.client.locked(func() {
		if e.client.events.len() != n {
			t.Errorf("the queue changed after the end: %d, was %d", e.client.events.len(), n)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range n {
		if _, err := e.client.NextEvent(ctx); err != nil {
			t.Fatalf("a queued event: %v", err)
		}
	}
	if _, err := e.client.NextEvent(ctx); err == nil || ctx.Err() != nil {
		t.Fatalf("after the events: %v", err)
	}
}

func isDone(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
