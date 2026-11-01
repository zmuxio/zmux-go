package zmux

import (
	"context"
	"slices"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// EventKind is the kind of a session event. The comment of each kind
// names the Event fields it sets; the other fields are zero.
type EventKind uint8

const (
	// EventPipeAdded reports that a local outbound pipe direction appeared
	// (Pipe, Direction).
	EventPipeAdded EventKind = iota + 1
	// EventPipeJoined reports that an outbound pipe direction became active
	// (Pipe, Direction).
	EventPipeJoined
	// EventPipeSuspect reports that an outbound pipe direction stopped making
	// progress (Pipe, Direction).
	EventPipeSuspect
	// EventPipeRecovered reports that a suspect outbound direction is active
	// again (Pipe, Direction).
	EventPipeRecovered
	// EventPipeFailed reports that an outbound pipe direction failed (Pipe,
	// Direction).
	EventPipeFailed
	// EventPipeClosed reports that a pipe direction ended: the outbound
	// one closed (DirSendOnly) or the inbound one ended (DirReceiveOnly)
	// (Pipe, Direction). Retirement is reported by EventPipesRetired.
	EventPipeClosed
	// EventPipePriorityChanged reports that the effective tier, standby state
	// or weight of an outbound pipe changed (Pipe, Priority, Standby, Weight).
	EventPipePriorityChanged
	// EventTierSaturation reports that the usable pipes of a tier became
	// saturated or stopped being so (Priority, Saturated).
	EventTierSaturation
	// EventJoinAuthFailed reports that a join failed authentication
	// (Reason).
	EventJoinAuthFailed
	// EventSessionSuspended reports that every pipe of a direction was lost and
	// the session waits for a resume (no fields).
	EventSessionSuspended
	// EventSessionResumed reports that a suspended session has a pipe again (no
	// fields).
	EventSessionResumed
	// EventGoAwayReceived reports that the peer sent GOAWAY (GoAway).
	EventGoAwayReceived
	// EventJoinSecurityUnavailable reports that both endpoints offered
	// multipath but no join secret is available, so no pipe can join (no
	// fields).
	EventJoinSecurityUnavailable
	// EventPipeQualityChanged reports that an outbound pipe left its
	// QualityBand (Pipe, SmoothedRTT, DeliveryRate).
	EventPipeQualityChanged
	// EventCapacityBelow reports that the outbound capacity fell below the
	// CapacityFloor (DeliveryRate: the capacity).
	EventCapacityBelow
	// EventCapacityRestored reports that the outbound capacity is back above
	// the CapacityFloor (DeliveryRate: the capacity).
	EventCapacityRestored
	// EventCarrierCandidate reports a try of a candidate of DialMode
	// (Carrier, Outcome).
	EventCarrierCandidate
	// EventCarrierChosen reports the carrier kind DialMode chose, and every
	// later switch to another kind (Carrier).
	EventCarrierChosen
	// EventPipesRetired is the one report of retirement (Retired, Failed,
	// Closed, Dropped, Last): it enters the queue at the first retirement
	// after the previous one was read and accumulates until it is read, the
	// unread events of each retired pipe leaving the queue for it.
	EventPipesRetired
)

var pipeEventNames = [...]string{"", "PipeAdded", "PipeJoined", "PipeSuspect", "PipeRecovered", "PipeFailed",
	"PipeClosed", "PipePriorityChanged", "TierSaturation", "JoinAuthFailed", "SessionSuspended",
	"SessionResumed", "GoAwayReceived", "JoinSecurityUnavailable", "PipeQualityChanged", "CapacityBelow",
	"CapacityRestored", "CarrierCandidate", "CarrierChosen", "PipesRetired"}

func (k EventKind) String() string {
	if int(k) < len(pipeEventNames) {
		return pipeEventNames[k]
	}
	return "PipeEventKind(?)"
}

// JoinAuthReason says which kind of join token failed verification.
type JoinAuthReason uint8

const (
	JoinAuthReasonMAC          = JoinAuthReason(core.JoinAuthMAC)             // a token without carrier binding
	JoinAuthReasonCarrierBound = JoinAuthReason(core.JoinAuthMACCarrierBound) // a carrier-bound token
)

// String returns "mac" or "mac_carrier_bound", the names of API.md.
func (r JoinAuthReason) String() string { return core.JoinAuthReason(r).String() }

// Event is a session event, as NextEvent and Tracer.Event report it. Kind
// says which of the other fields are set (see EventKind).
type Event struct {
	Kind EventKind
	// Direction of a pipe event, from the local endpoint.
	Direction Direction
	// Standby (EventPipePriorityChanged) and Saturated
	// (EventTierSaturation).
	Standby   bool
	Saturated bool
	// Reason of EventJoinAuthFailed.
	Reason JoinAuthReason
	// Carrier is the pipe's carrier kind (PipeIO.Carrier) of a pipe event,
	// the candidate's of EventCarrierCandidate and EventCarrierChosen;
	// Outcome that of EventCarrierCandidate.
	Carrier CarrierKind
	Outcome CandidateOutcome
	// Pipe is the pipe ID of a pipe event.
	Pipe uint64
	// Priority is the tier (EventPipePriorityChanged,
	// EventTierSaturation); Weight the pipe's weight
	// (EventPipePriorityChanged).
	Priority uint64
	Weight   uint64
	// GoAway of EventGoAwayReceived.
	GoAway *GoAway
	// SmoothedRTT and DeliveryRate (bytes per second) of
	// EventPipeQualityChanged;
	// DeliveryRate is also the capacity of EventCapacityBelow and
	// EventCapacityRestored.
	SmoothedRTT  time.Duration
	DeliveryRate float64
	// Retired counts the pipes retired since EventPipesRetired was last
	// read; Failed and Closed, of those, the ones whose failure or end
	// without failure had not been read; Dropped the non-terminal events
	// the queue's bound dropped meanwhile; Last the IDs of the last
	// retired pipes, at most four, oldest first. An application that
	// needs every retired ID compares Pipes with the handles it holds.
	Retired, Failed, Closed, Dropped uint64
	Last                             []uint64
}

// retiredLast is the number of IDs EventPipesRetired keeps.
const retiredLast = 4

// pipeEvent reports whether events of kind k belong to a pipe (Pipe set).
func pipeEvent(k EventKind) bool {
	switch k {
	case EventPipeAdded, EventPipeJoined, EventPipeSuspect, EventPipeRecovered, EventPipeFailed, EventPipeClosed,
		EventPipePriorityChanged, EventPipeQualityChanged:
		return true
	case EventTierSaturation, EventJoinAuthFailed, EventSessionSuspended, EventSessionResumed,
		EventGoAwayReceived, EventJoinSecurityUnavailable, EventCapacityBelow, EventCapacityRestored,
		EventCarrierCandidate, EventCarrierChosen, EventPipesRetired:
		return false
	}
	return false
}

// terminal reports a pipe's terminal event, which the queue's bound keeps.
func terminal(k EventKind) bool { return k == EventPipeFailed || k == EventPipeClosed }

// eventQueue holds session events for NextEvent; it is guarded by the
// session lock. It is bounded (eventQueueMax): when the application does
// not read events, the oldest non-terminal ones are dropped and counted;
// a retired pipe's unread events are folded into one EventPipesRetired
// (IMPLEMENTATION.md 2.5.7), so the queue stays bounded however many pipes
// a session goes through.
type eventQueue struct {
	q       []Event
	waiters []*core.Signal
	seenOut map[uint64]core.OutState
	dirs    map[uint64]uint8 // per pipe: dirInSeen, dirInEnded, dirOutEnded
	dropped uint64           // non-terminal events dropped and not yet in a summary
	fresh   []Event          // events added in this drain, for the tracer
}

const eventQueueMax = 1024

// Bits of eventQueue.dirs: an inbound direction was reported, and the
// terminal event of a direction entered the queue.
const (
	dirInSeen uint8 = 1 << iota
	dirInEnded
	dirOutEnded
)

// keptEvent reports the events the queue's bound never drops: terminal
// events and the retirement summary.
func keptEvent(k EventKind) bool { return terminal(k) || k == EventPipesRetired }

func (e *eventQueue) push(ev Event) {
	if len(e.q) >= eventQueueMax {
		i := slices.IndexFunc(e.q, func(x Event) bool { return !keptEvent(x.Kind) })
		if i < 0 && !keptEvent(ev.Kind) {
			// Only kept events are queued: the new one is dropped. Kept
			// events stay bounded, as retirement folds a pipe's away.
			e.drop()
			return
		}
		if i >= 0 {
			e.q = slices.Delete(e.q, i, i+1)
			e.drop()
		}
	}
	if terminal(ev.Kind) {
		bit := dirOutEnded
		if ev.Direction == DirReceiveOnly {
			bit = dirInEnded
		}
		e.mark(ev.Pipe, bit)
	}
	e.q = append(e.q, ev)
	e.fresh = append(e.fresh, ev)
}

// drop counts a dropped event, in the unread summary when there is one.
func (e *eventQueue) drop() {
	if i := slices.IndexFunc(e.q, func(x Event) bool { return x.Kind == EventPipesRetired }); i >= 0 {
		e.q[i].Dropped++
		return
	}
	e.dropped++
}

// mark sets bits of a pipe's dirs.
func (e *eventQueue) mark(id uint64, bits uint8) {
	if e.dirs == nil {
		e.dirs = map[uint64]uint8{}
	}
	e.dirs[id] |= bits
}

// retire folds the unread events of a retired pipe into the queue's
// EventPipesRetired, adding one when none is unread, and returns the
// retirement alone for a tracer.
func (e *eventQueue) retire(id uint64, failed bool) Event {
	// A terminal state that never entered the queue was not read: that of
	// a reported direction without its terminal event, or any when nothing
	// terminal of the pipe was ever queued.
	d := e.dirs[id]
	_, outSeen := e.seenOut[id]
	unreadEnd := outSeen && d&dirOutEnded == 0 || d&dirInSeen != 0 && d&dirInEnded == 0 ||
		d&(dirInEnded|dirOutEnded) == 0
	e.q = slices.DeleteFunc(e.q, func(x Event) bool {
		if x.Pipe != id || !pipeEvent(x.Kind) {
			return false
		}
		unreadEnd = unreadEnd || terminal(x.Kind)
		return true
	})
	delete(e.dirs, id)
	delete(e.seenOut, id)
	one := Event{Kind: EventPipesRetired, Retired: 1, Last: []uint64{id}}
	if unreadEnd && failed {
		one.Failed = 1
	} else if unreadEnd {
		one.Closed = 1
	}
	i := slices.IndexFunc(e.q, func(x Event) bool { return x.Kind == EventPipesRetired })
	if i < 0 {
		e.q = append(e.q, Event{Kind: EventPipesRetired})
		i = len(e.q) - 1
	}
	sum := &e.q[i]
	sum.Retired++
	sum.Failed += one.Failed
	sum.Closed += one.Closed
	sum.Dropped += e.dropped
	e.dropped = 0
	sum.Last = append(sum.Last, id)
	if n := len(sum.Last); n > retiredLast {
		sum.Last = append(sum.Last[:0], sum.Last[n-retiredLast:]...)
	}
	return one
}

// drainEventsLocked turns core events into pipe events and wakes waiters.
func (c *Conn) drainEventsLocked() {
	for _, id := range c.retiredDone {
		if pd := c.pipes[id]; pd != nil {
			delete(c.pipes, id)
			unnestPipe(c, pd.outer)
		}
	}
	c.retiredDone = append(c.retiredDone[:0], c.retiredIDs...)
	c.retiredIDs = c.retiredIDs[:0]
	q := &c.events
	clear(q.fresh)
	q.fresh = q.fresh[:0]
	for {
		e, ok := c.s.PollEvent()
		if !ok {
			break
		}
		switch e.Kind {
		case core.EvOOBJoin:
			c.oobHeaders[e.Pipe] = encodeJoinHeader(e.Frame)
		case core.EvPipeState:
			c.pipeStateEvent(e)
		case core.EvJoinAuthFailed:
			q.push(Event{Kind: EventJoinAuthFailed, Reason: JoinAuthReason(e.Reason)})
		case core.EvEstablished:
			c.hk.trace(traceItem{est: true})
		case core.EvSuspended:
			c.needRedial = true
			c.firePipeWaiters()
			q.push(Event{Kind: EventSessionSuspended})
			if c.lst != nil {
				if over := c.lst.sessionSuspended(c); len(over) > 0 {
					go closeSuspended(over)
				}
			}
		case core.EvResumed:
			q.push(Event{Kind: EventSessionResumed})
			if c.lst != nil {
				c.lst.sessionResumed(c)
			}
		case core.EvGoAwayReceived:
			q.push(Event{Kind: EventGoAwayReceived, GoAway: &GoAway{MaxBidi: e.MaxBidi, MaxUni: e.MaxUni,
				ErrorCode: ApplicationErrorCode(e.Code)}})
		case core.EvJoinSecurityUnavailable:
			q.push(Event{Kind: EventJoinSecurityUnavailable})
		case core.EvTierSaturation:
			q.push(Event{Kind: EventTierSaturation, Priority: e.Code, Saturated: e.Saturated})
		case core.EvPipeQuality:
			q.push(Event{Kind: EventPipeQualityChanged, Pipe: e.Pipe, SmoothedRTT: time.Duration(e.RTT),
				DeliveryRate: e.Rate, Carrier: CarrierKind(c.s.PipeKind(e.Pipe))})
		case core.EvCapacityBelow:
			q.push(Event{Kind: EventCapacityBelow, DeliveryRate: e.Rate})
		case core.EvCapacityRestored:
			q.push(Event{Kind: EventCapacityRestored, DeliveryRate: e.Rate})
		case core.EvPipePriority:
			q.push(Event{Kind: EventPipePriorityChanged, Pipe: e.Pipe, Priority: e.Code, Standby: e.Saturated,
				Weight: e.Count, Carrier: CarrierKind(c.s.PipeKind(e.Pipe))})
		case core.EvClosed, core.EvDatagramDropped:
			// The end surfaces through Wait and the drops through the
			// statistics; neither is a pipe event.
		}
	}
	if c.hk != nil {
		for _, ev := range q.fresh {
			c.hk.trace(traceItem{ev: ev})
		}
	}
	if len(q.fresh) > 0 || len(c.pipeWaiters) > 0 {
		for _, w := range q.waiters {
			w.Fire()
		}
		q.waiters = q.waiters[:0]
	}
}

// firePipeWaiters wakes the waiters of pipe state changes.
func (c *Conn) firePipeWaiters() {
	for _, w := range c.pipeWaiters {
		w.Fire()
	}
	c.pipeWaiters = c.pipeWaiters[:0]
}

// pipeStateEvent maps a state change of one direction onto the API's
// events and wakes the waiters of AddPipe.
func (c *Conn) pipeStateEvent(e core.Event) {
	q := &c.events
	c.firePipeWaiters()
	if e.Dir != core.DirOut {
		switch e.InState {
		case core.InJoining, core.InOpen:
			q.mark(e.Pipe, dirInSeen)
		case core.InRetired:
			delete(c.labels, e.Pipe)
			if e.Pipe != 0 {
				// Forgotten at the next critical section, once the
				// carrier commands of this one have run.
				c.retiredIDs = append(c.retiredIDs, e.Pipe)
			}
			q.fresh = append(q.fresh, q.retire(e.Pipe, e.Failed))
		case core.InEnded:
			if q.dirs[e.Pipe]&dirInEnded == 0 {
				q.push(Event{Kind: EventPipeClosed, Pipe: e.Pipe, Direction: DirReceiveOnly,
					Carrier: CarrierKind(e.Carrier)})
			}
		case core.InNone:
			// No inbound direction: nothing to report.
		}
		return
	}
	if q.seenOut == nil {
		q.seenOut = map[uint64]core.OutState{}
	}
	prev, seen := q.seenOut[e.Pipe]
	q.seenOut[e.Pipe] = e.OutState
	if !seen {
		q.push(Event{Kind: EventPipeAdded, Pipe: e.Pipe, Direction: DirSendOnly, Carrier: CarrierKind(e.Carrier)})
	}
	var k EventKind
	switch e.OutState {
	case core.OutActive:
		k = EventPipeJoined
		if prev == core.OutSuspect {
			k = EventPipeRecovered
		}
	case core.OutSuspect:
		k = EventPipeSuspect
	case core.OutFailed:
		k = EventPipeFailed
	case core.OutClosed:
		k = EventPipeClosed
	case core.OutNone, core.OutJoining, core.OutDraining, core.OutRetired:
		// No event of their own: a joining or draining direction is
		// reported once it settles, retirement by EventPipesRetired.
	}
	if k != 0 && prev != e.OutState {
		q.push(Event{Kind: k, Pipe: e.Pipe, Direction: DirSendOnly, Carrier: CarrierKind(e.Carrier)})
	}
}

// NextEvent returns the next session event, waiting until one arrives,
// ctx ends or the session ends. Once the session has ended and no event is
// left it returns the session's error.
//
// Events wait in a queue of the session bounded at 1024 events. Reading it
// is optional: when nobody calls NextEvent the oldest events are dropped,
// so a reader that falls
// behind sees the latest events and can read the current state with
// PipeStats. The queue is independent of Config.Tracer: a Tracer receives
// every event as well, on its own bounded queue, whether or not NextEvent
// is called. Concurrent callers each receive different events.
func (c *Conn) NextEvent(ctx context.Context) (Event, error) {
	sig := core.NewSignal()
	for {
		c.lock()
		q := &c.events
		if len(q.q) > 0 {
			ev := q.q[0]
			copy(q.q, q.q[1:])
			q.q[len(q.q)-1] = Event{}
			q.q = q.q[:len(q.q)-1]
			c.unlock()
			return ev, nil
		}
		closing := c.closing
		if !closing {
			sig.Drain()
			q.waiters = append(q.waiters, sig)
		}
		c.unlock()
		if closing {
			return Event{}, c.closedErr()
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			c.lock()
			for i, w := range c.events.waiters {
				if w == sig {
					c.events.waiters = append(c.events.waiters[:i], c.events.waiters[i+1:]...)
					break
				}
			}
			c.unlock()
			return Event{}, ctx.Err()
		case <-c.done:
		}
	}
}

// closeSuspended closes sessions beyond the listener's MaxSuspended, the
// longest-suspended first, with ZMUX_RESUME_TIMEOUT (RESUME.md 9).
func closeSuspended(cs []*Conn) {
	for _, c := range cs {
		c.lock()
		c.s.CloseResumeTimeout()
		c.unlock()
	}
}
