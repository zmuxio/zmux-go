package zmux

import (
	"context"
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
	// EventPipeClosed reports that a pipe direction ended; DirBidirectional
	// when the whole attachment was retired (Pipe, Direction).
	EventPipeClosed
	// EventPipePriorityChanged reports that the effective tier, standby state
	// or weight of an outbound pipe changed (Pipe, Priority, Standby, Weight).
	EventPipePriorityChanged
	// EventTierSaturation reports that the usable pipes of a tier became
	// saturated or stopped being so (Priority, Saturated).
	EventTierSaturation
	// EventJoinAuthFailed reports that a join failed authentication (Reason:
	// "mac" or "mac_carrier_bound").
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
)

var pipeEventNames = [...]string{"", "PipeAdded", "PipeJoined", "PipeSuspect", "PipeRecovered", "PipeFailed",
	"PipeClosed", "PipePriorityChanged", "TierSaturation", "JoinAuthFailed", "SessionSuspended",
	"SessionResumed", "GoAwayReceived", "JoinSecurityUnavailable", "PipeQualityChanged", "CapacityBelow",
	"CapacityRestored"}

func (k EventKind) String() string {
	if int(k) < len(pipeEventNames) {
		return pipeEventNames[k]
	}
	return "PipeEventKind(?)"
}

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
	// Pipe is the pipe ID of a pipe event.
	Pipe uint64
	// Priority is the tier (EventPipePriorityChanged,
	// EventTierSaturation); Weight the pipe's weight
	// (EventPipePriorityChanged).
	Priority uint64
	Weight   uint64
	// Reason of EventJoinAuthFailed.
	Reason string
	// GoAway of EventGoAwayReceived.
	GoAway *GoAway
	// SmoothedRTT and DeliveryRate (bytes/s) of EventPipeQualityChanged;
	// DeliveryRate is also the capacity of EventCapacityBelow and
	// EventCapacityRestored.
	SmoothedRTT  time.Duration
	DeliveryRate float64
}

// eventQueue holds session events for NextEvent; it is guarded by the
// session lock. It is bounded (eventQueueMax): when the application does
// not read events, the oldest are dropped and counted.
type eventQueue struct {
	q       []Event
	waiters []*core.Signal
	seenOut map[uint64]string
	dropped uint64
}

const eventQueueMax = 1024

func (e *eventQueue) push(ev Event) {
	if len(e.q) >= eventQueueMax {
		copy(e.q, e.q[1:])
		e.q = e.q[:len(e.q)-1]
		e.dropped++
	}
	e.q = append(e.q, ev)
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
	n0 := len(q.q)
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
			q.push(Event{Kind: EventJoinAuthFailed, Reason: e.Reason})
		case core.EvEstablished:
			c.hk.trace(traceItem{est: true})
		case core.EvSuspended:
			c.needRedial = true
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
				DeliveryRate: e.Rate})
		case core.EvCapacityBelow:
			q.push(Event{Kind: EventCapacityBelow, DeliveryRate: e.Rate})
		case core.EvCapacityRestored:
			q.push(Event{Kind: EventCapacityRestored, DeliveryRate: e.Rate})
		case core.EvPipePriority:
			q.push(Event{Kind: EventPipePriorityChanged, Pipe: e.Pipe, Priority: e.Code, Standby: e.Saturated,
				Weight: e.Count})
		case core.EvClosed, core.EvDatagramDropped:
			// The end surfaces through Wait and the drops through the
			// statistics; neither is a pipe event.
		}
	}
	if c.hk != nil {
		for _, ev := range q.q[n0:] {
			c.hk.trace(traceItem{ev: ev})
		}
	}
	if len(q.q) > n0 || len(c.pipeWaiters) > 0 {
		for _, w := range q.waiters {
			w.Fire()
		}
		q.waiters = q.waiters[:0]
	}
}

// pipeStateEvent maps a state change of one direction onto the API's
// events and wakes the waiters of AddPipe.
func (c *Conn) pipeStateEvent(e core.Event) {
	q := &c.events
	for _, w := range c.pipeWaiters {
		w.Fire()
	}
	c.pipeWaiters = c.pipeWaiters[:0]
	if e.Dir != core.DirOut {
		if e.State == "RETIRED" {
			delete(q.seenOut, e.Pipe)
			delete(c.labels, e.Pipe)
			if e.Pipe != 0 {
				// Forgotten at the next critical section, once the
				// carrier commands of this one have run.
				c.retiredIDs = append(c.retiredIDs, e.Pipe)
			}
			q.push(Event{Kind: EventPipeClosed, Pipe: e.Pipe, Direction: DirBidirectional})
		}
		return
	}
	if q.seenOut == nil {
		q.seenOut = map[uint64]string{}
	}
	prev, seen := q.seenOut[e.Pipe]
	q.seenOut[e.Pipe] = e.State
	if !seen {
		q.push(Event{Kind: EventPipeAdded, Pipe: e.Pipe, Direction: DirSendOnly})
	}
	var k EventKind
	switch e.State {
	case "ACTIVE":
		k = EventPipeJoined
		if prev == "SUSPECT" {
			k = EventPipeRecovered
		}
	case "SUSPECT":
		k = EventPipeSuspect
	case "FAILED":
		k = EventPipeFailed
	case "CLOSED":
		k = EventPipeClosed
	}
	if k != 0 && prev != e.State {
		q.push(Event{Kind: k, Pipe: e.Pipe, Direction: DirSendOnly})
	}
}

// NextEvent returns the next session event, waiting until one arrives,
// ctx ends or the session ends. Once the session has ended and no event is
// left it returns the session's error.
//
// Events wait in a queue of the session bounded at 1024 events. Reading it
// is optional: when nobody calls NextEvent the oldest events are dropped
// (ConnectionStats.EventsDropped counts them), so a reader that falls
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
