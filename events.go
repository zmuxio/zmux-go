package zmux

import (
	"context"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// PipeEventKind is the kind of a pipe or session event (API.md 4.2, 4.5).
type PipeEventKind uint8

const (
	PipeAdded PipeEventKind = iota + 1
	PipeJoined
	PipeSuspect
	PipeRecovered
	PipeFailed
	PipeClosed
	PipePriorityChanged
	TierSaturation
	JoinAuthFailed
	SessionSuspended
	SessionResumed
	GoAwayReceived
	JoinSecurityUnavailable
	PipeQualityChanged
	CapacityBelow
	CapacityRestored
)

var pipeEventNames = [...]string{"", "PipeAdded", "PipeJoined", "PipeSuspect", "PipeRecovered", "PipeFailed",
	"PipeClosed", "PipePriorityChanged", "TierSaturation", "JoinAuthFailed", "SessionSuspended",
	"SessionResumed", "GoAwayReceived", "JoinSecurityUnavailable", "PipeQualityChanged", "CapacityBelow",
	"CapacityRestored"}

func (k PipeEventKind) String() string {
	if int(k) < len(pipeEventNames) {
		return pipeEventNames[k]
	}
	return "PipeEventKind(?)"
}

// PipeEvent is one event of NextPipeEvent.
type PipeEvent struct {
	Kind PipeEventKind
	// Direction of the pipe events (with Pipe).
	Direction Direction
	// Standby of PipePriorityChanged; Saturated of TierSaturation.
	Standby   bool
	Saturated bool
	// Pipe of the pipe events.
	Pipe uint64
	// Priority and Weight of PipePriorityChanged; Priority of
	// TierSaturation.
	Priority uint64
	Weight   uint64
	// Reason of JoinAuthFailed ("mac", "mac_carrier_bound").
	Reason string
	// GoAway of GoAwayReceived.
	GoAway *GoAway
	// SmoothedRTT and DeliveryRate of PipeQualityChanged; DeliveryRate is
	// the capacity of CapacityBelow and CapacityRestored (bytes/s).
	SmoothedRTT  time.Duration
	DeliveryRate float64
}

// eventQueue holds session events for NextPipeEvent; it is guarded by the
// session lock. It is bounded: when the application does not read events,
// the oldest are dropped (the latest state of a pipe can always be read
// with Pipes()).
type eventQueue struct {
	q       []PipeEvent
	waiters []*core.Signal
	seenOut map[uint64]string
	dropped uint64
}

const eventQueueMax = 1024

func (e *eventQueue) push(ev PipeEvent) {
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
			q.push(PipeEvent{Kind: JoinAuthFailed, Reason: e.Reason})
		case core.EvEstablished:
			c.hk.trace(traceItem{est: true})
		case core.EvSuspended:
			c.needRedial = true
			q.push(PipeEvent{Kind: SessionSuspended})
			if c.lst != nil {
				if over := c.lst.sessionSuspended(c); len(over) > 0 {
					go closeSuspended(over)
				}
			}
		case core.EvResumed:
			q.push(PipeEvent{Kind: SessionResumed})
			if c.lst != nil {
				c.lst.sessionResumed(c)
			}
		case core.EvGoAwayReceived:
			q.push(PipeEvent{Kind: GoAwayReceived, GoAway: &GoAway{MaxBidi: e.MaxBidi, MaxUni: e.MaxUni,
				ErrorCode: ApplicationErrorCode(e.Code)}})
		case core.EvJoinSecurityUnavailable:
			q.push(PipeEvent{Kind: JoinSecurityUnavailable})
		case core.EvTierSaturation:
			q.push(PipeEvent{Kind: TierSaturation, Priority: e.Code, Saturated: e.Saturated})
		case core.EvPipeQuality:
			q.push(PipeEvent{Kind: PipeQualityChanged, Pipe: e.Pipe, SmoothedRTT: time.Duration(e.RTT),
				DeliveryRate: e.Rate})
		case core.EvCapacityBelow:
			q.push(PipeEvent{Kind: CapacityBelow, DeliveryRate: e.Rate})
		case core.EvCapacityRestored:
			q.push(PipeEvent{Kind: CapacityRestored, DeliveryRate: e.Rate})
		case core.EvPipePriority:
			q.push(PipeEvent{Kind: PipePriorityChanged, Pipe: e.Pipe, Priority: e.Code, Standby: e.Saturated,
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
			q.push(PipeEvent{Kind: PipeClosed, Pipe: e.Pipe, Direction: Bidirectional})
		}
		return
	}
	if q.seenOut == nil {
		q.seenOut = map[uint64]string{}
	}
	prev, seen := q.seenOut[e.Pipe]
	q.seenOut[e.Pipe] = e.State
	if !seen {
		q.push(PipeEvent{Kind: PipeAdded, Pipe: e.Pipe, Direction: SendOnly})
	}
	var k PipeEventKind
	switch e.State {
	case "ACTIVE":
		k = PipeJoined
		if prev == "SUSPECT" {
			k = PipeRecovered
		}
	case "SUSPECT":
		k = PipeSuspect
	case "FAILED":
		k = PipeFailed
	case "CLOSED":
		k = PipeClosed
	}
	if k != 0 && prev != e.State {
		q.push(PipeEvent{Kind: k, Pipe: e.Pipe, Direction: SendOnly})
	}
}

// NextPipeEvent returns the next pipe or session event (API.md 4.2). It
// returns the session's error once the session has ended and no event is
// left.
func (c *Conn) NextPipeEvent(ctx context.Context) (PipeEvent, error) {
	sig := core.NewSignal()
	for {
		c.lock()
		q := &c.events
		if len(q.q) > 0 {
			ev := q.q[0]
			copy(q.q, q.q[1:])
			q.q[len(q.q)-1] = PipeEvent{}
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
			return PipeEvent{}, c.closedErr()
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
			return PipeEvent{}, ctx.Err()
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
