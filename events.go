package zmux

import (
	"context"
	"errors"
	"net"
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
	// one closed (DirectionSendOnly) or the inbound one ended (DirectionReceiveOnly)
	// (Pipe, Direction). Retirement is reported by EventPipesRetired.
	EventPipeClosed
	// EventPipePriorityChanged reports that the effective tier, standby state
	// or weight of an outbound pipe changed (Pipe, Priority, Standby, Weight,
	// Source).
	EventPipePriorityChanged
	// EventTierSaturation reports that the usable pipes of a tier became
	// saturated or stopped being so (Priority, Saturated).
	EventTierSaturation
	// EventJoinAuthFailed reports that joins failed authentication for a
	// Reason (Reason, Count: the failures of that reason since the event
	// was last read).
	EventJoinAuthFailed
	// EventSessionSuspended reports that every pipe of a direction was lost and
	// the session waits for a resume (no fields).
	EventSessionSuspended
	// EventSessionResumed reports that a suspended session has a pipe again (no
	// fields).
	EventSessionResumed
	// EventGoAwayReceived reports that the peer sent GOAWAY (GoAway).
	EventGoAwayReceived
	// EventResumeUnattended follows EventSessionSuspended when nothing in
	// the library will dial for the session: it can rejoin (the peer gave
	// a Session Locator) but has neither a Config.Redial hook nor a running
	// MaintainPipes pool, so it ends at its resume timeout unless the
	// application adds a pipe (no fields). It is also logged as a warning
	// (Config.Logger). An application that resumes with its own AddPipe
	// calls reacts to EventSessionSuspended and can ignore it. An unread
	// one leaves the queue when EventSessionResumed occurs.
	EventResumeUnattended
	// EventPipeQualityChanged reports that an outbound pipe left its
	// QualityBand (Pipe, SmoothedRTT, DeliveryRate).
	EventPipeQualityChanged
	// EventCapacityBelow reports that the outbound capacity fell below the
	// CapacityFloor (DeliveryRate: the capacity).
	EventCapacityBelow
	// EventCapacityRestored reports that the outbound capacity is back above
	// the CapacityFloor (DeliveryRate: the capacity).
	EventCapacityRestored
	// EventCarrierCandidate reports a try of a candidate of DialChoice
	// (Carrier, Outcome).
	EventCarrierCandidate
	// EventCarrierChosen reports the carrier kind DialChoice chose, and every
	// later switch to another kind (Carrier).
	EventCarrierChosen
	// EventPipesRetired is the one report of retirement (Retired, Failed,
	// Closed, Last): it enters the queue at the first retirement after the
	// previous one was read and accumulates until it is read, the unread
	// events of each retired pipe leaving the queue for it.
	EventPipesRetired
	// EventEpochCommitted reports that the session committed an epoch (Epoch):
	// its secret is known, and joins of it are accepted.
	EventEpochCommitted
	// EventEpochDeleted reports that an epoch (Epoch), its keys and its
	// Locators were deleted.
	EventEpochDeleted
	// EventDatagramPathChanged reports a new ConnectionState.
	// PathDatagramPayload (DatagramPayload): the carrier's datagram size
	// changed (path MTU discovery), another pipe carries the datagrams, or
	// they moved between the carrier's datagrams and records. It is
	// noticed when the session sends or its pipes change.
	EventDatagramPathChanged
	// EventStreamPlaced reports a STREAM range handed to a multipath
	// carrier, including copies and retransmissions.
	EventStreamPlaced
	// EventStreamGapStarted reports the leading missing offset. PipeID is
	// the pipe of the arriving STREAM that exposed the gap.
	EventStreamGapStarted
	// EventStreamGapFilled reports that the leading missing byte arrived.
	// PipeID is the pipe of that arrival and GapWait is the elapsed time.
	EventStreamGapFilled
	// EventPipePathQuality reports evidence for local replacement or shared
	// path deterioration, and when the observed deterioration clears.
	EventPipePathQuality
)

var pipeEventNames = [...]string{"", "PipeAdded", "PipeJoined", "PipeSuspect", "PipeRecovered", "PipeFailed",
	"PipeClosed", "PipePriorityChanged", "TierSaturation", "JoinAuthFailed", "SessionSuspended",
	"SessionResumed", "GoAwayReceived", "ResumeUnattended", "PipeQualityChanged", "CapacityBelow",
	"CapacityRestored", "CarrierCandidate", "CarrierChosen", "PipesRetired", "EpochCommitted", "EpochDeleted",
	"DatagramPathChanged", "StreamPlaced", "StreamGapStarted", "StreamGapFilled", "PipePathQuality"}

func (k EventKind) String() string {
	if int(k) < len(pipeEventNames) {
		return pipeEventNames[k]
	}
	return "PipeEventKind(?)"
}

// JoinAuthReason says why a Hello failed verification.
type JoinAuthReason uint8

// Reasons of EventJoinAuthFailed: the Hello did not open (auth), its payload did
// not parse (the error class of the failure), or its key share was refused after
// the Pipe ID was used (kex).
const (
	JoinAuthReasonAuth               = JoinAuthReason(core.JoinAuthAEAD)
	JoinAuthReasonMissingField       = JoinAuthReason(core.JoinAuthMissingField)
	JoinAuthReasonBadField           = JoinAuthReason(core.JoinAuthBadField)
	JoinAuthReasonTLVTruncated       = JoinAuthReason(core.JoinAuthTLVTruncated)
	JoinAuthReasonTLVNonMinimal      = JoinAuthReason(core.JoinAuthTLVNonMinimal)
	JoinAuthReasonTLVOrder           = JoinAuthReason(core.JoinAuthTLVOrder)
	JoinAuthReasonTLVUnknownCritical = JoinAuthReason(core.JoinAuthTLVUnknownCritical)
	JoinAuthReasonKex                = JoinAuthReason(core.JoinAuthKex)
)

// String returns the name the API specification uses ("auth", "kex", ...).
func (r JoinAuthReason) String() string { return core.JoinAuthReason(r).String() }

// Event is a session event, as NextEvent and Tracer.Event report it. Kind
// says which of the other fields are set (see EventKind). Stream events
// are delivered through Tracer.Event when Tracer.StreamEvents is enabled.
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
	// PathQuality is the reason for EventPipePathQuality.
	PathQuality PipePathQualityReason
	// Epoch of EventEpochCommitted and EventEpochDeleted.
	Epoch uint64
	// Count of EventJoinAuthFailed: the failures with its Reason since the
	// event was last read, at least 1 (a Tracer gets one event per
	// failure, with Count 1).
	Count uint64
	// Variant is the PipeIO.Variant of a pipe event's carrier while the
	// pipe has one, and the candidate's of EventCarrierCandidate and
	// EventCarrierChosen.
	Variant string
	// PipeID is the pipe ID of a pipe event.
	PipeID PipeID
	// Stream events are opt-in through Tracer.StreamEvents. Offsets and
	// lengths are wire bytes, including any preamble. StreamLength is
	// set for placements. StreamAt is the event time on the session clock.
	StreamID                   StreamID
	StreamOffset, StreamLength uint64
	StreamAt                   time.Time
	GapWait                    time.Duration
	// Priority is the tier (EventPipePriorityChanged,
	// EventTierSaturation); Weight the pipe's weight
	// (EventPipePriorityChanged); Source where the priority comes from, as
	// PipeStats.SendPrioritySource says: "local" (a setting of this
	// endpoint), "peer" (the peer's request) or "default".
	Priority uint64
	Weight   uint64
	Source   string
	// GoAway of EventGoAwayReceived.
	GoAway *GoAway
	// SmoothedRTT and DeliveryRate (bytes per second) of
	// EventPipeQualityChanged;
	// DeliveryRate is also the capacity of EventCapacityBelow and
	// EventCapacityRestored.
	SmoothedRTT  time.Duration
	DeliveryRate uint64
	// Retired counts the pipes retired since EventPipesRetired was last
	// read; Failed and Closed, of those, the ones whose failure or end
	// without failure had not been read; Last the IDs of the last retired
	// pipes, at most four, oldest first. An application that needs every
	// retired ID compares Pipes with the handles it holds. A Tracer gets
	// one event per retired pipe (Retired 1, Last that pipe, Failed 1 if
	// the pipe failed, else Closed 1).
	Retired, Failed, Closed uint64
	Last                    []PipeID
	// DatagramPayload of EventDatagramPathChanged: the new
	// PathDatagramPayload of a session datagram.
	DatagramPayload int
	// FateGroup and QueuedBytes are set for EventPipePathQuality,
	// together with the recent SmoothedRTT and DeliveryRate estimates.
	// RetryAfter is a replacement delay suggestion; zero means no redial.
	FateGroup, QueuedBytes uint64
	RetryAfter             time.Duration
}

// retiredLast is the number of IDs EventPipesRetired keeps.
const retiredLast = 4

// terminal reports a pipe's terminal event.
func terminal(k EventKind) bool { return k == EventPipeFailed || k == EventPipeClosed }

// eventKey identifies the one unread event a queue holds for it (the event key
// table): the kind plus what distinguishes the occurrences of the
// kind. Fields a kind does not use are zero.
type eventKey struct {
	variant string // CarrierCandidate
	pipe    uint64 // the pipe events
	prio    uint64 // TierSaturation
	kind    EventKind
	dir     Direction // Suspect, Recovered, Failed, Closed
	carrier CarrierKind
	reason  JoinAuthReason
}

// pipeKeys lists the events keyed by pipe, and whether their key holds the
// direction: the keys a retired pipe leaves the queue with.
var pipeKeys = [...]struct {
	kind EventKind
	dir  bool
}{
	{EventPipeAdded, false}, {EventPipeJoined, false}, {EventPipePriorityChanged, false},
	{EventPipeQualityChanged, false}, {EventPipePathQuality, false}, {EventPipeSuspect, true}, {EventPipeRecovered, true},
	{EventPipeFailed, true}, {EventPipeClosed, true},
}

// pipeEvent reports whether events of kind k belong to a pipe (Pipe set).
func pipeEvent(k EventKind) bool {
	for _, p := range pipeKeys {
		if p.kind == k {
			return true
		}
	}
	return false
}

// keyOf returns the key of ev.
func keyOf(ev *Event) eventKey {
	k := eventKey{kind: ev.Kind}
	for _, p := range pipeKeys {
		if p.kind == ev.Kind {
			k.pipe = uint64(ev.PipeID)
			if p.dir {
				k.dir = ev.Direction
			}
			return k
		}
	}
	switch ev.Kind {
	case EventTierSaturation:
		k.prio = ev.Priority
	case EventJoinAuthFailed:
		k.reason = ev.Reason
	case EventCarrierCandidate:
		k.carrier, k.variant = ev.Carrier, ev.Variant
	case EventPipeAdded, EventPipeJoined, EventPipeSuspect, EventPipeRecovered, EventPipeFailed, EventPipeClosed,
		EventPipePriorityChanged, EventPipeQualityChanged, EventSessionSuspended, EventSessionResumed,
		EventGoAwayReceived, EventResumeUnattended, EventCapacityBelow, EventCapacityRestored, EventCarrierChosen,
		EventPipesRetired, EventEpochCommitted, EventEpochDeleted, EventDatagramPathChanged,
		EventStreamPlaced, EventStreamGapStarted, EventStreamGapFilled, EventPipePathQuality:
		// Keyed by the kind alone (the pipe events above by their entry in pipeKeys).
	}
	return k
}

// keepFirst reports the kinds whose unread event a later occurrence of the
// key leaves as it is: each occurs once per pipe (direction).
func keepFirst(k EventKind) bool {
	return k == EventPipeAdded || k == EventPipeJoined || terminal(k)
}

// eventNode is the one unread event of a key, on the queue's list.
type eventNode struct {
	prev, next *eventNode
	key        eventKey
	ev         Event
}

// eventQueue holds session events for NextEvent; it is guarded by the
// session lock. It is the keyed queue of the event key table: a map from
// key to a node of one list in the order of the keys' latest occurrences.
// It holds at most one unread event per key and never drops one, so its
// size is bounded by what the session holds (pipes, tiers, carrier
// candidates) plus a fixed number of keys; a retired pipe's keys leave it.
type eventQueue struct {
	held       map[uint64]*joinEventHold
	idx        map[eventKey]*eventNode
	seenOut    map[uint64]core.OutState
	dirs       map[uint64]uint8 // per pipe: dirInEnded, dirOutEnded
	head, tail *eventNode
	waiters    []*core.Signal
	fresh      []Event // occurrences in this drain, for the tracer and the waiters
	frozen     bool    // the session is CLOSED: nothing more enters
	pollAdvice bool    // NextEvent enabled advice; StopEvents releases it
}

// Bits of eventQueue.dirs: the terminal event of a direction entered the
// queue.
const (
	dirInEnded uint8 = 1 << iota
	dirOutEnded
)

// freeze ends what enters the queue and the tracer's view: the session is
// CLOSED (what is queued is still read).
func (e *eventQueue) freeze() { e.frozen = true }

func (e *eventQueue) link(n *eventNode) {
	n.prev = e.tail
	if e.tail != nil {
		e.tail.next = n
	} else {
		e.head = n
	}
	e.tail = n
}

func (e *eventQueue) unlink(n *eventNode) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		e.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		e.tail = n.prev
	}
	n.prev, n.next = nil, nil
}

// remove takes the unread event of k off the queue, if any.
func (e *eventQueue) remove(k eventKey) *eventNode {
	n := e.idx[k]
	if n != nil {
		e.unlink(n)
		delete(e.idx, k)
	}
	return n
}

// touch returns the node of k, appended at the tail when the key had none
// and moved there otherwise (isNew: it had none).
func (e *eventQueue) touch(k eventKey) (n *eventNode, isNew bool) {
	if n = e.idx[k]; n != nil {
		e.unlink(n)
		e.link(n)
		return n, false
	}
	if e.idx == nil {
		e.idx = map[eventKey]*eventNode{}
	}
	n = &eventNode{key: k}
	e.idx[k] = n
	e.link(n)
	return n, true
}

// len is the number of unread events.
func (e *eventQueue) len() int { return len(e.idx) }

// pop takes the oldest unread event.
func (e *eventQueue) pop() (Event, bool) {
	n := e.head
	if n == nil {
		return Event{}, false
	}
	e.unlink(n)
	delete(e.idx, n.key)
	return n.ev, true
}

// push records an occurrence of ev: it is appended at the tail, or, when
// its key has an unread event, merged into it as the event key table says
// and moved to the tail. The tracer and the waiters see every occurrence
// (fresh).
func (e *eventQueue) push(ev Event) {
	if e.frozen {
		return
	}
	if ev.Kind == EventJoinAuthFailed && ev.Count == 0 {
		ev.Count = 1
	}
	e.fresh = append(e.fresh, ev)
	if terminal(ev.Kind) {
		bit := dirOutEnded
		if ev.Direction == DirectionReceiveOnly {
			bit = dirInEnded
		}
		e.mark(uint64(ev.PipeID), bit)
	}
	if ev.Kind == EventSessionResumed {
		e.remove(eventKey{kind: EventResumeUnattended}) // stale once resumed
	}
	k := keyOf(&ev)
	if h := e.held[k.pipe]; h != nil {
		h.queue.merge(k, ev)
	} else {
		e.merge(k, ev)
	}
}

func (e *eventQueue) merge(k eventKey, ev Event) {
	if n := e.idx[k]; n != nil && keepFirst(ev.Kind) {
		return
	}
	n, isNew := e.touch(k)
	switch {
	case isNew:
		n.ev = ev
	case ev.Kind == EventJoinAuthFailed:
		n.ev.Count += ev.Count
	default:
		n.ev = ev
	}
}

// mark sets bits of a pipe's dirs.
func (e *eventQueue) mark(id uint64, bits uint8) {
	if e.dirs == nil {
		e.dirs = map[uint64]uint8{}
	}
	e.dirs[id] |= bits
}

// retire folds the unread events of a retired pipe into the queue's
// EventPipesRetired, adding one when none is unread; the tracer gets the
// retirement alone (Retired 1, Failed or Closed 1, by the pipe's own
// failure flag, whether or not its end was read). The queue counts the
// pipe, in Failed when failed and in Closed otherwise, only when the
// application read none of its terminal events; a terminal event that
// never entered the queue was not read.
func (e *eventQueue) retire(id uint64, failed bool) {
	if e.frozen {
		return
	}
	one := Event{Kind: EventPipesRetired, Retired: 1, Last: []PipeID{PipeID(id)}}
	if failed {
		one.Failed = 1
	} else {
		one.Closed = 1
	}
	e.fresh = append(e.fresh, one)
	if h := e.held[id]; h != nil {
		h.retired, h.failed = true, failed
		return
	}
	e.foldRetired(id, failed)
}

func (e *eventQueue) foldRetired(id uint64, failed bool) {
	queued := e.dirs[id] & (dirInEnded | dirOutEnded) // terminal events that entered the queue
	delete(e.dirs, id)
	delete(e.seenOut, id)
	var unread uint8 // of those, the ones still in the queue
	for _, p := range pipeKeys {
		dirs := [...]Direction{DirectionBidirectional, DirectionSendOnly, DirectionReceiveOnly}
		n := 1 // keys without a direction hold the zero value
		if p.dir {
			n = len(dirs)
		}
		for _, dir := range dirs[:n] {
			if e.remove(eventKey{kind: p.kind, pipe: id, dir: dir}) != nil && terminal(p.kind) {
				if dir == DirectionReceiveOnly {
					unread |= dirInEnded
				} else {
					unread |= dirOutEnded
				}
			}
		}
	}
	unreadEnd := queued&^unread == 0 // no queued terminal event was read (none queued counts)
	n, isNew := e.touch(eventKey{kind: EventPipesRetired})
	if isNew {
		n.ev = Event{Kind: EventPipesRetired}
	}
	sum := &n.ev
	sum.Retired++
	if unreadEnd && failed {
		sum.Failed++
	} else if unreadEnd {
		sum.Closed++
	}
	sum.Last = append(sum.Last, PipeID(id))
	if l := len(sum.Last); l > retiredLast {
		sum.Last = append(sum.Last[:0], sum.Last[l-retiredLast:]...)
	}
}

// endPipeLocked records how pipe id ended and releases its waiters.
func (c *Conn) endPipeLocked(id uint64, failed bool, cause error) {
	pd := c.pipes[id]
	if pd == nil {
		return
	}
	e := pd.end
	select {
	case <-e.retired:
		return
	default:
	}
	if failed {
		e.err = ErrPipeGone
	}
	if cause != nil {
		e.err = cause
	}
	if e.join != nil {
		_ = e.join.settleLocked(ErrPipeGone)
	}
	close(e.retired)
	// Pipe.Done closes once the driver's goroutines have ended too.
	e.drv.Load().gs.finishThen(e.end)
}

// drainEventsLocked turns core events into pipe events and wakes waiters.
// drainEventsLocked moves the core's events into the queue and reports
// whether NextEvent's waiters are due a wake (wakeEventWaitersLocked).
func (c *Conn) drainEventsLocked() (wakeWaiters bool) {
	for _, id := range c.retiredDone {
		if pd := c.pipes[id]; pd != nil {
			delete(c.pipes, id)
			pd.nest.drop()
		}
	}
	c.retiredDone = append(c.retiredDone[:0], c.retiredIDs...)
	c.retiredIDs = c.retiredIDs[:0]
	q := &c.events
	for {
		e, ok := c.s.PollEvent()
		if !ok {
			break
		}
		switch e.Kind {
		case core.EvStreamPlaced:
			c.traceStream(e, EventStreamPlaced, DirectionSendOnly)
		case core.EvStreamGapStarted:
			c.traceStream(e, EventStreamGapStarted, DirectionReceiveOnly)
		case core.EvStreamGapFilled:
			c.traceStream(e, EventStreamGapFilled, DirectionReceiveOnly)
		case core.EvPipeState:
			// A replacement join still counts as live in the core during
			// its handshake. A directional loss can need recovery even
			// when that pending join suppresses EvSuspended.
			if c.redial != nil && (e.Dir == core.DirOut && (e.OutState == core.OutFailed || e.OutState == core.OutClosed) ||
				e.Dir == core.DirIn && e.InState == core.InEnded) {
				c.needRedial = true
			}
			if pd := c.pipes[e.Pipe]; pd != nil && pd.end.join != nil && !pd.end.join.settled {
				switch {
				case errors.Is(e.Cause, core.ErrJoinTimeout):
					_ = pd.end.join.settleLocked(ErrJoinTimeout)
				case errors.Is(e.Cause, core.ErrJoinRefused):
					_ = pd.end.join.settleLocked(ErrJoinRefused)
				case e.Dir == core.DirOut && (e.OutState == core.OutFailed || e.OutState == core.OutClosed || e.OutState == core.OutDraining),
					e.Dir == core.DirIn && e.InState == core.InEnded && pd.end.opts.Direction == DirectionReceiveOnly:
					_ = pd.end.join.settleLocked(ErrPipeGone)
				}
			}
			c.pipeStateEvent(e)
		case core.EvJoinAuthFailed:
			q.push(Event{Kind: EventJoinAuthFailed, Reason: JoinAuthReason(e.Reason), Count: 1})
		case core.EvEstablished:
			c.hk.trace(traceItem{est: true})
		case core.EvSuspended:
			c.needRedial = true
			c.firePipeWaiters()
			q.push(Event{Kind: EventSessionSuspended})
			if c.unattendedLocked() {
				q.push(Event{Kind: EventResumeUnattended})
				c.hk.warn("zmux: session suspended without a redial source (Config.Redial or MaintainPipes): it ends at its resume timeout unless the application adds a pipe")
			}
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
		case core.EvTierSaturation:
			q.push(Event{Kind: EventTierSaturation, Priority: e.Code, Saturated: e.Saturated})
		case core.EvPipePathQuality:
			c.redial.qualityChanged(PipeID(e.Pipe))
			q.push(Event{Kind: EventPipePathQuality, PipeID: PipeID(e.Pipe), Direction: DirectionSendOnly,
				PathQuality: PipePathQualityReason(e.PathQuality), FateGroup: e.Code, QueuedBytes: e.Count,
				SmoothedRTT: time.Duration(e.RTT), DeliveryRate: rateBytes(e.Rate), RetryAfter: time.Duration(e.Wait)})
		case core.EvPipeQuality:
			q.push(Event{Kind: EventPipeQualityChanged, PipeID: PipeID(e.Pipe), SmoothedRTT: time.Duration(e.RTT),
				DeliveryRate: rateBytes(e.Rate), Carrier: CarrierKind(c.s.PipeKind(e.Pipe))})
		case core.EvCapacityBelow:
			q.push(Event{Kind: EventCapacityBelow, DeliveryRate: rateBytes(e.Rate)})
		case core.EvCapacityRestored:
			q.push(Event{Kind: EventCapacityRestored, DeliveryRate: rateBytes(e.Rate)})
		case core.EvDatagramPath:
			q.push(Event{Kind: EventDatagramPathChanged, DatagramPayload: int(e.Count)})
		case core.EvPipePriority:
			q.push(Event{Kind: EventPipePriorityChanged, PipeID: PipeID(e.Pipe), Priority: e.Code, Standby: e.Saturated,
				Weight: e.Count, Source: e.Source, Carrier: CarrierKind(c.s.PipeKind(e.Pipe))})
		case core.EvPipeAttached:
			if pd := c.pipes[e.Pipe]; pd != nil {
				if o := pd.end.join; o != nil {
					_ = o.settleLocked(nil)
				}
			}
			c.firePipeWaiters() // WaitAttached and the pool
		case core.EvJoinKeys:
			// Joins are possible: maintenance and redial go on.
			c.needRedial = true
			c.firePipeWaiters()
		case core.EvEpochCommitted:
			q.push(Event{Kind: EventEpochCommitted, Epoch: e.Code})
			c.firePipeWaiters()
		case core.EvEpochDeleted:
			q.push(Event{Kind: EventEpochDeleted, Epoch: e.Code})
		case core.EvLocatorAdded:
			if c.lst != nil {
				c.lst.addLocator(c, e.Frame)
			}
		case core.EvLocatorRemoved:
			if c.lst != nil {
				c.lst.removeLocator(c, e.Frame)
			}
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
	// New events and registration of an exclusive consumer wake NextEvent.
	// firePipeWaiters wakes AddPipe and
	// the pool, whose long-lived waiter would otherwise keep NextEvent
	// spinning.
	wakeWaiters = len(q.fresh) > 0 || (c.handlers != nil && c.handlers.eventsRegistered)
	// Pushed since the last drain, inside it or by another critical
	// section (reportCarrier): each traced and woken for once, here.
	clear(q.fresh)
	q.fresh = q.fresh[:0]
	return wakeWaiters
}

// wakeEventWaitersLocked appends NextEvent's waiters to wakes, which the
// epilogue fires outside the lock.

// traceStream hands one stream placement or gap event to the tracer.
func (c *Conn) traceStream(e core.Event, kind EventKind, dir Direction) {
	c.hk.trace(traceItem{ev: Event{Kind: kind, Direction: dir, PipeID: PipeID(e.Pipe), StreamID: StreamID(e.Stream),
		StreamOffset: e.Offset, StreamLength: e.Count, StreamAt: c.clk.wall(e.At), GapWait: time.Duration(e.Wait)}})
}

func (c *Conn) wakeEventWaitersLocked(wakes []*core.Signal) []*core.Signal {
	q := &c.events
	wakes = append(wakes, q.waiters...)
	clear(q.waiters)
	q.waiters = q.waiters[:0]
	return wakes
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
		case core.InRetired:
			c.endPipeLocked(e.Pipe, e.Failed, e.Cause)
			if e.Pipe != 0 {
				// Forgotten at the next critical section, once the
				// carrier commands of this one have run.
				c.retiredIDs = append(c.retiredIDs, e.Pipe)
			}
			q.retire(e.Pipe, e.Failed)
		case core.InEnded:
			if q.dirs[e.Pipe]&dirInEnded == 0 {
				q.push(Event{Kind: EventPipeClosed, PipeID: PipeID(e.Pipe), Direction: DirectionReceiveOnly,
					Carrier: CarrierKind(e.Carrier), Variant: c.variantLocked(e.Pipe)})
			}
		case core.InNone, core.InJoining, core.InOpen:
			// No inbound direction, or one not ended: nothing to report.
		}
		return
	}
	if q.seenOut == nil {
		q.seenOut = map[uint64]core.OutState{}
	}
	prev, seen := q.seenOut[e.Pipe]
	q.seenOut[e.Pipe] = e.OutState
	if !seen {
		q.push(Event{Kind: EventPipeAdded, PipeID: PipeID(e.Pipe), Direction: DirectionSendOnly, Carrier: CarrierKind(e.Carrier), Variant: c.variantLocked(e.Pipe)})
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
		q.push(Event{Kind: k, PipeID: PipeID(e.Pipe), Direction: DirectionSendOnly, Carrier: CarrierKind(e.Carrier), Variant: c.variantLocked(e.Pipe)})
	}
}

// NextEvent returns the next session event, waiting until one arrives,
// ctx ends or the session ends. Once the session has ended and no event is
// left it returns the session's error.
//
// Events wait in a keyed queue: at most one unread event per key (a pipe's
// state of one direction, a tier, a carrier candidate, a kind of session
// event), which a new occurrence of the key updates and moves to the tail,
// so events are read in the order of their keys' latest occurrences and the
// last one read of a key is its latest state. The queue never drops an
// event and needs no bound of its own: its keys are those of the pipes,
// tiers and candidates the session holds, and a retired pipe's keys leave
// it for EventPipesRetired. Reading it is optional. Once the session has
// ended nothing more enters it. It is independent of Config.Tracer, which
// receives every occurrence, on its own backlog, whether or not NextEvent
// is called. Concurrent callers each receive different events. Calling NextEvent
// enables path quality advice until StopEvents.
func (c *Conn) NextEvent(ctx context.Context) (Event, error) {
	if c == nil {
		return Event{}, net.ErrClosed
	}
	sig := core.NewSignal()
	first := true
	for {
		c.lock()
		q := &c.events
		if c.handlers != nil && c.handlers.eventsRegistered {
			c.unlock()
			return Event{}, ErrHandlerRegistered
		}
		if first && !q.frozen && !q.pollAdvice {
			q.pollAdvice = true
			c.s.EnablePathAdvice()
		}
		first = false
		if ev, ok := q.pop(); ok {
			c.unlock()
			return ev, nil
		}
		sessionDone := c.done
		closing := c.closing && len(q.held) == 0
		if c.closing {
			sessionDone = nil
		}
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
		case <-sessionDone:
		}
	}
}

// closeSuspended closes sessions beyond the listener's MaxSuspended, the
// longest-suspended first, with ZMUX_RESUME_TIMEOUT.
func closeSuspended(cs []suspension) {
	for _, ticket := range cs {
		ticket.close()
	}
}
