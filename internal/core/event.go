package core

// EventKind enumerates core events delivered to the driver.
type EventKind uint8

const (
	EvEstablished EventKind = iota + 1
	EvClosed
	EvGoAwayReceived
	EvPipeState
	EvJoinAuthFailed
	EvSuspended
	EvResumed
	EvDatagramDropped
	EvOOBJoin
	EvJoinSecurityUnavailable
	// EvTierSaturation reports that a tier (priority in Code) entered or left saturation.
	EvTierSaturation
	// EvPipePriority reports that the effective priority (Code), standby flag
	// (Saturated) or weight (Count) of an outbound direction changed.
	EvPipePriority
	// EvPipeQuality reports that a pipe's smoothed RTT (RTT) or rate (Rate) left its band.
	EvPipeQuality
	// EvCapacityBelow / EvCapacityRestored: the outbound capacity (Rate)
	// crossed the floor.
	EvCapacityBelow
	EvCapacityRestored
)

// Event is one core event.
type Event struct {
	Pipe     uint64
	Kind     EventKind
	Dir      Dir
	OutState OutState // EvPipeState of DirOut
	InState  InState  // EvPipeState of DirIn
	Carrier  uint8    // EvPipeState: the pipe's carrier kind
	Err      *SessionError
	MaxBidi  uint64
	MaxUni   uint64
	Code     uint64
	Count    uint64
	Frame    []byte
	Reason   JoinAuthReason // EvJoinAuthFailed
	Features Features
	Role     Role

	Saturated bool
	Failed    bool // retirement (EvPipeState of OutRetired): the attachment failed
	RTT       Duration
	Rate      float64
}

// Dir is a pipe direction from the local endpoint's view.
type Dir uint8

const (
	DirIn Dir = 1 << iota
	DirOut
	DirBidi = DirIn | DirOut
)

func (s *Session) emit(e Event) {
	if e.Kind == EvPipeState || e.Kind == EvPipePriority {
		s.capacityChanged()
	}
	s.events = append(s.events, e)
}

// PollEvent returns the next queued event.
func (s *Session) PollEvent() (Event, bool) {
	if s.evHead >= len(s.events) {
		if s.evHead > 0 {
			clear(s.events)
			s.events = s.events[:0]
			s.evHead = 0
		}
		return Event{}, false
	}
	e := s.events[s.evHead]
	s.events[s.evHead] = Event{}
	s.evHead++
	return e, true
}

// ActionKind is a carrier command for the driver.
type ActionKind uint8

const (
	// ActShutdown gracefully ends the write side of an outbound lane.
	ActShutdown ActionKind = iota + 1
	// ActAbort aborts both halves (failure).
	ActAbort
	// ActClose closes a carrier direction gracefully (after linger).
	ActClose
	// ActOpenLane opens a new outbound lane on the pipe's connection.
	ActOpenLane
	// ActResetLane resets an unconfirmed outbound lane (refused).
	ActResetLane
)

// Action is one carrier command.
type Action struct {
	Pipe uint64
	Lane uint64
	Kind ActionKind
	Dir  Dir
}

func (s *Session) act(a Action) { s.actions = append(s.actions, a) }

// TakeActions returns and clears queued carrier commands.
func (s *Session) TakeActions(dst []Action) []Action {
	dst = append(dst, s.actions...)
	clear(s.actions)
	s.actions = s.actions[:0]
	return dst
}

// JoinAuthReason says which join token failed verification (MULTIPATH.md
// 5.4 step 5).
type JoinAuthReason uint8

const (
	JoinAuthMAC             JoinAuthReason = iota + 1 // a token without carrier binding
	JoinAuthMACCarrierBound                           // a carrier-bound token
)

func (r JoinAuthReason) String() string {
	switch r {
	case JoinAuthMAC:
		return "mac"
	case JoinAuthMACCarrierBound:
		return "mac_carrier_bound"
	}
	return "JoinAuthReason(?)"
}
