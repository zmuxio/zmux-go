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
	// EvTierSaturation: a tier (priority in Code) entered or left saturation.
	EvTierSaturation
	// EvPipePriority: the effective priority (Code), standby flag
	// (Saturated) or weight (Count) of an outbound direction changed.
	EvPipePriority
	// EvPipeQuality: a pipe's smoothed RTT (RTT) or rate (Rate) left its band.
	EvPipeQuality
	// EvCapacityBelow / EvCapacityRestored: the outbound capacity (Rate)
	// crossed the floor.
	EvCapacityBelow
	EvCapacityRestored
)

// Event is one core event.
type Event struct {
	Kind     EventKind
	Pipe     uint64
	Dir      Dir
	State    string
	Err      *SessionError
	MaxBidi  uint64
	MaxUni   uint64
	Code     uint64
	Count    uint64
	Frame    []byte
	Reason   string
	Features Features
	Role     Role

	Saturated bool
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

func (s *Session) emit(e Event) { s.events = append(s.events, e) }

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
	// ActShutdown: gracefully end the write side of an outbound lane.
	ActShutdown ActionKind = iota + 1
	// ActAbort: abort both halves (failure).
	ActAbort
	// ActClose: close a carrier direction gracefully (after linger).
	ActClose
	// ActOpenLane: open a new outbound lane on the pipe's connection.
	ActOpenLane
	// ActResetLane: reset an unconfirmed outbound lane (refused).
	ActResetLane
	// ActRejectCarrier: close a carrier that was never attached.
	ActRejectCarrier
)

// Action is one carrier command.
type Action struct {
	Kind ActionKind
	Pipe uint64
	Lane uint64
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
