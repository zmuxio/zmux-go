package core

import (
	"errors"

	"github.com/zmuxio/zmux-go/internal/join"
)

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
	// EvJoinKeys reports that the deferred PSK derived the join keys.
	EvJoinKeys
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
	// EvPipeAttached reports that the peer attached a pipe the endpoint
	// joined (the peer sent on it, so it holds the attachment too).
	EvPipeAttached
	// EvEpochCommitted / EvEpochDeleted: an epoch (Code) was committed or
	// deleted.
	EvEpochCommitted
	EvEpochDeleted
	// EvLocatorAdded / EvLocatorRemoved: a local Locator (Frame) joins may now
	// name, or no longer.
	EvLocatorAdded
	EvLocatorRemoved
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

// JoinAuthReason says why a Hello failed verification.
type JoinAuthReason uint8

// Reasons of a failed join authentication: the Hello did not open (auth), its
// payload did not parse (the class of the failure), or its key share was refused
// after the Pipe ID's bit was set (kex).
const (
	JoinAuthAEAD JoinAuthReason = iota + 1
	JoinAuthMissingField
	JoinAuthBadField
	JoinAuthTLVTruncated
	JoinAuthTLVNonMinimal
	JoinAuthTLVOrder
	JoinAuthTLVUnknownCritical
	JoinAuthKex
)

var joinAuthNames = [...]string{"", "auth", "missing_field", "bad_field", "tlv_truncated", "tlv_non_minimal", "tlv_order", "tlv_unknown_critical", "kex"}

func (r JoinAuthReason) String() string {
	if int(r) > 0 && int(r) < len(joinAuthNames) {
		return joinAuthNames[r]
	}
	return "JoinAuthReason(?)"
}

// joinAuthReason classifies the failure of a Hello.
func joinAuthReason(err error) JoinAuthReason {
	for _, c := range []struct {
		err error
		r   JoinAuthReason
	}{
		{join.ErrMissingField, JoinAuthMissingField}, {join.ErrBadField, JoinAuthBadField},
		{join.ErrTLVTruncated, JoinAuthTLVTruncated}, {join.ErrTLVNonMinimal, JoinAuthTLVNonMinimal},
		{join.ErrTLVOrder, JoinAuthTLVOrder}, {join.ErrTLVUnknownCritical, JoinAuthTLVUnknownCritical},
		{join.ErrKex, JoinAuthKex},
	} {
		if errors.Is(err, c.err) {
			return c.r
		}
	}
	return JoinAuthAEAD
}
