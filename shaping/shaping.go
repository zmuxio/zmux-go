// Package shaping defines Shaper, which customizes record sizes and timing.
package shaping

import (
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// Shaper customizes record sizes and timing. The session calls its methods
// one at a time; they must not block or call methods of the session.
type Shaper interface {
	// Capabilities is called once, when the instance is installed.
	Capabilities() Caps
	// Plan is called before each outgoing record of the pipe; p is zeroed.
	Plan(v *RecordView, p *RecordPlan)
	// OnEvent receives a notification; r is zeroed. Unknown kinds should
	// be ignored. The values a Reply (also RecordPlan.Reply) points to are
	// read before the next call.
	OnEvent(e *Event, r *Reply)
}

// Caps is a set of capabilities.
type Caps uint32

const (
	CapPlan     Caps = 1 << iota // Plan is called
	CapRecordIn                  // EventRecordIn is delivered
	CapStreams                   // EventStreamOpen and EventStreamEnd are delivered
)

// RecordView describes the next outgoing record.
type RecordView struct {
	Pipe, Lane uint64
	Seq        uint64 // records of the pipe planned before this one
	Limit      int    // largest frames length, bytes
	// WriteStart: the record is the first of a carrier write call (every
	// record of a message-framed carrier, one message per write).
	WriteStart bool
	Now        time.Duration // session clock
}

// RecordPlan is the decision for one record. Values beyond limits are
// clamped.
type RecordPlan struct {
	MaxLen      int           // largest frames length, bytes; 0: Limit
	PadTo       int           // frames length padded up to, bytes; 0: none
	PadMultiple int           // frames length rounded up to a multiple, bytes; 0: none
	Pad         int           // PADDING bytes appended after PadTo and PadMultiple; 0: none
	Flush       bool          // the record ends its carrier write
	Stop        bool          // shaping of the pipe ends
	Delay       time.Duration // stream data of the pipe waits this long; 0: none
	Reply       Reply         // applied as an event reply when the plan is made
}

// EventKind identifies an Event.
type EventKind uint8

const (
	EventStart      EventKind = iota + 1 // the instance was installed
	EventReplaced                        // the instance was replaced; no further calls
	EventRecordIn                        // a record arrived on the pipe (CapRecordIn)
	EventInjected                        // a record of PADDING only was sent
	EventTimer                           // a timer fired
	EventStopped                         // shaping of the pipe ended; no further calls
	EventPipeFailed                      // the pipe's outbound direction failed; no further calls
	EventStreamOpen                      // a stream got its identifier (CapStreams)
	EventStreamEnd                       // a stream's state was discarded (CapStreams)
)

// Event is a notification.
type Event struct {
	Kind        EventKind
	PaddingOnly bool // EventRecordIn
	Pipe, Lane  uint64
	Now         time.Duration // session clock
	Len         int           // frames length, bytes (EventRecordIn, EventInjected)
	Timer       int           // EventTimer: slot, 0 or 1
	Stream      uint64        // EventStreamOpen, EventStreamEnd: stream ID
}

// Reply is the answer to an Event. Its zero value changes nothing; a nil
// field leaves its item unchanged.
type Reply struct {
	Inject *Inject   // replaces the pending injection
	Gate   *Gate     // replaces the current gate
	Timer  [2]*Timer // arms or cancels a timer slot
	Weight *float64  // PickPipe.ShapingWeight of the pipe; a value <= 0 is ignored
	Stop   bool      // shaping of the pipe ends
}

// Inject schedules a record of PADDING only on lane 0 of the pipe; one is
// pending at most.
type Inject struct {
	Cancel bool          // cancels the pending one; After and Len are ignored
	After  time.Duration // from now; negative: now
	Len    int           // frames length, bytes; raised to the smallest such record
}

// Gate bounds the stream data of the pipe; its zero value removes the
// bound.
type Gate struct {
	Pause time.Duration // from now; 0: none
	Rate  int64         // bytes per second; 0: no limit
	Burst int64         // bytes; 0: the record limit
}

// Timer arms or cancels a timer slot.
type Timer struct {
	Cancel bool          // disarms the slot; After is ignored
	After  time.Duration // from now; negative: now
}

// MinMaxLen is the smallest RecordPlan.MaxLen honoured (64 bytes, a
// STREAM frame header and some data); a smaller nonzero MaxLen is raised
// to it (Stats.Clamped).
const MinMaxLen = 64

// StartupAllowance is the PADDING a session may add regardless of
// Limits.MaxPaddingRatio or NoPaddingRatio (16382 bytes, one record of the default
// max_record_size), so that the first padding and injections of a session
// are not cut by the ratio. Limits.MaxPaddingBytes still applies.
const StartupAllowance = wire.DefaultMaxRecordSize

// Limits are upper bounds over a session. Zero values: unbounded; the
// No fields allow none. Every field must be non-negative and
// MaxPaddingRatio, a finite number, at most 1; a session configured
// otherwise is refused.
type Limits struct {
	// MaxPaddingBytes bounds the PADDING bytes added. Zero is unbounded.
	MaxPaddingBytes uint64
	// MaxPaddingRatio bounds the PADDING bytes over the other bytes sent,
	// from 0 to 1; once set, the StartupAllowance is allowed too. Zero is
	// unbounded; NoPaddingRatio sets a ratio of 0.
	MaxPaddingRatio float64
	// MaxInjected bounds the records of PADDING only. Zero is unbounded.
	MaxInjected uint64
	// MaxPauseTotal bounds the sum of pauses and delays. Zero is
	// unbounded.
	MaxPauseTotal time.Duration
	// MaxDelay bounds one pause or delay. Zero is unbounded.
	MaxDelay time.Duration
	// NoPadding allows no PADDING at all, injections included.
	NoPadding bool
	// NoPaddingRatio sets a padding ratio of 0, overriding
	// MaxPaddingRatio: only the StartupAllowance is allowed, within
	// MaxPaddingBytes. Unlike NoPadding, it still allows that PADDING.
	NoPaddingRatio bool
	// NoInjection allows no record of PADDING only.
	NoInjection bool
	// NoPause allows no pause or delay.
	NoPause bool
}

// Stats are counters over a session.
type Stats struct {
	PaddingBytes  uint64        // PADDING bytes added
	Injected      uint64        // records of PADDING only sent
	InjectDropped uint64        // injections not sent
	Clamped       uint64        // decisions reduced to limits
	PauseTotal    time.Duration // pauses and delays applied
	Faults        uint64        // calls that panicked
}
