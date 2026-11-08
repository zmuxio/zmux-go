// Package shaping defines the decisions on sizes and timing of what a session
// writes: Shaper for records and HandshakeShaper for handshake messages.
package shaping

import (
	"math"
	"time"
)

// UnitPlan is the size and timing of one unit written to a carrier. Lengths
// are wire bytes; zero values: none.
type UnitPlan struct {
	PadTo       int           // the unit is padded up to this length
	PadMultiple int           // then rounded up to a multiple of this
	Pad         int           // then this many bytes are added
	Delay       time.Duration // the write waits this long
}

// PadFor is the length of a unit of n bytes under p.
func PadFor(n int, p *UnitPlan) int {
	t := max(n, p.PadTo)
	if m := p.PadMultiple; m > 0 && t%m != 0 {
		t += m - t%m
	}
	return t + p.Pad
}

// ApplyLimits applies NoPadding, NoPause and MaxDelay of l to p: a MaxDelay of
// 0 is DefaultMaxDelay and a negative one no bound.
func ApplyLimits(p *UnitPlan, l *Limits) {
	if l.NoPadding {
		p.PadTo, p.PadMultiple, p.Pad = 0, 0, 0
	}
	if l.NoPause {
		p.Delay = 0
	}
	m := l.MaxDelay
	if m == 0 {
		m = DefaultMaxDelay
	}
	if m > 0 && p.Delay > m {
		p.Delay = m
	}
}

// HandshakeView names a handshake message about to be written.
type HandshakeView struct {
	Kind      string // the carrier's kind name; "": none
	Handshake string // "establish" or "join"
	Message   string // "hello", "hello_early", "reply", "finish" or "confirm"
}

// HandshakeShaper is asked for the plan of each handshake message; p is
// zeroed. It reports whether it has a plan for the message: a zero plan it
// returns true for is a decision to leave the message as it is, while false
// is a message it does not know, and p is then ignored. It may be called
// concurrently, outside every lock, and must not block. A panic counts as no
// plan.
type HandshakeShaper func(v *HandshakeView, p *UnitPlan) (planned bool)

// Ask calls h. Without a plan p is left zero. A panic is no plan and reports
// ok false.
func Ask(h HandshakeShaper, v *HandshakeView, p *UnitPlan) (planned, ok bool) {
	defer func() {
		if recover() != nil {
			*p = UnitPlan{}
			planned, ok = false, false
		}
	}()
	if planned = h(v, p); !planned {
		*p = UnitPlan{}
	}
	return planned, true
}

// Shaper customizes record sizes and timing. The session calls its methods
// one at a time under its lock; they must not block or call methods of the
// session, and a panic stops the shaping of the pipe.
type Shaper interface {
	// Capabilities is called once, when the instance is installed.
	Capabilities() Caps
	// Plan is called before each outgoing record of the pipe; p is zeroed.
	Plan(v *RecordView, p *RecordPlan)
	// OnEvent receives a notification; r is zeroed. Unknown kinds should
	// be ignored.
	OnEvent(e *Event, r *Effects)
}

// Caps is a set of capabilities.
type Caps uint32

const (
	CapPlan     Caps = 1 << iota // Plan is called
	CapRecordIn                  // EventRecordIn is delivered
	CapStreams                   // EventStreamOpen and EventStreamEnd are delivered
	CapBudget                    // RecordView.Meters and Event.Meters are filled
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
	Meters     Meters        // CapBudget
}

// RecordPlan is the decision for one record. Its lengths are wire bytes: the
// record's size field, its frames and, on the first record of a write, the
// pipe's write overhead. The session converts them to frame bytes; a length
// no record can have is taken to the next one, and values beyond limits are
// clamped. PadTo above MaxLen cuts to MaxLen and pads to PadTo.
type RecordPlan struct {
	UnitPlan
	MaxLen  int     // largest record length; 0: the limit
	Flush   bool    // the record ends its carrier write
	Stop    bool    // shaping of the pipe ends after this record
	Effects Effects // applied when the plan is made
}

// EventKind identifies an Event.
type EventKind uint8

const (
	EventStart         EventKind = iota + 1 // the instance was installed
	EventReplaced                           // the instance was replaced; no further calls
	EventRecordIn                           // a record arrived on the pipe (CapRecordIn)
	EventInjected                           // the pending injection was sent
	EventInjectDropped                      // the pending injection will not be sent
	EventTimer                              // Effects.Wake was reached
	EventStopped                            // shaping of the pipe ended; no further calls
	EventPipeFailed                         // the pipe's outbound direction failed; no further calls
	EventStreamOpen                         // a stream got its identifier (CapStreams)
	EventStreamEnd                          // a stream's state was discarded (CapStreams)
)

// Event is a notification.
type Event struct {
	Kind        EventKind
	PaddingOnly bool // EventRecordIn
	Pipe, Lane  uint64
	Now         time.Duration // session clock
	Len         int           // frames length, bytes (EventRecordIn, EventInjected)
	Stream      uint64        // EventStreamOpen, EventStreamEnd: stream ID
	Meters      Meters        // CapBudget
}

// EffectSet says which items of Effects change.
type EffectSet uint8

const (
	EffectInject EffectSet = 1 << iota
	EffectWake
	EffectGate
	EffectWeight
	EffectStop
)

// Effects is the answer to an event. The session holds one of each item per
// pipe; what Set does not name stays as it was. Times are on the session
// clock. A pending injection and a wake time stay until replaced, also once
// reached: the answer to EventInjected, EventInjectDropped or EventTimer
// replaces or clears them.
type Effects struct {
	Set       EffectSet
	HasInject bool   // EffectInject: an injection is pending; false clears it
	HasWake   bool   // EffectWake: a wake time is set; false clears it
	Weight    uint32 // EffectWeight: PickPipe.ShapingWeight; 0 is ignored

	InjectAt  time.Duration // HasInject: when to send a record of PADDING only on lane 0
	InjectLen int           // its wire length

	Wake time.Duration // HasWake: when to deliver EventTimer

	PauseUntil time.Duration // EffectGate: stream data waits until then
	Rate       uint64        // bytes per second; 0: no limit
	Burst      uint64        // bytes; 0: the record limit
}

// MinMaxLen is the smallest largest record honoured, in frame bytes (64, a
// STREAM frame header and some data); a smaller one is raised to it
// (Stats.Clamped), together with a PadTo equal to it.
const MinMaxLen = 64

// DefaultMaxDelay is the bound on one wait when Limits.MaxDelay is 0.
const DefaultMaxDelay = time.Second

// The largest values of an Account's fields.
const (
	MaxAccountInitial = 1 << 53
	MaxAccountRate    = 1 << 43
)

// Account is an allowance for records of PADDING only and injections merged
// into records. All fields zero: not set. Its unit is the wire byte. Padding
// that goes with a record is never refused and does not draw on it.
type Account struct {
	Initial uint64 // available from the start
	PerMB   uint64 // added per 10^6 bytes this endpoint sends of its own
	PerSec  uint64 // added per second
}

func (a Account) isZero() bool { return a == Account{} }

// Limits are local bounds over a session, set by the application and never
// sent. A configuration cannot widen them.
type Limits struct {
	Wire Account
	// MaxDelay bounds one wait, and so the lowest rate of a gate. Zero is
	// DefaultMaxDelay, unlike most zero durations; negative is no bound.
	MaxDelay time.Duration
	// NoPadding allows no padding at all, on handshakes and injections too.
	NoPadding bool
	// NoPause allows no pause or delay.
	NoPause bool
	// NoInjection allows no injection.
	NoInjection bool
}

// Budget is what a shaping configuration allows for a session.
type Budget struct {
	Wire Account
	// MaxDelay is the configuration's bound on one wait; 0 or negative: none.
	MaxDelay time.Duration
	// NoInjection: the configuration allows no injection.
	NoInjection bool
}

// With is l in force under b: the Wire account field by field the smaller where
// both are set, the smaller bound on one wait, which is then never 0. Two set
// accounts that leave nothing in any field allow no injection.
func (l Limits) With(b Budget) Limits {
	b.Wire.Initial = min(b.Wire.Initial, MaxAccountInitial)
	b.Wire.PerMB = min(b.Wire.PerMB, MaxAccountRate)
	b.Wire.PerSec = min(b.Wire.PerSec, MaxAccountRate)
	switch {
	case l.Wire.isZero():
		l.Wire = b.Wire
	case !b.Wire.isZero():
		l.Wire = Account{min(l.Wire.Initial, b.Wire.Initial), min(l.Wire.PerMB, b.Wire.PerMB), min(l.Wire.PerSec, b.Wire.PerSec)}
		l.NoInjection = l.NoInjection || l.Wire.isZero()
	}
	l.NoInjection = l.NoInjection || b.NoInjection
	if l.MaxDelay == 0 {
		l.MaxDelay = DefaultMaxDelay
	}
	if b.MaxDelay > 0 && (l.MaxDelay < 0 || b.MaxDelay < l.MaxDelay) {
		l.MaxDelay = b.MaxDelay
	}
	return l
}

// Meters are a session's readings. WireLeft is what the Wire account allows
// injections now and StallLeft is reserved; both are the largest uint64 when
// unbounded. WireSpent is all added wire bytes; StallSpent is in microseconds.
type Meters struct {
	WireLeft, StallLeft, WireSpent, StallSpent, SentBytes uint64
}

// WirePPM is s bytes in parts per million of e; the largest uint64 when e is 0.
// Past 2^44 both lose their low 20 bits.
func WirePPM(s, e uint64) uint64 {
	if max(s, e) >= 1<<44 {
		s >>= 20
		e >>= 20
	}
	if e == 0 {
		return math.MaxUint64
	}
	return s * 1_000_000 / e
}

// Stats are counters over a session.
type Stats struct {
	WireSpent          uint64        // wire bytes added: padding, splits, handshakes, injections
	StallSpent         time.Duration // waits applied
	Clamped            uint64        // decisions reduced by a structural limit, MaxDelay or a No switch
	Injected           uint64        // records of PADDING only sent
	InjectDropped      uint64        // injections not sent
	Faults             uint64        // calls that panicked
	HandshakeUnplanned uint64        // Join messages without a plan under a HandshakeShaper
}
