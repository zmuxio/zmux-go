// Package shaping defines the decisions on sizes and timing of what a session
// writes: Shaper for records and HandshakeShaper for handshake messages.
package shaping

import (
	"math"
	"math/bits"
	"time"

	"github.com/zmuxio/zmux-go/internal/join"
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
		t += min(m-t%m, math.MaxInt-t)
	}
	return t + min(max(p.Pad, 0), math.MaxInt-t)
}

// ApplyLimits bounds the final padding and hold of a handshake unit.
func ApplyLimits(p *UnitPlan, l *Limits) {
	if l.NoPadding {
		p.PadTo, p.PadMultiple, p.Pad = 0, 0, 0
	}
	if l.NoPause {
		p.Delay = 0
	}
	if l.Profile != nil {
		p.Delay = min(p.Delay, l.Profile.MaxAddedDelay)
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

// Capabilities a Shaper declares.
const (
	CapPlan           Caps = 1 << iota // Plan is called
	CapStreams                         // EventStreamOpen and EventStreamEnd are delivered
	CapBudget                          // RecordView.Meters and Event.Meters are filled
	CapDatagramPlan                    // PlanDatagram is called
	CapOutbound                        // Outbound is delivered after carrier acceptance
	CapIncludeControl                  // control-only units also count as Outbound
)

// DatagramPlanner is an optional size and timing planner for the native channel.
// DatagramPlan gives the selected native unit its timing and padding.
type DatagramPlanner interface {
	PlanDatagram(v *DatagramView, p *DatagramPlan)
}

// DatagramView measures the bytes the channel interface takes, including any
// zmux Pipe Stream ID. Limit is the structural capacity, and Room is the
// current capacity. The carrier has already accounted for its own envelope.
type DatagramView struct {
	Pipe             uint64
	Now              time.Duration
	Len, Limit, Room int
	Padding, Replica bool
	Meters           Meters
}

// DatagramPlan gives the selected native unit its timing and padding.
type DatagramPlan struct {
	UnitPlan
	Effects Effects
}

// UnitKind identifies an outbound carrier unit.
type UnitKind uint8

// Outbound unit kinds.
const (
	Record UnitKind = iota
	NativeDatagram
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

// Event kinds.
const (
	EventStart                     EventKind = iota + 1 // the instance was installed
	EventReplaced                                       // the instance was replaced; no further calls
	EventOutbound                                       // carrier accepted a complete unit
	EventInjectionAccepted                              // an injection was irreversibly submitted
	EventInjectionRejected                              // an injection could not be submitted
	EventTimer                                          // Effects.Wake was reached
	EventStopped                                        // shaping of the pipe ended; no further calls
	EventPipeFailed                                     // the pipe's outbound direction failed; no further calls
	EventStreamOpen                                     // a stream got its identifier (CapStreams)
	EventStreamEnd                                      // a stream's state was discarded (CapStreams)
	EventInjectionCarrierCompleted                      // carrier progress on committed cover
)

// Event is a notification.
type Event struct {
	Meters        Meters // CapBudget
	RequestID     uint64
	WireCost      int
	Pipe, Lane    uint64
	Now           time.Duration // session clock
	Len           int           // frames length, bytes (Outbound, InjectionAccepted)
	Stream        uint64        // EventStreamOpen, EventStreamEnd: stream ID
	Kind          EventKind
	Unit          UnitKind
	Data, Control bool
	Reason        RuntimeReason
}

// EffectSet says which items of Effects change.
type EffectSet uint8

// Effect items.
const (
	EffectInject EffectSet = 1 << iota
	EffectWake
	EffectGate
	EffectWeight
	EffectStop
)

// Effects is the answer to an event. The session holds one of each item per
// pipe; what Set does not name stays as it was. Times are on the session
// clock. The core consumes requests and wake times once.
type Effects struct {
	Injection  Injection     // EffectInject: ID zero clears the slot
	Wake       time.Duration // HasWake: when to deliver EventTimer
	PauseUntil time.Duration // EffectGate: stream data waits until then
	Rate       uint64        // bytes per second; 0: no limit
	Burst      uint64        // bytes; 0: the record limit
	Weight     uint32        // EffectWeight: PickPipe.ShapingWeight; 0 is ignored
	Set        EffectSet
	HasWake    bool // EffectWake: a wake time is set; false clears it
}

// Injection requests padding on the pipe's reliable lane 0.
type Injection struct {
	ID       uint64
	At       time.Duration
	PadBytes int
}

// RuntimeReason identifies a shaping outcome or clamp.
type RuntimeReason uint8

// Reasons used by the outbound shaping engine.
const (
	ReasonNone RuntimeReason = iota
	ReasonUnsupportedCapability
	ReasonClippedRoom
	ReasonPolicyDeadline
	ReasonApplicationExpired
	ReasonInsufficientCoverCredit
	ReasonOverheadLimit
	ReasonStaleGeneration
	ReasonCarrierRejected
	ReasonPartialProgress
	ReasonCancelled
	ReasonDisabled
	ReasonRateLimit
	runtimeReasons
)

var runtimeReasonNames = [...]string{
	"", "unsupported_capability", "clipped_room", "policy_deadline", "application_expired",
	"insufficient_cover_credit", "overhead_limit", "stale_generation", "carrier_rejected",
	"partial_progress", "cancelled", "disabled", "rate_limit",
}

// String returns the reason's name in the shaping contract.
func (r RuntimeReason) String() string {
	if int(r) < len(runtimeReasonNames) {
		return runtimeReasonNames[r]
	}
	return "unknown"
}

// MinMaxLen is the smallest largest record honoured, in frame bytes (64, a
// STREAM frame header and some data); a smaller one is raised to it
// (Stats.Clamped), together with a PadTo equal to it.
const MinMaxLen = 64

// The largest values of an Account's fields.
const (
	MaxAccountInitial = 1 << 53
	MaxAccountRate    = 1 << 43
)

// Account bounds accepted cover across one session's outbound direction.
type Account struct {
	Ceiling uint64
	Initial uint64
	PerMB   uint64
	PerSec  uint64
}

// Valid reports whether an optional account is within its ranges.
func (a *Account) Valid() bool {
	return a == nil || a.Ceiling <= MaxAccountInitial && a.Initial <= a.Ceiling &&
		a.PerMB <= MaxAccountRate && a.PerSec <= MaxAccountRate
}

// ProfileLimits bound one direction at the carrier interface.
type ProfileLimits struct {
	WindowSeconds uint32
	ExtraBytesPPM uint32
	FloorBytes    uint64
	MaxAddedDelay time.Duration
}

// Lightweight returns the default limits for an enabled selection.
func Lightweight() ProfileLimits {
	return ProfileLimits{WindowSeconds: 10, ExtraBytesPPM: 50000, FloorBytes: 540, MaxAddedDelay: 2 * time.Millisecond}
}

// Valid checks the representable profile fields.
func (p *ProfileLimits) Valid() bool {
	return p == nil || p.WindowSeconds != 0 && p.ExtraBytesPPM <= 1_000_000 &&
		p.FloorBytes <= MaxAccountInitial && p.MaxAddedDelay >= 0 && p.MaxAddedDelay <= 1_000_000_000_000_000*time.Microsecond
}

// Allowance returns the shared incremental byte limit for the proved baseline.
func (p *ProfileLimits) Allowance(b uint64) uint64 {
	hi, lo := bits.Mul64(b, uint64(p.ExtraBytesPPM))
	if hi >= 1_000_000 {
		return math.MaxUint64
	}
	q, _ := bits.Div64(hi, lo, 1_000_000)
	return max(q, p.FloorBytes)
}

// HandshakeRoom returns the one-shot padding allowance of this message. The
// planner selects the target before encoding; framing and protocol room may
// tighten it. A second message cannot spend the unused difference.
func HandshakeRoom(base, physicalRoom int, p *UnitPlan) int {
	if p == nil || base < 0 {
		return 0
	}
	basis := base
	if p.PadTo > 0 {
		// A message already larger than its chosen target needs no fill.
		basis = 0
	}
	return join.RawFill(PadFor(basis, p)-base, min(physicalRoom, join.HandshakeFloor))
}

// HandshakeWrite transfers one completed outbound Establish layout.
type HandshakeWrite struct {
	At               time.Time
	Baseline         uint64
	Allowance, Extra uint64
	Delay            time.Duration
}

// HandshakeWrites holds the at most two outbound messages of an Establish.
type HandshakeWrites [2]HandshakeWrite

// Fits validates the one-shot accounts and the effective per-unit delay.
func (h HandshakeWrites) Fits(policy, local *ProfileLimits) bool {
	for i, u := range h {
		if u == (HandshakeWrite{}) {
			continue
		}
		if u.At.IsZero() || i > 0 && (h[0].At.IsZero() || u.At.Before(h[0].At)) || u.Allowance > join.HandshakeFloor || u.Extra > u.Allowance || u.Delay < 0 ||
			policy != nil && u.Delay > policy.MaxAddedDelay || local != nil && u.Delay > local.MaxAddedDelay {
			return false
		}
	}
	return true
}

// Limits are local bounds over a session.
type Limits struct {
	Wire        *Account
	Profile     *ProfileLimits
	NoPadding   bool
	NoPause     bool
	NoInjection bool
}

// Budget is what a shaping configuration allows for a session.
type Budget struct {
	// RequiredGateWait is the longest rate recovery certified by the compiler.
	RequiredGateWait time.Duration
	MinGateRate      uint64
	Wire             *Account
	Profile          *ProfileLimits
	NoInjection      bool
	Boundary         Boundary
}

// Fits checks the rate certificate against both applicable delay bounds.
func (b Budget) Fits(l Limits) bool {
	if b.MinGateRate > MaxAccountRate {
		return false
	}
	p := b.Profile
	if p == nil {
		v := Lightweight()
		p = &v
	}
	return b.RequiredGateWait >= 0 && b.RequiredGateWait <= p.MaxAddedDelay && (l.Profile == nil || b.RequiredGateWait <= l.Profile.MaxAddedDelay)
}

// WithPreamble includes a reachable indivisible preamble in the rate certificate.
func (b Budget) WithPreamble(maxBytes uint64) Budget {
	if b.MinGateRate != 0 {
		b.RequiredGateWait = max(b.RequiredGateWait, GateWait(b.MinGateRate, 0, maxBytes))
	}
	return b
}

// GateWait gives the recovery time after draining a bucket or sending an
// indivisible preamble from the smallest balance that permits dispatch.
func GateWait(rate, burst, preamble uint64) time.Duration {
	if rate == 0 {
		return 0
	}
	quantum := max(uint64(MinMaxLen), min(burst, rate/1000))
	need := max(quantum, preamble)
	return time.Duration((need*1_000_000+rate-1)/rate) * time.Microsecond
}

// Clone takes ownership of a configuration snapshot.
func (l Limits) Clone() Limits {
	if l.Wire != nil {
		a := *l.Wire
		l.Wire = &a
	}
	if l.Profile != nil {
		p := *l.Profile
		l.Profile = &p
	}
	return l
}

// Clone takes ownership of a budget snapshot.
func (b Budget) Clone() Budget {
	l := (Limits{Wire: b.Wire, Profile: b.Profile}).Clone()
	b.Wire, b.Profile = l.Wire, l.Profile
	return b
}

// With applies the smaller bound from each configured object.
func (l Limits) With(b Budget) Limits {
	switch {
	case b.Wire == nil || l.Wire == nil:
		l.Wire = b.Wire
	case b.Wire != nil:
		a, b := l.Wire, b.Wire
		l.Wire = &Account{Ceiling: min(a.Ceiling, b.Ceiling), Initial: min(a.Initial, b.Initial),
			PerMB: min(a.PerMB, b.PerMB), PerSec: min(a.PerSec, b.PerSec)}
	}
	switch {
	case l.Profile == nil:
		l.Profile = b.Profile
	case b.Profile != nil:
		profile := *b.Profile
		profile.MaxAddedDelay = min(profile.MaxAddedDelay, l.Profile.MaxAddedDelay)
		l.Profile = &profile
	}
	l.NoInjection = l.NoInjection || b.NoInjection
	return l
}

// Boundary identifies the observation boundary of the overhead meter.
type Boundary uint8

// BoundaryCarrierInterface measures overhead where zmux submits encoded units.
const BoundaryCarrierInterface Boundary = 1

// Precision describes how much of the incremental cost the carrier proved.
type Precision string

// Supported precision readings at the carrier boundary.
const (
	CostExact       Precision = "exact"
	CostUnavailable Precision = "unavailable"
)

// WindowMeters describes one applicable sliding admission test.
type WindowMeters struct {
	ProfileLimits
	BaselineBytes, ConsumedBytes, ReservedBytes, AllowanceBytes uint64
	HistoryComplete                                             bool
}

// HandshakeMeters counts optional structural bytes in this sending direction.
// UnresolvedBytes is the upper bound left by writes with unknown progress.
type HandshakeMeters struct {
	ReservedBytes, ConsumedBytes, UnresolvedBytes uint64
}

// Meters reports accepted overhead and confirmed own bytes.
type Meters struct {
	Handshake                                               HandshakeMeters
	Boundary                                                Boundary
	Bounded                                                 bool // a profile enforces both limits at this boundary
	WireLeft, StallLeft, WireSpent, StallSpent, EarnedBytes uint64
	Profile, Local                                          WindowMeters
	ConsumedBytes                                           uint64 // lifetime measured ongoing overhead
	Precision                                               Precision
	MaxAddedDelay                                           time.Duration
}

// WirePPM is s bytes in parts per million of e, rounded down.
func WirePPM(s, e uint64) uint64 {
	if s == 0 {
		return 0
	}
	hi, lo := bits.Mul64(s, 1_000_000)
	if hi >= e {
		return math.MaxUint64
	}
	q, _ := bits.Div64(hi, lo, e)
	return q
}

// Stats are counters over a session.
type Stats struct {
	MaxAddedDelay          time.Duration
	WireSpent              uint64        // cumulative cover account debits
	StallSpent             time.Duration // waits applied
	Clamped                uint64        // decisions reduced by a structural limit, MaxAddedDelay or a No switch
	InjectionAcceptedBytes uint64
	InjectionSentBytes     uint64
	InjectionAccepted      uint64
	InjectionRejected      uint64
	InjectionRejections    [runtimeReasons]uint64
	RuntimeReasons         [runtimeReasons]uint64
	Faults                 uint64 // calls that panicked
	HandshakeUnplanned     uint64 // Join messages without a plan under a HandshakeShaper
}
