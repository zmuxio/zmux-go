package zmux

import (
	"fmt"
	"net"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// DatagramLossDelta describes authoritative finalized outcomes for originals
// on one outgoing path and representation. At uses the session clock.
type DatagramLossDelta struct {
	At              time.Time
	Generation      uint64 // nonzero source generation for this representation
	Finalized, Lost uint64
	Native          bool // true for the carrier channel, false for records
}

// ErrDatagramEvidence rejects stale or invalid outgoing outcome observations.
var ErrDatagramEvidence = core.ErrDatagramEvidence

// ErrDatagramStaleSource means the engine replaced the outgoing source.
// The provider must report from the replacement with a higher generation.
var ErrDatagramStaleSource = core.ErrDatagramStaleSource

// ReportDatagramLoss records a provider delta for Adaptive. Feed only
// finalized original outcomes from this pipe and representation. Missing ACKs
// and aggregate carrier loss do not establish a datagram outcome.
//
// Each representation starts with no registered source. Generation zero is
// invalid. A higher generation starts a fresh observation sequence, including
// timestamps. A higher generation with zero finalized outcomes declares the
// new source and may omit At. Such a declaration preserves existing evidence
// when it registers the first source. If the engine replaces the outgoing
// attachment or channel, a report using its expired generation returns
// ErrDatagramStaleSource.
func (p *Pipe) ReportDatagramLoss(delta DatagramLossDelta) error {
	return p.updateDatagramSource(func(c *Conn) error {
		at := core.Never
		if !delta.At.IsZero() {
			at = core.Time(delta.At.Sub(c.clk.base))
		}
		return c.s.ReportDatagramLoss(p.id, delta.Generation, delta.Native, at, delta.Finalized, delta.Lost)
	})
}

// SetDatagramSourceGeneration explicitly replaces the outgoing evidence
// source for records or, when native is true, for the carrier channel.
// generation must be strictly higher than the representation's current
// generation. This clears its evidence, including when installing generation 1.
// Use a zero-count report to declare the first source while retaining evidence.
func (p *Pipe) SetDatagramSourceGeneration(native bool, generation uint64) error {
	return p.updateDatagramSource(func(c *Conn) error {
		return c.s.SetDatagramSourceGeneration(p.id, native, generation)
	})
}

func (p *Pipe) updateDatagramSource(update func(*Conn) error) error {
	if p == nil || p.c == nil {
		return net.ErrClosed
	}
	c := p.c
	c.lock()
	defer c.unlock()
	if pd := c.pipes[p.id]; pd == nil || pd.end != p.end || pd.retiring {
		return ErrPipeGone
	}
	c.s.SetNow(c.clk.now())
	return update(c)
}

// DatagramCopyMode selects the local replica policy. Zero inherits.
type DatagramCopyMode uint8

// Datagram copy strategies.
const (
	DatagramSingle DatagramCopyMode = iota + 1
	DatagramDuplicate
	DatagramAdaptive
)

// DatagramCarrierMode selects the allowed carrier for a datagram. Zero inherits.
type DatagramCarrierMode uint8

// Datagram carrier requirements.
const (
	DatagramPreferNative DatagramCarrierMode = iota + 1
	DatagramRequireNative
)

// DatagramDeadline applies to one hop. A zero value explicitly removes a
// deadline when set as an option.
type DatagramDeadline struct {
	at   time.Time
	ttl  time.Duration
	mode core.DatagramDeadlineMode
}

// DatagramAfter sets a relative deadline. Zero expires at admission.
func DatagramAfter(ttl time.Duration) DatagramDeadline {
	return DatagramDeadline{mode: core.DatagramDeadlineAfter, ttl: ttl}
}

// DatagramAt sets an absolute deadline on the connection's clock.
func DatagramAt(at time.Time) DatagramDeadline {
	return DatagramDeadline{mode: core.DatagramDeadlineAt, at: at}
}

// DatagramSendOptions overrides the defaults of a Flow and its sending session.
type DatagramSendOptions struct {
	Deadline                Change[DatagramDeadline]
	Copies                  int // total copies for DatagramDuplicate
	Urgent, IndependentFate Change[bool]
	Copy                    DatagramCopyMode
	Carrier                 DatagramCarrierMode
}

func (o DatagramSendOptions) validate() error {
	if o.Copy > DatagramAdaptive || o.Carrier > DatagramRequireNative ||
		o.Copy == DatagramDuplicate && o.Copies < 1 || o.Copy != DatagramDuplicate && o.Copies != 0 {
		return fmt.Errorf("%w: invalid datagram policy, carrier or copy count", ErrInvalidConfig)
	}
	if d, ok := o.Deadline.get(); ok && (d.ttl < 0 || d.mode == core.DatagramDeadlineAt && d.at.IsZero()) {
		return fmt.Errorf("%w: invalid datagram deadline", ErrInvalidConfig)
	}
	return nil
}

func (c clock) datagramOptions(o DatagramSendOptions) core.DatagramOptions {
	out := core.DatagramOptions{Copy: uint8(o.Copy), Copies: o.Copies, Carrier: uint8(o.Carrier)}
	out.Urgent, out.HasUrgent = o.Urgent.get()
	out.Independent, out.HasIndependent = o.IndependentFate.get()
	if d, set := o.Deadline.get(); set {
		out.DeadlineMode, out.TTL = d.mode, core.Duration(d.ttl)
		if d.mode == core.DatagramDeadlineInherit {
			out.DeadlineMode = core.DatagramDeadlineNever
		}
		if d.mode == core.DatagramDeadlineAt {
			out.Deadline = core.Time(d.at.Sub(c.base))
		}
	}
	return out
}

// SetDatagramDefaults changes the options inherited by subsequent sends on s.
func (s *Stream) SetDatagramDefaults(o DatagramSendOptions) error {
	if s == nil {
		return net.ErrClosed
	}
	if err := o.validate(); err != nil {
		return err
	}
	h := s.h
	return h.do(func(session *core.Session) error {
		return session.SetDatagramOptions(h.st, h.c.clk.datagramOptions(o))
	})
}

// DatagramPathQuality is bounded arrival evidence for one outgoing path
// and transport. RoundTrip includes the reverse path used by the report.
type DatagramPathQuality struct {
	SampledAt                            time.Time
	Tracking, Awaiting, Arrived, Expired uint64
	RoundTrip, Variation                 time.Duration
	Samples                              uint64
}

// DatagramPath describes the current outgoing payload limits in bytes.
type DatagramPath struct {
	MaxPayload, PathPayload, NativePayload int
	Native                                 bool
}

// DatagramDropReason is a local admission refusal.
type DatagramDropReason = core.DatagramDropReason

// Reasons a datagram can be dropped before carrier handoff.
const (
	DatagramDropDeadline          = core.DatagramDropDeadline
	DatagramDropNoNativePath      = core.DatagramDropNoNativePath
	DatagramDropLimiter           = core.DatagramDropLimiter
	DatagramDropQueueOverflow     = core.DatagramDropQueueOverflow
	DatagramDropSequenceExhausted = core.DatagramDropSequenceExhausted
)

// DatagramDroppedError describes a send which did not enter the queue.
type DatagramDroppedError = core.DatagramDroppedError
