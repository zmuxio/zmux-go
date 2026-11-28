package carrier

import (
	"errors"
	"math/bits"
	"slices"
	"sync"
	"time"
)

// PhysicalDirection is the local direction at a billed observation point.
type PhysicalDirection uint8

// Local directions at the nominated observation boundary.
const (
	PhysicalSend PhysicalDirection = iota + 1
	PhysicalReceive
)

// PhysicalLegID identifies disjoint billed traffic. Reused pipes and lanes
// on the same leg use the same identity.
type PhysicalLegID struct {
	Line, Leg string
	Direction PhysicalDirection
}

// PhysicalLegConfig binds a nominated observer to its actual boundary and
// traffic scope. An empty observer declares a leg without physical coverage.
type PhysicalLegConfig struct {
	ID                        PhysicalLegID
	Observer, Boundary, Scope string
	Classes                   []string // empty denotes all traffic in Scope
}

// PhysicalCounter is a dated cumulative counter at the configured boundary.
// FromZero attests that the generation began at zero. MissingTraffic lists
// billed traffic classes or intervals absent from the provider's observation.
type PhysicalCounter struct {
	At             time.Time
	MissingTraffic []string
	Generation     uint64
	Bytes          uint64
	FromZero       bool
	Final          bool
}

// PhysicalPrecision describes the scope covered by known bytes.
type PhysicalPrecision string

// Coverage classifications for billed observations.
const (
	PhysicalUnavailable PhysicalPrecision = "unavailable"
	PhysicalLowerBound  PhysicalPrecision = "lower_bound"
	PhysicalExact       PhysicalPrecision = "exact"
)

// PhysicalGap identifies the generation and cause of missing coverage.
type PhysicalGap struct {
	Leg        PhysicalLegID
	Generation uint64
	Observer   string
	Reason     string
}

// PhysicalCoverage is a snapshot of known billed traffic and its coverage.
type PhysicalCoverage struct {
	Precision   PhysicalPrecision
	Scope       string
	Boundary    string
	Gaps        []PhysicalGap
	MissingLegs []PhysicalLegID
	KnownBytes  uint64
	LegsCounted int
	Final       bool
}

// Physical accounting input and counter errors.
var (
	ErrPhysicalConfiguration = errors.New("zmux: invalid physical accounting configuration")
	ErrPhysicalObserver      = errors.New("zmux: non-authoritative physical observer")
	ErrPhysicalObservation   = errors.New("zmux: invalid or stale physical observation")
	ErrPhysicalRegression    = errors.New("zmux: physical counter regression")
)

// PhysicalLedger reconciles host-supplied cumulative counters without
// querying any provider on the protocol or carrier I/O path.
type PhysicalLedger struct {
	mu    sync.Mutex
	legs  map[PhysicalLegID]*physicalLeg
	order []PhysicalLegID
}

type physicalLeg struct {
	config             PhysicalLegConfig
	at                 time.Time
	gaps               []PhysicalGap
	generation, latest uint64
	known              uint64
	cut, ended         bool
}

// NewPhysicalLedger defines one authoritative observer per billed leg and direction.
func NewPhysicalLedger(config []PhysicalLegConfig) (*PhysicalLedger, error) {
	if len(config) == 0 {
		return nil, ErrPhysicalConfiguration
	}
	l := &PhysicalLedger{legs: make(map[PhysicalLegID]*physicalLeg, len(config))}
	for _, c := range config {
		if !c.valid() || l.legs[c.ID] != nil {
			return nil, ErrPhysicalConfiguration
		}
		c.Classes = c.classes()
		l.legs[c.ID] = &physicalLeg{config: c}
		l.order = append(l.order, c.ID)
	}
	return l, nil
}

func (c PhysicalLegConfig) valid() bool {
	return c.ID.Line != "" && c.ID.Leg != "" && c.ID.Direction >= PhysicalSend && c.ID.Direction <= PhysicalReceive &&
		(c.Observer == "" || c.Boundary != "" && c.Scope != "")
}

func (c PhysicalLegConfig) classes() []string {
	classes := slices.Clone(c.Classes)
	slices.Sort(classes)
	return slices.Compact(classes)
}

func (p *physicalLeg) gap(reason string) {
	g := PhysicalGap{Leg: p.config.ID, Generation: p.generation, Observer: p.config.Observer, Reason: reason}
	if !slices.Contains(p.gaps, g) {
		p.gaps = append(p.gaps, g)
	}
}

// ReplaceObserver opens a new counter segment for the same billed leg. The
// initial sample is a baseline. Exact continuity requires the prior observer
// to finish at the same cut with the same boundary, scope and traffic classes.
func (l *PhysicalLedger) ReplaceObserver(next PhysicalLegConfig, baseline PhysicalCounter) error {
	if !next.valid() || next.Observer == "" || baseline.Generation == 0 || baseline.At.IsZero() {
		return ErrPhysicalConfiguration
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.legs[next.ID]
	if p == nil || p.ended || next.Observer == p.config.Observer || baseline.At.Before(p.at) {
		return ErrPhysicalObservation
	}
	next.Classes = next.classes()
	if !p.cut || !p.at.Equal(baseline.At) || p.config.Boundary != next.Boundary || p.config.Scope != next.Scope ||
		!slices.Equal(p.config.Classes, next.Classes) {
		p.gap("observer_cut")
	}
	p.config, p.at = next, baseline.At
	p.generation, p.latest, p.cut = baseline.Generation, baseline.Bytes, baseline.Final
	for _, missing := range baseline.MissingTraffic {
		p.gap(missing)
	}
	return nil
}

// Observe commits a cumulative counter from the currently nominated observer.
func (l *PhysicalLedger) Observe(id PhysicalLegID, observer string, s PhysicalCounter) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.legs[id]
	if p == nil || observer == "" || observer != p.config.Observer {
		return ErrPhysicalObserver
	}
	if p.ended || s.Generation == 0 || s.At.IsZero() || s.Generation < p.generation || s.At.Before(p.at) {
		return ErrPhysicalObservation
	}
	if s.Generation == p.generation && s.Bytes < p.latest {
		p.gap("counter_regression")
		return ErrPhysicalRegression
	}
	if s.Generation == p.generation && p.cut && s.Bytes != p.latest {
		return ErrPhysicalObservation
	}
	if s.Generation != p.generation {
		restarting := p.generation != 0
		if restarting && !p.cut {
			p.gap("tail")
		}
		p.generation, p.latest, p.cut = s.Generation, 0, false
		if !s.FromZero {
			p.latest = s.Bytes
			if restarting {
				p.gap("head")
			}
		}
	}
	for _, missing := range s.MissingTraffic {
		p.gap(missing)
	}
	delta := s.Bytes - p.latest
	sum, carry := bits.Add64(p.known, delta, 0)
	p.known = sum | -carry
	if carry != 0 {
		p.gap("overflow")
	}
	p.latest, p.at, p.cut = s.Bytes, s.At, p.cut || s.Final
	return nil
}

// Finish seals the billed leg. If the provider supplied no final cut, its
// known totals remain available with a closing-tail gap.
func (l *PhysicalLedger) Finish(id PhysicalLegID) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.legs[id]
	if p == nil {
		return ErrPhysicalConfiguration
	}
	if p.generation != 0 && !p.cut {
		p.gap("tail")
	}
	p.ended = true
	return nil
}

// Snapshot reads the covered aggregate. A nil filter selects all legs.
func (l *PhysicalLedger) Snapshot(only *PhysicalLegID) PhysicalCoverage {
	l.mu.Lock()
	defer l.mu.Unlock()
	view := PhysicalCoverage{Precision: PhysicalUnavailable, Final: true}
	for _, id := range l.order {
		if only != nil && *only != id {
			continue
		}
		p := l.legs[id]
		view.Final = view.Final && (p.cut || p.ended)
		if p.generation == 0 {
			view.MissingLegs = append(view.MissingLegs, id)
			continue
		}
		if view.LegsCounted == 0 {
			view.Scope, view.Boundary = p.config.Scope, p.config.Boundary
		} else {
			if view.Scope != p.config.Scope {
				view.Scope = "mixed"
			}
			if view.Boundary != p.config.Boundary {
				view.Boundary = "mixed"
			}
		}
		sum, carry := bits.Add64(view.KnownBytes, p.known, 0)
		view.KnownBytes = sum | -carry
		view.LegsCounted++
		view.Gaps = append(view.Gaps, p.gaps...)
		if carry != 0 {
			view.Gaps = append(view.Gaps, PhysicalGap{Leg: id, Reason: "overflow"})
		}
	}
	if view.LegsCounted != 0 {
		view.Precision = PhysicalLowerBound
		if view.Final && len(view.Gaps) == 0 && len(view.MissingLegs) == 0 {
			view.Precision = PhysicalExact
		}
	}
	return view
}
