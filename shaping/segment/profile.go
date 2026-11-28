// Package segment compiles deployment shaping tables before installation.
package segment

import (
	"errors"
	"slices"
	"sort"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

// Errors rejected before a segment profile is installed.
var (
	ErrRange      = errors.New("range")
	ErrVersion    = errors.New("unsupported_version")
	ErrCapability = errors.New("unsupported_capability")
)

// Range describes a half-open interval of unshaped encoded lengths.
type Range struct {
	Kind   shaping.UnitKind
	Lo, Hi int
	Plan   shaping.UnitPlan
}

// Profile is an immutable bounds and segment table installation.
type Profile struct {
	limits shaping.ProfileLimits
	native []Range
}

// Compile validates the entire table before it can replace a live profile.
func Compile(version uint32, limits shaping.ProfileLimits, ranges []Range) (*Profile, error) {
	if version != 1 {
		return nil, ErrVersion
	}
	if !limits.Valid() {
		return nil, ErrRange
	}
	var previous int
	maxUnit := wire.MaxRecordSize + varint.SizeVarint(varint.MaxVarint)
	for _, r := range ranges {
		if r.Kind != shaping.NativeDatagram {
			return nil, ErrCapability
		}
		if r.Lo < previous || r.Hi <= r.Lo || r.Lo < 0 || r.Hi > maxUnit+1 || r.Plan.Pad < 0 || r.Plan.Pad > maxUnit ||
			r.Plan.PadTo < 0 || r.Plan.PadTo > maxUnit || r.Plan.PadMultiple < 0 || r.Plan.PadMultiple > maxUnit ||
			r.Plan.Delay < 0 {
			return nil, ErrRange
		}
		previous = r.Hi
	}
	return &Profile{limits: limits, native: slices.Clone(ranges)}, nil
}

// Limits returns a value copy of the compiled hard bounds.
func (p *Profile) Limits() shaping.ProfileLimits { return p.limits }

// Plan returns the compiled native plan for a reachable encoded length.
func (p *Profile) Plan(length int) shaping.UnitPlan {
	i := sort.Search(len(p.native), func(i int) bool { return p.native[i].Hi > length })
	if i < len(p.native) && p.native[i].Lo <= length {
		return p.native[i].Plan
	}
	return shaping.UnitPlan{}
}

// Shaper makes a pipe-local planner backed by the immutable profile.
func (p *Profile) Shaper() shaping.Shaper { return nativeShaper{p} }

type nativeShaper struct{ profile *Profile }

func (nativeShaper) Capabilities() shaping.Caps                        { return shaping.CapDatagramPlan }
func (nativeShaper) Plan(_ *shaping.RecordView, _ *shaping.RecordPlan) {}
func (nativeShaper) OnEvent(_ *shaping.Event, _ *shaping.Effects)      {}
func (s nativeShaper) PlanDatagram(v *shaping.DatagramView, out *shaping.DatagramPlan) {
	out.UnitPlan = s.profile.Plan(v.Len)
}
