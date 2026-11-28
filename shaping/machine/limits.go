package machine

import (
	"math"
	"math/rand/v2"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

type budgetDef struct {
	requiredWait time.Duration
	minRate      uint64
	profile      *shaping.ProfileLimits
	wire         [4]value
	hasWire      bool
	noInjection  bool
}

var budgetFields = [4]struct {
	key string
	fam family
}{{"ceiling", famCounter}, {"initial", famCounter}, {"per_mb", famRate}, {"per_sec", famRate}}

func parseAccount(x any, t *tables) ([4]value, error) {
	var fields [4]value
	o, err := members(x, "wire", "ceiling", "initial", "per_mb", "per_sec")
	if err != nil {
		return fields, err
	}
	if _, err := required(o, "ceiling", "wire"); err != nil {
		return fields, err
	}
	for i, f := range budgetFields {
		y := o.get(f.key)
		if y == nil {
			continue
		}
		if t == nil {
			n, err := integer(y, f.key)
			if err != nil {
				return fields, err
			}
			if n < 0 || uint64(n) > f.fam.hi {
				return fields, fail("range", "%s: %d", f.key, n)
			}
			fields[i].c = uint64(n)
		} else if fields[i], err = parseValue(y, f.fam, t, false, f.key); err != nil {
			return fields, err
		}
	}
	ceiling := fields[0].lower()
	initial := fields[1].c
	for _, seg := range fields[1].segs {
		initial = max(initial, seg.hi)
	}
	if initial > ceiling {
		return fields, fail("range", "initial exceeds ceiling")
	}
	return fields, nil
}

func parseProfile(x any) (*shaping.ProfileLimits, error) {
	o, err := members(x, "profile", "extra_bytes_ppm", "max_added_delay", "floor_bytes", "window_seconds")
	if err != nil {
		return nil, err
	}
	ppm, err := profileNumber(o, "extra_bytes_ppm", 1_000_000)
	if err != nil {
		return nil, err
	}
	delay, err := profileNumber(o, "max_added_delay", famTime.hi)
	if err != nil {
		return nil, err
	}
	window, err := profileNumber(o, "window_seconds", math.MaxUint32)
	if err != nil {
		return nil, err
	}
	if window == 0 {
		return nil, fail("range", "window_seconds must be positive")
	}
	floor, err := profileNumber(o, "floor_bytes", shaping.MaxAccountInitial)
	if err != nil {
		return nil, err
	}
	return &shaping.ProfileLimits{WindowSeconds: uint32(window), ExtraBytesPPM: uint32(ppm), FloorBytes: floor, MaxAddedDelay: micros(delay)}, nil
}

func profileNumber(o *jobject, key string, limit uint64) (uint64, error) {
	v, err := required(o, key, "profile")
	if err != nil {
		return 0, err
	}
	n, err := integer(v, key)
	if err != nil {
		return 0, err
	}
	if n < 0 || uint64(n) > limit {
		return 0, fail("range", "%s: %d", key, n)
	}
	return uint64(n), nil
}

func parseBudget(x any, t *tables) (*budgetDef, error) {
	o, err := members(x, "budget", "wire", "profile", "no_injection")
	if err != nil {
		return nil, err
	}
	b := &budgetDef{}
	if v := o.get("wire"); v != nil {
		b.hasWire = true
		saved := t.budget
		t.budget = true
		b.wire, err = parseAccount(v, t)
		t.budget = saved
		if err != nil {
			return nil, err
		}
	}
	if v := o.get("profile"); v != nil {
		if b.profile, err = parseProfile(v); err != nil {
			return nil, err
		}
	}
	if v := o.get("no_injection"); v != nil {
		if b.noInjection, err = boolean(v, "no_injection"); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (b *budgetDef) draw(src rand.Source) shaping.Budget {
	out := shaping.Budget{RequiredGateWait: b.requiredWait, MinGateRate: b.minRate, NoInjection: b.noInjection, Profile: b.profile}
	if b.hasWire {
		out.Wire = drawAccount(&b.wire, src)
	}
	return out.Clone()
}

func drawAccount(a *[4]value, src rand.Source) *shaping.Account {
	return &shaping.Account{Ceiling: a[0].drawTable(src), Initial: a[1].drawTable(src),
		PerMB: a[2].drawTable(src), PerSec: a[3].drawTable(src)}
}

func boolean(v any, key string) (bool, error) {
	b, ok := v.(bool)
	if !ok {
		return false, fail("type", "%s", key)
	}
	return b, nil
}

// ParseLimits reads the local account and profile. Durations are in microseconds.
func ParseLimits(b []byte) (shaping.Limits, error) {
	var l shaping.Limits
	x, err := decode(b)
	if err != nil {
		return l, err
	}
	o, err := members(x, "limits", "wire", "profile", "no_padding", "no_pause", "no_injection")
	if err != nil {
		return l, err
	}
	for k, to := range map[string]*bool{"no_padding": &l.NoPadding, "no_pause": &l.NoPause, "no_injection": &l.NoInjection} {
		if v := o.get(k); v != nil {
			if *to, err = boolean(v, k); err != nil {
				return shaping.Limits{}, err
			}
		}
	}
	if v := o.get("wire"); v != nil {
		fields, err := parseAccount(v, nil)
		if err != nil {
			return shaping.Limits{}, err
		}
		l.Wire = drawAccount(&fields, nil)
	}
	if v := o.get("profile"); v != nil {
		if l.Profile, err = parseProfile(v); err != nil {
			return shaping.Limits{}, err
		}
	}
	return l, nil
}

// checkGates checks all reachable rates without enumerating individual draws.
// With a fixed burst the recovery time decreases between quantum changes.
// The first following multiple of 1000 attains the worst one-ms quantum wait.
func (c *Candidate) checkGates() error {
	var required time.Duration
	var slowest uint64
	for _, m := range c.machines {
		for _, st := range m.states {
			for _, a := range st.enter {
				if a.op != opRate {
					continue
				}
				burst := a.b.c
				if burst == 0 {
					burst = wire.MaxRecordSize
				}
				if a.b.segs != nil {
					burst = 0
					for _, v := range a.b.segs {
						if v.w != 0 {
							burst = max(burst, v.hi)
							if v.lo == 0 {
								burst = max(burst, wire.MaxRecordSize)
							}
						}
					}
				}
				check := func(lo, hi uint64) {
					if hi == 0 {
						return
					}
					lo = max(lo, 1)
					if slowest == 0 || lo < slowest {
						slowest = lo
					}
					required = max(required, shaping.GateWait(lo, burst, 0))
					if next := (lo + 999) / 1000 * 1000; next <= hi {
						required = max(required, shaping.GateWait(next, burst, 0))
					}
				}
				if a.a.segs == nil {
					check(a.a.c, a.a.c)
				}
				for _, v := range a.a.segs {
					if v.w != 0 {
						check(v.lo, v.hi)
					}
				}
			}
		}
	}
	if required == 0 {
		return nil
	}
	if c.budget == nil {
		c.budget = &budgetDef{}
	}
	c.budget.requiredWait, c.budget.minRate = required, slowest
	if !(shaping.Budget{Profile: c.budget.profile, RequiredGateWait: required}).Fits(shaping.Limits{}) {
		return fail("conflict", "rate and burst require %v of added delay", required)
	}
	return nil
}
