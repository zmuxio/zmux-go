package machine

import (
	"math/rand/v2"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// budgetDef is a parsed budget.
type budgetDef struct {
	wire     [3]value // initial, per_mb, per_sec
	hasWire  bool
	maxDelay time.Duration
}

var budgetFields = [3]struct {
	k   string
	fam family
}{{"initial", famCounter}, {"per_mb", famRate}, {"per_sec", famRate}}

func parseBudget(x any, t *tables) (*budgetDef, error) {
	o, err := members(x, "budget", "wire", "max_delay")
	if err != nil {
		return nil, err
	}
	b := &budgetDef{}
	if v := o.get("wire"); v != nil {
		w, err := members(v, "wire", "initial", "per_mb", "per_sec")
		if err != nil {
			return nil, err
		}
		b.hasWire = true
		saved := t.budget
		t.budget = true
		defer func() { t.budget = saved }()
		for i, f := range budgetFields {
			if y := w.get(f.k); y != nil {
				if b.wire[i], err = parseValue(y, f.fam, t, false, f.k); err != nil {
					return nil, err
				}
			}
		}
	}
	if v := o.get("max_delay"); v != nil {
		n, err := integer(v, "max_delay")
		if err != nil {
			return nil, err
		}
		if n < 1 || uint64(n) > famTime.hi {
			return nil, fail("range", "max_delay: %d", n)
		}
		b.maxDelay = micros(uint64(n))
	}
	return b, nil
}

// draw is one session's budget: initial, per_mb, per_sec in order. A wire of
// zeros allows no injection.
func (b *budgetDef) draw(src rand.Source) shaping.Budget {
	out := shaping.Budget{MaxDelay: b.maxDelay}
	if b.hasWire {
		out.Wire = shaping.Account{Initial: b.wire[0].drawTable(src), PerMB: b.wire[1].drawTable(src), PerSec: b.wire[2].drawTable(src)}
		out.NoInjection = out.Wire == shaping.Account{}
	}
	return out
}

// ParseLimits reads JSON local limits: wire of integer initial in [0, 2^53] and
// per_mb, per_sec in [0, 2^43]; max_delay in microseconds up to 10^15, 0 the
// default and negative no bound; and the switches no_padding, no_pause and
// no_injection. A wire of zeros maps to NoInjection.
func ParseLimits(b []byte) (shaping.Limits, error) {
	var l shaping.Limits
	x, err := decode(b)
	if err != nil {
		return l, err
	}
	o, err := members(x, "limits", "wire", "max_delay", "no_padding", "no_pause", "no_injection")
	if err != nil {
		return l, err
	}
	for k, to := range map[string]*bool{"no_padding": &l.NoPadding, "no_pause": &l.NoPause, "no_injection": &l.NoInjection} {
		if v := o.get(k); v != nil {
			f, ok := v.(bool)
			if !ok {
				return shaping.Limits{}, fail("type", "%s", k)
			}
			*to = f
		}
	}
	if v := o.get("wire"); v != nil {
		w, err := members(v, "wire", "initial", "per_mb", "per_sec")
		if err != nil {
			return shaping.Limits{}, err
		}
		to := [3]*uint64{&l.Wire.Initial, &l.Wire.PerMB, &l.Wire.PerSec}
		for i, f := range budgetFields {
			y := w.get(f.k)
			if y == nil {
				continue
			}
			n, err := integer(y, f.k)
			if err != nil {
				return shaping.Limits{}, err
			}
			if n < 0 || uint64(n) > f.fam.hi {
				return shaping.Limits{}, fail("range", "%s: %d", f.k, n)
			}
			*to[i] = uint64(n)
		}
		l.NoInjection = l.NoInjection || l.Wire == shaping.Account{}
	}
	if v := o.get("max_delay"); v != nil {
		n, err := integer(v, "max_delay")
		if err != nil {
			return shaping.Limits{}, err
		}
		if n > int64(famTime.hi) {
			return shaping.Limits{}, fail("range", "max_delay: %d", n)
		}
		if n < 0 {
			n = -1
		}
		l.MaxDelay = time.Duration(n) * time.Microsecond
	}
	return l, nil
}
