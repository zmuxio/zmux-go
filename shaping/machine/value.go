package machine

import (
	crand "crypto/rand"
	"encoding/json"
	"math/bits"
	"math/rand/v2"
)

// family is the range of a kind of value.
type family struct{ lo, hi uint64 }

const maxCount = 1 << 53

var (
	famLength    = family{0, 1<<31 - 1}
	famTime      = family{0, 1_000_000_000_000_000}
	famPPM       = family{0, 1_000_000}
	famWeight    = family{0, 1<<32 - 1}
	famCounter   = family{0, maxCount}
	famRate      = family{0, 1 << 43}
	famPoolCount = family{1, maxCount}
)

type scope uint8

const (
	scopeEach scope = iota
	scopePipe
	scopeShared
	scopeSession
)

// seg is one segment of a table.
type seg struct{ w, lo, hi uint64 }

// value is a parsed value: a constant, or a table with a scope. A value
// resolved at compile time is a constant in the compiled copy.
type value struct {
	segs  []seg // nil: the constant c
	cum   []uint64
	total uint64
	low   uint64 // the least segment bound
	c     uint64
	sc    scope
	set   bool
	slot  int32 // scopePipe: its place in the pipe's slots
}

// minBinary is the smallest number of segments searched by bisection.
const minBinary = 8

// table is a parsed list of segments. References share it; it is not changed.
type table struct {
	segs   []seg
	cum    []uint64
	total  uint64
	lo, hi uint64 // over every segment, those of weight 0 included
}

// tables is the state of parsing one candidate: its table sources, those parsed so
// far, its limits and what it has used of them.
type tables struct {
	raw    map[string]json.RawMessage
	parsed map[string]*table
	l      SizeLimits
	bytes  int
	slots  int64
	budget bool // parsing a budget: its tables have scope session
}

// named returns the table called name, parsed at its first reference.
func (t *tables) named(name, what string) (*table, error) {
	if t == nil {
		return nil, fail("table", "%q", name)
	}
	if tb, ok := t.parsed[name]; ok {
		return tb, nil
	}
	b, ok := t.raw[name]
	if !ok {
		return nil, fail("table", "%q", name)
	}
	if err := t.useBytes(len(b)); err != nil {
		return nil, err
	}
	x, err := decode(b)
	if err != nil {
		return nil, err
	}
	tb, err := t.segments(x, what)
	if err != nil {
		return nil, err
	}
	if t.parsed == nil {
		t.parsed = map[string]*table{}
	}
	t.parsed[name] = tb
	return tb, nil
}

// useBytes counts n JSON bytes against MaxBytes.
func (t *tables) useBytes(n int) error {
	t.bytes += n
	if m := t.l.MaxBytes; m > 0 && t.bytes > m {
		return fail("limit", "%d bytes, limit %d", t.bytes, m)
	}
	return nil
}

// useSlots counts a machine's counters and timers against MaxSlots.
func (t *tables) useSlots(n int) error {
	t.slots += int64(n)
	if m := t.l.maxSlots(); m >= 0 && t.slots > m {
		return fail("limit", "%d counters and timers, limit %d", t.slots, m)
	}
	return nil
}

// segments reads a list of segments. Its count is checked before anything is made
// of it.
func (t *tables) segments(x any, what string) (*table, error) {
	a, ok := x.([]any)
	if !ok || len(a) == 0 {
		return nil, fail("type", "%s: segments", what)
	}
	if t != nil && t.l.MaxSegments > 0 && len(a) > t.l.MaxSegments {
		return nil, fail("limit", "%s: %d segments", what, len(a))
	}
	tb := &table{segs: make([]seg, len(a)), lo: maxCount}
	for i, s := range a {
		q, ok := s.([]any)
		if !ok || len(q) != 3 {
			return nil, fail("type", "%s: segment", what)
		}
		var n [3]int64
		var err error
		for j := range n {
			if n[j], err = integer(q[j], what); err != nil {
				return nil, err
			}
		}
		if n[0] < 0 || n[0] > 1<<32-1 || n[1] < 0 || n[1] > n[2] || n[2] > maxCount {
			return nil, fail("range", "%s: segment %v", what, n)
		}
		tb.segs[i] = seg{uint64(n[0]), uint64(n[1]), uint64(n[2])}
		tb.total += uint64(n[0])
		tb.lo, tb.hi = min(tb.lo, uint64(n[1])), max(tb.hi, uint64(n[2]))
	}
	if tb.total == 0 {
		return nil, fail("range", "%s: total weight 0", what)
	}
	if len(tb.segs) >= minBinary {
		tb.cum = make([]uint64, len(tb.segs))
		var acc uint64
		for i := range tb.segs {
			acc += tb.segs[i].w
			tb.cum[i] = acc
		}
	}
	return tb, nil
}

func parseValue(x any, f family, t *tables, allowPipe bool, what string) (value, error) {
	if _, ok := x.(json.Number); ok {
		n, err := integer(x, what)
		if err != nil {
			return value{}, err
		}
		if n < 0 || uint64(n) < f.lo || uint64(n) > f.hi {
			return value{}, fail("range", "%s: %d", what, n)
		}
		return value{c: uint64(n), set: true}, nil
	}
	o, err := members(x, what, "t", "scope")
	if err != nil {
		return value{}, err
	}
	raw, err := required(o, "t", what)
	if err != nil {
		return value{}, err
	}
	v := value{set: true}
	switch s := o.get("scope").(type) {
	case nil:
	case string:
		switch s {
		case "each":
			v.sc = scopeEach
		case "pipe":
			v.sc = scopePipe
		case "shared":
			v.sc = scopeShared
		case "session":
			v.sc = scopeSession
		default:
			return value{}, fail("type", "%s: scope %q", what, s)
		}
	default:
		return value{}, fail("type", "%s: scope", what)
	}
	if v.sc == scopePipe && !allowPipe {
		return value{}, fail("scope", "%s: scope pipe", what)
	}
	if v.sc == scopeSession && (t == nil || !t.budget) {
		return value{}, fail("scope", "%s: scope session", what)
	}
	if v.sc != scopeSession && t != nil && t.budget {
		return value{}, fail("scope", "%s: only scope session", what)
	}
	var tb *table
	if name, ok := raw.(string); ok {
		tb, err = t.named(name, what)
	} else {
		tb, err = t.segments(raw, what)
	}
	if err != nil {
		return value{}, err
	}
	if tb.lo < f.lo || tb.hi > f.hi {
		return value{}, fail("range", "%s: segments in [%d, %d]", what, tb.lo, tb.hi)
	}
	v.segs, v.cum, v.total, v.low = tb.segs, tb.cum, tb.total, tb.lo
	return v, nil
}

// lower is the least value v can state, segments of weight 0 included.
func (v *value) lower() uint64 {
	if v.segs == nil {
		return v.c
	}
	return v.low
}

func mulhi64(a, b uint64) uint64 {
	hi, _ := bits.Mul64(a, b)
	return hi
}

// drawTable draws from the table of v; a constant takes nothing.
func (v *value) drawTable(src rand.Source) uint64 {
	segs := v.segs
	if segs == nil {
		return v.c
	}
	i := 0
	if len(segs) > 1 {
		c := mulhi64(src.Uint64(), v.total)
		if v.cum != nil {
			lo, hi := 0, len(segs)-1
			for lo < hi {
				m := int(uint(lo+hi) >> 1)
				if c < v.cum[m] {
					hi = m
				} else {
					lo = m + 1
				}
			}
			i = lo
		} else {
			var acc uint64
			for i = 0; i < len(segs)-1; i++ {
				acc += segs[i].w
				if c < acc {
					break
				}
			}
		}
	}
	s := &segs[i]
	if s.lo == s.hi {
		return s.lo
	}
	return s.lo + mulhi64(src.Uint64(), s.hi-s.lo+1)
}

// resolved is v with a shared table drawn into a constant.
func (v *value) resolved(src rand.Source, order *[]uint64) value {
	if v.sc != scopeShared || v.segs == nil {
		return *v
	}
	x := v.drawTable(src)
	*order = append(*order, x)
	return value{c: x, set: true}
}

// probTrue is the draw of a probability of p parts per million.
func probTrue(p uint64, src rand.Source) bool {
	if p == 0 {
		return false
	}
	if p >= 1_000_000 {
		return true
	}
	return mulhi64(src.Uint64(), 1_000_000) < p
}

// jitterScale is floor(2^64 / 10^6).
const jitterScale = 18446744073709

// jitterWith reduces x by up to j parts per million under the draw u.
func jitterWith(x uint64, j uint32, u uint64) uint64 {
	m := mulhi64(x, uint64(j)*jitterScale)
	return x - mulhi64(u, m+1)
}

type cryptoSeeded struct{ *rand.ChaCha8 }

// newSource returns a ChaCha8 generator seeded from crypto/rand.
func newSource() rand.Source {
	var seed [32]byte
	_, _ = crand.Read(seed[:])
	return cryptoSeeded{rand.NewChaCha8(seed)}
}
