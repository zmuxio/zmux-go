package core

import (
	"math"
	"math/bits"

	"github.com/zmuxio/zmux-go/shaping"
)

// refill adds what d units earn when 10^6 units earn per; rem keeps the remainder.
// per is at most 2^43, so r*per stays below 2^63.
func refill(d, per uint64, rem *uint64) uint64 {
	q, r := d/1_000_000, d%1_000_000
	x := r*per + *rem
	*rem = x % 1_000_000
	hi, lo := bits.Mul64(q, per)
	if hi != 0 {
		return math.MaxUint64
	}
	add, c := bits.Add64(lo, x/1_000_000, 0)
	if c != 0 {
		return math.MaxUint64
	}
	return add
}

// wireAccount owns the available cover balance and committed overhead.
type wireAccount struct {
	p                         shaping.Account
	initialized               bool
	level, rem, lastUS, spent uint64
}

func (a *wireAccount) accrue(us, own uint64) {
	elapsed := max(us, a.lastUS) - a.lastUS
	a.lastUS = max(us, a.lastUS)
	if a.level >= a.p.Ceiling {
		return
	}
	room := a.p.Ceiling - a.level
	add := refill(own, a.p.PerMB, &a.rem)
	if add >= room {
		a.level, a.rem = a.p.Ceiling, 0
		return
	}
	more := refill(elapsed, a.p.PerSec, &a.rem)
	if more >= room-add {
		a.level, a.rem = a.p.Ceiling, 0
		return
	}
	a.level += add + more
}

func (a *wireAccount) left() uint64 {
	if a.p.Ceiling == 0 {
		return 0
	}
	return a.level
}

func satAdd(a, b uint64) uint64 { return a + min(b, math.MaxUint64-a) }

func (a *wireAccount) spend(n uint64) { a.spent = satAdd(a.spent, n) }

func (a *wireAccount) charge(us, n uint64) bool {
	a.accrue(us, 0)
	if n == 0 || n > a.left() {
		return false
	}
	a.level -= n
	a.spend(n)
	return true
}

func (a *wireAccount) set(us uint64, p *shaping.Account) {
	a.accrue(us, 0)
	if p == nil {
		a.p, a.rem = shaping.Account{}, 0
		return
	}
	a.p = *p
	if !a.initialized && p.Ceiling > 0 {
		a.initialized, a.level = true, p.Initial
	}
	if a.level >= p.Ceiling {
		a.level, a.rem = p.Ceiling, 0
	}
}

// tokenBucket tracks byte credit on the microsecond session clock.
type tokenBucket struct {
	rate, rem, filled uint64
	burst, tokens     int64
}

func (b *tokenBucket) reset(us uint64) {
	b.tokens, b.rem, b.filled = b.burst, 0, us
}

func (b *tokenBucket) fill(us uint64) {
	if us <= b.filled {
		return
	}
	add := refill(us-b.filled, b.rate, &b.rem)
	b.filled = us
	if add >= uint64(b.burst)+uint64(max(-b.tokens, 0)) || b.tokens+int64(add) >= b.burst {
		b.tokens, b.rem = b.burst, 0
		return
	}
	b.tokens += int64(add)
}

// quantum is what the bucket must hold to let data through; a burst is never
// below it (shapeBurstFloor), and data never goes a byte a record.
func (b *tokenBucket) quantum() int64 {
	return max(shaping.MinMaxLen, min(b.burst, int64(b.rate*1000/1_000_000)))
}

// shapeBurstFloor is the burst in force for a burst asked: 0 is a record, and
// less than shaping.MinMaxLen is raised to it and reported as clamped.
func shapeBurstFloor(burst uint64, recordMax int64) (int64, bool) {
	if burst == 0 {
		return max(recordMax, shaping.MinMaxLen), false
	}
	if burst < shaping.MinMaxLen {
		return shaping.MinMaxLen, true
	}
	return int64(min(burst, 1<<53)), false
}

func (b *tokenBucket) held() bool { return b.tokens < b.quantum() }

// release is the microsecond at which a held bucket reaches its quantum; ok
// false when it lies beyond any clock.
func (b *tokenBucket) release() (us uint64, ok bool) {
	return b.readyAt(b.quantum())
}

// readyAt computes when this bucket will reach a level above its current tokens.
func (b *tokenBucket) readyAt(level int64) (us uint64, ok bool) {
	need := uint64(level - b.tokens)
	hi, lo := bits.Mul64(need, 1_000_000)
	lo, br := bits.Sub64(lo, b.rem, 0)
	hi -= br
	lo, c := bits.Add64(lo, b.rate-1, 0)
	hi += c
	if hi >= b.rate {
		return 0, false
	}
	q, _ := bits.Div64(hi, lo, b.rate)
	if q > math.MaxUint64-b.filled {
		return 0, false
	}
	return b.filled + q, true
}
