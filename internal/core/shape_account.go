package core

import (
	"math"
	"math/bits"

	"github.com/zmuxio/zmux-go/shaping"
)

// refill adds what d units earn at per per 10^6 units; rem keeps the remainder.
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

// wireAccount is the session's allowance for spontaneous actions and the meter of
// all added wire bytes. Its level never goes below zero.
type wireAccount struct {
	p            shaping.Account
	on           bool
	level        uint64 // at most math.MaxInt64
	remMB, remS  uint64
	lastUS, earn uint64
	spent        uint64
}

func (a *wireAccount) accrue(us, earn uint64) {
	if a.on {
		add := refill(earn-a.earn, a.p.PerMB, &a.remMB)
		b := refill(us-a.lastUS, a.p.PerSec, &a.remS)
		if sum := add + b; sum < add {
			add = math.MaxUint64
		} else {
			add = sum
		}
		if add > math.MaxInt64-a.level {
			a.level = math.MaxInt64
		} else {
			a.level += add
		}
	}
	a.lastUS, a.earn = us, earn
}

func (a *wireAccount) left() uint64 {
	if !a.on {
		return math.MaxUint64
	}
	return a.level
}

// spend meters an accompanying action; it never fails and takes nothing.
func (a *wireAccount) spend(c uint64) {
	if c > math.MaxUint64-a.spent {
		a.spent = math.MaxUint64
		return
	}
	a.spent += c
}

// charge takes c for a spontaneous action if the account has it.
func (a *wireAccount) charge(us, earn, c uint64) bool {
	a.accrue(us, earn)
	if a.on {
		if c > a.level {
			return false
		}
		a.level -= c
	}
	a.spend(c)
	return true
}

// set replaces the parameters; all zero: not set.
func (a *wireAccount) set(us, earn uint64, p shaping.Account) {
	a.accrue(us, earn)
	switch {
	case p == shaping.Account{}:
		a.on, a.level, a.remMB, a.remS = false, 0, 0, 0
	case !a.on:
		a.on, a.level, a.remMB, a.remS = true, p.Initial, 0, 0
	case p.Initial >= a.p.Initial:
		if d := p.Initial - a.p.Initial; d > math.MaxInt64-a.level {
			a.level = math.MaxInt64
		} else {
			a.level += d
		}
	default:
		a.level -= min(a.level, a.p.Initial-p.Initial)
	}
	a.p = p
}

// shapeRateFloor is the lowest rate a bound of md microseconds on one wait leaves;
// md < 0: none. A rate of 0 is no gate.
func shapeRateFloor(rate uint64, md int64) (uint64, bool) {
	if md < 0 || rate == 0 {
		return rate, false
	}
	md = max(md, 1)
	if f := (64*1_000_000 + uint64(md) - 1) / uint64(md); rate < f {
		return f, true
	}
	return rate, false
}

// tokenBucket is the integer gate of a pipe: bytes, in microseconds of the clock.
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
	need := uint64(b.quantum() - b.tokens)
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
