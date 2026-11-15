package core

// rangeSet is a sorted set of disjoint, non-adjacent half-open ranges.
type rangeSet struct{ q seq[rng] }

type rng struct{ lo, hi uint64 }

// len returns the number of ranges.
func (r *rangeSet) len() int { return r.q.len() }

// first returns the lowest range; the set must not be empty.
func (r *rangeSet) first() rng { return *r.q.first() }

// popFirst removes the lowest range.
func (r *rangeSet) popFirst() { r.q.popFirst() }

// from returns the position of the first range ending at or after lo
// (touching or above it).
func (r *rangeSet) from(lo uint64) seqPos {
	if r.q.n == 0 || r.q.last().hi < lo {
		return r.q.end()
	}
	if r.q.first().hi >= lo {
		return r.q.begin()
	}
	return r.q.search(func(x *rng) bool { return x.hi >= lo })
}

// add inserts [lo, hi) and returns how many bytes were new.
func (r *rangeSet) add(lo, hi uint64) uint64 {
	if lo >= hi {
		return 0
	}
	q := &r.q
	// Fast path: append or extend the last range.
	if q.n == 0 || lo > q.last().hi {
		q.push(rng{lo, hi})
		return hi - lo
	} else if l := q.last(); lo >= l.lo {
		if hi <= l.hi {
			return 0
		}
		nw := hi - l.hi // lo <= l.hi here: the new bytes start at its end
		l.hi = hi
		return nw
	}
	// General case: the first range ending at or after lo and the ranges
	// it merges with.
	i := r.from(lo)
	newBytes := hi - lo
	nlo, nhi := lo, hi
	k := 0
	for j := i; q.valid(j) && q.at(j).lo <= hi; j = q.next(j) {
		x := q.at(j)
		ol, oh := max64(x.lo, lo), min64(x.hi, hi)
		if oh > ol {
			newBytes -= oh - ol
		}
		nlo, nhi = min64(nlo, x.lo), max64(nhi, x.hi)
		k++
	}
	if k == 0 {
		q.insert(i, rng{nlo, nhi})
	} else {
		*q.at(i) = rng{nlo, nhi}
		if k > 1 {
			q.remove(q.next(i), k-1)
		}
	}
	return newBytes
}

// remove takes [lo, hi) out of the set.
func (r *rangeSet) remove(lo, hi uint64) {
	q := &r.q
	for i := r.from(lo + 1); q.valid(i) && lo < hi; {
		x := q.at(i)
		if x.lo >= hi {
			return
		}
		switch {
		case x.lo < lo && x.hi > hi:
			// Splits in two.
			right := rng{hi, x.hi}
			x.hi = lo
			q.insert(q.next(i), right)
			return
		case x.lo < lo:
			x.hi = lo
			i = q.next(i)
		case x.hi > hi:
			x.lo = hi
			return
		default:
			q.remove(i, 1)
			i = r.from(lo + 1) // positions after a removal move
		}
	}
}

// covered returns how many bytes of [lo, hi) are in the set.
func (r *rangeSet) covered(lo, hi uint64) uint64 {
	var n uint64
	q := &r.q
	for i := r.from(lo); q.valid(i); i = q.next(i) {
		x := q.at(i)
		if x.lo >= hi {
			break
		}
		if x.hi > lo {
			n += min64(x.hi, hi) - max64(x.lo, lo)
		}
	}
	return n
}

// contains reports whether [lo, hi) is fully in the set.
func (r *rangeSet) contains(lo, hi uint64) bool { return r.covered(lo, hi) == hi-lo }

// prefix returns the end of the range starting at 0 (0 when none).
func (r *rangeSet) prefix() uint64 {
	if r.q.n > 0 && r.q.first().lo == 0 {
		return r.q.first().hi
	}
	return 0
}

// gaps calls f with each maximal subrange of [lo, hi) not in r, in order.
func (r *rangeSet) gaps(lo, hi uint64, f func(a, b uint64)) {
	q := &r.q
	for i := r.from(lo); q.valid(i); i = q.next(i) {
		x := q.at(i)
		if x.hi <= lo {
			continue
		}
		if x.lo >= hi {
			break
		}
		if x.lo > lo {
			f(lo, x.lo)
		}
		lo = max64(lo, x.hi)
		if lo >= hi {
			return
		}
	}
	if lo < hi {
		f(lo, hi)
	}
}

// each calls f with every range, in order.
func (r *rangeSet) each(f func(rng)) {
	for _, c := range r.q.cs {
		for _, x := range c {
			f(x)
		}
	}
}

// total returns the number of bytes in the set.
func (r *rangeSet) total() uint64 {
	var n uint64
	r.each(func(x rng) { n += x.hi - x.lo })
	return n
}
