package core

// rangeSet is a sorted set of disjoint half-open ranges.
type rangeSet []rng

type rng struct{ lo, hi uint64 }

// add inserts [lo, hi) and returns how many bytes were new.
func (r *rangeSet) add(lo, hi uint64) uint64 {
	if lo >= hi {
		return 0
	}
	s := *r
	// Fast path: append or extend the last range.
	if n := len(s); n == 0 || lo > s[n-1].hi {
		*r = append(s, rng{lo, hi})
		return hi - lo
	} else if lo >= s[n-1].lo {
		if hi <= s[n-1].hi {
			return 0
		}
		nw := hi - s[n-1].hi
		if lo > s[n-1].hi {
			nw = hi - lo
		}
		s[n-1].hi = hi
		return nw
	}
	// General case: find the first range ending at or after lo.
	i := 0
	for i < len(s) && s[i].hi < lo {
		i++
	}
	newBytes := hi - lo
	nlo, nhi := lo, hi
	j := i
	for j < len(s) && s[j].lo <= hi {
		ol, oh := max64(s[j].lo, lo), min64(s[j].hi, hi)
		if oh > ol {
			newBytes -= oh - ol
		}
		nlo, nhi = min64(nlo, s[j].lo), max64(nhi, s[j].hi)
		j++
	}
	if i == j {
		s = append(s, rng{})
		copy(s[i+1:], s[i:])
		s[i] = rng{nlo, nhi}
	} else {
		s[i] = rng{nlo, nhi}
		s = append(s[:i+1], s[j:]...)
	}
	*r = s
	return newBytes
}

// covered returns how many bytes of [lo, hi) are in the set.
func (r *rangeSet) covered(lo, hi uint64) uint64 {
	var n uint64
	for _, x := range *r {
		if x.hi <= lo {
			continue
		}
		if x.lo >= hi {
			break
		}
		n += min64(x.hi, hi) - max64(x.lo, lo)
	}
	return n
}

// contains reports whether [lo, hi) is fully in the set.
func (r *rangeSet) contains(lo, hi uint64) bool { return r.covered(lo, hi) == hi-lo }

// prefix returns the end of the range starting at 0 (0 when none).
func (r *rangeSet) prefix() uint64 {
	if s := *r; len(s) > 0 && s[0].lo == 0 {
		return s[0].hi
	}
	return 0
}

// gaps calls f with each maximal subrange of [lo, hi) not in r, in order.
func (r *rangeSet) gaps(lo, hi uint64, f func(a, b uint64)) {
	for _, x := range *r {
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
