package core

import (
	"math/rand/v2"
	"testing"
)

// rangeSet against a bitmap: random adds and removals over a space wide
// enough for many chunks, checking every query and the set's invariants.
func TestRangeSetModel(t *testing.T) {
	const space = 8192
	rnd := rand.New(rand.NewPCG(1, 2))
	var r rangeSet
	var bits [space]bool
	for step := range 40000 {
		lo := rnd.Uint64N(space)
		hi := min(lo+1+rnd.Uint64N(8), space)
		if rnd.IntN(4) == 0 {
			r.remove(lo, hi)
			for x := lo; x < hi; x++ {
				bits[x] = false
			}
		} else {
			var want uint64
			for x := lo; x < hi; x++ {
				if !bits[x] {
					want++
					bits[x] = true
				}
			}
			if got := r.add(lo, hi); got != want {
				t.Fatalf("step %d: add(%d,%d) = %d, want %d", step, lo, hi, got, want)
			}
		}
		qlo := rnd.Uint64N(space)
		qhi := min(qlo+rnd.Uint64N(64), space)
		var cov uint64
		for x := qlo; x < qhi; x++ {
			if bits[x] {
				cov++
			}
		}
		if got := r.covered(qlo, qhi); got != cov {
			t.Fatalf("step %d: covered(%d,%d) = %d, want %d", step, qlo, qhi, got, cov)
		}
		var gap uint64
		r.gaps(qlo, qhi, func(a, b uint64) { gap += b - a })
		if gap != qhi-qlo-cov {
			t.Fatalf("step %d: gaps %d, want %d", step, gap, qhi-qlo-cov)
		}
		if step%997 == 0 {
			checkRanges(t, &r, bits[:])
		}
	}
	checkRanges(t, &r, bits[:])
}

func checkRanges(t *testing.T, r *rangeSet, bits []bool) {
	t.Helper()
	var x uint64
	n := 0
	var prev *rng
	r.each(func(g rng) {
		if g.lo >= g.hi || prev != nil && g.lo <= prev.hi {
			t.Fatalf("range %v after %v", g, prev)
		}
		for ; x < g.lo; x++ {
			if bits[x] {
				t.Fatalf("byte %d set outside the ranges", x)
			}
		}
		for ; x < g.hi; x++ {
			if !bits[x] {
				t.Fatalf("byte %d in range %v not set", x, g)
			}
		}
		gg := g
		prev = &gg
		n++
	})
	for ; x < uint64(len(bits)); x++ {
		if bits[x] {
			t.Fatalf("byte %d set after the ranges", x)
		}
	}
	if n != r.len() {
		t.Fatalf("%d ranges, len %d", n, r.len())
	}
	for _, c := range r.q.cs {
		if len(c) == 0 || len(c) > seqChunk {
			t.Fatalf("chunk of %d", len(c))
		}
	}
}

// recvBuf against a byte map: random out-of-order inserts, partial reads,
// discards from both ends.
func TestRecvBufModel(t *testing.T) {
	const space = 1 << 16
	rnd := rand.New(rand.NewPCG(3, 4))
	var memory recvMemory
	rb := recvBuf{memory: &memory}
	have := map[uint64]byte{}
	var readOff, top uint64 = 0, space
	src := make([]byte, space)
	for i := range src {
		src[i] = byte(rnd.Uint32())
	}
	buf := NewBuf(src)
	defer buf.Unref()
	for step := range 30000 {
		switch rnd.IntN(10) {
		case 0:
			// Partial read at the read offset.
			dst := make([]byte, rnd.IntN(64))
			n, _, _ := rb.consume(readOff, dst, len(dst), false)
			for i := range n {
				if b, ok := have[readOff]; !ok || b != dst[i] {
					t.Fatalf("step %d: byte %d read %d, have %d %v", step, readOff, dst[i], b, ok)
				}
				delete(have, readOff)
				readOff++
			}
			if _, ok := have[readOff]; ok && n < len(dst) {
				t.Fatalf("step %d: read stopped at %d with data", step, readOff)
			}
		case 1:
			if top > readOff+64 {
				top -= rnd.Uint64N(16)
				var want uint64
				for x := range have {
					if x >= top {
						want++
						delete(have, x)
					}
				}
				if got := rb.discardFrom(top); got != want {
					t.Fatalf("step %d: discardFrom %d dropped %d, want %d", step, top, got, want)
				}
			}
		default:
			off := readOff + rnd.Uint64N(min(4096, top-readOff))
			end := min(off+1+rnd.Uint64N(12), top)
			var want uint64
			for x := off; x < end; x++ {
				if _, ok := have[x]; !ok {
					want++
					have[x] = src[x]
				}
			}
			if got := rb.insert(off, buf, int(off), int(end-off)); got != want {
				t.Fatalf("step %d: insert [%d,%d) = %d, want %d", step, off, end, got, want)
			}
		}
		if rb.bytes != uint64(len(have)) {
			t.Fatalf("step %d: %d bytes, have %d", step, rb.bytes, len(have))
		}
		c := readOff
		for {
			if _, ok := have[c]; !ok {
				break
			}
			c++
		}
		if got := rb.contiguous(readOff); got != c {
			t.Fatalf("step %d: contiguous %d, want %d", step, got, c)
		}
	}
	rb.clear()
	if memory.queued != 0 || memory.live != 0 || rb.ps.len() != 0 {
		t.Fatalf("after clear: %d bytes queued, %d live, %d pieces", memory.queued, memory.live, rb.ps.len())
	}
}
