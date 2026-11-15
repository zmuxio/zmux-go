package core

import (
	"bytes"
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
	var ps pinSet
	rb := recvBuf{pin: &ps}
	have := map[uint64]byte{}
	var readOff, top uint64 = 0, space
	src := make([]byte, space)
	for i := range src {
		src[i] = byte(rnd.Uint32())
	}
	for step := range 30000 {
		switch rnd.IntN(10) {
		case 0:
			// Partial read at the read offset.
			dst := make([]byte, rnd.IntN(64))
			n := rb.read(readOff, dst)
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
			if got := rb.insert(off, src[off:end], nil); got != want {
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
	if ps.extra != 0 || rb.ps.len() != 0 {
		t.Fatalf("after clear: extra %d, %d pieces", ps.extra, rb.ps.len())
	}
}

// TestRecvBufPinnedModel: pieces that reference read buffers, cut by reads
// and truncated by discardFrom, and then compacted (released) in random
// order, keep their data and leave no buffer pinned or referenced.
func TestRecvBufPinnedModel(t *testing.T) {
	const slots, perBuf = 64, 4
	rnd := rand.New(rand.NewPCG(5, 6))
	src := make([]byte, slots*refThreshold)
	for i := range src {
		src[i] = byte(rnd.Uint32())
	}
	var bufs []*Buf
	for i := 0; i < slots; i += perBuf {
		b := &Buf{B: src[i*refThreshold : (i+perBuf)*refThreshold]}
		b.refs.Store(1)
		bufs = append(bufs, b)
	}
	var ps pinSet
	rb := recvBuf{pin: &ps}
	order := rnd.Perm(slots)
	var readOff uint64
	top := uint64(len(src))
	for step := range 4000 {
		switch rnd.IntN(8) {
		case 0, 1, 2:
			if len(order) == 0 {
				continue
			}
			s := order[0]
			order = order[1:]
			off := uint64(s * refThreshold)
			if off < readOff || off+refThreshold > top {
				continue
			}
			rb.insert(off, src[off:off+refThreshold], bufs[s/perBuf])
		case 3, 4:
			dst := make([]byte, rnd.IntN(refThreshold))
			n := rb.read(readOff, dst)
			if !bytes.Equal(dst[:n], src[readOff:readOff+uint64(n)]) {
				t.Fatalf("step %d: read at %d returned other data", step, readOff)
			}
			readOff += uint64(n)
		case 5:
			if top > readOff {
				top -= rnd.Uint64N(top-readOff) / 4
				rb.discardFrom(top)
			}
		default:
			if len(ps.bufs) > 0 {
				b := ps.bufs[rnd.IntN(len(ps.bufs))]
				b.release()
				if b.held != 0 {
					t.Fatalf("step %d: released buffer holds %d", step, b.held)
				}
			}
		}
	}
	for len(ps.bufs) > 0 {
		ps.bufs[0].release()
	}
	for readOff < top {
		dst := make([]byte, refThreshold)
		n := rb.read(readOff, dst)
		if !bytes.Equal(dst[:n], src[readOff:readOff+uint64(n)]) {
			t.Fatalf("final read at %d returned other data", readOff)
		}
		if n == 0 {
			break
		}
		readOff += uint64(n)
	}
	rb.clear()
	if ps.extra != 0 || ps.held != 0 || len(ps.bufs) != 0 {
		t.Fatalf("after clear: extra %d, held %d, %d buffers", ps.extra, ps.held, len(ps.bufs))
	}
	for i, b := range bufs {
		if b.Refs() != 1 {
			t.Fatalf("buffer %d keeps %d references", i, b.Refs())
		}
	}
}

// A release whose owner offset no longer locates the piece still finds it
// by scanning, instead of panicking with the session lock held.
func TestBufReleaseScansWhenOwnerMissed(t *testing.T) {
	var ps pinSet
	rb := recvBuf{pin: &ps}
	b := &Buf{B: make([]byte, 4*refThreshold)}
	b.refs.Store(1)
	rb.insert(0, b.B[:refThreshold], b)
	b.owners[0].off = 1 << 40 // after() now finds no piece
	b.release()
	if b.held != 0 || len(ps.bufs) != 0 || rb.ps.at(rb.ps.begin()).buf != nil {
		t.Fatalf("not released: held %d, %d buffers", b.held, len(ps.bufs))
	}
}
