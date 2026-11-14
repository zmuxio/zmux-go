package core

import (
	"sync"
	"sync/atomic"
)

// blockSize is the size of a pooled send block; STREAM frames carry at most
// one block's worth (16 KiB frames).
const blockSize = 16384

// block is a pooled send buffer. refs counts the send queue's hold plus
// every batch and every other session's send queue that references it
// (nested sessions write their batches into an outer stream without a
// copy), so it is atomic; n changes only under the owning session's lock,
// and bytes below n never change while referenced.
type block struct {
	b    [blockSize]byte
	n    int // bytes filled
	refs atomic.Int32
}

// blockPool holds free blocks for reuse; the garbage collector trims it,
// so idle processes keep none.
var blockPool = sync.Pool{New: func() any { return new(block) }}

func getBlock() *block {
	b := blockPool.Get().(*block)
	b.n = 0
	b.refs.Store(1)
	return b
}

func (b *block) ref() { b.refs.Add(1) }

func (b *block) unref() {
	switch r := b.refs.Add(-1); {
	case r > 0:
		return
	case r < 0:
		panic("zmux: block released twice")
	}
	blockPool.Put(b)
}

// sendQ holds the retained bytes of a sending part, [base, end), as slices
// of pooled blocks or owned small buffers.
type sendQ struct {
	tail *block
	segs []seg // a window of arr
	arr  []seg // the segment array, reused as the window moves on
	base uint64
	end  uint64
}

type seg struct {
	blk *block // a pooled block, or nil
	rb  *Buf   // a received buffer passed on without a copy (WriteChunk), or nil
	b   []byte
}

// pin keeps the memory of a frame's data alive while a batch references
// it: a pooled send block or a received buffer.
type pin struct {
	blk *block
	rb  *Buf
}

func (p pin) none() bool { return p.blk == nil && p.rb == nil }

func (p pin) ref() {
	switch {
	case p.blk != nil:
		p.blk.ref()
	case p.rb != nil:
		p.rb.Ref()
	}
}

func (p pin) unref() {
	switch {
	case p.blk != nil:
		p.blk.unref()
	case p.rb != nil:
		p.rb.Unref()
	}
}

// add appends a segment. The window reaching the end of the array moves
// back to its start when at most half of it is in use, else to an array
// twice its size, so a queue that keeps being written and released does
// not allocate.
func (q *sendQ) add(sg seg) {
	if n := len(q.segs); n == cap(q.segs) {
		if n > 0 && n <= len(q.arr)/2 {
			copy(q.arr, q.segs)
			clear(q.arr[n:])
		} else {
			arr := make([]seg, max(2*n, 4))
			copy(arr, q.segs)
			q.arr = arr
		}
		q.segs = q.arr[:n]
	}
	q.segs = append(q.segs, sg)
}

// keptSegs is the largest segment array an emptied queue keeps.
const keptSegs = 64

// writeRef appends a received buffer's bytes without copying; the queue
// takes one reference of rb.
func (q *sendQ) writeRef(p []byte, rb *Buf) {
	q.end += uint64(len(p))
	rb.Ref()
	q.add(seg{b: p, rb: rb})
	q.tail = nil
}

// writePin appends p, which pn keeps alive, without a copy.
func (q *sendQ) writePin(p []byte, pn pin) {
	q.end += uint64(len(p))
	pn.ref()
	q.add(seg{b: p, blk: pn.blk, rb: pn.rb})
	q.tail = nil
}

// write appends p by copying into blocks.
func (q *sendQ) write(p []byte) {
	q.end += uint64(len(p))
	for len(p) > 0 {
		if n := len(q.segs); n > 0 && q.tail != nil && q.segs[n-1].blk == q.tail && q.tail.n < blockSize {
			last := &q.segs[n-1]
			c := copy(q.tail.b[q.tail.n:], p)
			last.b = last.b[:len(last.b)+c]
			q.tail.n += c
			p = p[c:]
			continue
		}
		blk := getBlock()
		c := copy(blk.b[:], p)
		blk.n = c
		q.tail = blk
		q.add(seg{b: blk.b[:c:blockSize], blk: blk})
		p = p[c:]
	}
}

// lendTail returns the block to fill in place next, with a reference for
// the borrower: the tail block while it has a useful amount of room, else a
// fresh one.
func (q *sendQ) lendTail() *block {
	if n := len(q.segs); n > 0 && q.tail != nil && q.segs[n-1].blk == q.tail && q.tail.n <= blockSize-minLend {
		q.tail.ref()
		return q.tail
	}
	return getBlock()
}

// minLend is the least tail room worth lending; below it a fresh block
// gives the reader a full block to fill.
const minLend = blockSize / 8

// commitTail appends n bytes the borrower wrote at blk.b[blk.n:] and takes
// over its reference. The block is still the tail unless the bytes before
// it were all released meanwhile; then it starts a new segment.
func (q *sendQ) commitTail(blk *block, n int) {
	q.end += uint64(n)
	at := blk.n
	blk.n += n
	if k := len(q.segs); k > 0 && q.tail == blk && q.segs[k-1].blk == blk {
		last := &q.segs[k-1]
		last.b = last.b[:len(last.b)+n]
		blk.unref()
		return
	}
	q.tail = blk
	q.add(seg{b: blk.b[at : at+n : blockSize], blk: blk})
}

// prepend inserts owned bytes before base (the preamble at ID allocation).
func (q *sendQ) prepend(p []byte) {
	q.add(seg{})
	copy(q.segs[1:], q.segs)
	q.segs[0] = seg{b: p}
	q.end += uint64(len(p))
}

// peek returns up to limit bytes at offset off (base <= off < end) as one
// contiguous slice and its block (to pin).
func (q *sendQ) peek(off uint64, limit int) ([]byte, pin) {
	pos := q.base
	for i := range q.segs {
		sg := &q.segs[i]
		l := uint64(len(sg.b))
		if off < pos+l {
			b := sg.b[off-pos:]
			if len(b) > limit {
				b = b[:limit]
			}
			return b, pin{sg.blk, sg.rb}
		}
		pos += l
	}
	return nil, pin{}
}

// release drops bytes below off. A queue released to empty keeps a small
// segment array for the next writes.
func (q *sendQ) release(off uint64) {
	k := 0
	for q.base < off && k < len(q.segs) {
		sg := &q.segs[k]
		l := uint64(len(sg.b))
		if q.base+l <= off {
			if sg.blk != nil {
				if sg.blk == q.tail {
					q.tail = nil
				}
				sg.blk.unref()
			}
			if sg.rb != nil {
				sg.rb.Unref()
			}
			q.base += l
			*sg = seg{}
			k++
			continue
		}
		cut := off - q.base
		sg.b = sg.b[cut:]
		q.base = off
	}
	if k == len(q.segs) {
		q.segs = nil
		if len(q.arr) <= keptSegs {
			q.segs = q.arr[:0]
		} else {
			q.arr = nil
		}
		q.base = off
		return
	}
	q.segs = q.segs[k:]
}

// drop releases everything.
func (q *sendQ) drop() {
	for _, sg := range q.segs {
		if sg.blk != nil {
			sg.blk.unref()
		}
		if sg.rb != nil {
			sg.rb.Unref()
		}
	}
	q.segs, q.arr = nil, nil
	q.tail = nil
	q.base = q.end
}
