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
	b      *[blockSize]byte
	budget *budgetAllowance
	n      int // bytes filled
	refs   atomic.Int32
}

// Keep the payload in its own allocation so the descriptor does not push
// it into a larger size class. The pool reuses the pair and GC trims it.
var blockPool = sync.Pool{New: func() any { return &block{b: new([blockSize]byte)} }}

const sendBlockCharge = blockSize + 64

func getBlock(budget *budgetAllowance) *block {
	if !budget.adjust(sendBlockCharge) {
		return nil
	}
	return getReservedBlock(budget)
}

func getReservedBlock(budget *budgetAllowance) *block {
	b := blockPool.Get().(*block)
	b.n = 0
	b.budget = budget
	b.refs.Store(1)
	return b
}

func (b *block) ref() { b.refs.Add(1) }

func (b *block) unref() {
	if budget := b.recycle(); budget != nil {
		budget.adjust(-sendBlockCharge)
	}
}

// recycle returns the released backing's charge to its caller. Returning
// the block to the pool first keeps the old charge live until settlement.
func (b *block) recycle() *budgetAllowance {
	switch r := b.refs.Add(-1); {
	case r > 0:
		return nil
	case r < 0:
		panic("zmux: block released twice")
	}
	budget := b.budget
	b.budget = nil
	blockPool.Put(b)
	return budget
}

// sendQ holds the retained bytes of a sending part, [base, end), as slices
// of pooled blocks or owned small buffers.
type sendQ struct {
	budget   *budgetAllowance
	handoffs []queueHandoff
	tail     *block
	segs     []seg // a window of arr
	arr      []seg // the segment array, reused as the window moves on
	base     uint64
	end      uint64
}

type seg struct {
	pin
	b []byte
}

// pin keeps the memory of a frame's data alive while a batch references
// it: a pooled send block or a received buffer.
type pin struct {
	blk *block
	rb  *Buf
	raw bool // ordinary ownership when the backing has no receive lease
}

func (p pin) none() bool { return p.blk == nil && p.rb == nil }

func (p pin) ref() {
	switch {
	case p.blk != nil:
		p.blk.ref()
	case p.raw:
		p.rb.Ref()
	case p.rb != nil:
		p.rb.refLoan()
	}
}

func (p pin) unref() {
	switch {
	case p.blk != nil:
		p.blk.unref()
	case p.raw:
		p.rb.Unref()
	case p.rb != nil:
		p.rb.ReleaseLoan()
	}
}

// blockReturns amortizes the refund across contiguous blocks in one queue
// or output batch. Forwarded blocks can belong to a different allowance.
type blockReturns struct {
	budget *budgetAllowance
	charge int64
}

func (r *blockReturns) release(p pin) {
	if p.blk == nil {
		p.unref()
		return
	}
	if budget := p.blk.recycle(); budget != nil {
		if r.budget != budget {
			r.settle()
			r.budget = budget
		}
		r.charge += sendBlockCharge
	}
}

func (r *blockReturns) settle() {
	r.budget.adjust(-r.charge)
	r.charge = 0
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

func newBacking(a *budgetAllowance, n int) *Buf {
	if !a.adjust(int64(n)) {
		return nil
	}
	b := NewBuf(make([]byte, n))
	b.memory.Store(a)
	return b
}

func copyBacking(a *budgetAllowance, p []byte) *Buf {
	b := newBacking(a, len(p))
	if b != nil {
		copy(b.B, p)
	}
	return b
}

// writeRef appends a received buffer's bytes without copying; the queue
// takes one reference of rb.
func (q *sendQ) writeRef(p []byte, rb *Buf) {
	q.end += uint64(len(p))
	pn := pin{rb: rb, raw: !rb.tryRefLoan()}
	if pn.raw {
		rb.Ref()
	}
	q.add(seg{b: p, pin: pn})
	q.tail = nil
}

func (q *sendQ) writeAccounted(p []byte, rb *Buf, h *Handoff) {
	q.handoffs = append(q.handoffs, queueHandoff{start: q.end, end: q.end + uint64(len(p)), receipt: h})
	if rb != nil {
		q.writeRef(p, rb)
	} else {
		q.end += uint64(len(p))
		q.add(seg{b: p})
		q.tail = nil
	}
}

// writePin appends p, which pn keeps alive, without a copy.
func (q *sendQ) writePin(p []byte, pn pin) {
	q.end += uint64(len(p))
	pn.ref()
	q.add(seg{b: p, pin: pn})
	q.tail = nil
}

// write appends p by copying into blocks.
func (q *sendQ) write(p []byte) int {
	start := len(p)
	if n := len(q.segs); n > 0 && q.tail != nil && q.segs[n-1].blk == q.tail && q.tail.n < blockSize {
		last := &q.segs[n-1]
		c := copy(q.tail.b[q.tail.n:], p)
		last.b = last.b[:len(last.b)+c]
		q.tail.n += c
		p = p[c:]
	}
	if len(p) == 0 {
		q.end += uint64(start)
		return start
	}
	const maxBlocks = (1<<63 - 1) / sendBlockCharge
	blocks := 1 + (len(p)-1)/blockSize
	reserved := int64(blocks) <= maxBlocks && q.budget.adjustReserved(int64(blocks)*sendBlockCharge)
	for len(p) > 0 {
		var blk *block
		if reserved {
			blk = getReservedBlock(q.budget)
		} else {
			// A partly admitted write still takes every block that fits.
			blk = getBlock(q.budget)
			if blk == nil {
				break
			}
		}
		c := copy(blk.b[:], p)
		blk.n = c
		q.tail = blk
		lease := pin{blk: blk}
		q.add(seg{b: blk.b[:c:blockSize], pin: lease})
		p = p[c:]
	}
	n := start - len(p)
	q.end += uint64(n)
	return n
}

// lendTail returns the block to fill in place next, with a reference for
// the borrower: the tail block while it has a useful amount of room, else a
// fresh one.
func (q *sendQ) lendTail() *block {
	if n := len(q.segs); n > 0 && q.tail != nil && q.segs[n-1].blk == q.tail && q.tail.n <= blockSize-minLend {
		q.tail.ref()
		return q.tail
	}
	return getBlock(q.budget)
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
	lease := pin{blk: blk}
	q.add(seg{b: blk.b[at : at+n : blockSize], pin: lease})
}

// prepend inserts owned bytes before base (the preamble at ID allocation).
func (q *sendQ) prepend(p []byte, owner *Buf) {
	q.add(seg{})
	copy(q.segs[1:], q.segs)
	hold := pin{rb: owner, raw: owner != nil}
	q.segs[0] = seg{b: p, pin: hold}
	for i := range q.handoffs {
		q.handoffs[i].start += uint64(len(p))
		q.handoffs[i].end += uint64(len(p))
	}
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
			return b, sg.pin
		}
		pos += l
	}
	return nil, pin{}
}

// release drops bytes below off. A queue released to empty keeps a small
// segment array for the next writes.
func (q *sendQ) release(off uint64) {
	q.releaseHandoffs(off)
	var returned blockReturns
	defer returned.settle()
	k := 0
	for q.base < off && k < len(q.segs) {
		sg := &q.segs[k]
		l := uint64(len(sg.b))
		if q.base+l <= off {
			if sg.blk != nil && sg.blk == q.tail {
				q.tail = nil
			}
			returned.release(sg.pin)
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
	q.releaseHandoffs(q.end)
	var returned blockReturns
	defer returned.settle()
	for _, sg := range q.segs {
		returned.release(sg.pin)
	}
	q.segs, q.arr = nil, nil
	q.tail = nil
	q.base = q.end
}
