package core

import (
	"sync"
	"sync/atomic"
)

// blockSize is the size of a pooled send block; STREAM frames carry at most
// one block's worth (IMPLEMENTATION.md 9.1: 16 KiB frames).
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
	segs []seg
	tail *block
	base uint64
	end  uint64
}

type seg struct {
	b   []byte
	blk *block // a pooled block, or nil
	rb  *Buf   // a received buffer passed on without a copy (WriteChunk), or nil
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

// writeRef appends a received buffer's bytes without copying; the queue
// takes one reference of rb.
func (q *sendQ) writeRef(p []byte, rb *Buf) {
	q.end += uint64(len(p))
	rb.Ref()
	q.segs = append(q.segs, seg{b: p, rb: rb})
	q.tail = nil
}

// writePin appends p, which pn keeps alive, without a copy.
func (q *sendQ) writePin(p []byte, pn pin) {
	q.end += uint64(len(p))
	pn.ref()
	q.segs = append(q.segs, seg{b: p, blk: pn.blk, rb: pn.rb})
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
		q.segs = append(q.segs, seg{b: blk.b[:c:blockSize], blk: blk})
		p = p[c:]
	}
}

// prepend inserts owned bytes before base (the preamble at ID allocation).
func (q *sendQ) prepend(p []byte) {
	q.segs = append(q.segs, seg{})
	copy(q.segs[1:], q.segs)
	q.segs[0] = seg{b: p}
	q.end += uint64(len(p))
}

// peek returns up to max bytes at offset off (base <= off < end) as one
// contiguous slice and its block (to pin).
func (q *sendQ) peek(off uint64, max int) ([]byte, pin) {
	pos := q.base
	for i := range q.segs {
		sg := &q.segs[i]
		l := uint64(len(sg.b))
		if off < pos+l {
			b := sg.b[off-pos:]
			if len(b) > max {
				b = b[:max]
			}
			return b, pin{sg.blk, sg.rb}
		}
		pos += l
	}
	return nil, pin{}
}

// release drops bytes below off.
func (q *sendQ) release(off uint64) {
	for q.base < off && len(q.segs) > 0 {
		sg := &q.segs[0]
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
			q.segs[0] = seg{}
			q.segs = q.segs[1:]
			continue
		}
		cut := off - q.base
		sg.b = sg.b[cut:]
		q.base = off
	}
	if len(q.segs) == 0 {
		q.segs = nil
		q.base = off
	}
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
	q.segs = nil
	q.tail = nil
	q.base = q.end
}
