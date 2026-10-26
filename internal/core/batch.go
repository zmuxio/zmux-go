package core

import (
	"sync"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// Batch is the output of one PollTransmit: records for one outbound lane, as
// header bytes in an arena and referenced payloads (IMPLEMENTATION.md 4).
// The driver writes Bufs and then reports the write with WriteDone, which
// releases the referenced blocks. A Batch is reused across polls.
type Batch struct {
	arena []byte
	segs  []bseg
	recs  []recMark
	pins  []pin
	bufs  [][]byte
	n     int

	// Pipe and Lane the batch was produced for.
	Pipe uint64
	Lane uint64
	// Shutdown asks the driver to end the write side gracefully after
	// writing the batch (after CONNECTION_CLOSE, a drained pipe).
	Shutdown bool
	// Contiguous asks for one flat buffer (TLS carriers).
	Contiguous bool
}

type bseg struct {
	ext    []byte
	pin    pin // what keeps ext alive (none: owned bytes)
	a0, a1 int
}

type recMark struct {
	firstSeg int // the Size segment (byte framing) or the first frame segment
	endSeg   int
	size     int // frames bytes
}

// batchPool holds the storage of idle writers' batches: a writer takes one
// while it has something to send and gives it back before it waits, so an
// idle lane holds none and a busy one allocates nothing.
var batchPool = sync.Pool{New: func() any { return new(Batch) }}

// GetBatch returns an empty batch.
func GetBatch() *Batch { return batchPool.Get().(*Batch) }

// PutBatch gives back a batch whose write was reported (WriteDone).
func PutBatch(b *Batch) {
	b.reset()
	b.Pipe, b.Lane, b.Contiguous = 0, 0, false
	clear(b.bufs)
	b.bufs = b.bufs[:0]
	batchPool.Put(b)
}

// Len returns the number of bytes in the batch.
func (b *Batch) Len() int { return b.n }

// Reset empties the batch for reuse; it must have been released.
func (b *Batch) reset() {
	b.arena = b.arena[:0]
	clear(b.segs)
	b.segs = b.segs[:0]
	b.recs = b.recs[:0]
	b.n = 0
	b.Shutdown = false
}

// Bufs returns the batch as iovecs (byte framing).
func (b *Batch) Bufs() [][]byte {
	b.bufs = b.bufs[:0]
	if b.Contiguous {
		flat := make([]byte, 0, b.n)
		for _, sg := range b.segs {
			flat = append(flat, b.seg(sg)...)
		}
		return append(b.bufs, flat)
	}
	for _, sg := range b.segs {
		b.bufs = append(b.bufs, b.seg(sg))
	}
	return b.bufs
}

func (b *Batch) seg(sg bseg) []byte {
	if sg.ext != nil {
		return sg.ext
	}
	return b.arena[sg.a0:sg.a1]
}

// Messages returns one flat message per record (message framing, SPEC.md 3.4).
func (b *Batch) Messages() [][]byte {
	out := make([][]byte, 0, len(b.recs))
	for _, r := range b.recs {
		m := make([]byte, 0, r.size)
		for _, sg := range b.segs[r.firstSeg:r.endSeg] {
			m = append(m, b.seg(sg)...)
		}
		out = append(out, m)
	}
	return out
}

// Flat returns the batch as one buffer (tests).
func (b *Batch) Flat() []byte {
	out := make([]byte, 0, b.n)
	for _, sg := range b.segs {
		out = append(out, b.seg(sg)...)
	}
	return out
}

// Discard drops a batch whose write is never reported (its pipe was
// replaced); under the session lock.
func (b *Batch) Discard() { b.release() }

// release drops the block references of a written batch.
func (b *Batch) release() {
	for i, p := range b.pins {
		p.unref()
		b.pins[i] = pin{}
	}
	b.pins = b.pins[:0]
}

// recWriter builds records into a batch.
type recWriter struct {
	b            *Batch
	framing      Framing
	recMax       int // largest Size (frames bytes) of a record
	budget       int // bytes left in the batch
	open         bool
	sizeSeg      int
	recBytes     int
	recs         int
	elicit       bool // the open record has an ack-eliciting frame
	anyElic      bool // the batch has an ack-eliciting byte
	elicEnd      int  // batch bytes up to the end of the last ack-eliciting record
	activity     bool // an activity frame was written
	onlyPing     bool // records so far carry only QX_PING requests and PADDING
	pend         pendStream
	closed       bool // nothing else: CONNECTION_CLOSE written, or laneOnly
	laneOnly     bool // the first record of a lane: PIPE_LANE only
	touched      []*Stream
	retain       bool
	abandonsSent []*Pipe
	descs        []recDesc
	newData      bool   // new stream data was written (idle copy)
	dataSpent    uint64 // stream data bytes charged to the room
	dataRoom     int64  // stream data the limiter lets through; <0: no limit
	pipe         *Pipe  // the pipe of the batch (multipath retention)
	revalPing    bool   // the PIPE_PING of a revalidation was written
}

// pendPiece is one buffer segment of a pending STREAM frame.
type pendPiece struct {
	data []byte
	pin  pin
}

type pendStream struct {
	id   uint64
	off  uint64
	data []byte
	pin  pin
	more []pendPiece // further contiguous segments of the same STREAM frame
	n    int         // total data bytes (data and more)
	on   bool
	fin  bool
	dg   bool // a DATAGRAM instead of STREAM
}

func (w *recWriter) init(b *Batch, framing Framing, recMax, budget int) {
	*w = recWriter{b: b, framing: framing, recMax: recMax, budget: budget, onlyPing: true, touched: w.touched[:0],
		descs: w.descs[:0], dataRoom: -1}
}

// headroom returns the bytes a new record's Size field costs.
func (w *recWriter) sizeCost() int {
	if w.framing == FramingRecords {
		return 0
	}
	return wire.VarintLen(uint64(w.recMax))
}

// room returns the frame bytes that still fit in the open record (or a new
// one) within the batch budget.
func (w *recWriter) room() int {
	if w.closed {
		return 0
	}
	if w.open {
		r := w.recMax - w.recBytes - w.pendLen(true)
		if b := w.budget - w.pendLen(true); b < r {
			r = b
		}
		return r
	}
	r := w.recMax
	if b := w.budget - w.sizeCost(); b < r {
		r = b
	}
	return r
}

func (w *recWriter) pendLen(withLen bool) int {
	if !w.pend.on {
		return 0
	}
	if w.pend.dg {
		l := 1 + len(w.pend.data)
		if withLen {
			l += wire.VarintLen(uint64(len(w.pend.data)))
		}
		return l
	}
	return wire.StreamHeaderLen(w.pend.id, w.pend.off, w.pend.n, withLen) + w.pend.n
}

// begin opens a record when none is open.
func (w *recWriter) begin() {
	if w.open {
		return
	}
	w.open = true
	w.recBytes = 0
	w.elicit = false
	w.sizeSeg = len(w.b.segs)
	if w.framing == FramingBytes {
		w.b.segs = append(w.b.segs, bseg{}) // filled at end()
	}
}

// fits reports whether a frame of n bytes fits, opening a record if needed.
// A frame that does not fit, header included, waits (IMPLEMENTATION.md
// 13.6 P2).
func (w *recWriter) fits(n int) bool {
	if w.closed {
		return false
	}
	if w.open {
		return w.recBytes+w.pendLen(true)+n <= w.recMax && w.pendLen(true)+n <= w.budget
	}
	return n <= w.recMax && w.sizeCost()+n <= w.budget
}

// newRecordFits is fits for a fresh record.
func (w *recWriter) newRecordFits(n int) bool {
	return !w.closed && n <= w.recMax && w.sizeCost()+n <= w.budget-w.openCost()
}

func (w *recWriter) openCost() int {
	if !w.open {
		return 0
	}
	return w.pendLen(false)
}

// raw appends frame bytes built by f to the open record.
func (w *recWriter) frame(n int, elicit bool, f func([]byte) []byte) bool {
	if !w.fits(n) {
		if !w.open || !w.newRecordFits(n) {
			return false
		}
		w.end()
		if !w.fits(n) {
			return false
		}
	}
	w.begin()
	w.flushPend(true)
	a0 := len(w.b.arena)
	w.b.arena = f(w.b.arena)
	l := len(w.b.arena) - a0
	w.b.segs = append(w.b.segs, bseg{a0: a0, a1: len(w.b.arena)})
	w.recBytes += l
	w.budget -= l
	w.b.n += l
	if elicit {
		w.elicit = true
	}
	return true
}

// stream adds a STREAM frame; its header is decided when the record ends
// or the next frame follows (LEN elided on the last frame).
func (w *recWriter) stream(id, off uint64, data []byte, pn pin, fin bool) bool {
	if pd := &w.pend; w.open && pd.on && !pd.dg && !pd.fin && pd.id == id && pd.off+uint64(pd.n) == off && len(data) > 0 {
		// A contiguous continuation joins the pending frame.
		grow := wire.StreamHeaderLen(id, pd.off, pd.n+len(data), true) + len(data) -
			wire.StreamHeaderLen(id, pd.off, pd.n, true)
		if w.recBytes+w.pendLen(true)+grow <= w.recMax && w.pendLen(true)+grow <= w.budget {
			pd.more = append(pd.more, pendPiece{data, pn})
			pd.n += len(data)
			pd.fin = fin
			if !pn.none() {
				pn.ref()
				w.b.pins = append(w.b.pins, pn)
			}
			return true
		}
	}
	n := wire.StreamHeaderLen(id, off, len(data), true) + len(data)
	if !w.fits(n) {
		if !w.open || !w.newRecordFits(n) {
			return false
		}
		w.end()
	}
	w.begin()
	w.flushPend(true)
	w.pend = pendStream{on: true, id: id, off: off, data: data, pin: pn, fin: fin, more: w.pend.more[:0], n: len(data)}
	if !pn.none() {
		// Pinned now: the stream may release its bytes before the frame is
		// encoded; the batch keeps the block alive until WriteDone.
		pn.ref()
		w.b.pins = append(w.b.pins, pn)
	}
	w.elicit = true
	w.activity = true
	w.onlyPing = false
	return true
}

// datagram adds a DATAGRAM frame (LEN elided when last).
func (w *recWriter) datagram(data []byte) bool {
	n := 1 + wire.VarintLen(uint64(len(data))) + len(data)
	if !w.fits(n) {
		if !w.open || !w.newRecordFits(n) {
			return false
		}
		w.end()
	}
	w.begin()
	w.flushPend(true)
	w.pend = pendStream{on: true, data: data, dg: true, more: w.pend.more[:0], n: len(data)}
	w.elicit = true
	w.activity = true
	w.onlyPing = false
	return true
}

func (w *recWriter) flushPend(withLen bool) {
	if !w.pend.on {
		return
	}
	p := w.pend
	w.pend = pendStream{more: p.more[:0]}
	a0 := len(w.b.arena)
	if p.dg {
		w.b.arena = wire.AppendDatagramHeader(w.b.arena, len(p.data), withLen)
	} else {
		w.b.arena = wire.AppendStreamHeader(w.b.arena, p.id, p.off, p.n, p.fin, withLen)
	}
	hl := len(w.b.arena) - a0
	if len(p.more) == 0 && (len(p.data) < refThreshold || p.pin.none() && !p.dg) {
		w.b.arena = append(w.b.arena, p.data...)
		w.b.segs = append(w.b.segs, bseg{a0: a0, a1: len(w.b.arena)})
	} else {
		w.b.segs = append(w.b.segs, bseg{a0: a0, a1: a0 + hl})
		for i := -1; i < len(p.more); i++ {
			d, pn := p.data, p.pin
			if i >= 0 {
				d, pn = p.more[i].data, p.more[i].pin
			}
			if len(d) < refThreshold || pn.none() {
				// Small pieces are copied: they extend the arena segment.
				b0 := len(w.b.arena)
				w.b.arena = append(w.b.arena, d...)
				if last := &w.b.segs[len(w.b.segs)-1]; last.ext == nil && last.a1 == b0 {
					last.a1 = len(w.b.arena)
				} else {
					w.b.segs = append(w.b.segs, bseg{a0: b0, a1: len(w.b.arena)})
				}
				continue
			}
			w.b.segs = append(w.b.segs, bseg{ext: d, pin: pn})
		}
		clear(p.more)
	}
	l := hl + p.n
	w.recBytes += l
	w.budget -= l
	w.b.n += l
}

// end closes the open record and writes its Size.
func (w *recWriter) end() {
	if !w.open {
		return
	}
	w.flushPend(false)
	w.open = false
	if w.recBytes == 0 {
		// Nothing was added: drop the reserved Size segment.
		if w.framing == FramingBytes {
			w.b.segs = w.b.segs[:w.sizeSeg]
		}
		return
	}
	first := w.sizeSeg
	if w.framing == FramingBytes {
		a0 := len(w.b.arena)
		w.b.arena = wire.AppendVarint(w.b.arena, uint64(w.recBytes))
		w.b.segs[w.sizeSeg] = bseg{a0: a0, a1: len(w.b.arena)}
		l := len(w.b.arena) - a0
		w.budget -= l
		w.b.n += l
	}
	w.b.recs = append(w.b.recs, recMark{firstSeg: first, endSeg: len(w.b.segs), size: w.recBytes})
	w.recs++
	if w.elicit {
		w.anyElic = true
		w.elicEnd = w.b.n
	}
}

// recordPositionBytes returns the bytes a record counts in positions.
func recordPositionBytes(size int) int {
	return wire.VarintLen(uint64(size)) + size
}

// spendData charges n bytes of stream data to the limiter's room.
func (w *recWriter) spendData(n uint64) {
	w.dataSpent += n
	if w.dataRoom > 0 {
		w.dataRoom = max(w.dataRoom-int64(n), 0)
	}
}
