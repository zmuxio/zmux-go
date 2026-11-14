package core

import (
	"sync"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// Batch is the output of one PollTransmit: records for one outbound lane, as
// header bytes in an arena and referenced payloads.
// The driver writes Bufs and then reports the write with WriteDone, which
// releases the referenced blocks. A Batch is reused across polls.
type Batch struct {
	arena []byte
	segs  []bseg
	recs  []recMark
	pins  []pin
	bufs  [][]byte
	n     int
	pos   uint64 // the lane position bytes of the batch, owed to WriteDone

	// Pipe and Lane the batch was produced for.
	Pipe uint64
	Lane uint64
	// Shutdown asks the driver to end the write side gracefully after
	// writing the batch (after CONNECTION_CLOSE, a drained pipe).
	Shutdown bool

	msgs [][]byte // Messages' result, reused
	mbuf []byte   // Messages' storage, reused
}

type bseg struct {
	pin    pin // what keeps ext alive (none: owned bytes)
	ext    []byte
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
	b.Pipe, b.Lane = 0, 0
	clear(b.bufs)
	b.bufs = b.bufs[:0]
	clear(b.msgs)
	b.msgs = b.msgs[:0]
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
	b.pos = 0
	b.Shutdown = false
}

// Bufs returns the batch as iovecs (byte framing).
func (b *Batch) Bufs() [][]byte {
	b.bufs = b.bufs[:0]
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

// Messages returns one flat message per record (message framing). The messages share storage that the batch reuses: they are valid
// until the next call or PutBatch, and a carrier must not retain them.
func (b *Batch) Messages() [][]byte {
	if cap(b.mbuf) < b.n {
		b.mbuf = make([]byte, 0, b.n)
	}
	buf := b.mbuf[:0]
	b.msgs = b.msgs[:0]
	for _, r := range b.recs {
		start := len(buf)
		for _, sg := range b.segs[r.firstSeg:r.endSeg] {
			buf = append(buf, b.seg(sg)...)
		}
		b.msgs = append(b.msgs, buf[start:len(buf):len(buf)])
	}
	b.mbuf = buf[:0]
	return b.msgs
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
	b           *Batch
	recMax      int // largest Size (frames bytes) of a record
	budget      int // bytes left in the batch
	sizeSeg     int
	recBytes    int
	recs        int
	elicEnd     int // batch bytes up to the end of the last ack-eliciting record
	framing     Framing
	open        bool
	delayed     bool   // the next record waits for its shaping delay
	elicit      bool   // the open record has an ack-eliciting frame
	anyElic     bool   // the batch has an ack-eliciting byte
	activity    bool   // an activity frame was written
	traffic     bool   // a traffic frame: an activity frame but QX_PING
	onlyPing    bool   // records so far carry only QX_PING requests and PADDING
	epochAnswer uint64 // the epoch of a SESSION_EPOCH answer written, 0: none
	pend        pendStream
	closed      bool // nothing else: CONNECTION_CLOSE written, or laneOnly
	laneOnly    bool // the first record of a lane: PIPE_LANE only
	retain      bool
	newData     bool // new stream data was written (idle copy)
	revalPing   bool // the PIPE_PING of a revalidation was written
	pingWritten bool // a PIPE_PING was written
	suspPing    bool // it is the PIPE_PING of a suspicion
	capped      bool // nextRecord stopped at recBound while data may wait
	// Stream data opens no further record once the batch holds this many
	// bytes (nextRecord); 0: one record.
	recBound     int
	touched      []*Stream
	abandonsSent []*Pipe
	descs        []recDesc
	dataSpent    uint64 // stream data bytes charged to the room
	notEarned    uint64 // padding, injections and copies in the batch (earnBytes)
	dataRoom     int64  // stream data the limiter lets through; <0: no limit
	pipe         *Pipe  // the pipe of the batch (multipath retention)
	sh           *pipeShape
	cut          int // largest frames bytes of the open record when below recMax; 0: none
	// The shaping gate of the batch's pipe, read live: a gate that a
	// record's plan sets in the middle of the batch applies to the data
	// written after it.
	gate     *pipeShape
	gateGen  uint64 // gate generation the batch follows
	gateBase uint64 // dataSpent when that gate took effect in the batch
	gateShut bool   // that gate holds stream data for the rest of the batch
	gateOn   bool   // gateGen and gateBase are set
}

// batchRecBound is the bytes after which stream data opens no further
// record in a batch: the ceiling of the write-call bound, so that a batch of many small records
// does not hold a call longer than one full record does.
const batchRecBound = 64 << 10

// nextRecord closes the open record when it can take no further stream
// data and reports whether a following record may carry more: the batch
// stays within its record bound with the next record at its planned size
// (a full-size record goes alone) and the limiter and the shaping gate let data
// through. The following record is planned (shaping) when room is next
// asked; a delayed plan ends the batch there.
func (w *recWriter) nextRecord() bool {
	if !w.open || w.recBytes+w.pendLen(false) == 0 || w.closed || w.dataHeld() {
		return false
	}
	if w.b.n+w.pendLen(true) >= w.recBound {
		w.capped = true
		return false
	}
	w.end()
	r := w.room()
	if r > 0 && w.b.n+w.sizeCost()+r > w.recBound {
		// The next record, at its planned size, would pass the bound.
		w.capped = true
		return false
	}
	return r > 0
}

// pendPiece is one buffer segment of a pending STREAM frame.
type pendPiece struct {
	data []byte
	pin  pin
}

type pendStream struct {
	pin  pin
	data []byte
	more []pendPiece // further contiguous segments of the same STREAM frame
	id   uint64
	off  uint64
	n    int // total data bytes (data and more)
	on   bool
	fin  bool
	dg   bool // a DATAGRAM instead of STREAM
}

// blocked reports a batch that can take no record now: the record it
// would open is delayed by its shaping plan.
func (w *recWriter) blocked() bool {
	if w.sh != nil && !w.open && !w.delayed {
		w.sh.prep(w)
	}
	return w.delayed
}

func (w *recWriter) init(b *Batch, framing Framing, recMax, budget int) {
	*w = recWriter{b: b, framing: framing, recMax: recMax, budget: budget, onlyPing: true, touched: w.touched[:0],
		descs: w.descs[:0], dataRoom: -1, recBound: batchRecBound}
}

// headroom returns the bytes a new record's Size field costs.
func (w *recWriter) sizeCost() int {
	if w.framing == FramingRecords {
		return 0
	}
	return varint.SizeVarint(uint64(w.recMax))
}

// room returns the frame bytes that still fit in the open record (or a new
// one) within the batch budget.
func (w *recWriter) room() int {
	if w.closed || w.blocked() {
		return 0
	}
	if w.open {
		r := w.max() - w.recBytes - w.pendLen(true)
		if b := w.budget - w.pendLen(true); b < r {
			r = b
		}
		return r
	}
	r := w.max()
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
			l += varint.SizeVarint(uint64(len(w.pend.data)))
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
	if w.sh != nil {
		w.sh.prep(w)
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
// A frame that does not fit, header included, waits.
func (w *recWriter) fits(n int) bool {
	if w.closed || w.blocked() {
		return false
	}
	if w.open {
		m := w.max()
		if w.recBytes+w.pendLen(true) == 0 {
			m = w.recMax // a record takes its first frame
		}
		return w.recBytes+w.pendLen(true)+n <= m && w.pendLen(true)+n <= w.budget
	}
	return n <= w.recMax && w.sizeCost()+n <= w.budget
}

// newRecordFits is fits for a fresh record.
func (w *recWriter) newRecordFits(n int) bool {
	return !w.closed && !w.delayed && n <= w.recMax && w.sizeCost()+n <= w.budget-w.openCost()
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
		if w.recBytes+w.pendLen(true)+grow <= w.max() && w.pendLen(true)+grow <= w.budget {
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
		if w.blocked() {
			return false
		}
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
	w.activity, w.traffic = true, true
	w.onlyPing = false
	return true
}

// datagram adds a DATAGRAM frame (LEN elided when last).
func (w *recWriter) datagram(data []byte, frameMax uint64) (ok, never bool) {
	capy := min(uint64(w.recMax), frameMax)
	n := datagramFrameLen(len(data), true)
	if uint64(n) > capy {
		// Only without its Length field, as the last frame of a record,
		// and never in a shaped record (padding may follow it).
		last := datagramFrameLen(len(data), false)
		if uint64(last) > capy || w.sh != nil {
			return false, true
		}
		if w.open {
			w.end()
			if w.blocked() {
				return false, false
			}
		}
		if !w.newRecordFits(last) {
			return false, false
		}
		w.addDatagram(data)
		w.end()
		return true, false
	}
	if !w.fits(n) {
		if !w.open || !w.newRecordFits(n) {
			return false, false
		}
		w.end()
		if w.blocked() {
			return false, false
		}
	}
	w.addDatagram(data)
	return true, false
}

// addDatagram makes data the pending DATAGRAM frame of the open record.
func (w *recWriter) addDatagram(data []byte) {
	w.begin()
	w.flushPend(true)
	w.pend = pendStream{on: true, data: data, dg: true, more: w.pend.more[:0], n: len(data)}
	w.elicit = true
	w.activity, w.traffic = true, true
	w.onlyPing = false
}

// datagramFrameLen returns the bytes of a DATAGRAM frame with n bytes of
// data (Flow and Context ID included): its type, its Length field when
// withLen, the data.
func datagramFrameLen(n int, withLen bool) int {
	if withLen {
		return 1 + varint.SizeVarint(uint64(n)) + n
	}
	return 1 + n
}

// datagramDataMax returns the most data whose DATAGRAM frame fits capy
// bytes, with or without its Length field; 0 when none fits.
func datagramDataMax(capy uint64, withLen bool) int {
	if !withLen {
		return int(max(capy, 1) - 1)
	}
	best := uint64(0)
	for k, top := range [...]uint64{1: 1<<6 - 1, 2: 1<<14 - 1, 4: 1<<30 - 1, 8: varint.MaxVarint} {
		if top != 0 && capy >= 1+uint64(k) {
			best = max(best, min(capy-1-uint64(k), top))
		}
	}
	return int(best)
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
	if w.sh != nil && w.sh.planned && w.recBytes+w.pendLen(false) > 0 {
		w.endShaped()
	} else {
		w.flushPend(false)
	}
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
		w.b.arena = varint.AppendVarint(w.b.arena, uint64(w.recBytes))
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

// max is the largest frames bytes of the open record.
func (w *recWriter) max() int {
	if w.cut > 0 {
		return w.cut
	}
	return w.recMax
}

// recordPositionBytes returns the bytes a record counts in positions.
func recordPositionBytes(size int) int {
	return varint.SizeVarint(uint64(size)) + size
}

// spendData charges n bytes of stream data to the limiter's room.
func (w *recWriter) spendData(n uint64) {
	w.dataSpent += n
	if w.dataRoom > 0 {
		w.dataRoom = max(w.dataRoom-int64(n), 0)
	}
}

// dataFit bounds the data bytes of a STREAM frame, room, by the shaping
// gate.
func (w *recWriter) dataFit(room int) int {
	if g := w.gateRoom(); g >= 0 && int64(room) > g {
		return int(g)
	}
	return room
}

// dataHeld reports that the limiter or the shaping gate lets no more
// stream data through in this batch.
func (w *recWriter) dataHeld() bool { return w.dataRoom == 0 || w.gateRoom() == 0 }
