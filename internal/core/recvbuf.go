package core

import "sync/atomic"

// Buf is a read buffer of a pipe. Pieces of stream data may reference it;
// refs is atomic because the driver drops its own reference outside the
// session lock. Free returns it to its pool when the last reference goes.
type Buf struct {
	Free   func(*Buf)
	B      []byte
	owners []bufOwner
	// held is the bytes of B that buffered stream pieces reference, under
	// the lock of the one session whose pipe read B (recvBuf.pin), with
	// the pieces that took them (owners, for compaction) and B's index in
	// that session's pinSet while held is above 0.
	held uint64
	pidx int
	refs atomic.Int32
}

// bufOwner is a piece that referenced a Buf: its recvBuf and the offset it
// started at. The piece may since have been cut, truncated, compacted or
// dropped; compaction looks it up by offset.
type bufOwner struct {
	rb  *recvBuf
	off uint64
}

// pinSet is a session's read buffers that buffered pieces reference.
type pinSet struct {
	bufs []*Buf
	// extra is what the pinned buffers cost beyond the pieces' bytes: the
	// unreferenced rest of every pinned buffer plus pieceCost per piece.
	extra uint64
	size  uint64 // sum of len(B) over bufs
	held  uint64 // sum of held over bufs
}

// Compaction bounds what pinned buffers cost beyond the bytes their pieces
// reference: once that waste passes pinSlack and pinSparse times the
// referenced bytes (a slow reader of streams whose pieces each kept a
// mostly read or mostly empty buffer), the pieces of every buffer less
// than 1/pinSparse referenced are copied out and the buffers released.
// Afterwards each pinned buffer but the one being received into is at
// least 1/pinSparse referenced, so the waste is at most pinSparse-1 times
// the referenced bytes plus one buffer, below the trigger: the next
// compaction waits for at least pinSlack of new waste, and each byte is
// copied at most once. Bulk reading, whose buffers stay referenced in full
// until read, never reaches it and stays zero-copy.
const (
	pinSlack  = 1 << 20
	pinSparse = 4
)

// compactPins copies out the pieces of every sparsely referenced read
// buffer but cur (the one being received into) once the waste of the
// pinned buffers passed its bound.
func (s *Session) compactPins(cur *Buf) {
	ps := &s.conn.pins
	if w := ps.size - ps.held; w <= pinSlack || w <= pinSparse*ps.held {
		return
	}
	for i := 0; i < len(ps.bufs); {
		b := ps.bufs[i]
		if b == cur || b.held*pinSparse >= uint64(len(b.B)) {
			i++
			continue
		}
		b.release()
		// b left ps.bufs: bufs[i] is now another buffer.
	}
}

// release copies out every piece that references b, which leaves ps.
func (b *Buf) release() {
	for j := range len(b.owners) {
		o := b.owners[j]
		rb := o.rb
		// The piece, if still there, is the first one ending above where
		// it started (pieces are disjoint and sorted, and only shrink).
		// Should a change break that, the owner's pieces are scanned for
		// it: a piece of b is never skipped, or b would stay pinned.
		k := rb.after(o.off)
		if !rb.ps.valid(k) || rb.ps.at(k).buf != b {
			k = rb.find(b)
			if !rb.ps.valid(k) {
				continue
			}
		}
		p := rb.ps.at(k)
		c := make([]byte, len(p.b))
		copy(c, p.b)
		p.b, p.buf = c, nil
		last := b.held == uint64(len(c))
		rb.unhold(b, uint64(len(c)))
		b.Unref()
		if last {
			return // b may be in its pool already
		}
	}
	panic("zmux: pinned read buffer without its pieces")
}

// Ref adds a reference.
func (b *Buf) Ref() { b.refs.Add(1) }

// Unref drops a reference.
func (b *Buf) Unref() {
	if n := b.refs.Add(-1); n == 0 && b.Free != nil {
		b.Free(b)
	} else if n < 0 {
		panic("zmux: read buffer released twice")
	}
}

// refThreshold: pieces of at least this size reference the read buffer;
// smaller ones are copied.
const refThreshold = 4096

type rpiece struct {
	buf *Buf // nil: owned copy
	b   []byte
	off uint64
}

// recvBuf holds received, unread stream data as disjoint pieces sorted by
// offset.
type recvBuf struct {
	// pin is the session's count of read-buffer bytes that referenced
	// pieces keep from their pool beyond the pieces' own bytes: a piece
	// pins its whole buffer, so the session's memory and credit see
	// len(B) for every buffer a piece still references, not just the
	// piece's bytes (a peer would otherwise pin many buffers per byte it
	// is charged).
	pin   *pinSet
	ps    seq[rpiece]
	bytes uint64
}

// pieceCost is what a piece costs beyond its bytes (its entry and, for a
// copy, its allocation), charged with the pinned bytes (pin): a peer that
// splits its data into tiny pieces pays for them in the session's memory
// and credit, not just their bytes.
const pieceCost = 64

// hold accounts n more bytes of b referenced by a new piece at off.
func (rb *recvBuf) hold(b *Buf, n, off uint64) {
	ps := rb.pin
	if b.held == 0 {
		ps.extra += uint64(len(b.B))
		ps.size += uint64(len(b.B))
		b.pidx = len(ps.bufs)
		ps.bufs = append(ps.bufs, b)
	}
	b.held += n
	ps.held += n
	ps.extra -= n
	b.owners = append(b.owners, bufOwner{rb: rb, off: off})
}

// unhold accounts n bytes of b no longer referenced by a piece.
func (rb *recvBuf) unhold(b *Buf, n uint64) {
	ps := rb.pin
	b.held -= n
	ps.held -= n
	ps.extra += n
	if b.held == 0 {
		ps.extra -= uint64(len(b.B))
		ps.size -= uint64(len(b.B))
		last := len(ps.bufs) - 1
		ps.bufs[b.pidx] = ps.bufs[last]
		ps.bufs[b.pidx].pidx = b.pidx
		ps.bufs[last] = nil
		ps.bufs = ps.bufs[:last]
		clear(b.owners)
		b.owners = b.owners[:0]
	}
}

// cut drops the first n bytes of a piece.
func (rb *recvBuf) cut(p *rpiece, n uint64) {
	if p.buf != nil {
		rb.unhold(p.buf, n)
	}
	p.b = p.b[n:]
	p.off += n
}

// after returns the position of the first piece ending above off.
func (rb *recvBuf) after(off uint64) seqPos {
	q := &rb.ps
	if q.n == 0 || q.first().end() > off {
		return q.begin()
	}
	return q.search(func(p *rpiece) bool { return p.end() > off })
}

// find returns the position of the first piece that references b (the end
// if none): a linear scan, for release when the owner's offset misses.
func (rb *recvBuf) find(b *Buf) seqPos {
	q := &rb.ps
	i := q.begin()
	for ; q.valid(i); i = q.next(i) {
		if q.at(i).buf == b {
			break
		}
	}
	return i
}

func (p *rpiece) end() uint64 { return p.off + uint64(len(p.b)) }

// insert stores the parts of [off, off+len(d)) not already present and
// returns the number of new bytes.
func (rb *recvBuf) insert(off uint64, d []byte, buf *Buf) uint64 {
	end := off + uint64(len(d))
	q := &rb.ps
	if q.n == 0 || off >= q.last().end() {
		rb.appendPiece(off, d, buf)
		return uint64(len(d))
	}
	// Out of order: insert the gaps one by one.
	var added uint64
	cur := off
	for cur < end {
		i := rb.after(cur)
		gapEnd := end
		if q.valid(i) {
			p := q.at(i)
			if p.off <= cur {
				cur = p.end()
				continue
			}
			gapEnd = min64(end, p.off)
		}
		part := d[cur-off : gapEnd-off]
		rb.bytes += uint64(len(part))
		q.insert(i, rb.newPiece(cur, part, buf))
		rb.pin.extra += pieceCost
		added += uint64(len(part))
		cur = gapEnd
	}
	return added
}

func (rb *recvBuf) newPiece(off uint64, d []byte, buf *Buf) rpiece {
	if len(d) >= refThreshold && buf != nil {
		buf.Ref()
		rb.hold(buf, uint64(len(d)), off)
		return rpiece{off: off, b: d, buf: buf}
	}
	c := make([]byte, len(d))
	copy(c, d)
	return rpiece{off: off, b: c}
}

func (rb *recvBuf) appendPiece(off uint64, d []byte, buf *Buf) {
	rb.bytes += uint64(len(d))
	if rb.ps.n > 0 && (len(d) < refThreshold || buf == nil) {
		last := rb.ps.last()
		if last.buf == nil && last.end() == off && len(last.b)+len(d) <= refThreshold {
			last.b = append(last.b, d...)
			return
		}
	}
	rb.ps.push(rb.newPiece(off, d, buf))
	rb.pin.extra += pieceCost
}

// contiguous returns the end of data contiguous from off.
func (rb *recvBuf) contiguous(off uint64) uint64 {
	q := &rb.ps
	for i := rb.after(off); q.valid(i); i = q.next(i) {
		p := q.at(i)
		if p.off > off {
			break
		}
		off = p.end()
	}
	return off
}

// read copies contiguous data starting at off into dst and consumes it.
func (rb *recvBuf) read(off uint64, dst []byte) int {
	n := 0
	for rb.ps.n > 0 && n < len(dst) {
		p := rb.ps.first()
		if p.off > off+uint64(n) {
			break
		}
		skip := off + uint64(n) - p.off
		c := copy(dst[n:], p.b[skip:])
		n += c
		if skip+uint64(c) == uint64(len(p.b)) {
			rb.dropFirst()
		} else {
			rb.cut(p, skip+uint64(c))
		}
	}
	rb.bytes -= uint64(n)
	return n
}

// drop releases what piece p holds.
func (rb *recvBuf) drop(p *rpiece) {
	rb.pin.extra -= pieceCost
	if p.buf != nil {
		rb.unhold(p.buf, uint64(len(p.b)))
		p.buf.Unref()
	}
}

func (rb *recvBuf) dropFirst() {
	rb.drop(rb.ps.first())
	rb.ps.popFirst()
}

// discard drops everything below off and returns the bytes dropped.
func (rb *recvBuf) discard(off uint64) uint64 {
	var d uint64
	for rb.ps.n > 0 {
		p := rb.ps.first()
		if p.off >= off {
			break
		}
		if p.end() <= off {
			d += uint64(len(p.b))
			rb.dropFirst()
			continue
		}
		cut := off - p.off
		d += cut
		rb.cut(p, cut)
		break
	}
	rb.bytes -= d
	return d
}

// discardFrom drops everything at or above off and returns the bytes dropped.
func (rb *recvBuf) discardFrom(off uint64) uint64 {
	var d uint64
	for rb.ps.n > 0 {
		p := rb.ps.last()
		pe := p.end()
		if pe <= off {
			break
		}
		if p.off >= off {
			d += uint64(len(p.b))
			rb.drop(p)
			rb.ps.popLast()
			continue
		}
		d += pe - off
		if p.buf != nil {
			rb.unhold(p.buf, pe-off)
		}
		p.b = p.b[:off-p.off]
		break
	}
	rb.bytes -= d
	return d
}

// clear drops everything.
func (rb *recvBuf) clear() uint64 {
	d := rb.bytes
	for _, c := range rb.ps.cs {
		for i := range c {
			rb.drop(&c[i])
		}
	}
	rb.ps = seq[rpiece]{}
	rb.bytes = 0
	return d
}

// Refs returns the current reference count.
func (b *Buf) Refs() int32 { return b.refs.Load() }

// take consumes up to limit contiguous bytes at off without copying: the
// slice of the first piece and, when it references a read buffer, a new
// reference of that buffer for the caller.
func (rb *recvBuf) take(off uint64, limit int) ([]byte, *Buf) {
	if rb.ps.n == 0 || limit <= 0 {
		return nil, nil
	}
	p := rb.ps.first()
	if p.off > off {
		return nil, nil
	}
	skip := off - p.off
	d := p.b[skip:]
	if len(d) > limit {
		d = d[:limit]
	}
	buf := p.buf
	if buf != nil {
		buf.Ref()
	}
	n := uint64(len(d))
	if skip+n == uint64(len(p.b)) {
		if buf == nil {
			p.buf = nil // ownership of the copy passes to the caller
		}
		rb.dropFirst()
	} else {
		if buf == nil {
			// The caller gets its own copy; the rest stays.
			d = append([]byte(nil), d...)
		}
		rb.cut(p, skip+n)
	}
	rb.bytes -= n
	return d, buf
}
