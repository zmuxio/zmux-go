package core

import "sync/atomic"

// Buf is a read buffer of a pipe. Pieces of stream data may reference it;
// refs is atomic because the driver drops its own reference outside the
// session lock. Free returns it to its pool when the last reference goes.
type Buf struct {
	B    []byte
	refs atomic.Int32
	Free func(*Buf)
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
// smaller ones are copied (IMPLEMENTATION.md 3).
const refThreshold = 4096

type rpiece struct {
	off uint64
	b   []byte
	buf *Buf // nil: owned copy
}

// recvBuf holds received, unread stream data as disjoint pieces sorted by
// offset.
type recvBuf struct {
	ps    []rpiece
	bytes uint64
}

// insert stores the parts of [off, off+len(d)) not already present and
// returns the number of new bytes.
func (rb *recvBuf) insert(off uint64, d []byte, buf *Buf) uint64 {
	end := off + uint64(len(d))
	n := len(rb.ps)
	if n == 0 || off >= rb.ps[n-1].off+uint64(len(rb.ps[n-1].b)) {
		rb.appendPiece(off, d, buf)
		return uint64(len(d))
	}
	// Out of order: insert the gaps one by one.
	var added uint64
	cur := off
	for i := 0; i <= len(rb.ps) && cur < end; i++ {
		gapEnd := end
		if i < len(rb.ps) {
			p := rb.ps[i]
			pe := p.off + uint64(len(p.b))
			if pe <= cur {
				continue
			}
			if p.off <= cur {
				cur = pe
				continue
			}
			gapEnd = min64(end, p.off)
		}
		part := d[cur-off : gapEnd-off]
		rb.insertAt(i, cur, part, buf)
		added += uint64(len(part))
		cur = gapEnd
	}
	return added
}

func (rb *recvBuf) newPiece(off uint64, d []byte, buf *Buf) rpiece {
	if len(d) >= refThreshold && buf != nil {
		buf.Ref()
		return rpiece{off: off, b: d, buf: buf}
	}
	c := make([]byte, len(d))
	copy(c, d)
	return rpiece{off: off, b: c}
}

func (rb *recvBuf) appendPiece(off uint64, d []byte, buf *Buf) {
	rb.bytes += uint64(len(d))
	if n := len(rb.ps); n > 0 && (len(d) < refThreshold || buf == nil) {
		last := &rb.ps[n-1]
		if last.buf == nil && last.off+uint64(len(last.b)) == off && len(last.b)+len(d) <= refThreshold {
			last.b = append(last.b, d...)
			return
		}
	}
	rb.ps = append(rb.ps, rb.newPiece(off, d, buf))
}

func (rb *recvBuf) insertAt(i int, off uint64, d []byte, buf *Buf) {
	rb.bytes += uint64(len(d))
	rb.ps = append(rb.ps, rpiece{})
	copy(rb.ps[i+1:], rb.ps[i:])
	rb.ps[i] = rb.newPiece(off, d, buf)
}

// contiguous returns the end of data contiguous from off.
func (rb *recvBuf) contiguous(off uint64) uint64 {
	for _, p := range rb.ps {
		if p.off > off {
			break
		}
		if e := p.off + uint64(len(p.b)); e > off {
			off = e
		}
	}
	return off
}

// read copies contiguous data starting at off into dst and consumes it.
func (rb *recvBuf) read(off uint64, dst []byte) int {
	n := 0
	for len(rb.ps) > 0 && n < len(dst) {
		p := &rb.ps[0]
		if p.off > off+uint64(n) {
			break
		}
		skip := off + uint64(n) - p.off
		c := copy(dst[n:], p.b[skip:])
		n += c
		if skip+uint64(c) == uint64(len(p.b)) {
			rb.dropFirst()
		} else {
			p.b = p.b[skip+uint64(c):]
			p.off += skip + uint64(c)
		}
	}
	rb.bytes -= uint64(n)
	return n
}

func (rb *recvBuf) dropFirst() {
	if rb.ps[0].buf != nil {
		rb.ps[0].buf.Unref()
	}
	rb.ps[0] = rpiece{}
	rb.ps = rb.ps[1:]
	if len(rb.ps) == 0 {
		rb.ps = nil
	}
}

// discard drops everything below off and returns the bytes dropped.
func (rb *recvBuf) discard(off uint64) uint64 {
	var d uint64
	for len(rb.ps) > 0 {
		p := &rb.ps[0]
		pe := p.off + uint64(len(p.b))
		if p.off >= off {
			break
		}
		if pe <= off {
			d += uint64(len(p.b))
			rb.dropFirst()
			continue
		}
		cut := off - p.off
		d += cut
		p.b = p.b[cut:]
		p.off = off
		break
	}
	rb.bytes -= d
	return d
}

// discardFrom drops everything at or above off and returns the bytes dropped.
func (rb *recvBuf) discardFrom(off uint64) uint64 {
	var d uint64
	for len(rb.ps) > 0 {
		i := len(rb.ps) - 1
		p := &rb.ps[i]
		pe := p.off + uint64(len(p.b))
		if pe <= off {
			break
		}
		if p.off >= off {
			d += uint64(len(p.b))
			if p.buf != nil {
				p.buf.Unref()
			}
			rb.ps[i] = rpiece{}
			rb.ps = rb.ps[:i]
			continue
		}
		d += pe - off
		p.b = p.b[:off-p.off]
		break
	}
	rb.bytes -= d
	if len(rb.ps) == 0 {
		rb.ps = nil
	}
	return d
}

// clear drops everything.
func (rb *recvBuf) clear() uint64 {
	d := rb.bytes
	for _, p := range rb.ps {
		if p.buf != nil {
			p.buf.Unref()
		}
	}
	rb.ps = nil
	rb.bytes = 0
	return d
}

// Refs returns the current reference count.
func (b *Buf) Refs() int32 { return b.refs.Load() }

// take consumes up to max contiguous bytes at off without copying: the
// slice of the first piece and, when it references a read buffer, a new
// reference of that buffer for the caller.
func (rb *recvBuf) take(off uint64, max int) ([]byte, *Buf) {
	if len(rb.ps) == 0 || max <= 0 {
		return nil, nil
	}
	p := &rb.ps[0]
	if p.off > off {
		return nil, nil
	}
	skip := off - p.off
	d := p.b[skip:]
	if len(d) > max {
		d = d[:max]
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
		p.b = p.b[skip+n:]
		p.off += skip + n
	}
	rb.bytes -= n
	return d, buf
}
