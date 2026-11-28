package wire

import "github.com/zmuxio/zmux-go/varint"

// ByteParts supplies contiguous pieces of a bounded input window.
type ByteParts interface {
	BytesAt(offset int) []byte
}

// Parts is a bounded byte window. Byte carriers normally use Head alone,
// or Head and Tail at a block boundary. A nested carrier can retain more
// pieces of one incomplete record.
type Parts struct {
	source     ByteParts
	Head, Tail []byte
	start, end int
}

func Segmented(source ByteParts, start, end int) Parts {
	if start == end {
		return Parts{}
	}
	head := source.BytesAt(start)
	if n := end - start; n <= len(head) {
		return Parts{Head: head[:n:n]}
	}
	return Parts{Head: head, source: source, start: start, end: end}
}

func (p Parts) Len() int {
	if p.source != nil {
		return p.end - p.start
	}
	return len(p.Head) + len(p.Tail)
}

func (p Parts) Slice(lo, hi int) Parts {
	cut := len(p.Head)
	switch {
	case hi <= cut:
		return Parts{Head: p.Head[lo:hi:hi]}
	case p.source != nil:
		return Segmented(p.source, p.start+lo, p.start+hi)
	case lo >= cut:
		return Parts{Head: p.Tail[lo-cut : hi-cut : hi-cut]}
	default:
		return Parts{Head: p.Head[lo:cut:cut], Tail: p.Tail[: hi-cut : hi-cut]}
	}
}

// Contiguous gives parsers that need contiguous control data a view when
// available, or a copy of just this validated interval at a block boundary.
func (p Parts) Contiguous() []byte {
	if len(p.Tail) == 0 && p.source == nil {
		return p.Head
	}
	return p.AppendTo(make([]byte, 0, p.Len()))
}

func (p Parts) AppendTo(dst []byte) []byte {
	dst = append(dst, p.Head...)
	if p.source == nil {
		return append(dst, p.Tail...)
	}
	for i := p.start + len(p.Head); i < p.end; {
		b := p.source.BytesAt(i)
		n := min(p.end-i, len(b))
		dst = append(dst, b[:n]...)
		i += n
	}
	return dst
}

func (p Parts) Byte(i int) byte {
	return p.contiguousAt(i)[0]
}

func (p Parts) contiguousAt(i int) []byte {
	switch {
	case i < len(p.Head):
		return p.Head[i:]
	case p.source != nil:
		if i == p.Len() {
			return nil
		}
		b := p.source.BytesAt(p.start + i)
		return b[:min(len(b), p.Len()-i)]
	default:
		return p.Tail[i-len(p.Head):]
	}
}

func (p Parts) Varint() (uint64, int) {
	v, n := varint.ConsumeVarint(p.Head)
	if n > 0 || len(p.Tail) == 0 && p.source == nil {
		return v, n
	}
	return p.splitVarint()
}

func (p Parts) splitVarint() (uint64, int) {
	var scratch [8]byte
	prefix := p.Slice(0, min(len(scratch), p.Len()))
	return varint.ConsumeVarint(prefix.AppendTo(scratch[:0]))
}
