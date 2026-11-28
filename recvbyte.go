package zmux

import (
	"errors"
	"io"

	"github.com/zmuxio/zmux-go/internal/core"
)

// byteReceiver owns the append-only buffers of one reader. A carry is
// the tail of a full block, never more than one incomplete record. Its
// successor is large enough to complete that record without moving bytes.
type byteReceiver struct {
	carry, current core.Span
	bulk           bool
}

func (r *byteReceiver) read(src io.Reader) (int, core.Input, error) {
	if r.current.Buf == nil {
		size := core.RecvIdleSize
		if r.carry.Buf != nil || r.bulk {
			size = core.RecvBlockSize
		}
		r.current = core.GetRecvBuf(size).Span(0, 0)
	}
	v := &r.current
	n, err := appendRead(src, v)
	input := core.Input{First: *v}
	if r.carry.Buf != nil {
		input = core.Input{First: r.carry, Second: *v}
	}
	return n, input, err
}

func appendRead(src io.Reader, v *core.Span) (int, error) {
	dst := v.Buf.B[v.End:]
	n, err := src.Read(dst)
	if n < 0 || n > len(dst) {
		return 0, errors.New("zmux: invalid carrier read count")
	}
	v.End += n
	return n, err
}

// byteReadState hands off owned prefix and nested ranges and uses the
// two-block byte receiver once no replay ranges remain.
type byteReadState struct {
	nested   func(int) (core.Span, bool, error)
	tail     core.Span
	regular  byteReceiver
	spans    core.ReceiveSpans
	replayed bool
	staged   bool
}

func (r *byteReadState) read(src io.Reader) (int, core.Input, error) {
	if !r.replayed {
		if first := takeBytePrefix(src); first != nil {
			r.staged = true
			if first.view.Len() == 0 {
				first.release()
				return 0, r.spans.Input(), first.end
			}
			return first.view.Len(), r.spans.Append(first.view, false), first.end
		}
		r.replayed = true
		r.nested = byteSpanReader(src)
	}
	if r.nested != nil {
		r.staged = true
		v, borrowed, err := r.nested(core.RecvBlockSize)
		return v.Len(), r.spans.Append(v, borrowed), err
	}
	if r.spans.Len() == 0 {
		r.staged = false
		return r.regular.read(src)
	}
	if r.tail.Buf == nil {
		r.tail = core.GetRecvBuf(core.RecvBlockSize).Span(0, 0)
	}
	first := r.tail.End == 0
	n, err := appendRead(src, &r.tail)
	if first {
		if n == 0 {
			r.tail.Buf.Unref()
			r.tail = core.Span{}
			return n, r.spans.Input(), err
		}
		return n, r.spans.Append(r.tail, false), err
	}
	return n, r.spans.Extend(n), err
}

func (r *byteReadState) advance(n int) {
	if !r.staged {
		r.regular.advance(n)
		return
	}
	r.spans.Advance(n)
	if r.spans.Len() == 0 || r.tail.Buf != nil && r.tail.End == len(r.tail.Buf.B) {
		r.tail = core.Span{}
	}
}

func (r *byteReadState) release() {
	if r.tail.Buf != nil && r.tail.End == 0 {
		r.tail.Buf.Unref()
	}
	r.spans.Release()
	r.regular.release()
}

func (r *byteReceiver) advance(n int) {
	if r.carry.Buf != nil {
		if n == 0 {
			return
		}
		n -= r.carry.Len()
		r.carry.Buf.Unref()
		r.carry = core.Span{}
	}
	r.current.Start += n
	space := len(r.current.Buf.B) - r.current.End
	switch {
	case r.current.Len() == 0:
		// Keep reading into the untouched tail during a bulk transfer.
		// A short record switches back to the idle class, and a nearly
		// full block rolls over without reading through an idle block.
		r.bulk = len(r.current.Buf.B) == core.RecvBlockSize && n >= core.RecvIdleSize
		switch {
		case !r.bulk:
			r.current.Buf.Unref()
			r.current = core.Span{}
		case space >= core.RecvIdleSize:
		case r.current.Buf.Refs() == 1:
			// All published views have gone away. Rewind the exclusive
			// backing without returning and reacquiring its charge.
			r.current.Start, r.current.End = 0, 0
		default:
			r.current.Buf.Unref()
			r.current = core.Span{}
		}
	case space == 0:
		r.carry, r.current = r.current, core.Span{}
	}
}

func (r *byteReceiver) release() {
	for _, v := range [...]core.Span{r.carry, r.current} {
		if v.Buf != nil {
			v.Buf.Unref()
		}
	}
}
