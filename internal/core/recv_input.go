package core

import "github.com/zmuxio/zmux-go/internal/wire"

// Input is an immutable window into the byte carrier's pending blocks.
type Input struct {
	list          *ReceiveSpans
	First, Second Span
	start, end    int
}

func (v Input) Len() int {
	if v.list != nil {
		return v.end - v.start
	}
	return v.First.Len() + v.Second.Len()
}

func (v Input) Bytes() wire.Parts {
	if v.list != nil {
		return wire.Segmented(v.list, v.start, v.end)
	}
	return wire.Parts{Head: v.First.Bytes(), Tail: v.Second.Bytes()}
}

func (v Input) Slice(lo, hi int) Input {
	if v.list != nil {
		return v.list.window(v.start+lo, v.start+hi)
	}
	n := v.First.Len()
	switch {
	case hi <= n:
		a := v.First
		return Input{First: a.Buf.Span(a.Start+lo, a.Start+hi)}
	case lo >= n:
		b := v.Second
		return Input{First: b.Buf.Span(b.Start+lo-n, b.Start+hi-n)}
	default:
		v.First.Start += lo
		v.Second.End = v.Second.Start + hi - n
		return v
	}
}

func (v Input) spanAt(offset int) Span {
	if v.list != nil {
		x := v.list.spanAt(v.start + offset)
		x.End = min(x.End, x.Start+v.Len()-offset)
		return x
	}
	if offset < v.First.Len() {
		x := v.First
		x.Start += offset
		return x
	}
	x := v.Second
	x.Start += offset - v.First.Len()
	return x
}

// ReceiveSpans is the bounded carry of a nested byte carrier. Each
// constituent owns an independent hold on its source until Advance.
type ReceiveSpans struct {
	items       []receiveSpan
	head        int
	first, last uint64
}

type receiveSpan struct {
	view     Span
	end      uint64
	borrowed bool
}

func (s *ReceiveSpans) Append(v Span, borrowed bool) Input {
	if v.Len() > 0 {
		s.last += uint64(v.Len())
		if len(s.items) == cap(s.items) && s.head >= len(s.items)/2 && s.head != 0 {
			n := copy(s.items, s.items[s.head:])
			clear(s.items[n:])
			s.items, s.head = s.items[:n], 0
		}
		s.items = append(s.items, receiveSpan{v, s.last, borrowed})
	}
	return s.window(0, int(s.last-s.first))
}

// Len is the remaining number of bytes in the carry.
func (s *ReceiveSpans) Len() int { return int(s.last - s.first) }

// Extend grows only the unpublished tail of the last producer's block.
func (s *ReceiveSpans) Extend(n int) Input {
	s.last += uint64(n)
	last := &s.items[len(s.items)-1]
	last.view.End += n
	last.end = s.last
	return s.window(0, s.Len())
}

func (s *ReceiveSpans) Input() Input { return s.window(0, s.Len()) }

func (s *ReceiveSpans) window(start, end int) Input {
	if start == end {
		return Input{}
	}
	first := s.spanAt(start)
	n := end - start
	if first.Len() >= n {
		first.End = first.Start + n
		return Input{First: first}
	}
	second := s.spanAt(start + first.Len())
	if left := n - first.Len(); second.Len() >= left {
		second.End = second.Start + left
		return Input{First: first, Second: second}
	}
	return Input{list: s, start: start, end: end}
}

func (s *ReceiveSpans) spanAt(offset int) Span {
	pos := s.first + uint64(offset)
	lo, hi := s.head, len(s.items)
	for lo < hi {
		m := lo + (hi-lo)/2
		if s.items[m].end <= pos {
			lo = m + 1
		} else {
			hi = m
		}
	}
	item := s.items[lo]
	item.view.Start = item.view.End - int(item.end-pos)
	return item.view
}

// BytesAt belongs to the bounded wire decoder.
func (s *ReceiveSpans) BytesAt(offset int) []byte { return s.spanAt(offset).Bytes() }

func (s *ReceiveSpans) Advance(n int) {
	s.first += uint64(n)
	for s.head < len(s.items) && s.items[s.head].end <= s.first {
		item := s.items[s.head]
		s.items[s.head] = receiveSpan{}
		s.head++
		if item.borrowed {
			item.view.Buf.ReturnView()
		} else {
			item.view.Buf.Unref()
		}
	}
	if s.head == len(s.items) {
		s.items, s.head = s.items[:0], 0
		s.first, s.last = 0, 0
	}
}

func (s *ReceiveSpans) Release() { s.Advance(int(s.last - s.first)) }
