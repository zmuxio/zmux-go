package core

// seq is an ordered sequence kept in chunks of at most seqChunk elements,
// for the sorted sets a peer can fragment (received ranges and pieces): an
// insert or removal inside it moves at most one chunk's elements and, when
// a chunk splits, merges or empties, the chunk headers (one per many
// elements), not the whole sequence, so a peer that fills the gaps of a
// fragmented stream in the costliest order pays a bounded cost per piece.
// Appending and consuming from the front, the common uses, touch the last
// or the first chunk only.
type seq[T any] struct {
	cs [][]T
	n  int
}

// seqChunk is the most elements of a chunk; a chunk inside the sequence
// that falls below seqChunk/4 merges with a neighbour that has room.
const seqChunk = 64

// seqPos is a position in a seq: chunk c, element i. The end position is
// {len(cs), 0}.
type seqPos struct{ c, i int }

func (q *seq[T]) len() int { return q.n }

func (q *seq[T]) begin() seqPos { return seqPos{} }

func (q *seq[T]) end() seqPos { return seqPos{len(q.cs), 0} }

func (q *seq[T]) valid(p seqPos) bool { return p.c < len(q.cs) }

func (q *seq[T]) at(p seqPos) *T { return &q.cs[p.c][p.i] }

func (q *seq[T]) next(p seqPos) seqPos {
	if p.i++; p.i == len(q.cs[p.c]) {
		p.c, p.i = p.c+1, 0
	}
	return p
}

// first and last return the first and the last element; the sequence
// must not be empty.
func (q *seq[T]) first() *T { return &q.cs[0][0] }

func (q *seq[T]) last() *T {
	c := q.cs[len(q.cs)-1]
	return &c[len(c)-1]
}

// search returns the position of the first element for which f is true,
// f being false and then true over the sequence; end when there is none.
func (q *seq[T]) search(f func(*T) bool) seqPos {
	lo, hi := 0, len(q.cs)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if c := q.cs[m]; f(&c[len(c)-1]) {
			hi = m
		} else {
			lo = m + 1
		}
	}
	if lo == len(q.cs) {
		return q.end()
	}
	c := q.cs[lo]
	i, j := 0, len(c)-1 // f holds at j
	for i < j {
		m := int(uint(i+j) >> 1)
		if f(&c[m]) {
			j = m
		} else {
			i = m + 1
		}
	}
	return seqPos{lo, i}
}

// push appends v.
func (q *seq[T]) push(v T) {
	if k := len(q.cs); k > 0 && len(q.cs[k-1]) < seqChunk {
		q.cs[k-1] = append(q.cs[k-1], v)
	} else {
		q.cs = append(q.cs, []T{v})
	}
	q.n++
}

// insert puts v before position p (end: appends).
func (q *seq[T]) insert(p seqPos, v T) {
	if p.c == len(q.cs) {
		q.push(v)
		return
	}
	if p.i == 0 && p.c > 0 && len(q.cs[p.c-1]) < seqChunk {
		// At the end of the previous chunk, which has room.
		q.cs[p.c-1] = append(q.cs[p.c-1], v)
		q.n++
		return
	}
	c := q.cs[p.c]
	if len(c) == seqChunk {
		// Split the full chunk in halves.
		h := seqChunk / 2
		right := make([]T, seqChunk-h, seqChunk)
		copy(right, c[h:])
		clear(c[h:])
		q.cs = append(q.cs, nil)
		copy(q.cs[p.c+2:], q.cs[p.c+1:])
		q.cs[p.c], q.cs[p.c+1] = c[:h], right
		if p.i > h {
			p.c, p.i = p.c+1, p.i-h
		}
		c = q.cs[p.c]
	}
	var zero T
	c = append(c, zero)
	copy(c[p.i+1:], c[p.i:])
	c[p.i] = v
	q.cs[p.c] = c
	q.n++
}

// popFirst removes the first element.
func (q *seq[T]) popFirst() {
	c := q.cs[0]
	var zero T
	c[0] = zero
	if q.n--; q.n == 0 {
		q.emptied()
	} else if c = c[1:]; len(c) > 0 {
		q.cs[0] = c
	} else {
		q.cs[0] = nil
		q.cs = q.cs[1:]
	}
}

// popLast removes the last element.
func (q *seq[T]) popLast() {
	k := len(q.cs) - 1
	c := q.cs[k]
	var zero T
	c[len(c)-1] = zero
	if q.n--; q.n == 0 {
		q.emptied()
	} else if c = c[:len(c)-1]; len(c) > 0 {
		q.cs[k] = c
	} else {
		q.cs[k] = nil
		q.cs = q.cs[:k]
	}
}

// remove removes k elements from position p on.
func (q *seq[T]) remove(p seqPos, k int) {
	q.n -= k
	c0 := p.c
	for k > 0 {
		c := q.cs[p.c]
		m := min(k, len(c)-p.i)
		k -= m
		copy(c[p.i:], c[p.i+m:])
		clear(c[len(c)-m:])
		if c = c[:len(c)-m]; len(c) == 0 {
			q.dropChunk(p.c) // p.c is the next chunk now
			p.i = 0
			continue
		}
		q.cs[p.c] = c
		p = seqPos{p.c + 1, 0}
	}
	if q.n == 0 {
		q.emptied()
		return
	}
	// Only the chunks at c0 and after it shrank.
	if c0+1 < len(q.cs) {
		q.mergeSmall(c0 + 1)
	}
	if c0 < len(q.cs) {
		q.mergeSmall(c0)
	}
}

// emptied keeps a small chunk index for the next elements (a stream that
// buffers and reads repeatedly), not a large one.
func (q *seq[T]) emptied() {
	if cap(q.cs) > 4 {
		q.cs = nil
		return
	}
	clear(q.cs[:cap(q.cs)])
	q.cs = q.cs[:0]
}

func (q *seq[T]) dropChunk(c int) {
	copy(q.cs[c:], q.cs[c+1:])
	q.cs[len(q.cs)-1] = nil
	q.cs = q.cs[:len(q.cs)-1]
}

// mergeSmall merges chunk c, when it fell below seqChunk/4, with a
// neighbour that has room for it, keeping the chunk count proportional to
// the elements.
func (q *seq[T]) mergeSmall(c int) {
	if len(q.cs[c]) >= seqChunk/4 {
		return
	}
	switch {
	case c+1 < len(q.cs) && len(q.cs[c])+len(q.cs[c+1]) <= seqChunk:
		q.cs[c] = append(q.cs[c], q.cs[c+1]...)
		clear(q.cs[c+1])
		q.dropChunk(c + 1)
	case c > 0 && len(q.cs[c-1])+len(q.cs[c]) <= seqChunk:
		q.cs[c-1] = append(q.cs[c-1], q.cs[c]...)
		clear(q.cs[c])
		q.dropChunk(c)
	}
}
