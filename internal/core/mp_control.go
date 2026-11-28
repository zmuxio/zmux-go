package core

import (
	"cmp"
	"slices"
)

// pathControls indexes the owners of queued path control in wire order.
// It changes when work is queued, sent, or retired.
type pathControls []*Pipe

func pipeIDOrder(a, b *Pipe) int { return cmp.Compare(a.id, b.id) }

func (q *pathControls) contains(p *Pipe) bool {
	_, ok := slices.BinarySearchFunc(*q, p, pipeIDOrder)
	return ok
}

func (q *pathControls) add(p *Pipe) {
	i, ok := slices.BinarySearchFunc(*q, p, pipeIDOrder)
	if !ok {
		*q = slices.Insert(*q, i, p)
	}
}

func (q *pathControls) remove(p *Pipe) {
	i, ok := slices.BinarySearchFunc(*q, p, pipeIDOrder)
	if ok {
		q.removeAt(i)
	}
}

func (q *pathControls) removeAt(i int) { *q = slices.Delete(*q, i, i+1) }
