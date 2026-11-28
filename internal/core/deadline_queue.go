package core

import "container/heap"

// Deadline is an indexed timer entry. Its owner reuses it for every update.
// Queue operations and the associated protocol state share one lock.
type Deadline[T any] struct {
	Value T
	Order int // ordering among entries with the same instant
	at    Time
	index int // one plus the queue index, or zero when disarmed
}

// DeadlineQueue stores at most one entry per owner and deadline reason.
type DeadlineQueue[T any] struct {
	entries deadlineHeap[T]
}

func (q *DeadlineQueue[T]) Next() Time {
	if d := q.Peek(); d != nil {
		return d.at
	}
	return Never
}

func (q *DeadlineQueue[T]) Peek() *Deadline[T] {
	if len(q.entries) == 0 {
		return nil
	}
	return q.entries[0]
}

func (q *DeadlineQueue[T]) Set(d *Deadline[T], at Time) {
	if at == Never {
		q.Cancel(d)
		return
	}
	if d.index == 0 {
		d.at = at
		heap.Push(&q.entries, d)
	} else if d.at != at {
		d.at = at
		heap.Fix(&q.entries, d.index-1)
	}
}

func (q *DeadlineQueue[T]) Cancel(d *Deadline[T]) {
	if d.index != 0 {
		heap.Remove(&q.entries, d.index-1)
	}
}

func (q *DeadlineQueue[T]) PopDue(now Time) *Deadline[T] {
	if len(q.entries) > 0 && q.entries[0].at <= now {
		return heap.Pop(&q.entries).(*Deadline[T])
	}
	return nil
}

func (q *DeadlineQueue[T]) Clear() {
	for _, d := range q.entries {
		d.index = 0
	}
	clear(q.entries)
	q.entries = q.entries[:0]
}

type deadlineHeap[T any] []*Deadline[T]

func (h *deadlineHeap[T]) Len() int { return len(*h) }
func (h *deadlineHeap[T]) Less(i, j int) bool {
	entries := *h
	return entries[i].at < entries[j].at || entries[i].at == entries[j].at && entries[i].Order < entries[j].Order
}

func (h *deadlineHeap[T]) Swap(i, j int) {
	entries := *h
	entries[i], entries[j] = entries[j], entries[i]
	entries[i].index, entries[j].index = i+1, j+1
}

func (h *deadlineHeap[T]) Push(x any) {
	d := x.(*Deadline[T])
	*h = append(*h, d)
	d.index = len(*h)
}

func (h *deadlineHeap[T]) Pop() any {
	last := len(*h) - 1
	d := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	d.index = 0
	return d
}
