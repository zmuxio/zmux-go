package core

import "sort"

// Handoff reports charged application bytes at their first successful
// complete-record handoff, and returns bytes that can no longer be sent.
// All mutations happen under the owning session lock.
type Handoff struct {
	datagram    func(native bool)
	session     *Session
	report      func(sent, unsent int)
	covered     rangeSet
	size, first uint64
	refs        int
}

func (s *Session) newHandoff(n int, report func(sent, unsent int)) *Handoff {
	return &Handoff{session: s, size: uint64(n), report: report, refs: 1}
}

func (h *Handoff) admit(lo, hi uint64) {
	if h.report == nil {
		return
	}
	var n uint64
	if lo == 0 && hi == h.size && h.first == 0 {
		n = hi
	} else {
		n = h.covered.add(lo, hi)
	}
	h.first += n
	h.notify(int(n), 0)
	if h.first == h.size {
		h.report = nil
		h.covered = rangeSet{}
	}
}

func (h *Handoff) unref() {
	h.refs--
	if h.refs == 0 && h.report != nil {
		h.notify(0, int(h.size-h.first))
		h.report = nil
		h.covered = rangeSet{}
	}
}

func (h *Handoff) notify(sent, unsent int) {
	if sent != 0 || unsent != 0 {
		h.session.handoffEvents = append(h.session.handoffEvents, HandoffEvent{sent: sent, unsent: unsent, report: h.report})
	}
}

// HandoffEvent is a send accounting notification. Deliver it outside
// the session lock, after the owning session drains it.
type HandoffEvent struct {
	datagram     func(native bool)
	report       func(sent, unsent int)
	sent, unsent int
	native       bool
}

func (e HandoffEvent) Deliver() {
	if e.datagram != nil {
		e.datagram(e.native)
	} else {
		e.report(e.sent, e.unsent)
	}
}

func (h *Handoff) datagramAccepted(native bool) {
	if h.datagram != nil {
		h.session.handoffEvents = append(h.session.handoffEvents, HandoffEvent{datagram: h.datagram, native: native})
		h.datagram = nil
	}
}

// TakeHandoffEvents moves the accounting notifications out of the session.
func (s *Session) TakeHandoffEvents() []HandoffEvent {
	events := s.handoffEvents
	s.handoffEvents = nil
	return events
}

type batchHandoff struct {
	ref    *Handoff
	lo, hi uint64
	record int
}

func (b *Batch) addHandoff(h *Handoff, lo, hi uint64) {
	if h.report != nil || h.datagram != nil {
		h.hold()
		b.handoffs = append(b.handoffs, batchHandoff{ref: h, lo: lo, hi: hi, record: len(b.recs)})
	}
}

type queueHandoff struct {
	receipt    *Handoff
	start, end uint64
}

func (q *sendQ) markHandoffs(b *Batch, lo, hi uint64) {
	if len(q.handoffs) == 0 || lo == hi {
		return
	}
	i := sort.Search(len(q.handoffs), func(i int) bool { return q.handoffs[i].end > lo })
	for _, h := range q.handoffs[i:] {
		if h.start >= hi {
			break
		}
		b.addHandoff(h.receipt, max64(h.start, lo)-h.start, min64(h.end, hi)-h.start)
	}
}

func (q *sendQ) releaseHandoffs(off uint64) {
	k := 0
	for k < len(q.handoffs) && q.handoffs[k].end <= off {
		q.handoffs[k].receipt.unref()
		k++
	}
	if k > 0 {
		clear(q.handoffs[:k])
		q.handoffs = q.handoffs[k:]
		if len(q.handoffs) == 0 {
			q.handoffs = nil
		}
	}
}

func (q *sendQ) untrackedPrefix(off, n uint64) uint64 {
	for _, h := range q.handoffs {
		if h.start >= off+n {
			break
		}
		if h.end > off && h.receipt.report != nil {
			return max64(off, h.start) - off
		}
	}
	return n
}

func (b *Batch) settleHandoffs(complete uint64) {
	for _, a := range b.handoffs {
		if uint64(a.record) < complete {
			a.ref.datagramAccepted(false)
			a.ref.admit(a.lo, a.hi)
		}
		a.ref.unref()
	}
	clear(b.handoffs)
	b.handoffs = b.handoffs[:0]
}

// Handoffs reports whether the writer needs exact message-prefix progress.
func (b *Batch) Handoffs() bool { return len(b.handoffs) != 0 }
