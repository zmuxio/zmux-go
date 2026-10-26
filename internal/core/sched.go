package core

// Stream scheduling: strict priority by urgency, deficit round robin with a
// 16 KiB quantum within a level (IMPLEMENTATION.md 5).

const quantum = 16384

type schedLink struct {
	prev, next *Stream
	in         bool
	level      uint8
	deficit    int32
}

type scheduler struct {
	head [8]*Stream
	mask uint8
}

// sendable reports whether the stream has anything for the data scheduler:
// unsent bytes, a FIN, or an owed opening frame.
func (st *Stream) sendable() bool {
	if !st.hasS || st.cancelled || st.discarded {
		return false
	}
	sp := &st.snd
	if sp.state == sendDataRecvd || sp.state == sendResetRecvd || sp.state == sendResetSent {
		return false
	}
	if st.unsent() > 0 {
		return true
	}
	if m := sp.mps; m != nil && (len(m.resend) > 0 || m.finResend) {
		return true
	}
	if sp.finWr && !sp.finSent && !sp.reset {
		return true
	}
	return st.pinned && !st.opened || (!st.idSet && (sp.finWr || sp.q.end > sp.q.base))
}

func (sc *scheduler) ensure(st *Stream) {
	if st.sch.in || !st.sendable() {
		return
	}
	l := st.urgency
	st.sch.level = l
	st.sch.in = true
	st.sch.deficit = 0
	h := sc.head[l]
	if h == nil {
		st.sch.prev, st.sch.next = st, st
		sc.head[l] = st
		sc.mask |= 1 << l
		return
	}
	tail := h.sch.prev
	st.sch.prev, st.sch.next = tail, h
	tail.sch.next = st
	h.sch.prev = st
}

func (sc *scheduler) remove(st *Stream) {
	if !st.sch.in {
		return
	}
	l := st.sch.level
	st.sch.in = false
	if st.sch.next == st {
		sc.head[l] = nil
		sc.mask &^= 1 << l
	} else {
		st.sch.prev.sch.next = st.sch.next
		st.sch.next.sch.prev = st.sch.prev
		if sc.head[l] == st {
			sc.head[l] = st.sch.next
		}
	}
	st.sch.prev, st.sch.next = nil, nil
}

func (sc *scheduler) reprio(st *Stream) {
	if st.sch.in && st.sch.level != st.urgency {
		sc.remove(st)
		sc.ensure(st)
	}
}

// rotate moves the head of st's level to the next stream (end of turn).
func (sc *scheduler) rotate(st *Stream) {
	if st.sch.in && sc.head[st.sch.level] == st {
		sc.head[st.sch.level] = st.sch.next
		st.sch.deficit = 0
	}
}

// iterator over ready streams in priority order, each visited once.
type schedIter struct {
	sc    *scheduler
	level int
	cur   *Stream
	first *Stream
}

func (sc *scheduler) iter() schedIter { return schedIter{sc: sc, level: -1} }

func (it *schedIter) next() *Stream {
	if it.cur != nil {
		n := it.cur.sch.next
		if n != it.first && n != nil && n.sch.in && n.sch.level == uint8(it.level) {
			it.cur = n
			return n
		}
		it.cur = nil
	}
	it.level++
	for it.level < 8 && it.sc.mask&(1<<it.level) == 0 {
		it.level++
	}
	if it.level >= 8 {
		return nil
	}
	it.first = it.sc.head[it.level]
	it.cur = it.first
	return it.cur
}
