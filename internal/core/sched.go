package core

// Stream scheduling: strict priority by urgency, deficit round robin with a
// 16 KiB quantum within a level.

const quantum = 16384

type schedLink struct {
	prev, next *Stream
	in         bool
	connWait   bool
	level      uint8
	deficit    int32
}

type scheduler struct {
	head     [8]*Stream
	connWait *Stream
	mask     uint8
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
	if m := sp.mps; m != nil && (m.resend.len() > 0 || m.finResend) {
		return true
	}
	if sp.finWr && !sp.finSent && !sp.reset {
		return true
	}
	return (st.pinned || st.openWant) && !st.opened || (!st.idSet && (sp.finWr || sp.q.end > sp.q.base))
}

func (sc *scheduler) ensure(st *Stream) {
	if st.sch.in || st.sch.connWait || !st.sendable() {
		return
	}
	l := st.urgency
	st.sch.level = l
	st.sch.in = true
	st.sch.deficit = 0
	if sc.head[l] == nil {
		sc.mask |= 1 << l
	}
	linkSched(&sc.head[l], st)
}

func linkSched(head **Stream, st *Stream) {
	h := *head
	if h == nil {
		st.sch.prev, st.sch.next = st, st
		*head = st
		return
	}
	tail := h.sch.prev
	st.sch.prev, st.sch.next = tail, h
	tail.sch.next = st
	h.sch.prev = st
}

func (sc *scheduler) remove(st *Stream) {
	if st.sch.connWait {
		st.sch.connWait = false
		unlinkSched(&sc.connWait, st)
		return
	}
	if !st.sch.in {
		return
	}
	l := st.sch.level
	st.sch.in = false
	unlinkSched(&sc.head[l], st)
	if sc.head[l] == nil {
		sc.mask &^= 1 << l
	}
}

func unlinkSched(head **Stream, st *Stream) {
	if st.sch.next == st {
		*head = nil
	} else {
		st.sch.prev.sch.next = st.sch.next
		st.sch.next.sch.prev = st.sch.prev
		if *head == st {
			*head = st.sch.next
		}
	}
	st.sch.prev, st.sch.next = nil, nil
}

func (sc *scheduler) waitConnectionCredit(st *Stream) {
	if !st.sch.connWait {
		sc.remove(st)
		st.sch.connWait = true
		linkSched(&sc.connWait, st)
	}
}

func (sc *scheduler) releaseConnectionCredit() {
	for sc.connWait != nil {
		sc.reinstate(sc.connWait)
	}
}

func (sc *scheduler) reinstate(st *Stream) {
	if st.sch.connWait {
		sc.remove(st)
	}
	sc.ensure(st)
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
	cur   *Stream // next to return, captured before current can be unlinked
	last  *Stream
	level int
}

func (sc *scheduler) iter() schedIter { return schedIter{sc: sc, level: -1} }

func (it *schedIter) next() *Stream {
	if it.cur == nil {
		it.level++
		for it.level < 8 && it.sc.mask&(1<<it.level) == 0 {
			it.level++
		}
		if it.level >= 8 {
			return nil
		}
		it.cur = it.sc.head[it.level]
		it.last = it.cur.sch.prev
	}
	st := it.cur
	it.cur = st.sch.next
	if st == it.last {
		it.cur = nil
	}
	return st
}
