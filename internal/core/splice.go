package core

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// SpliceReadState describes the application byte coordinate of the next
// read and any announced FIN or reset boundary.
type SpliceReadState struct {
	Reset                  *StreamError
	Ready, Offset, ResetAt uint64
	End, FIN               bool
}

func (s *Session) SpliceReadState(st *Stream) SpliceReadState {
	r := &st.rcv
	n, end := s.ReadReady(st)
	state := SpliceReadState{Ready: n, Offset: st.cnt.appRead, End: end, FIN: r.finKnown && !r.reset,
		ResetAt: r.resetRel - min64(r.resetRel, r.preSize)}
	if r.reset {
		state.Reset = r.errState
	}
	return state
}

// SpliceBindable reports whether the application's two halves are
// available for a relay. A pending peer FIN or reset is still readable.
func (s *Session) SpliceBindable(st *Stream) bool {
	return st.bidi && !st.releasedR && !st.releasedS && s.writable(st)
}

// SpliceWriteOffset includes data written by the application before splice.
// The protocol core adds the wire preamble when encoding the boundary.
func (s *Session) SpliceWriteOffset(st *Stream) uint64 { return st.snd.appWritten }

// SpliceResetBoundary preserves a complete application prefix without
// extending it to an incomplete capsule. An earlier application promise
// remains valid.
func (s *Session) SpliceResetBoundary(st *Stream, boundary uint64) {
	sp := &st.snd
	sp.boundary = max64(sp.boundary, min64(sp.appWritten, boundary))
	sp.hasBound = true
}

// SpliceResetAt reports whether the peer can receive RESET_STREAM_AT.
func (s *Session) SpliceResetAt() bool { return s.peer.Has&wire.HasResetAt != 0 }

func (s *Session) canUseCapsule(st *Stream, cx uint64, size int, strict bool, deadline, now Time) bool {
	if strict || st == nil || st.appReleased() || s.err != nil || deadline <= now ||
		s.datagramExhausted() || cx > varint.MaxVarint {
		return false
	}
	limit, known := s.streamDatagramStructuralLimit(st, cx)
	return known && size > limit
}

// streamDatagramStructuralLimit returns a static payload bound. The bool
// distinguishes an established bound from a hop still negotiating.
func (s *Session) streamDatagramStructuralLimit(st *Stream, cx uint64) (int, bool) {
	if !s.peerSeen {
		return 0, false
	}
	if !s.feat.DatagramFamily || s.peerMaxDgram == 0 || !st.bidi {
		return -1, true
	}
	// An unassigned Flow may take more bytes when it opens. Use the
	// protocol minimum to avoid treating that transient state as oversized.
	flow := 1
	if st.idSet {
		flow = varint.SizeVarint(st.id + 1)
	}
	head := flow + varint.SizeVarint(cx) + s.datagramSequenceWidth()
	return int(s.peerMaxDgram) - 1 - head, true
}

// SpliceFlushPrefix starts the ordinary-reset fallback and returns the wire
// target for the complete application prefix.
func (s *Session) SpliceFlushPrefix(st *Stream, boundary uint64) (uint64, error) {
	_, err := s.Flush(st)
	if err != nil {
		return 0, err
	}
	return st.snd.preLen + boundary, nil
}

// SpliceDatagramClass prices the group in the target's current state before
// the direction asks its limiter. Admission rechecks the choice under lock.
func (s *Session) SpliceDatagramClass(r DatagramRequest, members []DatagramGroupMember) (DatagramAdmission, error) {
	_, class, err := s.planDatagramGroup(r, members)
	return class, err
}

// DatagramGroupMember is an owned view of one child of a transformed datagram.
type DatagramGroupMember struct {
	Payload []byte
	Backing *Buf
	Queued  bool
}

func (s *Session) planDatagramGroup(r DatagramRequest, members []DatagramGroupMember) (datagramPlan, DatagramAdmission, error) {
	largest := 0
	for _, member := range members {
		largest = max(largest, len(member.Payload))
	}
	plan, result, err := s.planDatagram(r, largest)
	if err == nil && s.feat.DatagramFamily && s.dg != nil &&
		uint64(len(members)) > datagramSequenceSpace-s.dg.nextSequence {
		return plan, DatagramRefused, s.rejectDatagram(DatagramDropSequenceExhausted)
	}
	return plan, result, err
}

// AdmitDatagramGroup chooses one representation and reserves all children
// before any child can enter the datagram queue.
func (s *Session) AdmitDatagramGroup(r DatagramRequest, members []DatagramGroupMember) (DatagramAdmission, error) {
	for i := range members {
		members[i].Queued = false
	}
	plan, result, err := s.planDatagramGroup(r, members)
	if result != DatagramQueued {
		return result, err
	}
	s.pruneDatagramHeads()
	d := s.dgState()
	slots, room := s.datagramSendSlots(), s.datagramSendRoom()
	if s.feat.DatagramFamily {
		flowSlots, flowBytes := s.cfg.DatagramFlowQ, s.cfg.DatagramFlowBytes
		if f := d.flows[r.Stream]; f != nil {
			flowSlots -= len(f.queue) + f.nativeCount
			flowBytes -= f.bytes + f.nativeBytes
		}
		slots, room = min(slots, flowSlots), min(room, flowBytes)
	}
	if len(members) > slots {
		return DatagramRefused, s.rejectDatagram(DatagramDropQueueOverflow)
	}
	var copied uint64
	views := make([]dgramView, 0, len(members))
	defer func() {
		for _, view := range views {
			view.unref()
		}
	}()
	for _, member := range members {
		r.Payload, r.Backing = member.Payload, member.Backing
		view, n, err := s.allocateDatagram(r, plan.prefix[:plan.prefixLen], d)
		if err != nil {
			return DatagramRefused, err
		}
		views = append(views, view)
		copied += uint64(n)
		room -= view.cost()
		if room < 0 {
			return DatagramRefused, s.rejectDatagram(DatagramDropQueueOverflow)
		}
	}
	if err := s.pinDatagramStream(r.Stream); err != nil {
		return DatagramRefused, err
	}
	for i, view := range views {
		member := members[i]
		candidate := outDgram{data: view, st: r.Stream, n: len(member.Payload), opts: plan.options, deadline: plan.deadline, at: s.now}
		if ready, _ := candidate.ready(); ready && s.datagramPathFor(&candidate, true).late {
			return DatagramRefused, s.rejectDatagram(DatagramDropDeadline)
		}
	}
	first := 0
	if f := d.flows[r.Stream]; f != nil {
		first = len(f.queue)
	}
	var f *datagramFlow
	for i := range members {
		r.Payload = members[i].Payload
		entry := s.datagramEntry(r, plan, views[i], d)
		s.attachDatagramHandoff(&entry, r)
		f = d.appendDatagram(entry)
		members[i].Queued = true
	}
	if f != nil {
		s.routeEnqueuedDatagrams(f, first)
	}
	if r.PayloadCopied != nil {
		*r.PayloadCopied += copied
	}
	return DatagramQueued, nil
}

// SpliceReadBudget describes the destination reservation before a read.
// Existing roots need no additional capacity in the assembly.
type SpliceReadBudget struct {
	Remaining int
	Existing  map[*Buf]struct{}
	// When the input needs a queue slot, an existing queue root is free there.
	ForQueue       bool
	QueueRemaining int
	Queued         map[*Buf]int
}

func (r SpliceReadBudget) fits(b *Buf) bool {
	_, shared := r.Existing[b]
	return (shared || cap(b.B) <= r.Remaining) && (!r.ForQueue || r.Queued[b] > 0 || cap(b.B) <= r.QueueRemaining)
}

func (r SpliceReadBudget) fitsNew(capacity int) bool {
	return capacity <= r.Remaining && (!r.ForQueue || capacity <= r.QueueRemaining)
}

// SpliceReadChunk transfers the receive loan at the same commit that advances
// credit. A refusal leaves the entire range available to an ordinary read.
func (s *Session) SpliceReadChunk(st *Stream, limit int, budget SpliceReadBudget) (Span, *SpliceLease, bool, error) {
	read := spliceRead{budget: budget}
	_, v, fin, err := s.consume(st, nil, max(0, limit), true, &read)
	return v, read.lease, fin, err
}

type spliceRead struct {
	budget SpliceReadBudget
	lease  *SpliceLease
}

// SpliceLease holds the external pin and descriptor transferred with an
// input range. Its children share both until the final range is released.
// The session's physical allowance covers ordinary and transferred pins.
type SpliceLease struct {
	account *recvAccount
	root    *Buf
	refs    atomic.Int64
}

// Reuse descriptors after the last child releases its view. Cleared
// descriptors retain neither a receive account nor a backing allocation.
var spliceLeasePool = sync.Pool{New: func() any { return new(SpliceLease) }}

func (a *recvAccount) spliceLease(b *Buf) *SpliceLease {
	if a == nil || !b.borrow() {
		return nil
	}
	a.spliceMu.Lock()
	defer a.spliceMu.Unlock()
	if a.splicePins == nil {
		a.splicePins = make(map[*Buf]int)
	}
	a.splicePins[b]++
	lease := spliceLeasePool.Get().(*SpliceLease)
	lease.account, lease.root = a, b
	lease.refs.Store(1)
	return lease
}

func (l *SpliceLease) Retain() {
	if l != nil {
		l.refs.Add(1)
	}
}
func (l *SpliceLease) Release() {
	if l == nil || l.refs.Add(-1) != 0 {
		return
	}
	a, b := l.account, l.root
	a.spliceMu.Lock()
	a.splicePins[b]--
	if a.splicePins[b] == 0 {
		delete(a.splicePins, b)
	}
	a.spliceMu.Unlock()
	b.ReturnView()
	l.account, l.root = nil, nil
	spliceLeasePool.Put(l)
}

func (rb *recvBuf) takeSplice(off uint64, limit int, read *spliceRead) (int, Span, error) {
	if limit == 0 || rb.ps.n == 0 {
		return 0, Span{}, nil
	}
	p := *rb.ps.first()
	if p.off != off {
		return 0, Span{}, nil
	}
	b := p.root.buf
	lease := rb.memory.accountFor(b).spliceLease(b)
	if lease == nil {
		return 0, Span{}, ErrReceiveLoanLimit
	}
	if !read.budget.fits(b) {
		lease.Release()
		return 0, Span{}, ErrReceiveLoanLimit
	}
	read.lease = lease
	n := min(limit, p.n)
	v := b.Span(p.start, p.start+n)
	b.Ref()
	rb.consumeFirst(p, n)
	return n, v, nil
}

func (s *Session) spliceDatagramHead(st *Stream) *streamDgrams {
	q := st.dg
	if q == nil {
		return nil
	}
	if q.expiry.index != 0 && q.expiry.at <= s.now {
		s.expireFlowItems(st, s.now)
	}
	for len(q.q) > 0 && q.cd.drop(q.at[0], s.now, s.cfg.DgramTarget, len(q.q) == 1) {
		if e := s.popStreamDatagram(q); e.buf != nil {
			e.buf.Unref()
		}
		s.dropRecv(1)
		st.cnt.dgDropped++
	}
	if len(q.q) == 0 {
		q.cd.idle()
		return nil
	}
	return q
}

// SpliceDatagramInput is the result of a single committed source read.
type SpliceDatagramInput struct {
	Data     []byte
	Backing  *Buf
	Lease    *SpliceLease
	Context  uint64
	Expires  Time
	Copied   int
	TooLarge bool
}

// TakeSpliceDatagram checks the external borrow and destination room before
// retiring the receive charge. Oversized input can be consumed as a drop
// without borrowing its backing or allocating a copy.
func (s *Session) TakeSpliceDatagram(st *Stream, maxPayload int, budget SpliceReadBudget) (SpliceDatagramInput, bool) {
	q := s.spliceDatagramHead(st)
	if q == nil {
		return SpliceDatagramInput{}, false
	}
	e := q.q[0]
	in := SpliceDatagramInput{Data: e.data, Backing: e.buf, Expires: e.expires, Context: e.context}
	if len(e.data) > maxPayload {
		in.TooLarge, in.Backing, in.Data = true, nil, nil
	} else if b := e.buf; b != nil {
		a := s.conn.rx.accountFor(b)
		lease := a.spliceLease(b)
		if lease == nil {
			s.dg.spliceBorrowRefused++
		} else if used := a.loaned.Load(); used > 0 {
			s.dg.spliceBorrowPeak = max(s.dg.spliceBorrowPeak, uint64(used))
		}
		switch {
		case lease != nil && budget.fits(b):
			in.Lease = lease
			root := s.dg.recvBacking[b]
			if root.items != 0 && !root.transferred {
				root.transferred = true
				s.dg.recvBacking[b] = root
				s.dg.spliceTransfers += uint64(cap(b.B))
			}
		default:
			lease.Release()
			if !budget.fitsNew(len(e.data)) {
				return SpliceDatagramInput{}, false
			}
			in.Backing = copyBacking(s.datagramMemory, e.data)
			if in.Backing == nil {
				return SpliceDatagramInput{}, false
			}
			in.Data, in.Copied = in.Backing.B, len(e.data)
		}
	} else if len(e.data) != 0 {
		if !budget.fitsNew(cap(e.data)) {
			return SpliceDatagramInput{}, false
		}
		in.Backing = NewBuf(e.data)
	}
	s.consumeStreamDatagram(st, q)
	if e.buf != nil && in.Backing != e.buf {
		e.buf.Unref()
	}
	return in, true
}

func (q *streamDgrams) armExpiry() {
	next := Never
	for _, e := range q.q {
		if e.expires != 0 {
			next = min(next, e.expires)
		}
	}
	q.deadlines.Set(&q.expiry, next)
}

func (s *Session) expireFlowItems(st *Stream, now Time) {
	q := st.dg
	for i := 0; i < len(q.q); {
		e := q.q[i]
		if e.expires == 0 || e.expires > now {
			i++
			continue
		}
		q.q = slices.Delete(q.q, i, i+1)
		q.at = slices.Delete(q.at, i, i+1)
		q.size -= uint64(len(e.data))
		if !q.detached {
			s.unaccountReceivedDatagram(e)
		}
		if e.buf != nil {
			e.buf.Unref()
		}
		q.expired++
		st.cnt.dgDropped++
		s.dropRecv(1)
	}
	if q.deadlines != nil {
		q.armExpiry()
	}
}

// SpliceReceiveState describes the source ledger at a read boundary.
type SpliceReceiveState struct {
	Held, Transferred, PeakBorrowed, BorrowRefused, Expired uint64
	Queued                                                  int
	OwnsRoot, SpliceOwnsRoot                                bool
}

func (s *Session) SpliceReceiveState(st *Stream, b *Buf) (v SpliceReceiveState) {
	if d := s.dg; d != nil {
		v.Held, v.Transferred, v.PeakBorrowed, v.BorrowRefused = d.recvCapacity, d.spliceTransfers, d.spliceBorrowPeak, d.spliceBorrowRefused
		for root := range d.recvBacking {
			if root.hasSpliceLease() {
				v.Held -= uint64(cap(root.B))
			}
		}
		v.SpliceOwnsRoot = b.hasSpliceLease()
		v.OwnsRoot = !v.SpliceOwnsRoot && d.recvBacking[b].items != 0
	}
	if q := st.dg; q != nil {
		v.Queued, v.Expired = len(q.q), q.expired
	}
	return v
}

func (b *Buf) hasSpliceLease() bool {
	if b == nil {
		return false
	}
	a := b.account.Load()
	if a == nil {
		return false
	}
	a.spliceMu.Lock()
	defer a.spliceMu.Unlock()
	return a.splicePins[b] != 0
}
