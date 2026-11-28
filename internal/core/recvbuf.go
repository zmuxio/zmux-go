package core

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Buf owns a receive allocation. The producer writes only unpublished
// ranges. Consumers retain immutable views; Free returns the allocation
// to its allocator and must perform only internal bookkeeping.
type Buf struct {
	Free     func(*Buf)
	account  atomic.Pointer[recvAccount]
	pool     *recvBin
	poolNext *Buf
	memory   atomic.Pointer[budgetAllowance]
	B        []byte
	// The low word counts driver and receive-queue references. The high
	// word counts loans, including their references in forwarded writes.
	// Keep this first for 64-bit atomic alignment on 32-bit hosts.
	refs    uint64
	message atomic.Uint64
}

func NewBuf(data []byte) *Buf {
	return &Buf{B: data, refs: 1}
}

// Reuse takes the initial reference after an exclusive checkout from the
// allocator. The previous lifetime ended before the block entered the pool.
func (b *Buf) Reuse() {
	b.refs = 1
}

// Span is an immutable published range in an owned backing.
type Span struct {
	Buf        *Buf
	Start, End int
}

func (v Span) Len() int { return v.End - v.Start }

func (v Span) Bytes() []byte {
	if v.Buf == nil {
		return nil
	}
	return v.Buf.B[v.Start:v.End:v.End]
}

func (b *Buf) Span(start, end int) Span { return Span{b, start, end} }

// BeginMessage reads the generation for the unique producer's message.
// EndMessage advances it before the corresponding reference is released.
func (b *Buf) BeginMessage() uint64 {
	return b.message.Load()
}

// EndMessage consumes the producer's ownership of the given generation.
func (b *Buf) EndMessage(token uint64) bool {
	return b.message.CompareAndSwap(token, token+1)
}

// Ref adds an independent owner, such as the receive queue.
func (b *Buf) Ref() { atomic.AddUint64(&b.refs, 1) }

// Unref releases an independent owner.
func (b *Buf) Unref() {
	if atomic.LoadUint64(&b.refs) == 1 {
		// The remaining owner has exclusive access until recycle publishes
		// the block to the allocator. No other holder can add a reference.
		b.refs = 0
		b.recycle()
		return
	}
	switch n := atomic.AddUint64(&b.refs, ^uint64(0)); {
	case n&(loanRef-1) == loanRef-1:
		panic("zmux: receive buffer released twice")
	case n == 0:
		b.recycle()
	}
}

// Refs returns the total number of independent owners.
func (b *Buf) Refs() int32 {
	n := atomic.LoadUint64(&b.refs)
	return int32(uint32(n) + uint32(n>>32))
}

// charge assigns the physical storage to one ledger, even if the view
// subsequently crosses a session or changes from a receive to a send.
func (b *Buf) charge(memory *budgetAllowance) bool {
	if memory == nil || b.memory.Load() != nil {
		return true
	}
	n := int64(cap(b.B))
	if !memory.adjust(n) {
		return b.memory.Load() != nil
	}
	if !b.memory.CompareAndSwap(nil, memory) {
		memory.adjust(-n)
	}
	return true
}

func (b *Buf) recycle() {
	n := int64(cap(b.B))
	if a := b.account.Swap(nil); a != nil {
		a.bytes.Add(-n)
	}
	if a := b.memory.Swap(nil); a != nil {
		a.adjust(-n)
	}
	if b.Free != nil {
		b.Free(b)
	}
}

const loanRef = uint64(1) << 32

func (b *Buf) borrow() bool {
	a := b.account.Load()
	if !a.reserveView() {
		return false
	}
	n := int64(cap(b.B))
	for refs := atomic.LoadUint64(&b.refs); ; refs = atomic.LoadUint64(&b.refs) {
		if refs >= loanRef {
			if atomic.CompareAndSwapUint64(&b.refs, refs, refs+loanRef) {
				return true
			}
			continue
		}
		if !a.reserveLoan(n) {
			a.releaseView()
			return false
		}
		if atomic.CompareAndSwapUint64(&b.refs, refs, refs+loanRef) {
			return true
		}
		a.releaseLoan(n)
	}
}

// tryRefLoan retains an existing loan while keeping its charge live.
func (b *Buf) tryRefLoan() bool {
	for n := atomic.LoadUint64(&b.refs); n >= loanRef; n = atomic.LoadUint64(&b.refs) {
		if atomic.CompareAndSwapUint64(&b.refs, n, n+loanRef) {
			return true
		}
	}
	return false
}

func (b *Buf) refLoan() { atomic.AddUint64(&b.refs, loanRef) }

// ReleaseLoan releases a borrowed view, including one taken by a write.
func (b *Buf) ReleaseLoan() {
	a, n := b.account.Load(), int64(cap(b.B))
	switch refs := atomic.AddUint64(&b.refs, ^(loanRef - 1)); {
	case refs>>32 == uint64(^uint32(0)):
		panic("zmux: receive loan released twice")
	case refs < loanRef:
		a.releaseLoan(n)
		if refs == 0 {
			b.recycle()
		}
	}
}

// ReturnView releases a lease issued by ReadChunk. Send queues and other
// holders that retain that lease account for their own descriptors.
func (b *Buf) ReturnView() {
	b.account.Load().releaseView()
	b.ReleaseLoan()
}

// recvAccount can outlive the session while an application or a forwarded
// write still holds its input. Account for each root capacity until recycle.
type recvAccount struct {
	spliceMu   sync.Mutex
	splicePins map[*Buf]int
	memory     *budgetAllowance
	bytes      atomic.Int64
	loaned     atomic.Int64
	leases     atomic.Int64
	loanCap    int64
	loanMu     sync.Mutex
}

// Descriptor count has its own quota even when the views share one root.
func (a *recvAccount) leaseLimit() int64 { return max(4, a.loanCap/compactSlot) }

func (a *recvAccount) reserveView() bool {
	limit := a.leaseLimit()
	for n := a.leases.Load(); n < limit; n = a.leases.Load() {
		if a.leases.CompareAndSwap(n, n+1) {
			if !a.memory.adjust(pieceCost) {
				a.leases.Add(-1)
				return false
			}
			return true
		}
	}
	return false
}

func (a *recvAccount) releaseView() {
	a.leases.Add(-1)
	a.memory.adjust(-pieceCost)
}

func (a *recvAccount) reserveLoan(n int64) bool { return a.changeLoan(n) }

func (a *recvAccount) releaseLoan(n int64) { a.changeLoan(-n) }

const loanCountLocked = -1

func (a *recvAccount) changeLoan(d int64) bool {
	for n := a.loaned.Load(); n >= 0; n = a.loaned.Load() {
		if d > a.loanCap-n {
			return false
		}
		next := n + d
		if a.memory != nil && max(next, n) > receiveLoanFloor {
			break
		}
		if a.loaned.CompareAndSwap(n, next) {
			return true
		}
	}
	a.loanMu.Lock()
	defer a.loanMu.Unlock()
	n := a.loaned.Swap(loanCountLocked)
	if d > a.loanCap-n {
		a.loaned.Store(n)
		return false
	}
	next := n + d
	change := max(next-receiveLoanFloor, 0) - max(n-receiveLoanFloor, 0)
	if change != 0 && !a.memory.changeLoanFloor(change) {
		a.loaned.Store(n)
		return false
	}
	a.loaned.Store(next)
	return true
}

// recvRoot belongs to a single session, even when the backing is flowing
// through another session. Its references can be found without taking
// any other session's lock.
type recvRoot struct {
	buf        *Buf
	first      *recvPiece
	prev, next *recvRoot
	live       uint64
	borrowed   bool
	loan       bool
}

type recvMemory struct {
	account     *recvAccount
	roots       map[*Buf]*recvRoot
	first       *recvRoot
	last        *recvRoot // the last backing seen by insert
	compactNext *recvRoot
	free        *recvPiece
	rootFree    *recvRoot
	live        uint64
	queued      uint64 // capacities of local and forwarded backings in the queue
	foreign     uint64 // forwarded capacity reserved in the originating ledger
	compacted   uint64 // bytes moved to compact pages
	nodes       uint64
	nfree       int
	nrootFree   int
	refused     bool
}

func (m *recvMemory) resident() uint64 {
	if m.account == nil {
		return 0
	}
	return uint64(m.account.bytes.Load()+m.account.leases.Load()*pieceCost) + m.foreign + m.metadata()
}

func (m *recvMemory) metadata() uint64 {
	return (m.nodes+uint64(m.nfree))*pieceCost + uint64(len(m.roots)+m.nrootFree)*rootCost
}

// accountFor charges each physical allocation in exactly one ledger.
func (m *recvMemory) accountFor(b *Buf) *recvAccount {
	if a := b.account.Load(); a != nil {
		return a
	}
	if m.account == nil {
		m.account = &recvAccount{loanCap: defaultLoanCap}
	}
	if !b.charge(m.account.memory) {
		return nil
	}
	if b.account.CompareAndSwap(nil, m.account) {
		m.account.bytes.Add(int64(cap(b.B)))
	}
	return b.account.Load()
}

func (m *recvMemory) root(b *Buf) *recvRoot {
	if m.last != nil && m.last.buf == b {
		return m.last
	}
	r := m.roots[b]
	if r == nil {
		a := m.accountFor(b)
		if a == nil {
			return nil
		}
		r = m.rootFree
		if r == nil {
			if !m.account.memory.adjust(rootCost) {
				return nil
			}
			r = new(recvRoot)
		} else {
			m.rootFree = r.next
			m.nrootFree--
		}
		*r = recvRoot{buf: b, next: m.first, borrowed: a != m.account}
		if r.borrowed {
			m.foreign += uint64(cap(b.B))
			r.loan = b.tryRefLoan()
		}
		if !r.loan {
			b.Ref()
		}
		m.queued += uint64(cap(b.B))
		if m.first != nil {
			m.first.prev = r
		}
		m.first = r
		if m.roots == nil {
			m.roots = make(map[*Buf]*recvRoot)
		}
		m.roots[b] = r
	}
	m.last = r
	return r
}

const (
	// Two default send buffers leave room for backing slack during a relay
	// while bounding retention by an application that holds its views.
	defaultLoanCap = 8 << 20
	pieceCost      = 64
	rootCost       = 64
	compactSlot    = 256
)

func (m *recvMemory) link(p *recvPiece, r *recvRoot, n uint64) {
	p.root = r
	if r.first != nil {
		r.first.prev = p
	}
	p.next, r.first = r.first, p
	r.live += n
	m.live += n
}

func (m *recvMemory) resize(p *recvPiece, n uint64) {
	p.root.live -= n
	m.live -= n
}

func (m *recvMemory) unlink(p *recvPiece) {
	r := p.root
	m.resize(p, uint64(p.n))
	if p.prev == nil {
		r.first = p.next
	} else {
		p.prev.next = p.next
	}
	if p.next != nil {
		p.next.prev = p.prev
	}
	p.prev, p.next = nil, nil
	if r.first != nil {
		return
	}
	if r.prev == nil {
		m.first = r.next
	} else {
		r.prev.next = r.next
	}
	if r.next != nil {
		r.next.prev = r.prev
	}
	if m.last == r {
		m.last = nil
	}
	if m.compactNext == r {
		m.compactNext = r.next
	}
	b := r.buf
	m.queued -= uint64(cap(b.B))
	delete(m.roots, b)
	if r.borrowed {
		m.foreign -= uint64(cap(b.B))
	}
	if r.loan {
		b.ReleaseLoan()
	} else {
		b.Unref()
	}
	*r = recvRoot{}
	if m.nrootFree < seqChunk {
		r.next = m.rootFree
		m.rootFree = r
		m.nrootFree++
	} else {
		m.account.memory.adjust(-rootCost)
	}
}

func (m *recvMemory) alloc() *recvPiece {
	p := m.free
	if p == nil {
		if !m.account.memory.adjust(pieceCost) {
			return nil
		}
		m.nodes++
		return new(recvPiece)
	}
	m.free = p.next
	m.nfree--
	m.nodes++
	*p = recvPiece{}
	return p
}

func (m *recvMemory) put(p *recvPiece) {
	m.nodes--
	*p = recvPiece{}
	if m.nfree < seqChunk {
		p.next = m.free
		m.free = p
		m.nfree++
	} else {
		m.account.memory.adjust(-pieceCost)
	}
}

// compact packs sparse, exclusively held roots when physical backing is
// more than twice the logical data. One pass copies at most one allocator
// block and reuses the interval nodes. At least one slot must be reclaimed.
func (m *recvMemory) compact(current *Buf) {
	if m.first == nil || m.queued <= 2*m.live || m.queued-m.live < compactSlot ||
		m.first.next == nil && m.first.buf == current {
		return
	}
	first := m.compactNext
	if first == nil {
		first = m.first
	}
	// In one pass examine at most as many roots as a receive block can
	// contain allocator slots. Resume at the next root if there are more.
	const scanLimit = RecvBlockSize / compactSlot
	var copied, reclaimed, scanned int
	r := first
	for ; r != nil && scanned < scanLimit && copied < RecvBlockSize; r = r.next {
		scanned++
		if r.compactable(current) && r.live <= uint64(RecvBlockSize-copied) {
			copied += int(r.live)
			reclaimed += cap(r.buf.B)
		}
	}
	m.compactNext = r
	if copied == 0 || reclaimed-recvClass(copied).size < compactSlot {
		return
	}
	if !m.account.memory.adjust(int64(recvClass(copied).size)) {
		return
	}
	b := GetRecvBuf(copied)
	b.memory.Store(m.account.memory)
	defer b.Unref()
	dest := m.root(b)
	if dest == nil {
		return
	}
	pos := 0
	for r := first; scanned > 0 && pos < copied; scanned-- {
		next := r.next
		if r.compactable(current) && r.live <= uint64(copied-pos) {
			for piece := r.first; piece != nil; {
				nextPiece := piece.next
				n := copy(b.B[pos:], piece.data())
				m.unlink(piece)
				piece.start = pos
				m.link(piece, dest, uint64(n))
				pos += n
				piece = nextPiece
			}
		}
		r = next
	}
	m.compacted += uint64(pos)
}

func (r *recvRoot) compactable(current *Buf) bool {
	return r.buf != current && !r.borrowed && r.live > 0 &&
		r.live < uint64(cap(r.buf.B))/2 && atomic.LoadUint64(&r.buf.refs) == 1
}

type recvPiece struct {
	root       *recvRoot
	prev, next *recvPiece // reverse membership on buf
	off        uint64
	start, n   int
}

func (p *recvPiece) end() uint64 { return p.off + uint64(p.n) }

func (p *recvPiece) data() []byte {
	end := p.start + p.n
	return p.root.buf.B[p.start:end:end]
}

// recvBuf is the ordered set of unread intervals. The sequence moves
// pointers, so the backing's reverse links stay valid during reassembly.
type recvBuf struct {
	memory *recvMemory
	seed   []*recvPiece
	ps     seq[*recvPiece]
	bytes  uint64
}

func (rb *recvBuf) after(off uint64) seqPos {
	q := &rb.ps
	if q.n == 0 || (*q.first()).end() > off {
		return q.begin()
	}
	return q.search(func(p **recvPiece) bool { return (*p).end() > off })
}

// insert saves the previously unseen parts of a range. start refers to
// the validated bytes in buf; the producer never overwrites those bytes.
func (rb *recvBuf) insert(off uint64, buf *Buf, start, n int) uint64 {
	end := off + uint64(n)
	q := &rb.ps
	if q.n == 0 || off >= (*q.last()).end() {
		if q.n != 0 {
			last := *q.last()
			if last.root.buf == buf && last.start+last.n == start && last.end() == off {
				last.n += n
				last.root.live += uint64(n)
				rb.memory.live += uint64(n)
				rb.bytes += uint64(n)
				return uint64(n)
			}
		}
		if rb.put(q.end(), off, buf, start, n) {
			return uint64(n)
		}
		return 0
	}
	var added uint64
	for cur := off; cur < end; {
		i := rb.after(cur)
		gapEnd := end
		if q.valid(i) {
			p := *q.at(i)
			if p.off <= cur {
				cur = p.end()
				continue
			}
			gapEnd = min(end, p.off)
		}
		if !rb.put(i, cur, buf, start+int(cur-off), int(gapEnd-cur)) {
			break
		}
		added += gapEnd - cur
		cur = gapEnd
	}
	return added
}

func (rb *recvBuf) put(i seqPos, off uint64, buf *Buf, start, n int) bool {
	m := rb.memory
	if m.account == nil {
		m.account = &recvAccount{loanCap: defaultLoanCap}
	}
	p := m.alloc()
	if p == nil {
		m.refused = true
		return false
	}
	r := m.root(buf)
	if r == nil && n < cap(buf.B) {
		// The input backing belongs to the transient reader reservation.
		// Retain only what this new interval actually needs.
		if packed := copyBacking(m.account.memory, buf.B[start:start+n]); packed != nil {
			r = m.root(packed)
			packed.Unref()
			start = 0
		}
	}
	if r == nil {
		m.put(p)
		m.refused = true
		return false
	}
	*p = recvPiece{off: off, start: start, n: n}
	m.link(p, r, uint64(n))
	rb.bytes += uint64(n)
	if rb.ps.n == 0 {
		if rb.seed == nil {
			rb.seed = make([]*recvPiece, 1, 8)
		}
		rb.seed[0] = p
		rb.ps.cs = append(rb.ps.cs, rb.seed)
		rb.ps.n = 1
	} else {
		rb.ps.insert(i, p)
	}
	return true
}

func (rb *recvBuf) contiguous(off uint64) uint64 {
	q := &rb.ps
	for i := rb.after(off); q.valid(i); i = q.next(i) {
		p := *q.at(i)
		if p.off > off {
			break
		}
		off = p.end()
	}
	return off
}

var ErrReceiveLoanLimit = errors.New("zmux: receive chunk loan budget exhausted")

// consume takes the available leading data. With a destination it copies
// into that destination, otherwise it lends a single immutable view.
func (rb *recvBuf) consume(off uint64, dst []byte, limit int, loan bool) (int, Span, error) {
	n := 0
	for rb.ps.n > 0 && n < limit {
		p := *rb.ps.first()
		if p.off != off+uint64(n) {
			break
		}
		take := min(p.n, limit-n)
		if loan {
			if !p.root.buf.borrow() {
				return 0, Span{}, ErrReceiveLoanLimit
			}
			data := p.root.buf.Span(p.start, p.start+take)
			rb.consumeFirst(p, take)
			return take, data, nil
		}
		copy(dst[n:], p.data()[:take])
		rb.consumeFirst(p, take)
		n += take
	}
	return n, Span{}, nil
}

func (rb *recvBuf) consumeFirst(p *recvPiece, n int) {
	rb.bytes -= uint64(n)
	if n == p.n {
		rb.ps.popFirst()
		rb.drop(p)
		return
	}
	rb.memory.resize(p, uint64(n))
	p.start += n
	p.n -= n
	p.off += uint64(n)
}

func (rb *recvBuf) drop(p *recvPiece) {
	rb.memory.unlink(p)
	rb.memory.put(p)
}

func (rb *recvBuf) discard(off uint64) uint64 {
	before := rb.bytes
	for rb.ps.n > 0 {
		p := *rb.ps.first()
		if p.off >= off {
			break
		}
		rb.consumeFirst(p, int(min(off-p.off, uint64(p.n))))
	}
	return before - rb.bytes
}

func (rb *recvBuf) discardFrom(off uint64) uint64 {
	before := rb.bytes
	for rb.ps.n > 0 {
		p := *rb.ps.last()
		if p.end() <= off {
			break
		}
		if p.off >= off {
			rb.bytes -= uint64(p.n)
			rb.ps.popLast()
			rb.drop(p)
			continue
		}
		n := p.end() - off
		rb.memory.resize(p, n)
		rb.bytes -= n
		p.n -= int(n)
	}
	return before - rb.bytes
}

func (rb *recvBuf) clear() uint64 {
	d := rb.bytes
	for _, c := range rb.ps.cs {
		for _, p := range c {
			rb.drop(p)
		}
	}
	clear(rb.seed[:cap(rb.seed)])
	rb.ps = seq[*recvPiece]{}
	rb.seed, rb.bytes = nil, 0
	return d
}
