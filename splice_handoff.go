package zmux

import "sync"

// SpliceHandoffStats counts charged bytes after transfer to the target session.
type SpliceHandoffStats struct {
	Accepted, FirstSent, Unsent, Pending uint64
	PreAdmissionRefunded, UnsentRefunded uint64
	NativeDatagrams, RecordDatagrams     uint64
}

// SpliceHandoffs tracks the asynchronous disposition of admitted bytes.
// FirstSent counts logical bytes once, including successful retries.
type SpliceHandoffs struct {
	mu    sync.Mutex
	stats SpliceHandoffStats
}

// Stats returns a consistent snapshot. Pending bytes can settle after
// the splice returns, as the target session drains or drops its queues.
func (h *SpliceHandoffs) Stats() SpliceHandoffStats {
	if h == nil {
		return SpliceHandoffStats{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stats
}

func (h *SpliceHandoffs) accept(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stats.Accepted += uint64(n)
	h.stats.Pending += uint64(n)
}

func (h *SpliceHandoffs) complete(sent, unsent int, limited bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if limited {
		h.stats.UnsentRefunded += uint64(unsent)
	}
	h.stats.FirstSent += uint64(sent)
	h.stats.Unsent += uint64(unsent)
	h.stats.Pending -= uint64(sent + unsent)
}

func (h *SpliceHandoffs) refunded(n int64) {
	if n == 0 {
		return
	}
	h.mu.Lock()
	h.stats.PreAdmissionRefunded += uint64(n)
	h.mu.Unlock()
}

func (h *SpliceHandoffs) datagram(native bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if native {
		h.stats.NativeDatagrams++
	} else {
		h.stats.RecordDatagrams++
	}
}

// splicePrefixWatch tracks actual first handoffs for a peer that can
// receive only an ordinary reset. Writes never cross a capsule boundary.
type splicePrefixWatch struct {
	pending map[*splicePrefixPart]struct{}
	wake    func()
	mu      sync.Mutex
}

type splicePrefixPart struct {
	offset    uint64
	remaining int
}

func (w *splicePrefixWatch) accept(p *splicePrefixPart, n int) {
	if n == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	p.remaining = n
	if w.pending == nil {
		w.pending = make(map[*splicePrefixPart]struct{})
	}
	w.pending[p] = struct{}{}
}

func (w *splicePrefixWatch) complete(p *splicePrefixPart, n int) {
	w.mu.Lock()
	p.remaining -= n
	done := p.remaining == 0
	if done {
		delete(w.pending, p)
	}
	w.mu.Unlock()
	if done {
		w.wake()
	}
}

func (w *splicePrefixWatch) finishedBefore(off uint64) bool {
	if w == nil {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for p := range w.pending {
		if p.offset < off {
			return false
		}
	}
	return true
}

// A direction's limiter accounts for every refund before transfer, including
// invalid positive grants rejected by guardedLimiter.
type spliceReservationLimiter struct {
	Limiter
	tally *SpliceHandoffs
}

func (l spliceReservationLimiter) Refund(n int64) {
	if n > 0 {
		l.Limiter.Refund(n)
		l.tally.refunded(n)
	}
}
