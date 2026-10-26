package core

import "github.com/zmuxio/zmux-go/internal/wire"

// Opportunistic reinjection and penalization (MULTIPATH.md 7.5.1): when the
// peer's flow control blocks new data and the blocking data was sent on a
// slower pipe long enough ago, it is sent again once on the eligible pipe
// with the earliest arrival, and the slow pipe is penalized.

// noteBlocked remembers a stream direction blocked by its stream credit.
func (s *Session) noteBlocked(st *Stream) {
	if s.mp == nil || st.snd.oppListed {
		return
	}
	st.snd.oppListed = true
	s.mp.blocked = append(s.mp.blocked, st)
}

// streamBlocked reports a direction that has data its stream credit holds.
func streamBlocked(st *Stream) bool {
	sp := &st.snd
	return !st.discarded && st.hasS && !sp.reset && st.idSet &&
		sp.sendEnd() > max64(sp.sent, sp.q.base) && sp.sent >= sp.maxData
}

// opportunistic runs at a write opportunity of the decision's pipe when it
// is the eligible pipe with the earliest arrival.
func (s *Session) opportunistic(d *decision, w *recWriter) {
	m := s.mp
	if d.g == nil || !d.takes || d.fastest != d.p || w.dataRoom == 0 {
		return
	}
	f := d.p
	connBlocked := s.conn.sent >= s.conn.sendMax && s.sched.mask != 0
	keep := m.blocked[:0]
	for _, st := range m.blocked {
		if !streamBlocked(st) {
			st.snd.oppListed = false
			continue
		}
		keep = append(keep, st)
	}
	clear(m.blocked[len(keep):])
	m.blocked = keep
	if len(m.blocked) == 0 && !connBlocked {
		return
	}
	for _, st := range m.blocked {
		if !s.oppSend(w, f, st) {
			return
		}
	}
	if connBlocked {
		s.oppSend(w, f, nil)
	}
}

// oppSend re-sends one chunk of blocking data on f; st nil means the
// session's blocking data (the unacknowledged stream data sent first). It
// returns false when the batch is full.
func (s *Session) oppSend(w *recWriter, f *Pipe, st *Stream) bool {
	p, e := s.blockingFrame(f, st)
	if e == nil {
		return true
	}
	a := p.mp
	fa := f.mp
	pto := s.pto(a)
	thr := maxD(2*fa.rtt.smoothed+s.ack.peerDelay, minD(pto, 2*e.due))
	if s.now.Sub(e.sent) <= thr {
		return true
	}
	st = s.streams[e.d.id]
	sp := &st.snd
	ms := sp.mpState()
	// The next part of the blocking data not acknowledged and not re-sent.
	lo := max64(e.d.off, ms.acked.prefix())
	hi := e.d.off + e.d.n
	for _, r := range ms.oppSent {
		if r.lo <= lo && lo < r.hi {
			lo = r.hi
		}
	}
	if lo < sp.preLen {
		lo = 0
	}
	hi = min64(hi, min64(sp.q.end, lo+uint64(s.cfg.SpreadChunk)))
	if lo >= hi {
		return true
	}
	room := w.room() - wire.StreamHeaderLen(st.id, lo, int(hi-lo), true)
	if room <= 0 {
		return false
	}
	n := min64(hi-lo, uint64(room))
	data, blk := sp.q.peek(lo, int(n))
	if len(data) == 0 {
		return true
	}
	n = uint64(len(data))
	if !w.stream(st.id, lo, data, blk, false) {
		return false
	}
	w.logFrame(frameDesc{kind: dStream, id: st.id, off: lo, n: n})
	w.spendData(n)
	ms.oppSent.add(lo, lo+n)
	s.stats.BytesOpportunistic += n
	f.mp.oppBytes += n
	s.penalize(p)
	return true
}

// blockingFrame finds the retained frame that carries the blocking data:
// st's lowest unacknowledged byte, or (st nil) the oldest unacknowledged
// stream frame; only frames on pipes other than f count.
func (s *Session) blockingFrame(f *Pipe, st *Stream) (*Pipe, *retained) {
	var lo uint64
	var id uint64
	if st != nil {
		id = st.id
		lo = 0
		if ms := st.snd.mps; ms != nil {
			lo = ms.acked.prefix()
		}
	}
	var bp *Pipe
	var best *retained
	for _, q := range s.pipes {
		if q.out == nil || q.mp == nil {
			continue
		}
		for _, l := range q.out.lanes {
			for i := range l.log {
				e := &l.log[i]
				if e.d.kind != dStream || e.d.n == 0 || e.reinjected {
					continue
				}
				if st != nil && (e.d.id != id || lo < e.d.off || lo >= e.d.off+e.d.n) {
					continue
				}
				if best == nil || e.sent < best.sent {
					bp, best = q, e
				}
				break // a lane's log is in sending order: its first match is its oldest
			}
		}
	}
	if best == nil || bp == f {
		return nil, nil
	}
	return bp, best
}

// penalize halves R_p, discards its samples and keeps new data off p until
// everything sent on it is acknowledged (once per episode).
func (s *Session) penalize(p *Pipe) {
	a := p.mp
	if a.penalized {
		return
	}
	a.penalized = true
	a.penaltyEnd = p.out.snd.so
	e := &p.out.snd.re
	r := e.value()
	clear(e.samples)
	e.samples = e.samples[:0]
	e.r, e.measured = r/2, false
	s.checkTakeover()
}

// penaltyAcked ends an episode once everything sent on p is acknowledged.
func (s *Session) penaltyAcked(p *Pipe) {
	if a := p.mp; a.penalized && p.out.snd.cum >= a.penaltyEnd {
		a.penalized = false
		s.checkTakeover()
		s.wakeAllWriters()
	}
}
