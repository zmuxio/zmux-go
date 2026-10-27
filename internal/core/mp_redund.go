package core

import "github.com/zmuxio/zmux-go/internal/wire"

// Redundant urgent data: a chunk of a redundant stream
// direction is taken by either pipe of the pair of its eligible tier and
// copied to the other; the first spread_chunk of new data after an idle
// period of a pipe is copied likewise. Copies wait outside the carrier for
// their pipe's next write opportunity, go ahead of its other stream data,
// and are bounded by the copy bytes outstanding.

const idleCopyMin = Second

// copyRec is one copy, queued or sent, counted against the bound until its
// data is acknowledged or no longer owed.
type copyRec struct {
	st       *Stream
	off, end uint64
	fin      bool
	queued   bool // still waiting for its pipe
	dead     bool
}

// redundant reports whether a stream direction's chunks are sent twice:
// its own setting, else the session's redundancy for urgency 0 to 2.
// Never under session-affinity, whose tiers are single pipes.
func (s *Session) redundant(st *Stream) bool {
	if !s.feat.Multipath || s.policyOf(st) == PolicySessionAffinity {
		return false
	}
	if st.redund != 0 {
		return st.redund > 0
	}
	return s.cfg.Redundancy && st.urgency <= 2
}

// idleCopied reports whether the first chunk after an idle period is
// copied for a stream direction.
func (s *Session) idleCopied(st *Stream) bool {
	if s.policyOf(st) == PolicySessionAffinity {
		return false
	}
	if st.redund != 0 {
		return st.redund > 0
	}
	return s.cfg.Redundancy
}

// pipeIdle: nothing ack-eliciting unacknowledged and no acknowledgement
// progress for max(1 s, T_suspect).
func (s *Session) pipeIdle(p *Pipe) bool {
	a := p.mp
	quiet := s.now.Sub(a.lastAdvance)
	return !a.unacked && quiet >= idleCopyMin && quiet >= s.tSuspect(a)
}

// copyBound is max(spread_chunk, retransmit budget / 64).
func (s *Session) copyBound() uint64 {
	return max64(uint64(s.cfg.SpreadChunk), s.retransmitBudget()/64)
}

// copyLeft is the part of a copy still owed and unacknowledged.
func (s *Session) copyLeft(c *copyRec) uint64 {
	st := c.st
	if c.dead || st.discarded || !st.hasS {
		return 0
	}
	sp := &st.snd
	end := c.end
	if sp.reset {
		end = min64(end, sp.resetRel)
	}
	lo := c.off
	if m := sp.mps; m != nil {
		lo = max64(lo, m.acked.prefix())
	}
	if lo >= end {
		if c.fin && sp.mps != nil && !sp.mps.finAcked && !sp.reset {
			return 1
		}
		return 0
	}
	return end - lo
}

// copiesOutstanding sums the bound's copy bytes and prunes finished ones.
func (s *Session) copiesOutstanding() uint64 {
	m := s.mp
	var sum uint64
	out := m.copies[:0]
	for _, c := range m.copies {
		n := s.copyLeft(c)
		if n == 0 {
			c.dead = true
			continue
		}
		sum += n
		out = append(out, c)
	}
	clear(m.copies[len(out):])
	m.copies = out
	return sum
}

// copyChunk queues a copy after new data [off, off+n) of st went on the
// decision's pipe: of all of it for a redundant direction, of its part
// within the first spread_chunk after an idle period otherwise.
func (s *Session) copyChunk(d *decision, w *recWriter, st *Stream, off, n uint64, fin bool) {
	p := d.p
	a := p.mp
	if !w.newData {
		w.newData = true
		if s.pipeIdle(p) {
			a.idleArm = uint64(s.cfg.SpreadChunk)
		}
	}
	idle := min64(a.idleArm, n)
	a.idleArm -= idle
	end := off + n
	switch {
	case s.redundant(st):
	case idle > 0 && s.idleCopied(st):
		end = off + idle
		fin = fin && idle == n
	default:
		return
	}
	g := d.g
	if d.tier < 0 || d.tier > g.last {
		return
	}
	target := s.fastestIn(g, d.tier, end-off, p)
	self := false
	if target == nil {
		// The tier's only pipe: its lane 0 and copy lane are the pair, for
		// redundant data (the idle copy needs a second pipe).
		if !s.redundant(st) || s.copyLaneOf(p) == nil {
			return
		}
		target, self = p, true
	}
	if s.copiesOutstanding()+(end-off) > s.copyBound() {
		return
	}
	c := &copyRec{st: st, off: off, end: end, fin: fin, queued: true}
	s.mp.copies = append(s.mp.copies, c)
	if self {
		a.selfCopyQ = append(a.selfCopyQ, c)
	} else {
		target.mp.copyQ = append(target.mp.copyQ, c)
	}
	s.wakePipe(target)
}

// writeCopies writes the copies of queue q (p's copies, or its copy
// lane's) ahead of its other stream data; all are dropped when p is no
// longer eligible.
func (s *Session) writeCopies(d *decision, w *recWriter, q *[]*copyRec) {
	p := d.p
	if len(*q) == 0 {
		return
	}
	if d.g == nil || !d.g.admits(s, p) {
		s.dropCopies(p)
		return
	}
	cq := *q
	defer func() { *q = cq }()
	for len(cq) > 0 && w.dataRoom != 0 {
		c := cq[0]
		if s.copyLeft(c) == 0 {
			c.dead = true
			cq = cq[1:]
			continue
		}
		st := c.st
		sp := &st.snd
		start := max64(c.off, sp.q.base)
		if sp.mps != nil {
			start = max64(start, sp.mps.acked.prefix())
		}
		if start < sp.preLen {
			start = 0 // the frame at offset 0 carries the whole preamble
		}
		end := min64(c.end, sp.q.end)
		if sp.reset {
			end = min64(end, sp.resetRel)
		}
		fin := c.fin && !sp.reset && (sp.mps == nil || !sp.mps.finAcked) && end == sp.q.end
		if start > end {
			start = end
		}
		n := end - start
		room := w.room() - wire.StreamHeaderLen(st.id, start, int(n), true)
		if room <= 0 {
			return
		}
		if n > uint64(room) {
			n = uint64(room)
			fin = false
		}
		var data []byte
		var blk pin
		if n > 0 {
			data, blk = sp.q.peek(start, int(n))
			if uint64(len(data)) < n {
				fin = false // a buffer segment ends first: the FIN goes with the rest
			}
			n = uint64(len(data))
		}
		if n == 0 && !fin {
			c.dead = true
			cq = cq[1:]
			continue
		}
		if !w.stream(st.id, start, data, blk, fin) {
			return
		}
		w.logFrame(frameDesc{kind: dStream, id: st.id, off: start, n: n, fin: fin})
		w.spendData(n)
		s.stats.BytesRedundant += n
		if start+n >= end {
			c.queued = false
			cq = cq[1:]
		} else {
			c.off = start + n
		}
	}
	if len(cq) == 0 {
		cq = nil
	}
}

// dropCopies drops the copies queued for p: they leave the bound.
func (s *Session) dropCopies(p *Pipe) {
	a := p.mp
	for _, c := range a.copyQ {
		c.dead = true
	}
	clear(a.copyQ)
	a.copyQ = nil
	for _, c := range a.selfCopyQ {
		c.dead = true
	}
	clear(a.selfCopyQ)
	a.selfCopyQ = nil
}

// unsettle runs when p becomes suspect, fails or starts draining: its
// queued copies are dropped, its idle copy and its revalidation end.
func (s *Session) unsettle(p *Pipe) {
	if p.mp == nil {
		return
	}
	s.dropCopies(p)
	p.mp.idleArm = 0
	s.endRevalidation(p)
}

// startReinjectHold: a new outbound pipe reinjects at most one chunk until
// the first PIPE_ACK received after it became live, for up to one smoothed
// RTT (RESUME.md 6). New stream data does not wait.
func (s *Session) startReinjectHold(p *Pipe) {
	a := p.mp
	a.held1, a.holdSent = true, 0
	a.holdUntil = s.now.Add(a.rtt.smoothed)
}

// reinjectRoom reports whether p may reinject now.
func (s *Session) reinjectRoom(p *Pipe) bool {
	a := p.mp
	if !a.held1 || s.now >= a.holdUntil {
		a.held1 = false
		return true
	}
	return a.holdSent < uint64(s.cfg.SpreadChunk)
}

// endReinjectHolds: a PIPE_ACK arrived.
func (s *Session) endReinjectHolds() {
	for _, p := range s.pipes {
		if p.mp != nil && p.mp.held1 {
			p.mp.held1 = false
			s.wakeAllWriters()
		}
	}
}
