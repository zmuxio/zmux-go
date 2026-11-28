package core

import (
	"cmp"
	"slices"
	"sort"
)

const relativePTOs = 3

func (s *Session) ackStall(p *Pipe) bool {
	a := p.mp
	return p.out != nil && a.unacked && !a.topologyOut && !a.retired && s.mp.clock.inbound != 0
}

func (s *Session) ackStallDeadline(p *Pipe) Time {
	if !s.ackStall(p) {
		return Never
	}
	a := p.mp
	at := a.tref().Add(s.tSuspect(a))
	if a.relativeDue != 0 {
		at = minT(at, a.relativeDue)
	}
	return at
}

func (s *Session) writeStallDeadline(p *Pipe) Time {
	if start, waiting := s.writeStall(p); waiting {
		return start.Add(s.tSuspect(p.mp))
	}
	return Never
}

func (s *Session) activeStallDeadline(p *Pipe) Time {
	p.mp.writeDue = s.writeStallDeadline(p)
	return minT(s.ackStallDeadline(p), p.mp.writeDue)
}

func (a *attach) sentDeliveryRecords(pos uint64, records []recMark) {
	for _, record := range records {
		pos += uint64(recordPositionBytes(record.size))
		if !record.eliciting {
			continue
		}
		if n := len(a.deliveryEnds); n == ackKeepMany {
			// A coarser entry can delay evidence, but never credits an
			// acknowledgement for an ack-eliciting record it did not cover.
			a.deliveryEnds[n-1] = pos
		} else {
			a.deliveryEnds = append(a.deliveryEnds, pos)
		}
	}
}

func (a *attach) ackedDeliveryRecords(cum uint64) bool {
	i := 0
	for i < len(a.deliveryEnds) && a.deliveryEnds[i] <= cum {
		i++
	}
	if i == 0 {
		return false
	}
	current := a.deliveryEnds[i-1] > a.health.send.startPosition
	a.deliveryEnds = append(a.deliveryEnds[:0], a.deliveryEnds[i:]...)
	return current
}

// deliveryUntil is the half-open window of current cumulative ACK evidence.
// A path waiting for local carrier capacity can still prove delivery.
func (s *Session) deliveryUntil(p *Pipe) Time {
	a := p.mp
	if a == nil || s.mp.clock.inbound == 0 || !s.pipeUsable(p, false) || !a.peerAttached(p.id) ||
		!a.health.snapshot.Send.Settled || !a.rtt.has ||
		a.deliveryGen != a.health.send.generation || a.measuredGen != a.deliveryGen || a.deliveryCost <= 0 {
		return 0
	}
	cost := s.stallCost(a)
	until := a.deliveryAt.Add(MulD(minD(a.deliveryCost, cost), 2))
	if s.ackStall(p) {
		until = minT(until, a.tref().Add(cost))
	}
	if a.writeDue != 0 {
		until = minT(until, a.writeDue)
	}
	return until
}

func (s *Session) hasDelivery(p *Pipe, anchor Time) bool {
	return s.deliveryUntil(p) > s.now && p.mp.deliveryAt > anchor && s.ackStallDeadline(p) > s.now
}

func (s *Session) relativeStallChanged() {
	s.mp.clock.relativeDirty = true
}

func (c *pathClock) relativeCheck() Time {
	if c.relativeAt == 0 {
		return Never
	}
	return c.relativeAt
}

// refreshRelativeStalls derives the active ACK arms for the current cut.
// Process cheaper paths first so a due path cannot pass its evidence on to
// a more expensive one. The reference frontier keeps just the ACK proofs
// that improve on all cheaper paths' progress, in increasing cost order.
func (s *Session) refreshRelativeStalls(now Time) {
	c := &s.mp.clock
	if !c.relativeDirty && c.relativeCheck() > now {
		return
	}
	c.relativeDirty = false
	c.relativeAt = 0
	clear(c.references)
	c.references = c.references[:0]
	for _, p := range c.stallOrder {
		if p.mp.relativeDue != 0 {
			p.mp.relativeDue = 0
			if p.mp.deadlines[pathStall].index != 0 {
				s.indexPathDeadline(p, pathStall)
			}
		}
	}
	clear(c.stallOrder)
	c.stallOrder = c.stallOrder[:0]
	if c.ackWork == 0 || c.activeOut < 2 {
		return
	}
	for _, p := range s.pipes {
		if p.mp != nil && !p.mp.retired && p.out != nil && p.mp.out == OutActive {
			c.stallOrder = append(c.stallOrder, p)
		}
	}
	slices.SortFunc(c.stallOrder, func(a, b *Pipe) int {
		if order := cmp.Compare(s.stallCost(a.mp), s.stallCost(b.mp)); order != 0 {
			return order
		}
		return cmp.Compare(a.id, b.id)
	})
	for _, p := range c.stallOrder {
		a := p.mp
		a.writeDue = s.writeStallDeadline(p)
		if s.ackStall(p) {
			anchor := a.tref()
			i := sort.Search(len(c.references), func(i int) bool {
				return c.references[i].mp.deliveryAt > anchor
			})
			if i < len(c.references) {
				ref := c.references[i]
				floor, _ := s.deadParams(a)
				delay := maxD(floor, MulD(s.stallCost(ref.mp), relativePTOs))
				if delay < s.tSuspect(a) {
					a.relativeDue = anchor.Add(delay)
					c.relativeAt = minT(c.relativeCheck(), minT(a.relativeDue, s.deliveryUntil(ref)))
				}
			}
		}
		s.indexPathDeadline(p, pathStall)
		if s.deliveryUntil(p) > now && s.ackStallDeadline(p) > now &&
			(len(c.references) == 0 || c.references[len(c.references)-1].mp.deliveryAt < a.deliveryAt) {
			c.references = append(c.references, p)
		}
	}
}
