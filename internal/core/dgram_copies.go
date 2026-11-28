package core

import (
	"cmp"
	"slices"

	"github.com/zmuxio/zmux-go/shaping"
)

type datagramCopy struct {
	handoff                                        *Handoff
	source                                         *Pipe
	st                                             *Stream
	data                                           dgramView
	deadline                                       Time
	delay                                          datagramDelay
	sequence                                       uint64
	originRevision, targetRevision                 uint64
	payload                                        int
	independent                                    bool
	adaptive, urgent, originChannel, targetChannel bool
	carrier                                        uint8
}

// commitDatagram consumes a sequence only after handing the original to a
// record or channel. Additional copies share the same immutable bytes.
func (s *Session) commitDatagram(p *Pipe, o *outDgram, data dgramView, channel bool) {
	if !s.feat.DatagramFamily {
		return
	}
	seq := s.dg.nextSequence
	if !channel {
		s.datagramDispatch(p, false, seq)
	}
	s.datagramDispatched()
	if s.mp == nil || p.mp == nil {
		return
	}
	s.replicaBytes(uint64(o.n), false)
	if _, unacked := s.datagramOpening(o.st); unacked {
		return
	}
	extras := 0
	adaptive, urgent := o.opts.Copy == DatagramAdaptive, false
	switch o.opts.Copy {
	case DatagramSingle:
		return
	case DatagramDuplicate:
		extras = min(o.opts.Copies, s.cfg.DatagramCopyLimit) - 1
	case DatagramAdaptive:
		urgent = o.opts.Urgent || !o.opts.HasUrgent && o.st != nil && o.st.urgency <= 2
		extras = min(1, s.cfg.DatagramCopyLimit-1)
	}
	if extras <= 0 {
		return
	}
	tier, _ := p.mp.effPrio()
	next := s.replicaTier(tier)
	// Bound temporary selection by the number of admitted attachments.
	var first [2]*Pipe
	targets := first[:0]
	independent := o.opts.Independent || adaptive
	for len(targets) < extras {
		if s.dg.copyCount >= s.cfg.DgramSendQ || s.dg.copyBytes+data.cost() > s.cfg.DatagramSendBytes ||
			uint64(max(o.n, 1)) > s.replicaRoom() {
			s.stats.DatagramCopyBudgetDropped++
			break
		}
		var best datagramPath
		var paths [8]datagramPath
		candidates := paths[:0]
		for _, q := range s.pipes {
			if !s.replicaEligible(p, q, next, independent) {
				continue
			}
			skip := false
			for _, selected := range targets {
				if q == selected || independent && !independentCarriers(q, selected) {
					skip = true
					break
				}
			}
			if skip {
				continue
			}
			path, ok := s.datagramPlacement(q, nil, data.len(), o.opts.Carrier, o.deadline, true)
			if !ok && path.pipe != nil && !path.channel {
				// The current batch owns the writer, but an accepting carrier
				// can take the queued copy after that batch completes.
				lane := q.out.lanes[0]
				ok = lane.writing && !lane.notWritable && !s.limHeld(q)
			}
			if !ok || path.late {
				continue
			}
			if adaptive && !urgent {
				candidates = append(candidates, path)
			} else if best.pipe == nil || path.eta < best.eta {
				best = path
			}
		}
		if adaptive && !urgent && len(candidates) == 0 {
			p.clearDatagramOriginPairs(channel)
		}
		if len(candidates) > 1 {
			slices.SortStableFunc(candidates, func(a, b datagramPath) int { return cmp.Compare(a.eta, b.eta) })
		}
		for _, path := range candidates {
			if s.adaptiveDatagramPair(p, path.pipe, channel, path.channel) {
				best = path
				break
			}
		}
		if best.pipe == nil {
			if !adaptive || urgent {
				s.stats.DatagramCopyPathDropped++
			}
			break
		}
		target := best.pipe
		targets = append(targets, target)
		replica := datagramCopy{
			data: data, handoff: o.handoff.hold(), source: p, st: o.st, deadline: o.deadline, sequence: seq,
			payload: o.n, independent: independent, carrier: o.opts.Carrier, adaptive: adaptive, urgent: urgent,
			originChannel: channel, targetChannel: best.channel,
		}
		if adaptive {
			replica.originRevision = p.datagramRevision(channel)
			replica.targetRevision = target.datagramRevision(best.channel)
		}
		data.ref()
		target.datagramCopies = append(target.datagramCopies, replica)
		if len(target.datagramCopies) == 1 && target.sh != nil && target.sh.delayActive {
			target.sh.refreshRecordCopyDelay()
		}
		s.dg.copyCount++
		s.dg.copyBytes += data.cost()
		s.dg.armDatagramExpiry(&target.datagramCopyExpiry, datagramExpiry{pipe: target}, o.deadline)
		s.wakeDatagramCopy(target)
	}
}

func (s *Session) removeDatagramCopy(p *Pipe, index int) {
	d := s.dg
	q := p.datagramCopies
	queued := q[index]
	wasNative := index == 0 && s.datagramCopyDue(p, true)
	if index == 0 && p.sh != nil {
		p.sh.settleRecordCopyDelay()
	}
	if ps := p.sh; index == 0 && ps != nil && ps.native.replica {
		r := shaping.ReasonCancelled
		if queued.deadline <= s.now {
			r = shaping.ReasonApplicationExpired
		}
		ps.cancelNative(r)
	}
	d.copyCount--
	d.copyBytes -= queued.data.cost()
	queued.data.unref()
	queued.handoff.datagramDone(false)
	clearAt(q, index)
	p.datagramCopies = q[:len(q)-1]
	if index == 0 && p.sh != nil && p.sh.delayActive {
		p.sh.refreshRecordCopyDelay()
	}
	if p.datagramCopyExpiry.index != 0 && queued.deadline == p.datagramCopyExpiry.at {
		s.refreshDatagramCopyExpiry(p)
	}
	if index == 0 && len(p.datagramCopies) > 0 && s.datagramCopyDue(p, !wasNative) {
		s.wakeDatagramCopy(p)
	}
}

func clearAt[T any](q []T, index int) {
	copy(q[index:], q[index+1:])
	clear(q[len(q)-1:])
}

func (s *Session) dropDatagramCopies(p *Pipe) {
	count := len(p.datagramCopies)
	if count == 0 {
		return
	}
	d := s.dg
	if p.sh != nil {
		p.sh.settleRecordCopyDelay()
	}
	for _, replica := range p.datagramCopies {
		d.copyBytes -= replica.data.cost()
		replica.data.unref()
		replica.handoff.datagramDone(false)
	}
	if ps := p.sh; ps != nil && ps.native.replica {
		ps.cancelNative(shaping.ReasonCancelled)
	}
	p.datagramCopies = nil
	d.copyCount -= count
	s.stats.DatagramCopyPathDropped += uint64(count)
	d.expiry.Cancel(&p.datagramCopyExpiry)
}

func (s *Session) refreshDatagramCopyExpiry(p *Pipe) {
	at := Never
	for i := range p.datagramCopies {
		at = min(at, p.datagramCopies[i].deadline)
	}
	s.dg.expiry.Set(&p.datagramCopyExpiry, at)
}

func (s *Session) datagramCopyDue(p *Pipe, channel bool) bool {
	if p == nil || len(p.datagramCopies) == 0 || !maySend(p, false) {
		return false
	}
	c := &p.datagramCopies[0]
	path, writable := s.datagramPlacement(p, p, c.data.len(), c.carrier, c.deadline, true)
	if path.pipe == nil || c.deadline <= s.now {
		return true
	}
	late := path.late && (p.sh == nil || !p.sh.native.replica)
	return writable && (late || path.channel == channel)
}

func (s *Session) wakeDatagramCopy(p *Pipe) {
	if s.datagramCopyDue(p, true) {
		s.wake(p.chSig)
	} else if s.datagramCopyDue(p, false) {
		s.wakePipe(p)
	}
}

// useDatagramCopy resolves the target and the shared budget again at the
// actual write opportunity. Refused copies are never reinjected.
func (s *Session) useDatagramCopy(p *Pipe, channel bool) *datagramCopy {
	for s.datagramCopyDue(p, channel) {
		c := &p.datagramCopies[0]
		tier, _ := c.source.mp.effPrio()
		path, ok := s.datagramPlacement(p, p, c.data.len(), c.carrier, c.deadline, true)
		switch {
		case uint64(max(c.payload, 1)) > s.replicaRoom():
			s.stats.DatagramCopyBudgetDropped++
		case c.deadline <= s.now || path.late && (p.sh == nil || !p.sh.native.replica) ||
			!ok || !s.replicaEligible(c.source, p, s.replicaTier(tier), c.independent) ||
			c.adaptive && channel != c.targetChannel:
			if c.adaptive && !c.urgent && c.source.datagramFeedback != nil {
				delete(c.source.datagramFeedback.pairs, datagramPairKey{p, c.originChannel, c.targetChannel})
			}
			s.stats.DatagramCopyPathDropped++
		case c.adaptive && !s.allowAdaptiveCopy(c, p, channel):
			s.stats.DatagramCopyPathDropped++
		default:
			return c
		}
		s.removeDatagramCopy(p, 0)
	}
	return nil
}

func (s *Session) allowAdaptiveCopy(c *datagramCopy, target *Pipe, channel bool) bool {
	return c.source.datagramRevision(c.originChannel) == c.originRevision &&
		target.datagramRevision(channel) == c.targetRevision &&
		(c.urgent || s.adaptiveDatagramPair(c.source, target, c.originChannel, channel))
}

func (s *Session) countDatagramCopy(p *Pipe, channel bool) {
	c := &p.datagramCopies[0]
	s.replicaBytes(uint64(max(c.payload, 1)), true)
	if !channel {
		s.datagramDispatch(p, false, c.sequence)
		s.stats.DatagramCopiesSent++
		s.stats.DatagramCopyBytes += uint64(c.payload)
	}
	s.removeDatagramCopy(p, 0)
}
