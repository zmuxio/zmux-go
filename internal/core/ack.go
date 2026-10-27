package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
)

// ackState is the acknowledgement machinery of a session with
// acknowledgement (MULTIPATH.md 3.5, 6): PIPE_ACK generation for inbound
// pipes and processing for outbound ones, with the per-pipe RTT
// estimator. Single-pipe sessions keep nothing else (no retention).
type ackState struct {
	s         *Session
	threshold uint64   // own zmux_ack_threshold
	delay     Duration // own zmux_max_ack_delay
	peerDelay Duration // peer's zmux_max_ack_delay
	immediate bool     // a PIPE_PING asked for an immediate PIPE_ACK
	woken     bool     // the delay expired and the writers were woken
}

// pipeSend is the sender's view of an outbound pipe as a whole, counted
// in send order across its lanes (MULTIPATH.md 2.5): estimates and
// detection use its cumulative acknowledgement point.
type pipeSend struct {
	so          uint64   // send-order number of the next byte
	cum         uint64   // cumulative acknowledgement point
	samples     []sample // per ack-eliciting batch in flight (ends in send order)
	delivered   uint64
	deliveredAt Time
	firstSentAt Time
	rtt         rttEst // single-pipe sessions; multipath uses the attachment's
	re          rateEst
	rttAt       Time     // last RTT sample
	elicitEnd   uint64   // send order after the last ack-eliciting byte
	lim         *limiter // carrier queue limiter; nil when not paced
}

// sendSeg maps lane positions [pos, end) to send order from so.
type sendSeg struct{ pos, end, so uint64 }

type sample struct {
	end         uint64
	sent        Time
	delivered   uint64
	deliveredAt Time
	firstSentAt Time
	epoch       uint64 // rateEst.blockEpoch at send
	bubble      bool   // sent while an application-limited bubble was in flight
}

// rttEst is RFC 9002 Section 5 with a 300 s minimum filter.
type rttEst struct {
	has      bool
	smoothed Duration
	rttvar   Duration
	min      Duration
	minAt    Time
	latest   Duration
}

// update takes one sample and returns the adjusted value.
func (r *rttEst) update(sample, ackDelay, maxAckDelay Duration, now Time) Duration {
	r.latest = sample
	if !r.has {
		r.has = true
		r.min, r.minAt = sample, now
		r.smoothed = sample
		r.rttvar = sample / 2
		return sample
	}
	// The minimum of the last 300 s; a minimum older than that gives way
	// to the current sample.
	if sample <= r.min || now.Sub(r.minAt) > 300*Second {
		r.min, r.minAt = sample, now
	}
	adj := sample
	ad := minD(ackDelay, maxAckDelay)
	if sample-ad >= r.min {
		adj = sample - ad
	}
	d := r.smoothed - adj
	if d < 0 {
		d = -d
	}
	r.rttvar = (3*r.rttvar + d) / 4
	r.smoothed = (7*r.smoothed + adj) / 8
	return adj
}

func newAckState(s *Session) *ackState {
	a := &ackState{s: s, threshold: 65536, delay: 25 * Millisecond, peerDelay: 25 * Millisecond}
	if l := &s.cfg.Local; l.Has&wire.HasAckThreshold != 0 {
		a.threshold = l.AckThreshold
	}
	if l := &s.cfg.Local; l.Has&wire.HasAckDelay != 0 {
		a.delay = MulD(int64(l.MaxAckDelay), Millisecond)
	}
	if s.peer.Has&wire.HasAckDelay != 0 {
		a.peerDelay = MulD(int64(s.peer.MaxAckDelay), Millisecond)
	}
	return a
}

// srttMax is the largest smoothed RTT over outbound lanes with a sample.
func (a *ackState) srttMax() Duration {
	var m Duration
	if a.s.mp != nil {
		for _, p := range a.s.pipes {
			if p.mp != nil && p.out != nil && p.mp.liveOut() && p.mp.rtt.smoothed > m {
				m = p.mp.rtt.smoothed
			}
		}
		return m
	}
	for _, p := range a.s.pipes {
		if p.out != nil && p.out.snd.rtt.has && p.out.snd.rtt.smoothed > m {
			m = p.out.snd.rtt.smoothed
		}
	}
	return m
}

// onRecord accounts a processed inbound record (MULTIPATH.md 6.3).
func (a *ackState) onRecord(il *inLane, n uint64, f recordFacts, now Time) {
	if f.elicit {
		if il.elicit == 0 {
			il.elicitAt = now
		}
		il.elicit += n
	}
	if f.pipePing {
		a.immediate = true
	}
	if a.due(now) {
		a.s.wakeAllWriters()
	}
}

// due reports whether a PIPE_ACK must be sent now.
func (a *ackState) due(now Time) bool {
	if a.immediate {
		return true
	}
	for _, p := range a.s.pipes {
		if p.in == nil {
			continue
		}
		var sum uint64
		first := Never
		for _, il := range p.in.lanes {
			if il.elicit > 0 {
				sum += il.elicit
				first = minT(first, il.elicitAt)
			}
			if il.ended && il.pos > il.reported {
				return true
			}
		}
		if sum >= a.threshold || sum > 0 && now >= first.Add(a.delay) {
			return true
		}
	}
	return false
}

// nextTimeout is the earliest acknowledgement delay deadline.
func (a *ackState) nextTimeout() Time {
	if a.woken {
		return Never // the writers were woken for it: no timer until it is sent
	}
	t := Never
	for _, p := range a.s.pipes {
		if p.in == nil {
			continue
		}
		for _, il := range p.in.lanes {
			if il.elicit > 0 {
				t = minT(t, il.elicitAt.Add(a.delay))
			}
		}
	}
	return t
}

func (a *ackState) handleTimeout(now Time) {
	if a.due(now) {
		a.woken = true
		a.s.wakeAllWriters()
	}
}

// write adds a PIPE_ACK snapshot when one is due (single pipe).
func (a *ackState) write(w *recWriter, _ *Pipe, now Time) {
	if a.due(now) {
		a.writeSnapshot(w, true)
	}
}

// writeSnapshot writes the positions of every inbound pipe and lane that
// is not retired and has a position above 0 (MULTIPATH.md 6.2), split
// over several frames when it does not fit one.
func (a *ackState) writeSnapshot(w *recWriter, _ bool) {
	now := a.s.now
	var ents [16]wire.AckEntry
	es := ents[:0]
	for _, p := range a.s.pipes {
		if p.in == nil || p.mp != nil && p.mp.retired {
			continue
		}
		for _, il := range p.in.lanes {
			if il.pos == 0 {
				continue
			}
			d := now.Sub(il.posAt) / Microsecond
			if d < 0 {
				d = 0
			}
			es = append(es, wire.AckEntry{PipeID: p.id, Lane: il.lane, Position: il.pos, Delay: uint64(d)})
		}
	}
	if len(es) == 0 {
		a.immediate = false
		return
	}
	sortEntries(es)
	for len(es) > 0 {
		k := len(es)
		n := 0
		for {
			n = wire.VarintLen(wire.TypePipeAck) + wire.VarintLen(uint64(k))
			for _, e := range es[:k] {
				n += wire.AckEntryLen(e)
			}
			if w.fits(n) || k == 1 {
				break
			}
			k = (k + 1) / 2
		}
		part := es[:k]
		if !w.frame(n, false, func(b []byte) []byte {
			b = wire.AppendPipeAckHeader(b, len(part))
			for _, e := range part {
				b = wire.AppendAckEntry(b, e)
			}
			return b
		}) {
			return
		}
		es = es[k:]
	}
	a.immediate, a.woken = false, false
	for _, p := range a.s.pipes {
		if p.in == nil {
			continue
		}
		for _, il := range p.in.lanes {
			il.elicit = 0
			il.reported = il.pos
		}
	}
}

func sortEntries(es []wire.AckEntry) {
	for i := 1; i < len(es); i++ {
		for j := i; j > 0 && (es[j].PipeID < es[j-1].PipeID ||
			es[j].PipeID == es[j-1].PipeID && es[j].Lane < es[j-1].Lane); j-- {
			es[j], es[j-1] = es[j-1], es[j]
		}
	}
}

// onSent records a produced batch for RTT samples and idle accounting.
// The PIPE_LANE record of an unconfirmed lane is outside the send order
// (MULTIPATH.md 2.5).
func (a *ackState) onSent(p *Pipe, ol *outLane, start uint64, w *recWriter, now Time) {
	if w.activity {
		ol.activity = append(ol.activity, ol.pos)
	}
	if ol.lane != 0 && !ol.confirmed {
		return
	}
	ps := &p.out.snd
	so := ps.so
	n := ol.pos - start
	if k := len(ol.segs); k > 0 && ol.segs[k-1].end == start && ol.segs[k-1].so+(start-ol.segs[k-1].pos) == so {
		ol.segs[k-1].end = ol.pos
	} else {
		ol.segs = append(ol.segs, sendSeg{pos: start, end: ol.pos, so: so})
	}
	ps.so += n
	if w.anyElic {
		// Send order up to the end of the last ack-eliciting record.
		end := so + uint64(w.elicEnd)
		if w.framing == FramingRecords {
			end = ps.so
		}
		if len(ps.samples) == 0 {
			ps.deliveredAt, ps.firstSentAt = now, now
		}
		ps.elicitEnd = max64(ps.elicitEnd, end)
		bubble, epoch := ps.re.sendFlags(ps.cum)
		ps.samples = append(ps.samples, sample{end: end, sent: now, delivered: ps.delivered,
			deliveredAt: ps.deliveredAt, firstSentAt: ps.firstSentAt, epoch: epoch, bubble: bubble})
	}
}

// laneAcked trims a lane's send-order map to its acknowledged position and
// returns the pipe's cumulative acknowledgement point.
func cumPoint(d *outDir, ol *outLane) uint64 {
	i := 0
	for i < len(ol.segs) && ol.segs[i].end <= ol.acked {
		i++
	}
	if i > 0 {
		ol.segs = append(ol.segs[:0], ol.segs[i:]...)
	}
	cum := d.snd.so
	for _, l := range d.lanes {
		if len(l.segs) == 0 {
			continue
		}
		g := l.segs[0]
		c := g.so
		if l.acked > g.pos {
			c += l.acked - g.pos
		}
		cum = min64(cum, c)
	}
	return cum
}

// onPipeAck processes a PIPE_ACK (MULTIPATH.md 3.5, 6.2).
func (a *ackState) onPipeAck(f *wire.Frame, now Time) *wire.Error {
	s := a.s
	if s.mp != nil {
		s.endReinjectHolds()
	}
	it := f.Iter()
	for e, ok := it.Next(); ok; e, ok = it.Next() {
		if s.mp != nil {
			if err := s.mpAckEntry(e, now); err != nil {
				return err
			}
			continue
		}
		if e.PipeID != 0 {
			return wire.NewError(wire.ProtocolViolation, "PIPE_ACK for a pipe other than 0", "pipe", e.PipeID)
		}
		if e.Position == 0 {
			continue
		}
		p := s.pipe(0)
		var ol *outLane
		if p != nil && p.out != nil {
			ol = p.out.lane(e.Lane)
		}
		if ol == nil {
			return wire.NewError(wire.ProtocolViolation, "PIPE_ACK for an unopened lane", "pipe", 0, "value", e.Lane)
		}
		if ol.refused {
			continue
		}
		if e.Position > ol.pos {
			return wire.NewError(wire.ProtocolViolation, "PIPE_ACK position beyond the bytes sent",
				"pipe", 0, "pos", e.Position)
		}
		if e.Position <= ol.acked {
			continue
		}
		ol.acked = e.Position
		a.advanced(p, ol, e, now)
	}
	return nil
}

// advanced takes RTT samples and idle-timer evidence from a lane's
// advance; estimates move with the pipe's cumulative point only.
func (a *ackState) advanced(p *Pipe, ol *outLane, e wire.AckEntry, now Time) {
	s := a.s
	j := 0
	for j < len(ol.activity) && ol.activity[j] <= e.Position {
		j++
	}
	if j > 0 {
		ol.activity = append(ol.activity[:0], ol.activity[j:]...)
		s.timers.lastActivity = now
	}
	ps := &p.out.snd
	cum := cumPoint(p.out, ol)
	if cum > ps.cum {
		newly := cum - ps.cum
		ps.cum = cum
		ps.delivered += newly
		var ev limAck
		ev.roundEnd = ps.re.advance(cum, ps.so)
		var last *sample
		i := 0
		for i < len(ps.samples) && ps.samples[i].end <= cum {
			last = &ps.samples[i]
			i++
		}
		if last != nil {
			rtt := now.Sub(last.sent)
			ad := MulD(int64(e.Delay), Microsecond)
			r := &ps.rtt
			if p.mp != nil {
				r = &p.mp.rtt
			}
			// A late entry (ACK Delay above the peer's maximum, typically
			// the redundant copy) gives no RTT sample, unless the pipe has
			// had none for 1 s.
			late := ad > a.peerDelay && (ps.rttAt == 0 || now.Sub(ps.rttAt) < Second)
			if !late && (p.mp == nil || p.mp.sampleOK(last.sent)) {
				ev.rtt = r.update(rtt, minD(ad, a.peerDelay), a.peerDelay, now)
				ev.rttSentAt = last.sent
				ps.rttAt = now
				s.tuneAckSample(r.min)
			}
			// Delivery rate sample.
			iv := maxD(last.sent.Sub(last.firstSentAt), now.Sub(last.deliveredAt))
			if iv > 0 && (p.mp == nil || p.mp.sampleOK(last.sent)) {
				ev.rate = ps.re.sample(float64(ps.delivered-last.delivered)*1e9/float64(iv), last.bubble, last.epoch, now)
			}
			ps.firstSentAt = last.sent
		}
		ps.samples = append(ps.samples[:0], ps.samples[i:]...)
		ps.deliveredAt = now
		s.paceAck(p, &ev)
		if s.q.band != nil || s.q.floorSet || p.out.q.band != nil {
			s.qualitySample(p)
		}
		if p.mp != nil {
			s.mpAdvanced(p, ol, ps, now)
		}
	} else if p.mp != nil {
		s.mpLaneAdvanced(p, ol)
	}
	s.lanesAcked(p, ol)
}

// hasEntries reports whether a snapshot would have an entry.
func (a *ackState) hasEntries() bool {
	for _, p := range a.s.pipes {
		if p.in == nil || p.mp != nil && p.mp.retired {
			continue
		}
		for _, il := range p.in.lanes {
			if il.pos > 0 {
				return true
			}
		}
	}
	return false
}

// unreported reports positions not yet in a PIPE_ACK.
func (a *ackState) unreported() bool {
	for _, p := range a.s.pipes {
		if p.in == nil {
			continue
		}
		for _, il := range p.in.lanes {
			if il.pos > il.reported {
				return true
			}
		}
	}
	return false
}
