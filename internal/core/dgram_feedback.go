package core

import (
	"slices"
	"sort"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

const (
	datagramHistory           = 1024
	datagramObservationWindow = 10 * Second
	datagramReportSpacing     = 25 * Millisecond
)

type datagramFeedback struct {
	received [2]datagramArrivals
	sent     [2]datagramDispatches
	pairs    map[datagramPairKey]datagramPair
}

type datagramArrival struct {
	seq uint64
	at  Time
}

type datagramArrivals struct {
	entries []datagramArrival
	timer   Deadline[datagramReport]
	next    int
	sentAt  Time
	sent    bool
}

type datagramReport struct {
	pipe    *Pipe
	channel uint8
}

type datagramDispatch struct {
	seq              uint64
	at               Time
	earlyAck         Time
	earlyDelay       uint64
	acknowledged     bool
	pending, refused bool
	earlyLargest     bool
}

type datagramDispatches struct {
	entries            []datagramDispatch
	first              int
	rtt, variation     Duration
	samples            uint64
	sampledAt          Time
	acknowledged       uint64
	recent             [3]Time
	loss               datagramLoss
	source             datagramSource
	lossRun, timingRun uint64
}

type datagramLoss struct {
	at        Time
	finalized uint64
	lost      uint64
	set       bool
}

func (p *Pipe) feedback() *datagramFeedback {
	if p.datagramFeedback == nil {
		p.datagramFeedback = &datagramFeedback{}
	}
	return p.datagramFeedback
}

func (s *Session) datagramArrival(p *Pipe, channel bool, seq uint64) {
	if p == nil || p.in == nil || p.in.ended || !s.feat.DatagramFamily || s.gates.MaxDatagram == 0 {
		return
	}
	s.dgState()
	c := uint8(b2i(channel))
	h := &p.feedback().received[c]
	v := datagramArrival{seq: seq, at: s.now}
	if len(h.entries) < datagramHistory {
		h.entries = append(h.entries, v)
	} else {
		h.entries[h.next] = v
		h.next = (h.next + 1) % datagramHistory
	}
	if h.timer.index == 0 {
		s.armDatagramReport(p, c, h)
		s.datagramReportTimeout(s.now)
	}
}

func (s *Session) datagramDispatch(p *Pipe, channel bool, seq uint64) {
	s.datagramDispatchAt(p, channel, seq, s.now)
}

func (s *Session) datagramDispatchAt(p *Pipe, channel bool, seq uint64, at Time) {
	if !s.feat.DatagramFamily || !s.gates.DatagramAck {
		return
	}
	h := &p.feedback().sent[b2i(channel)]
	h.add(datagramDispatch{seq: seq, at: at})
}

func (s *Session) datagramDispatchPending(p *Pipe, seq uint64) {
	if s.feat.DatagramFamily && s.gates.DatagramAck {
		p.feedback().sent[1].add(datagramDispatch{seq: seq, at: s.now, pending: true, earlyAck: Never})
	}
}

func (s *Session) datagramDispatchDone(p *Pipe, seq uint64, failed bool) {
	if p.datagramFeedback == nil {
		return
	}
	h := &p.datagramFeedback.sent[1]
	i := sort.Search(len(h.entries), func(i int) bool { return h.at(i).seq >= seq })
	if i == len(h.entries) {
		return
	}
	e := h.at(i)
	if e.seq != seq || !e.pending {
		return
	}
	e.pending, e.refused = false, failed
	if !failed && e.earlyAck != Never {
		h.observe(e, e.earlyAck, s.now, e.earlyLargest, e.earlyDelay)
	}
}

func (h *datagramDispatches) at(i int) *datagramDispatch {
	return &h.entries[(h.first+i)%len(h.entries)]
}

func (h *datagramDispatches) add(dispatch datagramDispatch) {
	n := len(h.entries)
	// Originals append in sequence order. Replicas can arrive after a
	// newer original, so locate the insertion point on that path only.
	i := n
	if n > 0 && h.at(n-1).seq >= dispatch.seq {
		i = sort.Search(n, func(i int) bool { return h.at(i).seq >= dispatch.seq })
		if h.at(i).seq == dispatch.seq {
			return
		}
	}
	if n == datagramHistory {
		if i == 0 {
			return
		}
		h.first = (h.first + 1) % datagramHistory
		i--
	} else {
		h.entries = append(h.entries, datagramDispatch{})
		n++
	}
	for j := n - 1; j > i; j-- {
		*h.at(j) = *h.at(j - 1)
	}
	*h.at(i) = dispatch
}

func (s *Session) onDatagramAck(a *wire.DatagramAck) {
	p := s.pipe(a.PipeID)
	if p == nil || p.datagramFeedback == nil {
		return
	}
	h := &p.datagramFeedback.sent[a.Channel]
	pos := len(h.entries) - 1
	it := a.Iter()
	for lo, hi, ok := it.Next(); ok && pos >= 0; lo, hi, ok = it.Next() {
		for pos >= 0 && h.at(pos).seq > hi {
			pos--
		}
		for pos >= 0 && h.at(pos).seq >= lo {
			e := h.at(pos)
			pos--
			if e.pending {
				if e.earlyAck == Never {
					e.earlyAck, e.earlyLargest, e.earlyDelay = s.now, e.seq == a.Largest, a.Delay
				}
			} else if !e.refused {
				h.observe(e, s.now, s.now, e.seq == a.Largest, a.Delay)
			}
		}
	}
}

func (h *datagramDispatches) observe(e *datagramDispatch, at, now Time, largest bool, delay uint64) {
	elapsed := at.Sub(e.at)
	if e.acknowledged || elapsed >= datagramObservationWindow {
		return
	}
	e.acknowledged = true
	h.acknowledged++
	if !largest || delay >= uint64(elapsed/Microsecond) {
		return
	}
	sample := elapsed/Microsecond - Duration(delay)
	if h.samples >= 3 && !h.timingFresh(now) {
		h.timingRun++
	}
	if h.samples == 0 || at.Sub(h.sampledAt) >= datagramObservationWindow {
		h.rtt, h.variation, h.samples = sample*Microsecond, sample/2*Microsecond, 0
	} else {
		rtt, variation := h.rtt/Microsecond, h.variation/Microsecond
		diff := rtt - sample
		if diff < 0 {
			diff = -diff
		}
		h.variation = (variation + (diff-variation)/4) * Microsecond
		h.rtt = (rtt + (sample-rtt)/8) * Microsecond
	}
	h.recent[h.samples%3] = at
	h.samples++
	h.sampledAt = max(h.sampledAt, at)
}

// DatagramPathQuality reports positive arrival evidence for a path and
// channel. Unconfirmed dispatches carry no loss conclusion.
type DatagramPathQuality struct {
	Tracking, Awaiting, Arrived, Expired uint64
	RoundTrip, Variation                 Duration
	SampledAt                            Time
	Samples                              uint64
}

func (s *Session) DatagramQuality(id uint64, channel bool) (q DatagramPathQuality, ok bool) {
	p := s.pipe(id)
	if p == nil || p.datagramFeedback == nil {
		return q, false
	}
	h := &p.datagramFeedback.sent[b2i(channel)]
	if len(h.entries) == 0 {
		return q, false
	}
	for _, entry := range h.entries {
		if entry.refused || entry.pending {
			continue
		}
		q.Tracking++
		switch {
		case s.now.Sub(entry.at) >= datagramObservationWindow:
			q.Expired++
		case entry.acknowledged:
			q.Arrived++
		default:
			q.Awaiting++
		}
	}
	if h.samples > 0 && s.now.Sub(h.sampledAt) < datagramObservationWindow {
		q.RoundTrip, q.Variation, q.SampledAt, q.Samples = h.rtt, h.variation, h.sampledAt, h.samples
	}
	return q, q.Tracking != 0
}

// timingFresh requires three recent samples. Carrier completion can apply
// a pending ACK after newer ACKs have already updated the estimator.
func (h *datagramDispatches) timingFresh(now Time) bool {
	return h.samples >= 3 && now.Sub(min(h.recent[0], h.recent[1], h.recent[2])) < datagramObservationWindow
}

func (s *Session) datagramAckSpacing(p *Pipe) Duration {
	if p.mp != nil && p.out != nil && p.mp.rtt.has {
		return maxD(datagramReportSpacing, p.mp.rtt.smoothed/4)
	}
	return datagramReportSpacing
}

func (s *Session) updateDatagramReportLimit() {
	if s.dg == nil {
		return
	}
	n := 0
	for _, p := range s.pipes {
		if p.in != nil && !p.in.ended && !p.retired() {
			n += 2
		}
	}
	s.dg.reportLimit = n
	s.resetReportWake()
}

func (s *Session) armDatagramReport(p *Pipe, channel uint8, h *datagramArrivals) {
	at := s.now
	if h.sent {
		at = h.sentAt.Add(s.datagramAckSpacing(p))
	}
	d := &h.timer
	d.Value = datagramReport{p, channel}
	d.Order = int(p.id*2 + uint64(channel))
	s.dg.reportTimers.Set(d, at)
}

func (s *Session) refreshDatagramReports(p *Pipe) {
	if s.dg == nil || p.datagramFeedback == nil {
		return
	}
	for c := range p.datagramFeedback.received {
		h := &p.datagramFeedback.received[c]
		if h.timer.index != 0 && h.sent {
			s.armDatagramReport(p, uint8(c), h)
		}
	}
	s.resetReportWake()
}

func (s *Session) cancelDatagramReports(p *Pipe) {
	if s.dg != nil && p.datagramFeedback != nil {
		for c := range p.datagramFeedback.received {
			s.dg.reportTimers.Cancel(&p.datagramFeedback.received[c].timer)
		}
	}
	s.updateDatagramReportLimit()
}

func (s *Session) resetReportWake() {
	if s.dg.reportWoken && s.nextDatagramReport() > s.now {
		s.dg.reportWoken = false
	}
}

func (s *Session) nextDatagramReport() Time {
	d := s.dg
	if d == nil || s.state >= StateClosing || !s.feat.DatagramFamily {
		return Never
	}
	next := d.reportTimers.Next()
	if next == Never || d.reportLimit == 0 {
		return Never
	}
	if i := len(d.reportTimes) - d.reportLimit; i >= 0 {
		next = max(next, d.reportTimes[i].Add(Second))
	}
	return next
}

func (s *Session) datagramReportWake() Time {
	if s.dg == nil || s.dg.reportWoken {
		return Never
	}
	return s.nextDatagramReport()
}

func (s *Session) datagramReportTimeout(now Time) {
	if s.datagramReportWake() <= now {
		s.dg.reportWoken = true
		s.wakeAllWriters()
	}
}

func (s *Session) writeDatagramReports(w *recWriter) {
	if s.nextDatagramReport() > s.now {
		return
	}
	d := s.dg
	d.reportWoken = false
	times := d.reportTimes
	for len(times) > 0 && times[0].Add(Second) <= s.now {
		times = times[1:]
	}
	d.reportTimes = times
	for len(d.reportTimes) < d.reportLimit {
		entry := d.reportTimers.Peek()
		if entry == nil || entry.at > s.now {
			break
		}
		item := entry.Value
		h := &item.pipe.datagramFeedback.received[item.channel]
		report := s.makeDatagramReport(item.pipe.id, item.channel, h)
		if len(report) != 0 {
			if !w.frame(len(report), false, func(b []byte) []byte { return append(b, report...) }) {
				d.reportWoken = true // batch room or shaping releases the pending report
				return
			}
			h.sent, h.sentAt = true, s.now
			d.reportTimes = append(d.reportTimes, s.now)
		}
		d.reportTimers.Cancel(entry)
	}
}

func (s *Session) makeDatagramReport(id uint64, channel uint8, h *datagramArrivals) []byte {
	d := s.dg
	arrivals := d.reportScratch[:0]
	for _, entry := range h.entries {
		if s.now.Sub(entry.at) < datagramObservationWindow {
			arrivals = append(arrivals, entry)
		}
	}
	d.reportScratch = arrivals
	if len(arrivals) == 0 {
		return nil
	}
	slices.SortFunc(arrivals, func(a, b datagramArrival) int {
		switch {
		case a.seq > b.seq || a.seq == b.seq && a.at > b.at:
			return -1
		case a.seq == b.seq && a.at == b.at:
			return 0
		default:
			return 1
		}
	})
	largest := arrivals[0]
	lo, hi := largest.seq, largest.seq
	ack := wire.DatagramAck{PipeID: id, Channel: channel, Largest: largest.seq,
		Delay: uint64(s.now.Sub(largest.at) / Microsecond)}
	ack.Ranges = d.reportRanges[:0]
	var prevLo uint64
	add := func() {
		if prevLo == 0 {
			ack.FirstRange = hi - lo
			return
		}
		ack.Ranges = varint.AppendVarint(ack.Ranges, prevLo-hi-2)
		ack.Ranges = varint.AppendVarint(ack.Ranges, hi-lo)
		ack.RangeCount++
	}
	for _, entry := range arrivals[1:] {
		if entry.seq >= lo {
			continue
		}
		if entry.seq == lo-1 {
			lo = entry.seq
			continue
		}
		// This leaves room for the header at every varint width.
		if len(ack.Ranges) > wire.DatagramAckMaxSize-64 {
			break
		}
		add()
		prevLo, lo, hi = lo, entry.seq, entry.seq
	}
	add()
	d.reportRanges = ack.Ranges
	d.reportFrame = wire.AppendDatagramAck(d.reportFrame[:0], ack)
	return d.reportFrame
}
