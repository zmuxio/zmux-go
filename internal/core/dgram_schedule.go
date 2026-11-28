package core

import (
	"slices"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

type datagramPath struct {
	pipe    *Pipe
	tier    uint64
	eta     float64
	channel bool
	late    bool
}

// datagramOpening is the S7 constraint. The opening can become temporarily
// unavailable, in which case an original must wait for a reinjection or ACK.
func (s *Session) datagramOpening(st *Stream) (*Pipe, bool) {
	if s.mp == nil || st == nil || !st.local {
		return nil, false
	}
	m := st.snd.mps
	if m != nil && m.openAcked {
		return nil, false
	}
	if m == nil || !m.openSet {
		return nil, true
	}
	return s.pipe(m.openPipe), true
}

func (s *Session) recordDatagramFits(p *Pipe, n int) bool {
	ol := p.out.lane(0)
	return ol != nil && ol.confirmed && !ol.shut &&
		datagramFrameLen(n, false) <= s.recMax(ol) &&
		uint64(datagramFrameLen(n, false)) <= s.peerMaxDgram
}

func (s *Session) datagramPathFor(o *outDgram, shaped bool) datagramPath {
	n := s.datagramDataLen(o)
	opening, constrained, class, usable := s.datagramClass(o.st)
	if ps := o.reserved; shaped && ps != nil && ps.p.sh == ps && s.datagramUsable(ps.p, opening, constrained, class, usable) &&
		s.channelOf(ps.p).takes(n) {
		return datagramPath{pipe: ps.p, channel: true}
	}
	var best datagramPath
	var leastTier, greatestTier uint64 = ^uint64(0), 0
	late, mayImprove, waitingNative := false, false, false
	carrier := o.opts.Carrier
	for _, p := range s.pipes {
		if !s.datagramUsable(p, opening, constrained, class, usable) {
			continue
		}
		path, writable := s.datagramPlacement(p, nil, n, carrier, o.deadline, shaped)
		if path.pipe == nil {
			continue
		}
		if path.channel && carrier != DatagramRequireNative {
			// A structurally eligible native path owns the representation
			// even while its room is temporarily too small.
			carrier = DatagramRequireNative
			best, late, mayImprove = datagramPath{}, false, false
			leastTier, greatestTier = ^uint64(0), 0
		}
		if path.channel && !writable {
			waitingNative = true
		} else {
			leastTier, greatestTier = min(leastTier, path.tier), max(greatestTier, path.tier)
		}
		if !writable || path.late {
			late = late || writable && path.late
			mayImprove = mayImprove || !writable && !path.late
			continue
		}
		if betterDatagramPath(path, best) {
			best = path
		}
	}
	if s.mp != nil && !constrained && greatestTier > leastTier {
		g := s.buildGateFor(&s.dg.pathGate, o, n, carrier)
		best, late, mayImprove = datagramPath{}, false, waitingNative
		if g != nil {
			for _, p := range g.pipes {
				if !s.takes(g, p) {
					continue
				}
				path, writable := s.datagramPlacement(p, nil, n, carrier, o.deadline, shaped)
				if !writable || path.late {
					late = late || writable && path.late
					mayImprove = mayImprove || !writable && !path.late
					continue
				}
				if betterDatagramPath(path, best) {
					best = path
				}
			}
		}
	}
	best.late = best.pipe == nil && late && !mayImprove
	return best
}

func (s *Session) datagramClass(st *Stream) (opening *Pipe, constrained bool, c class, usable bool) {
	opening, constrained = s.datagramOpening(st)
	usable = true
	if s.mp != nil && !constrained {
		c, usable = s.usableClass()
	}
	return opening, constrained, c, usable
}

func (s *Session) datagramUsable(p, opening *Pipe, constrained bool, c class, usable bool) bool {
	return maySend(p, false) && usable && (!constrained || p == opening) &&
		(p.mp == nil || constrained || s.classUsable(c, p))
}

func betterDatagramPath(path, best datagramPath) bool {
	return best.pipe == nil || path.channel && !best.channel ||
		path.channel == best.channel && path.eta < best.eta
}

func (s *Session) datagramFitsCarrier(p *Pipe, n int, carrier uint8) bool {
	return s.channelOf(p).takes(n) || carrier != DatagramRequireNative && s.recordDatagramFits(p, n)
}

func (s *Session) datagramPlacement(p, writer *Pipe, n int, carrier uint8, deadline Time, shaped bool) (datagramPath, bool) {
	use := s.channelOf(p)
	ch := use.supports(n)
	if !ch && (carrier == DatagramRequireNative || !s.recordDatagramFits(p, n)) {
		return datagramPath{}, false
	}
	path := datagramPath{pipe: p, channel: ch}
	if ch && !use.takes(n) {
		return path, false
	}
	if p.mp != nil {
		path.tier, _ = p.mp.effPrio()
		path.eta = float64(owdFrom(p.mp, s.owdBase())) / 1e9
		if !ch {
			path.eta += (s.queued(p) + float64(n)) / s.recentRate(p)
		}
	}
	if shaped && ch && p.sh != nil && p.sh.native.ready > s.now {
		path.eta += float64(p.sh.native.ready.Sub(s.now)) / float64(Second)
	}
	path.late = deadline != Never && path.eta > float64(deadline.Sub(s.now))/1e9
	return path, ch || p == writer || s.open(p)
}

func (s *Session) datagramCapacity(st *Stream) (record, limit, room int) {
	room = -1 // distinguish an empty-only channel from no channel
	opening, constrained := s.datagramOpening(st)
	class, usable := class{}, true
	if constrained && opening == nil {
		constrained = false
	}
	if s.mp != nil && !constrained {
		class, usable = s.usableClass()
	}
	for _, p := range s.pipes {
		if !s.datagramUsable(p, opening, constrained, class, usable) {
			continue
		}
		for _, lane := range p.out.lanes {
			if lane.lane == 0 && lane.confirmed && !lane.shut {
				record = max(record, s.recMax(lane))
			}
		}
		if c := s.channelOf(p); c.Limit > 0 {
			header := wire.ChannelDatagramLen(c.HasID, c.ID, 0)
			limit = max(limit, c.Limit-header)
			room = max(room, c.Room-header)
		}
	}
	return record, limit, room
}

type datagramFlow struct {
	stream                   *Stream
	queue                    []outDgram
	expiry                   Deadline[datagramExpiry]
	route                    datagramPath
	delay                    flowDelay
	codel                    codel
	bytes                    int
	nativeCount, nativeBytes int
}

// A flow or replica path owns its queued datagrams and its earliest
// expiry. Only that owner revisits its queue when its first expiry moves.
type datagramExpiry struct {
	flow *datagramFlow
	pipe *Pipe
}

func (f *datagramFlow) urgency(s *Session) uint8 {
	if f.stream == nil {
		return s.cfg.DatagramUrgency
	}
	return f.stream.urgency
}

func (s *Session) enqueueDatagram(o outDgram) {
	d := s.dgState()
	f := d.appendDatagram(o)
	if s.feat.DatagramFamily {
		for len(f.queue)+f.nativeCount > s.cfg.DatagramFlowQ || f.bytes+f.nativeBytes > s.cfg.DatagramFlowBytes {
			s.dropDatagram(f)
		}
	}
	for s.datagramSendSlots() < 0 || s.datagramSendRoom() < 0 {
		oldest := d.active[0]
		for _, q := range d.active[1:] {
			if q.queue[0].at < oldest.queue[0].at {
				oldest = q
			}
		}
		s.dropDatagram(oldest)
	}
	if len(f.queue) != 0 {
		s.routeEnqueuedDatagrams(f, len(f.queue)-1)
	}
}

// appendDatagram records a reservation. Group admission reserves all capacity
// before using it, so it can install every child in the same owner turn.
func (d *dgramState) appendDatagram(o outDgram) *datagramFlow {
	f := d.flows[o.st]
	if f == nil {
		f = &datagramFlow{stream: o.st}
		if d.flows == nil {
			d.flows = make(map[*Stream]*datagramFlow)
		}
		d.flows[o.st] = f
	}
	if len(f.queue) == 0 {
		d.active = append(d.active, f)
	}
	f.queue = append(f.queue, o)
	f.bytes += o.data.cost()
	d.sendCount++
	d.armDatagramExpiry(&f.expiry, datagramExpiry{flow: f}, o.deadline)
	d.sendBytes += o.data.cost()
	return f
}

func (s *Session) routeEnqueuedDatagrams(f *datagramFlow, first int) {
	if first == 0 {
		s.routeDatagram(f)
		s.checkNativeCapacity(f)
	} else if s.shp != nil {
		s.refreshFlowDelay(f)
	}
}

// A carrier that only exposes a capacity query can gain room without
// notifying us. A new head without a native route can request a channel
// observation even when it has a record fallback.
func (s *Session) checkNativeCapacity(f *datagramFlow) {
	if f.route.channel || !s.feat.CarrierDatagrams || len(f.queue) == 0 {
		return
	}
	o := &f.queue[0]
	if ready, _ := o.ready(); !ready {
		return
	}
	opening, constrained := s.datagramOpening(o.st)
	for _, p := range s.pipes {
		if maySend(p, false) && (!constrained || p == opening) && p.out.lanes[0].info.Channel {
			s.wake(p.chSig)
		}
	}
}

// routeDatagram moves the flow head's registration to its current consumer.
// The registration also identifies the frame that consumer may dispatch.
func (s *Session) routeDatagram(f *datagramFlow) {
	var next datagramPath
	if len(f.queue) != 0 && s.peerSeen && s.state < StateClosing {
		o := &f.queue[0]
		if ready, _ := o.ready(); ready && o.deadline > s.now {
			next = s.datagramPathFor(o, true)
		}
	}
	old := f.route
	f.route = next
	if s.shp != nil {
		s.refreshFlowDelay(f)
	}
	if old.pipe == next.pipe && old.channel == next.channel {
		return
	}
	if old.pipe != nil && !old.channel {
		old.pipe.datagramRecords--
	}
	if next.pipe == nil {
		return
	}
	if next.channel {
		s.wake(next.pipe.chSig)
	} else {
		next.pipe.datagramRecords++
		s.wakePipe(next.pipe)
	}
}

func (s *Session) routeDatagramStream(st *Stream) {
	if s.dg != nil {
		if f := s.dg.flows[st]; f != nil {
			s.routeDatagram(f)
		}
	}
}

func (s *Session) routeDatagrams() {
	if s.dg != nil {
		for _, f := range s.dg.active {
			s.routeDatagram(f)
		}
	}
}

func (s *Session) recordDatagrams(p *Pipe) bool {
	return p.datagramRecords != 0 || s.datagramCopyDue(p, false)
}

func (s *Session) dropDatagram(f *datagramFlow) {
	s.removeDatagram(f, false)
	s.stats.DatagramsDroppedSend++
}

func (s *Session) removeDatagram(f *datagramFlow, rotate bool) {
	d := s.dg
	s.removeDatagramAt(f, 0)

	if len(f.queue) != 0 && !rotate {
		return
	}
	for i, q := range d.active {
		if q != f {
			continue
		}
		copy(d.active[i:], d.active[i+1:])
		last := len(d.active) - 1
		if len(f.queue) != 0 {
			d.active[last] = f
		} else {
			d.active[last] = nil
			d.active = d.active[:last]
			d.releaseDatagramFlow(f)
		}
		return
	}
}

func (d *dgramState) releaseDatagramFlow(f *datagramFlow) {
	if len(f.queue) == 0 && f.nativeCount == 0 {
		delete(d.flows, f.stream)
	}
}

// nextDatagram arbitrates between eligible Flow heads by urgency. Rotating
// the winning flow after dispatch gives peers at the same level equal turns.
func (s *Session) nextDatagram(p *Pipe, channel bool, urgency uint8) *datagramFlow {
	d := s.dg
	if d == nil || d.sendCount == 0 || !s.peerSeen || s.state >= StateClosing {
		return nil
	}
	var best *datagramFlow
	for _, f := range d.active {
		u := f.urgency(s)
		if u > urgency || best != nil && u >= best.urgency(s) {
			continue
		}
		path := f.route
		o := &f.queue[0]
		if path.pipe != nil && (p == nil || path.pipe == p) && path.channel == channel &&
			(o.reserved == nil || path.pipe != o.reserved.p || !channel) {
			best = f
		}
	}
	return best
}

func (s *Session) pruneDatagramHeads() {
	d := s.dg
	if d == nil {
		return
	}
	for i := 0; i < len(d.active); {
		f := d.active[i]
		o := &f.queue[0]
		_, dead := o.ready()
		if o.deadline <= s.now || f.route.late {
			s.stats.DatagramDeadlineDropped++
			s.dropDatagram(f)
			continue
		}
		if dead || s.datagramExhausted() || uint64(1+s.datagramDataLen(o)) > s.peerMaxDgram {
			s.dropDatagram(f)
			continue
		}
		i++
	}
}

func (s *Session) datagramCoDel(f *datagramFlow) bool {
	if !f.codel.drop(f.queue[0].at, s.now, s.cfg.DgramTarget, len(f.queue) == 1) {
		return false
	}
	s.dropDatagram(f)
	return true
}

func (s *Session) removeDatagramAt(f *datagramFlow, index int) {
	d := s.dg
	s.settleFlowDelay(f)
	old := &f.queue[index]
	if old.reserved != nil {
		r := shaping.ReasonCancelled
		if old.deadline <= s.now {
			r = shaping.ReasonApplicationExpired
		}
		old.reserved.cancelNative(r)
	}
	size, deadline := old.data.cost(), old.deadline
	old.data.unref()
	old.handoff.datagramDone(false)
	f.bytes -= size
	d.sendBytes -= size
	d.sendCount--
	if index == 0 {
		*old = outDgram{}
		f.queue = f.queue[1:]
		s.routeDatagram(f)
		s.checkNativeCapacity(f)
	} else {
		copy(f.queue[index:], f.queue[index+1:])
		last := len(f.queue) - 1
		f.queue[last] = outDgram{}
		f.queue = f.queue[:last]
		if s.shp != nil {
			s.refreshFlowDelay(f)
		}
	}
	if f.expiry.index != 0 && deadline == f.expiry.at {
		s.refreshDatagramFlowExpiry(f)
	}
	if !s.DatagramQueueFull() {
		s.wakeSendRoom(1)
	}
}

func (d *dgramState) armDatagramExpiry(node *Deadline[datagramExpiry], owner datagramExpiry, at Time) {
	if at != Never && (node.index == 0 || at < node.at) {
		node.Value = owner
		d.expiry.Set(node, at)
	}
}

func (s *Session) refreshDatagramFlowExpiry(f *datagramFlow) {
	at := Never
	for i := range f.queue {
		at = min(at, f.queue[i].deadline)
	}
	s.dg.expiry.Set(&f.expiry, at)
}

func (s *Session) expireDatagramSends(now Time) {
	d := s.dg
	if d == nil {
		return
	}
	for timer := d.expiry.PopDue(now); timer != nil; timer = d.expiry.PopDue(now) {
		if f := timer.Value.flow; f != nil {
			for j, datagram := range slices.Backward(f.queue) {
				if datagram.deadline <= now {
					s.stats.DatagramDeadlineDropped++
					if j == 0 && len(f.queue) == 1 {
						s.dropDatagram(f)
					} else {
						s.removeDatagramAt(f, j)
						s.stats.DatagramsDroppedSend++
					}
				}
			}
			s.refreshDatagramFlowExpiry(f)
			continue
		}
		p := timer.Value.pipe
		for i, replica := range slices.Backward(p.datagramCopies) {
			if replica.deadline <= now {
				s.removeDatagramCopy(p, i)
				s.stats.DatagramCopyPathDropped++
			}
		}
		s.refreshDatagramCopyExpiry(p)
	}
}
