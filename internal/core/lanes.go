package core

import (
	"crypto/subtle"
	"slices"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// Lanes (MULTIPATH.md 2.5, 5.8). A pipe whose carrier is
// a stream of a multi-stream connection may carry its records over several
// streams of that connection. The outbound side opens lanes on demand and
// binds stream directions to them; the inbound side attaches the lanes the
// peer opens, routed by the Lane Key.

// laneOut is the lane state of an outbound pipe.
type laneOut struct {
	next      uint64 // next lane number
	pending   *outLane
	copyLane  *outLane // the copy lane, nil when none
	offered   bool     // the carrier offers lanes
	requested bool     // a lane open is pending at the driver
	refused   bool
}

// laneLimit is the number of lanes the peer accepts on our pipes.
func (s *Session) laneLimit() uint64 {
	if !s.feat.LanesOut {
		return 1
	}
	return s.peer.MaxLanes
}

// OutLanes returns the open outbound lanes of a pipe, lane 0 first.
func (s *Session) OutLanes(pipe uint64, dst []uint64) []uint64 {
	p := s.pipe(pipe)
	if p == nil || p.out == nil {
		return dst
	}
	for _, l := range p.out.lanes {
		if !l.refused {
			dst = append(dst, l.lane)
		}
	}
	return dst
}

// laneFor returns the lane a stream direction's next chunk would go on,
// without changing anything; needNew reports that every data lane is
// taken, so a new lane would be useful.
func (s *Session) laneFor(p *Pipe, st *Stream) (l *outLane, keep bool, needNew bool) {
	sp := &st.snd
	d := p.out
	if sp.laneBound && sp.lanePipe == p.id {
		if l := d.lane(sp.lane); l != nil && !l.refused && l.acked < sp.laneEnd {
			return l, true, false
		}
	}
	lane0 := d.lanes[0]
	if !d.lo.offered || s.onLane0(st) || s.laneLimit() <= 1 {
		return lane0, false, false
	}
	var best *outLane
	free := false
	for _, x := range d.lanes[1:] {
		if !x.confirmed || x.refused || x == d.lo.copyLane {
			continue
		}
		bound := len(x.boundSt)
		if sp.laneBound && sp.lanePipe == p.id && sp.lane == x.lane {
			bound--
		}
		if bound == 0 {
			free = true
		}
		if best == nil || bound < len(best.boundSt) || bound == len(best.boundSt) && x.pos-x.acked < best.pos-best.acked {
			best = x
		}
	}
	if best == nil {
		best = lane0
	}
	return best, false, !free
}

// bindLane binds the direction to the lane laneFor picks.
func (s *Session) bindLane(p *Pipe, st *Stream) *outLane {
	l, keep, needNew := s.laneFor(p, st)
	if keep {
		return l
	}
	s.unbindLane(st)
	if needNew {
		s.wantLane(p)
	}
	s.bind(p, st, l)
	return l
}

// onLane0 reports a stream direction that lane 0 carries: the redundant ones in
// multipath, urgency 0 to 2 without.
func (s *Session) onLane0(st *Stream) bool {
	if s.feat.Multipath {
		return s.redundant(st)
	}
	return st.urgency <= 2
}

func (s *Session) bind(p *Pipe, st *Stream, l *outLane) {
	st.snd.lanePipe = p.id
	st.snd.lane = l.lane
	st.snd.laneBound = true
	l.boundSt = append(l.boundSt, st)
}

// unbindLane drops a stream direction's lane binding, on whatever pipe.
func (s *Session) unbindLane(st *Stream) {
	sp := &st.snd
	if !sp.laneBound {
		return
	}
	sp.laneBound = false
	p := s.pipe(sp.lanePipe)
	if p == nil || p.out == nil {
		return
	}
	if l := p.out.lane(sp.lane); l != nil {
		for i, x := range l.boundSt {
			if x == st {
				last := len(l.boundSt) - 1
				l.boundSt[i] = l.boundSt[last]
				l.boundSt[last] = nil
				l.boundSt = l.boundSt[:last]
				break
			}
		}
	}
}

// wantLane asks the driver for a new lane when the rules allow one.
func (s *Session) wantLane(p *Pipe) {
	d := p.out
	lo := &d.lo
	if lo.refused || lo.requested || lo.pending != nil || !lo.offered {
		return
	}
	if d.lanes[0].acked == 0 || uint64(len(d.lanes)) >= s.laneLimit() || s.state >= StateClosing {
		return
	}
	lo.requested = true
	s.act(Action{Kind: ActOpenLane, Pipe: p.id})
}

// LaneOpened attaches the stream the driver opened for a requested lane
// and returns its lane number.
// sig wakes the lane's writer.
func (s *Session) LaneOpened(pipe uint64, info CarrierInfo, sig *Signal) (uint64, bool) {
	p := s.pipe(pipe)
	if p == nil || p.out == nil || s.state >= StateClosing {
		return 0, false
	}
	lo := &p.out.lo
	lo.requested = false
	if lo.refused {
		return 0, false
	}
	if lo.next == 0 {
		lo.next = 1
	}
	l := &outLane{lane: lo.next, info: info, laneFirst: true, openedAt: s.now, sig: sig}
	lo.next++
	lo.pending = l
	p.out.lanes = append(p.out.lanes, l)
	s.wakePipe(p)
	return l.lane, true
}

// LaneOpenFailed reports that a requested lane could not be opened now.
func (s *Session) LaneOpenFailed(pipe uint64) {
	if p := s.pipe(pipe); p != nil && p.out != nil {
		p.out.lo.requested = false
	}
}

// writeLaneFirst writes the PIPE_LANE record of a new lane.
func (s *Session) writeLaneFirst(w *recWriter, p *Pipe, ol *outLane) {
	key := s.peer.LaneKey[:]
	n := wire.VarintLen(wire.TypePipeLane) + 16 + wire.VarintLen(p.id) + wire.VarintLen(ol.lane)
	if !w.frame(n, true, func(b []byte) []byte { return wire.AppendPipeLane(b, key, p.id, ol.lane) }) {
		return
	}
	ol.laneFirst = false
	w.end()
	w.closed = true // nothing else until the lane is confirmed
	w.laneOnly = true
}

// laneConfirmed marks a lane confirmed by a PIPE_ACK above 0.
func (s *Session) laneConfirmed(p *Pipe, ol *outLane) {
	if ol.confirmed || ol.lane == 0 {
		return
	}
	ol.confirmed = true
	if p.out.lo.pending == ol {
		p.out.lo.pending = nil
	}
	s.wakeAllWriters()
}

// laneRefused handles a lane that ended before it was confirmed.
func (s *Session) laneRefused(p *Pipe, ol *outLane) {
	ol.refused = true
	p.out.lo.refused = true
	if p.out.lo.pending == ol {
		p.out.lo.pending = nil
	}
	s.act(Action{Kind: ActResetLane, Pipe: p.id, Lane: ol.lane})
}

// lanesAcked runs after an acknowledgement advance on a lane.
func (s *Session) lanesAcked(p *Pipe, ol *outLane) {
	if ol.acked > 0 {
		s.laneConfirmed(p, ol)
	}
	// A direction stays bound until its data on the lane is acknowledged.
	for i := 0; i < len(ol.boundSt); {
		if st := ol.boundSt[i]; st.snd.laneEnd <= ol.acked {
			s.unbindLane(st)
			continue
		}
		i++
	}
	if s.closeFr.waitLanes && s.lanesCovered() {
		s.closeFr.waitLanes = false
		s.wakeAllWriters()
	}
	s.wakeAllWriters()
}

// lanesCovered reports whether every record written on lanes other than 0
// is acknowledged (SPEC.md 6.5 step 1).
func (s *Session) lanesCovered() bool {
	for _, p := range s.pipes {
		if p.out == nil {
			continue
		}
		for _, l := range p.out.lanes[1:] {
			if !l.refused && l.acked < l.pos {
				return false
			}
		}
	}
	return true
}

// ---- inbound lanes ----

// laneTimeouts refuses lanes not confirmed within the handshake timeout.
func (s *Session) laneTimeouts(now Time, apply bool) Time {
	t := Never
	for _, p := range s.pipes {
		if p.out == nil || p.out.lo.pending == nil {
			continue
		}
		l := p.out.lo.pending
		d := l.openedAt.Add(s.cfg.HandshakeTimeout)
		if apply && now >= d {
			s.laneRefused(p, l)
			continue
		}
		t = minT(t, d)
	}
	return t
}

// LaneKey returns the session's own Lane Key (what PIPE_LANE toward it
// carries), and whether lanes are available toward it.
func (s *Session) LaneKey() ([16]byte, bool) {
	return s.cfg.Local.LaneKey, s.cfg.Local.Has&wire.HasLanes != 0
}

// LaneKeyMatches compares a key in constant time.
func (s *Session) LaneKeyMatches(key []byte) bool {
	k := s.cfg.Local.LaneKey
	return len(key) == 16 && subtle.ConstantTimeCompare(k[:], key) == 1
}

// SetCarrierConn records the carrier connection of a pipe's lane 0.
func (s *Session) SetCarrierConn(pipe uint64, conn any) {
	if p := s.pipe(pipe); p != nil {
		p.conn = conn
	}
}

// AcceptLane attaches an inbound lane whose first record starts with a
// PIPE_LANE naming (pipe, lane) and carrying this session's key. It returns
// false when the stream must be aborted without any effect (MULTIPATH.md
// 5.8 reader rule 1); protocol violations close the session.
func (s *Session) AcceptLane(conn any, pipe, lane uint64, info CarrierInfo) bool {
	if s.state >= StateClosing || !s.peerSeen {
		return false
	}
	p := s.pipe(pipe)
	if p == nil || p.in == nil || p.in.ended || p.conn == nil || p.conn != conn {
		return false
	}
	if !s.feat.LanesIn {
		s.failLocal(&wire.Error{Code: wire.FrameEncodingError, FrameType: wire.TypePipeLane,
			Reason: "PIPE_LANE without lanes in use"})
		return false
	}
	if lane == 0 || lane >= s.cfg.Local.MaxLanes || p.in.lane(lane) != nil {
		s.failLocal(wire.NewError(wire.ProtocolViolation, "invalid lane", "pipe", pipe, "value", lane))
		return false
	}
	p.in.lanes = append(p.in.lanes, &inLane{lane: lane, info: info, expectLane: true})
	return true
}

// dataTurn reports whether data lane ol may take new stream data from the
// pipe's paced room now: lane 0 and the copy
// lane always; a data lane once per round while another data lane that
// asked has not had its turn, so a lane bound later is not starved by the
// lanes whose writers come first.
func (s *Session) dataTurn(p *Pipe, ol *outLane) bool {
	if ol.lane == 0 || ol == p.out.lo.copyLane || !ol.served {
		return true
	}
	for _, x := range p.out.lanes[1:] {
		if x != ol && x.waiting && !x.served {
			ol.waiting = true
			return false
		}
	}
	for _, x := range p.out.lanes[1:] { // everyone had a turn: a new round
		x.served = false
	}
	return true
}

// tookTurn records a data batch of ol and wakes the lanes waiting for
// their turn.
func (s *Session) tookTurn(p *Pipe, ol *outLane) {
	if ol.lane == 0 || ol == p.out.lo.copyLane {
		return
	}
	ol.served, ol.waiting = true, false
	for _, x := range p.out.lanes[1:] {
		if x.waiting {
			s.wake(x.sig)
		}
	}
}

// copyLaneOf returns p's copy lane: the
// lowest-numbered confirmed lane above 0 with no bound stream direction,
// kept while it serves; when there is none a lane is requested and nil
// returned.
func (s *Session) copyLaneOf(p *Pipe) *outLane {
	lo := &p.out.lo
	if !lo.offered || s.laneLimit() <= 1 {
		return nil
	}
	if c := lo.copyLane; c != nil && !c.refused && !c.shut && slices.Contains(p.out.lanes, c) {
		return c
	}
	lo.copyLane = nil
	for _, x := range p.out.lanes[1:] {
		if x.confirmed && !x.refused && !x.shut && len(x.boundSt) == 0 {
			lo.copyLane = x
			return x
		}
	}
	s.wantLane(p)
	return nil
}
