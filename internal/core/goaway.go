package core

import "github.com/zmuxio/zmux-go/internal/wire"

// goawayState is EXTENSIONS.md 3.
type goawayState struct {
	reason        []byte // of the final CONNECTION_CLOSE
	sentWater     [2]uint64
	code          uint64
	phase         int // 1: phase 1 sent, fence pending; 2: final
	recvWater     [2]uint64
	recvCode      uint64
	maxOffered    [2]uint64 // highest offered peer index + 1
	recheck       bool      // a stream was discarded during a shutdown
	sent          bool
	received      bool
	closeWhenDone bool
}

// Shutdown starts a graceful close: two-phase GOAWAY when it is in use,
// then CONNECTION_CLOSE with code and reason once no stream remains.
func (s *Session) Shutdown(code uint64, reason []byte) {
	if s.isClosing() {
		return
	}
	g := &s.ga
	g.closeWhenDone = true
	for i := range s.openW {
		for _, w := range s.openW[i] {
			s.wake(w)
		}
		s.openW[i] = s.openW[i][:0]
	}
	g.code, g.reason = code, reason
	if !s.feat.GoAway || !s.peerSeen {
		s.checkShutdownDone()
		return
	}
	if !g.sent {
		g.sent = true
		g.phase = 1
		g.sentWater = [2]uint64{s.rem[0].limit, s.rem[1].limit}
		s.pend.goaway = true
		s.ping.want = true
		s.ping.fence = s.ping.next + 1
		if s.state == StateActive {
			s.state = StateDraining
		}
		s.wakeAllWriters()
	}
	s.checkShutdownDone()
}

// goawaySent runs when a GOAWAY frame left.
func (s *Session) goawaySent() {}

// fenceDone sends the final GOAWAY (EXTENSIONS.md 3.3 phase 2).
func (s *Session) fenceDone() {
	g := &s.ga
	s.ping.fence = 0
	if g.phase != 1 {
		return
	}
	// With multipath the wait would include SRTT as well; it is
	// approximated by sending at once.
	g.phase = 2
	w := [2]uint64{g.maxOffered[0], g.maxOffered[1]}
	lower := false
	for t := range 2 {
		if w[t] < g.sentWater[t] {
			g.sentWater[t] = w[t]
			lower = true
		}
	}
	if lower {
		s.pend.goaway = true
		s.wakeAllWriters()
	}
	// Streams at or above the new watermark that exist are rejected.
	for _, st := range s.streams {
		if !st.local && !st.offered && !st.stackReject && st.id>>2 >= g.sentWater[typeIndex(st.bidi)] {
			s.stackRejectStream(st)
		}
	}
	s.checkShutdownDone()
}

// SettlePending reports work for Settle.
func (s *Session) SettlePending() bool { return s.ga.recheck }

// Settle completes what an operation left pending; the driver calls it at
// the end of a critical section when SettlePending. It closes a
// shutting-down session whose last stream the operation discarded.
func (s *Session) Settle() {
	if s.ga.recheck {
		s.ga.recheck = false
		s.checkShutdownDone()
	}
}

// checkShutdownDone closes once no stream remains after Shutdown.
func (s *Session) checkShutdownDone() {
	if !s.ga.closeWhenDone || s.isClosing() || len(s.streams) > 0 || len(s.provisional) > 0 || s.ga.phase == 1 {
		return
	}
	s.Close(s.ga.code, s.ga.reason)
}

// onGoAway processes a received GOAWAY (EXTENSIONS.md 3.5).
func (s *Session) onGoAway(f *wire.Frame) {
	g := &s.ga
	lowered := false
	if !g.received {
		g.received = true
		g.recvWater = [2]uint64{f.MaxBidi, f.MaxUni}
		lowered = true
	} else {
		if f.MaxBidi < g.recvWater[0] {
			g.recvWater[0], lowered = f.MaxBidi, true
		}
		if f.MaxUni < g.recvWater[1] {
			g.recvWater[1], lowered = f.MaxUni, true
		}
	}
	if !lowered {
		if !s.feat.Multipath {
			s.charge()
		}
		return
	}
	g.recvCode = f.Code
	if s.state == StateActive {
		s.state = StateDraining
	}
	for _, st := range s.allLocalStreams() {
		if !st.idSet {
			// Neither emitted nor pinned: fails with "going away".
			st.snd.errState = &StreamError{Kind: StreamGoingAway}
			st.rcv.errState = st.snd.errState
			s.cancelUnopened(st)
			s.wake(st.wsig)
			s.wake(st.rsig)
			continue
		}
		t := typeIndex(st.bidi)
		if st.id>>2 >= g.recvWater[t] {
			e := &StreamError{Kind: StreamRejected, StreamID: st.id}
			if st.hasS && st.snd.errState == nil {
				st.snd.errState = e
			}
			if st.hasR && st.rcv.errState == nil {
				st.rcv.errState = e
			}
			s.wake(st.wsig)
			s.wake(st.rsig)
		}
	}
	for t := range 2 {
		for _, w := range s.openW[t] {
			s.wake(w)
		}
		s.openW[t] = nil
	}
	s.emit(Event{Kind: EvGoAwayReceived, MaxBidi: g.recvWater[0], MaxUni: g.recvWater[1], Code: f.Code})
}

// allLocalStreams returns local streams, including those without an ID.
func (s *Session) allLocalStreams() []*Stream {
	var out []*Stream
	for _, st := range s.streams {
		if st.local {
			out = append(out, st)
		}
	}
	out = append(out, s.provisional...)
	return out
}

// onPriorityUpdate processes PRIORITY_UPDATE (EXTENSIONS.md 5.4).
func (s *Session) onPriorityUpdate(f *wire.Frame) *wire.Error {
	id := f.StreamID
	if !isBidiID(id) && !isLocalID(id, s.role) {
		return wire.NewError(wire.StreamStateError, "PRIORITY_UPDATE for the sender's unidirectional stream", "stream", id)
	}
	st, e := s.lookupStream(id, isBidiID(id))
	if e != nil {
		return e
	}
	if st == nil || !st.hasS {
		return nil
	}
	if st.snd.state == sendDataRecvd || st.snd.state == sendResetRecvd {
		return nil
	}
	x := st.x()
	if f.Seq <= x.prioSeqApplied && (f.Seq > 0 || x.prioApplied) {
		if !s.feat.Multipath {
			s.charge()
		}
		return nil
	}
	x.prioSeqApplied, x.prioApplied = f.Seq, true
	s.SetPriority(st, f.Urgency, f.Incr)
	return nil
}

// PeerGoAway returns the received GOAWAY watermarks.
func (s *Session) PeerGoAway() (struct{ MaxBidi, MaxUni, Code uint64 }, bool) {
	g := &s.ga
	return struct{ MaxBidi, MaxUni, Code uint64 }{g.recvWater[0], g.recvWater[1], g.recvCode}, g.received
}
