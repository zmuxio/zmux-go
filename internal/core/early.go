package core

import "github.com/zmuxio/zmux-go/internal/wire"

// earlyState holds data sent under remembered plain QMux parameters.
type earlyState struct {
	assumed wire.Params
	sentAny bool // early STREAM frames were sent
	// carrier: the early streams travel in carrier early data (TLS 1.3 or
	// QUIC 0-RTT); every send stays provisional, and its data retained,
	// until the carrier accepted it or it was re-issued after the peer's
	// parameters.
	carrier bool
}

// holdSends reports whether sent stream data must be kept: carrier early
// data the carrier has not accepted yet.
func (s *Session) holdSends() bool { return s.early != nil && s.early.carrier || s.earlyHold != nil }

// earlyHold is a hold of the provisional sends once the early flight left
// s.early: waiting for the carrier's verdict, or (restart) for the peer's
// parameters after a rejected carrier restarted them, with the set they
// were made under when any went out.
type earlyHold struct {
	assumed wire.Params
	check   bool // a restart's: the re-issued sends took identifiers and preambles under assumed
}

// CarrierEarlyAccepted reports the carrier's verdict "accepted": the early
// sends are final.
func (s *Session) CarrierEarlyAccepted() {
	s.earlyHold, s.earlyAccepted = nil, true
	s.shapingEarn(s.earlyEarn)
	s.finishEarlyShape(true)
	s.earlyEarn = 0
	if s.early != nil {
		s.early.carrier = false
	}
	s.releaseProvisional()
}

// releaseProvisional releases the data that provisional sends retained.
func (s *Session) releaseProvisional() {
	if s.holdSends() || s.feat.Multipath {
		return
	}
	for _, st := range s.streams {
		if !st.local || !st.hasS {
			continue
		}
		sp := &st.snd
		h := s.heldEnter(st)
		sp.q.release(min64(sp.sent, sp.q.end))
		s.heldLeave(st, h)
		s.sendChanged(st)
		if sp.finSent && sp.state == sendDataSent && !sp.reset {
			s.sendTerminal(st, sendDataRecvd)
		}
	}
}

// EarlyDiscarded records that the carrier discarded what pipe 0 wrote
// (a rejected first flight) when no restart follows: the parameters lead
// pipe 0's next record again, so that a CONNECTION_CLOSE written there is
// one the peer can read. Nothing changes once the peer's parameters
// arrived (the peer saw the flight).
func (s *Session) EarlyDiscarded() {
	s.earlyEarn = 0
	s.finishEarlyShape(false)
	p := s.pipe(0)
	if p == nil || s.peerSeen || p.out == nil {
		return
	}
	if l := p.out.lane(0); l != nil {
		l.tpSent = false
	}
}

// RestartEarly restarts the bootstrap pipe after the carrier rejected its
// early data, on the byte stream the restart hook returned:
// QX_TRANSPORT_PARAMETERS first, and the early streams' data,
// FIN bits and resets after the peer's parameters. Nothing of the early
// carrier reached the peer. It reports false, changing nothing, when the
// session cannot restart: the peer's parameters already arrived on the
// early carrier (a verdict that contradicts the carrier) or pipe 0 is gone.
func (s *Session) RestartEarly(info CarrierInfo, sig *WriteSignal, replaced bool, now Time) bool {
	s.SetNow(now)
	p := s.pipe(0)
	if p == nil || s.peerSeen || s.state >= StateClosing {
		return false
	}
	if replaced {
		p.conn, p.chSig = nil, nil
		s.replaceDatagramPath(p)
	} else if p.out != nil {
		old := p.out.lane(0).info.CarrierIdentity
		if old != nil && info.CarrierIdentity != nil && old != info.CarrierIdentity {
			s.replaceDatagramPath(p)
		}
	}
	p.out = &outDir{lanes: []*outLane{{info: info, confirmed: true}}, sig: sig}
	p.out.lo.offered = info.Lanes
	p.in = &inDir{lanes: []*inLane{{info: info, expectTP: true}}}
	s.earlyEarn = 0
	s.finishEarlyShape(false)
	s.conn.sent = 0
	// The early sends are re-issued after the peer's parameters, with the
	// identifiers and preambles they took under the assumed set.
	h := &earlyHold{}
	if e := s.early; e != nil && e.sentAny {
		h.assumed, h.check = e.assumed, true
	}
	s.earlyHold = h
	s.early = nil
	s.peer = wire.Params{}
	s.conn.sendMax = 0
	for _, st := range s.streams {
		if !st.local || !st.hasS {
			continue
		}
		sp := &st.snd
		sp.sent = 0
		s.syncUnsent(st)
		sp.finSent = false
		sp.maxData = 0
		if sp.state == sendDataSent {
			sp.state = sendSend
		}
		// Not opened any more: the re-issue carries the opening frame
		// again; queued control frames do not wait for it (ctlWaits).
		st.opened = false
		if sp.reset {
			sp.resetSent = false
			s.queueCtl(st, ctlReset)
		}
		if x := st.ext; x != nil && x.reqPrio {
			// A PRIORITY_UPDATE written on the early carrier goes again.
			s.queueCtl(st, ctlPrio)
		}
		if r := &st.rcv; st.hasR {
			// Nothing of the early carrier reached the peer: a
			// STOP_SENDING or window update in it goes again.
			if r.stopped && r.stopSent {
				r.stopSent = false
				s.queueCtl(st, ctlStop)
			}
			if r.adv > s.cfg.Local.MaxStreamDataBidiLoc {
				st.forceMSD = true
				s.queueCtl(st, ctlMaxStreamData)
			}
		}
		s.sched.ensure(st)
	}
	s.wakePipe(p)
	return true
}

// earlyAllowed: the role must not depend on the peer.
func (s *Session) earlyAllowed() bool {
	l := &s.cfg.Local
	return s.cfg.Role != RoleAuto && (l.Has&wire.HasRole == 0 || l.Role != wire.RoleAuto)
}

// startEarly installs the assumed set before establishment.
func (s *Session) startEarly(assumed wire.Params) {
	s.early = &earlyState{assumed: assumed, carrier: true}
	s.peer = assumed
	s.conn.sendMax = assumed.InitialMaxData
	s.loc[0].limit, s.loc[1].limit = assumed.MaxStreamsBidi, assumed.MaxStreamsUni
}

// earlyActive reports whether early data may be sent now.
func (s *Session) earlyActive() bool { return s.early != nil && !s.peerSeen }

func (s *Session) earlyWants() bool { return s.dataPendingOn(nil, nil) }

// writeEarly writes the plain QMux STREAM data in a carrier's 0-RTT.
func (s *Session) writeEarly(w *recWriter) { s.writeData(w, nil, nil) }

// earlyOutcome decides the fate of early data when the peer's parameters
// arrive.
func (s *Session) earlyOutcome() *wire.Error {
	e := s.early
	p := &s.peer
	if h := s.earlyHold; e == nil && h != nil {
		// Re-issued after a rejection: what follows the peer's
		// parameters is final, once they admit what the sends assumed.
		s.earlyHold = nil
		if h.check && (p.IsZmux() || assumedBelow(&h.assumed, p)) {
			return wire.ErrParam("peer parameters below the assumed set")
		}
		s.earlyAdopt()
		return nil
	}
	if e == nil {
		return nil
	}
	if e.carrier {
		// Carrier early data: the carrier's verdict decides; the
		// remembered parameters only bound what was sent.
		e.carrier = false
		if !s.earlyAccepted {
			s.earlyHold = &earlyHold{} // the verdict still decides
		}
	}
	if !e.sentAny {
		s.early = nil
		s.earlyAdopt()
		return nil
	}
	if p.IsZmux() {
		return wire.ErrParam("plain QMux early data sent to a zmux peer")
	}
	s.early = nil
	if assumedBelow(&e.assumed, p) {
		return wire.ErrParam("peer parameters below the assumed set")
	}
	s.earlyAdopt()
	return nil
}

// assumedBelow reports whether the peer's parameters p admit less than the
// set a the early sends assumed.
func assumedBelow(a, p *wire.Params) bool {
	below := p.InitialMaxData < a.InitialMaxData || p.MaxStreamsBidi < a.MaxStreamsBidi ||
		p.MaxStreamDataBidiRem < a.MaxStreamDataBidiRem || p.MaxRecordSize < a.MaxRecordSize ||
		p.MaxStreamsUni < a.MaxStreamsUni || p.MaxStreamDataUni < a.MaxStreamDataUni
	if a.Has&wire.HasPreamble != 0 && (p.Has&wire.HasPreamble == 0 || p.Preamble < a.Preamble) {
		below = true
	}
	if a.Has&wire.HasPipeAck != 0 && p.Has&wire.HasPipeAck == 0 ||
		a.Has&wire.HasResetAt != 0 && p.Has&wire.HasResetAt == 0 {
		below = true
	}
	return below
}

// earlyAdopt raises the early streams' credit to the peer's values.
func (s *Session) earlyAdopt() {
	for _, st := range s.streams {
		if st.local && st.hasS {
			v := s.peer.MaxStreamDataUni
			if st.bidi {
				v = s.peer.MaxStreamDataBidiRem
			}
			if v > st.snd.maxData {
				st.snd.maxData = v
				s.sched.ensure(st)
			}
		}
	}
}

// CarrierEarlyIn marks a server whose bootstrap carrier accepted carrier
// early data: streams the peer opens are replayable until the carrier's
// handshake completes (CarrierHandshakeDone).
func (s *Session) CarrierEarlyIn() { s.carrierEarlyIn = true }

// CarrierHandshakeDone ends the replayable period: streams opened later
// are not replayable, while a stream that arrived in early data stays so
// (its request may have been repeated whatever followed).
func (s *Session) CarrierHandshakeDone() { s.carrierEarlyIn = false }
