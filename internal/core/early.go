package core

import "github.com/zmuxio/zmux-go/internal/wire"

// earlyState is early data under an assumed parameter set (SPEC.md 6.1.2):
// the zmux floor, or parameters remembered for carrier early data.
type earlyState struct {
	assumed   wire.Params
	floor     bool // assumed set is the zmux floor
	fenceSent bool
	sentAny   bool // early STREAM frames were sent
	// carrier: the early streams travel in carrier early data (TLS 1.3 or
	// QUIC 0-RTT); every send stays provisional, and its data retained,
	// until the carrier accepted it or it was re-issued after the peer's
	// parameters.
	carrier bool
}

// holdSends reports whether sent stream data must be kept: carrier early
// data the carrier has not accepted yet.
func (s *Session) holdSends() bool { return s.early != nil && s.early.carrier || s.earlyHeld }

// CarrierEarlyAccepted reports the carrier's verdict "accepted": the early
// sends are final.
func (s *Session) CarrierEarlyAccepted() {
	s.earlyHeld, s.earlyAccepted = false, true
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
		s.wake(st.wsig)
		if sp.finSent && sp.state == sendDataSent && !sp.reset {
			s.sendTerminal(st, sendDataRecvd)
		}
	}
}

// RestartEarly restarts the bootstrap pipe after the carrier rejected its
// early data, on the byte stream the restart hook returned (SPEC.md 6.1.2,
// Outcome): QX_TRANSPORT_PARAMETERS first, and the early streams' data,
// FIN bits and resets after the peer's parameters. Nothing of the early
// carrier reached the peer. It reports false, changing nothing, when the
// session cannot restart: the peer's parameters already arrived on the
// early carrier (a verdict that contradicts the carrier) or pipe 0 is gone.
func (s *Session) RestartEarly(info CarrierInfo, now Time) bool {
	s.SetNow(now)
	p := s.pipe(0)
	if p == nil || s.peerSeen || s.state >= StateClosing {
		return false
	}
	sig := p.out.sig
	p.out = &outDir{lanes: []*outLane{{info: info, confirmed: true}}, sig: sig}
	p.out.lo.offered = info.Lanes
	p.in = &inDir{lanes: []*inLane{{info: info, expectTP: true}}}
	s.conn.sent = 0
	// The early sends are re-issued after the peer's parameters.
	s.earlyHeld = true
	s.early = nil
	s.peer = wire.Params{}
	s.conn.sendMax = 0
	for _, st := range s.streams {
		if !st.local || !st.hasS {
			continue
		}
		sp := &st.snd
		sp.sent = 0
		sp.finSent = false
		sp.maxData = 0
		if sp.state == sendDataSent {
			sp.state = sendSend
		}
		if sp.reset {
			sp.resetSent = false
			s.queueCtl(st, ctlReset)
		}
		st.opened = false
		s.sched.ensure(st)
	}
	s.wakePipe(p)
	return true
}

// FloorParams is the zmux floor every zmux endpoint advertises at least.
func FloorParams() wire.Params {
	return wire.Params{
		Has: wire.HasMaxData | wire.HasStreamsBidi | wire.HasBidiRemote | wire.HasPreamble |
			wire.HasPipeAck | wire.HasResetAt,
		InitialMaxData:       16384,
		MaxStreamsBidi:       4,
		MaxStreamDataBidiRem: 4096,
		Preamble:             1024,
		MaxRecordSize:        wire.DefaultMaxRecordSize,
	}
}

// earlyAllowed: the role must not depend on the peer (SPEC.md 6.1.2 rule 1).
func (s *Session) earlyAllowed() bool {
	l := &s.cfg.Local
	return s.cfg.Role != RoleAuto && (l.Has&wire.HasRole == 0 || l.Role != wire.RoleAuto)
}

// startEarly installs the assumed set before establishment.
func (s *Session) startEarly(assumed wire.Params, floor bool) {
	s.early = &earlyState{assumed: assumed, floor: floor}
	s.peer = assumed
	s.feat.Preamble = s.cfg.Local.Has&wire.HasPreamble != 0 && assumed.Has&wire.HasPreamble != 0
	s.conn.sendMax = assumed.InitialMaxData
	s.loc[0].limit = assumed.MaxStreamsBidi
	if !floor {
		s.loc[1].limit = assumed.MaxStreamsUni
	}
}

// earlyActive reports whether early data may be sent now.
func (s *Session) earlyActive() bool { return s.early != nil && !s.peerSeen }

func (s *Session) earlyWants() bool { return s.dataPending(nil) }

// writeEarly writes the fence and the early STREAM frames.
func (s *Session) writeEarly(w *recWriter) {
	if !s.dataPending(nil) {
		return
	}
	e := s.early
	if !e.fenceSent && e.assumed.IsZmux() {
		if !w.frame(9, false, func(b []byte) []byte {
			b = wire.AppendPipeAckHeader(b, 1)
			return wire.AppendAckEntry(b, wire.AckEntry{})
		}) {
			return
		}
		e.fenceSent = true
	}
	s.writeData(w, nil, nil)
}

// earlyOutcome decides the fate of early data when the peer's parameters
// arrive (SPEC.md 6.1.2 Outcome).
func (s *Session) earlyOutcome() *wire.Error {
	e := s.early
	p := &s.peer
	if e == nil && s.earlyHeld {
		// Re-issued after a rejection: what follows the peer's
		// parameters is final.
		s.earlyHeld = false
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
		s.earlyHeld = !s.earlyAccepted
	}
	if !e.sentAny {
		s.early = nil
		s.earlyAdopt()
		return nil
	}
	acked := s.feat.Acked()
	if e.assumed.IsZmux() && !acked || e.floor && p.MaxStreamsBidi == 0 {
		s.early = nil
		s.closeWith(&SessionError{Kind: CloseEarlyRejected}, &closeFrame{code: wire.NoError})
		return nil
	}
	a := &e.assumed
	below := p.InitialMaxData < a.InitialMaxData || p.MaxStreamsBidi < a.MaxStreamsBidi ||
		p.MaxStreamDataBidiRem < a.MaxStreamDataBidiRem || p.MaxRecordSize < a.MaxRecordSize
	if a.Has&wire.HasPreamble != 0 && (p.Has&wire.HasPreamble == 0 || p.Preamble < a.Preamble) {
		below = true
	}
	if a.Has&wire.HasPipeAck != 0 && p.Has&wire.HasPipeAck == 0 ||
		a.Has&wire.HasResetAt != 0 && p.Has&wire.HasResetAt == 0 {
		below = true
	}
	s.early = nil
	if below {
		return wire.ErrParam("peer parameters below the assumed set")
	}
	s.earlyAdopt()
	return nil
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

// CarrierHandshakeDone ends the replayable period.
func (s *Session) CarrierHandshakeDone() {
	s.carrierEarlyIn = false
	for _, st := range s.streams {
		st.replayable = false
	}
}
