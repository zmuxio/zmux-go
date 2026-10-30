package core

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// State is the session state (STATE_MACHINE.md 4.1).
type State uint8

const (
	StateEstablishing State = iota
	StateActive
	StateDraining
	StateSuspended
	StateClosing
	StateClosed
)

var stateNames = [...]string{"ESTABLISHING", "ACTIVE", "DRAINING", "SUSPENDED", "CLOSING", "CLOSED"}

func (s State) String() string { return stateNames[s] }

// Features are the features in use (SPEC.md 5.5).
type Features struct {
	Role, GoAway, Preamble, Priority, Flows, Multipath, PipeAck, Resume bool
	// LanesOut: lanes available toward the peer (we may open them).
	LanesOut bool
	// LanesIn: lanes available toward us (the peer may open them).
	LanesIn bool
	// JoinSecurityUnavailable: both offered multipath, no secret source.
	JoinSecurityUnavailable bool
}

// Acked reports whether the session is a session with acknowledgement.
func (f *Features) Acked() bool { return f.Multipath || f.PipeAck }

// Session is the sans-IO state of one zmux session.
type Session struct {
	cfg   Config
	now   Time
	start Time
	state State

	role      Role // resolved role (initiator/responder)
	roleKnown bool
	peer      wire.Params
	peerSeen  bool
	feat      Features
	gates     wire.Gates
	peerZmux  bool

	// Peer limits for sending.
	peerMaxRecord uint64
	peerMaxDgram  uint64

	streams           map[uint64]*Stream
	loc               [2]localCount // [bidi, uni]
	rem               [2]remoteCount
	acceptQ           [2]streamFIFO
	acceptW           [2][]*Signal
	openW             [2][]*Signal
	provisional       []*Stream // locally opened, not yet given an ID
	sched             scheduler
	ctlHead           *Stream
	ctlTail           *Stream
	conn              connFlow
	pend              pendingCtl
	ping              pingState
	ga                goawayState
	dg                *dgramState
	budget            budget
	early             *earlyState
	pipes             []*Pipe    // pipe 0 first
	mp                *multiPath // nil unless multipath is in use
	ack               *ackState
	timers            timers
	err               *SessionError
	closeFr           closeFrame
	events            []Event
	evHead            int
	wakes             []*Signal
	actions           []Action
	closedW           []*Signal
	estW              []*Signal
	bcast             *Signal // session-level broadcast for waits that watch state
	stats             Stats
	greaseFirst       bool
	closedEmitted     bool
	openLocalBidiRecv uint64
	retiredPipes      []uint64
	preSuspend        State
	tn                tuneState
	rxPipe            *Pipe  // the pipe of the record being processed
	initConnWin       uint64 // the initial session window
	charged           uint64 // the charge on cfg.Budget
	earlyHeld         bool   // provisional sends wait for the carrier's verdict
	earlyAccepted     bool
	carrierEarlyIn    bool // the bootstrap carrier delivers early data not yet confirmed by its handshake
	capWaiters        []*Stream
	q                 sessQuality
	copyMaxData       bool
}

// Stats are exact counters of the session.
type Stats struct {
	BytesSent, BytesReceived     uint64
	RecordsSent, RecordsReceived uint64
	StreamsOpenedLocal           uint64
	StreamsOpenedRemote          uint64
	DatagramsSent                uint64
	DatagramsReceived            uint64
	DatagramsDroppedSend         uint64
	DatagramsDroppedRecv         uint64
	AppBytesWritten              uint64
	AppBytesRead                 uint64
	BytesRedundant               uint64
	BytesOpportunistic           uint64
	DatagramsDroppedLimit        uint64
}

type localCount struct {
	next     uint64 // next stream index to allocate
	reserved uint64 // streams opened by the application (credit reserved)
	limit    uint64 // peer's MAX_STREAMS
	blocked  uint64 // limit at which STREAMS_BLOCKED was sent (+1)
}

type remoteCount struct {
	opened      uint64 // highest opened index + 1
	limit       uint64 // advertised MAX_STREAMS
	initial     uint64 // concurrency (initial limit of SPEC 8.5 rule 3)
	discarded   uint64
	staged      bool // uni credit not yet raised (SPEC 8.5 rule 4)
	blockedSeen uint64
}

type pendingCtl struct {
	maxData        bool
	maxStreams     [2]bool
	streamsBlocked [2]bool
	dataBlocked    bool
	goaway         bool
	forceMaxData   bool
}

type closeFrame struct {
	code      uint64
	frameType uint64
	reason    []byte
	pending   bool
	app       bool
	sentAll   bool
	waitLanes bool // wait until lanes are covered (SPEC.md 6.5 step 1)
}

// New creates a session. The bootstrap pipe is attached with AttachBootstrap.
func New(cfg Config, now Time) *Session {
	s := &Session{cfg: cfg, now: now, start: now, streams: make(map[uint64]*Stream)}
	if s.cfg.Local.MaxRecordSize == 0 {
		s.cfg.Local.MaxRecordSize = wire.DefaultMaxRecordSize
	}
	s.peerMaxRecord = wire.DefaultMaxRecordSize
	l := &s.cfg.Local
	s.rem[0] = remoteCount{limit: l.MaxStreamsBidi, initial: l.MaxStreamsBidi}
	s.rem[1] = remoteCount{limit: l.MaxStreamsUni, initial: l.MaxStreamsUni, staged: cfg.StagedUni}
	s.conn.initRecv(l.InitialMaxData)
	s.initConnWin = l.InitialMaxData
	if cfg.Role != RoleAuto {
		s.role, s.roleKnown = cfg.Role, true
	} else if l.Has&wire.HasRole == 0 {
		s.role, s.roleKnown = cfg.TransportRole, true
	}
	s.gates.ResetStreamAt = l.Has&wire.HasResetAt != 0
	if l.Has&wire.HasDatagram != 0 {
		s.gates.MaxDatagram = l.MaxDatagramFrameSize
	}
	s.ping.next = cfg.Test.PingSeqStart
	if cfg.Test.Enabled {
		s.greaseFirst = cfg.Test.GreaseTLV != nil
	} else {
		s.greaseFirst = true
	}
	s.budget.tokens = budgetCap
	s.budget.last = now
	s.timers = timers{handshake: now.Add(cfg.HandshakeTimeout), linger: Never, grace: Never}
	switch {
	case cfg.EarlyRemembered != nil && cfg.EarlyReplayable && s.earlyAllowed():
		s.startEarly(*cfg.EarlyRemembered, false)
		s.early.carrier = true
	case cfg.EarlyZmuxPeer && s.earlyAllowed():
		s.startEarly(FloorParams(), true)
	}
	return s
}

// randUint64 returns a random value (nonces, GREASE).
func randUint64() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint64(b[:])
}

// SetNow advances the session clock for API calls; the driver calls it
// with the current time before every API operation.
func (s *Session) SetNow(now Time) {
	if now > s.now {
		s.now = now
	}
}

// Now returns the session's clock.
func (s *Session) Now() Time { return s.now }

// State returns the session state.
func (s *Session) State() State { return s.state }

// Err returns the terminal error, nil while open.
func (s *Session) Err() *SessionError { return s.err }

// Features returns the features in use (valid once established).
func (s *Session) Features() Features { return s.feat }

// Role returns the resolved role and whether it is known.
func (s *Session) Role() (Role, bool) { return s.role, s.roleKnown }

// PeerParams returns the peer's parameters once established.
func (s *Session) PeerParams() (*wire.Params, bool) { return &s.peer, s.peerSeen }

// LocalParams returns what the endpoint sends.
func (s *Session) LocalParams() *wire.Params { return &s.cfg.Local }

// Config returns the configuration.
func (s *Session) Config() *Config { return &s.cfg }

// SessionStats returns a copy of the counters.
func (s *Session) SessionStats() Stats { return s.stats }

// Established reports whether the peer's parameters were processed.
func (s *Session) Established() bool { return s.peerSeen }

func (s *Session) isClosing() bool { return s.state >= StateClosing }

// processPeerParams runs when the first frame of the inbound bootstrap pipe
// decoded (SPEC.md 5.1, 6.1).
func (s *Session) processPeerParams(p *wire.Params) *wire.Error {
	s.peer = *p
	s.peerSeen = true
	s.peerZmux = p.IsZmux()
	l := &s.cfg.Local
	s.feat = Features{}
	f := &s.feat
	// Role negotiation (EXTENSIONS.md 2.3).
	if l.Has&wire.HasRole != 0 && p.Has&wire.HasRole != 0 {
		f.Role = true
		lr, pr := l.Role, p.Role
		switch {
		case lr == wire.RoleAuto && pr == wire.RoleAuto:
			switch {
			case l.RoleNonce > p.RoleNonce:
				s.role = RoleInitiator
			case l.RoleNonce < p.RoleNonce:
				s.role = RoleResponder
			default:
				return &wire.Error{Code: wire.RoleConflict, Reason: "equal role nonces"}
			}
		case lr == wire.RoleAuto:
			switch pr {
			case wire.RoleInitiator:
				s.role = RoleResponder
			case wire.RoleResponder:
				s.role = RoleInitiator
			}
		case pr == wire.RoleAuto || lr != pr:
			s.role = Role(lr)
		default:
			return &wire.Error{Code: wire.RoleConflict, Reason: "both endpoints claim the same role"}
		}
	} else {
		tr := s.cfg.TransportRole
		if tr == RoleAuto || (s.cfg.Role != RoleAuto && s.cfg.Role != tr) {
			return &wire.Error{Code: wire.RoleConflict, Reason: "no transport role"}
		}
		s.role = tr
	}
	s.roleKnown = true
	both := func(bit uint32) bool { return l.Has&bit != 0 && p.Has&bit != 0 }
	f.GoAway = both(wire.HasGoAway)
	f.Preamble = both(wire.HasPreamble)
	f.Priority = both(wire.HasPriority)
	f.Flows = both(wire.HasFlows) && l.Has&wire.HasDatagram != 0
	if both(wire.HasMultipath) {
		if sel := l.SecretFlags & p.SecretFlags; sel&(wire.SecretExporter|wire.SecretPSK) != 0 || sel&wire.SecretInsecure != 0 {
			f.Multipath = true
		} else {
			f.JoinSecurityUnavailable = true
		}
	}
	f.PipeAck = both(wire.HasPipeAck) && !f.Multipath
	f.Resume = f.Multipath && both(wire.HasResume) && (len(l.Locator) > 0 || len(p.Locator) > 0)
	f.LanesOut = p.Has&wire.HasLanes != 0 && p.MaxLanes > 1 && f.Acked()
	f.LanesIn = l.Has&wire.HasLanes != 0 && l.MaxLanes > 1 && f.Acked()
	s.gates.GoAway = f.GoAway
	s.gates.Priority = f.Priority
	s.gates.Multipath = f.Multipath
	s.gates.PipeAck = f.PipeAck
	s.gates.Lanes = f.LanesIn
	s.peerMaxRecord = p.MaxRecordSize
	if p.Has&wire.HasDatagram != 0 {
		s.peerMaxDgram = p.MaxDatagramFrameSize
	}
	// Flow control from the peer.
	s.conn.sendMax = p.InitialMaxData
	s.loc[0].limit = p.MaxStreamsBidi
	s.loc[1].limit = p.MaxStreamsUni
	if err := s.earlyOutcome(); err != nil {
		return err
	}
	for _, st := range s.streams {
		st.applyPeerParams()
	}
	if f.Acked() {
		s.ack = newAckState(s)
	}
	if f.Multipath {
		s.startMultipath()
	}
	if s.state == StateEstablishing {
		s.state = StateActive
	}
	s.timers.handshake = Never
	s.timers.lastActivity = s.now
	s.timers.lastKeepRef = -1
	s.emit(Event{Kind: EvEstablished, Features: *f, Role: s.role})
	if f.JoinSecurityUnavailable {
		s.emit(Event{Kind: EvJoinSecurityUnavailable})
	}
	for _, w := range s.estW {
		s.wake(w)
	}
	s.estW = nil
	s.wakeAllWriters()
	s.wakeOpeners()
	return nil
}

// WaitEstablished registers sig until establishment or close.
func (s *Session) WaitEstablished(sig *Signal) bool {
	if s.peerSeen || s.isClosing() {
		return true
	}
	s.estW = append(s.estW, sig)
	return false
}

// WaitClosed registers sig until the session is CLOSED.
func (s *Session) WaitClosed(sig *Signal) bool {
	if s.state == StateClosed {
		return true
	}
	s.closedW = append(s.closedW, sig)
	return false
}

// idleTimeout is the effective idle timeout (SPEC.md 6.4).
func (s *Session) idleTimeout() Duration {
	a, b := s.cfg.Local.MaxIdleTimeout, s.peer.MaxIdleTimeout
	switch {
	case a == 0 && b == 0:
		return 0
	case a == 0:
		return MulD(int64(b), Millisecond)
	case b == 0 || a < b:
		return MulD(int64(a), Millisecond)
	default:
		return MulD(int64(b), Millisecond)
	}
}

// srtt is the session SRTT of SPEC.md 1.3.
func (s *Session) srtt() Duration {
	if s.cfg.Test.RTT > 0 && s.ack == nil {
		return s.cfg.Test.RTT
	}
	if s.ack != nil {
		if r := s.ack.srttMax(); r > 0 {
			return r
		}
	}
	if s.cfg.Test.RTT > 0 {
		return s.cfg.Test.RTT
	}
	return 333 * Millisecond
}

// lingerTimeout is min(2 s, max(250 ms, 3 x SRTT)) (SPEC.md 6.5).
func (s *Session) lingerTimeout() Duration {
	return minD(2*Second, maxD(250*Millisecond, MulD(s.srtt(), 3)))
}

// closeWith enters CLOSING and queues CONNECTION_CLOSE fr on every live
// outbound pipe; with fr nil, or without a live outbound pipe, it closes the
// carriers at once (CLOSED). The first error wins (SPEC.md 6.5).
func (s *Session) closeWith(e *SessionError, fr *closeFrame) {
	if s.state >= StateClosing {
		return
	}
	s.err = e
	s.state = StateClosing
	s.failAllStreams()
	s.wakeAll()
	if fr == nil || !s.hasLiveOutbound() {
		s.finishClose()
		return
	}
	s.closeFr = *fr
	s.closeFr.pending = true
	s.timers.linger = Never
	if !s.feat.Multipath && !e.Remote && (fr.app || fr.code == wire.NoError) && !s.lanesCovered() {
		s.closeFr.waitLanes = true
		s.timers.laneWait = s.now.Add(s.lingerTimeout())
	}
	s.wakeAllWriters()
}

// Close closes the session with an application error (SPEC.md 6.5).
func (s *Session) Close(code uint64, reason []byte) {
	s.closeWith(&SessionError{Kind: CloseApplication, Code: code, Reason: string(reason)},
		&closeFrame{app: true, code: code, reason: reason})
}

// failLocal closes with a locally detected transport error.
func (s *Session) failLocal(e *wire.Error) {
	s.closeTransport(&SessionError{Kind: CloseTransport, Code: e.Code, FrameType: e.FrameType,
		Reason: e.Reason, Details: e.Details})
}

// closeTransport closes with a transport CONNECTION_CLOSE carrying e.
func (s *Session) closeTransport(e *SessionError) {
	rs := e.Reason
	if e.Details != "" {
		rs += "\n" + e.Details
	}
	if len(rs) > 1024 {
		rs = rs[:1024]
	}
	s.closeWith(e, &closeFrame{code: e.Code, frameType: e.FrameType, reason: []byte(rs)})
}

// closeSilently closes without CONNECTION_CLOSE (handshake timeout, pipes
// lost on the single pipe, idle timeout without acknowledgement).
func (s *Session) closeSilently(e *SessionError, graceful bool) {
	if s.state >= StateClosing {
		return
	}
	s.err = e
	s.state = StateClosing
	s.failAllStreams()
	s.wakeAll()
	if graceful {
		// Shut down write sides without frames, then linger reading.
		for _, p := range s.pipes {
			if p.out != nil {
				for _, l := range p.out.lanes {
					if !l.shut {
						l.shut = true
						s.act(Action{Kind: ActShutdown, Pipe: p.id, Lane: l.lane, Dir: DirOut})
					}
				}
			}
		}
		s.timers.linger = s.now.Add(s.lingerTimeout())
		s.emitClosedEvent()
		return
	}
	s.finishClose()
}

// finishClose releases everything and enters CLOSED.
func (s *Session) finishClose() {
	if s.state == StateClosed {
		return
	}
	if s.err != nil && s.state != StateClosing {
		s.failAllStreams()
	}
	s.state = StateClosed
	for _, p := range s.pipes {
		s.act(Action{Kind: ActAbort, Pipe: p.id, Dir: DirBidi})
	}
	s.timers = timers{handshake: Never, linger: Never, grace: Never}
	s.emitClosedEvent()
	for _, w := range s.closedW {
		s.wake(w)
	}
	s.closedW = nil
	s.releaseAll()
	s.wakeAll()
}

func (s *Session) emitClosedEvent() {
	for _, e := range s.events[s.evHead:] {
		if e.Kind == EvClosed {
			return
		}
	}
	if s.closedEmitted {
		return
	}
	s.closedEmitted = true
	s.emit(Event{Kind: EvClosed, Err: s.err})
}

func (s *Session) hasLiveOutbound() bool {
	for _, p := range s.pipes {
		if s.canCarryClose(p) {
			return true
		}
	}
	return false
}

// wakeAll queues every waiter of the session (close, GOAWAY).
func (s *Session) wakeStream(st *Stream) {
	s.wake(st.rsig)
	s.wake(st.wsig)
	if st.dg != nil {
		s.wake(st.dg.sig)
	}
}

func (s *Session) wakeStreams() {
	for _, st := range s.streams {
		s.wakeStream(st)
	}
	for _, st := range s.provisional {
		s.wakeStream(st)
	}
}

func (s *Session) wakeAll() {
	s.wakeStreams()
	for i := range 2 {
		for _, w := range s.acceptW[i] {
			s.wake(w)
		}
		s.acceptW[i] = nil
		for _, w := range s.openW[i] {
			s.wake(w)
		}
		s.openW[i] = nil
	}
	for _, w := range s.estW {
		s.wake(w)
	}
	s.estW = nil
	s.ping.wakeAll(s)
	if s.dg != nil {
		s.dg.wakeAll(s)
	}
	s.wake(s.bcast)
}

func (s *Session) wakeAllWriters() {
	for _, p := range s.pipes {
		if p.out != nil {
			s.wakePipe(p)
		}
	}
}

// wakePipe wakes the writers of every lane of an outbound pipe.
func (s *Session) wakePipe(p *Pipe) {
	s.wake(p.out.sig)
	for _, l := range p.out.lanes[1:] {
		s.wake(l.sig)
	}
}

// StreamByID returns a live stream by identifier.
func (s *Session) StreamByID(id uint64) *Stream { return s.streams[id] }
