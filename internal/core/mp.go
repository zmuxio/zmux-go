package core

import (
	"crypto/rand"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// The path layer of a multipath session, with resume. It exists only once multipath is in use (s.mp != nil); every
// pipe of such a session carries an attach record (p.mp). Single-pipe
// sessions never allocate any of it.

// OutState is the state of an outbound pipe direction.
type OutState uint8

const (
	OutNone OutState = iota
	OutJoining
	OutActive
	OutSuspect
	OutDraining
	OutFailed
	OutClosed
	OutRetired // reported only: the attachment was retired
)

var outStateNames = [...]string{"NONE", "JOINING", "ACTIVE", "SUSPECT", "DRAINING", "FAILED", "CLOSED", "RETIRED"}

func (o OutState) String() string { return outStateNames[o] }

// InState is the state of an inbound pipe direction.
type InState uint8

const (
	InNone InState = iota
	InJoining
	InOpen
	InEnded
	InRetired // reported only: the attachment was retired
)

var inStateNames = [...]string{"NONE", "JOINING", "OPEN", "ENDED", "RETIRED"}

func (i InState) String() string { return inStateNames[i] }

type multiPath struct {
	// ep holds the accepted epochs, their join keys and Locators, and the
	// state of the epoch exchange.
	ep           epochs
	usable       []uint64 // the Suites of both sets, ascending
	usableGroups []uint64 // the Groups of both sets, ascending; holds Group 2
	phaseKey     []byte   // a random key of the process for a key phase no epoch has
	// joinFailures counts own consecutive failed joins.
	joinFailures uint64
	// exporter is the exporter value read at establishment, kept only
	// while keysPending: the keys wait for the deferred PSK or the declaration.
	exporter     []byte
	nextID       uint64          // next own pipe ID
	cap          int             // unretired attachments of the peer we keep
	ownCap       int             // unretired attachments we joined ourselves
	replayW      uint64          // W
	h            uint64          // highest verified peer pipe ID
	bits         map[uint64]bool // replay bits of peer IDs without state
	pendAbandon  map[uint64]bool // PIPE_ABANDON owed for IDs without state
	pendCode     map[uint64]uint64
	backoffUntil Time
	backoffExp   int
	pingTimes    [8]Time // send times of the last 8 PIPE_PINGs
	pingIdx      int
	pingCount    int
	redundantAt  Time   // when the redundant snapshot copy is due
	busyUntil    Time   // traffic frames within the busy window
	stateGen     uint64 // counts pipe state changes (mpHandleTimeout)
	retained     uint64 // unacknowledged stream bytes

	// Scheduler.
	gateBuf      gate
	dec          decision
	tierSince    []tierMark
	satRep       []satReport
	fateHist     map[fateKey]*history
	copies       []*copyRec
	aff          affinityState
	pickGen      uint64
	pickQ        []*Stream // directions waiting for a custom decision
	inputEpoch   uint64
	pickDeadline Time

	suspendCause error // the pipe loss that suspended the session

	endsBuf []uint64  // scratch of retainBatch
	blocked []*Stream // stream directions blocked by the peer's stream credit

	// Flags of the groups above, kept together.
	keysPending bool // the keys wait (exporter)
	usePSK      bool // the P source is configured
	hSet        bool // h is set
	ackChanged  bool // snapshot changed since the last copy
	pickWanted  bool
	pickTimer   bool // a direction waits for an answer until pickDeadline
}

// peerAttached reports whether the peer has attached the attachment
// (the peer holds it as an attachment of its own): one the endpoint joined
// itself once it has evidence (peerSeen), every other from its creation.
func (a *attach) peerAttached(id uint64) bool {
	return a.peerSeen || !a.joinerLocal || id == 0
}

// sawPeer records the evidence of the peer's attachment of a joined
// attachment, waking the waiters on the first.
func (s *Session) sawPeer(p *Pipe) {
	if a := p.mp; a != nil && !a.peerSeen {
		a.peerSeen = true
		if a.joinerLocal && p.id != 0 {
			s.emit(Event{Kind: EvPipeAttached, Pipe: p.id})
		}
	}
}

// attach is the per-attachment state of a multipath session.
type attach struct {
	group     *join.Group // a local join's key exchange
	joinEpoch uint64      // the epoch of a local join's Hello
	// The SESSION_EPOCH frame repeated until the next phase: sent on this pipe
	// (epochSent) at epochAt, again one probe interval later.
	epochAt      Time
	fate         uint64
	attachedAt   Time
	pingSince    Time
	joiner       *join.Joiner
	provenFrames []byte // the return frames of a proven Reply
	binding      []byte
	epochSent    bool
	joinerLocal  bool
	peerSeen     bool  // a joined attachment's evidence of the peer's attachment (peerAttached)
	dir          uint8 // the pipe direction as the joiner states it: 0 up, 1 down, 2 bidi
	out          OutState
	in           InState
	hs           bool // the Join handshake is running: nothing but it on the carrier
	mode         join.Mode
	tpSkipped    bool // the one transport parameters record before the Reply went by
	framing      join.Framing
	proven       bool // the Reply verified; JoinFinish is to come
	early        bool // JoinPrepare found no usable pipe
	held         bool
	resLimited   bool // abandoned by the peer with ZMUX_PIPE_RESOURCE_LIMIT // a local join waits for room or the backoff
	firstRecord  bool // our first record on the outbound direction is still due
	ackPair      bool // a PIPE_ACK answering a PIPE_PING is owed on this pair

	// Removal.
	abandonCode     uint64
	abandonRecvAt   Time
	abandonCodeRecv uint64
	lingerUntil     Time
	ackedAbandonAt  Time
	drainStart      Time
	abandonSent     bool // own PIPE_ABANDON handed to a carrier
	abandonQueued   bool
	abandonAcked    bool
	abandonRecv     bool
	unilateral      bool
	draining        bool
	drainSuspect    bool
	retired         bool

	// Sender view (estimates, detection).
	rtt         rttEst
	lastAdvance Time
	liveAt      Time
	oldestAt    Time // send time of the oldest unacknowledged ack-eliciting byte
	lastElicit  Time // when an ack-eliciting byte was last sent
	suspectAt   Time
	failCause   error
	// unsentAhead is the failure detector's queue snapshot: Q_p when the oldest
	// unacknowledged ack-eliciting byte was handed over (the bytes ahead
	// of it), fixed so that a pipe that stops moving is still detected.
	unsentAhead uint64
	queueD      Duration // queue_p of the snapshot
	noSampleAt  Time     // samples of bytes sent at or before this are discarded (E2)
	suspFloor   Duration // the SuspectFloor in force when the pipe turned suspect
	suspDead    Duration // the DeadTimeout in force then (0: derived)
	unacked     bool     // unacknowledged ack-eliciting bytes exist
	lingering   bool
	noSample    bool
	suspHeld    bool // suspFloor and suspDead hold: set on turning suspect

	// Tiers.
	statusHeld  bool // the owed request was changed while JOINING
	peerStandby bool
	recvStandby bool
	takeover    bool
	sendPrio    *uint64
	sendStandby *bool
	peerPrio    uint64
	peerSeq     uint64
	recvPrio    uint64
	statusSeq   uint64
	statusQ     []statusReq // PIPE_STATUS owed
	weight      uint64

	// Scheduler.
	probOpt    Duration     // per-pipe Probation; <0: the session default
	live       PipeLiveness // the pipe's own liveness settings
	probeH     Duration     // H_p of the current probe wait, taken when it started
	probeCut   float64      // the current probe wait is k_eff x (1 - probeCut): in [0, j], or [0, 1] for the first wait of a bootstrap pipe
	prob       probState
	hist       history // fate group 0
	revalEnd   uint64
	rrCur      float64
	copyQ      []*copyRec
	selfCopyQ  []*copyRec // copies for p's own copy lane (a single-pipe tier)
	idleArm    uint64     // bytes of the idle copy left
	placedFrom uint64
	wasSuspect bool
	reval      bool // awaiting revalidation
	revalPing  bool // its PIPE_PING is still to be written
	needPing   bool // a PIPE_PING is owed on this pipe
	placedSet  bool

	// Reinjection hold: a new pipe reinjects one chunk until
	// the first PIPE_ACK after it became live, or one smoothed RTT.
	penalized  bool // opportunistic penalization: no new data until penaltyEnd is acknowledged
	held1      bool
	penaltyEnd uint64
	holdUntil  Time
	holdSent   uint64

	// Counters.
	suspects, failures, reinjectedBytes, oppBytes, pingsSent uint64
	redundantBytes, dupBytes, pingsAnswered                  uint64
	bytesLost                                                uint64   // unacknowledged outbound bytes at the failure
	pingEnds                                                 []uint64 // outbound positions after unanswered PIPE_PINGs

	// Inbound silence.
	lastRecord Time
	maxGap     Duration
}

// startMultipath allocates the path layer at establishment.
// A failed read of the exporter is the session's INTERNAL_ERROR.
func (s *Session) startMultipath() *wire.Error {
	l, p := &s.cfg.Local, &s.peer
	m := &multiPath{bits: map[uint64]bool{}, pendAbandon: map[uint64]bool{}, usePSK: l.SecretFlags&wire.SecretPSK != 0}
	m.usable = wire.Intersect(l.SessionCrypto.Suites, p.SessionCrypto.Suites)
	m.usableGroups = wire.Intersect(l.SessionCrypto.Groups, p.SessionCrypto.Groups)
	if l.SecretFlags&wire.SecretExporter != 0 && s.cfg.Exporter != nil {
		b, err := s.cfg.Exporter()
		if err != nil {
			return &wire.Error{Code: wire.InternalError, Reason: "exporter: " + err.Error()}
		}
		m.exporter = b
	}
	if s.role == RoleInitiator {
		m.nextID = 2
	} else {
		m.nextID = 1
	}
	s.mp = m
	if s.keyInputsMissing() {
		m.keysPending = true
	} else if err := s.deriveKeys(); err != nil {
		return err
	}
	maxPipes := max(1, s.cfg.MaxPipes)
	m.cap = maxPipes + s.cfg.Headroom
	m.ownCap = m.cap
	if s.cfg.Test.PipeCap > 0 {
		m.cap = s.cfg.Test.PipeCap
	}
	m.replayW = uint64(max(64, 2*(maxPipes+s.cfg.Headroom)))
	if s.cfg.ReplayWindow > 0 {
		m.replayW = uint64(s.cfg.ReplayWindow) // at least maxPipes+Headroom
	}
	if s.role == RoleResponder {
		// The peer is the initiator: its bootstrap pair, pipe 0, is h.
		m.h, m.hSet = 0, true
	}
	m.busyUntil = s.now.Add(15 * Second)
	for _, pp := range s.pipes {
		a := &attach{mode: s.firstPipeMode(), joinerLocal: s.role == RoleInitiator, dir: 2, out: OutActive, in: InOpen,
			attachedAt: s.now, liveAt: s.now, lastAdvance: s.now, lastRecord: s.now, weight: 1, probOpt: -1}
		a.rtt = s.initialRTT(pp.id)
		s.drawProbe(a, pp, true)
		pp.mp = a
	}
	return nil
}

// unknownPhaseKey is the key a Hello of a key phase without an accepted epoch is
// opened with: fresh and random, so the Hello fails as auth like any wrong key.
func (m *multiPath) unknownPhaseKey() []byte {
	if m.phaseKey == nil {
		m.phaseKey = make([]byte, 64)
		_, _ = rand.Read(m.phaseKey) // crypto/rand does not fail
	}
	return m.phaseKey
}

// joinKeys are join_psk of the session's initiator and responder.
type joinKeys struct{ i, r []byte }

// keyInputsMissing reports what keeps a session join pending.
func (s *Session) keyInputsMissing() bool {
	return !s.declared || s.mp.usePSK && s.cfg.DeferredPSK && !s.secretSet
}

// deriveKeys derives the join keys once, from the configured
// sources. session_crypto is built from the local configured value and the value
// received from the peer; the local value is never read back from the wire.
func (s *Session) deriveKeys() *wire.Error {
	m := s.mp
	var psk []byte
	if m.usePSK {
		psk = s.cfg.PSK
	}
	initN, respN := s.cfg.Local.SessionNonce[:], s.peer.SessionNonce[:]
	setI, setR := s.cfg.Local.SessionCrypto, s.peer.SessionCrypto
	if s.role == RoleResponder {
		initN, respN = respN, initN
		setI, setR = setR, setI
	}
	enc, err := join.SessionCrypto(setI, setR)
	if err != nil {
		return wire.ErrParam("zmux_session_crypto")
	}
	e := &epoch{secret: wire.SessionSecret(initN, respN, m.exporter, psk), local: s.cfg.Local.Locator, peer: s.peer.Locator}
	m.ep = epochs{crypto: enc, list: []*epoch{e}, last: -1}
	m.ep.join = e
	if err := s.deriveEpochKeys(e); err != nil {
		return err
	}
	m.exporter, m.keysPending = nil, false
	m.ep.stableSince = s.now
	return nil
}

// deriveEpochKeys derives both join_psk values of every usable Suite of an
// epoch, kept while the epoch is.
func (s *Session) deriveEpochKeys(e *epoch) *wire.Error {
	e.keys = map[uint64]*joinKeys{}
	for _, id := range s.mp.usable {
		su, ok := join.LookupSuite(id)
		if !ok {
			continue
		}
		ki, e1 := join.PSK(su, e.secret, join.RoleInitiator, s.mp.ep.crypto)
		kr, e2 := join.PSK(su, e.secret, join.RoleResponder, s.mp.ep.crypto)
		if e1 != nil || e2 != nil {
			return &wire.Error{Code: wire.InternalError, Reason: "join keys"}
		}
		e.keys[id] = &joinKeys{i: ki, r: kr}
	}
	return nil
}

// keysArrived derives the keys when the last missing input came.
func (s *Session) keysArrived() {
	m := s.mp
	if m == nil || !m.keysPending || s.keyInputsMissing() {
		return
	}
	if err := s.deriveKeys(); err != nil {
		s.failLocal(err)
		return
	}
	s.emit(Event{Kind: EvJoinKeys})
	for _, w := range s.keyW {
		s.wake(w)
	}
	s.keyW = nil
}

// DeclareEndToEnd states that the host proved pipe 0 end to end.
func (s *Session) DeclareEndToEnd() {
	s.declared = true
	s.keysArrived()
}

// SetJoinSecret supplies the deferred PSK (Config.DeferredPSK). Before establishment
// it is kept for it.
func (s *Session) SetJoinSecret(psk []byte) error {
	if s.isClosing() {
		return s.err
	}
	if s.peerSeen && (s.mp == nil || !s.mp.usePSK) {
		return ErrNotNegotiated
	}
	if s.secretSet {
		return ErrJoinSecretSet
	}
	s.cfg.PSK, s.secretSet = append([]byte(nil), psk...), true
	s.keysArrived()
	return nil
}

// SessionBinding is the session's binding value (wire.SessionBinding),
// nil before establishment or without multipath, whose parameter carries
// the Session Nonces.
func (s *Session) SessionBinding() []byte {
	if s.mp == nil {
		return nil
	}
	initN, respN := s.cfg.Local.SessionNonce[:], s.peer.SessionNonce[:]
	if s.role == RoleResponder {
		initN, respN = respN, initN
	}
	return wire.SessionBinding(initN, respN)
}

// JoinPendingNow reports whether the session is join pending.
func (s *Session) JoinPendingNow() bool { return s.mp != nil && s.mp.keysPending }

// WaitJoinKeys registers sig until the join keys exist or the session
// closes; it reports true when that is already so.
func (s *Session) WaitJoinKeys(sig *Signal) bool {
	if !s.JoinPendingNow() || s.isClosing() {
		return true
	}
	s.keyW = append(s.keyW, sig)
	return false
}

// UnwaitJoinKeys removes sig registered by WaitJoinKeys.
func (s *Session) UnwaitJoinKeys(sig *Signal) {
	for i, w := range s.keyW {
		if w == sig {
			s.keyW = append(s.keyW[:i], s.keyW[i+1:]...)
			return
		}
	}
}

// initialRTT is the RFC 9002 initial estimate, or the test hook's.
func (s *Session) initialRTT(pipe uint64) rttEst {
	d := 333 * Millisecond
	if s.cfg.Test.RTT > 0 {
		d = s.cfg.Test.RTT
	}
	if v, ok := s.cfg.Test.PipeRTT[pipe]; ok {
		d = v
	}
	return rttEst{smoothed: d, rttvar: d / 2}
}

// ownParity reports whether a pipe ID has the local endpoint's parity.
func (s *Session) ownParity(id uint64) bool { return (id&1 == 1) == (s.role == RoleResponder) }

// peerIDState classifies a peer-parity pipe ID without attachment state:
// retired (bit set or stale), inWindow, or above.
func (m *multiPath) idRetired(id uint64) bool {
	if m.bits[id] {
		return true
	}
	return m.stale(id)
}

func (m *multiPath) stale(id uint64) bool {
	if !m.hSet {
		return false
	}
	return id+2*m.replayW < m.h
}

// inWindow reports whether id is within the bitmap's span.
func (m *multiPath) inWindow(id uint64) bool {
	if !m.hSet {
		return id < 2*m.replayW
	}
	return !m.stale(id) && id <= m.h+2*m.replayW
}

// raise moves h after a verified join and drops what slides out.
func (m *multiPath) raise(id uint64) {
	if m.hSet && id <= m.h {
		return
	}
	m.h, m.hSet = id, true
	for k := range m.bits {
		if m.stale(k) {
			delete(m.bits, k)
		}
	}
	for k := range m.pendAbandon {
		if m.stale(k) {
			delete(m.pendAbandon, k)
		}
	}
}

// PipeState returns the states of a pipe's two directions.
func (s *Session) PipeState(id uint64) (out OutState, in InState, ok bool) {
	p := s.pipe(id)
	if p == nil {
		return OutNone, InNone, false
	}
	if s.mp == nil || p.mp == nil {
		return OutActive, InOpen, true
	}
	a := p.mp
	if a.retired {
		return OutRetired, InRetired, true
	}
	return a.out, a.in, true
}

// sampleOK reports whether bytes sent at sent may give estimator samples:
// not when sent before or at the moment the pipe last became suspect.
func (a *attach) sampleOK(sent Time) bool { return !a.noSample || sent > a.noSampleAt }

// JoinPendingError is ErrJoinPending with what is missing: the end-to-end
// declaration, the deferred PSK, or both.
type JoinPendingError struct{ Declare, Secret bool }

func (e *JoinPendingError) Error() string {
	switch {
	case e.Declare && e.Secret:
		return "zmux: join pending: the first pipe is not declared end to end (DeclareEndToEnd) and the deferred PSK is not set (SetJoinSecret)"
	case e.Secret:
		return "zmux: join pending: the deferred PSK is not set (SetJoinSecret)"
	case e.Declare:
		return "zmux: join pending: the first pipe is not declared end to end (DeclareEndToEnd)"
	}
	return "zmux: join pending"
}

// Is matches ErrJoinPending.
func (e *JoinPendingError) Is(t error) bool { return t == ErrJoinPending }

// joinPendingErr says what keeps the session join pending.
func (s *Session) joinPendingErr() error {
	return &JoinPendingError{Declare: !s.declared, Secret: s.mp != nil && s.mp.usePSK && s.cfg.DeferredPSK && !s.secretSet}
}
