package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
)

// The path layer of a multipath session (MULTIPATH.md, SCHEDULER.md,
// RESUME.md). It exists only once multipath is in use (s.mp != nil); every
// pipe of such a session carries an attach record (p.mp). Single-pipe
// sessions never allocate any of it (IMPLEMENTATION.md 2.6).

// Outbound pipe states (STATE_MACHINE.md 3.1).
type outState uint8

const (
	outNone outState = iota
	outJoining
	outActive
	outSuspect
	outDraining
	outFailed
	outClosed
)

var outStateNames = [...]string{"NONE", "JOINING", "ACTIVE", "SUSPECT", "DRAINING", "FAILED", "CLOSED"}

// Inbound pipe states (STATE_MACHINE.md 3.2).
type inState uint8

const (
	inNone inState = iota
	inJoining
	inOpen
	inEnded
)

var inStateNames = [...]string{"NONE", "JOINING", "OPEN", "ENDED"}

type multiPath struct {
	keyOwn, keyPeer []byte
	locator         []byte // our locator (what the peer puts in PIPE_JOIN)
	peerLocator     []byte
	nextID          uint64 // next own pipe ID
	cap             int    // unretired attachments of the peer we keep
	ownCap          int    // unretired attachments we joined ourselves
	replayW         uint64 // W
	h               uint64 // highest verified peer pipe ID
	hSet            bool
	bits            map[uint64]bool // replay bits of peer IDs without state
	pendAbandon     map[uint64]bool // PIPE_ABANDON owed for IDs without state
	pendCode        map[uint64]uint64
	backoffUntil    Time
	backoffExp      int
	pingTimes       [8]Time // send times of the last 8 PIPE_PINGs
	pingIdx         int
	pingCount       int
	redundantAt     Time // when the redundant snapshot copy is due
	ackChanged      bool // snapshot changed since the last copy
	busyUntil       Time // traffic frames within the busy window
	joinReplays     uint64
	joinAuthFails   uint64
	retained        uint64 // unacknowledged stream bytes (SCHEDULER.md 13.1)

	// Scheduler (SCHEDULER.md).
	gateBuf      gate
	dec          decision
	tierSince    []tierMark
	satRep       []satReport
	fateHist     map[fateKey]*history
	copies       []*copyRec
	aff          affinityState
	pickGen      uint64
	pickWanted   bool
	pickQ        []*Stream // directions waiting for a custom decision
	inputEpoch   uint64
	pickMisses   uint64
	pickApplied  uint64
	pickTimer    bool // a direction waits for an answer until pickDeadline
	pickDeadline Time

	suspendCause error // the pipe loss that suspended the session

	endsBuf []uint64  // scratch of retainBatch
	blocked []*Stream // stream directions blocked by the peer's stream credit
}

// attach is the per-attachment state of a multipath session.
type attach struct {
	joinerLocal bool
	dir         uint8 // MULTIPATH 5.1 direction from the joiner: 0 up, 1 down, 2 bidi
	fate        uint64
	out         outState
	in          inState
	attachedAt  Time
	join        *wire.Join // local join waiting to be written (in band)
	oob         bool
	held        bool
	resLimited  bool // abandoned by the peer with ZMUX_PIPE_RESOURCE_LIMIT // a local join waits for room or the backoff
	firstRecord bool // our first record on the outbound direction is still due
	discardTP   bool // the joiner discards a leading QX_TRANSPORT_PARAMETERS record
	needPing    bool // a PIPE_PING is owed on this pipe
	pingSince   Time
	ackPair     bool // a PIPE_ACK answering a PIPE_PING is owed on this pair

	// Removal (MULTIPATH.md 8).
	abandonSent     bool // own PIPE_ABANDON handed to a carrier
	abandonQueued   bool
	abandonCode     uint64
	abandonAcked    bool
	abandonRecv     bool
	abandonRecvAt   Time
	abandonCodeRecv uint64
	unilateral      bool
	lingerUntil     Time
	ackedAbandonAt  Time
	draining        bool
	drainSuspect    bool
	drainStart      Time
	retired         bool

	// Sender view (estimates, detection).
	rtt         rttEst
	lastAdvance Time
	liveAt      Time
	unacked     bool // unacknowledged ack-eliciting bytes exist
	lingering   bool
	oldestAt    Time // send time of the oldest unacknowledged ack-eliciting byte
	lastElicit  Time // when an ack-eliciting byte was last sent
	suspectAt   Time
	failCause   error
	unsentAhead uint64
	noSampleAt  Time // samples of bytes sent at or before this are discarded (E2)
	noSample    bool

	// Tiers (MULTIPATH.md 9).
	sendPrio    *uint64
	sendStandby *bool
	peerPrio    uint64
	peerStandby bool
	peerSeq     uint64
	recvPrio    uint64
	recvStandby bool
	statusSeq   uint64
	statusQ     []statusReq // PIPE_STATUS owed
	weight      uint64
	takeover    bool

	// Scheduler (SCHEDULER.md).
	probOpt      Duration // per-pipe Probation; <0: the session default
	suspectFloor Duration // per-pipe SuspectFloor; 0: the session default
	deadTimeout  Duration // per-pipe DeadTimeout; 0: the session default
	prob         probState
	hist         history // fate group 0
	wasSuspect   bool
	reval        bool // awaiting revalidation
	revalPing    bool // its PIPE_PING is still to be written
	revalEnd     uint64
	rrCur        float64
	copyQ        []*copyRec
	selfCopyQ    []*copyRec // copies for p's own copy lane (a single-pipe tier)
	idleArm      uint64     // bytes of the idle copy left
	placedSet    bool
	placedFrom   uint64
	// Counters (API.md 4.5).
	suspects, failures, reinjectedBytes, oppBytes, pingsSent uint64

	penalized  bool // MULTIPATH.md 7.5.1: no new data until penaltyEnd is acknowledged
	penaltyEnd uint64
	// Reinjection hold (RESUME.md 6): a new pipe reinjects one chunk until
	// the first PIPE_ACK after it became live, or one smoothed RTT.
	holdUntil Time
	held1     bool
	holdSent  uint64

	// Inbound silence.
	lastRecord Time
	maxGap     Duration
}

// startMultipath allocates the path layer at establishment.
func (s *Session) startMultipath() {
	l, p := &s.cfg.Local, &s.peer
	m := &multiPath{locator: l.Locator, peerLocator: p.Locator, bits: map[uint64]bool{},
		pendAbandon: map[uint64]bool{}}
	var exporter, psk []byte
	sel := l.SecretFlags & p.SecretFlags
	if sel&wire.SecretExporter != 0 {
		exporter = s.cfg.Exporter
	}
	if sel&wire.SecretPSK != 0 {
		psk = s.cfg.PSK
	}
	initN, respN := l.SessionNonce[:], p.SessionNonce[:]
	if s.role == RoleResponder {
		initN, respN = respN, initN
	}
	ki, kr := wire.JoinKeys(wire.SessionSecret(initN, respN, exporter, psk))
	if s.role == RoleInitiator {
		m.keyOwn, m.keyPeer = ki, kr
		m.nextID = 2
	} else {
		m.keyOwn, m.keyPeer = kr, ki
		m.nextID = 1
	}
	maxPipes := max(1, s.cfg.MaxPipes)
	m.cap = maxPipes + s.cfg.Headroom
	m.ownCap = m.cap
	if s.cfg.Test.PipeCap > 0 {
		m.cap = s.cfg.Test.PipeCap
	}
	m.replayW = uint64(max(64, 2*(maxPipes+s.cfg.Headroom)))
	if s.cfg.ReplayWindow > 0 {
		m.replayW = uint64(max(s.cfg.ReplayWindow, maxPipes+s.cfg.Headroom))
	}
	if s.role == RoleResponder {
		// The peer is the initiator: its bootstrap pair, pipe 0, is h.
		m.h, m.hSet = 0, true
	}
	m.busyUntil = s.now.Add(15 * Second)
	s.mp = m
	for _, pp := range s.pipes {
		a := &attach{joinerLocal: s.role == RoleInitiator, dir: 2, out: outActive, in: inOpen,
			attachedAt: s.now, liveAt: s.now, lastAdvance: s.now, lastRecord: s.now, weight: 1, probOpt: -1}
		a.rtt = s.initialRTT(pp.id)
		pp.mp = a
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

// peerIDState classifies a peer-parity pipe ID without attachment state
// (MULTIPATH.md 4.3): retired (bit set or stale), inWindow, or above.
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

// PipeState returns the state names of a pipe for traces and stats.
func (s *Session) PipeState(id uint64) (out, in string, ok bool) {
	p := s.pipe(id)
	if p == nil {
		return "", "", false
	}
	if s.mp == nil || p.mp == nil {
		return "ACTIVE", "OPEN", true
	}
	a := p.mp
	if a.retired {
		return "RETIRED", "RETIRED", true
	}
	o := outStateNames[a.out]
	if a.out == outDraining && a.drainSuspect {
		o = "DRAINING"
	}
	return o, inStateNames[a.in], true
}

// sampleOK reports whether bytes sent at sent may give estimator samples:
// not when sent before or at the moment the pipe last became suspect
// (SCHEDULER.md 2.1).
func (a *attach) sampleOK(sent Time) bool { return !a.noSample || sent > a.noSampleAt }
