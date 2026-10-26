package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
)

// Sending part states (RFC 9000 Section 3.1).
type sendState uint8

const (
	sendReady sendState = iota
	sendSend
	sendDataSent
	sendDataRecvd
	sendResetSent
	sendResetRecvd
)

// Receiving part states (RFC 9000 Section 3.2).
type recvState uint8

const (
	recvRecv recvState = iota
	recvSizeKnown
	recvDataRecvd
	recvDataRead
	recvResetRecvd
	recvResetRead
)

// Stream is the core state of one stream. All fields are guarded by the
// session lock; handles in the public package wrap it.
type Stream struct {
	sess *Session
	id   uint64

	snd sendPart
	rcv recvPart

	// Local preamble settings (before the first frame; nil: empty).
	pre *wire.PreambleSpec
	// Peer preamble (copied out of the read buffer).
	peerPre *peerPreamble
	// Rarely used state (priority requests, capacity waiters).
	ext *streamExt

	rsig *Signal
	wsig *Signal

	// Scheduler and control links.
	sch     schedLink
	ctlNext *Stream

	dg *streamDgrams

	idSet bool
	local bool
	bidi  bool
	hasS  bool // has a sending part
	hasR  bool // has a receiving part

	// Life cycle (STATE_MACHINE.md 2.3).
	offered     bool // peer stream offered through accept
	accepted    bool
	releasedS   bool // the application gave up the sending part
	releasedR   bool // the application gave up the receiving part
	stackReject bool // rejected by the stack, never offered
	discarded   bool
	pinned      bool // identifier pinned, opening frame owed
	opened      bool // first frame emitted (local) or opened (peer)
	cancelled   bool // cancelled before any frame: never on the wire
	replayable  bool

	// Local scheduling priority.
	urgency  uint8
	incr     bool
	forceMSD bool
	copyMSD  bool

	ctl   uint8 // pending control frame bits
	inCtl bool

	pol    uint8 // scheduling policy + 1; 0: the session's
	redund int8  // SetRedundant: 1 on, -1 off, 0 the session default
}

// streamExt is the rarely used state of a stream, allocated on first use.
type streamExt struct {
	// The request for the peer's part's priority.
	reqSeq         uint64
	reqSent        uint64 // sequence acknowledged as sent
	reqAcked       uint64
	prioSeqApplied uint64
	reqPrio        bool // a priority request exists
	reqUrgency     uint8
	reqIncr        bool
	prioApplied    bool

	capSig *Signal // a WaitSendCapacity waiter
	capMin uint64
}

func (st *Stream) x() *streamExt {
	if st.ext == nil {
		st.ext = &streamExt{}
	}
	return st.ext
}

// preSpec returns the local preamble settings, allocated on first use.
func (st *Stream) preSpec() *wire.PreambleSpec {
	if st.pre == nil {
		st.pre = &wire.PreambleSpec{}
	}
	return st.pre
}

// preSize is the encoded size of the local preamble.
func (st *Stream) preSize() int {
	if st.pre == nil {
		return 1 // an empty preamble: its length
	}
	return st.pre.Size()
}

type peerPreamble struct {
	openInfo    []byte
	hasOpenInfo bool
	hasPrio     bool
	urgency     uint8
	incr        bool
	pipeCarrier bool
	app         []wire.TLV
}

// pending control bits of a stream
const (
	ctlMaxStreamData uint8 = 1 << iota
	ctlStop
	ctlReset
	ctlPrio
	ctlBlocked
)

type sendPart struct {
	q        sendQ
	sent     uint64 // every byte below was sent at least once
	maxData  uint64 // peer's limit
	blocked  uint64 // limit+1 at which STREAM_DATA_BLOCKED was sent
	preLen   uint64
	boundary uint64 // SetReliableBoundary (wire offset)
	// Reset state.
	resetCode  uint64
	resetRel   uint64 // reliable size (0: RESET_STREAM)
	stopCode   uint64
	errState   *StreamError // terminal outcome for writes
	bufLimit   uint64
	lane       uint64  // lane binding (lanes without multipath)
	lanePipe   uint64  // pipe of the bound lane
	laneEnd    uint64  // lane position after the direction's last frame on it
	mps        *mpSend // multipath retention state (nil outside multipath)
	appWritten uint64  // application bytes written (excludes the preamble)

	// Ages for tier saturation (SCHEDULER.md 10).
	waitAt    Time // the oldest unsent byte's write
	wwaitAt   Time // the writer's wait for buffer space began
	writeDone Time
	creditAt  Time // stream credit arrived while data waited for it

	state     sendState
	finWr     bool
	finSent   bool
	hasBound  bool
	reset     bool // the application or a STOP_SENDING asked to reset
	resetAt   bool // send RESET_STREAM_AT
	resetSent bool
	stopped   bool
	laneBound bool
	wwait     bool
	wwaitSet  bool
	oppListed bool // in the blocked list of opportunistic reinjection
}

type recvPart struct {
	buf       recvBuf
	arrived   rangeSet
	highest   uint64
	final     uint64
	readOff   uint64 // wire offset consumed (read, discarded or preamble)
	window    uint64
	adv       uint64 // advertised limit L
	stopCode  uint64
	resetCode uint64
	resetRel  uint64
	relLo     uint64 // lowest reliable size whose gap was released
	errState  *StreamError
	preSize   uint64
	blockSeen uint64 // largest STREAM_DATA_BLOCKED limit reported
	tune      *tuner
	maxWin    uint64 // SetReceiveWindow max (0: the session's cap)

	state    recvState
	finKnown bool
	stopped  bool
	stopSent bool
	reset    bool
	preDone  bool
}

// StreamID returns the identifier and whether it is allocated.
func (st *Stream) StreamID() (uint64, bool) { return st.id, st.idSet }

// Local reports whether the stream was opened locally.
func (st *Stream) Local() bool { return st.local }

// Bidi reports whether the stream is bidirectional.
func (st *Stream) Bidi() bool { return st.bidi }

// Replayable reports whether the opening frame arrived as carrier early data
// and the carrier handshake has not completed.
func (st *Stream) Replayable() bool { return st.replayable }

// SetSignals installs the waiter slots of the handle.
func (st *Stream) SetSignals(r, w *Signal) { st.rsig, st.wsig = r, w }

func typeIndex(bidi bool) int {
	if bidi {
		return 0
	}
	return 1
}

// streamID composes an identifier (RFC 9000 Section 2.1).
func streamID(idx uint64, bidi bool, responder bool) uint64 {
	id := idx << 2
	if !bidi {
		id |= 2
	}
	if responder {
		id |= 1
	}
	return id
}

func isLocalID(id uint64, role Role) bool { return (id&1 == 1) == (role == RoleResponder) }

func isBidiID(id uint64) bool { return id&2 == 0 }

// ---- opening ----

// OpenStream opens a local stream (API.md 5.1). Once established it reserves
// stream-count credit and fails with ErrStreamLimit without it; before
// establishment the stream is provisional.
func (s *Session) OpenStream(bidi bool) (*Stream, error) {
	if err := s.openErr(); err != nil {
		return nil, err
	}
	t := typeIndex(bidi)
	if s.peerSeen {
		if s.loc[t].reserved >= s.loc[t].limit {
			s.markStreamsBlocked(t)
			return nil, ErrStreamLimit
		}
	}
	s.loc[t].reserved++
	st := s.newStream(bidi, true)
	st.urgency, st.incr = 3, true
	s.provisional = append(s.provisional, st)
	s.stats.StreamsOpenedLocal++
	return st, nil
}

func (s *Session) openErr() error {
	if s.err != nil {
		return s.err
	}
	if s.ga.received {
		return &StreamError{Kind: StreamGoingAway}
	}
	return nil
}

// WaitOpen registers sig until credit for an open of this type may exist.
func (s *Session) WaitOpen(bidi bool, sig *Signal) {
	t := typeIndex(bidi)
	s.openW[t] = append(s.openW[t], sig)
}

// OpenersWaiting reports application openers waiting for stream credit.
func (s *Session) OpenersWaiting(bidi bool) bool { return len(s.openW[typeIndex(bidi)]) > 0 }

// CancelOpenWait removes sig from the open waiters.
func (s *Session) CancelOpenWait(bidi bool, sig *Signal) {
	t := typeIndex(bidi)
	s.openW[t] = removeSig(s.openW[t], sig)
}

func removeSig(l []*Signal, sig *Signal) []*Signal {
	for i, w := range l {
		if w == sig {
			copy(l[i:], l[i+1:])
			l[len(l)-1] = nil
			return l[:len(l)-1]
		}
	}
	return l
}

func (s *Session) wakeOpeners() {
	for t := range 2 {
		if s.loc[t].reserved < s.loc[t].limit && len(s.openW[t]) > 0 {
			// FIFO: wake the first waiter; it re-checks and the next one is
			// woken when it gets its credit (API.md 5.1).
			s.wake(s.openW[t][0])
		}
	}
}

func (s *Session) newStream(bidi, local bool) *Stream {
	st := &Stream{sess: s, local: local, bidi: bidi, replayable: !local && s.carrierEarlyIn}
	st.hasS = local || bidi
	st.hasR = !local || bidi
	if st.hasR {
		st.rcv.relLo = 1<<62 - 1
	}
	return st
}

// allocID gives a local stream its identifier (SPEC.md 7.2 rule 1).
func (s *Session) allocID(st *Stream) {
	t := typeIndex(st.bidi)
	idx := s.loc[t].next
	s.loc[t].next++
	st.id = streamID(idx, st.bidi, s.role == RoleResponder)
	st.idSet = true
	s.dropProvisional(st)
	if st.bidi {
		s.openLocalBidiRecv++
	}
	s.streams[st.id] = st
	st.applyPeerParams()
	if s.feat.Preamble {
		var pre wire.PreambleSpec
		if st.pre != nil {
			pre = *st.pre
		}
		if s.greaseFirst {
			s.greaseFirst = false
			if s.cfg.Test.Enabled {
				pre.Grease = s.cfg.Test.GreaseTLV
			} else {
				g := randUint64()
				pre.Grease = &wire.TLV{Type: 0x21 + 0x1f*(g%1000), Value: nil}
			}
		}
		b := wire.AppendPreamble(make([]byte, 0, pre.Size()), &pre)
		st.snd.preLen = uint64(len(b))
		h := s.heldEnter(st)
		st.snd.q.prepend(b)
		s.heldLeave(st, h)
		if st.snd.hasBound {
			st.snd.boundary += st.snd.preLen
		}
	}
	st.pre = nil
}

// applyPeerParams sets the initial send credit from the peer's parameters.
func (st *Stream) applyPeerParams() {
	s := st.sess
	if !s.peerSeen && !s.earlyActive() || !st.idSet {
		return
	}
	if st.hasS && st.snd.maxData == 0 {
		switch {
		case !st.bidi:
			st.snd.maxData = s.peer.MaxStreamDataUni
		case st.local:
			st.snd.maxData = s.peer.MaxStreamDataBidiRem
		default:
			st.snd.maxData = s.peer.MaxStreamDataBidiLoc
		}
	}
	if st.hasR && st.rcv.window == 0 {
		// Receive windows come from the local parameters.
		l := &s.cfg.Local
		switch {
		case !st.bidi:
			st.rcv.window = l.MaxStreamDataUni
		case st.local:
			st.rcv.window = l.MaxStreamDataBidiLoc
		default:
			st.rcv.window = l.MaxStreamDataBidiRem
		}
		st.rcv.adv = st.rcv.window
	}
}

// PinID returns the identifier, pinning it before the first frame (SPEC.md
// 7.2 rule 1).
func (s *Session) PinID(st *Stream) (uint64, error) {
	if st.idSet {
		return st.id, nil
	}
	if st.cancelled {
		return 0, st.snd.errState
	}
	if !s.peerSeen {
		return 0, ErrNotEstablished
	}
	s.allocID(st)
	st.pinned = true
	s.sched.ensure(st)
	s.wakeAllWriters()
	return st.id, nil
}

// ---- local preamble settings ----

func (s *Session) beforeFirstFrame(st *Stream) error {
	if !st.local || st.idSet {
		return ErrTooLate
	}
	if s.peerSeen && !s.feat.Preamble {
		return ErrNotNegotiated
	}
	return nil
}

// SetOpenInfo sets OPEN_INFO before the first frame.
func (s *Session) SetOpenInfo(st *Stream, b []byte) error {
	if err := s.beforeFirstFrame(st); err != nil {
		return err
	}
	ps := st.preSpec()
	ps.OpenInfo = append([]byte(nil), b...)
	ps.HasOpenInfo = true
	return nil
}

// AddPreambleTLV adds an application TLV before the first frame.
func (s *Session) AddPreambleTLV(st *Stream, t uint64, v []byte) error {
	if !wire.IsAppTLV(t) {
		return ErrInvalid
	}
	if err := s.beforeFirstFrame(st); err != nil {
		return err
	}
	ps := st.preSpec()
	ps.App = append(ps.App, wire.TLV{Type: t, Value: append([]byte(nil), v...)})
	return nil
}

// SetPipeCarrier marks the stream as a carrier before the first frame.
func (s *Session) SetPipeCarrier(st *Stream) error {
	if err := s.beforeFirstFrame(st); err != nil {
		return err
	}
	st.preSpec().PipeCarrier = true
	return nil
}

// SetPriority sets local scheduling priority.
func (s *Session) SetPriority(st *Stream, urgency uint8, incr bool) {
	if urgency > 7 {
		urgency = 7
	}
	st.urgency, st.incr = urgency, incr
	s.sched.reprio(st)
}

// SetPeerPriority requests a priority for the peer's sending part:
// the PRIORITY TLV before a local stream's first frame, PRIORITY_UPDATE
// otherwise (API.md 5.1).
func (s *Session) SetPeerPriority(st *Stream, urgency uint8, incr bool) error {
	if urgency > 7 {
		return ErrInvalid
	}
	if st.local && !st.idSet {
		ps := st.preSpec()
		ps.HasPriority, ps.Urgency, ps.Incr = true, urgency, incr
		return nil
	}
	if !s.feat.Priority {
		return ErrNotNegotiated
	}
	if !st.bidi && st.local {
		return ErrInvalid
	}
	x := st.x()
	x.reqPrio, x.reqUrgency, x.reqIncr = true, urgency, incr
	x.reqSeq++
	s.queueCtl(st, ctlPrio)
	return nil
}

func (s *Session) dropProvisional(st *Stream) {
	for i, x := range s.provisional {
		if x == st {
			copy(s.provisional[i:], s.provisional[i+1:])
			s.provisional[len(s.provisional)-1] = nil
			s.provisional = s.provisional[:len(s.provisional)-1]
			return
		}
	}
}

// PeerPreamble is the preamble a peer stream arrived with.
type PeerPreamble struct {
	OpenInfo    []byte
	HasOpenInfo bool
	HasPriority bool
	Urgency     uint8
	Incremental bool
	PipeCarrier bool
	App         []wire.TLV
}

// PeerPreamble returns the stream's peer preamble, ok false when none.
func (st *Stream) PeerPreamble() (PeerPreamble, bool) {
	p := st.peerPre
	if p == nil {
		return PeerPreamble{}, false
	}
	return PeerPreamble{OpenInfo: p.openInfo, HasOpenInfo: p.hasOpenInfo, HasPriority: p.hasPrio,
		Urgency: p.urgency, Incremental: p.incr, PipeCarrier: p.pipeCarrier, App: p.app}, true
}
