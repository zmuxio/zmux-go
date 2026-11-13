package core

import (
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
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

	// Life cycle.
	offered     bool // peer stream offered through accept
	accepted    bool
	releasedS   bool // the application gave up the sending part
	releasedR   bool // the application gave up the receiving part
	stackReject bool // rejected by the stack, never offered
	discarded   bool
	pinned      bool // identifier pinned, opening frame owed
	openWant    bool // Flush before establishment: opening frame owed once allowed
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

	cnt streamCounters
}

// streamCounters are the counts of StreamStats that no other field keeps;
// they are never reset, so a released stream keeps them.
type streamCounters struct {
	bytesSent       uint64 // payload bytes put into STREAM frames, every copy
	appRead         uint64 // payload bytes returned to the application
	dgSent          uint64
	dgBytesSent     uint64
	dgReceived      uint64
	dgBytesReceived uint64
	dgDropped       uint64 // dropped from the stream's full queue
}

// countFrame counts the payload bytes of a STREAM frame [off, off+n) of
// st that is put into a record, the preamble excluded.
func (st *Stream) countFrame(off, n uint64) {
	if pre := st.snd.preLen; off < pre {
		n -= min64(n, pre-off)
	}
	st.cnt.bytesSent += n
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
	res        *block  // block lent by WriteSpace until CommitWrite, with a reference

	// Ages for tier saturation.
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
	// drainWait: the session waits for the application to read this
	// part's buffered data before CLOSED (Session.readDrain).
	drainWait bool
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

// OpenStream opens a local stream. Once established it reserves
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
	if s.ga.received || s.ga.closeWhenDone {
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
			// woken when it gets its credit.
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

// allocID gives a local stream its identifier.
// allocID allocates the identifier and queues the preamble. A preamble
// above the peer's limit (which may have shrunk since it was set) fails
// the stream locally instead: nothing is sent, and it returns false.
func (s *Session) allocID(st *Stream) bool {
	if s.feat.Preamble {
		if lim := s.preambleMax(); uint64(st.preSize()) > lim {
			s.failPreamble(st, lim)
			return false
		}
	}
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
	if s.shp != nil {
		s.shapeStream(shaping.EventStreamOpen, st.id)
	}
	st.applyPeerParams()
	if s.feat.Preamble {
		var pre wire.PreambleSpec
		if st.pre != nil {
			pre = *st.pre
		}
		if s.greaseFirst {
			if s.cfg.Test.Enabled {
				pre.Grease = s.cfg.Test.GreaseTLV
			} else {
				g := randUint64()
				pre.Grease = &wire.TLV{Type: 0x21 + 0x1f*(g%1000), Value: nil}
			}
			if uint64(pre.Size()) > s.preambleMax() {
				pre.Grease = nil // a later stream carries it
			} else {
				s.greaseFirst = false
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
	return true
}

// floorPreamble is the preamble limit every zmux endpoint advertises at
// least (FloorParams).
const floorPreamble = 1024

// preambleMax is the largest preamble the peer accepts: its parameter,
// known or assumed for early data (remembered or the floor), else the
// floor.
func (s *Session) preambleMax() uint64 {
	if (s.peerSeen || s.earlyActive()) && s.peer.Has&wire.HasPreamble != 0 {
		return s.peer.Preamble
	}
	return floorPreamble
}

// preambleErr is the error of a preamble above lim.
func preambleErr(lim uint64) *StreamError {
	return &StreamError{Kind: StreamPreambleTooLarge, Code: lim}
}

// failPreamble fails a stream whose preamble the peer would not accept,
// before it is announced.
func (s *Session) failPreamble(st *Stream, lim uint64) {
	e := preambleErr(lim)
	if st.hasS && st.snd.errState == nil {
		st.snd.errState = e
	}
	if st.hasR && st.rcv.errState == nil {
		st.rcv.errState = e
	}
	s.cancelUnopened(st)
}

// editPreamble applies f to the local preamble and keeps the change only
// when the result fits the peer's limit.
func (s *Session) editPreamble(st *Stream, f func(ps *wire.PreambleSpec)) error {
	ps := st.preSpec()
	old := *ps
	f(ps)
	if lim := s.preambleMax(); uint64(ps.Size()) > lim {
		*ps = old
		return preambleErr(lim)
	}
	return nil
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
	if r := &st.rcv; st.hasR && r.adv == 0 {
		// Receive windows come from the local parameters, which are what
		// the peer knows: a window SetReceiveWindow raised before is
		// advertised by MAX_STREAM_DATA.
		l := &s.cfg.Local
		var w uint64
		switch {
		case !st.bidi:
			w = l.MaxStreamDataUni
		case st.local:
			w = l.MaxStreamDataBidiLoc
		default:
			w = l.MaxStreamDataBidiRem
		}
		r.adv = w
		if r.window == 0 {
			r.window = w
		}
	}
}

// PinID returns the identifier, pinning it before the first frame
// in the order identifiers are allocated.
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
	if !s.allocID(st) {
		return 0, st.snd.errState
	}
	st.pinned = true
	s.sched.ensure(st)
	s.wakeAllWriters()
	return st.id, nil
}

// Flush makes st's opening frame owed and reports the send offset that
// covers what is buffered now; FlushDone tells when it was sent. After
// establishment it pins the identifier (PinID); before, the opening frame
// goes as soon as the stream may open, in early data if that is in use.
// Data written before goes in the opening frame.
func (s *Session) Flush(st *Stream) (uint64, error) {
	sp := &st.snd
	switch {
	case !st.hasS:
		return 0, ErrInvalid
	case sp.errState != nil:
		return 0, sp.errState
	case s.err != nil:
		return 0, s.err
	}
	if !st.idSet {
		if st.cancelled {
			return 0, sp.errState
		}
		if s.peerSeen {
			if _, err := s.PinID(st); err != nil {
				return 0, err
			}
		} else {
			st.openWant = true
		}
	}
	s.becomeSend(st)
	return sp.q.end, nil
}

// FlushDone reports whether st's opening frame and its data up to target
// were sent (or are queued in the held first flight, which leaves with
// it), or the stream or session failed (err).
func (s *Session) FlushDone(st *Stream, target uint64, sig *Signal) (bool, error) {
	sp := &st.snd
	switch {
	case sp.reset || sp.errState != nil:
		return true, sp.errState
	case s.err != nil:
		return true, s.err
	case s.firstHeld:
		return true, nil
	case st.opened && sp.sent >= min64(target, sp.q.end):
		return true, nil
	}
	st.wsig = sig
	return false, nil
}

// ---- local preamble settings ----

func (s *Session) beforeFirstFrame(st *Stream) error {
	if !st.local || st.idSet || st.openWant {
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
	return s.editPreamble(st, func(ps *wire.PreambleSpec) {
		ps.OpenInfo = append([]byte(nil), b...)
		ps.HasOpenInfo = true
	})
}

// AddPreambleTLV adds an application TLV before the first frame.
func (s *Session) AddPreambleTLV(st *Stream, t uint64, v []byte) error {
	if !wire.IsAppTLV(t) {
		return ErrInvalid
	}
	if err := s.beforeFirstFrame(st); err != nil {
		return err
	}
	return s.editPreamble(st, func(ps *wire.PreambleSpec) {
		ps.App = append(ps.App, wire.TLV{Type: t, Value: append([]byte(nil), v...)})
	})
}

// SetPipeCarrier marks the stream as a carrier before the first frame.
func (s *Session) SetPipeCarrier(st *Stream) error {
	if err := s.beforeFirstFrame(st); err != nil {
		return err
	}
	return s.editPreamble(st, func(ps *wire.PreambleSpec) { ps.PipeCarrier = true })
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
// otherwise.
func (s *Session) SetPeerPriority(st *Stream, urgency uint8, incr bool) error {
	if urgency > 7 {
		return ErrInvalid
	}
	if st.local && !st.idSet {
		return s.editPreamble(st, func(ps *wire.PreambleSpec) {
			ps.HasPriority, ps.Urgency, ps.Incr = true, urgency, incr
		})
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
			if s.ga.closeWhenDone {
				s.ga.recheck = true
			}
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
