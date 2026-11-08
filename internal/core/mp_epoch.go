package core

import (
	"bytes"
	"crypto/rand"
	"strconv"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// Session epochs: the session secret evolves by
// key exchanges in SESSION_EPOCH frames. The state is the session's: at most
// one exchange, one set of private values, two epoch secrets and two local
// Locators at a time.

// Default epoch policy (the development notes): an exchange every hour or every
// 2^16 joins.
const (
	DefaultEpochPeriod = 3600 * Second
	DefaultEpochJoins  = 1 << 16
)

// epoch is one epoch an endpoint accepts.
type epoch struct {
	n      uint64
	secret []byte
	keys   map[uint64]*joinKeys // per usable Suite
	local  []byte               // the local Locator joins of this epoch name
	peer   []byte               // the peer's Locator for this epoch
}

func (e *epoch) wipe() {
	clear(e.secret)
	for _, k := range e.keys {
		clear(k.i)
		clear(k.r)
	}
	e.keys = nil
}

type epochState uint8

const (
	epStable    epochState = iota
	epOffering             // O: offer(d+1) sent
	epAnswered             // A: d+1 committed and answered; new joins still use d
	epRetaining            // both: new joins use d+1; d is deleted at dropAt
)

// MaxJoinDeadline is the longest join deadline any join may have, and
// EpochRetention, three of them, how long an endpoint keeps accepting the
// previous epoch after it starts using the new one.
const (
	MaxJoinDeadline = 60 * Second
	EpochRetention  = 3 * MaxJoinDeadline
)

type epochs struct {
	d      uint64
	list   []*epoch // the accepted epochs, ascending
	join   *epoch   // the epoch new local joins use
	crypto []byte   // session_crypto
	// The exchange: the private values of an offer, the bodies, the frame
	// repeated until the next phase and the last phase of d+1 processed or sent.
	offer     *join.Offer
	offerBody []byte
	frame     []byte
	last      int
	nextLocal []byte // the local Locator announced for d+1, nil to keep
	dropAt    Time   // Retaining: when d is deleted
	st        epochState
	// Policy: an exchange asked for (AdvanceEpoch) or due.
	advance     bool
	firstPipe   bool // the first Sealed or Bound pipe was seen
	stableSince Time
	joins       uint64 // joins since the last Stable
}

// byPhase is the accepted epoch of a key phase; at most one matches.
func (ep *epochs) byPhase(phase uint8) *epoch {
	for _, e := range ep.list {
		if uint8(e.n&1) == phase {
			return e
		}
	}
	return nil
}

// AcceptsLocator reports whether a Hello naming loc is for this session: the
// Locator of an accepted epoch.
func (s *Session) AcceptsLocator(loc []byte) bool {
	if s.mp == nil || len(s.mp.ep.list) == 0 {
		return bytes.Equal(loc, s.cfg.Local.Locator)
	}
	for _, e := range s.mp.ep.list {
		if bytes.Equal(e.local, loc) {
			return true
		}
	}
	return false
}

// Epoch returns the epoch new local joins use.
func (s *Session) Epoch() uint64 {
	if s.mp == nil || s.mp.ep.join == nil {
		return 0
	}
	return s.mp.ep.join.n
}

// AdvanceEpoch starts an exchange now when the session is Stable, or right after
// the one in progress.
func (s *Session) AdvanceEpoch() error {
	if s.isClosing() {
		return s.err
	}
	if s.mp == nil {
		return ErrNotNegotiated
	}
	if s.mp.keysPending {
		return s.joinPendingErr()
	}
	s.mp.ep.advance = true
	s.epochProgress()
	return nil
}

// epochPipe reports whether SESSION_EPOCH may go on p: a Sealed or Bound pipe.
func epochPipe(p *Pipe) bool {
	return p.mp != nil && (p.mp.mode == join.Sealed || p.mp.mode == join.Bound)
}

// newLocator is the local Locator of epoch n: the instance prefix and random
// bytes, at least 8, or for traces the configured prefix and n in decimal.
func (s *Session) newLocator(n uint64) []byte {
	if len(s.cfg.Local.Locator) == 0 {
		return nil // this end is not joined: it keeps having none
	}
	if p := s.cfg.Test.EpochLocatorPrefix; p != nil {
		return strconv.AppendUint(bytes.Clone(p), n, 10)
	}
	pre := s.cfg.LocatorPrefix
	l := max(len(s.cfg.Local.Locator), len(pre)+8)
	b := make([]byte, l)
	copy(b, pre)
	_, _ = rand.Read(b[len(pre):]) // crypto/rand does not fail
	return b
}

func (s *Session) setFrame(b []byte) {
	s.mp.ep.frame = b
	for _, p := range s.pipes {
		if p.mp != nil {
			p.mp.epochSent = false
		}
	}
	s.wakeAllWriters()
}

// startOffer writes offer(d+1) with the share of the usable Group of highest
// priority.
func (s *Session) startOffer() *wire.Error {
	m := s.mp
	ep := &m.ep
	g := join.HighestGroup(m.usableGroups)
	o, err := g.NewOffer(nil)
	if err != nil {
		return &wire.Error{Code: wire.InternalError, Reason: "epoch offer: " + err.Error()}
	}
	ep.nextLocal = s.newLocator(ep.d + 1)
	f := wire.AppendSessionEpoch(nil, wire.EpochOffer, ep.d+1, o.Share, ep.nextLocal)
	ep.offer, ep.offerBody, ep.st, ep.last, ep.advance = o, bodyOf(f), epOffering, -1, false
	s.setFrame(f)
	return nil
}

// bodyOf is the body of an encoded SESSION_EPOCH: the bytes after its type.
func bodyOf(f []byte) []byte {
	_, n := varint.ConsumeVarint(f)
	return f[n:]
}

// commit derives and accepts epoch d+1 from the two bodies and shared secrets.
func (s *Session) commit(offerBody, answerBody []byte, ss [][]byte, localLoc, peerLoc []byte) *wire.Error {
	ep := &s.mp.ep
	cur := ep.list[len(ep.list)-1]
	e := &epoch{n: ep.d + 1, secret: join.NextEpochSecret(cur.secret, ep.d+1, ep.crypto, offerBody, answerBody, ss...),
		local: cur.local, peer: cur.peer}
	for _, x := range ss {
		clear(x)
	}
	if len(localLoc) > 0 {
		e.local = localLoc
	}
	if len(peerLoc) > 0 {
		e.peer = bytes.Clone(peerLoc)
	}
	if err := s.deriveEpochKeys(e); err != nil {
		return err
	}
	ep.list = append(ep.list, e)
	s.emit(Event{Kind: EvEpochCommitted, Code: e.n})
	if !bytes.Equal(e.local, cur.local) {
		s.emit(Event{Kind: EvLocatorAdded, Frame: e.local})
	}
	return nil
}

// deleteOld deletes epoch d and enters Stable(d+1).
func (s *Session) deleteOld() {
	s.dropOld()
	s.enterStable()
}

// dropOld deletes epoch d, its keys and its Locators: only d+1 is accepted.
func (s *Session) dropOld() {
	ep := &s.mp.ep
	if len(ep.list) < 2 {
		return
	}
	old := ep.list[0]
	ep.list = ep.list[1:]
	ep.join = ep.list[0]
	if !bytes.Equal(old.local, ep.join.local) {
		s.emit(Event{Kind: EvLocatorRemoved, Frame: old.local})
	}
	old.wipe()
	s.emit(Event{Kind: EvEpochDeleted, Code: old.n})
}

// enterStable ends the exchange: Stable(d+1).
func (s *Session) enterStable() {
	ep := &s.mp.ep
	ep.d++
	ep.st, ep.last, ep.frame, ep.offer, ep.offerBody, ep.nextLocal = epStable, -1, nil, nil, nil, nil
	ep.stableSince, ep.joins = s.now, 0
}

// useNext makes new joins use d+1 and starts the retention of d.
func (s *Session) useNext() {
	ep := &s.mp.ep
	ep.join, ep.st = ep.list[len(ep.list)-1], epRetaining
	ep.dropAt = s.now.Add(EpochRetention)
}

// epochAnswerAcked is called when a record carrying the answer(n) is
// acknowledged: the answer is no longer repeated, and A switches.
func (s *Session) epochAnswerAcked(n uint64) {
	m := s.mp
	if m == nil || n != m.ep.d+1 || m.ep.last != wire.EpochAnswer {
		return
	}
	if m.ep.st == epAnswered || m.ep.st == epRetaining {
		m.ep.frame = nil
	}
	if m.ep.st == epAnswered {
		s.useNext()
	}
}

// epochPeerJoined is A's switch when a join of epoch n from the peer verifies.
func (s *Session) epochPeerJoined(n uint64) {
	if m := s.mp; m != nil && m.ep.st == epAnswered && n == m.ep.d+1 {
		s.useNext()
	}
}

// epochProgress takes the steps that wait for time: the deletion of d after
// the retention, and an asked or due exchange once Stable.
func (s *Session) epochProgress() {
	m := s.mp
	if m == nil || m.keysPending || len(m.ep.list) == 0 || s.isClosing() {
		return
	}
	ep := &m.ep
	if ep.st == epRetaining && s.now >= ep.dropAt {
		s.deleteOld()
	}
	if ep.st == epStable {
		d, n := s.epochPeriod(), s.epochJoins()
		due := d >= 0 && s.now >= ep.stableSince.Add(d) || n >= 0 && ep.joins >= uint64(n)
		if ep.advance || due {
			if err := s.startOffer(); err != nil {
				s.failLocal(err)
			}
		}
	}
}

// epochPeriod is the period of the policy, negative: off.
func (s *Session) epochPeriod() Duration {
	if s.cfg.EpochPeriod != 0 {
		return s.cfg.EpochPeriod
	}
	return DefaultEpochPeriod
}

// epochJoins is the join count of the policy, negative: off.
func (s *Session) epochJoins() int64 {
	if s.cfg.EpochJoins != 0 {
		return s.cfg.EpochJoins
	}
	return DefaultEpochJoins
}

// epochPipeJoined notes a joined pipe of mode: the first Sealed or Bound pipe
// of a session whose secret comes from a TLS exporter or a pre-shared key, not
// from the secure carrier, asks for one exchange.
func (s *Session) epochPipeJoined(mode join.Mode) {
	m := s.mp
	if m == nil || mode == join.Trusted || m.ep.firstPipe {
		return
	}
	m.ep.firstPipe = true
	if !s.cfg.NoEpochAfterFirstPipe && !s.cfg.SecureFirstPipe {
		m.ep.advance = true
		s.epochProgress()
	}
}

// epochResumed asks for one exchange after a suspension ends, when Stable.
func (s *Session) epochResumed() {
	if m := s.mp; m != nil && !s.cfg.NoEpochAfterResume && m.ep.st == epStable {
		m.ep.advance = true
		s.epochProgress()
	}
}

// epochJoined counts a join for the policy.
func (s *Session) epochJoined() {
	if s.mp != nil {
		s.mp.ep.joins++
	}
}

// onSessionEpoch processes a SESSION_EPOCH frame received on p.
func (s *Session) onSessionEpoch(p *Pipe, f *wire.Frame) *wire.Error {
	m := s.mp
	if m == nil || m.keysPending || len(m.ep.list) == 0 {
		return nil // join pending: no session secret yet
	}
	if !epochPipe(p) {
		return wire.NewError(wire.ProtocolViolation, "SESSION_EPOCH on a pipe that is neither Sealed nor Bound", "pipe", p.id)
	}
	ep := &m.ep
	e, phase := f.Seq, int(f.Status)
	pv := func(why string) *wire.Error {
		return wire.NewError(wire.ProtocolViolation, "SESSION_EPOCH "+why, "epoch", e)
	}
	switch {
	case e <= ep.d:
		return nil
	case e == ep.d+2 && phase == wire.EpochOffer && ep.st != epStable && ep.st != epOffering:
		// The peer deleted d before offering: end this exchange first.
		if ep.st == epAnswered {
			s.useNext()
		}
		s.deleteOld()
		return s.onSessionEpoch(p, f)
	case e >= ep.d+2:
		return pv("epoch too far ahead")
	}
	// e == d+1
	if phase <= ep.last {
		return nil // a copy
	}
	g := join.HighestGroup(m.usableGroups)
	switch {
	case phase == wire.EpochOffer && ep.st == epOffering:
		if s.role == RoleInitiator {
			return nil // collision: the initiator's offer stands
		}
		// The responder yields: its offer and private values are dropped.
		ep.offer, ep.offerBody, ep.st = nil, nil, epStable
		return s.answerOffer(f, g)
	case phase == wire.EpochOffer && ep.st == epStable:
		return s.answerOffer(f, g)
	case phase == wire.EpochAnswer && ep.st == epOffering:
		if len(f.Share) != g.ShareR {
			return pv("share length")
		}
		ss, err := ep.offer.Finish(f.Share)
		ep.offer = nil
		if err != nil {
			return pv("share refused")
		}
		if werr := s.commit(ep.offerBody, f.Body, ss, ep.nextLocal, f.Locator); werr != nil {
			return werr
		}
		// New joins use d+1; d stays accepted for the retention.
		ep.last, ep.frame = phase, nil
		s.useNext()
		return nil
	}
	return pv("in this state")
}

// answerOffer commits d+1 from a peer's offer and writes the answer.
func (s *Session) answerOffer(f *wire.Frame, g *join.Group) *wire.Error {
	ep := &s.mp.ep
	if len(f.Share) != g.ShareI {
		return wire.NewError(wire.ProtocolViolation, "SESSION_EPOCH share length", "epoch", f.Seq)
	}
	share, ss, err := g.Respond(f.Share, nil)
	if err != nil {
		return wire.NewError(wire.ProtocolViolation, "SESSION_EPOCH share refused", "epoch", f.Seq)
	}
	loc := s.newLocator(ep.d + 1)
	ans := wire.AppendSessionEpoch(nil, wire.EpochAnswer, ep.d+1, share, loc)
	if werr := s.commit(f.Body, bodyOf(ans), ss, loc, f.Locator); werr != nil {
		return werr
	}
	ep.st, ep.last = epAnswered, wire.EpochAnswer
	s.setFrame(ans)
	return nil
}

// writeEpoch puts the frame of the current phase on p once per probe
// interval; a record carrying an answer is tracked for its acknowledgement.
func (s *Session) writeEpoch(w *recWriter, p *Pipe) {
	m := s.mp
	a := p.mp
	if m == nil || !epochPipe(p) || a.out != OutActive {
		return
	}
	ep := &m.ep
	if ep.frame != nil && !a.epochSent {
		f := ep.frame
		if !w.frame(len(f), true, func(b []byte) []byte { return append(b, f...) }) {
			return
		}
		a.epochSent, a.epochAt = true, s.now.Add(s.epochRepeat(p))
		// A answers until a record carrying the answer is acknowledged, also
		// after a peer join of d+1 switched it (Retaining); O's frame is gone
		// by then.
		if ep.st == epAnswered || ep.st == epRetaining {
			w.epochAnswer = ep.d + 1
		}
	}
}

// epochRepeat is the probe interval of p, or 15 s where its probes are off.
func (s *Session) epochRepeat(p *Pipe) Duration {
	if k := s.probeInterval(p); k > 0 {
		return k
	}
	return 15 * Second
}

// epochTimers re-arms the repeat of each pipe and finds the policy deadlines.
func (s *Session) epochTimers(apply bool, at func(Time) bool) {
	m := s.mp
	if m == nil || m.keysPending || len(m.ep.list) == 0 {
		return
	}
	ep := &m.ep
	if ep.frame != nil {
		for _, p := range s.pipes {
			if a := p.mp; a != nil && a.epochSent && epochPipe(p) && at(a.epochAt) && apply {
				a.epochSent = false
				s.wakeAllWriters()
			}
		}
	}
	switch ep.st {
	case epRetaining:
		if at(ep.dropAt) && apply {
			s.epochProgress()
		}
	case epStable:
		if d := s.epochPeriod(); d >= 0 && at(ep.stableSince.Add(d)) && apply {
			s.epochProgress()
		}
	case epOffering, epAnswered:
		// No deadline: the peer's frames move these on, and the repeat
		// above sends the EPOCH frame again until they do.
	}
}
