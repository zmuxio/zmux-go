package core

import (
	"container/list"
	"net/netip"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/varint"
)

// Carrier classification of a listener.

// Class is the kind of a new carrier.
type Class uint8

const (
	ClassMore Class = iota // more bytes needed
	ClassSession
	ClassJoin
	ClassLane
	ClassReject
)

var lanePrefix = [4]byte{0xba, 0x05, 0xa1, 0xc9}

// Classify inspects the first bytes of a carrier incrementally and rejects a prefix
// as soon as it cannot begin a valid first record or Hello. First byte 02 is a Hello,
// whose parser takes over; recLen stays 0. For another class recLen is the length of
// the first record (Size field included) once it is complete.
func Classify(b []byte, maxRecord uint64) (Class, int) {
	if len(b) == 0 {
		return ClassMore, 0
	}
	if b[0] == join.ProfileJoin {
		return ClassJoin, 0
	}
	size, n := varint.ConsumeVarint(b)
	if n == 0 {
		return ClassMore, 0
	}
	if size > maxRecord {
		return ClassReject, 0
	}
	body := b[n:]
	match := func(p []byte) bool {
		for i := 0; i < len(p) && i < len(body); i++ {
			if body[i] != p[i] {
				return false
			}
		}
		return true
	}
	var c Class
	switch {
	case size >= 9 && match(tpTypeBytes[:]):
		c = ClassSession
		if len(body) < 8 {
			return ClassMore, 0
		}
	case size >= 22 && match(lanePrefix[:]):
		c = ClassLane
		if len(body) < 4 {
			return ClassMore, 0
		}
	default:
		return ClassReject, 0
	}
	if uint64(len(body)) < size {
		return c, 0
	}
	return c, n + int(size)
}

// Locator returns the session's own locator (joins name it).
func (s *Session) Locator() []byte { return s.cfg.Local.Locator }

// Recommended limits on pending and unverified carriers.
const (
	DefaultMaxPending          = 1024
	DefaultMaxPendingPerSource = 8
	DefaultFirstRecordTimeout  = 2 * Second
	DefaultHandshakeTimeout    = 10 * Second
)

// Session defaults; the public package exports them.
const (
	DefaultKeepAlive   = 15 * Second // keep-alive period
	DefaultIdleTimeout = 30 * Second // max_idle_timeout
	DefaultProbeJitter = 0.25        // probe jitter j
)

// SourceKey is the source a client address counts against:
// an IPv6 address is aggregated to its /64 prefix, so that a client
// cannot rotate addresses within its prefix; an IPv4 address (also
// IPv4-mapped) stands for itself; anything else is kept as given.
func SourceKey(addr string) string {
	a, err := netip.ParseAddr(addr)
	if err != nil {
		return addr
	}
	a = a.Unmap().WithZone("")
	if a.Is4() {
		return a.String()
	}
	p, _ := a.Prefix(64)
	return p.String()
}

// PendingCarriers bounds the carriers of one listener before they belong
// to a session: at most MaxPerSource per source and MaxTotal in all, from
// a carrier's arrival until it enters a session, is closed or is handed to
// a fallback, a stall included. A new carrier that finds MaxTotal taken
// evicts the oldest stalled carrier, or is refused when none is stalled.
// Stalled carriers hold at most MaxPerSource - 1 slots of their source and
// half of MaxTotal. Callers serialise access.
type PendingCarriers struct {
	MaxTotal     int      // 0: DefaultMaxPending; negative: unlimited
	MaxPerSource int      // 0: DefaultMaxPendingPerSource
	Timeout      Duration // 0: DefaultFirstRecordTimeout
	bySource     map[string]int
	stalledBy    map[string]int
	total        int
	stalls       list.List // of *PendingSlot, the stalled ones, oldest first
}

// PendingSlot is the source slot and the slot of the total that one
// carrier holds.
type PendingSlot struct {
	source string
	held   bool
	stall  *list.Element // non-nil while stalled
	evict  func()
}

// EnterResult is the outcome of Enter.
type EnterResult uint8

const (
	EnterOK         EnterResult = iota
	EnterSourceFull             // the source holds its share: close the carrier
	EnterFull                   // every slot is taken and none is stalled
)

func (p *PendingCarriers) maxPer() int {
	if p.MaxPerSource <= 0 {
		return DefaultMaxPendingPerSource
	}
	return p.MaxPerSource
}

// maxTotal is the bound of the total; 0: none.
func (p *PendingCarriers) maxTotal() int {
	switch {
	case p.MaxTotal < 0:
		return 0
	case p.MaxTotal == 0:
		return DefaultMaxPending
	}
	return p.MaxTotal
}

// Enter takes a source slot and a slot of the total for a carrier that
// arrived from source. When every slot of the total is taken it evicts the
// oldest stalled carrier, whose slots are free at once, and returns that
// carrier's evict function for the caller to call outside its lock. Leave
// gives the slots back.
func (p *PendingCarriers) Enter(source string) (s *PendingSlot, evicted func(), r EnterResult) {
	if p.bySource == nil {
		p.bySource = map[string]int{}
	}
	if p.bySource[source] >= p.maxPer() {
		return nil, nil, EnterSourceFull
	}
	if m := p.maxTotal(); m > 0 && p.total >= m {
		f := p.stalls.Front()
		if f == nil {
			return nil, nil, EnterFull
		}
		old := f.Value.(*PendingSlot)
		evicted = old.evict
		p.Leave(old)
		if p.bySource[source] >= p.maxPer() { // the evicted carrier was of this source
			return nil, evicted, EnterSourceFull
		}
	}
	p.bySource[source]++
	p.total++
	return &PendingSlot{source: source, held: true}, evicted, EnterOK
}

// Held is the number of source slots source holds.
func (p *PendingCarriers) Held(source string) int { return p.bySource[source] }

// Total is the number of slots of the total taken.
func (p *PendingCarriers) Total() int { return p.total }

// Leave gives back the slots of s, ending its stall. Leaving a slot again,
// or one that was evicted, does nothing.
func (p *PendingCarriers) Leave(s *PendingSlot) {
	if s == nil || !s.held {
		return
	}
	p.Unstall(s)
	s.held = false
	if n := p.bySource[s.source]; n > 1 {
		p.bySource[s.source] = n - 1
	} else {
		delete(p.bySource, s.source)
	}
	p.total--
}

// Stall marks the carrier of s stalled: it keeps its slots, and evict
// closes it when a new carrier needs its slot of the total. A source holds
// at most MaxPerSource - 1 stalled carriers, so that it keeps a slot for a
// new attempt, and the listener at most half of MaxTotal: false, close the
// carrier at once. Unstall ends the stall; Leave ends it too.
func (p *PendingCarriers) Stall(s *PendingSlot, evict func()) bool {
	if s == nil || !s.held || s.stall != nil {
		return false
	}
	if p.stalledBy == nil {
		p.stalledBy = map[string]int{}
	}
	if p.stalledBy[s.source] >= p.maxPer()-1 {
		return false
	}
	if m := p.maxTotal(); m > 0 && p.stalls.Len() >= m/2 {
		return false
	}
	p.stalledBy[s.source]++
	s.evict = evict
	s.stall = p.stalls.PushBack(s)
	return true
}

// Unstall ends the stall of s; it does nothing for a slot not stalled.
func (p *PendingCarriers) Unstall(s *PendingSlot) {
	if s == nil || s.stall == nil {
		return
	}
	p.stalls.Remove(s.stall)
	s.stall, s.evict = nil, nil
	if n := p.stalledBy[s.source]; n > 1 {
		p.stalledBy[s.source] = n - 1
	} else {
		delete(p.stalledBy, s.source)
	}
}

// Deadline is when a carrier accepted at t must have its first record.
func (p *PendingCarriers) Deadline(t Time) Time {
	d := p.Timeout
	if d <= 0 {
		d = DefaultFirstRecordTimeout
	}
	return t.Add(d)
}

// SplitPairing pairs the two unidirectional carriers of a split bootstrap
// by their pairing token. The listener holds the first carrier of a pair
// until the opposite one arrives; a token is used for exactly one pair.
// The waiting carriers are bounded by the slots they hold
// (PendingCarriers). Callers serialise access.
type SplitPairing struct {
	wait map[[16]byte]pairWait
	used map[[16]byte]bool
}

type pairWait struct {
	up      bool
	carrier any
}

// PairResult is the outcome of offering a carrier.
type PairResult uint8

const (
	PairWait   PairResult = iota // held until its partner arrives
	PairDone                     // paired: start the session on both carriers
	PairReject                   // close the carrier
)

// Offer presents a carrier with its pairing token and direction (up: the
// initiator writes on it). On PairDone it returns the partner's carrier
// value.
func (sp *SplitPairing) Offer(token [16]byte, up bool, carrier any) (PairResult, any) {
	if sp.wait == nil {
		sp.wait, sp.used = map[[16]byte]pairWait{}, map[[16]byte]bool{}
	}
	if sp.used[token] {
		return PairReject, nil
	}
	if w, ok := sp.wait[token]; ok {
		if w.up == up {
			return PairReject, nil
		}
		delete(sp.wait, token)
		sp.used[token] = true
		return PairDone, w.carrier
	}
	sp.wait[token] = pairWait{up: up, carrier: carrier}
	return PairWait, nil
}

// Withdraw removes a held carrier that gave up waiting (timeout or
// cancel): true when it was still held, false when a partner already took
// it (the session then owns the carrier).
func (sp *SplitPairing) Withdraw(token [16]byte, carrier any) bool {
	w, ok := sp.wait[token]
	if !ok || w.carrier != carrier {
		return false
	}
	delete(sp.wait, token)
	return true
}

// Forget releases a used token when its session ends.
func (sp *SplitPairing) Forget(token [16]byte) { delete(sp.used, token) }
