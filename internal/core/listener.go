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
	DefaultMaxPending          = 64
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

// PendingCarriers bounds the carriers of one listener that have not yet
// passed join verification: at most MaxPerSource per
// source, refused beyond it, and at most MaxTotal in all, a new carrier
// evicting the oldest when they are full; each is closed when its first
// record does not arrive within Timeout. A carrier holds its source slot
// (Enter) from its arrival, and a slot of the total (Take) only once it
// is read for its first record. Callers serialise access.
type PendingCarriers struct {
	MaxTotal     int      // 0: DefaultMaxPending
	MaxPerSource int      // 0: DefaultMaxPendingPerSource
	Timeout      Duration // 0: DefaultFirstRecordTimeout
	bySource     map[string]int
	stalled      map[string]int
	order        list.List // of *PendingCarrier, oldest first
}

// PendingCarrier is the slot of the total of one carrier.
type PendingCarrier struct {
	source string
	owns   bool // Admit took its source slot as well
	evict  func()
	elem   *list.Element // nil once released or evicted
}

func (p *PendingCarriers) maxPer() int {
	if p.MaxPerSource <= 0 {
		return DefaultMaxPendingPerSource
	}
	return p.MaxPerSource
}

// Enter takes a source slot for a carrier that arrived from source; false:
// the source holds its share, close the carrier. Leave gives it back.
func (p *PendingCarriers) Enter(source string) bool {
	if p.bySource == nil {
		p.bySource = map[string]int{}
	}
	if p.bySource[source] >= p.maxPer() {
		return false
	}
	p.bySource[source]++
	return true
}

// Held is the number of source slots source holds.
func (p *PendingCarriers) Held(source string) int { return p.bySource[source] }

// Leave gives back a source slot Enter took.
func (p *PendingCarriers) Leave(source string) {
	if n := p.bySource[source]; n > 1 {
		p.bySource[source] = n - 1
	} else {
		delete(p.bySource, source)
	}
}

// Take takes a slot of the total for a carrier that holds its source
// slot. evict closes the carrier when a newer one takes its slot; Take
// returns it as evicted instead of calling it, for the caller to call
// outside its lock.
func (p *PendingCarriers) Take(source string, evict func()) (c *PendingCarrier, evicted func()) {
	maxTotal := p.MaxTotal
	if maxTotal <= 0 {
		maxTotal = DefaultMaxPending
	}
	if f := p.order.Front(); f != nil && p.order.Len() >= maxTotal {
		old := f.Value.(*PendingCarrier)
		p.Release(old)
		evicted = old.evict
	}
	c = &PendingCarrier{source: source, evict: evict}
	c.elem = p.order.PushBack(c)
	return c, evicted
}

// Admit is Enter and Take in one: ok false, close the carrier. Release
// then gives back both slots.
func (p *PendingCarriers) Admit(source string, evict func()) (c *PendingCarrier, evicted func(), ok bool) {
	if !p.Enter(source) {
		return nil, nil, false
	}
	c, evicted = p.Take(source, evict)
	c.owns = true
	return c, evicted, true
}

// Release returns the slot of the total of a carrier that was classified
// as a new session or a lane, passed join verification, or was closed,
// and its source slot when Admit took it. Releasing a slot again, or one
// that was evicted, does nothing.
func (p *PendingCarriers) Release(c *PendingCarrier) {
	if c == nil || c.elem == nil {
		return
	}
	p.order.Remove(c.elem)
	c.elem = nil
	if c.owns {
		p.Leave(c.source)
	}
}

// Stall counts a carrier from source that an admission stalls; it keeps
// its source slot and takes no slot of the total, so it is neither
// evicted nor timed out. A source holds at most MaxPerSource - 1 stalled
// carriers, so that it keeps a slot for a new attempt: false, close the
// carrier at once. Unstall ends the count.
func (p *PendingCarriers) Stall(source string) bool {
	if p.stalled == nil {
		p.stalled = map[string]int{}
	}
	if p.stalled[source] >= p.maxPer()-1 {
		return false
	}
	p.stalled[source]++
	return true
}

// Unstall ends the count of a carrier Stall counted.
func (p *PendingCarriers) Unstall(source string) {
	if n := p.stalled[source]; n > 1 {
		p.stalled[source] = n - 1
	} else {
		delete(p.stalled, source)
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
// by their pairing token. The listener holds the first
// carrier of a pair until the opposite one arrives; a token is used for
// exactly one pair. Callers serialise access.
type SplitPairing struct {
	MaxTotal     int // 0: the default 64
	MaxPerSource int // 0: the default 8
	wait         map[[16]byte]pairWait
	used         map[[16]byte]bool
	bySource     map[string]int
}

type pairWait struct {
	up      bool
	source  string
	carrier any
}

// PairResult is the outcome of offering a carrier.
type PairResult uint8

const (
	PairWait   PairResult = iota // held until its partner arrives
	PairDone                     // paired: start the session on both carriers
	PairReject                   // close the carrier
)

// Offer presents a carrier with its pairing token, direction (up: the
// initiator writes on it) and source. On PairDone it returns the partner's
// carrier value.
func (sp *SplitPairing) Offer(token [16]byte, up bool, source string, carrier any) (PairResult, any) {
	if sp.wait == nil {
		sp.wait, sp.used, sp.bySource = map[[16]byte]pairWait{}, map[[16]byte]bool{}, map[string]int{}
	}
	if sp.used[token] {
		return PairReject, nil
	}
	if w, ok := sp.wait[token]; ok {
		if w.up == up {
			return PairReject, nil
		}
		delete(sp.wait, token)
		sp.release(w.source)
		sp.used[token] = true
		return PairDone, w.carrier
	}
	maxT, maxS := sp.MaxTotal, sp.MaxPerSource
	if maxT <= 0 {
		maxT = 64
	}
	if maxS <= 0 {
		maxS = 8
	}
	if len(sp.wait) >= maxT || sp.bySource[source] >= maxS {
		return PairReject, nil
	}
	sp.wait[token] = pairWait{up: up, source: source, carrier: carrier}
	sp.bySource[source]++
	return PairWait, nil
}

func (sp *SplitPairing) release(source string) {
	if n := sp.bySource[source]; n > 1 {
		sp.bySource[source] = n - 1
	} else {
		delete(sp.bySource, source)
	}
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
	sp.release(w.source)
	return true
}

// Forget releases a used token when its session ends.
func (sp *SplitPairing) Forget(token [16]byte) { delete(sp.used, token) }
