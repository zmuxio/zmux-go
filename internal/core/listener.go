package core

import "github.com/zmuxio/zmux-go/internal/wire"

// Carrier classification of a listener (MULTIPATH.md 5.5, SPEC.md 6.2).

// Class is the kind of a new carrier.
type Class uint8

const (
	ClassMore Class = iota // more bytes needed
	ClassSession
	ClassJoin
	ClassLane
	ClassReject
)

var (
	joinPrefix = [4]byte{0x9a, 0x24, 0xde, 0x36}
	lanePrefix = [4]byte{0xba, 0x05, 0xa1, 0xc9}
)

// Classify inspects the first bytes of a carrier incrementally and rejects
// a prefix as soon as it cannot begin a valid first record. When it
// returns a class other than ClassMore/ClassReject, recLen is the length of
// the first record (Size field included) once it is complete, else 0.
func Classify(b []byte, maxRecord uint64) (Class, int) {
	if len(b) == 0 {
		return ClassMore, 0
	}
	size, n := wire.ReadVarint(b)
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
			if match(joinPrefix[:]) && size >= 26 || match(lanePrefix[:]) && size >= 22 {
				return ClassMore, 0
			}
			return ClassMore, 0
		}
	case size >= 26 && match(joinPrefix[:]):
		c = ClassJoin
		if len(body) < 4 {
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

// ParseJoin decodes the PIPE_JOIN at the start of a first record body (or
// an out-of-band join) and returns it with its MAC input.
func ParseJoin(b []byte) (*wire.Join, []byte, int, bool) {
	var f wire.Frame
	n, err := wire.DecodeFrame(b, &f)
	if err != nil || f.Kind != wire.KindPipeJoin {
		return nil, nil, 0, false
	}
	j := f.Join
	j.Locator = append([]byte(nil), j.Locator...)
	j.Token = append([]byte(nil), j.Token...)
	return &j, append([]byte(nil), b[:j.MACLen]...), n, true
}

// Locator returns the session's own locator (joins name it).
func (s *Session) Locator() []byte { return s.cfg.Local.Locator }

// SkipJoinFrame marks the first record of an in-band join: its leading
// PIPE_JOIN was processed by the listener and is skipped (it still counts
// in the position).
func (s *Session) SkipJoinFrame(pipe uint64) {
	if p := s.pipe(pipe); p != nil && p.in != nil {
		p.in.lanes[0].skipJoin = true
	}
}

// PendingCarriers bounds the carriers of one listener that have not yet
// delivered a complete first record (MULTIPATH.md 5.6): at most
// MaxPerSource per source address; each is closed when its first record
// does not arrive within Timeout. Callers serialise access.
type PendingCarriers struct {
	MaxPerSource int      // 0: the default 8
	Timeout      Duration // 0: the default 2 s
	bySource     map[string]int
}

// Admit takes a slot for a new carrier from source; false: close it.
func (p *PendingCarriers) Admit(source string) bool {
	maxPer := p.MaxPerSource
	if maxPer <= 0 {
		maxPer = 8
	}
	if p.bySource == nil {
		p.bySource = map[string]int{}
	}
	if p.bySource[source] >= maxPer {
		return false
	}
	p.bySource[source]++
	return true
}

// Release returns the slot of a carrier whose first record arrived or that
// was closed.
func (p *PendingCarriers) Release(source string) {
	if n := p.bySource[source]; n > 1 {
		p.bySource[source] = n - 1
	} else {
		delete(p.bySource, source)
	}
}

// Deadline is when a carrier accepted at t must have its first record.
func (p *PendingCarriers) Deadline(t Time) Time {
	d := p.Timeout
	if d <= 0 {
		d = 2 * Second
	}
	return t.Add(d)
}

// SplitPairing pairs the two unidirectional carriers of a split bootstrap
// by their pairing token (SPEC.md 6.1.1). The listener holds the first
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
	up       bool
	source   string
	deadline Time
	carrier  any
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
func (sp *SplitPairing) Offer(token [16]byte, up bool, source string, carrier any, deadline Time) (PairResult, any) {
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
	sp.wait[token] = pairWait{up: up, source: source, deadline: deadline, carrier: carrier}
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

// Expire drops held carriers whose deadline passed and returns them.
func (sp *SplitPairing) Expire(now Time, dst []any) []any {
	for t, w := range sp.wait {
		if now >= w.deadline {
			delete(sp.wait, t)
			sp.release(w.source)
			dst = append(dst, w.carrier)
		}
	}
	return dst
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
