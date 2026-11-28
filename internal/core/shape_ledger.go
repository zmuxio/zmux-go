package core

import (
	"cmp"
	"math"
	"slices"

	"github.com/zmuxio/zmux-go/shaping"
)

// Bins use the original session clock. A live reservation has no expiry until
// its carrier reports a terminal result, even after its dispatch bin ages out.
type shapeLedger struct {
	handshake          shaping.HandshakeMeters
	bins               []shapeBin
	unknownBins        int
	reserved, measured uint64
	discardedThrough   int64 // one past the latest discarded bin with charges
	discarded          bool
}

type shapeBin struct {
	unknown                                     bool
	second                                      int64
	baseline, consumed, provisional, unresolved uint64
}

type shapeDispatch struct {
	second int64
	extra  uint64
}

func (s *Session) shapeSecond() int64 { return s.now.Sub(s.start) / Second }

func (s *Session) shapeProfiles() (policy, local *shaping.ProfileLimits) {
	return s.cfg.ShapeBudget.Profile, s.cfg.ShapeLimits.Profile
}

func (s *Session) shapeWindow() uint64 {
	p, l := s.shapeProfiles()
	w := uint64(0)
	if p != nil {
		w = uint64(p.WindowSeconds)
	}
	if l != nil {
		w = max(w, uint64(l.WindowSeconds))
	}
	if w == 0 {
		w = uint64(shaping.Lightweight().WindowSeconds)
	}
	return w
}

func (l *shapeLedger) forget(second int64) {
	if !l.discarded || second >= l.discardedThrough {
		l.discardedThrough = second + 1
		l.discarded = true
	}
}

func (s *Session) pruneShape() int64 {
	l := &s.shapeLedger
	now, window := s.shapeSecond(), s.shapeWindow()
	first := now - int64(window)
	for len(l.bins) > 0 && l.bins[0].second < first {
		b := l.bins[0]
		if b.unknown {
			l.unknownBins--
		}
		if b.consumed != 0 || b.unknown {
			l.forget(b.second)
		}
		l.bins = l.bins[1:]
	}
	return first
}

func (s *Session) shapeSnapshot(p *shaping.ProfileLimits, own uint64) shaping.WindowMeters {
	if p == nil {
		return shaping.WindowMeters{}
	}
	l := &s.shapeLedger
	now := s.shapeSecond()
	w := int64(p.WindowSeconds)
	m := shaping.WindowMeters{ProfileLimits: *p, ReservedBytes: l.reserved - own, HistoryComplete: !l.discarded || now-w >= l.discardedThrough}
	for i := len(l.bins) - 1; i >= 0; i-- {
		b := &l.bins[i]
		age := now - b.second
		if age > w {
			break
		}
		if age < w {
			m.BaselineBytes = satAdd(m.BaselineBytes, b.baseline)
		}
		m.ConsumedBytes = satAdd(m.ConsumedBytes, b.consumed)
		m.ReservedBytes = satAdd(m.ReservedBytes, b.unresolved)
	}
	m.AllowanceBytes = p.Allowance(m.BaselineBytes)
	return m
}

func (s *Session) shapeRoom() uint64 {
	return s.shapeRoomExcluding(0)
}

// Recheck a live intent against all charges except its own reservation.
func (s *Session) shapeRoomExcluding(own uint64) uint64 {
	if s.shapePrecision() == shaping.CostUnavailable {
		return 0
	}
	room := uint64(math.MaxUint64)
	p, l := s.shapeProfiles()
	for _, profile := range [...]*shaping.ProfileLimits{p, l} {
		if profile == nil {
			continue
		}
		m := s.shapeSnapshot(profile, own)
		if !m.HistoryComplete {
			return 0
		}
		room = min(room, m.AllowanceBytes-min(m.AllowanceBytes, satAdd(m.ConsumedBytes, m.ReservedBytes)))
	}
	return room
}

func (s *Session) reserveShape(extra uint64) shapeDispatch {
	s.shapeLedger.reserved = satAdd(s.shapeLedger.reserved, extra)
	return shapeDispatch{second: s.shapeSecond(), extra: extra}
}

func (s *Session) shapeBin(second int64) *shapeBin {
	l := &s.shapeLedger
	i, ok := slices.BinarySearchFunc(l.bins, second, func(b shapeBin, v int64) int { return cmp.Compare(b.second, v) })
	if !ok {
		l.bins = slices.Insert(l.bins, i, shapeBin{second: second})
	}
	return &l.bins[i]
}

func (s *Session) settleShape(d shapeDispatch, baseline, extra uint64) {
	l := &s.shapeLedger
	l.reserved -= d.extra
	l.measured = satAdd(l.measured, extra)
	if d.second < s.pruneShape() {
		if extra > 0 {
			l.forget(d.second)
		}
		return
	}
	if baseline|extra != 0 {
		b := s.shapeBin(d.second)
		b.baseline = satAdd(b.baseline, baseline)
		b.consumed = satAdd(b.consumed, extra)
	}
}

// An unidentified terminal result keeps the unproved increment in its original
// dispatch bin. Consumption only ever includes a proved carrier prefix.
func (s *Session) retainUnknown(second int64, unresolved uint64) {
	if second < s.pruneShape() {
		s.shapeLedger.forget(second)
		return
	}
	b := s.shapeBin(second)
	if !b.unknown {
		b.unknown = true
		s.shapeLedger.unknownBins++
	}
	b.unresolved = satAdd(b.unresolved, unresolved)
}

// finishEarlyShape applies a carrier verdict to the original dispatch bins.
func (s *Session) finishEarlyShape(accepted bool) {
	for i := range s.shapeLedger.bins {
		b := &s.shapeLedger.bins[i]
		if accepted {
			b.baseline = satAdd(b.baseline, b.provisional)
		}
		b.provisional = 0
	}
}

func (s *Session) shapePrecision() shaping.Precision {
	s.pruneShape()
	if s.shapeLedger.unknownBins != 0 {
		return shaping.CostUnavailable
	}
	return shaping.CostExact
}

// JoinWrite carries the plan and the resulting reservation through a single
// handshake write. The session owner calls Complete with its terminal prefix.
type JoinWrite struct {
	shaping.UnitPlan
	Changed  <-chan struct{}
	s        *Session
	reserved uint64
	second   int64
	offset   int
}

// recheck drops the optional layout if its policy changed during the callback or wait.
func (w *JoinWrite) recheck() {
	if w != nil {
		select {
		case <-w.Changed:
			w.UnitPlan = shaping.UnitPlan{}
		default:
		}
	}
}
func (w *JoinWrite) prepare(s *Session, extra, offset int) {
	if w != nil {
		w.s, w.reserved, w.offset, w.second = s, uint64(extra), offset, s.shapeSecond()
		h := &s.shapeLedger.handshake
		h.ReservedBytes = satAdd(h.ReservedBytes, w.reserved)
	}
}

// Complete settles this message once, using only its proved carrier prefix.
func (w *JoinWrite) Complete(prefix int, unknown bool) {
	if w == nil || w.s == nil {
		return
	}
	h := &w.s.shapeLedger.handshake
	extra := min(w.reserved, uint64(max(prefix-w.offset, 0)))
	h.ReservedBytes -= w.reserved
	h.ConsumedBytes = satAdd(h.ConsumedBytes, extra)
	w.s.settleShape(shapeDispatch{second: w.second}, uint64(max(0, prefix))-extra, 0)
	if unknown {
		h.UnresolvedBytes = satAdd(h.UnresolvedBytes, w.reserved-extra)
	}
	w.s = nil
}
