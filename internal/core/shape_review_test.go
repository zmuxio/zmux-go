package core

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// An answer that repeats a PauseUntil a limit shortened counts it once.
func TestRepeatedPauseCountsOnce(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.cfg.ShapeLimits.MaxDelay = 10 * time.Millisecond
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return &gateAtShaper{at: 1 << 62} })
	s.SetNow(Time(Second))
	ps := p.sh
	if ps == nil {
		s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return &gateAtShaper{at: 1 << 62} })
		ps = p.sh
	}
	if ps == nil {
		t.Fatal("no pipe shape")
	}
	r := shaping.Effects{Set: shaping.EffectGate, PauseUntil: time.Duration(Second) + time.Second}
	ps.apply(&r)
	hold := ps.hold
	for range 5 {
		s.SetNow(s.now.Add(Duration(time.Millisecond)))
		ps.apply(&r)
	}
	if ps.hold != hold || s.shp.stats.StallSpent != 10*time.Millisecond {
		t.Fatalf("hold moved by %v, StallSpent %v", ps.hold.Sub(hold), s.shp.stats.StallSpent)
	}
	r.PauseUntil += 5 * time.Millisecond
	ps.apply(&r)
	if ps.hold.Sub(hold) != Duration(5*time.Millisecond) || s.shp.stats.StallSpent != 15*time.Millisecond {
		t.Fatalf("extension: hold moved by %v, StallSpent %v", ps.hold.Sub(hold), s.shp.stats.StallSpent)
	}
}

// A record planned in the middle of a write and delayed, which then opens a later
// write, carries that write's overhead in its lengths.
func TestDelayedRecordFitsWhereItOpens(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return &gateAtShaper{at: 1 << 62} })
	ps := p.sh
	if ps == nil {
		t.Fatal("no pipe shape")
	}
	ps.sizeField, ps.over = true, 0
	ps.wireMaxLen, ps.wirePadTo = 0, 300
	ps.plan = shaping.RecordPlan{}
	ps.plan.PadTo = 300
	var w recWriter
	w.init(GetBatch(), FramingBytes, 16000, 1<<16)
	defer PutBatch(w.b)
	w.sh = ps
	over := 7
	p.out.lanes[0].info.WriteOverhead = over
	if got := writeOverhead(p); got != over {
		t.Skipf("overhead source: %d", got)
	}
	ps.planned = true
	ps.prep(&w)
	if ps.over != over {
		t.Fatalf("over %d", ps.over)
	}
	if n := ps.padFor(100); wireOf(100+n, true, over) != 300 {
		t.Fatalf("wire length %d", wireOf(100+n, true, over))
	}
}

// An injection added to an open record adds its frames bytes across a size-field
// boundary too.
func TestInjectionFramesAcrossVarintBoundary(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.SetShaper(func(uint64, uint8, int) shaping.Shaper { return &gateAtShaper{at: 1 << 62} })
	ps := p.sh
	ps.sizeField, ps.over = true, 0
	ps.plan = shaping.RecordPlan{}
	ps.injPad = 10
	if n := ps.padFor(60); n != 10 { // 60 and 70 frames bytes straddle a one-byte size
		t.Fatalf("pad %d", n)
	}
}
